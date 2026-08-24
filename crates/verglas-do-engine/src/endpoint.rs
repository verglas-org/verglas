//! Private Unix-socket control and apply endpoint for one `verglasd` replica.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use uuid::Uuid;

use crate::{
    CommitAuthority, DoEngine, DoSession, Error, IsolationLevel, LeaseIdentity,
    OffloadBatchArchive, OffloadBatchPolicy, SqliteReplicaStore, TableId, TransactionEnvelope,
};

const MAX_COMMAND_BYTES: usize = 40 * 1024 * 1024;

/// Worker role enforced by one supervised replica endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaEndpointRole {
    /// Owns one stateful Worker and may execute its events.
    Worker,
    /// Provides optional durable writes and fenced snapshot reads only.
    Replica,
}

impl ReplicaEndpointRole {
    /// Returns the stable control-protocol role label.
    fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Replica => "replica",
        }
    }
}

/// Socket binding or request I/O failure for one child process.
#[derive(Debug, thiserror::Error)]
pub enum ReplicaEndpointError {
    /// Local socket I/O failed.
    #[error("replica endpoint I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

/// One private child endpoint backed by the replica's durable SQLite pager.
pub struct ReplicaEndpoint {
    path: PathBuf,
    do_id: String,
    replica_id: u64,
    role: ReplicaEndpointRole,
    store: Arc<SqliteReplicaStore>,
    engine: Option<Arc<DoEngine>>,
    archive: Option<Arc<dyn OffloadBatchArchive>>,
    listener: UnixListener,
}

impl ReplicaEndpoint {
    /// Binds a fresh child-exclusive Unix socket around one durable replica store.
    pub async fn bind(
        path: impl AsRef<Path>,
        do_id: impl Into<String>,
        replica_id: u64,
        role: ReplicaEndpointRole,
        store: Arc<SqliteReplicaStore>,
    ) -> Result<Self, ReplicaEndpointError> {
        Self::bind_with(path, do_id, replica_id, role, store, None, None).await
    }

    /// Binds a stateful worker endpoint to its sole configured commit authority.
    pub async fn bind_worker(
        path: impl AsRef<Path>,
        do_id: impl Into<String>,
        replica_id: u64,
        store: Arc<SqliteReplicaStore>,
        authority: Arc<dyn CommitAuthority>,
    ) -> Result<Self, ReplicaEndpointError> {
        Self::bind_worker_with_archive(path, do_id, replica_id, store, authority, None).await
    }

    /// Binds a worker with optional managed compacted durability offload.
    pub async fn bind_worker_with_archive(
        path: impl AsRef<Path>,
        do_id: impl Into<String>,
        replica_id: u64,
        store: Arc<SqliteReplicaStore>,
        authority: Arc<dyn CommitAuthority>,
        archive: Option<Arc<dyn OffloadBatchArchive>>,
    ) -> Result<Self, ReplicaEndpointError> {
        let do_id = do_id.into();
        let engine = Arc::new(
            DoEngine::open_persistent(do_id.clone(), authority, store.clone()).map_err(
                |error| std::io::Error::other(format!("worker engine recovery failed: {error}")),
            )?,
        );
        Self::bind_with(
            path,
            do_id,
            replica_id,
            ReplicaEndpointRole::Worker,
            store,
            Some(engine),
            archive,
        )
        .await
    }

    /// Implements shared socket validation for worker and replica endpoints.
    async fn bind_with(
        path: impl AsRef<Path>,
        do_id: impl Into<String>,
        replica_id: u64,
        role: ReplicaEndpointRole,
        store: Arc<SqliteReplicaStore>,
        engine: Option<Arc<DoEngine>>,
        archive: Option<Arc<dyn OffloadBatchArchive>>,
    ) -> Result<Self, ReplicaEndpointError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let do_id = do_id.into();
        if store.do_id() != do_id {
            return Err(std::io::Error::other(format!(
                "pager belongs to {}, endpoint requested {do_id}",
                store.do_id()
            ))
            .into());
        }
        let listener = UnixListener::bind(&path)?;
        Ok(Self {
            path,
            do_id,
            replica_id,
            role,
            store,
            engine,
            archive,
            listener,
        })
    }

    /// Returns the child-exclusive bound Unix socket path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns this host's stable replica identity.
    pub fn replica_id(&self) -> u64 {
        self.replica_id
    }

    /// Returns the worker engine used by the event socket, when this is a worker.
    pub fn engine(&self) -> Option<Arc<DoEngine>> {
        self.engine.clone()
    }

    /// Accepts and executes one bounded newline-delimited private request.
    pub async fn serve_once(&mut self) -> Result<(), ReplicaEndpointError> {
        let (stream, _) = self.listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut command = Vec::new();
        let mut limited = BufReader::new(read_half).take((MAX_COMMAND_BYTES + 1) as u64);
        limited.read_until(b'\n', &mut command).await?;
        let response = if command.len() > MAX_COMMAND_BYTES {
            "ERR command exceeds replica endpoint limit\n".to_owned()
        } else {
            match std::str::from_utf8(&command) {
                Ok(line) => self.execute(line.trim()).await,
                Err(_) => "ERR command is not UTF-8\n".to_owned(),
            }
        };
        write_half.write_all(response.as_bytes()).await?;
        write_half.shutdown().await?;
        Ok(())
    }

    /// Serves child requests until the task is cancelled.
    pub async fn run(&mut self) -> Result<(), ReplicaEndpointError> {
        if self.archive.is_none() {
            loop {
                self.serve_once().await?;
            }
        }
        let interval = std::time::Duration::from_secs(10);
        let mut deadline = tokio::time::Instant::now() + interval;
        loop {
            match tokio::time::timeout_at(deadline, self.serve_once()).await {
                Ok(result) => result?,
                Err(_) => {
                    self.drain_archive()
                        .await
                        .map_err(|error| std::io::Error::other(error.to_string()))?;
                    deadline += interval;
                }
            }
        }
    }

    /// Explicitly drains managed compacted archive work through its verified watermark.
    async fn drain_archive(&self) -> crate::Result<u64> {
        let engine = self.engine.as_ref().ok_or_else(|| {
            Error::Authority("endpoint has no worker execution engine".to_owned())
        })?;
        let archive = self
            .archive
            .as_ref()
            .ok_or_else(|| Error::Archive("managed offload is disabled".to_owned()))?;
        Ok(engine
            .drain_offload(archive.as_ref(), OffloadBatchPolicy::production())
            .await?
            .through())
    }

    /// Parses and applies one private endpoint command.
    async fn execute(&self, line: &str) -> String {
        match self.execute_inner(line).await {
            Ok(payload) if payload.is_empty() => "OK\n".to_owned(),
            Ok(payload) => format!("OK {payload}\n"),
            Err(error) => format!("ERR {}\n", one_line(&error.to_string())),
        }
    }

    /// Executes one validated status, fencing, or committed-apply operation.
    async fn execute_inner(&self, line: &str) -> crate::Result<String> {
        let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
        match fields.as_slice() {
            ["DRAIN"] => Ok(self.drain_archive().await?.to_string()),
            ["STATUS"] => {
                let state = self.store.state()?;
                Ok(format!(
                    "{} {} {} {}",
                    self.role.as_str(),
                    state.applied_sequence(),
                    state.archive_sequence(),
                    state.checkpoint_sequence()
                ))
            }
            ["STATEFUL"] if self.role == ReplicaEndpointRole::Worker => Ok(String::new()),
            ["STATEFUL"] => Err(Error::Authority(
                "replica endpoint cannot execute a stateful Worker event".to_owned(),
            )),
            ["REGISTER", table, schema] => {
                let engine = self.engine.as_ref().ok_or_else(|| {
                    Error::Authority("endpoint has no worker execution engine".to_owned())
                })?;
                let schema = hex::decode(schema)
                    .map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
                let reader = StreamReader::try_new(Cursor::new(schema), None)?;
                engine
                    .create_table(TableId::new(*table), reader.schema())
                    .await?;
                Ok(String::new())
            }
            ["QUERY", table, sql] => {
                let engine = self.engine.as_ref().ok_or_else(|| {
                    Error::Authority("endpoint has no worker execution engine".to_owned())
                })?;
                let sql =
                    hex::decode(sql).map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
                let sql = String::from_utf8(sql)
                    .map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
                let session = DoSession::begin(
                    engine.clone(),
                    [TableId::new(*table)],
                    IsolationLevel::Snapshot,
                )
                .await?;
                let batches = session.execute(&sql).await?;
                let first = batches.first().ok_or_else(|| {
                    Error::Authority("DataFusion query returned no Arrow batches".to_owned())
                })?;
                let mut ipc = Vec::new();
                {
                    let mut writer = StreamWriter::try_new(&mut ipc, &first.schema())?;
                    for batch in &batches {
                        writer.write(batch)?;
                    }
                    writer.finish()?;
                }
                Ok(hex::encode(ipc))
            }
            ["COMMIT", canonical] => {
                let engine = self.engine.as_ref().ok_or_else(|| {
                    Error::Authority("endpoint has no worker commit engine".to_owned())
                })?;
                let canonical = hex::decode(canonical)
                    .map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
                let receipt = engine.commit_canonical(&canonical).await?;
                if self.archive.is_some()
                    && engine.pending_archive_bytes()?
                        >= OffloadBatchPolicy::production().max_bytes()
                {
                    self.drain_archive().await?;
                }
                Ok(receipt.commit_sequence().to_string())
            }
            ["SNAPSHOT", requested] => {
                let requested = parse_u64(requested, "snapshot fence")?;
                let applied = self.store.state()?.applied_sequence();
                if requested > applied {
                    return Err(Error::SnapshotAhead { requested, applied });
                }
                Ok(applied.to_string())
            }
            [
                "REPLICA_APPLY",
                generation,
                token,
                sequence,
                transaction_id,
                canonical,
            ] if self.role == ReplicaEndpointRole::Replica => {
                let lease = parse_lease(generation, token)?;
                self.apply_fields(&lease, sequence, transaction_id, canonical)
            }
            ["REPLICA_REPLAY", after, limit] if self.role == ReplicaEndpointRole::Replica => {
                let after = parse_u64(after, "replay sequence")?;
                let limit = parse_u64(limit, "replay limit")?;
                let limit = usize::try_from(limit)
                    .map_err(|_| Error::InvalidEnvelope("replay limit is too large".to_owned()))?;
                let entries = self.store.replay_after(after, limit)?;
                Ok(entries
                    .iter()
                    .map(|entry| {
                        format!(
                            "{}:{}:{}",
                            entry.commit_sequence(),
                            entry.transaction_id(),
                            hex::encode(entry.canonical_envelope())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(","))
            }
            ["REPLICA_CLEAN", generation, token, through]
                if self.role == ReplicaEndpointRole::Replica =>
            {
                let lease = parse_lease(generation, token)?;
                self.store
                    .clean_replicated(&lease, parse_u64(through, "clean sequence")?)?;
                Ok(String::new())
            }
            _ => Err(Error::InvalidEnvelope(
                "invalid endpoint command".to_owned(),
            )),
        }
    }

    /// Validates canonical identity and applies one local or lease-fenced replica write.
    fn apply_fields(
        &self,
        lease: &LeaseIdentity,
        sequence: &str,
        transaction_id: &str,
        canonical: &str,
    ) -> crate::Result<String> {
        let sequence = parse_u64(sequence, "commit sequence")?;
        let transaction_id = Uuid::parse_str(transaction_id)
            .map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
        let canonical =
            hex::decode(canonical).map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
        let envelope = TransactionEnvelope::from_canonical_bytes(&canonical)?;
        if envelope.do_id() != self.do_id {
            return Err(Error::WrongDo {
                expected: self.do_id.clone(),
                actual: envelope.do_id().to_owned(),
            });
        }
        if envelope.transaction_id() != transaction_id {
            return Err(Error::InvalidEnvelope(
                "APPLY transaction identity does not match canonical bytes".to_owned(),
            ));
        }
        self.store
            .apply_replicated(lease, sequence, transaction_id, &canonical)?;
        Ok(String::new())
    }
}

impl Drop for ReplicaEndpoint {
    /// Removes the socket name when the child endpoint exits.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Decodes one opaque token and monotonic generation from private wire fields.
fn parse_lease(generation: &str, token: &str) -> crate::Result<LeaseIdentity> {
    let token = hex::decode(token).map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
    let token =
        String::from_utf8(token).map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
    Ok(LeaseIdentity::new(
        token,
        parse_u64(generation, "lease generation")?,
    ))
}

/// Parses one unsigned protocol field.
fn parse_u64(value: &str, field: &str) -> crate::Result<u64> {
    value
        .parse::<u64>()
        .map_err(|_| Error::InvalidEnvelope(format!("{field} must be an unsigned integer")))
}

/// Prevents protocol errors from injecting another response line.
fn one_line(error: &str) -> String {
    error.replace(['\r', '\n'], " ")
}
