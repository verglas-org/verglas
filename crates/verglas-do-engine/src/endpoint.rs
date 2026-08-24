//! Private Unix-socket control and apply endpoint for one `verglasd` replica.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use object_store::path::Path as ObjectPath;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use uuid::Uuid;
use verglas_graph::Direction;
use verglas_vector::Metric;

use crate::{
    CommitAuthority, DoEngine, DoSession, Error, GraphIndexConfig, IsolationLevel, LeaseIdentity,
    MutationDomain, ObjectStoreCheckpointPublisher, OffloadBatchArchive, OffloadBatchPolicy,
    SqliteReplicaStore, TableId, TransactionEnvelope, UnixReplicaSink, VectorIndexConfig,
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

/// Managed-CAS archive and checkpoint facilities attached to one worker.
struct ManagedCasContext {
    publisher: Arc<ObjectStoreCheckpointPublisher>,
    prefix: ObjectPath,
}

/// Replica-service coverage transport attached to one worker checkpoint.
struct ReplicaCoverageContext {
    sink: Arc<UnixReplicaSink>,
    lease: LeaseIdentity,
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
    managed_cas: Option<ManagedCasContext>,
    checkpoint_publisher: Option<Arc<ObjectStoreCheckpointPublisher>>,
    replica_coverage: Option<ReplicaCoverageContext>,
    index_declaration_path: Option<PathBuf>,
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
        Self::bind_with(
            path, do_id, replica_id, role, store, None, None, None, None, None,
        )
        .await
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
            None,
            None,
            None,
        )
        .await
    }

    /// Binds a worker with managed CAS archive and checkpoint support.
    pub async fn bind_worker_with_cas(
        path: impl AsRef<Path>,
        do_id: impl Into<String>,
        replica_id: u64,
        store: Arc<SqliteReplicaStore>,
        authority: Arc<dyn CommitAuthority>,
        publisher: Arc<ObjectStoreCheckpointPublisher>,
        prefix: impl AsRef<str>,
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
            None,
            Some(ManagedCasContext {
                publisher,
                prefix: ObjectPath::from(prefix.as_ref()),
            }),
            None,
            None,
        )
        .await
    }

    /// Binds a replica-backed worker with checkpoint publication and coverage propagation.
    #[allow(clippy::too_many_arguments)]
    pub async fn bind_worker_with_replica_checkpoint(
        path: impl AsRef<Path>,
        do_id: impl Into<String>,
        replica_id: u64,
        store: Arc<SqliteReplicaStore>,
        authority: Arc<dyn CommitAuthority>,
        archive: Arc<dyn OffloadBatchArchive>,
        publisher: Arc<ObjectStoreCheckpointPublisher>,
        replica: Arc<UnixReplicaSink>,
        lease: LeaseIdentity,
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
            Some(archive),
            None,
            Some(publisher),
            Some(ReplicaCoverageContext {
                sink: replica,
                lease,
            }),
        )
        .await
    }

    /// Implements shared socket validation for worker and replica endpoints.
    #[allow(clippy::too_many_arguments)]
    async fn bind_with(
        path: impl AsRef<Path>,
        do_id: impl Into<String>,
        replica_id: u64,
        role: ReplicaEndpointRole,
        store: Arc<SqliteReplicaStore>,
        engine: Option<Arc<DoEngine>>,
        archive: Option<Arc<dyn OffloadBatchArchive>>,
        managed_cas: Option<ManagedCasContext>,
        checkpoint_publisher: Option<Arc<ObjectStoreCheckpointPublisher>>,
        replica_coverage: Option<ReplicaCoverageContext>,
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
            managed_cas,
            checkpoint_publisher,
            replica_coverage,
            index_declaration_path: None,
            listener,
        })
    }

    /// Returns the child-exclusive bound Unix socket path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the Worker engine when this endpoint executes stateful events.
    pub fn engine(&self) -> Option<Arc<DoEngine>> {
        self.engine.as_ref().map(Arc::clone)
    }

    /// Returns this host's stable replica identity.
    pub fn replica_id(&self) -> u64 {
        self.replica_id
    }

    /// Configures and restores durable vector/graph declarations for this worker.
    pub async fn configure_index_declarations(
        &mut self,
        path: impl AsRef<Path>,
    ) -> crate::Result<()> {
        if self.role != ReplicaEndpointRole::Worker {
            return Err(Error::Authority(
                "replica endpoint cannot own index declarations".to_owned(),
            ));
        }
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| Error::Materialization(error.to_string()))?;
        }
        self.index_declaration_path = Some(path.clone());
        let contents = match tokio::fs::read_to_string(path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(Error::Materialization(error.to_string())),
        };
        for declaration in contents.lines().filter(|line| !line.is_empty()) {
            self.restore_index_declaration(declaration).await?;
        }
        Ok(())
    }

    /// Restores one persisted vector or graph declaration into the live engine.
    async fn restore_index_declaration(&self, declaration: &str) -> crate::Result<()> {
        let fields = declaration.split('\t').collect::<Vec<_>>();
        let engine = self.engine.as_ref().ok_or_else(|| {
            Error::Authority("index declarations require a worker execution engine".to_owned())
        })?;
        match fields.as_slice() {
            ["VECTOR", table, id_column, vector_column, dimension, metric] => {
                let dimension = parse_usize(dimension, "vector dimension")?;
                let metric = Metric::parse(metric).ok_or_else(|| {
                    Error::InvalidEnvelope(format!("unknown vector metric {metric}"))
                })?;
                engine
                    .register_vector_index(VectorIndexConfig::new(
                        TableId::new(*table),
                        *id_column,
                        *vector_column,
                        dimension,
                        metric,
                    ))
                    .await
            }
            ["GRAPH", table] => {
                engine
                    .register_graph_index(GraphIndexConfig::new(TableId::new(*table)))
                    .await
            }
            _ => Err(Error::InvalidEnvelope(
                "invalid persisted index declaration".to_owned(),
            )),
        }
    }

    /// Persists one declaration exactly once in the shared managed root.
    async fn persist_index_declaration(&self, declaration: &str) -> crate::Result<()> {
        let path = self.index_declaration_path.as_ref().ok_or_else(|| {
            Error::Materialization("durable index declarations are not configured".to_owned())
        })?;
        let existing = match tokio::fs::read_to_string(path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(Error::Materialization(error.to_string())),
        };
        let kind = declaration.split('\t').next().unwrap_or_default();
        let table = declaration.split('\t').nth(1).unwrap_or_default();
        for prior in existing.lines() {
            if prior.split('\t').next() == Some(kind)
                && prior.split('\t').nth(1) == Some(table)
                && prior != declaration
            {
                return Err(Error::InvalidEnvelope(format!(
                    "index declaration for table {table} conflicts with persisted content"
                )));
            }
        }
        if existing.lines().any(|prior| prior == declaration) {
            return Ok(());
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?;
        file.write_all(format!("{declaration}\n").as_bytes())
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?;
        file.flush()
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?;
        Ok(())
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
        match write_half.shutdown().await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotConnected => {}
            Err(error) => return Err(error.into()),
        }
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
        let report = engine
            .drain_offload(archive.as_ref(), OffloadBatchPolicy::production())
            .await?;
        Ok(report.through().max(self.store.state()?.archive_sequence()))
    }

    /// Advances the local managed-CAS archive watermark for one verified object.
    fn mark_managed_commit(&self, sequence: u64, transaction_id: Uuid) -> crate::Result<()> {
        let Some(context) = &self.managed_cas else {
            return Ok(());
        };
        let state = self.store.state()?;
        if sequence <= state.archive_sequence() {
            return Ok(());
        }
        let expected = state.archive_sequence().saturating_add(1);
        if sequence != expected {
            return Err(Error::ReplicaSequence(format!(
                "managed CAS archive sequence {sequence} does not follow {expected}"
            )));
        }
        let path = context
            .prefix
            .clone()
            .join(self.do_id.as_str())
            .join("transactions")
            .join(format!("{sequence:020}-{transaction_id}.arrow"));
        self.store.mark_archived(sequence, path.as_ref())
    }

    /// Removes the temporary local file after verified checkpoint publication.
    async fn remove_checkpoint_temporary(&self, temporary: &Path) -> crate::Result<()> {
        match tokio::fs::remove_file(temporary).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::Materialization(error.to_string())),
        }
        Ok(())
    }

    /// Publishes the current managed-CAS SQLite state as a verified checkpoint.
    async fn publish_managed_checkpoint(&self) -> crate::Result<u64> {
        let context = self.managed_cas.as_ref().ok_or_else(|| {
            Error::Materialization("managed CAS checkpointing is disabled".to_owned())
        })?;
        let temporary = self.path.with_extension("managed-checkpoint.sqlite");
        let receipt = context.publisher.publish(&self.store, &temporary).await?;
        self.remove_checkpoint_temporary(&temporary).await?;
        Ok(receipt.through_sequence())
    }

    /// Publishes a replica checkpoint and propagates its coverage to the service.
    async fn publish_replica_checkpoint(&self) -> crate::Result<u64> {
        let publisher = self.checkpoint_publisher.as_ref().ok_or_else(|| {
            Error::Materialization("replica checkpointing is disabled".to_owned())
        })?;
        let temporary = self.path.with_extension("replica-checkpoint.sqlite");
        let receipt = publisher.publish(&self.store, &temporary).await?;
        self.remove_checkpoint_temporary(&temporary).await?;
        let coverage = self.replica_coverage.as_ref().ok_or_else(|| {
            Error::Authority("replica checkpoint coverage transport is missing".to_owned())
        })?;
        let state = self.store.state()?;
        coverage
            .sink
            .cover(
                &coverage.lease,
                state.archive_sequence(),
                receipt.through_sequence(),
                receipt.object_path(),
            )
            .await?;
        Ok(receipt.through_sequence())
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
                let archived = if self.replica_coverage.is_some() && state.checkpoint_sequence() > 0
                {
                    state.archive_sequence().max(state.applied_sequence())
                } else {
                    state.archive_sequence()
                };
                Ok(format!(
                    "{} {} {} {}",
                    self.role.as_str(),
                    state.applied_sequence(),
                    archived,
                    state.checkpoint_sequence()
                ))
            }
            ["STATEFUL"] if self.role == ReplicaEndpointRole::Worker => Ok(String::new()),
            ["STATEFUL"] => Err(Error::Authority(
                "replica endpoint cannot execute a stateful Worker event".to_owned(),
            )),
            ["DOMAINS"] if self.role == ReplicaEndpointRole::Worker => {
                let engine = self.engine.as_ref().ok_or_else(|| {
                    Error::Authority("endpoint has no worker execution engine".to_owned())
                })?;
                Ok(format!(
                    "{} {} {} {}",
                    engine.applied_sequence(),
                    engine.domain_watermark(MutationDomain::Relational),
                    engine.domain_watermark(MutationDomain::Vector),
                    engine.domain_watermark(MutationDomain::Graph)
                ))
            }
            ["INDEX_STATUS", table] if self.role == ReplicaEndpointRole::Worker => {
                let engine = self.engine.as_ref().ok_or_else(|| {
                    Error::Authority("endpoint has no worker execution engine".to_owned())
                })?;
                let vector = engine.vector_index_through(&TableId::new(*table));
                let graph = engine.graph_index_through(&TableId::new(*table));
                match (vector, graph) {
                    (Some(vector), Some(graph)) => Ok(format!("vector={vector} graph={graph}")),
                    _ => Err(Error::UnknownTable((*table).to_owned())),
                }
            }
            [
                "REGISTER_VECTOR",
                table,
                id_column,
                vector_column,
                dimension,
                metric,
            ] if self.role == ReplicaEndpointRole::Worker => {
                let engine = self.engine.as_ref().ok_or_else(|| {
                    Error::Authority("endpoint has no worker execution engine".to_owned())
                })?;
                let dimension = parse_usize(dimension, "vector dimension")?;
                let metric = Metric::parse(metric).ok_or_else(|| {
                    Error::InvalidEnvelope(format!("unknown vector metric {metric}"))
                })?;
                engine
                    .register_vector_index(VectorIndexConfig::new(
                        TableId::new(*table),
                        *id_column,
                        *vector_column,
                        dimension,
                        metric,
                    ))
                    .await?;
                self.persist_index_declaration(&format!(
                    "VECTOR\t{table}\t{id_column}\t{vector_column}\t{dimension}\t{}",
                    metric.as_str()
                ))
                .await?;
                Ok(String::new())
            }
            ["REGISTER_GRAPH", table] if self.role == ReplicaEndpointRole::Worker => {
                let engine = self.engine.as_ref().ok_or_else(|| {
                    Error::Authority("endpoint has no worker execution engine".to_owned())
                })?;
                engine
                    .register_graph_index(GraphIndexConfig::new(TableId::new(*table)))
                    .await?;
                self.persist_index_declaration(&format!("GRAPH\t{table}"))
                    .await?;
                Ok(String::new())
            }
            ["VECTOR_SEARCH", table, k, query] if self.role == ReplicaEndpointRole::Worker => {
                let engine = self.engine.as_ref().ok_or_else(|| {
                    Error::Authority("endpoint has no worker execution engine".to_owned())
                })?;
                let query = decode_vector(query)?;
                let neighbors = engine.vector_search(
                    &TableId::new(*table),
                    &query,
                    usize::try_from(parse_u64(k, "vector result count")?).map_err(|_| {
                        Error::InvalidEnvelope("vector result count too large".to_owned())
                    })?,
                )?;
                Ok(neighbors
                    .iter()
                    .map(|neighbor| format!("{}:{:.9}", neighbor.id, neighbor.distance))
                    .collect::<Vec<_>>()
                    .join(","))
            }
            ["GRAPH_NEIGHBORS", table, node, direction, predicate]
                if self.role == ReplicaEndpointRole::Worker =>
            {
                let engine = self.engine.as_ref().ok_or_else(|| {
                    Error::Authority("endpoint has no worker execution engine".to_owned())
                })?;
                let node = decode_text(node, "graph node")?;
                let direction = parse_direction(direction)?;
                let predicate =
                    (*predicate != "-").then(|| decode_text(predicate, "graph predicate"));
                let predicate = predicate.transpose()?;
                let neighbors = engine.graph_neighbors(
                    &TableId::new(*table),
                    &node,
                    direction,
                    predicate.as_deref(),
                )?;
                Ok(neighbors
                    .iter()
                    .map(|neighbor| {
                        format!(
                            "{}:{}:{}",
                            neighbor.node_id, neighbor.edge_id, neighbor.predicate
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(","))
            }
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
                let envelope = TransactionEnvelope::from_canonical_bytes(&canonical)?;
                let receipt = engine.commit_canonical(&canonical).await?;
                self.mark_managed_commit(receipt.commit_sequence(), envelope.transaction_id())?;
                if self.archive.is_some()
                    && engine.pending_archive_bytes()?
                        >= OffloadBatchPolicy::production().max_bytes()
                {
                    self.drain_archive().await?;
                }
                Ok(receipt.commit_sequence().to_string())
            }
            ["CHECKPOINT"]
                if self.role == ReplicaEndpointRole::Worker && self.replica_coverage.is_some() =>
            {
                Ok(self.publish_replica_checkpoint().await?.to_string())
            }
            ["CHECKPOINT"] if self.role == ReplicaEndpointRole::Worker => {
                Ok(self.publish_managed_checkpoint().await?.to_string())
            }
            ["CHECKPOINT"] => Err(Error::Materialization(
                "replica endpoint cannot publish a managed checkpoint".to_owned(),
            )),
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
            [
                "REPLICA_COVER",
                generation,
                token,
                archived,
                checkpointed,
                identity,
            ] if self.role == ReplicaEndpointRole::Replica => {
                let lease = parse_lease(generation, token)?;
                let identity = hex::decode(identity)
                    .map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
                let identity = String::from_utf8(identity)
                    .map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
                self.store.mark_coverage(
                    &lease,
                    parse_u64(archived, "archive coverage")?,
                    parse_u64(checkpointed, "checkpoint coverage")?,
                    &identity,
                )?;
                Ok(String::new())
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

/// Decodes one fixed-width little-endian vector query.
fn decode_vector(value: &str) -> crate::Result<Vec<f32>> {
    let bytes = hex::decode(value).map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
    let chunks = bytes.chunks_exact(std::mem::size_of::<f32>());
    if !chunks.remainder().is_empty() {
        return Err(Error::InvalidEnvelope(
            "vector query must contain complete f32 values".to_owned(),
        ));
    }
    Ok(chunks
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

/// Decodes one UTF-8 field carried as hexadecimal protocol bytes.
fn decode_text(value: &str, field: &str) -> crate::Result<String> {
    let bytes = hex::decode(value).map_err(|error| Error::InvalidEnvelope(error.to_string()))?;
    String::from_utf8(bytes).map_err(|error| Error::InvalidEnvelope(format!("{field}: {error}")))
}

/// Parses one graph traversal direction label.
fn parse_direction(value: &str) -> crate::Result<Direction> {
    match value {
        "out" => Ok(Direction::Out),
        "in" => Ok(Direction::In),
        "both" => Ok(Direction::Both),
        _ => Err(Error::InvalidEnvelope(
            "graph direction must be out, in, or both".to_owned(),
        )),
    }
}

/// Parses one usize endpoint field without narrowing a large unsigned value.
fn parse_usize(value: &str, field: &str) -> crate::Result<usize> {
    value
        .parse::<usize>()
        .map_err(|_| Error::InvalidEnvelope(format!("{field} must be an unsigned integer")))
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
