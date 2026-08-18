//! Async ingest ack path for `mode=append` (issue: cut warm NDJSON append
//! latency): rows are validated against the table schema and durably
//! journaled to local disk before the HTTP ack returns, and a background task
//! performs the real Iceberg CAS commit, coalescing consecutive appends to the
//! same table into one commit.
//!
//! ## Durability level (read before relying on this)
//!
//! This is a bounded in-process commit-coalescing queue with
//! fsync-to-local-disk durability and replay-on-restart. It is **not** the
//! consensus-admitted write-back journal in `verglas-writeback`
//! (erasure-coded fragments acked only after a quorum of peers fsyncs them,
//! used for S3 object PUTs). That crate's domain — fragment placement,
//! Reed-Solomon coding, propagation to an origin bucket — does not fit
//! Iceberg row batches, and `verglas-iceberg` is deliberately a pure-client
//! crate with no consensus/cluster dependency: its hermetic `MemoryCatalog`
//! test harness depends on that separation. So the contract here is narrower:
//! an acked row survives *this process* restarting (fsynced to one local
//! disk, replayed by [`AsyncIngestQueue::replay`] on the next start), but not
//! the loss of the node between ack and commit — there is no peer replication
//! of the journal. A caller that needs the row durable before it can be lost
//! to a single node failure must pass `wait=true`/`commit=sync` at the HTTP
//! layer and take the synchronous commit path instead (unchanged, still the
//! full CAS append).
//!
//! ## Coalescing
//!
//! Every ack durably journals its own entry (one Arrow IPC stream file per
//! request). A background task, spawned the first time a table has queued
//! work with no committer already running for it
//! ([`AsyncIngestQueue::spawn_commit_if_idle`]), drains and commits
//! *everything* currently queued for that table as one Iceberg append; if
//! more acks land while that commit is in flight, the same task picks them up
//! on its next iteration before it exits, so a burst of appends to one table
//! costs one CAS commit, not one per request.
//!
//! ## Duplicate-row window
//!
//! Each coalesced commit carries an idempotency key derived from the sorted
//! set of WAL entry ids it commits, so a crash between a successful commit
//! and the WAL cleanup that follows it does not double-count rows on the next
//! replay *when the cleanup had not yet started* (the replayed key matches
//! and [`crate::tables_api::commit_batches`] recognises it). If the process
//! crashes mid-cleanup — after deleting some but not all of a batch's WAL
//! files — the remaining files no longer match the original batch's key, and
//! replay commits them again as new rows. This is a narrow, documented
//! window, not eliminated: closing it fully needs a two-phase commit marker,
//! which is more machinery than this queue's fallback durability level calls
//! for.

use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::{Catalog, TableIdent};
use verglas_api::table::IngestAckResponse;

use crate::error::{AgentError, Result};
use crate::ident::{ident_to_dotted, parse_table_ident};
use crate::tables_api;
use crate::write::coerce_batches;

/// One journaled-but-not-yet-committed request, held in memory between its
/// WAL write and the coalesced commit that consumes it.
struct PendingEntry {
    /// The WAL entry's id (also its filename stem), used to derive the
    /// coalesced commit's idempotency key.
    id: String,
    /// The fsynced WAL file backing this entry, deleted once committed.
    path: PathBuf,
    /// The schema-coerced batches this entry acked. Cloning a `RecordBatch`
    /// is cheap (its columns are `Arc`-backed), so keeping this in memory
    /// avoids re-reading the WAL file on the common (non-crash) path.
    batches: Vec<RecordBatch>,
}

/// Per-table queue state: the entries waiting for a coalesced commit, and
/// whether a background committer is currently draining them. Both are
/// mutated together under one lock so "is a committer running" and "is the
/// queue empty" are never observed inconsistently.
struct TableQueueState {
    /// Entries acked but not yet committed, oldest first.
    pending: VecDeque<PendingEntry>,
    /// Whether a background commit task is currently draining this queue.
    committing: bool,
}

/// One table's pending-append queue.
struct TableQueue {
    /// Guards `TableQueueState`. A `std::sync::Mutex` is enough because every
    /// critical section is pure in-memory bookkeeping — no I/O and no
    /// `.await` happen while it is held.
    state: Mutex<TableQueueState>,
}

/// The async ingest queue: durable WAL plus per-table coalescing state for
/// every table with outstanding async appends. One instance is shared by the
/// whole ingest HTTP surface.
pub struct AsyncIngestQueue {
    /// Directory holding one Arrow-IPC WAL file per unconsumed entry.
    dir: PathBuf,
    /// Lazily created per-table queues, keyed by table identity.
    tables: Mutex<HashMap<TableIdent, Arc<TableQueue>>>,
}

impl AsyncIngestQueue {
    /// Opens (creating if needed) the WAL directory. Does not read or replay
    /// any journal left by a previous run — call [`Self::replay`] explicitly
    /// once the catalog is available, before serving new ingest traffic.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)
            .map_err(|e| AgentError::AsyncIngest(format!("create {}: {e}", dir.display())))?;
        Ok(Self {
            dir,
            tables: Mutex::new(HashMap::new()),
        })
    }

    /// Returns (creating if needed) the queue for `ident`.
    fn table_queue(&self, ident: &TableIdent) -> Arc<TableQueue> {
        let mut guard = self.tables.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(guard.entry(ident.clone()).or_insert_with(|| {
            Arc::new(TableQueue {
                state: Mutex::new(TableQueueState {
                    pending: VecDeque::new(),
                    committing: false,
                }),
            })
        }))
    }

    /// Validates `batches` against `ident`'s current schema (rejecting a
    /// mismatched column by name, exactly as the synchronous path does) and,
    /// on success, durably journals them (fsynced Arrow IPC WAL entry) before
    /// returning. Returns the accepted row count. Nothing is committed by
    /// this call alone: pair it with [`Self::spawn_commit_if_idle`] to get
    /// the production ack-then-async-commit behaviour, or leave it unpaired
    /// to exercise the crash-recovery window deterministically.
    pub async fn ack_append(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        batches: Vec<RecordBatch>,
    ) -> Result<IngestAckResponse> {
        let table = catalog.load_table(ident).await?;
        let table_name = ident_to_dotted(ident);
        let target = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        let coerced = coerce_batches(&batches, &target, &table_name)?;
        let row_count: u64 = coerced.iter().map(|b| b.num_rows() as u64).sum();
        if row_count == 0 {
            return Ok(IngestAckResponse {
                successful_rows: 0,
                committed: false,
            });
        }

        let id = uuid::Uuid::new_v4().to_string();
        let path = write_wal_entry(&self.dir, &id, ident, &coerced)?;

        let queue = self.table_queue(ident);
        let mut guard = queue.state.lock().unwrap_or_else(|p| p.into_inner());
        guard.pending.push_back(PendingEntry {
            id,
            path,
            batches: coerced,
        });
        drop(guard);

        Ok(IngestAckResponse {
            successful_rows: row_count,
            committed: false,
        })
    }

    /// Spawns a background coalesced commit for `ident` if none is already
    /// running. A no-op when a committer is already in flight or nothing is
    /// queued — the in-flight committer drains everything queued (including
    /// entries added after it started) before it exits, so callers never
    /// need to retry this.
    pub fn spawn_commit_if_idle(self: &Arc<Self>, catalog: Arc<dyn Catalog>, ident: TableIdent) {
        let queue = self.table_queue(&ident);
        let should_run = {
            let mut guard = queue.state.lock().unwrap_or_else(|p| p.into_inner());
            if guard.committing || guard.pending.is_empty() {
                false
            } else {
                guard.committing = true;
                true
            }
        };
        if !should_run {
            return;
        }
        let owner = Arc::clone(self);
        tokio::spawn(async move {
            owner
                .drain_and_commit_loop(catalog.as_ref(), &ident, &queue)
                .await;
        });
    }

    /// Drains `queue` and commits it as one coalesced append, repeating until
    /// the queue is empty (so entries that arrive while a commit is in
    /// flight are picked up without a second spawn). Clears `committing`
    /// under the same lock as the final empty check, so a concurrent
    /// `spawn_commit_if_idle` never both observes `committing == true` and
    /// fails to have its entry serviced.
    async fn drain_and_commit_loop(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        queue: &Arc<TableQueue>,
    ) {
        loop {
            let drained: Vec<PendingEntry> = {
                let mut guard = queue.state.lock().unwrap_or_else(|p| p.into_inner());
                if guard.pending.is_empty() {
                    guard.committing = false;
                    break;
                }
                guard.pending.drain(..).collect()
            };

            let key = idempotency_key(&drained);
            let batches: Vec<RecordBatch> = drained
                .iter()
                .flat_map(|entry| entry.batches.clone())
                .collect();
            match tables_api::commit_batches(catalog, ident, batches, Some(key)).await {
                Ok(_) => {
                    for entry in &drained {
                        // Best-effort: see the module-level doc comment on the
                        // duplicate-row window this leaves if it fails partway
                        // through a batch.
                        let _ = remove_wal_entry(&self.dir, &entry.path);
                    }
                }
                Err(_) => {
                    // Leave the WAL entries on disk and re-queue them so a
                    // later ack (or `replay` after a restart) retries the
                    // commit. Do not spin: stop this task now.
                    let mut guard = queue.state.lock().unwrap_or_else(|p| p.into_inner());
                    for entry in drained.into_iter().rev() {
                        guard.pending.push_front(entry);
                    }
                    guard.committing = false;
                    break;
                }
            }
        }
    }

    /// Reports whether no entries are queued and no commit is in flight for
    /// `ident`. Used to drain gracefully on shutdown and by tests that need
    /// to wait for a background commit to finish deterministically.
    pub fn is_idle(&self, ident: &TableIdent) -> bool {
        let queue = self.table_queue(ident);
        let guard = queue.state.lock().unwrap_or_else(|p| p.into_inner());
        !guard.committing && guard.pending.is_empty()
    }

    /// Replays journaled-but-uncommitted rows found on disk — the acked-but-
    /// not-yet-committed window a previous run left behind — grouping them by
    /// table and committing each group as one coalesced append, then deleting
    /// the WAL files it committed. Call once at startup after the catalog
    /// opens, before serving new ingest traffic. Returns the number of rows
    /// replayed.
    pub async fn replay(&self, catalog: &dyn Catalog) -> Result<u64> {
        let mut by_table: HashMap<TableIdent, Vec<(String, PathBuf, Vec<RecordBatch>)>> =
            HashMap::new();
        let entries = fs::read_dir(&self.dir)
            .map_err(|e| AgentError::AsyncIngest(format!("list {}: {e}", self.dir.display())))?;
        for entry in entries {
            let entry = entry.map_err(|e| AgentError::AsyncIngest(format!("dir entry: {e}")))?;
            let path = entry.path();
            if path.extension().and_then(|x| x.to_str()) != Some("arrow") {
                continue;
            }
            let (id, ident, batches) = read_wal_entry(&path)?;
            by_table.entry(ident).or_default().push((id, path, batches));
        }

        let mut total = 0u64;
        for (ident, group) in by_table {
            let mut ids: Vec<&str> = group.iter().map(|(id, _, _)| id.as_str()).collect();
            ids.sort_unstable();
            let key = ids.join(",");

            let mut batches = Vec::new();
            let mut paths = Vec::new();
            for (_, path, entry_batches) in &group {
                batches.extend(entry_batches.iter().cloned());
                paths.push(path.clone());
            }
            let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            if row_count > 0 {
                tables_api::commit_batches(catalog, &ident, batches, Some(key)).await?;
            }
            for path in paths {
                remove_wal_entry(&self.dir, &path)?;
            }
            total += row_count;
        }
        Ok(total)
    }
}

/// Derives a coalesced commit's idempotency key from the sorted WAL entry ids
/// it commits, so a replay of the exact same still-present entry set is
/// recognised as a duplicate by [`crate::tables_api::commit_batches`] instead
/// of writing the rows again.
fn idempotency_key(entries: &[PendingEntry]) -> String {
    let mut ids: Vec<&str> = entries.iter().map(|e| e.id.as_str()).collect();
    ids.sort_unstable();
    ids.join(",")
}

/// Writes one WAL entry: a 4-byte little-endian table-name length, the UTF-8
/// dotted table name, then an Arrow IPC stream of `batches`. Written to a
/// temp file, fsynced, and renamed into place, then the directory is fsynced
/// — the same write-fsync-rename-fsync pattern the write-back journal uses,
/// so a crash mid-write never leaves a partially-written entry at the final
/// path.
fn write_wal_entry(
    dir: &Path,
    id: &str,
    ident: &TableIdent,
    batches: &[RecordBatch],
) -> Result<PathBuf> {
    let table_name = ident_to_dotted(ident);
    let name_bytes = table_name.as_bytes();
    let mut buf = Vec::new();
    buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(name_bytes);
    {
        let schema = batches.first().map(|b| b.schema()).ok_or_else(|| {
            AgentError::AsyncIngest("cannot journal an empty batch set".to_owned())
        })?;
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, schema.as_ref())
            .map_err(|e| AgentError::AsyncIngest(format!("encode ingest wal entry: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| AgentError::AsyncIngest(format!("encode ingest wal entry: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| AgentError::AsyncIngest(format!("encode ingest wal entry: {e}")))?;
    }

    let path = dir.join(format!("{id}.arrow"));
    let tmp = dir.join(format!("{id}.{}.tmp", std::process::id()));
    {
        let mut file = File::create(&tmp)
            .map_err(|e| AgentError::AsyncIngest(format!("create {}: {e}", tmp.display())))?;
        file.write_all(&buf)
            .map_err(|e| AgentError::AsyncIngest(format!("write {}: {e}", tmp.display())))?;
        file.sync_all()
            .map_err(|e| AgentError::AsyncIngest(format!("fsync {}: {e}", tmp.display())))?;
    }
    fs::rename(&tmp, &path)
        .map_err(|e| AgentError::AsyncIngest(format!("rename into {}: {e}", path.display())))?;
    fsync_dir(dir)?;
    Ok(path)
}

/// Reads one WAL entry back: the id (its filename stem), the table it
/// belongs to, and its batches.
fn read_wal_entry(path: &Path) -> Result<(String, TableIdent, Vec<RecordBatch>)> {
    let id = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| {
            AgentError::AsyncIngest(format!("{} has no usable file stem", path.display()))
        })?
        .to_owned();
    let bytes = fs::read(path)
        .map_err(|e| AgentError::AsyncIngest(format!("read {}: {e}", path.display())))?;
    if bytes.len() < 4 {
        return Err(AgentError::AsyncIngest(format!(
            "{} is truncated (missing header)",
            path.display()
        )));
    }
    let name_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let name_end = 4usize
        .checked_add(name_len)
        .filter(|&end| end <= bytes.len())
        .ok_or_else(|| {
            AgentError::AsyncIngest(format!("{} has a corrupt header", path.display()))
        })?;
    let table_name = std::str::from_utf8(&bytes[4..name_end]).map_err(|e| {
        AgentError::AsyncIngest(format!("{} has a non-utf8 table name: {e}", path.display()))
    })?;
    let ident = parse_table_ident(table_name)?;

    let cursor = std::io::Cursor::new(&bytes[name_end..]);
    let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|e| AgentError::AsyncIngest(format!("decode {}: {e}", path.display())))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(
            batch
                .map_err(|e| AgentError::AsyncIngest(format!("decode {}: {e}", path.display())))?,
        );
    }
    Ok((id, ident, batches))
}

/// Deletes a WAL entry and fsyncs `dir` so the deletion is durable. A missing
/// file is not an error (a concurrent `replay` may have already removed it).
fn remove_wal_entry(dir: &Path, path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(AgentError::AsyncIngest(format!(
                "delete {}: {e}",
                path.display()
            )));
        }
    }
    fsync_dir(dir)
}

/// Fsyncs a directory so file creations, renames, and deletions within it are
/// durable, not just the files themselves.
fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(|e| AgentError::AsyncIngest(format!("fsync dir {}: {e}", dir.display())))
}
