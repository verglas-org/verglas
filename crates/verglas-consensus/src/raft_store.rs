//! Crash-safe OpenRaft log and state-machine storage.
//!
//! Every metadata mutation and log-flush callback waits for its own durable write.
//! OpenRaft remains the sole owner of votes, log matching, commit indexes, and membership.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftLogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, mpsc, oneshot};

use crate::catalog::CatalogRecords;
use crate::{AppliedOutcome, RaftCommand, RaftResponse, RequestId, VerglasRaftConfig};

/// Persistent term, vote, log, and committed-index storage for one Raft replica.
#[derive(Clone)]
pub struct PersistentLogStore {
    path: PathBuf,
    entries_path: PathBuf,
    state: Arc<RwLock<LogStateData>>,
    /// Ordered durability actor for compact term, vote, and committed metadata.
    metadata_persistence: PersistenceActor,
    /// Ordered durability actor for append-only Raft entry frames.
    entries_persistence: PersistenceActor,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct LogData {
    last_purged: Option<LogId<u64>>,
    committed: Option<LogId<u64>>,
    vote: Option<Vote<u64>>,
}

/// In-memory Raft log state reconstructed from the durable metadata and frames.
#[derive(Clone, Debug, Default)]
struct LogStateData {
    /// Compact values persisted independently from every entry frame.
    metadata: LogData,
    /// Entries visible after replaying the ordered append/truncate/purge frames.
    entries: BTreeMap<u64, Entry<VerglasRaftConfig>>,
}

/// One durable mutation to the append-only Raft entry journal.
#[derive(Deserialize, Serialize)]
enum LogRecord {
    /// Makes the exact entries durable before OpenRaft observes its flush callback.
    Append(Vec<Entry<VerglasRaftConfig>>),
    /// Removes entries at and after this index.
    Truncate { since: u64 },
    /// Removes entries through this exact log identity.
    Purge { log_id: LogId<u64> },
}

impl PersistentLogStore {
    /// Opens or creates one replica's Raft log file.
    pub async fn open(path: PathBuf) -> Result<Self, StorageError<u64>> {
        let metadata: LogData =
            read_json(&path).map_err(|error| StorageIOError::read_logs(&error))?;
        let entries_path = path.with_extension("entries");
        let recovered = recover_log_records(&entries_path)
            .map_err(|error| StorageIOError::read_logs(&error))?;
        let mut metadata = metadata;
        if recovered.last_purged.is_some() {
            metadata.last_purged = recovered.last_purged;
        }
        Ok(Self {
            path,
            entries_path,
            state: Arc::new(RwLock::new(LogStateData {
                metadata,
                entries: recovered.entries,
            })),
            metadata_persistence: PersistenceActor::new(),
            entries_persistence: PersistenceActor::new(),
        })
    }

    /// Queues compact Raft metadata behind every earlier mutation without
    /// letting its fsync occupy an async runtime worker.
    async fn enqueue_metadata(
        &self,
        state: LogData,
    ) -> std::io::Result<oneshot::Receiver<std::io::Result<()>>> {
        self.metadata_persistence
            .enqueue_json(self.path.clone(), state)
            .await
    }

    /// Queues one append-only log record behind every earlier durable mutation.
    async fn enqueue_record(
        &self,
        record: LogRecord,
    ) -> std::io::Result<oneshot::Receiver<std::io::Result<()>>> {
        self.entries_persistence
            .enqueue_log_record(self.entries_path.clone(), record)
            .await
    }

    /// Adds entries to the visible in-memory log and dispatches their ordered durable frame.
    async fn dispatch_append(
        &self,
        entries: Vec<Entry<VerglasRaftConfig>>,
    ) -> std::io::Result<oneshot::Receiver<std::io::Result<()>>> {
        let mut state = self.state.write().await;
        let previous: Vec<_> = entries
            .iter()
            .map(|entry| {
                (
                    entry.log_id.index,
                    state.entries.get(&entry.log_id.index).cloned(),
                )
            })
            .collect();
        for entry in &entries {
            state.entries.insert(entry.log_id.index, entry.clone());
        }
        match self
            .entries_persistence
            .try_enqueue_log_record(self.entries_path.clone(), LogRecord::Append(entries))
        {
            Ok(completion) => Ok(completion),
            Err(error) => {
                for (index, prior) in previous {
                    if let Some(prior) = prior {
                        state.entries.insert(index, prior);
                    } else {
                        state.entries.remove(&index);
                    }
                }
                Err(error)
            }
        }
    }
}

impl RaftLogReader<VerglasRaftConfig> for PersistentLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<VerglasRaftConfig>>, StorageError<u64>> {
        Ok(self
            .state
            .read()
            .await
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<VerglasRaftConfig> for PersistentLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<VerglasRaftConfig>, StorageError<u64>> {
        let state = self.state.read().await;
        let last_log_id = state
            .entries
            .last_key_value()
            .map(|(_, entry)| *entry.get_log_id())
            .or(state.metadata.last_purged);
        Ok(LogState {
            last_purged_log_id: state.metadata.last_purged,
            last_log_id,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let mut state = self.state.write().await;
        let mut metadata = state.metadata.clone();
        metadata.committed = committed;
        let persisted = self.enqueue_metadata(metadata.clone()).await;
        let persisted = persisted.map_err(|error| StorageIOError::write_logs(&error))?;
        await_persisted(persisted)
            .await
            .map_err(|error| StorageIOError::write_logs(&error))?;
        state.metadata = metadata;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.state.read().await.metadata.committed)
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut state = self.state.write().await;
        let mut metadata = state.metadata.clone();
        metadata.vote = Some(*vote);
        let persisted = self.enqueue_metadata(metadata.clone()).await;
        let persisted = persisted.map_err(|error| StorageIOError::write_vote(&error))?;
        await_persisted(persisted)
            .await
            .map_err(|error| StorageIOError::write_vote(&error))?;
        state.metadata = metadata;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.state.read().await.metadata.vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<VerglasRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<VerglasRaftConfig>> + Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        match self.dispatch_append(entries).await {
            Ok(completion) => {
                tokio::spawn(async move {
                    match await_persisted(completion).await {
                        Ok(()) => callback.log_io_completed(Ok(())),
                        Err(error) => callback.log_io_completed(Err(std::io::Error::new(
                            error.kind(),
                            error.to_string(),
                        ))),
                    }
                });
                Ok(())
            }
            Err(error) => {
                callback
                    .log_io_completed(Err(std::io::Error::new(error.kind(), error.to_string())));
                Err(StorageIOError::write_logs(&error).into())
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut state = self.state.write().await;
        let persisted = self
            .enqueue_record(LogRecord::Truncate {
                since: log_id.index,
            })
            .await;
        let persisted = persisted.map_err(|error| StorageIOError::write_logs(&error))?;
        await_persisted(persisted)
            .await
            .map_err(|error| StorageIOError::write_logs(&error))?;
        state.entries.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut state = self.state.write().await;
        let persisted = self.enqueue_record(LogRecord::Purge { log_id }).await;
        let persisted = persisted.map_err(|error| StorageIOError::write_logs(&error))?;
        await_persisted(persisted)
            .await
            .map_err(|error| StorageIOError::write_logs(&error))?;
        state.metadata.last_purged = Some(log_id);
        state.entries = state.entries.split_off(&(log_id.index + 1));
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

/// Persistent deterministic state machine for committed entry headers.
#[derive(Clone)]
pub struct PersistentStateMachine {
    path: PathBuf,
    snapshot_path: PathBuf,
    state: Arc<RwLock<StateMachineData>>,
    /// Dedicated ordered durability actor for state and snapshot images.
    persistence: PersistenceActor,
    snapshot_index: Arc<AtomicU64>,
    current_snapshot: Arc<RwLock<Option<StoredSnapshot>>>,
    payloads: Arc<RwLock<Option<Arc<dyn crate::PayloadStore>>>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct StateMachineData {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    committed: BTreeMap<u64, crate::EntryHeader>,
    committed_log_ids: BTreeMap<u64, LogId<u64>>,
    repairs: BTreeMap<u64, RepairAllocation>,
    retries: BTreeMap<RequestId, AppliedIdentity>,
    writer_epoch: u64,
    writer_active: bool,
    wal_archive_bucket: Option<String>,
    wal_initialized: bool,
    wal_end: u64,
    archive_lsn: u64,
    namespaces: BTreeSet<String>,
    tables: BTreeMap<String, String>,
    records: CatalogRecords,
    warehouses: BTreeMap<String, String>,
    catalog_checkpoint: Option<(u64, String)>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AppliedIdentity {
    index: u64,
    group: String,
    kind: crate::CommandKind,
    hash: [u8; 32],
    writer_epoch: Option<u64>,
    wal_end: Option<u64>,
    archive_lsn: Option<u64>,
    catalog_changed: bool,
}

/// Latest committed replacement allocation and the Raft identity that sealed it.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RepairAllocation {
    /// Configuration generation that owns the replacement fragment namespace.
    pub configuration_generation: u64,
    /// Exact replacement holder allocation.
    pub certificate: crate::PayloadCertificate,
    /// Repair command identity used to seal every replacement representation.
    pub log_id: LogId<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

impl PersistentStateMachine {
    /// Opens or creates the applied-state file for one Raft replica.
    pub async fn open(path: PathBuf) -> Result<Self, StorageError<u64>> {
        let state = read_json(&path).map_err(|error| StorageIOError::read_state_machine(&error))?;
        let snapshot_path = path.with_extension("snapshot");
        let current_snapshot = read_json(&snapshot_path)
            .map_err(|error| StorageIOError::read_snapshot(None, &error))?;
        Ok(Self {
            path,
            snapshot_path,
            state: Arc::new(RwLock::new(state)),
            persistence: PersistenceActor::new(),
            snapshot_index: Arc::new(AtomicU64::new(0)),
            current_snapshot: Arc::new(RwLock::new(current_snapshot)),
            payloads: Arc::new(RwLock::new(None)),
        })
    }

    /// Queues one state image in mutation order and returns the durable
    /// acknowledgement receiver for its callback to await.
    async fn enqueue_persist(
        &self,
        state: StateMachineData,
    ) -> std::io::Result<oneshot::Receiver<std::io::Result<()>>> {
        self.persistence
            .enqueue_json(self.path.clone(), state)
            .await
    }

    /// Queues matching state and snapshot images as one ordered durability
    /// operation, so an interrupted state write cannot publish a newer snapshot.
    async fn enqueue_snapshot_persist(
        &self,
        state: StateMachineData,
        snapshot: StoredSnapshot,
    ) -> std::io::Result<oneshot::Receiver<std::io::Result<()>>> {
        let state_path = self.path.clone();
        let snapshot_path = self.snapshot_path.clone();
        self.persistence
            .enqueue(move || {
                write_json(&state_path, &state)?;
                write_json(&snapshot_path, &Some(snapshot))
            })
            .await
    }

    /// Attaches the durable payload store before snapshot compaction can reclaim bodies.
    pub async fn attach_payload_store(
        &self,
        payloads: Arc<dyn crate::PayloadStore>,
    ) -> Result<(), crate::PayloadError> {
        *self.payloads.write().await = Some(payloads);
        Ok(())
    }

    /// Releases every WAL allocation covered by the current committed archive watermark.
    ///
    /// The caller must commit a matching `ReleaseWal` command only after this
    /// succeeds, so every replica prunes the same metadata after physical
    /// reclamation has completed on all certified holders.
    pub async fn release_checkpointed_wal_payloads(&self) -> Result<(), crate::PayloadError> {
        let payloads = self.payloads.read().await.clone().ok_or_else(|| {
            crate::PayloadError::Transport("payload store is not attached".to_owned())
        })?;
        // Physical reclamation is remote I/O. Clone the durable view before it
        // begins so Raft apply can continue taking the live state write lock.
        let state = self.state.read().await.clone();
        let covered = state
            .committed
            .iter()
            .filter_map(|(index, header)| match header.metadata() {
                crate::EntryMetadata::Wal { end, .. } if end <= state.archive_lsn => {
                    Some((*index, header.clone()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        for (index, header) in covered {
            release_committed_payload(&state, index, &header, payloads.as_ref()).await?;
        }
        Ok(())
    }

    /// Returns one header only after the Raft state machine has applied it.
    pub async fn committed_header(&self, index: u64) -> Option<crate::EntryHeader> {
        self.state.read().await.committed.get(&index).cloned()
    }

    /// Returns the exact OpenRaft term/index that committed a stored header.
    pub async fn committed_log_id(&self, index: u64) -> Option<LogId<u64>> {
        self.state
            .read()
            .await
            .committed_log_ids
            .get(&index)
            .copied()
    }

    /// Returns the latest committed replacement allocation for one immutable header.
    pub async fn repair_certificate(&self, index: u64) -> Option<crate::PayloadCertificate> {
        self.repair_allocation(index)
            .await
            .map(|allocation| allocation.certificate)
    }

    /// Returns the complete latest replacement allocation identity.
    pub async fn repair_allocation(&self, index: u64) -> Option<RepairAllocation> {
        self.state.read().await.repairs.get(&index).cloned()
    }

    /// Returns the currently applied exclusive WAL writer epoch.
    pub async fn writer_epoch(&self) -> u64 {
        self.state.read().await.writer_epoch
    }

    /// Returns the immutable archive bucket committed for this timeline group.
    pub async fn wal_archive_bucket(&self) -> Option<String> {
        self.state.read().await.wal_archive_bucket.clone()
    }

    /// Returns the committed WAL tail and applied archive checkpoint watermark.
    pub async fn wal_archive_state(&self) -> (u64, u64) {
        let state = self.state.read().await;
        (state.wal_end, state.archive_lsn)
    }

    /// Returns ordered checkpoint identities that exactly cover the archived WAL prefix.
    pub async fn archived_wal_segments(
        &self,
        through_lsn: u64,
    ) -> Result<Vec<crate::WalArchiveSegment>, crate::PayloadError> {
        let state = self.state.read().await;
        if through_lsn != state.archive_lsn {
            return Err(crate::PayloadError::CorruptRepresentation);
        }
        let mut cursor = None;
        let mut segments = Vec::new();
        for header in state.committed.values() {
            let crate::EntryMetadata::Archive { segment } = header.metadata() else {
                continue;
            };
            if let Some(previous_end) = cursor
                && segment.start_lsn() != previous_end
            {
                return Err(crate::PayloadError::CorruptRepresentation);
            }
            cursor = Some(segment.end_lsn());
            segments.push(segment.clone());
        }
        if cursor.is_none() || cursor == Some(through_lsn) {
            Ok(segments)
        } else {
            Err(crate::PayloadError::CorruptRepresentation)
        }
    }

    /// Returns the applied metadata pointer for one catalog table.
    pub async fn catalog_table(&self, table: &str) -> Option<String> {
        self.state.read().await.tables.get(table).cloned()
    }

    /// Returns every applied namespace in deterministic lexical order.
    pub async fn catalog_namespaces(&self) -> Vec<String> {
        self.state.read().await.namespaces.iter().cloned().collect()
    }

    /// Returns every table and metadata pointer in deterministic lexical order.
    pub async fn catalog_tables(&self) -> Vec<(String, String)> {
        self.state
            .read()
            .await
            .tables
            .iter()
            .map(|(table, metadata)| (table.clone(), metadata.clone()))
            .collect()
    }

    /// Returns one linearizable hosted-domain document after application.
    pub async fn catalog_record(&self, entity: crate::CatalogEntity, id: &str) -> Option<String> {
        self.state.read().await.records.get(entity, id).cloned()
    }

    /// Returns every hosted-domain document in one deterministic collection.
    pub async fn catalog_records(&self, entity: crate::CatalogEntity) -> Vec<(String, String)> {
        self.state.read().await.records.list(entity)
    }

    /// Returns the committed tenant-root route for one warehouse.
    pub async fn warehouse_group(&self, warehouse: &str) -> Option<String> {
        self.state.read().await.warehouses.get(warehouse).cloned()
    }

    /// Returns the highest state-machine index durably applied on this replica.
    pub async fn applied_index(&self) -> u64 {
        self.state
            .read()
            .await
            .last_applied
            .map_or(0, |log_id| log_id.index)
    }

    /// Returns committed headers in index order for deterministic stream reads.
    pub async fn committed_headers(&self) -> Vec<(u64, crate::EntryHeader)> {
        self.state
            .read()
            .await
            .committed
            .iter()
            .map(|(index, header)| (*index, header.clone()))
            .collect()
    }

    /// Returns the voter identities in the last membership configuration applied by Raft.
    ///
    /// Callers must use this committed set for placement and quorum decisions;
    /// a desired configuration is not authoritative during a joint transition.
    pub async fn committed_voters(&self) -> BTreeSet<u64> {
        self.state
            .read()
            .await
            .membership
            .membership()
            .voter_ids()
            .collect()
    }

    /// Returns the Raft log index of the last committed membership configuration.
    ///
    /// Zero denotes a group that has not yet applied an initialized membership.
    pub async fn membership_generation(&self) -> u64 {
        self.state
            .read()
            .await
            .membership
            .log_id()
            .map_or(0, |log_id| log_id.index)
    }

    /// Returns whether this replica has applied a non-empty Raft membership.
    pub async fn is_initialized(&self) -> bool {
        self.state
            .read()
            .await
            .membership
            .membership()
            .voter_ids()
            .next()
            .is_some()
    }
}

impl RaftStateMachine<VerglasRaftConfig> for PersistentStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let state = self.state.read().await;
        Ok((state.last_applied, state.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<VerglasRaftConfig>> + Send,
    {
        let mut state = self.state.write().await;
        let mut responses = Vec::new();
        for entry in entries {
            state.last_applied = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => responses.push(RaftResponse {
                    index: entry.log_id.index,
                    writer_epoch: None,
                    outcome: AppliedOutcome::Committed,
                    wal_end: None,
                    archive_lsn: None,
                    catalog_changed: false,
                }),
                EntryPayload::Membership(membership) => {
                    state.membership = StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(RaftResponse {
                        index: entry.log_id.index,
                        writer_epoch: None,
                        outcome: AppliedOutcome::Committed,
                        wal_end: None,
                        archive_lsn: None,
                        catalog_changed: false,
                    });
                }
                EntryPayload::Normal(RaftCommand::Commit(header)) => {
                    if let Some(previous) = state.retries.get(&header.request()) {
                        let same = previous.group == header.group()
                            && previous.kind == header.kind()
                            && previous.hash == header.payload_hash()
                            && previous.writer_epoch == header.writer_epoch();
                        responses.push(RaftResponse {
                            index: previous.index,
                            writer_epoch: previous.writer_epoch,
                            outcome: if same {
                                AppliedOutcome::Duplicate
                            } else {
                                AppliedOutcome::ConflictingRetry
                            },
                            wal_end: same.then_some(previous.wal_end).flatten(),
                            archive_lsn: same.then_some(previous.archive_lsn).flatten(),
                            catalog_changed: same && previous.catalog_changed,
                        });
                        continue;
                    }
                    let epoch_valid = match header.kind() {
                        crate::CommandKind::TimelineOpen => state.wal_archive_bucket.is_some(),
                        crate::CommandKind::WalArchiveBinding => true,
                        crate::CommandKind::WriterLease => {
                            state.wal_initialized
                                && header.writer_epoch() == Some(state.writer_epoch + 1)
                        }
                        crate::CommandKind::Wal => {
                            state.writer_active && header.writer_epoch() == Some(state.writer_epoch)
                        }
                        crate::CommandKind::ReleaseWriter => {
                            state.writer_active && header.writer_epoch() == Some(state.writer_epoch)
                        }
                        crate::CommandKind::Catalog => true,
                        crate::CommandKind::CatalogCheckpoint => true,
                        crate::CommandKind::TenantRoot => true,
                        crate::CommandKind::Object => true,
                        crate::CommandKind::ArchiveCheckpoint => true,
                    };
                    if !epoch_valid {
                        responses.push(RaftResponse {
                            index: entry.log_id.index,
                            writer_epoch: Some(state.writer_epoch),
                            outcome: AppliedOutcome::StaleWriterEpoch,
                            wal_end: None,
                            archive_lsn: None,
                            catalog_changed: false,
                        });
                        continue;
                    }
                    let mut wal_end = None;
                    let mut archive_lsn = None;
                    let mut catalog_changed = false;
                    match header.metadata() {
                        crate::EntryMetadata::WalStart { start }
                            if state.wal_initialized && start > state.wal_end =>
                        {
                            responses.push(RaftResponse {
                                index: entry.log_id.index,
                                writer_epoch: None,
                                outcome: AppliedOutcome::WalGap,
                                wal_end: Some(state.wal_end),
                                archive_lsn: None,
                                catalog_changed: false,
                            });
                            continue;
                        }
                        crate::EntryMetadata::WalStart { start } => {
                            if !state.wal_initialized {
                                state.wal_initialized = true;
                                state.wal_end = start;
                                state.archive_lsn = start;
                            }
                            wal_end = Some(state.wal_end);
                        }
                        crate::EntryMetadata::WalArchiveBinding { bucket } => {
                            if let Some(existing) = &state.wal_archive_bucket {
                                if existing != &bucket {
                                    responses.push(RaftResponse {
                                        index: entry.log_id.index,
                                        writer_epoch: None,
                                        outcome: AppliedOutcome::CatalogConflict,
                                        wal_end: None,
                                        archive_lsn: None,
                                        catalog_changed: false,
                                    });
                                    continue;
                                }
                            } else {
                                state.wal_archive_bucket = Some(bucket);
                            }
                        }
                        crate::EntryMetadata::Wal { start, end: _ } if start != state.wal_end => {
                            responses.push(RaftResponse {
                                index: entry.log_id.index,
                                writer_epoch: Some(state.writer_epoch),
                                outcome: AppliedOutcome::WalGap,
                                wal_end: Some(state.wal_end),
                                archive_lsn: None,
                                catalog_changed: false,
                            });
                            continue;
                        }
                        crate::EntryMetadata::Wal { end, .. } => {
                            state.wal_end = end;
                            wal_end = Some(end);
                        }
                        crate::EntryMetadata::Archive { segment }
                            if segment.start_lsn() != state.archive_lsn
                                || segment.end_lsn() > state.wal_end =>
                        {
                            responses.push(RaftResponse {
                                index: entry.log_id.index,
                                writer_epoch: Some(state.writer_epoch),
                                outcome: AppliedOutcome::InvalidArchiveCheckpoint,
                                wal_end: Some(state.wal_end),
                                archive_lsn: Some(state.archive_lsn),
                                catalog_changed: false,
                            });
                            continue;
                        }
                        crate::EntryMetadata::Archive { segment } => {
                            state.archive_lsn = segment.end_lsn();
                            archive_lsn = Some(segment.end_lsn());
                        }
                        crate::EntryMetadata::Catalog { batch } => {
                            let mut namespaces = state.namespaces.clone();
                            let mut tables = state.tables.clone();
                            let mut records = state.records.clone();
                            if batch.apply(&mut namespaces, &mut tables).is_err()
                                || batch.apply_records(&mut records).is_err()
                            {
                                responses.push(RaftResponse {
                                    index: entry.log_id.index,
                                    writer_epoch: None,
                                    outcome: AppliedOutcome::CatalogConflict,
                                    wal_end: None,
                                    archive_lsn: None,
                                    catalog_changed: false,
                                });
                                continue;
                            }
                            state.namespaces = namespaces;
                            state.tables = tables;
                            state.records = records;
                            catalog_changed = true;
                        }
                        crate::EntryMetadata::CatalogCheckpoint { applied_index }
                            if applied_index > entry.log_id.index =>
                        {
                            responses.push(RaftResponse {
                                index: entry.log_id.index,
                                writer_epoch: None,
                                outcome: AppliedOutcome::CatalogConflict,
                                wal_end: None,
                                archive_lsn: None,
                                catalog_changed: false,
                            });
                            continue;
                        }
                        crate::EntryMetadata::CatalogCheckpoint { applied_index } => {
                            state.catalog_checkpoint =
                                Some((applied_index, hex::encode(header.payload_hash())));
                        }
                        crate::EntryMetadata::WarehouseRoute { warehouse, group } => {
                            if let Some(existing) = state.warehouses.get(&warehouse) {
                                if existing != &group {
                                    responses.push(RaftResponse {
                                        index: entry.log_id.index,
                                        writer_epoch: None,
                                        outcome: AppliedOutcome::CatalogConflict,
                                        wal_end: None,
                                        archive_lsn: None,
                                        catalog_changed: false,
                                    });
                                    continue;
                                }
                            } else {
                                state.warehouses.insert(warehouse, group);
                            }
                        }
                        crate::EntryMetadata::None => {}
                    }
                    if header.kind() == crate::CommandKind::WriterLease {
                        wal_end = Some(state.wal_end);
                    }
                    let writer_epoch = header.writer_epoch();
                    if header.kind() == crate::CommandKind::WriterLease {
                        state.writer_epoch = writer_epoch.unwrap_or(state.writer_epoch);
                        state.writer_active = true;
                    } else if header.kind() == crate::CommandKind::ReleaseWriter {
                        state.writer_active = false;
                    }
                    state.retries.insert(
                        header.request(),
                        AppliedIdentity {
                            index: entry.log_id.index,
                            group: header.group().to_owned(),
                            kind: header.kind(),
                            hash: header.payload_hash(),
                            writer_epoch,
                            wal_end,
                            archive_lsn,
                            catalog_changed,
                        },
                    );
                    state.committed.insert(entry.log_id.index, header);
                    state
                        .committed_log_ids
                        .insert(entry.log_id.index, entry.log_id);
                    responses.push(RaftResponse {
                        index: entry.log_id.index,
                        writer_epoch,
                        outcome: AppliedOutcome::Committed,
                        wal_end,
                        archive_lsn,
                        catalog_changed,
                    });
                }
                EntryPayload::Normal(RaftCommand::Repair {
                    index,
                    configuration_generation,
                    certificate,
                }) => {
                    let Some(header) = state.committed.get(&index) else {
                        return Err(StorageIOError::write_state_machine(&std::io::Error::other(
                            "repair references an unknown committed header",
                        ))
                        .into());
                    };
                    if certificate.mode() != header.certificate().mode()
                        || certificate.k() != header.certificate().k()
                        || certificate.m() != header.certificate().m()
                        || certificate.voters().len() != header.certificate().voters().len()
                        || configuration_generation <= header.configuration_generation()
                        || state.repairs.get(&index).is_some_and(|existing| {
                            configuration_generation <= existing.configuration_generation
                        })
                    {
                        return Err(StorageIOError::write_state_machine(&std::io::Error::other(
                            "repair changes immutable payload coding identity",
                        ))
                        .into());
                    }
                    state.repairs.insert(
                        index,
                        RepairAllocation {
                            configuration_generation,
                            certificate,
                            log_id: entry.log_id,
                        },
                    );
                    responses.push(RaftResponse {
                        index: entry.log_id.index,
                        writer_epoch: None,
                        outcome: AppliedOutcome::Committed,
                        wal_end: None,
                        archive_lsn: None,
                        catalog_changed: false,
                    });
                }
                EntryPayload::Normal(RaftCommand::ReleaseWal { through_lsn }) => {
                    if through_lsn > state.archive_lsn {
                        return Err(StorageIOError::write_state_machine(&std::io::Error::other(
                            "WAL release exceeds the committed archive checkpoint",
                        ))
                        .into());
                    }
                    compact_checkpointed_wal_history(&mut state, through_lsn);
                    responses.push(RaftResponse {
                        index: entry.log_id.index,
                        writer_epoch: None,
                        outcome: AppliedOutcome::Committed,
                        wal_end: Some(state.wal_end),
                        archive_lsn: Some(state.archive_lsn),
                        catalog_changed: false,
                    });
                }
            }
        }
        let persisted = self.enqueue_persist(state.clone()).await;
        drop(state);
        let persisted = persisted.map_err(|error| StorageIOError::write_state_machine(&error))?;
        await_persisted(persisted)
            .await
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let mut decoded: StateMachineData = serde_json::from_slice(snapshot.get_ref())
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;
        decoded.last_applied = meta.last_log_id;
        decoded.membership = meta.last_membership.clone();
        let local = self.state.read().await.clone();
        let payloads = self.payloads.read().await.clone();
        if let Some(payloads) = payloads {
            release_catalog_payloads_pruned_by_snapshot(&local, &decoded, payloads.as_ref())
                .await
                .map_err(|error| {
                    StorageIOError::write_state_machine(&std::io::Error::other(error.to_string()))
                })?;
        }
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.get_ref().clone(),
        };
        let mut state = self.state.write().await;
        let persisted = self
            .enqueue_snapshot_persist(decoded.clone(), stored.clone())
            .await;
        *state = decoded;
        drop(state);
        let persisted = persisted
            .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
        await_persisted(persisted)
            .await
            .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
        *self.current_snapshot.write().await = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<VerglasRaftConfig>>, StorageError<u64>> {
        Ok(self
            .current_snapshot
            .read()
            .await
            .clone()
            .map(|stored| Snapshot {
                meta: stored.meta,
                snapshot: Box::new(Cursor::new(stored.data)),
            }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

impl RaftSnapshotBuilder<VerglasRaftConfig> for PersistentStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<VerglasRaftConfig>, StorageError<u64>> {
        let mut state = self.state.write().await;
        if let Some(payloads) = self.payloads.read().await.clone() {
            release_checkpointed_catalog_payloads(&state, payloads.as_ref())
                .await
                .map_err(|error| {
                    StorageIOError::write_state_machine(&std::io::Error::other(error.to_string()))
                })?;
        }
        compact_checkpointed_catalog_history(&mut state);
        let durable = state.clone();
        let bytes = serde_json::to_vec(&durable)
            .map_err(|error| StorageIOError::read_state_machine(&error))?;
        let sequence = self.snapshot_index.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = durable.last_applied.map_or_else(
            || format!("empty-{sequence}"),
            |last| format!("{}-{}-{sequence}", last.leader_id, last.index),
        );
        let meta = SnapshotMeta {
            last_log_id: durable.last_applied,
            last_membership: durable.membership.clone(),
            snapshot_id,
        };
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        };
        let persisted = self.enqueue_snapshot_persist(durable, stored.clone()).await;
        drop(state);
        let persisted = persisted
            .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
        await_persisted(persisted)
            .await
            .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
        *self.current_snapshot.write().await = Some(stored);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

/// Releases every checkpoint-covered catalog allocation before its metadata can be pruned.
async fn release_checkpointed_catalog_payloads(
    state: &StateMachineData,
    payloads: &dyn crate::PayloadStore,
) -> Result<(), crate::PayloadError> {
    let Some((applied_index, _)) = state.catalog_checkpoint else {
        return Ok(());
    };
    for (index, header) in state.committed.range(..=applied_index) {
        if header.kind() != crate::CommandKind::Catalog {
            continue;
        }
        release_committed_payload(state, *index, header, payloads).await?;
    }
    Ok(())
}

/// Releases local catalog bodies that an incoming compacted snapshot has pruned.
async fn release_catalog_payloads_pruned_by_snapshot(
    local: &StateMachineData,
    installed: &StateMachineData,
    payloads: &dyn crate::PayloadStore,
) -> Result<(), crate::PayloadError> {
    let Some((checkpoint_index, _)) = installed.catalog_checkpoint else {
        return Ok(());
    };
    for (index, header) in local.committed.range(..=checkpoint_index) {
        if header.kind() == crate::CommandKind::Catalog && !installed.committed.contains_key(index)
        {
            release_committed_payload(local, *index, header, payloads).await?;
        }
    }
    Ok(())
}

/// Releases the original and latest repaired representation for one catalog header.
async fn release_committed_payload(
    state: &StateMachineData,
    index: u64,
    header: &crate::EntryHeader,
    payloads: &dyn crate::PayloadStore,
) -> Result<(), crate::PayloadError> {
    let log_id = state
        .committed_log_ids
        .get(&index)
        .ok_or(crate::PayloadError::CorruptRepresentation)?;
    payloads
        .release(crate::ReleaseRequest {
            hash: header.payload_hash(),
            group: header.group(),
            configuration_generation: header.configuration_generation(),
            request: header.request(),
            length: header.payload_len(),
            term: log_id.leader_id.term,
            index: log_id.index,
            certificate: header.certificate(),
        })
        .await?;
    if let Some(repair) = state.repairs.get(&index) {
        payloads
            .release(crate::ReleaseRequest {
                hash: header.payload_hash(),
                group: header.group(),
                configuration_generation: repair.configuration_generation,
                request: header.request(),
                length: header.payload_len(),
                term: repair.log_id.leader_id.term,
                index: repair.log_id.index,
                certificate: &repair.certificate,
            })
            .await?;
    }
    Ok(())
}

/// Removes checkpoint-covered WAL headers after every certified representation was reclaimed.
/// Retry identities and archive checkpoint headers remain authoritative.
fn compact_checkpointed_wal_history(state: &mut StateMachineData, through_lsn: u64) {
    let compacted = state
        .committed
        .iter()
        .filter_map(|(index, header)| match header.metadata() {
            crate::EntryMetadata::Wal { end, .. } if end <= through_lsn => Some(*index),
            _ => None,
        })
        .collect::<Vec<_>>();
    for index in compacted {
        state.committed.remove(&index);
        state.committed_log_ids.remove(&index);
        state.repairs.remove(&index);
    }
}

/// Removes catalog command records whose materialized effects are covered by the checkpoint.
/// Retry identities remain because callers must receive the original result after compaction.
fn compact_checkpointed_catalog_history(state: &mut StateMachineData) {
    let Some((applied_index, _)) = state.catalog_checkpoint else {
        return;
    };
    let compacted: Vec<u64> = state
        .committed
        .range(..=applied_index)
        .filter_map(|(index, header)| {
            (header.kind() == crate::CommandKind::Catalog).then_some(*index)
        })
        .collect();
    for index in compacted {
        state.committed.remove(&index);
        state.committed_log_ids.remove(&index);
        state.repairs.remove(&index);
    }
}

/// Reads a JSON image, returning an empty state when it does not exist.
fn read_json<T: for<'de> Deserialize<'de> + Default>(path: &Path) -> std::io::Result<T> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(T::default()),
        Err(error) => Err(error),
    }
}

/// Number of queued Raft image writes allowed per replica. This is a hard
/// backpressure ceiling; a node never accumulates unbounded serialized state
/// while its NVMe is slow.
const PERSISTENCE_QUEUE_CAPACITY: usize = 16;

/// One ordered write plus the acknowledgement its owning Raft callback awaits.
struct PersistenceRequest {
    /// Filesystem work that must reach a durable result before acknowledgement.
    operation: Box<dyn FnOnce() -> std::io::Result<()> + Send>,
    /// Delivers the exact I/O outcome after the dedicated worker finishes.
    completion: oneshot::Sender<std::io::Result<()>>,
}

/// Per-replica ordered filesystem actor. It runs on Tokio's blocking pool,
/// keeping fsync out of Raft RPC workers while preserving mutation order.
#[derive(Clone)]
struct PersistenceActor {
    /// Bounded queue shared by every clone of one persistent store.
    sender: mpsc::Sender<PersistenceRequest>,
}

impl PersistenceActor {
    /// Starts the dedicated actor for one replica's log, state, and snapshots.
    fn new() -> Self {
        let (sender, mut receiver) =
            mpsc::channel::<PersistenceRequest>(PERSISTENCE_QUEUE_CAPACITY);
        tokio::task::spawn_blocking(move || {
            while let Some(request) = receiver.blocking_recv() {
                let result = (request.operation)();
                let _ = request.completion.send(result);
            }
        });
        Self { sender }
    }

    /// Enqueues arbitrary ordered filesystem work and returns its acknowledgement.
    async fn enqueue<F>(
        &self,
        operation: F,
    ) -> std::io::Result<oneshot::Receiver<std::io::Result<()>>>
    where
        F: FnOnce() -> std::io::Result<()> + Send + 'static,
    {
        let (completion, received) = oneshot::channel();
        self.sender
            .send(PersistenceRequest {
                operation: Box::new(operation),
                completion,
            })
            .await
            .map_err(|_| std::io::Error::other("Raft persistence actor stopped"))?;
        Ok(received)
    }

    /// Schedules one operation without waiting behind a saturated durable queue.
    fn try_enqueue<F>(
        &self,
        operation: F,
    ) -> std::io::Result<oneshot::Receiver<std::io::Result<()>>>
    where
        F: FnOnce() -> std::io::Result<()> + Send + 'static,
    {
        let (completion, received) = oneshot::channel();
        self.sender
            .try_send(PersistenceRequest {
                operation: Box::new(operation),
                completion,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    std::io::Error::other("Raft entry persistence queue is full")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    std::io::Error::other("Raft persistence actor stopped")
                }
            })?;
        Ok(received)
    }

    /// Serializes and queues one owned JSON image on the dedicated actor.
    async fn enqueue_json<T>(
        &self,
        path: PathBuf,
        value: T,
    ) -> std::io::Result<oneshot::Receiver<std::io::Result<()>>>
    where
        T: Serialize + Send + 'static,
    {
        self.enqueue(move || write_json(&path, &value)).await
    }

    /// Serializes and queues one framed append-only log record.
    async fn enqueue_log_record(
        &self,
        path: PathBuf,
        record: LogRecord,
    ) -> std::io::Result<oneshot::Receiver<std::io::Result<()>>> {
        self.enqueue(move || append_log_record(&path, &record))
            .await
    }

    /// Schedules one framed append-only log record without delaying the Raft core.
    fn try_enqueue_log_record(
        &self,
        path: PathBuf,
        record: LogRecord,
    ) -> std::io::Result<oneshot::Receiver<std::io::Result<()>>> {
        self.try_enqueue(move || append_log_record(&path, &record))
    }
}

/// Awaits the ordered actor acknowledgement and fails closed if the worker stops.
async fn await_persisted(
    completion: oneshot::Receiver<std::io::Result<()>>,
) -> std::io::Result<()> {
    completion
        .await
        .map_err(|_| std::io::Error::other("Raft persistence actor stopped"))?
}

/// Serializes and atomically persists one JSON image.
fn write_json<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    write_bytes(path, &bytes)
}

/// Replays complete checksummed log records, ignoring only an interrupted final frame.
fn recover_log_records(path: &Path) -> std::io::Result<RecoveredLog> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RecoveredLog::default());
        }
        Err(error) => return Err(error),
    };
    let mut offset = 0_usize;
    let mut recovered = RecoveredLog::default();
    while offset < bytes.len() {
        let Some(header_end) = offset.checked_add(12) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "log frame overflow",
            ));
        };
        if header_end > bytes.len() {
            break;
        }
        let length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "log frame length")
        })?);
        let length = usize::try_from(length).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "log frame too large")
        })?;
        let checksum =
            u32::from_le_bytes(bytes[offset + 8..header_end].try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "log frame checksum")
            })?);
        let Some(frame_end) = header_end.checked_add(length) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "log frame overflow",
            ));
        };
        if frame_end > bytes.len() {
            break;
        }
        let frame = &bytes[header_end..frame_end];
        if crc32c::crc32c(frame) != checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "corrupt Raft log frame",
            ));
        }
        let record = serde_json::from_slice(frame)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        apply_log_record(&mut recovered, record);
        offset = frame_end;
    }
    if offset != bytes.len() {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(u64::try_from(offset).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "log offset too large")
        })?)?;
        file.sync_all()?;
        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent")
        })?;
        File::open(parent)?.sync_all()?;
    }
    Ok(recovered)
}

/// Recovered journal state whose compaction boundary is authoritative over metadata.
#[derive(Default)]
struct RecoveredLog {
    /// Entries visible after every verified record applies.
    entries: BTreeMap<u64, Entry<VerglasRaftConfig>>,
    /// Last exact compaction boundary recorded by the journal.
    last_purged: Option<LogId<u64>>,
}

/// Applies one verified journal record exactly as it was ordered by the Raft storage actor.
fn apply_log_record(recovered: &mut RecoveredLog, record: LogRecord) {
    match record {
        LogRecord::Append(appended) => {
            for entry in appended {
                recovered.entries.insert(entry.log_id.index, entry);
            }
        }
        LogRecord::Truncate { since } => {
            recovered.entries.split_off(&since);
        }
        LogRecord::Purge { log_id } => {
            recovered.last_purged = Some(log_id);
            recovered.entries = recovered.entries.split_off(&(log_id.index + 1));
        }
    }
}

/// Appends and fsyncs one complete checksummed record before OpenRaft receives an acknowledgement.
fn append_log_record(path: &Path, record: &LogRecord) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(record)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let length = u64::try_from(bytes.len()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "log frame too large")
    })?;
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent")
    })?;
    fs::create_dir_all(parent)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&length.to_le_bytes())?;
    file.write_all(&crc32c::crc32c(&bytes).to_le_bytes())?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    File::open(parent)?.sync_all()
}

/// Atomically replaces one file and fsyncs the file and parent directory.
fn write_bytes(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent")
    })?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use openraft::storage::{RaftLogReader, RaftLogStorage};
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Vote};
    use sha2::Digest;
    use tempfile::TempDir;

    use super::{LogRecord, PersistenceActor, PersistentLogStore, await_persisted};
    use crate::{
        CatalogAction, CatalogBatch, CatalogEntity, CommandKind, EntryHeader, PayloadCertificate,
        RaftCommand, ReplicationMode, RequestId, VerglasRaftConfig,
    };

    /// Builds a deliberately large Raft command matching the failed-election reproduction.
    fn large_entry(index: u64) -> Entry<VerglasRaftConfig> {
        let document = format!("\"{}\"", "x".repeat(8 * 1024 * 1024));
        let batch = CatalogBatch::new(
            Vec::new(),
            vec![CatalogAction::PutRecord {
                entity: CatalogEntity::Table,
                id: format!("table-{index}"),
                document,
            }],
        )
        .expect("large catalog batch");
        let header = EntryHeader::new(
            "warehouse/a",
            1,
            CommandKind::Catalog,
            RequestId::from_u128(index.into()),
            0,
            sha2::Sha256::digest([index as u8]).into(),
            None,
            PayloadCertificate::new(ReplicationMode::Coded, 3, 2, vec![1, 2, 3, 4, 5])
                .expect("certificate"),
        )
        .expect("header")
        .with_catalog_batch(batch)
        .expect("catalog metadata");
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(RaftCommand::Commit(header)),
        }
    }

    /// Persists the specified entries through the storage path used by Raft callbacks.
    async fn append_entries(
        store: &PersistentLogStore,
        entries: impl IntoIterator<Item = Entry<VerglasRaftConfig>>,
    ) {
        let mut state = store.state.write().await;
        for entry in entries {
            state.entries.insert(entry.log_id.index, entry);
        }
        let persisted = store
            .enqueue_record(LogRecord::Append(state.entries.values().cloned().collect()))
            .await
            .expect("queue log image");
        drop(state);
        await_persisted(persisted).await.expect("durable log image");
    }

    /// A vote fsync must not serialize or replace the durable large-entry log.
    #[tokio::test]
    async fn vote_persistence_is_independent_of_a_large_append_only_log() {
        let root = TempDir::new().expect("temporary replica root");
        let metadata_path = root.path().join("raft-log.json");
        let framed_log_path = metadata_path.with_extension("entries");
        let mut store = PersistentLogStore::open(metadata_path.clone())
            .await
            .expect("open log store");
        append_entries(&store, (1..=4).map(large_entry)).await;
        let before_vote = fs::read(&framed_log_path).expect("append-only log exists");

        store
            .save_vote(&Vote::new(2, 2))
            .await
            .expect("durable vote");

        assert_eq!(
            fs::read(&framed_log_path).expect("append-only log after vote"),
            before_vote,
            "a RequestVote acknowledgement must persist compact metadata, not rewrite entries"
        );
        assert!(
            fs::metadata(&metadata_path).expect("metadata exists").len() < 1024,
            "vote metadata remains independent of the 32 MiB log"
        );
        drop(store);

        let mut recovered = PersistentLogStore::open(metadata_path)
            .await
            .expect("recover log store");
        assert_eq!(
            recovered.read_vote().await.expect("read recovered vote"),
            Some(Vote::new(2, 2))
        );
        assert_eq!(
            recovered
                .try_get_log_entries(..)
                .await
                .expect("read recovered entries")
                .len(),
            4
        );
    }

    /// Truncation and purge records must survive restart without rewriting prior frames.
    #[tokio::test]
    async fn append_only_log_recovers_truncation_and_purge() {
        let root = TempDir::new().expect("temporary replica root");
        let metadata_path = root.path().join("raft-log.json");
        let framed_log_path = metadata_path.with_extension("entries");
        let mut store = PersistentLogStore::open(metadata_path.clone())
            .await
            .expect("open log store");
        append_entries(&store, (1..=4).map(large_entry)).await;
        let before_compaction = fs::metadata(&framed_log_path)
            .expect("append-only log exists")
            .len();

        store
            .truncate(LogId::new(CommittedLeaderId::new(1, 1), 4))
            .await
            .expect("durable truncate");
        store
            .purge(LogId::new(CommittedLeaderId::new(1, 1), 2))
            .await
            .expect("durable purge");
        assert!(
            fs::metadata(&framed_log_path)
                .expect("append-only log remains")
                .len()
                > before_compaction,
            "truncate and purge append compact durable records"
        );
        drop(store);

        let mut recovered = PersistentLogStore::open(metadata_path)
            .await
            .expect("recover log store");
        let state = recovered.get_log_state().await.expect("recovered state");
        assert_eq!(state.last_purged_log_id.map(|id| id.index), Some(2));
        assert_eq!(state.last_log_id.map(|id| id.index), Some(3));
        assert_eq!(
            recovered
                .try_get_log_entries(..)
                .await
                .expect("recovered entries")
                .into_iter()
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>(),
            vec![3]
        );
    }

    /// A delayed entry fsync must never queue a RequestVote durability barrier behind it.
    #[tokio::test(flavor = "current_thread")]
    async fn delayed_entry_frame_does_not_delay_vote_metadata() {
        let root = TempDir::new().expect("temporary replica root");
        let mut store = PersistentLogStore::open(root.path().join("raft-log.json"))
            .await
            .expect("open log store");
        let (persistence_started, started) = tokio::sync::oneshot::channel();
        let delayed = store
            .entries_persistence
            .enqueue(move || {
                let _ = persistence_started.send(());
                // The vote below must finish while this frame is still held;
                // the sleep is 2x the vote's timeout so a slow CI fsync cannot
                // fake a bypass, and the timeout is generous enough that the
                // vote's own fsync never trips it on a loaded runner.
                std::thread::sleep(Duration::from_millis(1000));
                Ok(())
            })
            .await
            .expect("queue delayed entry frame");
        started.await.expect("entry persistence started");

        tokio::time::timeout(Duration::from_millis(500), store.save_vote(&Vote::new(2, 2)))
            .await
            .expect("vote metadata bypasses the delayed entry actor")
            .expect("durable vote");
        await_persisted(delayed)
            .await
            .expect("delayed entry frame completes");
    }

    /// Entry dispatch returns before fsync, while its completion remains fenced by the frame write.
    #[tokio::test(flavor = "current_thread")]
    async fn append_dispatch_returns_before_its_fsync_completes() {
        let root = TempDir::new().expect("temporary replica root");
        let store = PersistentLogStore::open(root.path().join("raft-log.json"))
            .await
            .expect("open log store");
        let (persistence_started, started) = tokio::sync::oneshot::channel();
        let delayed = store
            .entries_persistence
            .enqueue(move || {
                let _ = persistence_started.send(());
                std::thread::sleep(Duration::from_millis(200));
                Ok(())
            })
            .await
            .expect("queue delayed entry frame");
        started.await.expect("entry persistence started");

        let mut completion = tokio::time::timeout(
            Duration::from_millis(50),
            store.dispatch_append(vec![large_entry(1)]),
        )
        .await
        .expect("append dispatch returns without waiting for fsync")
        .expect("append frame is queued");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut completion)
                .await
                .is_err(),
            "the callback completion cannot precede its frame fsync"
        );
        await_persisted(delayed)
            .await
            .expect("delayed entry frame completes");
        await_persisted(completion)
            .await
            .expect("append frame completion follows fsync");
    }

    /// A slow durable Raft image must not monopolize the runtime that receives
    /// the next vote RPC.
    #[tokio::test(flavor = "current_thread")]
    async fn slow_raft_persistence_keeps_the_runtime_responsive() {
        let (persistence_started, started) = tokio::sync::oneshot::channel();
        let actor = PersistenceActor::new();
        let persist = actor
            .enqueue(move || {
                let _ = persistence_started.send(());
                std::thread::sleep(Duration::from_millis(200));
                Ok(())
            })
            .await;

        started.await.expect("blocking persistence started");
        let mut persist = persist.expect("enqueue persistence");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut persist)
                .await
                .is_err(),
            "the Raft callback must wait for its durable image"
        );
        tokio::time::timeout(Duration::from_millis(50), tokio::task::yield_now())
            .await
            .expect("a vote RPC remains schedulable while the image fsyncs");
        await_persisted(persist).await.expect("durable persistence");
    }

    /// Writes queued before and after a slow image remain in enqueue order, so
    /// a later Raft callback can never overwrite its predecessor on disk.
    #[tokio::test]
    async fn persistence_actor_preserves_queued_image_order() {
        let actor = PersistenceActor::new();
        let writes = Arc::new(Mutex::new(Vec::new()));
        let first_writes = Arc::clone(&writes);
        let first = actor
            .enqueue(move || {
                std::thread::sleep(Duration::from_millis(100));
                first_writes.lock().expect("writes lock").push(1_u8);
                Ok(())
            })
            .await
            .expect("queue first image");
        let second_writes = Arc::clone(&writes);
        let second = actor
            .enqueue(move || {
                second_writes.lock().expect("writes lock").push(2_u8);
                Ok(())
            })
            .await
            .expect("queue second image");

        await_persisted(first).await.expect("first durable image");
        await_persisted(second).await.expect("second durable image");
        assert_eq!(*writes.lock().expect("writes lock"), vec![1, 2]);
    }
}
