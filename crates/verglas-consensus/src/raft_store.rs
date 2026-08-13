//! Crash-safe OpenRaft log and state-machine storage.
//!
//! Every mutating storage callback persists an atomic image before returning.
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
use tokio::sync::RwLock;

use crate::catalog::CatalogRecords;
use crate::{AppliedOutcome, RaftCommand, RaftResponse, RequestId, VerglasRaftConfig};

/// Persistent term, vote, log, and committed-index storage for one Raft replica.
#[derive(Clone)]
pub struct PersistentLogStore {
    path: PathBuf,
    state: Arc<RwLock<LogData>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct LogData {
    last_purged: Option<LogId<u64>>,
    committed: Option<LogId<u64>>,
    vote: Option<Vote<u64>>,
    entries: BTreeMap<u64, Entry<VerglasRaftConfig>>,
}

impl PersistentLogStore {
    /// Opens or creates one replica's Raft log file.
    pub async fn open(path: PathBuf) -> Result<Self, StorageError<u64>> {
        let state = read_json(&path).map_err(|error| StorageIOError::read_logs(&error))?;
        Ok(Self {
            path,
            state: Arc::new(RwLock::new(state)),
        })
    }

    /// Persists the small Raft metadata and log image atomically.
    fn persist(&self, state: &LogData) -> std::io::Result<()> {
        write_json(&self.path, state)
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
            .or(state.last_purged);
        Ok(LogState {
            last_purged_log_id: state.last_purged,
            last_log_id,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let mut state = self.state.write().await;
        state.committed = committed;
        self.persist(&state)
            .map_err(|error| StorageIOError::write_logs(&error).into())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.state.read().await.committed)
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut state = self.state.write().await;
        state.vote = Some(*vote);
        self.persist(&state)
            .map_err(|error| StorageIOError::write_vote(&error).into())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.state.read().await.vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<VerglasRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<VerglasRaftConfig>> + Send,
    {
        let mut state = self.state.write().await;
        for entry in entries {
            state.entries.insert(entry.log_id.index, entry);
        }
        match self.persist(&state) {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
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
        state.entries.split_off(&log_id.index);
        self.persist(&state)
            .map_err(|error| StorageIOError::write_logs(&error).into())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut state = self.state.write().await;
        state.last_purged = Some(log_id);
        state.entries = state.entries.split_off(&(log_id.index + 1));
        self.persist(&state)
            .map_err(|error| StorageIOError::write_logs(&error).into())
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
            snapshot_index: Arc::new(AtomicU64::new(0)),
            current_snapshot: Arc::new(RwLock::new(current_snapshot)),
            payloads: Arc::new(RwLock::new(None)),
        })
    }

    /// Persists one complete applied-state image atomically.
    fn persist(&self, state: &StateMachineData) -> std::io::Result<()> {
        write_json(&self.path, state)
    }

    /// Attaches the durable payload store before snapshot compaction can reclaim bodies.
    pub async fn attach_payload_store(
        &self,
        payloads: Arc<dyn crate::PayloadStore>,
    ) -> Result<(), crate::PayloadError> {
        *self.payloads.write().await = Some(payloads);
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

    /// Returns the committed WAL tail and applied archive checkpoint watermark.
    pub async fn wal_archive_state(&self) -> (u64, u64) {
        let state = self.state.read().await;
        (state.wal_end, state.archive_lsn)
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
                        crate::CommandKind::TimelineOpen => true,
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
                        crate::EntryMetadata::Archive { end }
                            if end < state.archive_lsn || end > state.wal_end =>
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
                        crate::EntryMetadata::Archive { end } => {
                            state.archive_lsn = end;
                            archive_lsn = Some(end);
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
            }
        }
        self.persist(&state)
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
        self.persist(&decoded)
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.get_ref().clone(),
        };
        write_json(&self.snapshot_path, &Some(stored.clone()))
            .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
        *self.current_snapshot.write().await = Some(stored);
        *self.state.write().await = decoded;
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
        self.persist(&state)
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        let bytes = serde_json::to_vec(&*state)
            .map_err(|error| StorageIOError::read_state_machine(&error))?;
        let sequence = self.snapshot_index.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = state.last_applied.map_or_else(
            || format!("empty-{sequence}"),
            |last| format!("{}-{}-{sequence}", last.leader_id, last.index),
        );
        let meta = SnapshotMeta {
            last_log_id: state.last_applied,
            last_membership: state.membership.clone(),
            snapshot_id,
        };
        drop(state);
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        };
        write_json(&self.snapshot_path, &Some(stored.clone()))
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
        release_catalog_payload(state, *index, header, payloads).await?;
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
            release_catalog_payload(local, *index, header, payloads).await?;
        }
    }
    Ok(())
}

/// Releases the original and latest repaired representation for one catalog header.
async fn release_catalog_payload(
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

/// Serializes and atomically persists one JSON image.
fn write_json<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    write_bytes(path, &bytes)
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
