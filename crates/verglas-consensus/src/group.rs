//! Safe leader-facing composition of durable payload staging and Raft agreement.
//!
//! Callers cannot submit a bare storage certificate. This boundary stages and
//! verifies the large body first, commits its small header, then serves it only
//! after a linearizable Raft fence and local state-machine application.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::{
    AppliedOutcome, CatalogBatch, CatalogEntity, CommandKind, EntryHeader, PayloadError,
    PayloadStore, PersistentStateMachine, RaftCommand, RaftResponse, ReplicationMode, RequestId,
    VerglasRaftConfig, WalArchiveSegment,
};

/// One independently elected consensus group as seen by its current leader.
pub struct ConsensusGroup {
    group: String,
    configuration_generation: AtomicU64,
    raft: openraft::Raft<VerglasRaftConfig>,
    state_machine: PersistentStateMachine,
    payloads: Arc<dyn PayloadStore>,
    writer_lock: tokio::sync::Mutex<()>,
}

/// A linearizable summary of the committed and object-archived WAL boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalArchiveState {
    applied_index: u64,
    committed_end: u64,
    checkpointed_end: u64,
}

/// A single fenced view used to compose an archived WAL prefix with its retained EC tail.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalReadPlan {
    applied_index: u64,
    committed_end: u64,
    checkpointed_end: u64,
    archived_segments: Vec<WalArchiveSegment>,
    retained_tail: Vec<u8>,
}

/// A deterministic, linearizable export of current catalog names and metadata pointers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CatalogExport {
    /// Applied index the export represents.
    pub applied_index: u64,
    /// Canonical JSON bytes containing ordered namespaces and table pointers.
    pub bytes: Vec<u8>,
}

impl WalArchiveState {
    /// Returns the state-machine index covered by the linearizable fence.
    pub fn applied_index(self) -> u64 {
        self.applied_index
    }
    /// Returns the exclusive LSN at the committed WAL tail.
    pub fn committed_end(self) -> u64 {
        self.committed_end
    }

    /// Returns the exclusive LSN verified by a committed archive checkpoint.
    pub fn checkpointed_end(self) -> u64 {
        self.checkpointed_end
    }
}

impl WalReadPlan {
    /// Returns the applied-index fence all archive identities in this plan satisfy.
    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }

    /// Returns the exclusive committed WAL tail covered by this plan.
    pub fn committed_end(&self) -> u64 {
        self.committed_end
    }

    /// Returns the exclusive end of the object-resident WAL prefix.
    pub fn checkpointed_end(&self) -> u64 {
        self.checkpointed_end
    }

    /// Returns ordered immutable objects that exactly cover the archived prefix.
    pub fn archived_segments(&self) -> &[WalArchiveSegment] {
        &self.archived_segments
    }

    /// Returns exact retained EC bytes for the requested suffix behind this plan's fence.
    pub fn retained_tail(&self) -> &[u8] {
        &self.retained_tail
    }
}

impl ConsensusGroup {
    /// Publishes the Raft-applied uniform membership generation for subsequent headers.
    pub fn refresh_configuration_generation(&self, generation: u64) {
        if generation != 0 {
            self.configuration_generation
                .store(generation, Ordering::Release);
        }
    }

    /// Publishes the uniform committed voter allocation for subsequent staging.
    pub async fn refresh_voters(&self, voters: Vec<u64>) -> Result<(), GroupError> {
        self.payloads.set_voters(voters).await?;
        Ok(())
    }

    /// Returns the leader currently observed by this local Raft replica.
    pub async fn leader_id(&self) -> Option<u64> {
        self.raft.current_leader().await
    }

    /// Executes one universal catalog or WAL operation through this group.
    pub async fn execute(&self, request: GroupRequest) -> Result<GroupResponse, GroupError> {
        match request {
            GroupRequest::BindWalArchive {
                request_id,
                bucket,
                holders,
            } => Ok(GroupResponse::Applied(
                self.bind_wal_archive(RequestId::from_u128(request_id), bucket, &holders)
                    .await?,
            )),
            GroupRequest::WalArchiveBinding => {
                self.read_fence().await?;
                Ok(GroupResponse::WalArchiveBinding(
                    self.state_machine.wal_archive_bucket().await,
                ))
            }
            GroupRequest::OpenTimeline {
                request_id,
                start_lsn,
                holders,
            } => Ok(GroupResponse::Applied(
                self.open_timeline(RequestId::from_u128(request_id), start_lsn, &holders)
                    .await?,
            )),
            GroupRequest::AcquireWriter {
                request_id,
                writer,
                holders,
            } => Ok(GroupResponse::Applied(
                self.acquire_writer(RequestId::from_u128(request_id), &writer, &holders)
                    .await?,
            )),
            GroupRequest::AppendWal {
                request_id,
                writer_epoch,
                start_lsn,
                payload,
                holders,
                mode,
            } => Ok(GroupResponse::Applied(
                self.append_wal_mode(
                    RequestId::from_u128(request_id),
                    writer_epoch,
                    start_lsn,
                    Bytes::from(payload),
                    mode,
                    &holders,
                )
                .await?,
            )),
            GroupRequest::ReadWal {
                from,
                to,
                minimum_index,
            } => self.wal_read_response(from, to, minimum_index).await,
            GroupRequest::WalState { minimum_index } => {
                let state = self.wal_archive_state().await?;
                if state.applied_index < minimum_index {
                    return Err(GroupError::NotCommitted(minimum_index));
                }
                Ok(GroupResponse::WalState(state))
            }
            GroupRequest::ReleaseWriter {
                request_id,
                writer_epoch,
                holders,
            } => Ok(GroupResponse::Applied(
                self.release_writer(RequestId::from_u128(request_id), writer_epoch, &holders)
                    .await?,
            )),
            GroupRequest::ArchiveCheckpoint {
                request_id,
                end_lsn,
                object,
                holders,
            } => Ok(GroupResponse::Applied(
                self.archive_checkpoint(
                    RequestId::from_u128(request_id),
                    end_lsn,
                    &object,
                    &holders,
                )
                .await?,
            )),
            GroupRequest::CommitCatalog {
                request_id,
                batch,
                holders,
            } => Ok(GroupResponse::Applied(
                self.commit_catalog(RequestId::from_u128(request_id), batch, &holders)
                    .await?,
            )),
            GroupRequest::CommitObject {
                request_id,
                payload,
                holders,
                mode,
            } => Ok(GroupResponse::Applied(
                self.commit(
                    RequestId::from_u128(request_id),
                    CommandKind::Object,
                    mode,
                    Bytes::from(payload),
                    None,
                    &holders,
                )
                .await?,
            )),
            GroupRequest::RegisterWarehouse {
                request_id,
                warehouse,
                group,
                holders,
            } => Ok(GroupResponse::WarehouseGroup(
                self.register_warehouse(
                    RequestId::from_u128(request_id),
                    warehouse,
                    group,
                    &holders,
                )
                .await?,
            )),
            GroupRequest::ResolveWarehouse { warehouse } => {
                self.read_fence().await?;
                Ok(GroupResponse::WarehouseGroup(
                    self.state_machine.warehouse_group(&warehouse).await,
                ))
            }
            GroupRequest::CatalogTable { table } => Ok(GroupResponse::CatalogTable(
                self.catalog_table(&table).await?,
            )),
            GroupRequest::CatalogNamespaces => {
                self.read_fence().await?;
                Ok(GroupResponse::CatalogNamespaces(
                    self.state_machine.catalog_namespaces().await,
                ))
            }
            GroupRequest::CatalogTables => {
                self.read_fence().await?;
                Ok(GroupResponse::CatalogTables(
                    self.state_machine.catalog_tables().await,
                ))
            }
            GroupRequest::CatalogRecord { entity, id } => Ok(GroupResponse::CatalogRecord(
                self.catalog_record(entity, &id).await?,
            )),
            GroupRequest::CatalogRecords { entity } => Ok(GroupResponse::CatalogRecords(
                self.catalog_records(entity).await?,
            )),
            GroupRequest::ExportCatalog => {
                Ok(GroupResponse::CatalogExport(self.catalog_export().await?))
            }
            GroupRequest::CatalogCheckpoint {
                request_id,
                applied_index,
                object,
                holders,
            } => Ok(GroupResponse::Applied(
                self.catalog_checkpoint(
                    RequestId::from_u128(request_id),
                    applied_index,
                    &object,
                    &holders,
                )
                .await?,
            )),
        }
    }
    /// Binds a Raft replica and its payload stores to one stable group identity.
    pub fn new(
        group: impl Into<String>,
        configuration_generation: u64,
        raft: openraft::Raft<VerglasRaftConfig>,
        state_machine: PersistentStateMachine,
        payloads: Arc<dyn PayloadStore>,
    ) -> Result<Self, GroupError> {
        let group = group.into();
        if group.is_empty() || configuration_generation == 0 {
            return Err(GroupError::InvalidIdentity);
        }
        Ok(Self {
            group,
            configuration_generation: AtomicU64::new(configuration_generation),
            raft,
            state_machine,
            payloads,
            writer_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Stages a body durably and commits only its immutable header through Raft.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit(
        &self,
        request: RequestId,
        kind: CommandKind,
        mode: ReplicationMode,
        body: Bytes,
        writer_epoch: Option<u64>,
        holders: &[u64],
    ) -> Result<RaftResponse, GroupError> {
        self.commit_header(request, kind, mode, body, writer_epoch, holders, |header| {
            Ok(header)
        })
        .await
    }

    /// Establishes the immutable first PostgreSQL WAL position for a timeline.
    pub async fn open_timeline(
        &self,
        request: RequestId,
        start_lsn: u64,
        holders: &[u64],
    ) -> Result<RaftResponse, GroupError> {
        self.commit_header(
            request,
            CommandKind::TimelineOpen,
            ReplicationMode::Complete,
            Bytes::new(),
            None,
            holders,
            |header| header.with_wal_start(start_lsn),
        )
        .await
    }

    /// Commits the one immutable archive bucket used by this timeline group.
    pub async fn bind_wal_archive(
        &self,
        request: RequestId,
        bucket: String,
        holders: &[u64],
    ) -> Result<RaftResponse, GroupError> {
        self.commit_header(
            request,
            CommandKind::WalArchiveBinding,
            ReplicationMode::Complete,
            Bytes::copy_from_slice(bucket.as_bytes()),
            None,
            holders,
            |header| header.with_wal_archive_bucket(bucket),
        )
        .await
    }

    /// Reads the committed archive bucket after a current-term quorum fence.
    pub async fn wal_archive_binding(&self) -> Result<Option<String>, GroupError> {
        self.read_fence().await?;
        Ok(self.state_machine.wal_archive_bucket().await)
    }

    /// Commits the next exclusive writer epoch for one Neon timeline group.
    pub async fn acquire_writer(
        &self,
        request: RequestId,
        writer: &str,
        holders: &[u64],
    ) -> Result<RaftResponse, GroupError> {
        let _guard = self.writer_lock.lock().await;
        let next = self.state_machine.writer_epoch().await + 1;
        self.commit_header(
            request,
            CommandKind::WriterLease,
            ReplicationMode::Complete,
            Bytes::copy_from_slice(writer.as_bytes()),
            Some(next),
            holders,
            Ok,
        )
        .await
    }

    /// Commits one contiguous WAL byte range under the current writer fence.
    pub async fn append_wal(
        &self,
        request: RequestId,
        writer_epoch: u64,
        start_lsn: u64,
        body: Bytes,
        holders: &[u64],
    ) -> Result<RaftResponse, GroupError> {
        self.append_wal_mode(
            request,
            writer_epoch,
            start_lsn,
            body,
            ReplicationMode::Coded,
            holders,
        )
        .await
    }

    /// Commits WAL using the coded or complete representation selected by the leader.
    pub async fn append_wal_mode(
        &self,
        request: RequestId,
        writer_epoch: u64,
        start_lsn: u64,
        body: Bytes,
        mode: ReplicationMode,
        holders: &[u64],
    ) -> Result<RaftResponse, GroupError> {
        let end = start_lsn
            .checked_add(body.len() as u64)
            .ok_or(GroupError::WalRangeOverflow)?;
        self.commit_header(
            request,
            CommandKind::Wal,
            mode,
            body,
            Some(writer_epoch),
            holders,
            |header| header.with_wal_range(start_lsn, end),
        )
        .await
    }

    /// Commits explicit release of the current writer without permitting reuse.
    pub async fn release_writer(
        &self,
        request: RequestId,
        writer_epoch: u64,
        holders: &[u64],
    ) -> Result<RaftResponse, GroupError> {
        self.commit_header(
            request,
            CommandKind::ReleaseWriter,
            ReplicationMode::Complete,
            Bytes::new(),
            Some(writer_epoch),
            holders,
            Ok,
        )
        .await
    }

    /// Commits a WAL archive watermark only after its object identity is durable.
    pub async fn archive_checkpoint(
        &self,
        request: RequestId,
        end_lsn: u64,
        object: &str,
        holders: &[u64],
    ) -> Result<RaftResponse, GroupError> {
        let segment = WalArchiveSegment::from_key(object.to_owned())
            .map_err(|_| GroupError::InvalidArchiveCheckpoint)?;
        if segment.end_lsn() != end_lsn {
            return Err(GroupError::InvalidArchiveCheckpoint);
        }
        let response = self
            .commit_header(
                request,
                CommandKind::ArchiveCheckpoint,
                ReplicationMode::Complete,
                Bytes::copy_from_slice(object.as_bytes()),
                None,
                holders,
                |header| header.with_archive_segment(segment),
            )
            .await?;
        self.state_machine
            .release_checkpointed_wal_payloads()
            .await?;
        self.raft
            .client_write(crate::RaftCommand::ReleaseWal {
                through_lsn: end_lsn,
            })
            .await
            .map_err(|error| GroupError::Raft(error.to_string()))?;
        Ok(response)
    }

    /// Returns a current-term fenced WAL archive state for background release decisions.
    pub async fn wal_archive_state(&self) -> Result<WalArchiveState, GroupError> {
        // This boundary exposes only Raft-applied WAL metadata, never bytes.
        // A checkpoint may legitimately release its local payload allocation,
        // so verifying every historical payload here would prevent the archiver
        // from advancing the next complete segment after that release.
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|error| GroupError::Raft(error.to_string()))?;
        let (committed_end, checkpointed_end) = self.state_machine.wal_archive_state().await;
        Ok(WalArchiveState {
            applied_index: self.state_machine.applied_index().await,
            committed_end,
            checkpointed_end,
        })
    }

    /// Returns an exact baseline WAL body or an archive-prefix composition response.
    ///
    /// The first attempt is byte-for-byte the regular `ReadWal` path. Only an
    /// unavailable reconstruction below a committed archive watermark selects
    /// object-resident composition; no origin or alternate-data fallback exists.
    async fn wal_read_response(
        &self,
        from: u64,
        to: u64,
        minimum_index: u64,
    ) -> Result<GroupResponse, GroupError> {
        match self.read_wal(from, to, minimum_index).await {
            Ok(bytes) => Ok(GroupResponse::Bytes(bytes.to_vec())),
            Err(error @ GroupError::Payload(PayloadError::ReconstructionUnavailable))
            | Err(error @ GroupError::WalOutOfRange) => {
                let (_, checkpointed_end) = self.state_machine.wal_archive_state().await;
                if from >= checkpointed_end {
                    return Err(error);
                }
                Ok(GroupResponse::ArchivedWalReadPlan(
                    self.wal_read_plan(from, to, minimum_index).await?,
                ))
            }
            Err(error) => Err(error),
        }
    }

    /// Returns archive identities and retained bytes behind the same fence as a WAL read.
    ///
    /// WAL recovery verifies only the selected archive identities and retained
    /// allocations. It must not reconstruct unrelated catalog payloads before
    /// serving a fenced WAL range.
    pub async fn wal_read_plan(
        &self,
        from: u64,
        to: u64,
        minimum_index: u64,
    ) -> Result<WalReadPlan, GroupError> {
        if from >= to {
            return Err(GroupError::WalOutOfRange);
        }
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|error| GroupError::Raft(error.to_string()))?;
        let applied_index = self.state_machine.applied_index().await;
        if applied_index < minimum_index {
            return Err(GroupError::NotCommitted(minimum_index));
        }
        let (committed_end, checkpointed_end) = self.state_machine.wal_archive_state().await;
        if to > committed_end {
            return Err(GroupError::WalOutOfRange);
        }
        let archived_segments = self
            .state_machine
            .archived_wal_segments(checkpointed_end)
            .await?;
        let tail_start = from.max(checkpointed_end);
        let retained_tail = if tail_start < to {
            self.read_wal_after_fence(tail_start, to, applied_index)
                .await?
                .to_vec()
        } else {
            Vec::new()
        };
        Ok(WalReadPlan {
            applied_index,
            committed_end,
            checkpointed_end,
            archived_segments,
            retained_tail,
        })
    }

    /// Permits releasing local WAL only through the committed archive watermark.
    pub async fn release_wal_through(&self, end_lsn: u64) -> Result<WalArchiveState, GroupError> {
        let state = self.wal_archive_state().await?;
        if end_lsn > state.checkpointed_end {
            return Err(GroupError::ArchiveNotCheckpointed {
                requested: end_lsn,
                checkpointed: state.checkpointed_end,
            });
        }
        Ok(state)
    }

    /// Commits one optimistic Iceberg catalog transaction in warehouse order.
    pub async fn commit_catalog(
        &self,
        request: RequestId,
        batch: CatalogBatch,
        holders: &[u64],
    ) -> Result<RaftResponse, GroupError> {
        let body = serde_json::to_vec(&batch).map_err(|_| GroupError::CatalogEncoding)?;
        self.commit_header(
            request,
            CommandKind::Catalog,
            ReplicationMode::Complete,
            Bytes::from(body),
            None,
            holders,
            |header| header.with_catalog_batch(batch),
        )
        .await
    }

    /// Exports the current catalog snapshot after a current-term quorum fence.
    pub async fn catalog_export(&self) -> Result<CatalogExport, GroupError> {
        self.read_fence().await?;
        let namespaces = self.state_machine.catalog_namespaces().await;
        let tables = self.state_machine.catalog_tables().await;
        let bytes =
            serde_json::to_vec(&(namespaces, tables)).map_err(|_| GroupError::CatalogEncoding)?;
        Ok(CatalogExport {
            applied_index: self.state_machine.applied_index().await,
            bytes,
        })
    }

    /// Commits a verified immutable export identity for the exact exported applied index.
    pub async fn catalog_checkpoint(
        &self,
        request: RequestId,
        applied_index: u64,
        object: &str,
        holders: &[u64],
    ) -> Result<RaftResponse, GroupError> {
        self.commit_header(
            request,
            CommandKind::CatalogCheckpoint,
            ReplicationMode::Complete,
            Bytes::copy_from_slice(object.as_bytes()),
            None,
            holders,
            |header| header.with_catalog_checkpoint(applied_index),
        )
        .await
    }

    /// Commits a stable warehouse route through this tenant-root group.
    pub async fn register_warehouse(
        &self,
        request: RequestId,
        warehouse: String,
        route: String,
        holders: &[u64],
    ) -> Result<Option<String>, GroupError> {
        let body = Bytes::from(format!("{warehouse}\0{route}"));
        self.commit_header(
            request,
            CommandKind::TenantRoot,
            ReplicationMode::Complete,
            body,
            None,
            holders,
            |header| header.with_warehouse_route(warehouse.clone(), route),
        )
        .await?;
        self.read_fence().await?;
        Ok(self.state_machine.warehouse_group(&warehouse).await)
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_header(
        &self,
        request: RequestId,
        kind: CommandKind,
        mode: ReplicationMode,
        body: Bytes,
        writer_epoch: Option<u64>,
        holders: &[u64],
        decorate: impl FnOnce(EntryHeader) -> Result<EntryHeader, crate::raft::CertificateError>,
    ) -> Result<RaftResponse, GroupError> {
        let configuration_generation = self.configuration_generation.load(Ordering::Acquire);
        let staged = self
            .payloads
            .stage(
                request,
                &self.group,
                configuration_generation,
                mode,
                &body,
                holders,
            )
            .await?;
        let base = EntryHeader::new(
            self.group.clone(),
            configuration_generation,
            kind,
            request,
            staged.length(),
            staged.hash(),
            writer_epoch,
            staged.certificate().clone(),
        )
        .map_err(|_| GroupError::InvalidCertificate)?;
        let header = decorate(base).map_err(|_| GroupError::InvalidCertificate)?;
        let response = self
            .raft
            .client_write(RaftCommand::Commit(header))
            .await
            .map_err(|error| GroupError::Raft(error.to_string()))?
            .data;
        if matches!(
            response.outcome,
            AppliedOutcome::Committed | AppliedOutcome::Duplicate
        ) {
            let log_id = self
                .state_machine
                .committed_log_id(response.index)
                .await
                .ok_or(GroupError::NotCommitted(response.index))?;
            self.payloads
                .seal(crate::SealRequest {
                    hash: staged.hash(),
                    group: &self.group,
                    configuration_generation,
                    request,
                    term: log_id.leader_id.term,
                    index: log_id.index,
                    certificate: staged.certificate(),
                })
                .await?;
        }
        match response.outcome {
            AppliedOutcome::Committed | AppliedOutcome::Duplicate => Ok(response),
            AppliedOutcome::ConflictingRetry => Err(GroupError::ConflictingRetry),
            AppliedOutcome::StaleWriterEpoch => Err(GroupError::StaleWriterEpoch),
            AppliedOutcome::WalGap => Err(GroupError::WalGap),
            AppliedOutcome::InvalidArchiveCheckpoint => Err(GroupError::InvalidArchiveCheckpoint),
            AppliedOutcome::CatalogConflict => Err(GroupError::CatalogConflict),
        }
    }

    /// Returns a committed body only after a current-term quorum read fence.
    pub async fn read(&self, index: u64) -> Result<Bytes, GroupError> {
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|error| GroupError::Raft(error.to_string()))?;
        let header = self
            .state_machine
            .committed_header(index)
            .await
            .ok_or(GroupError::NotCommitted(index))?;
        if header.group() != self.group {
            return Err(GroupError::WrongGroup);
        }
        let (certificate, configuration_generation, log_id) =
            if let Some(repair) = self.state_machine.repair_allocation(index).await {
                (
                    repair.certificate,
                    repair.configuration_generation,
                    repair.log_id,
                )
            } else {
                (
                    header.certificate().clone(),
                    header.configuration_generation(),
                    self.state_machine
                        .committed_log_id(index)
                        .await
                        .ok_or(GroupError::NotCommitted(index))?,
                )
            };
        self.payloads
            .seal(crate::SealRequest {
                hash: header.payload_hash(),
                group: header.group(),
                configuration_generation,
                request: header.request(),
                term: log_id.leader_id.term,
                index: log_id.index,
                certificate: &certificate,
            })
            .await?;
        self.payloads
            .reconstruct(crate::ReconstructRequest {
                hash: header.payload_hash(),
                group: header.group(),
                configuration_generation,
                request: header.request(),
                length: header.payload_len(),
                term: log_id.leader_id.term,
                index: log_id.index,
                certificate: &certificate,
            })
            .await
            .map_err(GroupError::Payload)
    }

    /// Repairs each committed immutable body then commits its target allocation.
    pub async fn repair_committed(&self, target_voters: Vec<u64>) -> Result<(), GroupError> {
        for (index, header) in self.state_machine.committed_headers().await {
            let (source_certificate, source_generation, source) =
                if let Some(repair) = self.state_machine.repair_allocation(index).await {
                    (
                        repair.certificate,
                        repair.configuration_generation,
                        repair.log_id,
                    )
                } else {
                    (
                        header.certificate().clone(),
                        header.configuration_generation(),
                        self.state_machine
                            .committed_log_id(index)
                            .await
                            .ok_or(GroupError::NotCommitted(index))?,
                    )
                };
            self.payloads
                .seal(crate::SealRequest {
                    hash: header.payload_hash(),
                    group: header.group(),
                    configuration_generation: source_generation,
                    request: header.request(),
                    term: source.leader_id.term,
                    index: source.index,
                    certificate: &source_certificate,
                })
                .await?;
            let target_generation = source_generation
                .checked_add(1)
                .ok_or(GroupError::InvalidCertificate)?;
            let certificate = self
                .payloads
                .repair(crate::RepairRequest {
                    hash: header.payload_hash(),
                    group: header.group(),
                    source_configuration_generation: source_generation,
                    target_configuration_generation: target_generation,
                    request: header.request(),
                    length: header.payload_len(),
                    source_term: source.leader_id.term,
                    source_index: source.index,
                    certificate: &source_certificate,
                    target_voters: target_voters.clone(),
                })
                .await?;
            let response = self
                .raft
                .client_write(RaftCommand::Repair {
                    index,
                    configuration_generation: target_generation,
                    certificate,
                })
                .await
                .map_err(|error| GroupError::Raft(error.to_string()))?
                .data;
            let repair_log_id = self
                .state_machine
                .repair_allocation(index)
                .await
                .ok_or(GroupError::NotCommitted(response.index))?
                .log_id;
            self.payloads
                .seal(crate::SealRequest {
                    hash: header.payload_hash(),
                    group: header.group(),
                    configuration_generation: target_generation,
                    request: header.request(),
                    term: repair_log_id.leader_id.term,
                    index: repair_log_id.index,
                    certificate: self
                        .state_machine
                        .repair_certificate(index)
                        .await
                        .as_ref()
                        .ok_or(GroupError::NotCommitted(index))?,
                })
                .await?;
        }
        Ok(())
    }

    /// Returns one catalog pointer after a current-term quorum read fence.
    pub async fn catalog_table(&self, table: &str) -> Result<Option<String>, GroupError> {
        self.read_fence().await?;
        Ok(self.state_machine.catalog_table(table).await)
    }

    /// Returns one hosted-domain document after a current-term quorum read fence.
    pub async fn catalog_record(
        &self,
        entity: CatalogEntity,
        id: &str,
    ) -> Result<Option<String>, GroupError> {
        self.read_fence().await?;
        Ok(self.state_machine.catalog_record(entity, id).await)
    }

    /// Returns one complete hosted-domain collection after a current-term quorum read fence.
    pub async fn catalog_records(
        &self,
        entity: CatalogEntity,
    ) -> Result<Vec<(String, String)>, GroupError> {
        self.read_fence().await?;
        Ok(self.state_machine.catalog_records(entity).await)
    }

    /// Establishes a current-term quorum fence before an authoritative read.
    async fn read_fence(&self) -> Result<(), GroupError> {
        self.raft
            .ensure_linearizable()
            .await
            .map(|_| ())
            .map_err(|error| GroupError::Raft(error.to_string()))?;
        self.verify_committed_prefix().await
    }

    /// Verifies every committed representation before this replica serves authority.
    async fn verify_committed_prefix(&self) -> Result<(), GroupError> {
        // Prefix entries are independent immutable allocations. Validate them
        // concurrently so one failed former holder adds one transport timeout,
        // not one timeout per retained catalog command. Await every task before
        // admitting the read, so a completed corrupt response cannot be hidden
        // by another valid entry.
        let verified = stream::iter(
            self.state_machine
                .committed_headers()
                .await
                .into_iter()
                .map(|(index, header)| async move {
                    self.verify_committed_header(index, &header).await
                }),
        )
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
        for result in verified {
            result?;
        }
        Ok(())
    }

    /// Seals and reconstructs one exact committed header before authoritative service.
    async fn verify_committed_header(
        &self,
        index: u64,
        header: &EntryHeader,
    ) -> Result<(), GroupError> {
        let (certificate, configuration_generation, log_id) =
            if let Some(repair) = self.state_machine.repair_allocation(index).await {
                (
                    repair.certificate,
                    repair.configuration_generation,
                    repair.log_id,
                )
            } else {
                (
                    header.certificate().clone(),
                    header.configuration_generation(),
                    self.state_machine
                        .committed_log_id(index)
                        .await
                        .ok_or(GroupError::NotCommitted(index))?,
                )
            };
        self.payloads
            .seal(crate::SealRequest {
                hash: header.payload_hash(),
                group: header.group(),
                configuration_generation,
                request: header.request(),
                term: log_id.leader_id.term,
                index: log_id.index,
                certificate: &certificate,
            })
            .await?;
        self.payloads
            .reconstruct(crate::ReconstructRequest {
                hash: header.payload_hash(),
                group: header.group(),
                configuration_generation,
                request: header.request(),
                length: header.payload_len(),
                term: log_id.leader_id.term,
                index: log_id.index,
                certificate: &certificate,
            })
            .await?;
        Ok(())
    }

    /// Reconstructs one committed WAL range after a current leader/read watermark fence.
    pub async fn read_wal(
        &self,
        from: u64,
        to: u64,
        minimum_index: u64,
    ) -> Result<Bytes, GroupError> {
        if from >= to {
            return Err(GroupError::WalOutOfRange);
        }
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|error| GroupError::Raft(error.to_string()))?;
        self.read_wal_after_fence(from, to, minimum_index).await
    }

    /// Reconstructs a retained WAL suffix at an already-established applied-index fence.
    ///
    /// The archive read plan establishes the sole current-term fence for a
    /// composed read. A later routed suffix request need only prove it applied
    /// that exact plan before reconstructing its retained interval.
    pub async fn read_wal_after_fence(
        &self,
        from: u64,
        to: u64,
        minimum_index: u64,
    ) -> Result<Bytes, GroupError> {
        if self.state_machine.applied_index().await < minimum_index {
            return Err(GroupError::NotCommitted(minimum_index));
        }
        let mut cursor = from;
        let mut output = Vec::with_capacity((to - from) as usize);
        for (index, header) in self.state_machine.committed_headers().await {
            let crate::EntryMetadata::Wal { start, end } = header.metadata() else {
                continue;
            };
            if end <= cursor || start >= to {
                continue;
            }
            if start > cursor {
                return Err(GroupError::WalOutOfRange);
            }
            let (certificate, configuration_generation, log_id) =
                if let Some(repair) = self.state_machine.repair_allocation(index).await {
                    (
                        repair.certificate,
                        repair.configuration_generation,
                        repair.log_id,
                    )
                } else {
                    (
                        header.certificate().clone(),
                        header.configuration_generation(),
                        self.state_machine
                            .committed_log_id(index)
                            .await
                            .ok_or(GroupError::NotCommitted(index))?,
                    )
                };
            self.payloads
                .seal(crate::SealRequest {
                    hash: header.payload_hash(),
                    group: header.group(),
                    configuration_generation,
                    request: header.request(),
                    term: log_id.leader_id.term,
                    index: log_id.index,
                    certificate: &certificate,
                })
                .await?;
            let body = self
                .payloads
                .reconstruct(crate::ReconstructRequest {
                    hash: header.payload_hash(),
                    group: header.group(),
                    configuration_generation,
                    request: header.request(),
                    length: header.payload_len(),
                    term: log_id.leader_id.term,
                    index: log_id.index,
                    certificate: &certificate,
                })
                .await?;
            let take_from = cursor.saturating_sub(start) as usize;
            let take_to = (to.min(end) - start) as usize;
            output.extend_from_slice(
                body.get(take_from..take_to)
                    .ok_or(GroupError::WalOutOfRange)?,
            );
            cursor = to.min(end);
            if cursor == to {
                return Ok(Bytes::from(output));
            }
        }
        Err(GroupError::WalOutOfRange)
    }
}

/// Universal command envelope routed through any cache ingress.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GroupRequest {
    /// Binds a timeline group to its immutable WAL archive bucket before writes.
    BindWalArchive {
        request_id: u128,
        bucket: String,
        holders: Vec<u64>,
    },
    /// Reads the linearizable archive bucket binding for this timeline group.
    WalArchiveBinding,
    /// Establishes the immutable first PostgreSQL WAL position for a timeline.
    OpenTimeline {
        request_id: u128,
        start_lsn: u64,
        holders: Vec<u64>,
    },
    /// Acquire the next exclusive timeline writer.
    AcquireWriter {
        request_id: u128,
        writer: String,
        holders: Vec<u64>,
    },
    /// Append one exact WAL range using the leader-selected representation mode.
    AppendWal {
        request_id: u128,
        writer_epoch: u64,
        start_lsn: u64,
        payload: Vec<u8>,
        holders: Vec<u64>,
        mode: ReplicationMode,
    },
    /// Read a fenced committed WAL range.
    ReadWal {
        from: u64,
        to: u64,
        minimum_index: u64,
    },
    /// Read current WAL boundaries at a linearizable applied-index fence.
    WalState { minimum_index: u64 },
    /// Release one live writer epoch.
    ReleaseWriter {
        request_id: u128,
        writer_epoch: u64,
        holders: Vec<u64>,
    },
    /// Commit a verified archive watermark.
    ArchiveCheckpoint {
        request_id: u128,
        end_lsn: u64,
        object: String,
        holders: Vec<u64>,
    },
    /// Apply one authoritative Iceberg transaction.
    CommitCatalog {
        request_id: u128,
        batch: CatalogBatch,
        holders: Vec<u64>,
    },
    /// Publish one immutable object's already-staged placement certificate.
    CommitObject {
        request_id: u128,
        payload: Vec<u8>,
        holders: Vec<u64>,
        /// The representation mode selected by the staged immutable object.
        mode: ReplicationMode,
    },
    /// Register a warehouse route in the tenant-root group.
    RegisterWarehouse {
        request_id: u128,
        warehouse: String,
        group: String,
        holders: Vec<u64>,
    },
    /// Resolve a warehouse route from the tenant-root group.
    ResolveWarehouse { warehouse: String },
    /// Read one authoritative catalog pointer.
    CatalogTable { table: String },
    /// Lists authoritative namespaces at a linearizable fence.
    CatalogNamespaces,
    /// Lists authoritative tables and metadata pointers at a linearizable fence.
    CatalogTables,
    /// Reads one typed hosted catalog document.
    CatalogRecord { entity: CatalogEntity, id: String },
    /// Lists one typed hosted catalog document collection.
    CatalogRecords { entity: CatalogEntity },
    /// Exports the current catalog state at a linearizable fence for lifecycle checkpointing.
    ExportCatalog,
    /// Commits a verified immutable catalog export checkpoint.
    CatalogCheckpoint {
        request_id: u128,
        applied_index: u64,
        object: String,
        holders: Vec<u64>,
    },
}

/// Universal response produced only by an applied state machine or read fence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GroupResponse {
    /// The immutable WAL archive bucket bound to this timeline, if provisioned.
    WalArchiveBinding(Option<String>),
    /// One state transition applied through Raft.
    Applied(RaftResponse),
    /// One committed WAL range.
    Bytes(Vec<u8>),
    /// Current committed WAL and archive boundaries.
    WalState(WalArchiveState),
    /// One current-term-fenced archive manifest and retained WAL boundary.
    ArchivedWalReadPlan(WalReadPlan),
    /// One current Iceberg metadata pointer.
    CatalogTable(Option<String>),
    /// Lexically ordered authoritative namespaces.
    CatalogNamespaces(Vec<String>),
    /// Lexically ordered table identifiers and metadata pointers.
    CatalogTables(Vec<(String, String)>),
    /// One typed hosted catalog document, if present.
    CatalogRecord(Option<String>),
    /// Lexically ordered typed hosted catalog documents.
    CatalogRecords(Vec<(String, String)>),
    /// Deterministic catalog export bound to its applied index.
    CatalogExport(CatalogExport),
    /// One resolved tenant-root warehouse route.
    WarehouseGroup(Option<String>),
}

/// A closed failure at the sole payload-and-agreement boundary.
#[derive(Debug, thiserror::Error)]
pub enum GroupError {
    /// The stable group identity or generation is invalid.
    #[error("invalid consensus group identity")]
    InvalidIdentity,
    /// Durable staging failed before Raft submission.
    #[error(transparent)]
    Payload(#[from] PayloadError),
    /// The staged proof could not form a valid immutable header.
    #[error("invalid payload certificate")]
    InvalidCertificate,
    /// Raft refused leadership, quorum, or storage progress.
    #[error("Raft operation failed: {0}")]
    Raft(String),
    /// The requested index has not applied on this replica.
    #[error("consensus index {0} is not committed locally")]
    NotCommitted(u64),
    /// The committed header belongs to another group generation.
    #[error("committed header belongs to another group")]
    WrongGroup,
    /// An exact retry identity was reused for different content.
    #[error("request identity conflicts with an earlier command")]
    ConflictingRetry,
    /// A WAL writer epoch was fenced before the command applied.
    #[error("writer epoch is stale")]
    StaleWriterEpoch,
    /// The WAL range overflowed its unsigned LSN space.
    #[error("WAL range overflows the LSN space")]
    WalRangeOverflow,
    /// The WAL append did not begin at the committed tail.
    #[error("WAL append creates a gap or conflicting overlap")]
    WalGap,
    /// The archive watermark regressed or passed the committed WAL tail.
    #[error("archive checkpoint is outside committed WAL")]
    InvalidArchiveCheckpoint,
    /// Local WAL release was requested beyond the committed archive checkpoint.
    #[error("WAL release through {requested} exceeds archive checkpoint {checkpointed}")]
    ArchiveNotCheckpointed {
        /// Exclusive release LSN.
        requested: u64,
        /// Highest committed archive watermark.
        checkpointed: u64,
    },
    /// The typed catalog batch could not be encoded canonically.
    #[error("catalog transaction encoding failed")]
    CatalogEncoding,
    /// An optimistic catalog requirement failed at its applied index.
    #[error("catalog transaction conflicts with current state")]
    CatalogConflict,
    /// The requested WAL range is not fully present in the committed stream.
    #[error("WAL range is outside the committed stream")]
    WalOutOfRange,
}
