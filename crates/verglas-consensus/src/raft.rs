//! Raft-replicated command types for the universal consensus substrate.
//!
//! Payload bytes never enter this log. A leader first stages verifiable payload
//! representations, then commits the immutable certificate here. Terms, votes,
//! log matching, commit indexes, and membership are owned by OpenRaft.

use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use sha2::Digest;

use crate::{CatalogBatch, CommandKind, ReplicationMode, RequestId};

/// Immutable object identity for one contiguous WAL segment named by a committed checkpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalArchiveSegment {
    start_lsn: u64,
    end_lsn: u64,
    key: String,
    hash: [u8; 32],
}

impl WalArchiveSegment {
    /// Decodes the canonical content-addressed key emitted by the WAL archiver.
    pub fn from_key(key: String) -> Result<Self, CertificateError> {
        let Some(name) = key.rsplit('/').next() else {
            return Err(CertificateError { required: 1 });
        };
        let mut fields = name.split('-');
        let (Some(start), Some(end), Some(hash), None) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            return Err(CertificateError { required: 1 });
        };
        if start.len() != 16 || end.len() != 16 || hash.len() != 64 {
            return Err(CertificateError { required: 1 });
        }
        let start_lsn =
            u64::from_str_radix(start, 16).map_err(|_| CertificateError { required: 1 })?;
        let end_lsn = u64::from_str_radix(end, 16).map_err(|_| CertificateError { required: 1 })?;
        let bytes = hex::decode(hash).map_err(|_| CertificateError { required: 1 })?;
        if hex::encode(&bytes) != hash {
            return Err(CertificateError { required: 1 });
        }
        let hash: [u8; 32] = bytes
            .try_into()
            .map_err(|_| CertificateError { required: 1 })?;
        if start_lsn >= end_lsn || key.is_empty() {
            return Err(CertificateError { required: 1 });
        }
        Ok(Self {
            start_lsn,
            end_lsn,
            key,
            hash,
        })
    }

    /// Returns the inclusive first LSN covered by this object.
    pub fn start_lsn(&self) -> u64 {
        self.start_lsn
    }

    /// Returns the exclusive final LSN covered by this object.
    pub fn end_lsn(&self) -> u64 {
        self.end_lsn
    }

    /// Returns the immutable object key recorded in the checkpoint payload.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the SHA-256 hash that the object key names.
    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }

    /// Returns the exact byte length implied by the checkpoint LSN interval.
    pub fn length(&self) -> u64 {
        self.end_lsn - self.start_lsn
    }
}

openraft::declare_raft_types!(
    /// Concrete OpenRaft types used by every Verglas consensus group.
    pub VerglasRaftConfig:
        D = RaftCommand,
        R = RaftResponse,
        NodeId = u64,
        Node = BasicNode,
);

/// One payload durability certificate committed by Raft.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PayloadCertificate {
    mode: ReplicationMode,
    k: usize,
    m: usize,
    voters: Vec<u64>,
    holders: Vec<u64>,
}

impl PayloadCertificate {
    /// Validates coded-quorum intersection or complete-entry majority durability.
    pub fn new(
        mode: ReplicationMode,
        k: usize,
        m: usize,
        holders: Vec<u64>,
    ) -> Result<Self, CertificateError> {
        let voters = (1..=k.checked_add(m).ok_or(CertificateError {
            required: usize::MAX,
        })? as u64)
            .collect();
        Self::with_voters(mode, k, m, voters, holders)
    }

    /// Validates one certificate against its exact immutable voter ordering.
    pub fn with_voters(
        mode: ReplicationMode,
        k: usize,
        m: usize,
        voters: Vec<u64>,
        mut holders: Vec<u64>,
    ) -> Result<Self, CertificateError> {
        holders.sort_unstable();
        holders.dedup();
        let voter_count = k.checked_add(m).ok_or(CertificateError {
            required: usize::MAX,
        })?;
        if k == 0
            || voter_count == 0
            || voters.len() != voter_count
            || voters.contains(&0)
            || voters
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != voter_count
            || holders.contains(&0)
            || !holders.iter().all(|holder| voters.contains(holder))
        {
            return Err(CertificateError {
                required: voter_count,
            });
        }
        let election_quorum = voter_count / 2 + 1;
        let required = match mode {
            ReplicationMode::Coded => voter_count - election_quorum + k,
            ReplicationMode::Complete => election_quorum,
        };
        if holders.len() < required || holders.len() > voter_count {
            return Err(CertificateError { required });
        }
        Ok(Self {
            mode,
            k,
            m,
            voters,
            holders,
        })
    }

    /// Returns the immutable payload storage mode.
    pub fn mode(&self) -> ReplicationMode {
        self.mode
    }

    /// Returns the data-fragment reconstruction threshold.
    pub fn k(&self) -> usize {
        self.k
    }

    /// Returns the parity-fragment count.
    pub fn m(&self) -> usize {
        self.m
    }

    /// Returns the exact committed voter ordering that assigns representation slots.
    pub fn voters(&self) -> &[u64] {
        &self.voters
    }

    /// Returns the voters that durably staged the selected representation.
    pub fn holders(&self) -> &[u64] {
        &self.holders
    }
}

/// A payload certificate that cannot meet the selected commit rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("payload certificate requires {required} distinct durable voters")]
pub struct CertificateError {
    required: usize,
}

impl CertificateError {
    /// Returns the minimum number of durable representations required.
    pub fn required(&self) -> usize {
        self.required
    }
}

/// The immutable small command replicated identically by Raft.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EntryHeader {
    group: String,
    configuration_generation: u64,
    kind: CommandKind,
    request: RequestId,
    payload_len: u64,
    payload_hash: [u8; 32],
    writer_epoch: Option<u64>,
    certificate: PayloadCertificate,
    metadata: EntryMetadata,
}

impl EntryHeader {
    /// Builds a header after validating group identity and coding geometry.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        group: impl Into<String>,
        configuration_generation: u64,
        kind: CommandKind,
        request: RequestId,
        payload_len: u64,
        payload_hash: [u8; 32],
        writer_epoch: Option<u64>,
        certificate: PayloadCertificate,
    ) -> Result<Self, CertificateError> {
        let group = group.into();
        if group.is_empty() {
            return Err(CertificateError { required: 1 });
        }
        Ok(Self {
            group,
            configuration_generation,
            kind,
            request,
            payload_len,
            payload_hash,
            writer_epoch,
            certificate,
            metadata: EntryMetadata::None,
        })
    }

    /// Binds a WAL byte range whose length must equal the staged payload length.
    pub fn with_wal_range(mut self, start: u64, end: u64) -> Result<Self, CertificateError> {
        if self.kind != CommandKind::Wal || end.checked_sub(start) != Some(self.payload_len) {
            return Err(CertificateError { required: 1 });
        }
        self.metadata = EntryMetadata::Wal { start, end };
        Ok(self)
    }

    /// Binds the immutable first LSN of a newly created PostgreSQL timeline.
    pub fn with_wal_start(mut self, start: u64) -> Result<Self, CertificateError> {
        if self.kind != CommandKind::TimelineOpen || self.payload_len != 0 {
            return Err(CertificateError { required: 1 });
        }
        self.metadata = EntryMetadata::WalStart { start };
        Ok(self)
    }

    /// Binds this timeline group to one non-empty WAL archive bucket.
    pub fn with_wal_archive_bucket(mut self, bucket: String) -> Result<Self, CertificateError> {
        if self.kind != CommandKind::WalArchiveBinding
            || self.payload_len != bucket.len() as u64
            || bucket.is_empty()
        {
            return Err(CertificateError { required: 1 });
        }
        self.metadata = EntryMetadata::WalArchiveBinding { bucket };
        Ok(self)
    }

    /// Binds a verified object-store WAL archive watermark.
    pub fn with_archive_segment(
        mut self,
        segment: WalArchiveSegment,
    ) -> Result<Self, CertificateError> {
        let key_hash: [u8; 32] = sha2::Sha256::digest(segment.key.as_bytes()).into();
        if self.kind != CommandKind::ArchiveCheckpoint
            || self.payload_len != segment.key.len() as u64
            || self.payload_hash != key_hash
        {
            return Err(CertificateError { required: 1 });
        }
        self.metadata = EntryMetadata::Archive { segment };
        Ok(self)
    }

    /// Binds one deterministic catalog batch into the replicated header.
    pub fn with_catalog_batch(mut self, batch: CatalogBatch) -> Result<Self, CertificateError> {
        if self.kind != CommandKind::Catalog {
            return Err(CertificateError { required: 1 });
        }
        self.metadata = EntryMetadata::Catalog { batch };
        Ok(self)
    }

    /// Binds a verified catalog export object and the applied index it represents.
    pub fn with_catalog_checkpoint(mut self, applied_index: u64) -> Result<Self, CertificateError> {
        if self.kind != CommandKind::CatalogCheckpoint || applied_index == 0 {
            return Err(CertificateError { required: 1 });
        }
        self.metadata = EntryMetadata::CatalogCheckpoint { applied_index };
        Ok(self)
    }

    /// Binds a warehouse route into a tenant-root group.
    pub fn with_warehouse_route(
        mut self,
        warehouse: String,
        group: String,
    ) -> Result<Self, CertificateError> {
        if self.kind != CommandKind::TenantRoot || warehouse.is_empty() || group.is_empty() {
            return Err(CertificateError { required: 1 });
        }
        self.metadata = EntryMetadata::WarehouseRoute { warehouse, group };
        Ok(self)
    }

    /// Returns the independently routed consensus-group identity.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Returns the committed membership generation used for representation placement.
    pub fn configuration_generation(&self) -> u64 {
        self.configuration_generation
    }

    /// Returns the exact client retry identity.
    pub fn request(&self) -> RequestId {
        self.request
    }

    /// Returns the logical command family.
    pub fn kind(&self) -> CommandKind {
        self.kind
    }

    /// Returns the immutable content hash.
    pub fn payload_hash(&self) -> [u8; 32] {
        self.payload_hash
    }

    /// Returns the logical body length bound into this immutable header.
    pub fn payload_len(&self) -> u64 {
        self.payload_len
    }

    /// Returns the WAL writer fence bound into this command, when present.
    pub fn writer_epoch(&self) -> Option<u64> {
        self.writer_epoch
    }

    /// Returns the durable payload certificate.
    pub fn certificate(&self) -> &PayloadCertificate {
        &self.certificate
    }

    /// Returns deterministic command metadata needed without decoding its body.
    pub fn metadata(&self) -> EntryMetadata {
        self.metadata.clone()
    }
}

/// Small domain metadata fully replicated with every Raft header.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EntryMetadata {
    /// No extra ordering fields are needed for this command.
    None,
    /// Immutable first LSN for a PostgreSQL timeline.
    WalStart {
        /// First byte accepted by the timeline WAL stream.
        start: u64,
    },
    /// Immutable object-store bucket assigned to this timeline's WAL archive.
    WalArchiveBinding { bucket: String },
    /// Exact half-open WAL byte range.
    Wal {
        /// First appended LSN.
        start: u64,
        /// One past the final appended LSN.
        end: u64,
    },
    /// One exact immutable WAL object verified in object storage.
    Archive {
        /// Exact interval, key, and content identity of the archive object.
        segment: WalArchiveSegment,
    },
    /// Typed Iceberg catalog requirements and actions.
    Catalog {
        /// Atomic transaction batch.
        batch: CatalogBatch,
    },
    /// Verified immutable export bound to the catalog applied index it represents.
    CatalogCheckpoint { applied_index: u64 },
    /// Tenant-root route registration.
    WarehouseRoute { warehouse: String, group: String },
}

/// Application commands carried by the Raft log.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RaftCommand {
    /// Makes a previously staged payload visible at this committed log index.
    Commit(EntryHeader),
    /// Records a target allocation after every representation was durably repaired.
    Repair {
        index: u64,
        configuration_generation: u64,
        certificate: PayloadCertificate,
    },
    /// Prunes WAL allocation metadata only after its object-store checkpoint is committed.
    ReleaseWal { through_lsn: u64 },
}

/// Deterministic state-machine response returned after an entry applies.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RaftResponse {
    /// Raft log index that committed the header.
    pub index: u64,
    /// Writer epoch installed by the command, when applicable.
    pub writer_epoch: Option<u64>,
    /// Deterministic result produced by applying the command in log order.
    pub outcome: AppliedOutcome,
    /// Applied WAL tail, for append commands.
    pub wal_end: Option<u64>,
    /// Applied archive watermark, for checkpoint commands.
    pub archive_lsn: Option<u64>,
    /// True when this command atomically changed catalog state.
    pub catalog_changed: bool,
}

/// Deterministic semantic result of an applied Raft command.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AppliedOutcome {
    /// The command changed the authoritative state at this index.
    Committed,
    /// The same exact request had already committed and returned its old index.
    Duplicate,
    /// The request identity was previously bound to different command content.
    ConflictingRetry,
    /// A WAL command or lease transition carried a stale writer epoch.
    StaleWriterEpoch,
    /// A WAL append did not begin at the current committed tail.
    WalGap,
    /// An archive watermark regressed or exceeded committed WAL.
    InvalidArchiveCheckpoint,
    /// One or more optimistic catalog requirements failed.
    CatalogConflict,
}
