//! Consensus-commit seam for immutable write-back objects.
//!
//! Fragment staging remains in the write-back plane, but an object becomes
//! acknowledged only after the injected universal consensus plane commits its
//! immutable header and durable-placement certificate.

use async_trait::async_trait;

use verglas_cache::writeback_codec::Geometry;
use verglas_core::CacheKey;

use crate::journal::Placement;

/// The immutable object header submitted after coded fragments are durable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedObject {
    /// Customer object identity covered by this command.
    pub key: CacheKey,
    /// Stable fragment namespace for idempotent retry and recovery.
    pub object_id: String,
    /// Logical object length before stripe padding.
    pub object_len: u64,
    /// SHA-256 of the logical immutable object bytes.
    pub payload_hash: [u8; 32],
    /// Committed coded representation geometry.
    pub geometry: Geometry,
    /// Durable fragment holders that certify this representation.
    pub placements: Vec<Placement>,
}

/// The result of committing a staged immutable object header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectCommit {
    /// Universal consensus log index that made the staged object visible.
    pub index: u64,
}

/// One logical key's placement inside a flushed offload-stream pack (#164
/// §4): which byte range of the pack object holds it, and the synthetic
/// identity a read-your-writes response reports for it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedEntry {
    /// Origin key this entry resolves to (the key the client originally PUT).
    pub key: String,
    /// Byte offset of this entry's slice inside the pack object.
    pub offset: u64,
    /// Byte length of this entry's slice inside the pack object.
    pub length: u64,
    /// Synthetic ETag reported for this logical object.
    pub etag: String,
    /// Millisecond Unix time the object was originally acked.
    pub created_ms: u64,
}

/// One flushed offload-stream pack, submitted for a single consensus commit
/// covering every logical key it packed (#164 §4). The index this produces —
/// key to `(pack key, offset, length)` — is authoritative state and commits
/// through [`ConsensusCommitter::commit_pack`], never a sidecar file (gate
/// G8 in `tests/cluster-local/OBJECTIVE.md`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedPack {
    /// Storage binding the pack and every entry inside it belong to.
    pub storage_binding_id: String,
    /// Origin bucket the pack object was uploaded to.
    pub bucket: String,
    /// Origin key of the packed S3 object.
    pub pack_key: String,
    /// Total byte length of the uploaded pack object.
    pub pack_len: u64,
    /// SHA-256 of the exact uploaded pack bytes.
    pub payload_hash: [u8; 32],
    /// Every logical key packed into this flush.
    pub entries: Vec<PackedEntry>,
}

/// The result of committing a staged pack index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackCommit {
    /// Universal consensus log index that made the pack index visible.
    pub index: u64,
}

/// Commits immutable object headers through a configured universal consensus group.
#[async_trait]
pub trait ConsensusCommitter: Send + Sync + 'static {
    /// Commits `staged` only after validating its durable coded certificate.
    async fn commit(&self, staged: StagedObject) -> Result<ObjectCommit, String>;

    /// Commits one flushed offload-stream pack's index as a single
    /// authoritative record (#164 §4). Every logical key inside `staged`
    /// resolves through this commit once it applies.
    async fn commit_pack(&self, staged: StagedPack) -> Result<PackCommit, String>;
}
