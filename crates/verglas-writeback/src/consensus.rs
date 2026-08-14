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

/// Commits immutable object headers through a configured universal consensus group.
#[async_trait]
pub trait ConsensusCommitter: Send + Sync + 'static {
    /// Commits `staged` only after validating its durable coded certificate.
    async fn commit(&self, staged: StagedObject) -> Result<ObjectCommit, String>;
}
