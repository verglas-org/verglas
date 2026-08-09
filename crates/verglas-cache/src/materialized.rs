//! Versioned PostgreSQL page identities stored in the shared hybrid cache.
//! The physical key uses the relation block as the object and the requested
//! LSN as its immutable version, so Neon pages and Iceberg blocks share one
//! admission and eviction domain without aliasing page versions.

use verglas_core::{BlockKey, CacheKey};

/// PostgreSQL's fixed page size. The shared cache accepts only complete pages.
pub const POSTGRES_PAGE_BYTES: usize = 8192;

/// Exact identity of one reconstructed PostgreSQL page.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaterializedPageKey {
    /// Neon tenant shard that owns the page.
    pub tenant_shard_id: String,
    /// Neon timeline containing the requested version.
    pub timeline_id: String,
    /// PostgreSQL tablespace OID.
    pub spcnode: u32,
    /// PostgreSQL database OID.
    pub dbnode: u32,
    /// PostgreSQL relation file OID.
    pub relnode: u32,
    /// PostgreSQL relation fork number.
    pub forknum: u8,
    /// Block number within the relation fork.
    pub block: u32,
    /// Exact Neon LSN used to reconstruct the page.
    pub lsn: u64,
}

impl MaterializedPageKey {
    /// Converts the page identity into the same physical block key used by the
    /// hybrid Iceberg cache. The stable relation block is the object identity;
    /// the LSN is the immutable version.
    pub(crate) fn block_key(&self) -> BlockKey {
        BlockKey {
            object: CacheKey {
                bucket: "@verglas-neon-pages".to_owned(),
                key: format!(
                    "{}/{}/{}/{}/{}/{}/{}",
                    self.tenant_shard_id,
                    self.timeline_id,
                    self.spcnode,
                    self.dbnode,
                    self.relnode,
                    self.forknum,
                    self.block
                ),
            },
            etag: format!("{:016x}", self.lsn),
            block_bytes: POSTGRES_PAGE_BYTES as u64,
            block_index: 0,
        }
    }
}

/// A reconstructed page cannot enter the shared cache unless it is complete.
#[derive(Debug, thiserror::Error)]
pub enum MaterializedPageError {
    /// The supplied value was not one PostgreSQL page.
    #[error("materialized PostgreSQL page must be {POSTGRES_PAGE_BYTES} bytes, got {actual}")]
    InvalidSize {
        /// Supplied byte length.
        actual: usize,
    },
    /// A peer attempted to place a non-page key or sent it to a non-owner.
    #[error("invalid materialized PostgreSQL page block or rendezvous owner")]
    InvalidBlock,
}
