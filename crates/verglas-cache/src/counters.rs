//! Lock-free counters for the single-node cache read path.
//!
//! Every field is monotonic except the cache usage gauge exposed by the engine.
//! Relaxed atomics keep recording independent of request coordination.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic counters recorded by the cache engine.
#[derive(Debug, Default)]
pub struct CacheCounters {
    /// Block lookups answered by DRAM.
    pub dram_hits: AtomicU64,
    /// Block lookups that missed DRAM.
    pub dram_misses: AtomicU64,
    /// Block lookups answered by persistent storage.
    pub disk_hits: AtomicU64,
    /// Block lookups that missed persistent storage.
    pub disk_misses: AtomicU64,
    /// Bytes served by DRAM.
    pub dram_bytes_served: AtomicU64,
    /// Bytes served by persistent storage.
    pub disk_bytes_served: AtomicU64,
    /// Bytes served by an origin fill.
    pub backend_bytes_served: AtomicU64,
    /// Origin GETs used for block fills.
    pub backend_fills: AtomicU64,
    /// Bytes fetched by successful fills.
    pub backend_fill_bytes: AtomicU64,
    /// Origin HEAD requests used to resolve ETags.
    pub backend_heads: AtomicU64,
    /// Requests that bypassed admission because the origin omitted an ETag.
    pub non_cacheable_passthroughs: AtomicU64,
    /// Waiters that joined an in-flight fill.
    pub deduped_fills: AtomicU64,
    /// Blocks admitted to Foyer.
    pub blocks_admitted: AtomicU64,
    /// Blocks rejected by the admission policy.
    pub blocks_rejected: AtomicU64,
    /// Foyer storage writes dropped because its bounded queue was full.
    pub storage_buffer_overflows: Arc<AtomicU64>,
}

/// Point-in-time copy of [`CacheCounters`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CountersSnapshot {
    /// See [`CacheCounters::dram_hits`].
    pub dram_hits: u64,
    /// See [`CacheCounters::dram_misses`].
    pub dram_misses: u64,
    /// See [`CacheCounters::disk_hits`].
    pub disk_hits: u64,
    /// See [`CacheCounters::disk_misses`].
    pub disk_misses: u64,
    /// See [`CacheCounters::dram_bytes_served`].
    pub dram_bytes_served: u64,
    /// See [`CacheCounters::disk_bytes_served`].
    pub disk_bytes_served: u64,
    /// See [`CacheCounters::backend_bytes_served`].
    pub backend_bytes_served: u64,
    /// See [`CacheCounters::backend_fills`].
    pub backend_fills: u64,
    /// See [`CacheCounters::backend_fill_bytes`].
    pub backend_fill_bytes: u64,
    /// See [`CacheCounters::backend_heads`].
    pub backend_heads: u64,
    /// See [`CacheCounters::non_cacheable_passthroughs`].
    pub non_cacheable_passthroughs: u64,
    /// See [`CacheCounters::deduped_fills`].
    pub deduped_fills: u64,
    /// See [`CacheCounters::blocks_admitted`].
    pub blocks_admitted: u64,
    /// See [`CacheCounters::blocks_rejected`].
    pub blocks_rejected: u64,
    /// See [`CacheCounters::storage_buffer_overflows`].
    pub storage_buffer_overflows: u64,
}

impl CacheCounters {
    /// Increments one counter without imposing ordering between counters.
    pub(crate) fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds bytes to one counter without imposing ordering between counters.
    pub(crate) fn add(counter: &AtomicU64, bytes: u64) {
        counter.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Reads all counters as a point-in-time snapshot.
    pub fn snapshot(&self) -> CountersSnapshot {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        CountersSnapshot {
            dram_hits: read(&self.dram_hits),
            dram_misses: read(&self.dram_misses),
            disk_hits: read(&self.disk_hits),
            disk_misses: read(&self.disk_misses),
            dram_bytes_served: read(&self.dram_bytes_served),
            disk_bytes_served: read(&self.disk_bytes_served),
            backend_bytes_served: read(&self.backend_bytes_served),
            backend_fills: read(&self.backend_fills),
            backend_fill_bytes: read(&self.backend_fill_bytes),
            backend_heads: read(&self.backend_heads),
            non_cacheable_passthroughs: read(&self.non_cacheable_passthroughs),
            deduped_fills: read(&self.deduped_fills),
            blocks_admitted: read(&self.blocks_admitted),
            blocks_rejected: read(&self.blocks_rejected),
            storage_buffer_overflows: self.storage_buffer_overflows.load(Ordering::Relaxed),
        }
    }
}
