//! Read-path counters: plain atomics the engine bumps on the hot path.
//! Issue #46 wires these into Prometheus; this module's job is that the
//! numbers exist, are cheap to record (no locks, no aggregation on the hot
//! path), and are testable via [`CacheCounters::snapshot`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic counters for the cache read path. All updates and reads use
/// relaxed ordering: each counter is independent and only ever added to, so
/// there are no cross-counter ordering requirements — a scrape (#46) or test
/// snapshot may observe counters mid-request, which is fine for monotonic
/// gauges.
#[derive(Debug, Default)]
pub struct CacheCounters {
    /// Block lookups answered by the DRAM tier.
    pub dram_hits: AtomicU64,
    /// Block lookups that missed the DRAM tier.
    pub dram_misses: AtomicU64,
    /// Block lookups answered by the disk tier (after a DRAM miss).
    pub disk_hits: AtomicU64,
    /// Block lookups that missed the disk tier too.
    pub disk_misses: AtomicU64,
    /// Block lookups answered by a peer node (always 0 while N=1).
    pub peer_hits: AtomicU64,
    /// Peer fetches that came back a clean miss (the owner did not have the
    /// block) — the read then falls through to a local backend fill.
    pub peer_misses: AtomicU64,
    /// Peer fetches that failed at the transport (timeout, refused, auth
    /// reject) — the read degrades to a backend fill (slow is acceptable).
    pub peer_errors: AtomicU64,
    /// Blocks this node served to *peers* out of its own cache (the donor side
    /// of a peer fetch). Distinct from `peer_hits`, which is the requester side.
    pub peer_served_blocks: AtomicU64,
    /// Bytes this node served to peers (#46 intra-pod NIC amplification). Rising
    /// with `peer_served_blocks` is the pod acting as one logical cache.
    pub peer_served_bytes: AtomicU64,
    /// Bytes served to clients out of DRAM-tier blocks.
    pub dram_bytes_served: AtomicU64,
    /// Bytes served to clients out of disk-tier blocks.
    pub disk_bytes_served: AtomicU64,
    /// Bytes served to clients out of peer-fetched blocks (0 while N=1).
    pub peer_bytes_served: AtomicU64,
    /// Bytes served to clients straight out of backend-filled blocks.
    pub backend_bytes_served: AtomicU64,
    /// Backend GETs issued to fill blocks.
    pub backend_fills: AtomicU64,
    /// Bytes fetched from the backend by successful block fills (counts the
    /// whole block, which can exceed the bytes actually served to a client).
    pub backend_fill_bytes: AtomicU64,
    /// Backend HEADs issued to resolve object metadata (key→ETag mapping).
    pub backend_heads: AtomicU64,
    /// Requests served passthrough without admission because the origin
    /// reported no ETag for the object (GETs and HEADs alike).
    pub non_cacheable_passthroughs: AtomicU64,
    /// Cold fills that joined an in-flight fill instead of issuing their own
    /// backend request — the singleflight (#19) collapse count. Monotonic:
    /// each waiter that attaches bumps it once. Suffix-fill collapses are
    /// counted separately in [`CacheCounters::deduped_suffix_fills`].
    pub deduped_fills: AtomicU64,
    /// Cold *suffix* fills that joined an in-flight buffered suffix fill instead
    /// of issuing their own backend GET (#149). The footer-stampede collapse
    /// count: N concurrent cold reads of one small object tail attach to one
    /// buffered fill, and each waiter past the winner bumps this once. Broken
    /// out from [`CacheCounters::deduped_fills`] because only tails at or under
    /// `SUFFIX_BUFFER_CAP` are bufferable and thus dedupable — over-cap
    /// suffixes stream serve-only and never register a flight.
    pub deduped_suffix_fills: AtomicU64,
    /// Data blocks the scan-resistant admission policy (#15) let into the
    /// cache. One bump per fetched block that was inserted.
    pub blocks_admitted: AtomicU64,
    /// Data blocks the admission policy (#15) rejected — a one-touch scan's
    /// bytes flow through to the client but are not cached. A rising rejection
    /// count next to a flat working-set hit rate is scan resistance working.
    pub blocks_rejected: AtomicU64,
    /// Metadata-store (#50) block lookups answered from the pinned store. The
    /// hard-isolation acceptance criterion asserts this stays ~100% of metadata
    /// reads even under sustained data-scan pressure.
    pub meta_hits: AtomicU64,
    /// Metadata-store block lookups that missed and had to fill from the
    /// backend (a cold metadata read, or the very first read after a refresh).
    pub meta_misses: AtomicU64,
    /// Bytes served to clients out of pinned metadata-store blocks.
    pub meta_bytes_served: AtomicU64,
    /// Backend GETs issued to fill metadata-store blocks.
    pub meta_fills: AtomicU64,
    /// Catalog-pointer objects refreshed by the watcher (#47/#50): the mapping
    /// is dropped so the next planning read re-resolves the moved snapshot.
    pub meta_refreshes: AtomicU64,
    /// Conditional `If-None-Match` revalidations issued for expired
    /// *mutable-classified* mappings (#14). The immutability acceptance
    /// criterion asserts this never moves for immutable-classified keys.
    pub meta_revalidations: AtomicU64,
    /// Revalidations that came back `304 Unchanged` (#14): the mapping held and
    /// its TTL was refreshed with no bytes transferred and no block refetch.
    pub meta_revalidations_unchanged: AtomicU64,
    /// Revalidations that found a new version (#14): the object was overwritten
    /// behind Verglas, so the mapping rotated to the new ETag. The next read
    /// serves the fresh version; the old version's blocks are unreachable.
    pub meta_revalidations_rotated: AtomicU64,
    /// Data-block disk writes foyer silently dropped because they did not fit
    /// its flush buffer (#278). Each drop means a block that was warmed (and, if
    /// `flush()` was called, reported durable) is on neither tier and will
    /// refill from the origin on its next read — degradation, never wrong bytes.
    /// This is an [`Arc`] because foyer's flush runs on its own threads: the
    /// same handle backs the metrics registry installed on the block cache
    /// (`foyer_metrics`) and this snapshot, so the drop foyer records off-thread
    /// is visible here. Not on any verglas serve path.
    pub storage_buffer_overflows: Arc<AtomicU64>,
    /// Bytes currently held by demoted (retired, grace-pending) objects
    /// (#305). A gauge, not monotonic: demotion adds an object's size, hard
    /// eviction subtracts it. This is the "how much of the resident cache is
    /// dead" figure — a 3.6 TB cache holding 300 GB of live data reads as warm
    /// without it.
    pub retired_bytes_pending: AtomicU64,
    /// Bytes physically reclaimed by grace-window hard eviction (#305):
    /// enumerated block entries removed from the stores, returning their space
    /// to the shared budget. Monotonic.
    pub retired_bytes_reclaimed: AtomicU64,
    /// Retired files physically reclaimed by hard eviction (#305). Monotonic.
    pub retired_files_reclaimed: AtomicU64,
}

/// Point-in-time copy of every counter, for tests and (later, #46) scrapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CountersSnapshot {
    /// See [`CacheCounters::dram_hits`].
    pub dram_hits: u64,
    /// See [`CacheCounters::dram_misses`].
    pub dram_misses: u64,
    /// See [`CacheCounters::disk_hits`].
    pub disk_hits: u64,
    /// See [`CacheCounters::disk_misses`].
    pub disk_misses: u64,
    /// See [`CacheCounters::peer_hits`].
    pub peer_hits: u64,
    /// See [`CacheCounters::peer_misses`].
    pub peer_misses: u64,
    /// See [`CacheCounters::peer_errors`].
    pub peer_errors: u64,
    /// See [`CacheCounters::peer_served_blocks`].
    pub peer_served_blocks: u64,
    /// See [`CacheCounters::peer_served_bytes`].
    pub peer_served_bytes: u64,
    /// See [`CacheCounters::dram_bytes_served`].
    pub dram_bytes_served: u64,
    /// See [`CacheCounters::disk_bytes_served`].
    pub disk_bytes_served: u64,
    /// See [`CacheCounters::peer_bytes_served`].
    pub peer_bytes_served: u64,
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
    /// See [`CacheCounters::deduped_suffix_fills`].
    pub deduped_suffix_fills: u64,
    /// See [`CacheCounters::blocks_admitted`].
    pub blocks_admitted: u64,
    /// See [`CacheCounters::blocks_rejected`].
    pub blocks_rejected: u64,
    /// See [`CacheCounters::meta_hits`].
    pub meta_hits: u64,
    /// See [`CacheCounters::meta_misses`].
    pub meta_misses: u64,
    /// See [`CacheCounters::meta_bytes_served`].
    pub meta_bytes_served: u64,
    /// See [`CacheCounters::meta_fills`].
    pub meta_fills: u64,
    /// See [`CacheCounters::meta_refreshes`].
    pub meta_refreshes: u64,
    /// See [`CacheCounters::meta_revalidations`].
    pub meta_revalidations: u64,
    /// See [`CacheCounters::meta_revalidations_unchanged`].
    pub meta_revalidations_unchanged: u64,
    /// See [`CacheCounters::meta_revalidations_rotated`].
    pub meta_revalidations_rotated: u64,
    /// See [`CacheCounters::storage_buffer_overflows`].
    pub storage_buffer_overflows: u64,
    /// See [`CacheCounters::retired_bytes_pending`].
    pub retired_bytes_pending: u64,
    /// See [`CacheCounters::retired_bytes_reclaimed`].
    pub retired_bytes_reclaimed: u64,
    /// See [`CacheCounters::retired_files_reclaimed`].
    pub retired_files_reclaimed: u64,
}

impl CacheCounters {
    /// Adds 1 to a counter; the single mutation primitive the engine uses.
    pub(crate) fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds a byte count to a counter.
    pub(crate) fn add(counter: &AtomicU64, bytes: u64) {
        counter.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Copies every counter. Not atomic across counters (see type docs).
    pub fn snapshot(&self) -> CountersSnapshot {
        let read = |c: &AtomicU64| c.load(Ordering::Relaxed);
        CountersSnapshot {
            dram_hits: read(&self.dram_hits),
            dram_misses: read(&self.dram_misses),
            disk_hits: read(&self.disk_hits),
            disk_misses: read(&self.disk_misses),
            peer_hits: read(&self.peer_hits),
            peer_misses: read(&self.peer_misses),
            peer_errors: read(&self.peer_errors),
            peer_served_blocks: read(&self.peer_served_blocks),
            peer_served_bytes: read(&self.peer_served_bytes),
            dram_bytes_served: read(&self.dram_bytes_served),
            disk_bytes_served: read(&self.disk_bytes_served),
            peer_bytes_served: read(&self.peer_bytes_served),
            backend_bytes_served: read(&self.backend_bytes_served),
            backend_fills: read(&self.backend_fills),
            backend_fill_bytes: read(&self.backend_fill_bytes),
            backend_heads: read(&self.backend_heads),
            non_cacheable_passthroughs: read(&self.non_cacheable_passthroughs),
            deduped_fills: read(&self.deduped_fills),
            deduped_suffix_fills: read(&self.deduped_suffix_fills),
            blocks_admitted: read(&self.blocks_admitted),
            blocks_rejected: read(&self.blocks_rejected),
            meta_hits: read(&self.meta_hits),
            meta_misses: read(&self.meta_misses),
            meta_bytes_served: read(&self.meta_bytes_served),
            meta_fills: read(&self.meta_fills),
            meta_refreshes: read(&self.meta_refreshes),
            meta_revalidations: read(&self.meta_revalidations),
            meta_revalidations_unchanged: read(&self.meta_revalidations_unchanged),
            meta_revalidations_rotated: read(&self.meta_revalidations_rotated),
            storage_buffer_overflows: read(&self.storage_buffer_overflows),
            retired_bytes_pending: read(&self.retired_bytes_pending),
            retired_bytes_reclaimed: read(&self.retired_bytes_reclaimed),
            retired_files_reclaimed: read(&self.retired_files_reclaimed),
        }
    }
}
