//! The hybrid cache engine: a foyer DRAM+disk cache behind the `ObjectRead`
//! interface, with the M1 miss ladder — local DRAM → local disk → peer fetch →
//! backend fill — and the ETag discipline that makes cached blocks immutable.
//!
//! # Three caches, one budget
//!
//! Data blocks, key→ETag mappings, and *pinned table metadata* (#50) live in
//! *separate* foyer instances on purpose. The metadata store
//! ([`crate::meta_store`]) holds the raw bytes of metadata.json, manifest
//! lists, manifests, and Parquet footers in its own eviction domain, so a data
//! scan flowing through the block cache structurally cannot evict a footer.
//! Its DRAM is carved from *inside* the block-tier budget (see
//! [`meta_store_capacity`]), so the ceiling is unchanged. The rest of this
//! section is about the block cache and the mapping cache:
//!
//! Blocks and key→ETag mappings live in *separate* caches on purpose. A
//! mapping is DRAM-only state whose loss costs a backend round trip to
//! recover; a block is disk-backed and costs at most an NVMe reload. When
//! both shared one LRU, MiB-scale data-block traffic (fresh fills and the reloads
//! foyer re-admits on disk hits) swept the byte-sized mappings out of the
//! tier under any sustained load — measured: a working set two blocks over
//! the memory tier turned every re-read into a fresh backend fill because
//! the mappings died before the blocks were re-read. Separate eviction
//! domains remove that interference by construction: mappings only compete
//! with mappings.
//!
//! # How the config budgets map onto foyer's knobs (hard ceilings)
//!
//! **Disk (`cache.capacity_bytes`).** The foyer device is built over
//! `<cache.dir>/blocks.data` with exactly its carved capacity. foyer
//! preallocates that one sparse backing file at open and only ever writes
//! inside it (tombstone log stays off, so there are no other block-tier
//! files). The ceiling therefore holds by construction: on-disk usage is at
//! most `capacity_bytes` from boot onward, with the sub-one-disk-block
//! remainder unused. Foyer regions are sized to hold at least one full cache
//! block plus its entry framing and grow with capacity to keep startup
//! bookkeeping bounded ([`foyer_disk_block_bytes`]). Object reads remain
//! cache-block aligned while Foyer manages larger regions internally.
//!
//! The backing files are *sparse*: their logical length is the budget, but
//! physical (allocated) disk grows only as blocks are admitted. That is what
//! lets the write-back fragment store share the one NVMe budget first come,
//! first served (#223): the server's disk poll reads
//! [`HybridCacheEngine::disk_growth_room_bytes`] — the logical capacity the
//! stores have not physically consumed — and lets fragments take exactly
//! that, pausing block admission before it would grow into fragment-held
//! bytes. There is no carve; enforcement is accounting. Extension point: if a
//! future foyer supports resizing a live store, the accounting could become
//! an explicit resize instead.
//!
//! **DRAM (`cache.dram_bytes`).** Three carve-outs sum to the budget:
//! foyer's disk-write pipeline gets the fixed
//! [`DRAM_PIPELINE_RESERVED_BYTES`], the metadata cache gets the fixed
//! [`META_CACHE_BYTES`], and the block memory tier gets the remainder as its
//! capacity. Both caches use byte-exact weighters (encoded size plus
//! [`ENTRY_OVERHEAD_BYTES`] of bookkeeping), and the block tier's shard
//! count is derived so one shard always fits several blocks (see
//! [`memory_shards`] — that is what makes foyer's per-shard eviction an
//! exact ceiling, issue #122). Documented overhead expectation for
//! operators: engine-attributable RSS stays within `cache.dram_bytes` plus
//! ~5% (foyer's disk-index bookkeeping and allocator slack are outside the
//! weighters' accounting; with MiB-scale blocks they are dozens of bytes per
//! block) plus up to [`BLOCK_FILL_WINDOW`] blocks per in-flight multi-block
//! request — blocks being streamed to a client or retained by its ordered
//! look-ahead window are pinned by refcount and outlive eviction. This
//! per-request transient memory is bounded by front-end concurrency rather
//! than by the cache budget.
//!
//! # Persistence (#16)
//!
//! Both foyer stores (the data block cache and the #50 metadata store) persist
//! their disk tiers across a process restart and rebuild their in-memory index
//! from the on-disk Foyer metadata on the next open, so a restarting server
//! comes back serving its warm NVMe cache instead of re-fetching from the
//! origin. Two knobs make that work end-to-end:
//!
//! - **Write-through (`HybridCachePolicy::WriteOnInsertion`).** Every admitted
//!   block is enqueued to NVMe on insertion rather than only on eviction, so a
//!   block that stays hot in DRAM is still durable. This is what lets an
//!   *unclean* `kill -9` — which never runs a graceful flush — still recover a
//!   warm cache: whatever reached NVMe before the crash comes back.
//! - **`RecoverMode::Quiet`.** Recovery scans the regions and rebuilds the
//!   index, silently dropping any entry whose checksum fails — exactly the
//!   partially written tail a crash leaves behind. Recovery therefore always
//!   completes, and a corrupted entry is a clean miss that refills, never a
//!   served wrong byte.
//!
//! Recovered block entries are safe by construction: their keys embed the ETag,
//! so a resurrected block can never serve a *different version's* bytes, and
//! foyer's read-path checksum means it can never serve a *corrupted* byte of
//! the right version either. The key→ETag mapping (the only staleness-prone
//! state) lives in a purely in-memory cache with no codec at all, so a restart
//! *cannot* resurrect a stale mapping — the first read after boot re-resolves
//! metadata against the origin (one round trip) and then reuses any surviving
//! blocks of that version with zero re-fills of block bytes.
//!
//! # Purge: generation-epoch, no racy `clear()` (#178)
//!
//! `POST /cache/purge` resets the cache to cold by bumping a single global
//! **cache generation** ([`HybridCacheEngine::purge`]), the memcached
//! `flush_all` / Redis lazyfree convergent pattern. Every block and metadata
//! entry is keyed by `(logical identity, generation)` ([`BlockEntryKey`],
//! [`MetaEntryKey`]); every lookup and insert stamps the current generation.
//! Purge increments it, so every resident entry is unreachable in one atomic
//! store, and reclaim is lazy (natural LRU aging — foyer 0.22.3 exposes no
//! iteration for a proactive sweep). **foyer's bulk `clear()` is banned** on the
//! block and metadata stores: it panics against a concurrent `insert` (its LRU
//! weight accounting is not insert-safe — foyer-rs/foyer#1305), so the racy
//! operation is removed *by construction* rather than serialized behind a guard.
//! The generation is in the on-disk codec and persisted to [`GEN_FILE`], so a
//! restart recovers old-generation entries as correctly-unreachable garbage
//! while current-generation entries recover warm (#16). The mapping cache is
//! cleared (not epoched) — it is fence-serialized, not the racy foyer store; see
//! the purge method doc. Extension point: the per-table generation vector of #51
//! is the general case of which this whole-cache generation is the degenerate.
//!
//! Serve-gating lives in the server, not here: `verglas-server` reports `starting` on
//! `/admin/healthz` until the engine has finished building (recovery included),
//! so a load balancer never routes to a node that would cold-miss everything.
//! Compression stays off — block payloads are typically already-compressed
//! columnar formats (Parquet/ORC); a compression pass for the uncompressed
//! minority is a possible future tuning, not required here.

use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, DeviceBuilder, FileDeviceBuilder, HybridCache,
    HybridCacheBuilder, HybridCacheEntry, HybridCachePolicy, LfuConfig, RecoverMode,
};
use futures::TryStreamExt;
use futures::future::{BoxFuture, FutureExt, Shared};
use futures::stream::{self, StreamExt};
use tracing::Instrument;
use verglas_core::admin::PurgeReport;
use verglas_core::config::Cache as CacheConfig;
use verglas_core::node::NodeId;
use verglas_core::peer::{NoopPeerFetch, PeerFetch};
use verglas_core::read::{
    AttributesRequest, BodyStream, DirectGet, DirectMeta, DirectReadOptions, ObjectAttributes,
    ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange, Revalidation, ServedTier, TierCell,
};
use verglas_core::ring::{RendezvousRing, Ring};
use verglas_core::write::Invalidation;
use verglas_core::{BlockKey, CacheKey};

use crate::admission::Admission;
use crate::block::{block_len, covering_blocks, resolve_range};
use crate::classify::{MetaClass, MetaRouter, Mutability, classify_mutability};
use crate::counters::CacheCounters;
use crate::demotion::{DEMOTED_GENERATION_BIT, Demotions};
use crate::entry::{BlockEntryKey, CachedMeta};
use crate::materialized::{MaterializedPageError, MaterializedPageKey, POSTGRES_PAGE_BYTES};
use crate::meta_store::{
    META_DISK_MIN_BYTES, META_PIPELINE_RESERVED_BYTES, MetaEntryKey, MetaStore, foyer_block_size,
};

/// Smallest Foyer disk region (distinct from the cache block): one full cache
/// block, its entry header and key rounded up to a 4 KiB page, Foyer's 4 KiB
/// blob index, and one spare page — 16 KiB of framing headroom in total.
/// Larger cache budgets use larger regions to bound startup partition count.
/// Returns a Foyer region large enough for one configured data block and framing.
fn foyer_disk_block_bytes(block_bytes: u64) -> usize {
    block_bytes as usize + 16 * 1024
}

/// Aligned fills allowed to continue past the bytes an organic reader needs.
/// Excess cold ranges serve their exact bytes directly, so background cache
/// population can consume only a small fixed share of the backend budget.
const DEFAULT_BACKGROUND_FILL_SLOTS: usize = 4;

/// Total DRAM carved out of `cache.dram_bytes` for foyer's disk-write pipeline:
/// the flush buffer pool plus the submit queue. Fixed and geometry-independent
/// so the DRAM budget arithmetic (and the documented 80 MB constrained profile)
/// is unchanged. [`flush_buffer_pool_bytes`] and [`submit_queue_bytes`] split
/// this reservation — the flush buffer is sized from the block geometry (#278),
/// the submit queue takes the remainder — so their sum stays constant.
const DRAM_PIPELINE_RESERVED_BYTES: usize = 48 * 1024 * 1024;

/// foyer's on-disk page/alignment unit (`PAGE`, foyer 0.22.3). Every flush-buffer
/// entry is padded up to a multiple of this before it is written.
const FOYER_PAGE_BYTES: usize = 4096;

/// The page-aligned DRAM one data-block entry occupies in foyer's flush buffer:
/// the block value plus foyer's per-entry header and key, rounded up to a page.
/// The header and key are a few hundred bytes, so one extra page is a safe
/// ceiling on that overhead — an 8 MiB block lands at 8,392,704 bytes, matching
/// foyer's own accounting. `block_bytes` is a validated power of two of at least
/// a page, so it is already page-aligned; the `next_multiple_of` keeps the
/// function correct for any geometry.
fn foyer_flush_entry_bytes(block_bytes: u64) -> usize {
    (block_bytes as usize).next_multiple_of(FOYER_PAGE_BYTES) + FOYER_PAGE_BYTES
}

/// foyer's flush buffer pool, sized to hold two block entries (#278). foyer
/// drains queued disk writes into one buffer per rotation and silently discards
/// any entry that does not fit; with the old fixed 16 MiB pool and 8 MiB blocks,
/// two writes stacked into one rotation lost the second (#273). Two entries'
/// worth means two back-to-back writes both land. Two 8 MiB entries are
/// ~16 MiB, comfortably under [`DRAM_PIPELINE_RESERVED_BYTES`] (48 MiB) at the
/// 8 MiB maximum geometry, so the submit queue always keeps a positive
/// remainder. A future geometry above ~23 MiB would break that split and needs
/// its own budget revisit (the config caps `data_block_bytes` at 8 MiB today).
fn flush_buffer_pool_bytes(block_bytes: u64) -> usize {
    2 * foyer_flush_entry_bytes(block_bytes)
}

/// foyer's submit queue threshold: entries evicted from the memory tier and
/// awaiting a flush slot; foyer drops writes beyond it (a refill for a cache,
/// never an error). It takes the pipeline reservation left after the flush
/// buffer, so the DRAM the pipeline reserves is exactly
/// [`DRAM_PIPELINE_RESERVED_BYTES`] at every geometry.
fn submit_queue_bytes(block_bytes: u64) -> usize {
    DRAM_PIPELINE_RESERVED_BYTES - flush_buffer_pool_bytes(block_bytes)
}

/// DRAM carved out of `cache.dram_bytes` for the metadata cache (the
/// key→ETag mappings): 16 MiB ≈ 45k mappings — far more objects than the
/// disk tier can hold blocks for at any plausible M1 sizing. #14/#50 revisit
/// the mapping lifetime story wholesale.
const META_CACHE_BYTES: usize = 16 * 1024 * 1024;

/// Minimum workable DRAM budget: the pipeline and metadata reservations plus
/// a block memory tier of at least two cache blocks.
/// Returns the geometry-specific minimum DRAM budget for the engine.
fn min_dram_budget_bytes(block_bytes: u64) -> u64 {
    (DRAM_PIPELINE_RESERVED_BYTES + META_CACHE_BYTES) as u64 + 2 * block_bytes
}

/// Largest suffix (Parquet footer) tail pinned at suffix granularity in the
/// metadata store (#50). Real footers and engines' speculative footer reads
/// are ≤ ~64 KiB; the cap only exists so a pathological giant suffix request
/// cannot monopolize the small store (it streams serve-only instead). Must fit
/// one metadata disk region including framing.
const META_SUFFIX_PIN_MAX_BYTES: u64 = 512 * 1024;

/// Largest suffix (Parquet footer) tail buffered into `Bytes` so a cold
/// suffix fill's value is cloneable and can collapse a footer stampede in the
/// singleflight (#149). At or under this cap the winning fill drains the body
/// into memory and every concurrent cold reader streams from that one shared
/// buffer — N reads, one backend GET. One configured block wide: footers are
/// ~64 KiB, so this is generous headroom while still bounding the transient
/// per-flight allocation (the ceiling is one configured data block × live suffix
/// flights, not × request concurrency, because waiters share the buffer).
/// Above the cap a suffix stays serve-only and non-deduped (its body could be
/// whole-object-sized and cannot be buffered without blowing the budget) —
/// the pre-#149 #131 behavior. Distinct from and larger than
/// [`META_SUFFIX_PIN_MAX_BYTES`]: buffering enables dedup for any small tail;
/// *pinning* a tail into the small metadata store is the stricter,
/// metadata-only subset.
/// Returns the maximum suffix body buffered for one deduplicated footer fill.
fn suffix_buffer_cap(block_bytes: u64) -> u64 {
    block_bytes
}

/// Floor on the dedicated metadata store's DRAM capacity (#50): enough to pin a
/// handful of footers/manifests even when `meta_fraction` of a tiny budget
/// rounds down below it. Carved from *inside* the block-tier DRAM (see
/// [`meta_store_capacity`]), never on top of the DRAM ceiling.
const MIN_META_STORE_BYTES: usize = META_PIPELINE_RESERVED_BYTES + 1024 * 1024;

/// Minimum data-store disk budget: four Foyer regions, the least that gives
/// the reclaimer room to keep a clean block while others hold data.
fn data_disk_floor_bytes(block_bytes: u64) -> u64 {
    4 * foyer_disk_block_bytes(block_bytes) as u64
}

/// Minimum workable disk budget for both cache stores. The data and metadata
/// Foyer engines each require four regions, so this floor reserves both
/// before any configurable carve-out is applied.
fn min_disk_budget_bytes(block_bytes: u64) -> u64 {
    data_disk_floor_bytes(block_bytes) + META_DISK_MIN_BYTES
}

/// Per-entry DRAM bookkeeping the weighters charge on top of the encoded
/// entry size: foyer's handle/eviction-state per entry plus hash-table
/// slack. Generous for block entries (0.003% of a block), and what keeps a
/// flood of tiny metadata entries from escaping the budget.
const ENTRY_OVERHEAD_BYTES: usize = 256;

/// Filename of the block tier's sparse backing file under `cache.dir`.
const BLOCKS_DEVICE_FILE: &str = "blocks.data";

/// Filename of the metadata tier's sparse backing file under `cache.dir`.
const META_DEVICE_FILE: &str = "meta.data";

/// Filename under `cache.dir` where the purge generation is persisted (issue
/// #178). A restart reads it so a purge survives the restart — old-generation
/// entries recover as unreachable garbage — while entries written under the
/// current generation recover warm (#16). Absent ⇒ generation 0 (a fresh cache).
const GEN_FILE: &str = "PURGE_GENERATION";

/// Minimum per-shard capacity of the block memory tier: four cache blocks.
///
/// This is the #122 invariant. foyer evicts per shard, and its insert path
/// (verified in foyer 0.22.3, `RawCacheShard::emplace`) evicts down to
/// `shard_capacity − entry_weight` *before* inserting — so a shard can only
/// end up over its capacity when a single entry doesn't fit at all
/// (`saturating_sub` clamps the target to 0, everything evictable is
/// evicted, and the entry is inserted anyway, stranded until that shard's
/// next insert). Keeping per-shard capacity ≥ 4 blocks means every entry
/// always fits, so per-shard eviction keeps every shard within its share and
/// the tier within its budget — while still restoring shard parallelism for
/// budgets that can afford it.
/// Returns the smallest per-shard capacity that can hold four data blocks.
fn shard_min_capacity_bytes(block_bytes: u64) -> usize {
    4 * block_bytes as usize
}

/// Upper bound on block-tier shards; foyer's own default parallelism level.
const MAX_MEMORY_SHARDS: usize = 8;

/// Upper bound on live singleflight entries across all three in-flight maps
/// (#19). Past it, deduplication is bypassed and the fill runs inline — a hard
/// bound on the map's memory, never a bound on request concurrency. Generous:
/// an entry is a key plus a shared future handle, and lives only for the
/// duration of one in-flight fill.
const MAX_INFLIGHT: u64 = 16 * 1024;

/// Number of covering blocks one response may dispatch ahead of the next block
/// it yields. This is a per-response memory bound; the backend's per-bucket
/// limiter remains the authority for origin-wide concurrency.
const BLOCK_FILL_WINDOW: usize = 4;

/// Node identity of the M1 cluster-of-one member. Placeholder until gossip
/// membership (#27) supplies real identities; the name never leaves the
/// process while N=1 because there are no peers to compare it against.
const SINGLE_NODE_ID: &str = "single";

/// Emits a structured origin-fill-failure event (#61). A cold read whose origin
/// GET or HEAD fails otherwise degrades silently — a `NeedHead` retry, or (with
/// unusable credentials) an empty serve — so a failure like #245 (stale
/// credentials hanging every cold fill) or #233 (unusable credentials serving
/// nothing) is invisible from outside. This makes it loud: the request id (from
/// the current trace scope, so it ties to the front-end line and the client's
/// `x-amz-request-id`), a redacted key hash, the fill stage, and the error. Not
/// on the hot path — only a backend error reaches it, never a cache hit.
fn log_fill_error(key: &CacheKey, stage: &str, error: &ReadError) {
    tracing::warn!(
        request_id = verglas_core::trace::current().as_ref().map(|r| r.as_str()),
        key_hash = %verglas_core::trace::key_hash(&key.bucket, &key.key),
        stage,
        error = %error,
        "origin fill failed"
    );
}

/// Shard count for a block memory tier of `memory_capacity` bytes: one shard
/// per four configured blocks, at least 1, at most [`MAX_MEMORY_SHARDS`]. See
/// [`shard_min_capacity_bytes`] for why the floor
/// is what makes the budget a hard ceiling (#122).
fn memory_shards(memory_capacity: usize, block_bytes: u64) -> usize {
    (memory_capacity / shard_min_capacity_bytes(block_bytes)).clamp(1, MAX_MEMORY_SHARDS)
}

/// The metadata store's DRAM capacity (#50): `meta_fraction` of the block-tier
/// DRAM, carved from *inside* it (the data tier gets the remainder), floored at
/// [`MIN_META_STORE_BYTES`] and capped so the data tier keeps at least one full
/// block. `block_dram` is the DRAM left after the pipeline and mapping
/// reservations; the caller has already validated it is at least two blocks
/// ([`min_dram_budget_bytes`]), so the cap `block_dram − one block` is at least
/// one block and never below the floor. This is why the meta store is a hard
/// ceiling *within* the existing DRAM budget rather than an addition to it.
fn meta_store_capacity(block_dram: usize, meta_fraction: f64, block_bytes: u64) -> usize {
    let raw = (block_dram as f64 * meta_fraction) as usize;
    let cap = block_dram.saturating_sub(block_bytes as usize);
    raw.clamp(MIN_META_STORE_BYTES, cap)
}

/// Failure to construct the cache engine. Startup-only: the read path never
/// produces these.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// `cache.dram_bytes` cannot cover the write pipeline, the metadata
    /// cache, and a useful block memory tier.
    #[error(
        "cache.dram_bytes is {configured} bytes; the engine needs at least {minimum} \
         ({DRAM_PIPELINE_RESERVED_BYTES} for the disk-write pipeline, {META_CACHE_BYTES} for \
         the metadata cache, plus a two-block memory tier)"
    )]
    DramBudgetTooSmall {
        /// Configured DRAM budget.
        configured: u64,
        /// Geometry-specific minimum DRAM budget.
        minimum: u64,
    },
    /// `cache.capacity_bytes` cannot hold enough disk blocks to cycle.
    #[error(
        "cache.capacity_bytes is {configured} bytes; the engine needs at least {minimum} \
         (four configured data blocks including Foyer framing and four metadata regions)"
    )]
    DiskBudgetTooSmall {
        /// Configured disk budget.
        configured: u64,
        /// Geometry-specific minimum disk budget.
        minimum: u64,
    },
    /// foyer failed to open the device or build the cache (bad directory,
    /// IO error, unrecoverable on-disk state).
    #[error("cannot build cache engine: {0}")]
    Build(String),
}

/// Which rung of the miss ladder produced a block — drives per-tier counter
/// attribution when the served slice is cut from the block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tier {
    /// Local DRAM tier.
    Dram,
    /// Local disk tier.
    Disk,
    /// A peer node (unreachable while N=1).
    Peer,
    /// Backend fill.
    Backend,
    /// The dedicated metadata store (#50): a pinned metadata block served from
    /// its own hard-isolated DRAM domain.
    Meta,
}

impl Tier {
    /// Maps the internal serving tier onto the public [`ServedTier`] the
    /// request-duration histogram labels by (#46). The metadata store is a
    /// DRAM-resident warm serve, so it folds into `Dram` for the label.
    fn served(self) -> ServedTier {
        match self {
            Tier::Dram => ServedTier::Dram,
            Tier::Disk => ServedTier::Nvme,
            Tier::Peer => ServedTier::Peer,
            Tier::Backend => ServedTier::Backend,
            Tier::Meta => ServedTier::Dram,
        }
    }
}

/// A monotonic clock the mapping-TTL logic (#14) reads. Injected behind an
/// `Arc` so tests drive expiry deterministically (a manual clock) instead of
/// sleeping on wall time; production uses [`SystemClock`]. Never read on the
/// warm path for immutable-classified keys — those mappings never expire.
trait Clock: Send + Sync + 'static {
    /// The current monotonic instant.
    fn now(&self) -> Instant;
}

/// The production clock: `Instant::now()`.
struct SystemClock;

impl Clock for SystemClock {
    /// Reads the process monotonic clock.
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// The value stored in the metadata cache: the key→ETag mapping plus the #14
/// staleness bookkeeping. Wrapping [`CachedMeta`] keeps the block path (which
/// only wants the ETag/size) unchanged while giving the mapping cache the
/// mutability class and refresh instant the TTL logic needs.
#[derive(Clone)]
struct MappingEntry {
    /// The resolved whole-object metadata (the mapping proper).
    meta: CachedMeta,
    /// Mutability class, decided from the key at admission (a pure function of
    /// the path, so it never changes for a given key).
    mutability: Mutability,
    /// Monotonic instant this mapping was admitted or last revalidated
    /// `Unchanged`. Read only for [`Mutability::Mutable`] entries — immutable
    /// mappings never expire, so their `refreshed_at` is never consulted.
    refreshed_at: Instant,
}

/// Shareable outcome of revalidating one mutable mapping (#14), so the
/// singleflight (#19) hands a burst of expired reads one conditional request's
/// result. `Clone` for [`Shared`].
#[derive(Clone)]
enum RevalOutcome {
    /// The origin confirmed the version still holds; the mapping's TTL was
    /// refreshed. Carries the (unchanged) mapping to serve.
    Fresh(CachedMeta),
    /// The object changed; the new mapping was admitted. Carries it to serve.
    Rotated(CachedMeta),
    /// The object vanished, became non-cacheable, or revalidation failed at the
    /// backend: the mapping is a miss now, so the caller re-resolves (a backend
    /// fill) — degrading to a re-resolution, never to serving unverified bytes.
    Miss,
}

/// Outcome of resolving an object's metadata (the key→ETag mapping). `Clone`
/// so the singleflight (#19) can hand one HEAD resolution to every waiter.
#[derive(Clone)]
enum MetaLookup {
    /// The origin reports an ETag; blocks of this version are cacheable.
    Cacheable(CachedMeta),
    /// The origin reports no ETag. Nothing about this object may be
    /// admitted; the caller serves passthrough and counts it.
    NonCacheable(ObjectMeta),
}

/// Per-key invalidation fence closing the stale-mapping admission race
/// (issue #124): a metadata-resolving backend response produced before a
/// racing write became durable must not be admitted after that write's
/// invalidation ran, or the acked write would be hidden behind a reinstated
/// stale mapping (served from cache with zero backend traffic and no
/// self-correction). This is the lease idea from the memcache paper
/// (NSDI'13 §3.2.1), scoped per key.
///
/// Protocol: [`MetaFence::register`] is called *before* issuing any
/// metadata-resolving backend request and captures the key's current epoch
/// in an RAII guard; invalidation bumps the epoch and removes the mapping
/// under the same lock; admission re-checks the epoch under that lock and
/// drops the result if it moved. Slots are refcounted by in-flight guards
/// and removed at zero, so the map is bounded by resolution concurrency,
/// never by the keyspace.
///
/// Locking notes (constraints the code cannot express): the fence lock is
/// taken once per cold-read metadata resolution and once per invalidated
/// key — never on the warm path (`mapping_entry` stays lock-free through
/// foyer) — and it is never held across an await.
#[derive(Debug, Default)]
struct MetaFence {
    /// Fence slots for keys with in-flight metadata resolutions.
    slots: Mutex<HashMap<CacheKey, FenceSlot>>,
}

/// Fence state for one key while at least one resolution is in flight.
#[derive(Debug)]
struct FenceSlot {
    /// Bumped by every invalidation of the key; an admission whose guard
    /// captured an older epoch is discarded.
    epoch: u64,
    /// In-flight resolutions holding a guard; the slot is removed when this
    /// drops to zero.
    readers: usize,
}

/// RAII registration of one in-flight metadata resolution. Dropping it
/// (success, error, or early return alike) releases the slot.
struct FenceGuard<'a> {
    /// The fence this guard is registered with.
    fence: &'a MetaFence,
    /// The key being resolved.
    key: &'a CacheKey,
    /// The key's epoch when the resolution started.
    epoch: u64,
}

impl MetaFence {
    /// Locks the slot map. A poisoned lock is recovered: the fenced
    /// sections are short, never panic in practice, and the state (a
    /// counter and a refcount) stays meaningful even if one did.
    fn lock_slots(&self) -> std::sync::MutexGuard<'_, HashMap<CacheKey, FenceSlot>> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Bumps the epoch of *every* slot with an in-flight resolution, under a
    /// single lock acquisition. This is [`Inner::invalidate_key`]'s bulk dual,
    /// used by [`HybridCacheEngine::purge`] (#138): after this returns, no
    /// metadata resolution that began before the purge can re-admit its
    /// mapping (its guard captured a now-stale epoch), so a purge is a clean
    /// reset even with resolutions in flight. See the purge method doc for why
    /// this is required rather than relying on ETag-keyed blocks alone.
    fn purge_all_epochs(&self) {
        let mut slots = self.lock_slots();
        for slot in slots.values_mut() {
            slot.epoch += 1;
        }
    }

    /// Registers an in-flight resolution for `key`, capturing its current
    /// epoch. Call before issuing the backend request.
    fn register<'a>(&'a self, key: &'a CacheKey) -> FenceGuard<'a> {
        let mut slots = self.lock_slots();
        let slot = slots.entry(key.clone()).or_insert(FenceSlot {
            epoch: 0,
            readers: 0,
        });
        slot.readers += 1;
        FenceGuard {
            fence: self,
            key,
            epoch: slot.epoch,
        }
    }
}

impl Drop for FenceGuard<'_> {
    /// Releases the registration; the last guard out removes the slot.
    fn drop(&mut self) {
        let mut slots = self.fence.lock_slots();
        if let Some(slot) = slots.get_mut(self.key) {
            slot.readers -= 1;
            if slot.readers == 0 {
                slots.remove(self.key);
            }
        }
    }
}

/// Outcome of resolving an unmapped GET's metadata from the first fill GET
/// itself (the cold-read fast path: one backend request, no separate HEAD).
enum FirstFill {
    /// The fill's response carried an ETag: the mapping and the fetched
    /// block are admitted, and the block rides along so the body stream can
    /// serve it without a second lookup.
    Cacheable {
        /// The freshly admitted metadata.
        meta: CachedMeta,
        /// The block the fill produced: `(block_index, payload)`.
        block: (u64, Bytes),
    },
    /// The response carried no ETag; the caller serves passthrough.
    NonCacheable,
    /// The suffix fast path (#131): the mapping was resolved (and admitted if
    /// fresh) from a suffix GET's own `206`, and its bytes are already in hand.
    /// The caller serves `body` directly for `range` — no second ladder lookup
    /// and, deliberately, no caching of the unaligned tail block (issue #131
    /// option 1: serve-only). `meta` is the resolved mapping.
    Served {
        /// The mapping resolved from the suffix response.
        meta: CachedMeta,
        /// The absolute half-open byte range the suffix resolved to.
        range: Range<u64>,
        /// The suffix bytes, streamed straight from the backend response.
        body: BodyStream,
    },
    /// The probe could not resolve metadata (a malformed/range-ignored suffix
    /// response, or the origin rejected the block-aligned range — empty
    /// object, start beyond EOF). The caller falls back to the HEAD-based
    /// resolution, which sorts real objects from range errors with correct S3
    /// semantics.
    NeedHead,
}

/// The shareable result of a first-fill probe (#19): everything a
/// [`FirstFill`] carries except the suffix serve-only body (a live,
/// potentially whole-object-sized stream that cannot be cloned to waiters).
/// `Clone` so every deduplicated waiter gets its own copy.
#[derive(Clone)]
enum ProbeOutcome {
    /// The probe resolved a cacheable mapping and fetched the block (the
    /// block index is the probe key's).
    Cacheable {
        /// The resolved mapping.
        meta: CachedMeta,
        /// The fetched block payload.
        block: Bytes,
    },
    /// The probe response carried no ETag.
    NonCacheable,
    /// The probe could not resolve; fall back to the HEAD path.
    NeedHead,
}

/// The cloneable result of an exact, single-block partial first fill. The
/// requested bytes are buffered so concurrent identical cold readers share one
/// origin GET; aligned cache population remains a separate bounded fill.
#[derive(Clone)]
enum PartialOutcome {
    /// The exact response resolved a cacheable mapping and byte range.
    Cacheable {
        /// The mapping resolved from the partial response.
        meta: CachedMeta,
        /// The absolute half-open range returned by the origin.
        range: Range<u64>,
        /// The exact requested bytes shared by singleflight waiters.
        bytes: Bytes,
    },
    /// The response carried no ETag, so it cannot populate the cache.
    NonCacheable,
    /// The exact response could not safely resolve the mapping or byte count.
    NeedHead,
}

/// The shareable result of a buffered cold suffix fill (#149): a bufferable
/// tail's bytes are drained into `Bytes` so the value is `Clone` and the
/// singleflight can hand one backend GET's worth of footer to N concurrent
/// waiters. The stampede-collapsing analogue of [`ProbeOutcome`] for the
/// suffix path — over-cap suffixes never produce one (they stream serve-only,
/// outside the flight map).
#[derive(Clone)]
enum SuffixOutcome {
    /// The suffix GET resolved a cacheable mapping (admitted if fresh, and
    /// pinned into the metadata store when metadata-classified and pinnable)
    /// and its tail bytes are buffered. Every waiter serves `bytes` for
    /// `range` from this one shared buffer.
    Buffered {
        /// The mapping resolved from the suffix response.
        meta: CachedMeta,
        /// The absolute half-open byte range the suffix resolved to.
        range: Range<u64>,
        /// The buffered tail bytes, cloned (cheaply, `Bytes` is refcounted) to
        /// each deduplicated waiter.
        bytes: Bytes,
    },
    /// The suffix response carried no ETag; the caller serves passthrough.
    NonCacheable,
    /// The suffix response did not cleanly resolve to a tail (range ignored,
    /// short/long body, degenerate range); fall back to the HEAD path.
    NeedHead,
}

/// A shared, awaitable in-flight fill (#19): the fill runs once in a detached
/// task and every waiter clones this handle to await the same result. The
/// error is `Arc`-wrapped so the output is `Clone` — [`ReadError`] is not, and
/// [`Shared`] requires a `Clone` output.
type SharedFill<V> = Shared<BoxFuture<'static, Result<V, Arc<ReadError>>>>;

/// Result of [`Inner::start_flight`]: either a registered shared fill or an
/// inline bypass future when the in-flight map is at capacity.
enum FlightStart<V> {
    /// Deduplicated fill already counted against [`InFlight::size`].
    Shared(SharedFill<V>),
    /// Cap bypass: run this future without map registration.
    Bypass(BoxFuture<'static, Result<V, ReadError>>),
}

/// One singleflight in-flight map: keys of `K` to shared fills yielding `V`.
type FlightMap<K, V> = Mutex<HashMap<K, SharedFill<V>>>;

/// One block's serving shape. Hits and joined fills are complete immutable
/// buffers; the leader of a cold backend fill receives a live stream whose
/// producer validates and admits the complete block off the client path.
enum BlockServe {
    /// A complete block from a cache tier, peer, or joined singleflight.
    Buffered(Bytes, Tier),
    /// The requested slice of a backend fill as origin chunks arrive.
    Streaming(BodyStream, Tier),
}

/// Winner selection for a streaming block fill under the existing
/// singleflight map.
enum StreamingFillStart {
    /// Another reader owns the origin stream; await its completed block.
    Join(SharedFill<Bytes>),
    /// This reader owns the live client stream and completion signal.
    Lead {
        /// Bounded response channel fed by the detached origin reader.
        receiver: tokio::sync::mpsc::Receiver<Result<Bytes, ReadError>>,
        /// Producer side of the bounded response channel.
        client: tokio::sync::mpsc::Sender<Result<Bytes, ReadError>>,
        /// Signals once the origin request has resolved its response headers.
        started: tokio::sync::oneshot::Sender<()>,
        /// Keeps the ordered outer future pending until that request starts,
        /// allowing the bounded window to poll and start later fills.
        start_waiter: tokio::sync::oneshot::Receiver<()>,
        /// Publishes the validated complete block to singleflight waiters.
        completion: tokio::sync::oneshot::Sender<Result<Bytes, Arc<ReadError>>>,
        /// Holds one background-fill slot through aligned-tail completion.
        background_slot: Option<tokio::sync::OwnedSemaphorePermit>,
    },
    /// Serve the exact requested range without admission because background
    /// fill capacity is already occupied by organic reads.
    Direct,
    /// The global in-flight bound is full; use the buffered fail-open path.
    Bypass,
    /// The block landed in DRAM between the ladder's tier probes and this map
    /// check. A completed fill inserts its block *before* removing its map
    /// entry, so an empty map plus a DRAM re-probe under the map lock is
    /// conclusive — without it, a reader that probed the tiers just before the
    /// insert and reached the map just after the removal would re-fill a block
    /// that is already resident (the #273 race family).
    Hit(Bytes),
}

/// The singleflight in-flight maps (#19): one per fill site, each keyed at the
/// granularity that fill dedups on. Concurrent identical cold reads collapse
/// to one backend request; the first requester runs the fill (in a detached
/// task) and later requesters await the same [`SharedFill`].
///
/// Keying is by *ownership* (#17): the map key carries the object identity
/// that ring routing keys on, so at N=1 this local map already is the owner's
/// map. Cross-node collapse — the home node absorbing the whole pod's stampede
/// — is the M2 extension point this leaves open (#29's peer metadata protocol runs before
/// these fills); this structure does not itself reach across nodes.
#[derive(Default)]
struct InFlight {
    /// Mapping-resolving first-fill probes, keyed by `(object, block_index)`.
    /// Pre-ETag: the probe is what *resolves* the ETag, so it cannot key by
    /// one (unlike [`InFlight::blocks`]).
    probes: FlightMap<(CacheKey, u64), ProbeOutcome>,
    /// Exact partial first fills keyed by object and inclusive requested range.
    /// This preserves collapsed forwarding without making the foreground wait
    /// for the rest of the aligned cache block.
    partials: FlightMap<(CacheKey, u64, u64), PartialOutcome>,
    /// Verified block fills, keyed by the full [`BlockKey`] (object + ETag +
    /// index): concurrent readers of the same version's same block share one
    /// backend GET.
    blocks: FlightMap<BlockKey, Bytes>,
    /// HEAD metadata resolutions, keyed by object.
    heads: FlightMap<CacheKey, MetaLookup>,
    /// Mutable-mapping revalidations (#14), keyed by object: a burst of reads
    /// that all find the same mapping expired collapse to one conditional
    /// request.
    revalidations: FlightMap<CacheKey, RevalOutcome>,
    /// Buffered cold suffix fills (#149), keyed by `(object, suffix_len)`.
    /// Pre-ETag like [`InFlight::probes`] — the suffix GET is what resolves the
    /// ETag — and keyed by length too, since different footer lengths are
    /// different tails. A footer stampede (N cold `bytes=-len` reads of one
    /// object) collapses to one backend GET, every waiter serving from the one
    /// buffered tail. Only tails at or under the configured data-block size
    /// register here.
    suffixes: FlightMap<(CacheKey, u64), SuffixOutcome>,
    /// Total live entries across every map — the gauge #46 exports (via
    /// [`HybridCacheEngine::inflight_len`]) and the [`MAX_INFLIGHT`] bound's
    /// subject. Kept as one atomic so the gauge needs no map locks.
    size: AtomicU64,
    /// Notified every time a detached fill task finishes (after it has admitted
    /// its result and decremented [`InFlight::size`]). [`Inner::wait_for_inflight_fills`]
    /// — the flush barrier (#273) — waits on this so `flush()` can drain writes
    /// only once every in-flight admission has actually run. Off the read path.
    quiesced: tokio::sync::Notify,
}

/// The hybrid DRAM+disk cache engine, implementing [`ObjectRead`] over a
/// fill backend (issue #12). Cheap to clone (one `Arc` bump) so response
/// body streams can outlive the request that created them.
pub struct HybridCacheEngine<B, P, R> {
    /// Shared state; `Arc` so body streams are `'static`.
    inner: Arc<Inner<B, P, R>>,
}

impl<B, P, R> Clone for HybridCacheEngine<B, P, R> {
    /// Clones the handle, not the engine (manual impl: `derive` would
    /// needlessly require `B: Clone`).
    fn clone(&self) -> Self {
        HybridCacheEngine {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Engine state shared between the handle and in-flight body streams.
struct Inner<B, P, R> {
    /// The foyer hybrid (DRAM+disk) cache holding block entries.
    blocks: HybridCache<BlockEntryKey, Bytes>,
    /// Configured data-cache block geometry. Every alignment, fill, admission
    /// sketch, and Foyer region calculation uses this one value.
    block_bytes: u64,
    /// The DRAM-only metadata cache holding the key→ETag mappings, in its
    /// own eviction domain (see the module comment). Values carry the #14
    /// mutability class and refresh instant alongside the mapping.
    meta: Cache<CacheKey, MappingEntry>,
    /// The dedicated metadata store (#50): a second foyer instance pinning the
    /// raw bytes of table metadata (metadata.json, manifest lists, manifests,
    /// Parquet footers) in its own eviction domain, hard-isolated from data
    /// pressure. A data scan flowing through `blocks` structurally cannot evict
    /// anything here.
    meta_store: MetaStore,
    /// Routes each read to the metadata store or the data store (#50). Pure and
    /// deterministic; consults the mapper hook (#49) when the server wires one.
    router: MetaRouter,
    /// Fill source: on a full miss, blocks are fetched from here. In the
    /// server this is #117's passthrough over the origin bucket.
    backend: B,
    /// Peer rung of the miss ladder ([`NoopPeerFetch`] while N=1).
    peers: P,
    /// Ownership ring. Every request resolves `ring.owner(key)` before any
    /// cache access — no code path may assume a key is locally owned.
    ring: R,
    /// This node's identity, compared against ring owners.
    node: NodeId,
    /// Read-path counters (Prometheus wiring is #46's job).
    counters: CacheCounters,
    /// Per-key invalidation fence for metadata admissions (#124).
    fence: MetaFence,
    /// Singleflight in-flight maps (#19): concurrent identical cold fills
    /// collapse to one backend request.
    inflight: InFlight,
    /// Hard ceiling for aligned fill work continuing after client bytes are
    /// ready. Organic exact-range reads never acquire this semaphore.
    background_fills: Arc<tokio::sync::Semaphore>,
    /// Scan-resistant admission policy (#15): gates every data-block insert so
    /// a one-touch scan cannot evict the working set. Metadata mappings bypass
    /// it (always admitted).
    admission: Admission,
    /// Monotonic clock the mutable-mapping TTL reads (#14). Injectable for
    /// deterministic expiry tests; production is [`SystemClock`].
    clock: Arc<dyn Clock>,
    /// TTL for a mutable-classified mapping (#14): past it, the next read
    /// revalidates with a conditional `If-None-Match`. `0` revalidates every
    /// read. Immutable-classified mappings ignore this.
    mutable_mapping_ttl: Duration,
    /// The global cache generation (issue #178). Every block/metadata lookup
    /// and insert stamps this value into its key; [`HybridCacheEngine::purge`]
    /// bumps it (an O(1) logical clear), instantly stranding every entry under
    /// an earlier generation. It never crosses the wire (peers exchange logical
    /// [`BlockKey`]s only) — it is a node-local physical epoch. Persisted to
    /// [`GEN_FILE`] so a purge survives a restart. Acquire/Release: a read that
    /// starts after `purge` returns observes the bumped generation.
    purge_generation: AtomicU64,
    /// Where [`Inner::purge_generation`] is persisted (`cache.dir/`[`GEN_FILE`]).
    generation_path: PathBuf,
    /// Block-tier DRAM bytes admitted since the last purge (issue #178). A
    /// gauge input only: `min(this, blocks.memory().usage())` is the live
    /// (current-generation) resident bytes, and the remainder of the foyer
    /// usage is the reclaimable (stale-generation) bytes still awaiting LRU
    /// reclaim. Relaxed — an approximate gauge, never a correctness gate.
    live_block_bytes: AtomicU64,
    /// Metadata-store DRAM-front bytes admitted since the last purge — the
    /// [`Inner::live_block_bytes`] analogue for the pinned metadata store.
    live_meta_bytes: AtomicU64,
    /// Key→ETag mapping-cache bytes admitted since the last purge. The mapping
    /// cache is *cleared* on purge (not epoched), but foyer's memory `clear()`
    /// does not reset its `usage()` gauge in 0.22.3 — the stale bytes churn down
    /// as fresh mappings replace them — so the live gauge tracks admitted bytes
    /// here (clamped by `usage()`) rather than trusting the post-clear gauge.
    live_mapping_bytes: AtomicU64,
    /// Warm-from-peers deadline (#30): Unix-epoch milliseconds until which this
    /// node, having recently joined and taken ownership of ~1/N of the
    /// keyspace, pulls a locally-missed *owned* block from its previous holder
    /// before a cold backend fill. `0` means not warming. A deadline (rather
    /// than a task-cleared flag) auto-expires with no timer: once it passes, an
    /// owned cold miss goes straight to the backend, so a settled pod pays no
    /// extra peer hop. A draining donor is warmed from regardless of this
    /// window (that pull is unconditional — see [`Inner::read_block`]).
    warming_until_ms: AtomicU64,
    /// Evict-first demotion set (#198): objects a compaction retired. A demoted
    /// object's block lookups resolve at a poisoned generation, so its cached
    /// blocks are unreachable misses that LRU ages out before live data, and its
    /// fills skip admission (never re-admitted). The hot path pays one relaxed
    /// atomic load ([`Demotions::any`]); the set itself is only consulted on the
    /// cold miss ladder, and only while something is demoted.
    demotions: Demotions,
    /// Runtime disk-full guardrail (#96). When set, [`Inner::admit_block`]
    /// admits nothing: reads still serve (from what is resident, then the
    /// origin), but the cache stops writing new blocks so a full disk never
    /// crashes the node. The server's background free-space poll raises and
    /// clears this; the fill path pays one relaxed atomic load to read it, never
    /// a syscall. Shared behind an `Arc` so the poll and the engine see one flag.
    caching_paused: Arc<AtomicBool>,
    /// The two sparse foyer backing files (blocks and metadata), kept so the
    /// shared-budget accounting (#223) can measure how much of the logical
    /// capacity they have physically consumed. Read only by the background disk
    /// poll, never on a request path.
    device_paths: [PathBuf; 2],
    /// Total logical NVMe budget shared by the sparse device files.
    disk_capacity_bytes: u64,
}

impl<B> HybridCacheEngine<B, NoopPeerFetch, RendezvousRing>
where
    B: ObjectRead,
{
    /// Builds the M1 cluster-of-one engine: a single-member rendezvous ring
    /// that owns every key, and the no-op peer fetch. Must be called from
    /// within a Tokio runtime (foyer spawns its flush/reclaim workers on it).
    pub async fn single_node(backend: B, config: &CacheConfig) -> Result<Self, EngineError> {
        let node = NodeId::new(SINGLE_NODE_ID);
        Self::new(
            backend,
            NoopPeerFetch,
            RendezvousRing::single(node.clone()),
            node,
            config,
        )
        .await
    }
}

impl<B, P, R> HybridCacheEngine<B, P, R>
where
    B: ObjectRead,
    P: PeerFetch + 'static,
    R: Ring + Send + Sync + 'static,
{
    /// Builds the engine over an explicit ring, peer transport, and node
    /// identity — the constructor M2's real membership plugs into, and what
    /// tests use to prove the read path routes by ownership. Must be called
    /// from within a Tokio runtime.
    pub async fn new(
        backend: B,
        peers: P,
        ring: R,
        node: NodeId,
        config: &CacheConfig,
    ) -> Result<Self, EngineError> {
        Self::new_with_background_fill_limit(
            backend,
            peers,
            ring,
            node,
            config,
            DEFAULT_BACKGROUND_FILL_SLOTS,
        )
        .await
    }

    /// Builds the engine with a caller-sized lane for aligned fill tails that
    /// continue after requested bytes are ready. Servers derive this from the
    /// configured backend concurrency so background cache work always leaves
    /// most origin capacity available to user reads.
    pub async fn new_with_background_fill_limit(
        backend: B,
        peers: P,
        ring: R,
        node: NodeId,
        config: &CacheConfig,
        background_fill_limit: usize,
    ) -> Result<Self, EngineError> {
        Self::build_engine(
            backend,
            peers,
            ring,
            node,
            config,
            MetaRouter::heuristics_only(),
            Arc::new(SystemClock),
            background_fill_limit,
        )
        .await
    }

    /// Builds the engine with an explicit metadata router (#50) — the hook the
    /// server uses to wire the logical-key mapper (#49) in as a classifier, so
    /// catalogued metadata routes to the pinned store on top of the always-on
    /// pre-mapper heuristics. `new` uses the heuristics-only router, which
    /// already pins every un-onboarded table's planning objects. Must be called
    /// from within a Tokio runtime.
    pub async fn new_with_router(
        backend: B,
        peers: P,
        ring: R,
        node: NodeId,
        config: &CacheConfig,
        router: MetaRouter,
    ) -> Result<Self, EngineError> {
        Self::build_engine(
            backend,
            peers,
            ring,
            node,
            config,
            router,
            Arc::new(SystemClock),
            DEFAULT_BACKGROUND_FILL_SLOTS,
        )
        .await
    }

    /// The full constructor, taking an explicit [`Clock`] — the seam the #14
    /// mutable-mapping-TTL tests use to drive expiry deterministically. Every
    /// public constructor funnels here with the production [`SystemClock`].
    #[allow(clippy::too_many_arguments)]
    async fn build_engine(
        backend: B,
        peers: P,
        ring: R,
        node: NodeId,
        config: &CacheConfig,
        router: MetaRouter,
        clock: Arc<dyn Clock>,
        background_fill_limit: usize,
    ) -> Result<Self, EngineError> {
        // Shared with the block cache's metrics registry: foyer accumulates its
        // silent flush-buffer drops here, off-thread, and the counter snapshot
        // reads it (#278).
        let overflows = Arc::new(AtomicU64::new(0));
        let (blocks, meta, meta_store) = build_caches(config, Arc::clone(&overflows)).await?;
        // Recover the persisted purge generation (#178): a restart resumes at
        // the generation it last ran at, so entries written under it recover
        // warm (#16) while any pre-purge generation stays unreachable garbage.
        let generation = read_generation(&config.dir);
        Ok(HybridCacheEngine {
            inner: Arc::new(Inner {
                blocks,
                block_bytes: config.data_block_bytes.0,
                meta,
                meta_store,
                router,
                backend,
                peers,
                ring,
                node,
                counters: CacheCounters {
                    storage_buffer_overflows: overflows,
                    ..CacheCounters::default()
                },
                fence: MetaFence::default(),
                inflight: InFlight::default(),
                background_fills: Arc::new(tokio::sync::Semaphore::new(background_fill_limit)),
                admission: Admission::new(
                    &config.admission,
                    config.capacity_bytes.0,
                    config.data_block_bytes.0,
                ),
                clock,
                mutable_mapping_ttl: Duration::from_secs(config.mutable_mapping_ttl_secs),
                purge_generation: AtomicU64::new(generation),
                generation_path: config.dir.join(GEN_FILE),
                live_block_bytes: AtomicU64::new(0),
                live_meta_bytes: AtomicU64::new(0),
                live_mapping_bytes: AtomicU64::new(0),
                warming_until_ms: AtomicU64::new(0),
                demotions: Demotions::new(),
                caching_paused: Arc::new(AtomicBool::new(false)),
                device_paths: [
                    config.dir.join(BLOCKS_DEVICE_FILE),
                    config.dir.join(META_DEVICE_FILE),
                ],
                disk_capacity_bytes: config.capacity_bytes.0,
            }),
        })
    }

    /// The engine's read-path counters, live (see [`CacheCounters`]).
    pub fn counters(&self) -> &CacheCounters {
        &self.inner.counters
    }

    /// The shared runtime disk-full flag (#96). The server's background
    /// free-space poll owns raising and clearing it; while it is set the engine
    /// admits no new blocks and serves reads from what is resident plus origin
    /// fills. Handed out as a clone of the `Arc` so the poll and the engine
    /// drive one flag. Setting it never blocks a read — the fill path reads it
    /// with a single relaxed load.
    pub fn caching_paused_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.inner.caching_paused)
    }

    /// Whether caching is currently paused by the disk-full guardrail (#96).
    pub fn is_caching_paused(&self) -> bool {
        self.inner.caching_paused.load(Ordering::Relaxed)
    }

    /// Bytes of this engine's logical disk capacity its sparse device files
    /// have not physically consumed yet (#223) — the share of the one NVMe
    /// budget still unclaimed by the block and metadata stores. The server's
    /// disk poll grants the write-back fragment store exactly this, so the two
    /// share `cache.capacity_bytes` first come, first served. Two `stat` calls;
    /// only ever called from the background poll, never on a request path.
    pub fn disk_growth_room_bytes(&self) -> u64 {
        self.inner
            .device_paths
            .iter()
            .map(|path| verglas_core::disk::file_growth_room(path))
            .sum()
    }

    /// Bytes physically allocated from the configured NVMe budget.
    pub fn disk_usage_bytes(&self) -> u64 {
        self.inner
            .disk_capacity_bytes
            .saturating_sub(self.disk_growth_room_bytes())
    }

    /// Opens the warm-from-peers window (#30) for `window` from now: while it is
    /// open, an *owned* block this node misses locally is pulled from its
    /// previous holder over the peer network before a cold backend fill, so a
    /// freshly-joined node recovers its hit rate from the pod instead of from
    /// the backend. The server calls this at startup when it joins a pod; a
    /// single-node server never does, so its read path is unchanged. The window
    /// auto-expires (a deadline, no timer); calling again extends it. A draining
    /// donor is warmed from regardless of this window (that pull is
    /// unconditional), so drain (#31) does not depend on it.
    pub fn begin_warming(&self, window: Duration) {
        let deadline = now_unix_millis().saturating_add(window.as_millis() as u64);
        // Store the later of any existing deadline and the new one, so an
        // extend never shortens an in-progress window.
        let current = self.inner.warming_until_ms.load(Ordering::Relaxed);
        self.inner
            .warming_until_ms
            .store(deadline.max(current), Ordering::Relaxed);
    }

    /// Whether the warm-from-peers window is currently open (#30) — exposed so
    /// the server and tests can observe the join warm-up state.
    pub fn is_warming(&self) -> bool {
        self.inner.is_warming()
    }

    /// Refreshes catalog-pointer objects for watched tables (#47/#50): drops
    /// each key's mapping under the invalidation fence, so the next planning
    /// read re-resolves the moved snapshot and pins the new metadata. This
    /// replaces TTL expiry for watched tables — the watcher already knows when
    /// the pointer moved, so metadata is refreshed on commit, not on a timer.
    ///
    /// The pinned bytes of the *old* version become unreachable the instant the
    /// mapping is dropped (blocks are ETag-keyed); the store being DRAM-only
    /// with its own LRU, they age out within the meta budget rather than
    /// accumulating without bound. Not on the hot path — the watcher calls this
    /// on a snapshot change.
    pub fn refresh_watched(&self, keys: &[CacheKey]) {
        for key in keys {
            self.inner.invalidate_key(key);
            CacheCounters::bump(&self.inner.counters.meta_refreshes);
        }
    }

    /// Bytes currently held by the DRAM tiers (block memory tier plus
    /// metadata cache), as charged by their weighters — the gauge #46
    /// exports for the DRAM ceiling. Between requests this never exceeds
    /// `cache.dram_bytes` minus the disk-write pipeline reservation; during
    /// requests, refcount-pinned in-flight blocks may add transient weight
    /// (see the module comment on budgets).
    pub fn dram_usage(&self) -> u64 {
        (self.inner.blocks.memory().usage() + self.inner.meta.usage()) as u64
            + self.inner.meta_store.usage()
    }

    /// Of [`HybridCacheEngine::dram_usage`], the bytes resident under the
    /// *current* generation — the usable working set (issue #178). Honest at
    /// T=0: right after a purge this reads ~0 (the generation bump reset the
    /// live gauges and nothing current-generation is resident yet), even though
    /// `dram_usage` still reports the physically resident, now-reclaimable
    /// bytes. It converges up to `dram_usage` as the working set refills.
    ///
    /// Accounting: each store contributes `min(bytes admitted since the last
    /// purge, its current foyer occupancy)` — clamped by occupancy so eviction
    /// of live entries can never make this exceed what is actually resident, and
    /// clamped low to ~0 right after a purge resets the admitted-bytes counters.
    /// The mapping cache is counted the same way (not by its raw `usage()`)
    /// because foyer's memory `clear()` leaves that gauge stale in 0.22.3. A
    /// gauge, not a ceiling: the hard DRAM ceiling is enforced by foyer over
    /// live+reclaimable together.
    pub fn dram_live_bytes(&self) -> u64 {
        let live_blocks = self
            .inner
            .live_block_bytes
            .load(Ordering::Relaxed)
            .min(self.inner.blocks.memory().usage() as u64);
        let live_meta = self
            .inner
            .live_meta_bytes
            .load(Ordering::Relaxed)
            .min(self.inner.meta_store.usage());
        let live_mapping = self
            .inner
            .live_mapping_bytes
            .load(Ordering::Relaxed)
            .min(self.inner.meta.usage() as u64);
        live_blocks + live_meta + live_mapping
    }

    /// Of [`HybridCacheEngine::dram_usage`], the bytes resident under a
    /// *superseded* generation — logically purged, physically resident until
    /// LRU reclaim ages them out (issue #178). This is `dram_usage -
    /// dram_live_bytes`, i.e. the reclaimable tail a repeat-cold benchmark
    /// should see shrink toward zero as the cold leg refills the cache.
    pub fn dram_reclaimable_bytes(&self) -> u64 {
        self.dram_usage().saturating_sub(self.dram_live_bytes())
    }

    /// Demotes `files` (evict-first, issue #198): retirement's schedule marks a
    /// compaction-orphaned object demoted, and from here on its cached blocks are
    /// unreachable misses — refilled straight through the cache (never
    /// re-admitted). Idempotent and off the request hot path. Time travel is
    /// unaffected: the object still resolves (the mapper retains old snapshots
    /// through the grace window); a demoted read is simply served cold from the
    /// backend rather than warm.
    ///
    /// Returns one [`DemoteReceipt`] per file capturing the ETag (from the
    /// still-warm mapping, when present), the size, and the current admission
    /// generation — everything [`HybridCacheEngine::hard_evict`] later needs to
    /// enumerate and physically remove the object's blocks (#305). A cold
    /// mapping yields `etag: None`; that object cannot be enumerated and its
    /// stale blocks are left to LRU aging (bounded, logged by the caller).
    pub fn demote(&self, files: &[DemoteRequest]) -> Vec<DemoteReceipt> {
        let generation = self.inner.purge_generation.load(Ordering::Acquire);
        let mut receipts = Vec::with_capacity(files.len());
        let mut keys = Vec::with_capacity(files.len());
        for file in files {
            let mapping = self
                .inner
                .meta
                .get(&file.key)
                .map(|entry| entry.value().clone());
            let etag = mapping.as_ref().map(|m| m.meta.etag.clone());
            let size = if file.size > 0 {
                file.size
            } else {
                mapping.as_ref().map(|m| m.meta.size).unwrap_or(0)
            };
            receipts.push(DemoteReceipt {
                key: file.key.clone(),
                etag,
                size,
                generation,
            });
            keys.push(file.key.clone());
        }
        // The pending-bytes gauge counts each object once, however many
        // commits re-demote it: only newly demoted keys add their size.
        let newly = self.inner.demotions.demote(&keys);
        for receipt in &receipts {
            if newly.contains(&receipt.key) {
                CacheCounters::add(&self.inner.counters.retired_bytes_pending, receipt.size);
            }
        }
        receipts
    }

    /// Hard-evicts expired demotions (#198/#305): the server's grace-window
    /// sweep calls this once a retired file's snapshots have expired. Each
    /// eviction with a known ETag has its block entries **physically removed**
    /// from the stores — data blocks and pinned metadata blocks alike — so the
    /// space returns to the shared budget immediately instead of waiting for
    /// eviction pressure that a fully-preallocated device may never generate.
    /// The stale key→ETag mapping is dropped too, and the demotion cleared, so
    /// the object is cacheable again on its next (backend) read. Returns the
    /// bytes reclaimed. Off the hot path.
    pub fn hard_evict(&self, evictions: &[HardEviction]) -> u64 {
        let block_bytes = self.inner.block_bytes;
        let mut reclaimed = 0u64;
        let mut keys = Vec::with_capacity(evictions.len());
        for eviction in evictions {
            if let Some(etag) = &eviction.etag
                && eviction.size > 0
            {
                for index in 0..eviction.size.div_ceil(block_bytes) {
                    let block = BlockKey {
                        object: eviction.key.clone(),
                        etag: etag.clone(),
                        block_bytes,
                        block_index: index,
                    };
                    self.inner.blocks.remove(&BlockEntryKey {
                        block: block.clone(),
                        generation: eviction.generation,
                    });
                    self.inner.meta_store.remove(&MetaEntryKey::Block {
                        block,
                        generation: eviction.generation,
                    });
                }
                reclaimed = reclaimed.saturating_add(eviction.size);
                CacheCounters::bump(&self.inner.counters.retired_files_reclaimed);
                CacheCounters::add(&self.inner.counters.retired_bytes_reclaimed, eviction.size);
            }
            // The stale mapping goes regardless: the next read re-resolves at
            // the origin instead of chasing a dead ETag.
            self.inner.meta.remove(&eviction.key);
            keys.push(eviction.key.clone());
        }
        // Subtract from the pending gauge only what demotion actually added.
        let cleared = self.inner.demotions.clear(&keys);
        for eviction in evictions {
            if cleared.contains(&eviction.key) {
                self.inner
                    .counters
                    .retired_bytes_pending
                    .fetch_sub(eviction.size, Ordering::Relaxed);
            }
        }
        reclaimed
    }

    /// Whether `key` is currently demoted (evict-first). For metrics and tests.
    pub fn is_demoted(&self, key: &CacheKey) -> bool {
        self.inner.demotions.contains(key)
    }

    /// The number of currently-demoted objects (issue #198), for metrics.
    pub fn demoted_count(&self) -> usize {
        self.inner.demotions.len()
    }

    /// Number of in-flight singleflight fills across every map (#19) — the
    /// gauge #46 exports and the [`MAX_INFLIGHT`] bound's subject. Cheap: one
    /// relaxed atomic load, no map locks.
    pub fn inflight_len(&self) -> u64 {
        self.inner.inflight.size.load(Ordering::Relaxed)
    }

    /// Reads one exact reconstructed PostgreSQL page from the shared DRAM/NVMe
    /// cache without consulting the object-store backend. Foyer observes every
    /// DRAM and disk hit, so its W-TinyLFU policy learns page demand alongside
    /// ordinary Iceberg block demand.
    pub async fn get_materialized_page(&self, key: &MaterializedPageKey) -> Option<Bytes> {
        let block = key.block_key();
        let block_key = self.inner.block_entry_key(block.clone());
        if let Some(entry) = self.inner.blocks.memory().get(&block_key) {
            self.inner.admission.record_hit(&block_key);
            CacheCounters::bump(&self.inner.counters.dram_hits);
            CacheCounters::add(
                &self.inner.counters.dram_bytes_served,
                POSTGRES_PAGE_BYTES as u64,
            );
            return Some(entry.value().clone());
        }
        CacheCounters::bump(&self.inner.counters.dram_misses);

        match self.inner.blocks.get(&block_key).await {
            Ok(Some(entry)) if entry.value().len() == POSTGRES_PAGE_BYTES => {
                self.inner.admission.record_hit(&block_key);
                CacheCounters::bump(&self.inner.counters.disk_hits);
                CacheCounters::add(
                    &self.inner.counters.disk_bytes_served,
                    POSTGRES_PAGE_BYTES as u64,
                );
                return Some(entry.value().clone());
            }
            Ok(_) | Err(_) => {
                CacheCounters::bump(&self.inner.counters.disk_misses);
            }
        }

        let owner = self.inner.ring.owner(&block.object);
        if owner == self.inner.node {
            return None;
        }
        match self.inner.peers.fetch(owner, &block).await {
            Ok(Some(page)) if page.len() == POSTGRES_PAGE_BYTES => {
                CacheCounters::bump(&self.inner.counters.peer_hits);
                if self.inner.admit_block(&block_key, page.len()) {
                    self.inner.insert_block(block_key, page.clone());
                }
                Some(page)
            }
            Ok(_) => {
                CacheCounters::bump(&self.inner.counters.peer_misses);
                None
            }
            Err(_) => {
                CacheCounters::bump(&self.inner.counters.peer_errors);
                None
            }
        }
    }

    /// Offers one exact reconstructed PostgreSQL page to the shared admission
    /// policy. Rejection is a normal result: the caller already owns the page
    /// bytes and serves them without caching.
    pub async fn put_materialized_page(
        &self,
        key: MaterializedPageKey,
        page: Bytes,
    ) -> Result<bool, MaterializedPageError> {
        if page.len() != POSTGRES_PAGE_BYTES {
            return Err(MaterializedPageError::InvalidSize { actual: page.len() });
        }
        let block = key.block_key();
        let owner = self.inner.ring.owner(&block.object);
        if owner != self.inner.node {
            return Ok(self
                .inner
                .peers
                .store(owner, &block, page)
                .await
                .unwrap_or(false));
        }
        self.put_local_materialized_block(block, page)
    }

    /// Admits a peer-routed materialized page on this node after validating
    /// that the request names the reserved page namespace and current owner.
    pub fn put_local_materialized_block(
        &self,
        block: BlockKey,
        page: Bytes,
    ) -> Result<bool, MaterializedPageError> {
        if page.len() != POSTGRES_PAGE_BYTES
            || block.object.bucket != "@verglas-neon-pages"
            || block.block_bytes != POSTGRES_PAGE_BYTES as u64
            || block.block_index != 0
            || self.inner.ring.owner(&block.object) != self.inner.node
        {
            return Err(MaterializedPageError::InvalidBlock);
        }
        let block_key = self.inner.block_entry_key(block);
        if !self.inner.admit_block(&block_key, page.len()) {
            return Ok(false);
        }
        self.inner.insert_block(block_key, page);
        Ok(true)
    }

    /// Serves one block to a *peer* (#29): the exact [`BlockKey`], from this
    /// node's local cache tiers only (DRAM then disk), or `None` on a miss.
    ///
    /// This is the donor side of the peer-fetch rung, wired to the peer RPC
    /// server. Two invariants make it safe as an intra-pod endpoint:
    ///
    /// - **Cache-only, never a fill.** It never touches the backend or another
    ///   peer, so a peer request cannot trigger a backend fill — no recursion,
    ///   no fill amplification. A miss is a clean miss.
    /// - **Owners only, never replicas.** It answers only for keys this node
    ///   owns per the ring; a local DRAM replica of a peer-fetched block does
    ///   not satisfy peer requests, so owners stay the single intra-pod source
    ///   and replicas never fan out (WHITEPAPER §3.3 replica policy).
    ///
    /// ETag safety is structural: the lookup key is the full `BlockKey`, so a
    /// request for a version this node does not hold simply misses. On a hit it
    /// meters the bytes served to peers (#46 NIC-amplification visibility).
    pub async fn local_block(&self, block: &BlockKey) -> Option<Bytes> {
        // Owners are the single intra-pod source: refuse to serve keys this
        // node does not own (a replica or a misrouted request) — a clean miss,
        // so the requester fills from the backend rather than fanning out. The
        // one widening is a **draining** donor (#31): it has shed ownership but
        // keeps serving the blocks it still holds so its successors warm from
        // it on their way to owning them. `should_serve_peer` encodes both.
        if !self
            .inner
            .ring
            .should_serve_peer(&block.object, &self.inner.node)
        {
            return None;
        }
        // Stamp the current generation (#178): a peer serves only entries from
        // its live generation — a purged (stale-generation) block is a clean
        // miss, so the requester fills from the backend. The generation never
        // crosses the wire; the peer request carries the logical `BlockKey`.
        let block_key = self.inner.block_entry_key(block.clone());
        // Rung 1: DRAM, probed directly.
        if let Some(entry) = self.inner.blocks.memory().get(&block_key) {
            self.inner.admission.record_hit(&block_key);
            return Some(self.record_peer_served(entry.value().clone()));
        }
        // Rung 2: disk. A failing disk tier is a miss to the peer, never an
        // error — the requester degrades to a backend fill.
        if let Ok(Some(entry)) = self.inner.blocks.get(&block_key).await {
            self.inner.admission.record_hit(&block_key);
            return Some(self.record_peer_served(entry.value().clone()));
        }
        None
    }

    /// Meters one block served to a peer and returns its bytes unchanged.
    fn record_peer_served(&self, bytes: Bytes) -> Bytes {
        CacheCounters::bump(&self.inner.counters.peer_served_blocks);
        CacheCounters::add(&self.inner.counters.peer_served_bytes, bytes.len() as u64);
        bytes
    }

    /// Makes every already-served block durable: waits for in-flight fills to
    /// finish admitting, then waits until every queued disk write has landed.
    /// Not part of the read path — tests use it to make tier placement
    /// deterministic, and a graceful shutdown uses it to minimize refills after
    /// restart.
    ///
    /// # Why the fill barrier comes first (#273)
    ///
    /// A streaming fill (#255) serves its bytes to the client and returns while
    /// a detached task validates and admits the complete block off the client
    /// path. So a block a client has fully read may not yet be inserted into
    /// foyer when `flush()` is called — draining the write queue alone would
    /// return before that admission runs, reporting a durability it has not
    /// achieved. Under CPU starvation the detached task can still be pending at
    /// flush time, so the block reaches neither DRAM nor NVMe and a re-read
    /// refills it. `flush()` therefore first quiesces in-flight fills (every
    /// detached fill task decrements [`InFlight::size`] only after it admits),
    /// then drains the disk queue those admissions fed. Losing a warm block to a
    /// refill is acceptable degradation in production; a `flush()` that claims
    /// durability it has not reached is not — that is the contract this restores.
    pub async fn flush(&self) {
        self.inner.wait_for_inflight_fills().await;
        tokio::join!(
            self.inner.blocks.storage().wait(),
            self.inner.meta_store.flush()
        );
    }

    /// Resets the cache to cold by **bumping the global cache generation**
    /// (issue #178), an O(1) logical clear that returns immediately. Not on the
    /// read path — the admin control surface calls this so a repeat-cold
    /// benchmark leg (or an operator recovering from a bad state) needs no
    /// server restart.
    ///
    /// # Generation-epoch purge (no racy `clear()`)
    ///
    /// This is the memcached `flush_all` / Redis lazyfree convergent pattern.
    /// Every block and metadata entry is keyed by `(logical identity,
    /// generation)`; every lookup and insert stamps the current generation.
    /// Purge just increments that generation, so **every resident entry becomes
    /// unreachable in one atomic store** — a lookup at the new generation cannot
    /// match a key written at an old one. No `blocks.clear()`, no
    /// `meta_store.clear()`: foyer's bulk `clear()` panics against a concurrent
    /// `insert` (its LRU weight accounting is not insert-safe;
    /// foyer-rs/foyer#1305), and the whole point of this rework is that the racy
    /// operation is *gone by construction*, not merely serialized behind a flag.
    /// A fill that lands an insert the instant after this bump simply writes a
    /// stale-generation entry — unreachable garbage, never a panic, never a
    /// stale serve.
    ///
    /// # What purge still does synchronously
    ///
    /// - **Persist the generation** first, so the purge survives a restart
    ///   (recovered old-generation entries stay unreachable garbage; #16 warm
    ///   entries written under the current generation still recover).
    /// - **Bump the #124 fence epochs**, so an in-flight metadata resolution
    ///   that began before the purge cannot re-admit its mapping.
    /// - **Clear the key→ETag mapping cache.** This DRAM-only cache is *not* the
    ///   racy foyer store: every mapping insert goes through
    ///   [`Inner::admit_meta_if_fresh`], which holds the fence lock across the
    ///   insert, and [`MetaFence::purge_all_epochs`] takes that same lock
    ///   immediately before this clear — so no mapping insert can be in flight
    ///   concurrently with the clear (a resolution that registers afterward must
    ///   still complete an async backend round-trip before it could insert, long
    ///   after this synchronous clear returns). Clearing it (rather than
    ///   epoching it) keeps mappings keyed by logical identity, which the ring
    ///   and #14 revalidation rely on.
    /// - **Reset the scan-resistant admission policy (#15)** to cold, so a
    ///   repeat-cold leg admits freely again, exactly as a fresh server would.
    ///
    /// # Capacity: how post-purge DRAM/NVMe converge
    ///
    /// Old-generation entries still occupy real foyer capacity until reclaimed,
    /// so the report splits *reclaimable* (resident but logically freed) from
    /// what was *actually freed* (the mapping cache). foyer 0.22.3 exposes no
    /// entry iteration, so there is no proactive sweep; reclaim is **natural LRU
    /// aging**: stale entries are never touched again, so they are the LRU tail
    /// and are evicted first as fresh (current-generation) fills arrive. The
    /// budget ceilings hold *harder* than before — foyer's weighter charges
    /// every resident entry (live or stale) against the tier capacity and evicts
    /// to stay under it, so the DRAM/NVMe hard ceilings (a standing invariant)
    /// are never breached; a purge cannot even transiently over-commit them.
    /// Total occupancy therefore stays pinned near capacity and *converges* from
    /// all-reclaimable at T=0 to all-live as the working set refills.
    ///
    /// Extension point: when foyer exposes iteration/selective eviction, a
    /// low-priority background reclaimer would evict stale-generation entries
    /// eagerly rather than waiting for LRU pressure — a latency optimization, not
    /// a correctness requirement. The per-table generations of #51 are the other
    /// extension: this global generation is their degenerate (whole-cache) case.
    pub async fn purge(&self) -> PurgeReport {
        // Sampled before the gauges reset: the mapping-cache DRAM the clear
        // below drops (logically freed now), and the block/meta-store DRAM that
        // was live an instant ago and is now reclaimable (resident until LRU
        // ages it out). Both come from the clamped live gauges, not foyer's raw
        // `usage()` (which its `clear()` leaves stale in 0.22.3).
        let live = self.dram_live_bytes();
        let mapping_bytes_freed = self
            .inner
            .live_mapping_bytes
            .load(Ordering::Relaxed)
            .min(self.inner.meta.usage() as u64);
        // Saturating: `live` and the mapping gauge are separate atomic
        // snapshots, and fills running between the two loads can make the
        // second exceed the first. The report is an approximation of a moving
        // system; underflow here was a crash (CI, 2026-07-09), not an error
        // in accounting.
        let reclaimable_bytes = live.saturating_sub(mapping_bytes_freed);

        // Bump the generation (Release: a read that starts after purge returns
        // observes it) and persist it before anything else, so the purge is
        // durable across a restart the instant it takes effect.
        let generation = self.inner.purge_generation.fetch_add(1, Ordering::Release) + 1;
        // Best-effort persistence: the in-process purge is already correct (the
        // in-memory generation moved), so an IO error here only costs durability
        // across a restart — never correctness. It degrades silently rather than
        // failing the purge; the cache dir is foyer's own device directory, so a
        // write failure here means the whole cache is already in trouble.
        let _ = persist_generation(&self.inner.generation_path, generation);
        // The generation bump reset the live working set to empty; the gauges
        // must forget the pre-purge fills so `dram_live_bytes` reads ~0 at T=0.
        self.inner.live_block_bytes.store(0, Ordering::Relaxed);
        self.inner.live_meta_bytes.store(0, Ordering::Relaxed);
        self.inner.live_mapping_bytes.store(0, Ordering::Relaxed);

        // Fence off every pre-purge in-flight metadata resolution, then clear
        // the mapping cache under that same lock ordering (see the doc above).
        self.inner.fence.purge_all_epochs();
        self.inner.meta.clear();

        // Reset the scan-resistant admission policy (#15) to cold.
        self.inner.admission.reset();

        PurgeReport {
            generation,
            mapping_bytes_freed,
            reclaimable_bytes,
        }
    }
}

/// Object-safe purge surface the server's admin listener holds as
/// `Arc<dyn CachePurger>` (issue #138) — so `verglas-server`'s admin router can call
/// [`HybridCacheEngine::purge`] without carrying the engine's `B, P, R` type
/// parameters. The only implementor is the engine.
#[async_trait::async_trait]
pub trait CachePurger: Send + Sync {
    /// Resets the cache to cold; see [`HybridCacheEngine::purge`].
    async fn purge(&self) -> PurgeReport;
}

/// A demotion request from the retirement layer (#305): the orphaned object
/// plus its manifest-reported size. `size: 0` means the caller has none (a
/// superseded metadata object, say) — the receipt then carries the mapping's
/// size when the mapping is still warm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemoteRequest {
    /// The compaction-orphaned object.
    pub key: CacheKey,
    /// Its size in bytes, from the manifest entry; 0 when unknown.
    pub size: u64,
}

/// What the engine captured at demotion time (#305) — everything hard eviction
/// later needs to enumerate and physically remove the object's blocks. The
/// retirement layer persists this through the grace window (restarts included)
/// and hands it back as a [`HardEviction`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemoteReceipt {
    /// The demoted object.
    pub key: CacheKey,
    /// The ETag its cached blocks were admitted under, when the mapping was
    /// still warm at demotion time. `None` means the blocks cannot be
    /// enumerated; they stay unreachable (poisoned generation) and age out
    /// under LRU instead.
    pub etag: Option<String>,
    /// The object size in bytes (manifest-reported, else the mapping's).
    pub size: u64,
    /// The admission generation the blocks live under (#178).
    pub generation: u64,
}

/// One expired demotion to physically remove (#305): a persisted
/// [`DemoteReceipt`] come back to the engine after its grace window closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardEviction {
    /// The retired object.
    pub key: CacheKey,
    /// The ETag to enumerate block entries under; `None` skips physical
    /// removal (the blocks stay unreachable and age out under LRU).
    pub etag: Option<String>,
    /// The object size in bytes, bounding the block enumeration.
    pub size: u64,
    /// The admission generation the blocks live under.
    pub generation: u64,
}

/// Object-safe evict-first demotion surface (issue #198). Retirement in
/// verglas-tables holds an `Arc<dyn BlockDemoter>` so the compaction coordinator
/// can demote orphaned files and the server's grace sweep can hard-evict them
/// without carrying the engine's `B, P, R` type parameters. The only implementor
/// is the engine.
pub trait BlockDemoter: Send + Sync {
    /// Demotes `files` (evict-first) and returns the receipts hard eviction
    /// needs later; see [`HybridCacheEngine::demote`].
    fn demote(&self, files: &[DemoteRequest]) -> Vec<DemoteReceipt>;
    /// Physically removes expired demotions' blocks and clears their demotion,
    /// returning the bytes reclaimed; see [`HybridCacheEngine::hard_evict`].
    fn hard_evict(&self, evictions: &[HardEviction]) -> u64;
}

#[async_trait::async_trait]
impl<B, P, R> CachePurger for HybridCacheEngine<B, P, R>
where
    B: ObjectRead + Send + Sync,
    P: PeerFetch + 'static,
    R: Ring + Send + Sync + 'static,
{
    /// Delegates to the inherent [`HybridCacheEngine::purge`].
    async fn purge(&self) -> PurgeReport {
        HybridCacheEngine::purge(self).await
    }
}

impl<B, P, R> BlockDemoter for HybridCacheEngine<B, P, R>
where
    B: ObjectRead + Send + Sync,
    P: PeerFetch + 'static,
    R: Ring + Send + Sync + 'static,
{
    /// Delegates to the inherent [`HybridCacheEngine::demote`].
    fn demote(&self, files: &[DemoteRequest]) -> Vec<DemoteReceipt> {
        HybridCacheEngine::demote(self, files)
    }

    /// Delegates to the inherent [`HybridCacheEngine::hard_evict`].
    fn hard_evict(&self, evictions: &[HardEviction]) -> u64 {
        HybridCacheEngine::hard_evict(self, evictions)
    }
}

/// Reads the persisted purge generation from `dir/`[`GEN_FILE`] (issue #178),
/// defaulting to 0 when the file is absent (a fresh cache) or unreadable. A
/// malformed file degrades to 0 — the conservative choice: at worst it strands
/// warm entries as cold, never resurrects purged ones (a fresh run at
/// generation 0 only matches genuinely never-purged entries).
fn read_generation(dir: &Path) -> u64 {
    std::fs::read(dir.join(GEN_FILE))
        .ok()
        .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0)
}

/// Persists the current purge generation to `path` (issue #178). Called from
/// [`HybridCacheEngine::purge`] so the bump survives a restart. Best-effort: on
/// an IO error the in-process purge is still correct (the in-memory generation
/// moved), only its durability across a restart is lost — the returned error is
/// surfaced to the caller's log, never failing the purge.
fn persist_generation(path: &Path, generation: u64) -> std::io::Result<()> {
    std::fs::write(path, generation.to_le_bytes())
}

/// Current wall-clock time in Unix-epoch milliseconds, saturating to `0` before
/// the epoch (never in practice). The warm-from-peers window (#30) is stored and
/// compared as this scalar; a clock that cannot read the epoch reads `0`, which
/// closes the window (fail safe: warm-from-peers off, backend fill on).
fn now_unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Builds the block cache (foyer hybrid) and the metadata cache (foyer
/// in-memory) from the validated cache config, applying the budget mapping
/// documented in the module comment.
async fn build_caches(
    config: &CacheConfig,
    overflows: Arc<AtomicU64>,
) -> Result<
    (
        HybridCache<BlockEntryKey, Bytes>,
        Cache<CacheKey, MappingEntry>,
        MetaStore,
    ),
    EngineError,
> {
    let block_bytes = config.data_block_bytes.0;
    let min_dram = min_dram_budget_bytes(block_bytes);
    let dram = config.dram_bytes.0;
    if dram < min_dram {
        return Err(EngineError::DramBudgetTooSmall {
            configured: dram,
            minimum: min_dram,
        });
    }
    let min_disk = min_disk_budget_bytes(block_bytes);
    let data_disk_floor = data_disk_floor_bytes(block_bytes);
    let disk = config.capacity_bytes.0;
    if disk < min_disk {
        return Err(EngineError::DiskBudgetTooSmall {
            configured: disk,
            minimum: min_disk,
        });
    }
    // Budgets validated ≥ the minimums above, so these fit usize on 64-bit;
    // try_from guards the (unsupported) 32-bit case rather than truncating.
    let block_dram = usize::try_from(dram).map_err(|_| EngineError::DramBudgetTooSmall {
        configured: dram,
        minimum: min_dram,
    })? - DRAM_PIPELINE_RESERVED_BYTES
        - META_CACHE_BYTES;
    // The metadata store (#50) is carved from *inside* the block-tier DRAM and
    // the disk ceiling: the data tiers get the remainders, so both ceilings are
    // unchanged — the meta budgets are accounted within them, never on top.
    // The meta carve covers BOTH the metadata DRAM front and the metadata
    // tier's small disk-write pipeline, so the engine's minimum budget (and
    // the 80 MB constrained profile) is unchanged.
    let meta_store_carve = meta_store_capacity(block_dram, config.meta_fraction, block_bytes);
    let meta_store_dram = meta_store_carve - META_PIPELINE_RESERVED_BYTES;
    let memory_capacity = block_dram - meta_store_carve;
    // The metadata NVMe tier (#50) is carved from *inside* the disk ceiling:
    // meta_fraction of it, floored at the four small regions foyer needs. The
    // data tier gets the remainder, so the on-disk ceiling holds exactly.
    let meta_store_disk = ((disk as f64 * config.meta_fraction) as u64)
        .clamp(META_DISK_MIN_BYTES, disk - data_disk_floor);
    // The write-back fragment store gets no carve (#223): the block cache's
    // logical capacity is the full remainder, and fragments share the budget
    // first come, first served by accounting — the device file is sparse, so
    // the server's disk poll grants fragments exactly the bytes this store has
    // not physically grown into (see `disk_growth_room_bytes`), and pauses
    // admission before the store would grow into fragment-held bytes.
    let disk_capacity =
        usize::try_from(disk - meta_store_disk).map_err(|_| EngineError::DiskBudgetTooSmall {
            configured: disk,
            minimum: min_disk,
        })?;

    // A single file keeps Foyer's logical regions as offsets, not open file
    // descriptors. Future per-node state uses distinct names in cache.dir.
    let device = FileDeviceBuilder::new(config.dir.join(BLOCKS_DEVICE_FILE))
        .with_capacity(disk_capacity)
        .build()
        .map_err(|e| EngineError::Build(e.to_string()))?;

    let blocks = HybridCacheBuilder::new()
        .with_name("verglas")
        // Surface foyer's silent flush-buffer drops (#278). foyer records an
        // overflow only in its `storage_queue_buffer_overflow` metric; this
        // registry mirrors that one counter into `overflows` and emits a WARN on
        // each drop. Every other foyer metric is discarded (scrape wiring is
        // #46). Installed only on the data-block cache — the metadata tier's
        // writes are tiny and never collide.
        .with_metrics_registry(Box::new(
            crate::foyer_metrics::BlockCacheMetricsRegistry::new(Arc::clone(&overflows)),
        ))
        // Write-through to NVMe (#16): every admitted block is enqueued to the
        // disk tier on insertion, not deferred until eviction. This is what
        // makes warm restart durable — a block that is still hot in DRAM (and
        // so would never have spilled under the default WriteOnEviction policy)
        // is nonetheless already on NVMe, so a restart (graceful *or* `kill -9`)
        // recovers it with zero backend re-fills. The cost is writing every
        // admitted block to NVMe once; for an NVMe cache that durability is the
        // point, and scan-resistant admission (#15) still gates what is written.
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(memory_capacity)
        // Foyer 0.22.3 documents W-TinyLFU as the default but constructs its
        // memory cache with LRU unless this is explicit. Pin the intended
        // policy so repeated Neon pages and Iceberg blocks beat one-touch scan
        // candidates in the same victim comparison.
        .with_eviction_config(LfuConfig {
            // Keep the unfiltered recency window small. A large window lets a
            // sequential scan displace reused entries before TinyLFU compares
            // candidate and victim frequencies.
            window_capacity_ratio: 0.01,
            ..LfuConfig::default()
        })
        // Shard count derived so a shard always fits several blocks — the
        // #122 ceiling invariant (see `shard_min_capacity_bytes`). Regression
        // tripwire: the shard-parameterized DRAM ceiling test.
        .with_shards(memory_shards(memory_capacity, block_bytes))
        // Charge each entry its exact encoded size plus fixed bookkeeping,
        // so `memory_capacity` is a byte ceiling, not an entry-count ceiling.
        .with_weighter(|key: &BlockEntryKey, value: &Bytes| {
            use foyer::Code;
            key.estimated_size() + value.len() + ENTRY_OVERHEAD_BYTES
        })
        .storage()
        // Disk-tier recovery on startup (#16): rebuild the in-memory index from
        // the on-disk Foyer metadata. `Quiet` recovers every intact entry and
        // silently drops corrupted ones (a torn tail write from `kill -9`) via
        // foyer's per-entry checksums, so recovery always completes and a
        // damaged block is a clean miss that refills — never wrong bytes. Pinned
        // explicitly (it is also foyer's default) so it can never silently
        // regress to `None`, which would turn every restart cold.
        .with_recover_mode(RecoverMode::Quiet)
        .with_engine_config(
            BlockEngineConfig::new(device)
                .with_block_size(foyer_block_size(
                    disk_capacity,
                    foyer_disk_block_bytes(block_bytes),
                ))
                // Sized from the block geometry so two back-to-back block writes
                // in one flusher rotation both land (#278); the two together are
                // exactly the fixed pipeline reservation.
                .with_buffer_pool_size(flush_buffer_pool_bytes(block_bytes))
                .with_submit_queue_size_threshold(submit_queue_bytes(block_bytes)),
        )
        // Everything else — IO engine, eviction pickers, compression — stays on
        // foyer defaults; tuning is #16's scope.
        .build()
        .await
        .map_err(|e| EngineError::Build(e.to_string()))?;

    // The metadata cache: in-memory only, byte-weighted, default sharding
    // (entries are a few hundred bytes, so every entry fits any shard and
    // the #122 stranding concern does not apply).
    let meta = CacheBuilder::new(META_CACHE_BYTES)
        .with_name("verglas-meta")
        .with_weighter(|key: &CacheKey, value: &MappingEntry| {
            // The #14 bookkeeping (a class byte and a monotonic instant) is
            // fixed-size and folded into the entry overhead.
            value.meta.weight(key) + ENTRY_OVERHEAD_BYTES
        })
        .build();

    // The dedicated metadata store (#50): its own eviction domain, DRAM front
    // sized within the DRAM budget above, NVMe long tail carved from the disk
    // ceiling, in its own subdirectory beside the data tier's.
    let meta_store = MetaStore::new(
        meta_store_dram,
        usize::try_from(meta_store_disk).map_err(|_| EngineError::DiskBudgetTooSmall {
            configured: disk,
            minimum: min_disk,
        })?,
        &config.dir.join(META_DEVICE_FILE),
    )
    .await
    .map_err(EngineError::Build)?;

    Ok((blocks, meta, meta_store))
}

impl<B, P, R> ObjectRead for HybridCacheEngine<B, P, R>
where
    B: ObjectRead,
    P: PeerFetch + 'static,
    R: Ring + Send + Sync + 'static,
{
    /// Serves a ranged read: resolve ownership, resolve metadata (the
    /// key→ETag mapping — from cache, else from the first fill GET itself so
    /// a cold read costs one backend request per covering block), map the
    /// range to covering blocks, and stream each block's slice through the
    /// miss ladder. Non-cacheable objects (no origin ETag) are served
    /// passthrough with no admission.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        // The serving tier this read resolves to (#46), written as the first
        // block streams and read by the front-end after the body drains. Shared
        // into every return path so the request-duration histogram is labelled
        // by the tier that actually served the read.
        let tier_cell = TierCell::new();

        // Ring discipline (#17): ownership is resolved before any cache
        // access and threaded through the whole ladder.
        let owner = self.inner.ring.owner(key);

        // Store routing (#50): decided once per request and threaded through the
        // fill and serve path so a classified metadata read fills and serves
        // through the pinned, hard-isolated metadata store rather than the
        // data cache. Deterministic and pure (see `classify::MetaRouter`).
        let class = self.inner.router.route(key, range);

        // Resolve the key→ETag mapping once for the whole request (#14): a hit
        // that is still fresh (immutable-classified, or mutable within its TTL)
        // returns immediately; an expired mutable mapping is revalidated with a
        // conditional `If-None-Match` here, before any block is served. A
        // multi-block read of a mutable mapping additionally commits its version
        // here so the whole stream serves one version — read atomicity (#213).
        let mapping = self.inner.resolve_mapping_for_read(key, range).await;

        // A *suffix* read with a resolved mapping is served through the suffix
        // fill site (#131/#149), never the block ladder. Two reasons, one per
        // class:
        //
        // - Metadata (a Parquet footer) is pinned at SUFFIX granularity — the
        //   exact tail bytes — because the data-block ladder would cache the
        //   whole covering block (for a sub-block-size file, the entire data file)
        //   into the small meta store and evict the real planning metadata. Warm
        //   hits serve the pinned tail; misses re-fetch the suffix and pin it.
        // - Data suffixes are never cached (the unaligned tail block is dropped,
        //   #131), so a resolved-mapping data suffix read would otherwise fall to
        //   the block ladder and become a *second* fill site for a request the
        //   cold read served through `first_fill_suffix`. That split is #214: a
        //   reader that resolved the mapping from a concurrent cold suffix fill
        //   would issue its own backend GET instead of joining that fill. Routing
        //   both classes through `first_fill_suffix` keeps suffix reads on one
        //   fill site, so the `suffixes` singleflight collapses them whether or
        //   not the mapping was already resolved.
        //
        // Tails above the buffer cap stream serve-only and stay non-deduped,
        // exactly the #131 path — a bounded, deliberate exception noted there.
        if let ReadRange::Suffix(len) = range
            && len > 0
            && let Some(meta) = mapping.as_ref()
        {
            if class == MetaClass::Meta {
                let suffix_key = self
                    .inner
                    .meta_suffix_key(key.clone(), meta.etag.clone(), len);
                if let Some(bytes) = self.inner.meta_store.get(&suffix_key).await {
                    CacheCounters::bump(&self.inner.counters.meta_hits);
                    let resolved = resolve_range(range, meta.size)?;
                    self.inner.count_served(Tier::Meta, bytes.len() as u64);
                    // A pinned metadata tail is a DRAM-resident warm serve (#46).
                    tier_cell.set(ServedTier::Dram);
                    return Ok(ObjectGet {
                        meta: meta.to_object_meta(),
                        range: resolved,
                        body: stream::once(async move { Ok(bytes) }).boxed(),
                        served_from: tier_cell,
                    });
                }
                CacheCounters::bump(&self.inner.counters.meta_misses);
            }
            // Mapping known but tail not served from cache (a data suffix, or a
            // metadata suffix that was never pinned / was evicted): re-fetch
            // through the suffix fill — one backend GET, the same cost as a block
            // fill, deduped on the `suffixes` map so concurrent readers collapse.
            // For metadata it also re-pins the tail; the block ladder is never
            // involved for suffix reads.
            if let FirstFill::Served { meta, range, body } =
                self.inner.first_fill_suffix(key, len, class).await?
            {
                tier_cell.set(ServedTier::Backend);
                let inner = Arc::clone(&self.inner);
                let counted = body
                    .inspect_ok(move |chunk| {
                        inner.count_served(Tier::Backend, chunk.len() as u64);
                    })
                    .boxed();
                return Ok(ObjectGet {
                    meta: meta.to_object_meta(),
                    range,
                    body: counted,
                    served_from: tier_cell,
                });
            }
            // NeedHead/NonCacheable oddities degrade to the ladder — as DATA,
            // so metadata suffixes can never block-pollute the meta store.
        }
        // The ladder never pins suffix reads into the metadata store (see
        // above); anything else keeps its classified store.
        let store_class = match (range, class) {
            (ReadRange::Suffix(_), MetaClass::Meta) => MetaClass::Data,
            _ => class,
        };

        // ETag discipline (#14 foundation) in both passthrough returns
        // below: no ETag ⇒ nothing to key blocks under ⇒ serve straight
        // through, admit nothing; the origin resolves the range itself,
        // exactly like the plain passthrough.
        let (meta, prefetched) = match mapping {
            Some(meta) => (meta, None),
            // `class`, not `store_class`: the suffix arm inside first_fill
            // pins metadata tails (suffix-granular); the ladder's store_class
            // demotion only applies to block fills below.
            None => match self.inner.first_fill(key, range, class).await? {
                FirstFill::Cacheable { meta, block } => (meta, Some(block)),
                FirstFill::Served { meta, range, body } => {
                    // Suffix fast path (#131): the bytes are already in hand
                    // from the suffix GET. Serve them straight through
                    // (attributed to the backend tier, where they came from),
                    // counting per chunk as they flow so a client disconnect
                    // does not over-count. No second ladder lookup; the tail
                    // block is deliberately not cached.
                    tier_cell.set(ServedTier::Backend);
                    let inner = Arc::clone(&self.inner);
                    let counted = body
                        .inspect_ok(move |chunk| {
                            inner.count_served(Tier::Backend, chunk.len() as u64);
                        })
                        .boxed();
                    return Ok(ObjectGet {
                        meta: meta.to_object_meta(),
                        range,
                        body: counted,
                        served_from: tier_cell,
                    });
                }
                FirstFill::NonCacheable => {
                    CacheCounters::bump(&self.inner.counters.non_cacheable_passthroughs);
                    // Forwarded straight to the origin: the passthrough tier (#46).
                    return self.inner.backend.get(key, range).await;
                }
                FirstFill::NeedHead => match self.inner.resolve_meta_via_head(key).await? {
                    MetaLookup::Cacheable(meta) => (meta, None),
                    MetaLookup::NonCacheable(_) => {
                        CacheCounters::bump(&self.inner.counters.non_cacheable_passthroughs);
                        return self.inner.backend.get(key, range).await;
                    }
                },
            },
        };

        let resolved = resolve_range(range, meta.size)?;
        let object_meta = meta.to_object_meta();
        let body = block_body(
            Arc::clone(&self.inner),
            key.clone(),
            owner,
            meta,
            resolved.clone(),
            prefetched,
            store_class,
            tier_cell.clone(),
        );
        Ok(ObjectGet {
            meta: object_meta,
            range: resolved,
            body,
            served_from: tier_cell,
        })
    }

    /// Serves HEAD from cached metadata when present, resolving it via a
    /// backend HEAD (and admitting it) otherwise — same values a GET would
    /// carry. This is the one place a backend HEAD remains after the
    /// cold-read fast path: a HEAD request has no fill to derive the
    /// mapping from.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        // Ring discipline (#17): resolved for every lookup even though HEAD
        // has no peer rung yet — M2's peer metadata protocol (#29) will ask
        // `owner` for the mapping before falling back to a backend HEAD.
        let _owner = self.inner.ring.owner(key);

        // TTL-aware mapping resolution (#14): a fresh mapping is served from
        // cache; an expired mutable one is revalidated first. No version-commit
        // force (#213) — a HEAD serves only metadata (one version, no block
        // stream), so it cannot tear.
        if let Some(meta) = self.inner.resolve_mapping(key, false).await {
            return Ok(meta.to_object_meta());
        }
        match self.inner.resolve_meta_via_head(key).await? {
            MetaLookup::Cacheable(meta) => Ok(meta.to_object_meta()),
            MetaLookup::NonCacheable(meta) => {
                CacheCounters::bump(&self.inner.counters.non_cacheable_passthroughs);
                Ok(meta)
            }
        }
    }

    /// Escape-hatch reads (version-pinned, single-part, or checksum-enabled —
    /// issues #153/#154/#156) name an immutable, version- or part-scoped view
    /// the cache does not model, so the engine forwards them straight to its
    /// fill backend with no admission, exactly as a non-cacheable read.
    async fn get_direct(
        &self,
        key: &CacheKey,
        range: ReadRange,
        options: DirectReadOptions,
    ) -> Result<DirectGet, ReadError> {
        self.inner.backend.get_direct(key, range, options).await
    }

    /// The HEAD twin of [`get_direct`](Self::get_direct): forwarded to the
    /// backend, never cached.
    async fn head_direct(
        &self,
        key: &CacheKey,
        options: DirectReadOptions,
    ) -> Result<DirectMeta, ReadError> {
        self.inner.backend.head_direct(key, options).await
    }

    /// GetObjectAttributes (issue #154): pure passthrough to the backend,
    /// the cache is never consulted or filled.
    async fn object_attributes(
        &self,
        key: &CacheKey,
        request: AttributesRequest,
    ) -> Result<ObjectAttributes, ReadError> {
        self.inner.backend.object_attributes(key, request).await
    }
}

/// The write path's invalidation hook (#9/#21): fired by the front-end after
/// a write is durable on the backend and before the client is acked.
///
/// Invalidation = removing the key→ETag mapping. That is sufficient: every
/// block is keyed by ETag, so once the mapping is gone the overwritten
/// version's blocks are unreachable (they age out via normal eviction) and
/// the next read re-resolves the mapping against the origin, seeing the new
/// version. The mapping cache is DRAM-only and `remove` is synchronous and
/// infallible, so this hook never blocks a write ack in practice — the `Err`
/// arm of the hook contract stays reachable only for future implementations
/// (e.g. M2 pods invalidating the owner's mapping over the network).
#[async_trait::async_trait]
impl<B, P, R> Invalidation for HybridCacheEngine<B, P, R>
where
    B: ObjectRead,
    P: PeerFetch + 'static,
    R: Ring + Send + Sync + 'static,
{
    /// Removes the key→ETag mapping for every affected key. Keys that were
    /// never cached are a no-op success — deletes of cold objects must not
    /// fail the write ack.
    async fn invalidate(&self, keys: &[CacheKey]) -> Result<(), String> {
        for key in keys {
            // Bump the fence epoch and remove the mapping atomically under
            // the fence lock (#124): any in-flight metadata admission whose
            // guard captured the pre-bump epoch is now discarded, so this
            // invalidation cannot be undone by a stale response still in
            // flight from before the racing write became durable.
            self.inner.invalidate_key(key);
        }
        Ok(())
    }
}

/// Builds the response body in block order. Complete hits yield one slice;
/// the leader of a cold backend fill yields origin chunks immediately while a
/// detached producer finishes and admits the complete block. A four-block
/// ordered window overlaps origin request latency without reordering bytes. At
/// most [`BLOCK_FILL_WINDOW`] block values are in flight or retained for one
/// response; the backend's per-bucket limiter remains the origin-wide bound.
#[allow(clippy::too_many_arguments)]
fn block_body<B, P, R>(
    inner: Arc<Inner<B, P, R>>,
    key: CacheKey,
    owner: NodeId,
    meta: CachedMeta,
    range: Range<u64>,
    // A block the mapping-resolving first fill already produced for this
    // request: served directly instead of a second ladder lookup.
    prefetched: Option<(u64, Bytes)>,
    // Which store this read routes to (#50): metadata reads read/fill through
    // the pinned store, data reads through the big hybrid cache.
    class: MetaClass,
    // The request's serving-tier cell (#46): set to the tier that produced the
    // FIRST served block, so the request-duration histogram is labelled by what
    // determined the observed latency. A single relaxed store, off any lock.
    tier_cell: TierCell,
) -> BodyStream
where
    B: ObjectRead,
    P: PeerFetch + 'static,
    R: Ring + Send + Sync + 'static,
{
    if range.is_empty() {
        // A full GET of an empty object: 200 with an empty body.
        return stream::empty().boxed();
    }
    let (first, last) = covering_blocks(&range, inner.block_bytes);
    stream::iter(first..=last)
        .map(move |index| {
            let inner = Arc::clone(&inner);
            let key = key.clone();
            let owner = owner.clone();
            let meta = meta.clone();
            let range = range.clone();
            let prefetched = prefetched.clone();
            let tier_cell = tier_cell.clone();
            async move {
                let block_start = index * inner.block_bytes;
                let from = range.start.max(block_start) - block_start;
                let to = range
                    .end
                    .min(block_start + block_len(meta.size, index, inner.block_bytes) as u64)
                    - block_start;
                let served = match prefetched {
                    // A first-fill block: for a metadata read it was pinned in
                    // the meta store by the probe, so attribute it to that tier;
                    // for data it came straight off the backend fill.
                    Some((i, block)) if i == index => BlockServe::Buffered(
                        block,
                        match class {
                            MetaClass::Meta => Tier::Meta,
                            MetaClass::Data => Tier::Backend,
                        },
                    ),
                    _ => {
                        inner
                            .read_block(
                                &key,
                                &owner,
                                &meta,
                                index,
                                class,
                                from as usize..to as usize,
                            )
                            .await?
                    }
                };
                let tier = match &served {
                    BlockServe::Buffered(_, tier) | BlockServe::Streaming(_, tier) => *tier,
                };
                // Attribute the whole request to the first block's tier (#46):
                // it is the one that determined the request's observed latency.
                if index == first {
                    tier_cell.set(tier.served());
                }
                let body: BodyStream = match served {
                    BlockServe::Buffered(block, _) => {
                        let slice = block.slice(from as usize..to as usize);
                        inner.count_served(tier, slice.len() as u64);
                        stream::once(async move { Ok(slice) }).boxed()
                    }
                    BlockServe::Streaming(body, _) => {
                        let counted = Arc::clone(&inner);
                        body.inspect_ok(move |chunk| {
                            counted.count_served(tier, chunk.len() as u64);
                        })
                        .boxed()
                    }
                };
                Ok(body)
            }
        })
        .buffered(BLOCK_FILL_WINDOW)
        .try_flatten()
        .boxed()
}

impl<B, P, R> Inner<B, P, R>
where
    B: ObjectRead,
    P: PeerFetch + 'static,
    R: Ring + Send + Sync + 'static,
{
    /// Waits until no detached fill task is in flight, so a subsequent storage
    /// drain captures every admission those tasks performed (#273). Each fill
    /// task decrements [`InFlight::size`] and notifies `quiesced` only after it
    /// has admitted its result, so `size == 0` means every served block has been
    /// inserted. Not on the read path — only [`HybridCacheEngine::flush`] (tests,
    /// graceful shutdown) calls this, and it may wait indefinitely under a
    /// continuous fill stream, which a shutdown/quiescent flush never sees.
    async fn wait_for_inflight_fills(&self) {
        loop {
            // Register as a waiter *before* observing the count: a task that
            // decrements to zero and notifies between the check and the await
            // then wakes this already-registered waiter instead of being missed.
            let quiesced = self.inflight.quiesced.notified();
            tokio::pin!(quiesced);
            quiesced.as_mut().enable();
            if self.inflight.size.load(Ordering::Acquire) == 0 {
                return;
            }
            quiesced.await;
        }
    }

    /// Peeks the cached mapping entry (mapping + #14 bookkeeping), lock-free
    /// through foyer. The warm path: no backend traffic, no TTL logic — that
    /// lives in [`Inner::resolve_mapping`].
    fn mapping_entry(&self, key: &CacheKey) -> Option<MappingEntry> {
        self.meta.get(key).map(|entry| entry.value().clone())
    }

    /// Resolves the key→ETag mapping for a read, applying the #14 classification
    /// policy. Returns the mapping to serve, or `None` when there is nothing
    /// usable and the caller must re-resolve from a fill (an uncached key, a
    /// vanished object, or a revalidation that could not confirm freshness).
    ///
    /// - **Immutable-classified** mappings (Iceberg data/metadata files) are
    ///   returned straight from cache and *never* revalidated — this is what
    ///   makes their revalidation traffic exactly zero (acceptance criterion).
    /// - **Mutable-classified** mappings within their TTL are likewise served
    ///   from cache; once expired, a conditional `If-None-Match` revalidation
    ///   runs (deduplicated across a burst) and either refreshes the TTL, rotates
    ///   to the new version, or reports a miss.
    ///
    /// `force_revalidate` bypasses the TTL for a mutable mapping and always
    /// revalidates. It is the read-atomicity commit of issue #213 — see
    /// [`Inner::resolve_mapping_for_read`] for why a multi-block mutable read
    /// must pin its version before streaming.
    ///
    /// Failure mode (documented): a revalidation the backend could not answer
    /// yields `None`, so the read degrades to a full re-resolution (a backend
    /// fill), never to serving the expired mapping's warm blocks unverified —
    /// slow is acceptable, wrong is never.
    async fn resolve_mapping(
        self: &Arc<Self>,
        key: &CacheKey,
        force_revalidate: bool,
    ) -> Option<CachedMeta> {
        let entry = self.mapping_entry(key)?;
        match entry.mutability {
            // The whole point of the immutability bet: no clock read, no
            // backend request, no revalidation counter — ever. An immutable
            // key's bytes never change under a fixed ETag, so a multi-block
            // read of one cannot tear; `force_revalidate` is ignored here.
            Mutability::Immutable => Some(entry.meta),
            Mutability::Mutable => {
                let age = self
                    .clock
                    .now()
                    .saturating_duration_since(entry.refreshed_at);
                if !force_revalidate && age < self.mutable_mapping_ttl {
                    return Some(entry.meta);
                }
                match self.revalidate_mapping(key, entry.meta).await {
                    RevalOutcome::Fresh(meta) | RevalOutcome::Rotated(meta) => Some(meta),
                    RevalOutcome::Miss => None,
                }
            }
        }
    }

    /// Resolves the mapping for a block-ladder read, committing the object
    /// version before the body is streamed when that is needed for read
    /// atomicity (issue #213).
    ///
    /// # The torn read this closes
    ///
    /// A ranged read emits covering blocks in order, while a bounded look-ahead
    /// can fill later blocks before byte 0 is emitted. Every block is keyed by
    /// ETag, so a warm cache hit always returns the mapping's version — but a
    /// *fill* returns whatever version the backend currently holds. If a mutable
    /// object is overwritten while that mapping is still warm, later fills can
    /// see the new version. S3 guarantees a GET returns the whole old object or
    /// the whole new one; a mix (or a mid-stream abort after old bytes went out)
    /// violates that. Old bytes are gone from a non-versioned overwrite, so the
    /// only atomic outcome is to commit the read to a single version *before*
    /// the body begins.
    ///
    /// # The commit
    ///
    /// For a **mutable** mapping whose read spans **more than one block**, this
    /// forces one revalidation (the #14 conditional `If-None-Match`, deduplicated
    /// and fence-checked exactly as an expired-TTL revalidation would be) before
    /// the stream is built. The revalidation rotates the mapping to the current
    /// version if the object changed, so every covering block — hit or fill —
    /// then resolves at that one committed ETag. Nothing is emitted before the
    /// commit, and after it every rung is ETag-keyed, so no interleaving can
    /// tear: a later overwrite either stays invisible (blocks still resolve at
    /// the committed ETag) or fails a fill's ETag check, which errors the whole
    /// request cleanly — never a partial mix. Immutable keys (Iceberg data and
    /// metadata files, per WHITEPAPER §4.4) cannot change under a fixed ETag, so
    /// they skip the commit and stay zero-revalidation and lock-free; so does any
    /// single-block read, which is already atomic (its one block is one version).
    async fn resolve_mapping_for_read(
        self: &Arc<Self>,
        key: &CacheKey,
        range: ReadRange,
    ) -> Option<CachedMeta> {
        // Peek the cached size to decide the covering-block span without a
        // backend round trip. A miss here means the read cold-fills, which is
        // atomic by construction (the fill resolves the version; there is no
        // warm hit of a stale version to mix with).
        let force = match self.mapping_entry(key) {
            Some(entry) if entry.mutability == Mutability::Mutable => {
                resolve_range(range, entry.meta.size)
                    .map(|r| {
                        if r.is_empty() {
                            return false;
                        }
                        // Multi-block: the read spans a boundary, so it can mix
                        // versions unless the version is committed first.
                        let (first, last) = covering_blocks(&r, self.block_bytes);
                        first != last
                    })
                    .unwrap_or(false)
            }
            _ => false,
        };
        self.resolve_mapping(key, force).await
    }

    /// Revalidates one expired mutable mapping, deduplicated by object (#19) so
    /// a burst of reads that all find it expired issues one conditional request.
    async fn revalidate_mapping(
        self: &Arc<Self>,
        key: &CacheKey,
        current: CachedMeta,
    ) -> RevalOutcome {
        self.run_flight(
            |inner| &inner.inflight.revalidations,
            |inner| &inner.counters.deduped_fills,
            key.clone(),
            {
                let key = key.clone();
                move |inner| Box::pin(async move { Ok(inner.do_revalidate(&key, current).await) })
            },
        )
        .await
        // `do_revalidate` never returns `Err`; a spawn/join failure of the
        // detached fill task (runtime shutdown only) degrades to a miss, so the
        // caller re-resolves rather than serving anything stale.
        .unwrap_or(RevalOutcome::Miss)
    }

    /// Issues the conditional `If-None-Match` revalidation and applies its
    /// result (#14). `&self` — runs inside the detached singleflight task over
    /// `Arc<Inner>`, so its fence guard lives entirely within this call.
    ///
    /// Fence discipline (#124): a guard is registered *before* the backend
    /// request, and every admission goes through [`Inner::admit_meta_if_fresh`],
    /// so a write-through invalidation that races the revalidation is never
    /// undone — the confirmed version is still served (serve-without-admit), but
    /// nothing stale is memoized.
    async fn do_revalidate(&self, key: &CacheKey, current: CachedMeta) -> RevalOutcome {
        let guard = self.fence.register(key);
        CacheCounters::bump(&self.counters.meta_revalidations);
        match self.backend.revalidate(key, &current.etag).await {
            Ok(Revalidation::Unchanged) => {
                CacheCounters::bump(&self.counters.meta_revalidations_unchanged);
                // Refresh the TTL by re-admitting the same mapping with a new
                // `refreshed_at`. If a racing invalidation fenced it off we
                // still serve the version we just confirmed current, but
                // memoize nothing — the next read re-resolves.
                self.admit_meta_if_fresh(&guard, key, &current);
                RevalOutcome::Fresh(current)
            }
            Ok(Revalidation::Changed(new_meta)) => match CachedMeta::from_object_meta(&new_meta) {
                Some(meta) => {
                    CacheCounters::bump(&self.counters.meta_revalidations_rotated);
                    // Admit the new mapping (fence-checked). The old version's
                    // blocks are keyed by the old ETag, so they are unreachable
                    // the instant this mapping rotates.
                    self.admit_meta_if_fresh(&guard, key, &meta);
                    RevalOutcome::Rotated(meta)
                }
                // The object turned non-cacheable (lost its ETag): drop the
                // mapping so the next read serves passthrough.
                None => {
                    let _ = self.meta.remove(key);
                    RevalOutcome::Miss
                }
            },
            // Gone, or a transient backend failure: drop the stale mapping and
            // report a miss. The caller's re-resolution surfaces the origin's
            // own answer (a 404, or the backend error) — never stale bytes.
            Ok(Revalidation::Vanished) | Err(_) => {
                let _ = self.meta.remove(key);
                RevalOutcome::Miss
            }
        }
    }

    /// Runs `make` deduplicated under `key` in `map` — the singleflight
    /// primitive (#19). The first caller inserts a detached fill task and
    /// awaits it; concurrent callers await the same [`SharedFill`], so N cold
    /// readers of one block/mapping/suffix produce one backend request. Each
    /// join bumps the caller-selected `dedup_counter`, so the suffix site
    /// (#149) can meter its footer-stampede collapses apart from block/mapping
    /// fills. The fill runs
    /// in a `tokio::spawn`ed task (waiter-cancellation safety: a disconnecting
    /// waiter cannot abort the fill others await — the task is detached), and
    /// the task removes its own map entry once the fill completes, *after* the
    /// fill admitted its result. That ordering is what makes the collapse
    /// exact: a late reader either joins the still-registered fill or hits the
    /// freshly admitted cache entry, never a third state; and because the entry
    /// is removed on error too, errors are never memoized.
    ///
    /// Cold path only: the map lock is taken on a fill (a miss), never on a
    /// warm read (which stays lock-free through foyer) and never across an
    /// await. Over [`MAX_INFLIGHT`] live entries the dedup is bypassed and the
    /// fill runs inline — a bound on the map, not on request concurrency.
    async fn run_flight<K, V>(
        self: &Arc<Self>,
        select: fn(&Inner<B, P, R>) -> &FlightMap<K, V>,
        dedup_counter: fn(&Inner<B, P, R>) -> &AtomicU64,
        key: K,
        make: impl FnOnce(Arc<Inner<B, P, R>>) -> BoxFuture<'static, Result<V, ReadError>>,
    ) -> Result<V, ReadError>
    where
        K: Eq + Hash + Clone + Send + 'static,
        V: Clone + Send + 'static,
    {
        match self.start_flight(select, dedup_counter, key, make) {
            // Awaited outside the lock. Every waiter reconstructs its own error
            // from the one shared error (ReadError is not Clone).
            FlightStart::Shared(shared) => shared.await.map_err(|err| clone_read_error(&err)),
            // Over the bound: fill inline, undeduplicated.
            FlightStart::Bypass(fill) => fill.await,
        }
    }

    /// Registers a singleflight fill without awaiting it. Used by
    /// [`Inner::run_flight`] and by the unmapped-partial path, which must put
    /// the aligned background fill on the flush barrier *before* the foreground
    /// partial flight drops its own in-flight count — otherwise `flush()` can
    /// return between those two moments and a warm re-read refills from origin.
    fn start_flight<K, V>(
        self: &Arc<Self>,
        select: fn(&Inner<B, P, R>) -> &FlightMap<K, V>,
        dedup_counter: fn(&Inner<B, P, R>) -> &AtomicU64,
        key: K,
        make: impl FnOnce(Arc<Inner<B, P, R>>) -> BoxFuture<'static, Result<V, ReadError>>,
    ) -> FlightStart<V>
    where
        K: Eq + Hash + Clone + Send + 'static,
        V: Clone + Send + 'static,
    {
        // `make` is consumed by exactly one of the winner / bypass branches
        // (never by a join); an `Option` makes that conditional move explicit.
        let mut make = Some(make);
        // Decide under the lock, then act outside it — no await is ever
        // reached while the (non-`Send`) map guard is alive.
        let map = select(self);
        let mut guard = map.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(existing) = guard.get(&key) {
            // Join the in-flight fill: no backend request of our own. The
            // per-site counter selector lets the suffix path (#149) break
            // its footer-stampede collapses out from block/mapping fills.
            CacheCounters::bump(dedup_counter(self));
            FlightStart::Shared(existing.clone())
        } else if self.inflight.size.load(Ordering::Relaxed) >= MAX_INFLIGHT {
            // Bounded map: over the cap, fill inline without registering
            // (still correct, just not collapsed).
            let make = make.take().expect("bypass runs the fill factory once");
            FlightStart::Bypass(make(Arc::clone(self)))
        } else {
            let make = make.take().expect("winner runs the fill factory once");
            let inner = Arc::clone(self);
            let entry_key = key.clone();
            let fill = make(Arc::clone(self));
            // `tokio::spawn` inherits neither the task-local request id nor
            // the active span, so a fill running in this detached task would
            // log its failure with no request id (#61). Capture both here, in
            // the requesting task, and re-establish them inside the fill.
            let request_id = verglas_core::trace::current();
            let span = tracing::Span::current();
            let task = tokio::spawn(async move {
                let scoped = fill.instrument(span);
                let result = match request_id {
                    Some(id) => verglas_core::trace::scope(id, scoped).await,
                    None => scoped.await,
                };
                // Remove the entry *after* the fill admitted its result, so
                // the next request retries (on error) or hits the freshly
                // admitted cache state (on success) — never a fresh fill of
                // something already in flight.
                select(&inner)
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&entry_key);
                // Release so the flush barrier's Acquire load, once it sees
                // this decrement, also sees the admission this task just did.
                inner.inflight.size.fetch_sub(1, Ordering::Release);
                inner.inflight.quiesced.notify_waiters();
                result
            });
            let shared = task
                .map(|joined| match joined {
                    Ok(result) => result.map_err(Arc::new),
                    // A panicked/aborted fill task: surface it to every
                    // waiter as a backend error (never a wrong or missing
                    // byte). The detached task is only aborted on runtime
                    // shutdown, not by any waiter.
                    Err(join) => Err(Arc::new(ReadError::Backend(format!(
                        "fill task failed: {join}"
                    )))),
                })
                .boxed()
                .shared();
            guard.insert(key, shared.clone());
            self.inflight.size.fetch_add(1, Ordering::Relaxed);
            FlightStart::Shared(shared)
        }
    }

    /// Resolves the key→ETag mapping with a backend HEAD, admitting it on
    /// success (unless a racing invalidation fenced it off — the caller still
    /// serves the resolved version, see [`MetaFence`]). Deduplicated by object
    /// (#19): concurrent HEAD resolutions of one key share one backend HEAD.
    /// Used for HEAD requests on unmapped keys (no fill to derive the mapping
    /// from) and as [`Inner::first_fill`]'s fallback.
    async fn resolve_meta_via_head(
        self: &Arc<Self>,
        key: &CacheKey,
    ) -> Result<MetaLookup, ReadError> {
        self.run_flight(
            |inner| &inner.inflight.heads,
            |inner| &inner.counters.deduped_fills,
            key.clone(),
            {
                let key = key.clone();
                move |inner| Box::pin(async move { inner.head_lookup(&key).await })
            },
        )
        .await
    }

    /// The HEAD-based metadata resolution, factored out of
    /// [`Inner::resolve_meta_via_head`] so the singleflight can run it once for
    /// concurrent identical resolutions. `&self` — invoked inside the detached
    /// fill task over `Arc<Inner>`, so its fence guard lives entirely within
    /// this call.
    async fn head_lookup(&self, key: &CacheKey) -> Result<MetaLookup, ReadError> {
        // Fence discipline (#124): register before the request goes out.
        let guard = self.fence.register(key);
        CacheCounters::bump(&self.counters.backend_heads);
        let head = match self.backend.head(key).await {
            Ok(head) => head,
            Err(error) => {
                log_fill_error(key, "head", &error);
                return Err(error);
            }
        };
        match CachedMeta::from_object_meta(&head) {
            Some(meta) => {
                // Serve-without-admit on rejection: a request that overlapped a
                // write may serve the version it resolved (fills are
                // ETag-verified, so bytes can never mix), and because nothing
                // was admitted the next request re-resolves and sees the new
                // version.
                self.admit_meta_if_fresh(&guard, key, &meta);
                Ok(MetaLookup::Cacheable(meta))
            }
            None => Ok(MetaLookup::NonCacheable(head)),
        }
    }

    /// Resolves an unmapped GET's metadata from the first fill GET itself:
    /// fetch the block-aligned range the request starts in, take the mapping
    /// from that response's own metadata, and admit both — so a cold read
    /// costs exactly one backend request per covering block instead of
    /// HEAD + fills. Suffix ranges take the serve-only path
    /// ([`Inner::first_fill_suffix`], #131).
    ///
    /// Singleflight (#19): the block-probe is deduplicated by
    /// `(object, block_index)` — pre-ETag, since the probe is what resolves the
    /// ETag. The suffix path deduplicates *bufferable* tails too (#149,
    /// [`Inner::first_fill_suffix`]); only over-cap tails, whose live body
    /// cannot be cloned to waiters, stay non-deduplicated.
    ///
    /// Ring note (#17/#29): like the HEAD-based resolution, this goes to the
    /// backend without a peer rung — an unmapped key has no ETag to ask a peer
    /// for. M2's peer metadata protocol (#29) slots in before this GET (ask the
    /// owner for the mapping, then run the block ladder); until then the
    /// backend GET is the degradation anchor.
    async fn first_fill(
        self: &Arc<Self>,
        key: &CacheKey,
        range: ReadRange,
        class: MetaClass,
    ) -> Result<FirstFill, ReadError> {
        // An unmapped partial data read must not wait for the rest of its
        // aligned cache block. Resolve the mapping from the exact origin range,
        // stream those bytes to the reader, then populate the aligned block
        // after the foreground body completes. Metadata reads retain the
        // singleflight probe because their small bodies are pinned as a unit.
        //
        // An *exactly block-aligned* request is excluded (#273): its exact
        // range IS the aligned block, so there is nothing left to populate in
        // the background. Routing it through the partial path would fetch the
        // same bytes twice (the exact GET plus the detached aligned re-fetch)
        // and admit the block from a detached task at an uncontrolled time.
        // That uncontrolled write is what broke the warm-then-flush discipline
        // this engine documents for tests and graceful shutdown: foyer's
        // bounded flush buffer holds only one block-sized entry per drain
        // window and silently drops the second, so a block whose admission a
        // flush had already waited out could still reach neither tier. The
        // probe below serves the same bytes with one GET and admits within the
        // awaited flight, so the read returns with the block already admitted
        // and its disk write already queued — which per-block `flush()` then
        // makes durable, deterministically.
        if class == MetaClass::Data
            && let ReadRange::Bounded(first, last) = range
            && first / self.block_bytes == last / self.block_bytes
            && !(first % self.block_bytes == 0 && last - first + 1 == self.block_bytes)
        {
            return self.first_partial_fill(key, range).await;
        }

        // The block the request starts in, computable without the object size
        // for everything but suffix ranges — those resolve the size from the
        // suffix GET itself (#131).
        let block_start = match range {
            ReadRange::Full => 0,
            ReadRange::From(first) | ReadRange::Bounded(first, _) => {
                (first / self.block_bytes) * self.block_bytes
            }
            // A suffix read (a Parquet footer) resolves the mapping from the
            // response's own 206 and serves the tail (one GET, no HEAD, #131).
            // Bufferable tails collapse concurrent footer reads into one GET and
            // pin metadata-classified footers (#149/#50); the covering *data*
            // block is never cached (block-granular pinning would cache the
            // whole block — see `get`'s `store_class`). `class` is preserved
            // here (Meta for a classified footer, Data otherwise).
            ReadRange::Suffix(len) => return self.first_fill_suffix(key, len, class).await,
        };
        let index = block_start / self.block_bytes;

        let outcome = self
            .run_flight(
                |inner| &inner.inflight.probes,
                |inner| &inner.counters.deduped_fills,
                (key.clone(), index),
                {
                    let key = key.clone();
                    move |inner| {
                        Box::pin(async move { inner.probe_fill(&key, block_start, class).await })
                    }
                },
            )
            .await?;
        Ok(match outcome {
            ProbeOutcome::Cacheable { meta, block } => FirstFill::Cacheable {
                meta,
                block: (index, block),
            },
            ProbeOutcome::NonCacheable => FirstFill::NonCacheable,
            ProbeOutcome::NeedHead => FirstFill::NeedHead,
        })
    }

    /// Resolves an unmapped partial read from the exact requested range, shares
    /// those bytes across concurrent readers, and only then schedules aligned
    /// cache population. This gives the foreground origin request strict
    /// priority while preserving the ETag fence, singleflight, admission
    /// policy, and bounded background-fill ceiling.
    async fn first_partial_fill(
        self: &Arc<Self>,
        key: &CacheKey,
        range: ReadRange,
    ) -> Result<FirstFill, ReadError> {
        let ReadRange::Bounded(first, last) = range else {
            return Ok(FirstFill::NeedHead);
        };
        let outcome = self
            .run_flight(
                |inner| &inner.inflight.partials,
                |inner| &inner.counters.deduped_fills,
                (key.clone(), first, last),
                {
                    let key = key.clone();
                    move |inner| {
                        Box::pin(async move { inner.probe_partial_fill(&key, range).await })
                    }
                },
            )
            .await?;
        let (meta, resolved, bytes) = match outcome {
            PartialOutcome::Cacheable { meta, range, bytes } => (meta, range, bytes),
            PartialOutcome::NonCacheable => return Ok(FirstFill::NonCacheable),
            PartialOutcome::NeedHead => return Ok(FirstFill::NeedHead),
        };

        Ok(FirstFill::Served {
            meta,
            range: resolved,
            body: stream::once(async move { Ok(bytes) }).boxed(),
        })
    }

    /// Fetches and buffers one exact single-block range for the partial
    /// first-fill singleflight, resolving and fence-admitting its mapping.
    async fn probe_partial_fill(
        self: &Arc<Self>,
        key: &CacheKey,
        range: ReadRange,
    ) -> Result<PartialOutcome, ReadError> {
        let fence_guard = self.fence.register(key);
        let got = match self.backend.get(key, range).await {
            Ok(got) => got,
            Err(e @ (ReadError::NoSuchKey | ReadError::NoSuchBucket)) => return Err(e),
            Err(error) => {
                log_fill_error(key, "partial_get", &error);
                return Ok(PartialOutcome::NeedHead);
            }
        };
        let Some(meta) = CachedMeta::from_object_meta(&got.meta) else {
            return Ok(PartialOutcome::NonCacheable);
        };
        let resolved = got.range.clone();
        let expected_len = (resolved.end - resolved.start) as usize;
        let Some(bytes) = collect_block(got.body, expected_len).await? else {
            return Ok(PartialOutcome::NeedHead);
        };
        if !self.admit_meta_if_fresh(&fence_guard, key, &meta) {
            return Ok(PartialOutcome::NeedHead);
        }
        if let Ok(permit) = Arc::clone(&self.background_fills).try_acquire_owned() {
            let inner = Arc::clone(self);
            let object = key.clone();
            let version = meta.clone();
            let index = resolved.start / self.block_bytes;
            let expected_len = block_len(meta.size, index, self.block_bytes);
            let logical = BlockKey {
                object: key.clone(),
                etag: meta.etag.clone(),
                block_bytes: self.block_bytes,
                block_index: index,
            };
            let block_key = self.block_entry_key(logical);
            let flight_key = block_key.block.clone();
            // Register the aligned fill against the flush barrier *before* this
            // partial flight returns and drops its own in-flight count. Spawning
            // `run_flight` after return left a window where `flush()` saw size==0
            // and a warm re-read refilled from origin under CPU starvation.
            match inner.start_flight(
                |inner| &inner.inflight.blocks,
                |inner| &inner.counters.deduped_fills,
                flight_key,
                move |inner| {
                    Box::pin(async move {
                        let _permit = permit;
                        inner
                            .fill_block(&object, &version, index, expected_len, block_key)
                            .await
                    })
                },
            ) {
                FlightStart::Shared(shared) => {
                    tokio::spawn(async move {
                        let _ = shared.await;
                    });
                }
                FlightStart::Bypass(fill) => {
                    // Cap bypass still has to be visible to flush: hold a manual
                    // in-flight slot around the detached work.
                    inner.inflight.size.fetch_add(1, Ordering::Relaxed);
                    let tracked = Arc::clone(&inner);
                    tokio::spawn(async move {
                        let _ = fill.await;
                        tracked.inflight.size.fetch_sub(1, Ordering::Release);
                        tracked.inflight.quiesced.notify_waiters();
                    });
                }
            }
        }
        Ok(PartialOutcome::Cacheable {
            meta,
            range: resolved,
            bytes,
        })
    }

    /// The mapping-resolving block-probe of [`Inner::first_fill`], factored out
    /// so the singleflight (#19) can run it once for concurrent identical cold
    /// reads. `&self` — invoked inside the detached fill task over `Arc<Inner>`,
    /// so its fence guard lives entirely within this call.
    async fn probe_fill(
        &self,
        key: &CacheKey,
        block_start: u64,
        class: MetaClass,
    ) -> Result<ProbeOutcome, ReadError> {
        // Fence discipline (#124): register before the probe goes out.
        let fence_guard = self.fence.register(key);
        let got = match self
            .backend
            .get(
                key,
                ReadRange::Bounded(block_start, block_start + self.block_bytes - 1),
            )
            .await
        {
            Ok(got) => got,
            // Missing key/bucket are definitive answers for the client.
            Err(e @ (ReadError::NoSuchKey | ReadError::NoSuchBucket)) => return Err(e),
            // Anything else (empty object, request start beyond EOF, origin
            // range quirks — the passthrough folds them all into `Backend`)
            // is disambiguated by the HEAD fallback with proper semantics.
            Err(error) => {
                log_fill_error(key, "block_get", &error);
                return Ok(ProbeOutcome::NeedHead);
            }
        };

        let Some(meta) = CachedMeta::from_object_meta(&got.meta) else {
            // Non-cacheable probe; the unread body is dropped. Deliberately
            // not counted as a fill — nothing was (or may be) admitted.
            return Ok(ProbeOutcome::NonCacheable);
        };
        // Counted once the response proves cacheable: this GET is a block
        // fill, admitted below exactly like `fill_block`'s.
        CacheCounters::bump(&self.counters.backend_fills);

        let index = block_start / self.block_bytes;
        let expected_len = block_len(meta.size, index, self.block_bytes);
        let block = match collect_block(got.body, expected_len).await? {
            Some(block) => block,
            // Response length disagrees with its own metadata; don't admit,
            // don't serve — the HEAD fallback re-resolves and the ladder's
            // verified fills take over (slow is acceptable, wrong is never).
            None => return Ok(ProbeOutcome::NeedHead),
        };

        CacheCounters::add(&self.counters.backend_fill_bytes, expected_len as u64);
        // Two gates, both fail-open to serving the bytes: the #124 fence (don't
        // reinstate a mapping a racing invalidation cleared) and the #15
        // scan-resistant admission (don't let a one-touch block evict the
        // working set). The mapping is admitted whenever the fence allows —
        // mappings bypass scan admission (tiny, disproportionately valuable) —
        // and the *block* is inserted only if it also clears admission. Either
        // way the probe's bytes ride out in `ProbeOutcome::Cacheable`.
        if self.admit_meta_if_fresh(&fence_guard, key, &meta) {
            let logical = BlockKey {
                object: key.clone(),
                etag: meta.etag.clone(),
                block_bytes: self.block_bytes,
                block_index: index,
            };
            match class {
                // Metadata is always admitted (bypasses scan admission, #15) and
                // pinned in its own store, hard-isolated from data pressure.
                MetaClass::Meta => self.insert_meta(self.meta_block_key(logical), block.clone()),
                MetaClass::Data => {
                    let block_key = self.block_entry_key(logical);
                    if self.admit_block(&block_key, block.len()) {
                        self.insert_block(block_key, block.clone());
                    }
                }
            }
        }
        Ok(ProbeOutcome::Cacheable { meta, block })
    }

    /// Resolves a cold suffix-range read's mapping from the suffix GET's own
    /// response and serves its bytes directly (#131) — dropping the extra HEAD
    /// the suffix path used to pay. S3 serves `bytes=-N` natively, and its
    /// `206` carries the object's total size (so block alignment becomes
    /// computable) plus the ETag/Last-Modified/Content-Type the HEAD fetched.
    ///
    /// Tail-block decision (issue #131 option 1, "serve-only"): the returned
    /// suffix bytes almost never align to a block boundary, so only the
    /// *mapping* is admitted — the unaligned tail is served to the client and
    /// then dropped, never cached. The next read of that block refetches it
    /// aligned through the ladder. This removes the round trip that dominates
    /// cold footer p95 without teaching the block store about odd-shaped
    /// entries; caching the containing tail block in the background (option 2)
    /// is a measured follow-up, not v1.
    ///
    /// Fence discipline (#124): the fence guard is registered before the
    /// suffix GET, and admission is serve-without-admit on rejection — a
    /// request that overlapped a write serves the version it resolved but
    /// memoizes nothing, so the next read re-resolves.
    ///
    /// Safety net: any response that does not cleanly resolve to a tail range
    /// (a range-ignored `200`, a `Content-Range` that does not end at EOF, a
    /// degenerate `bytes=-0`, an empty object) falls back to [`FirstFill::NeedHead`],
    /// so the existing HEAD path stays the backstop for malformed backends.
    ///
    /// Deduplicated (#149) for bufferable tails: a `Suffix(len)` with
    /// `len <= cache.data_block_bytes` runs through the singleflight `suffixes` map,
    /// so N concurrent cold footer reads of one object collapse to one backend
    /// GET and every waiter streams from the one shared buffer — the footer
    /// stampede #19 could not collapse while the served value was a live,
    /// unclonable body stream. Over-cap suffixes keep the pre-#149 serve-only,
    /// non-deduplicated path (their body could be whole-object-sized and cannot
    /// be buffered within budget); the block/HEAD fill sites they share still
    /// dedup.
    async fn first_fill_suffix(
        self: &Arc<Self>,
        key: &CacheKey,
        len: u64,
        class: MetaClass,
    ) -> Result<FirstFill, ReadError> {
        // `bytes=-0` is unsatisfiable; let the HEAD path produce the correct
        // InvalidRange rather than issuing a pointless suffix GET.
        if len == 0 {
            return Ok(FirstFill::NeedHead);
        }

        // Over-cap tails cannot be buffered within budget: keep the #131
        // serve-only stream, undeduplicated (its live body is not clonable).
        if len > suffix_buffer_cap(self.block_bytes) {
            return self.first_fill_suffix_streaming(key, len).await;
        }

        // Bufferable tail: collapse the stampede. The winning fill drains the
        // body into `Bytes` (cloneable) so every deduplicated waiter gets its
        // own view of one backend GET's footer. Keyed by `(object, len)` —
        // pre-ETag, since the GET is what resolves the ETag (mirroring the
        // block probe), and per length because different footer lengths are
        // different tails.
        let outcome = self
            .run_flight(
                |inner| &inner.inflight.suffixes,
                |inner| &inner.counters.deduped_suffix_fills,
                (key.clone(), len),
                {
                    let key = key.clone();
                    move |inner| {
                        Box::pin(async move { inner.suffix_fill_buffered(&key, len, class).await })
                    }
                },
            )
            .await?;
        Ok(match outcome {
            // Each waiter reconstructs its own one-shot stream over the shared
            // buffer — the bytes are refcounted, so this clone is cheap.
            SuffixOutcome::Buffered { meta, range, bytes } => FirstFill::Served {
                meta,
                range,
                body: stream::once(async move { Ok(bytes) }).boxed(),
            },
            SuffixOutcome::NonCacheable => FirstFill::NonCacheable,
            SuffixOutcome::NeedHead => FirstFill::NeedHead,
        })
    }

    /// The buffered suffix fill (#149), factored out so the singleflight can run
    /// it once for concurrent identical footer reads. Resolves the mapping from
    /// the suffix GET's own `206` (#131), admits it if fresh (#124), drains the
    /// tail into `Bytes`, and — for a metadata-classified, pinnable-sized tail —
    /// pins it at suffix granularity in the metadata store (#50). `&self`:
    /// invoked inside the detached fill task over `Arc<Inner>`, so its fence
    /// guard lives entirely within this call.
    ///
    /// Fence discipline (#124): the guard is registered before the GET, and
    /// admission is serve-without-admit on rejection — a request that
    /// overlapped a write serves the version it resolved but memoizes nothing.
    ///
    /// Safety net: any response that does not cleanly resolve to a tail (a
    /// range-ignored `200`, a `Content-Range` that does not match, a short/long
    /// body) yields [`SuffixOutcome::NeedHead`], so the HEAD + block ladder
    /// stays the backstop for malformed backends — slow is acceptable, wrong is
    /// never (the whole flight falls back together rather than serving bytes
    /// that disagree with the resolved range to N waiters).
    async fn suffix_fill_buffered(
        &self,
        key: &CacheKey,
        len: u64,
        class: MetaClass,
    ) -> Result<SuffixOutcome, ReadError> {
        // Fence discipline (#124): register before the suffix GET goes out.
        let fence_guard = self.fence.register(key);
        let got = match self.backend.get(key, ReadRange::Suffix(len)).await {
            Ok(got) => got,
            // Missing key/bucket are definitive answers for the client.
            Err(e @ (ReadError::NoSuchKey | ReadError::NoSuchBucket)) => return Err(e),
            // Anything else (a range the origin rejected, an empty object) is
            // disambiguated by the HEAD fallback with proper semantics.
            Err(error) => {
                log_fill_error(key, "suffix_get", &error);
                return Ok(SuffixOutcome::NeedHead);
            }
        };

        let Some(meta) = CachedMeta::from_object_meta(&got.meta) else {
            // Non-cacheable object; the unread body is dropped.
            return Ok(SuffixOutcome::NonCacheable);
        };

        // The response's own `Content-Range` (surfaced as `got.range` over the
        // full size in `meta.size`) must resolve to the tail the suffix names.
        // If the origin ignored the range or reported a mismatch, fall back to
        // the HEAD + block ladder, which re-fetches with correct semantics.
        let expected = match resolve_range(ReadRange::Suffix(len), meta.size) {
            Ok(range) => range,
            Err(_) => return Ok(SuffixOutcome::NeedHead),
        };
        if got.range != expected {
            return Ok(SuffixOutcome::NeedHead);
        }

        // Drain the tail into `Bytes` before admitting anything: this is what
        // makes the value cloneable to every waiter. Bounded by
        // configured suffix buffer cap (the caller gated `len`), so the allocation is a
        // footer, not an object.
        let tail_len = expected.end - expected.start;
        let mut buf = bytes::BytesMut::with_capacity(tail_len as usize);
        let mut body = got.body;
        while let Some(chunk) = body.try_next().await? {
            buf.extend_from_slice(&chunk);
        }
        let bytes = buf.freeze();
        // A short/long body means the origin lied about its own range: don't
        // serve or pin those bytes to N waiters — fall back to the HEAD ladder.
        if bytes.len() as u64 != tail_len {
            return Ok(SuffixOutcome::NeedHead);
        }

        // Admission policy (#15/#124): admit the mapping only (serve-without-
        // admit on a racing invalidation); never the unaligned tail block in
        // the DATA store.
        self.admit_meta_if_fresh(&fence_guard, key, &meta);

        // Footer pinning (#50): a metadata-classified, pinnable-sized tail is
        // also pinned at suffix granularity in the metadata store — keyed by
        // ETag, so a racing invalidation only strands an unreachable
        // old-version entry, never serves it. Larger (still bufferable) tails
        // are deduped but not pinned; over-cap tails never reach here.
        if class == MetaClass::Meta && tail_len <= META_SUFFIX_PIN_MAX_BYTES {
            self.insert_meta(
                self.meta_suffix_key(key.clone(), meta.etag.clone(), len),
                bytes.clone(),
            );
        }
        Ok(SuffixOutcome::Buffered {
            meta,
            range: expected,
            bytes,
        })
    }

    /// The over-cap suffix fast path (#131): resolve the mapping from the suffix
    /// GET's own `206` and stream its bytes straight through, admitting only the
    /// *mapping* (serve-only — the unaligned tail block is never cached). Used
    /// for tails above the configured suffix buffer cap, whose bodies could be
    /// whole-object-sized and so cannot be buffered for singleflight sharing;
    /// these therefore stay non-deduplicated (each concurrent reader issues its
    /// own GET), exactly the pre-#149 behavior. `&self`: no fill task here (the
    /// stream must outlive this call), so the fence guard is held only across
    /// the GET and admission.
    ///
    /// Fence discipline (#124) and the malformed-response safety net match
    /// [`Inner::suffix_fill_buffered`]; the only difference is that the body is
    /// streamed rather than drained.
    async fn first_fill_suffix_streaming(
        &self,
        key: &CacheKey,
        len: u64,
    ) -> Result<FirstFill, ReadError> {
        // Fence discipline (#124): register before the suffix GET goes out.
        let fence_guard = self.fence.register(key);
        let got = match self.backend.get(key, ReadRange::Suffix(len)).await {
            Ok(got) => got,
            Err(e @ (ReadError::NoSuchKey | ReadError::NoSuchBucket)) => return Err(e),
            Err(error) => {
                log_fill_error(key, "suffix_get", &error);
                return Ok(FirstFill::NeedHead);
            }
        };

        let Some(meta) = CachedMeta::from_object_meta(&got.meta) else {
            return Ok(FirstFill::NonCacheable);
        };

        let expected = match resolve_range(ReadRange::Suffix(len), meta.size) {
            Ok(range) => range,
            Err(_) => return Ok(FirstFill::NeedHead),
        };
        if got.range != expected {
            return Ok(FirstFill::NeedHead);
        }

        // Admit the mapping only (serve-without-admit on a racing invalidation);
        // the oversized tail is streamed to the client and then dropped.
        self.admit_meta_if_fresh(&fence_guard, key, &meta);
        Ok(FirstFill::Served {
            meta,
            range: expected,
            body: got.body,
        })
    }

    /// Admits (or replaces) the key→ETag mapping — but only if no
    /// invalidation of `key` ran since `guard` was registered; returns
    /// whether it admitted. The epoch check and the insert happen under one
    /// fence-lock acquisition, which is what makes admission atomic against
    /// [`Inner::invalidate_key`] (#124).
    ///
    /// The mapping never touches disk — the metadata cache has no codec and
    /// no disk tier, which is what guarantees a restart cannot recover a
    /// stale mapping (block entries are immutable, so recovering *them* is
    /// always safe — see module comment).
    fn admit_meta_if_fresh(
        &self,
        guard: &FenceGuard<'_>,
        key: &CacheKey,
        meta: &CachedMeta,
    ) -> bool {
        let slots = self.fence.lock_slots();
        // The guard keeps the slot alive (readers ≥ 1), so it is present.
        let fresh = slots.get(key).is_some_and(|slot| slot.epoch == guard.epoch);
        if fresh {
            // Charge the mapping's DRAM weight to the live gauge (#178), matching
            // the meta cache's weighter, so `dram_live_bytes` tracks admitted
            // mappings rather than foyer's post-clear-stale `usage()`.
            let weight = (meta.weight(key) + ENTRY_OVERHEAD_BYTES) as u64;
            self.live_mapping_bytes.fetch_add(weight, Ordering::Relaxed);
            // Classify the mapping's mutability (#14) and stamp its refresh
            // instant now, so an immutable key is pinned indefinitely and a
            // mutable one starts a fresh TTL window from this admission.
            self.meta.insert(
                key.clone(),
                MappingEntry {
                    meta: meta.clone(),
                    mutability: classify_mutability(&key.key),
                    refreshed_at: self.clock.now(),
                },
            );
        }
        fresh
    }

    /// Invalidates one key: bumps its fence epoch (fencing off every
    /// in-flight metadata resolution that started earlier) and removes its
    /// mapping, atomically with respect to [`Inner::admit_meta_if_fresh`]
    /// (both run under the fence lock). This is `admit`'s dual — the
    /// ordering in #21 relies on it.
    fn invalidate_key(&self, key: &CacheKey) {
        let mut slots = self.fence.lock_slots();
        if let Some(slot) = slots.get_mut(key) {
            // No slot = no in-flight resolution = nothing to fence.
            slot.epoch += 1;
        }
        let _ = self.meta.remove(key);
    }

    /// Whether the warm-from-peers window is currently open (#30): the stored
    /// deadline is in the future. `0` (never armed, or a single-node server) is
    /// always closed. Relaxed — an approximate gate, never a correctness anchor
    /// (a stale read only costs, or saves, one peer hop).
    fn is_warming(&self) -> bool {
        let deadline = self.warming_until_ms.load(Ordering::Relaxed);
        deadline != 0 && now_unix_millis() < deadline
    }

    /// The miss ladder for one block: local DRAM → local disk → peer fetch →
    /// backend fill. Returns the whole block and the tier that produced it.
    ///
    /// Cache-internal failures degrade down the ladder instead of failing
    /// the read (slow is acceptable; wrong is never) — the backend fill at
    /// the bottom is the correctness anchor.
    async fn read_block(
        self: &Arc<Self>,
        key: &CacheKey,
        owner: &NodeId,
        meta: &CachedMeta,
        index: u64,
        class: MetaClass,
        client_slice: Range<usize>,
    ) -> Result<BlockServe, ReadError> {
        let expected_len = block_len(meta.size, index, self.block_bytes);
        let block_key = self.block_entry_key(BlockKey {
            object: key.clone(),
            etag: meta.etag.clone(),
            block_bytes: self.block_bytes,
            block_index: index,
        });

        // Metadata reads (#50) live in the pinned store, not the data ladder: a
        // DRAM-front (then NVMe-tail) lookup, then a backend fill that is
        // always admitted. No peer rung — peer metadata serving is #29's
        // domain.
        if let MetaClass::Meta = class {
            let (block, tier) = self
                .read_meta_block(key, meta, index, expected_len, block_key)
                .await?;
            return Ok(BlockServe::Buffered(block, tier));
        }

        // Rung 1: local DRAM. Probed directly (rather than through the
        // hybrid getter) so DRAM and disk hits are separately observable.
        if let Some(block) = block_payload(self.blocks.memory().get(&block_key), expected_len) {
            self.admission.record_hit(&block_key);
            CacheCounters::bump(&self.counters.dram_hits);
            return Ok(BlockServe::Buffered(block, Tier::Dram));
        }
        CacheCounters::bump(&self.counters.dram_misses);

        // Rung 2: local disk. A hit is also re-admitted to DRAM by foyer.
        // (A concurrent insert between rung 1 and here can surface a DRAM
        // hit — attributed to the disk tier, an acceptable approximation.)
        match self.blocks.get(&block_key).await {
            Ok(entry) => {
                if let Some(block) = block_payload(entry, expected_len) {
                    self.admission.record_hit(&block_key);
                    CacheCounters::bump(&self.counters.disk_hits);
                    return Ok(BlockServe::Buffered(block, Tier::Disk));
                }
                CacheCounters::bump(&self.counters.disk_misses);
            }
            // A failing disk tier must not fail the read; fall through.
            Err(_) => CacheCounters::bump(&self.counters.disk_misses),
        }

        // Rung 3: peer fetch. One transfer, two shapes, decided by who owns the
        // key and whether ownership recently moved:
        //
        // - **Not the owner (#29/#17):** ask the owner before the backend, so
        //   only the owner fills (pod-wide singleflight).
        // - **The owner, but ownership only just moved here (#30/#31):** the
        //   block's previous holder may still have it hot, so warm from it
        //   before a *cold* backend fill. [`Ring::warm_donor`] names that holder
        //   from the rendezvous ranking. A **draining** donor (it is leaving and
        //   holds the hot copy of a key this node just inherited) is pulled
        //   unconditionally — that is what makes a graceful drain (#31) shed
        //   warmth, not cold-start it. A healthy predecessor (#30) is pulled
        //   only while this node is still warming from a join, so a settled pod
        //   never pays an extra hop on a genuinely cold miss (the predecessor
        //   would miss too).
        //
        // The whole `BlockKey` (ETag included) is the request, so a peer serves
        // this exact version or misses — never a different one. Peer errors and
        // misses degrade to the backend fill below (slow is acceptable; wrong is
        // never), so a dead or stale donor only costs a re-fetch.
        let peer_target = if *owner != self.node {
            Some(owner.clone())
        } else {
            self.ring
                .warm_donor(key, &self.node)
                .and_then(|donor| (donor.draining || self.is_warming()).then_some(donor.node))
        };
        if let Some(target) = peer_target {
            match self.peers.fetch(target, &block_key.block).await {
                Ok(Some(block)) if block.len() == expected_len => {
                    CacheCounters::bump(&self.counters.peer_hits);
                    // Replica policy (#29, WHITEPAPER §3.3): the fetching node
                    // keeps a DRAM replica of a peer-fetched block so hot keys
                    // stop paying the peer hop — subject to scan-resistant
                    // admission (#15). For a warmed-from-donor owned block this
                    // replica *is* the migration: the owner now holds it. Peer
                    // replicas never satisfy peer-fetch requests (owners stay the
                    // single intra-pod source); `local_block` enforces that.
                    if self.admit_block(&block_key, block.len()) {
                        self.insert_block(block_key, block.clone());
                    }
                    return Ok(BlockServe::Buffered(block, Tier::Peer));
                }
                // A clean miss (the peer lacks this exact block, including a
                // wrong-length payload) falls through to a local backend fill.
                Ok(_) => CacheCounters::bump(&self.counters.peer_misses),
                // A transport failure degrades to a backend fill too — slow is
                // acceptable, wrong is never; the read never errors to here.
                Err(_) => CacheCounters::bump(&self.counters.peer_errors),
            }
        }

        // Rung 4a: attach to a first-fill probe still in flight for this block
        // (#214). A reader that resolved the mapping from a *prior* reader's
        // probe can reach here before that probe's block is visible in the store;
        // joining the probe collapses the two paths to one backend GET instead of
        // issuing a redundant one.
        if let Some(block) = self.attach_to_probe_fill(key, meta, index).await {
            return Ok(BlockServe::Buffered(block, Tier::Backend));
        }

        // Rung 4b: backend fill, deduplicated by the full `BlockKey` (#19) —
        // N concurrent cold reads of one version's one block become one
        // backend GET. Keyed by the block's identity (which carries the object
        // identity ring ownership routes on), never by "local".
        self.stream_or_join_block_fill(key, meta, index, expected_len, block_key, client_slice)
            .await
    }

    /// Attaches a covering-block read to a first-fill probe that is still in
    /// flight for the same `(object, index)`, if one exists (#214).
    ///
    /// The bug this closes: a cold read resolves the mapping through the
    /// first-fill probe (in-flight map keyed by `(object, index)`, pre-ETag).
    /// Once the mapping is admitted, a second reader sees it and takes the
    /// block-fill path (in-flight map keyed by the full [`BlockKey`], with ETag).
    /// If it arrives after the mapping is admitted but before the probe's block
    /// is visible in the block store, the two in-flight maps cannot see each
    /// other, so it issues its own backend GET — the same block fetched twice.
    ///
    /// This collapses the two paths: the block fill consults the probe map for
    /// `(object, index)` first. A live probe fetches exactly the block this read
    /// needs, so the reader awaits that one shared fill instead of starting its
    /// own GET. `Some(block)` means the probe resolved a cacheable block whose
    /// ETag matches the resolved mapping — the reader serves those bytes. `None`
    /// means there is no usable probe to join (none in flight, or it resolved to
    /// a non-cacheable / need-HEAD / different-version outcome); the caller runs
    /// its normal block fill. A probe error is a clean miss too, not this
    /// reader's error — the block fill re-fetches, so a poisoned probe never
    /// fails a read that could succeed on its own.
    ///
    /// Correctness: the block is served only if the probe's ETag matches the
    /// mapping this read resolved, so bytes can never cross versions. The
    /// admission the probe already performed stands; this reader adds none, so
    /// it neither double-charges nor bypasses the #15 gate the probe honored.
    async fn attach_to_probe_fill(
        self: &Arc<Self>,
        key: &CacheKey,
        meta: &CachedMeta,
        index: u64,
    ) -> Option<Bytes> {
        // Peek the probe map under its lock, clone the shared fill, then await
        // outside the lock — the map guard is never held across an await.
        let shared = {
            let probes = self
                .inflight
                .probes
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            probes.get(&(key.clone(), index)).cloned()
        }?;
        // Joining an in-flight probe is a collapse, same as a block-fill join.
        CacheCounters::bump(&self.counters.deduped_fills);
        match shared.await {
            Ok(ProbeOutcome::Cacheable {
                meta: probe_meta,
                block,
            }) if probe_meta.etag == meta.etag => Some(block),
            // No usable block to share: a different version, a non-cacheable or
            // need-HEAD probe, or a probe error. Fall back to the block fill.
            _ => None,
        }
    }

    /// Starts or joins a backend block fill while giving the winning reader a
    /// live origin stream. The detached producer owns completion, validation,
    /// admission, and singleflight cleanup, so client cancellation cannot
    /// strand the fill and cache work never sits in front of an available
    /// response chunk.
    #[allow(clippy::too_many_arguments)]
    async fn stream_or_join_block_fill(
        self: &Arc<Self>,
        key: &CacheKey,
        meta: &CachedMeta,
        index: u64,
        expected_len: usize,
        block_key: BlockEntryKey,
        client_slice: Range<usize>,
    ) -> Result<BlockServe, ReadError> {
        let flight_key = block_key.block.clone();
        let partial = client_slice.start != 0 || client_slice.end != expected_len;
        let start = {
            let mut guard = self
                .inflight
                .blocks
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(existing) = guard.get(&flight_key) {
                if partial {
                    StreamingFillStart::Direct
                } else {
                    CacheCounters::bump(&self.counters.deduped_fills);
                    StreamingFillStart::Join(existing.clone())
                }
            } else if let Some(block) =
                block_payload(self.blocks.memory().get(&block_key), expected_len)
            {
                // DRAM re-probe under the map lock (the #273 race family): a
                // fill that completed between this reader's tier probes and
                // this map check inserted its block before removing its entry,
                // so re-probing here is conclusive and the reader serves the
                // resident block instead of re-filling it. Cold path only —
                // a warm read never reaches this rung.
                StreamingFillStart::Hit(block)
            } else if self.inflight.size.load(Ordering::Relaxed) >= MAX_INFLIGHT {
                if partial {
                    StreamingFillStart::Direct
                } else {
                    StreamingFillStart::Bypass
                }
            } else if partial && !self.admission.repeated(&block_key) {
                StreamingFillStart::Direct
            } else {
                let background_slot = if partial {
                    Arc::clone(&self.background_fills).try_acquire_owned().ok()
                } else {
                    None
                };
                if partial && background_slot.is_none() {
                    StreamingFillStart::Direct
                } else {
                    // Two origin chunks of response slack is a hard per-fill
                    // ceiling. The cache branch is only an in-task buffer copy;
                    // admission and NVMe persistence never occupy this channel.
                    let (client, receiver) = tokio::sync::mpsc::channel(2);
                    let (completion, completed) = tokio::sync::oneshot::channel();
                    let (started, start_waiter) = tokio::sync::oneshot::channel();
                    let shared = async move {
                        completed.await.unwrap_or_else(|_| {
                            Err(Arc::new(ReadError::Backend(
                                "streaming fill task ended before completion".into(),
                            )))
                        })
                    }
                    .boxed()
                    .shared();
                    guard.insert(flight_key, shared);
                    self.inflight.size.fetch_add(1, Ordering::Relaxed);
                    StreamingFillStart::Lead {
                        receiver,
                        client,
                        started,
                        start_waiter,
                        completion,
                        background_slot,
                    }
                }
            }
        };

        match start {
            StreamingFillStart::Join(shared) => shared
                .await
                .map(|block| BlockServe::Buffered(block, Tier::Backend))
                .map_err(|error| clone_read_error(&error)),
            StreamingFillStart::Lead {
                receiver,
                client,
                started,
                start_waiter,
                completion,
                background_slot,
            } => {
                let inner = Arc::clone(self);
                let object = key.clone();
                let version = meta.clone();
                let cleanup_key = block_key.block.clone();
                tokio::spawn(async move {
                    let _background_slot = background_slot;
                    let result = inner
                        .fill_block_streaming(
                            &object,
                            &version,
                            index,
                            expected_len,
                            block_key,
                            client_slice,
                            client,
                            started,
                        )
                        .await;
                    inner
                        .inflight
                        .blocks
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(&cleanup_key);
                    // Release so the flush barrier's Acquire load, once it sees
                    // this decrement, also sees the admission this task just did.
                    inner.inflight.size.fetch_sub(1, Ordering::Release);
                    inner.inflight.quiesced.notify_waiters();
                    let _ = completion.send(result.map_err(Arc::new));
                });
                let _ = start_waiter.await;
                let body = stream::unfold(receiver, |mut receiver| async move {
                    receiver.recv().await.map(|item| (item, receiver))
                })
                .boxed();
                Ok(BlockServe::Streaming(body, Tier::Backend))
            }
            StreamingFillStart::Direct => {
                self.direct_block_slice(key, meta, index, client_slice)
                    .await
            }
            StreamingFillStart::Bypass => self
                .fill_block(key, meta, index, expected_len, block_key)
                .await
                .map(|block| BlockServe::Buffered(block, Tier::Backend)),
            // The block became DRAM-resident after this reader's tier probes;
            // serve it as the DRAM hit it is. No fill, no admission (the fill
            // that produced it already admitted it once).
            StreamingFillStart::Hit(block) => {
                CacheCounters::bump(&self.counters.dram_hits);
                Ok(BlockServe::Buffered(block, Tier::Dram))
            }
        }
    }

    /// Serves only the requested block intersection from the backend without
    /// admission. This keeps organic reads moving when all background aligned
    /// fill slots are occupied; the turn-off behavior remains a plain origin
    /// range read and no cache work consumes another backend permit.
    async fn direct_block_slice(
        &self,
        key: &CacheKey,
        meta: &CachedMeta,
        index: u64,
        client_slice: Range<usize>,
    ) -> Result<BlockServe, ReadError> {
        let block_start = index * self.block_bytes;
        let start = block_start + client_slice.start as u64;
        let end = block_start + client_slice.end as u64 - 1;
        let got = self
            .backend
            .get(key, ReadRange::Bounded(start, end))
            .await?;
        if got.meta.e_tag.as_deref() != Some(meta.etag.as_str()) {
            self.meta.remove(key);
            return Err(ReadError::Backend(format!(
                "object {}/{} changed during read (expected etag {}, origin now serves {})",
                key.bucket,
                key.key,
                meta.etag,
                got.meta.e_tag.as_deref().unwrap_or("<none>"),
            )));
        }
        Ok(BlockServe::Streaming(got.body, Tier::Backend))
    }

    /// Reads one aligned origin block into a bounded accumulator while
    /// forwarding the requested intersection to the leader as chunks arrive.
    /// Once that intersection has been sent, the response channel closes and
    /// the remaining aligned overfetch completes in the background. Only a
    /// complete, ETag-matching block is admitted.
    #[allow(clippy::too_many_arguments)]
    async fn fill_block_streaming(
        &self,
        key: &CacheKey,
        meta: &CachedMeta,
        index: u64,
        expected_len: usize,
        block_key: BlockEntryKey,
        client_slice: Range<usize>,
        client: tokio::sync::mpsc::Sender<Result<Bytes, ReadError>>,
        started: tokio::sync::oneshot::Sender<()>,
    ) -> Result<Bytes, ReadError> {
        let start = index * self.block_bytes;
        CacheCounters::bump(&self.counters.backend_fills);
        let got = self
            .backend
            .get(
                key,
                ReadRange::Bounded(start, start + expected_len as u64 - 1),
            )
            .await;
        let _ = started.send(());
        let got = match got {
            Ok(got) => got,
            Err(error) => {
                let _ = client.send(Err(clone_read_error(&error))).await;
                return Err(error);
            }
        };
        if got.meta.e_tag.as_deref() != Some(meta.etag.as_str()) {
            self.meta.remove(key);
            let error = ReadError::Backend(format!(
                "object {}/{} changed during read (expected etag {}, origin now serves {})",
                key.bucket,
                key.key,
                meta.etag,
                got.meta.e_tag.as_deref().unwrap_or("<none>"),
            ));
            let _ = client.send(Err(clone_read_error(&error))).await;
            return Err(error);
        }

        let mut origin = got.body;
        let mut block = BytesMut::with_capacity(expected_len);
        let mut client = Some(client);
        while let Some(chunk) = match origin.try_next().await {
            Ok(chunk) => chunk,
            Err(error) => {
                if let Some(sender) = client.take() {
                    let _ = sender.send(Err(clone_read_error(&error))).await;
                }
                return Err(error);
            }
        } {
            let offset = block.len();
            if offset + chunk.len() > expected_len {
                let error = ReadError::Backend(format!(
                    "origin returned an over-long body for block {index} of {}/{}",
                    key.bucket, key.key,
                ));
                if let Some(sender) = client.take() {
                    let _ = sender.send(Err(clone_read_error(&error))).await;
                }
                return Err(error);
            }
            block.extend_from_slice(&chunk);
            let chunk_end = offset + chunk.len();
            let from = offset.max(client_slice.start);
            let to = chunk_end.min(client_slice.end);
            if from < to
                && let Some(sender) = client.as_ref()
                && sender
                    .send(Ok(chunk.slice(from - offset..to - offset)))
                    .await
                    .is_err()
            {
                client = None;
            }
            if chunk_end >= client_slice.end {
                client = None;
            }
        }
        if block.len() != expected_len {
            let error = ReadError::Backend(format!(
                "origin returned a wrong-length body for block {index} of {}/{} (expected {expected_len} bytes)",
                key.bucket, key.key,
            ));
            if let Some(sender) = client.take() {
                let _ = sender.send(Err(clone_read_error(&error))).await;
            }
            return Err(error);
        }

        let block = block.freeze();
        CacheCounters::add(&self.counters.backend_fill_bytes, expected_len as u64);
        if self.admit_block(&block_key, block.len()) {
            self.insert_block(block_key, block.clone());
        }
        Ok(block)
    }

    /// Fills one block from the backend, verifies it belongs to the resolved
    /// ETag, admits it, and returns it. `&self` — invoked inside the detached
    /// singleflight fill task ([`Inner::run_flight`], the rung-4 call site),
    /// which is what collapses N concurrent cold reads of one block into one
    /// backend GET.
    async fn fill_block(
        &self,
        key: &CacheKey,
        meta: &CachedMeta,
        index: u64,
        expected_len: usize,
        block_key: BlockEntryKey,
    ) -> Result<Bytes, ReadError> {
        let start = index * self.block_bytes;
        CacheCounters::bump(&self.counters.backend_fills);
        let got = self
            .backend
            .get(
                key,
                ReadRange::Bounded(start, start + expected_len as u64 - 1),
            )
            .await?;

        // ETag discipline: the fill's bytes belong to whatever version the
        // origin served *now*; if that is not the version this request
        // promised (`meta.etag`), serving them would mix versions. A
        // mismatch means our mapping is stale, so drop it and fail this
        // request — the client's retry re-resolves and reads the new
        // version coherently. We deliberately do NOT re-admit a fresh
        // mapping from this data-path response (#124): a fill GET is not
        // fence-registered, so admitting here could reinstate a mapping a
        // concurrent invalidation just cleared.
        if got.meta.e_tag.as_deref() != Some(meta.etag.as_str()) {
            self.meta.remove(key);
            return Err(ReadError::Backend(format!(
                "object {}/{} changed during read (expected etag {}, origin now serves {})",
                key.bucket,
                key.key,
                meta.etag,
                got.meta.e_tag.as_deref().unwrap_or("<none>"),
            )));
        }

        let Some(block) = collect_block(got.body, expected_len).await? else {
            return Err(ReadError::Backend(format!(
                "origin returned a wrong-length body for block {index} of {}/{} \
                 (expected {expected_len} bytes)",
                key.bucket, key.key,
            )));
        };
        CacheCounters::add(&self.counters.backend_fill_bytes, expected_len as u64);

        // Scan-resistant admission (#15): a one-touch scan block is served to
        // the client but not cached, so it cannot evict the working set. The
        // singleflight (#19) already collapsed concurrent identical fills, so
        // the policy sees each admitted block exactly once here.
        if self.admit_block(&block_key, block.len()) {
            self.insert_block(block_key, block.clone());
        }
        Ok(block)
    }

    /// The metadata-store read path (#50) for one block: a DRAM lookup in the
    /// pinned store, then — on a miss — a backend fill that is *always* admitted
    /// (metadata bypasses scan admission) and pinned. Deduplicated by the full
    /// [`BlockKey`] like the data ladder's rung 4, so concurrent cold planning
    /// reads of one metadata block collapse to one backend GET. No disk or peer
    /// rung: the store is DRAM-only in this PR and peer serving is #29's domain.
    async fn read_meta_block(
        self: &Arc<Self>,
        key: &CacheKey,
        meta: &CachedMeta,
        index: u64,
        expected_len: usize,
        block_key: BlockEntryKey,
    ) -> Result<(Bytes, Tier), ReadError> {
        if let Some(block) = self
            .meta_store
            .get(&self.meta_block_key(block_key.block.clone()))
            .await
            && block.len() == expected_len
        {
            CacheCounters::bump(&self.counters.meta_hits);
            return Ok((block, Tier::Meta));
        }
        CacheCounters::bump(&self.counters.meta_misses);
        // Attach to a first-fill probe still in flight for this metadata block
        // (#214): the probe path handles `MetaClass::Meta` too, so a reader that
        // resolved the mapping from a prior probe can reach here before the
        // probe's block is pinned. Joining it collapses to one backend GET.
        if let Some(block) = self.attach_to_probe_fill(key, meta, index).await {
            return Ok((block, Tier::Meta));
        }
        let block = self
            .run_flight(
                |inner| &inner.inflight.blocks,
                |inner| &inner.counters.deduped_fills,
                block_key.block.clone(),
                {
                    let key = key.clone();
                    let meta = meta.clone();
                    move |inner| {
                        Box::pin(async move {
                            inner
                                .fill_meta_block(&key, &meta, index, expected_len, block_key)
                                .await
                        })
                    }
                },
            )
            .await?;
        Ok((block, Tier::Meta))
    }

    /// Fills one metadata block from the backend, verifies it belongs to the
    /// resolved ETag (same discipline as [`Inner::fill_block`]), and pins it in
    /// the metadata store. Always admitted — metadata is tiny and
    /// disproportionately valuable (#15) — so there is no admission gate.
    async fn fill_meta_block(
        &self,
        key: &CacheKey,
        meta: &CachedMeta,
        index: u64,
        expected_len: usize,
        block_key: BlockEntryKey,
    ) -> Result<Bytes, ReadError> {
        let start = index * self.block_bytes;
        CacheCounters::bump(&self.counters.meta_fills);
        let got = self
            .backend
            .get(
                key,
                ReadRange::Bounded(start, start + expected_len as u64 - 1),
            )
            .await?;

        // ETag discipline: a mismatch means our mapping is stale; drop it and
        // fail this request so the retry re-resolves (slow, never wrong). We do
        // not re-admit a mapping from this fill response (#124): it is not
        // fence-registered.
        if got.meta.e_tag.as_deref() != Some(meta.etag.as_str()) {
            self.meta.remove(key);
            return Err(ReadError::Backend(format!(
                "metadata object {}/{} changed during read (expected etag {}, origin now serves {})",
                key.bucket,
                key.key,
                meta.etag,
                got.meta.e_tag.as_deref().unwrap_or("<none>"),
            )));
        }

        let Some(block) = collect_block(got.body, expected_len).await? else {
            return Err(ReadError::Backend(format!(
                "origin returned a wrong-length body for metadata block {index} of {}/{} \
                 (expected {expected_len} bytes)",
                key.bucket, key.key,
            )));
        };
        self.insert_meta(self.meta_block_key(block_key.block.clone()), block.clone());
        Ok(block)
    }

    /// The engine's current cache generation (issue #178). Every lookup and
    /// insert stamps this into its key; a purge bumps it. Acquire pairs with
    /// the Release bump in [`HybridCacheEngine::purge`], so a read that begins
    /// after a purge returns resolves at the new generation.
    fn generation(&self) -> u64 {
        self.purge_generation.load(Ordering::Acquire)
    }

    /// Wraps a logical [`BlockKey`] in the current-generation [`BlockEntryKey`]
    /// — the identity every block lookup and insert uses. A purge bumps the
    /// generation, so keys built before and after a purge never collide, which
    /// is what makes a purged entry an unreachable miss by construction.
    ///
    /// Retirement (#198) reuses the same mechanism per-object: a demoted object's
    /// key resolves at a **poisoned generation** ([`DEMOTED_GENERATION_BIT`]) no
    /// live entry was written under, so its cached blocks are unreachable misses
    /// that LRU ages out before live data. The demotion set is only consulted
    /// when something is demoted ([`Demotions::any`]) — the warm hot path pays
    /// one relaxed atomic load and no set lookup.
    fn block_entry_key(&self, block: BlockKey) -> BlockEntryKey {
        let generation = if self.demotions.any() && self.demotions.contains(&block.object) {
            self.generation() | DEMOTED_GENERATION_BIT
        } else {
            self.generation()
        };
        BlockEntryKey { block, generation }
    }

    /// The current-generation metadata-store key for a block-aligned metadata
    /// object (metadata.json, manifest list, manifest).
    fn meta_block_key(&self, block: BlockKey) -> MetaEntryKey {
        MetaEntryKey::Block {
            block,
            generation: self.generation(),
        }
    }

    /// The current-generation metadata-store key for a Parquet footer pinned at
    /// suffix granularity.
    fn meta_suffix_key(&self, object: CacheKey, etag: String, len: u64) -> MetaEntryKey {
        MetaEntryKey::Suffix {
            object,
            etag,
            len,
            generation: self.generation(),
        }
    }

    /// Inserts an admitted block into the hybrid store and charges its DRAM
    /// weight to the live-bytes gauge (#178). The key already carries the
    /// current generation, so this only ever adds live bytes; a later purge
    /// resets the gauge and the entry ages out as reclaimable garbage.
    fn insert_block(&self, key: BlockEntryKey, value: Bytes) {
        use foyer::Code;
        let weight = (key.estimated_size() + value.len() + ENTRY_OVERHEAD_BYTES) as u64;
        self.live_block_bytes.fetch_add(weight, Ordering::Relaxed);
        self.blocks.insert(key, value);
    }

    /// Pins a metadata entry in the dedicated store and charges its DRAM-front
    /// weight to the metadata live-bytes gauge (#178) — the [`Inner::insert_block`]
    /// analogue for the pinned metadata store.
    fn insert_meta(&self, key: MetaEntryKey, value: Bytes) {
        use foyer::Code;
        let weight = (key.estimated_size() + value.len() + ENTRY_OVERHEAD_BYTES) as u64;
        self.live_meta_bytes.fetch_add(weight, Ordering::Relaxed);
        self.meta_store.insert(key, value);
    }

    /// Scan-resistant admission gate (#15) for one data block: records the
    /// miss in the frequency sketch, bumps the admitted/rejected counter, and
    /// returns whether the block should be inserted. Every caller is inside a
    /// backend/peer fill. Warm hits update the same lock-free sketch at their
    /// lookup sites. A rejected block is still served to the client; it is
    /// simply not cached.
    fn admit_block(&self, key: &BlockEntryKey, weight: usize) -> bool {
        // Runtime disk-full guardrail (#96): while caching is paused the disk is
        // out of room, so admit nothing. The block is still served to the client
        // from this fill; it is simply not written. One relaxed atomic load — no
        // syscall on the fill path. Checked before the sketch so a paused cache
        // does no admission bookkeeping at all.
        if self.caching_paused.load(Ordering::Relaxed) {
            CacheCounters::bump(&self.counters.blocks_rejected);
            return false;
        }
        // Retirement (#198): a demoted object's key carries the poison bit, and
        // a demoted block must never be re-admitted — it refills straight
        // through the cache so the object goes cold and stays cold. Checked off
        // the poison bit already on the key, so this needs no set lookup.
        if key.generation & DEMOTED_GENERATION_BIT != 0 {
            CacheCounters::bump(&self.counters.blocks_rejected);
            return false;
        }
        let admit = self.admission.admit(key, weight as u64);
        if admit {
            CacheCounters::bump(&self.counters.blocks_admitted);
        } else {
            CacheCounters::bump(&self.counters.blocks_rejected);
        }
        admit
    }

    /// Attributes served bytes to the tier that produced the block.
    fn count_served(&self, tier: Tier, bytes: u64) {
        let counter = match tier {
            Tier::Dram => &self.counters.dram_bytes_served,
            Tier::Disk => &self.counters.disk_bytes_served,
            Tier::Peer => &self.counters.peer_bytes_served,
            Tier::Backend => &self.counters.backend_bytes_served,
            Tier::Meta => &self.counters.meta_bytes_served,
        };
        CacheCounters::add(counter, bytes);
    }
}

/// Reconstructs a [`ReadError`] from a shared one. `ReadError` is not `Clone`
/// (it does not need to be on the direct read path), but the singleflight (#19)
/// hands one `Arc`-wrapped error to every waiter, so each rebuilds its own copy
/// here. The variants are a fixed set of unit variants plus one `String`
/// payload, so this is a total, allocation-only mapping.
fn clone_read_error(error: &ReadError) -> ReadError {
    match error {
        ReadError::NoSuchBucket => ReadError::NoSuchBucket,
        ReadError::AccessDenied => ReadError::AccessDenied,
        ReadError::NoSuchKey => ReadError::NoSuchKey,
        ReadError::InvalidRange => ReadError::InvalidRange,
        ReadError::InvalidPart => ReadError::InvalidPart,
        ReadError::Backend(msg) => ReadError::Backend(msg.clone()),
    }
}

/// Collects a fill response body into exactly one block — bounded memory by
/// construction. Returns `Ok(None)` when the body's length disagrees with
/// `expected_len` (an over-long body is abandoned as soon as it overflows);
/// stream errors propagate.
async fn collect_block(body: BodyStream, expected_len: usize) -> Result<Option<Bytes>, ReadError> {
    let mut buf = BytesMut::with_capacity(expected_len);
    let mut body = body;
    while let Some(chunk) = body.try_next().await? {
        if buf.len() + chunk.len() > expected_len {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk);
    }
    if buf.len() != expected_len {
        return Ok(None);
    }
    Ok(Some(buf.freeze()))
}

/// Extracts a block payload of exactly `expected_len` bytes from a cache
/// entry, treating anything else — no entry or a wrong-length payload — as a
/// miss. Suspect bytes are never served; the ladder falls through and the
/// backend fill overwrites the entry.
fn block_payload(
    entry: Option<HybridCacheEntry<BlockEntryKey, Bytes>>,
    expected_len: usize,
) -> Option<Bytes> {
    let entry = entry?;
    let bytes = entry.value();
    (bytes.len() == expected_len).then(|| bytes.clone())
}

#[cfg(test)]
mod pipeline_sizing_tests {
    //! The disk-write pipeline DRAM split (#278): the flush buffer must hold two
    //! block entries at every supported geometry, and the buffer plus submit
    //! queue must always equal the fixed pipeline reservation so the DRAM budget
    //! arithmetic is unchanged.

    use super::*;
    use verglas_core::config::{
        DEFAULT_DATA_BLOCK_BYTES, MAX_DATA_BLOCK_BYTES, MIN_DATA_BLOCK_BYTES,
    };

    /// The valid data-block geometries: powers of two from the minimum to the
    /// maximum the config permits.
    fn geometries() -> Vec<u64> {
        std::iter::successors(Some(MIN_DATA_BLOCK_BYTES), |b| {
            (*b < MAX_DATA_BLOCK_BYTES).then(|| b * 2)
        })
        .collect()
    }

    /// The flush buffer holds two whole block entries at every geometry, so two
    /// back-to-back writes in one flusher rotation both fit.
    #[test]
    fn flush_buffer_holds_two_block_entries_at_every_geometry() {
        for block in geometries() {
            let entry = foyer_flush_entry_bytes(block);
            assert!(
                flush_buffer_pool_bytes(block) >= 2 * entry,
                "geometry {block}: buffer {} must hold two {entry}-byte entries",
                flush_buffer_pool_bytes(block)
            );
        }
    }

    /// The 8 MiB block entry matches foyer's own accounting (block plus one page)
    /// and two of them exceed the old fixed 16 MiB buffer that dropped the second
    /// write (#273).
    #[test]
    fn max_geometry_entry_matches_foyers_accounting() {
        assert_eq!(foyer_flush_entry_bytes(8 * 1024 * 1024), 8_392_704);
        assert!(flush_buffer_pool_bytes(8 * 1024 * 1024) > 16 * 1024 * 1024);
    }

    /// The buffer plus the submit queue is exactly the fixed pipeline
    /// reservation at every geometry, so `min_dram_budget_bytes` (and the
    /// documented 80 MB constrained profile) does not move.
    #[test]
    fn buffer_plus_submit_queue_equals_the_fixed_reservation() {
        for block in geometries() {
            assert_eq!(
                flush_buffer_pool_bytes(block) + submit_queue_bytes(block),
                DRAM_PIPELINE_RESERVED_BYTES,
                "geometry {block}: buffer + submit queue must equal the reservation"
            );
            // The submit queue stays a meaningful positive remainder.
            assert!(submit_queue_bytes(block) >= flush_buffer_pool_bytes(block));
        }
        // The default geometry is covered by the loop; assert it explicitly so a
        // regression on the production profile is unmissable.
        assert_eq!(
            flush_buffer_pool_bytes(DEFAULT_DATA_BLOCK_BYTES)
                + submit_queue_bytes(DEFAULT_DATA_BLOCK_BYTES),
            DRAM_PIPELINE_RESERVED_BYTES
        );
    }
}

#[cfg(test)]
mod ttl_clock_tests {
    //! Deterministic mutable-mapping TTL-window tests (#14) driven by a manual
    //! clock, so expiry is proven exactly without sleeping on wall time. The
    //! coarse-grained integration tests (TTL=0, real backend) live in
    //! `tests/engine.rs`; these prove the *window* and its reset on `Unchanged`.

    use super::*;
    use std::sync::atomic::AtomicU64;

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};
    use verglas_core::config::ByteSize;
    use verglas_core::peer::NoopPeerFetch;
    use verglas_s3::{BackendStore, PassthroughRead};

    /// A clock whose time only advances when the test says so.
    struct ManualClock {
        /// Fixed origin instant; all reported times are offsets from it.
        base: Instant,
        /// Milliseconds elapsed since `base`, moved only by [`ManualClock::advance`].
        offset_ms: AtomicU64,
    }

    impl ManualClock {
        /// A clock parked at `now`.
        fn new() -> Arc<ManualClock> {
            Arc::new(ManualClock {
                base: Instant::now(),
                offset_ms: AtomicU64::new(0),
            })
        }

        /// Moves the clock forward by `secs` seconds.
        fn advance_secs(&self, secs: u64) {
            self.offset_ms.fetch_add(secs * 1000, Ordering::Relaxed);
        }
    }

    impl Clock for ManualClock {
        /// `base` plus the accumulated manual offset.
        fn now(&self) -> Instant {
            self.base + Duration::from_millis(self.offset_ms.load(Ordering::Relaxed))
        }
    }

    /// Builds a single-node engine over an in-memory origin, driven by `clock`,
    /// with the given mutable-mapping TTL.
    async fn engine_with_clock(
        dir: &std::path::Path,
        store: Arc<InMemory>,
        clock: Arc<ManualClock>,
        ttl_secs: u64,
    ) -> HybridCacheEngine<PassthroughRead, NoopPeerFetch, RendezvousRing> {
        let config = CacheConfig {
            dir: dir.to_path_buf(),
            capacity_bytes: ByteSize(64 * 1024 * 1024),
            dram_bytes: ByteSize(128 * 1024 * 1024),
            mutable_mapping_ttl_secs: ttl_secs,
            ..CacheConfig::default()
        };
        let node = NodeId::new(SINGLE_NODE_ID);
        HybridCacheEngine::build_engine(
            PassthroughRead::new(BackendStore::single("default", "b", store)),
            NoopPeerFetch,
            RendezvousRing::single(node.clone()),
            node,
            &config,
            MetaRouter::heuristics_only(),
            clock,
            DEFAULT_BACKGROUND_FILL_SLOTS,
        )
        .await
        .expect("build engine")
    }

    #[tokio::test]
    async fn mutable_mapping_revalidates_only_after_its_ttl_window() {
        let tmp = std::env::temp_dir().join(format!("verglas-ttl-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).expect("scratch dir");
        let store = Arc::new(InMemory::new());
        store
            .put(
                &Path::from("notes.txt"),
                PutPayload::from(Bytes::from_static(b"hello")),
            )
            .await
            .expect("seed");
        let clock = ManualClock::new();
        let engine = engine_with_clock(&tmp, store, clock.clone(), 5).await;
        let k = CacheKey {
            storage_binding_id: "default".to_owned(),
            bucket: "b".to_owned(),
            key: "notes.txt".to_owned(),
        };

        // First read admits the mapping at t=0; no revalidation yet.
        engine.head(&k).await.expect("resolve mapping");
        assert_eq!(engine.counters().snapshot().meta_revalidations, 0);

        // t=3s: still inside the 5s window — no revalidation.
        clock.advance_secs(3);
        engine.head(&k).await.expect("read within window");
        assert_eq!(
            engine.counters().snapshot().meta_revalidations,
            0,
            "a mutable mapping must not revalidate before its TTL elapses"
        );

        // t=6s: past the window — exactly one revalidation, which (unchanged)
        // resets the window from t=6s.
        clock.advance_secs(3);
        engine.head(&k).await.expect("read after window");
        let snap = engine.counters().snapshot();
        assert_eq!(snap.meta_revalidations, 1, "one revalidation after expiry");
        assert_eq!(snap.meta_revalidations_unchanged, 1);

        // t=9s: only 3s since the reset — no further revalidation.
        clock.advance_secs(3);
        engine.head(&k).await.expect("read within reset window");
        assert_eq!(
            engine.counters().snapshot().meta_revalidations,
            1,
            "the Unchanged revalidation must have reset the TTL window"
        );

        // t=12s: 6s since the reset — a second revalidation.
        clock.advance_secs(3);
        engine.head(&k).await.expect("read after reset window");
        assert_eq!(engine.counters().snapshot().meta_revalidations, 2);
    }

    #[tokio::test]
    async fn immutable_mapping_ignores_the_clock_entirely() {
        let tmp = std::env::temp_dir().join(format!("verglas-ttl-imm-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).expect("scratch dir");
        let store = Arc::new(InMemory::new());
        store
            .put(
                &Path::from("t/data/f.parquet"),
                PutPayload::from(Bytes::from_static(b"parquet-bytes")),
            )
            .await
            .expect("seed");
        let clock = ManualClock::new();
        // TTL=1s: a mutable key would revalidate almost immediately.
        let engine = engine_with_clock(&tmp, store, clock.clone(), 1).await;
        let k = CacheKey {
            storage_binding_id: "default".to_owned(),
            bucket: "b".to_owned(),
            key: "t/data/f.parquet".to_owned(),
        };

        engine.head(&k).await.expect("resolve mapping");
        // Jump far past any TTL; an immutable mapping still never revalidates.
        clock.advance_secs(3600);
        engine.head(&k).await.expect("re-read much later");
        assert_eq!(
            engine.counters().snapshot().meta_revalidations,
            0,
            "an immutable mapping must never revalidate, no matter the clock"
        );
    }
}
