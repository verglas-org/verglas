# verglas-cache worklog

Append-only log of changes to this crate, by issue. Every PR touching this
crate adds an entry (see /AGENTS.md, "Worklog discipline").

- #1: Scaffolded as part of the initial cargo workspace: stub with module-level
  docs, placeholder types wiring real dependency edges, and an integration
  test directory. Toolchain pinned (1.96.1), workspace clippy lints applied.
- #12: Implemented the hybrid DRAM+NVMe cache engine on foyer 0.22 behind the
  `ObjectRead` trait. Objects are cached as 8 MiB blocks keyed by
  `(bucket, key, etag, block_index)` (ETag discipline: no origin ETag means
  passthrough with no admission; a mid-read ETag change fails the request
  rather than mixing versions). Miss ladder is local DRAM → local disk →
  peer fetch → backend fill, with ownership resolved through the ring for
  every lookup; extension points left doc-commented for singleflight (#19), write-path
  invalidation (#9/#21), admission policy (#15), and persistence tuning
  (#16). Budgets map to foyer as hard ceilings (disk preallocated to
  `capacity_bytes`; DRAM = memory-tier capacity + fixed write-pipeline
  reservation, one shard so eviction is exact) and read-path counters are
  exposed as plain atomics for #46.
- #9/#120: Implemented the `Invalidation` trait on the engine: invalidate =
  remove the key→ETag mapping (ETag-keyed blocks become unreachable and age
  out). Red→green: with the cache wired, #120's write smoke test served a
  stale 200 after DELETE until this landed.
- #121 review follow-ups: the shared traits now come from verglas-core (verglas-s3
  is dev-dependency-only); cold GETs resolve the mapping from the first fill
  GET's own response instead of a separate HEAD (one backend request per
  covering block; suffix ranges and origin-rejected probes fall back to the
  HEAD path).
- #122: Split the key→ETag mappings into their own DRAM-only cache with a
  fixed 16 MiB carve-out — sharing one LRU with 8 MiB blocks let block
  churn (and foyer's disk-hit re-admissions) sweep the mappings out,
  degenerating warm re-reads into backend fills (measured). Restored block
  memory-tier shard parallelism with a derived shard count (one shard per
  32 MiB, capped at 8) so per-shard capacity always fits several blocks —
  verified against foyer 0.22.3's emplace semantics, which only strand
  entries over budget when a single entry exceeds shard capacity. Added a
  `dram_usage()` gauge and a shard-parameterized ceiling test as the
  regression tripwire (forced 8 shards on a 32 MiB tier trips it).
- #122 follow-up (CI flake): the shard-parameterized ceiling test gave the
  disk tier 2x the working set and tolerates <=2 stray refills on the
  re-read pass — foyer's async region reclaimer can evict one live block
  when clean-region headroom is tight under slow IO (seen once under
  coverage instrumentation), which is disk behavior, not the DRAM pathology
  the test pins.
- #124: Closed the invalidation/admission race that could reinstate a stale
  key->ETag mapping after an acked write. Added a per-key `MetaFence` (an
  epoch + in-flight refcount per active key under one `Mutex`, entries
  dropped at refcount zero): metadata-resolving paths (`first_fill`'s probe
  GET, `resolve_meta_via_head`) register an RAII guard capturing the epoch
  before issuing the backend request, admission only inserts the mapping if
  the epoch is unchanged (else the response is still served but nothing is
  memoized), and `invalidate` bumps the epoch and removes the mapping under
  the same lock. `fill_block`'s ETag-mismatch arm now just removes the stale
  mapping instead of re-admitting from the data-path response. Also softened
  the module comment about block payloads always being pre-compressed
  (Avro can be uncompressed).
- #124 follow-up: widened the ceiling test's refill tripwire from ≤2 to a
  quarter of the working set. Coverage-instrumented CI hit 3 reclaimer
  evictions (previously 1) — slow-IO disk noise, not the eviction-domain
  pathology, which refills the *whole* working set; the threshold now sits
  an order of magnitude below the failure it pins instead of a hair above
  the noise.
- #132: Added the multi-bucket acceptance tests to `tests/engine.rs`: a
  key-collision test (two buckets, identical object key, different bytes ->
  correct bytes for each, cold and warm) and a bucket-scoped invalidation test
  (invalidating bucket A leaves B's cached mapping intact, zero backend
  traffic). Both drive one engine over a two-origin registry; no engine changes
  were needed since `CacheKey` already carries the bucket.
- #131: Cold suffix-range reads now derive the key→ETag mapping from the
  suffix GET's own `206` instead of paying a separate HEAD. `first_fill`
  dispatches `ReadRange::Suffix` to a new `first_fill_suffix` that
  fence-registers (#124) before the suffix GET, reads the total size and
  ETag off the response (`got.range` + `meta.size`), admits only the mapping,
  and serves the already-fetched bytes straight through (new
  `FirstFill::Served` variant) — the unaligned tail block is deliberately NOT
  cached (issue option 1, serve-only). Malformed/range-ignored responses
  (served range ≠ the expected tail, `bytes=-0`, empty object) fall back to
  the existing HEAD path. Cold footer reads drop from (1 GET, 1 HEAD) to
  (1 GET, 0 HEAD).
- #19: Singleflight fill deduplication. Added an in-flight map (three
  sub-maps: first-fill probes keyed by `(object, block_index)` pre-ETag,
  verified block fills keyed by the full `BlockKey`, and HEAD resolutions
  keyed by object) so N concurrent cold reads of one block/mapping collapse
  to one backend request. `run_flight` runs the first requester's fill in a
  detached `tokio::spawn` task (waiter-cancellation safety: a disconnecting
  client cannot abort the fill others await) and shares the result via a
  `Shared` future; the task removes its own map entry only after the fill has
  admitted, so late readers either join the in-flight fill or hit the freshly
  admitted cache — never a duplicate fill — and errors (the entry is removed
  on failure too) are never memoized. The map is bounded by `MAX_INFLIGHT`
  (dedup bypassed inline past it) and its size is exposed via
  `HybridCacheEngine::inflight_len()` for #46; a `deduped_fills` counter
  records collapses. Keyed by object identity (ring-ownership extension point for M2's
  cross-node collapse, #17/#29). The #131 suffix serve-only path is
  deliberately not deduplicated — its body is a non-clonable, potentially
  whole-object-sized stream — but the block/HEAD sites it shares still dedup.
- #138: Added `HybridCacheEngine::purge()` — drops every key→ETag mapping and
  clears both foyer block tiers (via `HybridCache::clear`), returning a
  `PurgeReport` of DRAM bytes freed. Purge bumps every active `MetaFence` epoch
  (`purge_all_epochs`) before clearing, so an in-flight metadata resolution
  from before the purge cannot re-admit a mapping into the just-cleared cache
  (the #124 interaction); the fence reasoning and foyer's stale-usage-gauge
  quirk are documented on the method. Exposed an object-safe `CachePurger`
  trait so the daemon's admin listener can call purge without the engine's type
  parameters.
- #27/#28: adapted the `FixedOwnerRing` test double to the evolved `Ring` trait
  (`members()` now returns an owned `Vec<NodeId>` snapshot so a live gossip ring
  can implement it). Test-only change; the engine's read/fill path is untouched
  and still routes every request through `ring.owner`.
- #15: added scan-resistant block admission. A one-touch bulk read (a full-table
  scan larger than the cache) must not evict the reused working set, so a new
  `admission` module keeps a lock-free count-min frequency sketch (TinyLFU-style,
  with aging) and gates every data-block `blocks.insert` at the three fill sites
  (`probe_fill`, the peer rung, `fill_block`): below a pressure line (half the
  disk budget) the cache is still filling cold and everything is admitted, and
  above it a block is admitted only once the sketch has seen it
  `frequency_threshold` times (default 2) — a scan block, seen once, flows through
  to the client but is never cached. foyer ships no key-level admission hook (its
  `StorageFilter` sees only a hash/size), so the decision lives at the engine's
  insert sites; the metadata mapping cache is explicitly exempt (always admitted,
  tiny and valuable). Rejected/admitted counts are exported (`blocks_rejected`,
  `blocks_admitted`) and the policy resets on purge. The sketch is lock-free and
  touched only on the cold fill path, so the warm read path stays lock-free and
  the DRAM/disk ceiling tripwires are unaffected (admission only ever inserts
  less).
- #29: added the peer-serving endpoint `HybridCacheEngine::local_block`, which
  serves an owned, cached block to a peer from the local tiers only — never a
  backend fill (a peer miss returns a miss, so peer requests cannot amplify
  fills) and never for a key this node does not own (replicas stay off the
  peer path; owners are the single intra-pod source). The peer rung now sends
  the full `BlockKey` (ETag exact) and records `peer_misses`/`peer_errors`;
  `local_block` meters `peer_served_blocks`/`peer_served_bytes` (#46). Tests
  cover cache-only serving, stale-ETag/absent-index misses, and the
  replica-refusal policy.

- #50: added the dedicated metadata store — a second, DRAM-only foyer instance
  (`meta_store.rs`, one shard to avoid the #122 stranding hazard) that pins the
  raw bytes of table metadata (metadata.json, manifest lists, manifests, Parquet
  footers) in its own eviction domain, hard-isolated from data-block pressure. A
  new `classify.rs` routes each read to the meta or data store on deterministic,
  pure heuristics (`**/metadata/*.json`, `**/metadata/*.avro`, `*/snap-*.avro`,
  suffix-range GET on `*.parquet`) plus an optional mapper hook (`MetaClassifier`
  trait, no dependency on verglas-tables). The engine threads the routing through
  the fill/serve path: metadata reads fill/serve through the meta store, always
  admitted (bypassing scan admission), and Parquet footers pin from the second
  read once the mapping is warm (the #131 cold serve-only fast path is
  preserved). The meta store's DRAM is carved from *inside* the existing DRAM
  ceiling via `cache.meta_fraction` (default 5%); the on-disk ceiling is
  untouched (DRAM-only in this PR — the NVMe long-tail tier is a filed
  follow-up). Added meta hit/miss/bytes/fills/refreshes counters and a
  `refresh_watched` method for watcher-driven catalog-pointer refresh (#47).

- #50: footer suffix reads no longer pin their covering block in the meta store.
  A live constrained-profile run exposed that a sub-8 MiB Parquet file's footer
  read caches the whole file (block 0) as "metadata", evicting the real planning
  manifests from the small pinned store and regressing warm planning to
  ~220 ms. Footers keep the #131 serve-only cold path and the data store on warm
  reads; footer-granular pinning (the ~64 KiB speculative read) moves to the
  eager-warming follow-up. Non-suffix metadata (metadata.json, manifest lists,
  manifests) still pins in the meta store — after the fix warm planning holds at
  ~0.4 ms across repeated runs.
- #50 review round (user-directed): the metadata store is now a foyer HYBRID —
  DRAM front plus an NVMe long tail on 1 MiB regions, both carved from inside
  the existing ceilings (the meta DRAM carve also covers the small disk-write
  pipeline, so the 80 MB constrained profile still boots). Parquet footers are
  pinned at SUFFIX granularity under a new MetaEntryKey::Suffix identity —
  cold suffix fills buffer pinnable tails (≤512 KiB) into the meta store, warm
  reads serve them with zero backend traffic, and the block ladder is never
  used for metadata suffixes (the whole-file block-0 pollution is structurally
  gone, pinned by two regression tests).

- #16: made the disk tiers survive a restart. The data block cache now writes
  through to NVMe on insertion (`HybridCachePolicy::WriteOnInsertion`) so a
  block that stays hot in DRAM is still durable — the key to recovering a warm
  cache after an unclean `kill -9`, which never runs a graceful flush. Both
  stores pin `RecoverMode::Quiet` explicitly (foyer's default, made
  regression-proof) so startup rebuilds the in-memory index from on-disk region
  metadata and silently drops any checksum-failed (torn) tail entry. The
  metadata store (#50) deliberately keeps the default eviction policy: its tier
  is a few 1 MiB regions, and writing through would churn its reclaimer and
  break hard isolation. New integration tests (`tests/recovery.rs`) prove:
  post-restart reads re-resolve the DRAM-only mapping (one backend round trip)
  then reuse surviving blocks with zero re-fills; a torn tail write is dropped
  and never served (never wrong bytes); the meta store recovers its NVMe tail;
  and recovery time is measured (~11 ms for a 256 MiB / 32-block directory
  cache).
 #164: made admission pressure occupancy-aware and resident-biased for cyclic
  scans. `Admission::admit` now takes foyer's live memory-tier `usage()`/
  `capacity()` and treats the cache as under pressure when *either* that real
  gauge is ~full or the existing disk-byte proxy has crossed its threshold
  (foyer 0.22.3 exposes no live disk-residency gauge — noted as an upstream
  seam). Once under pressure, a candidate that clears the frequency gate is
  admitted only with `churn_admit_probability` via a deterministic, lock-free
  Bresenham-style fractional admitter, so a cyclic sweep is thinned to a stable
  resident subset. The #15 one-touch scan test and both capacity ceilings stay
  green; default `p = 1.0` keeps pre-#164 behavior byte-for-byte.
#14: classified each key→ETag mapping's mutability (`classify::Mutability`):
  immutable Iceberg files (`*.parquet`/`*.orc`/`*.avro` data — all three formats
  per the addendum — and `metadata/*.json`) pin their mapping indefinitely with
  zero revalidation traffic; everything else carries the `mutable_mapping_ttl_secs`
  TTL and is revalidated with a conditional `If-None-Match` after expiry (refresh
  TTL on 304, rotate on change, re-resolve on vanish/error). Revalidation runs
  under the #124 fence and is singleflight-deduped; a manual clock makes the TTL
  window deterministic in tests. Added `meta_revalidations{,_unchanged,_rotated}`
  counters.
- #149: Wrapped the cold suffix-read fill in the #19 singleflight so a Parquet-footer
  stampede collapses. Tails at or under `SUFFIX_BUFFER_CAP` (one block, 8 MiB) are
  drained into `Bytes` by the winning fill and shared through a new `suffixes` flight
  map keyed by `(object, len)`; N concurrent cold suffix reads of one object now issue
  exactly one backend GET, every waiter streaming from the one buffer. Over-cap tails
  keep the #131 serve-only, non-deduplicated stream. Reconciled with #50 pinning
  (metadata-classified pinnable tails are still pinned inside the buffered fill) and
  kept #124 fence discipline (register-before-GET, admit-if-fresh). Added a
  `deduped_suffix_fills` counter (run_flight now takes a per-site dedup-counter
  selector) so footer-stampede collapses are metered apart from block/mapping fills.
- #143: extended `CachedMeta` with a boxed `ObjectHeaders` (Cache-Control,
  Content-Encoding, Content-Disposition, Content-Language, and the user-metadata
  map) so a cache-hit GET/HEAD reports the same headers a cold read would —
  parity the write path now depends on. Boxed to keep `CachedMeta` small inside
  the engine's fill enums (large-enum-variant); the metadata cache is DRAM-only
  and never serialized, so no framing change.
 #178: Reworked cache purge from a physical `clear()` into a generation-epoch
  bump (memcached `flush_all` / Redis lazyfree convergent pattern). `BlockEntryKey`
  and `MetaEntryKey` gained a `generation` field, included in the foyer on-disk
  codec; every lookup/insert stamps the engine's current `purge_generation`, and
  `purge()` just bumps it (Release), persists it to `cache.dir/PURGE_GENERATION`,
  bumps the #124 fence epochs, and clears the DRAM-only mapping cache. No
  `blocks.clear()`/`meta_store.clear()` remain — foyer's clear-vs-insert panic
  (foyer-rs/foyer#1305) is now unreachable by construction. Stale-generation
  entries are reclaimed by natural LRU aging (foyer 0.22.3 exposes no iteration);
  the DRAM ceiling holds harder since foyer charges live+stale alike. Added honest
  `dram_live_bytes`/`dram_reclaimable_bytes` gauges and a live/reclaimable split in
  `PurgeReport`, so a repeat-cold benchmark reads truth at T=0. The generation is
  persisted so a purge survives a restart (old-gen entries recover as unreachable
  garbage; current-gen entries still recover warm, #16).
 #189: added `expires` to `ObjectHeaders` and threaded it through
  `CachedMeta::{from,to}_object_meta` and the weighter, so a cache-hit GET/HEAD
  reports the same `Expires` header a cold read filled from the origin. The
  metadata cache is DRAM-only and unserialized, so no framing change.
- #30/#31: warm-from-peers on an owned local miss. When this node owns a block
  but misses DRAM+disk, `read_block` now consults `ring.warm_donor` before the
  backend: a draining donor is pulled unconditionally (drain, #31), a healthy
  predecessor only while the new `begin_warming` window is open (join, #30), so
  a settled pod pays no extra hop. `local_block` (the donor endpoint) now gates
  on `ring.should_serve_peer` so a draining node keeps serving its shed keys.
  Added the `warming_until_ms` deadline, `begin_warming`/`is_warming`, and a
  `now_unix_millis` helper.
 #51 CI fix: purge's reclaimable-bytes math used two separate atomic
  snapshots and could underflow when fills ran between the loads. Now
  saturating. Found by the purge hammer test on CI.
- #180: Added `writeback_codec`, a streaming Reed-Solomon fragment codec for
  the erasure-coded write-back tier. Objects are striped into `k` data plus `m`
  parity fragments (any `k` reconstruct) using `reed-solomon-simd`; the striped
  `StripeEncoder` encodes a stripe as soon as its bytes arrive so encoding can
  overlap the client upload. `examples/codec_throughput.rs` compares SIMD vs
  `reed-solomon-erasure` encode GB/s and justifies the backend choice.
- #180: Added `StreamingStripeEncoder`, which encodes each stripe and hands its
  k+m shards to a sink then drops them, so encoding a large object holds one
  stripe rather than accumulating whole fragments. The engine's disk carve now
  subtracts `Cache::fragment_budget_bytes()` too, so blocks + metadata +
  fragments never exceed `capacity_bytes` (a hard ceiling by construction).
- #24: Added `tests/property_invariants.rs`, a seeded property suite that drives
  the real `HybridCacheEngine` through many randomized scenarios for five cache
  correctness invariants: ETag immutability, singleflight uniqueness (#19),
  no-stale-read-after-ack (#21/#124), purge generation semantics (#178), and the
  DRAM/disk budget ceilings (#122). Tests-only, no src changes; randomness comes
  from a hand-rolled SplitMix64 (the #21 pattern) so every failure prints a seed
  for exact replay, and `VERGLAS_PROPTEST_ITERS` scales the sweep for deeper
  local runs.
- #24 follow-up: the concurrent-cold-reads property found a real singleflight
  gap (filed as #214) — a second GET can fire across the first-fill/block-fill
  boundary under CI-shaped timing. Marked that one property #[ignore] with the
  issue number so the other four properties run; #214 fixes the engine and
  un-ignores it.
- #214: Collapsed the first-fill/block-fill boundary so a cold read never issues
  a redundant backend GET. Two paths were split across two in-flight maps: the
  mapping-resolving probe (keyed by object+block index) and the block fill (keyed
  by the full BlockKey with ETag). A reader that resolved the mapping and then
  took the block-fill path could not see the still-in-flight probe, so it fetched
  the same block again. Fix: the block ladder's backend rung now consults the
  probe map first and attaches to a live probe for the same block, and a
  resolved-mapping suffix read is served through the one suffix fill site (#131)
  instead of the block ladder — so both classes of suffix read stay on a single
  fill site and collapse. Un-ignored the property, fixed a test-harness bug where
  the cumulative dedup counter was compared against a per-seed fan-out (the
  parked winner released before this seed's readers attached), and added a parked
  variant that deterministically forces the first-fill/block-fill race and
  asserts one GET. No change to the #124 fence, #149 suffix dedup, or #178 purge
  behavior.
- #153/#154/#156: the hybrid engine forwards the new escape-hatch reads
  (`get_direct`/`head_direct`/`object_attributes`) straight to its fill backend
  with no admission — version-, part-, and checksum-scoped reads name an
  immutable view the cache does not model, so they are served passthrough like a
  non-cacheable read.
 #198: Added the evict-first demotion API for retired files. A new `Demotions`
  set marks compaction-orphaned objects; a demoted object's block lookups
  resolve at a poisoned generation (reusing #178's generation-epoch machinery),
  so its cached blocks are unreachable misses that LRU ages out before live data
  and its fills skip admission (never re-admitted). The engine exposes
  `demote`/`hard_evict`/`is_demoted`/`demoted_count` plus an object-safe
  `BlockDemoter` trait for verglas-tables to call. The warm read path pays one
  relaxed atomic load when nothing is demoted; the DRAM/disk ceilings are
  unchanged (demoted reads refill straight through the cache).
- #226: reverted to single-bucket serving; deleted the #132 per-bucket registry; backend.bucket is now required and gates serving. Multi-bucket is deferred to #226.
- #46: the engine's `get` now records the tier that produced the first served
  block into the read's `TierCell` (one relaxed store on the hot path), so the
  request-duration histogram is labelled by dram/nvme/peer/backend/passthrough.
  Existing counters (hits/misses, bytes-by-tier, admitted/rejected, fill
  inflight) are read at scrape time; no new hot-path aggregation.
- #220: write-back codec now carries a per-fragment CRC32C. `Fragment` gained a
  `checksum` field and a `Fragment::new` constructor that computes it; a public
  `checksum()` helper hashes with crc32c (Castagnoli). `reassemble` verifies each
  fragment before use and drops any that fail as an erasure, so a bit-flipped
  fragment is reconstructed from the good ones instead of producing silent
  garbage; with more than `m` corrupt/missing it fails loudly with
  InsufficientFragments.
- #213: fixed a torn read across a block boundary during a concurrent
  overwrite. A ranged read serves its covering blocks one at a time and lazily,
  and each block is keyed by ETag: a warm cache hit returns the mapping's
  version while a fill returns whatever the backend now holds, so a mutable
  object overwritten while its mapping was still warm could serve old cached
  blocks mixed with a fill of the new version. A multi-block read of a mutable
  mapping now commits the object version once, before the body streams, by
  forcing a single #14 revalidation (fence-checked, deduplicated) regardless of
  TTL; every covering block then resolves at that one committed ETag. Immutable
  keys and single-block reads are untouched (they cannot tear), so the
  zero-revalidation immutable hot path stays lock-free. Added a deterministic
  reproduction in engine.rs and a randomized interleaving property in
  property_invariants.rs.
- #240: changed both Foyer disk tiers from per-region filesystem devices to
  single sparse backing files, so a multi-TiB cache does not consume one file
  descriptor per region. Added a 7 TiB construction regression and kept the
  existing capacity and torn-write recovery coverage against the new layout.
- #250: changed multi-block response bodies from serial cold fills to a fixed
  four-block ordered look-ahead window. The existing per-bucket backend limiter
  remains the origin-wide authority, while the response preserves byte order,
  ETag/version discipline, and a bounded per-request number of unresolved blocks.
  A deterministic parked-origin regression proves four cold block fills start
  before any is allowed to complete and the final body remains byte-exact.
- #164 follow-up: corrected the occupancy shortcut for hybrid caches. A full
  DRAM tier now demotes blocks into the configured NVMe capacity instead of
  rejecting first-touch blocks; scan admission still starts at the configured
  disk pressure threshold and respects its existing settings.
- #250: Cold data fills now stream requested bytes as origin chunks arrive, while a detached producer validates and admits the complete aligned block afterward. Partial reads fetch their exact range on first sight; repeated blocks may use the bounded aligned-fill lane, so one-touch scans cannot spend background bandwidth or pollute the cache.
- #250: Unmapped partial data reads now fetch their exact requested S3 range before starting the aligned cache fill, so the foreground request no longer waits for an entire cache block. Concurrent readers share that exact request, while one aligned fill runs inside the existing background concurrency ceiling and still produces a backend-free warm hit.
- #16: Changed the isolated metadata cache to write every admitted entry to
  NVMe on insertion, matching the data cache. Metadata that remains hot in DRAM
  now survives an unclean daemon restart instead of refilling from S3.
- #16: Kept metadata eviction isolated in DRAM while explicitly enqueueing each
  admitted entry for NVMe persistence. This preserves unclean-restart recovery
  without letting disk write pressure demote pinned catalog metadata.
- #252: Made data-block alignment, fills, admission sizing, Foyer regions, and
  response slicing use `cache.data_block_bytes` rather than a fixed 8 MiB
  constant. The default is 2 MiB, which reduced local SF1000 cold origin bytes
  from 70.67 GB to 33.29 GB without changing returned bytes.
- #252: Applied the configured block geometry to the #250 streaming and
  background-fill paths that merged from main. The exact-range first fetch,
  direct block slice, streaming fill, and background aligned overfetch now
  align on `cache.data_block_bytes` instead of the fixed constant, so the two
  features compose.
- #96: Added a runtime disk-full guardrail. `Inner.admit_block` admits nothing
  while a shared `caching_paused` flag is set, so a full disk degrades to origin
  fills instead of crashing the node; reads still serve. Exposed
  `caching_paused_handle()`/`is_caching_paused()`; the fill path reads the flag
  with one relaxed load, no syscall.
- #223: The block-cache disk carve now reads `Cache::fragment_ceiling_bytes()`
  (the write-back safety ceiling) instead of the retired fixed fraction, so the
  read cache and the fragment store still partition `capacity_bytes` and the
  on-disk ceiling holds by construction.
- #223: Removed the fragment disk carve entirely — the block cache's logical
  disk capacity is the full budget remainder after the metadata carve. Verified
  empirically that foyer's device files are sparse (physical 0 at build, grows
  with admissions; see tests/foyer_prealloc_probe.rs), which is what makes
  first-come-first-served budget sharing possible. Added
  `HybridCacheEngine::disk_growth_room_bytes()` (logical capacity minus
  allocated bytes of the two device files) for the daemon's budget accounting;
  background poll only, never a request path.
- #61: A cold fill whose origin GET/HEAD fails now emits a structured
  `origin fill failed` warning with the request id, a redacted key hash, the
  fill stage, and the error — the visibility the #245/#233 postmortems needed,
  where a failing origin degraded silently. The singleflight fill task now
  re-establishes the request id and span across its `tokio::spawn`, which would
  otherwise drop both.

- #273: `flush()` now waits for in-flight fills to finish admitting before it
  drains the disk write queue. A #255 streaming fill serves its bytes to the
  client and returns while a detached task admits the complete block off the
  client path, so a block a client had fully read could still be unadmitted when
  `flush()` ran; draining the queue alone then reported a durability it had not
  reached, and under CPU starvation the block reached neither DRAM nor NVMe and a
  re-read refilled it (the `working_set_larger_than_dram_spills_to_disk_and_serves`
  flake). Every detached fill task now decrements the in-flight counter and
  notifies a barrier only after it admits; `flush()` waits on that barrier first.
  The barrier is off the read path.

- #273 (second window): an exactly block-aligned cold read now resolves through
  the aligned probe instead of the partial first-fill path. The partial path
  served the exact bytes and then re-fetched the same aligned block from a
  detached background task — a second backend GET for bytes already in hand,
  admitted at an uncontrolled time. That uncontrolled write could share a foyer
  flusher drain window with a later warm block's write, and foyer's 16 MiB
  flush buffer holds only one 8 MiB block entry per window: the second write is
  silently dropped (foyer treats it as a future refill), `flush()` cannot see
  it, and the block's only copy — the one-block DRAM tier — is evicted by later
  inserts. That is how a warmed-and-flushed block ended up on neither tier and
  the spill test's re-read paid a sixth fill under CPU starvation. The probe
  path serves the same bytes with one GET and admits within the awaited flight,
  so block-by-block warm-then-flush is deterministic again.

- #273 (ladder re-probe): closed the read-ladder TOCTOU in the same race
  family. A reader that probed DRAM and disk just before a detached fill's
  insert and reached the singleflight map just after that fill removed its
  entry would re-fill a block that is already resident. A fill inserts its
  block before removing its map entry, so the block-fill site now re-probes
  DRAM under the map lock when the map has no entry and serves the resident
  block instead of filling again. Cold path only; this is what made
  cold_fill_streams_before_origin_body_completes_then_caches flake ~2/10 even
  unloaded on main.

- #275 (timing-test flakes): removed the hard-coded 250 ms wall-clock deadlines
  from three engine tests that starved out under CPU oversubscription, replacing
  them with deterministic synchronization. `cold_multiblock_read_starts_four_
  fills_before_first_completes` and `first_unmapped_partial_read_does_not_wait_
  for_aligned_tail` now gate the origin (parked bodies / a withheld aligned tail)
  and await the fill announcements or the served range directly — the property
  holds by construction because nothing is released until the assertion is made.
  `cold_fill_streams_before_origin_body_completes_then_caches` now takes the #274
  quiesce barrier (`flush()`) before asserting the block is resident, since that
  assertion is about eventual background admission, not timing. No sleeps, no
  retry loops, no loosened assertions. Verified 20/20 under the #274 saturation
  harness (TOKIO_WORKER_THREADS=2, 56 busy loops on 28 cores, --test-threads=2);
  the pre-fix tests fail under the same harness.

- #278 (flush buffer sizing + overflow visibility): foyer drains queued disk
  writes into one flush buffer per rotation and silently drops any entry that
  does not fit. The old fixed 16 MiB buffer held only one 8 MiB block entry, so
  two back-to-back writes in one rotation lost the second. The buffer is now
  sized from the block geometry to hold two entries (`flush_buffer_pool_bytes`),
  and the submit queue takes the remainder of the unchanged fixed pipeline
  reservation, so `min_dram_budget_bytes` and the DRAM budget arithmetic do not
  move. foyer's `storage_queue_buffer_overflow` metric is now surfaced through a
  small metrics registry (`foyer_metrics`) installed on the block cache: each
  drop bumps the new `storage_buffer_overflows` counter and emits a WARN
  structured event (#61-style), so silent write drops are observable. Regression
  test warms two blocks back-to-back, flushes, restarts, and asserts both recover
  from NVMe at the 2 MiB default and 8 MiB maximum geometry; it fails 10/10 under
  the saturation harness with the old buffer and passes 20/20 with the new sizing.

- #305: Hard eviction now physically removes a retired object's blocks.
  `demote` takes sized requests and returns receipts (ETag from the warm
  mapping, size, admission generation); `hard_evict` enumerates the block
  entries from those facts and removes them from the hybrid store and the
  pinned metadata store, dropping the stale mapping and returning the bytes
  reclaimed. Before, the sweep only cleared the demotion marker — dead
  compaction generations stayed resident until eviction pressure that a
  preallocated device never generates (3.6 TB of them, live incident
  2026-07-18). New counters: retired_bytes_pending (gauge),
  retired_bytes_reclaimed, retired_files_reclaimed.

- #95: Added `shadow`, the cache-managed store for Verglas-derived Puffin
  artifacts. Cluster-local and NVMe-resident under `<cache.dir>/shadow`, keyed by
  `(ArtifactKind, target, id)` with the reflected source snapshot as the version;
  the layout `<root>/<kind>/<target>/<id>/<seq:020>-<snapshot>.puffin` is
  self-describing. `latest` orders by a store-global monotonic write sequence, NOT
  by snapshot id (Iceberg snapshot ids are random i64). The key space carries an
  ArtifactKind so graph adjacency indexes and the heat/prewarm/pruning rollups of
  #95 slot in later; only `VectorIndex` is exercised now. The store is
  budget-bounded by a hard byte ceiling: a put that would exceed it evicts the
  least-recently-written artifacts (smallest seq) until the new one fits, and a
  single artifact larger than the whole ceiling is refused (`ShadowError::TooLarge`)
  rather than admitted unbounded. Durable across restart: blobs are written
  temp-then-rename before they become visible, and `open` rebuilds the in-memory
  index (per-key versions, total bytes, next sequence) by scanning the tree. The
  store's only sink is its own directory — it holds no bucket/catalog/origin
  handle, so no path can reach a customer table or bucket. TDD: tests/shadow.rs
  written first (put/get/list/latest round-trip, budget ceiling evict+refuse,
  durability across reopen, the never-writes-outside-its-root property).
- #3: Kept erasure coding as an internal write mode while the owning package became `verglas-write`.
