//! Property tests for the cache's correctness invariants (issue #24).
//!
//! The example-based tests in `engine.rs` pin each invariant at one point; this
//! file drives the **real** [`HybridCacheEngine`] through many randomized,
//! seeded scenarios so a refactor that breaks an invariant in a corner the
//! examples miss still trips a test. Every scenario derives all of its choices
//! (sizes, ranges, fan-out, op mix) from a single seed via a dependency-free
//! SplitMix64 PRNG — the same hand-rolled pattern `verglas-s3`'s
//! `write_ordering.rs` uses (#21) — so a failure reproduces exactly: the seed
//! is printed in every assertion message, and setting `VERGLAS_PROPTEST_SEED`
//! replays that single seed.
//!
//! The invariants covered, one property each:
//!
//! - **ETag immutability**: a block keyed `(bucket, key, etag)` always serves
//!   its own version's bytes; an overwrite behind the cache's back produces a
//!   new key and never mutates the old entry, and no read ever mixes versions.
//! - **Singleflight uniqueness (#19)**: N concurrent identical cold reads share
//!   each fill site. Full and suffix reads issue one GET; exact partial reads
//!   issue one foreground GET plus one aligned background fill.
//! - **No stale read after ack (#21/#124)**: after an overwrite/delete is
//!   durable and the mapping is invalidated (the front-end's ack point), no
//!   read returns pre-write bytes; concurrent readers see only old or new.
//! - **Purge generation semantics (#178)**: after a purge, live bytes read ~0,
//!   the generation advances monotonically, and every prior-generation entry is
//!   unreachable — the next reads are cold backend fills, byte-identical.
//! - **Budget ceilings (#122)**: under a random read workload the DRAM and disk
//!   gauges never exceed the configured budgets.
//!
//! # Running deeper sweeps locally
//!
//! The default iteration counts are tuned to keep the whole file well under a
//! minute in CI. To sweep harder locally, scale every property up:
//!
//! ```text
//! VERGLAS_PROPTEST_ITERS=20 cargo test -p verglas-cache --test property_invariants
//! ```
//!
//! To replay one failing seed (printed in the assertion message):
//!
//! ```text
//! VERGLAS_PROPTEST_SEED=12345 cargo test -p verglas-cache --test property_invariants \
//!     no_stale_read_after_ack_against_the_real_engine
//! ```

use std::ops::Range;
use std::path::Path as StdPath;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use tempfile::TempDir;
use verglas_cache::HybridCacheEngine;
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_core::peer::NoopPeerFetch;
use verglas_core::ring::RendezvousRing;
use verglas_s3::{
    BackendStore, Invalidation, ObjectGet, ObjectMeta, ObjectRead, PassthroughRead, ReadError,
    ReadRange,
};

/// One mebibyte.
const MIB: u64 = 1024 * 1024;
/// The block size these scenarios configure the engine with. Fixed at 8 MiB so
/// the property assertions reason about one known geometry, independent of the
/// product default.
const BLOCK: u64 = 8 * MIB;
/// The bucket every scenario reads through.
const BUCKET: &str = "prop-bucket";

/// A SplitMix64 PRNG: tiny, dependency-free, and fully deterministic from its
/// seed, so a failing randomized scenario reproduces exactly (#21's pattern).
struct Rng(u64);

impl Rng {
    /// Seeds the generator.
    fn new(seed: u64) -> Self {
        Rng(seed)
    }

    /// Next 64-bit value (SplitMix64 finalizer).
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform integer in `lo..=hi`.
    fn in_range(&mut self, lo: u64, hi: u64) -> u64 {
        debug_assert!(lo <= hi);
        lo + self.next_u64() % (hi - lo + 1)
    }

    /// True with probability `1 / den`.
    fn one_in(&mut self, den: u64) -> bool {
        self.next_u64().is_multiple_of(den)
    }
}

/// Scale factor for iteration counts, from `VERGLAS_PROPTEST_ITERS` (default 1).
/// Raising it sweeps every property harder without touching the source.
fn scale() -> u64 {
    std::env::var("VERGLAS_PROPTEST_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(1)
}

/// The seeds a property iterates: `base × scale()` seeds spread across the
/// property's `namespace`, or exactly the one pinned by `VERGLAS_PROPTEST_SEED`.
fn seeds(namespace: u64, base: u64) -> Vec<u64> {
    if let Some(seed) = std::env::var("VERGLAS_PROPTEST_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        return vec![seed];
    }
    (0..base * scale())
        .map(|i| namespace ^ i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .collect()
}

/// Deterministic payload: byte `i` is `(i + salt) % 251` (prime, so no block
/// alignment can mask an offset bug). `salt` distinguishes object versions.
fn pattern(salt: u64, len: u64) -> Bytes {
    (0..len)
        .map(|i| ((i + salt) % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// The logical key for an object in the test bucket.
fn key(k: &str) -> CacheKey {
    CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: BUCKET.to_owned(),
        key: k.to_owned(),
    }
}

/// Cache config over a temp dir with explicit budgets; defaults elsewhere.
fn cache_config(dir: &TempDir, capacity: u64, dram: u64) -> CacheConfig {
    CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(capacity),
        dram_bytes: ByteSize(dram),
        data_block_bytes: ByteSize(BLOCK),
        ..CacheConfig::default()
    }
}

/// Seeds (or overwrites) an object in the in-memory origin, out of band —
/// exactly a write that lands behind the cache's back.
async fn put_object(store: &Arc<InMemory>, k: &str, bytes: Bytes) {
    store
        .put(&Path::from(k), PutPayload::from(bytes))
        .await
        .expect("seed object");
}

/// Independent counts of the backend GETs an engine issued — the ground truth
/// the singleflight and purge properties assert against.
#[derive(Clone, Default)]
struct BackendCalls {
    /// Number of `get` calls (fills and passthroughs alike).
    gets: Arc<AtomicU64>,
}

impl BackendCalls {
    /// Current get count.
    fn gets(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }
}

/// An `ObjectRead` that counts calls before delegating.
struct CountingRead<R> {
    /// The wrapped reader.
    inner: R,
    /// Shared call counts.
    calls: BackendCalls,
}

impl<R: ObjectRead> ObjectRead for CountingRead<R> {
    /// Counts, then delegates.
    async fn get(&self, k: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.calls.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get(k, range).await
    }

    /// Delegates (only backend GETs are counted for these properties).
    async fn head(&self, k: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.inner.head(k).await
    }
}

/// The engine type these properties drive: the real hybrid engine over a
/// call-counting passthrough to the in-memory origin, single-node ring.
type CountedEngine =
    HybridCacheEngine<CountingRead<PassthroughRead>, NoopPeerFetch, RendezvousRing>;

/// Builds the real engine over a fresh in-memory origin: returns the origin
/// (for out-of-band seeding), the engine, and the backend call counts.
async fn build_engine(
    dir: &TempDir,
    capacity: u64,
    dram: u64,
) -> (Arc<InMemory>, CountedEngine, BackendCalls) {
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let backend = CountingRead {
        inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        calls: calls.clone(),
    };
    let engine = HybridCacheEngine::single_node(backend, &cache_config(dir, capacity, dram))
        .await
        .expect("build engine");
    (store, engine, calls)
}

/// Builds the real engine with an explicit mutable-mapping TTL (#14) — the
/// #213 atomicity property uses a long `ttl_secs` so warm mutable mappings are
/// served without a #14 revalidation, isolating the read-side version commit.
async fn build_engine_ttl(
    dir: &TempDir,
    capacity: u64,
    dram: u64,
    ttl_secs: u64,
) -> (Arc<InMemory>, CountedEngine, BackendCalls) {
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let backend = CountingRead {
        inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        calls: calls.clone(),
    };
    let config = CacheConfig {
        mutable_mapping_ttl_secs: ttl_secs,
        ..cache_config(dir, capacity, dram)
    };
    let engine = HybridCacheEngine::single_node(backend, &config)
        .await
        .expect("build engine");
    (store, engine, calls)
}

/// Reads a range through any engine, collecting the streamed body into one
/// buffer. Returns the metadata, resolved range, and bytes.
async fn read_all<E: ObjectRead>(
    engine: &E,
    k: &CacheKey,
    range: ReadRange,
) -> Result<(ObjectMeta, Range<u64>, Bytes), ReadError> {
    let got = engine.get(k, range).await?;
    let mut buf = BytesMut::new();
    let mut body = got.body;
    while let Some(chunk) = body.try_next().await? {
        buf.extend_from_slice(&chunk);
    }
    Ok((got.meta, got.range, buf.freeze()))
}

/// Returns a file's size or sums every file under a directory — the on-disk gauge.
fn dir_bytes(path: &StdPath) -> u64 {
    if path.is_file() {
        return std::fs::metadata(path).expect("file metadata").len();
    }
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries {
        let entry = entry.expect("dir entry");
        let meta = entry.metadata().expect("entry metadata");
        if meta.is_dir() {
            total += dir_bytes(&entry.path());
        } else {
            total += meta.len();
        }
    }
    total
}

/// Resolves an HTTP-shaped [`ReadRange`] against a known object size into the
/// concrete half-open byte window the engine serves — the oracle the ETag
/// property compares against.
fn resolve(range: ReadRange, total: u64) -> Range<u64> {
    match range {
        ReadRange::Full => 0..total,
        ReadRange::From(first) => first.min(total)..total,
        ReadRange::Bounded(first, last) => first.min(total)..last.saturating_add(1).min(total),
        ReadRange::Suffix(len) => total.saturating_sub(len)..total,
    }
}

/// Picks a random satisfiable read shape for an object of `size` bytes: full,
/// open-ended, bounded, or a suffix. Never produces an unsatisfiable range
/// (`first >= size`), so a 416 never masks a byte-comparison bug.
fn random_range(rng: &mut Rng, size: u64) -> ReadRange {
    match rng.in_range(0, 3) {
        0 => ReadRange::Full,
        1 => ReadRange::From(rng.in_range(0, size - 1)),
        2 => {
            let first = rng.in_range(0, size - 1);
            let last = rng.in_range(first, size - 1);
            ReadRange::Bounded(first, last)
        }
        _ => ReadRange::Suffix(rng.in_range(1, size)),
    }
}

// --- Property 1: ETag immutability ------------------------------------------

/// A block keyed `(bucket, key, etag)` always serves its own version's bytes,
/// and an overwrite produces a new key rather than mutating the old entry. The
/// property drives both halves against the real engine:
///
/// - **A warmed version is immutable**: once a version is cached, reads of any
///   range (whole, open-ended, bounded, suffix) return exactly that version's
///   bytes, sequentially and concurrently — a cached block never varies.
/// - **Overwrites rotate cleanly**: a write-through (overwrite the origin, then
///   invalidate — the front-end's contract) re-resolves the key to the new
///   ETag, so reads return the new version whole and never a byte of the old.
///
/// The old version's blocks are addressed by the old ETag; once the mapping
/// rotates, they are unreachable through the key — so serving the new version
/// after a rotation is exactly the proof that an ETag-keyed block never leaks
/// into a different version's read.
///
/// Randomized over object sizes (0–2 blocks), version count, and read shapes.
#[tokio::test(flavor = "multi_thread")]
async fn immutable_blocks_never_serve_cross_version_bytes() {
    let dir = TempDir::new().expect("temp dir");
    // One engine, distinct key per scenario: a fresh key is always a cold read.
    let (store, engine, _calls) = build_engine(&dir, 256 * MIB, 160 * MIB).await;
    let engine = Arc::new(engine);

    for seed in seeds(0x1234_0000, 40) {
        let mut rng = Rng::new(seed);
        let name = format!("v/{seed}.parquet");
        let k = key(&name);
        let versions = rng.in_range(2, 4);

        for v in 1..=versions {
            let size = rng.in_range(1, 2 * BLOCK);
            let body = pattern(seed ^ (v << 32), size);

            // A write-through: overwrite the origin, then invalidate. For v1 the
            // key is cold, so the invalidation is a harmless no-op; from v2 on it
            // is the rotation whose cleanliness the reads below assert.
            put_object(&store, &name, body.clone()).await;
            engine
                .invalidate(std::slice::from_ref(&k))
                .await
                .expect("invalidate");

            // Warm the whole object: this must be the new version whole, never a
            // mix with any prior version's (now unreachable) blocks.
            let (_, _, warm) = read_all(&*engine, &k, ReadRange::Full)
                .await
                .unwrap_or_else(|e| panic!("seed {seed} v{v}: warm read failed: {e:?}"));
            assert_eq!(
                warm, body,
                "seed {seed} v{v}: read after rotation did not serve the new version whole"
            );

            // The warmed version is immutable: every range read returns exactly
            // its bytes.
            for _ in 0..3 {
                let range = random_range(&mut rng, size);
                let want = body.slice(as_slice(resolve(range, size)));
                let (_, _, got) = read_all(&*engine, &k, range)
                    .await
                    .unwrap_or_else(|e| panic!("seed {seed} v{v}: range read failed: {e:?}"));
                assert_eq!(
                    got, want,
                    "seed {seed} v{v}: a read of a fixed immutable version returned \
                     different bytes (range {range:?}, size {size})"
                );
            }

            // Concurrent reads of the same warmed version must all agree — a
            // cached block is byte-stable under contention.
            let mut tasks = Vec::new();
            for _ in 0..4 {
                let engine = Arc::clone(&engine);
                let k = k.clone();
                let want = body.clone();
                tasks.push(tokio::spawn(async move {
                    let (_, _, got) = read_all(&*engine, &k, ReadRange::Full)
                        .await
                        .expect("concurrent read");
                    got == want
                }));
            }
            for t in tasks {
                assert!(
                    t.await.expect("read task"),
                    "seed {seed} v{v}: concurrent reads of one version disagreed"
                );
            }
        }
    }
}

/// Converts a `Range<u64>` into the `usize` slice indices `Bytes::slice` wants.
fn as_slice(range: Range<u64>) -> Range<usize> {
    range.start as usize..range.end as usize
}

// --- Property 2: Singleflight uniqueness (#19) ------------------------------

/// A gate that parks the first backend response after it is armed, so a whole
/// stampede of readers attaches to one in-flight fill before it completes.
/// Mirrors `engine.rs`'s `Gate`, kept local (test crates do not share code).
#[derive(Clone)]
struct Gate {
    /// Arms the next backend call to park.
    armed: Arc<AtomicBool>,
    /// Signals the test that a response is parked.
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    /// Permits the test releases to let the parked response through.
    release: Arc<tokio::sync::Semaphore>,
}

impl Gate {
    /// Builds a gate and the receiver the test awaits the park signal on.
    fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<()>) {
        let (entered, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Gate {
                armed: Arc::new(AtomicBool::new(false)),
                entered,
                release: Arc::new(tokio::sync::Semaphore::new(0)),
            },
            rx,
        )
    }

    /// Parks the next backend response.
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Releases one parked response.
    fn open(&self) {
        self.release.add_permits(1);
    }

    /// Parks the calling backend response if the gate is armed (disarming it).
    async fn park(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.send(()).expect("test alive");
            self.release
                .acquire()
                .await
                .expect("semaphore open")
                .forget();
        }
    }
}

/// An `ObjectRead` that resolves each response first (fixing its content at
/// call time) and then parks it while the gate is armed.
struct ParkedRead<R> {
    /// The wrapped reader.
    inner: R,
    /// The park/release gate.
    gate: Gate,
}

impl<R: ObjectRead> ObjectRead for ParkedRead<R> {
    /// Resolves, then parks while armed.
    async fn get(&self, k: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let got = self.inner.get(k, range).await;
        self.gate.park().await;
        got
    }

    /// Resolves, then parks while armed.
    async fn head(&self, k: &CacheKey) -> Result<ObjectMeta, ReadError> {
        let meta = self.inner.head(k).await;
        self.gate.park().await;
        meta
    }
}

/// The parked-engine type for the singleflight property.
type ParkedEngine =
    HybridCacheEngine<ParkedRead<CountingRead<PassthroughRead>>, NoopPeerFetch, RendezvousRing>;

/// The cumulative count of dedup collapses across both fill sites (block fills
/// and buffered suffix fills). The engine is reused across seeds, so this is
/// read against a per-seed baseline — never as an absolute.
fn dedup_collapses(engine: &ParkedEngine) -> u64 {
    let c = engine.counters().snapshot();
    c.deduped_fills + c.deduped_suffix_fills
}

/// Spins (yielding) until the engine has recorded `target` *new* dedup collapses
/// since `baseline`, so the parked winner is released only once every straggler
/// has attached to the one in-flight fill. The baseline matters: the engine is
/// reused across seeds and the counter is cumulative, so comparing the absolute
/// count to a per-seed fan-out would return immediately after the first seed and
/// release the winner before this seed's readers attach. Bounded so a logic bug
/// reports the seed instead of hanging CI forever.
async fn wait_for_dedup(engine: &ParkedEngine, baseline: u64, target: u64, seed: u64) {
    let mut spins = 0u64;
    loop {
        let collapses = dedup_collapses(engine).saturating_sub(baseline);
        if collapses >= target {
            return;
        }
        spins += 1;
        assert!(
            spins < 50_000_000,
            "seed {seed}: only {collapses}/{target} readers attached to the in-flight fill",
        );
        tokio::task::yield_now().await;
    }
}

/// N concurrent identical cold reads of one object collapse at each fill site,
/// and every caller gets identical bytes. Full and suffix reads issue one GET;
/// an exact single-block range issues one shared foreground GET plus one shared
/// aligned background fill. The winner's foreground request is parked so every
/// other reader attaches before it completes.
///
/// Randomized over fan-out (2–12), single-block object size, and read shape —
/// including small suffixes, which exercise the buffered-suffix dedup path.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cold_reads_collapse_to_one_backend_get() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let (gate, mut entered) = Gate::new();
    let backend = ParkedRead {
        inner: CountingRead {
            inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
            calls: calls.clone(),
        },
        gate: gate.clone(),
    };
    let engine: ParkedEngine =
        HybridCacheEngine::single_node(backend, &cache_config(&dir, 128 * MIB, 128 * MIB))
            .await
            .expect("build engine");

    for seed in seeds(0x2345_0000, 28) {
        let mut rng = Rng::new(seed);
        let name = format!("sf/{seed}.bin");
        let k = key(&name);
        // Single-block object so every covering read needs exactly one fill.
        let size = rng.in_range(1, BLOCK);
        let body = pattern(seed, size);
        put_object(&store, &name, body.clone()).await;

        // A read shape that resolves to exactly one covering block.
        let range = match rng.in_range(0, 2) {
            0 => ReadRange::Full,
            1 => {
                let first = rng.in_range(0, size - 1);
                let last = rng.in_range(first, size - 1);
                ReadRange::Bounded(first, last)
            }
            // Small suffix: <= one block, so it flows through the buffered
            // suffix fill and its dedicated dedup path.
            _ => ReadRange::Suffix(rng.in_range(1, size)),
        };
        let want = body.slice(as_slice(resolve(range, size)));

        let n = rng.in_range(2, 12);
        let gets_before = calls.gets();
        // Baseline the cumulative dedup counter for THIS seed: the engine is
        // reused across seeds, so the parked-winner release must wait for N-1
        // *new* collapses, not an absolute count that is already satisfied.
        let dedup_before = dedup_collapses(&engine);
        gate.arm();
        let mut handles = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let engine = engine.clone();
            let k = k.clone();
            handles.push(tokio::spawn(
                async move { read_all(&engine, &k, range).await },
            ));
        }

        // The winner's GET is parked; wait until the other N-1 have attached,
        // then release it.
        entered.recv().await.expect("winner parked");
        wait_for_dedup(&engine, dedup_before, n - 1, seed).await;

        // Mid-flight, with the winner still parked and all N-1 stragglers
        // attached: exactly one backend GET has been issued. This is the #214
        // check made deterministic — a straggler that resolved the mapping from
        // the winner's fill and then took the *other* fill site (a covering
        // block fill, or the block ladder for a data suffix) would have issued a
        // second GET here, before release. Both fill sites now collapse onto the
        // one in-flight fill, so the count is 1 while it is still in flight, not
        // just after it drains.
        let in_flight = calls.gets() - gets_before;
        assert_eq!(
            in_flight, 1,
            "seed {seed}: {n} concurrent cold reads (range {range:?}) issued {in_flight} \
             backend GETs while the winner was still parked — the first-fill and \
             block-fill paths did not collapse to one (#214)"
        );

        gate.open();

        for h in handles {
            let (_, _, got) = h
                .await
                .expect("reader join")
                .unwrap_or_else(|e| panic!("seed {seed}: concurrent read failed: {e:?}"));
            assert_eq!(got, want, "seed {seed}: a caller got non-identical bytes");
        }
        let expected_gets = match range {
            ReadRange::Bounded(first, last) if first / BLOCK == last / BLOCK => 2,
            _ => 1,
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while engine.inflight_len() != 0 || calls.gets() - gets_before < expected_gets {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!("seed {seed}: the collapsed foreground/background fills did not complete")
        });
        let issued = calls.gets() - gets_before;
        assert_eq!(
            issued, expected_gets,
            "seed {seed}: {n} concurrent cold reads (range {range:?}) issued {issued} \
             backend GETs, not the expected {expected_gets} collapsed requests"
        );
        assert_eq!(
            engine.inflight_len(),
            0,
            "seed {seed}: the in-flight map must drain to empty"
        );
    }
}

/// Deterministically forces the exact first-fill/block-fill interleaving #214
/// describes and asserts it collapses to one backend GET.
///
/// The property above parks the winner and holds it until every reader attaches
/// to the *same* fill, so its readers never split across the first-fill and
/// block-fill sites — that is what the singleflight is supposed to guarantee,
/// but it cannot exhibit the race the fix closes. This test builds the split by
/// hand:
///
/// 1. Reader A starts a cold read of block 0. It finds no mapping, enters the
///    first-fill probe, registers it on the `(object, index)` probe map, and its
///    backend GET parks (the winner).
/// 2. With A's probe in flight, the mapping is resolved out of band via a HEAD
///    (which admits it into the metadata cache but issues no GET).
/// 3. Reader B now reads the same block. It sees the freshly admitted mapping,
///    so it skips the probe and takes the block-fill path — the *other* fill
///    site, keyed by the full `BlockKey`. This is precisely the window where B
///    used to issue its own backend GET because the two in-flight maps could not
///    see each other.
///
/// With the fix, B's block fill consults the probe map first, finds A's probe in
/// flight for the same block, and attaches to it. So while A is still parked,
/// exactly one backend GET has been issued, both readers get the same bytes, and
/// the in-flight maps drain to empty.
#[tokio::test(flavor = "multi_thread")]
async fn parked_first_fill_and_block_fill_collapse_to_one_get() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let (gate, mut entered) = Gate::new();
    let backend = ParkedRead {
        inner: CountingRead {
            inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
            calls: calls.clone(),
        },
        gate: gate.clone(),
    };
    let engine: ParkedEngine =
        HybridCacheEngine::single_node(backend, &cache_config(&dir, 128 * MIB, 128 * MIB))
            .await
            .expect("build engine");

    // A single-block object so block 0 is the whole read.
    let name = "sf/parked-boundary.bin";
    let k = key(name);
    let size = 3 * MIB;
    let body = pattern(0xB0DE, size);
    put_object(&store, name, body.clone()).await;

    let gets_before = calls.gets();
    gate.arm();

    // Reader A: cold read. Enters the first-fill probe and parks at the GET.
    let a = {
        let engine = engine.clone();
        let k = k.clone();
        tokio::spawn(async move { read_all(&engine, &k, ReadRange::Full).await })
    };
    // A's probe is registered and its GET is parked.
    entered.recv().await.expect("reader A parked in the probe");

    // Resolve the mapping out of band while A's probe is still in flight. The
    // gate disarmed when A parked, so this HEAD is not parked; it issues no GET
    // (HEAD only), and it admits the mapping into the metadata cache.
    engine.head(&k).await.expect("head resolves the mapping");

    // Reader B: now that the mapping is resolved, B skips the probe and takes the
    // block-fill path — the other fill site. It must attach to A's in-flight
    // probe rather than issue a second GET.
    let b = {
        let engine = engine.clone();
        let k = k.clone();
        tokio::spawn(async move { read_all(&engine, &k, ReadRange::Full).await })
    };
    // Wait until B has attached to A's probe (one dedup collapse), so the check
    // below observes the state with A still parked.
    wait_for_dedup(&engine, 0, 1, 0).await;

    // THE INVARIANT: with A still parked and B attached, exactly one GET issued.
    let in_flight = calls.gets() - gets_before;
    assert_eq!(
        in_flight, 1,
        "the block-fill reader issued its own GET across the first-fill/block-fill \
         boundary instead of joining the in-flight probe (#214): {in_flight} GETs"
    );

    // Release A; both readers get the same bytes, one GET total, maps drained.
    gate.open();
    let (_, _, got_a) = a.await.expect("join A").expect("A read ok");
    let (_, _, got_b) = b.await.expect("join B").expect("B read ok");
    assert_eq!(got_a, body, "reader A got wrong bytes");
    assert_eq!(got_b, body, "reader B got wrong bytes");
    assert_eq!(
        calls.gets() - gets_before,
        1,
        "the two readers must collapse to exactly one backend GET"
    );
    assert_eq!(
        engine.inflight_len(),
        0,
        "in-flight maps must drain to empty"
    );
}

// --- Property 3: No stale read after ack (#21/#124) -------------------------

/// The mutating op a scenario applies, both models of a write that has become
/// durable on the origin.
#[derive(Clone, Copy, Debug)]
enum WriteOp {
    /// Overwrite the key with new bytes.
    Overwrite,
    /// Delete the key.
    Delete,
}

/// After a write is durable on the origin and the mapping is invalidated (the
/// front-end's ack point — durable, then invalidate, then ack), no read ever
/// returns the pre-write bytes. This is #21 driven against the **real** engine
/// (the `write_ordering.rs` model stands in only where the engine cannot be
/// depended on): warm the key, race a burst of readers against the write, and
/// assert the first post-ack read reflects the write while every concurrent
/// observation is old-or-new, never torn or stale-after-ack.
///
/// Immutable-classified keys (`*.parquet`) are used deliberately: their mapping
/// is pinned, so the *only* way a read can ever leave the old version is the
/// invalidation under test — there is no TTL revalidation to muddy the result.
///
/// Randomized over op (overwrite/delete), payload sizes, reader fan-out, and
/// reads per reader.
#[tokio::test(flavor = "multi_thread")]
async fn no_stale_read_after_ack_against_the_real_engine() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _calls) = build_engine(&dir, 256 * MIB, 160 * MIB).await;
    let engine = Arc::new(engine);

    for seed in seeds(0x3456_0000, 32) {
        let mut rng = Rng::new(seed);
        let name = format!("w/{seed}.parquet");
        let k = key(&name);
        let op = if rng.one_in(3) {
            WriteOp::Delete
        } else {
            WriteOp::Overwrite
        };
        let old = pattern(seed, rng.in_range(1, 2 * BLOCK));
        let new = pattern(seed ^ 0xFFFF, rng.in_range(1, 2 * BLOCK));

        // Warm the cache with the old version so a stale post-ack read is
        // *possible* if the ordering were wrong.
        put_object(&store, &name, old.clone()).await;
        let (_, _, warm) = read_all(&engine, &k, ReadRange::Full)
            .await
            .unwrap_or_else(|e| panic!("seed {seed}: warm read failed: {e:?}"));
        assert_eq!(warm, old, "seed {seed}: warm read must see the old version");

        // Fan out readers that race the write, each recording what it observed.
        let readers = rng.in_range(2, 6);
        let reads_each = rng.in_range(4, 16);
        let mut tasks = Vec::with_capacity(readers as usize);
        for _ in 0..readers {
            let engine = Arc::clone(&engine);
            let k = k.clone();
            tasks.push(tokio::spawn(async move {
                let mut seen = Vec::new();
                for _ in 0..reads_each {
                    seen.push(observe(&engine, &k).await);
                    tokio::task::yield_now().await;
                }
                seen
            }));
        }

        // The durable write, then the invalidation (the ack fence), concurrent
        // with the readers.
        match op {
            WriteOp::Overwrite => put_object(&store, &name, new.clone()).await,
            WriteOp::Delete => store
                .delete(&Path::from(name.as_str()))
                .await
                .expect("delete"),
        }
        engine
            .invalidate(std::slice::from_ref(&k))
            .await
            .expect("invalidate");

        // THE INVARIANT: the first read strictly after the ack.
        let post_ack = observe(&engine, &k).await;
        match op {
            WriteOp::Overwrite => assert_eq!(
                post_ack,
                Observation::Bytes(new.clone()),
                "seed {seed}: post-ack read returned stale bytes"
            ),
            WriteOp::Delete => assert_eq!(
                post_ack,
                Observation::Missing,
                "seed {seed}: post-ack read found the deleted key"
            ),
        }

        // Every concurrent observation must be self-consistent: old, the
        // write's outcome, or a transient error (a fill racing the write may
        // degrade to a backend error — slow/failed is acceptable) — but never a
        // torn or third *value*. A byte string equal to neither version is the
        // violation this rejects.
        for task in tasks {
            for obs in task.await.expect("reader task") {
                let ok = match op {
                    WriteOp::Overwrite => {
                        obs == Observation::Bytes(old.clone())
                            || obs == Observation::Bytes(new.clone())
                            || obs == Observation::Errored
                    }
                    WriteOp::Delete => {
                        obs == Observation::Bytes(old.clone())
                            || obs == Observation::Missing
                            || obs == Observation::Errored
                    }
                };
                assert!(
                    ok,
                    "seed {seed} ({op:?}): a concurrent read saw an inconsistent value: {obs:?}"
                );
            }
        }
    }
}

/// One observed read outcome: the whole-object bytes, a not-found, or a
/// non-fatal error (a fill racing a delete may surface a transient backend
/// error, which is not a staleness violation).
#[derive(Clone, Debug, PartialEq, Eq)]
enum Observation {
    /// A 200 with these full-object bytes.
    Bytes(Bytes),
    /// The key was absent (a 404).
    Missing,
    /// A non-`NoSuchKey` read error.
    Errored,
}

/// Reads the whole object once, mapping the result into an [`Observation`].
async fn observe(engine: &CountedEngine, k: &CacheKey) -> Observation {
    match read_all(engine, k, ReadRange::Full).await {
        Ok((_, _, bytes)) => Observation::Bytes(bytes),
        Err(ReadError::NoSuchKey) => Observation::Missing,
        Err(_) => Observation::Errored,
    }
}

// --- Property 3b: Read atomicity across a block boundary (#213) --------------

/// A multi-block read of a **mutable** object concurrent with an overwrite is
/// always wholly one version — never a torn mix of blocks from two versions.
///
/// This is the #213 invariant. A read emits its covering blocks in order while
/// a bounded look-ahead may fill later blocks first: a warm cache hit returns
/// the mapping's version, a fill returns whatever the backend now holds. A
/// mutable object whose mapping is still warm
/// can therefore tear — old cached blocks mixed with a fill of the new version.
/// The fix commits the object version once, before the body streams, so every
/// covering block resolves at one ETag.
///
/// The property, driven against the real engine over many seeds: warm the old
/// version of a multi-block mutable key (its mapping stays warm — no
/// invalidation), then race a burst of readers against a plain origin overwrite
/// (no invalidation, so only the read's own version commit can keep it
/// coherent). Every observation must be the whole old version, the whole new
/// version, or a transient error — never a byte string equal to neither (a
/// tear). Unlike property 3, this uses `*.bin` (mutable) keys and never
/// invalidates: it isolates the read-side atomicity commit from the write-side
/// ack fence.
///
/// Randomized over payload sizes (always ≥ 2 blocks, so every read crosses a
/// boundary), reader fan-out, and reads per reader.
#[tokio::test(flavor = "multi_thread")]
async fn multi_block_read_is_atomic_under_concurrent_overwrite() {
    let dir = TempDir::new().expect("temp dir");
    // A long mutable TTL: the #14 TTL never fires during the test, so a warm
    // mutable mapping is served straight from cache with no revalidation. The
    // ONLY thing that can keep a multi-block read coherent across the overwrite
    // is the #213 version commit — without it, block 0 is a warm old hit and a
    // later block fills at the new version, a tear. (With TTL 0 the #14
    // revalidation would mask the bug, so it must be large here.)
    let (store, engine, _calls) = build_engine_ttl(&dir, 256 * MIB, 160 * MIB, 3600).await;
    let engine = Arc::new(engine);

    for seed in seeds(0x2137_0000, 24) {
        let mut rng = Rng::new(seed);
        let name = format!("a/{seed}.bin");
        let k = key(&name);
        // Always at least two covering blocks: the read must cross a boundary.
        let old = pattern(seed, rng.in_range(BLOCK + 1, 3 * BLOCK));
        let new = pattern(seed ^ 0xFFFF, rng.in_range(BLOCK + 1, 3 * BLOCK));

        // Warm the old version so its mapping and block 0 are cached; a torn
        // read is only possible once a warm old version exists to mix with.
        put_object(&store, &name, old.clone()).await;
        let (_, _, warm) = read_all(&engine, &k, ReadRange::Full)
            .await
            .unwrap_or_else(|e| panic!("seed {seed}: warm read failed: {e:?}"));
        assert_eq!(warm, old, "seed {seed}: warm read must see the old version");

        // Fan out readers racing the overwrite, each recording what it saw.
        let readers = rng.in_range(2, 6);
        let reads_each = rng.in_range(4, 12);
        let mut tasks = Vec::with_capacity(readers as usize);
        for _ in 0..readers {
            let engine = Arc::clone(&engine);
            let k = k.clone();
            tasks.push(tokio::spawn(async move {
                let mut seen = Vec::new();
                for _ in 0..reads_each {
                    seen.push(observe(&engine, &k).await);
                    tokio::task::yield_now().await;
                }
                seen
            }));
        }

        // The overwrite, durable on the origin, with NO invalidation — the read
        // path's own version commit is what must keep every read coherent.
        put_object(&store, &name, new.clone()).await;

        // THE INVARIANT: every observation is wholly one version (or a
        // transient error), never a third value spliced from both.
        for task in tasks {
            for obs in task.await.expect("reader task") {
                let ok = obs == Observation::Bytes(old.clone())
                    || obs == Observation::Bytes(new.clone())
                    || obs == Observation::Errored;
                assert!(
                    ok,
                    "seed {seed}: a multi-block read tore across the block boundary \
                     (returned bytes equal to neither the whole old nor the whole \
                     new version)"
                );
            }
        }

        // After the race settles, the object reads wholly-new (the committed
        // current version), never a lingering torn or stale value. Compared as
        // a boolean so a failure does not dump multi-megabyte bodies.
        let settled = observe(&engine, &k).await;
        assert!(
            settled == Observation::Bytes(new.clone()),
            "seed {seed}: settled read must serve the whole new version, got {}",
            match &settled {
                Observation::Bytes(b) if *b == old => "the whole old version".to_owned(),
                Observation::Bytes(b) => format!("{} bytes of neither version", b.len()),
                other => format!("{other:?}"),
            }
        );
    }
}

// --- Property 4: Purge generation semantics (#178) --------------------------

/// After a purge, the live gauge reads ~0, the generation advances by exactly
/// one, and every prior-generation entry is unreachable: re-reading the same
/// keys issues fresh cold backend fills and returns byte-identical bytes. Run
/// on one engine across many seeds, so each purge is asserted to advance the
/// generation monotonically from the last.
///
/// Randomized over the working-set size and per-object sizes.
#[tokio::test(flavor = "multi_thread")]
async fn purge_strands_the_old_generation_and_next_reads_are_cold() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = build_engine(&dir, 512 * MIB, 200 * MIB).await;

    let mut expected_generation = 0u64;
    for seed in seeds(0x4567_0000, 20) {
        let mut rng = Rng::new(seed);
        let count = rng.in_range(2, 6);
        let objects: Vec<(String, Bytes)> = (0..count)
            .map(|i| {
                let name = format!("p/{seed}-{i}.bin");
                let body = pattern(seed ^ i, rng.in_range(1, BLOCK + BLOCK / 2));
                (name, body)
            })
            .collect();

        // Warm the working set: every object resident under the current
        // generation.
        for (name, body) in &objects {
            put_object(&store, name, body.clone()).await;
            let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: warm failed: {e:?}"));
            assert_eq!(&got, body, "seed {seed}: warm read must match origin");
        }
        engine.flush().await;

        // Purge: the generation advances by one and the live gauge resets.
        let report = engine.purge().await;
        expected_generation += 1;
        assert_eq!(
            report.generation, expected_generation,
            "seed {seed}: purge must advance the generation by exactly one"
        );
        assert_eq!(
            engine.dram_live_bytes(),
            0,
            "seed {seed}: no bytes may be live under the new generation right after purge"
        );

        // Every re-read is a cold backend fill (old-generation entries are
        // unreachable) and byte-identical to the origin.
        for (name, body) in &objects {
            let covering = covering_blocks(body.len() as u64);
            let before = calls.gets();
            let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: post-purge read failed: {e:?}"));
            assert_eq!(
                &got, body,
                "seed {seed}: post-purge read must be byte-identical to the origin"
            );
            let issued = calls.gets() - before;
            assert_eq!(
                issued, covering,
                "seed {seed}: post-purge read of {name} issued {issued} backend GETs, \
                 expected {covering} cold fills (old generation must be unreachable)"
            );
        }
    }
}

/// Number of covering blocks for a whole-object read of `size` bytes (at least
/// one, even for an empty object).
fn covering_blocks(size: u64) -> u64 {
    size.div_ceil(BLOCK).max(1)
}

// --- Property 5: Budget ceilings under random workloads (#122) --------------

/// Under a random read workload over a keyspace larger than the budget, the
/// DRAM and disk gauges never exceed the configured ceilings. Budgets are hard
/// ceilings (a standing invariant): foyer preallocates the disk device and
/// evicts per shard to stay under the DRAM budget, so this checks the gauges at
/// many points through a churny, seeded workload rather than at one endpoint.
///
/// A handful of budget shapes are used (each needs a fresh engine, which
/// preallocates its device); the workload within each is fully randomized.
#[tokio::test(flavor = "multi_thread")]
async fn dram_and_disk_gauges_never_exceed_budgets() {
    // (capacity_bytes, dram_bytes). DRAM >= the 80 MiB engine minimum; disk >=
    // the four-block minimum. Kept modest so device preallocation stays cheap.
    let budgets = [
        (64 * MIB, 96 * MIB),
        (96 * MIB, 128 * MIB),
        (128 * MIB, 112 * MIB),
    ];

    for (config_index, &(capacity, dram)) in budgets.iter().enumerate() {
        // One seed sweep per budget shape; scale() lengthens each workload.
        for seed in seeds(0x5678_0000 ^ config_index as u64, 2) {
            let dir = TempDir::new().expect("temp dir");
            let (store, engine, _calls) = build_engine(&dir, capacity, dram).await;
            let blocks_dir = dir.path().join("blocks.data");
            // The DRAM caches get the budget minus the fixed 48 MiB disk-write
            // pipeline reservation (16 MiB flush pool + 32 MiB submit queue).
            let dram_cache_budget = dram - 48 * MIB;

            let mut rng = Rng::new(seed);
            // A keyspace whose warm set dwarfs both budgets, driving eviction.
            let keyspace = rng.in_range(24, 40);

            // Seed every key exactly once, up front: each object's size and
            // bytes are fixed for the whole workload, so it never changes behind
            // the cache (that is a separate property) and its ETag stays stable.
            // Mostly single-block, occasionally two, so the warm set is several
            // times either budget.
            let bodies: Vec<Bytes> = (0..keyspace)
                .map(|idx| {
                    let mut kr = Rng::new(0xB0D0_0000 ^ idx);
                    let size = if kr.one_in(4) {
                        kr.in_range(BLOCK, 2 * BLOCK)
                    } else {
                        kr.in_range(1, BLOCK)
                    };
                    pattern(idx, size)
                })
                .collect();
            for (idx, body) in bodies.iter().enumerate() {
                put_object(&store, &format!("ws/{idx}.bin"), body.clone()).await;
            }

            let ops = 30 * scale();
            for op in 0..ops {
                let idx = rng.in_range(0, keyspace - 1);
                let name = format!("ws/{idx}.bin");
                let body = &bodies[idx as usize];
                let size = body.len() as u64;

                let range = random_range(&mut rng, size);
                let want = body.slice(as_slice(resolve(range, size)));
                let (_, _, got) = read_all(&engine, &key(&name), range)
                    .await
                    .unwrap_or_else(|e| panic!("seed {seed} op {op}: read failed: {e:?}"));
                assert_eq!(
                    got, want,
                    "seed {seed} op {op}: served bytes must match origin"
                );

                // Occasionally flush so queued disk writes land, then check both
                // ceilings under the resulting pressure.
                if rng.one_in(4) {
                    engine.flush().await;
                }
                assert!(
                    engine.dram_usage() <= dram_cache_budget,
                    "seed {seed} op {op}: DRAM usage {} exceeds the {dram_cache_budget}-byte \
                     ceiling (budget dram={dram})",
                    engine.dram_usage()
                );
                assert!(
                    dir_bytes(&blocks_dir) <= capacity,
                    "seed {seed} op {op}: disk usage {} exceeds the {capacity}-byte capacity",
                    dir_bytes(&blocks_dir)
                );
            }

            // Final settle: flush and re-check both ceilings hold at rest.
            engine.flush().await;
            assert!(
                engine.dram_usage() <= dram_cache_budget,
                "seed {seed}: DRAM over the ceiling after the final flush"
            );
            assert!(
                dir_bytes(&blocks_dir) <= capacity,
                "seed {seed}: disk over capacity after the final flush"
            );
        }
    }
}
