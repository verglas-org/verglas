//! Integration tests for the hybrid cache engine (issue #12), mapped to the
//! acceptance criteria: byte-identical hit/miss paths against the origin,
//! zero backend requests on re-read, DRAM spill to the disk tier, the
//! non-cacheable (no-ETag) passthrough, the hard disk-capacity ceiling,
//! refetch after eviction, ETag-change coherence, the multi-TiB backing-file
//! layout, and ring/peer discipline. Origin is `object_store`'s in-memory
//! backend; disk tiers live in temp dirs.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use tempfile::TempDir;
use verglas_cache::{EngineError, HybridCacheEngine};
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_core::node::NodeId;
use verglas_core::peer::{PeerFetch, PeerFetchError};
use verglas_core::ring::{RendezvousRing, Ring};
use verglas_s3::{
    BackendRegistry, BackendStore, Invalidation, ObjectGet, ObjectMeta, ObjectRead,
    PassthroughRead, ReadError, ReadRange, Revalidation,
};

/// The bucket the in-memory origin serves.
const BUCKET: &str = "test-bucket";
/// One mebibyte, for readable object sizes.
const MIB: u64 = 1024 * 1024;
/// Legacy geometry used by the broad engine-invariant suite. Dedicated tests
/// below exercise the 2 MiB product default and tuned geometries explicitly.
const TEST_BLOCK_BYTES: u64 = 8 * MIB;

/// Deterministic payload: byte `i` is `(i + seed) % 251` (prime, so block
/// alignment can never mask offset bugs).
fn pattern(seed: u64, len: u64) -> Bytes {
    (0..len)
        .map(|i| ((i + seed) % 251) as u8)
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

/// Cache config over a temp dir with explicit budgets and scan-resistant
/// admission (#15) at its default (on).
fn cache_config(dir: &TempDir, capacity: u64, dram: u64) -> CacheConfig {
    CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(capacity),
        dram_bytes: ByteSize(dram),
        data_block_bytes: ByteSize(TEST_BLOCK_BYTES),
        ..CacheConfig::default()
    }
}

/// Cache config with an explicit data-block geometry.
fn cache_config_block_size(
    dir: &TempDir,
    capacity: u64,
    dram: u64,
    data_block_bytes: u64,
) -> CacheConfig {
    CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(capacity),
        dram_bytes: ByteSize(dram),
        data_block_bytes: ByteSize(data_block_bytes),
        ..CacheConfig::default()
    }
}

/// Cache config with scan-resistant admission (#15) explicitly toggled — used
/// to build the differential control in the scan-resistance test and to run
/// the eviction-ceiling test against the pre-#15 admit-everything path.
fn cache_config_admission(
    dir: &TempDir,
    capacity: u64,
    dram: u64,
    admission_enabled: bool,
) -> CacheConfig {
    CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(capacity),
        dram_bytes: ByteSize(dram),
        data_block_bytes: ByteSize(TEST_BLOCK_BYTES),
        admission: verglas_core::config::Admission {
            enabled: admission_enabled,
            ..verglas_core::config::Admission::default()
        },
        ..CacheConfig::default()
    }
}

/// Cache config with an explicit mutable-mapping TTL (#14). `ttl_secs = 0`
/// forces a conditional revalidation on every read of a mutable key, which is
/// how these tests exercise expiry deterministically without a wall clock.
fn cache_config_ttl(dir: &TempDir, capacity: u64, dram: u64, ttl_secs: u64) -> CacheConfig {
    CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(capacity),
        dram_bytes: ByteSize(dram),
        data_block_bytes: ByteSize(TEST_BLOCK_BYTES),
        mutable_mapping_ttl_secs: ttl_secs,
        ..CacheConfig::default()
    }
}

/// Seeds an object into the in-memory origin.
async fn seed(store: &Arc<InMemory>, k: &str, bytes: Bytes) {
    store
        .put(&Path::from(k), PutPayload::from(bytes))
        .await
        .expect("seed object");
}

/// Independent counts of the backend requests an engine actually issued —
/// the ground truth the engine's own counters are checked against.
#[derive(Clone, Default)]
struct BackendCalls {
    /// Number of `get` calls (fills and passthroughs alike).
    gets: Arc<AtomicU64>,
    /// Number of `head` calls.
    heads: Arc<AtomicU64>,
    /// Number of `revalidate` calls (#14 conditional `If-None-Match`).
    revalidates: Arc<AtomicU64>,
    /// Exact GET ranges, recorded so geometry tests prove cold fills align to
    /// the configured cache block rather than the historical fixed size.
    get_ranges: Arc<Mutex<Vec<ReadRange>>>,
}

impl BackendCalls {
    /// Loads `(gets, heads)`.
    fn snapshot(&self) -> (u64, u64) {
        (
            self.gets.load(Ordering::Relaxed),
            self.heads.load(Ordering::Relaxed),
        )
    }

    /// Loads the count of conditional revalidations the backend has seen (#14).
    fn revalidates(&self) -> u64 {
        self.revalidates.load(Ordering::Relaxed)
    }

    /// Returns the GET ranges the origin received in request order.
    fn get_ranges(&self) -> Vec<ReadRange> {
        self.get_ranges.lock().expect("range recorder lock").clone()
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
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.calls.gets.fetch_add(1, Ordering::Relaxed);
        self.calls
            .get_ranges
            .lock()
            .expect("range recorder lock")
            .push(range);
        self.inner.get(key, range).await
    }

    /// Counts, then delegates.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.calls.heads.fetch_add(1, Ordering::Relaxed);
        self.inner.head(key).await
    }

    /// Counts, then delegates to the wrapped reader's revalidation (#14) — so
    /// the tests both observe revalidation traffic at the backend boundary and
    /// exercise the passthrough's native conditional `If-None-Match` request.
    async fn revalidate(&self, key: &CacheKey, etag: &str) -> Result<Revalidation, ReadError> {
        self.calls.revalidates.fetch_add(1, Ordering::Relaxed);
        self.inner.revalidate(key, etag).await
    }
}

/// An origin wrapper whose next armed GET yields half of its body immediately
/// and withholds the second half until the test releases it. This makes
/// response progress independently observable from origin completion.
struct SplitBodyRead<R> {
    /// The wrapped origin reader.
    inner: R,
    /// Arms exactly one GET body for splitting.
    armed: Arc<std::sync::atomic::AtomicBool>,
    /// Releases the withheld second body half.
    release: Arc<tokio::sync::Semaphore>,
}

/// An origin wrapper that delays only the tail of block-aligned GETs. Exact
/// partial GETs complete immediately, matching the optimization choice between
/// foreground range serving and background aligned cache population.
struct DelayedAlignedRead<R> {
    /// The wrapped origin reader.
    inner: R,
    /// Releases the delayed aligned-block tail.
    release: Arc<tokio::sync::Semaphore>,
}

impl<R: ObjectRead> ObjectRead for DelayedAlignedRead<R> {
    /// Returns exact partial ranges immediately and gates aligned-block tails.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let mut got = self.inner.get(key, range).await?;
        let bytes = got.body.try_collect::<BytesMut>().await?.freeze();
        if bytes.len() != TEST_BLOCK_BYTES as usize {
            got.body = stream::once(async move { Ok(bytes) }).boxed();
            return Ok(got);
        }
        let first = bytes.slice(..TEST_BLOCK_BYTES as usize / 4);
        let tail = bytes.slice(TEST_BLOCK_BYTES as usize / 4..);
        let release = Arc::clone(&self.release);
        got.body = stream::once(async move { Ok(first) })
            .chain(stream::once(async move {
                release
                    .acquire()
                    .await
                    .map_err(|_| ReadError::Backend("aligned-tail gate closed".into()))?
                    .forget();
                Ok(tail)
            }))
            .boxed();
        Ok(got)
    }

    /// Delegates metadata reads unchanged.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.inner.head(key).await
    }
}

/// An origin wrapper that announces each GET and withholds its whole body.
/// It proves how many aligned fills a multi-block response starts before the
/// first block is allowed to complete.
struct ParkedBodiesRead<R> {
    /// The wrapped origin reader.
    inner: R,
    /// Announces a started origin GET.
    started: tokio::sync::mpsc::UnboundedSender<()>,
    /// Releases parked origin bodies.
    release: Arc<tokio::sync::Semaphore>,
}

/// An origin wrapper that records each requested range and withholds its body.
/// It distinguishes aligned cache fills from exact serve-only reads while the
/// background-fill lane is saturated.
struct RangeRecordingBodiesRead<R> {
    /// The wrapped origin reader.
    inner: R,
    /// Records the range of each started origin GET.
    started: tokio::sync::mpsc::UnboundedSender<ReadRange>,
    /// Releases parked origin bodies.
    release: Arc<tokio::sync::Semaphore>,
}

impl<R: ObjectRead> ObjectRead for RangeRecordingBodiesRead<R> {
    /// Resolves response headers, records the requested range, then parks its body.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let mut got = self.inner.get(key, range).await?;
        let bytes = got.body.try_collect::<BytesMut>().await?.freeze();
        self.started
            .send(range)
            .map_err(|_| ReadError::Backend("started receiver closed".into()))?;
        let release = Arc::clone(&self.release);
        got.body = stream::once(async move {
            release
                .acquire()
                .await
                .map_err(|_| ReadError::Backend("parked-body gate closed".into()))?
                .forget();
            Ok(bytes)
        })
        .boxed();
        Ok(got)
    }

    /// Delegates metadata reads unchanged.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.inner.head(key).await
    }
}

impl<R: ObjectRead> ObjectRead for ParkedBodiesRead<R> {
    /// Resolves the response headers, announces the GET, then parks its body.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let mut got = self.inner.get(key, range).await?;
        let bytes = got.body.try_collect::<BytesMut>().await?.freeze();
        self.started
            .send(())
            .map_err(|_| ReadError::Backend("started receiver closed".into()))?;
        let release = Arc::clone(&self.release);
        got.body = stream::once(async move {
            release
                .acquire()
                .await
                .map_err(|_| ReadError::Backend("parked-body gate closed".into()))?
                .forget();
            Ok(bytes)
        })
        .boxed();
        Ok(got)
    }

    /// Delegates metadata reads unchanged.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.inner.head(key).await
    }
}

impl<R: ObjectRead> ObjectRead for SplitBodyRead<R> {
    /// Splits the next armed response into an immediate first half and a gated
    /// second half; other responses pass through unchanged.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let mut got = self.inner.get(key, range).await?;
        if !self.armed.swap(false, Ordering::SeqCst) {
            return Ok(got);
        }
        let bytes = got.body.try_collect::<BytesMut>().await?.freeze();
        let midpoint = bytes.len() / 2;
        let first = bytes.slice(..midpoint);
        let second = bytes.slice(midpoint..);
        let release = Arc::clone(&self.release);
        got.body = stream::once(async move { Ok(first) })
            .chain(stream::once(async move {
                release
                    .acquire()
                    .await
                    .map_err(|_| ReadError::Backend("split-body gate closed".into()))?
                    .forget();
                Ok(second)
            }))
            .boxed();
        Ok(got)
    }

    /// Delegates metadata reads unchanged.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.inner.head(key).await
    }
}

/// Reproduction harness for #273. When armed, the next full-block GET streams
/// the whole block to the client in one chunk, then withholds end-of-stream
/// behind `release`. This parks the streaming fill task (#255) *after* the
/// client has its bytes but *before* it admits the block — the exact window a
/// CPU-starved runner hits between a served read and its background admission,
/// made explicit and deterministic.
struct TailGateRead<R> {
    /// The wrapped origin reader.
    inner: R,
    /// Arms exactly one full-block GET for tail gating.
    armed: Arc<std::sync::atomic::AtomicBool>,
    /// Releases the withheld end-of-stream.
    release: Arc<tokio::sync::Semaphore>,
}

impl<R: ObjectRead> ObjectRead for TailGateRead<R> {
    /// Passes responses through until armed; the armed response delivers the
    /// whole block, then gates end-of-stream so the fill task cannot admit.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let mut got = self.inner.get(key, range).await?;
        if !self.armed.swap(false, Ordering::SeqCst) {
            return Ok(got);
        }
        let bytes = got.body.try_collect::<BytesMut>().await?.freeze();
        let release = Arc::clone(&self.release);
        got.body = stream::once(async move { Ok(bytes) })
            .chain(stream::once(async move {
                release
                    .acquire()
                    .await
                    .map_err(|_| ReadError::Backend("tail gate closed".into()))?
                    .forget();
                Ok(Bytes::new())
            }))
            .boxed();
        Ok(got)
    }

    /// Delegates metadata reads unchanged.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.inner.head(key).await
    }
}

/// Reproduction harness for the second #273 window. Passes every GET through
/// until the aligned block-0 range has been fetched once; any repeated GET of
/// that same range announces itself and parks forever. Pre-fix, an exactly
/// block-aligned first read takes the partial fill path and spawns a detached
/// background re-fetch of the bytes it already served — the repeated GET this
/// wrapper catches.
struct RepeatAlignedGetParkedRead<R> {
    /// The wrapped origin reader.
    inner: R,
    /// GETs of the aligned block-0 range seen so far.
    aligned_gets: Arc<AtomicU64>,
    /// Announces a repeated aligned GET before parking it.
    repeated: tokio::sync::mpsc::UnboundedSender<()>,
}

impl<R: ObjectRead> ObjectRead for RepeatAlignedGetParkedRead<R> {
    /// Serves the first GET of the aligned block-0 range (and every other
    /// range) through; announces and parks any repeat of that range.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        if range == ReadRange::Bounded(0, TEST_BLOCK_BYTES - 1)
            && self.aligned_gets.fetch_add(1, Ordering::SeqCst) > 0
        {
            let _ = self.repeated.send(());
            std::future::pending::<()>().await;
        }
        self.inner.get(key, range).await
    }

    /// Delegates metadata reads unchanged.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.inner.head(key).await
    }
}

/// An `ObjectRead` that strips ETags from every response, simulating an
/// origin whose objects are non-cacheable.
struct NoEtagRead<R>(R);

impl<R: ObjectRead> ObjectRead for NoEtagRead<R> {
    /// Delegates, then drops the ETag.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let mut got = self.0.get(key, range).await?;
        got.meta.e_tag = None;
        Ok(got)
    }

    /// Delegates, then drops the ETag.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        let mut meta = self.0.head(key).await?;
        meta.e_tag = None;
        Ok(meta)
    }
}

/// A peer transport that records how often it is asked and always misses,
/// like the M1 no-op — but observable.
#[derive(Clone, Default)]
struct RecordingPeer {
    /// Number of `fetch` calls received.
    calls: Arc<AtomicU64>,
}

impl PeerFetch for RecordingPeer {
    /// Records the call and reports a clean miss.
    fn fetch(
        &self,
        _node: NodeId,
        _block: &verglas_core::BlockKey,
    ) -> impl Future<Output = Result<Option<Bytes>, PeerFetchError>> + Send {
        self.calls.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Ok(None))
    }

    /// Records owner placement as another peer call and accepts the value.
    fn store(
        &self,
        _node: NodeId,
        _block: &verglas_core::BlockKey,
        _value: Bytes,
    ) -> impl Future<Output = Result<bool, PeerFetchError>> + Send {
        self.calls.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Ok(true))
    }
}

/// A ring whose owner is always the fixed (remote) node — the mock that
/// proves the read path routes by ownership rather than assuming "local".
struct FixedOwnerRing {
    /// Member list: `[local, owner]`.
    members: Vec<NodeId>,
}

impl Ring for FixedOwnerRing {
    /// The second member owns every key.
    fn owner(&self, _key: &CacheKey) -> NodeId {
        self.members[1].clone()
    }

    /// Both members.
    fn members(&self) -> Vec<NodeId> {
        self.members.clone()
    }
}

/// Builds a counting engine over a fresh in-memory origin: returns the
/// origin store (for seeding and truth reads), the engine, and the backend
/// call counts.
async fn counting_engine(
    dir: &TempDir,
    capacity: u64,
    dram: u64,
) -> (
    Arc<InMemory>,
    HybridCacheEngine<
        CountingRead<PassthroughRead>,
        verglas_core::peer::NoopPeerFetch,
        RendezvousRing,
    >,
    BackendCalls,
) {
    counting_engine_cfg(&cache_config(dir, capacity, dram)).await
}

/// Builds a counting engine over an explicit config — the hook the
/// admission-toggle tests (#15) build on.
async fn counting_engine_cfg(
    config: &CacheConfig,
) -> (
    Arc<InMemory>,
    HybridCacheEngine<
        CountingRead<PassthroughRead>,
        verglas_core::peer::NoopPeerFetch,
        RendezvousRing,
    >,
    BackendCalls,
) {
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let backend = CountingRead {
        inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        calls: calls.clone(),
    };
    let engine = HybridCacheEngine::single_node(backend, config)
        .await
        .expect("build engine");
    (store, engine, calls)
}

/// Reads a range through any `ObjectRead`, collecting the streamed body.
async fn read_all<R: ObjectRead>(
    reader: &R,
    key: &CacheKey,
    range: ReadRange,
) -> Result<(ObjectMeta, Range<u64>, Bytes), ReadError> {
    let got = reader.get(key, range).await?;
    let mut buf = BytesMut::new();
    let mut body = got.body;
    while let Some(chunk) = body.try_next().await? {
        buf.extend_from_slice(&chunk);
    }
    Ok((got.meta, got.range, buf.freeze()))
}

/// Returns a file's size or sums the sizes of all files under a directory.
fn disk_bytes(path: &std::path::Path) -> u64 {
    if path.is_file() {
        return std::fs::metadata(path).expect("file metadata").len();
    }
    let mut total = 0;
    for entry in std::fs::read_dir(path).expect("read dir") {
        let entry = entry.expect("dir entry");
        let meta = entry.metadata().expect("entry metadata");
        if meta.is_dir() {
            total += disk_bytes(&entry.path());
        } else {
            total += meta.len();
        }
    }
    total
}

/// Miss then hit paths return bytes identical to a direct origin read for
/// every range shape, including ranges spanning block boundaries and
/// suffix/open-ended ranges, with matching metadata and resolved ranges.
#[tokio::test(flavor = "multi_thread")]
async fn hit_and_miss_paths_are_byte_identical_to_origin() {
    let dir = TempDir::new().expect("temp dir");
    // DRAM budget large enough that nothing is evicted mid-test.
    let (store, engine, _) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let size = 20 * MIB + 12_345; // three blocks, odd tail
    seed(&store, "data/f.parquet", pattern(7, size)).await;
    let origin = PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone()));
    let k = key("data/f.parquet");

    let ranges = [
        ReadRange::Full,
        ReadRange::Bounded(0, 999),
        ReadRange::Bounded(10 * MIB, 10 * MIB + 999), // inside block 1
        ReadRange::Bounded(TEST_BLOCK_BYTES - 100, TEST_BLOCK_BYTES + 99), // spans 0→1
        ReadRange::Bounded(TEST_BLOCK_BYTES - 1, 2 * TEST_BLOCK_BYTES), // spans 0→1→2
        ReadRange::Bounded(5, 5),                     // single byte
        ReadRange::Bounded(size - 1, size + 50),      // clamped at the end
        ReadRange::From(0),
        ReadRange::From(2 * TEST_BLOCK_BYTES + 5),
        ReadRange::Suffix(100),
        ReadRange::Suffix(size + 999), // longer than the object → whole object
    ];

    for range in ranges {
        let (want_meta, want_range, want) = read_all(&origin, &k, range)
            .await
            .unwrap_or_else(|e| panic!("origin read {range:?}: {e}"));
        // Twice: first exercises the miss/fill path, second the hit path.
        for pass in ["miss", "hit"] {
            let (meta, resolved, got) = read_all(&engine, &k, range)
                .await
                .unwrap_or_else(|e| panic!("engine read {range:?} ({pass}): {e}"));
            assert_eq!(got, want, "bytes differ for {range:?} ({pass} path)");
            assert_eq!(resolved, want_range, "resolved range differs for {range:?}");
            assert_eq!(meta.size, want_meta.size, "size differs for {range:?}");
            assert_eq!(meta.e_tag, want_meta.e_tag, "etag differs for {range:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn storage_bindings_isolate_blocks_metadata_and_invalidation() {
    let dir = TempDir::new().expect("temp dir");
    let managed_origin = Arc::new(InMemory::new());
    let customer_origin = Arc::new(InMemory::new());
    seed(
        &managed_origin,
        "same.parquet",
        Bytes::from_static(b"managed"),
    )
    .await;
    seed(
        &customer_origin,
        "same.parquet",
        Bytes::from_static(b"customer"),
    )
    .await;

    let registry = BackendRegistry::new(vec![
        BackendStore::single("managed", BUCKET, managed_origin.clone()),
        BackendStore::single("customer", BUCKET, customer_origin.clone()),
    ])
    .expect("distinct bindings");
    let engine = HybridCacheEngine::single_node(
        PassthroughRead::new(registry),
        &cache_config(&dir, 64 * MIB, 128 * MIB),
    )
    .await
    .expect("build engine");
    let managed = CacheKey {
        storage_binding_id: "managed".to_owned(),
        bucket: BUCKET.to_owned(),
        key: "same.parquet".to_owned(),
    };
    let customer = CacheKey {
        storage_binding_id: "customer".to_owned(),
        ..managed.clone()
    };

    assert_eq!(
        read_all(&engine, &managed, ReadRange::Full)
            .await
            .expect("managed read")
            .2,
        Bytes::from_static(b"managed")
    );
    assert_eq!(
        read_all(&engine, &customer, ReadRange::Full)
            .await
            .expect("customer read")
            .2,
        Bytes::from_static(b"customer")
    );

    seed(
        &managed_origin,
        "same.parquet",
        Bytes::from_static(b"managed-v2"),
    )
    .await;
    engine
        .invalidate(std::slice::from_ref(&managed))
        .await
        .expect("managed invalidation");
    assert_eq!(
        read_all(&engine, &managed, ReadRange::Full)
            .await
            .expect("managed reread")
            .2,
        Bytes::from_static(b"managed-v2")
    );
    assert_eq!(
        read_all(&engine, &customer, ReadRange::Full)
            .await
            .expect("customer cached read")
            .2,
        Bytes::from_static(b"customer")
    );
}

/// The configured two-MiB geometry drives cold covering-block boundaries: the
/// mapping-resolving first block is filled aligned to the 2 MiB boundary, and a
/// first-touch subsequent block fetches its exact requested slice from the next
/// 2 MiB boundary (#255 defers the aligned overfetch until the block repeats).
/// Both origin requests start on a 2 MiB boundary, and the client receives only
/// the bytes it asked for.
#[tokio::test(flavor = "multi_thread")]
async fn configured_two_mib_geometry_drives_cold_fill_boundaries_without_changing_bytes() {
    let dir = TempDir::new().expect("temp dir");
    let config = cache_config_block_size(&dir, 64 * MIB, 128 * MIB, 2 * MIB);
    let (store, engine, calls) = counting_engine_cfg(&config).await;
    let body = pattern(29, 4 * MIB);
    seed(&store, "geometry.parquet", body.clone()).await;
    let k = key("geometry.parquet");

    let (_, range, got) = read_all(&engine, &k, ReadRange::Bounded(MIB, 3 * MIB + 127))
        .await
        .expect("cold two-block read");

    assert_eq!(range, MIB..3 * MIB + 128);
    assert_eq!(got, body.slice(MIB as usize..(3 * MIB + 128) as usize));
    let ranges = calls.get_ranges();
    assert_eq!(
        ranges.len(),
        2,
        "one origin request for each configured block"
    );
    assert!(
        ranges.contains(&ReadRange::Bounded(0, 2 * MIB - 1)),
        "first covering block is filled aligned to the configured 2 MiB block: {ranges:?}"
    );
    assert!(
        ranges.contains(&ReadRange::Bounded(2 * MIB, 3 * MIB + 127)),
        "first-touch subsequent block fetches its exact slice from the configured \
         2 MiB boundary; the aligned overfetch is deferred until it repeats: {ranges:?}"
    );
}

/// The smallest valid two-MiB cache reserves both the data and metadata disk
/// tiers, so startup does not panic while carving the hard disk ceiling.
#[tokio::test(flavor = "multi_thread")]
async fn configured_two_mib_minimum_disk_budget_builds() {
    let dir = TempDir::new().expect("temp dir");
    let data_block_bytes = 2 * MIB;
    let data_disk_floor = 4 * (data_block_bytes + 16 * 1024);
    let metadata_disk_floor = 4 * MIB;
    let config = cache_config_block_size(
        &dir,
        data_disk_floor + metadata_disk_floor,
        128 * MIB,
        data_block_bytes,
    );

    let _engine = counting_engine_cfg(&config).await;
}

/// The data-tier-only floor is rejected before the metadata-store carve, so a
/// geometry-dependent undersized cache reports a configuration error instead
/// of panicking during startup.
#[tokio::test(flavor = "multi_thread")]
async fn configured_two_mib_data_only_disk_floor_is_rejected() {
    let dir = TempDir::new().expect("temp dir");
    let data_block_bytes = 2 * MIB;
    let data_disk_floor = 4 * (data_block_bytes + 16 * 1024);
    let config = cache_config_block_size(&dir, data_disk_floor, 128 * MIB, data_block_bytes);
    let backend = PassthroughRead::new(BackendStore::single(
        "default",
        BUCKET,
        Arc::new(InMemory::new()),
    ));

    let result = HybridCacheEngine::single_node(backend, &config).await;
    assert!(matches!(
        result,
        Err(EngineError::DiskBudgetTooSmall { .. })
    ));
}

/// The second read of an object issues zero backend requests: everything is
/// served from the DRAM tier, proven by the engine's fill counter and by
/// independent backend call counts.
#[tokio::test(flavor = "multi_thread")]
async fn second_read_issues_zero_backend_requests() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let body = pattern(1, 12 * MIB); // two blocks
    seed(&store, "hot.parquet", body.clone()).await;
    let k = key("hot.parquet");

    let (_, _, first) = read_all(&engine, &k, ReadRange::Full).await.expect("first");
    assert_eq!(first, body);
    let after_first = calls.snapshot();
    let fills_after_first = engine.counters().snapshot().backend_fills;
    assert_eq!(fills_after_first, 2, "one fill per covering block");

    let (_, _, second) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("second");
    assert_eq!(second, body);

    let counters = engine.counters().snapshot();
    assert_eq!(counters.backend_fills, fills_after_first, "no new fills");
    assert_eq!(calls.snapshot(), after_first, "no backend requests at all");
    assert_eq!(counters.dram_hits, 2, "both blocks served from DRAM");
    assert_eq!(counters.dram_bytes_served, 12 * MIB);
}

/// The serving-tier provenance the request-duration histogram reads (#46) tracks
/// the first-block source: a cold read reports `Backend` (it filled), the warm
/// re-read reports `Dram` (served from memory). The tier resolves as the body
/// streams, so the body is drained before the cell is read.
#[tokio::test(flavor = "multi_thread")]
async fn served_from_reports_backend_on_a_cold_read_then_dram_when_warm() {
    use verglas_core::read::ServedTier;

    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let body = pattern(3, 6 * MIB); // one block
    seed(&store, "cold.parquet", body.clone()).await;
    let k = key("cold.parquet");

    // Cold: the read fills from the backend, so its first block is Backend.
    let got = engine.get(&k, ReadRange::Full).await.expect("cold get");
    let cell = got.served_from.clone();
    let mut stream = got.body;
    while let Some(chunk) = stream.try_next().await.expect("chunk") {
        let _ = chunk;
    }
    assert_eq!(
        cell.get(),
        ServedTier::Backend,
        "a cold read serves its first block from a backend fill"
    );

    // Warm: the block is now DRAM-resident.
    let got = engine.get(&k, ReadRange::Full).await.expect("warm get");
    let cell = got.served_from.clone();
    let mut stream = got.body;
    while let Some(chunk) = stream.try_next().await.expect("chunk") {
        let _ = chunk;
    }
    assert_eq!(
        cell.get(),
        ServedTier::Dram,
        "a warm read serves from the DRAM tier"
    );
}

/// A cold data fill gives the client origin bytes as they arrive. Cache
/// validation and admission happen after the origin body completes, so a slow
/// second origin chunk cannot withhold an already-available first chunk.
#[tokio::test(flavor = "multi_thread")]
async fn cold_fill_streams_before_origin_body_completes_then_caches() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let backend = SplitBodyRead {
        inner: CountingRead {
            inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
            calls: calls.clone(),
        },
        armed: Arc::clone(&armed),
        release: Arc::clone(&release),
    };
    let engine = HybridCacheEngine::single_node(backend, &cache_config(&dir, 64 * MIB, 128 * MIB))
        .await
        .expect("build engine");
    let expected = pattern(17, TEST_BLOCK_BYTES);
    seed(&store, "stream-first.parquet", expected.clone()).await;
    let k = key("stream-first.parquet");

    // Populate only the key-to-ETag mapping so the GET exercises a data-block
    // miss rather than the mapping probe.
    engine.head(&k).await.expect("prime mapping");
    let (_, _, first_touch) =
        read_all(&engine, &k, ReadRange::Bounded(0, TEST_BLOCK_BYTES / 4 - 1))
            .await
            .expect("first partial touch");
    assert_eq!(first_touch, expected.slice(..expected.len() / 4));
    armed.store(true, Ordering::SeqCst);

    let got = engine
        .get(&k, ReadRange::Bounded(0, TEST_BLOCK_BYTES / 4 - 1))
        .await
        .expect("cold streaming get");
    let mut body = got.body;
    // The origin's second body half is withheld (`release` holds no permits), so
    // awaiting these chunks is deterministic rather than a wall-clock race: the
    // client's requested first quarter is delivered from the first (available)
    // half and the response ends there. A fill that wrongly waited for the
    // withheld tail would deadlock (caught by the harness timeout), not flake.
    let first = body
        .try_next()
        .await
        .expect("first chunk read")
        .expect("first chunk present");
    assert_eq!(first, expected.slice(..expected.len() / 4));
    let end = body.try_next().await.expect("response end");
    assert!(end.is_none(), "requested range has already been delivered");

    // Release the withheld tail so the background tee completes, then quiesce the
    // in-flight fill (#274 barrier) before asserting residency. The tee's
    // admission runs off the client path, so without the barrier this assertion
    // races it — the property is eventual admission, not timing.
    release.add_permits(1);
    engine.flush().await;
    let gets_after_admission = calls.snapshot().0;
    let (_, _, warm) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("warm read");
    assert_eq!(warm, expected);
    assert_eq!(
        calls.snapshot().0,
        gets_after_admission,
        "completed background tee must populate cache"
    );
}

/// An unmapped small range near the end of a block must not wait for the
/// aligned fill's delayed tail, while the complete block must still become a
/// backend-free warm hit afterward.
#[tokio::test(flavor = "multi_thread")]
async fn first_unmapped_partial_read_does_not_wait_for_aligned_tail() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let backend = DelayedAlignedRead {
        inner: CountingRead {
            inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
            calls: calls.clone(),
        },
        release: Arc::clone(&release),
    };
    let engine = HybridCacheEngine::single_node(backend, &cache_config(&dir, 64 * MIB, 128 * MIB))
        .await
        .expect("build engine");
    let expected = pattern(29, TEST_BLOCK_BYTES);
    seed(&store, "first-fill.parquet", expected.clone()).await;
    let key = key("first-fill.parquet");
    let start = 6 * MIB;
    let end = start + 64 * 1024 - 1;

    // The aligned overfetch tail is gated: `release` holds no permits, so the
    // background aligned fill cannot complete. The exact foreground range must
    // still serve — proven by construction, since the read below completes while
    // the tail is withheld. A fill that waited for the tail would deadlock (the
    // harness timeout catches it), not flake on a wall-clock deadline.
    let got = engine
        .get(&key, ReadRange::Bounded(start, end))
        .await
        .expect("cold partial get");
    let mut body = got.body;
    let first = body
        .try_next()
        .await
        .expect("first client chunk")
        .expect("non-empty response");
    assert_eq!(first, expected.slice(start as usize..=end as usize));
    assert!(body.try_next().await.expect("response end").is_none());

    // While the aligned tail is still gated, flush must not report quiescence —
    // the background block flight is registered before the partial path returns
    // so the #273 barrier observes it.
    let flushing = engine.clone();
    let mut flush = tokio::spawn(async move { flushing.flush().await });
    let pending = tokio::time::timeout(std::time::Duration::from_millis(500), &mut flush).await;
    assert!(
        pending.is_err(),
        "flush() returned while the aligned background fill was still gated"
    );

    // Release the aligned tail; the fill admits and flush completes. The
    // completed aligned block must then be a backend-free warm hit.
    release.add_permits(1);
    flush.await.expect("flush task");
    let gets_after_fill = calls.snapshot().0;
    assert!(
        gets_after_fill >= 2,
        "exact partial GET plus aligned block fill must both have run, got {gets_after_fill}"
    );
    let (_, _, warm) = read_all(&engine, &key, ReadRange::Full)
        .await
        .expect("warm full-block read");
    assert_eq!(warm, expected);
    assert_eq!(
        calls.snapshot().0,
        gets_after_fill,
        "warm read must use the completed aligned cache fill"
    );
}

/// A multi-block cold response starts a bounded four-block origin window
/// before the first body completes, while still emitting bytes in block order.
#[tokio::test(flavor = "multi_thread")]
async fn cold_multiblock_read_starts_four_fills_before_first_completes() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let (started, mut entered) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let backend = ParkedBodiesRead {
        inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        started,
        release: Arc::clone(&release),
    };
    let engine = HybridCacheEngine::single_node(backend, &cache_config(&dir, 64 * MIB, 128 * MIB))
        .await
        .expect("build engine");
    let expected = pattern(23, 4 * TEST_BLOCK_BYTES);
    seed(&store, "pipeline.parquet", expected.clone()).await;
    let k = key("pipeline.parquet");
    engine.head(&k).await.expect("prime mapping");

    let got = engine.get(&k, ReadRange::Full).await.expect("cold get");
    let reader = tokio::spawn(async move {
        let mut bytes = BytesMut::new();
        let mut body = got.body;
        while let Some(chunk) = body.try_next().await? {
            bytes.extend_from_slice(&chunk);
        }
        Ok::<Bytes, ReadError>(bytes.freeze())
    });

    // All four origin bodies are parked (`release` holds no permits), so no
    // block can complete until the release below. The four aligned fills must
    // therefore announce themselves before any block completes — awaited
    // directly, which is deterministic synchronization, not a wall-clock
    // deadline that starves out under CPU oversubscription (#275).
    for fill in 1..=4 {
        entered
            .recv()
            .await
            .unwrap_or_else(|| panic!("fill {fill} did not start before block 1 completed"));
    }
    release.add_permits(4);
    assert_eq!(
        reader.await.expect("reader task").expect("reader body"),
        expected
    );
}

/// Four unfinished aligned tails are the hard background-fill ceiling. A
/// fifth partial read remains user work: it fetches only the requested bytes
/// instead of waiting for or adding another aligned cache fill.
#[tokio::test(flavor = "multi_thread")]
async fn saturated_background_fills_serve_next_partial_range_directly() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let (started, mut entered) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let backend = RangeRecordingBodiesRead {
        inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        started,
        release: Arc::clone(&release),
    };
    let engine = Arc::new(
        HybridCacheEngine::single_node(backend, &cache_config(&dir, 64 * MIB, 128 * MIB))
            .await
            .expect("build engine"),
    );
    let requested_len = TEST_BLOCK_BYTES / 4;
    let mut readers = Vec::new();
    let mut keys = Vec::new();

    for object in 0..5 {
        let object_key = format!("background-lane-{object}.parquet");
        seed(&store, &object_key, pattern(object, TEST_BLOCK_BYTES)).await;
        let k = key(&object_key);
        engine.head(&k).await.expect("prime mapping");
        keys.push(k.clone());
        let reader_engine = Arc::clone(&engine);
        let reader = tokio::spawn(async move {
            read_all(&reader_engine, &k, ReadRange::Bounded(0, requested_len - 1)).await
        });

        let range = tokio::time::timeout(std::time::Duration::from_millis(250), entered.recv())
            .await
            .unwrap_or_else(|_| panic!("origin GET {object} did not start"))
            .expect("range announcement");
        assert_eq!(
            range,
            ReadRange::Bounded(0, requested_len - 1),
            "first partial touch must remain exact"
        );
        release.add_permits(1);
        reader
            .await
            .expect("first-touch reader task")
            .expect("first-touch response");
    }

    for (object, k) in keys.into_iter().enumerate() {
        let reader_engine = Arc::clone(&engine);
        readers.push(tokio::spawn(async move {
            read_all(&reader_engine, &k, ReadRange::Bounded(0, requested_len - 1)).await
        }));

        let range = tokio::time::timeout(std::time::Duration::from_millis(250), entered.recv())
            .await
            .unwrap_or_else(|_| panic!("repeated origin GET {object} did not start"))
            .expect("range announcement");
        let expected = if object < 4 {
            ReadRange::Bounded(0, TEST_BLOCK_BYTES - 1)
        } else {
            ReadRange::Bounded(0, requested_len - 1)
        };
        assert_eq!(
            range, expected,
            "fifth partial read must bypass aligned fill"
        );
    }

    release.add_permits(5);
    for reader in readers {
        reader.await.expect("reader task").expect("reader response");
    }
}

/// A working set larger than the DRAM budget spills to the disk tier and is
/// still served correctly, with zero backend fills on re-read.
#[tokio::test(flavor = "multi_thread")]
async fn working_set_larger_than_dram_spills_to_disk_and_serves() {
    let dir = TempDir::new().expect("temp dir");
    // Minimum DRAM budget: a 16 MiB block memory tier (two blocks) after
    // the pipeline + metadata reservations. The 40 MiB object cannot fit.
    let (store, engine, calls) = counting_engine(&dir, 96 * MIB, 80 * MIB).await;
    let size = 40 * MIB; // five blocks
    let body = pattern(3, size);
    seed(&store, "big.parquet", body.clone()).await;
    let k = key("big.parquet");

    // Warm block by block, flushing so every spilled block is durable on the
    // disk tier before the next request (keeps tier placement deterministic).
    for i in 0..5 {
        let range = ReadRange::Bounded(i * TEST_BLOCK_BYTES, (i + 1) * TEST_BLOCK_BYTES - 1);
        let (_, _, got) = read_all(&engine, &k, range).await.expect("warm block");
        assert_eq!(
            got,
            body.slice((i * TEST_BLOCK_BYTES) as usize..((i + 1) * TEST_BLOCK_BYTES) as usize)
        );
        engine.flush().await;
    }
    let gets_after_warm = calls.snapshot().0;
    assert_eq!(engine.counters().snapshot().backend_fills, 5);

    let (_, _, reread) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("re-read");
    assert_eq!(reread, body, "spilled object must re-read byte-identically");

    let counters = engine.counters().snapshot();
    assert_eq!(counters.backend_fills, 5, "re-read must not fill");
    assert_eq!(
        calls.snapshot().0,
        gets_after_warm,
        "no backend GETs on re-read"
    );
    assert!(
        counters.disk_hits >= 3,
        "most blocks must come off the disk tier (DRAM holds at most two), got {}",
        counters.disk_hits
    );
    assert!(counters.disk_bytes_served > 0);
}

/// #278: two block writes queued back-to-back must both reach NVMe at the given
/// geometry. foyer drains queued writes into one flush buffer per rotation and
/// silently drops any entry that does not fit; with the old fixed 16 MiB buffer
/// and 8 MiB blocks, two writes stacked into one rotation lost the second, and a
/// "warmed and flushed" block was then on neither tier (#273). The buffer is now
/// sized to hold two entries. Proven by a warm restart: recovery reloads only
/// disk-resident blocks, so a dropped write surfaces as a post-restart refill.
async fn two_back_to_back_block_writes_both_reach_disk(block_bytes: u64) {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    // Generous disk so the reclaimer never evicts a live block, and generous
    // DRAM so neither block is lost to DRAM eviction before the flush — the only
    // way a block goes missing here is the flush-buffer drop this test guards.
    let config = cache_config_block_size(&dir, 256 * block_bytes, 128 * MIB, block_bytes);
    let body = pattern(41, 2 * block_bytes);
    seed(&store, "pair.parquet", body.clone()).await;
    let k = key("pair.parquet");

    // Warm phase: fill both blocks back-to-back with no flush between them, so
    // both disk writes are queued together and can drain into one flusher
    // rotation, then flush once and drop the engine (a clean stop).
    {
        let calls = BackendCalls::default();
        let backend = CountingRead {
            inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
            calls: calls.clone(),
        };
        let engine = HybridCacheEngine::single_node(backend, &config)
            .await
            .expect("build engine");
        for i in 0..2 {
            let range = ReadRange::Bounded(i * block_bytes, (i + 1) * block_bytes - 1);
            let (_, _, got) = read_all(&engine, &k, range).await.expect("warm block");
            assert_eq!(
                got,
                body.slice((i * block_bytes) as usize..((i + 1) * block_bytes) as usize)
            );
        }
        engine.flush().await;
        assert_eq!(engine.counters().snapshot().backend_fills, 2);
    }

    // Restart phase: a fresh engine over the SAME directory. Both blocks must
    // recover from NVMe — zero backend GETs, two disk hits. A dropped write in
    // the warm phase would refill here.
    let calls = BackendCalls::default();
    let backend = CountingRead {
        inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        calls: calls.clone(),
    };
    let engine = HybridCacheEngine::single_node(backend, &config)
        .await
        .expect("rebuild engine");
    engine.head(&k).await.expect("re-resolve mapping");
    let (_, _, reread) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("post-restart read");
    assert_eq!(reread, body, "both blocks recover byte-identically");
    let counters = engine.counters().snapshot();
    assert_eq!(
        counters.backend_fills, 0,
        "both back-to-back block writes reached disk; neither refilled after restart"
    );
    assert_eq!(
        counters.disk_hits, 2,
        "both blocks serve from the recovered NVMe tier"
    );
    assert_eq!(calls.snapshot().0, 0, "no backend GETs after restart");
}

/// #278 at the 2 MiB product-default geometry.
#[tokio::test(flavor = "multi_thread")]
async fn two_back_to_back_block_writes_both_reach_disk_default_geometry() {
    two_back_to_back_block_writes_both_reach_disk(2 * MIB).await;
}

/// #278 at the 8 MiB maximum configured geometry — the geometry where the old
/// fixed 16 MiB buffer could hold only one of two block entries per rotation.
#[tokio::test(flavor = "multi_thread")]
async fn two_back_to_back_block_writes_both_reach_disk_max_geometry() {
    two_back_to_back_block_writes_both_reach_disk(8 * MIB).await;
}

/// Reproduction for #273: `flush()` must not report durability for a block
/// whose background admission (#255 streaming fill) has not completed. A full
/// aligned block read serves its bytes to the client and returns while the fill
/// task validates and admits the block afterward, off the client path. If
/// `flush()` only drains foyer's write queue, it returns before that admission
/// runs, so a block it reported durable is absent on re-read and refills. Under
/// CPU starvation that lost admission is what makes
/// `working_set_larger_than_dram_spills_to_disk_and_serves` flake; here the
/// window is explicit and the failure deterministic.
#[tokio::test(flavor = "multi_thread")]
async fn flush_waits_for_background_block_admission() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let backend = TailGateRead {
        inner: CountingRead {
            inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
            calls: calls.clone(),
        },
        armed: Arc::clone(&armed),
        release: Arc::clone(&release),
    };
    // Generous DRAM: this test isolates the flush/admission ordering, not the
    // spill path, so nothing is evicted and any refill is purely a lost block.
    let engine = HybridCacheEngine::single_node(backend, &cache_config(&dir, 64 * MIB, 128 * MIB))
        .await
        .expect("build engine");
    let body = pattern(7, 2 * TEST_BLOCK_BYTES);
    seed(&store, "gated.parquet", body.clone()).await;
    let k = key("gated.parquet");

    // Block 0 resolves the mapping and admits synchronously (the first-fill probe
    // path awaits admission before the read returns), so the block 1 read below
    // takes the streaming-fill lead path whose admission runs off the client path.
    let b0 = ReadRange::Bounded(0, TEST_BLOCK_BYTES - 1);
    read_all(&engine, &k, b0).await.expect("warm block 0");
    engine.flush().await;

    // Arm the gate and read block 1. The client receives the whole block and the
    // read returns; the streaming fill task is now parked at end-of-stream,
    // before it admits the block — the CPU-starvation window, held open.
    armed.store(true, Ordering::SeqCst);
    let b1 = ReadRange::Bounded(TEST_BLOCK_BYTES, 2 * TEST_BLOCK_BYTES - 1);
    let (_, _, got) = read_all(&engine, &k, b1).await.expect("warm block 1");
    assert_eq!(got, body.slice(TEST_BLOCK_BYTES as usize..));

    // flush() must block until the parked admission completes. A flush() that
    // drains only foyer's write queue returns immediately here — the #273 bug.
    // The bounded wait asserts non-completion; it does not stabilize a flake
    // (buggy flush completes well within it every time, fixed flush never does).
    let flushing = engine.clone();
    let mut flush = tokio::spawn(async move { flushing.flush().await });
    let pending = tokio::time::timeout(std::time::Duration::from_millis(500), &mut flush).await;
    assert!(
        pending.is_err(),
        "flush() returned while a served block's admission was still pending: \
         flush() reported durability it had not achieved (#273)"
    );

    // Release end-of-stream; the fill task admits the block and flush() completes.
    release.add_permits(1);
    flush.await.expect("flush task");

    // The block flush() waited for is now durable: re-reading it issues no fill.
    let fills_before = engine.counters().snapshot().backend_fills;
    read_all(&engine, &k, b1).await.expect("re-read block 1");
    assert_eq!(
        engine.counters().snapshot().backend_fills,
        fills_before,
        "a block flush() reported durable must not refill on re-read"
    );
}

/// Second #273 window: a cold read of exactly one aligned block must resolve
/// through the aligned probe — one backend GET, block admitted before the read
/// returns — not through the partial first fill, which serves the exact bytes
/// and then re-fetches the SAME aligned range from a detached background task.
///
/// That detached admission lands at an uncontrolled time. Its disk write can
/// share one foyer flusher drain window with a later warm block's write, and
/// foyer's bounded flush buffer silently drops the second block-sized entry in
/// a window (two 8 MiB entries page-align to 16,785,408 bytes, over the
/// 16 MiB pool — `Buffer::push` fails and the entry is discarded with only a
/// metric). `flush()` cannot see the drop: the write was never queued. The
/// dropped block's only copy is the one-block DRAM tier, which later inserts
/// evict — so a "warmed and flushed" block was on neither tier and the re-read
/// paid a sixth fill. An exactly aligned request IS its aligned block: there is
/// nothing left to populate in the background, so it takes the probe.
#[tokio::test(flavor = "multi_thread")]
async fn cold_aligned_block_read_probes_once_and_admits_before_returning() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let (repeated_tx, mut repeated) = tokio::sync::mpsc::unbounded_channel();
    let backend = RepeatAlignedGetParkedRead {
        inner: CountingRead {
            inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
            calls: calls.clone(),
        },
        aligned_gets: Arc::new(AtomicU64::new(0)),
        repeated: repeated_tx,
    };
    let engine = HybridCacheEngine::single_node(backend, &cache_config(&dir, 96 * MIB, 80 * MIB))
        .await
        .expect("build engine");
    let body = pattern(11, 2 * TEST_BLOCK_BYTES);
    seed(&store, "aligned.parquet", body.clone()).await;
    let k = key("aligned.parquet");

    let range = ReadRange::Bounded(0, TEST_BLOCK_BYTES - 1);
    let (_, _, got) = read_all(&engine, &k, range)
        .await
        .expect("cold aligned read");
    assert_eq!(got, body.slice(..TEST_BLOCK_BYTES as usize));

    // Pre-fix the partial path's detached task re-fetches the aligned range the
    // read just served; the wrapper announces the duplicate. Post-fix nothing
    // ever fetches twice, so this wait must come up empty. The bounded wait
    // asserts absence; it does not stabilize a flake (the duplicate GET, when
    // the bug exists, is issued by a task spawned before the read returned).
    let dup = tokio::time::timeout(std::time::Duration::from_millis(500), repeated.recv()).await;
    assert!(
        dup.is_err(),
        "an exactly aligned block read must not background-re-fetch its own bytes (#273)"
    );

    let counters = engine.counters().snapshot();
    assert_eq!(
        calls.snapshot().0,
        1,
        "one backend GET for one aligned block"
    );
    assert_eq!(counters.backend_fills, 1, "the probe GET is the fill");
    assert_eq!(
        counters.blocks_admitted, 1,
        "admitted within the awaited probe flight, before the read returned"
    );

    // The admission was synchronous, so after a flush the block re-reads with
    // zero origin traffic.
    engine.flush().await;
    let (_, _, warm) = read_all(&engine, &k, range).await.expect("warm re-read");
    assert_eq!(warm, body.slice(..TEST_BLOCK_BYTES as usize));
    assert_eq!(calls.snapshot().0, 1, "no backend GETs on re-read");
}

/// An object whose origin responses carry no ETag is served correctly but
/// never admitted: every read goes passthrough and the counters prove no
/// cache activity happened.
#[tokio::test(flavor = "multi_thread")]
async fn missing_etag_object_is_served_passthrough_and_never_admitted() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let backend = NoEtagRead(CountingRead {
        inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        calls: calls.clone(),
    });
    let engine = HybridCacheEngine::single_node(backend, &cache_config(&dir, 64 * MIB, 128 * MIB))
        .await
        .expect("build engine");
    let body = pattern(9, 12 * MIB);
    seed(&store, "no-etag.bin", body.clone()).await;
    let k = key("no-etag.bin");

    for _ in 0..2 {
        let (meta, _, got) = read_all(&engine, &k, ReadRange::Full).await.expect("GET");
        assert_eq!(got, body);
        assert_eq!(meta.e_tag, None);
    }
    let head = engine.head(&k).await.expect("HEAD");
    assert_eq!(head.size, 12 * MIB);
    assert_eq!(head.e_tag, None);

    let counters = engine.counters().snapshot();
    assert_eq!(counters.non_cacheable_passthroughs, 3, "2 GETs + 1 HEAD");
    assert_eq!(counters.backend_fills, 0, "no block fills ever");
    assert_eq!(
        counters.dram_hits + counters.disk_hits,
        0,
        "nothing admitted"
    );
    // Each GET costs two backend GETs on this path: the mapping-resolving
    // probe (whose response reveals there is no ETag) plus the passthrough
    // serve. Only the explicit HEAD request issues a backend HEAD.
    let (gets, heads) = calls.snapshot();
    assert_eq!(gets, 4, "probe + passthrough per GET, every time");
    assert_eq!(heads, 1, "only the HEAD request HEADs");
}

/// When caching is paused (the runtime disk-full guardrail, #96) the engine
/// serves reads correctly from the origin but admits nothing to the cache; once
/// caching resumes, fills admit again. A full disk never breaks a read, and the
/// cache simply stops growing while the disk is under pressure.
#[tokio::test(flavor = "multi_thread")]
async fn paused_caching_serves_from_origin_without_admitting() {
    let dir = TempDir::new().expect("temp dir");
    // Admission off so a single read would normally admit; the pause is what
    // must suppress it here, not scan resistance.
    let (store, engine, _) =
        counting_engine_cfg(&cache_config_admission(&dir, 64 * MIB, 80 * MIB, false)).await;

    let paused = engine.caching_paused_handle();
    paused.store(true, Ordering::Relaxed);
    assert!(engine.is_caching_paused(), "the pause flag is observable");

    // A read while paused serves the right bytes but admits nothing.
    let body = pattern(1, 4 * MIB);
    seed(&store, "paused.bin", body.clone()).await;
    let (_, _, got) = read_all(&engine, &key("paused.bin"), ReadRange::Full)
        .await
        .expect("serve while paused");
    assert_eq!(got, body, "a paused read still serves correct bytes");
    engine.flush().await;
    assert_eq!(
        engine.counters().snapshot().blocks_admitted,
        0,
        "nothing is admitted while caching is paused"
    );

    // A re-read while paused refetches from the origin (nothing was cached).
    let fills_before = engine.counters().snapshot().backend_fills;
    let (_, _, got) = read_all(&engine, &key("paused.bin"), ReadRange::Full)
        .await
        .expect("re-read while paused");
    assert_eq!(got, body);
    assert!(
        engine.counters().snapshot().backend_fills > fills_before,
        "a paused re-read refills from the origin, proving nothing cached"
    );

    // Resume caching: a fresh fill admits again.
    paused.store(false, Ordering::Relaxed);
    assert!(!engine.is_caching_paused());
    let body2 = pattern(2, 4 * MIB);
    seed(&store, "resumed.bin", body2.clone()).await;
    let (_, _, got) = read_all(&engine, &key("resumed.bin"), ReadRange::Full)
        .await
        .expect("serve after resume");
    assert_eq!(got, body2);
    engine.flush().await;
    assert!(
        engine.counters().snapshot().blocks_admitted > 0,
        "admission resumes once caching is un-paused"
    );
}

/// Filling far beyond `capacity_bytes` keeps on-disk usage within the budget
/// (the ceiling holds by construction — foyer preallocates the whole budget
/// at open and never writes outside it), and a re-read of an evicted object
/// refetches from the backend correctly.
#[tokio::test(flavor = "multi_thread")]
async fn capacity_ceiling_holds_and_evicted_objects_refetch() {
    let dir = TempDir::new().expect("temp dir");
    let capacity = 64 * MIB;
    // Admission off (#15): this test drives the cache with a stream of distinct
    // one-touch objects to prove the *eviction* ceiling and the refetch of an
    // evicted object. Scan-resistant admission would (correctly) reject those
    // one-touch fills once the cache fills, so the churn this test needs only
    // exists on the admit-everything path — the ceiling itself holds either way.
    let (store, engine, _) =
        counting_engine_cfg(&cache_config_admission(&dir, capacity, 80 * MIB, false)).await;

    let blocks_data = dir.path().join("blocks.data");
    assert!(
        disk_bytes(&blocks_data) <= capacity,
        "preallocated device must already fit the budget"
    );

    // Fill 2x the disk budget: 16 single-block objects.
    let objects: Vec<(String, Bytes)> = (0..16)
        .map(|i| (format!("obj-{i}.bin"), pattern(100 + i, 8 * MIB)))
        .collect();
    for (name, body) in &objects {
        seed(&store, name, body.clone()).await;
        let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("fill read");
        assert_eq!(&got, body);
        engine.flush().await;
        assert!(
            disk_bytes(&blocks_data) <= capacity,
            "disk usage exceeded capacity_bytes during fill"
        );
    }

    // The first object was written 15 objects (120 MiB > 64 MiB) ago; both
    // tiers have long evicted it. Re-reading must refetch, not panic or
    // serve anything stale.
    let fills_before = engine.counters().snapshot().backend_fills;
    let (_, _, got) = read_all(&engine, &key("obj-0.bin"), ReadRange::Full)
        .await
        .expect("re-read evicted");
    assert_eq!(got, objects[0].1, "refetched bytes must match the origin");
    assert!(
        engine.counters().snapshot().backend_fills > fills_before,
        "evicted object must be refetched from the backend"
    );
    assert!(disk_bytes(&blocks_data) <= capacity);
}

/// A multi-TiB cache uses one sparse backing file for each Foyer disk tier,
/// rather than one open descriptor per Foyer region.
#[tokio::test(flavor = "multi_thread")]
async fn multi_tib_cache_uses_two_sparse_backing_files() {
    let dir = TempDir::new().expect("temp dir");
    let tebibyte = 1024_u64 * 1024 * 1024 * 1024;
    let (_, engine, _) = counting_engine(&dir, 7 * tebibyte, 80 * MIB).await;

    let blocks = dir.path().join("blocks.data");
    let metadata = dir.path().join("meta.data");
    assert!(blocks.is_file(), "data tier must use one backing file");
    assert!(
        metadata.is_file(),
        "metadata tier must use one backing file"
    );

    engine.flush().await;
}

/// Runs the scan-resistance scenario for one admission setting and returns
/// `(refills_re_reading_the_working_set, blocks_rejected_during_the_scan)`:
///
/// 1. Warm a hot working set `W` (6 single-block objects, 48 MiB) with two
///    passes so every block is admitted and resident and the cache is under
///    pressure (48 MiB > the 40 MiB half-of-80-MiB pressure threshold).
/// 2. Stream a one-touch scan of 3× the disk budget (30 distinct single-block
///    objects, 240 MiB) — each read exactly once, never reused.
/// 3. Re-read all of `W` and count the backend fills that cost.
///
/// With admission on, the scan's blocks are rejected, foyer never evicts `W`,
/// and the re-read is free. With admission off, the scan evicts `W` from the
/// small cache and the re-read refills it.
async fn scan_resistance_run(admission_enabled: bool) -> (u64, u64) {
    let dir = TempDir::new().expect("temp dir");
    // Disk budget = 2× the working set: the pressure line (half the budget)
    // then sits exactly at the warmed working-set size, so scan resistance is
    // engaged for the scan while the working set keeps 50% of the disk to
    // itself (the same headroom the #122 DRAM-ceiling test gives, so foyer's
    // async reclaimer never has to touch a live working-set block).
    let capacity = 96 * MIB; // ~12 usable 8 MiB regions after foyer framing.
    let dram = 80 * MIB; // engine floor: a 2-block memory tier.
    let (store, engine, _calls) = counting_engine_cfg(&cache_config_admission(
        &dir,
        capacity,
        dram,
        admission_enabled,
    ))
    .await;

    // The hot working set: 6 single-block objects (48 MiB), half the disk
    // budget, so warming it brings the cache exactly to the pressure line.
    let working_set: Vec<(String, Bytes)> = (0..6)
        .map(|i| (format!("ws-{i}.parquet"), pattern(700 + i, 8 * MIB)))
        .collect();
    for (name, body) in &working_set {
        seed(&store, name, body.clone()).await;
    }
    // Two warm passes so every block is admitted and genuinely hot before the
    // scan (the first pass fills the cache to the pressure line; the second
    // confirms the whole set is resident and re-read for free). Flush after
    // every block, like the spill test: a burst of fills without draining the
    // flush queue would overflow foyer's submit-queue threshold and drop blocks
    // before they reach disk — a warm-up artifact, not the behavior under test.
    for _ in 0..2 {
        for (name, body) in &working_set {
            let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
                .await
                .expect("warm working set");
            assert_eq!(&got, body);
            engine.flush().await;
        }
    }

    // The one-touch scan: 3× the disk budget in distinct, never-reused blocks.
    let rejected_before = engine.counters().snapshot().blocks_rejected;
    for i in 0..36u64 {
        let name = format!("scan-{i}.parquet");
        let body = pattern(9000 + i, 8 * MIB);
        seed(&store, &name, body.clone()).await;
        let (_, _, got) = read_all(&engine, &key(&name), ReadRange::Full)
            .await
            .expect("scan read");
        assert_eq!(got, body, "scan bytes must always be served correctly");
        engine.flush().await;
    }
    let rejected_during_scan = engine.counters().snapshot().blocks_rejected - rejected_before;

    // Re-read the working set and count the backend fills it costs.
    let fills_before = engine.counters().snapshot().backend_fills;
    for (name, body) in &working_set {
        let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("re-read working set");
        assert_eq!(&got, body, "working set must re-read byte-identically");
    }
    let refills = engine.counters().snapshot().backend_fills - fills_before;
    (refills, rejected_during_scan)
}

/// Scan resistance (#15): a one-touch scan larger than the cache must not evict
/// the hot working set. With admission on, re-reading the working set after a
/// 3×-cache scan costs (near-)zero backend refills and the scan's own blocks are
/// mostly rejected; with admission off (the pre-#15 admit-everything path) the
/// same scan evicts the working set and the re-read refills it. This is the
/// pathology behind the constrained TPC-H profile's poor warm speedup.
#[tokio::test(flavor = "multi_thread")]
async fn scan_resistant_admission_protects_the_working_set() {
    let (refills_on, rejected_on) = scan_resistance_run(true).await;
    let (refills_off, _rejected_off) = scan_resistance_run(false).await;

    // The scan's blocks are (almost) all rejected: 36 one-touch blocks, none
    // reused, none worth a working-set slot.
    assert!(
        rejected_on >= 34,
        "admission must reject the one-touch scan's blocks, rejected {rejected_on} of 36"
    );
    // The acceptance criterion: the hot set survives the scan, so its re-read is
    // (near-)free. Allow a tiny slack for foyer's async reclaimer under load.
    assert!(
        refills_on <= 2,
        "working set re-read after the scan must be near-zero refills with admission on, \
         got {refills_on}"
    );
    // The control proves the pathology exists: without admission the same scan
    // evicts the working set and the re-read pays for it.
    assert!(
        refills_off >= 5,
        "without admission the scan should evict the working set (expected the re-read to \
         refill most of the 6 blocks), got {refills_off}"
    );
    assert!(
        refills_on < refills_off,
        "admission must cut working-set refills: on={refills_on} off={refills_off}"
    );
}

/// HEAD is served from cached metadata when present (no backend traffic) and
/// carries the same values a direct origin HEAD would.
#[tokio::test(flavor = "multi_thread")]
async fn head_serves_cached_metadata_with_origin_parity() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    seed(&store, "meta.parquet", pattern(5, 3 * MIB)).await;
    let origin = PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone()));
    let k = key("meta.parquet");

    let want = origin.head(&k).await.expect("origin HEAD");
    let first = engine.head(&k).await.expect("first HEAD");
    assert_eq!(first, want, "HEAD must match the origin's metadata");
    assert_eq!(calls.snapshot().1, 1);

    let second = engine.head(&k).await.expect("second HEAD");
    assert_eq!(second, want);
    assert_eq!(calls.snapshot(), (0, 1), "second HEAD is served from cache");

    // GET after HEAD reuses the mapping: fills happen, but no second HEAD.
    let (meta, _, _) = read_all(&engine, &k, ReadRange::Full).await.expect("GET");
    assert_eq!(meta, want, "GET/HEAD metadata parity");
    assert_eq!(calls.snapshot().1, 1);
}

/// Unsatisfiable ranges are rejected with `InvalidRange` (the cache resolves
/// ranges locally, per the interface contract), empty objects serve empty bodies,
/// and missing keys/buckets propagate the origin's errors.
#[tokio::test(flavor = "multi_thread")]
async fn range_resolution_matches_s3_semantics() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let size = 4 * MIB;
    seed(&store, "small.bin", pattern(2, size)).await;
    seed(&store, "empty.bin", Bytes::new()).await;

    let k = key("small.bin");
    for bad in [
        ReadRange::From(size),
        ReadRange::From(size + 1),
        ReadRange::Bounded(size, size + 10),
        ReadRange::Bounded(10, 5), // inverted
        ReadRange::Suffix(0),
    ] {
        match read_all(&engine, &k, bad).await {
            Err(ReadError::InvalidRange) => {}
            other => panic!("{bad:?} must be InvalidRange, got {other:?}"),
        }
    }

    // A full GET of an empty object is an empty 200, not an error.
    let (meta, range, got) = read_all(&engine, &key("empty.bin"), ReadRange::Full)
        .await
        .expect("empty GET");
    assert_eq!((meta.size, range, got.len()), (0, 0..0, 0));
    // Any explicit range against an empty object is unsatisfiable.
    match read_all(&engine, &key("empty.bin"), ReadRange::Suffix(5)).await {
        Err(ReadError::InvalidRange) => {}
        other => panic!("suffix of empty object must be InvalidRange, got {other:?}"),
    }

    match read_all(&engine, &key("nope.bin"), ReadRange::Full).await {
        Err(ReadError::NoSuchKey) => {}
        other => panic!("missing key must be NoSuchKey, got {other:?}"),
    }
    // The server serves a configured bucket set (#235). A request for a bucket
    // outside the set is rejected with NoSuchBucket before it reaches the origin.
    let other_bucket = CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: "other-bucket".to_owned(),
        key: "nope.bin".to_owned(),
    };
    match read_all(&engine, &other_bucket, ReadRange::Full).await {
        Err(ReadError::NoSuchBucket) => {}
        other => panic!("unconfigured bucket must be NoSuchBucket, got {other:?}"),
    }
}

/// Overwriting an object mid-read is detected by the ETag check: the
/// in-flight request fails rather than serving mixed versions, and the very
/// next read serves the new version coherently — never stale bytes.
#[tokio::test(flavor = "multi_thread")]
async fn etag_change_never_serves_mixed_or_stale_bytes() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let size = 16 * MIB; // two blocks
    let old = pattern(11, size);
    let new = pattern(77, size);
    seed(&store, "mut.bin", old.clone()).await;
    let k = key("mut.bin");

    // Cache the mapping and block 0 of the old version.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Bounded(0, TEST_BLOCK_BYTES - 1))
        .await
        .expect("read old block 0");
    assert_eq!(got, old.slice(0..TEST_BLOCK_BYTES as usize));

    // Overwrite behind the cache's back: same size, new ETag.
    seed(&store, "mut.bin", new.clone()).await;

    // Block 1 was never cached; its fill sees the new ETag and must fail the
    // request instead of serving new bytes under the old version's metadata.
    match read_all(&engine, &k, ReadRange::From(TEST_BLOCK_BYTES)).await {
        Err(ReadError::Backend(msg)) => {
            assert!(
                msg.contains("changed during read"),
                "unexpected error: {msg}"
            )
        }
        Ok(_) => panic!("mixed-version read must fail"),
        Err(other) => panic!("expected a backend error, got {other:?}"),
    }

    // The failed fill refreshed the key→ETag mapping, so subsequent reads
    // serve the new version — including block 0, whose old-version cache
    // entry is now unreachable by key construction.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("re-read");
    assert_eq!(got, new, "post-overwrite read must serve the new version");
}

/// Read atomicity across a block boundary under a concurrent overwrite (#213).
///
/// S3 guarantees a GET returns the complete old object or the complete new
/// one, never a mix. Verglas serves a multi-block read one covering block at a
/// time, and every block is keyed by ETag: a warm cache hit returns the
/// mapping's version, but a fill returns whatever the backend now holds. So a
/// mutable object overwritten while its old mapping is still warm can tear —
/// block 0 served from cache (old), a later block filled at the new version.
///
/// The fix (#213) commits the object version once, before the body stream is
/// built: a multi-block read of a mutable mapping revalidates first, rotating
/// to the current version if it changed. Every covering block then resolves at
/// that one committed ETag, so the whole read is wholly one version.
///
/// This pins the tear deterministically: warm the old version's mapping and
/// block 0 (within the mutable TTL, so a plain read would serve them straight
/// from cache), overwrite the backend, then a full multi-block read must serve
/// the whole *new* object — never the cached old block 0 followed by a fill of
/// the new version's block 1.
#[tokio::test(flavor = "multi_thread")]
async fn read_across_block_boundary_is_atomic_under_concurrent_overwrite() {
    let dir = TempDir::new().expect("temp dir");
    // A generous mutable TTL: without the #213 commit, the warm old mapping and
    // block 0 would be served straight from cache with no revalidation, and the
    // uncached block 1 would fill at the new version — the tear.
    let (store, engine, _) =
        counting_engine_cfg(&cache_config_ttl(&dir, 64 * MIB, 128 * MIB, 3600)).await;
    let size = 16 * MIB; // exactly two 8 MiB blocks
    let old = pattern(11, size);
    let new = pattern(77, size);
    seed(&store, "atomic.bin", old.clone()).await;
    let k = key("atomic.bin");

    // Warm block 0 and the mapping under the old version; leave block 1
    // uncached (only read the first block's range).
    let (_, _, got) = read_all(&engine, &k, ReadRange::Bounded(0, TEST_BLOCK_BYTES - 1))
        .await
        .expect("warm old block 0");
    assert_eq!(got, old.slice(0..TEST_BLOCK_BYTES as usize));

    // Overwrite behind the cache's back: same size, new ETag. The mapping is
    // NOT invalidated — the read must commit the version itself.
    seed(&store, "atomic.bin", new.clone()).await;

    // A full multi-block read. Every covering block must be the same version:
    // the read commits the version up front, sees the object changed, and
    // serves the whole new object. It must never mix the cached old block 0
    // with a new-version block 1.
    let (meta, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("multi-block read must commit one version, not tear");
    assert_eq!(
        got, new,
        "a multi-block read of an overwritten object must serve wholly the new \
         version, never a torn mix of the cached old block 0 and a new block 1"
    );
    assert_eq!(meta.size, new.len() as u64);

    // A single-block read stays on the zero-revalidation warm path: it cannot
    // span a boundary, so it never tears and needs no version commit. Its one
    // block is served at whatever version resolves — here still coherent.
    let (_, _, one_block) = read_all(&engine, &k, ReadRange::Bounded(0, TEST_BLOCK_BYTES - 1))
        .await
        .expect("single-block re-read");
    assert_eq!(
        one_block,
        new.slice(0..TEST_BLOCK_BYTES as usize),
        "the committed new version is now cached and served"
    );
}

/// Ownership discipline (#17): when the ring says another node owns the key,
/// the peer rung runs (once per block) before the backend fill; when this
/// node owns the key, the peer rung is skipped and the fill happens here.
#[tokio::test(flavor = "multi_thread")]
async fn miss_ladder_consults_peer_rung_by_ring_ownership() {
    let body = pattern(4, 12 * MIB); // two blocks

    // Case 1: a remote node owns every key → peer asked once per block, and
    // the no-op miss degrades to a correct backend fill.
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    seed(&store, "owned-remote.bin", body.clone()).await;
    let local = NodeId::new("local");
    let peer = RecordingPeer::default();
    let engine = HybridCacheEngine::new(
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        peer.clone(),
        FixedOwnerRing {
            members: vec![local.clone(), NodeId::new("remote")],
        },
        local.clone(),
        &cache_config(&dir, 64 * MIB, 128 * MIB),
    )
    .await
    .expect("build engine");
    // Resolve the mapping first (HEAD) so the GET exercises the block
    // ladder for every block; an unmapped GET's first block rides the
    // mapping-resolving fill instead, which is where #29's peer metadata
    // protocol will slot in at N>1.
    engine
        .head(&key("owned-remote.bin"))
        .await
        .expect("resolve mapping");
    let (_, _, got) = read_all(&engine, &key("owned-remote.bin"), ReadRange::Full)
        .await
        .expect("read");
    assert_eq!(got, body);
    assert_eq!(
        peer.calls.load(Ordering::Relaxed),
        2,
        "one peer fetch per block before the backend fill"
    );
    assert_eq!(engine.counters().snapshot().backend_fills, 2);

    // Case 2: this node owns every key (cluster of one) → it is the pod's
    // fill point; the peer transport is never consulted.
    let dir = TempDir::new().expect("temp dir");
    let peer = RecordingPeer::default();
    let engine = HybridCacheEngine::new(
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        peer.clone(),
        RendezvousRing::single(local.clone()),
        local,
        &cache_config(&dir, 64 * MIB, 128 * MIB),
    )
    .await
    .expect("build engine");
    let (_, _, got) = read_all(&engine, &key("owned-remote.bin"), ReadRange::Full)
        .await
        .expect("read");
    assert_eq!(got, body);
    assert_eq!(peer.calls.load(Ordering::Relaxed), 0, "owner fills locally");
}

/// The peer-serving endpoint (#29): `local_block` serves an owned, cached
/// block to a peer straight from the local tiers, meters it, and — crucially —
/// never fills from the backend, so a peer request can never trigger a fill.
/// A version this node does not hold (a stale ETag) is a clean miss, never the
/// wrong bytes.
#[tokio::test(flavor = "multi_thread")]
async fn local_block_serves_owned_cached_block_from_cache_only() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let body = pattern(9, 3 * MIB); // one block
    seed(&store, "served.bin", body.clone()).await;
    let k = key("served.bin");

    // Warm the block into the local cache via a normal read.
    let (meta, _, got) = read_all(&engine, &k, ReadRange::Full).await.expect("warm");
    assert_eq!(got, body);
    let etag = meta.e_tag.clone().expect("cacheable object has an etag");
    let bk = verglas_core::BlockKey {
        object: k.clone(),
        etag: etag.clone(),
        block_bytes: TEST_BLOCK_BYTES,
        block_index: 0,
    };

    let (gets_before, _) = calls.snapshot();
    let served = engine.local_block(&bk).await;
    assert_eq!(
        served.as_ref(),
        Some(&body),
        "a peer is served the exact cached block"
    );
    let (gets_after, _) = calls.snapshot();
    assert_eq!(
        gets_before, gets_after,
        "serving a peer must never fill from the backend"
    );
    let snap = engine.counters().snapshot();
    assert_eq!(snap.peer_served_blocks, 1);
    assert_eq!(snap.peer_served_bytes, body.len() as u64);

    // A version this node does not hold is a clean miss, never wrong bytes.
    let stale = verglas_core::BlockKey {
        object: k.clone(),
        etag: "etag-does-not-exist".to_owned(),
        block_bytes: TEST_BLOCK_BYTES,
        block_index: 0,
    };
    assert_eq!(engine.local_block(&stale).await, None, "stale ETag misses");
    // An index past the object is a miss too.
    let past = verglas_core::BlockKey {
        object: k,
        etag,
        block_bytes: TEST_BLOCK_BYTES,
        block_index: 9,
    };
    assert_eq!(engine.local_block(&past).await, None);
    let (gets_end, _) = calls.snapshot();
    assert_eq!(gets_after, gets_end, "peer misses never touch the backend");
}

/// A non-owner keeps a DRAM replica of a peer-fetched block, but `local_block`
/// must not serve it to peers: owners are the single intra-pod source, so
/// replicas never fan out (WHITEPAPER §3.3 replica policy).
#[tokio::test(flavor = "multi_thread")]
async fn local_block_refuses_a_non_owned_replica() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let body = pattern(11, 3 * MIB);
    seed(&store, "replica.bin", body.clone()).await;
    let local = NodeId::new("local");
    let engine = HybridCacheEngine::new(
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        RecordingPeer::default(),
        FixedOwnerRing {
            members: vec![local.clone(), NodeId::new("remote")],
        },
        local.clone(),
        &cache_config(&dir, 64 * MIB, 128 * MIB),
    )
    .await
    .expect("build engine");
    let k = key("replica.bin");

    // Read the remote-owned key: it backend-fills locally and keeps a replica.
    let (meta, _, got) = read_all(&engine, &k, ReadRange::Full).await.expect("read");
    assert_eq!(got, body);
    let etag = meta.e_tag.expect("etag");
    let bk = verglas_core::BlockKey {
        object: k,
        etag,
        block_bytes: TEST_BLOCK_BYTES,
        block_index: 0,
    };
    assert_eq!(
        engine.local_block(&bk).await,
        None,
        "a replica must not satisfy a peer fetch"
    );
    assert_eq!(engine.counters().snapshot().peer_served_blocks, 0);
}

/// The write path's invalidation hook (#9/#120): invalidating a key removes
/// its key→ETag mapping, so the next read re-resolves against the origin and
/// serves the new version — while un-invalidated cached objects keep serving
/// with zero backend traffic.
#[tokio::test(flavor = "multi_thread")]
async fn invalidation_drops_the_mapping_so_reads_serve_the_new_version() {
    use verglas_s3::Invalidation;

    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let old = pattern(21, 4 * MIB);
    let new = pattern(42, 4 * MIB);
    seed(&store, "mut.bin", old.clone()).await;
    seed(&store, "other.bin", pattern(63, MIB)).await;
    let k = key("mut.bin");

    // Cache the object, then overwrite it at the origin (as the write
    // passthrough does after a durable PUT, before firing invalidation).
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full).await.expect("warm");
    assert_eq!(got, old);
    let (_, _, got) = read_all(&engine, &key("other.bin"), ReadRange::Full)
        .await
        .expect("warm other");
    assert_eq!(got, pattern(63, MIB));
    seed(&store, "mut.bin", new.clone()).await;

    // v0 pre-invalidation staleness: the mapping still points at the old
    // version (this is exactly what the hook exists to fix).
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full).await.expect("stale");
    assert_eq!(got, old, "without invalidation the old version is served");

    engine
        .invalidate(std::slice::from_ref(&k))
        .await
        .expect("invalidate");

    // The very next read must serve the new version — no stale bytes.
    let (meta, _, got) = read_all(&engine, &k, ReadRange::Full).await.expect("fresh");
    assert_eq!(
        got, new,
        "post-invalidation read must serve the new version"
    );
    assert_ne!(meta.e_tag, None);

    // Un-invalidated keys are untouched: served with zero backend requests.
    let before = calls.snapshot();
    let (_, _, got) = read_all(&engine, &key("other.bin"), ReadRange::Full)
        .await
        .expect("other still cached");
    assert_eq!(got, pattern(63, MIB));
    assert_eq!(calls.snapshot(), before, "invalidation must be per-key");

    // Invalidating a never-cached key is a no-op success (deletes of cold
    // objects must not fail the write ack).
    engine
        .invalidate(&[key("never-seen.bin")])
        .await
        .expect("invalidate cold key");
}

/// A cold GET of an uncached object costs exactly one backend request per
/// covering block — the key→ETag mapping is derived from the first fill's
/// own response metadata, not from a separate HEAD (review follow-up on
/// #121: kill the cold-object double round-trip).
#[tokio::test(flavor = "multi_thread")]
async fn cold_get_issues_a_single_backend_request() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let body = pattern(31, 4 * MIB); // single block
    seed(&store, "cold.parquet", body.clone()).await;
    let k = key("cold.parquet");

    let (meta, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("cold GET");
    assert_eq!(got, body);
    assert_ne!(meta.e_tag, None);
    assert_eq!(
        calls.snapshot(),
        (1, 0),
        "cold GET must be exactly one backend GET and zero HEADs"
    );
    let counters = engine.counters().snapshot();
    assert_eq!(counters.backend_fills, 1);
    assert_eq!(counters.backend_heads, 0);

    // The fill both resolved the mapping and admitted the block: the second
    // read is served entirely from cache.
    let (_, _, again) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("re-read");
    assert_eq!(again, body);
    assert_eq!(
        calls.snapshot(),
        (1, 0),
        "re-read must not touch the backend"
    );

    // Multi-block cold GET: one backend GET per covering block, still no
    // HEAD, even for a range that starts mid-object.
    let big = pattern(33, 20 * MIB); // three blocks
    seed(&store, "cold-big.parquet", big.clone()).await;
    let range = ReadRange::From(9 * MIB); // starts inside block 1
    let (_, _, got) = read_all(&engine, &key("cold-big.parquet"), range)
        .await
        .expect("cold ranged GET");
    assert_eq!(got, big.slice((9 * MIB) as usize..));
    assert_eq!(
        calls.snapshot(),
        (3, 0),
        "two covering blocks, two backend GETs, still zero HEADs"
    );
}

/// The DRAM ceiling tripwire (#122): across shard counts, a warmed working
/// set larger than the memory tier must not stay DRAM-resident beyond the
/// budget. foyer evicts per shard and only guarantees a shard stays within
/// its capacity if every entry fits in one shard — the engine's shard math
/// must therefore never produce a per-shard capacity below one block (that
/// is exactly the 8-shard stranding bug this test would have caught).
#[tokio::test(flavor = "multi_thread")]
async fn dram_ceiling_holds_across_shard_counts() {
    // (dram budget, block-memory-tier capacity in blocks). The engine
    // derives its shard count from the tier: 32 MiB -> 1 shard,
    // 128 MiB -> 4, 256 MiB -> 8 (one shard per 32 MiB, capped at 8).
    for (dram, mem_blocks) in [(96 * MIB, 4u64), (192 * MIB, 16), (320 * MIB, 32)] {
        let dir = TempDir::new().expect("temp dir");
        let working_set = mem_blocks + 8;
        // 2x the working set: foyer's disk blocks carry ~16 KiB of framing
        // each (the device floor-divides capacity into fewer regions than
        // capacity/8 MiB) and its reclaimer keeps clean regions ahead of
        // demand, evicting live blocks when headroom is tight. This test
        // pins DRAM behavior; give the disk enough slack that reclaim never
        // touches a live block.
        let capacity = working_set * 2 * 8 * MIB;
        let (store, engine, calls) = counting_engine(&dir, capacity, dram).await;

        // Warm a working set 8 blocks larger than the memory tier.
        let objects: Vec<(String, Bytes)> = (0..working_set)
            .map(|i| (format!("ws-{i}.bin"), pattern(500 + i, 8 * MIB)))
            .collect();
        for (name, body) in &objects {
            seed(&store, name, body.clone()).await;
            let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
                .await
                .expect("warm");
            assert_eq!(&got, body);
            engine.flush().await;
        }

        // The direct ceiling check: everything the two DRAM caches hold,
        // as charged by their weighters, must fit the budget minus the
        // fixed 48 MiB disk-write pipeline reservation. Stranded entries
        // (a shard whose capacity can't fit one block, #122) show up here.
        let dram_cache_budget = dram - 48 * MIB;
        assert!(
            engine.dram_usage() <= dram_cache_budget,
            "dram={dram}: {} bytes resident in DRAM against a {dram_cache_budget}-byte \
             cache budget — entries are stranded over the ceiling",
            engine.dram_usage()
        );

        // Re-read every object once: byte-identical, no backend traffic,
        // and the ceiling still holds afterwards.
        let gets_after_warm = calls.snapshot().0;
        for (name, body) in &objects {
            let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
                .await
                .expect("re-read");
            assert_eq!(&got, body, "dram={dram}: re-read must be byte-identical");
        }
        assert!(
            engine.dram_usage() <= dram_cache_budget,
            "dram={dram}: over the ceiling after the re-read pass"
        );
        // The pathology this pins — block churn sweeping mappings out of a
        // shared eviction domain — refills on the order of the whole working
        // set. Foyer's async disk reclaimer can evict a handful of live blocks
        // under slow IO, and the exact set it picks is hash-distribution
        // dependent (the #178 generation field is part of the block key's hash,
        // so the per-shard placement — and thus which few blocks the reclaimer
        // evicts — differs from the pre-generation layout). The tripwire sits an
        // order of magnitude below the pathology (≈24 refills if the domains
        // merge), not a hair above the reclaimer noise: a third of the working
        // set still fails instantly if the eviction domains merge.
        let extra_fills = calls.snapshot().0 - gets_after_warm;
        assert!(
            extra_fills <= working_set / 3,
            "dram={dram}: {extra_fills} backend refills re-reading a {working_set}-block \
             working set (mappings and blocks must not share an eviction domain)"
        );
    }
}

/// Test gate for deterministic race interleavings: when armed, the next
/// backend response is computed and then *parked* — the test learns the
/// response is in hand (`entered`), does its racing work, and then releases
/// it. This reproduces "backend response produced before a write became
/// durable, delivered after its invalidation ran" exactly (#124).
#[derive(Clone)]
struct Gate {
    /// Arm the gate for the next backend call.
    armed: Arc<std::sync::atomic::AtomicBool>,
    /// Signals the test that a response is parked.
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    /// Permits released by the test to let the parked response through.
    release: Arc<tokio::sync::Semaphore>,
}

/// An `ObjectRead` that parks every GET after the origin has accepted it, so a
/// test can observe how many distinct block fills the body stream dispatches
/// before any one completes.
struct WindowedRead<R> {
    /// The wrapped origin reader.
    inner: R,
    /// Reports the index of each block whose origin GET began.
    entered: tokio::sync::mpsc::UnboundedSender<u64>,
    /// Permits the test releases to let parked GETs complete.
    release: Arc<tokio::sync::Semaphore>,
}

impl<R: ObjectRead> ObjectRead for WindowedRead<R> {
    /// Starts the origin GET, reports its block index, then waits for release.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let index = match range {
            ReadRange::Bounded(start, _) => start / TEST_BLOCK_BYTES,
            other => panic!("block fill must use a bounded range, got {other:?}"),
        };
        let got = self.inner.get(key, range).await;
        self.entered.send(index).expect("test receiver alive");
        self.release
            .acquire()
            .await
            .expect("test releases every parked fill")
            .forget();
        got
    }

    /// Delegates metadata resolution without parking it, so the test can warm
    /// only the mapping before it drives the data-block body stream.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.inner.head(key).await
    }
}

/// Builds an engine whose data-block fills remain parked until the test opens
/// them, while metadata resolution continues normally.
async fn windowed_engine(
    dir: &TempDir,
) -> (
    Arc<InMemory>,
    HybridCacheEngine<
        WindowedRead<PassthroughRead>,
        verglas_core::peer::NoopPeerFetch,
        RendezvousRing,
    >,
    tokio::sync::mpsc::UnboundedReceiver<u64>,
    Arc<tokio::sync::Semaphore>,
) {
    let store = Arc::new(InMemory::new());
    let (entered_tx, entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let backend = WindowedRead {
        inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        entered: entered_tx,
        release: Arc::clone(&release),
    };
    let engine = HybridCacheEngine::single_node(backend, &cache_config(dir, 64 * MIB, 128 * MIB))
        .await
        .expect("build engine");
    (store, engine, entered_rx, release)
}

/// #250: a cold multi-block response keeps a fixed look-ahead window in flight
/// while yielding exact bytes in order. The mapping is warmed by HEAD first so
/// every observed GET comes from the body stream rather than its first-fill
/// probe.
#[tokio::test(flavor = "multi_thread")]
async fn cold_multi_block_body_starts_four_fills_before_any_complete() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, mut entered, release) = windowed_engine(&dir).await;
    let body = pattern(67, 5 * TEST_BLOCK_BYTES);
    let k = key("windowed.bin");
    seed(&store, "windowed.bin", body.clone()).await;

    // Establish only the mapping. The first data block must then be dispatched
    // by `block_body`, where this regression exercises the look-ahead policy.
    let meta = engine.head(&k).await.expect("warm mapping");
    assert_eq!(meta.size, body.len() as u64);

    let reader = {
        let engine = engine.clone();
        let k = k.clone();
        tokio::spawn(async move { read_all(&engine, &k, ReadRange::Full).await })
    };

    let mut first_window = Vec::new();
    for _ in 0..4 {
        let index = tokio::time::timeout(std::time::Duration::from_millis(250), entered.recv())
            .await
            .expect("four fills must begin before any fill completes")
            .expect("fill task reports its block index");
        first_window.push(index);
    }
    first_window.sort_unstable();
    assert_eq!(first_window, vec![0, 1, 2, 3]);

    // Releasing the first window makes room for the final block. Its completion
    // proves the bounded window continues through the whole response.
    release.add_permits(4);
    let final_index = tokio::time::timeout(std::time::Duration::from_millis(250), entered.recv())
        .await
        .expect("the fifth fill begins after one window completes")
        .expect("fill task reports its block index");
    assert_eq!(final_index, 4);
    release.add_permits(1);

    let (_, _, got) = reader
        .await
        .expect("reader joins")
        .expect("body remains readable");
    assert_eq!(got, body, "the ordered response remains byte-exact");
}

impl Gate {
    /// Builds a gate and the receiver the test awaits `entered` on.
    fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<()>) {
        let (entered, entered_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Gate {
                armed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                entered,
                release: Arc::new(tokio::sync::Semaphore::new(0)),
            },
            entered_rx,
        )
    }

    /// Parks the next backend response.
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Lets the parked response proceed.
    fn open(&self) {
        self.release.add_permits(1);
    }

    /// Parks the calling backend response if the gate is armed.
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

/// An `ObjectRead` that resolves each response from the wrapped reader
/// first (so its content is fixed at call time) and then parks it while the
/// gate is armed.
struct ParkedRead<R> {
    /// The wrapped reader.
    inner: R,
    /// The park/release gate.
    gate: Gate,
}

impl<R: ObjectRead> ObjectRead for ParkedRead<R> {
    /// Resolves, then parks while armed.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let got = self.inner.get(key, range).await;
        self.gate.park().await;
        got
    }

    /// Resolves, then parks while armed.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        let meta = self.inner.head(key).await;
        self.gate.park().await;
        meta
    }
}

/// Builds an engine whose backend responses can be parked by a [`Gate`],
/// with independent backend call counts.
async fn parked_engine(
    dir: &TempDir,
) -> (
    Arc<InMemory>,
    HybridCacheEngine<
        ParkedRead<CountingRead<PassthroughRead>>,
        verglas_core::peer::NoopPeerFetch,
        RendezvousRing,
    >,
    BackendCalls,
    Gate,
    tokio::sync::mpsc::UnboundedReceiver<()>,
) {
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let (gate, entered) = Gate::new();
    let backend = ParkedRead {
        inner: CountingRead {
            inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
            calls: calls.clone(),
        },
        gate: gate.clone(),
    };
    let engine = HybridCacheEngine::single_node(backend, &cache_config(dir, 64 * MIB, 128 * MIB))
        .await
        .expect("build engine");
    (store, engine, calls, gate, entered)
}

/// The #124 race, GET path: a cold GET's probe response is produced before a
/// racing write becomes durable and delivered after that write's
/// invalidation ran. The racing GET may serve the version it resolved (it
/// overlapped the write), but it must NOT reinstate the stale mapping — the
/// next read must serve the new version (read-after-write for writes
/// through Verglas).
#[tokio::test(flavor = "multi_thread")]
async fn racing_stale_get_response_cannot_reinstate_an_invalidated_mapping() {
    use verglas_s3::Invalidation;

    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _, gate, mut entered) = parked_engine(&dir).await;
    let old = pattern(1, 4 * MIB);
    let new = pattern(2, 4 * MIB);
    seed(&store, "raced.bin", old.clone()).await;
    let k = key("raced.bin");

    // 1. Cold GET issues its mapping-resolving probe; the response (old
    //    version) is computed and parked.
    gate.arm();
    let racer = {
        let engine = engine.clone();
        let k = k.clone();
        tokio::spawn(async move { read_all(&engine, &k, ReadRange::Full).await })
    };
    entered.recv().await.expect("probe parked");

    // 2. The write path: overwrite becomes durable at the origin, then the
    //    key is invalidated, then the writer is acked (#120/#21 ordering).
    seed(&store, "raced.bin", new.clone()).await;
    engine
        .invalidate(std::slice::from_ref(&k))
        .await
        .expect("invalidate");

    // 3. The parked (stale) response is delivered.
    gate.open();
    let (_, _, raced) = racer
        .await
        .expect("join")
        .expect("racing GET must not error");
    assert_eq!(
        raced, old,
        "the overlapped GET serves the version it resolved"
    );

    // 4. Read-after-write: the acked write must be visible now.
    let (meta, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("read after acked write");
    assert_eq!(
        got, new,
        "stale mapping was reinstated after invalidation (#124)"
    );
    assert_eq!(meta.size, new.len() as u64);
}

/// The #124 race, HEAD path — with the old version's blocks already cached,
/// so a reinstated stale mapping would serve stale bytes with zero backend
/// traffic and no self-correction.
#[tokio::test(flavor = "multi_thread")]
async fn racing_stale_head_response_cannot_reinstate_an_invalidated_mapping() {
    use verglas_s3::Invalidation;

    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _, gate, mut entered) = parked_engine(&dir).await;
    let old = pattern(3, 4 * MIB);
    let new = pattern(4, 4 * MIB);
    seed(&store, "raced-head.bin", old.clone()).await;
    let k = key("raced-head.bin");

    // Warm the old version (mapping + block cached), then drop the mapping
    // so the next HEAD has to resolve against the backend. The old block
    // stays cached — that is by design (unreachable once the mapping moves).
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full).await.expect("warm");
    assert_eq!(got, old);
    engine
        .invalidate(std::slice::from_ref(&k))
        .await
        .expect("drop mapping");

    // 1. Cold HEAD parks with the old version's metadata in hand.
    gate.arm();
    let racer = {
        let engine = engine.clone();
        let k = k.clone();
        tokio::spawn(async move { engine.head(&k).await })
    };
    entered.recv().await.expect("HEAD parked");

    // 2. Durable overwrite + invalidation + ack.
    seed(&store, "raced-head.bin", new.clone()).await;
    engine
        .invalidate(std::slice::from_ref(&k))
        .await
        .expect("invalidate");

    // 3. Deliver the stale HEAD response.
    gate.open();
    let raced = racer.await.expect("join").expect("racing HEAD");
    assert_eq!(raced.size, old.len() as u64, "overlapped HEAD may see old");

    // 4. Read-after-write: with the old block still cached, a reinstated
    //    stale mapping would serve `old` from cache with zero backend
    //    traffic. The acked write must win instead.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("read after acked write");
    assert_eq!(
        got, new,
        "stale mapping was reinstated after invalidation (#124)"
    );
}

// --- Cold suffix reads: derive the mapping from the suffix GET (issue #131) --

/// #131: a cold suffix-range read derives the key→ETag mapping from the suffix
/// GET's own 206 response — exactly one backend GET and zero HEADs. This is
/// the cold Parquet-footer read shape; the old path paid an extra HEAD.
#[tokio::test(flavor = "multi_thread")]
async fn cold_suffix_read_issues_one_get_and_no_head() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let size = 5 * MIB; // single block: the suffix's tail block is block 0
    let body = pattern(41, size);
    seed(&store, "footer.parquet", body.clone()).await;
    let k = key("footer.parquet");
    let tail = (size as usize) - 4096;

    let (meta, range, got) = read_all(&engine, &k, ReadRange::Suffix(4096))
        .await
        .expect("cold suffix read");
    assert_eq!(got, body.slice(tail..), "suffix bytes");
    assert_eq!(range, (size - 4096)..size, "resolved tail range");
    assert_ne!(
        meta.e_tag, None,
        "mapping resolved from the suffix response"
    );
    assert_eq!(
        calls.snapshot(),
        (1, 0),
        "cold suffix read must be exactly one backend GET and zero HEADs"
    );
    assert_eq!(engine.counters().snapshot().backend_heads, 0);

    // The mapping was admitted (#131 option 1: serve-only, tail block NOT
    // cached). The re-read reuses the cached mapping — no new HEAD — and
    // fills the aligned block through the ladder.
    let (_, _, again) = read_all(&engine, &k, ReadRange::Suffix(4096))
        .await
        .expect("re-read");
    assert_eq!(again, body.slice(tail..));
    assert_eq!(
        engine.counters().snapshot().backend_heads,
        0,
        "re-read reuses the cached mapping"
    );
}

/// #131: a suffix larger than the object serves the whole object (S3 clamps
/// `bytes=-N` with N≥size to the whole object), byte-identical to a direct
/// read, still one GET and zero HEADs.
#[tokio::test(flavor = "multi_thread")]
async fn suffix_larger_than_object_serves_whole_object() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let size = 3 * MIB;
    let body = pattern(43, size);
    seed(&store, "small-footer.parquet", body.clone()).await;
    let origin = PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone()));
    let k = key("small-footer.parquet");

    let (want_meta, want_range, want) = read_all(&origin, &k, ReadRange::Suffix(size + 999))
        .await
        .expect("origin suffix");
    let (meta, range, got) = read_all(&engine, &k, ReadRange::Suffix(size + 999))
        .await
        .expect("engine suffix");
    assert_eq!(got, want, "whole object served");
    assert_eq!(range, want_range);
    assert_eq!(range, 0..size, "whole-object range");
    assert_eq!(meta.size, want_meta.size);
    assert_eq!(meta.e_tag, want_meta.e_tag);
    assert_eq!(
        calls.snapshot(),
        (1, 0),
        "one GET, zero HEADs even when the suffix exceeds the object"
    );
}

/// #131 + #124: a stale suffix response produced before a racing write became
/// durable, delivered after that write's invalidation ran, must NOT reinstate
/// the old mapping (mirror of the GET/HEAD race tests, for the suffix path).
#[tokio::test(flavor = "multi_thread")]
async fn racing_stale_suffix_response_cannot_reinstate_an_invalidated_mapping() {
    use verglas_s3::Invalidation;

    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _, gate, mut entered) = parked_engine(&dir).await;
    let old = pattern(5, 4 * MIB);
    let new = pattern(6, 4 * MIB);
    seed(&store, "raced-suffix.bin", old.clone()).await;
    let k = key("raced-suffix.bin");
    let tail = ((4 * MIB) as usize) - 1024;

    // 1. Cold suffix read issues its suffix GET; the response (old version) is
    //    computed and parked.
    gate.arm();
    let racer = {
        let engine = engine.clone();
        let k = k.clone();
        tokio::spawn(async move { read_all(&engine, &k, ReadRange::Suffix(1024)).await })
    };
    entered.recv().await.expect("suffix response parked");

    // 2. Durable overwrite + invalidation (+ ack), while the suffix is parked.
    seed(&store, "raced-suffix.bin", new.clone()).await;
    engine
        .invalidate(std::slice::from_ref(&k))
        .await
        .expect("invalidate");

    // 3. Deliver the stale suffix response.
    gate.open();
    let (_, _, raced) = racer
        .await
        .expect("join")
        .expect("racing suffix read must not error");
    assert_eq!(
        raced,
        old.slice(tail..),
        "the overlapped suffix serves the version it resolved"
    );

    // 4. Read-after-write: the acked write must be visible now.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("read after acked write");
    assert_eq!(
        got, new,
        "stale suffix mapping was reinstated after invalidation (#124)"
    );
}

// --- Singleflight fill deduplication (issue #19) ----------------------------

/// An `ObjectRead` that fails every request with a backend (500) error while
/// its flag is set — the injectable "backend returns 500" of #19's failure
/// acceptance test.
struct FlakyRead<R> {
    /// The wrapped reader, used when not failing.
    inner: R,
    /// When true, every get/head returns a backend error.
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl<R: ObjectRead> ObjectRead for FlakyRead<R> {
    /// Fails while armed, else delegates.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        if self.fail.load(Ordering::Acquire) {
            return Err(ReadError::Backend("injected failure".to_owned()));
        }
        self.inner.get(key, range).await
    }

    /// Fails while armed, else delegates.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        if self.fail.load(Ordering::Acquire) {
            return Err(ReadError::Backend("injected failure".to_owned()));
        }
        self.inner.head(key).await
    }
}

/// Spins (yielding) until the engine's `deduped_fills` counter reaches `target`
/// — i.e. `target` waiters have joined an in-flight fill. Used to make the
/// stampede tests deterministic: release the parked winner only once every
/// other reader has attached, so no straggler can race a second backend GET.
async fn wait_for_deduped<B, P, R>(engine: &HybridCacheEngine<B, P, R>, target: u64)
where
    B: ObjectRead,
    P: PeerFetch + 'static,
    R: Ring + Send + Sync + 'static,
{
    while engine.counters().snapshot().deduped_fills < target {
        tokio::task::yield_now().await;
    }
}

/// #19 acceptance: 500 concurrent cold GETs of one object collapse to exactly
/// one backend GET. The winner's fill is parked so every other reader attaches
/// to the same in-flight fill before it completes (deterministic).
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cold_gets_of_one_object_issue_one_backend_get() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls, gate, mut entered) = parked_engine(&dir).await;
    let body = pattern(51, 4 * MIB); // single block
    seed(&store, "stampede.parquet", body.clone()).await;
    let k = key("stampede.parquet");

    const N: u64 = 500;
    gate.arm();
    let mut handles = Vec::with_capacity(N as usize);
    for _ in 0..N {
        let engine = engine.clone();
        let k = k.clone();
        handles.push(tokio::spawn(async move {
            read_all(&engine, &k, ReadRange::Full).await
        }));
    }

    // The winner's backend GET is parked; wait until the other N-1 readers have
    // joined the in-flight fill, then release.
    entered.recv().await.expect("winner parked");
    wait_for_deduped(&engine, N - 1).await;
    gate.open();

    for h in handles {
        let (_, _, got) = h.await.expect("join").expect("read");
        assert_eq!(got, body);
    }
    assert_eq!(
        calls.snapshot(),
        (1, 0),
        "500 concurrent cold GETs collapse to exactly one backend GET"
    );
    assert_eq!(engine.counters().snapshot().backend_fills, 1);
    assert_eq!(
        engine.inflight_len(),
        0,
        "the in-flight map drains to empty"
    );
}

/// #19 acceptance: a backend 500 reaches every concurrent waiter promptly, the
/// errored entry is removed (errors are never cached), and the next request
/// after the backend recovers triggers a fresh, successful fill.
#[tokio::test(flavor = "multi_thread")]
async fn backend_failure_propagates_to_all_waiters_then_refills() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let backend = CountingRead {
        inner: FlakyRead {
            inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
            fail: fail.clone(),
        },
        calls: calls.clone(),
    };
    let engine = HybridCacheEngine::single_node(backend, &cache_config(&dir, 64 * MIB, 128 * MIB))
        .await
        .expect("build engine");
    let body = pattern(53, 4 * MIB);
    seed(&store, "flaky.parquet", body.clone()).await;
    let k = key("flaky.parquet");

    const N: usize = 50;
    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let engine = engine.clone();
        let k = k.clone();
        handles.push(tokio::spawn(async move {
            read_all(&engine, &k, ReadRange::Full).await
        }));
    }
    for h in handles {
        match h.await.expect("join") {
            Err(ReadError::Backend(_)) => {}
            other => panic!("every waiter must get a backend error, got {other:?}"),
        }
    }

    // Errors are never cached: with the backend recovered, the next read fills
    // fresh (the failed in-flight entries were removed).
    fail.store(false, Ordering::Release);
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("refill after recovery");
    assert_eq!(got, body, "next request after failure refills successfully");
    assert_eq!(engine.inflight_len(), 0);
}

/// #19 waiter-cancellation safety: a disconnecting client must not cancel the
/// shared fill others await. With the fill parked, all-but-one reader are
/// aborted mid-fill; the survivor still completes, off a single backend GET.
#[tokio::test(flavor = "multi_thread")]
async fn disconnecting_waiters_do_not_cancel_the_shared_fill() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls, gate, mut entered) = parked_engine(&dir).await;
    let body = pattern(57, 4 * MIB); // single block
    seed(&store, "survivor.parquet", body.clone()).await;
    let k = key("survivor.parquet");

    const N: u64 = 5;
    gate.arm();
    let mut handles = Vec::with_capacity(N as usize);
    for _ in 0..N {
        let engine = engine.clone();
        let k = k.clone();
        handles.push(tokio::spawn(async move {
            read_all(&engine, &k, ReadRange::Full).await
        }));
    }
    entered.recv().await.expect("winner parked");
    wait_for_deduped(&engine, N - 1).await;

    // One survivor keeps waiting; every other client "disconnects" (its task is
    // aborted) while the fill is still parked.
    let survivor = handles.pop().expect("survivor handle");
    for h in handles {
        h.abort();
    }
    gate.open();

    let (_, _, got) = survivor
        .await
        .expect("survivor join")
        .expect("survivor read");
    assert_eq!(got, body, "the shared fill completes for the survivor");
    assert_eq!(
        calls.snapshot(),
        (1, 0),
        "the fill ran once and was not cancelled by the disconnects"
    );
}

/// #19 × #131 interaction: concurrent suffix and bounded reads of one cold
/// object serve correct bytes for both shapes, and both stampedes collapse.
/// Post-#149 the bufferable suffix reads collapse too, so the assertion bounds
/// the total GETs (bounded-probe + at most one suffix fill), not just the
/// bounded collapse.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_suffix_and_bounded_reads_dedup_correctly() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let size = 5 * MIB; // single block
    let body = pattern(59, size);
    seed(&store, "mixed.parquet", body.clone()).await;
    let k = key("mixed.parquet");
    let tail = (size as usize) - 2048;
    let want_full = body.clone();
    let want_suffix = body.slice(tail..);

    const BOUNDED: usize = 100;
    const SUFFIX: u64 = 5;
    let mut handles = Vec::new();
    for _ in 0..BOUNDED {
        let engine = engine.clone();
        let k = k.clone();
        let want = want_full.clone();
        handles.push(tokio::spawn(async move {
            let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
                .await
                .expect("bounded read");
            assert_eq!(got, want, "bounded read bytes");
        }));
    }
    for _ in 0..SUFFIX {
        let engine = engine.clone();
        let k = k.clone();
        let want = want_suffix.clone();
        handles.push(tokio::spawn(async move {
            let (_, _, got) = read_all(&engine, &k, ReadRange::Suffix(2048))
                .await
                .expect("suffix read");
            assert_eq!(got, want, "suffix read bytes");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    let (gets, heads) = calls.snapshot();
    assert!(
        gets <= SUFFIX + 5,
        "the {BOUNDED} bounded reads must collapse; got {gets} backend GETs"
    );
    assert_eq!(
        heads, 0,
        "neither the bounded nor the suffix path issues a HEAD"
    );
}

// --- Singleflight for cold suffix reads (issue #149) ------------------------

/// Spins (yielding) until `deduped_suffix_fills` reaches `target` — the suffix
/// analogue of [`wait_for_deduped`], so a footer stampede can release its
/// parked winner only once every other reader has attached to the buffered
/// suffix fill. Bounded by the caller's `timeout` so an un-collapsed path fails
/// fast instead of hanging.
async fn wait_for_deduped_suffix<B, P, R>(engine: &HybridCacheEngine<B, P, R>, target: u64)
where
    B: ObjectRead,
    P: PeerFetch + 'static,
    R: Ring + Send + Sync + 'static,
{
    while engine.counters().snapshot().deduped_suffix_fills < target {
        tokio::task::yield_now().await;
    }
}

/// #149 acceptance: N concurrent cold suffix reads of one object (tail under the
/// buffer cap) collapse to exactly one backend GET, and every waiter's bytes are
/// byte-identical to a direct suffix read. The winner's suffix GET is parked so
/// every other reader attaches to the buffered fill before it completes
/// (deterministic, mirroring the #19 block stampede test). This is the Parquet-
/// footer stampede #131 could not collapse while the served value was a stream.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cold_suffix_reads_collapse_to_one_backend_get() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls, gate, mut entered) = parked_engine(&dir).await;
    let size = 5 * MIB; // single block; the tail is well under the buffer cap
    let body = pattern(61, size);
    seed(&store, "stampede-footer.parquet", body.clone()).await;
    let k = key("stampede-footer.parquet");
    let tail = (size as usize) - 4096;
    let want = body.slice(tail..);

    const N: u64 = 200;
    gate.arm();
    let mut handles = Vec::with_capacity(N as usize);
    for _ in 0..N {
        let engine = engine.clone();
        let k = k.clone();
        handles.push(tokio::spawn(async move {
            read_all(&engine, &k, ReadRange::Suffix(4096)).await
        }));
    }

    // The winner's suffix GET is parked; wait until the other N-1 readers have
    // joined the buffered suffix fill, then release. Bounded so a regression to
    // the non-deduplicated path fails fast rather than hanging.
    entered.recv().await.expect("winner parked");
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        wait_for_deduped_suffix(&engine, N - 1),
    )
    .await
    .expect("all suffix waiters attach to one buffered fill");
    gate.open();

    for h in handles {
        let (_, range, got) = h.await.expect("join").expect("suffix read");
        assert_eq!(got, want, "every waiter serves the identical tail");
        assert_eq!(range, (size - 4096)..size, "resolved tail range");
    }
    assert_eq!(
        calls.snapshot(),
        (1, 0),
        "N concurrent cold suffix reads collapse to exactly one backend GET, zero HEADs"
    );
    assert_eq!(
        engine.counters().snapshot().deduped_suffix_fills,
        N - 1,
        "every reader past the winner is counted as a deduped suffix fill"
    );
    assert_eq!(
        engine.counters().snapshot().backend_heads,
        0,
        "the collapsed suffix stampede pays no HEAD"
    );
    assert_eq!(
        engine.inflight_len(),
        0,
        "the suffix flight map drains empty"
    );
}

/// #149: a small DATA-classified suffix (a non-Parquet tail: no footer
/// heuristic, so it routes to the data cache) also collapses under the
/// singleflight — buffering enables dedup independently of metadata pinning.
/// Every waiter serves the identical tail off one backend GET.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cold_data_suffix_reads_collapse() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls, gate, mut entered) = parked_engine(&dir).await;
    let size = 4 * MIB;
    let body = pattern(63, size);
    // `.bin` is neither a Parquet footer nor metadata — a DATA-class suffix.
    seed(&store, "blob.bin", body.clone()).await;
    let k = key("blob.bin");
    let tail = (size as usize) - 8192;
    let want = body.slice(tail..);

    const N: u64 = 50;
    gate.arm();
    let mut handles = Vec::with_capacity(N as usize);
    for _ in 0..N {
        let engine = engine.clone();
        let k = k.clone();
        handles.push(tokio::spawn(async move {
            read_all(&engine, &k, ReadRange::Suffix(8192)).await
        }));
    }
    entered.recv().await.expect("winner parked");
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        wait_for_deduped_suffix(&engine, N - 1),
    )
    .await
    .expect("all data-suffix waiters attach to one buffered fill");
    gate.open();

    for h in handles {
        let (_, _, got) = h.await.expect("join").expect("data suffix read");
        assert_eq!(got, want, "identical data tail off one GET");
    }
    assert_eq!(
        calls.snapshot(),
        (1, 0),
        "the DATA-class suffix stampede collapses to one backend GET",
    );
    assert_eq!(engine.counters().snapshot().deduped_suffix_fills, N - 1);
}

/// #149 boundary: a suffix whose tail exceeds the buffer cap keeps the pre-#149
/// serve-only behavior — it is never deduplicated (each concurrent reader issues
/// its own GET, `deduped_suffix_fills` never moves) and still serves bytes
/// identical to a direct read. Pins the over-cap side of the cap boundary.
#[tokio::test(flavor = "multi_thread")]
async fn over_cap_suffix_reads_are_not_deduplicated() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 256 * MIB, 512 * MIB).await;
    // Object larger than the buffer cap; the suffix tail is above the cap too.
    let size = TEST_BLOCK_BYTES + 2 * MIB;
    let over_cap_len = TEST_BLOCK_BYTES + 1; // one byte past the cap
    let body = pattern(65, size);
    seed(&store, "huge-footer.parquet", body.clone()).await;
    let origin = PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone()));
    let k = key("huge-footer.parquet");
    let (_, _, want) = read_all(&origin, &k, ReadRange::Suffix(over_cap_len))
        .await
        .expect("origin over-cap suffix");

    const N: u64 = 8;
    let mut handles = Vec::with_capacity(N as usize);
    for _ in 0..N {
        let engine = engine.clone();
        let k = k.clone();
        let want = want.clone();
        handles.push(tokio::spawn(async move {
            let (_, _, got) = read_all(&engine, &k, ReadRange::Suffix(over_cap_len))
                .await
                .expect("over-cap suffix read");
            assert_eq!(got, want, "over-cap suffix serves correct bytes");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    assert_eq!(
        engine.counters().snapshot().deduped_suffix_fills,
        0,
        "over-cap suffixes never register a flight, so none are deduplicated"
    );
    let (gets, heads) = calls.snapshot();
    assert_eq!(
        gets, N,
        "each over-cap reader issues its own GET (serve-only, not collapsed)"
    );
    assert_eq!(heads, 0, "the over-cap suffix path still pays no HEAD");
}

/// #149 boundary: a suffix whose tail is exactly the buffer cap IS bufferable,
/// so a stampede at the boundary still collapses to one GET. Pins the at-cap
/// side of the boundary (complementing the over-cap test above). Uses a real
/// tail (object strictly larger than the cap) so the cap-sized suffix is not a
/// whole-object read.
#[tokio::test(flavor = "multi_thread")]
async fn at_cap_suffix_reads_collapse() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls, gate, mut entered) = parked_engine(&dir).await;
    let size = TEST_BLOCK_BYTES + 4096;
    let cap_len = TEST_BLOCK_BYTES; // exactly the buffer cap
    let body = pattern(67, size);
    seed(&store, "cap-footer.parquet", body.clone()).await;
    let k = key("cap-footer.parquet");
    let tail = (size - cap_len) as usize;
    let want = body.slice(tail..);

    const N: u64 = 8;
    gate.arm();
    let mut handles = Vec::with_capacity(N as usize);
    for _ in 0..N {
        let engine = engine.clone();
        let k = k.clone();
        handles.push(tokio::spawn(async move {
            read_all(&engine, &k, ReadRange::Suffix(cap_len)).await
        }));
    }
    entered.recv().await.expect("winner parked");
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        wait_for_deduped_suffix(&engine, N - 1),
    )
    .await
    .expect("at-cap suffix waiters attach to one buffered fill");
    gate.open();

    for h in handles {
        let (_, _, got) = h.await.expect("join").expect("at-cap suffix read");
        assert_eq!(got, want, "identical tail at the cap boundary");
    }
    assert_eq!(
        calls.snapshot(),
        (1, 0),
        "a cap-sized suffix stampede is bufferable and collapses to one GET",
    );
    assert_eq!(engine.counters().snapshot().deduped_suffix_fills, N - 1);
}

// --- Cache purge (issue #138) -----------------------------------------------

/// Purge acceptance (#138): warm a working set, purge, and the next reads are
/// genuine backend fills again — the backend counter proves it — returning
/// bytes byte-identical to a direct origin read. The purge report accounts for
/// the DRAM it freed, and the DRAM tiers are empty afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn purge_makes_next_reads_cold_backend_fills_and_byte_identical() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let origin = PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone()));

    // A small working set of a few objects with an odd tail each.
    let objs = [("a.parquet", 3u64), ("b.parquet", 7), ("c.parquet", 11)];
    for (name, s) in objs {
        seed(&store, name, pattern(s, 5 * MIB + 123)).await;
    }

    // Warm: one cold read each, then confirm a re-read hits cache (zero
    // backend gets) — the precondition purge has to undo.
    for (name, _) in objs {
        read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("warm");
    }
    let after_warm = calls.snapshot();
    for (name, _) in objs {
        read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("hot");
    }
    assert_eq!(
        calls.snapshot(),
        after_warm,
        "warm re-reads must hit cache, issuing no backend gets"
    );

    // Flush so blocks are on disk too; the DRAM tiers still carry the mappings
    // and hot blocks. Purge bumps the generation (#178): the mapping cache is
    // freed synchronously, and the block DRAM becomes reclaimable.
    engine.flush().await;
    let report = engine.purge().await;
    assert_eq!(
        report.generation, 1,
        "the first purge bumps to generation 1"
    );
    assert!(report.mapping_bytes_freed > 0, "warmed mappings freed");
    assert!(
        report.reclaimable_bytes > 0,
        "warmed DRAM blocks are now reclaimable"
    );
    // The bytes stay physically resident (generation-epoch purge, not a wipe),
    // so the live working set reads ~0 while total usage still reflects them.
    assert_eq!(engine.dram_live_bytes(), 0, "no live bytes after purge");
    assert!(
        engine.dram_reclaimable_bytes() > 0,
        "purged bytes are reclaimable, not yet freed"
    );

    // Post-purge: every read is a cold backend fill again, byte-identical to a
    // direct origin read.
    let before_cold = calls.snapshot();
    for (name, _) in objs {
        let k = key(name);
        let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
            .await
            .expect("cold read after purge");
        let (_, _, direct) = read_all(&origin, &k, ReadRange::Full)
            .await
            .expect("origin read");
        assert_eq!(
            got, direct,
            "post-purge read must be byte-identical to the origin"
        );
    }
    assert!(
        calls.snapshot().0 > before_cold.0,
        "post-purge reads must issue backend fills (the cache was cold)"
    );
}

/// Purge under concurrent reads (#138 acceptance): with readers hammering the
/// engine, purges in the middle produce zero errors and zero stale bytes —
/// every read returns the object's true bytes whether it hit cache, raced a
/// generation bump, or refilled cold afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_reads_during_purge_are_never_stale_or_errors() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let want = pattern(5, 6 * MIB + 77);
    seed(&store, "hot.parquet", want.clone()).await;
    let k = key("hot.parquet");

    // Warm once so there is real state to clear.
    read_all(&engine, &k, ReadRange::Full).await.expect("warm");

    // Readers loop reading the object while purges run underneath them.
    let mut readers = Vec::new();
    for _ in 0..8 {
        let engine = engine.clone();
        let k = k.clone();
        let want = want.clone();
        readers.push(tokio::spawn(async move {
            for _ in 0..20 {
                let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
                    .await
                    .expect("read must never error during purge");
                assert_eq!(got, want, "read must never serve stale or partial bytes");
            }
        }));
    }
    // Interleave several purges with the in-flight readers.
    for _ in 0..5 {
        engine.purge().await;
        tokio::task::yield_now().await;
    }
    for r in readers {
        r.await.expect("reader task");
    }
}

/// Purge vs the #124 fence, deterministic (#138): a cold GET's mapping-
/// resolving probe is produced *before* a purge and delivered *after* it. The
/// parked pre-purge response must repopulate nothing — after it is served the
/// DRAM tiers are still empty, and the next read is a fresh cold fill. This
/// pins the fence-interaction reasoning: `purge` bumps every active fence
/// epoch, so an in-flight resolution cannot re-admit into the just-cleared
/// cache.
#[tokio::test(flavor = "multi_thread")]
async fn parked_resolution_cannot_repopulate_a_purged_cache() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls, gate, mut entered) = parked_engine(&dir).await;
    let bytes = pattern(6, 4 * MIB);
    seed(&store, "parked.bin", bytes.clone()).await;
    let k = key("parked.bin");

    // 1. Cold GET issues its probe; the response is computed and parked.
    gate.arm();
    let racer = {
        let engine = engine.clone();
        let k = k.clone();
        tokio::spawn(async move { read_all(&engine, &k, ReadRange::Full).await })
    };
    entered.recv().await.expect("probe parked");

    // 2. Purge runs while the probe is parked (a resolution is in flight).
    let report = engine.purge().await;
    assert_eq!(report.generation, 1, "purge bumped the generation");

    // 3. Deliver the parked (pre-purge) response. It may still serve the
    //    version it resolved, but must admit nothing into the purged cache.
    gate.open();
    let (_, _, raced) = racer
        .await
        .expect("join")
        .expect("racing GET must not error");
    assert_eq!(
        raced, bytes,
        "the in-flight read still serves correct bytes"
    );
    assert_eq!(
        engine.dram_live_bytes(),
        0,
        "a pre-purge resolution must not re-admit a mapping or block after purge"
    );

    // 4. The next read is a genuine cold fill (a new backend get),
    //    byte-identical — nothing was repopulated behind the purge.
    let before = calls.snapshot();
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("post-purge read");
    assert_eq!(got, bytes);
    assert!(
        calls.snapshot().0 > before.0,
        "post-purge read must refill from the backend"
    );
}

/// Generation-epoch purge, the core new semantic (#178): after a purge the
/// previously-warmed bytes are **still physically resident** (nothing was
/// cleared), yet a re-read is a genuine cold backend fill — the old-generation
/// entry is unreachable by construction (a lookup at the new generation cannot
/// match a key written at the old one). The honest gauges show the split: live
/// bytes drop to zero while total usage still reflects the resident,
/// now-reclaimable bytes, so a repeat-cold benchmark reads truth at T=0.
#[tokio::test(flavor = "multi_thread")]
async fn post_purge_is_cold_despite_the_old_generation_staying_resident() {
    let dir = TempDir::new().expect("temp dir");
    // Generous DRAM so the small working set stays fully DRAM-resident: that is
    // what lets the test prove residency survives the purge.
    let (store, engine, _calls) = counting_engine(&dir, 256 * MIB, 128 * MIB).await;
    let objs = [("a.parquet", 3u64), ("b.parquet", 7), ("c.parquet", 11)];
    for (name, s) in objs {
        seed(&store, name, pattern(s, 5 * MIB + 123)).await;
        read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("warm");
    }

    // Warm precondition: the working set is live and resident, nothing stale.
    let usage_warm = engine.dram_usage();
    assert!(engine.dram_live_bytes() > 0, "warmed bytes are live");
    assert_eq!(
        engine.dram_reclaimable_bytes(),
        0,
        "nothing is reclaimable before a purge"
    );

    let report = engine.purge().await;
    assert_eq!(report.generation, 1);
    assert!(report.reclaimable_bytes > 0);

    // The block bytes stay physically resident (only the tiny mapping cache is
    // freed), but they are all reclaimable garbage now — zero live bytes.
    assert_eq!(engine.dram_live_bytes(), 0, "no live bytes at T=0");
    assert!(
        engine.dram_reclaimable_bytes() > 0,
        "purged bytes are resident-but-reclaimable, not freed"
    );
    assert!(
        engine.dram_usage() > usage_warm / 2,
        "the block bytes must still be physically resident after purge \
         (was {usage_warm}, now {})",
        engine.dram_usage()
    );

    // A re-read of a resident-but-stale object is a cold backend fill: the
    // generation mismatch makes the resident old-gen block an unreachable miss.
    let fills_before = engine.counters().snapshot().backend_fills;
    let (_, _, got) = read_all(&engine, &key("a.parquet"), ReadRange::Full)
        .await
        .expect("post-purge read");
    assert_eq!(got, pattern(3, 5 * MIB + 123), "still byte-identical");
    assert!(
        engine.counters().snapshot().backend_fills > fills_before,
        "post-purge read must be a backend fill despite the old block being resident"
    );
    // The refill re-populated the live (current-generation) working set.
    assert!(
        engine.dram_live_bytes() > 0,
        "a post-purge fill grows the live gauge again"
    );
}

/// The race is gone by construction (#178, adapted hammer test): with scan
/// admission off so *every* cold fill inserts, many readers hammer distinct
/// cold fills into the block store while purges run underneath them. Purge no
/// longer calls foyer's `clear()` (it bumps the generation), so there is no
/// clear-vs-insert race to panic on — this asserts zero panics, zero errors,
/// and zero stale bytes across the storm.
#[tokio::test(flavor = "multi_thread")]
async fn hammering_inserts_during_purge_never_panic_or_stale() {
    let dir = TempDir::new().expect("temp dir");
    // Admission disabled: every cold fill genuinely inserts, maximizing the
    // insert pressure a clear would have raced.
    let cfg = cache_config_admission(&dir, 256 * MIB, 128 * MIB, false);
    let (store, engine, _calls) = counting_engine_cfg(&cfg).await;
    // A pool of distinct objects so readers keep producing fresh cold inserts.
    let mut wanted = Vec::new();
    for i in 0..64u64 {
        let body = pattern(1000 + i, 2 * MIB + i);
        seed(&store, &format!("hot-{i}.bin"), body.clone()).await;
        wanted.push(body);
    }
    let wanted = Arc::new(wanted);

    let mut readers = Vec::new();
    for r in 0..8 {
        let engine = engine.clone();
        let wanted = Arc::clone(&wanted);
        readers.push(tokio::spawn(async move {
            for round in 0..6 {
                for i in 0..64u64 {
                    let idx = ((r + round + i) % 64) as usize;
                    let name = format!("hot-{idx}.bin");
                    let (_, _, got) = read_all(&engine, &key(&name), ReadRange::Full)
                        .await
                        .expect("read must never error while purges hammer inserts");
                    assert_eq!(
                        got, wanted[idx],
                        "read must never serve stale or partial bytes"
                    );
                }
            }
        }));
    }
    // Interleave purges with the insert storm.
    for _ in 0..12 {
        engine.purge().await;
        tokio::task::yield_now().await;
    }
    for r in readers {
        r.await.expect("reader task must not panic");
    }
}

/// Ceiling tripwire after a purge (#178): stale old-generation entries still
/// occupy real capacity until LRU reclaims them, so the hard DRAM ceiling must
/// hold *through* a purge and the refill that follows — foyer's weighter charges
/// live and stale bytes alike and evicts to stay under capacity. If a purge ever
/// let occupancy escape the budget (double-counting, or stale entries pinned),
/// this fails.
#[tokio::test(flavor = "multi_thread")]
async fn post_purge_refill_never_breaches_the_dram_ceiling() {
    let dir = TempDir::new().expect("temp dir");
    let dram = 96 * MIB;
    let working_set = 12u64; // 12 blocks against a ~4-block memory tier.
    let capacity = working_set * 2 * 8 * MIB;
    // Admission off: admit-everything is the maximal-pressure path (#122).
    let cfg = cache_config_admission(&dir, capacity, dram, false);
    let (store, engine, _calls) = counting_engine_cfg(&cfg).await;
    let objects: Vec<(String, Bytes)> = (0..working_set)
        .map(|i| (format!("ws-{i}.bin"), pattern(700 + i, 8 * MIB)))
        .collect();

    // The DRAM cache budget minus the fixed 48 MiB disk-write pipeline reserve.
    let dram_cache_budget = dram - 48 * MIB;
    for (name, body) in &objects {
        seed(&store, name, body.clone()).await;
    }

    // Fill once (ceiling holds), purge (stale garbage now resident), refill
    // (ceiling must still hold as live entries evict the stale tail).
    for _pass in 0..2 {
        for (name, _) in &objects {
            read_all(&engine, &key(name), ReadRange::Full)
                .await
                .expect("read");
            engine.flush().await;
            assert!(
                engine.dram_usage() <= dram_cache_budget,
                "DRAM occupancy {} exceeded the {dram_cache_budget}-byte ceiling",
                engine.dram_usage()
            );
        }
        // Between the two fill passes, purge: the first pass's blocks become
        // resident stale garbage the refill must evict without breaching budget.
        if _pass == 0 {
            engine.purge().await;
            assert!(
                engine.dram_usage() <= dram_cache_budget,
                "the ceiling must hold the instant after a purge"
            );
        }
    }
}

/// Footer pinning (#50): a metadata-classified suffix read (a Parquet footer)
/// is pinned at suffix granularity — the repeat read serves from the metadata
/// store with ZERO backend traffic, and the file's body never enters the
/// small store (only the tail bytes are pinned).
#[tokio::test(flavor = "multi_thread")]
async fn parquet_footer_suffix_reads_pin_and_repeat_serve_from_the_meta_store() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let size = 5 * MIB;
    let body = pattern(43, size);
    seed(&store, "warehouse/data/f1.parquet", body.clone()).await;
    let k = key("warehouse/data/f1.parquet");
    let tail_len = 64 * 1024u64;
    let tail = body.slice((size - tail_len) as usize..);

    // Cold: one GET, no HEAD — and the tail gets pinned.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Suffix(tail_len))
        .await
        .expect("cold footer read");
    assert_eq!(got, tail, "cold suffix bytes");
    assert_eq!(calls.snapshot(), (1, 0), "cold footer costs one GET");

    // Warm: the pinned tail serves with zero backend traffic.
    let (_, range, got) = read_all(&engine, &k, ReadRange::Suffix(tail_len))
        .await
        .expect("warm footer read");
    assert_eq!(got, tail, "warm suffix bytes identical");
    assert_eq!(range, (size - tail_len)..size, "resolved tail range");
    assert_eq!(
        calls.snapshot(),
        (1, 0),
        "repeat footer read must not touch the backend (pinned, #50)"
    );

    // The pin is footer-granular: DRAM in use stays near the tail size, not
    // the 5 MiB file (the whole-file block-0 pollution this test pins against).
    assert!(
        engine.dram_usage() < 1024 * 1024,
        "only the tail bytes may be resident, found {} bytes",
        engine.dram_usage()
    );
}

/// Footer pinning, warm-miss path (#50): when the mapping is already cached
/// (a full read happened first) but no tail is pinned, a metadata suffix read
/// re-fetches and pins the tail — it must never route through the block
/// ladder, which would pin the whole file body into the small meta store.
#[tokio::test(flavor = "multi_thread")]
async fn footer_suffix_with_cached_mapping_pins_without_block_pollution() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    let size = 5 * MIB;
    let body = pattern(47, size);
    seed(&store, "warehouse/data/f2.parquet", body.clone()).await;
    let k = key("warehouse/data/f2.parquet");
    let tail_len = 64 * 1024u64;
    let tail = body.slice((size - tail_len) as usize..);

    // Full read caches the mapping (and the file's blocks, in the DATA store).
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full).await.expect("full");
    assert_eq!(got, body);
    let gets_after_full = calls.snapshot().0;

    // First suffix read: mapping present, tail unpinned — one GET to pin.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Suffix(tail_len))
        .await
        .expect("pinning suffix read");
    assert_eq!(got, tail);
    assert_eq!(calls.snapshot().0, gets_after_full + 1, "one pinning GET");

    // Second suffix read: served from the pinned tail, zero backend traffic.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Suffix(tail_len))
        .await
        .expect("pinned suffix read");
    assert_eq!(got, tail);
    assert_eq!(
        calls.snapshot().0,
        gets_after_full + 1,
        "pinned tail must serve with zero backend traffic"
    );
}

// ---------------------------------------------------------------------------
// Issue #14: key→ETag mapping classification (immutable indefinite / mutable
// short-TTL + conditional revalidation). The TTL is set to 0 in these tests so
// a mutable mapping is revalidated on *every* read — deterministic expiry with
// no wall clock. The mutable-TTL *window* itself is proven separately with a
// controllable clock in the engine's in-crate unit tests.
// ---------------------------------------------------------------------------

/// Immutability acceptance criterion: an immutable-classified key
/// (`*.parquet` data file, `metadata/*.json`) generates ZERO revalidation
/// traffic even under an aggressive TTL=0 that would revalidate any mutable
/// key on every read — verified against both the backend call count and the
/// engine's own counter.
#[tokio::test]
async fn immutable_classified_mappings_never_revalidate() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) =
        counting_engine_cfg(&cache_config_ttl(&dir, 64 * MIB, 128 * MIB, 0)).await;

    let data = pattern(3, 4096); // one sub-block object
    let meta_json = Bytes::from_static(b"{\"format-version\":2}");
    seed(&store, "warehouse/db/t/data/00000-0.parquet", data.clone()).await;
    seed(
        &store,
        "warehouse/db/t/metadata/v1.metadata.json",
        meta_json.clone(),
    )
    .await;
    let data_key = key("warehouse/db/t/data/00000-0.parquet");
    let meta_key = key("warehouse/db/t/metadata/v1.metadata.json");

    // Read each object twice under TTL=0.
    for _ in 0..2 {
        let (_, _, got) = read_all(&engine, &data_key, ReadRange::Full)
            .await
            .expect("read parquet");
        assert_eq!(got, data);
        let (_, _, got) = read_all(&engine, &meta_key, ReadRange::Full)
            .await
            .expect("read metadata.json");
        assert_eq!(got, meta_json);
    }

    // Zero revalidation traffic, both at the backend and in the engine counter.
    assert_eq!(
        calls.revalidates(),
        0,
        "immutable-classified keys must never revalidate the backend"
    );
    assert_eq!(
        engine.counters().snapshot().meta_revalidations,
        0,
        "immutable-classified keys must never bump the revalidation counter"
    );
}

/// Mutable acceptance criterion #1: an object overwritten *behind Verglas's
/// back* (straight to the origin) is served fresh within one TTL window —
/// here, on the next read, since TTL=0. The revalidation detects the new ETag
/// and rotates the mapping to the new version.
#[tokio::test]
async fn mutable_mapping_served_fresh_after_behind_the_back_overwrite() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) =
        counting_engine_cfg(&cache_config_ttl(&dir, 64 * MIB, 128 * MIB, 0)).await;

    let old = pattern(1, 4096);
    let new = pattern(2, 4096);
    seed(&store, "logs/notes.txt", old.clone()).await;
    let k = key("logs/notes.txt");

    // Warm the mapping and block of the old version.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("first read");
    assert_eq!(got, old);

    // Overwrite behind Verglas: new bytes, new ETag.
    seed(&store, "logs/notes.txt", new.clone()).await;

    // Next read revalidates (TTL=0), sees the new ETag, and serves fresh.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("second read");
    assert_eq!(
        got, new,
        "mutable key must be served fresh within one TTL window"
    );

    let snap = engine.counters().snapshot();
    assert!(calls.revalidates() >= 1, "the mutable key must revalidate");
    assert!(snap.meta_revalidations >= 1);
    assert!(
        snap.meta_revalidations_rotated >= 1,
        "the behind-the-back overwrite must rotate the mapping"
    );
}

/// Mutable acceptance criterion #2: when a mutable object has NOT changed, the
/// conditional revalidation returns `Unchanged`, the TTL is refreshed, and the
/// cached block is served straight from DRAM — the revalidation never refetches
/// the block. This is the payoff of conditional revalidation over re-resolution.
#[tokio::test]
async fn mutable_unchanged_revalidation_keeps_block_warm() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) =
        counting_engine_cfg(&cache_config_ttl(&dir, 64 * MIB, 128 * MIB, 0)).await;

    let body = pattern(9, 4096);
    seed(&store, "logs/notes.txt", body.clone()).await;
    let k = key("logs/notes.txt");

    // Warm the mapping and block.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("first read");
    assert_eq!(got, body);
    let before = engine.counters().snapshot();
    let gets_before = calls.snapshot().0;

    // Re-read without any overwrite: revalidates (TTL=0) → Unchanged → warm.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("second read");
    assert_eq!(got, body);

    let after = engine.counters().snapshot();
    assert!(calls.revalidates() >= 1, "expired mutable key revalidates");
    assert!(after.meta_revalidations_unchanged > before.meta_revalidations_unchanged);
    assert_eq!(
        after.backend_fills, before.backend_fills,
        "an Unchanged revalidation must not refetch the block"
    );
    assert_eq!(
        calls.snapshot().0,
        gets_before,
        "the warm block is served from DRAM, not re-GET from the origin"
    );
    assert!(after.dram_hits > before.dram_hits, "block served from DRAM");
}

/// A reader that does not override [`ObjectRead::revalidate`] (`PlainRead`),
/// exercising the trait's default HEAD-based implementation across all three
/// outcomes — the correct-everywhere fallback a backend without a native
/// conditional request uses.
struct PlainRead(PassthroughRead);

impl ObjectRead for PlainRead {
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.0.get(key, range).await
    }
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.0.head(key).await
    }
    // revalidate: inherits the trait default (HEAD + ETag compare).
}

#[tokio::test]
async fn default_revalidate_reports_unchanged_changed_and_vanished() {
    let store = Arc::new(InMemory::new());
    seed(&store, "obj.bin", pattern(5, 1024)).await;
    let reader = PlainRead(PassthroughRead::new(BackendStore::single(
        "default",
        BUCKET,
        store.clone(),
    )));
    let k = key("obj.bin");

    let etag = reader.head(&k).await.expect("head").e_tag.expect("etag");

    // Matching ETag → Unchanged.
    assert_eq!(
        reader.revalidate(&k, &etag).await.expect("revalidate"),
        Revalidation::Unchanged
    );
    // Wrong ETag → Changed, carrying the current metadata.
    match reader
        .revalidate(&k, "not-the-etag")
        .await
        .expect("revalidate")
    {
        Revalidation::Changed(meta) => assert_eq!(meta.e_tag, Some(etag)),
        other => panic!("expected Changed, got {other:?}"),
    }
    // Deleted object → Vanished.
    store.delete(&Path::from("obj.bin")).await.expect("delete");
    assert_eq!(
        reader.revalidate(&k, "any").await.expect("revalidate"),
        Revalidation::Vanished
    );
}

/// Backend wrapper that scripts the outcome of [`ObjectRead::revalidate`] so the
/// engine's revalidation-outcome branches (#14) can be driven directly. `get`
/// and `head` delegate unchanged.
struct ScriptedReval<R> {
    /// The delegate for get/head (and for `Delegate`d revalidations).
    inner: R,
    /// The scripted revalidation behavior.
    script: Arc<std::sync::Mutex<RevalScript>>,
}

/// What a [`ScriptedReval`] does on the next revalidation.
#[derive(Clone)]
enum RevalScript {
    /// Delegate to the wrapped reader.
    Delegate,
    /// Return this outcome verbatim.
    Give(Revalidation),
    /// Fail with a transient backend error.
    Fail,
}

impl<R: ObjectRead> ObjectRead for ScriptedReval<R> {
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.inner.head(key).await
    }
    async fn revalidate(&self, key: &CacheKey, etag: &str) -> Result<Revalidation, ReadError> {
        let script = self.script.lock().expect("script lock").clone();
        match script {
            RevalScript::Delegate => self.inner.revalidate(key, etag).await,
            RevalScript::Give(outcome) => Ok(outcome),
            RevalScript::Fail => Err(ReadError::Backend("scripted revalidation failure".into())),
        }
    }
}

/// Builds a single-node engine over a scripted-revalidation backend at TTL=0.
async fn scripted_engine(
    dir: &TempDir,
    store: Arc<InMemory>,
    script: Arc<std::sync::Mutex<RevalScript>>,
) -> HybridCacheEngine<
    ScriptedReval<PassthroughRead>,
    verglas_core::peer::NoopPeerFetch,
    RendezvousRing,
> {
    let backend = ScriptedReval {
        inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store)),
        script,
    };
    HybridCacheEngine::single_node(backend, &cache_config_ttl(dir, 64 * MIB, 128 * MIB, 0))
        .await
        .expect("build engine")
}

/// A revalidation that reports the object *vanished* drops the mapping and lets
/// the read re-resolve, which surfaces the origin's own 404 — never stale bytes.
#[tokio::test]
async fn mutable_revalidation_vanished_surfaces_not_found() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) =
        counting_engine_cfg(&cache_config_ttl(&dir, 64 * MIB, 128 * MIB, 0)).await;
    seed(&store, "logs/notes.txt", pattern(7, 4096)).await;
    let k = key("logs/notes.txt");

    read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("warm read");
    store
        .delete(&Path::from("logs/notes.txt"))
        .await
        .expect("delete");

    match read_all(&engine, &k, ReadRange::Full).await {
        Err(ReadError::NoSuchKey) => {}
        other => panic!("expected NoSuchKey after the object vanished, got {other:?}"),
    }
    assert!(calls.revalidates() >= 1);
    assert!(engine.counters().snapshot().meta_revalidations >= 1);
}

/// A revalidation that reports the object *changed* but now carries no ETag
/// (turned non-cacheable) drops the mapping; the read re-resolves and still
/// serves correctly, and the mapping is not counted as a rotation.
#[tokio::test]
async fn mutable_revalidation_changed_to_non_cacheable_reresolves() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let body = pattern(8, 4096);
    seed(&store, "notes.txt", body.clone()).await;
    let script = Arc::new(std::sync::Mutex::new(RevalScript::Delegate));
    let engine = scripted_engine(&dir, store.clone(), script.clone()).await;
    let k = key("notes.txt");

    read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("warm read");
    // Next revalidation claims the object changed to a version with no ETag.
    *script.lock().expect("lock") =
        RevalScript::Give(Revalidation::Changed(Box::new(ObjectMeta {
            size: body.len() as u64,
            e_tag: None,
            last_modified: None,
            content_type: None,
            ..ObjectMeta::default()
        })));
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("re-read");
    assert_eq!(got, body);
    let snap = engine.counters().snapshot();
    assert!(snap.meta_revalidations >= 1);
    assert_eq!(
        snap.meta_revalidations_rotated, 0,
        "a non-cacheable change is not a rotation"
    );
}

/// A transient revalidation *failure* degrades to a full re-resolution (a
/// backend fill), never to serving the expired mapping's warm blocks unverified.
#[tokio::test]
async fn mutable_revalidation_transient_error_degrades_to_reresolution() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let body = pattern(6, 4096);
    seed(&store, "notes.txt", body.clone()).await;
    let script = Arc::new(std::sync::Mutex::new(RevalScript::Delegate));
    let engine = scripted_engine(&dir, store.clone(), script.clone()).await;
    let k = key("notes.txt");

    read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("warm read");
    *script.lock().expect("lock") = RevalScript::Fail;
    // The read still succeeds by re-resolving from the origin.
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("re-read");
    assert_eq!(got, body);
    assert!(engine.counters().snapshot().meta_revalidations >= 1);
}

// ---------------------------------------------------------------------------
// Evict-first hard eviction for retired files (issue #198).
//
// A demoted (object, current-ETag) identity becomes an unreachable miss: its
// blocks are never served from cache again and refill from the backend, so a
// retired file is evicted before live entries under pressure without ever
// being re-admitted. A live file is untouched. Hard-evicting (clearing the
// demotion after the grace window) leaves the object cacheable again.
// ---------------------------------------------------------------------------

/// A demoted object's blocks are gone: the next read refills from the backend
/// (counter-asserted), even though it was warm before demotion.
#[tokio::test(flavor = "multi_thread")]
async fn demoted_object_blocks_refill_from_backend() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    seed(&store, "old.parquet", pattern(3, 4 * MIB)).await;
    let k = key("old.parquet");

    // Warm it: fill then confirm a free hit.
    read_all(&engine, &k, ReadRange::Full).await.expect("fill");
    let after_warm = engine.counters().snapshot().backend_fills;
    read_all(&engine, &k, ReadRange::Full).await.expect("hit");
    assert_eq!(
        engine.counters().snapshot().backend_fills,
        after_warm,
        "a warm hit must not refill"
    );

    // Demote it (retirement). The next read must miss the cache and refill.
    let _ = engine.demote(&[verglas_cache::DemoteRequest {
        key: k.clone(),
        size: 0,
    }]);
    assert!(engine.is_demoted(&k));
    let before = engine.counters().snapshot().backend_fills;
    let (_, _, got) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("read demoted");
    assert_eq!(
        got,
        pattern(3, 4 * MIB),
        "demoted read still serves correct bytes"
    );
    assert!(
        engine.counters().snapshot().backend_fills > before,
        "a demoted object must refill from the backend, not hit the cache"
    );

    // And it stays a miss (no re-admission): a second read refills again.
    let before2 = engine.counters().snapshot().backend_fills;
    read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("re-read");
    assert!(
        engine.counters().snapshot().backend_fills > before2,
        "a demoted object must not be re-admitted"
    );
}

/// A live (non-demoted) object served warm is untouched when a *different*
/// object is demoted.
#[tokio::test(flavor = "multi_thread")]
async fn demotion_leaves_live_object_untouched() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    seed(&store, "old.parquet", pattern(3, 4 * MIB)).await;
    seed(&store, "live.parquet", pattern(9, 4 * MIB)).await;
    let old = key("old.parquet");
    let live = key("live.parquet");

    read_all(&engine, &old, ReadRange::Full)
        .await
        .expect("warm old");
    read_all(&engine, &live, ReadRange::Full)
        .await
        .expect("warm live");

    let _ = engine.demote(&[verglas_cache::DemoteRequest {
        key: old.clone(),
        size: 0,
    }]);

    // The live object still hits warm (no refill).
    let before = engine.counters().snapshot().backend_fills;
    read_all(&engine, &live, ReadRange::Full)
        .await
        .expect("hit live");
    assert_eq!(
        engine.counters().snapshot().backend_fills,
        before,
        "demoting one object must not evict a live object"
    );
    assert!(!engine.is_demoted(&live));
}

/// Hard-evicting (clearing the demotion once the grace window closes) makes the
/// object cacheable again: while demoted every read refills and nothing is
/// admitted; after hard-evict a fill is admitted and the next read hits warm.
#[tokio::test(flavor = "multi_thread")]
async fn hard_evict_clears_demotion_and_reallows_caching() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _) = counting_engine(&dir, 64 * MIB, 128 * MIB).await;
    seed(&store, "old.parquet", pattern(3, 4 * MIB)).await;
    let k = key("old.parquet");

    // Demote *before* first read, so no live-generation blocks are ever cached:
    // while demoted, every read refills (never admitted).
    let _ = engine.demote(&[verglas_cache::DemoteRequest {
        key: k.clone(),
        size: 0,
    }]);
    assert!(engine.is_demoted(&k));
    read_all(&engine, &k, ReadRange::Full).await.expect("cold");
    let demoted_fills = engine.counters().snapshot().backend_fills;
    read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("cold again");
    assert!(
        engine.counters().snapshot().backend_fills > demoted_fills,
        "a demoted object refills every read (never admitted)"
    );

    // Hard-evict: the demotion is cleared and the object is cacheable again.
    let _ = engine.hard_evict(&[verglas_cache::HardEviction {
        key: k.clone(),
        etag: None,
        size: 0,
        generation: 0,
    }]);
    assert!(!engine.is_demoted(&k));
    assert_eq!(engine.demoted_count(), 0);

    // The first post-undemote read fills and is now admitted...
    let before = engine.counters().snapshot().backend_fills;
    read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("refill");
    assert!(engine.counters().snapshot().backend_fills > before);
    // ...so the next read hits warm (re-admission works once undemoted).
    let after = engine.counters().snapshot().backend_fills;
    read_all(&engine, &k, ReadRange::Full).await.expect("hit");
    assert_eq!(
        engine.counters().snapshot().backend_fills,
        after,
        "an undemoted object caches normally again"
    );
}

/// Demotion never breaches the DRAM ceiling: churn a stream of demoted objects
/// through the cache and the resident DRAM stays under the budget (the #122
/// tripwire, extended to the demotion path).
#[tokio::test(flavor = "multi_thread")]
async fn demotion_holds_the_dram_ceiling() {
    let dir = TempDir::new().expect("temp dir");
    let dram = 80 * MIB;
    let (store, engine, _) =
        counting_engine_cfg(&cache_config_admission(&dir, 96 * MIB, dram, false)).await;

    // Fill and demote a long stream of distinct objects: every read is an
    // unreachable miss that refills, exercising the demotion path under churn.
    for i in 0..24 {
        let name = format!("orphan-{i}.parquet");
        seed(&store, &name, pattern(200 + i, 8 * MIB)).await;
        let k = key(&name);
        let _ = engine.demote(&[verglas_cache::DemoteRequest {
            key: k.clone(),
            size: 0,
        }]);
        read_all(&engine, &k, ReadRange::Full).await.expect("read");
        engine.flush().await;
        // The same ceiling the meta-store tripwire uses: everything resident
        // in DRAM stays within the budget minus the pipeline reservation.
        let ceiling = dram - 48 * MIB;
        assert!(
            engine.dram_usage() <= ceiling,
            "DRAM residency {} exceeded the {ceiling}-byte ceiling under demotion churn",
            engine.dram_usage()
        );
    }
}

/// #305: hard eviction physically removes a retired object's blocks. Demote
/// with the manifest-reported size, then hard-evict with the receipt: every
/// block must miss the cache-only probe and a re-read must pay fresh backend
/// fills. Before the fix, `hard_evict` only cleared the demotion marker —
/// the dead generations stayed resident (3.6 TB of them, live incident
/// 2026-07-18) and the "evicted" blocks even served warm again afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn hard_evict_physically_removes_blocks() {
    use verglas_cache::{DemoteRequest, HardEviction};
    use verglas_core::BlockKey;

    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = counting_engine(&dir, 256 * MIB, 128 * MIB).await;
    let body = pattern(7, 2 * TEST_BLOCK_BYTES + MIB); // three blocks
    seed(
        &store,
        "table/data/dead-after-compaction.parquet",
        body.clone(),
    )
    .await;
    let k = key("table/data/dead-after-compaction.parquet");

    let (_, _, bytes) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("cold read");
    assert_eq!(bytes, body);
    engine.flush().await;
    let (gets_after_fill, _) = calls.snapshot();

    // Demote with the manifest size; the receipt captures the etag and the
    // admission generation that hard eviction later needs to enumerate blocks.
    let receipts = engine.demote(&[DemoteRequest {
        key: k.clone(),
        size: body.len() as u64,
    }]);
    assert_eq!(receipts.len(), 1);
    let etag = receipts[0]
        .etag
        .clone()
        .expect("the mapping was warm, so the etag is captured");
    let probe = |idx: u64| BlockKey {
        object: k.clone(),
        etag: etag.clone(),
        block_bytes: TEST_BLOCK_BYTES,
        block_index: idx,
    };

    let reclaimed = engine.hard_evict(&[HardEviction {
        key: k.clone(),
        etag: Some(etag.clone()),
        size: receipts[0].size,
        generation: receipts[0].generation,
    }]);
    assert_eq!(
        reclaimed,
        body.len() as u64,
        "hard eviction reports the object's bytes reclaimed"
    );

    // Physically gone: every block misses the cache-only probe.
    for idx in 0..3 {
        assert!(
            engine.local_block(&probe(idx)).await.is_none(),
            "block {idx} must be physically removed after hard eviction"
        );
    }

    // A re-read refills from the backend rather than serving stale warm bytes.
    let (_, _, again) = read_all(&engine, &k, ReadRange::Full)
        .await
        .expect("re-read");
    assert_eq!(again, body);
    let (gets_after_evict, _) = calls.snapshot();
    assert!(
        gets_after_evict > gets_after_fill,
        "the re-read pays fresh backend fills ({gets_after_fill} -> {gets_after_evict})"
    );
}
