//! Frozen RIME evaluator: write-allocate cache population.
//!
//! After a client write is durable at the origin, the engine may convert the
//! bytes it already holds into cache residency instead of paying an origin
//! GET on first read-back. The contract under test:
//!
//! 1. A populated object serves byte-identical reads with zero backend
//!    requests.
//! 2. Population enters through the same scan-resistant admission as reads:
//!    a bulk write burst must not evict a warmed read working set.
//! 3. `populate_on_write` queues and returns without awaiting admission;
//!    `flush()` is the residency barrier (the contract read-fills follow).
//!
//! Candidates implement `HybridCacheEngine::populate_on_write(&self, key,
//! etag, body)` making this suite pass without editing it.

use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt as _;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStoreExt as _, PutPayload};
use tempfile::TempDir;
use verglas_cache::HybridCacheEngine;
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_s3::{
    BackendStore, ObjectGet, ObjectMeta, ObjectRead, PassthroughRead, ReadError, ReadRange,
    Revalidation,
};

/// The bucket the in-memory origin serves.
const BUCKET: &str = "test-bucket";
const MIB: u64 = 1024 * 1024;
const TEST_BLOCK_BYTES: u64 = 8 * MIB;

/// Deterministic payload: byte `i` is `(i + seed) % 251`.
fn pattern(seed: u64, len: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(len as usize);
    for i in 0..len {
        buf.extend_from_slice(&[(((i + seed) % 251) & 0xFF) as u8]);
    }
    buf.freeze()
}

/// Shared backend call counters.
#[derive(Default, Clone)]
struct BackendCalls {
    gets: Arc<AtomicU64>,
    heads: Arc<AtomicU64>,
    revalidates: Arc<AtomicU64>,
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
}

/// An `ObjectRead` that counts calls before delegating.
struct CountingRead<R> {
    inner: R,
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

    /// Counts, then delegates.
    async fn revalidate(&self, key: &CacheKey, etag: &str) -> Result<Revalidation, ReadError> {
        self.calls.revalidates.fetch_add(1, Ordering::Relaxed);
        self.inner.revalidate(key, etag).await
    }
}

/// Drains a full response body.
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

/// Engine test config mirroring the engine suite's geometry.
fn config(dir: &TempDir, capacity: u64, dram: u64, admission: bool) -> CacheConfig {
    let mut config = CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(capacity),
        dram_bytes: ByteSize(dram),
        data_block_bytes: ByteSize(TEST_BLOCK_BYTES),
        ..CacheConfig::default()
    };
    config.admission.enabled = admission;
    config
}

/// Builds a counting single-node engine over a fresh in-memory origin.
async fn engine_over(
    dir: &TempDir,
    capacity: u64,
    dram: u64,
    admission: bool,
) -> (
    Arc<InMemory>,
    HybridCacheEngine<
        CountingRead<PassthroughRead>,
        verglas_core::peer::NoopPeerFetch,
        verglas_core::ring::RendezvousRing,
    >,
    BackendCalls,
) {
    let store = Arc::new(InMemory::new());
    let calls = BackendCalls::default();
    let backend = CountingRead {
        inner: PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        calls: calls.clone(),
    };
    let engine = HybridCacheEngine::single_node(backend, &config(dir, capacity, dram, admission))
        .await
        .expect("build engine");
    (store, engine, calls)
}

/// Writes `body` to the origin (the durable write the population mirrors) and
/// returns the origin ETag the populated mapping must carry.
async fn origin_put(store: &Arc<InMemory>, key: &str, body: &Bytes) -> String {
    let result = store
        .put(&StorePath::from(key), PutPayload::from(body.clone()))
        .await
        .expect("origin put");
    result.e_tag.expect("origin etag")
}

fn key(name: &str) -> CacheKey {
    CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: BUCKET.to_owned(),
        key: name.to_owned(),
    }
}

/// Gate 1: a populated object serves byte-identical reads and its first
/// read-back issues zero backend requests.
#[tokio::test(flavor = "multi_thread")]
async fn populated_write_serves_reads_with_zero_backend_requests() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = engine_over(&dir, 64 * MIB, 128 * MIB, true).await;
    let body = pattern(61, 12 * MIB); // spans two blocks
    let etag = origin_put(&store, "fresh.parquet", &body).await;
    let k = key("fresh.parquet");

    engine
        .populate_on_write(&k, &etag, body.clone())
        .await
        .expect("populate");
    // Population is admission-asynchronous; flush() is the residency barrier.
    engine.flush().await;

    let (_, _, got) = read_all(&engine, &k, ReadRange::Full).await.expect("read");
    assert_eq!(got, body, "read-back must be byte-identical to the write");
    let (gets, _) = calls.snapshot();
    assert_eq!(
        gets, 0,
        "a populated write must serve its first read without any backend request"
    );
    assert_eq!(
        engine.counters().snapshot().backend_fills,
        0,
        "no fill ran"
    );
}

/// Gate 1b: population is keyed to the durable write. A read of a DIFFERENT
/// key still fills from the backend.
#[tokio::test(flavor = "multi_thread")]
async fn population_does_not_affect_other_keys() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = engine_over(&dir, 64 * MIB, 128 * MIB, true).await;
    let written = pattern(62, TEST_BLOCK_BYTES);
    let other = pattern(63, TEST_BLOCK_BYTES);
    let etag = origin_put(&store, "written.parquet", &written).await;
    origin_put(&store, "other.parquet", &other).await;

    engine
        .populate_on_write(&key("written.parquet"), &etag, written.clone())
        .await
        .expect("populate");
    engine.flush().await;

    let (_, _, got) = read_all(&engine, &key("other.parquet"), ReadRange::Full)
        .await
        .expect("read other");
    assert_eq!(got, other);
    let (gets, _) = calls.snapshot();
    assert!(gets >= 1, "the unwritten key fills from the backend");
}

/// Gate 2: bulk write population enters scan-resistant admission. A warmed
/// read working set survives a 3×-cache write burst; re-reading it costs
/// (near-)zero refills, mirroring the read-side scan-resistance contract.
#[tokio::test(flavor = "multi_thread")]
async fn bulk_write_population_does_not_evict_the_working_set() {
    let dir = TempDir::new().expect("temp dir");
    // Same pressure geometry as the read-side scan-resistance test.
    let (store, engine, _calls) = engine_over(&dir, 96 * MIB, 80 * MIB, true).await;

    // Warm a 6-object read working set (48 MiB, half the disk budget).
    let working_set: Vec<(String, Bytes)> = (0..6)
        .map(|i| (format!("ws-{i}.parquet"), pattern(700 + i, TEST_BLOCK_BYTES)))
        .collect();
    for (name, body) in &working_set {
        origin_put(&store, name, body).await;
    }
    for _ in 0..2 {
        for (name, body) in &working_set {
            let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
                .await
                .expect("warm working set");
            assert_eq!(&got, body);
            engine.flush().await;
        }
    }

    // The write burst: 36 one-touch objects, 3× the disk budget, populated on
    // write and never read.
    for i in 0..36u64 {
        let name = format!("bulk-{i}.parquet");
        let body = pattern(9000 + i, TEST_BLOCK_BYTES);
        let etag = origin_put(&store, &name, &body).await;
        engine
            .populate_on_write(&key(&name), &etag, body)
            .await
            .expect("populate bulk");
        engine.flush().await;
    }

    // The working set survives: re-reading it is (near-)free.
    let fills_before = engine.counters().snapshot().backend_fills;
    for (name, body) in &working_set {
        let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("re-read working set");
        assert_eq!(&got, body);
    }
    let refills = engine.counters().snapshot().backend_fills - fills_before;
    assert!(
        refills <= 2,
        "bulk write population must not evict the read working set: {refills} refills"
    );
}

/// Gate 3: populate queues and returns without awaiting admission; one flush
/// then covers a whole burst, and every object serves with zero backend
/// requests. A populate that awaited admission inline would still pass reads
/// but shows up as serialized latency; the structural assertion here is that
/// residency is complete after a single flush barrier.
#[tokio::test(flavor = "multi_thread")]
async fn populate_queues_without_awaiting_admission() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = engine_over(&dir, 256 * MIB, 128 * MIB, false).await;
    let mut bodies = Vec::new();
    for i in 0..4u64 {
        let name = format!("burst-{i}.parquet");
        let body = pattern(400 + i, TEST_BLOCK_BYTES);
        let etag = origin_put(&store, &name, &body).await;
        engine
            .populate_on_write(&key(&name), &etag, body.clone())
            .await
            .expect("populate");
        bodies.push((name, body));
    }
    engine.flush().await;
    for (name, body) in &bodies {
        let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("read burst");
        assert_eq!(&got, body);
    }
    let (gets, _) = calls.snapshot();
    assert_eq!(gets, 0, "every burst object served from population");
}
