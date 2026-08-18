//! Frozen RIME evaluator: demand-driven disk reclamation for write-through.
//!
//! The cache must always be evictable to make room for write-back: when
//! fragment demand exceeds the physically unconsumed growth room, the engine
//! yields disk by evicting cold data blocks and physically returning the
//! space (`disk_growth_room_bytes` must observe the gain — that is the
//! accounting the fragment store spends). Metadata stores are never touched:
//! the table-metadata store and materialized pages live in their own
//! domains, and reclamation is a data-block affair only.
//!
//! Candidates implement
//! `HybridCacheEngine::reclaim_disk_for_writeback(&self, bytes: u64) -> u64`
//! (returns the bytes made available) making this suite pass without editing
//! it.

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
    /// Loads the GET count.
    fn gets(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
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

/// Builds a counting single-node engine over a fresh in-memory origin.
async fn engine_over(
    dir: &TempDir,
    capacity: u64,
    dram: u64,
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
    let config = CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(capacity),
        dram_bytes: ByteSize(dram),
        data_block_bytes: ByteSize(TEST_BLOCK_BYTES),
        ..CacheConfig::default()
    };
    let engine = HybridCacheEngine::single_node(backend, &config)
        .await
        .expect("build engine");
    (store, engine, calls)
}

/// Seeds the origin.
async fn seed(store: &Arc<InMemory>, key: &str, body: &Bytes) {
    store
        .put(&StorePath::from(key), PutPayload::from(body.clone()))
        .await
        .expect("seed");
}

fn key(name: &str) -> CacheKey {
    CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: BUCKET.to_owned(),
        key: name.to_owned(),
    }
}

/// Physically fills most of the disk budget with cold data blocks and flushes
/// so the backing store's allocation is real, then returns the object names.
async fn fill_cold(
    store: &Arc<InMemory>,
    engine: &HybridCacheEngine<
        CountingRead<PassthroughRead>,
        verglas_core::peer::NoopPeerFetch,
        verglas_core::ring::RendezvousRing,
    >,
    objects: u64,
) -> Vec<String> {
    let mut names = Vec::new();
    for i in 0..objects {
        let name = format!("cold-{i}.parquet");
        let body = pattern(100 + i, TEST_BLOCK_BYTES);
        seed(store, &name, &body).await;
        let (_, _, got) = read_all(engine, &key(&name), ReadRange::Full)
            .await
            .expect("fill");
        assert_eq!(got, body);
        engine.flush().await;
        names.push(name);
    }
    names
}

/// Gate 1: reclamation yields at least the requested bytes and the growth-room
/// accounting the fragment store spends observes the gain.
#[tokio::test(flavor = "multi_thread")]
async fn reclaim_frees_growth_room_by_at_least_the_request() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _calls) = engine_over(&dir, 96 * MIB, 128 * MIB).await;
    fill_cold(&store, &engine, 10).await;

    let room_before = engine.disk_growth_room_bytes();
    let request = 3 * TEST_BLOCK_BYTES;
    let yielded = engine.reclaim_disk_for_writeback(request).await;
    assert!(
        yielded >= request,
        "requested {request}, reclaimed only {yielded}"
    );
    let room_after = engine.disk_growth_room_bytes();
    assert!(
        room_after >= room_before + request,
        "growth room must observe the reclaimed space: before {room_before}, after {room_after}"
    );
}

/// Gate 2: reclamation never touches the metadata store. A warmed
/// metadata object still serves with zero backend requests after a maximal
/// reclaim of the data tier.
#[tokio::test(flavor = "multi_thread")]
async fn reclaim_spares_the_metadata_store() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = engine_over(&dir, 96 * MIB, 128 * MIB).await;

    // A metadata-routed object: `**/metadata/*.json` per the heuristic router.
    let meta_body = pattern(7, 64 * 1024);
    seed(
        &store,
        "warehouse/tbl/metadata/v3.metadata.json",
        &meta_body,
    )
    .await;
    let meta_key = key("warehouse/tbl/metadata/v3.metadata.json");
    let (_, _, got) = read_all(&engine, &meta_key, ReadRange::Full)
        .await
        .expect("warm metadata");
    assert_eq!(got, meta_body);
    engine.flush().await;

    fill_cold(&store, &engine, 10).await;
    let _ = engine.reclaim_disk_for_writeback(u64::MAX).await;

    let gets_before = calls.gets();
    let (_, _, again) = read_all(&engine, &meta_key, ReadRange::Full)
        .await
        .expect("re-read metadata");
    assert_eq!(again, meta_body);
    assert_eq!(
        calls.gets(),
        gets_before,
        "metadata must survive a maximal data-tier reclaim"
    );
}

/// Gate 3: reclaimed blocks degrade to correct backend refills — never wrong
/// bytes — and the engine keeps serving new fills after a shrink.
#[tokio::test(flavor = "multi_thread")]
async fn reclaimed_blocks_refill_byte_identical_and_the_engine_stays_live() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _calls) = engine_over(&dir, 96 * MIB, 128 * MIB).await;
    let names = fill_cold(&store, &engine, 10).await;

    let yielded = engine
        .reclaim_disk_for_writeback(4 * TEST_BLOCK_BYTES)
        .await;
    assert!(yielded >= 4 * TEST_BLOCK_BYTES);

    // Every previously cached object still reads byte-identical (reclaimed
    // ones refill; surviving ones hit).
    for (i, name) in names.iter().enumerate() {
        let body = pattern(100 + i as u64, TEST_BLOCK_BYTES);
        let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("post-reclaim read");
        assert_eq!(got, body, "{name} must never serve wrong bytes");
    }

    // A brand-new cold object still fills and serves.
    let fresh = pattern(999, TEST_BLOCK_BYTES);
    seed(&store, "fresh-after-shrink.parquet", &fresh).await;
    let (_, _, got) = read_all(&engine, &key("fresh-after-shrink.parquet"), ReadRange::Full)
        .await
        .expect("fresh fill after shrink");
    assert_eq!(got, fresh);
}

/// Gate 4: bounded and idempotent. Zero-byte requests are free; an oversized
/// request returns only what was evictable; a second maximal reclaim finds
/// (near-)nothing; accounting never exceeds the configured capacity.
#[tokio::test(flavor = "multi_thread")]
async fn reclaim_is_bounded_and_idempotent() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, _calls) = engine_over(&dir, 96 * MIB, 128 * MIB).await;
    fill_cold(&store, &engine, 6).await;

    assert_eq!(engine.reclaim_disk_for_writeback(0).await, 0);

    let first = engine.reclaim_disk_for_writeback(u64::MAX).await;
    assert!(first <= 96 * MIB, "cannot reclaim more than the budget");
    let second = engine.reclaim_disk_for_writeback(u64::MAX).await;
    assert!(
        second <= TEST_BLOCK_BYTES,
        "a second maximal reclaim finds nothing substantial, got {second}"
    );
    assert!(
        engine.disk_growth_room_bytes() <= 96 * MIB,
        "growth room can never exceed the configured capacity"
    );
}
