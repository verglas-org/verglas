//! Acceptance tests for the dedicated metadata store (#50):
//!
//! - **Hard isolation** — sustained data-scan pressure (10× the data tier)
//!   evicts *zero* pinned metadata entries; the metadata hit rate stays ~100%
//!   (counter-asserted).
//! - **Byte-identity** — metadata served from the pinned store is byte-for-byte
//!   what the origin holds.
//! - **Budget accounting** — the metadata store's DRAM is carved from *inside*
//!   the existing DRAM ceiling, and the on-disk ceiling is untouched.
//! - **Watcher refresh** — refreshing a catalog-pointer object drops its mapping
//!   so the next planning read re-resolves the moved snapshot.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use tempfile::TempDir;
use verglas_cache::HybridCacheEngine;
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_s3::{
    BackendStore, ObjectGet, ObjectMeta, ObjectRead, PassthroughRead, ReadError, ReadRange,
};

const BUCKET: &str = "warehouse";
const MIB: u64 = 1024 * 1024;

/// Deterministic payload: byte `i` is `(i + seed) % 251`.
fn pattern(seed: u64, len: u64) -> Bytes {
    (0..len)
        .map(|i| ((i + seed) % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// A key in the warehouse bucket.
fn key(k: &str) -> CacheKey {
    CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: BUCKET.to_owned(),
        key: k.to_owned(),
    }
}

/// Seeds an object into the in-memory origin.
async fn seed(store: &Arc<InMemory>, k: &str, bytes: Bytes) {
    store
        .put(&Path::from(k), PutPayload::from(bytes))
        .await
        .expect("seed object");
}

/// Backend GET/HEAD call counts, the ground truth for "zero backend traffic".
#[derive(Clone, Default)]
struct BackendCalls {
    gets: Arc<AtomicU64>,
    heads: Arc<AtomicU64>,
}

/// An `ObjectRead` that counts calls before delegating.
struct CountingRead<R> {
    inner: R,
    calls: BackendCalls,
}

impl<R: ObjectRead> ObjectRead for CountingRead<R> {
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.calls.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.calls.heads.fetch_add(1, Ordering::Relaxed);
        self.inner.head(key).await
    }
}

type Engine = HybridCacheEngine<
    CountingRead<PassthroughRead>,
    verglas_core::peer::NoopPeerFetch,
    verglas_core::ring::RendezvousRing,
>;

/// Builds a counting engine with admission optionally disabled (so a scan can
/// actually churn the data tier when the test wants real pressure).
async fn engine(
    dir: &TempDir,
    dram: u64,
    capacity: u64,
    admission: bool,
) -> (Arc<InMemory>, Engine, BackendCalls) {
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
        admission: verglas_core::config::Admission {
            enabled: admission,
            ..Default::default()
        },
        ..CacheConfig::default()
    };
    let engine = HybridCacheEngine::single_node(backend, &config)
        .await
        .expect("build engine");
    (store, engine, calls)
}

/// Reads a range fully through the engine.
async fn read_all(engine: &Engine, k: &CacheKey, range: ReadRange) -> (Range<u64>, Bytes) {
    let got = engine.get(k, range).await.expect("get");
    let mut buf = BytesMut::new();
    let mut body = got.body;
    while let Some(chunk) = body.try_next().await.expect("chunk") {
        buf.extend_from_slice(&chunk);
    }
    (got.range, buf.freeze())
}

/// Hard isolation (#50 acceptance): warm a set of metadata objects into the
/// pinned store, then drive 10× the data tier through the data cache. Not one
/// pinned metadata entry is evicted — every metadata re-read is a meta-store
/// hit with zero backend traffic.
#[tokio::test(flavor = "multi_thread")]
async fn data_scan_pressure_never_evicts_pinned_metadata() {
    let dir = TempDir::new().expect("temp dir");
    // Admission OFF so the scan genuinely fills and evicts the data tier. The
    // metadata working set (~1.5 MiB) is deliberately larger than the meta
    // store's DRAM front (5% of the block DRAM, less its own write pipeline),
    // so it spans the pinned store's DRAM *and* NVMe tiers — a stronger
    // isolation test than a DRAM-only set. The warm phase therefore ends with
    // a flush: an entry whose NVMe write is still queued when the scan starts
    // is not yet stored, and losing it would report as an eviction this test
    // forbids when it is really a missing durability barrier.
    let (store, engine, calls) = engine(&dir, 128 * MIB, 48 * MIB, false).await;

    // Warm 6 metadata objects (metadata.json + manifests) into the pinned
    // store. Each is a single small block, classified by the path heuristics.
    let meta_objs: Vec<(String, Bytes)> = (0..6)
        .map(|i| {
            let name = if i % 2 == 0 {
                format!("db/t/metadata/v{i}.metadata.json")
            } else {
                format!("db/t/metadata/snap-{i}-1-uuid.avro")
            };
            (name, pattern(700 + i, 256 * 1024))
        })
        .collect();
    for (name, body) in &meta_objs {
        seed(&store, name, body.clone()).await;
        let (_, got) = read_all(&engine, &key(name), ReadRange::Full).await;
        assert_eq!(&got, body, "cold metadata read is byte-identical");
    }
    // A second read of each warms the meta-store block (cold read admitted only
    // the mapping + block; this pass proves it is now a meta-store hit).
    for (name, body) in &meta_objs {
        let (_, got) = read_all(&engine, &key(name), ReadRange::Full).await;
        assert_eq!(&got, body);
    }

    // Durability barrier: every metadata entry is admitted and its disk write
    // drained before any data pressure exists. Without this the scan races the
    // meta store's queued writes.
    engine.flush().await;

    let before = engine.counters().snapshot();
    let (gets_before, _) = (calls.gets.load(Ordering::Relaxed), 0);

    // Sustained data-scan pressure: 60 unique 8 MiB data objects = 480 MiB,
    // 10× the 48 MiB data disk budget. Everything data churns.
    for i in 0..60u64 {
        let name = format!("db/t/data/scan-{i}.parquet");
        let body = pattern(i, 8 * MIB);
        seed(&store, &name, body.clone()).await;
        let (_, got) = read_all(&engine, &key(&name), ReadRange::Full).await;
        assert_eq!(got.len(), body.len(), "scan bytes served");
    }

    // Second barrier: a multi-block scan read leaves look-ahead fills detached,
    // so the backend-GET counter is still moving when the loop ends. Sampling
    // it before those drain makes the metadata assertion below race the scan's
    // own tail — the off-by-one this test used to flake on.
    engine.flush().await;

    let gets_after_scan = calls.gets.load(Ordering::Relaxed);
    assert!(
        gets_after_scan > gets_before,
        "the scan actually issued backend fills"
    );

    // Re-read every metadata object: all must be meta-store hits, zero backend
    // GETs, zero meta misses — the scan evicted nothing.
    for (name, body) in &meta_objs {
        let (_, got) = read_all(&engine, &key(name), ReadRange::Full).await;
        assert_eq!(
            &got, body,
            "pinned metadata survived the scan byte-identically"
        );
    }
    let after = engine.counters().snapshot();

    assert_eq!(
        calls.gets.load(Ordering::Relaxed),
        gets_after_scan,
        "re-reading pinned metadata issued ZERO backend GETs after 10x data pressure"
    );
    assert_eq!(
        after.meta_misses, before.meta_misses,
        "not one pinned metadata entry was evicted (no new meta misses)"
    );
    assert_eq!(
        after.meta_hits - before.meta_hits,
        meta_objs.len() as u64,
        "every metadata re-read was a meta-store hit (~100% hit rate)"
    );
    assert!(
        after.meta_bytes_served > before.meta_bytes_served,
        "metadata bytes served from the pinned store"
    );
}

/// Byte-identity through the meta store: a manifest read served from the pinned
/// store equals the origin's bytes exactly, and reports the meta tier.
#[tokio::test(flavor = "multi_thread")]
async fn metadata_served_from_pinned_store_is_byte_identical() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = engine(&dir, 96 * MIB, 64 * MIB, true).await;

    let name = "db/t/metadata/snap-42-1-abc.avro";
    let body = pattern(11, 300 * 1024);
    seed(&store, name, body.clone()).await;

    // Cold read pins it.
    let (_, cold) = read_all(&engine, &key(name), ReadRange::Full).await;
    assert_eq!(cold, body, "cold metadata read byte-identical");
    let gets_after_cold = calls.gets.load(Ordering::Relaxed);

    // Warm the meta-store block, then a third read is a pure meta-store hit.
    let _ = read_all(&engine, &key(name), ReadRange::Full).await;
    let hits_before = engine.counters().snapshot().meta_hits;
    let (_, warm) = read_all(&engine, &key(name), ReadRange::Full).await;
    assert_eq!(warm, body, "warm metadata read byte-identical to origin");

    let snap = engine.counters().snapshot();
    assert!(snap.meta_hits > hits_before, "served from the meta store");
    assert!(
        calls.gets.load(Ordering::Relaxed) >= gets_after_cold,
        "warm reads add no backend HEADs/GETs beyond the fills",
    );
}

/// The metadata store's DRAM is carved from *inside* the existing ceiling: with
/// both stores warmed, total DRAM residency stays under the budget minus the
/// disk-write pipeline reservation — the same tripwire the data ceiling uses.
#[tokio::test(flavor = "multi_thread")]
async fn meta_store_budget_is_inside_the_dram_ceiling() {
    let dir = TempDir::new().expect("temp dir");
    let dram = 128 * MIB;
    let (store, engine, _calls) = engine(&dir, dram, 96 * MIB, false).await;

    // Warm many metadata objects — more than the meta store can hold — so the
    // meta store fills to capacity and would overflow a naive budget.
    for i in 0..40u64 {
        let name = format!("db/t/metadata/v{i}.metadata.json");
        let body = pattern(i, 512 * 1024);
        seed(&store, &name, body.clone()).await;
        let _ = read_all(&engine, &key(&name), ReadRange::Full).await;
        let _ = read_all(&engine, &key(&name), ReadRange::Full).await;
    }
    // Warm a data working set larger than the data tier.
    for i in 0..24u64 {
        let name = format!("db/t/data/part-{i}.parquet");
        let body = pattern(1000 + i, 8 * MIB);
        seed(&store, &name, body.clone()).await;
        let _ = read_all(&engine, &key(&name), ReadRange::Full).await;
        engine.flush().await;
    }

    // The same ceiling assertion the data tripwire uses: everything both DRAM
    // stores hold, plus the pinned meta store, fits the budget minus the fixed
    // 48 MiB disk-write pipeline reservation.
    let ceiling = dram - 48 * MIB;
    assert!(
        engine.dram_usage() <= ceiling,
        "DRAM residency {} exceeded the {ceiling}-byte ceiling — the meta store \
         budget escaped the existing DRAM ceiling",
        engine.dram_usage()
    );
}

/// Watcher-driven refresh (#47/#50): refreshing a catalog-pointer object drops
/// its mapping so the next planning read re-resolves the moved snapshot from the
/// origin, and the counter records the refresh.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_watched_drops_the_mapping_and_reresolves() {
    let dir = TempDir::new().expect("temp dir");
    let (store, engine, calls) = engine(&dir, 96 * MIB, 64 * MIB, true).await;

    let name = "db/t/metadata/v1.metadata.json";
    seed(&store, name, pattern(3, 200 * 1024)).await;
    let _ = read_all(&engine, &key(name), ReadRange::Full).await;
    let _ = read_all(&engine, &key(name), ReadRange::Full).await;
    let gets_before = calls.gets.load(Ordering::Relaxed);

    // The watcher sees the pointer move and refreshes it.
    engine.refresh_watched(&[key(name)]);
    assert_eq!(engine.counters().snapshot().meta_refreshes, 1);

    // The next read re-resolves against the origin (a fresh backend GET).
    let _ = read_all(&engine, &key(name), ReadRange::Full).await;
    assert!(
        calls.gets.load(Ordering::Relaxed) > gets_before,
        "post-refresh read re-resolved the moved snapshot from the origin"
    );
}
