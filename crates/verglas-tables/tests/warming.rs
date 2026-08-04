//! Acceptance tests for the eager metadata warming pipeline (#168), driven
//! against a real cache engine so backend GETs are ground truth.
//!
//! - **Zero cold-planning GETs** (#50/#168 headline): after warming a watched
//!   fixture table, a cold client planning walk — metadata.json, manifest list,
//!   manifests, and every Parquet footer — issues *zero* backend metadata GETs.
//! - **Footer economy**: footer warming uses ≤2 GETs per file, 1 when the
//!   footer fits the speculative window.
//! - **Parquet-only**: an Avro data file is never suffix-read.
//! - **Resumable**: a second warming pass issues zero backend GETs.
//! - **Budget + concurrency**: the byte budget and the semaphore bound warming
//!   deterministically.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tempfile::TempDir;
use verglas_cache::HybridCacheEngine;
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_s3::{
    BackendStore, ObjectGet, ObjectMeta, ObjectRead, PassthroughRead, ReadError, ReadRange,
};
use verglas_tables::catalog::{
    CatalogWatcher, SnapshotEntry, TableChanged, TableIdent, TableState,
};
use verglas_tables::fetch::ObjectStoreFetch;
use verglas_tables::iceberg::{self, DataFileEntry};
use verglas_tables::mapper::FileFormat;
use verglas_tables::warming::budget::TokenBucket;
use verglas_tables::warming::{WarmConfig, WarmTarget, Warmer, WarmingCoordinator};

/// The sidecar `regenerate.py` writes next to the fixture objects.
#[derive(serde::Deserialize)]
struct Sidecar {
    bucket: String,
    metadata_key: String,
    current_snapshot_id: i64,
}

/// Absolute path of the checked-in pyiceberg-v2 fixture directory.
fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pyiceberg-v2")
}

/// Loads every fixture object into an in-memory store (key relative to
/// `objects/`) plus the parsed sidecar.
async fn load_fixture() -> (Arc<InMemory>, Sidecar) {
    let dir = fixture_dir();
    let sidecar: Sidecar = serde_json::from_slice(
        &std::fs::read(dir.join("fixture.json")).expect("fixture.json present"),
    )
    .expect("sidecar parses");
    let store = Arc::new(InMemory::new());
    let objects_root = dir.join("objects");
    let mut stack = vec![objects_root.clone()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).expect("fixture dir readable") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let key = path
                .strip_prefix(&objects_root)
                .expect("under objects/")
                .to_string_lossy()
                .into_owned();
            let bytes = std::fs::read(&path).expect("object readable");
            store
                .put(&StorePath::from(key.as_str()), PutPayload::from(bytes))
                .await
                .expect("put");
        }
    }
    (store, sidecar)
}

/// Backend GET/HEAD call counts — the ground truth for "zero backend traffic".
#[derive(Clone, Default)]
struct BackendCalls {
    gets: Arc<AtomicU64>,
    heads: Arc<AtomicU64>,
}

/// An `ObjectRead` that counts calls (and tracks max concurrent in-flight
/// GETs) before delegating.
struct CountingRead<R> {
    inner: R,
    calls: BackendCalls,
    in_flight: Arc<AtomicU64>,
    max_in_flight: Arc<AtomicU64>,
}

impl<R: ObjectRead> ObjectRead for CountingRead<R> {
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.calls.gets.fetch_add(1, Ordering::SeqCst);
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        let out = self.inner.get(key, range).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        out
    }
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.calls.heads.fetch_add(1, Ordering::SeqCst);
        self.inner.head(key).await
    }
}

type Engine = HybridCacheEngine<
    CountingRead<PassthroughRead>,
    verglas_core::peer::NoopPeerFetch,
    verglas_core::ring::RendezvousRing,
>;

/// Builds a counting cache engine over the fixture store.
async fn build_engine(
    store: Arc<InMemory>,
    dir: &TempDir,
) -> (Arc<Engine>, BackendCalls, Arc<AtomicU64>) {
    let calls = BackendCalls::default();
    let max_in_flight = Arc::new(AtomicU64::new(0));
    let backend = CountingRead {
        inner: PassthroughRead::new(BackendStore::single("lake", store)),
        calls: calls.clone(),
        in_flight: Arc::new(AtomicU64::new(0)),
        max_in_flight: max_in_flight.clone(),
    };
    let config = CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(256 * 1024 * 1024),
        dram_bytes: ByteSize(128 * 1024 * 1024),
        ..CacheConfig::default()
    };
    let engine = Arc::new(
        HybridCacheEngine::single_node(backend, &config)
            .await
            .expect("build engine"),
    );
    (engine, calls, max_in_flight)
}

/// Reads a range fully through the engine, returning the served bytes.
async fn read_all(engine: &Engine, k: &CacheKey, range: ReadRange) -> Bytes {
    let got = engine.get(k, range).await.expect("get");
    let mut buf = BytesMut::new();
    let mut body = got.body;
    while let Some(chunk) = body.try_next().await.expect("chunk") {
        buf.extend_from_slice(&chunk);
    }
    buf.freeze()
}

/// A cache key in the fixture bucket.
fn key(bucket: &str, k: &str) -> CacheKey {
    CacheKey {
        bucket: bucket.to_owned(),
        key: k.to_owned(),
    }
}

/// The default footer speculative window a warmer and a planning engine agree
/// on (arrow-rs / DuckDB read the last 64 KiB speculatively).
const FOOTER_WINDOW: u64 = 64 * 1024;

/// Discovers a snapshot's metadata keys and Parquet footers directly from the
/// store (bypassing the counting engine) so the cold-walk test knows exactly
/// what to re-read.
async fn discover(store: &Arc<InMemory>, sidecar: &Sidecar) -> (String, Vec<String>, Vec<String>) {
    let fetch = ObjectStoreFetch::new(store.clone() as Arc<dyn ObjectStore>);
    let plan = iceberg::walk_snapshot(
        &fetch,
        &sidecar.bucket,
        &sidecar.metadata_key,
        sidecar.current_snapshot_id,
    )
    .await
    .expect("walk");
    let parquet = plan
        .data_files
        .iter()
        .filter(|f| f.format == FileFormat::Parquet)
        .map(|f| f.path.clone())
        .collect();
    (plan.manifest_list_key, plan.manifest_keys, parquet)
}

/// Headline (#50/#168): after warming, a cold planning walk issues ZERO
/// backend metadata GETs.
#[tokio::test]
async fn warmed_table_serves_cold_planning_walk_with_zero_backend_gets() {
    let (store, sidecar) = load_fixture().await;
    let (list_key, manifest_keys, footers) = discover(&store, &sidecar).await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, calls, _) = build_engine(store, &dir).await;

    let warmer = Warmer::new(
        engine.clone(),
        WarmConfig::default(),
        Arc::new(TokenBucket::unlimited()),
    );
    let summary = warmer
        .warm_table(&WarmTarget {
            bucket: sidecar.bucket.clone(),
            metadata_key: sidecar.metadata_key.clone(),
            snapshot_id: sidecar.current_snapshot_id,
        })
        .await
        .expect("warm");
    assert_eq!(summary.footers_warmed, footers.len() as u64);

    // Cold client planning walk: metadata.json, manifest list, manifests (Full
    // GETs), then every footer (speculative suffix). All must hit the pinned
    // meta store.
    let gets_before = calls.gets.load(Ordering::SeqCst);
    let _ = read_all(
        &engine,
        &key(&sidecar.bucket, &sidecar.metadata_key),
        ReadRange::Full,
    )
    .await;
    let _ = read_all(&engine, &key(&sidecar.bucket, &list_key), ReadRange::Full).await;
    for m in &manifest_keys {
        let _ = read_all(&engine, &key(&sidecar.bucket, m), ReadRange::Full).await;
    }
    for f in &footers {
        let _ = read_all(
            &engine,
            &key(&sidecar.bucket, f),
            ReadRange::Suffix(FOOTER_WINDOW),
        )
        .await;
    }
    let gets_after = calls.gets.load(Ordering::SeqCst);
    assert_eq!(
        gets_after - gets_before,
        0,
        "a cold planning walk after warming issued backend metadata GETs"
    );
    assert_eq!(
        calls.heads.load(Ordering::SeqCst),
        0,
        "warming issues no HEADs"
    );
}

/// Footer economy: a footer that fits the speculative window costs exactly one
/// GET per file.
#[tokio::test]
async fn footer_warming_uses_one_get_for_fitting_footers() {
    let (store, sidecar) = load_fixture().await;
    let (_, _, footers) = discover(&store, &sidecar).await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, calls, _) = build_engine(store, &dir).await;

    // Warm only the footers (curated list) so we count footer GETs in
    // isolation from block-metadata reads.
    let files: Vec<DataFileEntry> = footers
        .iter()
        .map(|p| DataFileEntry {
            path: p.clone(),
            format: FileFormat::Parquet,
            size: 4096,
            partition: vec![],
        })
        .collect();
    let warmer = Warmer::new(
        engine.clone(),
        WarmConfig::default(),
        Arc::new(TokenBucket::unlimited()),
    );
    let gets_before = calls.gets.load(Ordering::SeqCst);
    let summary = warmer.warm_footers(&sidecar.bucket, &files).await;
    let footer_gets = calls.gets.load(Ordering::SeqCst) - gets_before;
    assert_eq!(summary.footers_warmed, footers.len() as u64);
    assert_eq!(summary.footer_refetches, 0, "fixture footers fit 64 KiB");
    assert_eq!(
        footer_gets,
        footers.len() as u64,
        "one GET per fitting footer"
    );
}

/// Parquet-only (Avro addendum): an Avro data file is never suffix-read.
#[tokio::test]
async fn avro_data_files_are_never_suffix_read() {
    let (store, sidecar) = load_fixture().await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, calls, _) = build_engine(store, &dir).await;

    let files = vec![DataFileEntry {
        path: "warehouse/db/events/data/rows-0.avro".to_owned(),
        format: FileFormat::Avro,
        size: 8192,
        partition: vec![],
    }];
    let warmer = Warmer::new(
        engine.clone(),
        WarmConfig::default(),
        Arc::new(TokenBucket::unlimited()),
    );
    let gets_before = calls.gets.load(Ordering::SeqCst);
    let summary = warmer.warm_footers(&sidecar.bucket, &files).await;
    assert_eq!(summary.footers_warmed, 0);
    assert_eq!(summary.skipped_non_parquet, 1);
    assert_eq!(
        calls.gets.load(Ordering::SeqCst) - gets_before,
        0,
        "no backend GET against an Avro data file"
    );
}

/// Resumable: a second warming pass over an already-warm table issues zero
/// backend GETs.
#[tokio::test]
async fn second_warming_pass_issues_no_backend_gets() {
    let (store, sidecar) = load_fixture().await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, calls, _) = build_engine(store, &dir).await;
    let warmer = Warmer::new(
        engine.clone(),
        WarmConfig::default(),
        Arc::new(TokenBucket::unlimited()),
    );
    let target = WarmTarget {
        bucket: sidecar.bucket.clone(),
        metadata_key: sidecar.metadata_key.clone(),
        snapshot_id: sidecar.current_snapshot_id,
    };
    warmer.warm_table(&target).await.expect("first warm");
    let gets_before = calls.gets.load(Ordering::SeqCst);
    warmer.warm_table(&target).await.expect("second warm");
    assert_eq!(
        calls.gets.load(Ordering::SeqCst) - gets_before,
        0,
        "re-warming an already-warm snapshot issued backend GETs"
    );
}

/// The semaphore bounds in-flight footer reads.
#[tokio::test]
async fn semaphore_bounds_in_flight_footer_reads() {
    let (store, sidecar) = load_fixture().await;
    let (_, _, footers) = discover(&store, &sidecar).await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, _, max_in_flight) = build_engine(store, &dir).await;
    let files: Vec<DataFileEntry> = footers
        .iter()
        .map(|p| DataFileEntry {
            path: p.clone(),
            format: FileFormat::Parquet,
            size: 4096,
            partition: vec![],
        })
        .collect();
    let warmer = Warmer::new(
        engine.clone(),
        WarmConfig {
            concurrency: 2,
            ..WarmConfig::default()
        },
        Arc::new(TokenBucket::unlimited()),
    );
    warmer.warm_footers(&sidecar.bucket, &files).await;
    assert!(
        max_in_flight.load(Ordering::SeqCst) <= 2,
        "semaphore let more than 2 footer reads run at once: {}",
        max_in_flight.load(Ordering::SeqCst)
    );
}

/// A table whose footer working set exceeds the meta budget raises an alert
/// and caps warming instead of thrashing.
#[tokio::test]
async fn over_budget_table_alerts_and_caps() {
    let (store, sidecar) = load_fixture().await;
    let (_, _, footers) = discover(&store, &sidecar).await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, _, _) = build_engine(store, &dir).await;
    let files: Vec<DataFileEntry> = footers
        .iter()
        .map(|p| DataFileEntry {
            path: p.clone(),
            format: FileFormat::Parquet,
            size: 4096,
            partition: vec![],
        })
        .collect();
    // Budget below one footer's estimated cost: warming must alert.
    let warmer = Warmer::new(
        engine.clone(),
        WarmConfig {
            meta_budget_bytes: 100,
            ..WarmConfig::default()
        },
        Arc::new(TokenBucket::unlimited()),
    );
    let summary = warmer.warm_footers(&sidecar.bucket, &files).await;
    assert!(summary.budget_alerted, "over-budget table did not alert");
    assert!(
        summary.footers_warmed < footers.len() as u64,
        "warming did not cap under budget"
    );
    assert_eq!(warmer.progress().snapshot().budget_alerts, 1);
}

/// Warming honors a Parquet footer larger than the speculative window with a
/// single second read (≤2 GETs), then serves it warm.
#[tokio::test]
async fn large_footer_uses_two_gets_and_serves_warm() {
    // A tiny speculative window forces the second read on the fixture footers.
    let (store, sidecar) = load_fixture().await;
    let (_, _, footers) = discover(&store, &sidecar).await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, calls, _) = build_engine(store, &dir).await;
    let files: Vec<DataFileEntry> = footers
        .iter()
        .map(|p| DataFileEntry {
            path: p.clone(),
            format: FileFormat::Parquet,
            size: 4096,
            partition: vec![],
        })
        .collect();
    let warmer = Warmer::new(
        engine.clone(),
        WarmConfig {
            footer_read_bytes: 8, // only the trailer fits; forces a refetch
            ..WarmConfig::default()
        },
        Arc::new(TokenBucket::unlimited()),
    );
    let gets_before = calls.gets.load(Ordering::SeqCst);
    let summary = warmer.warm_footers(&sidecar.bucket, &files).await;
    let footer_gets = calls.gets.load(Ordering::SeqCst) - gets_before;
    assert_eq!(summary.footers_warmed, footers.len() as u64);
    assert_eq!(summary.footer_refetches, footers.len() as u64);
    assert_eq!(
        footer_gets,
        2 * footers.len() as u64,
        "a too-small window costs exactly two GETs per footer"
    );
}

// ---- Watcher-driven refresh (#168) -----------------------------------------

use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::broadcast;

/// A hand-driven catalog watcher for the coordinator tests: its state is set
/// directly and change events are pushed on demand.
struct FakeWatcher {
    state: Mutex<HashMap<TableIdent, TableState>>,
    tx: broadcast::Sender<TableChanged>,
}

impl FakeWatcher {
    fn new() -> Arc<FakeWatcher> {
        let (tx, _) = broadcast::channel(64);
        Arc::new(FakeWatcher {
            state: Mutex::new(HashMap::new()),
            tx,
        })
    }
    /// Points a table at a metadata.json + snapshot.
    fn set(&self, table: TableIdent, location: String, snapshot: Option<i64>) {
        self.state.lock().expect("watcher state lock").insert(
            table,
            TableState {
                metadata_location: location,
                current_snapshot_id: snapshot,
            },
        );
    }
    /// Fires a change event (as the poll loop would on a commit).
    fn fire(&self, change: TableChanged) {
        let _ = self.tx.send(change);
    }
}

impl CatalogWatcher for FakeWatcher {
    fn watched_tables(&self) -> Vec<TableIdent> {
        self.state
            .lock()
            .expect("watcher state lock")
            .keys()
            .cloned()
            .collect()
    }
    fn table_state(&self, table: &TableIdent) -> Option<TableState> {
        self.state
            .lock()
            .expect("watcher state lock")
            .get(table)
            .cloned()
    }
    fn lineage(&self, _table: &TableIdent) -> Vec<SnapshotEntry> {
        vec![]
    }
    fn subscribe(&self) -> broadcast::Receiver<TableChanged> {
        self.tx.subscribe()
    }
    fn seeded(&self) -> tokio::sync::watch::Receiver<bool> {
        // Hand-driven state is set before the coordinator binds, so the fake
        // is seeded from birth.
        let (tx, rx) = tokio::sync::watch::channel(true);
        drop(tx);
        rx
    }
}

/// The metadata.json location the fixture's catalog pointer would name.
fn fixture_location(sidecar: &Sidecar) -> String {
    format!("s3://{}/{}", sidecar.bucket, sidecar.metadata_key)
}

/// `warm_all` onboards every watched table by walking its current pointer.
#[tokio::test]
async fn coordinator_warms_all_watched_tables() {
    let (store, sidecar) = load_fixture().await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, _, _) = build_engine(store, &dir).await;
    let warmer = Arc::new(Warmer::new(
        engine.clone(),
        WarmConfig::default(),
        Arc::new(TokenBucket::unlimited()),
    ));
    let watcher = FakeWatcher::new();
    let table = TableIdent::new(&["db"], "events");
    watcher.set(
        table,
        fixture_location(&sidecar),
        Some(sidecar.current_snapshot_id),
    );

    let coord = WarmingCoordinator::new(warmer.clone(), watcher);
    coord.warm_all().await;

    let progress = warmer.progress().snapshot();
    assert_eq!(progress.tables_completed, 1);
    assert_eq!(
        progress.footers_warmed, 4,
        "all four fixture footers warmed"
    );
}

/// A commit event re-warms the changed table's new pointer.
#[tokio::test]
async fn coordinator_refreshes_on_commit() {
    let (store, sidecar) = load_fixture().await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, _, _) = build_engine(store, &dir).await;
    let warmer = Arc::new(Warmer::new(
        engine.clone(),
        WarmConfig::default(),
        Arc::new(TokenBucket::unlimited()),
    ));
    let watcher = FakeWatcher::new();
    let table = TableIdent::new(&["db"], "events");
    watcher.set(
        table.clone(),
        fixture_location(&sidecar),
        Some(sidecar.current_snapshot_id),
    );
    let coord = WarmingCoordinator::new(warmer.clone(), watcher);

    coord.warm_all().await;
    assert_eq!(warmer.progress().snapshot().tables_started, 1);

    // A commit swings the pointer; the coordinator warms the new snapshot.
    coord
        .on_change(&TableChanged {
            table: table.clone(),
            old_snapshot: Some(sidecar.current_snapshot_id),
            new_snapshot: Some(sidecar.current_snapshot_id),
        })
        .await;
    assert_eq!(
        warmer.progress().snapshot().tables_started,
        2,
        "commit triggered a refresh warm"
    );
}

/// A dropped table (new snapshot None) is not warmed.
#[tokio::test]
async fn coordinator_ignores_dropped_tables() {
    let (store, sidecar) = load_fixture().await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, _, _) = build_engine(store, &dir).await;
    let warmer = Arc::new(Warmer::new(
        engine.clone(),
        WarmConfig::default(),
        Arc::new(TokenBucket::unlimited()),
    ));
    let watcher = FakeWatcher::new();
    let table = TableIdent::new(&["db"], "events");
    watcher.set(
        table.clone(),
        fixture_location(&sidecar),
        Some(sidecar.current_snapshot_id),
    );
    let coord = WarmingCoordinator::new(warmer.clone(), watcher);
    coord
        .on_change(&TableChanged {
            table,
            old_snapshot: Some(sidecar.current_snapshot_id),
            new_snapshot: None,
        })
        .await;
    assert_eq!(warmer.progress().snapshot().tables_started, 0);
}

/// The spawned loop warms on startup and on a fired commit event.
#[tokio::test]
async fn coordinator_spawn_warms_on_startup_and_events() {
    let (store, sidecar) = load_fixture().await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, _, _) = build_engine(store, &dir).await;
    let warmer = Arc::new(Warmer::new(
        engine.clone(),
        WarmConfig::default(),
        Arc::new(TokenBucket::unlimited()),
    ));
    let watcher = FakeWatcher::new();
    let table = TableIdent::new(&["db"], "events");
    watcher.set(
        table.clone(),
        fixture_location(&sidecar),
        Some(sidecar.current_snapshot_id),
    );
    let progress = warmer.progress();
    let coord = WarmingCoordinator::new(warmer, watcher.clone());
    let _handle = coord.spawn();

    // Startup warm.
    wait_until(|| progress.snapshot().tables_completed >= 1).await;
    // A fired commit event drives a second warm.
    watcher.fire(TableChanged {
        table,
        old_snapshot: Some(sidecar.current_snapshot_id),
        new_snapshot: Some(sidecar.current_snapshot_id),
    });
    wait_until(|| progress.snapshot().tables_started >= 2).await;
}

/// Polls `cond` until true or a bounded number of yields elapse (the background
/// warm completes near-instantly against the in-memory fixture).
async fn wait_until(cond: impl Fn() -> bool) {
    for _ in 0..1000 {
        if cond() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    panic!("condition not met within the deadline");
}

// ---- Startup warming of a pre-existing table (#168 reproduction) -----------

use verglas_tables::catalog::{CatalogError, CatalogSource, PollingWatcher, WatcherOptions};

/// A catalog source whose table set exists BEFORE any watcher polls it — the
/// "server starts against an already-populated catalog" scenario. Each call
/// takes a few milliseconds, as any real REST catalog does: the first poll
/// must still be in flight while the coordinator starts up, which is exactly
/// the timing window the server hits against a real catalog.
struct PreExistingSource {
    table: TableIdent,
    state: TableState,
}

/// Modeled per-request catalog latency (real REST calls are 1–100+ ms).
const CATALOG_LATENCY: std::time::Duration = std::time::Duration::from_millis(25);

#[async_trait::async_trait]
impl CatalogSource for PreExistingSource {
    /// The one pre-existing table, present from the very first poll.
    async fn list_tables(&self) -> Result<Vec<TableIdent>, CatalogError> {
        tokio::time::sleep(CATALOG_LATENCY).await;
        Ok(vec![self.table.clone()])
    }
    /// The fixed pointer — no commit ever swings it during the test.
    async fn table_pointer(&self, _table: &TableIdent) -> Result<TableState, CatalogError> {
        tokio::time::sleep(CATALOG_LATENCY).await;
        Ok(self.state.clone())
    }
}

/// #168 bug reproduction: a table that already exists in the catalog when the
/// server (watcher + coordinator) starts MUST be warmed by the startup pass,
/// without waiting for its next commit. Before the fix the coordinator's
/// initial `warm_all` ran against the watcher's still-empty pre-first-poll
/// state, and the seeding poll emits no events, so the table was never warmed.
#[tokio::test]
async fn preexisting_table_is_warmed_on_startup_without_a_commit() {
    let (store, sidecar) = load_fixture().await;
    let dir = TempDir::new().expect("tempdir");
    let (engine, _, _) = build_engine(store, &dir).await;
    let warmer = Arc::new(Warmer::new(
        engine.clone(),
        WarmConfig::default(),
        Arc::new(TokenBucket::unlimited()),
    ));
    let progress = warmer.progress();

    // The table exists in the catalog BEFORE the watcher's first poll.
    let source = PreExistingSource {
        table: TableIdent::new(&["db"], "events"),
        state: TableState {
            metadata_location: fixture_location(&sidecar),
            current_snapshot_id: Some(sidecar.current_snapshot_id),
        },
    };
    let options = WatcherOptions {
        interval: std::time::Duration::from_millis(20),
        jitter: std::time::Duration::ZERO,
        ..WatcherOptions::default()
    };
    // Server startup order: watcher spawns, then the coordinator binds to it.
    let watcher = Arc::new(PollingWatcher::spawn(source, options));
    let _handle = WarmingCoordinator::new(warmer, watcher).spawn();

    // No commit is ever fired. The startup pass alone must warm the table.
    wait_until(|| progress.snapshot().tables_completed >= 1).await;
    assert_eq!(
        progress.snapshot().footers_warmed,
        4,
        "all four fixture footers warmed by the startup pass"
    );
}
