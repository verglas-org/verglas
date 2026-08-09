//! Acceptance tests for snapshot-driven prefetch and orphan retirement (#51),
//! driven against a real cache engine so cache hit/miss is ground truth.
//!
//! - **Compaction is classified and diffed**: a `replace` commit's diff names
//!   the added (compacted) file and the removed (rewritten) files, read from the
//!   changed manifests only.
//! - **Heat carries logically**: a column hot before compaction is scheduled for
//!   prefetch in the rewritten file — by `(partition, column)` identity — before
//!   any access to the new file.
//! - **Prefetched before first access**: after the executor drains the plan, a
//!   client's first read of the hot chunk of the new file is a cache hit (zero
//!   backend GETs).
//! - **Append triggers no data prefetch**; **Avro carries by partition** to a
//!   whole-file prefetch (the addendum); **retirement** demotes removed files
//!   with a grace window.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use apache_avro::types::Value as AvroValue;
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
use verglas_tables::fetch::{MetadataFetch, ObjectStoreFetch};
use verglas_tables::lifecycle::PrefetchCoordinator;
use verglas_tables::lifecycle::diff::{SnapshotOp, diff_snapshot};
use verglas_tables::lifecycle::heat::{
    EpochClock, HeatChannel, HeatFeed, HeatLedger, HeatSample, PartitionHash, resolve_key,
    run_aggregator,
};
use verglas_tables::lifecycle::prefetch::{
    PrefetchConfig, PrefetchExecutor, PrefetchPriority, plan_prefetch,
};
use verglas_tables::lifecycle::retire::{DEFAULT_GRACE_MS, RetirementScheduler};
use verglas_tables::mapper::{BuildInputs, ColumnId, FileFormat, Mapper};

#[path = "support/mod.rs"]
mod support;

const BUCKET: &str = "lake";
const ROOT: &str = "warehouse/db/events";
const SNAP_APPEND: i64 = 1001;
const SNAP_REPLACE: i64 = 1002;

// ---- Fixture: a two-snapshot table, append then a compaction (replace) -------

/// A built compaction fixture: the store plus the keys a test drives.
struct Fixture {
    store: Arc<InMemory>,
    metadata_key: String,
    /// The two small Parquet files snapshot 1 appended (partition category=a).
    small: Vec<String>,
    /// The single big Parquet file the replace commit added.
    compacted: String,
    /// Etag of the first small file (for mapper footer warming).
    small0_etag: String,
}

/// Builds `store` objects for the append→replace Parquet fixture and returns the
/// keys. Snapshot 1 appends two small files in partition `category=a`; snapshot
/// 2 (`replace`) rewrites them into one compacted file (ADDED) and marks the two
/// small files DELETED — all in the manifest snapshot 2 wrote.
async fn build_parquet_fixture() -> Fixture {
    let store = Arc::new(InMemory::new());
    let small0 = format!("{ROOT}/data/category=a/f0.parquet");
    let small1 = format!("{ROOT}/data/category=a/f1.parquet");
    let compacted = format!("{ROOT}/data/category=a/compacted.parquet");
    let small0_etag = put(&store, &small0, support::parquet_flat(0)).await;
    put(&store, &small1, support::parquet_flat(1)).await;
    // The compacted file is larger (two appends' rows), same partition/columns.
    put(&store, &compacted, support::parquet_flat(9)).await;

    // Snapshot 1 manifest: both small files ADDED.
    let m1 = format!("{ROOT}/metadata/{SNAP_APPEND}-m0.avro");
    let m1_bytes = manifest(&[
        entry(1, &small0, FileFormat::Parquet, "a"),
        entry(1, &small1, FileFormat::Parquet, "a"),
    ]);
    put(&store, &m1, m1_bytes.clone()).await;
    let l1 = format!("{ROOT}/metadata/snap-{SNAP_APPEND}.avro");
    put(&store, &l1, manifest_list(&[(&m1, SNAP_APPEND)])).await;

    // Snapshot 2 manifest: compacted ADDED, both small files DELETED.
    let m2 = format!("{ROOT}/metadata/{SNAP_REPLACE}-m0.avro");
    let m2_bytes = manifest(&[
        entry(1, &compacted, FileFormat::Parquet, "a"),
        entry(2, &small0, FileFormat::Parquet, "a"),
        entry(2, &small1, FileFormat::Parquet, "a"),
    ]);
    put(&store, &m2, m2_bytes).await;
    let l2 = format!("{ROOT}/metadata/snap-{SNAP_REPLACE}.avro");
    put(&store, &l2, manifest_list(&[(&m2, SNAP_REPLACE)])).await;

    let metadata_key = format!("{ROOT}/metadata/v2.metadata.json");
    let md = metadata_json(&[
        (SNAP_APPEND, &l1, "append", None),
        (SNAP_REPLACE, &l2, "replace", Some(SNAP_APPEND)),
    ]);
    put(&store, &metadata_key, md.into_bytes()).await;

    Fixture {
        store,
        metadata_key,
        small: vec![small0, small1],
        compacted,
        small0_etag,
    }
}

/// PUTs bytes and returns the store-assigned ETag.
async fn put(store: &Arc<InMemory>, key: &str, bytes: Vec<u8>) -> String {
    store
        .put(&StorePath::from(key), PutPayload::from(bytes))
        .await
        .expect("put");
    store
        .head(&StorePath::from(key))
        .await
        .expect("head")
        .e_tag
        .unwrap_or_default()
}

/// Reads an object's whole body straight from the origin store — the ground
/// truth a cache read is checked against (#198).
async fn read_backend(store: &Arc<InMemory>, key: &str) -> Bytes {
    store
        .get(&StorePath::from(key))
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes")
}

/// The current wall-clock time in Unix milliseconds (for grace-window tests).
fn now_unix_ms_test() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The manifest-entry Avro schema (mapper-read fields plus status).
const MANIFEST_SCHEMA: &str = r#"{
  "type":"record","name":"manifest_entry","fields":[
    {"name":"status","type":"int"},
    {"name":"snapshot_id","type":"long"},
    {"name":"data_file","type":{"type":"record","name":"data_file","fields":[
      {"name":"file_path","type":"string"},
      {"name":"file_format","type":"string"},
      {"name":"file_size_in_bytes","type":"long"},
      {"name":"record_count","type":"long"},
      {"name":"partition","type":{"type":"map","values":"string"}}
    ]}}
  ]}"#;

/// One manifest entry with an explicit status (1=ADDED, 2=DELETED).
fn entry(status: i32, key: &str, format: FileFormat, category: &str) -> AvroValue {
    let mut partition = HashMap::new();
    partition.insert(
        "category".to_owned(),
        AvroValue::String(category.to_owned()),
    );
    let format_str = match format {
        FileFormat::Parquet => "PARQUET",
        FileFormat::Avro => "AVRO",
        FileFormat::Orc => "ORC",
    };
    let data_file = AvroValue::Record(vec![
        (
            "file_path".to_owned(),
            AvroValue::String(format!("s3://{BUCKET}/{key}")),
        ),
        (
            "file_format".to_owned(),
            AvroValue::String(format_str.to_owned()),
        ),
        ("file_size_in_bytes".to_owned(), AvroValue::Long(4096)),
        ("record_count".to_owned(), AvroValue::Long(0)),
        ("partition".to_owned(), AvroValue::Map(partition)),
    ]);
    AvroValue::Record(vec![
        ("status".to_owned(), AvroValue::Int(status)),
        ("snapshot_id".to_owned(), AvroValue::Long(0)),
        ("data_file".to_owned(), data_file),
    ])
}

/// Writes a manifest Avro from entries.
fn manifest(entries: &[AvroValue]) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(MANIFEST_SCHEMA).expect("schema");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for e in entries {
        writer.append(e.clone()).expect("append");
    }
    writer.into_inner().expect("finalize")
}

/// The manifest-list Avro schema (carries `added_snapshot_id`).
const LIST_SCHEMA: &str = r#"{
  "type":"record","name":"manifest_file","fields":[
    {"name":"manifest_path","type":"string"},
    {"name":"manifest_length","type":"long"},
    {"name":"partition_spec_id","type":"int"},
    {"name":"added_snapshot_id","type":"long"}
  ]}"#;

/// Writes a manifest-list Avro from `(manifest_key, added_snapshot_id)` pairs.
fn manifest_list(entries: &[(&str, i64)]) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(LIST_SCHEMA).expect("schema");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for (key, added) in entries {
        writer
            .append(AvroValue::Record(vec![
                (
                    "manifest_path".to_owned(),
                    AvroValue::String(format!("s3://{BUCKET}/{key}")),
                ),
                ("manifest_length".to_owned(), AvroValue::Long(0)),
                ("partition_spec_id".to_owned(), AvroValue::Int(0)),
                ("added_snapshot_id".to_owned(), AvroValue::Long(*added)),
            ]))
            .expect("append");
    }
    writer.into_inner().expect("finalize")
}

/// Builds a metadata.json naming the snapshots with their operation + parent.
fn metadata_json(snaps: &[(i64, &str, &str, Option<i64>)]) -> String {
    let snapshots: Vec<serde_json::Value> = snaps
        .iter()
        .map(|(id, list, op, parent)| {
            let mut s = serde_json::json!({
                "snapshot-id": id,
                "sequence-number": 1,
                "timestamp-ms": 0,
                "manifest-list": format!("s3://{BUCKET}/{list}"),
                "summary": {"operation": op},
                "schema-id": 0
            });
            if let Some(p) = parent {
                s["parent-snapshot-id"] = serde_json::json!(p);
            }
            s
        })
        .collect();
    serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000000",
        "location": format!("s3://{BUCKET}/{ROOT}"),
        "current-snapshot-id": snaps.last().map(|s| s.0).unwrap_or(0),
        "properties": {"history.expire.max-snapshot-age-ms": "3600000"},
        "snapshots": snapshots
    })
    .to_string()
}

// ---- Engine harness (mirrors the warming acceptance harness) ----------------

#[derive(Clone, Default)]
struct BackendCalls {
    gets: Arc<AtomicU64>,
}

struct CountingRead<R> {
    inner: R,
    calls: BackendCalls,
}

impl<R: ObjectRead> ObjectRead for CountingRead<R> {
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.calls.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.inner.head(key).await
    }
}

type Engine = HybridCacheEngine<
    CountingRead<PassthroughRead>,
    verglas_core::peer::NoopPeerFetch,
    verglas_core::ring::RendezvousRing,
>;

/// Builds a counting cache engine over `store`.
async fn build_engine(store: Arc<InMemory>, dir: &TempDir) -> (Arc<Engine>, BackendCalls) {
    let calls = BackendCalls::default();
    let backend = CountingRead {
        inner: PassthroughRead::new(BackendStore::single("managed-lakehouse", BUCKET, store)),
        calls: calls.clone(),
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
            .expect("engine"),
    );
    (engine, calls)
}

/// Reads a range fully through the engine.
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
fn key(k: &str) -> CacheKey {
    CacheKey {
        storage_binding_id: "managed-lakehouse".to_owned(),
        bucket: BUCKET.to_owned(),
        key: k.to_owned(),
    }
}

/// Builds a mapper indexed at the append snapshot (heat is earned pre-compaction).
async fn mapper_at_append(fetch: &dyn MetadataFetch, metadata_key: &str) -> Mapper {
    let mapper = Mapper::new();
    mapper
        .apply_table(
            fetch,
            &BuildInputs {
                storage_binding_id: "managed-lakehouse".to_owned(),
                ident: verglas_tables::catalog::TableIdent::new(&["db"], "events"),
                bucket: BUCKET.to_owned(),
                metadata_key: metadata_key.to_owned(),
                snapshot_ids: vec![SNAP_APPEND],
                current_snapshot_id: Some(SNAP_APPEND),
            },
        )
        .await
        .expect("build mapper");
    mapper
}

// ---- Tests ------------------------------------------------------------------

/// A `replace` commit is classified as compaction and diffed to the added
/// (compacted) file and removed (rewritten) files, from the changed manifest.
#[tokio::test]
async fn replace_commit_diffs_added_and_removed() {
    let fx = build_parquet_fixture().await;
    let fetch = ObjectStoreFetch::new(fx.store.clone() as Arc<dyn ObjectStore>);
    let diff = diff_snapshot(
        &fetch,
        "managed-lakehouse",
        BUCKET,
        &fx.metadata_key,
        SNAP_REPLACE,
    )
    .await
    .expect("diff");
    assert_eq!(diff.op, SnapshotOp::Replace);
    assert!(diff.op.is_compaction());
    let added: Vec<&str> = diff.added.iter().map(|f| f.path.as_str()).collect();
    let removed: Vec<&str> = diff.removed.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(added, vec![fx.compacted.as_str()]);
    assert_eq!(removed.len(), 2);
    assert!(removed.contains(&fx.small[0].as_str()));
    assert!(removed.contains(&fx.small[1].as_str()));
}

/// An `append` commit prefetches no data (its added files are cold by
/// definition). The plan is empty.
#[tokio::test]
async fn append_commit_prefetches_no_data() {
    let fx = build_parquet_fixture().await;
    let fetch = ObjectStoreFetch::new(fx.store.clone() as Arc<dyn ObjectStore>);
    let diff = diff_snapshot(
        &fetch,
        "managed-lakehouse",
        BUCKET,
        &fx.metadata_key,
        SNAP_APPEND,
    )
    .await
    .expect("diff");
    assert_eq!(diff.op, SnapshotOp::Append);
    let ledger = HeatLedger::with_defaults();
    let plan = plan_prefetch(
        &fetch,
        &ledger,
        verglas_tables::mapper::TableId(0),
        "managed-lakehouse",
        BUCKET,
        &diff,
        &PrefetchConfig::default(),
        0,
    )
    .await;
    assert!(plan.is_empty(), "append must not prefetch data");
}

/// Heat carries logically: after recording heat on a hot column of a small file,
/// the compaction plan schedules that same column's chunks in the rewritten file
/// — before any access to the new file — and not the cold columns.
#[tokio::test]
async fn hot_column_carries_into_compacted_file() {
    let fx = build_parquet_fixture().await;
    let fetch = ObjectStoreFetch::new(fx.store.clone() as Arc<dyn ObjectStore>);
    let mapper = mapper_at_append(&fetch, &fx.metadata_key).await;
    let table = mapper
        .table_id(&verglas_tables::catalog::TableIdent::new(&["db"], "events"))
        .expect("table id");

    // Parse the small file's footer so its column ranges resolve, then record
    // heat for column 0 (the hot column) via the request-path resolver.
    mapper
        .warm_footer(&fetch, table, &fx.small[0], &fx.small0_etag)
        .await
        .expect("footer");
    let col0 = column_range(&fetch, &fx.small[0], ColumnId(0)).await;
    let sample = HeatSample {
        bucket: BUCKET.to_owned(),
        key: fx.small[0].clone(),
        range: col0.clone(),
    };
    let hot_key = resolve_key(&mapper, &sample).expect("resolves to a data key");
    assert_eq!(hot_key.column, ColumnId(0));
    let ledger = HeatLedger::with_defaults();
    for _ in 0..20 {
        ledger.record(hot_key, col0.end - col0.start, 0);
    }

    // Plan the compaction. The hot column carried by (partition, column) must be
    // scheduled in the compacted file; cold columns must not.
    let diff = diff_snapshot(
        &fetch,
        "managed-lakehouse",
        BUCKET,
        &fx.metadata_key,
        SNAP_REPLACE,
    )
    .await
    .expect("diff");
    let plan = plan_prefetch(
        &fetch,
        &ledger,
        table,
        "managed-lakehouse",
        BUCKET,
        &diff,
        &PrefetchConfig::default(),
        0,
    )
    .await;

    assert!(
        !plan.footers.is_empty(),
        "the compacted file's footer is always prefetched"
    );
    assert!(
        plan.chunks.iter().all(|c| c.key == fx.compacted),
        "chunk prefetch targets the compacted (added) file only"
    );
    assert!(
        !plan.chunks.is_empty(),
        "the hot column must schedule chunks in the compacted file before first access"
    );
    // The hottest scheduled chunk maps to the hot partition's heat (> 0).
    assert!(plan.chunks.iter().all(|c| c.score > 0.0));
    // A cold-only ledger schedules no chunks — proving the heat gate works.
    let cold = HeatLedger::with_defaults();
    let cold_plan = plan_prefetch(
        &fetch,
        &cold,
        table,
        "managed-lakehouse",
        BUCKET,
        &diff,
        &PrefetchConfig::default(),
        0,
    )
    .await;
    assert!(cold_plan.chunks.is_empty(), "no heat, no chunk prefetch");
}

/// The prefetch executor pulls the compacted file's hot chunk into cache, so a
/// client's first read of it is a cache hit (zero backend GETs) — warmth carried
/// across the rewrite before first access.
#[tokio::test]
async fn executor_prefetches_hot_chunk_before_first_access() {
    let fx = build_parquet_fixture().await;
    let store_fetch = ObjectStoreFetch::new(fx.store.clone() as Arc<dyn ObjectStore>);
    let mapper = mapper_at_append(&store_fetch, &fx.metadata_key).await;
    let table = mapper
        .table_id(&verglas_tables::catalog::TableIdent::new(&["db"], "events"))
        .expect("table id");
    mapper
        .warm_footer(&store_fetch, table, &fx.small[0], &fx.small0_etag)
        .await
        .expect("footer");

    // Heat: column 0 of the small file is hot.
    let col0 = column_range(&store_fetch, &fx.small[0], ColumnId(0)).await;
    let hot_key = resolve_key(
        &mapper,
        &HeatSample {
            bucket: BUCKET.to_owned(),
            key: fx.small[0].clone(),
            range: col0.clone(),
        },
    )
    .expect("resolves");
    let ledger = HeatLedger::with_defaults();
    for _ in 0..20 {
        ledger.record(hot_key, 4096, 0);
    }

    let dir = TempDir::new().expect("dir");
    let (engine, calls) = build_engine(fx.store.clone(), &dir).await;

    // Plan against the store (direct) and drain through the engine executor.
    let diff = diff_snapshot(
        &store_fetch,
        "managed-lakehouse",
        BUCKET,
        &fx.metadata_key,
        SNAP_REPLACE,
    )
    .await
    .expect("diff");
    let plan = plan_prefetch(
        &store_fetch,
        &ledger,
        table,
        "managed-lakehouse",
        BUCKET,
        &diff,
        &PrefetchConfig::default(),
        0,
    )
    .await;
    // The compacted file's hot column 0 range, as the client will read it.
    let compacted_col0 = column_range(&store_fetch, &fx.compacted, ColumnId(0)).await;

    let budget = Arc::new(verglas_tables::warming::budget::TokenBucket::unlimited());
    let executor = PrefetchExecutor::spawn(engine.clone(), budget, &PrefetchConfig::default());
    executor.submit_plan(PrefetchPriority::CompactionRepair, &plan);

    // Wait for the plan to drain.
    wait_until(|| executor.metrics().completed.load(Ordering::Relaxed) >= plan.len() as u64).await;

    // First client read of the compacted file's hot chunk: served from cache.
    let gets_before = calls.gets.load(Ordering::SeqCst);
    let _ = read_all(
        &engine,
        &key(&fx.compacted),
        ReadRange::Bounded(compacted_col0.start, compacted_col0.end - 1),
    )
    .await;
    assert_eq!(
        calls.gets.load(Ordering::SeqCst) - gets_before,
        0,
        "the prefetched hot chunk should serve from cache with no backend GET"
    );
    assert!(
        executor
            .metrics()
            .bytes_compaction_repair
            .load(Ordering::Relaxed)
            > 0,
        "compaction-repair bytes recorded"
    );
}

/// The request-path feed resolves and folds a data read into the ledger through
/// the drop-on-full channel and background aggregator (end-to-end feed path).
#[tokio::test]
async fn heat_feed_channel_folds_a_read_into_the_ledger() {
    let fx = build_parquet_fixture().await;
    let store_fetch = ObjectStoreFetch::new(fx.store.clone() as Arc<dyn ObjectStore>);
    let mapper = Arc::new(Mapper::new());
    mapper
        .apply_table(
            &store_fetch,
            &BuildInputs {
                storage_binding_id: "managed-lakehouse".to_owned(),
                ident: verglas_tables::catalog::TableIdent::new(&["db"], "events"),
                bucket: BUCKET.to_owned(),
                metadata_key: fx.metadata_key.clone(),
                snapshot_ids: vec![SNAP_APPEND],
                current_snapshot_id: Some(SNAP_APPEND),
            },
        )
        .await
        .expect("mapper");
    let table = mapper
        .table_id(&verglas_tables::catalog::TableIdent::new(&["db"], "events"))
        .expect("id");
    mapper
        .warm_footer(&store_fetch, table, &fx.small[0], &fx.small0_etag)
        .await
        .expect("footer");

    let dir = TempDir::new().expect("dir");
    let (engine, _calls) = build_engine(fx.store.clone(), &dir).await;

    let mut channel = HeatChannel::new(1024);
    let sender = channel.sender();
    let rx = channel.take_receiver().expect("receiver");
    // Drop the channel so the feed decorator holds the only remaining sender —
    // dropping it later is what ends the aggregator.
    drop(channel);
    let ledger = Arc::new(HeatLedger::with_defaults());
    let clock = EpochClock::new(300);
    let agg = tokio::spawn(run_aggregator(rx, mapper.clone(), ledger.clone(), clock));

    // Read the small file's column 0 through the feed decorator.
    let feed = HeatFeed::new(engine.clone(), sender);
    let col0 = column_range(&store_fetch, &fx.small[0], ColumnId(0)).await;
    let got = feed
        .get(
            &key(&fx.small[0]),
            ReadRange::Bounded(col0.start, col0.end - 1),
        )
        .await
        .expect("read");
    drop(got.body);

    // The aggregator folds it: the (partition, column) heat becomes positive.
    let hot_key = resolve_key(
        &mapper,
        &HeatSample {
            bucket: BUCKET.to_owned(),
            key: fx.small[0].clone(),
            range: col0.clone(),
        },
    )
    .expect("resolves");
    wait_until(|| ledger.score(&hot_key, clock.now()) > 0.0).await;
    assert!(ledger.score(&hot_key, clock.now()) > 0.0);
    drop(feed); // drops the last sender, ending the aggregator
    let _ = agg.await;
}

/// A read served through the telemetry-wired feed decorator attributes to the
/// RIGHT table's per-table family (#60): the served bytes land on `db.events`,
/// never on `_unmapped`, and a cache-tier serve counts as a request avoided.
#[tokio::test]
async fn telemetry_feed_attributes_a_served_read_to_its_table() {
    use verglas_core::telemetry::Telemetry;

    let fx = build_parquet_fixture().await;
    let store_fetch = ObjectStoreFetch::new(fx.store.clone() as Arc<dyn ObjectStore>);
    let mapper = Arc::new(Mapper::new());
    mapper
        .apply_table(
            &store_fetch,
            &BuildInputs {
                storage_binding_id: "managed-lakehouse".to_owned(),
                ident: verglas_tables::catalog::TableIdent::new(&["db"], "events"),
                bucket: BUCKET.to_owned(),
                metadata_key: fx.metadata_key.clone(),
                snapshot_ids: vec![SNAP_APPEND],
                current_snapshot_id: Some(SNAP_APPEND),
            },
        )
        .await
        .expect("mapper");
    let table = mapper
        .table_id(&verglas_tables::catalog::TableIdent::new(&["db"], "events"))
        .expect("id");
    mapper
        .warm_footer(&store_fetch, table, &fx.small[0], &fx.small0_etag)
        .await
        .expect("footer");

    let dir = TempDir::new().expect("dir");
    let (engine, _calls) = build_engine(fx.store.clone(), &dir).await;

    // The serving decorator with telemetry + mapper wired (no heat feed / yield
    // gate needed for this test).
    let telemetry = Arc::new(Telemetry::with_shards(
        2,
        verglas_core::telemetry::Rollup::new(),
    ));
    let feed = HeatFeed::with_options(
        engine.clone(),
        None,
        None,
        Some(telemetry.clone()),
        Some(mapper.clone()),
    );
    let col0 = column_range(&store_fetch, &fx.small[0], ColumnId(0)).await;

    // Read the same range twice: the first warms the cache (a backend fill /
    // miss), the second is served from a cache tier (a hit → a request avoided).
    for _ in 0..2 {
        let got = feed
            .get(
                &key(&fx.small[0]),
                ReadRange::Bounded(col0.start, col0.end - 1),
            )
            .await
            .expect("read");
        // Drain the body so the tier is resolved and the event records.
        let _ = futures::TryStreamExt::try_collect::<Vec<_>>(got.body).await;
    }

    telemetry.roll_up();
    let report = telemetry.table_report(|id| {
        (id == verglas_core::telemetry::encode_table_id(table.0)).then(|| "db.events".to_owned())
    });

    // Exactly one real table row, and it is db.events — never _unmapped.
    let events = report
        .tables
        .iter()
        .find(|t| t.table == "db.events")
        .expect("db.events row present");
    assert_eq!(
        events.hits + events.misses,
        2,
        "both reads attribute to db.events"
    );
    assert!(
        report.tables.iter().all(|t| t.table != "_unmapped"),
        "a watched-table read must not land on _unmapped"
    );
    // The second read hit a cache tier, so at least one request was avoided, and
    // requests_avoided tracks hits exactly.
    assert!(events.hits >= 1, "the repeat read is a cache hit");
    assert_eq!(events.requests_avoided, events.hits);
}

/// Retirement demotes the removed files with the grace window read from table
/// properties, and they remain demoted until the window closes.
#[tokio::test]
async fn retirement_demotes_removed_with_grace() {
    let fx = build_parquet_fixture().await;
    let fetch = ObjectStoreFetch::new(fx.store.clone() as Arc<dyn ObjectStore>);
    let diff = diff_snapshot(
        &fetch,
        "managed-lakehouse",
        BUCKET,
        &fx.metadata_key,
        SNAP_REPLACE,
    )
    .await
    .expect("diff");
    // Grace from the fixture's property (1 hour), not the 7-day default.
    let props = verglas_tables::lifecycle::retire::parse_table_properties(
        &fetch_bytes(&fetch, &fx.metadata_key).await,
    );
    let grace = verglas_tables::lifecycle::retire::grace_ms_from_properties(&props);
    assert_eq!(grace, 3_600_000);
    assert_ne!(grace, DEFAULT_GRACE_MS);

    let sched = RetirementScheduler::new();
    let demotions = verglas_tables::lifecycle::retire::demotions_for_commit(
        "managed-lakehouse",
        BUCKET,
        &diff.removed,
        &[],
        grace,
        0,
    );
    sched.record(&demotions);
    assert_eq!(demotions.len(), 2);
    assert!(sched.is_demoted("managed-lakehouse", BUCKET, &fx.small[0]));
    // Within grace nothing hard-evicts; after it, both drain.
    assert!(sched.drain_expired(grace - 1).is_empty());
    assert_eq!(sched.drain_expired(grace).len(), 2);
}

// ---- Avro variant (the addendum) --------------------------------------------

/// A compaction whose added file is Avro carries heat by partition to a
/// whole-file prefetch (no footer, sequential shape) — the Avro addendum.
#[tokio::test]
async fn avro_compaction_carries_partition_heat_to_whole_file() {
    let store = Arc::new(InMemory::new());
    let old = format!("{ROOT}/data/category=a/rows-old.avro");
    let compacted = format!("{ROOT}/data/category=a/rows-compacted.avro");
    put(&store, &old, support::avro_file(0)).await;
    put(&store, &compacted, support::avro_file(9)).await;

    let m2 = format!("{ROOT}/metadata/2002-m0.avro");
    put(
        &store,
        &m2,
        manifest(&[
            entry(1, &compacted, FileFormat::Avro, "a"),
            entry(2, &old, FileFormat::Avro, "a"),
        ]),
    )
    .await;
    let l2 = format!("{ROOT}/metadata/snap-2002.avro");
    put(&store, &l2, manifest_list(&[(&m2, 2002)])).await;
    let metadata_key = format!("{ROOT}/metadata/v2.metadata.json");
    put(
        &store,
        &metadata_key,
        metadata_json(&[(2002, &l2, "replace", None)]).into_bytes(),
    )
    .await;

    let fetch = ObjectStoreFetch::new(store.clone() as Arc<dyn ObjectStore>);
    let diff = diff_snapshot(&fetch, "managed-lakehouse", BUCKET, &metadata_key, 2002)
        .await
        .expect("diff");
    assert_eq!(diff.op, SnapshotOp::Replace);

    // Heat on the partition (WHOLE_FILE column, since Avro has no columns).
    let ledger = HeatLedger::with_defaults();
    let table = verglas_tables::mapper::TableId(0);
    let part = PartitionHash::of(&[("category".to_owned(), Some("a".to_owned()))]);
    for _ in 0..20 {
        ledger.record(
            verglas_tables::lifecycle::heat::HeatKey {
                table,
                partition: part,
                column: ColumnId::WHOLE_FILE,
            },
            8192,
            0,
        );
    }
    let plan = plan_prefetch(
        &fetch,
        &ledger,
        table,
        "managed-lakehouse",
        BUCKET,
        &diff,
        &PrefetchConfig::default(),
        0,
    )
    .await;
    assert!(plan.footers.is_empty(), "Avro has no footer to prefetch");
    assert_eq!(
        plan.whole_files.len(),
        1,
        "the hot Avro file is prefetched whole"
    );
    assert_eq!(plan.whole_files[0].key, compacted);
    assert!(matches!(plan.whole_files[0].range, ReadRange::Full));
}

// ---- Helpers ----------------------------------------------------------------

/// The absolute byte range of the first chunk of `column` in `key`'s footer.
async fn column_range(
    fetch: &dyn MetadataFetch,
    key: &str,
    column: ColumnId,
) -> std::ops::Range<u64> {
    let path = CacheKey {
        storage_binding_id: "managed-lakehouse".to_owned(),
        bucket: BUCKET.to_owned(),
        key: key.to_owned(),
    };
    let spans = verglas_tables::parquet_meta::parse_footer(fetch, &path)
        .await
        .expect("footer");
    let span = spans
        .iter()
        .find(|s| s.column == column)
        .expect("column present");
    span.start..span.end
}

/// Reads a whole small object through the fetch interface.
async fn fetch_bytes(fetch: &dyn MetadataFetch, key: &str) -> Bytes {
    let path = CacheKey {
        storage_binding_id: "managed-lakehouse".to_owned(),
        bucket: BUCKET.to_owned(),
        key: key.to_owned(),
    };
    fetch.fetch_suffix(&path, 1024 * 1024).await.expect("bytes")
}

/// Polls `cond` until true or a bounded number of yields elapse.
async fn wait_until(cond: impl Fn() -> bool) {
    for _ in 0..2000 {
        if cond() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    panic!("condition not met within the deadline");
}

// ---- Watcher-driven coordinator (server wiring) -----------------------------

/// A hand-driven watcher: state set directly, events pushed on demand.
struct FakeWatcher {
    state: std::sync::Mutex<HashMap<TableIdent, TableState>>,
    tx: tokio::sync::broadcast::Sender<TableChanged>,
}

impl FakeWatcher {
    fn new() -> Arc<FakeWatcher> {
        let (tx, _) = tokio::sync::broadcast::channel(64);
        Arc::new(FakeWatcher {
            state: std::sync::Mutex::new(HashMap::new()),
            tx,
        })
    }
    fn set(&self, table: TableIdent, location: String, snapshot: Option<i64>) {
        self.state.lock().expect("lock").insert(
            table,
            TableState {
                metadata_location: location,
                current_snapshot_id: snapshot,
            },
        );
    }
}

impl CatalogWatcher for FakeWatcher {
    fn watched_tables(&self) -> Vec<TableIdent> {
        self.state.lock().expect("lock").keys().cloned().collect()
    }
    fn table_state(&self, table: &TableIdent) -> Option<TableState> {
        self.state.lock().expect("lock").get(table).cloned()
    }
    fn lineage(&self, _table: &TableIdent) -> Vec<SnapshotEntry> {
        vec![]
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<TableChanged> {
        self.tx.subscribe()
    }
    fn seeded(&self) -> tokio::sync::watch::Receiver<bool> {
        let (tx, rx) = tokio::sync::watch::channel(true);
        drop(tx);
        rx
    }
}

/// The coordinator handles a compaction commit end-to-end: it submits a repair
/// plan (so the compacted file's hot chunk serves warm) and demotes the removed
/// files with the table's grace window.
#[tokio::test]
async fn coordinator_repairs_and_retires_on_replace_commit() {
    let fx = build_parquet_fixture().await;
    let store_fetch = Arc::new(ObjectStoreFetch::new(
        fx.store.clone() as Arc<dyn ObjectStore>
    ));
    let mapper = Arc::new(mapper_at_append(store_fetch.as_ref(), &fx.metadata_key).await);
    let table = mapper
        .table_id(&TableIdent::new(&["db"], "events"))
        .expect("table id");
    mapper
        .warm_footer(store_fetch.as_ref(), table, &fx.small[0], &fx.small0_etag)
        .await
        .expect("footer");

    // Column 0 is hot before compaction.
    let col0 = column_range(store_fetch.as_ref(), &fx.small[0], ColumnId(0)).await;
    let hot_key = resolve_key(
        &mapper,
        &HeatSample {
            bucket: BUCKET.to_owned(),
            key: fx.small[0].clone(),
            range: col0.clone(),
        },
    )
    .expect("resolves");
    let ledger = Arc::new(HeatLedger::with_defaults());
    for _ in 0..20 {
        ledger.record(hot_key, 4096, 0);
    }

    let dir = TempDir::new().expect("dir");
    let (engine, calls) = build_engine(fx.store.clone(), &dir).await;
    let budget = Arc::new(verglas_tables::warming::budget::TokenBucket::unlimited());
    let executor = Arc::new(PrefetchExecutor::spawn(
        engine.clone(),
        budget,
        &PrefetchConfig::default(),
    ));
    let retire = Arc::new(RetirementScheduler::new());

    let watcher = FakeWatcher::new();
    let ident = TableIdent::new(&["db"], "events");
    watcher.set(
        ident.clone(),
        format!("s3://{BUCKET}/{}", fx.metadata_key),
        Some(SNAP_REPLACE),
    );

    let coord = PrefetchCoordinator::new(
        "managed-lakehouse",
        watcher,
        store_fetch.clone(),
        mapper.clone(),
        ledger.clone(),
        executor.clone(),
        retire.clone(),
        None,
        None,
        PrefetchConfig::default(),
        EpochClock::new(300),
    );

    coord
        .on_change(&TableChanged {
            table: ident,
            old_snapshot: Some(SNAP_APPEND),
            new_snapshot: Some(SNAP_REPLACE),
        })
        .await;

    // Retirement demoted both removed files with the fixture's 1-hour grace.
    assert_eq!(retire.demoted_count(), 2);
    assert!(retire.is_demoted("managed-lakehouse", BUCKET, &fx.small[0]));

    // The repair plan drains: the compacted file's hot chunk then serves warm.
    wait_until(|| executor.metrics().completed.load(Ordering::Relaxed) >= 1).await;
    let compacted_col0 = column_range(store_fetch.as_ref(), &fx.compacted, ColumnId(0)).await;
    // Give the executor a moment to finish the chunk fills, then read warm.
    wait_until(|| {
        executor
            .metrics()
            .bytes_compaction_repair
            .load(Ordering::Relaxed)
            > 0
    })
    .await;
    // Drain any in-flight fills for the hot chunk.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let gets_before = calls.gets.load(Ordering::SeqCst);
    let _ = read_all(
        &engine,
        &key(&fx.compacted),
        ReadRange::Bounded(compacted_col0.start, compacted_col0.end - 1),
    )
    .await;
    assert_eq!(
        calls.gets.load(Ordering::SeqCst) - gets_before,
        0,
        "compacted hot chunk should be prefetched warm by the coordinator"
    );
}

/// End-to-end evict-first retirement (#198): on a compaction commit the
/// coordinator demotes the removed files in the cache engine, so their warm
/// blocks become unreachable misses (a re-read refills from the backend) while a
/// live file stays warm. A time-travel read within the grace window still
/// resolves and serves correct bytes. The grace sweep hard-evicts the removed
/// files once their deadline passes.
#[tokio::test]
async fn coordinator_demotes_removed_files_in_the_engine() {
    let fx = build_parquet_fixture().await;
    let store_fetch = Arc::new(ObjectStoreFetch::new(
        fx.store.clone() as Arc<dyn ObjectStore>
    ));
    let mapper = Arc::new(mapper_at_append(store_fetch.as_ref(), &fx.metadata_key).await);
    let ledger = Arc::new(HeatLedger::with_defaults());

    let dir = TempDir::new().expect("dir");
    let (engine, calls) = build_engine(fx.store.clone(), &dir).await;

    // Warm both removed files and the (live) compacted file through the cache.
    let removed0 = key(&fx.small[0]);
    let removed1 = key(&fx.small[1]);
    let live = key(&fx.compacted);
    for k in [&removed0, &removed1, &live] {
        read_all(&engine, k, ReadRange::Full).await;
    }
    // They are warm: a re-read costs no backend GET.
    let gets_after_warm = calls.gets.load(Ordering::SeqCst);
    read_all(&engine, &removed0, ReadRange::Full).await;
    assert_eq!(
        calls.gets.load(Ordering::SeqCst),
        gets_after_warm,
        "a warm removed file must not refill before demotion"
    );

    let budget = Arc::new(verglas_tables::warming::budget::TokenBucket::unlimited());
    let executor = Arc::new(PrefetchExecutor::spawn(
        engine.clone(),
        budget,
        &PrefetchConfig::default(),
    ));
    let retire = Arc::new(RetirementScheduler::new());

    let watcher = FakeWatcher::new();
    let ident = TableIdent::new(&["db"], "events");
    watcher.set(
        ident.clone(),
        format!("s3://{BUCKET}/{}", fx.metadata_key),
        Some(SNAP_REPLACE),
    );

    // The coordinator holds the engine as its demotion sink.
    let coord = PrefetchCoordinator::new(
        "managed-lakehouse",
        watcher,
        store_fetch.clone(),
        mapper.clone(),
        ledger.clone(),
        executor.clone(),
        retire.clone(),
        Some(engine.clone() as Arc<dyn verglas_cache::BlockDemoter>),
        None,
        PrefetchConfig::default(),
        EpochClock::new(300),
    );

    coord
        .on_change(&TableChanged {
            table: ident,
            old_snapshot: Some(SNAP_APPEND),
            new_snapshot: Some(SNAP_REPLACE),
        })
        .await;

    // The engine demoted both removed files.
    assert!(engine.is_demoted(&removed0));
    assert!(engine.is_demoted(&removed1));
    assert_eq!(engine.demoted_count(), 2);

    // A re-read of a removed file now refills from the backend (its warm blocks
    // are unreachable), and still serves correct bytes — time travel within the
    // grace window resolves and reads, just cold.
    let before = calls.gets.load(Ordering::SeqCst);
    let got = read_all(&engine, &removed0, ReadRange::Full).await;
    assert!(
        calls.gets.load(Ordering::SeqCst) > before,
        "a demoted removed file must refill from the backend"
    );
    assert_eq!(
        got,
        read_backend(&fx.store, &fx.small[0]).await,
        "a time-travel read within grace still serves correct bytes"
    );

    // The live compacted file is untouched: still a warm hit.
    let before_live = calls.gets.load(Ordering::SeqCst);
    read_all(&engine, &live, ReadRange::Full).await;
    assert_eq!(
        calls.gets.load(Ordering::SeqCst),
        before_live,
        "demoting the removed files must not evict the live compacted file"
    );

    // The grace sweep past the deadline hard-evicts the removed files: the
    // scheduler drains them and the engine clears their demotion.
    engine.flush().await;
    let past = now_unix_ms_test() + DEFAULT_GRACE_MS + 1;
    let swept = coord.sweep_expired(past);
    assert_eq!(swept, 2, "both removed files are swept once grace closes");
    assert!(!engine.is_demoted(&removed0));
    assert!(!engine.is_demoted(&removed1));
    assert_eq!(engine.demoted_count(), 0);
    assert_eq!(retire.demoted_count(), 0);

    // #305: the sweep physically removed the dead blocks. A post-sweep re-read
    // must refill from the backend — before the fix, the sweep only cleared
    // the demotion marker and the stale pre-demotion blocks served warm again.
    let before_swept = calls.gets.load(Ordering::SeqCst);
    read_all(&engine, &removed0, ReadRange::Full).await;
    assert!(
        calls.gets.load(Ordering::SeqCst) > before_swept,
        "a swept file's blocks are physically gone; the re-read refills"
    );
    let counters = engine.counters().snapshot();
    assert!(
        counters.retired_bytes_reclaimed > 0,
        "the sweep accounts the bytes it physically reclaimed"
    );
    assert_eq!(
        counters.retired_bytes_pending, 0,
        "nothing remains pending once the sweep drains the schedule"
    );
}

// ---- Hit-rate recovery benchmark (hermetic, the headline number) ------------

/// Builds an N-partition table where each partition has two small Parquet files
/// (snapshot 1, append) that a compaction (snapshot 2, replace) rewrites into one
/// file. Returns `(metadata_key, per-partition (small0, small1, compacted))`.
async fn build_multi_partition_fixture(
    store: &Arc<InMemory>,
    partitions: usize,
) -> (String, Vec<(String, String, String)>) {
    let mut files = Vec::new();
    let mut m1_entries = Vec::new();
    let mut m2_entries = Vec::new();
    for i in 0..partitions {
        let cat = format!("p{i}");
        let s0 = format!("{ROOT}/data/category={cat}/f0.parquet");
        let s1 = format!("{ROOT}/data/category={cat}/f1.parquet");
        let comp = format!("{ROOT}/data/category={cat}/compacted.parquet");
        put(store, &s0, support::parquet_flat(i as i64)).await;
        put(store, &s1, support::parquet_flat((i + 100) as i64)).await;
        put(store, &comp, support::parquet_flat((i + 900) as i64)).await;
        m1_entries.push(entry(1, &s0, FileFormat::Parquet, &cat));
        m1_entries.push(entry(1, &s1, FileFormat::Parquet, &cat));
        m2_entries.push(entry(1, &comp, FileFormat::Parquet, &cat));
        m2_entries.push(entry(2, &s0, FileFormat::Parquet, &cat));
        m2_entries.push(entry(2, &s1, FileFormat::Parquet, &cat));
        files.push((s0, s1, comp));
    }
    let m1 = format!("{ROOT}/metadata/{SNAP_APPEND}-m0.avro");
    put(store, &m1, manifest(&m1_entries)).await;
    let l1 = format!("{ROOT}/metadata/snap-{SNAP_APPEND}.avro");
    put(store, &l1, manifest_list(&[(&m1, SNAP_APPEND)])).await;
    let m2 = format!("{ROOT}/metadata/{SNAP_REPLACE}-m0.avro");
    put(store, &m2, manifest(&m2_entries)).await;
    let l2 = format!("{ROOT}/metadata/snap-{SNAP_REPLACE}.avro");
    put(store, &l2, manifest_list(&[(&m2, SNAP_REPLACE)])).await;
    let metadata_key = format!("{ROOT}/metadata/v2.metadata.json");
    put(
        store,
        &metadata_key,
        metadata_json(&[
            (SNAP_APPEND, &l1, "append", None),
            (SNAP_REPLACE, &l2, "replace", Some(SNAP_APPEND)),
        ])
        .into_bytes(),
    )
    .await;
    (metadata_key, files)
}

/// Measures the cache hit rate of one post-compaction query wave: for each
/// partition, read the compacted file's hot column 0 and count how many served
/// with zero backend GETs.
async fn post_compaction_hit_rate(
    engine: &Engine,
    calls: &BackendCalls,
    store_fetch: &dyn MetadataFetch,
    files: &[(String, String, String)],
) -> f64 {
    let mut hits = 0usize;
    for (_, _, comp) in files {
        let col0 = column_range(store_fetch, comp, ColumnId(0)).await;
        let before = calls.gets.load(Ordering::SeqCst);
        let _ = read_all(
            engine,
            &key(comp),
            ReadRange::Bounded(col0.start, col0.end - 1),
        )
        .await;
        if calls.gets.load(Ordering::SeqCst) == before {
            hits += 1;
        }
    }
    hits as f64 / files.len() as f64
}

/// THE BENCHMARK (hermetic): steady query load hot-loads column 0 of every
/// partition; a compaction rewrites every file. With prefetch ON the compacted
/// files' hot column is repaired into cache before the next wave (near-100% hit
/// rate immediately); with prefetch OFF the first wave cold-misses (~0%) and
/// only re-earns its hit rate query by query. Prints the comparison and asserts
/// the recovery claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn benchmark_hit_rate_recovery_prefetch_on_vs_off() {
    const PARTITIONS: usize = 12;

    // Run one configuration (prefetch on/off) end to end and return the first
    // post-compaction wave's hit rate.
    async fn run(prefetch_on: bool) -> f64 {
        let store = Arc::new(InMemory::new());
        let (metadata_key, files) = build_multi_partition_fixture(&store, PARTITIONS).await;
        let store_fetch = Arc::new(ObjectStoreFetch::new(store.clone() as Arc<dyn ObjectStore>));

        // Map at the append snapshot; parse footers so heat resolves columns.
        let mapper = Arc::new(mapper_at_append(store_fetch.as_ref(), &metadata_key).await);
        let table = mapper
            .table_id(&TableIdent::new(&["db"], "events"))
            .expect("id");

        // Steady query load: read column 0 of every small file, recording heat.
        let ledger = Arc::new(HeatLedger::with_defaults());
        for (s0, s1, _) in &files {
            for f in [s0, s1] {
                mapper
                    .warm_footer(store_fetch.as_ref(), table, f, "etag")
                    .await
                    .ok();
                let col0 = column_range(store_fetch.as_ref(), f, ColumnId(0)).await;
                if let Some(hkey) = resolve_key(
                    &mapper,
                    &HeatSample {
                        bucket: BUCKET.to_owned(),
                        key: f.clone(),
                        range: col0.clone(),
                    },
                ) {
                    for _ in 0..10 {
                        ledger.record(hkey, col0.end - col0.start, 0);
                    }
                }
            }
        }

        // The engine the post-compaction wave is served from.
        let dir = TempDir::new().expect("dir");
        let (engine, calls) = build_engine(store.clone(), &dir).await;

        if prefetch_on {
            // Repair: plan + drain the compaction prefetch through the engine.
            let diff = diff_snapshot(
                store_fetch.as_ref(),
                "managed-lakehouse",
                BUCKET,
                &metadata_key,
                SNAP_REPLACE,
            )
            .await
            .expect("diff");
            let plan = plan_prefetch(
                store_fetch.as_ref(),
                &ledger,
                table,
                "managed-lakehouse",
                BUCKET,
                &diff,
                &PrefetchConfig::default(),
                0,
            )
            .await;
            let budget = Arc::new(verglas_tables::warming::budget::TokenBucket::unlimited());
            let executor =
                PrefetchExecutor::spawn(engine.clone(), budget, &PrefetchConfig::default());
            executor.submit_plan(PrefetchPriority::CompactionRepair, &plan);
            wait_until(|| {
                executor.metrics().completed.load(Ordering::Relaxed) >= plan.len() as u64
            })
            .await;
        }

        post_compaction_hit_rate(&engine, &calls, store_fetch.as_ref(), &files).await
    }

    let off = run(false).await;
    let on = run(true).await;

    eprintln!("\n=== #51 hit-rate recovery (hermetic, {PARTITIONS} partitions) ===");
    eprintln!("first post-compaction wave hit rate:");
    eprintln!("  prefetch OFF (baseline): {:.1}%", off * 100.0);
    eprintln!("  prefetch ON  (repair):   {:.1}%", on * 100.0);
    eprintln!("=================================================================\n");

    assert!(
        off <= 0.05,
        "baseline should cold-miss the rewritten files, got {:.1}%",
        off * 100.0
    );
    assert!(
        on >= 0.90,
        "prefetch should recover >=90% immediately, got {:.1}%",
        on * 100.0
    );
}

/// The coordinator submits nothing for an `append` commit (cold new data) and
/// demotes nothing (no removed files), but a dropped table / unknown table is a
/// clean no-op — exercising the early-return and append branches.
#[tokio::test]
async fn coordinator_append_and_dropped_are_noops() {
    let fx = build_parquet_fixture().await;
    let store_fetch = Arc::new(ObjectStoreFetch::new(
        fx.store.clone() as Arc<dyn ObjectStore>
    ));
    let mapper = Arc::new(mapper_at_append(store_fetch.as_ref(), &fx.metadata_key).await);
    let ident = TableIdent::new(&["db"], "events");

    let dir = TempDir::new().expect("dir");
    let (engine, _calls) = build_engine(fx.store.clone(), &dir).await;
    let budget = Arc::new(verglas_tables::warming::budget::TokenBucket::unlimited());
    let executor = Arc::new(PrefetchExecutor::spawn(
        engine.clone(),
        budget,
        &PrefetchConfig::default(),
    ));
    let retire = Arc::new(RetirementScheduler::new());
    let ledger = Arc::new(HeatLedger::with_defaults());

    let watcher = FakeWatcher::new();
    watcher.set(
        ident.clone(),
        format!("s3://{BUCKET}/{}", fx.metadata_key),
        Some(SNAP_APPEND),
    );
    let coord = PrefetchCoordinator::new(
        "managed-lakehouse",
        watcher,
        store_fetch.clone(),
        mapper.clone(),
        ledger,
        executor.clone(),
        retire.clone(),
        None,
        None,
        PrefetchConfig::default(),
        EpochClock::new(300),
    );

    // Append commit: diff runs, but no data prefetch and no demotion.
    coord
        .on_change(&TableChanged {
            table: ident.clone(),
            old_snapshot: None,
            new_snapshot: Some(SNAP_APPEND),
        })
        .await;
    assert_eq!(executor.metrics().submitted.load(Ordering::Relaxed), 0);
    assert_eq!(retire.demoted_count(), 0);

    // Dropped table (new_snapshot None): a clean no-op.
    coord
        .on_change(&TableChanged {
            table: ident.clone(),
            old_snapshot: Some(SNAP_APPEND),
            new_snapshot: None,
        })
        .await;
    assert_eq!(executor.metrics().submitted.load(Ordering::Relaxed), 0);

    // Unknown table (no id assigned): also a clean no-op.
    let unknown = TableIdent::new(&["db"], "nope");
    coord.retire(); // touch the accessor
    coord
        .on_change(&TableChanged {
            table: unknown,
            old_snapshot: None,
            new_snapshot: Some(SNAP_APPEND),
        })
        .await;
    assert_eq!(executor.metrics().submitted.load(Ordering::Relaxed), 0);
}

/// Restart survival (#305): the retirement schedule persists, a fresh
/// coordinator restores it — re-demoting the objects in the engine instead of
/// amnestying them — and the post-restart sweep still physically reclaims the
/// blocks using the ETag captured before the "restart".
#[tokio::test]
async fn restore_after_restart_still_physically_reclaims() {
    let fx = build_parquet_fixture().await;
    let store_fetch = Arc::new(ObjectStoreFetch::new(
        fx.store.clone() as Arc<dyn ObjectStore>
    ));
    let mapper = Arc::new(mapper_at_append(store_fetch.as_ref(), &fx.metadata_key).await);
    let ledger = Arc::new(HeatLedger::with_defaults());
    let dir = TempDir::new().expect("dir");
    let (engine, calls) = build_engine(fx.store.clone(), &dir).await;
    let state_dir = TempDir::new().expect("state dir");
    let state_path = state_dir.path().join("retire-state.json");

    // Warm a removed file, then process the replace commit with persistence on.
    let removed0 = key(&fx.small[0]);
    read_all(&engine, &removed0, ReadRange::Full).await;
    engine.flush().await;

    let make_coord = |retire: Arc<RetirementScheduler>| {
        let budget = Arc::new(verglas_tables::warming::budget::TokenBucket::unlimited());
        let executor = Arc::new(PrefetchExecutor::spawn(
            engine.clone(),
            budget,
            &PrefetchConfig::default(),
        ));
        let watcher = FakeWatcher::new();
        let ident = TableIdent::new(&["db"], "events");
        watcher.set(
            ident.clone(),
            format!("s3://{BUCKET}/{}", fx.metadata_key),
            Some(SNAP_REPLACE),
        );
        PrefetchCoordinator::new(
            "managed-lakehouse",
            watcher,
            store_fetch.clone(),
            mapper.clone(),
            ledger.clone(),
            executor,
            retire,
            Some(engine.clone() as Arc<dyn verglas_cache::BlockDemoter>),
            Some(state_path.clone()),
            PrefetchConfig::default(),
            EpochClock::new(300),
        )
    };

    let first = make_coord(Arc::new(RetirementScheduler::new()));
    first
        .on_change(&TableChanged {
            table: TableIdent::new(&["db"], "events"),
            old_snapshot: Some(SNAP_APPEND),
            new_snapshot: Some(SNAP_REPLACE),
        })
        .await;
    assert!(engine.is_demoted(&removed0));
    assert!(state_path.exists(), "the schedule persisted");
    drop(first);

    // "Restart": a fresh scheduler and coordinator restore from disk. The
    // engine's demotion set is rebuilt (no amnesty) and the schedule is back.
    let second = make_coord(Arc::new(RetirementScheduler::new()));
    // Before restore the fresh engine-side state has been... kept (same engine
    // object survives in this test), so clear it to prove restore rebuilds it.
    let _ = engine.hard_evict(&[verglas_cache::HardEviction {
        key: removed0.clone(),
        etag: None,
        size: 0,
        generation: 0,
    }]);
    assert!(!engine.is_demoted(&removed0), "cleared to simulate restart");
    second.restore(now_unix_ms_test());
    assert!(
        engine.is_demoted(&removed0),
        "restore re-demotes grace-pending objects in the engine"
    );
    assert_eq!(second.retire().demoted_count(), 2);

    // The post-restart sweep physically reclaims with the persisted ETag: a
    // re-read must refill from the backend.
    engine.flush().await;
    let swept = second.sweep_expired(now_unix_ms_test() + DEFAULT_GRACE_MS + 1);
    assert_eq!(swept, 2);
    let before = calls.gets.load(Ordering::SeqCst);
    read_all(&engine, &removed0, ReadRange::Full).await;
    assert!(
        calls.gets.load(Ordering::SeqCst) > before,
        "post-restart sweep physically removed the persisted demotion's blocks"
    );
}
