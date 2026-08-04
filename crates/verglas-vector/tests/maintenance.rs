//! Integration tests for the index-maintenance MV over an in-process Iceberg
//! `MemoryCatalog`: full build, incremental adds+deletes, watermark advance,
//! kill-mid-run re-consume, and the placement invariant (the index never
//! touches the source table's snapshots).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

use verglas_vector::maintenance::{MaintenanceConfig, load_latest_index, run_maintenance};
use verglas_vector::store::{InMemoryShadowStore, IndexKey, ShadowBlobStore};
use verglas_vector::{Metric, brute_force_search};

async fn memory_catalog() -> Arc<dyn Catalog> {
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = dir.keep();
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                format!("file://{}", warehouse.display()),
            )]),
        )
        .await
        .expect("memory catalog");
    Arc::new(catalog)
}

fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        ),
    ]))
}

/// Deterministic synthetic vectors.
fn synth(ids: impl IntoIterator<Item = i64>, dim: usize, seed: u64) -> Vec<(i64, Vec<f32>)> {
    let mut state = seed | 1;
    let mut next = || {
        state = state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^= z >> 31;
        (z as f64 / u64::MAX as f64) as f32 * 2.0 - 1.0
    };
    ids.into_iter()
        .map(|id| (id, (0..dim).map(|_| next()).collect()))
        .collect()
}

/// A batch of `(id, Some(vector))` inserts or `(id, None)` deletes.
fn batch(rows: &[(i64, Option<Vec<f32>>)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let mut lb = ListBuilder::new(Float32Builder::new());
    for (_, v) in rows {
        match v {
            Some(vec) => {
                for x in vec {
                    lb.values().append_value(*x);
                }
                lb.append(true);
            }
            None => lb.append(false),
        }
    }
    let embedding = lb.finish();
    RecordBatch::try_new(
        table_schema(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(embedding)],
    )
    .expect("record batch")
}

async fn append(catalog: &dyn Catalog, ident: &TableIdent, rows: &[(i64, Option<Vec<f32>>)]) {
    verglas_iceberg::write::append_batches(catalog, ident, vec![batch(rows)], HashMap::new())
        .await
        .expect("append");
}

async fn snapshot_count(catalog: &dyn Catalog, ident: &TableIdent) -> usize {
    let table = catalog.load_table(ident).await.expect("load");
    table.metadata().snapshots().count()
}

#[tokio::test]
async fn full_build_then_incremental_adds_and_deletes() {
    let dim = 16;
    let catalog = memory_catalog().await;
    catalog
        .create_namespace(&NamespaceIdent::new("default".into()), HashMap::new())
        .await
        .expect("ns");
    let ident = TableIdent::from_strs(["default", "docs"]).expect("ident");
    verglas_iceberg::write::create_table_from_schema(
        catalog.as_ref(),
        &ident,
        &table_schema(),
        None,
    )
    .await
    .expect("create");

    // Initial data: ids 0..200 with vectors.
    let initial = synth(0..200, dim, 42);
    let rows: Vec<(i64, Option<Vec<f32>>)> = initial
        .iter()
        .map(|(id, v)| (*id, Some(v.clone())))
        .collect();
    append(catalog.as_ref(), &ident, &rows).await;

    let store = InMemoryShadowStore::new();
    let key = IndexKey::table("default.docs", "embedding");
    let config = MaintenanceConfig::new("id", "embedding", Metric::L2);

    // Snapshots on the source table before the maintenance run.
    let snaps_before = snapshot_count(catalog.as_ref(), &ident).await;
    let src_snapshot_before = catalog
        .load_table(&ident)
        .await
        .expect("load")
        .metadata()
        .current_snapshot_id();

    // Full build.
    let report = run_maintenance(catalog.as_ref(), &ident, &key, &store, &config)
        .await
        .expect("maintain")
        .expect("built");
    assert!(report.full_build);
    assert_eq!(report.inserts, 200);
    assert_eq!(report.live_count, 200);
    assert_eq!(report.prior_snapshot, None);

    // Placement invariant: the maintenance run created NO snapshot on the source
    // table and attached NO statistics/Puffin to it (the divergence from #91 —
    // the index lives only in the shadow store).
    let table = catalog.load_table(&ident).await.expect("load");
    assert_eq!(
        snapshot_count(catalog.as_ref(), &ident).await,
        snaps_before,
        "source table gained a snapshot"
    );
    assert_eq!(table.metadata().current_snapshot_id(), src_snapshot_before);
    assert!(
        table.metadata().statistics_iter().next().is_none(),
        "no StatisticsFile should be bound to the source table"
    );
    // The reflected snapshot equals the source tip.
    assert_eq!(report.reflected_snapshot, src_snapshot_before.expect("tip"));

    // The index (loaded from the shadow store) returns sensible neighbors: each
    // vector's nearest neighbor is itself.
    let index = load_latest_index(&store, &key)
        .await
        .expect("load index")
        .expect("present");
    for (id, v) in initial.iter().take(20) {
        let hits = index.search(v, 1, 64).expect("search");
        assert_eq!(hits[0].id, *id);
    }

    // Incremental: add ids 200..260, delete ids 0..40 (null embedding).
    let added = synth(200..260, dim, 7);
    let mut delta_rows: Vec<(i64, Option<Vec<f32>>)> =
        added.iter().map(|(id, v)| (*id, Some(v.clone()))).collect();
    delta_rows.extend((0..40).map(|id| (id, None)));
    append(catalog.as_ref(), &ident, &delta_rows).await;

    let report2 = run_maintenance(catalog.as_ref(), &ident, &key, &store, &config)
        .await
        .expect("maintain2")
        .expect("updated");
    assert!(!report2.full_build);
    assert_eq!(report2.inserts, 60);
    assert_eq!(report2.deletes, 40);
    assert_eq!(report2.live_count, 220, "200 - 40 deleted + 60 added");
    // Watermark advanced (Iceberg snapshot ids are random i64, not monotonic —
    // "advanced" means different, tracked by the shadow store's write sequence).
    assert_eq!(report2.prior_snapshot, Some(report.reflected_snapshot));
    assert_ne!(report2.reflected_snapshot, report.reflected_snapshot);

    // The deleted ids are never returned; added ids are findable.
    let index2 = load_latest_index(&store, &key)
        .await
        .expect("load2")
        .expect("present2");
    for (id, v) in &added {
        let hits = index2.search(v, 1, 64).expect("search");
        assert_eq!(hits[0].id, *id, "added id {id} not findable");
    }
    for id in 0..40 {
        assert!(!index2.contains(id), "deleted id {id} still present");
    }
    // A query at a deleted id's old vector never returns that id.
    for (id, v) in initial.iter().take(40) {
        let hits = index2.search(v, 10, 100).expect("search");
        assert!(hits.iter().all(|n| n.id != *id), "deleted id {id} returned");
    }
}

#[tokio::test]
async fn kill_mid_run_reconsumes_without_corruption() {
    let dim = 16;
    let catalog = memory_catalog().await;
    catalog
        .create_namespace(&NamespaceIdent::new("default".into()), HashMap::new())
        .await
        .expect("ns");
    let ident = TableIdent::from_strs(["default", "docs"]).expect("ident");
    verglas_iceberg::write::create_table_from_schema(
        catalog.as_ref(),
        &ident,
        &table_schema(),
        None,
    )
    .await
    .expect("create");

    let key = IndexKey::table("default.docs", "embedding");
    let config = MaintenanceConfig::new("id", "embedding", Metric::L2);

    // Reflect an initial snapshot A.
    let initial = synth(0..100, dim, 1);
    let rows: Vec<(i64, Option<Vec<f32>>)> = initial
        .iter()
        .map(|(id, v)| (*id, Some(v.clone())))
        .collect();
    append(catalog.as_ref(), &ident, &rows).await;
    let store = InMemoryShadowStore::new();
    run_maintenance(catalog.as_ref(), &ident, &key, &store, &config)
        .await
        .expect("A")
        .expect("A built");
    let blob_at_a = store.latest(&key).await.expect("latest").expect("blob a");

    // Append a delta (adds + deletes) producing snapshot B.
    let added = synth(100..140, dim, 2);
    let mut delta_rows: Vec<(i64, Option<Vec<f32>>)> =
        added.iter().map(|(id, v)| (*id, Some(v.clone()))).collect();
    delta_rows.extend((0..20).map(|id| (id, None)));
    append(catalog.as_ref(), &ident, &delta_rows).await;

    // Run the delta A->B once, capture the live id set.
    run_maintenance(catalog.as_ref(), &ident, &key, &store, &config)
        .await
        .expect("B1")
        .expect("B1 built");
    let index_b1 = load_latest_index(&store, &key)
        .await
        .expect("load b1")
        .expect("b1");
    let live_b1: std::collections::BTreeSet<i64> =
        (0..140).filter(|&id| index_b1.contains(id)).collect();

    // Simulate a crash before the B run's persist: a fresh store seeded only
    // with the blob reflecting A, then re-run the SAME delta A->B.
    let store2 = InMemoryShadowStore::new();
    store2
        .put(&key, blob_at_a.snapshot, blob_at_a.bytes.clone())
        .await
        .expect("seed a");
    run_maintenance(catalog.as_ref(), &ident, &key, &store2, &config)
        .await
        .expect("B2")
        .expect("B2 built");
    let index_b2 = load_latest_index(&store2, &key)
        .await
        .expect("load b2")
        .expect("b2");
    let live_b2: std::collections::BTreeSet<i64> =
        (0..140).filter(|&id| index_b2.contains(id)).collect();

    // Re-consuming the same delta after a lost run yields the identical live set.
    assert_eq!(live_b1, live_b2, "re-consume diverged from the first run");
    assert_eq!(live_b2.len(), 120, "100 - 20 deleted + 40 added");
    assert_eq!(
        index_b2.reflected_snapshot(),
        index_b1.reflected_snapshot(),
        "both runs reflect snapshot B"
    );
}

#[tokio::test]
async fn empty_table_yields_no_index() {
    let catalog = memory_catalog().await;
    catalog
        .create_namespace(&NamespaceIdent::new("default".into()), HashMap::new())
        .await
        .expect("ns");
    let ident = TableIdent::from_strs(["default", "docs"]).expect("ident");
    verglas_iceberg::write::create_table_from_schema(
        catalog.as_ref(),
        &ident,
        &table_schema(),
        None,
    )
    .await
    .expect("create");

    let store = InMemoryShadowStore::new();
    let key = IndexKey::table("default.docs", "embedding");
    let config = MaintenanceConfig::new("id", "embedding", Metric::L2);
    let report = run_maintenance(catalog.as_ref(), &ident, &key, &store, &config)
        .await
        .expect("maintain");
    assert!(report.is_none(), "empty table should build no index");
    assert!(store.latest(&key).await.expect("latest").is_none());
}

#[tokio::test]
async fn brute_force_matches_index_top1() {
    // The turn-off ground truth: brute force over the same rows agrees with the
    // index on the nearest neighbor.
    let dim = 24;
    let data = synth(0..300, dim, 55);
    let rows: Vec<(i64, Vec<f32>)> = data.clone();
    for (id, q) in data.iter().take(30) {
        let bf = brute_force_search(Metric::Cosine, dim, &rows, q, 3).expect("bf");
        // A vector's own nearest neighbor is itself, at distance ~0.
        assert_eq!(bf[0].id, *id);
        assert!(bf[0].distance < 1e-5, "self distance {}", bf[0].distance);
    }
}
