//! Reboot rehydration at the service level: a declared index whose blob is in
//! the (temp-dir) shadow store re-opens into a fresh `VectorService` and serves
//! the SAME nearest neighbors WITHOUT rebuilding; a declared index whose blob is
//! missing takes the rebuild path instead.
//!
//! This is the engine half of the reboot proof — the durable-registry round-trip
//! that reconstructs the maintenance config lives in the daemon's tests (it needs
//! the platform `SystemCatalog`). Here the config is handed to `rehydrate`
//! directly, and the store is a real directory that outlives each service, so
//! "reboot" is a new `VectorService` over the same on-disk shadow store.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

use verglas_vector::Metric;
use verglas_vector::maintenance::MaintenanceConfig;
use verglas_vector::service::{RehydrateOutcome, SearchOptions, SearchSource, VectorService};
use verglas_vector::store::{IndexKey, LocalDirShadowStore, ShadowBlobStore};

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

fn batch(rows: &[(i64, Vec<f32>)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let mut lb = ListBuilder::new(Float32Builder::new());
    for (_, v) in rows {
        for x in v {
            lb.values().append_value(*x);
        }
        lb.append(true);
    }
    RecordBatch::try_new(
        table_schema(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(lb.finish())],
    )
    .expect("batch")
}

async fn memory_catalog() -> Arc<dyn Catalog> {
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = dir.keep();
    Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", warehouse.display()),
                )]),
            )
            .await
            .expect("catalog"),
    )
}

fn search_opts(k: usize) -> SearchOptions {
    SearchOptions {
        k,
        l: 64,
        default_metric: Metric::L2,
        default_id_field: "id".to_owned(),
    }
}

/// The full corpus + a declared index, returning the catalog, ident, key,
/// config, the shadow-store temp dir path, and the top-3 ids for a probe query.
async fn declared_corpus() -> (
    Arc<dyn Catalog>,
    TableIdent,
    IndexKey,
    MaintenanceConfig,
    tempfile::TempDir,
    Vec<i64>,
) {
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
    let corpus = vec![
        (1i64, vec![0.0, 0.0]),
        (2, vec![1.0, 0.0]),
        (3, vec![0.0, 1.0]),
        (4, vec![1.0, 1.0]),
        (5, vec![0.5, 0.5]),
    ];
    verglas_iceberg::write::append_batches(
        catalog.as_ref(),
        &ident,
        vec![batch(&corpus)],
        HashMap::new(),
    )
    .await
    .expect("append");

    let shadow_dir = tempfile::tempdir().expect("shadow dir");
    let key = IndexKey::table("default.docs", "embedding");
    let config = MaintenanceConfig::new("id", "embedding", Metric::L2);

    // Declare (initial build) through a first service instance, then let it drop.
    let store = Arc::new(LocalDirShadowStore::at(shadow_dir.path()));
    let service = VectorService::new(store);
    service
        .declare(catalog.as_ref(), &ident, key.clone(), config.clone())
        .await
        .expect("declare")
        .expect("built");
    let probe = vec![0.9f32, 0.1];
    let before = service
        .search(catalog.as_ref(), &ident, &key, &probe, &search_opts(3))
        .await
        .expect("search");
    assert_eq!(before.source, SearchSource::Index);
    let ids: Vec<i64> = before.neighbors.iter().map(|n| n.id).collect();
    (catalog, ident, key, config, shadow_dir, ids)
}

#[tokio::test]
async fn present_blob_rehydrates_and_serves_without_rebuild() {
    let (catalog, ident, key, config, shadow_dir, ids_before) = declared_corpus().await;

    // "Reboot": a brand-new service over the SAME on-disk shadow store, with a
    // fresh (empty) in-memory registry and cache.
    let store = Arc::new(LocalDirShadowStore::at(shadow_dir.path()));
    let service = VectorService::new(store);
    let outcome = service
        .rehydrate(catalog.as_ref(), &ident, key.clone(), config)
        .await
        .expect("rehydrate");
    match outcome {
        RehydrateOutcome::ServedFromBlob { live_count, .. } => {
            assert_eq!(live_count, 5, "the present blob was loaded, not rebuilt");
        }
        RehydrateOutcome::Rebuilt(_) => panic!("present blob must NOT trigger a rebuild"),
    }

    // Same query returns the same neighbors, served from the index.
    let probe = vec![0.9f32, 0.1];
    let after = service
        .search(catalog.as_ref(), &ident, &key, &probe, &search_opts(3))
        .await
        .expect("search");
    assert_eq!(after.source, SearchSource::Index);
    let ids_after: Vec<i64> = after.neighbors.iter().map(|n| n.id).collect();
    assert_eq!(
        ids_after, ids_before,
        "rehydrated index served different NN"
    );
    assert_eq!(ids_after[0], 2, "nearest to (0.9,0.1) is id 2");
}

#[tokio::test]
async fn missing_blob_takes_the_rebuild_path() {
    let (catalog, ident, key, config, _shadow_dir, ids_before) = declared_corpus().await;

    // "Reboot" against an EMPTY shadow store (the blob was lost): rehydrate must
    // rebuild from the source table rather than serve nothing.
    let empty_dir = tempfile::tempdir().expect("empty shadow dir");
    let store = Arc::new(LocalDirShadowStore::at(empty_dir.path()));
    assert!(
        store.latest(&key).await.expect("latest").is_none(),
        "the reboot store starts with no blob"
    );
    let service = VectorService::new(store);
    let outcome = service
        .rehydrate(catalog.as_ref(), &ident, key.clone(), config)
        .await
        .expect("rehydrate");
    match outcome {
        RehydrateOutcome::Rebuilt(Some(report)) => {
            assert!(report.full_build, "a missing blob rebuilds from empty");
            assert_eq!(report.live_count, 5);
        }
        other => panic!("missing blob must rebuild, got {other:?}"),
    }

    // The rebuilt index serves the same neighbors.
    let probe = vec![0.9f32, 0.1];
    let after = service
        .search(catalog.as_ref(), &ident, &key, &probe, &search_opts(3))
        .await
        .expect("search");
    assert_eq!(after.source, SearchSource::Index);
    let ids_after: Vec<i64> = after.neighbors.iter().map(|n| n.id).collect();
    assert_eq!(ids_after, ids_before);
}
