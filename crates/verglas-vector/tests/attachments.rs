//! Integration tests for catalog-discovered Vamana attachments and strict
//! snapshot matching across service restarts.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use verglas_vector::Metric;
use verglas_vector::error::VectorError;
use verglas_vector::maintenance::MaintenanceConfig;
use verglas_vector::service::{IndexKey, SearchOptions, VectorService};

/// Creates a temporary Iceberg catalog.
async fn memory_catalog() -> Arc<dyn Catalog> {
    let warehouse = tempfile::tempdir().expect("tempdir").keep();
    Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    format!("file://{}", warehouse.display()),
                )]),
            )
            .await
            .expect("catalog"),
    )
}

/// The test table schema.
fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            false,
        ),
    ]))
}

/// Builds one record batch.
fn batch(rows: &[(i64, Vec<f32>)]) -> RecordBatch {
    let ids = Arc::new(Int64Array::from(
        rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
    ));
    let mut vectors = ListBuilder::new(Float32Builder::new());
    for (_, vector) in rows {
        for value in vector {
            vectors.values().append_value(*value);
        }
        vectors.append(true);
    }
    RecordBatch::try_new(table_schema(), vec![ids, Arc::new(vectors.finish())]).expect("batch")
}

/// Creates and populates the test table.
async fn corpus() -> (Arc<dyn Catalog>, TableIdent, IndexKey) {
    let catalog = memory_catalog().await;
    catalog
        .create_namespace(&NamespaceIdent::new("default".into()), HashMap::new())
        .await
        .expect("namespace");
    let ident = TableIdent::from_strs(["default", "docs"]).expect("ident");
    verglas_iceberg::write::create_table_from_schema(
        catalog.as_ref(),
        &ident,
        &table_schema(),
        None,
    )
    .await
    .expect("create");
    verglas_iceberg::write::append_batches(
        catalog.as_ref(),
        &ident,
        vec![batch(&[
            (1, vec![0.0, 0.0]),
            (2, vec![1.0, 0.0]),
            (3, vec![0.0, 1.0]),
            (4, vec![1.0, 1.0]),
        ])],
        HashMap::new(),
    )
    .await
    .expect("append");
    let key = IndexKey::table("default.docs", "embedding");
    (catalog, ident, key)
}

/// Standard test search options.
fn options() -> SearchOptions {
    SearchOptions { k: 2, l: 64 }
}

#[tokio::test]
async fn fresh_service_discovers_the_snapshot_attachment() {
    let (catalog, ident, key) = corpus().await;
    VectorService::new()
        .declare(
            catalog.as_ref(),
            &ident,
            key.clone(),
            MaintenanceConfig::new("id", "embedding", Metric::L2),
        )
        .await
        .expect("declare")
        .expect("built");

    let restarted = VectorService::new();
    let result = restarted
        .search(catalog.as_ref(), &ident, &key, &[0.9, 0.1], &options())
        .await
        .expect("search attached index");
    assert_eq!(result.neighbors[0].id, 2);
}

#[tokio::test]
async fn search_rejects_a_snapshot_without_an_attachment() {
    let (catalog, ident, key) = corpus().await;
    let service = VectorService::new();
    service
        .declare(
            catalog.as_ref(),
            &ident,
            key.clone(),
            MaintenanceConfig::new("id", "embedding", Metric::L2),
        )
        .await
        .expect("declare")
        .expect("built");

    verglas_iceberg::write::append_batches(
        catalog.as_ref(),
        &ident,
        vec![batch(&[(5, vec![0.8, 0.2])])],
        HashMap::new(),
    )
    .await
    .expect("append new snapshot");

    let error = service
        .search(catalog.as_ref(), &ident, &key, &[0.9, 0.1], &options())
        .await
        .expect_err("stale index must not be used");
    assert!(matches!(error, VectorError::IndexNotFound { .. }));

    service
        .refresh(catalog.as_ref(), &ident, &key)
        .await
        .expect("refresh")
        .expect("rebuilt attachment");
    let result = service
        .search(catalog.as_ref(), &ident, &key, &[0.8, 0.2], &options())
        .await
        .expect("search refreshed attachment");
    assert_eq!(result.neighbors[0].id, 5);
}

#[tokio::test]
async fn undeclared_field_is_an_error_without_brute_force() {
    let (catalog, ident, _) = corpus().await;
    let key = IndexKey::table("default.docs", "embedding");
    let error = VectorService::new()
        .search(catalog.as_ref(), &ident, &key, &[0.9, 0.1], &options())
        .await
        .expect_err("missing attachment must fail");
    assert!(matches!(error, VectorError::IndexNotFound { .. }));
}
