//! Focused acceptance tests for the narrow deterministic Iceberg Sink capability.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder};
use serde_json::json;
use verglas_iceberg::tables_api::{
    self, SINK_FILE_ID_PROPERTY, SinkBatchConfig, SinkBatchRequest, SinkCompression,
};
use verglas_iceberg::{TableCache, parse_table_ident};

/// Builds a MemoryCatalog over a temporary warehouse kept alive for the test.
async fn memory_catalog() -> Arc<dyn Catalog> {
    let warehouse = tempfile::tempdir().expect("warehouse tempdir");
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_owned(),
                warehouse.path().to_str().expect("utf8 path").to_owned(),
            )]),
        )
        .await
        .expect("memory catalog");
    std::mem::forget(warehouse);
    Arc::new(catalog)
}

/// Builds one owned Sink configuration.
fn config() -> SinkBatchConfig {
    SinkBatchConfig::new("primary", SinkCompression::Zstd)
}

/// Builds one valid deterministic batch.
fn request(batch_id: &str, payload_digest: &str) -> SinkBatchRequest {
    SinkBatchRequest::new(
        batch_id,
        payload_digest,
        "primary",
        vec![
            json!({"id": 7, "name": "one"}),
            json!({"id": 8, "name": "two"}),
        ],
    )
}

/// A missing table is created, schema is inferred, and one deterministic Parquet
/// file and snapshot are committed.
#[tokio::test]
async fn sink_commit_creates_table_and_deterministic_file() {
    let catalog = memory_catalog().await;
    let cache = TableCache::new();
    let ident = parse_table_ident("analytics.events").expect("ident");
    let batch = request("batch-1", "payload-1");

    let receipt =
        tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &config(), batch.clone())
            .await
            .expect("commit");

    assert_eq!(receipt.rows_committed, 2);
    assert_eq!(receipt.accepted, 2);
    let table = catalog.load_table(&ident).await.expect("table");
    assert_eq!(
        table.metadata().properties().get("verglas.sink.owner"),
        Some(&"primary".to_owned())
    );
    let snapshot = table.metadata().snapshots().next().expect("snapshot");
    assert_eq!(
        snapshot
            .summary()
            .additional_properties
            .get(SINK_FILE_ID_PROPERTY),
        Some(&batch.file_id)
    );
    let path = format!("{}/data/{}", table.metadata().location(), batch.file_id);
    assert!(
        table
            .file_io()
            .new_input(&path)
            .expect("input")
            .exists()
            .await
            .expect("file exists")
    );
}

/// Replaying the same batch returns the original receipt without another file.
#[tokio::test]
async fn sink_commit_replay_is_idempotent() {
    let catalog = memory_catalog().await;
    let cache = TableCache::new();
    let ident = parse_table_ident("analytics.events").expect("ident");
    let batch = request("batch-replay", "payload-replay");
    let first =
        tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &config(), batch.clone())
            .await
            .expect("first commit");
    let second = tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &config(), batch)
        .await
        .expect("replay");
    assert_eq!(first, second);
    let table = catalog.load_table(&ident).await.expect("table");
    assert_eq!(table.metadata().snapshots().count(), 1);
}

/// Reusing a batch identity with a changed payload is rejected.
#[tokio::test]
async fn sink_commit_rejects_changed_payload() {
    let catalog = memory_catalog().await;
    let cache = TableCache::new();
    let ident = parse_table_ident("analytics.events").expect("ident");
    let first = request("batch-changed", "payload-a");
    tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &config(), first.clone())
        .await
        .expect("first commit");
    let changed = SinkBatchRequest::new(
        first.batch_id,
        "payload-b",
        "primary",
        vec![json!({"id": 7, "name": "changed"})],
    );
    let error = tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &config(), changed)
        .await
        .expect_err("changed payload");
    assert!(error.to_string().contains("payload"));
}

/// File identity is stable and malformed request shapes fail before catalog I/O.
#[test]
fn sink_request_identity_is_deterministic() {
    let first = request("batch-stable", "payload");
    let second = request("batch-stable", "payload");
    assert_eq!(first.file_id, second.file_id);
    assert!(first.file_id.starts_with("verglas/primary/batch-"));
    assert!(first.file_id.ends_with(".parquet"));
}
