//! Hermetic tests for the SDK-facing table API (`tables_api`).
//!
//! These exercise the endpoint contract `@verglas/sdk` speaks — commit, current
//! snapshot, paged row scan, and delta-since — fully in-process against an
//! Iceberg `MemoryCatalog` over an in-memory warehouse. No daemon, no MinIO, no
//! network: the daemon's admin routes are thin wrappers over these functions, so
//! proving the contract here proves the served behaviour.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use serde_json::{Value, json};
use verglas_iceberg::tables_api::{self, CommitRequest};
use verglas_iceberg::{parse_table_ident, write};

/// Builds an in-process memory catalog over a fresh temp warehouse path. The
/// guard is leaked because the test process is short-lived.
async fn memory_catalog() -> Arc<dyn Catalog> {
    let warehouse = tempfile::tempdir().expect("warehouse tempdir");
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.path().to_str().expect("utf8 path").to_string(),
            )]),
        )
        .await
        .expect("memory catalog");
    std::mem::forget(warehouse);
    Arc::new(catalog)
}

/// A two-column schema: a required id and a nullable name.
fn simple_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// Creates `dotted` with `schema` (optionally partitioned) and returns its ident.
async fn create(
    catalog: &dyn Catalog,
    dotted: &str,
    schema: &SchemaRef,
    partition_by: Option<&str>,
) -> TableIdent {
    let ident = parse_table_ident(dotted).expect("ident");
    write::create_table_from_schema(catalog, &ident, schema, partition_by)
        .await
        .expect("create table");
    ident
}

/// Acceptance: commit converts JSON rows to Arrow, appends them, and returns the
/// new snapshot id as both `snapshotId` and `watermark`, with the row count.
#[tokio::test]
async fn commit_appends_rows_and_returns_snapshot_and_watermark() {
    let catalog = memory_catalog().await;
    let ident = create(catalog.as_ref(), "sdk.events", &simple_schema(), None).await;

    let response = tables_api::commit(
        catalog.as_ref(),
        &ident,
        CommitRequest {
            rows: vec![
                json!({"id": 1, "name": "alice"}),
                json!({"id": 2, "name": "bob"}),
            ],
            idempotency_key: None,
        },
    )
    .await
    .expect("commit");

    assert_eq!(response.rows_committed, 2);
    assert!(!response.idempotent);
    assert!(!response.snapshot_id.is_empty());
    assert_eq!(
        response.watermark, response.snapshot_id,
        "watermark is the new snapshot id"
    );

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(snap.record_count, 2);
    assert_eq!(snap.snapshot_id, response.snapshot_id);
    assert_eq!(snap.watermark, response.snapshot_id);
}

/// Acceptance: a repeated `idempotencyKey` writes nothing the second time and
/// returns the original snapshot/watermark with `idempotent: true`.
#[tokio::test]
async fn commit_with_idempotency_key_dedups() {
    let catalog = memory_catalog().await;
    let ident = create(catalog.as_ref(), "sdk.events", &simple_schema(), None).await;

    let request = || CommitRequest {
        rows: vec![json!({"id": 1, "name": "alice"})],
        idempotency_key: Some("batch-42".to_owned()),
    };

    let first = tables_api::commit(catalog.as_ref(), &ident, request())
        .await
        .expect("first commit");
    assert!(!first.idempotent);
    assert_eq!(first.rows_committed, 1);

    let second = tables_api::commit(catalog.as_ref(), &ident, request())
        .await
        .expect("second commit");
    assert!(second.idempotent, "second commit is a replay");
    assert_eq!(second.snapshot_id, first.snapshot_id);
    assert_eq!(second.watermark, first.watermark);
    assert_eq!(second.rows_committed, 1, "returns the original row count");

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(snap.record_count, 1, "the duplicate wrote nothing");
}

/// Acceptance: `rows` scans the current snapshot and pages with an opaque cursor.
#[tokio::test]
async fn rows_scan_is_paged() {
    let catalog = memory_catalog().await;
    let ident = create(catalog.as_ref(), "sdk.events", &simple_schema(), None).await;
    tables_api::commit(
        catalog.as_ref(),
        &ident,
        CommitRequest {
            rows: vec![
                json!({"id": 1, "name": "a"}),
                json!({"id": 2, "name": "b"}),
                json!({"id": 3, "name": "c"}),
            ],
            idempotency_key: None,
        },
    )
    .await
    .expect("commit");

    let first = tables_api::rows(catalog.as_ref(), &ident, Some(2), None)
        .await
        .expect("rows page 1");
    assert_eq!(first.rows.len(), 2);
    let cursor = first.next_cursor.clone().expect("more rows remain");

    let second = tables_api::rows(catalog.as_ref(), &ident, Some(2), Some(cursor))
        .await
        .expect("rows page 2");
    assert_eq!(second.rows.len(), 1);
    assert!(second.next_cursor.is_none(), "no more rows");

    // Every committed id appears exactly once across the two pages.
    let mut ids: Vec<i64> = first
        .rows
        .iter()
        .chain(second.rows.iter())
        .map(|r| r["id"].as_i64().expect("id is an int"))
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);
}

/// Acceptance: `delta` returns only rows committed after the `since` watermark,
/// and reports the new tip as its watermark.
#[tokio::test]
async fn delta_returns_only_rows_after_since() {
    let catalog = memory_catalog().await;
    let ident = create(catalog.as_ref(), "sdk.events", &simple_schema(), None).await;

    let first = tables_api::commit(
        catalog.as_ref(),
        &ident,
        CommitRequest {
            rows: vec![json!({"id": 1, "name": "old"})],
            idempotency_key: None,
        },
    )
    .await
    .expect("first commit");

    tables_api::commit(
        catalog.as_ref(),
        &ident,
        CommitRequest {
            rows: vec![
                json!({"id": 2, "name": "new"}),
                json!({"id": 3, "name": "newer"}),
            ],
            idempotency_key: None,
        },
    )
    .await
    .expect("second commit");

    let delta = tables_api::delta(
        catalog.as_ref(),
        &ident,
        Some(first.watermark.clone()),
        None,
    )
    .await
    .expect("delta");
    let mut ids: Vec<i64> = delta
        .rows
        .iter()
        .map(|r| r["id"].as_i64().expect("id"))
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![2, 3], "only rows from after the since watermark");

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(
        delta.watermark, snap.snapshot_id,
        "delta advances to the tip"
    );

    // A delta from the current tip is empty.
    let empty = tables_api::delta(catalog.as_ref(), &ident, Some(snap.watermark), None)
        .await
        .expect("delta at tip");
    assert!(empty.rows.is_empty());
}

/// Acceptance: a row carrying a column the table does not have is rejected with a
/// clear, column-named error, and nothing is written.
#[tokio::test]
async fn commit_rejects_a_row_that_does_not_match_the_schema() {
    let catalog = memory_catalog().await;
    let ident = create(catalog.as_ref(), "sdk.events", &simple_schema(), None).await;

    let err = tables_api::commit(
        catalog.as_ref(),
        &ident,
        CommitRequest {
            rows: vec![json!({"id": 1, "name": "alice", "surprise": "extra"})],
            idempotency_key: None,
        },
    )
    .await
    .expect_err("mismatched row is rejected");
    let message = err.to_string();
    assert!(
        message.contains("surprise"),
        "the error names the offending column: {message}"
    );

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(snap.record_count, 0, "the rejected commit wrote nothing");
}

/// Acceptance: the custom-data table the platform integration creates — an
/// explicit ten-column schema with a nullable `symbol_value`, partitioned by
/// `day` — is created and committed to through the same path.
#[tokio::test]
async fn custom_data_table_with_explicit_schema_and_day_partition() {
    let catalog = memory_catalog().await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("provider", DataType::Utf8, false),
        Field::new("venue", DataType::Utf8, false),
        Field::new("feed", DataType::Utf8, false),
        Field::new("symbol_value", DataType::Utf8, true),
        Field::new("time_ns", DataType::Int64, false),
        Field::new("end_time_ns", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("fields_json", DataType::Utf8, false),
        Field::new("resolution", DataType::Utf8, false),
        Field::new("day", DataType::Utf8, false),
    ]));
    let ident = create(catalog.as_ref(), "market.trades", &schema, Some("day")).await;

    // The table is partitioned by day.
    let table = catalog.load_table(&ident).await.expect("load");
    let partition_cols: Vec<String> = table
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert_eq!(partition_cols, vec!["day".to_owned()]);

    let response = tables_api::commit(
        catalog.as_ref(),
        &ident,
        CommitRequest {
            rows: vec![
                json!({
                    "provider": "acme", "venue": "xnas", "feed": "trades",
                    "symbol_value": "ABC", "time_ns": 1, "end_time_ns": 2,
                    "value": "10.5", "fields_json": "{}", "resolution": "tick",
                    "day": "2026-07-20"
                }),
                json!({
                    "provider": "acme", "venue": "xnas", "feed": "trades",
                    "time_ns": 3, "end_time_ns": 4,
                    "value": "11.0", "fields_json": "{}", "resolution": "tick",
                    "day": "2026-07-21"
                }),
            ],
            idempotency_key: None,
        },
    )
    .await
    .expect("commit");
    assert_eq!(response.rows_committed, 2);

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(snap.record_count, 2);

    let scanned = tables_api::rows(catalog.as_ref(), &ident, None, None)
        .await
        .expect("rows");
    assert_eq!(scanned.rows.len(), 2);
    let days: std::collections::HashSet<&str> = scanned
        .rows
        .iter()
        .filter_map(|r: &Value| r["day"].as_str())
        .collect();
    assert_eq!(days.len(), 2);
}
