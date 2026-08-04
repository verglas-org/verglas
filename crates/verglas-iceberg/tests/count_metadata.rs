//! Acceptance tests for the metadata-answered `count(*)` fast path and the
//! bounded scan fallback (tiny-files defect).
//!
//! These run in-process against an Iceberg `MemoryCatalog`. A table built from
//! many single-record appends stands in for the production table with thousands
//! of tiny data files.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder};
use verglas_iceberg::{parse_table_ident, query, write};

/// Builds an in-process memory catalog over a fresh temp warehouse, kept alive
/// for the catalog's lifetime.
async fn catalog_at() -> Arc<dyn Catalog> {
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

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("sym", DataType::Utf8, true),
    ]))
}

fn one_row(i: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![i])),
            Arc::new(StringArray::from(vec![Some(format!("S{i}"))])),
        ],
    )
    .expect("batch")
}

/// Creates `rlean.custom_points` and appends `n` single-record data files.
async fn build_tiny_files(n: i64) -> Arc<dyn Catalog> {
    let catalog = catalog_at().await;
    let ident = parse_table_ident("rlean.custom_points").expect("ident");
    write::create_table_from_schema(catalog.as_ref(), &ident, &schema(), None)
        .await
        .expect("create");
    for i in 0..n {
        write::append_batches(catalog.as_ref(), &ident, vec![one_row(i)], HashMap::new())
            .await
            .expect("append");
    }
    catalog
}

/// The count in a single-column, single-row report.
fn scalar(report: &verglas_iceberg::QueryReport, column: &str) -> i64 {
    assert_eq!(report.row_count, 1, "count is one row");
    assert_eq!(report.columns, vec![column.to_string()], "column name");
    report.rows[0]
        .get(column)
        .and_then(|v| v.as_i64())
        .expect("integer count")
}

/// Acceptance: an unfiltered `count(*)` over a many-tiny-file table returns the
/// true row count, and the result column keeps the query's alias.
#[tokio::test]
async fn count_star_counts_all_rows() {
    let catalog = build_tiny_files(240).await;
    let report = query::query(
        catalog,
        "SELECT count(*) AS n FROM rlean.custom_points",
        None,
    )
    .await
    .expect("query");
    assert_eq!(scalar(&report, "n"), 240);
}

/// The metadata fast path agrees with the ground-truth full scan. A bare
/// `count(*)` (fast path) and an all-rows filtered `count(*)` (which defeats the
/// fast path and scans every file) return the same total.
#[tokio::test]
async fn count_star_matches_forced_scan() {
    let catalog = build_tiny_files(200).await;
    let fast = query::query(
        catalog.clone(),
        "SELECT count(*) FROM rlean.custom_points",
        None,
    )
    .await
    .expect("fast count");
    let scanned = query::query(
        catalog,
        "SELECT count(*) FROM rlean.custom_points WHERE id >= -1",
        None,
    )
    .await
    .expect("scanned count");
    assert_eq!(scalar(&fast, "count(*)"), 200);
    assert_eq!(scalar(&scanned, "count(*)"), 200);
}

/// `count(1)` takes the fast path too, and a filtered count falls back to the
/// scan and returns the correct filtered total.
#[tokio::test]
async fn count_one_fast_path_and_filtered_fallback() {
    let catalog = build_tiny_files(120).await;
    // DataFusion normalizes `count(1)`'s output name to `count(Int64(1))`.
    let all = query::query(
        catalog.clone(),
        "SELECT count(1) FROM rlean.custom_points",
        None,
    )
    .await
    .expect("count(1)");
    assert_eq!(scalar(&all, "count(Int64(1))"), 120);

    let filtered = query::query(
        catalog,
        "SELECT count(*) AS c FROM rlean.custom_points WHERE id < 30",
        None,
    )
    .await
    .expect("filtered count");
    assert_eq!(scalar(&filtered, "c"), 30);
}

/// `count(NULL)` counts no rows (a null is never counted). It must not take the
/// all-rows fast path; the scan returns zero over a non-empty table.
#[tokio::test]
async fn count_null_literal_is_zero() {
    let catalog = build_tiny_files(50).await;
    let report = query::query(
        catalog,
        "SELECT count(NULL) AS c FROM rlean.custom_points",
        None,
    )
    .await
    .expect("count(NULL)");
    assert_eq!(scalar(&report, "c"), 0);
}

/// `count(column)` is not the fast path (it skips nulls). With a null in the
/// column it must return the non-null count, proving it did not take the
/// all-rows metadata shortcut.
#[tokio::test]
async fn count_column_skips_nulls_via_scan() {
    let catalog = catalog_at().await;
    let ident = parse_table_ident("rlean.custom_points").expect("ident");
    write::create_table_from_schema(catalog.as_ref(), &ident, &schema(), None)
        .await
        .expect("create");
    // Two rows, one with a null `sym`.
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a".to_string()), None])),
        ],
    )
    .expect("batch");
    write::append_batches(catalog.as_ref(), &ident, vec![batch], HashMap::new())
        .await
        .expect("append");

    let star = query::query(
        catalog.clone(),
        "SELECT count(*) FROM rlean.custom_points",
        None,
    )
    .await
    .expect("count(*)");
    assert_eq!(scalar(&star, "count(*)"), 2);

    let col = query::query(
        catalog,
        "SELECT count(sym) AS c FROM rlean.custom_points",
        None,
    )
    .await
    .expect("count(sym)");
    assert_eq!(scalar(&col, "c"), 1, "count(column) skips the null");
}
