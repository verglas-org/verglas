//! Acceptance for compaction as a manual daemon mechanism.
//!
//! The daemon does not register or schedule compaction. Policy belongs to a
//! container-backed worker; this test covers only the explicit admin trigger.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use tempfile::TempDir;
use tower::ServiceExt;
use verglas_iceberg::compaction::DEFAULT_MIN_SMALL_FILES;
use verglas_iceberg::{inspect, parse_table_ident, write};
use verglasd::admin::{self, Health, Slots, TablesSlot};

/// A hermetic catalog plus the temp dirs backing it.
struct TestCatalog {
    catalog: Arc<dyn Catalog>,
    _warehouse: TempDir,
    _db_dir: TempDir,
}

impl TestCatalog {
    /// Builds a fresh hermetic SQLite catalog with a local-filesystem warehouse.
    async fn new() -> TestCatalog {
        let warehouse = TempDir::new().expect("warehouse temp dir");
        let db_dir = TempDir::new().expect("sqlite temp dir");
        let db_path = db_dir.path().join("catalog.db");
        let uri = format!("sqlite:{}?mode=rwc", db_path.display());
        let warehouse_uri = format!("file://{}", warehouse.path().display());
        let catalog = SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "compaction-test",
                HashMap::from_iter([
                    (SQL_CATALOG_PROP_URI.to_string(), uri),
                    (SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri),
                    (
                        SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                        SqlBindStyle::QMark.to_string(),
                    ),
                ]),
            )
            .await
            .expect("build hermetic sqlite catalog");
        TestCatalog {
            catalog: Arc::new(catalog),
            _warehouse: warehouse,
            _db_dir: db_dir,
        }
    }
}

/// Writes `contents` to a temp CSV, returning its path and retaining its directory.
fn source_file(name: &str, contents: &str) -> PathBuf {
    let dir = TempDir::new().expect("source tempdir");
    let path = dir.path().join(name);
    let mut file = std::fs::File::create(&path).expect("create source");
    file.write_all(contents.as_bytes()).expect("write source");
    file.flush().expect("flush source");
    std::mem::forget(dir);
    path
}

/// Returns the live data-file count of a table's current snapshot.
async fn file_count(catalog: &dyn Catalog, ident: &TableIdent) -> u64 {
    inspect::show(catalog, ident)
        .await
        .expect("show")
        .file_count
        .expect("a file count")
}

/// Creates a table with enough single-row files to clear the compaction floor.
async fn table_with_small_files(catalog: &dyn Catalog, dotted: &str) -> (TableIdent, usize) {
    let ident = parse_table_ident(dotted).expect("ident");
    let files = DEFAULT_MIN_SMALL_FILES + 4;
    write::create_table(
        catalog,
        &ident,
        &source_file("seed.csv", "k,v\n0,zero\n"),
        None,
    )
    .await
    .expect("create");
    for i in 1..files {
        write::append(
            catalog,
            &ident,
            &source_file("more.csv", &format!("k,v\n{i},row{i}\n")),
        )
        .await
        .expect("append");
    }
    let before = file_count(catalog, &ident).await;
    assert_eq!(before as usize, files, "every append is its own file");
    (ident, files)
}

/// The manual route compacts a table and returns the committed pass report.
#[tokio::test]
async fn compact_admin_route_runs_a_pass_and_returns_a_report() {
    let tc = TestCatalog::new().await;
    let catalog = tc.catalog.clone();
    let (ident, before) = table_with_small_files(catalog.as_ref(), "rlean.on_demand").await;

    let slot: TablesSlot = Arc::new(OnceLock::new());
    let _ = slot.set(catalog.clone());
    let app = admin::router(
        verglasd::VERSION,
        Health::ready(),
        Slots {
            tables: Some(slot),
            ..Slots::default()
        },
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/compact")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .expect("request"),
        )
        .await
        .expect("router responds");
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let report: serde_json::Value = serde_json::from_slice(&bytes).expect("json report");
    assert!(
        report["tables_scanned"].as_u64().expect("tables_scanned") >= 1,
        "the pass examined at least our table"
    );
    let ours = report["compacted"]
        .as_array()
        .expect("compacted array")
        .iter()
        .find(|entry| entry["table"] == "rlean.on_demand")
        .expect("our table was compacted");
    assert!(
        ours["output_data_files"].as_u64().expect("output count")
            < ours["input_data_files"].as_u64().expect("input count"),
        "the rewrite produced fewer files than it read"
    );
    assert!(
        ours["snapshot_id"].as_i64().is_some(),
        "a REPLACE snapshot committed"
    );
    let after = file_count(catalog.as_ref(), &ident).await;
    assert!(
        (after as usize) < before,
        "the manual pass reduced the file count: {before} -> {after}"
    );
}
