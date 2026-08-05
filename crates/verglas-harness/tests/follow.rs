//! End-to-end follow-mode tests against a hermetic SQLite-backed catalog: run a
//! real command, capture its stdout and stderr as rows, and read them back.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use tempfile::TempDir;
use tokio::sync::watch;

use verglas_harness::follow::{FollowEnd, FollowSource, follow_table_ident, run_follow};
use verglas_harness::worker::WorkerExec;

/// A hermetic catalog plus the temp dirs backing it (kept alive for the test).
struct TestCatalog {
    catalog: Arc<dyn Catalog>,
    _warehouse: TempDir,
    _db_dir: TempDir,
}

async fn hermetic_catalog() -> TestCatalog {
    let warehouse = TempDir::new().expect("warehouse temp dir");
    let db_dir = TempDir::new().expect("sqlite temp dir");
    let db_path = db_dir.path().join("catalog.db");
    let uri = format!("sqlite:{}?mode=rwc", db_path.display());
    let warehouse_uri = format!("file://{}", warehouse.path().display());
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "follow-test",
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

/// A wrapped command that exits captures its stdout and stderr as follow rows,
/// creating the target table on first write, and reports Completed.
#[tokio::test]
async fn command_follow_captures_stdout_and_stderr() {
    let tc = hermetic_catalog().await;
    let ident = follow_table_ident("logs.app").expect("ident");

    // A shell that prints one line to stdout and one to stderr, then exits.
    let exec = WorkerExec {
        command: "sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            "echo hello-out; echo hello-err 1>&2".to_owned(),
        ],
        cwd: None,
        env: Default::default(),
    };
    // The command exits on its own, so shutdown is never signalled.
    let (_tx, rx) = watch::channel(false);
    let end = run_follow(
        tc.catalog.clone(),
        ident.clone(),
        "tail-app".to_owned(),
        FollowSource::Command(exec),
        rx,
    )
    .await;
    assert_eq!(end, FollowEnd::Completed);

    let page = verglas_iceberg::tables_api::rows(tc.catalog.as_ref(), &ident, None, None)
        .await
        .expect("read rows");
    let lines: Vec<(String, String)> = page
        .rows
        .iter()
        .map(|r| {
            (
                r["stream"].as_str().unwrap_or_default().to_owned(),
                r["line"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    assert!(
        lines.contains(&("stdout".to_owned(), "hello-out".to_owned())),
        "stdout line captured: {lines:?}"
    );
    assert!(
        lines.contains(&("stderr".to_owned(), "hello-err".to_owned())),
        "stderr line captured: {lines:?}"
    );
    // Every row carries the worker identity and the fixed columns.
    for r in &page.rows {
        assert_eq!(r["worker"].as_str(), Some("tail-app"));
        assert!(r["run_id"].as_str().is_some());
        assert!(r["seq"].as_i64().is_some());
        assert!(r["ts"].as_str().is_some() || r["ts"].is_number());
        assert!(r["day"].as_str().is_some());
    }
}

/// A shutdown signal ends a still-running follow and flushes what was captured.
#[tokio::test]
async fn shutdown_ends_a_running_follow() {
    let tc = hermetic_catalog().await;
    let ident = follow_table_ident("logs.svc").expect("ident");

    // A long-lived command: print one line, then sleep well past the test.
    let exec = WorkerExec {
        command: "sh".to_owned(),
        args: vec!["-c".to_owned(), "echo up; sleep 30".to_owned()],
        cwd: None,
        env: Default::default(),
    };
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(run_follow(
        tc.catalog.clone(),
        ident.clone(),
        "svc".to_owned(),
        FollowSource::Command(exec),
        rx,
    ));

    // Wait for the first line to be captured (the table appears on first flush),
    // then request shutdown. No fixed sleep decides correctness: we poll until
    // the row is visible, bounded by a generous deadline.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if let Ok(page) =
            verglas_iceberg::tables_api::rows(tc.catalog.as_ref(), &ident, None, None).await
            && !page.rows.is_empty()
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "first line never landed"
        );
        tokio::task::yield_now().await;
    }

    tx.send(true).expect("signal shutdown");
    let end = handle.await.expect("join follow");
    assert_eq!(end, FollowEnd::ShutdownRequested);

    let page = verglas_iceberg::tables_api::rows(tc.catalog.as_ref(), &ident, None, None)
        .await
        .expect("read rows");
    assert!(
        page.rows.iter().any(|r| r["line"].as_str() == Some("up")),
        "the captured line survived shutdown"
    );
}
