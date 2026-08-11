//! Integration tests for the `verglas-query` role's HTTP surface, over real
//! HTTP (a bound TCP listener + `axum::serve`, driven with `reqwest`) against
//! a real Iceberg table (an in-process `MemoryCatalog` over a local-filesystem
//! warehouse, built the same way `crates/verglas-iceberg/tests/table_verbs.rs`
//! builds its fixtures) — the same real-router-over-real-HTTP technique
//! `bins/verglas-server/tests` already uses to test its own admin routes without
//! spawning a subprocess.
//!
//! # What this does and does not cover
//!
//! This proves `verglas-query`'s own admin.rs/serve.rs/sizing.rs — the new
//! code this PR adds — end to end: `/v1/query` and `/v1/query/estimate` both
//! answer over real HTTP against a real table, a query whose estimate exceeds
//! the initial grant grows the held grant before running (the grant flow
//! tonight's design centers on), and — `a_large_result_completes_by_streaming_not_buffering`
//! — a result far bigger than a deliberately tiny starting grant still
//! completes, because `/v1/query` streams its response instead of collecting
//! the whole result first.
//!
//! It does **not** spawn `verglas-server` and `verglas-query` as two separate OS
//! processes talking over a real Iceberg REST catalog. That would need a real
//! REST catalog *service* reachable over HTTP: `verglas_iceberg::catalog::open_catalog`
//! (used by both the server's embedded path and this binary) only builds a
//! REST catalog client — there is no in-repo REST catalog *server* to point it
//! at. Issue #294 ("CI: add an Iceberg REST catalog service for true
//! end-to-end verb tests") tracks exactly this gap and is still open; standing
//! one up is out of scope for this PR. The server-side dispatch wiring
//! (`bins/verglas-server/src/query_worker.rs`, `admin::query_sql`'s dispatch-then-
//! fallback) is covered instead by `verglas-server`'s existing in-process router
//! tests (`bins/verglas-server/tests/admin.rs`, `query_route_*`), which pass
//! unchanged with the dispatcher wired in as `None` — proving the embedded
//! fallback path the dispatch code always has to degrade to correctly.
//!
//! Format coverage: fixtures here are Parquet, matching the scope of
//! `crates/verglas-iceberg/tests/table_verbs.rs` (this repo's Rust write path,
//! `verglas_iceberg::write`, only produces Parquet data files — Avro data
//! files need the JVM-based builder `tests/engine-matrix` uses, which is not
//! reachable from `cargo test`). Avro *read* correctness for the embedded
//! DataFusion/iceberg-datafusion engine is proven separately by that harness;
//! nothing in the query-role split touches format-specific code, so it is not
//! re-proven here.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use futures::StreamExt;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder};
use tokio::sync::Mutex;
use verglas_iceberg::{QueryReport, parse_table_ident, write};
use verglas_query::admin::{self, AppState};
use verglas_sdk::grant::{LocalGrantHost, MemoryGrant};

/// Builds an in-process memory catalog over a fresh temp warehouse path, and a
/// table with `rows` wide-ish CSV rows in it — enough real bytes on disk that
/// a plan-based estimate has something meaningful to size against, matching
/// the fixture shape `crates/verglas-iceberg/src/estimate.rs`'s own tests use.
async fn catalog_with_table(ident: &iceberg::TableIdent, rows: u64) -> Arc<dyn Catalog> {
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
    let catalog: Arc<dyn Catalog> = Arc::new(catalog);

    let mut body = "id,amount,customer,note\n".to_owned();
    for i in 0..rows {
        body.push_str(&format!(
            "{i},{amt}.00,customer-{i},a bit of padding text to give this row real width\n",
            amt = i % 1000
        ));
    }
    let dir = tempfile::tempdir().expect("source tempdir");
    let path = dir.path().join("rows.csv");
    let mut file = std::fs::File::create(&path).expect("create source");
    file.write_all(body.as_bytes()).expect("write source");
    file.flush().expect("flush source");
    std::mem::forget(dir);

    write::create_table(catalog.as_ref(), ident, &path, None)
        .await
        .expect("create table");
    catalog
}

/// Serves `verglas-query`'s real admin router on a real bound port, backed by
/// `catalog` and starting from `initial_grant` bytes. Returns the base URL and
/// the state handle (so tests can inspect the grant after requests).
async fn serve(catalog: Arc<dyn Catalog>, initial_grant_bytes: u64) -> (String, AppState) {
    let prepared_catalog = verglas_query::admin::PreparedQueryCatalog::open(
        catalog.clone(),
        None,
        None,
        64 * 1024 * 1024,
        None,
    )
    .await
    .expect("prepare catalog");
    let state = AppState {
        catalog,
        prepared_catalog,
        grant_host: Arc::new(LocalGrantHost),
        grant: Arc::new(Mutex::new(MemoryGrant {
            bytes: initial_grant_bytes,
        })),
        last_activity: Arc::new(AtomicU64::new(0)),
        estimate_on_request: true,
    };
    let app = admin::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), state)
}

/// `GET /healthz` answers as soon as the router is serving — this binary has
/// no recovery phase to wait on.
#[tokio::test]
async fn healthz_answers_ok() {
    let ident = parse_table_ident("sales.orders").expect("ident");
    let catalog = catalog_with_table(&ident, 10).await;
    let (base, _state) = serve(catalog, 64 * 1024 * 1024).await;

    let status = reqwest::get(format!("{base}/healthz"))
        .await
        .expect("request")
        .status();
    assert!(status.is_success());
}

/// `POST /v1/query` runs real SQL over real HTTP against the real table and
/// returns the stable `{columns, rows, row_count}` report.
#[tokio::test]
async fn query_runs_sql_over_http() {
    let ident = parse_table_ident("sales.orders").expect("ident");
    let catalog = catalog_with_table(&ident, 25).await;
    let (base, _state) = serve(catalog, 64 * 1024 * 1024).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{base}/v1/query"))
        .json(&serde_json::json!({"sql": "SELECT COUNT(*) AS n FROM sales.orders"}))
        .send()
        .await
        .expect("request");
    assert!(
        response.status().is_success(),
        "status: {}",
        response.status()
    );
    let report: QueryReport = response.json().await.expect("report json");
    assert_eq!(report.row_count, 1);
    assert_eq!(report.rows[0].get("n").and_then(|v| v.as_i64()), Some(25));
}

/// Arrow clients receive a bounded IPC stream directly from the query role.
#[tokio::test]
async fn query_streams_arrow_ipc_when_requested() {
    let ident = parse_table_ident("sales.orders").expect("ident");
    let catalog = catalog_with_table(&ident, 25).await;
    let (base, _state) = serve(catalog, 64 * 1024 * 1024).await;

    let response = reqwest::Client::new()
        .post(format!("{base}/v1/query"))
        .header("accept", "application/vnd.apache.arrow.stream")
        .json(&serde_json::json!({"sql": "SELECT * FROM sales.orders"}))
        .send()
        .await
        .expect("request");
    assert!(response.status().is_success());
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/vnd.apache.arrow.stream")
    );
    let bytes = response.bytes().await.expect("Arrow body");
    let rows = arrow_ipc::reader::StreamReader::try_new(bytes.as_ref(), None)
        .expect("Arrow stream")
        .map(|batch| batch.expect("batch").num_rows())
        .sum::<usize>();
    assert_eq!(rows, 25);
}

/// A statement the engine cannot plan is a 400 — the caller's bad input, not a
/// server error — matching the embedded server path's behavior.
#[tokio::test]
async fn query_bad_sql_is_a_400() {
    let ident = parse_table_ident("sales.orders").expect("ident");
    let catalog = catalog_with_table(&ident, 5).await;
    let (base, _state) = serve(catalog, 64 * 1024 * 1024).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{base}/v1/query"))
        .json(&serde_json::json!({"sql": "SELECT * FROM sales.nonexistent"}))
        .send()
        .await
        .expect("request");
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
}

/// `POST /v1/query/estimate` answers the plan-based memory estimate without
/// running the query — the table is untouched (still queryable for the exact
/// same count afterward), and the estimate reports the documented caveats.
#[tokio::test]
async fn estimate_answers_without_executing() {
    let ident = parse_table_ident("sales.orders").expect("ident");
    let catalog = catalog_with_table(&ident, 25).await;
    let (base, _state) = serve(catalog, 64 * 1024 * 1024).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{base}/v1/query/estimate"))
        .json(&serde_json::json!({"sql": "SELECT * FROM sales.orders"}))
        .send()
        .await
        .expect("request");
    assert!(response.status().is_success());
    let estimate: verglas_iceberg::QueryMemoryEstimate =
        response.json().await.expect("estimate json");
    assert!(
        estimate.scan_bytes > 0,
        "a real table should have real scan bytes"
    );
    assert!(estimate.approximate);
    assert!(!estimate.notes.is_empty());

    // The table is unaffected: a real query afterward still sees every row.
    let count_response = client
        .post(format!("{base}/v1/query"))
        .json(&serde_json::json!({"sql": "SELECT COUNT(*) AS n FROM sales.orders"}))
        .send()
        .await
        .expect("request");
    let report: QueryReport = count_response.json().await.expect("report json");
    assert_eq!(report.rows[0].get("n").and_then(|v| v.as_i64()), Some(25));
}

/// A query whose estimate exceeds the initial grant grows the held grant
/// before running — the grow-only escape hatch this PR's design centers the
/// grant flow on. Started at a floor far below what a real scan needs.
#[tokio::test]
async fn a_large_query_grows_the_grant() {
    let ident = parse_table_ident("sales.orders").expect("ident");
    let catalog = catalog_with_table(&ident, 2000).await;
    let floor = 1024; // 1 KiB: certainly below this table's real scan size.
    let (base, state) = serve(catalog, floor).await;

    {
        let grant = state.grant.lock().await;
        assert_eq!(grant.bytes, floor);
    }

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{base}/v1/query"))
        .json(&serde_json::json!({"sql": "SELECT * FROM sales.orders"}))
        .send()
        .await
        .expect("request");
    assert!(response.status().is_success());

    let grant = state.grant.lock().await;
    assert!(
        grant.bytes > floor,
        "grant should have grown past the {floor}-byte floor, got {}",
        grant.bytes
    );
}

/// `--version` works without a config file — the one thing this binary does
/// before requiring one — proving the actual compiled binary (not just the
/// library) runs.
#[tokio::test]
async fn binary_reports_its_version() {
    let binary = env!("CARGO_BIN_EXE_verglas-query");
    let output = tokio::process::Command::new(binary)
        .arg("--version")
        .output()
        .await
        .expect("spawn");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("verglas-query"), "{stdout}");
}

/// A query returning many more rows than the worker's tiny initial memory
/// grant still completes successfully — the wire contract this PR's
/// coordinator flagged as load-bearing: `/v1/query` streams its response
/// (one JSON chunk per Arrow `RecordBatch`, see `admin.rs`'s module doc) so a
/// large result never *requires* the whole thing to sit in memory before the
/// first byte goes out. This is checked two ways: (1) end to end, the request
/// succeeds and every row comes back correct, starting from a 1 KiB grant
/// nowhere near big enough to hold a fully materialized 50,000-row JSON body;
/// (2) structurally, the raw HTTP body arrives over more than one
/// `bytes_stream()` chunk with no single chunk anywhere near the total body
/// size — evidence the server wrote it incrementally rather than handing the
/// transport one already-complete buffer. (The airtight proof that execution
/// itself never collects upfront is the white-box
/// `query_stream_yields_multiple_batches_without_collecting` test in
/// `crates/verglas-iceberg/tests/table_verbs.rs`; this test is the black-box
/// companion, over the real HTTP path this PR adds.)
#[tokio::test]
async fn a_large_result_completes_by_streaming_not_buffering() {
    let ident = parse_table_ident("sales.huge").expect("ident");
    let rows = 50_000;
    let catalog = catalog_with_table(&ident, rows).await;
    let floor = 1024; // 1 KiB — far smaller than the eventual JSON body.
    let (base, _state) = serve(catalog, floor).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{base}/v1/query"))
        .json(&serde_json::json!({"sql": "SELECT * FROM sales.huge"}))
        .send()
        .await
        .expect("request");
    assert!(
        response.status().is_success(),
        "status: {}",
        response.status()
    );

    let mut stream = response.bytes_stream();
    let mut chunk_count = 0usize;
    let mut chunk_sizes = Vec::new();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("chunk");
        chunk_count += 1;
        chunk_sizes.push(chunk.len());
        body.extend_from_slice(&chunk);
    }

    assert!(
        chunk_count > 1,
        "a 50,000-row result should arrive over more than one HTTP chunk, got {chunk_count}"
    );
    let total = body.len();
    let largest_chunk = chunk_sizes.iter().copied().max().unwrap_or(0);
    assert!(
        largest_chunk < total / 2,
        "no single chunk ({largest_chunk} bytes) should hold most of the {total}-byte body — \
         that would mean the server wrote it as one buffered blob, not incrementally"
    );

    let report: QueryReport = serde_json::from_slice(&body).expect("report json");
    assert_eq!(report.row_count, rows as usize);
    assert_eq!(report.rows.len(), rows as usize);
}

/// A spawned worker with no reachable catalog fails fast and cleanly — no
/// hang, a non-zero exit, an actionable message on stderr — rather than
/// serving broken. Exercises `main.rs`'s connection/catalog-open error path in
/// the real compiled binary.
#[tokio::test]
async fn binary_exits_cleanly_on_an_unreachable_catalog() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path: PathBuf = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        "[cache]\ns3_endpoint = \"http://127.0.0.1:1\"\n\n\
         [metadata]\nuri = \"http://127.0.0.1:1\"\n",
    )
    .expect("write config");

    let binary = env!("CARGO_BIN_EXE_verglas-query");
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        tokio::process::Command::new(binary)
            .arg("--config")
            .arg(&config_path)
            .arg("estimate")
            .arg("--sql")
            .arg("SELECT 1")
            .output(),
    )
    .await
    .expect("did not hang")
    .expect("spawn");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("verglas-query"), "{stderr}");
}
