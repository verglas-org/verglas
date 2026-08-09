//! The CLI keeps catalog metadata and Verglas execution on separate paths (#3).
//!
//! These stand up separate server and catalog routes on one ephemeral mock and
//! drive the real binary against it. Metadata reads go directly to the Iceberg
//! REST routes while execution requests go to Verglas. The server-down test
//! proves execution has no embedded fallback.

use std::net::TcpListener;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde_json::{Value, json};

/// What the mock server records: how many ingest bytes arrived, so the test
/// can prove the CLI streamed the file body.
#[derive(Clone, Default)]
struct MockState {
    ingest_bytes: Arc<AtomicUsize>,
    /// The last graph-query body the mock received, so a test can assert the CLI
    /// assembled the traversal request from its flags (the CLI reserializes the
    /// typed response, so request fields cannot be echoed back through it).
    last_query: Arc<std::sync::Mutex<Option<Value>>>,
}

/// Builds the mock server router: the table, query, and graph routes the CLI
/// verbs are expected to call.
fn mock_server(state: MockState) -> Router {
    Router::new()
        .route(
            "/v1/config",
            get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }),
        )
        .route(
            "/v1/namespaces",
            get(|| async { Json(json!({"namespaces": [["sales"]]})) }),
        )
        .route(
            "/v1/namespaces/{ns}/tables",
            get(|AxumPath(ns): AxumPath<String>| async move {
                Json(json!({"identifiers": [{"namespace": [ns], "name": "orders"}]}))
            }),
        )
        .route(
            "/v1/namespaces/{ns}/tables/{name}",
            get(
                |AxumPath((_ns, _name)): AxumPath<(String, String)>| async move {
                    Json(json!({"metadata": {
                        "current-schema-id": 0,
                        "schemas": [{
                            "schema-id": 0,
                            "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]
                        }],
                        "default-spec-id": 0,
                        "partition-specs": [{"spec-id": 0, "fields": []}],
                        "current-snapshot-id": 42,
                        "snapshots": [{
                            "snapshot-id": 42,
                            "timestamp-ms": 1753305600000i64,
                            "summary": {
                                "operation": "append",
                                "total-records": "3",
                                "total-data-files": "1",
                                "total-files-size": "128"
                            }
                        }]
                    }}))
                },
            ),
        )
        .route(
            "/v1/tables",
            get(|| async { Json(json!({"tables": [{"namespace": "sales", "name": "orders"}]})) }),
        )
        .route(
            "/v1/tables/{name}/describe",
            get(|AxumPath(name): AxumPath<String>| async move {
                Json(json!({
                    "table": name,
                    "schema": [
                        {"id": 1, "name": "id", "type": "long", "required": true}
                    ],
                    "partition_by": [],
                    "row_count": 3,
                    "file_count": 1,
                    "size_bytes": 128,
                    "current_snapshot_id": 42,
                }))
            }),
        )
        .route(
            "/v1/tables/{name}/history",
            get(|AxumPath(name): AxumPath<String>| async move {
                Json(json!({
                    "table": name,
                    "snapshots": [{
                        "snapshot_id": 42,
                        "parent_snapshot_id": null,
                        "timestamp_ms": 1753305600000i64,
                        "timestamp": "2026-07-24T00:00:00Z",
                        "operation": "append",
                        "summary": {},
                    }],
                }))
            }),
        )
        .route(
            "/v1/ingest/{name}",
            post(
                |AxumPath(name): AxumPath<String>,
                 Query(query): Query<Value>,
                 State(state): State<MockState>,
                 body: Bytes| async move {
                    state.ingest_bytes.fetch_add(body.len(), Ordering::SeqCst);
                    let mode = query
                        .get("mode")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned();
                    if mode == "create" {
                        Json(json!({
                            "table": name,
                            "operation": "create",
                            "schema": [
                                {"id": 1, "name": "id", "type": "long", "required": false}
                            ],
                            "partition_by": [],
                            "records_added": 2,
                            "data_files_added": 1,
                            "snapshot_id": 7,
                        }))
                        .into_response()
                    } else {
                        Json(json!({
                            "table": name,
                            "operation": "append",
                            "records_added": 1,
                            "data_files_added": 1,
                            "snapshot_id": 8,
                        }))
                        .into_response()
                    }
                },
            ),
        )
        .route(
            "/v1/databases/analytics/query",
            post(|Json(body): Json<Value>| async move {
                assert_eq!(body["sql"], "SELECT 1 AS n");
                assert_eq!(
                    body["at"],
                    json!({"reference": "42", "table": "sales.orders"})
                );
                Json(json!({
                    "columns": ["n"],
                    "rows": [{"n": 1}],
                    "row_count": 1,
                }))
            }),
        )
        // The graph verb-family routes: create/show on the namespace path,
        // node/edge insert, index build, and the traversal query.
        .route(
            "/v1/graphs/{ns}",
            post(|AxumPath(ns): AxumPath<String>| async move {
                Json(json!({
                    "namespace": ns,
                    "nodesTable": format!("{ns}.nodes"),
                    "edgesTable": format!("{ns}.edges"),
                }))
            })
            .get(|AxumPath(ns): AxumPath<String>| async move {
                Json(json!({
                    "namespace": ns,
                    "nodesTable": format!("{ns}.nodes"),
                    "edgesTable": format!("{ns}.edges"),
                    "nodeCount": 2,
                    "edgeCount": 1,
                    "indexed": true,
                    "snapshotId": 11,
                }))
            }),
        )
        .route(
            "/v1/graphs/{ns}/nodes",
            post(|Json(body): Json<Value>| async move {
                let count = body["nodes"].as_array().map(|a| a.len()).unwrap_or(0);
                Json(json!({"snapshotId": 5, "count": count}))
            }),
        )
        .route(
            "/v1/graphs/{ns}/edges",
            post(|Json(body): Json<Value>| async move {
                let count = body["edges"].as_array().map(|a| a.len()).unwrap_or(0);
                Json(json!({"snapshotId": 6, "count": count}))
            }),
        )
        .route(
            "/v1/graphs/{ns}/index",
            post(|| async {
                Json(json!({
                    "built": true,
                    "snapshotId": 6,
                    "nodeCount": 2,
                    "edgeCount": 1,
                    "blobPath": "s3://x/idx.puffin",
                    "blobBytes": 128,
                    "mode": "full",
                }))
            }),
        )
        .route(
            "/v1/graphs/{ns}/query",
            post(
                |State(state): State<MockState>, Json(body): Json<Value>| async move {
                    // Record the traversal request so the test can assert the CLI
                    // built it from the flags, then answer per op.
                    *state.last_query.lock().expect("lock") = Some(body.clone());
                    let op = body["op"].as_str().unwrap_or("");
                    let mut response = json!({
                        "op": op,
                        "backend": "scan",
                        "snapshotId": 6,
                    });
                    match op {
                        "neighbors" => {
                            response["neighbors"] = json!([
                                {"nodeId":"b","predicate":"knows","confidence":1.0,
                                 "edgeId":"e1","provenance":"m1","direction":"out"}
                            ])
                        }
                        "kHop" => {
                            response["reached"] = json!([
                                {"nodeId":"b","hops":1,"pathConfidence":1.0},
                                {"nodeId":"c","hops":2,"pathConfidence":0.5}
                            ])
                        }
                        "paths" => {
                            response["paths"] = json!([
                                {"nodes":["a","c"],"edges":[],"confidence":0.5}
                            ])
                        }
                        _ => {}
                    }
                    Json(response)
                },
            ),
        )
        .with_state(state)
}

/// Binds the mock server on an ephemeral port and serves it from a background
/// runtime thread. Returns the base URL.
fn spawn_mock(state: MockState) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock");
    let addr = listener.local_addr().expect("mock addr");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let app = mock_server(state);
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("mock runtime");
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, app).await.expect("serve mock");
        });
    });
    format!("http://{addr}")
}

/// Runs the CLI against `endpoint` and returns (status, stdout, stderr).
fn run_cli(endpoint: &str, args: &[&str]) -> (std::process::ExitStatus, String, String) {
    let home = tempfile::tempdir().expect("temporary CLI home");
    let config_dir = home.path().join(".verglas");
    std::fs::create_dir_all(&config_dir).expect("create config directory");
    std::fs::write(
        config_dir.join("config.toml"),
        format!("[catalog]\nuri = \"{endpoint}\"\n"),
    )
    .expect("write catalog config");
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("--server-endpoint")
        .arg(endpoint)
        .args(args)
        .env("HOME", home.path())
        .output()
        .expect("run verglas");
    (
        out.status,
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Metadata reads speak Iceberg REST directly, while create, append, and query
/// use the server execution routes.
#[test]
fn metadata_uses_catalog_and_execution_uses_server() {
    let state = MockState::default();
    let endpoint = spawn_mock(state.clone());

    let (status, stdout, stderr) = run_cli(&endpoint, &["--json", "table", "list"]);
    assert!(status.success(), "table list failed: {stderr}");
    let v: Value = serde_json::from_str(&stdout).expect("list json");
    assert_eq!(v["tables"][0]["name"], "orders");

    let (status, stdout, _) = run_cli(&endpoint, &["--json", "table", "show", "sales.orders"]);
    assert!(status.success());
    let v: Value = serde_json::from_str(&stdout).expect("show json");
    assert_eq!(v["row_count"], 3);

    let (status, stdout, _) = run_cli(&endpoint, &["--json", "table", "history", "sales.orders"]);
    assert!(status.success());
    let v: Value = serde_json::from_str(&stdout).expect("history json");
    assert_eq!(v["snapshots"][0]["snapshot_id"], 42);

    // Create streams the file bytes to the ingest route.
    let dir = tempfile::tempdir().expect("dir");
    let csv = dir.path().join("rows.csv");
    std::fs::write(&csv, "id\n1\n2\n").expect("write csv");
    let (status, stdout, stderr) = run_cli(
        &endpoint,
        &[
            "--json",
            "table",
            "create",
            "sales.new",
            csv.to_str().expect("utf8"),
        ],
    );
    assert!(status.success(), "table create failed: {stderr}");
    let v: Value = serde_json::from_str(&stdout).expect("create json");
    assert_eq!(v["operation"], "create");
    assert_eq!(v["records_added"], 2);
    assert_eq!(
        state.ingest_bytes.load(Ordering::SeqCst),
        "id\n1\n2\n".len(),
        "the CLI streamed the file body to the server"
    );

    // Append reuses the same route with mode=append.
    let (status, stdout, _) = run_cli(
        &endpoint,
        &[
            "--json",
            "table",
            "append",
            "sales.new",
            csv.to_str().expect("utf8"),
        ],
    );
    assert!(status.success());
    let v: Value = serde_json::from_str(&stdout).expect("append json");
    assert_eq!(v["operation"], "append");

    // Query posts the SQL and optional time-travel selection to its database.
    let (status, stdout, stderr) = run_cli(
        &endpoint,
        &[
            "--json",
            "query",
            "analytics",
            "SELECT 1 AS n",
            "--at",
            "42",
            "sales.orders",
        ],
    );
    assert!(status.success(), "query failed: {stderr}");
    let v: Value = serde_json::from_str(&stdout).expect("query json");
    assert_eq!(v["row_count"], 1);
    assert_eq!(v["rows"][0]["n"], 1);
}

/// The CLI rejects names that cannot address a declared database before sending SQL.
#[test]
fn query_rejects_invalid_database_name() {
    let (status, _, stderr) = run_cli("http://127.0.0.1:9", &["query", "9analytics", "SELECT 1"]);
    assert!(!status.success(), "invalid database name must fail");
    assert!(
        stderr.contains("database name must start with a letter or underscore"),
        "query reports the resource-name grammar: {stderr}"
    );
}

/// The registry verbs speak the server registry routes: register posts the
/// The graph verbs speak the server graph routes: create/show on the namespace
/// path, add-node/add-edge post the JSON batch, index builds, and the traversal
/// verbs post a query the CLI assembled from the flags.
#[test]
fn graph_verbs_speak_the_server_api() {
    let state = MockState::default();
    let endpoint = spawn_mock(state.clone());

    // create.
    let (status, stdout, stderr) = run_cli(&endpoint, &["--json", "graph", "create", "kg"]);
    assert!(status.success(), "graph create failed: {stderr}");
    let v: Value = serde_json::from_str(&stdout).expect("create json");
    assert_eq!(v["nodesTable"], "kg.nodes");
    assert_eq!(v["edgesTable"], "kg.edges");

    // add-node reads a JSON array from a file.
    let dir = tempfile::tempdir().expect("dir");
    let nodes = dir.path().join("nodes.json");
    std::fs::write(&nodes, r#"[{"id":"a"},{"id":"b"}]"#).expect("write nodes");
    let (status, stdout, stderr) = run_cli(
        &endpoint,
        &[
            "--json",
            "graph",
            "add-node",
            "kg",
            nodes.to_str().expect("utf8"),
        ],
    );
    assert!(status.success(), "add-node failed: {stderr}");
    let v: Value = serde_json::from_str(&stdout).expect("add-node json");
    assert_eq!(v["count"], 2, "the CLI posted both nodes");

    // add-edge reads a JSON array from a file.
    let edges = dir.path().join("edges.json");
    std::fs::write(
        &edges,
        r#"[{"srcId":"a","predicate":"knows","dstId":"b","provenance":"m1"}]"#,
    )
    .expect("write edges");
    let (status, stdout, _) = run_cli(
        &endpoint,
        &[
            "--json",
            "graph",
            "add-edge",
            "kg",
            edges.to_str().expect("utf8"),
        ],
    );
    assert!(status.success());
    let v: Value = serde_json::from_str(&stdout).expect("add-edge json");
    assert_eq!(v["count"], 1);

    // index build.
    let (status, stdout, _) = run_cli(&endpoint, &["--json", "graph", "index", "kg"]);
    assert!(status.success());
    let v: Value = serde_json::from_str(&stdout).expect("index json");
    assert_eq!(v["built"], true);
    assert_eq!(v["mode"], "full");

    // k-hop assembles the traversal request from --hops and --direction.
    let (status, stdout, stderr) = run_cli(
        &endpoint,
        &[
            "--json",
            "graph",
            "k-hop",
            "kg",
            "a",
            "--hops",
            "2",
            "--direction",
            "both",
            "--predicate",
            "knows",
        ],
    );
    assert!(status.success(), "k-hop failed: {stderr}");
    let v: Value = serde_json::from_str(&stdout).expect("k-hop json");
    assert_eq!(v["op"], "kHop");
    assert_eq!(v["reached"][1]["nodeId"], "c");
    // The CLI assembled the traversal request from its flags.
    let q = state
        .last_query
        .lock()
        .expect("lock")
        .clone()
        .expect("query recorded");
    assert_eq!(q["k"], 2, "the CLI sent --hops as k");
    assert_eq!(q["direction"], "both");
    assert_eq!(q["filter"]["predicate"], "knows");

    // neighbors.
    let (status, stdout, _) = run_cli(&endpoint, &["--json", "graph", "neighbors", "kg", "a"]);
    assert!(status.success());
    let v: Value = serde_json::from_str(&stdout).expect("neighbors json");
    assert_eq!(v["op"], "neighbors");
    assert_eq!(v["neighbors"][0]["nodeId"], "b");

    // paths sends dst and --max-hops.
    let (status, stdout, stderr) = run_cli(
        &endpoint,
        &[
            "--json",
            "graph",
            "paths",
            "kg",
            "a",
            "c",
            "--max-hops",
            "3",
        ],
    );
    assert!(status.success(), "paths failed: {stderr}");
    let v: Value = serde_json::from_str(&stdout).expect("paths json");
    assert_eq!(v["op"], "paths");
    assert_eq!(v["paths"][0]["nodes"][1], "c");
    let q = state
        .last_query
        .lock()
        .expect("lock")
        .clone()
        .expect("query recorded");
    assert_eq!(q["dst"], "c");
    assert_eq!(q["maxHops"], 3);

    // show.
    let (status, stdout, _) = run_cli(&endpoint, &["--json", "graph", "show", "kg"]);
    assert!(status.success());
    let v: Value = serde_json::from_str(&stdout).expect("show json");
    assert_eq!(v["nodeCount"], 2);
    assert_eq!(v["indexed"], true);
}

/// With no server listening, every data-plane verb fails fast with a clear
/// error that names the endpoint and how to start a server — and there is no
/// fallback path that could touch a backend.
#[test]
fn server_down_is_a_clear_error_with_no_fallback() {
    // A dead port: nothing listens here.
    let endpoint = "http://127.0.0.1:9";
    for args in [
        vec!["table", "compact"],
        vec!["query", "analytics", "SELECT 1"],
        vec!["graph", "show", "kg"],
        vec!["graph", "neighbors", "kg", "a"],
    ] {
        let (status, _, stderr) = run_cli(endpoint, &args);
        assert!(!status.success(), "{args:?} must fail without a server");
        assert!(
            stderr.contains("127.0.0.1:9"),
            "{args:?} error names the endpoint: {stderr}"
        );
        assert!(
            stderr.contains("Docker") || stderr.contains("server"),
            "{args:?} error says how to recover: {stderr}"
        );
    }
}
