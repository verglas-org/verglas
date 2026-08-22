//! Mock-listener tests for `verglas graph`/`verglas vector` (#144): every verb
//! must POST the correctly named operation to the S3 semantic listener with a
//! SigV4 `Authorization` header and a signed `x-amz-content-sha256`. The CLI
//! wraps `verglas_sdk::semantic` clients only — it must never sign requests
//! itself.

use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use serde_json::{Value, json};

/// One captured request: method, path, and headers. The body is not needed by
/// these assertions (the SDK's own tests cover canonicalization); this file
/// checks that the CLI reaches the right operation, signed.
#[derive(Debug, Clone)]
struct Captured {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
}

#[derive(Clone, Default)]
struct MockState {
    last: Arc<Mutex<Option<Captured>>>,
}

/// Boots a mock S3 semantic listener on an ephemeral port. Every request is
/// captured and answered with the fixed JSON registered for its path below.
fn spawn_server() -> (String, MockState) {
    let state = MockState::default();
    let app = Router::new().fallback(handle).with_state(state.clone());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    listener.set_nonblocking(true).expect("nonblocking");
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, app).await.expect("serve");
        });
    });
    std::thread::sleep(std::time::Duration::from_millis(50));
    (format!("http://{addr}"), state)
}

/// Captures the request and answers with the canned JSON for its path.
async fn handle(
    State(state): State<MockState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    _body: Bytes,
) -> impl IntoResponse {
    let path = uri.path().to_owned();
    *state.last.lock().expect("lock") = Some(Captured {
        method: method.to_string(),
        path: path.clone(),
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_owned()))
            .collect(),
    });
    (StatusCode::OK, axum::Json(canned_response(&path)))
}

/// Fixed JSON responses keyed by operation path, standing in for the cache
/// node's semantic listener.
fn canned_response(path: &str) -> Value {
    match path {
        "/CreateGraph"
        | "/DeleteGraph"
        | "/PutVectors"
        | "/DeleteVectors"
        | "/DeleteIndex"
        | "/DeleteVectorBucket" => json!({}),
        "/AddNodes" | "/AddEdges" => json!({ "snapshotId": 1 }),
        "/RetrieveNeighbors" => json!({
            "neighbors": [{
                "confidence": 0.9,
                "direction": "out",
                "edgeId": "e1",
                "nodeId": "b",
                "predicate": "rel",
                "provenance": "test"
            }]
        }),
        "/BuildIndex" => json!({ "index": true }),
        "/DescribeGraph" => json!({ "graphName": "testgraph", "edgesSnapshotId": 1 }),
        "/ListGraphs" => json!({ "graphs": [{ "graphName": "testgraph" }], "nextToken": null }),
        "/SearchKHop" => json!({ "nodes": [{ "hops": 1, "nodeId": "b", "pathConfidence": 0.9 }] }),
        "/SearchPaths" => json!({ "paths": [] }),
        "/SearchPrecedents" => json!({
            "precedents": [{
                "decisionId": "decision:a",
                "score": 1.5,
                "lexicalScore": 0.5,
                "structuralScore": 1.0,
                "sharedEntities": ["entity:x"],
                "objective": "migrate the billing pipeline"
            }]
        }),
        "/CreateVectorBucket" => json!({ "vectorBucketArn": "arn:test:bucket" }),
        "/CreateIndex" => json!({ "indexArn": "arn:test:index" }),
        "/QueryVectors" => json!({
            "vectors": [{ "key": "near", "distance": 0.01 }],
            "distanceMetric": "cosine",
            "nextToken": null
        }),
        "/ListVectors" => json!({ "vectors": [], "nextToken": null }),
        "/GetVectors" => json!({ "vectors": [] }),
        "/ListVectorBuckets" => json!({ "vectorBuckets": [], "nextToken": null }),
        "/ListIndexes" => json!({ "indexes": [], "nextToken": null }),
        other => panic!("unexpected mock path {other}"),
    }
}

/// Runs the CLI against the mock listener with fixed test credentials.
fn run(endpoint: &str, args: &[&str], stdin: Option<&str>) -> (bool, String, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verglas"));
    command
        .args(args)
        .env("VERGLAS_S3_ENDPOINT", endpoint)
        .env("VERGLAS_S3_ACCESS_KEY_ID", "test-key")
        .env("VERGLAS_S3_SECRET_ACCESS_KEY", "test-secret")
        .env("VERGLAS_S3_REGION", "auto")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command.stdin(if stdin.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    let mut child = command.spawn().expect("binary runs");
    if let Some(input) = stdin {
        child
            .stdin
            .take()
            .expect("stdin piped")
            .write_all(input.as_bytes())
            .expect("write stdin");
    }
    let out = child.wait_with_output().expect("wait");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Asserts the last captured request matches `method`/`path` and carries a
/// signed SigV4 `Authorization` header plus the payload-hash header.
fn assert_signed(state: &MockState, method: &str, path: &str) {
    let captured = state
        .last
        .lock()
        .expect("lock")
        .clone()
        .unwrap_or_else(|| panic!("no request captured for {method} {path}"));
    assert_eq!(captured.method, method, "unexpected method for {path}");
    assert_eq!(captured.path, path, "unexpected path");
    let authorization = captured
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| panic!("missing authorization header for {path}"));
    assert!(
        authorization.starts_with("AWS4-HMAC-SHA256"),
        "authorization header must be SigV4 for {path}: {authorization}"
    );
    assert!(
        authorization.contains("Signature="),
        "authorization header must carry a signature for {path}: {authorization}"
    );
    assert!(
        captured
            .headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("x-amz-content-sha256")),
        "missing x-amz-content-sha256 header for {path}"
    );
}

#[test]
fn graph_verbs_post_the_correct_signed_operations() {
    let (endpoint, state) = spawn_server();

    let (ok, _stdout, stderr) = run(&endpoint, &["--json", "graph", "create", "g1"], None);
    assert!(ok, "graph create failed: {stderr}");
    assert_signed(&state, "POST", "/CreateGraph");

    let nodes = r#"[{"id":"a","labels":["x"],"properties":{"p":"1"}},{"id":"b"}]"#;
    let (ok, _stdout, stderr) = run(
        &endpoint,
        &["--json", "graph", "add-node", "g1", "-"],
        Some(nodes),
    );
    assert!(ok, "graph add-node failed: {stderr}");
    assert_signed(&state, "POST", "/AddNodes");

    let edges =
        r#"[{"sourceId":"a","predicate":"rel","targetId":"b","provenance":"t","confidence":0.9}]"#;
    let (ok, _stdout, stderr) = run(
        &endpoint,
        &["--json", "graph", "add-edge", "g1", "-"],
        Some(edges),
    );
    assert!(ok, "graph add-edge failed: {stderr}");
    assert_signed(&state, "POST", "/AddEdges");

    let (ok, stdout, stderr) = run(
        &endpoint,
        &["--json", "graph", "neighbors", "g1", "a"],
        None,
    );
    assert!(ok, "graph neighbors failed: {stderr}");
    assert_signed(&state, "POST", "/RetrieveNeighbors");
    assert!(
        stdout.contains("b"),
        "neighbors output must include \"b\": {stdout}"
    );

    let (ok, _stdout, stderr) = run(&endpoint, &["--json", "graph", "index", "g1"], None);
    assert!(ok, "graph index failed: {stderr}");
    assert_signed(&state, "POST", "/BuildIndex");

    let (ok, stdout, stderr) = run(&endpoint, &["--json", "graph", "show", "g1"], None);
    assert!(ok, "graph show failed: {stderr}");
    assert_signed(&state, "POST", "/DescribeGraph");
    assert!(
        stdout.contains("testgraph"),
        "show output must include the graph name: {stdout}"
    );

    let (ok, _stdout, stderr) = run(&endpoint, &["--json", "graph", "list"], None);
    assert!(ok, "graph list failed: {stderr}");
    assert_signed(&state, "POST", "/ListGraphs");

    let (ok, _stdout, stderr) = run(
        &endpoint,
        &["--json", "graph", "k-hop", "g1", "a", "--hops", "2"],
        None,
    );
    assert!(ok, "graph k-hop failed: {stderr}");
    assert_signed(&state, "POST", "/SearchKHop");

    let (ok, _stdout, stderr) = run(
        &endpoint,
        &[
            "--json",
            "graph",
            "paths",
            "g1",
            "a",
            "b",
            "--max-hops",
            "3",
        ],
        None,
    );
    assert!(ok, "graph paths failed: {stderr}");
    assert_signed(&state, "POST", "/SearchPaths");

    let (ok, stdout, stderr) = run(
        &endpoint,
        &[
            "--json",
            "graph",
            "precedents",
            "g1",
            "--query",
            "migrate billing",
            "--limit",
            "5",
            "--entity",
            "entity:x",
        ],
        None,
    );
    assert!(ok, "graph precedents failed: {stderr}");
    assert_signed(&state, "POST", "/SearchPrecedents");
    assert!(
        stdout.contains("decision:a"),
        "precedents output must include the ranked decision id: {stdout}"
    );

    let (ok, _stdout, stderr) = run(&endpoint, &["--json", "graph", "delete", "g1"], None);
    assert!(ok, "graph delete failed: {stderr}");
    assert_signed(&state, "POST", "/DeleteGraph");
}

#[test]
fn vector_verbs_post_the_correct_signed_operations() {
    let (endpoint, state) = spawn_server();

    let (ok, _stdout, stderr) = run(
        &endpoint,
        &["--json", "vector", "create-bucket", "b1"],
        None,
    );
    assert!(ok, "vector create-bucket failed: {stderr}");
    assert_signed(&state, "POST", "/CreateVectorBucket");

    let (ok, _stdout, stderr) = run(
        &endpoint,
        &[
            "--json",
            "vector",
            "create-index",
            "b1",
            "i1",
            "--dimension",
            "3",
            "--metric",
            "cosine",
        ],
        None,
    );
    assert!(ok, "vector create-index failed: {stderr}");
    assert_signed(&state, "POST", "/CreateIndex");

    let vectors = r#"[{"key":"near","data":{"float32":[0.1,0.2,0.3]}},{"key":"far","data":{"float32":[0.9,0.1,0.9]}}]"#;
    let (ok, _stdout, stderr) = run(
        &endpoint,
        &["--json", "vector", "put", "b1", "i1", "-"],
        Some(vectors),
    );
    assert!(ok, "vector put failed: {stderr}");
    assert_signed(&state, "POST", "/PutVectors");

    let (ok, stdout, stderr) = run(
        &endpoint,
        &[
            "--json",
            "vector",
            "query",
            "b1",
            "i1",
            "--top-k",
            "1",
            "--query-vector",
            "[0.1,0.2,0.3]",
        ],
        None,
    );
    assert!(ok, "vector query failed: {stderr}");
    assert_signed(&state, "POST", "/QueryVectors");
    assert!(
        stdout.contains("near"),
        "query output must include \"near\": {stdout}"
    );

    let (ok, _stdout, stderr) = run(&endpoint, &["--json", "vector", "list", "b1", "i1"], None);
    assert!(ok, "vector list failed: {stderr}");
    assert_signed(&state, "POST", "/ListVectors");

    let (ok, _stdout, stderr) = run(
        &endpoint,
        &["--json", "vector", "get", "b1", "i1", "near"],
        None,
    );
    assert!(ok, "vector get failed: {stderr}");
    assert_signed(&state, "POST", "/GetVectors");

    let (ok, _stdout, stderr) = run(
        &endpoint,
        &["--json", "vector", "delete", "b1", "i1", "near"],
        None,
    );
    assert!(ok, "vector delete failed: {stderr}");
    assert_signed(&state, "POST", "/DeleteVectors");

    let (ok, _stdout, stderr) = run(
        &endpoint,
        &["--json", "vector", "delete-index", "b1", "i1"],
        None,
    );
    assert!(ok, "vector delete-index failed: {stderr}");
    assert_signed(&state, "POST", "/DeleteIndex");

    let (ok, _stdout, stderr) = run(&endpoint, &["--json", "vector", "list-buckets"], None);
    assert!(ok, "vector list-buckets failed: {stderr}");
    assert_signed(&state, "POST", "/ListVectorBuckets");

    let (ok, _stdout, stderr) = run(&endpoint, &["--json", "vector", "list-indexes", "b1"], None);
    assert!(ok, "vector list-indexes failed: {stderr}");
    assert_signed(&state, "POST", "/ListIndexes");

    let (ok, _stdout, stderr) = run(
        &endpoint,
        &["--json", "vector", "delete-bucket", "b1"],
        None,
    );
    assert!(ok, "vector delete-bucket failed: {stderr}");
    assert_signed(&state, "POST", "/DeleteVectorBucket");
}

#[test]
fn add_node_reads_json_array_from_a_file_path() {
    let (endpoint, state) = spawn_server();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("nodes.json");
    std::fs::write(&path, r#"[{"id":"a"}]"#).expect("write");
    let (ok, _stdout, stderr) = run(
        &endpoint,
        &[
            "--json",
            "graph",
            "add-node",
            "g1",
            path.to_str().expect("utf8 path"),
        ],
        None,
    );
    assert!(ok, "graph add-node (file) failed: {stderr}");
    assert_signed(&state, "POST", "/AddNodes");
}
