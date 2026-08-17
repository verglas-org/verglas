//! CLI contract tests for scoped-secret creation (#84). The dynamic-database
//! (`lakehouse create`) tests that used to live here were removed with the
//! retired `/v1/databases` access-service surface (no client here anymore).

use std::io::Write;
use std::net::SocketAddr;
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;

/// Captured request sent by the CLI to the local resource API.
#[derive(Debug, Clone)]
struct CapturedRequest {
    path: String,
    authorization: Option<String>,
    body: serde_json::Value,
}

/// Starts a local API that captures database and secret creation requests.
async fn spawn_api() -> (String, Arc<Mutex<Vec<CapturedRequest>>>) {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route(
            "/v1/secrets",
            post({
                let captured = Arc::clone(&captured);
                move |headers: HeaderMap, body: Bytes| {
                    capture("/v1/secrets", captured, headers, body)
                }
            }),
        )
        .route(
            "/v1/queues",
            post({
                let captured = Arc::clone(&captured);
                move |headers: HeaderMap, body: Bytes| {
                    capture("/v1/queues", captured, headers, body)
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address: SocketAddr = listener.local_addr().expect("local address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock API serves");
    });
    (format!("http://{address}"), captured)
}

/// Records one request and returns a generic created-resource response.
async fn capture(
    path: &'static str,
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, &'static str) {
    let body = serde_json::from_slice(&body).expect("request is JSON");
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    captured
        .lock()
        .expect("capture lock")
        .push(CapturedRequest {
            path: path.to_owned(),
            authorization,
            body,
        });
    (StatusCode::CREATED, r#"{"created":true}"#)
}

/// Runs the CLI against a local API and optionally provides standard input.
fn run(endpoint: &str, args: &[&str], stdin: Option<&str>) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_verglas"));
    child
        .env("VERGLAS_ENDPOINT", endpoint)
        .env("VERGLAS_ACCESS_ENDPOINT", endpoint)
        .env("VERGLAS_TOKEN", "local-test-token")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = child.spawn().expect("CLI starts");
    if let Some(input) = stdin {
        child
            .stdin
            .as_mut()
            .expect("stdin pipe")
            .write_all(input.as_bytes())
            .expect("stdin writes");
    }
    drop(child.stdin.take());
    child.wait_with_output().expect("CLI exits")
}

/// Returns the single request captured by a test.
fn one_request(captured: &Arc<Mutex<Vec<CapturedRequest>>>) -> CapturedRequest {
    let requests = captured.lock().expect("capture lock");
    assert_eq!(requests.len(), 1, "exactly one request must be sent");
    requests[0].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secret_create_reads_material_from_stdin_and_never_accepts_value_argument() {
    let (endpoint, captured) = spawn_api().await;
    let output = run(
        &endpoint,
        &[
            "secret",
            "create",
            "customer_s3",
            "--type",
            "s3",
            "--scope",
            "s3://customer-bucket/team",
        ],
        Some("access_key=abc\nsecret_key=def\n"),
    );
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let request = one_request(&captured);
    assert_eq!(request.path, "/v1/secrets");
    assert_eq!(
        request.authorization.as_deref(),
        Some("Bearer local-test-token")
    );
    assert_eq!(
        request.body,
        serde_json::json!({
            "name": "customer_s3",
            "type": "s3",
            "scope": "s3://customer-bucket/team",
            "value": "access_key=abc\nsecret_key=def"
        })
    );

    let (catalog_endpoint, catalog_captured) = spawn_api().await;
    let catalog_output = run(
        &catalog_endpoint,
        &[
            "secret",
            "create",
            "customer_catalog",
            "--type",
            "iceberg-rest",
            "--scope",
            "https://catalog.customer.com",
        ],
        Some("catalog-bearer-token\n"),
    );
    assert!(
        catalog_output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&catalog_output.stderr)
    );
    assert_eq!(
        one_request(&catalog_captured).body,
        serde_json::json!({
            "name": "customer_catalog",
            "type": "iceberg-rest",
            "scope": "https://catalog.customer.com",
            "value": "catalog-bearer-token"
        })
    );

    let invalid = run(
        &endpoint,
        &[
            "secret",
            "create",
            "bad",
            "--type",
            "s3",
            "--scope",
            "s3://bucket",
            "--value",
            "visible-in-argv",
        ],
        None,
    );
    assert!(!invalid.status.success());
}
