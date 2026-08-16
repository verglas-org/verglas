//! CLI contract tests for access-token lifecycle and local credential storage.

use std::fs;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, post};
use serde_json::{Value, json};
use tempfile::TempDir;

/// Records token service requests without retaining their sensitive response value.
#[derive(Debug, Clone)]
struct CapturedRequest {
    path: String,
    authorization: Option<String>,
    body: Option<Value>,
}

/// Token creation persists the returned one-time value in an owner-only credentials file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_create_stores_plaintext_with_owner_only_permissions() {
    let (endpoint, captured) = spawn_api().await;
    let temp = TempDir::new().expect("tempdir");
    let credentials = temp.path().join("credentials.json");
    let output = command(
        &endpoint,
        &credentials,
        Some("parent-token"),
        &[
            "token",
            "create",
            "local-cli",
            "--audience",
            "data-plane",
            "--expires-in-seconds",
            "86400",
            "--grant",
            "analytics=query,discover",
        ],
    )
    .output()
    .expect("CLI runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let request = captured
        .lock()
        .expect("lock")
        .first()
        .cloned()
        .expect("request");
    assert_eq!(request.path, "/v1/access/tokens");
    assert_eq!(
        request.authorization.as_deref(),
        Some("Bearer parent-token")
    );
    assert_eq!(
        request.body.expect("request body"),
        json!({
            "name": "local-cli",
            "audience": "data-plane",
            "expires_in_seconds": 86400,
            "grants": [{ "resource_id": "analytics", "actions": ["query", "discover"] }]
        })
    );
    let stored: Value = serde_json::from_slice(&fs::read(&credentials).expect("credentials"))
        .expect("credential JSON");
    assert_eq!(stored["tokens"][endpoint]["token"], "plaintext-once");
    #[cfg(unix)]
    assert_eq!(
        fs::metadata(&credentials)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("plaintext-once"));
}

/// Stored credentials supply the authorization header for later token lifecycle commands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_list_and_revoke_use_stored_credential() {
    let (endpoint, captured) = spawn_api().await;
    let temp = TempDir::new().expect("tempdir");
    let credentials = temp.path().join("credentials.json");
    fs::write(
        &credentials,
        json!({ "tokens": { endpoint.clone(): { "token": "stored-token", "token_id": "token-1" } } }).to_string(),
    )
    .expect("credentials");
    #[cfg(unix)]
    fs::set_permissions(&credentials, fs::Permissions::from_mode(0o600))
        .expect("credential permissions");

    for arguments in [
        ["token", "list"].as_slice(),
        ["token", "revoke", "token-1"].as_slice(),
    ] {
        let output = command(&endpoint, &credentials, None, arguments)
            .output()
            .expect("CLI runs");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let requests = captured.lock().expect("lock");
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].authorization.as_deref(),
        Some("Bearer stored-token")
    );
    assert_eq!(requests[1].path, "/v1/access/tokens/token-1");
    assert_eq!(
        requests[1].authorization.as_deref(),
        Some("Bearer stored-token")
    );
}

/// Builds a CLI process with an explicit credential file and parent token.
fn command(
    endpoint: &str,
    credentials: &std::path::Path,
    token: Option<&str>,
    arguments: &[&str],
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verglas"));
    command
        .env("VERGLAS_ACCESS_ENDPOINT", endpoint)
        .env("VERGLAS_CREDENTIALS_FILE", credentials);
    if let Some(token) = token {
        command.env("VERGLAS_TOKEN", token);
    }
    command.args(arguments);
    command
}

/// Serves the token endpoint fixture and captures each request.
async fn spawn_api() -> (String, Arc<Mutex<Vec<CapturedRequest>>>) {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route(
            "/v1/access/tokens",
            post({
                let captured = Arc::clone(&captured);
                move |headers: HeaderMap, body: Bytes| capture_create(captured, headers, body)
            })
            .get({
                let captured = Arc::clone(&captured);
                move |headers: HeaderMap| capture_list(captured, headers)
            }),
        )
        .route(
            "/v1/access/tokens/{id}",
            delete({
                let captured = Arc::clone(&captured);
                move |headers: HeaderMap| capture_revoke(captured, headers)
            }),
        )
        .route(
            "/v1/access/database-tokens",
            post({
                let captured = Arc::clone(&captured);
                move |headers: HeaderMap, body: Bytes| {
                    capture_database_token(captured, headers, body)
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address: SocketAddr = listener.local_addr().expect("address");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    (format!("http://{address}"), captured)
}

/// Captures creation and returns a one-time secret value with metadata.
async fn capture_create(
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, axum::Json<Value>) {
    captured.lock().expect("lock").push(CapturedRequest {
        path: "/v1/access/tokens".to_owned(),
        authorization: headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        body: Some(serde_json::from_slice(&body).expect("JSON")),
    });
    (StatusCode::CREATED, axum::Json(issued_metadata()))
}

/// Captures listing and returns token metadata without a plaintext token.
async fn capture_list(
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    headers: HeaderMap,
) -> axum::Json<Value> {
    captured.lock().expect("lock").push(CapturedRequest {
        path: "/v1/access/tokens".to_owned(),
        authorization: headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        body: None,
    });
    axum::Json(json!([metadata(Value::Null)]))
}

/// Captures revocation and returns the revoked token metadata.
async fn capture_revoke(
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    headers: HeaderMap,
) -> axum::Json<Value> {
    captured.lock().expect("lock").push(CapturedRequest {
        path: "/v1/access/tokens/token-1".to_owned(),
        authorization: headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        body: None,
    });
    axum::Json(metadata(json!(1_786_252_800)))
}

/// Captures the database-token exchange and returns a short-lived Neon JWT.
async fn capture_database_token(
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, axum::Json<Value>) {
    captured.lock().expect("lock").push(CapturedRequest {
        path: "/v1/access/database-tokens".to_owned(),
        authorization: headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        body: Some(serde_json::from_slice(&body).expect("JSON")),
    });
    (
        StatusCode::CREATED,
        axum::Json(json!({ "token": "neon-password-jwt", "expires_at": 1786250100_u64 })),
    )
}

/// Returns a response metadata fixture.
fn metadata(revoked_at: Value) -> Value {
    json!({
        "id": "token-1", "parent_principal_id": "user-a",
        "principal_id": "token-principal-a", "name": "local-cli", "audience": "data-plane",
        "created_at": 1786249200, "expires_at": 1786335600,
        "revoked_at": revoked_at, "last_used_at": null
    })
}

/// Adds the creation-only plaintext field to otherwise non-secret metadata.
fn issued_metadata() -> Value {
    let mut value = metadata(Value::Null);
    value
        .as_object_mut()
        .expect("metadata object")
        .insert("token".to_owned(), json!("plaintext-once"));
    value
}
