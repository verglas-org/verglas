//! FROZEN RIME EVALUATOR — WorkOS device-flow login for the Verglas CLI.
//! Candidates may not edit this file. Coordinator wrote it 2026-08-17.
//!
//! Contract:
//!
//! - `verglas login --no-browser` runs the OAuth 2.0 Device Authorization
//!   Grant against WorkOS as a public client (client_id only, no secret):
//!   POST {VERGLAS_WORKOS_API_BASE}/user_management/authorize/device, then
//!   poll POST {VERGLAS_WORKOS_API_BASE}/user_management/authenticate with
//!   grant_type=urn:ietf:params:oauth:grant-type:device_code, honoring
//!   `authorization_pending` responses. It prints the user_code and
//!   verification URI, never opens a browser under --no-browser, then
//!   exchanges the WorkOS access token at
//!   POST {VERGLAS_ACCESS_ENDPOINT}/v1/provision with
//!   `Authorization: Bearer <workos access token>`.
//! - WorkOS tokens persist to ~/.verglas/credentials/workos-tokens.json
//!   (owner-only 0600) with at least `access_token` and `refresh_token`.
//!   They never appear in config.toml.
//! - When a cloud API call gets 401 with a stored refresh token, the CLI
//!   refreshes (grant_type=refresh_token), persists the rotated pair, and
//!   retries the call once.
//! - `verglas logout` deletes workos-tokens.json along with the profile.
//! - Env overrides: VERGLAS_WORKOS_API_BASE, VERGLAS_WORKOS_CLIENT_ID.

use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use serde_json::{Value, json};
use tempfile::TempDir;

const CLIENT_ID: &str = "client_test_cli";
const ACCESS_TOKEN_1: &str = "workos-access-token-1";
const REFRESH_TOKEN_1: &str = "workos-refresh-token-1";
const ACCESS_TOKEN_2: &str = "workos-access-token-2";
const REFRESH_TOKEN_2: &str = "workos-refresh-token-2";

fn body_param(body: &Bytes, key: &str) -> Option<String> {
    if let Some(found) = serde_json::from_slice::<Value>(body)
        .ok()
        .as_ref()
        .and_then(|value| value.get(key))
        .and_then(Value::as_str)
    {
        return Some(found.to_owned());
    }
    let text = String::from_utf8_lossy(body);
    for pair in text.split('&') {
        match pair.split_once('=') {
            Some((k, v)) if k == key => return Some(urlencoding_decode(v)),
            _ => {}
        }
    }
    None
}

fn urlencoding_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                match u8::from_str_radix(&value[index + 1..index + 3], 16) {
                    Ok(byte) => {
                        out.push(byte);
                        index += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        index += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn spawn(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock");
    let address = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve mock");
    });
    format!("http://{address}")
}

fn workos_router(poll_count: Arc<AtomicUsize>) -> Router {
    Router::new()
        .route(
            "/user_management/authorize/device",
            post(move |body: Bytes| async move {
                assert_eq!(
                    body_param(&body, "client_id").as_deref(),
                    Some(CLIENT_ID),
                    "device authorize must send the public client_id"
                );
                (
                    StatusCode::OK,
                    axum::Json(json!({
                        "device_code": "dc_frozen_1",
                        "user_code": "RRGQ-BJVS",
                        "verification_uri": "https://auth.test/device",
                        "verification_uri_complete": "https://auth.test/device?user_code=RRGQ-BJVS",
                        "expires_in": 300,
                        "interval": 1
                    })),
                )
            }),
        )
        .route(
            "/user_management/authenticate",
            post(move |body: Bytes| {
                let poll_count = poll_count.clone();
                async move {
                    let grant = body_param(&body, "grant_type").unwrap_or_default();
                    if grant == "urn:ietf:params:oauth:grant-type:device_code" {
                        assert_eq!(
                            body_param(&body, "device_code").as_deref(),
                            Some("dc_frozen_1")
                        );
                        assert_eq!(body_param(&body, "client_id").as_deref(), Some(CLIENT_ID));
                        let polls = poll_count.fetch_add(1, Ordering::SeqCst);
                        if polls == 0 {
                            return (
                                StatusCode::BAD_REQUEST,
                                axum::Json(json!({ "error": "authorization_pending" })),
                            );
                        }
                        return (
                            StatusCode::OK,
                            axum::Json(json!({
                                "access_token": ACCESS_TOKEN_1,
                                "refresh_token": REFRESH_TOKEN_1,
                                "user": { "object": "user", "id": "user_cli_1", "email": "dev@example.test" },
                                "organization_id": "org_frozen",
                                "authentication_method": "GitHubOAuth"
                            })),
                        );
                    }
                    if grant == "refresh_token" {
                        assert_eq!(
                            body_param(&body, "refresh_token").as_deref(),
                            Some(REFRESH_TOKEN_1),
                            "refresh must present the stored refresh token"
                        );
                        assert_eq!(body_param(&body, "client_id").as_deref(), Some(CLIENT_ID));
                        return (
                            StatusCode::OK,
                            axum::Json(json!({
                                "access_token": ACCESS_TOKEN_2,
                                "refresh_token": REFRESH_TOKEN_2,
                                "user": { "object": "user", "id": "user_cli_1", "email": "dev@example.test" }
                            })),
                        );
                    }
                    (
                        StatusCode::BAD_REQUEST,
                        axum::Json(json!({ "error": "unsupported_grant_type", "got": grant })),
                    )
                }
            }),
        )
}

fn provision_body() -> Value {
    json!({
        "s3_url": "https://tenant.fly.dev:8443",
        "catalog_url": "https://tenant.fly.dev",
        "query_url": "https://tenant.fly.dev",
        "slug": "acme",
        "s3_access_key_id": "VG0123456789ABCDEF01",
        "s3_secret_access_key": "endpoint-signing-secret",
        "catalog_token": "catalog-query-bearer-secret",
        "tier": "free"
    })
}

fn base_command(binary_env: &TempDir, workos: &str, control_plane: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verglas"));
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", binary_env.path())
        .env("VERGLAS_WORKOS_API_BASE", workos)
        .env("VERGLAS_WORKOS_CLIENT_ID", CLIENT_ID)
        .env("VERGLAS_ACCESS_ENDPOINT", control_plane);
    command
}

fn workos_tokens_path(home: &TempDir) -> std::path::PathBuf {
    home.path()
        .join(".verglas")
        .join("credentials")
        .join("workos-tokens.json")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_login_persists_workos_tokens_and_provisions() {
    let polls = Arc::new(AtomicUsize::new(0));
    let workos = spawn(workos_router(polls.clone())).await;

    let provision_auth: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let seen = provision_auth.clone();
    let control_plane = spawn(Router::new().route(
        "/v1/provision",
        post(move |headers: HeaderMap, _body: Bytes| {
            let seen = seen.clone();
            async move {
                let authorization = headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                seen.lock().expect("lock").push(authorization);
                (StatusCode::OK, axum::Json(provision_body()))
            }
        }),
    ))
    .await;

    let home = TempDir::new().expect("home");
    let output = tokio::task::spawn_blocking({
        let workos = workos.clone();
        let control_plane = control_plane.clone();
        let mut command = base_command(&home, &workos, &control_plane);
        move || {
            command
                .args(["login", "--no-browser"])
                .output()
                .expect("login runs")
        }
    })
    .await
    .expect("join");

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "login failed:\n{combined}"
    );
    assert!(combined.contains("RRGQ-BJVS"), "must print the user code:\n{combined}");
    assert!(
        combined.contains("https://auth.test/device"),
        "must print the verification URI:\n{combined}"
    );
    assert!(
        polls.load(Ordering::SeqCst) >= 2,
        "must poll through authorization_pending"
    );

    let bearers = provision_auth.lock().expect("lock").clone();
    assert_eq!(
        bearers
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect::<Vec<_>>(),
        vec![format!("bearer {ACCESS_TOKEN_1}")],
        "provision must present the WorkOS access token"
    );

    let tokens_path = workos_tokens_path(&home);
    let stored: Value =
        serde_json::from_slice(&fs::read(&tokens_path).expect("workos tokens stored"))
            .expect("token JSON");
    assert_eq!(stored["access_token"], ACCESS_TOKEN_1);
    assert_eq!(stored["refresh_token"], REFRESH_TOKEN_1);
    #[cfg(unix)]
    assert_eq!(
        fs::metadata(&tokens_path).expect("metadata").permissions().mode() & 0o777,
        0o600,
        "workos token store must be owner-only"
    );

    let config = fs::read_to_string(home.path().join(".verglas").join("config.toml"))
        .expect("profile config");
    for secret in [ACCESS_TOKEN_1, REFRESH_TOKEN_1, "endpoint-signing-secret"] {
        assert!(!config.contains(secret), "config.toml leaked a secret");
    }
    assert!(
        home.path()
            .join(".verglas")
            .join("credentials")
            .join("endpoint.ini")
            .exists(),
        "vended S3 credentials must persist as before"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_access_token_refreshes_rotates_and_retries() {
    let polls = Arc::new(AtomicUsize::new(0));
    let workos = spawn(workos_router(polls)).await;

    let list_calls: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let seen = list_calls.clone();
    let control_plane = spawn(Router::new().route(
        "/v1/access/tokens",
        get(move |headers: HeaderMap| {
            let seen = seen.clone();
            async move {
                let authorization = headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                seen.lock().expect("lock").push(authorization.clone());
                if authorization == format!("bearer {ACCESS_TOKEN_2}") {
                    (StatusCode::OK, axum::Json(json!([])))
                } else {
                    (
                        StatusCode::UNAUTHORIZED,
                        axum::Json(json!({ "error": "token expired" })),
                    )
                }
            }
        }),
    ))
    .await;

    let home = TempDir::new().expect("home");
    let credentials_dir = home.path().join(".verglas").join("credentials");
    fs::create_dir_all(&credentials_dir).expect("credentials dir");
    let tokens_path = credentials_dir.join("workos-tokens.json");
    fs::write(
        &tokens_path,
        json!({
            "access_token": "workos-access-token-stale",
            "refresh_token": REFRESH_TOKEN_1
        })
        .to_string(),
    )
    .expect("seed tokens");
    #[cfg(unix)]
    fs::set_permissions(&tokens_path, fs::Permissions::from_mode(0o600)).expect("chmod");

    let output = tokio::task::spawn_blocking({
        let workos = workos.clone();
        let control_plane = control_plane.clone();
        let mut command = base_command(&home, &workos, &control_plane);
        move || {
            command
                .args(["token", "list"])
                .output()
                .expect("token list runs")
        }
    })
    .await
    .expect("join");
    assert!(
        output.status.success(),
        "token list failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = list_calls.lock().expect("lock").clone();
    assert!(
        calls.contains(&format!("bearer {ACCESS_TOKEN_2}")),
        "must retry with the refreshed token: {calls:?}"
    );

    let stored: Value = serde_json::from_slice(&fs::read(&tokens_path).expect("tokens"))
        .expect("token JSON");
    assert_eq!(stored["access_token"], ACCESS_TOKEN_2, "rotated access token persists");
    assert_eq!(stored["refresh_token"], REFRESH_TOKEN_2, "rotated refresh token persists");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logout_deletes_the_workos_token_store() {
    let home = TempDir::new().expect("home");
    let credentials_dir = home.path().join(".verglas").join("credentials");
    fs::create_dir_all(&credentials_dir).expect("credentials dir");
    let tokens_path = credentials_dir.join("workos-tokens.json");
    fs::write(
        &tokens_path,
        json!({ "access_token": "a", "refresh_token": "r" }).to_string(),
    )
    .expect("seed tokens");

    let output = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .arg("logout")
        .output()
        .expect("logout runs");
    assert!(
        output.status.success(),
        "logout failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !tokens_path.exists(),
        "logout must delete the WorkOS token store"
    );
}
