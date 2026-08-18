//! FROZEN RIME EVALUATOR v2 — WorkOS device login exchanges for a durable
//! first-party token. Candidates may not edit this file. Coordinator rewrote
//! it 2026-08-17 (user directive: no runtime WorkOS dependency).
//!
//! Contract:
//!
//! - `verglas login --no-browser` runs the OAuth 2.0 Device Authorization
//!   Grant against WorkOS as a public client (client_id only), printing the
//!   user_code and verification URI and honoring `authorization_pending`.
//!   The WorkOS access token is used EXACTLY ONCE: as the bearer on
//!   POST {VERGLAS_ACCESS_ENDPOINT}/v1/provision. The response's `api_token`
//!   (the durable tenant token) persists to
//!   ~/.verglas/credentials/control-plane-token (owner-only 0600).
//! - NO WorkOS material survives login: no workos-tokens.json, no refresh
//!   token on disk, nothing WorkOS-shaped in config.toml.
//! - Cloud API commands resolve their bearer VERGLAS_TOKEN → scoped
//!   credentials.json → control-plane-token, and NEVER contact WorkOS.
//! - A 401 from the cloud API fails loud with guidance to run
//!   `verglas login` — no silent refresh, no retry loop.
//! - `verglas logout` deletes the durable token file.
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
const WORKOS_ACCESS_TOKEN: &str = "workos-access-token-1";
const API_TOKEN: &str = "vg-durable-api-token-1";

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
            Some((k, v)) if k == key => return Some(v.replace('+', " ")),
            _ => {}
        }
    }
    None
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

/// WorkOS mock: counts every request so runtime paths can assert ZERO
/// contact; serves the device grant for the login test.
fn workos_router(hits: Arc<AtomicUsize>, polls: Arc<AtomicUsize>) -> Router {
    let count_hits = hits.clone();
    let device_hits = hits.clone();
    Router::new()
        .route(
            "/user_management/authorize/device",
            post(move |body: Bytes| {
                let _ = device_hits.fetch_add(1, Ordering::SeqCst);
                async move {
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
                }
            }),
        )
        .route(
            "/user_management/authenticate",
            post(move |body: Bytes| {
                let _ = count_hits.fetch_add(1, Ordering::SeqCst);
                let polls = polls.clone();
                async move {
                    let grant = body_param(&body, "grant_type").unwrap_or_default();
                    assert_eq!(
                        grant, "urn:ietf:params:oauth:grant-type:device_code",
                        "only the device grant is ever allowed; got {grant}"
                    );
                    assert_eq!(body_param(&body, "device_code").as_deref(), Some("dc_frozen_1"));
                    if polls.fetch_add(1, Ordering::SeqCst) == 0 {
                        return (
                            StatusCode::BAD_REQUEST,
                            axum::Json(json!({ "error": "authorization_pending" })),
                        );
                    }
                    (
                        StatusCode::OK,
                        axum::Json(json!({
                            "access_token": WORKOS_ACCESS_TOKEN,
                            "refresh_token": "workos-refresh-token-1",
                            "user": { "object": "user", "id": "user_cli_1", "email": "dev@example.test" },
                            "organization_id": "org_frozen",
                            "authentication_method": "GitHubOAuth"
                        })),
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
        "api_token": API_TOKEN,
        "tier": "free"
    })
}

fn base_command(home: &TempDir, workos: &str, control_plane: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verglas"));
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .env("VERGLAS_WORKOS_API_BASE", workos)
        .env("VERGLAS_WORKOS_CLIENT_ID", CLIENT_ID)
        .env("VERGLAS_ACCESS_ENDPOINT", control_plane);
    command
}

fn credentials_dir(home: &TempDir) -> std::path::PathBuf {
    home.path().join(".verglas").join("credentials")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_login_exchanges_for_the_durable_token_and_keeps_no_workos_material() {
    let hits = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let workos = spawn(workos_router(hits.clone(), polls.clone())).await;

    let provision_bearers: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let seen = provision_bearers.clone();
    let control_plane = spawn(Router::new().route(
        "/v1/provision",
        post(move |headers: HeaderMap, _body: Bytes| {
            let seen = seen.clone();
            async move {
                let authorization = headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                seen.lock().expect("lock").push(authorization);
                (StatusCode::OK, axum::Json(provision_body()))
            }
        }),
    ))
    .await;

    let home = TempDir::new().expect("home");
    let output = tokio::task::spawn_blocking({
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
    assert!(output.status.success(), "login failed:\n{combined}");
    assert!(
        combined.contains("RRGQ-BJVS"),
        "must print the user code:\n{combined}"
    );
    assert!(
        combined.contains("https://auth.test/device"),
        "must print the verification URI:\n{combined}"
    );
    assert!(
        polls.load(Ordering::SeqCst) >= 2,
        "must poll through authorization_pending"
    );

    assert_eq!(
        provision_bearers.lock().expect("lock").clone(),
        vec![format!("bearer {WORKOS_ACCESS_TOKEN}")],
        "the WorkOS token is spent exactly once, on provision"
    );

    let token_path = credentials_dir(&home).join("control-plane-token");
    let stored = fs::read_to_string(&token_path).expect("durable token stored");
    assert_eq!(stored.trim(), API_TOKEN);
    #[cfg(unix)]
    assert_eq!(
        fs::metadata(&token_path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600,
        "durable token must be owner-only"
    );

    assert!(
        !credentials_dir(&home).join("workos-tokens.json").exists(),
        "no WorkOS token store may exist after login"
    );
    let config = fs::read_to_string(home.path().join(".verglas").join("config.toml"))
        .expect("profile config");
    for secret in [
        WORKOS_ACCESS_TOKEN,
        "workos-refresh-token-1",
        API_TOKEN,
        "endpoint-signing-secret",
    ] {
        assert!(!config.contains(secret), "config.toml leaked a secret");
    }
    assert!(
        credentials_dir(&home).join("endpoint.ini").exists(),
        "vended S3 credentials must persist as before"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloud_commands_use_the_stored_token_and_never_contact_workos() {
    let hits = Arc::new(AtomicUsize::new(0));
    let workos = spawn(workos_router(hits.clone(), Arc::new(AtomicUsize::new(0)))).await;

    let list_bearers: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let seen = list_bearers.clone();
    let control_plane = spawn(Router::new().route(
        "/v0/tokens",
        get(move |headers: HeaderMap| {
            let seen = seen.clone();
            async move {
                let authorization = headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                seen.lock().expect("lock").push(authorization);
                (StatusCode::OK, axum::Json(json!({ "tokens": [] })))
            }
        }),
    ))
    .await;

    let home = TempDir::new().expect("home");
    fs::create_dir_all(credentials_dir(&home)).expect("credentials dir");
    let token_path = credentials_dir(&home).join("control-plane-token");
    fs::write(&token_path, API_TOKEN).expect("seed token");
    #[cfg(unix)]
    fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600)).expect("chmod");

    let output = tokio::task::spawn_blocking({
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
    assert_eq!(
        list_bearers.lock().expect("lock").clone(),
        vec![format!("bearer {API_TOKEN}")],
        "cloud commands present the durable token"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "runtime paths must never contact WorkOS"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_token_fails_loud_with_relogin_guidance() {
    let hits = Arc::new(AtomicUsize::new(0));
    let workos = spawn(workos_router(hits.clone(), Arc::new(AtomicUsize::new(0)))).await;
    let control_plane = spawn(Router::new().route(
        "/v0/tokens",
        get(|| async {
            (
                StatusCode::UNAUTHORIZED,
                axum::Json(json!({ "error": "token expired" })),
            )
        }),
    ))
    .await;

    let home = TempDir::new().expect("home");
    fs::create_dir_all(credentials_dir(&home)).expect("credentials dir");
    let token_path = credentials_dir(&home).join("control-plane-token");
    fs::write(&token_path, "vg-expired-token").expect("seed token");
    #[cfg(unix)]
    fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600)).expect("chmod");

    let output = tokio::task::spawn_blocking({
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
    assert!(!output.status.success(), "a 401 must fail the command");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("verglas login"),
        "failure must tell the user to re-login: {stderr}"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "no silent WorkOS refresh on 401"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logout_deletes_the_durable_token() {
    let home = TempDir::new().expect("home");
    fs::create_dir_all(credentials_dir(&home)).expect("credentials dir");
    let token_path = credentials_dir(&home).join("control-plane-token");
    fs::write(&token_path, API_TOKEN).expect("seed token");

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
    assert!(!token_path.exists(), "logout must delete the durable token");
}
