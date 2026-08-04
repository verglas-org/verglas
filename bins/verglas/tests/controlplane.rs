//! End-to-end tests for `verglas login` and the control-plane-enriched platform
//! primitives (`source`/`mv`/`sink`).
//!
//! These stand up a local mock of the control plane API (an axum server bound
//! to an ephemeral port) and drive the real `verglas` binary against it. The
//! CLI never contacts a real cloud endpoint here. HOME is redirected to a
//! tempdir so the token file and config are written under the test's own
//! `~/.verglas`, never the developer's.
//!
//! The generic `verglas deployments` command was removed. Its cross-node/cloud
//! view now lives inside the primitives: `verglas source|mv|sink list` merge the
//! tenant's cloud deployments (from the control plane) with the local
//! self-managed ones when logged in, and `verglas source|mv|sink show` fetch an
//! artifact's code from the registry. Not logged in, the primitives stay
//! local-only and never call the control plane.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::extract::{Form, Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

/// The one API key the mock control plane accepts. Any other key gets a 401.
const GOOD_KEY: &str = "vg_live_test_key_abc123";

/// The tenant's scoped S3 secret and catalog bearer token the mock `/v1/lakehouse`
/// returns. These are secrets: `verglas login` must write them only to 0600
/// credential files and must never print them.
const LAKE_SECRET_KEY: &str = "s3-secret-access-key-value-do-not-print";
const LAKE_CATALOG_TOKEN: &str = "catalog-bearer-token-do-not-print";
const LAKE_ACCESS_KEY_ID: &str = "lake-access-key-id-1234";
const LAKE_BUCKET: &str = "verglas-personal";

/// The OAuth access token the mock `/oauth/token` returns for both the browser
/// and device flows. It is a bearer credential for `/v1/provision` only and must
/// never be written to disk or printed by the CLI.
const ACCESS_TOKEN: &str = "oauth-access-token-do-not-print";
/// The long-lived control-plane API key the mock `/v1/provision` returns. This is
/// what the CLI stores in the 0600 token file after an OAuth flow (NOT the access
/// token). It is a secret and must never be printed.
const PROV_API_KEY: &str = "vgk_provisioned_key_do_not_print";
/// The short user code the mock device endpoint issues; the CLI must print it for
/// the human to type on the verification page.
const DEVICE_USER_CODE: &str = "WXYZ-1234";
/// The verification URI the mock device endpoint issues; the CLI must print it so
/// the human can open it on any device.
const DEVICE_VERIFICATION_URI: &str = "https://cloud.example.com/cli/device";
/// The pre-filled verification URI the mock device endpoint issues.
const DEVICE_VERIFICATION_URI_COMPLETE: &str =
    "https://cloud.example.com/cli/device?user_code=WXYZ-1234";

/// The code the mock registry stores for the `orders_src` source artifact. The
/// `source show --code` path must print this verbatim.
const ORDERS_SRC_CODE: &str = ".decoded, err = parse_json(.message)\n.order_id = .decoded.id";

/// The mock control plane's state: the single key it honors and a counter of
/// how many requests it has served, so a test can assert the CLI made none.
#[derive(Clone)]
struct MockState {
    good_key: String,
    hits: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
}

/// A running mock control plane: its base URL, the shared request counter, and
/// the device-token poll counter (so a test can assert the CLI polled).
struct MockControlPlane {
    url: String,
    #[allow(dead_code)]
    hits: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
}

/// Rejects a request whose bearer token is not the mock's good key, so the
/// tests can exercise the 401 path exactly as the real control plane would.
/// Counts every request that reaches a handler.
fn authorize(headers: &HeaderMap, state: &MockState) -> Result<(), StatusCode> {
    state.hits.fetch_add(1, Ordering::SeqCst);
    let got = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if got == format!("Bearer {}", state.good_key) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// `GET /v1/me` — returns the account the key belongs to.
async fn me(headers: HeaderMap, State(state): State<MockState>) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({
        "tenant_id": "tenant-42",
        "account_email": "j.brown9513@gmail.com"
    })))
}

/// The tenant's scoped lakehouse config, shared by `GET /v1/lakehouse` (api-key
/// flow) and the `lakehouse` field of `POST /v1/provision` (OAuth flows). Both
/// carry identical field names, so the CLI decodes both into the same struct.
fn lakehouse_json() -> Value {
    json!({
        "bucket": LAKE_BUCKET,
        "s3_endpoint": "https://storage.example.com",
        "s3_region": "auto",
        "warehouse": "acct_verglas-personal",
        "catalog_uri": "https://catalog.example.com/acct/verglas-personal",
        "access_key_id": LAKE_ACCESS_KEY_ID,
        "secret_access_key": LAKE_SECRET_KEY,
        "catalog_token": LAKE_CATALOG_TOKEN
    })
}

/// `GET /v1/lakehouse` — the tenant's scoped lakehouse config. `verglas login`
/// writes the daemon config and its 0600 credential files from this.
async fn lakehouse(
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(lakehouse_json()))
}

/// `POST /oauth/device/code` — starts the device flow, returning the user code,
/// verification URIs, and a short poll interval so the test runs fast.
async fn oauth_device_code(Form(form): Form<HashMap<String, String>>) -> Json<Value> {
    // The CLI always identifies itself as `verglas-cli`.
    assert_eq!(
        form.get("client_id").map(String::as_str),
        Some("verglas-cli"),
        "device-code request must carry client_id=verglas-cli"
    );
    Json(json!({
        "device_code": "device-code-secret-abc",
        "user_code": DEVICE_USER_CODE,
        "verification_uri": DEVICE_VERIFICATION_URI,
        "verification_uri_complete": DEVICE_VERIFICATION_URI_COMPLETE,
        "expires_in": 300,
        "interval": 1
    }))
}

/// `POST /oauth/token` — handles both grants. The authorization-code grant
/// (browser flow) checks the PKCE verifier round-trips and returns an access
/// token. The device-code grant returns `authorization_pending` on the first
/// poll, then an access token, so the test proves the CLI polls and honors the
/// interval.
async fn oauth_token(
    State(state): State<MockState>,
    Form(form): Form<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    assert_eq!(
        form.get("client_id").map(String::as_str),
        Some("verglas-cli"),
        "token request must carry client_id=verglas-cli"
    );
    let grant = form.get("grant_type").map(String::as_str).unwrap_or("");
    let token = json!({
        "access_token": ACCESS_TOKEN,
        "token_type": "Bearer",
        "expires_in": 3600
    });
    match grant {
        "authorization_code" => {
            let has_code = form.get("code").is_some_and(|c| !c.is_empty());
            let has_verifier = form.get("code_verifier").is_some_and(|c| !c.is_empty());
            assert!(has_code, "authorization-code grant must carry a code");
            assert!(
                has_verifier,
                "authorization-code grant must carry the PKCE code_verifier"
            );
            assert!(
                form.get("redirect_uri")
                    .is_some_and(|u| u.starts_with("http://127.0.0.1:")),
                "authorization-code grant must echo the loopback redirect_uri"
            );
            Ok(Json(token))
        }
        "urn:ietf:params:oauth:grant-type:device_code" => {
            assert!(
                form.get("device_code").is_some_and(|c| !c.is_empty()),
                "device-code grant must carry the device_code"
            );
            let n = state.polls.fetch_add(1, Ordering::SeqCst);
            if n < 1 {
                Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "authorization_pending" })),
                ))
            } else {
                Ok(Json(token))
            }
        }
        other => Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("unsupported_grant_type: {other}") })),
        )),
    }
}

/// `POST /v1/provision` — both OAuth flows converge here. Requires the OAuth
/// access token as the bearer (NOT the stored api key), and returns the tenant
/// identity plus the long-lived api key and the scoped lakehouse config.
async fn provision(headers: HeaderMap) -> Result<Json<Value>, StatusCode> {
    let got = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if got != format!("Bearer {ACCESS_TOKEN}") {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(Json(json!({
        "tenant_id": "tenant-42",
        "account_email": "j.brown9513@gmail.com",
        "api_key": PROV_API_KEY,
        "lakehouse": lakehouse_json()
    })))
}

/// `GET /v1/deployments` — a mixed registry: a cloud source, a cloud MV, and a
/// local sink, so the primitives can each filter to their own kind and span
/// both placements. The list payload carries no `code` (that lives on the
/// detail endpoint).
async fn deployments(
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    // The list endpoint wraps its rows in a `deployments` field, matching the
    // control plane and the CLI's decoder.
    Ok(Json(json!({ "deployments": [
        {
            "id": "d1",
            "name": "orders_src",
            "kind": "source",
            "placement": "cloud",
            "node_id": null,
            "status": "running",
            "schedule": "continuous",
            "target_tables": ["agent_data.orders"],
            "updated_at": "2026-07-19T10:00:00Z"
        },
        {
            "id": "d2",
            "name": "daily_rollup",
            "kind": "mv",
            "placement": "cloud",
            "node_id": null,
            "status": "running",
            "schedule": "0 2 * * *",
            "target_tables": ["agent_data.rollup"],
            "updated_at": "2026-07-19T11:00:00Z"
        },
        {
            "id": "d3",
            "name": "pager",
            "kind": "sink",
            "placement": "local",
            "node_id": "node-a",
            "status": "paused",
            "schedule": null,
            "target_tables": [],
            "updated_at": "2026-07-19T12:00:00Z"
        }
    ] })))
}

/// `GET /v1/deployments/:id` — the detail record, which carries the artifact's
/// `code`. Only `d1` (`orders_src`) is known to the mock.
async fn deployment_detail(
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    if id != "d1" {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(json!({
        "id": "d1",
        "name": "orders_src",
        "kind": "source",
        "placement": "cloud",
        "node_id": null,
        "status": "running",
        "schedule": "continuous",
        "target_tables": ["agent_data.orders"],
        "updated_at": "2026-07-19T10:00:00Z",
        "code": ORDERS_SRC_CODE
    })))
}

/// Boots the mock control plane on an ephemeral port and returns its base URL
/// and request counter. The server runs on its own thread with a current-thread
/// runtime so the test body stays synchronous.
fn spawn_control_plane() -> MockControlPlane {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    let hits = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let server_hits = hits.clone();
    let server_polls = polls.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async move {
            let app = Router::new()
                .route("/v1/me", get(me))
                .route("/v1/lakehouse", get(lakehouse))
                .route("/v1/deployments", get(deployments))
                .route("/v1/deployments/{id}", get(deployment_detail))
                .route("/oauth/device/code", post(oauth_device_code))
                .route("/oauth/token", post(oauth_token))
                .route("/v1/provision", post(provision))
                .with_state(MockState {
                    good_key: GOOD_KEY.to_owned(),
                    hits: server_hits,
                    polls: server_polls,
                });
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, app).await.expect("serve");
        });
    });
    MockControlPlane {
        url: format!("http://{addr}"),
        hits,
        polls,
    }
}

/// The token file the CLI writes the API key to under a given HOME.
fn token_path(home: &Path) -> std::path::PathBuf {
    home.join(".verglas/credentials/control-plane-token")
}

/// The config file the CLI writes the control plane URL into under a given HOME.
fn config_path(home: &Path) -> std::path::PathBuf {
    home.join(".verglas/config.toml")
}

/// The backend (S3) credentials file `verglas login` writes under a given HOME.
fn backend_credentials_path(home: &Path) -> std::path::PathBuf {
    home.join(".verglas/credentials/backend.credentials")
}

/// The catalog credentials file `verglas login` writes under a given HOME.
fn catalog_credentials_path(home: &Path) -> std::path::PathBuf {
    home.join(".verglas/credentials/catalog.token")
}

/// Asserts a file exists and is mode 0600.
#[cfg(unix)]
fn assert_0600(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = std::fs::metadata(path)
        .unwrap_or_else(|_| panic!("stat {}", path.display()))
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "{} must be private (0600)", path.display());
}

/// Logs the CLI in against `url` under `home`, asserting success. Shared setup
/// for the tests that need a stored login.
#[allow(dead_code)]
fn login(home: &Path, url: &str) {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["login", "--api-key", "--url", url, GOOD_KEY])
        .env("HOME", home)
        .output()
        .expect("login runs");
    assert!(out.status.success(), "login must succeed");
}

#[test]
fn login_stores_token_0600_and_url_and_confirms_the_account() {
    let cp = spawn_control_plane();
    let home = tempfile::tempdir().expect("tempdir");

    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["login", "--api-key", "--url", &cp.url, GOOD_KEY])
        .env("HOME", home.path())
        .output()
        .expect("binary runs");

    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        out.status.success(),
        "login must succeed with a good key: {stderr}"
    );
    assert!(
        stdout.contains("logged in as") && stdout.contains("j.brown9513@gmail.com"),
        "login must confirm the account it logged in as: {stdout}"
    );

    // The API key lands in the token file, and nothing else.
    let token = std::fs::read_to_string(token_path(home.path())).expect("token file");
    assert_eq!(token.trim(), GOOD_KEY, "the API key is stored verbatim");

    // The key must never be printed.
    assert!(
        !stdout.contains(GOOD_KEY),
        "the API key must not be echoed to stdout: {stdout}"
    );

    // The token file is mode 0600.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(token_path(home.path()))
            .expect("stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "the token file must be private (0600)");
    }

    // The URL is recorded in the config under [control_plane].
    let config = std::fs::read_to_string(config_path(home.path())).expect("config file");
    assert!(
        config.contains("[control_plane]") && config.contains(&cp.url),
        "the control plane URL must be recorded in config.toml: {config}"
    );
}

#[test]
fn login_writes_the_daemon_config_and_scoped_credential_files() {
    // After login, the daemon is fully configured for the tenant from
    // `/v1/lakehouse`: [backend] and [catalog] point at the tenant's bucket and
    // catalog, and the scoped S3 key pair + catalog token land in 0600 credential
    // files. The secret values are never printed.
    let cp = spawn_control_plane();
    let home = tempfile::tempdir().expect("tempdir");

    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["login", "--api-key", "--url", &cp.url, GOOD_KEY])
        .env("HOME", home.path())
        .output()
        .expect("binary runs");
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(out.status.success(), "login must succeed: {stderr}");
    let stdout = String::from_utf8(out.stdout).expect("utf8");

    // The daemon config carries the scoped backend + catalog, pointing at the
    // credential files (never inlining the secret values).
    let config = std::fs::read_to_string(config_path(home.path())).expect("config file");
    assert!(
        config.contains("[backend]"),
        "backend section written: {config}"
    );
    assert!(
        config.contains(&format!("bucket = \"{LAKE_BUCKET}\"")),
        "backend bucket set: {config}"
    );
    assert!(
        config.contains("endpoint = \"https://storage.example.com\""),
        "backend endpoint set: {config}"
    );
    assert!(
        config.contains("[catalog]"),
        "catalog section written: {config}"
    );
    assert!(
        config.contains("warehouse = \"acct_verglas-personal\""),
        "catalog warehouse set: {config}"
    );
    assert!(
        config.contains("uri = \"https://catalog.example.com/acct/verglas-personal\""),
        "catalog uri set: {config}"
    );
    assert!(
        !config.contains(LAKE_SECRET_KEY) && !config.contains(LAKE_CATALOG_TOKEN),
        "secrets must never be inlined in the config: {config}"
    );

    // The backend AWS-INI file carries the scoped S3 key pair, mode 0600.
    let backend_creds =
        std::fs::read_to_string(backend_credentials_path(home.path())).expect("backend creds file");
    assert!(
        backend_creds.contains(LAKE_ACCESS_KEY_ID),
        "access key id written"
    );
    assert!(
        backend_creds.contains(LAKE_SECRET_KEY),
        "secret key written to the file"
    );
    assert!(
        backend_creds.contains("aws_access_key_id")
            && backend_creds.contains("aws_secret_access_key"),
        "AWS-INI format: {backend_creds}"
    );

    // The catalog file carries the bearer token, mode 0600.
    let catalog_creds =
        std::fs::read_to_string(catalog_credentials_path(home.path())).expect("catalog creds file");
    assert_eq!(
        catalog_creds.trim(),
        LAKE_CATALOG_TOKEN,
        "catalog token written to the file"
    );

    #[cfg(unix)]
    {
        assert_0600(&backend_credentials_path(home.path()));
        assert_0600(&catalog_credentials_path(home.path()));
    }

    // No secret value is ever printed.
    assert!(
        !stdout.contains(LAKE_SECRET_KEY) && !stdout.contains(LAKE_CATALOG_TOKEN),
        "login must not print secret values: {stdout}"
    );
    // The non-secret summary confirms what it configured.
    assert!(
        stdout.contains(LAKE_BUCKET) && stdout.contains("acct_verglas-personal"),
        "login prints the configured bucket + warehouse: {stdout}"
    );
}

#[test]
fn login_rejects_a_bad_key_and_writes_nothing() {
    let cp = spawn_control_plane();
    let home = tempfile::tempdir().expect("tempdir");

    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["login", "--api-key", "--url", &cp.url, "not-the-real-key"])
        .env("HOME", home.path())
        .output()
        .expect("binary runs");

    assert!(
        !out.status.success(),
        "login must fail when the control plane rejects the key"
    );
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        !stderr.contains("panicked"),
        "a rejected key is a plain error, not a panic: {stderr}"
    );
    assert!(
        !token_path(home.path()).exists(),
        "a rejected key must not leave a token file behind"
    );
}

#[test]
fn login_reads_the_key_from_stdin_when_not_an_argument() {
    let cp = spawn_control_plane();
    let home = tempfile::tempdir().expect("tempdir");

    let mut child = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["login", "--api-key", "--url", &cp.url])
        .env("HOME", home.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    {
        use std::io::Write as _;
        let stdin = child.stdin.as_mut().expect("stdin");
        writeln!(stdin, "{GOOD_KEY}").expect("write key");
    }
    let out = child.wait_with_output().expect("wait");

    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        out.status.success(),
        "login must accept the key from stdin: {stderr}"
    );
    let token = std::fs::read_to_string(token_path(home.path())).expect("token file");
    assert_eq!(token.trim(), GOOD_KEY, "the stdin key is stored");
}

/// Parses the loopback port out of the CLI's `listening on http://127.0.0.1:<port>/callback`
/// line. Returns `None` for any other line.
fn parse_listening_port(line: &str) -> Option<u16> {
    let marker = "127.0.0.1:";
    let idx = line.find(marker)?;
    let rest = &line[idx + marker.len()..];
    let end = rest.find('/').unwrap_or(rest.len());
    rest[..end].trim().parse().ok()
}

/// Extracts the value of query parameter `key` from a line that contains a URL
/// (used to read the `state` the CLI put in the printed authorize URL).
fn parse_query_value(line: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=");
    let idx = line.find(&needle)?;
    let rest = &line[idx + needle.len()..];
    let end = rest
        .find(|c: char| c == '&' || c.is_whitespace())
        .unwrap_or(rest.len());
    let value = &rest[..end];
    (!value.is_empty()).then(|| value.to_owned())
}

/// Delivers the browser redirect to the CLI's loopback listener, standing in for
/// a real browser. Reads only enough to hand over the code and state; the CLI
/// reads the request line and responds, so any error reading the reply is fine.
fn deliver_callback(port: u16, state: &str) {
    let url = format!("http://127.0.0.1:{port}/callback?code=test-auth-code&state={state}");
    let _ = reqwest::blocking::get(&url);
}

#[test]
fn browser_login_completes_pkce_flow_and_writes_files() {
    // Default `verglas login` runs the browser authorization-code + PKCE flow.
    // A real browser can't run here, so the CLI (with VERGLAS_LOGIN_NO_BROWSER=1)
    // prints its loopback URL and the authorize URL, and the test plays the
    // browser: it reads the port and state, then GETs the loopback /callback.
    let cp = spawn_control_plane();
    let home = tempfile::tempdir().expect("tempdir");

    let mut child = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["login", "--url", &cp.url])
        .env("HOME", home.path())
        .env("VERGLAS_LOGIN_NO_BROWSER", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");

    let stdout = child.stdout.take().expect("stdout");
    let mut reader = std::io::BufReader::new(stdout);
    let mut collected = String::new();
    let mut port: Option<u16> = None;
    let mut state: Option<String> = None;
    let mut delivered = false;
    loop {
        let mut line = String::new();
        let n = std::io::BufRead::read_line(&mut reader, &mut line).expect("read line");
        if n == 0 {
            break;
        }
        collected.push_str(&line);
        if port.is_none() {
            port = parse_listening_port(&line);
        }
        if state.is_none() {
            state = parse_query_value(&line, "state");
        }
        if !delivered && let (Some(p), Some(s)) = (port, state.as_ref()) {
            deliver_callback(p, s);
            delivered = true;
        }
    }
    let out = child.wait_with_output().expect("wait");
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        delivered,
        "the test must have delivered the callback: {collected}"
    );
    assert!(
        out.status.success(),
        "browser login must succeed: {stderr}\n{collected}"
    );

    // The token file holds the PROVISIONED api key, not the OAuth access token.
    let token = std::fs::read_to_string(token_path(home.path())).expect("token file");
    assert_eq!(
        token.trim(),
        PROV_API_KEY,
        "the provisioned api key is stored, not the access token"
    );
    #[cfg(unix)]
    assert_0600(&token_path(home.path()));

    // The scoped credential + config files are written from the provision payload.
    let backend_creds =
        std::fs::read_to_string(backend_credentials_path(home.path())).expect("backend creds");
    assert!(backend_creds.contains(LAKE_ACCESS_KEY_ID) && backend_creds.contains(LAKE_SECRET_KEY));
    let catalog_creds =
        std::fs::read_to_string(catalog_credentials_path(home.path())).expect("catalog creds");
    assert_eq!(catalog_creds.trim(), LAKE_CATALOG_TOKEN);
    #[cfg(unix)]
    {
        assert_0600(&backend_credentials_path(home.path()));
        assert_0600(&catalog_credentials_path(home.path()));
    }
    let config = std::fs::read_to_string(config_path(home.path())).expect("config");
    assert!(
        config.contains("[backend]") && config.contains(&format!("bucket = \"{LAKE_BUCKET}\"")),
        "backend section written: {config}"
    );
    assert!(
        config.contains("[catalog]") && config.contains("acct_verglas-personal"),
        "catalog section written: {config}"
    );

    // The summary confirms the account, and no secret is ever printed.
    assert!(
        collected.contains("logged in as") && collected.contains("j.brown9513@gmail.com"),
        "the summary must confirm the account: {collected}"
    );
    for secret in [
        PROV_API_KEY,
        ACCESS_TOKEN,
        LAKE_SECRET_KEY,
        LAKE_CATALOG_TOKEN,
    ] {
        assert!(
            !collected.contains(secret),
            "secret leaked to stdout: {collected}"
        );
        assert!(
            !stderr.contains(secret),
            "secret leaked to stderr: {stderr}"
        );
    }
}

#[test]
fn device_login_polls_then_writes_files() {
    // `verglas login --device` runs the headless device-code flow: it prints the
    // user code and verification URI, polls the token endpoint (pending, then a
    // token), provisions, and writes the same files as every other flow.
    let cp = spawn_control_plane();
    let home = tempfile::tempdir().expect("tempdir");

    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["login", "--device", "--url", &cp.url])
        .env("HOME", home.path())
        .output()
        .expect("binary runs");
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        out.status.success(),
        "device login must succeed: {stderr}\n{stdout}"
    );

    // The human-facing code and URI are printed so the user can authorize.
    assert!(
        stdout.contains(DEVICE_USER_CODE),
        "the user code must be shown: {stdout}"
    );
    assert!(
        stdout.contains(DEVICE_VERIFICATION_URI),
        "the verification URI must be shown: {stdout}"
    );

    // The CLI polled more than once: at least one pending, then the token.
    assert!(
        cp.polls.load(Ordering::SeqCst) > 1,
        "the CLI must have polled the token endpoint more than once"
    );

    // The provisioned api key + scoped files land on disk exactly as other flows.
    let token = std::fs::read_to_string(token_path(home.path())).expect("token file");
    assert_eq!(
        token.trim(),
        PROV_API_KEY,
        "the provisioned api key is stored"
    );
    #[cfg(unix)]
    assert_0600(&token_path(home.path()));
    let config = std::fs::read_to_string(config_path(home.path())).expect("config");
    assert!(
        config.contains("[backend]") && config.contains(&format!("bucket = \"{LAKE_BUCKET}\"")),
        "backend section written: {config}"
    );

    for secret in [
        PROV_API_KEY,
        ACCESS_TOKEN,
        LAKE_SECRET_KEY,
        LAKE_CATALOG_TOKEN,
    ] {
        assert!(
            !stdout.contains(secret) && !stderr.contains(secret),
            "secret leaked: {stdout}\n{stderr}"
        );
    }
}

#[test]
fn deployments_command_no_longer_exists() {
    // The generic `verglas deployments` command was removed: its functionality
    // folded into the platform primitives. Invoking it must be a clap
    // unknown-command error, exit non-zero, and never reach a control plane.
    let home = tempfile::tempdir().expect("tempdir");
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("deployments")
        .env("HOME", home.path())
        .output()
        .expect("binary runs");
    assert!(
        !out.status.success(),
        "`verglas deployments` must fail as an unknown command"
    );
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        stderr.contains("unrecognized subcommand") || stderr.contains("unexpected argument"),
        "`verglas deployments` must be a clap unknown-command error: {stderr}"
    );
}
