//! End-to-end tests for `verglas table delete`.
//!
//! Unlike the other table verbs (which speak the server), `delete` drops a table
//! through the tenant's Iceberg REST catalog. These stand up a local mock catalog
//! (an axum server on an ephemeral port) and drive the real binary against it,
//! with HOME redirected to a tempdir whose `~/.verglas/config.toml` points its
//! `[catalog]` section at the mock. The mock records the request path and bearer
//! it received, so the wire call is asserted end to end. No real catalog is
//! contacted.

use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde_json::{Value, json};

/// The bearer token the mock catalog honors; written to the credentials file the
/// config points at.
const CATALOG_TOKEN: &str = "cat_tok_abc123";

/// The record of DELETEs the mock saw: `(namespace/table, authorization-header)`.
type Deletes = Arc<Mutex<Vec<(String, String)>>>;

/// What the mock catalog records: the DELETE paths it received and the bearer on
/// each, plus the route prefix it advertises from `/v1/config`.
#[derive(Clone)]
struct MockState {
    deletes: Deletes,
    prefix: String,
}

/// `GET /v1/config` — advertises the configured route prefix (empty for none).
async fn get_config(State(state): State<MockState>) -> Json<Value> {
    Json(json!({ "defaults": {}, "overrides": { "prefix": state.prefix } }))
}

/// `DELETE .../namespaces/{ns}/tables/{table}` — records the hit and 204s.
async fn drop_table(
    State(state): State<MockState>,
    AxumPath((ns, table)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> StatusCode {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    state
        .deletes
        .lock()
        .expect("lock")
        .push((format!("{ns}/{table}"), bearer));
    StatusCode::NO_CONTENT
}

/// Spawns a mock catalog with the given advertised prefix; returns its base URL
/// and the shared record of DELETE calls.
fn spawn_catalog(prefix: &str) -> (String, Deletes) {
    let deletes = Arc::new(Mutex::new(Vec::new()));
    let state = MockState {
        deletes: deletes.clone(),
        prefix: prefix.to_owned(),
    };
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async move {
            let prefixed = if state.prefix.is_empty() {
                "/v1/namespaces/{ns}/tables/{table}".to_owned()
            } else {
                format!("/v1/{}/namespaces/{{ns}}/tables/{{table}}", state.prefix)
            };
            let app = Router::new()
                .route("/v1/config", get(get_config))
                .route(&prefixed, delete(drop_table))
                .with_state(state);
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, app).await.expect("serve");
        });
    });
    (format!("http://{addr}"), deletes)
}

/// Writes a `~/.verglas/config.toml` whose `[catalog]` points at `url`, plus the
/// bearer token file it references.
fn write_catalog_config(home: &Path, url: &str) {
    let creds = home.join(".verglas/credentials");
    std::fs::create_dir_all(&creds).expect("mkdir creds");
    let token_path = creds.join("cloud-catalog.token");
    std::fs::write(&token_path, CATALOG_TOKEN).expect("write token");
    std::fs::write(
        home.join(".verglas/config.toml"),
        format!(
            "[catalog]\nuri = \"{url}\"\ncredentials_file = \"{}\"\n",
            token_path.display()
        ),
    )
    .expect("write config");
}

/// Runs the binary with HOME redirected and stdin from `stdin` (empty when
/// `None`, which reads as EOF).
fn run(home: &Path, args: &[&str], stdin: Option<&str>) -> (bool, String, String) {
    use std::io::Write;
    let mut child = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(args)
        .env("HOME", home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    if let Some(text) = stdin {
        child
            .stdin
            .as_mut()
            .expect("stdin")
            .write_all(text.as_bytes())
            .expect("write stdin");
    }
    let out = child.wait_with_output().expect("wait");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn delete_with_yes_drops_via_the_catalog() {
    let (url, deletes) = spawn_catalog("");
    let home = tempfile::tempdir().expect("tempdir");
    write_catalog_config(home.path(), &url);

    let (ok, stdout, stderr) = run(
        home.path(),
        &["table", "delete", "agent_data.orders", "--yes"],
        None,
    );
    assert!(ok, "delete must succeed: {stderr}");
    assert!(
        stdout.contains("dropped table agent_data.orders"),
        "stdout: {stdout}"
    );

    let recorded = deletes.lock().expect("lock");
    assert_eq!(recorded.len(), 1, "exactly one DELETE: {recorded:?}");
    assert_eq!(recorded[0].0, "agent_data/orders", "path: {recorded:?}");
    assert_eq!(
        recorded[0].1,
        format!("Bearer {CATALOG_TOKEN}"),
        "bearer from the token file must be sent"
    );
    // The token value itself must never be printed.
    assert!(!stdout.contains(CATALOG_TOKEN) && !stderr.contains(CATALOG_TOKEN));
}

#[test]
fn delete_honors_the_catalog_route_prefix() {
    let (url, deletes) = spawn_catalog("demo");
    let home = tempfile::tempdir().expect("tempdir");
    write_catalog_config(home.path(), &url);

    let (ok, _stdout, stderr) = run(
        home.path(),
        &["table", "delete", "rlean.custom_points", "--yes"],
        None,
    );
    assert!(ok, "delete must succeed with a prefixed catalog: {stderr}");
    let recorded = deletes.lock().expect("lock");
    assert_eq!(
        recorded.len(),
        1,
        "one DELETE reached the prefixed route: {recorded:?}"
    );
    assert_eq!(recorded[0].0, "rlean/custom_points");
}

#[test]
fn delete_without_yes_and_no_confirmation_aborts() {
    let (url, deletes) = spawn_catalog("");
    let home = tempfile::tempdir().expect("tempdir");
    write_catalog_config(home.path(), &url);

    // No --yes and stdin answers "n": the drop must be refused and no DELETE
    // may reach the catalog.
    let (ok, _stdout, stderr) = run(
        home.path(),
        &["table", "delete", "agent_data.orders"],
        Some("n\n"),
    );
    assert!(!ok, "an unconfirmed delete must exit non-zero");
    assert!(
        stderr.contains("aborted"),
        "stderr must explain the abort: {stderr}"
    );
    assert!(
        deletes.lock().expect("lock").is_empty(),
        "no DELETE may be sent when the drop is not confirmed"
    );
}

#[test]
fn delete_rejects_a_bare_table_name() {
    let (url, _deletes) = spawn_catalog("");
    let home = tempfile::tempdir().expect("tempdir");
    write_catalog_config(home.path(), &url);

    let (ok, _stdout, stderr) = run(home.path(), &["table", "delete", "orders", "--yes"], None);
    assert!(!ok, "a bare name (no namespace) must be rejected");
    assert!(
        stderr.contains("namespace.name"),
        "the error must name the expected form: {stderr}"
    );
}
