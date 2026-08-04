//! End-to-end tests for the cloud resource command groups (`verglas workers |
//! containers | db | volumes | secrets`).
//!
//! Each test stands up a local mock of the control plane (an axum server on an
//! ephemeral port) and drives the real `verglas` binary against it, with HOME
//! redirected to a tempdir so the stored login lives under the test's own
//! `~/.verglas`. The CLI never contacts a real cloud endpoint. Two mocks are
//! used: a FULL one that implements every route (the happy paths), and a
//! DEPLOYMENTS-ONLY one whose fallback 404s every other route (the "server does
//! not support this yet" path for containers/db).

use std::net::TcpListener;
use std::path::Path;
use std::process::Command;

use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use serde_json::{Value, json};

/// The one API key the mock control plane accepts.
const GOOD_KEY: &str = "vg_live_cloud_key_abc123";

/// The mock's shared state: the key it honors.
#[derive(Clone)]
struct MockState {
    good_key: String,
}

/// Rejects a request whose bearer token is not the good key.
fn authorize(headers: &HeaderMap, state: &MockState) -> Result<(), StatusCode> {
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

/// A worker (deployment) row the mock returns.
fn worker_row(id: &str, name: &str, kind: &str) -> Value {
    json!({
        "id": id,
        "name": name,
        "kind": kind,
        "trigger": "cron",
        "placement": "cloud",
        "status": "active",
        "schedule": "0 5 * * *",
        "target_tables": [format!("agent_data.{name}")],
        "code": "// worker code",
        "config": { "min_instances": 1 },
        "updated_at": "2026-08-01T00:00:00Z"
    })
}

async fn deployments_list(
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({ "deployments": [
        worker_row("d1", "orders", "source"),
        worker_row("d2", "rollup", "mv"),
    ] })))
}

async fn deployments_create(
    headers: HeaderMap,
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    // Echo the posted spec with a minted id, so the test can assert the override
    // fields (name/schedule) reached the server in the request body.
    let mut created = body;
    created["id"] = json!("d-new");
    created["status"] = json!("registered");
    Ok(Json(created))
}

async fn deployment_get(
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    match id.as_str() {
        "d1" => Ok(Json(worker_row("d1", "orders", "source"))),
        "d2" => Ok(Json(worker_row("d2", "rollup", "mv"))),
        _ => Err(StatusCode::NOT_FOUND),
    }
}

async fn deployment_patch(
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    let mut row = worker_row(&id, "orders", "source");
    // Reflect the patched fields so the test can assert they were sent.
    if let Some(obj) = body.as_object() {
        for (k, v) in obj {
            row[k] = v.clone();
        }
    }
    Ok(Json(row))
}

async fn deployment_delete(
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({ "deleted": id })))
}

async fn deployment_run(
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({ "dispatched": id, "run_id": "run-1" })))
}

async fn containers_list(
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({ "containers": [
        { "id": "c1", "name": "pg", "image": "postgres:16", "mode": "stateful", "status": "running", "instances": 1 }
    ] })))
}

async fn containers_create(
    headers: HeaderMap,
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    let mut created = body;
    created["id"] = json!("c-new");
    created["status"] = json!("created");
    Ok(Json(created))
}

async fn container_scale(
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(
        json!({ "id": id, "instances": body["instances"], "status": "scaling" }),
    ))
}

/// The curated catalog the mock returns: one headless MCP default (memory) and one
/// UI-only app (netdata), so the CLI's yes/no columns are exercised both ways.
async fn catalog_list(
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({ "catalog": [
        {
            "id": "memory", "name": "Memory (cognee)", "description": "Durable agent memory via MCP.",
            "has_ui": false, "has_mcp": true, "is_default": true,
            "hostname": null, "mcp_endpoint": "https://memory-acme.verglas.dev/mcp"
        },
        {
            "id": "netdata", "name": "Netdata", "description": "Per-node metrics dashboard.",
            "has_ui": true, "has_mcp": false, "is_default": false,
            "hostname": "netdata-acme.verglas.dev", "mcp_endpoint": null
        }
    ] })))
}

/// Deploy a catalog app. `rill` is a fresh UI app (201, created); `memory` is a
/// headless MCP app reported as already deployed (200, created:false) so the CLI's
/// idempotent branch, UI line, and MCP line are all exercised.
async fn catalog_deploy(
    AxumPath(catalog_id): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    authorize(&headers, &state)?;
    match catalog_id.as_str() {
        "rill" => Ok((
            StatusCode::CREATED,
            Json(json!({
                "id": "c-rill", "name": "rill", "catalog_id": "rill", "has_ui": true,
                "hostname": "rill-acme.verglas.dev", "mcp_endpoint": null, "created": true
            })),
        )),
        "memory" => Ok((
            StatusCode::OK,
            Json(json!({
                "id": "c-mem", "name": "memory", "catalog_id": "memory", "has_ui": false,
                "hostname": null, "mcp_endpoint": "https://memory-acme.verglas.dev/mcp", "created": false
            })),
        )),
        _ => Err(StatusCode::NOT_FOUND),
    }
}

/// A config-schema response for the memory container: a locked default mode plus a
/// bring-your-own mode carrying a secret field, so the CLI can learn which keys are
/// secret. `web1` is a non-configurable container.
fn memory_config(mode: &str, values: &Value) -> Value {
    // Reflect only the NON-secret provided values; report the secret as set when it
    // was provided — the value itself is never returned.
    let llm_endpoint = values.get("LLM_ENDPOINT").cloned().unwrap_or(Value::Null);
    let secret_set = values.get("LLM_API_KEY").is_some();
    json!({
        "configurable": true,
        "schema": {
            "modeKey": "mode", "defaultMode": "default",
            "modes": [
                { "id": "default", "label": "Platform default", "locked": true, "fields": [] },
                { "id": "custom", "label": "Bring your own", "locked": false, "fields": [
                    { "key": "LLM_ENDPOINT", "label": "LLM endpoint", "secret": false },
                    { "key": "LLM_API_KEY", "label": "LLM API key", "secret": true }
                ] }
            ]
        },
        "config": {
            "mode": mode,
            "values": { "LLM_ENDPOINT": llm_endpoint },
            "secretsSet": { "LLM_API_KEY": secret_set }
        }
    })
}

async fn container_config_get(
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    match id.as_str() {
        "mem1" => Ok(Json(memory_config("default", &json!({})))),
        "web1" => Ok(Json(json!({ "configurable": false }))),
        _ => Err(StatusCode::NOT_FOUND),
    }
}

async fn container_config_put(
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    if id != "mem1" {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mode = body["mode"].as_str().unwrap_or("");
    if mode.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let values = &body["values"];
    // If a secret was sent, it must equal the test value — proving it crossed the
    // wire — but it is never echoed back in the response.
    if let Some(secret) = values.get("LLM_API_KEY").and_then(Value::as_str)
        && secret != SECRET_VALUE
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Json(memory_config(mode, values)))
}

async fn dbs_list(
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    // The real control plane returns each database with its engine `type` and
    // `state` (typed databases: postgres | mysql | clickhouse).
    Ok(Json(json!({ "dbs": [
        { "name": "analytics", "type": "postgres", "state": "ready", "compute": "running", "created_at": "2026-08-01" },
        { "name": "events", "type": "clickhouse", "state": "creating", "compute": "paused", "created_at": "2026-08-02" }
    ] })))
}

/// The one-time password the mock returns from `POST /v1/dbs`. The CLI must print
/// it (that is what create is for) and warn it is shown once — but this is the
/// only place it may appear.
const DB_ONE_TIME_PASSWORD: &str = "one-time-secret-pw-9f3a";

async fn dbs_create(
    headers: HeaderMap,
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    let name = body["name"].as_str().unwrap_or("db");
    // Echo the requested engine back (default postgres) so a test can assert the
    // `--type` flag crossed the wire; the port varies by engine.
    let db_type = body["type"].as_str().unwrap_or("postgres");
    let port = match db_type {
        "mysql" => 3306,
        "clickhouse" => 8123,
        _ => 5432,
    };
    Ok(Json(json!({
        "name": name,
        "type": db_type,
        "state": "creating",
        "compute": "running",
        "connection": {
            "host": "db.example.com",
            "port": port,
            "dbname": name,
            "user": name,
            "password": DB_ONE_TIME_PASSWORD
        }
    })))
}

async fn db_delete(
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({ "deleted": name })))
}

async fn volumes_list(
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({
        "volumes": [
            {
                "name": "data",
                "device_id": "blk-t1-data",
                "size_bytes": 10_737_418_240u64,
                "state": "available",
                "attached_deployment_id": null,
                "created_at": "2026-08-01"
            },
            {
                "name": "scratch",
                "device_id": "blk-t1-scratch",
                "size_bytes": 21_474_836_480u64,
                "state": "available",
                "attached_deployment_id": "dep-9",
                "created_at": "2026-08-01"
            }
        ]
    })))
}

/// Echoes back the created volume with the size the CLI computed from `--size`, so
/// a test can assert the human size was parsed into the right byte count on the wire.
async fn volumes_create(
    headers: HeaderMap,
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    authorize(&headers, &state)?;
    let name = body["name"].as_str().unwrap_or("vol");
    let size = body["size_bytes"].as_u64().ok_or(StatusCode::BAD_REQUEST)?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "name": name,
            "device_id": format!("blk-t1-{name}"),
            "size_bytes": size,
            "state": "available",
            "attached_deployment_id": null,
            "created_at": "2026-08-01"
        })),
    ))
}

async fn volume_get(
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({
        "name": name,
        "device_id": format!("blk-t1-{name}"),
        "size_bytes": 10_737_418_240u64,
        "state": "available",
        "attached_deployment_id": null,
        "created_at": "2026-08-01"
    })))
}

async fn volume_resize(
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    let size = body["size_bytes"].as_u64().ok_or(StatusCode::BAD_REQUEST)?;
    Ok(Json(json!({
        "name": name,
        "device_id": format!("blk-t1-{name}"),
        "size_bytes": size,
        "state": "available",
        "attached_deployment_id": null,
        "created_at": "2026-08-01"
    })))
}

async fn volume_delete(
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({ "name": name, "state": "deleted" })))
}

/// The exact secret value the set tests pipe/pass. The mock accepts a POST only
/// when the transmitted value equals this, so a passing test proves the value
/// crossed the wire — while the test itself asserts the value is never printed.
const SECRET_VALUE: &str = "s3cr3t-value-xyz";

async fn secrets_list(
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    // Names only — the control plane never returns a stored value.
    Ok(Json(json!({ "secrets": ["EXAMPLE_API_KEY", "OTHER_KEY"] })))
}

async fn secrets_set(
    headers: HeaderMap,
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    authorize(&headers, &state)?;
    let name = body["name"].as_str().unwrap_or("");
    let value = body["value"].as_str().unwrap_or("");
    // Enforce the contract: a name and a non-empty value are required, and the
    // value must be exactly what the test provided (proving transmission).
    if name.is_empty() || value != SECRET_VALUE {
        return Err(StatusCode::BAD_REQUEST);
    }
    // 201 with the name and set flag only — never echoing the value.
    Ok((
        StatusCode::CREATED,
        Json(json!({ "name": name, "set": true })),
    ))
}

async fn secret_delete(
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({ "name": name, "removed": true })))
}

async fn table_snapshot(
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    State(state): State<MockState>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers, &state)?;
    Ok(Json(json!({
        "table": name,
        "snapshot_id": "snap-123",
        "watermark": "2026-08-01T00:00:00Z",
        "row_count": 42
    })))
}

/// Boots a mock control plane with the given router builder on an ephemeral port
/// and returns its base URL.
fn spawn_with(router: fn(MockState) -> Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async move {
            let app = router(MockState {
                good_key: GOOD_KEY.to_owned(),
            });
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, app).await.expect("serve");
        });
    });
    format!("http://{addr}")
}

/// The FULL mock: every cloud route implemented.
fn full_router(state: MockState) -> Router {
    Router::new()
        .route(
            "/v1/deployments",
            get(deployments_list).post(deployments_create),
        )
        .route(
            "/v1/deployments/{id}",
            get(deployment_get)
                .patch(deployment_patch)
                .delete(deployment_delete),
        )
        .route("/v1/deployments/{id}/run", post(deployment_run))
        .route(
            "/v1/containers",
            get(containers_list).post(containers_create),
        )
        .route("/v1/containers/catalog", get(catalog_list))
        .route("/v1/containers/catalog/{id}/deploy", post(catalog_deploy))
        .route(
            "/v1/containers/{id}/config",
            get(container_config_get).put(container_config_put),
        )
        .route("/v1/containers/{id}/scale", post(container_scale))
        .route("/v1/dbs", get(dbs_list).post(dbs_create))
        .route("/v1/dbs/{name}", delete(db_delete))
        .route("/v1/volumes", get(volumes_list).post(volumes_create))
        .route(
            "/v1/volumes/{name}",
            get(volume_get).patch(volume_resize).delete(volume_delete),
        )
        .route("/v1/tables/{name}/snapshot", get(table_snapshot))
        .route("/v1/secrets", get(secrets_list).post(secrets_set))
        .route("/v1/secrets/{name}", delete(secret_delete))
        // Present so patch/delete route matching compiles for containers too.
        .route(
            "/v1/containers/{id}",
            patch(|| async { StatusCode::NOT_IMPLEMENTED }),
        )
        .with_state(state)
}

/// The DEPLOYMENTS-ONLY mock: only the deployments list exists; every other route
/// falls through to a 404, standing in for a control plane that has not shipped
/// the containers/db API yet.
fn deployments_only_router(state: MockState) -> Router {
    Router::new()
        .route("/v1/deployments", get(deployments_list))
        .fallback(|| async { (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))) })
        .with_state(state)
}

/// Writes a stored login (token file 0600 + config URL) under `home`, so the
/// cloud verbs run as "logged in" without executing the OAuth flow.
fn write_login(home: &Path, url: &str) {
    let creds = home.join(".verglas/credentials");
    std::fs::create_dir_all(&creds).expect("mkdir creds");
    std::fs::write(creds.join("control-plane-token"), GOOD_KEY).expect("write token");
    std::fs::write(
        home.join(".verglas/config.toml"),
        format!("[control_plane]\nurl = \"{url}\"\n"),
    )
    .expect("write config");
}

/// Runs the CLI with the given args under a logged-in HOME, returning
/// (success, stdout, stderr).
fn run(home: &Path, args: &[&str]) -> (bool, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(args)
        .env("HOME", home)
        .output()
        .expect("binary runs");
    (
        out.status.success(),
        String::from_utf8(out.stdout).expect("utf8"),
        String::from_utf8(out.stderr).expect("utf8"),
    )
}

/// Runs the CLI with the given args under a logged-in HOME, feeding `input` on
/// stdin. Used by the `secrets set` stdin path (the plain `run` helper leaves
/// stdin null).
fn run_with_stdin(home: &Path, args: &[&str], input: &str) -> (bool, String, String) {
    use std::io::Write as _;
    let mut child = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(args)
        .env("HOME", home)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("binary spawns");
    child
        .stdin
        .take()
        .expect("stdin pipe")
        .write_all(input.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("binary runs");
    (
        out.status.success(),
        String::from_utf8(out.stdout).expect("utf8"),
        String::from_utf8(out.stderr).expect("utf8"),
    )
}

#[test]
fn workers_list_renders_a_table_and_json() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["workers", "list"]);
    assert!(ok, "workers list must succeed: {stderr}");
    assert!(
        stdout.contains("NAME") && stdout.contains("KIND"),
        "header: {stdout}"
    );
    assert!(
        stdout.contains("orders") && stdout.contains("rollup"),
        "rows: {stdout}"
    );

    let (ok, stdout, _) = run(home.path(), &["--json", "workers", "list"]);
    assert!(ok);
    let value: Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(value["deployments"].as_array().expect("array").len(), 2);
}

#[test]
fn workers_get_resolves_a_name_to_its_detail() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    // By name (not an id): the CLI resolves it through the list, then GETs by id.
    let (ok, stdout, stderr) = run(home.path(), &["workers", "get", "orders"]);
    assert!(ok, "workers get by name must succeed: {stderr}");
    assert!(
        stdout.contains("orders") && stdout.contains("code"),
        "detail: {stdout}"
    );
}

#[test]
fn workers_create_sends_the_spec_with_overrides() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let spec = home.path().join("worker.json");
    std::fs::write(
        &spec,
        r#"{"kind":"source","name":"from_spec","trigger":"cron","placement":"cloud","code":"x"}"#,
    )
    .expect("write spec");

    let (ok, stdout, stderr) = run(
        home.path(),
        &[
            "--json",
            "workers",
            "create",
            "--file",
            spec.to_str().expect("utf8 path"),
            "--name",
            "overridden",
            "--schedule",
            "0 9 * * *",
        ],
    );
    assert!(ok, "workers create must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json");
    // The overrides won over the spec fields, and reached the server body.
    assert_eq!(value["name"], "overridden");
    assert_eq!(value["schedule"], "0 9 * * *");
    assert_eq!(value["id"], "d-new");
}

#[test]
fn workers_update_requires_something_to_change() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, _stdout, stderr) = run(home.path(), &["workers", "update", "orders"]);
    assert!(!ok, "an empty update must fail");
    assert!(
        stderr.contains("nothing to update"),
        "the error must explain the empty update: {stderr}"
    );

    // A --status override alone is a valid patch.
    let (ok, stdout, stderr) = run(
        home.path(),
        &[
            "--json", "workers", "update", "orders", "--status", "paused",
        ],
    );
    assert!(ok, "a status update must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(value["status"], "paused");
}

#[test]
fn workers_run_and_delete() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["--json", "workers", "run", "d1"]);
    assert!(ok, "run must succeed: {stderr}");
    assert!(stdout.contains("run-1"), "run id shown: {stdout}");

    let (ok, stdout, stderr) = run(home.path(), &["workers", "delete", "orders"]);
    assert!(ok, "delete must succeed: {stderr}");
    assert!(
        stdout.contains("deleted worker orders"),
        "delete note: {stdout}"
    );
}

#[test]
fn workers_logs_errors_honestly() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, _stdout, stderr) = run(home.path(), &["workers", "logs", "orders"]);
    assert!(!ok, "logs must fail (no route)");
    assert!(
        stderr.contains("no per-worker logs route"),
        "the error must say logs are unavailable: {stderr}"
    );
}

#[test]
fn containers_scale_posts_the_instance_count() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(
        home.path(),
        &["--json", "containers", "scale", "c1", "--instances", "3"],
    );
    assert!(ok, "scale must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(value["instances"], 3);
}

#[test]
fn containers_list_renders() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["containers", "list"]);
    assert!(ok, "containers list must succeed: {stderr}");
    assert!(
        stdout.contains("postgres:16") && stdout.contains("pg"),
        "rows: {stdout}"
    );
}

#[test]
fn db_create_prints_the_one_time_credentials_with_a_warning() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["db", "create", "analytics"]);
    assert!(ok, "db create must succeed: {stderr}");
    assert!(
        stdout.contains(DB_ONE_TIME_PASSWORD),
        "the one-time password must be printed by create: {stdout}"
    );
    assert!(
        stdout.contains("shown once"),
        "create must warn the password is shown once: {stdout}"
    );

    // In --json the raw JSON is on stdout and the warning is on stderr, so stdout
    // stays parseable.
    let (ok, stdout, stderr) = run(home.path(), &["--json", "db", "create", "analytics"]);
    assert!(ok);
    let value: Value = serde_json::from_str(&stdout).expect("stdout is pure json");
    assert_eq!(value["connection"]["password"], DB_ONE_TIME_PASSWORD);
    // No `--type` defaults to postgres.
    assert_eq!(value["type"], "postgres");
    assert!(stderr.contains("shown once"), "warning on stderr: {stderr}");
}

#[test]
fn db_create_passes_the_engine_type_on_the_wire() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    // `--type clickhouse` must reach the control plane; the mock echoes it back and
    // returns the engine's port (8123 for ClickHouse).
    let (ok, stdout, stderr) = run(
        home.path(),
        &["--json", "db", "create", "events", "--type", "clickhouse"],
    );
    assert!(ok, "db create --type clickhouse must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(value["type"], "clickhouse");
    assert_eq!(value["connection"]["port"], 8123);
}

#[test]
fn db_list_renders_the_engine_type() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["db", "list"]);
    assert!(ok, "db list must succeed: {stderr}");
    assert!(
        stdout.contains("analytics") && stdout.contains("events"),
        "both dbs: {stdout}"
    );
    // The engine type is a column, so both engines are visible.
    assert!(
        stdout.contains("postgres"),
        "postgres engine shown: {stdout}"
    );
    assert!(
        stdout.contains("clickhouse"),
        "clickhouse engine shown: {stdout}"
    );
    // Each db is its own deployment, so each row shows ITS OWN compute state.
    assert!(stdout.contains("running"), "per-db compute state: {stdout}");
    assert!(stdout.contains("paused"), "per-db compute state: {stdout}");
}

#[test]
fn db_delete_by_name() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["db", "delete", "analytics"]);
    assert!(ok, "db delete must succeed: {stderr}");
    assert!(
        stdout.contains("deleted database analytics"),
        "delete line: {stdout}"
    );
}

#[test]
fn volumes_list_renders_size_and_attachment() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["volumes", "list"]);
    assert!(ok, "volumes list must succeed: {stderr}");
    // Size rendered human, attachment as yes/no.
    assert!(stdout.contains("10 GiB"), "human size: {stdout}");
    assert!(
        stdout.contains("data") && stdout.contains("scratch"),
        "both volumes listed: {stdout}"
    );
    // The attached one reads yes, the standalone one no.
    let scratch_line = stdout.lines().find(|l| l.contains("scratch")).expect("row");
    assert!(
        scratch_line.contains("yes"),
        "attached volume: {scratch_line}"
    );
    let data_line = stdout.lines().find(|l| l.contains("data")).expect("row");
    assert!(data_line.contains("no"), "standalone volume: {data_line}");
}

#[test]
fn volumes_create_parses_size_to_bytes_on_the_wire() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    // The mock echoes size_bytes back; --size 10GiB must cross the wire as bytes.
    let (ok, stdout, stderr) = run(
        home.path(),
        &["--json", "volumes", "create", "data", "--size", "10GiB"],
    );
    assert!(ok, "volumes create must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(value["size_bytes"], 10u64 * 1024 * 1024 * 1024);
    assert_eq!(value["name"], "data");
}

#[test]
fn volumes_create_rejects_a_bad_size_before_calling_the_server() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, _stdout, stderr) = run(
        home.path(),
        &["volumes", "create", "data", "--size", "notasize"],
    );
    assert!(!ok, "a malformed size must fail");
    assert!(!stderr.contains("panicked"), "not a panic: {stderr}");
}

#[test]
fn volumes_resize_sends_the_new_size() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(
        home.path(),
        &["--json", "volumes", "resize", "data", "--size", "20GiB"],
    );
    assert!(ok, "volumes resize must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(value["size_bytes"], 20u64 * 1024 * 1024 * 1024);
}

#[test]
fn volumes_get_and_delete() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["volumes", "get", "data"]);
    assert!(ok, "volumes get must succeed: {stderr}");
    assert!(stdout.contains("blk-t1-data"), "device id shown: {stdout}");

    let (ok, stdout, stderr) = run(home.path(), &["volumes", "delete", "data"]);
    assert!(ok, "volumes delete must succeed: {stderr}");
    assert!(
        stdout.contains("deleted volume data"),
        "delete line: {stdout}"
    );
}

#[test]
fn containers_unsupported_route_fails_clearly() {
    // Against a control plane that has not shipped the containers API, the CLI
    // must report "does not support containers yet", not a bare 404 or a panic.
    let url = spawn_with(deployments_only_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, _stdout, stderr) = run(home.path(), &["containers", "list"]);
    assert!(
        !ok,
        "containers list must fail on a server without the route"
    );
    assert!(
        stderr.contains("does not support containers yet"),
        "the error must be the unsupported message: {stderr}"
    );
    assert!(!stderr.contains("panicked"), "not a panic: {stderr}");
}

#[test]
fn db_unsupported_route_fails_clearly() {
    let url = spawn_with(deployments_only_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, _stdout, stderr) = run(home.path(), &["db", "list"]);
    assert!(!ok);
    assert!(
        stderr.contains("does not support databases yet"),
        "the error must be the unsupported message: {stderr}"
    );
}

#[test]
fn secrets_list_shows_names_only() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["secrets", "list"]);
    assert!(ok, "secrets list must succeed: {stderr}");
    assert!(stdout.contains("NAME"), "header: {stdout}");
    assert!(
        stdout.contains("EXAMPLE_API_KEY") && stdout.contains("OTHER_KEY"),
        "names: {stdout}"
    );

    let (ok, stdout, _) = run(home.path(), &["--json", "secrets", "list"]);
    assert!(ok);
    let value: Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(value["secrets"].as_array().expect("array").len(), 2);
}

#[test]
fn secrets_set_reads_stdin_without_echoing_the_value() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    // The value arrives on stdin (never as an argument, so it stays out of shell
    // history). The mock accepts the POST only when the value crossed the wire
    // intact, so success proves transmission.
    let (ok, stdout, stderr) = run_with_stdin(
        home.path(),
        &["secrets", "set", "EXAMPLE_API_KEY"],
        SECRET_VALUE,
    );
    assert!(ok, "secrets set from stdin must succeed: {stderr}");
    assert!(
        stdout.contains("set secret EXAMPLE_API_KEY"),
        "set note names the secret: {stdout}"
    );
    // The value must never appear in the CLI's output.
    assert!(
        !stdout.contains(SECRET_VALUE) && !stderr.contains(SECRET_VALUE),
        "the secret value must never be printed: stdout={stdout} stderr={stderr}"
    );
}

#[test]
fn secrets_set_from_value_flag() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(
        home.path(),
        &["secrets", "set", "EXAMPLE_API_KEY", "--value", SECRET_VALUE],
    );
    assert!(ok, "secrets set --value must succeed: {stderr}");
    assert!(
        stdout.contains("set secret EXAMPLE_API_KEY"),
        "set note: {stdout}"
    );
    assert!(
        !stdout.contains(SECRET_VALUE) && !stderr.contains(SECRET_VALUE),
        "the secret value must never be printed: stdout={stdout} stderr={stderr}"
    );
}

#[test]
fn secrets_set_from_file() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let secret_file = home.path().join("secret.txt");
    // A trailing newline is stripped, so a plain `echo > file` value works.
    std::fs::write(&secret_file, format!("{SECRET_VALUE}\n")).expect("write secret file");

    let (ok, stdout, stderr) = run(
        home.path(),
        &[
            "secrets",
            "set",
            "EXAMPLE_API_KEY",
            "--file",
            secret_file.to_str().expect("utf8 path"),
        ],
    );
    assert!(ok, "secrets set --file must succeed: {stderr}");
    assert!(
        stdout.contains("set secret EXAMPLE_API_KEY"),
        "set note: {stdout}"
    );
    assert!(
        !stdout.contains(SECRET_VALUE) && !stderr.contains(SECRET_VALUE),
        "the secret value must never be printed: stdout={stdout} stderr={stderr}"
    );
}

#[test]
fn secrets_set_refuses_an_empty_value() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    // Empty stdin: the CLI refuses before it ever calls the control plane.
    let (ok, _stdout, stderr) =
        run_with_stdin(home.path(), &["secrets", "set", "EXAMPLE_API_KEY"], "");
    assert!(!ok, "an empty value must be refused");
    assert!(
        stderr.contains("empty"),
        "the error must explain the empty value: {stderr}"
    );
}

#[test]
fn secrets_delete_removes_by_name() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["secrets", "delete", "EXAMPLE_API_KEY"]);
    assert!(ok, "secrets delete must succeed: {stderr}");
    assert!(
        stdout.contains("deleted secret EXAMPLE_API_KEY"),
        "delete note: {stdout}"
    );
}

#[test]
fn containers_catalog_lists_with_yes_no_columns() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["containers", "catalog"]);
    assert!(ok, "containers catalog must succeed: {stderr}");
    assert!(
        stdout.contains("ID")
            && stdout.contains("UI")
            && stdout.contains("MCP")
            && stdout.contains("DEFAULT")
    );
    assert!(
        stdout.contains("memory") && stdout.contains("netdata"),
        "rows: {stdout}"
    );
    // The booleans render as yes/no, not true/false.
    assert!(
        stdout.contains("yes") && stdout.contains("no"),
        "yes/no: {stdout}"
    );
    assert!(
        !stdout.contains("true") && !stdout.contains("false"),
        "no raw booleans: {stdout}"
    );

    let (ok, stdout, _) = run(home.path(), &["--json", "containers", "catalog"]);
    assert!(ok);
    let value: Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(value["catalog"].as_array().expect("array").len(), 2);
}

#[test]
fn containers_deploy_reports_created_ui_and_idempotent_mcp() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    // A fresh UI app: created, with its UI hostname.
    let (ok, stdout, stderr) = run(home.path(), &["containers", "deploy", "rill"]);
    assert!(ok, "deploy rill must succeed: {stderr}");
    assert!(
        stdout.contains("deployed rill as container c-rill"),
        "created line: {stdout}"
    );
    assert!(
        stdout.contains("https://rill-acme.verglas.dev/"),
        "UI line: {stdout}"
    );

    // An already-deployed headless MCP app: reported as already deployed (exit 0)
    // with its MCP endpoint, no UI line.
    let (ok, stdout, stderr) = run(home.path(), &["containers", "deploy", "memory"]);
    assert!(ok, "deploy memory (idempotent) must still exit 0: {stderr}");
    assert!(
        stdout.contains("already deployed"),
        "idempotent line: {stdout}"
    );
    assert!(
        stdout.contains("MCP: https://memory-acme.verglas.dev/mcp"),
        "MCP line: {stdout}"
    );
    assert!(
        !stdout.contains("UI:"),
        "a headless app prints no UI line: {stdout}"
    );
}

#[test]
fn containers_config_shows_the_schema_and_mode() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    let (ok, stdout, stderr) = run(home.path(), &["--json", "containers", "config", "mem1"]);
    assert!(ok, "config show must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(value["configurable"], true);
    assert_eq!(value["config"]["mode"], "default");
    assert_eq!(value["schema"]["modeKey"], "mode");
}

#[test]
fn containers_config_sets_fields_with_a_stdin_secret_without_echoing_it() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    // The non-secret field is passed inline; the secret is read from stdin via
    // `KEY=-` (keeping it out of shell history). Success proves the secret crossed
    // the wire (the mock requires it), and the value must never be printed.
    let (ok, stdout, stderr) = run_with_stdin(
        home.path(),
        &[
            "--json",
            "containers",
            "config",
            "mem1",
            "--set",
            "LLM_ENDPOINT=https://byo.example.com/v1",
            "--set",
            "LLM_API_KEY=-",
            "--mode",
            "custom",
        ],
        SECRET_VALUE,
    );
    assert!(ok, "config set must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(value["config"]["mode"], "custom");
    assert_eq!(value["config"]["secretsSet"]["LLM_API_KEY"], true);
    assert!(
        !stdout.contains(SECRET_VALUE) && !stderr.contains(SECRET_VALUE),
        "the secret value must never be printed: stdout={stdout} stderr={stderr}"
    );
}

#[test]
fn containers_config_warns_when_a_secret_is_set_inline() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    // Passing the secret inline works but warns and steers to stdin. Our own output
    // never echoes the value (it is only in argv, which is the user's choice).
    let (ok, stdout, stderr) = run(
        home.path(),
        &[
            "containers",
            "config",
            "mem1",
            "--set",
            &format!("LLM_API_KEY={SECRET_VALUE}"),
            "--mode",
            "custom",
        ],
    );
    assert!(ok, "inline secret set must still succeed: {stderr}");
    assert!(
        stderr.contains("shell history"),
        "must warn about shell history: {stderr}"
    );
    assert!(
        !stdout.contains(SECRET_VALUE),
        "the value must not be echoed to stdout: {stdout}"
    );
}

#[test]
fn containers_config_on_a_non_configurable_container_says_so() {
    let url = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &url);

    // Showing config for a non-configurable container reads {configurable:false}.
    let (ok, stdout, stderr) = run(home.path(), &["--json", "containers", "config", "web1"]);
    assert!(ok, "config show must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(value["configurable"], false);

    // Trying to SET config on it fails with a clear message.
    let (ok, _stdout, stderr) = run(
        home.path(),
        &["containers", "config", "web1", "--mode", "custom"],
    );
    assert!(
        !ok,
        "setting config on a non-configurable container must fail"
    );
    assert!(
        stderr.contains("takes no configuration"),
        "clear error: {stderr}"
    );
}

#[test]
fn cloud_verbs_require_a_login() {
    // No stored login: a cloud verb must fail with the "run `verglas login`"
    // pointer, and never contact any endpoint.
    let home = tempfile::tempdir().expect("tempdir");
    let (ok, _stdout, stderr) = run(home.path(), &["workers", "list"]);
    assert!(!ok, "workers list must fail when not logged in");
    assert!(
        stderr.contains("verglas login"),
        "the error must point at login: {stderr}"
    );
}

// --- portable workers: create --local, push, pull, containers push ----------

/// A local worker row the mock DAEMON returns (its JSON string columns match the
/// daemon's `verglas_sys.workers` shape).
fn local_worker_row() -> Value {
    json!({
        "name": "collector",
        "code": "{\"exec\":[\"python3\",\"collect.py\"],\"cwd\":\"/app\"}",
        "triggers": "[{\"type\":\"cron\",\"schedule\":\"*/5 * * * *\"}]",
        "output": "metrics.samples",
        "config": "{\"env\":{\"API_KEY\":\"@secret:MY_KEY\"}}",
        "state": "running",
        "placement": "local",
        "created_by": "cli",
        "created_at": "2026-08-01T00:00:00Z",
        "revision": 1
    })
}

/// A stand-in for the LOCAL daemon: only the worker read route the push path uses.
fn daemon_router(_state: MockState) -> Router {
    async fn worker_get(AxumPath(name): AxumPath<String>) -> Result<Json<Value>, StatusCode> {
        if name == "collector" {
            Ok(Json(local_worker_row()))
        } else {
            Err(StatusCode::NOT_FOUND)
        }
    }
    Router::new()
        .route("/v1/workers/{name}", get(worker_get))
        .with_state(MockState {
            good_key: GOOD_KEY.to_owned(),
        })
}

#[test]
fn workers_push_translates_a_local_worker_to_a_cloud_deployment() {
    let cp = spawn_with(full_router);
    let daemon = spawn_with(daemon_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &cp);

    let (ok, stdout, stderr) = run(
        home.path(),
        &[
            "--daemon-endpoint",
            &daemon,
            "--json",
            "workers",
            "push",
            "collector",
        ],
    );
    assert!(ok, "workers push must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json body");
    // The pushed body is a worker-kind deployment carrying the exec array in config.
    assert_eq!(value["kind"], "worker");
    assert_eq!(value["name"], "collector");
    assert_eq!(value["config"]["exec"][0], "python3");
    assert_eq!(value["trigger"], "cron");
    // The referenced secret the cloud lacks is reported on stderr (never a value).
    assert!(
        stderr.contains("MY_KEY"),
        "the missing secret is reported: {stderr}"
    );
    assert!(!stderr.contains("@secret"), "no secret value is printed");
}

#[test]
fn workers_pull_writes_a_portable_spec() {
    let cp = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &cp);

    // Pull by name; the CLI resolves it through the list, then GETs the detail.
    let (ok, stdout, stderr) = run(home.path(), &["--json", "workers", "pull", "orders"]);
    assert!(ok, "workers pull must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json spec");
    assert_eq!(value["name"], "orders");
    assert_eq!(value["spec_version"], 1);
}

#[test]
fn workers_follow_needs_a_command_or_file() {
    let home = tempfile::tempdir().expect("tempdir");
    // No `-- <command>` and no `--file`: a clear error, no daemon contacted.
    let (ok, _stdout, stderr) = run(home.path(), &["workers", "follow"]);
    assert!(!ok, "follow with no target must fail");
    assert!(
        stderr.contains("command") || stderr.contains("--file"),
        "the error names the missing target: {stderr}"
    );
}

#[test]
fn containers_push_registers_a_byo_image() {
    let cp = spawn_with(full_router);
    let home = tempfile::tempdir().expect("tempdir");
    write_login(home.path(), &cp);

    let (ok, stdout, stderr) = run(
        home.path(),
        &[
            "--json",
            "containers",
            "push",
            "docker://ghcr.io/acme/app:1.2",
        ],
    );
    assert!(ok, "containers push must succeed: {stderr}");
    let value: Value = serde_json::from_str(&stdout).expect("json body");
    assert_eq!(value["image_ref"], "docker://ghcr.io/acme/app:1.2");
    assert_eq!(value["name"], "app");
    assert_eq!(value["tag"], "1.2");
}
