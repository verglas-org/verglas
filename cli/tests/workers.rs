//! Local workers CLI against a mock server `/v0/workers` registry (#66).

use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde_json::{Value, json};

/// Shared mock state: registered workers and the last run idempotency key.
#[derive(Clone, Default)]
struct MockState {
    /// Workers keyed by name (last POST wins).
    workers: Arc<Mutex<Vec<Value>>>,
    /// Last Idempotency-Key seen on `/run`.
    last_run_key: Arc<Mutex<Option<String>>>,
}

/// Boots a mock local workers API on an ephemeral port.
fn spawn_server() -> (String, MockState) {
    let state = MockState::default();
    let app = Router::new()
        .route("/v0/workers", get(list_workers).post(register_worker))
        .route("/v0/workers/{name}", get(get_worker))
        .route("/v0/workers/{name}/state", put(set_state))
        .route("/v0/workers/{name}/run", post(run_worker))
        .with_state(state.clone());
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

/// `GET /v0/workers` — bare array, matching the server.
async fn list_workers(State(state): State<MockState>) -> Json<Value> {
    let workers = state.workers.lock().expect("lock");
    Json(Value::Array(workers.clone()))
}

/// `POST /v0/workers` — echo the body with a running state.
async fn register_worker(
    State(state): State<MockState>,
    Json(mut body): Json<Value>,
) -> Json<Value> {
    body["state"] = json!("running");
    body["revision"] = json!(1);
    let mut workers = state.workers.lock().expect("lock");
    if let Some(name) = body.get("name").and_then(Value::as_str) {
        workers.retain(|w| w.get("name").and_then(Value::as_str) != Some(name));
    }
    workers.push(body.clone());
    Json(body)
}

/// `GET /v0/workers/{name}`.
async fn get_worker(
    State(state): State<MockState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<Value>, StatusCode> {
    let workers = state.workers.lock().expect("lock");
    workers
        .iter()
        .find(|w| w.get("name").and_then(Value::as_str) == Some(name.as_str()))
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// `PUT /v0/workers/{name}/state`.
async fn set_state(
    State(state): State<MockState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let state_name = body
        .get("state")
        .and_then(Value::as_str)
        .ok_or(StatusCode::BAD_REQUEST)?;
    let mut workers = state.workers.lock().expect("lock");
    let row = workers
        .iter_mut()
        .find(|w| w.get("name").and_then(Value::as_str) == Some(name.as_str()))
        .ok_or(StatusCode::NOT_FOUND)?;
    row["state"] = json!(state_name);
    Ok(Json(row.clone()))
}

/// `POST /v0/workers/{name}/run` — requires Idempotency-Key.
async fn run_worker(
    State(state): State<MockState>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, StatusCode> {
    let key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .ok_or(StatusCode::BAD_REQUEST)?;
    {
        let workers = state.workers.lock().expect("lock");
        if !workers
            .iter()
            .any(|w| w.get("name").and_then(Value::as_str) == Some(name.as_str()))
        {
            return Err(StatusCode::NOT_FOUND);
        }
    }
    *state.last_run_key.lock().expect("lock") = Some(key.to_owned());
    Ok(Json(json!({ "job_id": "job-1", "created": true })))
}

/// Runs the CLI with HOME redirected and `VERGLAS_ENDPOINT` pointed at a target.
fn run(home: &Path, endpoint: &str, args: &[&str]) -> (bool, String, String) {
    let mut cmd_args = vec!["--json"];
    cmd_args.extend_from_slice(args);
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(&cmd_args)
        .env("HOME", home)
        .env("VERGLAS_ENDPOINT", endpoint)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("binary runs");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Writes a minimal cron worker TOML spec under `dir`.
fn write_spec(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("worker.toml");
    std::fs::write(
        &path,
        r#"
name = "collector"
runtime = "python"
entrypoint = ["python", "collect.py"]

[files]
"pyproject.toml" = "[project]\nname='collector'\nversion='0.1.0'\n"
"uv.lock" = "version = 1\n"
"collect.py" = "print('ok')\n"

[[triggers]]
type = "cron"
cron = "*/5 * * * *"
"#,
    )
    .expect("write spec");
    path
}

#[test]
fn workers_reject_the_oss_stack() {
    let (endpoint, _state) = spawn_server();
    let home = tempfile::tempdir().expect("home");
    let spec = write_spec(home.path());

    let (ok, _stdout, stderr) = run(
        home.path(),
        &endpoint,
        &["workers", "create", "--file", spec.to_str().expect("utf8")],
    );
    assert!(!ok, "create against OSS must fail");
    assert!(
        stderr.contains("Verglas Cloud") && stderr.contains("OSS"),
        "the error names Cloud vs OSS: {stderr}"
    );

    let (ok, _stdout, stderr) = run(home.path(), &endpoint, &["workers", "list"]);
    assert!(!ok, "list against OSS must fail");
    assert!(
        stderr.contains("Verglas Cloud"),
        "list error names Cloud: {stderr}"
    );
}

#[test]
fn workers_follow_needs_a_command_or_file() {
    let home = tempfile::tempdir().expect("tempdir");
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["workers", "follow"])
        .env("HOME", home.path())
        .output()
        .expect("binary runs");
    assert!(!out.status.success(), "follow must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not supported") && stderr.contains("create --file"),
        "follow is not an OSS runtime: {stderr}"
    );
}

#[test]
fn workers_help_lists_local_verbs_only() {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["workers", "--help"])
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    for verb in ["list", "get", "create", "delete", "run", "follow"] {
        assert!(
            stdout
                .lines()
                .any(|line| line.trim_start().starts_with(verb)),
            "workers help must list {verb}: {stdout}"
        );
    }
}
