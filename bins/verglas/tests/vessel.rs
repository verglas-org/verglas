//! End-to-end CLI contract for local Vessel declaration and HTTP calls.

use std::net::TcpListener;
use std::process::Command;

use axum::Router;
use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::{delete, get, post, put};

/// Starts a mock runtime manager that checks bearer auth and Vessel wire shapes.
fn spawn_mock() -> String {
    let app = Router::new()
        .route(
            "/v1/vessels/linear",
            put(|headers: HeaderMap, body: Bytes| async move {
                assert_eq!(headers[header::AUTHORIZATION], "Bearer runtime-secret");
                let body: serde_json::Value = serde_json::from_slice(&body).expect("vessel json");
                assert_eq!(body["name"], "linear");
                assert_eq!(body["role"], "integration");
                assert_eq!(body["http"]["port"], 8371);
                assert!(body.get("token").is_none());
                axum::Json(serde_json::json!({"outcome":"created","vessel":body}))
            })
            .get(|headers: HeaderMap| async move {
                assert_eq!(headers[header::AUTHORIZATION], "Bearer runtime-secret");
                axum::Json(serde_json::json!({
                    "name":"linear",
                    "role":"integration",
                    "image":"verglas/integration-linear:local",
                    "state":"running",
                    "health":"ready"
                }))
            })
            .delete(|headers: HeaderMap| async move {
                assert_eq!(headers[header::AUTHORIZATION], "Bearer runtime-secret");
                StatusCode::NO_CONTENT
            }),
        )
        .route(
            "/v1/vessels",
            get(|headers: HeaderMap| async move {
                assert_eq!(headers[header::AUTHORIZATION], "Bearer runtime-secret");
                axum::Json(serde_json::json!([{
                    "name":"linear",
                    "role":"integration",
                    "image":"verglas/integration-linear:local",
                    "state":"running",
                    "health":"ready"
                }]))
            }),
        )
        .route(
            "/v1/vessels/linear/http/v1/query",
            post(|headers: HeaderMap, body: Bytes| async move {
                assert_eq!(headers[header::AUTHORIZATION], "Bearer runtime-secret");
                let body: serde_json::Value = serde_json::from_slice(&body).expect("query json");
                assert_eq!(body, serde_json::json!({"operation":"viewer"}));
                axum::Json(serde_json::json!({"id":"viewer-1","name":"Test User"}))
            }),
        )
        .route(
            "/v1/vessels/linear/http/v1/viewer",
            get(|headers: HeaderMap| async move {
                assert_eq!(headers[header::AUTHORIZATION], "Bearer runtime-secret");
                axum::Json(serde_json::json!({"id":"viewer-1","name":"Test User"}))
            }),
        )
        .route("/unused", delete(|| async { StatusCode::NO_CONTENT }));
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock");
    let address = listener.local_addr().expect("mock address");
    listener.set_nonblocking(true).expect("nonblocking");
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, app).await.expect("serve mock");
        });
    });
    format!("http://{address}")
}

/// Runs one CLI invocation against the mock local runtime manager.
fn run(endpoint: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_verglas"))
        .env("VERGLAS_CONTAINER_RUNTIME_URL", endpoint)
        .env("VERGLAS_CONTAINER_RUNTIME_TOKEN", "runtime-secret")
        .args(args)
        .output()
        .expect("run verglas")
}

#[test]
fn add_list_get_remove_and_call_a_local_vessel() {
    let endpoint = spawn_mock();
    let directory = tempfile::tempdir().expect("tempdir");
    let manifest = directory.path().join("linear.yaml");
    std::fs::write(
        &manifest,
        r#"name: linear
role: integration
image: verglas/integration-linear:local
http:
  port: 8371
  healthPath: /health
"#,
    )
    .expect("write manifest");

    let add = run(
        &endpoint,
        &[
            "--json",
            "vessel",
            "add",
            "--file",
            manifest.to_str().expect("path"),
        ],
    );
    assert!(
        add.status.success(),
        "{}",
        String::from_utf8_lossy(&add.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&add.stdout).expect("add output"),
        serde_json::json!({
            "outcome":"created",
            "vessel":{
                "name":"linear",
                "role":"integration",
                "image":"verglas/integration-linear:local",
                "http":{"port":8371,"healthPath":"/health"}
            }
        })
    );

    for args in [
        vec!["--json", "vessel", "list"],
        vec!["--json", "vessel", "get", "linear"],
    ] {
        let output = run(&endpoint, &args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("linear"));
    }

    let query = run(
        &endpoint,
        &["vessel", "query", "linear", r#"{"operation":"viewer"}"#],
    );
    assert!(
        query.status.success(),
        "{}",
        String::from_utf8_lossy(&query.stderr)
    );
    assert!(String::from_utf8_lossy(&query.stdout).contains("Test User"));

    let curl = run(&endpoint, &["vessel", "curl", "linear", "/v1/viewer"]);
    assert!(
        curl.status.success(),
        "{}",
        String::from_utf8_lossy(&curl.stderr)
    );
    assert!(String::from_utf8_lossy(&curl.stdout).contains("Test User"));

    let remove = run(&endpoint, &["vessel", "remove", "linear"]);
    assert!(
        remove.status.success(),
        "{}",
        String::from_utf8_lossy(&remove.stderr)
    );
}
