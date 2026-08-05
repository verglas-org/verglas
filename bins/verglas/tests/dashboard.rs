//! Dashboard CLI contract against the optional on-prem Rill integration (#16).

use std::net::TcpListener;
use std::process::Command;

use axum::extract::Path;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

/// Serves the dashboard API shape expected from `verglas-rest`.
fn spawn_mock() -> String {
    let app = Router::new()
        .route(
            "/v1/dashboards",
            post(|Json(body): Json<Value>| async move {
                assert_eq!(body["table"], "sales.orders");
                Json(json!({
                    "name": "sales_orders",
                    "table": "sales.orders",
                    "url": "http://127.0.0.1:9009/explore/sales_orders",
                }))
            })
            .get(|| async {
                Json(json!({"dashboards": [{
                    "name": "sales_orders",
                    "table": "sales.orders",
                    "url": "http://127.0.0.1:9009/explore/sales_orders",
                }]}))
            }),
        )
        .route(
            "/v1/dashboards/{name}",
            get(|Path(name): Path<String>| async move {
                assert_eq!(name, "sales_orders");
                Json(json!({
                    "name": name,
                    "table": "sales.orders",
                    "url": "http://127.0.0.1:9009/explore/sales_orders",
                }))
            })
            .delete(|Path(name): Path<String>| async move {
                assert_eq!(name, "sales_orders");
                Json(json!({"deleted": name}))
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock");
    let addr = listener.local_addr().expect("mock address");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("mock runtime");
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, app).await.expect("serve mock");
        });
    });
    format!("http://{addr}")
}

/// Runs the real CLI against the mock endpoint.
fn run(endpoint: &str, args: &[&str]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("--server-endpoint")
        .arg(endpoint)
        .args(args)
        .output()
        .expect("run CLI");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Create, list, show, and delete use only the composed on-prem REST API.
#[test]
fn dashboard_verbs_speak_the_server_api() {
    let endpoint = spawn_mock();

    let (ok, stdout, stderr) = run(
        &endpoint,
        &["--json", "dashboard", "create", "sales.orders"],
    );
    assert!(ok, "dashboard create failed: {stderr}");
    let created: Value = serde_json::from_str(&stdout).expect("create JSON");
    assert_eq!(created["name"], "sales_orders");

    let (ok, stdout, stderr) = run(&endpoint, &["--json", "dashboard", "list"]);
    assert!(ok, "dashboard list failed: {stderr}");
    let listed: Value = serde_json::from_str(&stdout).expect("list JSON");
    assert_eq!(listed["dashboards"][0]["table"], "sales.orders");

    let (ok, stdout, stderr) = run(&endpoint, &["--json", "dashboard", "show", "sales_orders"]);
    assert!(ok, "dashboard show failed: {stderr}");
    let shown: Value = serde_json::from_str(&stdout).expect("show JSON");
    assert_eq!(shown["url"], "http://127.0.0.1:9009/explore/sales_orders");

    let (ok, stdout, stderr) = run(
        &endpoint,
        &["--json", "dashboard", "delete", "sales_orders"],
    );
    assert!(ok, "dashboard delete failed: {stderr}");
    let deleted: Value = serde_json::from_str(&stdout).expect("delete JSON");
    assert_eq!(deleted["deleted"], "sales_orders");
}
