//! End-to-end CLI contract for the built-in KV set/get surface (#29).

use std::net::TcpListener;
use std::process::Command;

use axum::Router;
use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::put;

/// Starts a mock KV endpoint that checks auth, TTL, namespace, key, and bytes.
fn spawn_mock() -> String {
    let app = Router::new().route(
        "/v1/kv/workshop.blueprints/featured",
        put(|headers: HeaderMap, body: Bytes| async move {
            assert_eq!(headers[header::AUTHORIZATION], "Bearer secret");
            assert_eq!(headers["x-verglas-ttl-seconds"], "300");
            assert_eq!(body, Bytes::from_static(b"blue"));
            let mut response = axum::Json(serde_json::json!({"version": "\"7\""})).into_response();
            response
                .headers_mut()
                .insert(header::ETAG, HeaderValue::from_static("\"7\""));
            response
        })
        .get(|headers: HeaderMap| async move {
            assert_eq!(headers[header::AUTHORIZATION], "Bearer secret");
            (StatusCode::OK, Bytes::from_static(b"blue"))
        }),
    );
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

/// `kv set --ttl` and `kv get` are direct, configuration-free CLI operations.
#[test]
fn set_and_get_raw_value_with_ttl() {
    let endpoint = spawn_mock();
    let set = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args([
            "--server-endpoint",
            &endpoint,
            "--token",
            "secret",
            "kv",
            "set",
            "workshop.blueprints",
            "featured",
            "blue",
            "--ttl",
            "300",
        ])
        .output()
        .expect("run set");
    assert!(
        set.status.success(),
        "{}",
        String::from_utf8_lossy(&set.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&set.stdout), "\"7\"\n");

    let get = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args([
            "--server-endpoint",
            &endpoint,
            "--token",
            "secret",
            "kv",
            "get",
            "workshop.blueprints",
            "featured",
        ])
        .output()
        .expect("run get");
    assert!(
        get.status.success(),
        "{}",
        String::from_utf8_lossy(&get.stderr)
    );
    assert_eq!(get.stdout, b"blue\n");
}
