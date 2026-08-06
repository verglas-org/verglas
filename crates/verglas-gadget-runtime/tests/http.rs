//! HTTP contract tests for the Gadget runtime control and content surfaces.

use std::collections::BTreeMap;
use std::time::Duration;

use axum::Json;
use axum::http::HeaderMap;
use axum::routing::post;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use verglas_gadget_runtime::{DataPlaneConfig, HostConfig, RuntimeConfig, RuntimeService};

const TOKEN: &str = "test-runtime-token";

/// Starts one runtime router on an ephemeral loopback port.
async fn serve(config: RuntimeConfig) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind runtime");
    let address = listener.local_addr().expect("runtime address");
    let app = RuntimeService::new(config, TOKEN.to_owned())
        .expect("runtime service")
        .router();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("runtime serves");
    });
    format!("http://{address}")
}

/// Builds one registration request body.
fn bundle(version: &str, message: &str) -> Value {
    json!({
        "version": version,
        "serverModule": format!(
            "export class Gadget {{ message() {{ return {message:?}; }} }}"
        ),
        "clientModule": "export default {};",
        "files": BTreeMap::<String, String>::new(),
    })
}

fn capability_token(gadget_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"verglas-gadget-data-capability\0");
    digest.update(gadget_id.as_bytes());
    digest.update(b"\0");
    digest.update(TOKEN.as_bytes());
    hex::encode(digest.finalize())
}

#[tokio::test]
async fn health_is_public_but_control_routes_require_the_runtime_token() {
    let base = serve(RuntimeConfig::local(4)).await;
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .expect("health")
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("{base}/v1/gadgets"))
            .send()
            .await
            .expect("list")
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn local_api_registers_lists_serves_and_deletes_multiple_gadgets() {
    let base = serve(RuntimeConfig::local(4)).await;
    let client = reqwest::Client::new();

    for id in ["alpha", "beta"] {
        let response = client
            .put(format!("{base}/v1/gadgets/{id}"))
            .bearer_auth(TOKEN)
            .json(&bundle("1", id))
            .send()
            .await
            .expect("register");
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let gadgets: Value = client
        .get(format!("{base}/v1/gadgets"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("list")
        .json()
        .await
        .expect("list JSON");
    assert_eq!(gadgets.as_array().map(Vec::len), Some(2));

    let client_module = client
        .get(format!("{base}/v1/gadgets/alpha/client.js"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("client module");
    assert_eq!(client_module.status(), StatusCode::OK);
    assert_eq!(
        client_module.text().await.expect("client body"),
        "export default {};"
    );

    assert_eq!(
        client
            .delete(format!("{base}/v1/gadgets/alpha"))
            .bearer_auth(TOKEN)
            .send()
            .await
            .expect("delete")
            .status(),
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn cloud_api_rejects_every_identity_except_its_target() {
    let base = serve(RuntimeConfig::single("only-gadget")).await;
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .put(format!("{base}/v1/gadgets/other-gadget"))
            .bearer_auth(TOKEN)
            .json(&bundle("1", "other"))
            .send()
            .await
            .expect("rejected register")
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .put(format!("{base}/v1/gadgets/only-gadget"))
            .bearer_auth(TOKEN)
            .json(&bundle("1", "only"))
            .send()
            .await
            .expect("target register")
            .status(),
        StatusCode::CREATED
    );
}

#[tokio::test]
async fn gadget_data_capability_proxies_sdk_routes_without_disclosing_upstream_token() {
    let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let upstream_address = upstream_listener.local_addr().expect("upstream address");
    let upstream = axum::Router::new().route(
        "/v1/query",
        post(|headers: HeaderMap, Json(body): Json<Value>| async move {
            Json(json!({
                "authorization": headers.get("authorization").and_then(|v| v.to_str().ok()),
                "sql": body["sql"],
            }))
        }),
    );
    tokio::spawn(async move {
        axum::serve(upstream_listener, upstream)
            .await
            .expect("upstream serves");
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind runtime");
    let address = listener.local_addr().expect("runtime address");
    let base = format!("http://{address}");
    let host = HostConfig {
        command: "/bin/false".into(),
        arguments: Vec::new(),
        startup_timeout: Duration::from_secs(1),
        environment: BTreeMap::new(),
    };
    let app = RuntimeService::with_host(
        RuntimeConfig::local(4),
        TOKEN.to_owned(),
        host,
        DataPlaneConfig {
            endpoint: format!("http://{upstream_address}"),
            token: "upstream-secret".to_owned(),
            capability_base_url: base.clone(),
        },
    )
    .expect("runtime service")
    .router();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("runtime serves");
    });

    let client = reqwest::Client::new();
    assert_eq!(
        client
            .put(format!("{base}/v1/gadgets/alpha"))
            .bearer_auth(TOKEN)
            .json(&bundle("1", "alpha"))
            .send()
            .await
            .expect("register")
            .status(),
        StatusCode::CREATED
    );
    let response: Value = client
        .post(format!("{base}/v1/gadgets/alpha/data/v1/query"))
        .bearer_auth(capability_token("alpha"))
        .json(&json!({"sql": "SELECT 42"}))
        .send()
        .await
        .expect("proxy query")
        .json()
        .await
        .expect("proxy JSON");
    assert_eq!(response["authorization"], "Bearer upstream-secret");
    assert_eq!(response["sql"], "SELECT 42");

    assert_eq!(
        client
            .post(format!("{base}/v1/gadgets/alpha/data/v1/query"))
            .bearer_auth(TOKEN)
            .json(&json!({"sql": "SELECT 42"}))
            .send()
            .await
            .expect("reject runtime token")
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(format!(
                "{base}/v1/gadgets/alpha/data/v1/workers/anything/run"
            ))
            .bearer_auth(capability_token("alpha"))
            .send()
            .await
            .expect("reject control route")
            .status(),
        StatusCode::FORBIDDEN
    );
}
