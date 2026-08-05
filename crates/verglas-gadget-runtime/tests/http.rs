//! HTTP contract tests for the Gadget runtime control and content surfaces.

use std::collections::BTreeMap;

use reqwest::StatusCode;
use serde_json::{Value, json};
use verglas_gadget_runtime::{RuntimeConfig, RuntimeService};

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
