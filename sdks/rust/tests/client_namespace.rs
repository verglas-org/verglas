//! Reflection and invocation contract tests for composable Integration namespaces.

use axum::body::Body;
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use verglas_sdk::{Client, ConnectOptions, NamespaceMethodMode};

/// A Rust client discovers, invokes, and streams the same reflected namespace contract as TypeScript.
#[tokio::test]
async fn reflected_namespace_is_composable_from_rust() {
    async fn list(headers: HeaderMap) -> Json<Value> {
        assert_eq!(headers["authorization"], "Bearer scoped");
        Json(json!([manifest()]))
    }
    async fn show(Path(namespace): Path<String>) -> Json<Value> {
        assert_eq!(namespace, "crm");
        Json(manifest())
    }
    async fn invoke(
        Path((namespace, method)): Path<(String, String)>,
        headers: HeaderMap,
        Json(input): Json<Value>,
    ) -> Response {
        assert_eq!(namespace, "crm");
        match method.as_str() {
            "contacts.get" => {
                assert_eq!(input, json!({"id":"c-1"}));
                Json(json!({"id":"c-1", "name":"Ada"})).into_response()
            }
            "contacts.watch" => {
                assert_eq!(headers["accept"], "application/x-ndjson");
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/x-ndjson")
                    .body(Body::from_stream(stream::iter([
                        Ok::<_, std::convert::Infallible>("{\"id\":\"c-1\"}\n"),
                        Ok("{\"id\":\"c-2\"}\n"),
                    ])))
                    .expect("stream response")
            }
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }
    let app = Router::new()
        .route("/v1/namespaces", get(list))
        .route("/v1/namespaces/{namespace}", get(show))
        .route("/v1/namespaces/{namespace}/invoke/{method}", post(invoke));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let client = Client::connect(
        ConnectOptions::new(&endpoint)
            .with_query_uri(&endpoint)
            .with_catalog_uri("http://127.0.0.1:1")
            .with_s3_endpoint("http://127.0.0.1:8333")
            .with_token("scoped"),
    )
    .await
    .expect("connect");

    let listed = client.namespaces().await.expect("list namespaces");
    assert_eq!(listed[0].namespace, "crm");
    assert_eq!(
        listed[0].methods["contacts.watch"].mode,
        NamespaceMethodMode::Stream
    );

    let crm = client.namespace("crm").expect("namespace handle");
    assert_eq!(crm.manifest().await.expect("manifest").title, "Example CRM");
    let contact: Value = crm
        .invoke("contacts.get", &json!({"id":"c-1"}))
        .await
        .expect("invoke");
    assert_eq!(contact, json!({"id":"c-1", "name":"Ada"}));

    let mut contacts = crm
        .stream::<_, Value>("contacts.watch", &json!({"team":"platform"}))
        .await
        .expect("open stream");
    assert_eq!(
        contacts.next().await.expect("first").expect("first row"),
        json!({"id":"c-1"})
    );
    assert_eq!(
        contacts.next().await.expect("second").expect("second row"),
        json!({"id":"c-2"})
    );
    assert!(contacts.next().await.is_none());
}

/// Builds the stable reflection document served by the mock Integration gateway.
fn manifest() -> Value {
    json!({
        "namespace":"crm",
        "title":"Example CRM",
        "description":"A reflected test integration.",
        "methods": {
            "contacts.get": {"description":"Gets one contact.", "mode":"read", "input":{}, "output":{}},
            "contacts.watch": {"description":"Streams contacts.", "mode":"stream", "input":{}, "output":{}}
        }
    })
}
