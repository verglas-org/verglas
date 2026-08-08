//! Namespace gateway contract tests against a private runtime manager.

use axum::body::{Body, to_bytes};
use axum::extract::Path;
use axum::http::{Request, StatusCode, header};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn list(headers: axum::http::HeaderMap) -> (StatusCode, Json<Value>) {
    assert_eq!(
        headers
            .get(header::AUTHORIZATION)
            .expect("gateway authorization"),
        "Bearer manager"
    );
    (StatusCode::OK, Json(json!([manifest()])))
}

async fn show(Path(namespace): Path<String>) -> Json<Value> {
    assert_eq!(namespace, "crm");
    Json(manifest())
}

async fn invoke(Path((namespace, method)): Path<(String, String)>, body: String) -> Json<Value> {
    assert_eq!(namespace, "crm");
    assert_eq!(method, "contacts.get");
    assert_eq!(body, r#"{"id":"c-1"}"#);
    Json(json!({"id":"c-1","name":"Ada"}))
}

fn manifest() -> Value {
    json!({
        "namespace": "crm",
        "title": "CRM",
        "description": "Customer records.",
        "methods": {
            "contacts.get": {
                "description": "Gets a contact.",
                "mode": "read",
                "input": {"type":"object"},
                "output": {"type":"object"}
            }
        }
    })
}

#[tokio::test]
async fn server_gateway_relays_reflection_and_invocation_to_the_runtime() {
    let upstream = Router::new()
        .route("/v1/namespaces", get(list))
        .route("/v1/namespaces/{namespace}", get(show))
        .route("/v1/namespaces/{namespace}/invoke/{method}", post(invoke));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let address = listener.local_addr().expect("upstream address");
    let server = tokio::spawn(async move {
        axum::serve(listener, upstream)
            .await
            .expect("serve upstream");
    });
    let gateway =
        verglas_rest::namespace::NamespaceGateway::new(format!("http://{address}"), "manager")
            .expect("gateway configuration");
    let app = verglas_rest::namespace::router(gateway);

    let reflected = app
        .clone()
        .oneshot(
            Request::get("/v1/namespaces")
                .body(Body::empty())
                .expect("reflection request"),
        )
        .await
        .unwrap();
    assert_eq!(reflected.status(), StatusCode::OK);

    let invoked = app
        .oneshot(
            Request::post("/v1/namespaces/crm/invoke/contacts.get")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"id":"c-1"}"#))
                .expect("invocation request"),
        )
        .await
        .unwrap();
    assert_eq!(invoked.status(), StatusCode::OK);
    let body = to_bytes(invoked.into_body(), 1024)
        .await
        .expect("invocation body");
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("invocation json")["name"],
        "Ada"
    );
    server.abort();
}
