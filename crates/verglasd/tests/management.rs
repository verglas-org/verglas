//! Cloudflare-shaped management metadata acceptance tests.

use std::path::Path;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use verglasd::{ChildCommand, HostId, HostSupervisor, ManagementApi};

fn upload_request(script_name: &str, metadata: &str) -> Request<Body> {
    let boundary = "verglasd-test-boundary";
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"metadata\"\r\nContent-Type: application/json\r\n\r\n{metadata}\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"worker.js\"; filename=\"worker.js\"\r\nContent-Type: application/javascript\r\n\r\nexport default {{ fetch() {{ return new Response(\"ok\"); }} }}\r\n--{boundary}--\r\n"
    );
    Request::builder()
        .method("PUT")
        .uri(format!("/workers/scripts/{script_name}"))
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from(body))
        .expect("upload request")
}

async fn body_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("JSON response")
}

fn api(root: &Path) -> axum::Router {
    let supervisor = Arc::new(tokio::sync::Mutex::new(HostSupervisor::new(
        HostId::new("cell-test"),
        root,
        ChildCommand::new("verglas-runtime"),
    )));
    ManagementApi::new(root, supervisor).router()
}

#[tokio::test]
async fn upload_list_fetch_delete_and_namespace_metadata_work() {
    let root = tempfile::tempdir().expect("root");
    let app = api(root.path());
    let metadata = r#"{"main_module":"worker.js","bindings":[{"name":"OBJECTS","type":"durable_object_namespace","class_name":"Counter"}]}"#;
    let uploaded = app
        .clone()
        .oneshot(upload_request("example", metadata))
        .await
        .expect("upload response");
    assert_eq!(uploaded.status(), StatusCode::OK);
    assert_eq!(body_json(uploaded).await["result"]["name"], "example");

    let namespace = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/durable_objects/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"objects","script":"example","class":"Counter"}"#,
                ))
                .expect("namespace request"),
        )
        .await
        .expect("namespace response");
    assert_eq!(namespace.status(), StatusCode::CREATED);

    let listed = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/workers/scripts")
                .body(Body::empty())
                .expect("list request"),
        )
        .await
        .expect("list response");
    assert_eq!(body_json(listed).await["success"], true);

    let deleted = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/workers/scripts/example")
                .body(Body::empty())
                .expect("delete request"),
        )
        .await
        .expect("delete response");
    assert_eq!(deleted.status(), StatusCode::OK);
}

#[tokio::test]
async fn object_activation_fails_closed_without_component_deployment() {
    let root = tempfile::tempdir().expect("root");
    let app = api(root.path());
    let metadata = r#"{"main_module":"worker.js","bindings":[{"name":"OBJECTS","type":"durable_object_namespace","class_name":"Counter"}]}"#;
    app.clone()
        .oneshot(upload_request("example", metadata))
        .await
        .expect("upload response");
    let namespace = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/durable_objects/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"objects","script":"example","class":"Counter"}"#,
                ))
                .expect("namespace request"),
        )
        .await
        .expect("namespace response");
    let namespace_id = body_json(namespace).await["result"]["id"]
        .as_str()
        .expect("namespace id")
        .to_owned();
    let object = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/workers/durable_objects/namespaces/{namespace_id}/objects/counter"
                ))
                .body(Body::empty())
                .expect("object request"),
        )
        .await
        .expect("object response");
    assert_eq!(object.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        body_json(object).await["errors"][0]["message"]
            .as_str()
            .expect("error")
            .contains("component")
    );
}
