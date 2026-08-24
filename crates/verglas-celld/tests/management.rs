//! Cloudflare-shaped management API acceptance tests for scripts, namespaces, and objects.

use std::path::Path;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use verglas_celld::{ChildCommand, HostId, HostSupervisor, ManagementApi};

fn socket_child() -> ChildCommand {
    ChildCommand::new("python3").arg("-c").arg(
        "import socket,sys,time; p=sys.argv[sys.argv.index('--socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep(30)",
    )
}

fn upload_request(script_name: &str, metadata: &str) -> Request<Body> {
    let boundary = "celld-test-boundary";
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
        socket_child(),
    )));
    ManagementApi::new(root, supervisor).router()
}

#[tokio::test]
async fn upload_list_fetch_and_delete_return_cf_envelopes() {
    let root = tempfile::tempdir().expect("root");
    let app = api(root.path());
    let metadata = r#"{"main_module":"worker.js","bindings":[{"name":"OBJECTS","type":"durable_object_namespace","class_name":"Counter"}]}"#;

    let uploaded = app
        .clone()
        .oneshot(upload_request("example", metadata))
        .await
        .expect("upload response");
    assert_eq!(uploaded.status(), StatusCode::OK);
    let uploaded_json = body_json(uploaded).await;
    assert_eq!(uploaded_json["success"], true);
    assert_eq!(uploaded_json["result"]["name"], "example");

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
    let listed_json = body_json(listed).await;
    assert_eq!(listed_json["success"], true);
    assert_eq!(listed_json["result"][0]["name"], "example");

    let fetched = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/workers/scripts/example")
                .body(Body::empty())
                .expect("fetch request"),
        )
        .await
        .expect("fetch response");
    assert_eq!(
        body_json(fetched).await["result"]["main_module"],
        "worker.js"
    );

    let restarted = api(root.path());
    let restarted_list = restarted
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/workers/scripts")
                .body(Body::empty())
                .expect("restarted list request"),
        )
        .await
        .expect("restarted list response");
    assert_eq!(
        body_json(restarted_list).await["result"][0]["name"],
        "example"
    );

    let deleted = app
        .clone()
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
    assert!(
        !root
            .path()
            .join("workers/scripts/example/worker.js")
            .exists()
    );
}

#[tokio::test]
async fn namespace_creation_and_object_route_use_supervised_worker() {
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
    assert_eq!(namespace.status(), StatusCode::CREATED);
    let namespace_json = body_json(namespace).await;
    let namespace_id = namespace_json["result"]["id"]
        .as_str()
        .expect("namespace id");

    let object = app
        .clone()
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
    let object_status = object.status();
    let object_json = body_json(object).await;
    assert_eq!(object_status, StatusCode::CREATED, "{object_json}");
    assert_eq!(object_json["success"], true);
    assert_eq!(object_json["result"]["name"], "counter");
    assert!(object_json["result"]["socket_path"].as_str().is_some());
    let object_id = object_json["result"]["id"].as_str().expect("object id");

    let same_object = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/workers/durable_objects/namespaces/{namespace_id}/objects/counter"
                ))
                .body(Body::empty())
                .expect("same object request"),
        )
        .await
        .expect("same object response");
    assert_eq!(same_object.status(), StatusCode::OK);
    assert_eq!(body_json(same_object).await["result"]["id"], object_id);

    let route = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/workers/durable_objects/namespaces/{namespace_id}/objects/counter/route"
                ))
                .body(Body::empty())
                .expect("route request"),
        )
        .await
        .expect("route response");
    assert_eq!(route.status(), StatusCode::OK);

    let objects = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/workers/durable_objects/namespaces/{namespace_id}/objects"
                ))
                .body(Body::empty())
                .expect("objects request"),
        )
        .await
        .expect("objects response");
    let objects_json = body_json(objects).await;
    assert_eq!(objects_json["result"][0]["name"], "counter");

    let suspended = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/workers/durable_objects/namespaces/{namespace_id}/objects/counter/suspend"
                ))
                .body(Body::empty())
                .expect("suspend request"),
        )
        .await
        .expect("suspend response");
    assert_eq!(suspended.status(), StatusCode::OK);
    assert_eq!(body_json(suspended).await["result"]["status"], "suspended");
}

#[tokio::test]
async fn unknown_script_duplicate_namespace_and_bad_metadata_are_errors() {
    let root = tempfile::tempdir().expect("root");
    let app = api(root.path());
    let bad_metadata = r#"{"main_module":"worker.js","bindings":[{"name":"OBJECTS","type":"durable_object_namespace"}]}"#;
    let bad_upload = app
        .clone()
        .oneshot(upload_request("bad", bad_metadata))
        .await
        .expect("bad response");
    assert_eq!(bad_upload.status(), StatusCode::BAD_REQUEST);
    assert_eq!(body_json(bad_upload).await["success"], false);

    let valid_metadata = r#"{"main_module":"worker.js","bindings":[{"name":"OBJECTS","type":"durable_object_namespace","class_name":"Counter"}]}"#;
    app.clone()
        .oneshot(upload_request("valid", valid_metadata))
        .await
        .expect("valid upload response");
    let namespace_body = Body::from(r#"{"name":"objects","script":"valid","class":"Counter"}"#);
    let created = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/durable_objects/namespaces")
                .header("content-type", "application/json")
                .body(namespace_body)
                .expect("namespace request"),
        )
        .await
        .expect("namespace response");
    assert_eq!(created.status(), StatusCode::CREATED);
    let duplicate = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/durable_objects/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"objects","script":"valid","class":"Counter"}"#,
                ))
                .expect("duplicate request"),
        )
        .await
        .expect("duplicate response");
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);

    let unknown = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/durable_objects/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"objects","script":"missing","class":"Counter"}"#,
                ))
                .expect("unknown request"),
        )
        .await
        .expect("unknown response");
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

    let malformed = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/durable_objects/namespaces")
                .header("content-type", "application/json")
                .body(Body::from("not json"))
                .expect("malformed request"),
        )
        .await
        .expect("malformed response");
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
}
