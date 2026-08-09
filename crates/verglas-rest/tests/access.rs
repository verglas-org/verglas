//! Access administration and explainable policy checks over HTTP.

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_authz::MemoryAuthorizer;

#[tokio::test]
async fn access_routes_administer_and_evaluate_policy() {
    let app = verglas_rest::access::router(Arc::new(MemoryAuthorizer::new()));

    for (path, body) in [
        (
            "/v1/access/principals",
            json!({"tenant_id":"tenant-a","id":"job-1","kind":"job"}),
        ),
        (
            "/v1/access/resources",
            json!({"tenant_id":"tenant-a","id":"db-1","kind":"database"}),
        ),
        (
            "/v1/access/resources",
            json!({"tenant_id":"tenant-a","id":"table-1","kind":"table","parent_id":"db-1"}),
        ),
        (
            "/v1/access/grants",
            json!({"id":"grant-1","tenant_id":"tenant-a","principal_id":"job-1","resource_id":"db-1","actions":["query"]}),
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::post(path)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/access/check")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"tenant_id":"tenant-a","principal_id":"job-1","resource_id":"table-1","action":"query"}).to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 4096).await.expect("body");
    let decision: Value = serde_json::from_slice(&body).expect("decision");
    assert_eq!(decision["allowed"], true);
    assert_eq!(decision["reason"], "inherited_grant");

    let response = app
        .oneshot(
            Request::get("/v1/access/grants?tenant_id=tenant-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}
