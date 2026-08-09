//! Typed SDK access administration and policy-check calls.

use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use verglas_authz::{AccessCheck, Action};
use verglas_sdk::{Client, ConnectOptions};

#[tokio::test]
async fn client_checks_access_with_its_scoped_bearer_token() {
    async fn check(headers: axum::http::HeaderMap, Json(body): Json<Value>) -> Json<Value> {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(body["principal_id"], "job-1");
        Json(json!({
            "allowed": true,
            "reason": "exact_grant",
            "grant_id": "grant-1",
            "matched_resource_id": "table-1",
            "policy_version": 3
        }))
    }
    let app = Router::new().route("/v1/access/check", post(check));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let client = Client::connect(
        ConnectOptions::new(&endpoint)
            .with_query_uri(&endpoint)
            .with_access_uri(&endpoint)
            .with_catalog_uri("http://127.0.0.1:1")
            .with_s3_endpoint("http://127.0.0.1:8333")
            .with_token("scoped"),
    )
    .await
    .expect("connect");

    let decision = client
        .check_access(&AccessCheck::new(
            "tenant-a",
            "job-1",
            "table-1",
            Action::Query,
        ))
        .await
        .expect("check access");
    assert!(decision.allowed);
    assert_eq!(decision.grant_id.as_deref(), Some("grant-1"));
}
