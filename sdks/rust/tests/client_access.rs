//! Typed SDK access administration and policy-check calls.

use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use verglas_sdk::{
    AccessCheck, AccessDecision, Action, Client, ConnectOptions, DecisionReason, Principal,
    PrincipalKind, Resource, ResourceKind,
};

#[test]
fn access_dtos_match_the_hosted_wire_contract() {
    let principal = Principal {
        tenant_id: "tenant-a".to_owned(),
        id: "agent-1".to_owned(),
        kind: PrincipalKind::Agent,
        parent_id: None,
    };
    let resource = Resource {
        tenant_id: "tenant-a".to_owned(),
        id: "table-1".to_owned(),
        kind: ResourceKind::GenericTable,
        parent_id: Some("database-1".to_owned()),
    };
    let decision = AccessDecision {
        allowed: true,
        reason: DecisionReason::InheritedGrant,
        grant_id: Some("grant-1".to_owned()),
        matched_resource_id: Some("database-1".to_owned()),
        policy_version: 3,
    };

    assert_eq!(
        serde_json::to_value(principal).expect("serialize principal"),
        json!({"tenant_id":"tenant-a","id":"agent-1","kind":"agent"})
    );
    assert_eq!(
        serde_json::to_value(resource).expect("serialize resource"),
        json!({
            "tenant_id":"tenant-a",
            "id":"table-1",
            "kind":"generic_table",
            "parent_id":"database-1"
        })
    );
    assert_eq!(
        serde_json::to_value(decision).expect("serialize decision"),
        json!({
            "allowed":true,
            "reason":"inherited_grant",
            "grant_id":"grant-1",
            "matched_resource_id":"database-1",
            "policy_version":3
        })
    );
}

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
