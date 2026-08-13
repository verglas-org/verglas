use std::sync::{Arc, Mutex};

use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
use lakekeeper_authz_verglas::{
    DecisionClient, DecisionClientError, ResourceSync, SyncResourceKind, VerglasAction,
};
use serde_json::{Value, json};

type SeenDecisions = Arc<Mutex<Vec<(String, Value)>>>;
type SeenLifecycleCalls = Arc<Mutex<Vec<(String, String, Value)>>>;

#[tokio::test]
async fn forwards_caller_bearer_without_caller_supplied_identity() {
    let seen = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
    let app =
        Router::new()
            .route(
                "/v1/access/authorize",
                post(
                    |State(seen): State<SeenDecisions>,
                     headers: HeaderMap,
                     Json(body): Json<Value>| async move {
                        let authorization = headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_owned();
                        seen.lock().expect("seen lock").push((authorization, body));
                        Json(json!({
                            "identity": {
                                "tenant_id": "tenant-a",
                                "principal_id": "user/alice@example.com",
                                "token_id": "token-1",
                                "audience": "data-plane"
                            },
                            "decision": {"allowed": true, "reason": "exact_grant"}
                        }))
                    },
                ),
            )
            .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind decision service");
    let endpoint = url::Url::parse(&format!(
        "http://{}/",
        listener.local_addr().expect("local address")
    ))
    .expect("endpoint");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });

    let credentials = tempfile::NamedTempFile::new().expect("credential file");
    std::fs::write(credentials.path(), "policy-token\n").expect("policy credential");
    let client = DecisionClient::new(endpoint, credentials.path()).expect("client");
    assert!(
        client
            .authorize(
                "caller-token-one",
                "database/analytics/lakekeeper/table/1",
                VerglasAction::Query,
            )
            .await
            .expect("first decision")
            .allowed
    );
    assert!(
        client
            .authorize(
                "caller-token-two",
                "database/analytics",
                VerglasAction::Describe,
            )
            .await
            .expect("second decision")
            .allowed
    );

    assert_eq!(
        seen.lock().expect("seen lock").as_slice(),
        &[
            (
                "Bearer caller-token-one".to_owned(),
                json!({
                    "audience":"data-plane",
                    "resource_id":"database/analytics/lakekeeper/table/1",
                    "action":"query"
                }),
            ),
            (
                "Bearer caller-token-two".to_owned(),
                json!({
                    "audience":"data-plane",
                    "resource_id":"database/analytics",
                    "action":"describe"
                }),
            ),
        ]
    );
}

#[tokio::test]
async fn caller_authorization_does_not_depend_on_the_policy_credential() {
    let app = Router::new().route(
        "/v1/access/authorize",
        post(|| async {
            Json(json!({
                "identity": {
                    "tenant_id": "tenant-a",
                    "principal_id": "user/alice@example.com",
                    "token_id": "token-1",
                    "audience": "data-plane"
                },
                "decision": {"allowed": true}
            }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind decision service");
    let endpoint = url::Url::parse(&format!(
        "http://{}/",
        listener.local_addr().expect("local address")
    ))
    .expect("endpoint");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let credentials = tempfile::NamedTempFile::new().expect("credential file");
    let client = DecisionClient::new(endpoint, credentials.path()).expect("client");
    let decision = client
        .authorize("caller-token", "database/analytics", VerglasAction::Query)
        .await
        .expect("caller bearer is sufficient");
    assert!(decision.allowed);
}

#[tokio::test]
async fn lifecycle_sync_rejects_an_empty_policy_credential() {
    let credentials = tempfile::NamedTempFile::new().expect("credential file");
    let client = DecisionClient::new(
        url::Url::parse("http://127.0.0.1:9/").expect("endpoint"),
        credentials.path(),
    )
    .expect("client");
    let error = client
        .upsert_resource(&ResourceSync::new(
            "lakekeeper/project/p1",
            SyncResourceKind::Project,
            "lakekeeper",
        ))
        .await
        .expect_err("empty policy credential fails closed");
    assert!(matches!(error, DecisionClientError::EmptyCredential(_)));
}

#[tokio::test]
async fn synchronizes_resources_with_stable_parent_edges_and_encoded_deletes() {
    let seen = Arc::new(Mutex::new(Vec::<(String, String, Value)>::new()));
    let app = Router::new()
        .route(
            "/v1/access/policy/resources",
            post(
                |State(seen): State<SeenLifecycleCalls>,
                 headers: HeaderMap,
                 Json(body): Json<Value>| async move {
                    seen.lock().expect("seen lock").push((
                        "POST".to_owned(),
                        headers["authorization"]
                            .to_str()
                            .expect("bearer")
                            .to_owned(),
                        body,
                    ));
                    Json(json!({"synced": true}))
                },
            ),
        )
        .route(
            "/v1/access/policy/resources/{id}",
            axum::routing::delete(
                |State(seen): State<SeenLifecycleCalls>,
                 headers: HeaderMap,
                 axum::extract::Path(id): axum::extract::Path<String>| async move {
                    seen.lock().expect("seen lock").push((
                        "DELETE".to_owned(),
                        headers["authorization"]
                            .to_str()
                            .expect("bearer")
                            .to_owned(),
                        json!({"id": id}),
                    ));
                    Json(json!({"deleted": true}))
                },
            ),
        )
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind sync service");
    let endpoint = url::Url::parse(&format!(
        "http://{}/",
        listener.local_addr().expect("local address")
    ))
    .expect("endpoint");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let credentials = tempfile::NamedTempFile::new().expect("credential file");
    std::fs::write(credentials.path(), "policy-token").expect("credential");
    let client = DecisionClient::new(endpoint, credentials.path()).expect("client");
    let resource = ResourceSync::new(
        "database/analytics/lakekeeper/project/p1",
        SyncResourceKind::Project,
        "database/analytics",
    );
    client.upsert_resource(&resource).await.expect("upsert");
    client
        .delete_resource("database/analytics/lakekeeper/project/p1")
        .await
        .expect("delete");

    assert_eq!(
        seen.lock().expect("seen lock").as_slice(),
        &[
            (
                "POST".to_owned(),
                "Bearer policy-token".to_owned(),
                json!({
                    "id":"database/analytics/lakekeeper/project/p1",
                    "kind":"project",
                    "parent_id":"database/analytics"
                }),
            ),
            (
                "DELETE".to_owned(),
                "Bearer policy-token".to_owned(),
                json!({"id":"database/analytics/lakekeeper/project/p1"}),
            ),
        ]
    );
}
