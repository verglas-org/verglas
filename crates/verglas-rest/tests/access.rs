//! Authenticated access administration and explainable policy checks over HTTP.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_authz::{
    AccessTokenService, AccessTokenSigner, Action, Authorizer, Grant, MemoryAccessTokenRegistry,
    MemoryAuthorizer, Principal, PrincipalKind, Resource, ResourceKind, TokenMintRequest,
};
use verglas_rest::access::AccessHttpRuntime;

/// Creates one access session backed by an owner or unprivileged human principal.
async fn app(owner: bool) -> (axum::Router, String, Arc<MemoryAuthorizer>) {
    let authorizer = Arc::new(MemoryAuthorizer::new());
    authorizer
        .create_resource(Resource::new("tenant-a", "tenant", ResourceKind::Tenant))
        .await
        .expect("tenant");
    authorizer
        .create_principal(Principal::new("tenant-a", "user-1", PrincipalKind::User))
        .await
        .expect("user");
    if owner {
        authorizer
            .create_grant(Grant::new(
                "owner",
                "tenant-a",
                "user-1",
                "tenant",
                BTreeSet::from([Action::Own]),
            ))
            .await
            .expect("owner grant");
    }
    authorizer
        .create_principal(
            Principal::new("tenant-a", "session-1", PrincipalKind::Agent).with_parent("user-1"),
        )
        .await
        .expect("session");
    let tokens = Arc::new(AccessTokenService::new(
        AccessTokenSigner::new([3; 32]),
        Arc::new(MemoryAccessTokenRegistry::new()),
    ));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let minted = tokens
        .mint(
            TokenMintRequest::new(
                "session-1",
                "tenant-a",
                "user-1",
                "session-1",
                "Test session",
                "access",
                authorizer
                    .policy_version("tenant-a")
                    .await
                    .expect("version"),
                now,
                now + 3600,
            )
            .with_run("identity-session"),
        )
        .await
        .expect("token");
    let token = minted.token.expose().to_owned();
    let runtime = AccessHttpRuntime::new(authorizer.clone(), tokens, "tenant-a");
    (verglas_rest::access::router(runtime), token, authorizer)
}

/// Adds the authenticated access-session bearer to one request builder.
fn authorized(builder: axum::http::request::Builder, token: &str) -> axum::http::request::Builder {
    builder.header(header::AUTHORIZATION, format!("Bearer {token}"))
}

#[tokio::test]
async fn access_routes_administer_and_evaluate_policy() {
    let (app, token, _) = app(true).await;
    for (path, body) in [
        ("/v1/access/principals", json!({"id":"job-1","kind":"job"})),
        (
            "/v1/access/resources",
            json!({"id":"db-1","kind":"database","parent_id":"tenant"}),
        ),
        (
            "/v1/access/resources",
            json!({"id":"table-1","kind":"table","parent_id":"db-1"}),
        ),
        (
            "/v1/access/grants",
            json!({"id":"grant-1","principal_id":"job-1","resource_id":"db-1","actions":["query"]}),
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                authorized(Request::post(path), &token)
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
            authorized(Request::post("/v1/access/authorize"), &token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"audience":"access","resource_id":"table-1","action":"query"})
                        .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 4096).await.expect("body");
    let answer: Value = serde_json::from_slice(&body).expect("answer");
    assert_eq!(answer["decision"]["allowed"], true);
    assert_eq!(answer["decision"]["reason"], "inherited_grant");

    let response = app
        .oneshot(
            authorized(Request::get("/v1/access/grants"), &token)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn delegation_route_rejects_privilege_escalation() {
    let (app, token, authorizer) = app(false).await;
    authorizer
        .create_principal(Principal::new("tenant-a", "job-1", PrincipalKind::Job))
        .await
        .expect("job");
    authorizer
        .create_resource(Resource::new("tenant-a", "table-1", ResourceKind::Table))
        .await
        .expect("table");

    let response = app
        .oneshot(
            authorized(Request::post("/v1/access/delegations"), &token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "grant":{"id":"grant-1","principal_id":"job-1","resource_id":"table-1","actions":["query"]}
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn resource_inventory_does_not_reveal_ungranted_names() {
    let (app, token, authorizer) = app(false).await;
    authorizer
        .create_resource(Resource::new(
            "tenant-a",
            "database/secret",
            ResourceKind::Database,
        ))
        .await
        .expect("secret database");

    let list = app
        .clone()
        .oneshot(
            authorized(Request::get("/v1/access/resources"), &token)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(list.status(), StatusCode::OK);
    let body = to_bytes(list.into_body(), 4096).await.expect("body");
    let resources: Value = serde_json::from_slice(&body).expect("resources");
    assert_eq!(resources, json!([]));

    let guessed = app
        .oneshot(
            authorized(
                Request::get("/v1/access/resources/database%2Fsecret"),
                &token,
            )
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(guessed.status(), StatusCode::FORBIDDEN);
}
