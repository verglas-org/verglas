//! Authenticated access administration and scoped token lifecycle over HTTP.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use tower::ServiceExt;
use verglas_authz::{
    AccessTokenService, AccessTokenSigner, Action, Authorizer, Grant, MemoryAccessTokenRegistry,
    MemoryAuthorizer, Principal, PrincipalKind, Resource, ResourceKind, TargetJwtSigner,
    TokenMintRequest,
};
use verglas_rest::access::{AccessHttpRuntime, CLI_AUDIENCE, DATA_PLANE_AUDIENCE};
use verglas_rest::data_plane::{AuthorizationQuestion, DataPlaneAuthorizer};

/// Creates an authenticated access router and an owner session credential.
async fn authenticated_runtime() -> (AccessHttpRuntime, String) {
    let authorizer = Arc::new(MemoryAuthorizer::new());
    authorizer
        .create_resource(Resource::new("tenant-a", "tenant", ResourceKind::Tenant))
        .await
        .expect("tenant");
    authorizer
        .create_principal(Principal::new(
            "tenant-a",
            "user/alice@example.com",
            PrincipalKind::User,
        ))
        .await
        .expect("owner");
    authorizer
        .create_grant(Grant::new(
            "owner",
            "tenant-a",
            "user/alice@example.com",
            "tenant",
            BTreeSet::from([Action::Own]),
        ))
        .await
        .expect("owner grant");
    authorizer
        .create_resource(
            Resource::new("tenant-a", "database/analytics", ResourceKind::Database)
                .with_parent("tenant"),
        )
        .await
        .expect("database");
    authorizer
        .create_principal(
            Principal::new("tenant-a", "session/test", PrincipalKind::Agent)
                .with_parent("user/alice@example.com"),
        )
        .await
        .expect("session principal");

    let tokens = AccessTokenService::new(
        AccessTokenSigner::new([7; 32]),
        Arc::new(MemoryAccessTokenRegistry::new()),
    );
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let session = tokens
        .mint(
            TokenMintRequest::new(
                "session-test",
                "tenant-a",
                "user/alice@example.com",
                "session/test",
                "Test session",
                "access",
                authorizer
                    .policy_version("tenant-a")
                    .await
                    .expect("policy version"),
                now,
                now + 3_600,
            )
            .with_run("identity-session"),
        )
        .await
        .expect("session");
    let token = session.token.expose().to_owned();
    let runtime = AccessHttpRuntime::new(authorizer, Arc::new(tokens), "tenant-a")
        .with_identity_assertion_key([9_u8; 32])
        .with_target_jwt_signer(TargetJwtSigner::new("test-key", [8; 32]).expect("target signer"));
    (runtime, token)
}

/// Creates an authenticated access router and an owner session credential.
async fn authenticated_app() -> (axum::Router, String) {
    let (runtime, token) = authenticated_runtime().await;
    (verglas_rest::access::router(runtime), token)
}

/// Signs a compact OS identity assertion for one requested session exchange.
fn identity_assertion(now: u64, jti: &str, subject: &str) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let claims = URL_SAFE_NO_PAD.encode(
        json!({
            "sub":subject,
            "tenant_id":"tenant-a",
            "aud":"verglas-access",
            "iat":now,
            "exp":now + 60,
            "jti":jti
        })
        .to_string(),
    );
    let signed = format!("{header}.{claims}");
    let mut mac = Hmac::<Sha256>::new_from_slice(&[9_u8; 32]).expect("HMAC key");
    mac.update(signed.as_bytes());
    format!(
        "{signed}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}

#[tokio::test]
async fn identity_session_supports_an_allowlisted_data_plane_audience() {
    let (app, _) = authenticated_app().await;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/access/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "assertion":identity_assertion(now, "assertion-data-plane", "user/alice@example.com"),
                        "audience":"data-plane"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), 8192).await.expect("body");
    let session: Value = serde_json::from_slice(&body).expect("json");
    assert!(
        session["token"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );

    let authorized = app
        .oneshot(
            Request::post("/v1/access/authorize")
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", session["token"].as_str().expect("token")),
                )
                .body(Body::from(
                    json!({
                        "audience":"data-plane",
                        "resource_id":"database/analytics",
                        "action":"query"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(authorized.status(), StatusCode::OK);
    let body = to_bytes(authorized.into_body(), 4096).await.expect("body");
    let value: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(value["identity"]["principal_id"], "user/alice@example.com");
    assert_eq!(value["decision"]["allowed"], true);
}

#[tokio::test]
async fn signed_session_creates_a_new_default_deny_user() {
    let (app, _) = authenticated_app().await;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/access/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "assertion":identity_assertion(now, "assertion-bob", "user/bob@example.com"),
                        "audience":"data-plane"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), 8192).await.expect("body");
    let session: Value = serde_json::from_slice(&body).expect("json");

    let denied = app
        .oneshot(
            Request::post("/v1/access/authorize")
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", session["token"].as_str().expect("token")),
                )
                .body(Body::from(
                    json!({
                        "audience":"data-plane",
                        "resource_id":"database/analytics",
                        "action":"query"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(denied.status(), StatusCode::OK);
    let body = to_bytes(denied.into_body(), 4096).await.expect("body");
    let value: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(value["identity"]["principal_id"], "user/bob@example.com");
    assert_eq!(value["decision"]["allowed"], false);
}

#[tokio::test]
async fn database_token_requires_connect_and_is_bound_to_the_database() {
    let (app, owner_token) = authenticated_app().await;
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/access/database-tokens")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {owner_token}"))
                .body(Body::from(
                    json!({"database_id":"analytics","expires_in_seconds":60}).to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), 8192).await.expect("body");
    let value: Value = serde_json::from_slice(&body).expect("json");
    let token = value["token"].as_str().expect("target token");
    let claims = TargetJwtSigner::new("test-key", [8; 32])
        .expect("target signer")
        .verify(
            token,
            "verglas-neon",
            "analytics",
            value["expires_at"].as_u64().expect("expiry") - 1,
        )
        .expect("target claims");
    assert_eq!(claims.subject, "user/alice@example.com");
    assert_eq!(claims.database_id, "analytics");

    let invalid = app
        .clone()
        .oneshot(
            Request::post("/v1/access/database-tokens")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {owner_token}"))
                .body(Body::from(
                    json!({"database_id":"analytics/escape"}).to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    let jwks = app
        .oneshot(
            Request::get("/.well-known/jwks.json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(jwks.status(), StatusCode::OK);
}

#[tokio::test]
async fn authorize_derives_identity_and_rejects_missing_bearer() {
    let (app, token) = authenticated_app().await;
    let body = json!({
        "audience":"access",
        "resource_id":"database/analytics",
        "action":"query"
    });

    let missing = app
        .clone()
        .oneshot(
            Request::post("/v1/access/authorize")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .oneshot(
            Request::post("/v1/access/authorize")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 4096).await.expect("body");
    let value: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(value["identity"]["tenant_id"], "tenant-a");
    assert_eq!(value["identity"]["principal_id"], "user/alice@example.com");
    assert_eq!(value["identity"]["audience"], "access");
    assert_eq!(value["decision"]["allowed"], true);
}

#[tokio::test]
async fn token_creation_delegates_only_the_authenticated_users_access() {
    let (runtime, owner_token) = authenticated_runtime().await;
    let app = verglas_rest::access::router(runtime.clone());
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/access/tokens")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {owner_token}"))
                .body(Body::from(
                    json!({
                        "name":"Local CLI",
                        "audience":"verglas-cli",
                        "expires_in_seconds":3600,
                        "grants":[{
                            "resource_id":"database/analytics",
                            "actions":["query"]
                        }]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), 8192).await.expect("body");
    let created: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(created["name"], "Local CLI");
    assert_eq!(created["parent_principal_id"], "user/alice@example.com");
    assert!(
        created["principal_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("token/"))
    );
    assert!(
        created["token"]
            .as_str()
            .is_some_and(|token| !token.is_empty())
    );

    let cli_token = created["token"].as_str().expect("CLI token");
    let cli_inventory = app
        .clone()
        .oneshot(
            Request::get("/v1/access/tokens")
                .header(header::AUTHORIZATION, format!("Bearer {cli_token}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(cli_inventory.status(), StatusCode::OK);

    let cli_query = app
        .clone()
        .oneshot(
            Request::post("/v1/access/authorize")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {cli_token}"))
                .body(Body::from(
                    json!({
                        "audience":"data-plane",
                        "resource_id":"database/analytics",
                        "action":"query"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(cli_query.status(), StatusCode::OK);
    let cli_query = to_bytes(cli_query.into_body(), 4096).await.expect("body");
    let cli_query: Value = serde_json::from_slice(&cli_query).expect("json");
    assert_eq!(cli_query["identity"]["audience"], "verglas-cli");
    assert_eq!(cli_query["decision"]["allowed"], true);

    let local_data_plane_identity = runtime
        .authorize(
            &format!("Bearer {cli_token}"),
            AuthorizationQuestion {
                audience: DATA_PLANE_AUDIENCE.into(),
                resource_id: "database/analytics".to_owned(),
                action: Action::Query,
            },
        )
        .await;
    let Ok(local_data_plane_identity) = local_data_plane_identity else {
        panic!("CLI bearer must cross the colocated data-plane boundary");
    };
    assert_eq!(local_data_plane_identity.audience, CLI_AUDIENCE);

    let list = app
        .oneshot(
            Request::get("/v1/access/tokens")
                .header(header::AUTHORIZATION, format!("Bearer {owner_token}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(list.status(), StatusCode::OK);
    let body = to_bytes(list.into_body(), 8192).await.expect("body");
    let tokens: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(tokens.as_array().expect("tokens").len(), 2);
    assert!(
        tokens
            .as_array()
            .expect("tokens")
            .iter()
            .all(|entry| entry.get("token").is_none())
    );
}

#[tokio::test]
async fn delegation_ignores_actor_spoofing_by_omitting_actor_from_the_wire_contract() {
    let (app, owner_token) = authenticated_app().await;
    let response = app
        .oneshot(
            Request::post("/v1/access/delegations")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {owner_token}"))
                .body(Body::from(
                    json!({
                        "actor_principal_id":"attacker",
                        "grant":{
                            "id":"grant-query",
                            "principal_id":"job/reader",
                            "resource_id":"database/analytics",
                            "actions":["query"]
                        }
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}
