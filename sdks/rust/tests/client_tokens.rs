//! Typed SDK contract tests for access-token lifecycle routes.

use axum::{
    Json, Router,
    extract::Path,
    http::HeaderMap,
    routing::{delete, post},
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use verglas_sdk::server::ServerClient;
use verglas_sdk::{
    AccessTokenCreateRequest, AccessTokenGrant, AccessTokenSummary, Client, ConnectOptions,
    DatabaseConnectionTokenRequest,
};

/// Connects an SDK client to an isolated access-service fixture.
async fn client(endpoint: &str) -> Client {
    Client::connect(
        ConnectOptions::new(endpoint)
            .with_access_uri(endpoint)
            .with_query_uri(endpoint)
            .with_s3_endpoint("http://127.0.0.1:8333")
            .with_token("parent-token"),
    )
    .await
    .expect("connect")
}

/// The lifecycle calls use the scoped parent credential and preserve the wire contract.
#[tokio::test]
async fn client_mints_lists_and_revokes_access_tokens() {
    async fn create(headers: HeaderMap, Json(body): Json<Value>) -> Json<Value> {
        assert_eq!(headers["authorization"], "Bearer parent-token");
        assert_eq!(body["name"], "local-cli");
        assert_eq!(body["audience"], "data-plane");
        assert_eq!(body["expires_in_seconds"], 86_400);
        assert_eq!(
            body["grants"],
            json!([{
                "resource_id": "analytics",
                "actions": ["query", "discover"]
            }])
        );
        Json(issued_metadata())
    }

    async fn list(headers: HeaderMap) -> Json<Value> {
        assert_eq!(headers["authorization"], "Bearer parent-token");
        Json(json!([metadata("token-1", Value::Null)]))
    }

    async fn revoke(headers: HeaderMap, Path(id): Path<String>) -> Json<Value> {
        assert_eq!(headers["authorization"], "Bearer parent-token");
        assert_eq!(id, "token-1");
        Json(metadata("token-1", json!(1_786_252_800)))
    }

    let app = Router::new()
        .route("/v1/access/tokens", post(create).get(list))
        .route("/v1/access/tokens/{id}", delete(revoke));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });

    let request = AccessTokenCreateRequest::new("local-cli", "data-plane")
        .with_expiration_seconds(86_400)
        .with_grant(AccessTokenGrant::new("analytics", ["query", "discover"]));
    let issued = client(&endpoint)
        .await
        .create_access_token(&request)
        .await
        .expect("mint token");
    assert_eq!(issued.token, "plaintext-once");
    assert_eq!(issued.summary.id, "token-1");

    let listed: Vec<AccessTokenSummary> = client(&endpoint)
        .await
        .list_access_tokens()
        .await
        .expect("list tokens");
    assert_eq!(listed.len(), 1);
    assert!(listed[0].revoked_at.is_none());

    let revoked = client(&endpoint)
        .await
        .revoke_access_token("token-1")
        .await
        .expect("revoke token");
    assert_eq!(revoked.revoked_at, Some(1_786_252_800));
}

/// Produces a metadata fixture shared by each token lifecycle route.
fn metadata(id: &str, revoked_at: Value) -> Value {
    json!({
        "id": id,
        "parent_principal_id": "user-a",
        "principal_id": "token-principal-a",
        "name": "local-cli",
        "audience": "data-plane",
        "created_at": 1_786_249_200,
        "expires_at": 1_786_335_600,
        "revoked_at": revoked_at,
        "last_used_at": null
    })
}

/// Adds the plaintext creation result without changing list/revoke metadata.
fn issued_metadata() -> Value {
    let mut value = metadata("token-1", Value::Null);
    value
        .as_object_mut()
        .expect("metadata object")
        .insert("token".to_owned(), json!("plaintext-once"));
    value
}

/// Generic server transport applies the scoped bearer token to every CLI-backed route.
#[tokio::test]
async fn generic_server_client_applies_bearer_token() {
    async fn protected(headers: HeaderMap) -> Json<Value> {
        assert_eq!(headers["authorization"], "Bearer scoped-server-token");
        Json(json!({"ok": true}))
    }
    let app = Router::new().route("/v1/protected", axum::routing::get(protected));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let client =
        ServerClient::new_with_token(&endpoint, Some("scoped-server-token")).expect("client");
    let response: Value = client.get("/v1/protected").await.expect("response");
    assert_eq!(response["ok"], true);
}

/// The SDK exchanges an authorized access credential for one short-lived Neon password token.
#[tokio::test]
async fn client_mints_a_database_scoped_connection_token() {
    async fn create(headers: HeaderMap, Json(body): Json<Value>) -> Json<Value> {
        assert_eq!(headers["authorization"], "Bearer parent-token");
        assert_eq!(
            body,
            json!({ "database_id": "analytics", "expires_in_seconds": 300 })
        );
        Json(json!({ "token": "neon-password-jwt", "expires_at": 1_786_250_100_u64 }))
    }
    let app = Router::new().route("/v1/access/database-tokens", post(create));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });

    let token = client(&endpoint)
        .await
        .create_database_connection_token(
            &DatabaseConnectionTokenRequest::new("analytics").with_expiration_seconds(300),
        )
        .await
        .expect("mint connection token");
    assert_eq!(token.token, "neon-password-jwt");
    assert_eq!(token.expires_at, 1_786_250_100);
}
