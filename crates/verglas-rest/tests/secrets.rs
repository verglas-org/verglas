//! Access-service HTTP lifecycle for encrypted scoped secrets.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_authz::{
    AccessTokenService, AccessTokenSigner, Action, Authorizer, Grant, MemoryAccessTokenRegistry,
    MemoryAuthorizer, MemorySecretRepository, Principal, PrincipalKind, SecretCipher, SecretError,
    SecretService, TokenMintRequest,
};
use verglas_rest::access::AccessHttpRuntime;

/// Reversible test cipher that lets the route tests remain service-independent.
struct TestCipher;

impl SecretCipher for TestCipher {
    fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, SecretError> {
        Ok(plaintext.iter().rev().copied().collect())
    }

    fn open(&self, ciphertext: &[u8]) -> Result<Vec<u8>, SecretError> {
        self.seal(ciphertext)
    }
}

#[tokio::test]
async fn secret_routes_hide_values_and_gate_resolution_with_use_secret() {
    let authorizer = Arc::new(MemoryAuthorizer::new());
    authorizer
        .create_principal(Principal::new(
            "tenant-a",
            "runtime",
            PrincipalKind::Application,
        ))
        .await
        .expect("principal");
    authorizer
        .create_principal(Principal::new(
            "tenant-a",
            "other-runtime",
            PrincipalKind::Application,
        ))
        .await
        .expect("other principal");
    for (id, kind) in [
        ("user-1", PrincipalKind::User),
        ("session-1", PrincipalKind::Agent),
        ("service-1", PrincipalKind::ServiceAccount),
        ("service-2", PrincipalKind::ServiceAccount),
    ] {
        let mut principal = Principal::new("tenant-a", id, kind);
        if id == "session-1" {
            principal = principal.with_parent("user-1");
        }
        authorizer
            .create_principal(principal)
            .await
            .expect("support principal");
    }
    let secrets = Arc::new(SecretService::new(
        authorizer.clone(),
        Arc::new(MemorySecretRepository::new()),
        Arc::new(TestCipher),
    ));
    let tokens = Arc::new(AccessTokenService::new(
        AccessTokenSigner::new([4; 32]),
        Arc::new(MemoryAccessTokenRegistry::new()),
    ));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let mint = |id: &str, parent: &str, principal: &str, run: Option<&str>| {
        let mut request = TokenMintRequest::new(
            id,
            "tenant-a",
            parent,
            principal,
            id,
            "access",
            1,
            now,
            now + 3600,
        );
        if let Some(run) = run {
            request = request.with_run(run);
        }
        request
    };
    let owner = tokens
        .mint(mint(
            "session-1",
            "user-1",
            "session-1",
            Some("identity-session"),
        ))
        .await
        .expect("owner token");
    let runtime_token = tokens
        .mint(mint("runtime-token", "service-1", "runtime", None))
        .await
        .expect("runtime token");
    let other_token = tokens
        .mint(mint("other-token", "service-2", "other-runtime", None))
        .await
        .expect("other token");
    let owner_token = owner.token.expose().to_owned();
    let runtime_token = runtime_token.token.expose().to_owned();
    let other_token = other_token.token.expose().to_owned();
    let app = verglas_rest::access::router(
        AccessHttpRuntime::new(authorizer.clone(), tokens, "tenant-a").with_secrets(secrets),
    );

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/secrets")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {owner_token}"))
                .body(Body::from(
                    json!({
                        "name":"customer-s3",
                        "type":"s3",
                        "scope":"s3://customer-bucket/team",
                        "value":"sensitive-value"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), 4096).await.expect("body");
    assert!(!String::from_utf8_lossy(&body).contains("sensitive-value"));

    authorizer
        .create_grant(Grant::new(
            "runtime-secret",
            "tenant-a",
            "runtime",
            "customer-s3",
            std::collections::BTreeSet::from([Action::UseSecret]),
        ))
        .await
        .expect("runtime secret grant");

    for path in ["/v1/secrets", "/v1/secrets/customer-s3"] {
        let response = app
            .clone()
            .oneshot(
                Request::get(path)
                    .header(header::AUTHORIZATION, format!("Bearer {owner_token}"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.expect("body");
        assert!(!String::from_utf8_lossy(&body).contains("sensitive-value"));
    }

    let resolve = |token: &str| {
        Request::post("/v1/access/secrets/resolve")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::from(
                json!({
                    "kind":"s3",
                    "uri":"s3://customer-bucket/team/object.parquet"
                })
                .to_string(),
            ))
            .expect("request")
    };
    let forbidden = app
        .clone()
        .oneshot(resolve(&other_token))
        .await
        .expect("response");
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

    let resolved = app
        .oneshot(resolve(&runtime_token))
        .await
        .expect("response");
    assert_eq!(resolved.status(), StatusCode::OK);
    let body = to_bytes(resolved.into_body(), 4096).await.expect("body");
    let value: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(value["resource_id"], "customer-s3");
    assert_eq!(value["value"], "sensitive-value");
}
