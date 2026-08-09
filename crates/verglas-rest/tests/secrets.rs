//! Access-service HTTP lifecycle for encrypted scoped secrets.

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_authz::{
    Authorizer, MemoryAuthorizer, MemorySecretRepository, Principal, PrincipalKind, SecretCipher,
    SecretError, SecretService,
};

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
    let secrets = Arc::new(SecretService::new(
        authorizer.clone(),
        Arc::new(MemorySecretRepository::new()),
        Arc::new(TestCipher),
    ));
    let app = verglas_rest::access::router_with_secrets(
        authorizer.clone(),
        secrets,
        "tenant-a",
        "runtime",
    );

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/secrets")
                .header(header::CONTENT_TYPE, "application/json")
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

    for path in ["/v1/secrets", "/v1/secrets/customer-s3"] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).expect("request"))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.expect("body");
        assert!(!String::from_utf8_lossy(&body).contains("sensitive-value"));
    }

    let resolve = |principal_id: &str| {
        Request::post("/v1/access/secrets/resolve")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({
                    "tenant_id":"tenant-a",
                    "principal_id":principal_id,
                    "kind":"s3",
                    "uri":"s3://customer-bucket/team/object.parquet"
                })
                .to_string(),
            ))
            .expect("request")
    };
    let forbidden = app
        .clone()
        .oneshot(resolve("other-runtime"))
        .await
        .expect("response");
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

    let resolved = app.oneshot(resolve("runtime")).await.expect("response");
    assert_eq!(resolved.status(), StatusCode::OK);
    let body = to_bytes(resolved.into_body(), 4096).await.expect("body");
    let value: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(value["resource_id"], "customer-s3");
    assert_eq!(value["value"], "sensitive-value");
}
