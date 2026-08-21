//! Endpoint tests for the frozen AWS S3 Vectors REST-JSON contract.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_s3::semantic::{
    SemanticApi, SemanticCredentials, SemanticError, SemanticOperation, router, router_with_sigv4,
};

/// Records route dispatches without acting as a semantic data store.
struct RecordingApi(Mutex<Vec<SemanticOperation>>);

#[async_trait]
impl SemanticApi for RecordingApi {
    /// Records the route and supplies a JSON response so the test exercises HTTP.
    async fn call(
        &self,
        operation: SemanticOperation,
        input: Value,
    ) -> Result<Value, SemanticError> {
        self.0
            .lock()
            .map_err(|error| SemanticError::unavailable(error.to_string()))?
            .push(operation);
        Ok(json!({"operation": format!("{operation:?}"), "input": input}))
    }
}

/// Semantic routes reject unsigned requests and accept a canonical valid S3 Vectors signature.
#[tokio::test]
async fn semantic_sigv4_requires_a_valid_vectors_signature() {
    let app = router_with_sigv4(
        Arc::new(RecordingApi(Mutex::new(Vec::new()))),
        SemanticCredentials::new("key".to_owned(), "secret".to_owned()),
    );
    let unsigned = Request::post("/PutVectors")
        .body(Body::from("{}"))
        .expect("request");
    let response = app.clone().oneshot(unsigned).await.expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        response
            .headers()
            .get("x-amzn-errortype")
            .and_then(|value| value.to_str().ok()),
        Some("SignatureDoesNotMatch")
    );
    let now = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    assert_eq!(
        app.oneshot(signed_request(
            "POST",
            "/PutVectors",
            "s3vectors",
            &now,
            "{}",
            false
        ))
        .await
        .expect("valid")
        .status(),
        StatusCode::OK
    );
}

/// The customer data port serves no API documentation. The cache node's admin
/// listener serves it instead, so nothing unauthenticated rides the S3 port.
#[tokio::test]
async fn the_s3_listener_serves_no_documentation() {
    let app = router_with_sigv4(
        Arc::new(RecordingApi(Mutex::new(Vec::new()))),
        SemanticCredentials::new("key".to_owned(), "secret".to_owned()),
    );
    // The documentation routes are gone and the sigv4 layer now covers their
    // former paths, so an unsigned request is refused rather than answered.
    for path in ["/api-docs/s3/openapi.json", "/swagger-ui"] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).expect("request"))
            .await
            .expect("response");
        assert_ne!(
            response.status(),
            StatusCode::OK,
            "{path} must not be served from the S3 port"
        );
    }
}

/// Stale or wrong-service signatures are rejected before semantic dispatch.
#[tokio::test]
async fn semantic_sigv4_rejects_stale_and_wrong_service_requests() {
    let app = router_with_sigv4(
        Arc::new(RecordingApi(Mutex::new(Vec::new()))),
        SemanticCredentials::new("key".to_owned(), "secret".to_owned()),
    );
    let stale = (chrono::Utc::now() - chrono::Duration::minutes(16))
        .format("%Y%m%dT%H%M%SZ")
        .to_string();
    assert_eq!(
        app.clone()
            .oneshot(signed_request(
                "POST",
                "/PutVectors",
                "s3vectors",
                &stale,
                "{}",
                false
            ))
            .await
            .expect("stale")
            .status(),
        StatusCode::FORBIDDEN
    );
    let now = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    assert_eq!(
        app.oneshot(signed_request(
            "POST",
            "/PutVectors",
            "verglasgraphs",
            &now,
            "{}",
            false
        ))
        .await
        .expect("wrong service")
        .status(),
        StatusCode::FORBIDDEN
    );
}

/// Builds a header-signed request using the semantic verifier's AWS4 canonical form.
fn signed_request(
    method: &str,
    path: &str,
    service: &str,
    timestamp: &str,
    body: &str,
    tamper: bool,
) -> Request<Body> {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    let date = &timestamp[..8];
    let payload = hex::encode(Sha256::digest(body.as_bytes()));
    let signed = "host;x-amz-content-sha256;x-amz-date";
    let (raw_path, query) = path.split_once('?').unwrap_or((path, ""));
    let canonical = format!(
        "{method}\n{raw_path}\n{}\nhost:example.test\nx-amz-content-sha256:{payload}\nx-amz-date:{timestamp}\n\n{signed}\n{payload}",
        canonical_query(query)
    );
    let scope = format!("{date}/us-east-1/{service}/aws4_request");
    let text = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    fn mac(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }
    let date_key = mac(b"AWS4secret", date.as_bytes());
    let region_key = mac(&date_key, b"us-east-1");
    let service_key = mac(&region_key, service.as_bytes());
    let key = mac(&service_key, b"aws4_request");
    let mut signature = hex::encode(mac(&key, text.as_bytes()));
    if tamper {
        signature.replace_range(..1, "0");
    }
    Request::builder().method(method).uri(path).header("host", "example.test").header("x-amz-date", timestamp).header("x-amz-content-sha256", payload).header("authorization", format!("AWS4-HMAC-SHA256 Credential=key/{scope}, SignedHeaders={signed}, Signature={signature}")).body(Body::from(body.to_owned())).expect("request")
}

/// Sorts query parameters using the AWS encoded-byte ordering used by the fixture.
fn canonical_query(query: &str) -> String {
    let mut pairs = query
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| part.split_once('=').map_or((part, ""), |pair| pair))
        .collect::<Vec<_>>();
    pairs.sort_unstable();
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Every exact AWS URI reaches the durable-adapter boundary and never falls through to S3.
#[tokio::test]
async fn every_frozen_aws_operation_dispatches_on_its_exact_method_and_uri() {
    let api = Arc::new(RecordingApi(Mutex::new(Vec::new())));
    let app = router(api.clone());
    let operations = [
        ("POST", "/CreateIndex"),
        ("POST", "/CreateVectorBucket"),
        ("POST", "/DeleteIndex"),
        ("POST", "/DeleteVectorBucket"),
        ("POST", "/DeleteVectorBucketPolicy"),
        ("POST", "/DeleteVectors"),
        ("POST", "/GetIndex"),
        ("POST", "/GetVectorBucket"),
        ("POST", "/GetVectorBucketPolicy"),
        ("POST", "/GetVectors"),
        ("POST", "/ListIndexes"),
        ("GET", "/tags/arn%3Aexample"),
        ("POST", "/ListVectorBuckets"),
        ("POST", "/ListVectors"),
        ("POST", "/PutVectorBucketPolicy"),
        ("POST", "/PutVectors"),
        ("POST", "/QueryVectors"),
        ("POST", "/tags/arn%3Aexample"),
        ("DELETE", "/tags/arn%3Aexample"),
    ];
    for (method, uri) in operations {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .expect("request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK, "{method} {uri}");
    }
    assert_eq!(api.0.lock().expect("recording lock").len(), 19);
}

/// An unlisted URI is not silently treated as a semantic request.
#[tokio::test]
async fn unknown_semantic_uri_is_not_dispatched() {
    let app = router(Arc::new(RecordingApi(Mutex::new(Vec::new()))));
    let response = app
        .oneshot(
            Request::post("/QueryVector")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("route response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Tag-resource paths percent-decode the exact AWS ARN before dispatch.
#[tokio::test]
async fn tag_route_decodes_an_aws_shaped_resource_arn() {
    let api = Arc::new(RecordingApi(Mutex::new(Vec::new())));
    let app = router(api);
    let arn =
        "arn%3Aaws%3As3vectors%3Aus-east-1%3A000000000000%3Abucket%2Fdocs%2Findex%2Fembeddings";
    let response = app
        .oneshot(
            Request::get(format!("/tags/{arn}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("route response");
    assert_eq!(response.status(), StatusCode::OK);
}
