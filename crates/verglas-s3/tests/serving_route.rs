//! The `/v1` serving surface on the SigV4-gated S3 port.
//!
//! Query and logical-write execution are served on the same s3s
//! surface engines point at, gated by the existing SigV4 auth path. These tests
//! prove the two halves of that contract with a stub serving API:
//!
//! - an unsigned `/v1` request is rejected with `AccessDenied` before the stub
//!   ever runs (the s3s route's default access check is the gate), and
//! - a SigV4-signed `/v1/query` and `/v1/write/{t}` reach the stub and
//!   round-trip its response verbatim.

mod support;

use std::sync::Arc;
use std::sync::Mutex;

use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use chrono::Utc;
use serde_json::Value;
use support::sigv4::{ACCESS_KEY, BUCKET, REGION, SECRET_KEY, error_code, sign_headers};
use verglas_s3::{
    ApiRequest, ApiResponse, BackendStore, NoopInvalidation, PassthroughList, PassthroughRead,
    PassthroughWrite, ServingApi,
};

/// One request the stub saw, captured for assertions.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SeenRequest {
    tenant: String,
    method: String,
    path: String,
    body: Vec<u8>,
}

/// A [`ServingApi`] that records every request it receives and echoes a small
/// JSON summary, so a test can prove a request reached it and read back what it
/// carried.
struct StubApi {
    seen: Arc<Mutex<Vec<SeenRequest>>>,
}

#[async_trait::async_trait]
impl ServingApi for StubApi {
    async fn handle(&self, req: ApiRequest) -> ApiResponse {
        self.seen.lock().expect("stub lock").push(SeenRequest {
            tenant: req.tenant.clone(),
            method: req.method.to_string(),
            path: req.uri.path().to_owned(),
            body: req.body.to_vec(),
        });
        let echo = serde_json::json!({
            "seen_method": req.method.as_str(),
            "seen_path": req.uri.path(),
            "seen_body_len": req.body.len(),
        });
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().expect("mime"));
        ApiResponse {
            status: StatusCode::OK,
            headers,
            body: Bytes::from(serde_json::to_vec(&echo).expect("serialize echo")),
        }
    }
}

/// Boots the front-end with SigV4 auth enabled and the stub `/v1` serving API
/// wired in. Returns the base URL and the shared capture buffer.
async fn serve_with_stub() -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
    let store = Arc::new(object_store::memory::InMemory::new());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let api: Arc<dyn ServingApi> = Arc::new(StubApi { seen: seen.clone() });
    let app = verglas_s3::router_with_passthrough(
        PassthroughRead::new(BackendStore::single(BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single(BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(BUCKET, store))),
        Arc::new(NoopInvalidation),
        Some((ACCESS_KEY.to_owned(), SECRET_KEY.to_owned())),
        None,
        Some(api),
        None,
        None,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (format!("http://{addr}"), seen)
}

/// The SigV4 unsigned-payload sentinel: signing a body-carrying request over
/// this instead of the body's hash keeps the test signer simple while still
/// exercising the real signature verification path.
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

#[tokio::test]
async fn unsigned_v1_query_is_access_denied() {
    let (base, seen) = serve_with_stub().await;
    let url = format!("{base}/v1/query");
    let response = reqwest::Client::new()
        .post(&url)
        .body(r#"{"sql":"select 1"}"#)
        .send()
        .await
        .expect("POST");
    assert_eq!(response.status(), 403);
    let text = response.text().await.expect("body");
    assert_eq!(error_code(&text).as_deref(), Some("AccessDenied"));
    // The gate runs before the route body: the stub never saw the request.
    assert!(seen.lock().expect("stub lock").is_empty());
}

#[tokio::test]
async fn signed_v1_query_reaches_the_stub() {
    let (base, seen) = serve_with_stub().await;
    let url = format!("{base}/v1/query");
    let body = r#"{"sql":"select 42"}"#;
    let headers = sign_headers(
        "POST",
        &url,
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        Utc::now(),
        &[("x-amz-content-sha256", UNSIGNED_PAYLOAD)],
    );
    let response = reqwest::Client::new()
        .post(&url)
        .headers(headers)
        .body(body)
        .send()
        .await
        .expect("POST");
    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.expect("json body");
    assert_eq!(json["seen_method"], "POST");
    assert_eq!(json["seen_path"], "/v1/query");
    assert_eq!(json["seen_body_len"], body.len());

    let captured = seen.lock().expect("stub lock");
    assert_eq!(
        captured.as_slice(),
        &[SeenRequest {
            tenant: ACCESS_KEY.to_owned(),
            method: "POST".to_owned(),
            path: "/v1/query".to_owned(),
            body: body.as_bytes().to_vec(),
        }]
    );
}

/// A signed KV request reaches the extension with the access key as its tenant.
#[tokio::test]
async fn signed_v1_kv_reaches_the_stub_with_authenticated_tenant() {
    let (base, seen) = serve_with_stub().await;
    let path = "/v1/kv/workshop.blueprints/featured";
    let url = format!("{base}{path}");
    let headers = sign_headers(
        "PUT",
        &url,
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        Utc::now(),
        &[("x-amz-content-sha256", UNSIGNED_PAYLOAD)],
    );
    let response = reqwest::Client::new()
        .put(&url)
        .headers(headers)
        .body("blue")
        .send()
        .await
        .expect("PUT");
    assert_eq!(response.status(), StatusCode::OK);

    let captured = seen.lock().expect("stub lock");
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].tenant, ACCESS_KEY);
    assert_eq!(captured[0].path, path);
    assert_eq!(captured[0].body, b"blue");
}

#[tokio::test]
async fn signed_v1_write_reaches_the_stub() {
    let (base, seen) = serve_with_stub().await;
    let path = "/v1/write/analytics.events";
    let url = format!("{base}{path}");
    let body = r#"{"rows":[{"a":1}]}"#;
    let headers = sign_headers(
        "POST",
        &url,
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        Utc::now(),
        &[("x-amz-content-sha256", UNSIGNED_PAYLOAD)],
    );
    let response = reqwest::Client::new()
        .post(&url)
        .headers(headers)
        .body(body)
        .send()
        .await
        .expect("POST");
    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.expect("json body");
    assert_eq!(json["seen_path"], path);
    assert_eq!(json["seen_body_len"], body.len());

    let captured = seen.lock().expect("stub lock");
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].path, path);
    assert_eq!(captured[0].body, body.as_bytes());
}
