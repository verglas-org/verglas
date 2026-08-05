//! HTTP contract tests for the authenticated native KV API.

use std::collections::HashMap;

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use bytes::Bytes;
use tower::ServiceExt;
use verglas_kv::{Store, StoreConfig};
use verglas_rest::kv::{KvAuthorizer, KvGrant, KvRuntime};

/// Builds a router with read-only and read/write namespace-scoped tokens.
fn app(capacity_bytes: u64) -> axum::Router {
    let dir = tempfile::tempdir().expect("tempdir").keep();
    let store = Store::open(
        &dir,
        StoreConfig {
            capacity_bytes,
            ram_bytes: capacity_bytes.min(4096),
        },
    )
    .expect("store");
    let grants = HashMap::from([
        (
            "rw-blueprints".to_owned(),
            KvGrant {
                tenant: "tenant-a".to_owned(),
                namespace: "workshop.blueprints".to_owned(),
                read: true,
                write: true,
            },
        ),
        (
            "read-blueprints".to_owned(),
            KvGrant {
                tenant: "tenant-a".to_owned(),
                namespace: "workshop.blueprints".to_owned(),
                read: true,
                write: false,
            },
        ),
        (
            "rw-other-tenant".to_owned(),
            KvGrant {
                tenant: "tenant-b".to_owned(),
                namespace: "workshop.blueprints".to_owned(),
                read: true,
                write: true,
            },
        ),
    ]);
    verglas_rest::kv::router(KvRuntime {
        store,
        authorizer: KvAuthorizer::new(grants),
    })
}

/// Builds one authenticated request.
fn request(method: Method, uri: &str, token: Option<&str>, body: impl Into<Body>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    builder.body(body.into()).expect("request")
}

/// Extracts a response body with a test-only bound.
async fn body(response: axum::response::Response) -> Bytes {
    to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("body")
}

/// Put/get/delete expose raw bytes, metadata headers, conditions, and idempotency.
#[tokio::test]
async fn exact_value_operations_and_conditions() {
    let app = app(1024 * 1024);
    let put = Request::builder()
        .method(Method::PUT)
        .uri("/v1/kv/workshop.blueprints/featured")
        .header(header::AUTHORIZATION, "Bearer rw-blueprints")
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("x-verglas-meta-kind", "demo")
        .header("idempotency-key", "put-1")
        .body(Body::from("blue"))
        .expect("put");
    let response = app.clone().oneshot(put).await.expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let version = response.headers()[header::ETAG]
        .to_str()
        .expect("etag")
        .to_owned();

    let replay = Request::builder()
        .method(Method::PUT)
        .uri("/v1/kv/workshop.blueprints/featured")
        .header(header::AUTHORIZATION, "Bearer rw-blueprints")
        .header("idempotency-key", "put-1")
        .body(Body::from("changed"))
        .expect("replay");
    let response = app.clone().oneshot(replay).await.expect("response");
    assert_eq!(response.headers()[header::ETAG], version);
    assert_eq!(response.headers()["x-verglas-idempotent"], "true");

    let response = app
        .clone()
        .oneshot(request(
            Method::GET,
            "/v1/kv/workshop.blueprints/featured",
            Some("read-blueprints"),
            Body::empty(),
        ))
        .await
        .expect("get");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::ETAG], version);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/octet-stream"
    );
    assert_eq!(response.headers()["x-verglas-meta-kind"], "demo");
    assert_eq!(response.headers()["x-verglas-kv-tier"], "ram");
    assert_eq!(body(response).await, Bytes::from_static(b"blue"));

    let conflict = Request::builder()
        .method(Method::PUT)
        .uri("/v1/kv/workshop.blueprints/featured")
        .header(header::AUTHORIZATION, "Bearer rw-blueprints")
        .header(header::IF_MATCH, "\"wrong\"")
        .body(Body::from("wrong"))
        .expect("conflict");
    assert_eq!(
        app.clone()
            .oneshot(conflict)
            .await
            .expect("response")
            .status(),
        StatusCode::PRECONDITION_FAILED
    );

    let delete = Request::builder()
        .method(Method::DELETE)
        .uri("/v1/kv/workshop.blueprints/featured")
        .header(header::AUTHORIZATION, "Bearer rw-blueprints")
        .header(header::IF_MATCH, version)
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        body(response).await,
        Bytes::from_static(br#"{"removed":true}"#)
    );
    assert_eq!(
        app.oneshot(request(
            Method::GET,
            "/v1/kv/workshop.blueprints/featured",
            Some("read-blueprints"),
            Body::empty(),
        ))
        .await
        .expect("missing")
        .status(),
        StatusCode::NOT_FOUND
    );
}

/// List is bounded and metadata-only with stable opaque continuation.
#[tokio::test]
async fn list_is_prefix_bounded_paginated_and_never_contains_values() {
    let app = app(1024 * 1024);
    for key in ["user/c", "other", "user/a", "user/b"] {
        let response = app
            .clone()
            .oneshot(request(
                Method::PUT,
                &format!("/v1/kv/workshop.blueprints/{key}"),
                Some("rw-blueprints"),
                Body::from(format!("secret-{key}")),
            ))
            .await
            .expect("put");
        assert_eq!(response.status(), StatusCode::CREATED);
    }
    let response = app
        .clone()
        .oneshot(request(
            Method::GET,
            "/v1/kv/workshop.blueprints?prefix=user%2F&limit=2",
            Some("read-blueprints"),
            Body::empty(),
        ))
        .await
        .expect("list");
    assert_eq!(response.status(), StatusCode::OK);
    let page: serde_json::Value = serde_json::from_slice(&body(response).await).expect("json");
    assert_eq!(page["entries"][0]["key"], "user/a");
    assert_eq!(page["entries"][1]["key"], "user/b");
    assert!(!page.to_string().contains("secret-"));
    let cursor = page["next_cursor"].as_str().expect("cursor");

    let response = app
        .oneshot(request(
            Method::GET,
            &format!("/v1/kv/workshop.blueprints?prefix=user%2F&limit=2&cursor={cursor}"),
            Some("read-blueprints"),
            Body::empty(),
        ))
        .await
        .expect("list");
    let page: serde_json::Value = serde_json::from_slice(&body(response).await).expect("json");
    assert_eq!(page["entries"].as_array().expect("entries").len(), 1);
    assert_eq!(page["entries"][0]["key"], "user/c");
}

/// Authentication, exact namespace grants, verbs, and tenant identity are enforced first.
#[tokio::test]
async fn authorization_enforces_namespace_verb_and_tenant_isolation() {
    let app = app(1024 * 1024);
    assert_eq!(
        app.clone()
            .oneshot(request(
                Method::GET,
                "/v1/kv/workshop.blueprints/key",
                None,
                Body::empty(),
            ))
            .await
            .expect("response")
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                Method::PUT,
                "/v1/kv/workshop.blueprints/key",
                Some("read-blueprints"),
                Body::from("forbidden"),
            ))
            .await
            .expect("response")
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                Method::GET,
                "/v1/kv/other/key",
                Some("read-blueprints"),
                Body::empty(),
            ))
            .await
            .expect("response")
            .status(),
        StatusCode::FORBIDDEN
    );
    app.clone()
        .oneshot(request(
            Method::PUT,
            "/v1/kv/workshop.blueprints/key",
            Some("rw-blueprints"),
            Body::from("tenant-a"),
        ))
        .await
        .expect("put");
    assert_eq!(
        app.oneshot(request(
            Method::GET,
            "/v1/kv/workshop.blueprints/key",
            Some("rw-other-tenant"),
            Body::empty(),
        ))
        .await
        .expect("other tenant")
        .status(),
        StatusCode::NOT_FOUND
    );
}

/// TTL, malformed cursors, and capacity failures map to explicit statuses.
#[tokio::test]
async fn expiration_validation_and_capacity_have_honest_statuses() {
    let app = app(700);
    let expired = Request::builder()
        .method(Method::PUT)
        .uri("/v1/kv/workshop.blueprints/expired")
        .header(header::AUTHORIZATION, "Bearer rw-blueprints")
        .header("x-verglas-expires-at-ms", "1")
        .body(Body::from("gone"))
        .expect("put");
    assert_eq!(
        app.clone().oneshot(expired).await.expect("put").status(),
        StatusCode::CREATED
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                Method::GET,
                "/v1/kv/workshop.blueprints/expired",
                Some("read-blueprints"),
                Body::empty()
            ))
            .await
            .expect("get")
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                Method::GET,
                "/v1/kv/workshop.blueprints?limit=0",
                Some("read-blueprints"),
                Body::empty()
            ))
            .await
            .expect("bad limit")
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                Method::GET,
                "/v1/kv/workshop.blueprints?cursor=bad",
                Some("read-blueprints"),
                Body::empty()
            ))
            .await
            .expect("bad cursor")
            .status(),
        StatusCode::BAD_REQUEST
    );

    let mut saw_capacity = false;
    for n in 0..20 {
        let response = app
            .clone()
            .oneshot(request(
                Method::PUT,
                &format!("/v1/kv/workshop.blueprints/fill-{n}"),
                Some("rw-blueprints"),
                Body::from(vec![7; 128]),
            ))
            .await
            .expect("put");
        if response.status() == StatusCode::INSUFFICIENT_STORAGE {
            saw_capacity = true;
            break;
        }
    }
    assert!(saw_capacity);
}
