//! Regression coverage for the S3-only data port.
//!
//! Scoped Verglas APIs live on the bearer-authenticated admin listener. The
//! SigV4 endpoint serves only S3 objects and must never reinterpret an access
//! key as a tenant or user principal.

mod support;

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use object_store::ObjectStoreExt;
use object_store::PutPayload;
use object_store::path::Path;
use support::sigv4::{ACCESS_KEY, BUCKET, REGION, SECRET_KEY, error_code, sign_headers};
use verglas_s3::{
    BackendStore, NoopInvalidation, PassthroughList, PassthroughRead, PassthroughWrite,
};

/// Boots the authenticated object endpoint over one in-memory bucket.
async fn serve_s3_only() -> String {
    let store = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &Path::from("data/example.txt"),
            PutPayload::from(Bytes::from_static(b"object data")),
        )
        .await
        .expect("seed object");
    let app = verglas_s3::router_with_passthrough(
        "default",
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single("default", BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default", BUCKET, store,
        ))),
        Arc::new(NoopInvalidation),
        Some((ACCESS_KEY.to_owned(), SECRET_KEY.to_owned())),
        None,
        None,
        None,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("address");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    format!("http://{addr}")
}

#[tokio::test]
async fn signed_object_get_still_uses_the_s3_path() {
    let base = serve_s3_only().await;
    let url = format!("{base}/{BUCKET}/data/example.txt");
    let headers = sign_headers("GET", &url, ACCESS_KEY, SECRET_KEY, REGION, Utc::now(), &[]);
    let response = reqwest::Client::new()
        .get(url)
        .headers(headers)
        .send()
        .await
        .expect("GET object");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.bytes().await.expect("body"), b"object data"[..]);
}

#[tokio::test]
async fn signed_v1_path_is_not_a_verglas_api() {
    let base = serve_s3_only().await;
    let url = format!("{base}/v1/query");
    let headers = sign_headers(
        "POST",
        &url,
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        Utc::now(),
        &[],
    );
    let response = reqwest::Client::new()
        .post(url)
        .headers(headers)
        .body(r#"{"sql":"select 1"}"#)
        .send()
        .await
        .expect("POST legacy path");
    assert!(!response.status().is_success());
    let body = response.text().await.expect("error body");
    assert_eq!(error_code(&body).as_deref(), Some("InvalidBucketName"));
}
