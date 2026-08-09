//! Integration tests for aws-chunked (streaming SigV4) PUT uploads (issue #7).

mod support;

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use support::sigv4::{ACCESS_KEY, BUCKET, REGION, SECRET_KEY, sign_aws_chunked_put, sign_headers};
use verglas_s3::{
    BackendStore, NoopInvalidation, PassthroughList, PassthroughRead, PassthroughWrite,
};

/// Boots the authed front-end over an empty in-memory origin.
async fn serve_authed_write() -> String {
    let store = Arc::new(InMemory::new());
    let app = verglas_s3::router(
        "default",
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single("default", BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default", BUCKET, store,
        ))),
        Arc::new(NoopInvalidation),
        Some((ACCESS_KEY.to_owned(), SECRET_KEY.to_owned())),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

/// Deterministic payload; byte `i` is `i % 251`.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

#[tokio::test]
async fn aws_chunked_put_single_chunk_round_trips() {
    let base = serve_authed_write().await;
    let key = "chunked/single.bin";
    let decoded = pattern(5);
    let url = format!("{base}/{BUCKET}/{key}");
    let now = Utc::now();
    let (headers, body) = sign_aws_chunked_put(
        &url,
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        now,
        &decoded,
        decoded.len(),
    );

    let client = reqwest::Client::new();
    let response = client
        .put(&url)
        .headers(headers)
        .body(body)
        .send()
        .await
        .expect("aws-chunked PUT");
    assert_eq!(response.status(), 200, "aws-chunked PUT should succeed");
    assert!(response.headers().contains_key("etag"));

    let get_headers = sign_headers("GET", &url, ACCESS_KEY, SECRET_KEY, REGION, now, &[]);
    let response = client
        .get(&url)
        .headers(get_headers)
        .send()
        .await
        .expect("GET after aws-chunked PUT");
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.expect("body").as_ref(), &decoded[..]);
}

#[tokio::test]
async fn aws_chunked_put_multiple_chunks_round_trips() {
    let base = serve_authed_write().await;
    let key = "chunked/multi.bin";
    let decoded = pattern(100_000);
    let url = format!("{base}/{BUCKET}/{key}");
    let now = Utc::now();
    let (headers, body) = sign_aws_chunked_put(
        &url,
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        now,
        &decoded,
        32 * 1024,
    );

    let client = reqwest::Client::new();
    let response = client
        .put(&url)
        .headers(headers)
        .body(body)
        .send()
        .await
        .expect("aws-chunked PUT");
    assert_eq!(response.status(), 200);

    let get_headers = sign_headers("GET", &url, ACCESS_KEY, SECRET_KEY, REGION, now, &[]);
    let response = client
        .get(&url)
        .headers(get_headers)
        .send()
        .await
        .expect("GET after aws-chunked PUT");
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.expect("body").as_ref(), &decoded[..]);
}

#[tokio::test]
async fn aws_chunked_put_persists_at_origin() {
    let store = Arc::new(InMemory::new());
    let app = verglas_s3::router(
        "default",
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single("default", BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default",
            BUCKET,
            store.clone(),
        ))),
        Arc::new(NoopInvalidation),
        Some((ACCESS_KEY.to_owned(), SECRET_KEY.to_owned())),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let base = format!("http://{addr}");

    let decoded = Bytes::from_static(b"aws-chunked-bytes");
    let url = format!("{base}/{BUCKET}/chunked/durable.bin");
    let now = Utc::now();
    let (headers, body) =
        sign_aws_chunked_put(&url, ACCESS_KEY, SECRET_KEY, REGION, now, &decoded, 4);

    let client = reqwest::Client::new();
    let response = client
        .put(&url)
        .headers(headers)
        .body(body)
        .send()
        .await
        .expect("PUT");
    assert_eq!(response.status(), 200);

    let stored = store
        .get(&Path::from("chunked/durable.bin"))
        .await
        .expect("object at origin")
        .bytes()
        .await
        .expect("bytes");
    assert_eq!(stored.as_ref(), decoded.as_ref());
}
