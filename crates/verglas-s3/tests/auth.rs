//! Integration tests for SigV4 validation with static dev credentials (issue #7).

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use chrono::{Duration, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use support::sigv4::{
    ACCESS_KEY, BUCKET, REGION, SECRET_KEY, error_code, error_xml_fields, presign_get, sign_headers,
};
use verglas_core::CacheKey;
use verglas_s3::{
    BackendStore, NoopInvalidation, ObjectGet, ObjectMeta, ObjectRead, PassthroughList,
    PassthroughRead, PassthroughWrite, ReadError, ReadRange,
};

/// Boots the front-end with auth enabled over an in-memory object.
async fn serve_authed(objects: &[(&str, Bytes)]) -> String {
    let store = Arc::new(InMemory::new());
    for (key, bytes) in objects {
        store
            .put(&Path::from(*key), PutPayload::from(bytes.clone()))
            .await
            .expect("seed object");
    }
    let app = verglas_s3::router(
        PassthroughRead::new(BackendStore::single(BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single(BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(BUCKET, store))),
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

/// Boots the front-end with auth over an arbitrary [`ObjectRead`] source.
async fn serve_authed_reader<R: ObjectRead + 'static>(reader: R) -> String {
    let store = Arc::new(InMemory::new());
    let app = verglas_s3::router(
        reader,
        PassthroughWrite::new(BackendStore::single(BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(BUCKET, store))),
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

/// Deterministic byte pattern for streaming proofs.
fn pattern(offset: u64, len: u64) -> Bytes {
    (offset..offset + len)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// [`ObjectRead`] that streams on demand and counts bytes produced.
struct SyntheticRead {
    size: u64,
    content_type: Option<String>,
    produced: Arc<AtomicU64>,
}

const SYNTH_CHUNK: u64 = 1024 * 1024;

impl ObjectRead for SyntheticRead {
    async fn get(&self, _key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        if range != ReadRange::Full {
            return Err(ReadError::Backend(
                "synthetic reader only serves full objects".to_owned(),
            ));
        }
        let size = self.size;
        let produced = Arc::clone(&self.produced);
        let body = futures::stream::unfold(0u64, move |offset| {
            let produced = Arc::clone(&produced);
            async move {
                if offset >= size {
                    return None;
                }
                let len = SYNTH_CHUNK.min(size - offset);
                produced.fetch_add(len, Ordering::SeqCst);
                Some((Ok(pattern(offset, len)), offset + len))
            }
        })
        .boxed();
        Ok(ObjectGet {
            meta: self.meta(),
            range: 0..size,
            body,
            served_from: verglas_s3::TierCell::new(),
        })
    }

    async fn head(&self, _key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        Ok(self.meta())
    }
}

impl SyntheticRead {
    fn meta(&self) -> ObjectMeta {
        ObjectMeta {
            size: self.size,
            e_tag: Some("\"synthetic\"".to_owned()),
            last_modified: Some(std::time::SystemTime::UNIX_EPOCH),
            content_type: self.content_type.clone(),
            ..ObjectMeta::default()
        }
    }
}

#[tokio::test]
async fn signed_get_succeeds() {
    let body = Bytes::from_static(b"signed-payload");
    let base = serve_authed(&[("obj.bin", body.clone())]).await;
    let url = format!("{base}/{BUCKET}/obj.bin");
    let now = Utc::now();
    let headers = sign_headers("GET", &url, ACCESS_KEY, SECRET_KEY, REGION, now, &[]);
    let client = reqwest::Client::new();
    let response = client.get(&url).headers(headers).send().await.expect("GET");
    assert_eq!(response.status(), 200);
    assert!(response.headers().contains_key("x-amz-request-id"));
    assert_eq!(response.bytes().await.expect("body"), body);
}

#[tokio::test]
async fn unsigned_request_is_access_denied() {
    let base = serve_authed(&[("obj.bin", Bytes::from_static(b"x"))]).await;
    let url = format!("{base}/{BUCKET}/obj.bin");
    let response = reqwest::get(&url).await.expect("GET");
    assert_eq!(response.status(), 403);
    let text = response.text().await.expect("body");
    assert_eq!(error_code(&text).as_deref(), Some("AccessDenied"));
    assert!(text.contains("<RequestId>"));
    assert!(text.contains("<Message>"));
}

#[tokio::test]
async fn unknown_access_key_is_invalid_access_key_id() {
    let base = serve_authed(&[("obj.bin", Bytes::from_static(b"x"))]).await;
    let url = format!("{base}/{BUCKET}/obj.bin");
    let now = Utc::now();
    let headers = sign_headers(
        "GET",
        &url,
        "unknown-access-key",
        SECRET_KEY,
        REGION,
        now,
        &[],
    );
    let response = reqwest::Client::new()
        .get(&url)
        .headers(headers)
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 403);
    let text = response.text().await.expect("body");
    assert_eq!(error_code(&text).as_deref(), Some("InvalidAccessKeyId"));
    assert!(text.contains("<RequestId>"));
}

#[tokio::test]
async fn wrong_secret_is_signature_does_not_match_with_s3_xml_shape() {
    let base = serve_authed(&[("obj.bin", Bytes::from_static(b"x"))]).await;
    let url = format!("{base}/{BUCKET}/obj.bin");
    let now = Utc::now();
    let headers = sign_headers(
        "GET",
        &url,
        ACCESS_KEY,
        "wrong-secret-key-value-not-the-real-one",
        REGION,
        now,
        &[],
    );
    let response = reqwest::Client::new()
        .get(&url)
        .headers(headers)
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 403);
    let text = response.text().await.expect("body");
    assert_eq!(error_code(&text).as_deref(), Some("SignatureDoesNotMatch"));
    assert!(
        text.contains("<RequestId>"),
        "expected RequestId in error XML, got: {text}"
    );

    let fixture = include_str!("fixtures/signature_does_not_match.xml");
    let fixture_fields = error_xml_fields(fixture);
    let got_fields = error_xml_fields(&text);
    assert_eq!(
        got_fields.get("Code").map(String::as_str),
        fixture_fields.get("Code").map(String::as_str)
    );
    assert_eq!(got_fields.get("Message"), fixture_fields.get("Message"));
}

#[tokio::test]
async fn presigned_get_succeeds() {
    let body = Bytes::from_static(b"presigned-bytes");
    let base = serve_authed(&[("pre.bin", body.clone())]).await;
    let path = format!("/{BUCKET}/pre.bin");
    let now = Utc::now();
    let url = presign_get(&base, &path, ACCESS_KEY, SECRET_KEY, REGION, now, 300);
    let response = reqwest::get(&url).await.expect("GET");
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.expect("body"), body);
}

#[tokio::test]
async fn expired_presigned_url_is_rejected() {
    let base = serve_authed(&[("pre.bin", Bytes::from_static(b"x"))]).await;
    let path = format!("/{BUCKET}/pre.bin");
    let issued = Utc::now() - Duration::minutes(30);
    let url = presign_get(&base, &path, ACCESS_KEY, SECRET_KEY, REGION, issued, 60);
    let response = reqwest::get(&url).await.expect("GET");
    assert_eq!(response.status(), 403);
    let text = response.text().await.expect("body");
    assert_eq!(error_code(&text).as_deref(), Some("AccessDenied"));
    assert!(
        text.contains("expired") || text.contains("Expired"),
        "expected expiry message, got: {text}"
    );
}

#[tokio::test]
async fn clock_skew_beyond_fifteen_minutes_is_rejected() {
    let base = serve_authed(&[("obj.bin", Bytes::from_static(b"x"))]).await;
    let url = format!("{base}/{BUCKET}/obj.bin");
    let skewed = Utc::now() - Duration::minutes(20);
    let headers = sign_headers("GET", &url, ACCESS_KEY, SECRET_KEY, REGION, skewed, &[]);
    let response = reqwest::Client::new()
        .get(&url)
        .headers(headers)
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 403);
    let text = response.text().await.expect("body");
    assert_eq!(error_code(&text).as_deref(), Some("RequestTimeTooSkewed"));
    assert!(text.contains("<RequestId>"));
}

#[tokio::test]
async fn unsigned_payload_header_auth_succeeds() {
    let body = Bytes::from_static(b"unsigned-payload-object");
    let base = serve_authed(&[("unsigned.bin", body.clone())]).await;
    let url = format!("{base}/{BUCKET}/unsigned.bin");
    let now = Utc::now();
    let headers = sign_headers(
        "GET",
        &url,
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        now,
        &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
    );
    let response = reqwest::Client::new()
        .get(&url)
        .headers(headers)
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.expect("body"), body);
}

/// Regression: error-response middleware must not buffer success bodies. A large
/// `application/xml` object through the authed front-end must still stream.
#[tokio::test]
async fn authed_xml_success_body_streams_without_buffering() {
    const SIZE: u64 = 64 * 1024 * 1024;
    const SLACK: u64 = 16 * 1024 * 1024;

    let produced = Arc::new(AtomicU64::new(0));
    let base = serve_authed_reader(SyntheticRead {
        size: SIZE,
        content_type: Some("application/xml".to_owned()),
        produced: Arc::clone(&produced),
    })
    .await;
    let url = format!("{base}/{BUCKET}/huge.xml");
    let headers = sign_headers("GET", &url, ACCESS_KEY, SECRET_KEY, REGION, Utc::now(), &[]);
    let response = reqwest::Client::new()
        .get(&url)
        .headers(headers)
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .expect("content-type")
            .to_str()
            .expect("ascii"),
        "application/xml"
    );
    assert!(response.headers().contains_key("x-amz-request-id"));

    let mut consumed = 0u64;
    let mut produced_at_first_chunk = None;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("chunk");
        if produced_at_first_chunk.is_none() {
            produced_at_first_chunk = Some(produced.load(Ordering::SeqCst));
        }
        consumed += chunk.len() as u64;
        let lead = produced.load(Ordering::SeqCst).saturating_sub(consumed);
        assert!(
            lead <= SLACK,
            "producer ran {lead} bytes ahead of the consumer (> {SLACK}); middleware buffered the body"
        );
    }
    assert_eq!(consumed, SIZE, "short body");
    assert_eq!(produced.load(Ordering::SeqCst), SIZE);
    let first = produced_at_first_chunk.expect("saw at least one chunk");
    assert!(
        first < SIZE,
        "producer finished all {SIZE} bytes before the first chunk was consumed"
    );
}
