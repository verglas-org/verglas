//! Integration tests for conditional GET/HEAD (issue #10): `If-Match`,
//! `If-None-Match`, `If-Modified-Since`, and `If-Unmodified-Since` over the
//! real s3s protocol layer.
//!
//! The reader is a call-counting stub that answers `head` from in-memory
//! metadata (standing in for a cached key→ETag mapping) and only issues a body
//! on `get`. Counting `get` calls is how these tests prove the acceptance
//! criterion "a cached object answered with 304 generates zero backend
//! requests" without a live fill-manager: a 304/412 must be decided from
//! metadata alone, never a body fetch.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::stream;
use futures::stream::StreamExt;
use verglas_core::CacheKey;
use verglas_s3::{
    BackendStore, NoopInvalidation, ObjectGet, ObjectMeta, ObjectRead, PassthroughList,
    PassthroughWrite, ReadError, ReadRange,
};

/// The bucket every test addresses.
const BUCKET: &str = "test-bucket";
/// The object key every test addresses.
const KEY: &str = "data/foo";
/// The object's ETag (quoted, as an origin reports it).
const ETAG: &str = "\"9a0364b9e99bb480dd25e1f0284c8555\"";
/// The object body served on a 200.
const BODY: &[u8] = b"bar";

/// An [`ObjectRead`] that answers `head` from fixed metadata and counts how
/// many times each method is called. `get` also returns the body; a test that
/// expects a 304/412 asserts `get` was never called.
struct CountingRead {
    /// How many times `get` (a body fetch) was invoked.
    gets: Arc<AtomicU64>,
    /// How many times `head` (a metadata resolve) was invoked.
    heads: Arc<AtomicU64>,
}

impl CountingRead {
    /// The fixed metadata both `get` and `head` report: ETag plus a
    /// last-modified of 2020-01-01T00:00:00Z.
    fn meta(&self) -> ObjectMeta {
        ObjectMeta {
            size: BODY.len() as u64,
            e_tag: Some(ETAG.to_owned()),
            last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_577_836_800)),
            ..ObjectMeta::default()
        }
    }
}

impl ObjectRead for CountingRead {
    async fn get(&self, _key: &CacheKey, _range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        Ok(ObjectGet {
            meta: self.meta(),
            range: 0..BODY.len() as u64,
            body: stream::once(async { Ok(Bytes::from_static(BODY)) }).boxed(),
            served_from: verglas_s3::TierCell::new(),
        })
    }

    async fn head(&self, _key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.heads.fetch_add(1, Ordering::SeqCst);
        Ok(self.meta())
    }
}

/// Serves an axum router on an ephemeral loopback port, returning its base URL.
async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

/// Boots the front-end over a fresh counting reader, returning the base URL and
/// the two call counters.
async fn serve_counting() -> (String, Arc<AtomicU64>, Arc<AtomicU64>) {
    let gets = Arc::new(AtomicU64::new(0));
    let heads = Arc::new(AtomicU64::new(0));
    let reader = CountingRead {
        gets: Arc::clone(&gets),
        heads: Arc::clone(&heads),
    };
    let store = Arc::new(object_store::memory::InMemory::new());
    let base = serve(verglas_s3::router(
        "default",
        reader,
        PassthroughWrite::new(BackendStore::single("default", BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default", BUCKET, store,
        ))),
        Arc::new(NoopInvalidation),
        None,
    ))
    .await;
    (base, gets, heads)
}

/// The object URL under test.
fn url(base: &str) -> String {
    format!("{base}/{BUCKET}/{KEY}")
}

/// GET with a matching `If-None-Match` returns 304, the ETag header, and an
/// empty body — and never fetches the body (zero `get` calls).
#[tokio::test]
async fn get_if_none_match_hit_returns_304_without_body_fetch() {
    let (base, gets, _heads) = serve_counting().await;
    let response = reqwest::Client::new()
        .get(url(&base))
        .header("If-None-Match", ETAG)
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 304);
    assert_eq!(
        response.headers().get("etag").expect("etag header"),
        ETAG,
        "304 must echo the object's ETag"
    );
    let body = response.bytes().await.expect("body");
    assert!(body.is_empty(), "304 must carry no body");
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "304 must not fetch the body"
    );
}

/// GET with a non-matching `If-Match` returns 412 PreconditionFailed and never
/// fetches the body.
#[tokio::test]
async fn get_if_match_mismatch_returns_412() {
    let (base, gets, _heads) = serve_counting().await;
    let response = reqwest::Client::new()
        .get(url(&base))
        .header("If-Match", "\"ABCORZ\"")
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 412);
    let body = response.text().await.expect("body");
    assert!(
        body.contains("PreconditionFailed"),
        "412 body must name the error code, got: {body}"
    );
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "412 must not fetch the body"
    );
}

/// GET with an `If-Modified-Since` after the object's mtime returns 304.
#[tokio::test]
async fn get_if_modified_since_not_modified_returns_304() {
    let (base, gets, _heads) = serve_counting().await;
    let response = reqwest::Client::new()
        .get(url(&base))
        .header("If-Modified-Since", "Wed, 01 Jan 2020 00:00:01 GMT")
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 304);
    assert_eq!(gets.load(Ordering::SeqCst), 0);
}

/// GET with an `If-Unmodified-Since` before the object's mtime returns 412.
#[tokio::test]
async fn get_if_unmodified_since_modified_returns_412() {
    let (base, gets, _heads) = serve_counting().await;
    let response = reqwest::Client::new()
        .get(url(&base))
        .header("If-Unmodified-Since", "Sat, 29 Oct 1994 19:43:31 GMT")
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 412);
    assert_eq!(gets.load(Ordering::SeqCst), 0);
}

/// GET with a non-matching `If-None-Match` serves the object normally (200,
/// body present).
#[tokio::test]
async fn get_if_none_match_miss_serves_object() {
    let (base, gets, _heads) = serve_counting().await;
    let response = reqwest::Client::new()
        .get(url(&base))
        .header("If-None-Match", "\"nomatch\"")
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 200);
    let body = response.bytes().await.expect("body");
    assert_eq!(body.as_ref(), BODY);
    assert_eq!(
        gets.load(Ordering::SeqCst),
        1,
        "a served read fetches the body once"
    );
}

/// HEAD with a matching `If-None-Match` returns 304 with the ETag and no body.
#[tokio::test]
async fn head_if_none_match_hit_returns_304() {
    let (base, gets, _heads) = serve_counting().await;
    let response = reqwest::Client::new()
        .head(url(&base))
        .header("If-None-Match", ETAG)
        .send()
        .await
        .expect("HEAD");
    assert_eq!(response.status(), 304);
    assert_eq!(response.headers().get("etag").expect("etag header"), ETAG);
    assert_eq!(gets.load(Ordering::SeqCst), 0, "HEAD never fetches a body");
}

/// An unconditional GET is unaffected: 200 with the body.
#[tokio::test]
async fn plain_get_still_serves() {
    let (base, gets, _heads) = serve_counting().await;
    let response = reqwest::get(url(&base)).await.expect("GET");
    assert_eq!(response.status(), 200);
    let body = response.bytes().await.expect("body");
    assert_eq!(body.as_ref(), BODY);
    assert_eq!(gets.load(Ordering::SeqCst), 1);
}
