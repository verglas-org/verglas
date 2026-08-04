//! Addressing-style tests for the S3 front-end (issue #11): with a base domain
//! configured, the endpoint accepts virtual-hosted-style requests
//! (`Host: bucket.<domain>`, key in the path) *and* path-style requests
//! (`Host: <domain>`, `/bucket/key`), and both resolve the same object. Without
//! a domain the endpoint is path-style only, unchanged from before.
//!
//! A recording reader captures the `(bucket, key)` each request resolves to, so
//! the tests prove the bucket was extracted from the right place — not merely
//! that some bytes came back.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt;
use object_store::memory::InMemory;
use reqwest::header::HOST;
use verglas_s3::{
    BackendStore, CacheKey, NoopInvalidation, ObjectGet, ObjectMeta, ObjectRead, PassthroughList,
    PassthroughWrite, ReadError, ReadRange,
};

/// The endpoint's configured base domain in these tests.
const DOMAIN: &str = "s3.verglas.test";
/// The object body every resolved read returns.
const BODY: &[u8] = b"virtual-hosted-and-path-style-resolve-the-same-object";

/// An [`ObjectRead`] that records the logical key of every read and answers with
/// a fixed body. Recording the key is the whole point: it lets a test assert the
/// bucket came out of the Host header (virtual-hosted) or the path (path-style).
#[derive(Clone, Default)]
struct RecordingReader {
    seen: Arc<Mutex<Vec<CacheKey>>>,
}

impl RecordingReader {
    /// The keys resolved so far, in request order.
    fn seen(&self) -> Vec<CacheKey> {
        self.seen.lock().expect("lock").clone()
    }

    /// Builds the fixed whole-object metadata every read reports.
    fn meta() -> ObjectMeta {
        ObjectMeta {
            size: BODY.len() as u64,
            e_tag: Some("\"fixed\"".to_owned()),
            ..ObjectMeta::default()
        }
    }
}

impl ObjectRead for RecordingReader {
    fn get(
        &self,
        key: &CacheKey,
        _range: ReadRange,
    ) -> impl std::future::Future<Output = Result<ObjectGet, ReadError>> + Send {
        self.seen.lock().expect("lock").push(key.clone());
        async move {
            let body = futures::stream::once(async { Ok(Bytes::from_static(BODY)) }).boxed();
            Ok(ObjectGet {
                meta: Self::meta(),
                range: 0..BODY.len() as u64,
                body,
                served_from: verglas_s3::TierCell::new(),
            })
        }
    }

    fn head(
        &self,
        key: &CacheKey,
    ) -> impl std::future::Future<Output = Result<ObjectMeta, ReadError>> + Send {
        self.seen.lock().expect("lock").push(key.clone());
        async move { Ok(Self::meta()) }
    }
}

/// Serves the front-end (auth off) over a loopback port with the given base
/// domain, returning the reader (to inspect resolved keys) and the base URL.
async fn serve(domain: Option<&str>) -> (RecordingReader, String) {
    let reader = RecordingReader::default();
    let store = Arc::new(InMemory::new());
    let app = verglas_s3::router_with_passthrough(
        reader.clone(),
        PassthroughWrite::new(BackendStore::single("lake", store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single("lake", store))),
        Arc::new(NoopInvalidation),
        None,
        None,
        None,
        domain,
        None,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (reader, format!("http://{addr}"))
}

#[tokio::test]
async fn virtual_hosted_and_path_style_resolve_the_same_object() {
    let (reader, base) = serve(Some(DOMAIN)).await;
    let client = reqwest::Client::new();

    // Path-style: Host is the bare domain, the bucket rides in the path.
    let path_style = client
        .get(format!("{base}/lake/warehouse/part-0.parquet"))
        .header(HOST, DOMAIN)
        .send()
        .await
        .expect("path-style GET");
    assert_eq!(path_style.status(), 200);
    let path_body = path_style.bytes().await.expect("path body");

    // Virtual-hosted style: the bucket is a Host subdomain, the key is the path.
    let vhost = client
        .get(format!("{base}/warehouse/part-0.parquet"))
        .header(HOST, format!("lake.{DOMAIN}"))
        .send()
        .await
        .expect("virtual-hosted GET");
    assert_eq!(vhost.status(), 200);
    let vhost_body = vhost.bytes().await.expect("vhost body");

    // Same bytes both ways.
    assert_eq!(path_body.as_ref(), BODY);
    assert_eq!(vhost_body.as_ref(), BODY);

    // And both resolved the identical logical object: bucket `lake`, key
    // `warehouse/part-0.parquet`.
    let seen = reader.seen();
    assert_eq!(seen.len(), 2, "one resolve per request");
    let expected = CacheKey {
        bucket: "lake".to_owned(),
        key: "warehouse/part-0.parquet".to_owned(),
    };
    assert_eq!(seen[0], expected, "path-style must resolve bucket=lake");
    assert_eq!(
        seen[1], expected,
        "virtual-hosted must resolve the same object"
    );
}

#[tokio::test]
async fn without_a_domain_the_endpoint_is_path_style_only() {
    // The default router ignores the Host for bucket extraction: a request that
    // looks virtual-hosted is parsed path-style, so the "bucket" is the first
    // path segment. This is the dev default and must not change.
    let (reader, base) = serve(None).await;
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{base}/lake/warehouse/part-0.parquet"))
        .header(HOST, format!("lake.{DOMAIN}"))
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 200);

    let seen = reader.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(
        seen[0],
        CacheKey {
            bucket: "lake".to_owned(),
            key: "warehouse/part-0.parquet".to_owned(),
        },
        "path-style parsing takes the bucket from the path, not the Host"
    );
}
