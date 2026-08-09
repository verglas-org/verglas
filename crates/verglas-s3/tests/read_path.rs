//! Integration tests for the S3 read path (issue #6): byte-identical round
//! trips, all three `Range` forms with correct `Content-Range`, HEAD/GET
//! metadata parity, S3 error XML, and bounded-memory streaming — all against
//! `object_store`'s in-memory backend (or a synthetic reader), no network.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{Attribute, Attributes, ObjectStore, PutOptions, PutPayload};
use verglas_core::CacheKey;
use verglas_s3::{
    BackendStore, NoopInvalidation, ObjectGet, ObjectMeta, ObjectRead, PassthroughList,
    PassthroughRead, PassthroughWrite, ReadError, ReadRange,
};

/// The bucket the in-memory origin serves.
const BUCKET: &str = "test-bucket";

/// Deterministic test payload: byte `i` is `i % 251` (prime, so no chunk
/// alignment can mask offset bugs).
fn pattern(offset: u64, len: u64) -> Bytes {
    (offset..offset + len)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// Serves an axum router on an ephemeral loopback port, returning a base URL.
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

/// Boots the front-end over an in-memory origin holding `objects`
/// (key, bytes, content type). No auth: these tests exercise the read path.
async fn serve_memory(objects: &[(&str, Bytes, &str)]) -> String {
    let store = Arc::new(InMemory::new());
    for (key, bytes, content_type) in objects {
        let options = PutOptions {
            attributes: Attributes::from_iter([(Attribute::ContentType, content_type.to_string())]),
            ..PutOptions::default()
        };
        store
            .put_opts(&Path::from(*key), PutPayload::from(bytes.clone()), options)
            .await
            .expect("seed object");
    }
    serve(verglas_s3::router(
        "default",
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single("default", BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default", BUCKET, store,
        ))),
        Arc::new(NoopInvalidation),
        None,
    ))
    .await
}

#[tokio::test]
async fn full_get_round_trips_bytes_and_headers() {
    let body = pattern(0, 4 * 1024 * 1024 + 13);
    let base =
        serve_memory(&[("data/part-0.parquet", body.clone(), "application/x-parquet")]).await;

    let response = reqwest::get(format!("{base}/{BUCKET}/data/part-0.parquet"))
        .await
        .expect("GET");
    assert_eq!(response.status(), 200);
    let headers = response.headers().clone();
    assert_eq!(
        headers.get("content-length").expect("content-length"),
        &body.len().to_string()
    );
    assert_eq!(
        headers.get("accept-ranges").expect("accept-ranges"),
        "bytes"
    );
    assert_eq!(
        headers.get("content-type").expect("content-type"),
        "application/x-parquet"
    );
    let etag = headers.get("etag").expect("etag").to_str().expect("ascii");
    assert!(
        etag.starts_with('"') && etag.ends_with('"'),
        "ETag must be quoted like S3's, got {etag}"
    );
    assert!(
        headers.contains_key("last-modified"),
        "Last-Modified missing"
    );
    let got = response.bytes().await.expect("body");
    assert_eq!(got, body, "round-tripped bytes differ");
}

#[tokio::test]
async fn range_requests_return_206_with_correct_content_range() {
    let body = pattern(0, 1000);
    let base = serve_memory(&[("range.bin", body.clone(), "application/octet-stream")]).await;
    let url = format!("{base}/{BUCKET}/range.bin");
    let client = reqwest::Client::new();

    // (Range header, expected Content-Range, expected byte span)
    let cases: &[(&str, &str, std::ops::Range<usize>)] = &[
        ("bytes=200-499", "bytes 200-499/1000", 200..500),
        ("bytes=700-", "bytes 700-999/1000", 700..1000),
        ("bytes=-100", "bytes 900-999/1000", 900..1000),
        // Over-long ranges clamp to the end of the object, as on real S3.
        ("bytes=900-1999", "bytes 900-999/1000", 900..1000),
        ("bytes=-5000", "bytes 0-999/1000", 0..1000),
    ];
    for (range, content_range, span) in cases {
        let response = client
            .get(&url)
            .header("range", *range)
            .send()
            .await
            .expect("ranged GET");
        assert_eq!(response.status(), 206, "{range} should be partial content");
        assert_eq!(
            response
                .headers()
                .get("content-range")
                .expect("content-range"),
            content_range,
            "wrong Content-Range for {range}"
        );
        assert_eq!(
            response
                .headers()
                .get("content-length")
                .expect("content-length"),
            &span.len().to_string(),
            "wrong Content-Length for {range}"
        );
        let got = response.bytes().await.expect("body");
        assert_eq!(&got[..], &body[span.clone()], "wrong bytes for {range}");
    }
}

#[tokio::test]
async fn head_metadata_matches_get() {
    let base = serve_memory(&[("meta.bin", pattern(0, 12345), "text/plain")]).await;
    let url = format!("{base}/{BUCKET}/meta.bin");
    let client = reqwest::Client::new();

    let head = client.head(&url).send().await.expect("HEAD");
    let get = client.get(&url).send().await.expect("GET");
    assert_eq!(head.status(), 200);
    assert_eq!(get.status(), 200);
    for header in [
        "etag",
        "content-length",
        "content-type",
        "last-modified",
        "accept-ranges",
    ] {
        assert_eq!(
            head.headers().get(header),
            get.headers().get(header),
            "HEAD and GET disagree on {header}"
        );
        assert!(head.headers().contains_key(header), "{header} missing");
    }
}

#[tokio::test]
async fn missing_key_and_bucket_return_s3_error_xml() {
    let base = serve_memory(&[("exists.bin", pattern(0, 10), "text/plain")]).await;
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{base}/{BUCKET}/no-such-object"))
        .send()
        .await
        .expect("GET missing key");
    assert_eq!(response.status(), 404);
    let content_type = response
        .headers()
        .get("content-type")
        .expect("content-type")
        .to_str()
        .expect("ascii")
        .to_owned();
    assert!(
        content_type.contains("xml"),
        "error body must be XML, got {content_type}"
    );
    let body = response.text().await.expect("body");
    assert!(
        body.contains("<Code>NoSuchKey</Code>"),
        "expected NoSuchKey XML, got: {body}"
    );

    // Bucket-set serving (#235): this store serves a single bucket. A request
    // for a bucket outside the set is rejected with NoSuchBucket before the
    // origin is consulted — it never reads through for an unserved bucket.
    let response = client
        .get(format!("{base}/other-bucket/no-such-object"))
        .send()
        .await
        .expect("GET unconfigured bucket");
    assert_eq!(response.status(), 404);
    let body = response.text().await.expect("body");
    assert!(
        body.contains("<Code>NoSuchBucket</Code>"),
        "a bucket other than the served one must be NoSuchBucket, got: {body}"
    );

    // HEAD has no body to carry XML; the status still must be 404.
    let response = client
        .head(format!("{base}/{BUCKET}/no-such-object"))
        .send()
        .await
        .expect("HEAD missing key");
    assert_eq!(response.status(), 404);
}

/// [`ObjectRead`] that synthesizes its body chunk-by-chunk on demand and
/// counts every byte it has produced, so a test can prove the front-end
/// never runs ahead of the consumer (i.e. never buffers the object).
struct SyntheticRead {
    /// Total object size.
    size: u64,
    /// Bytes handed to the front-end so far.
    produced: Arc<AtomicU64>,
}

/// Chunk size the synthetic body yields.
const SYNTH_CHUNK: u64 = 1024 * 1024;

impl ObjectRead for SyntheticRead {
    /// Serves the full object as a lazy chunk stream; each chunk is only
    /// generated (and counted) when the transport actually polls for it.
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

    /// Metadata for the synthetic object.
    async fn head(&self, _key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        Ok(self.meta())
    }
}

impl SyntheticRead {
    /// Whole-object metadata shared by `get` and `head`.
    fn meta(&self) -> ObjectMeta {
        ObjectMeta {
            size: self.size,
            e_tag: Some("\"synthetic\"".to_owned()),
            last_modified: Some(std::time::SystemTime::UNIX_EPOCH),
            content_type: None,
            ..ObjectMeta::default()
        }
    }
}

/// Bounded-memory proof, pragmatic version: serve a 64 MiB object from a
/// producer that counts bytes generated, consume the HTTP response chunk by
/// chunk, and assert the producer never runs more than a fixed slack ahead
/// of the consumer. The slack (16 MiB) covers hyper/axum write buffers plus
/// kernel loopback socket buffers, which autotune to several MiB on macOS
/// (~8.7 MiB observed); if the front-end collected the body anywhere, the
/// producer would be the full 64 MiB ahead on the first chunk and the
/// assertion would fail immediately.
#[tokio::test]
async fn streaming_body_stays_bounded_ahead_of_consumer() {
    const SIZE: u64 = 64 * 1024 * 1024;
    const SLACK: u64 = 16 * 1024 * 1024;

    let produced = Arc::new(AtomicU64::new(0));
    let base = serve(verglas_s3::router(
        "default",
        SyntheticRead {
            size: SIZE,
            produced: Arc::clone(&produced),
        },
        PassthroughWrite::new(BackendStore::single(
            "default",
            BUCKET,
            Arc::new(InMemory::new()),
        )),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default",
            BUCKET,
            Arc::new(InMemory::new()),
        ))),
        Arc::new(NoopInvalidation),
        None,
    ))
    .await;

    let response = reqwest::get(format!("{base}/{BUCKET}/huge.bin"))
        .await
        .expect("GET");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-length")
            .expect("content-length"),
        &SIZE.to_string()
    );

    let mut consumed = 0u64;
    let mut produced_at_first_chunk = None;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("chunk");
        if produced_at_first_chunk.is_none() {
            produced_at_first_chunk = Some(produced.load(Ordering::SeqCst));
        }
        // Spot-check the pattern at the chunk edges; full byte equality is
        // covered by the round-trip test.
        if !chunk.is_empty() {
            assert_eq!(chunk[0], (consumed % 251) as u8, "bad byte at {consumed}");
            let last = consumed + chunk.len() as u64 - 1;
            assert_eq!(
                chunk[chunk.len() - 1],
                (last % 251) as u8,
                "bad byte at {last}"
            );
        }
        consumed += chunk.len() as u64;
        let lead = produced.load(Ordering::SeqCst).saturating_sub(consumed);
        assert!(
            lead <= SLACK,
            "producer ran {lead} bytes ahead of the consumer (> {SLACK}); the body is being buffered"
        );
    }
    assert_eq!(consumed, SIZE, "short body");
    assert_eq!(produced.load(Ordering::SeqCst), SIZE);
    // Sanity: production was still in flight when the first bytes arrived —
    // the response was not synthesized-then-sent as one buffer.
    let first = produced_at_first_chunk.expect("saw at least one chunk");
    assert!(
        first < SIZE,
        "producer finished all {SIZE} bytes before the first chunk was consumed"
    );
}
