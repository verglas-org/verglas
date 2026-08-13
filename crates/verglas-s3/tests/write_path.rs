//! Integration tests for the S3 write path (issue #9): put/delete/copy and
//! multipart round trips over the passthrough, S3 error XML for the write
//! surface, the #21 ordering invariant (backend-durable before ack,
//! invalidation before ack, nothing on failure), and bounded-memory
//! streaming for large PUT bodies — all against `object_store`'s in-memory
//! backend (or a synthetic writer), no network.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::{ObjectStoreExt, PutPayload, path::Path};
use verglas_core::CacheKey;
use verglas_s3::{
    BackendStore, CompletedPartRef, CopyOutcome, Invalidation, MultipartCreation, NoopInvalidation,
    ObjectWrite, PartInfo, PartUpload, PassthroughList, PassthroughRead, PassthroughWrite,
    PutOutcome, WriteBodyStream, WriteChecksum, WriteError, WriteMetadata,
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

/// Boots the passthrough front-end over an empty in-memory origin. No auth:
/// these tests exercise the write path.
async fn serve_memory() -> String {
    let store = Arc::new(InMemory::new());
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

/// Extracts the text of the first `<tag>...</tag>` in an XML body.
fn xml_tag(body: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body
        .find(&open)
        .unwrap_or_else(|| panic!("no <{tag}> in: {body}"))
        + open.len();
    let end = start + body[start..].find(&close).expect("closing tag");
    body[start..end].to_owned()
}

#[tokio::test]
async fn put_then_get_round_trips_bytes() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();

    // Small body: single atomic backend PUT. Large body: crosses the 8 MiB
    // part threshold, so it streams through the backend's multipart API.
    for (key, size) in [("small.bin", 100_000u64), ("large.bin", 20 * 1024 * 1024)] {
        let body = pattern(0, size);
        let url = format!("{base}/{BUCKET}/{key}");
        let response = client
            .put(&url)
            .header("content-type", "application/x-parquet")
            .body(body.clone())
            .send()
            .await
            .expect("PUT");
        assert_eq!(response.status(), 200, "PUT {key}");
        let put_etag = response
            .headers()
            .get("etag")
            .expect("PUT must return the backend ETag")
            .to_str()
            .expect("ascii")
            .to_owned();
        assert!(
            put_etag.starts_with('"') && put_etag.ends_with('"'),
            "unquoted ETag {put_etag}"
        );

        let response = client.get(&url).send().await.expect("GET");
        assert_eq!(response.status(), 200, "GET {key}");
        assert_eq!(
            response
                .headers()
                .get("etag")
                .expect("etag")
                .to_str()
                .expect("ascii"),
            put_etag,
            "GET ETag must match what the PUT acked"
        );
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .expect("content-type"),
            "application/x-parquet",
            "content type must round-trip"
        );
        let got = response.bytes().await.expect("body");
        assert_eq!(got, body, "round-tripped bytes differ for {key}");
    }
}

#[tokio::test]
async fn delete_then_get_returns_no_such_key() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/{BUCKET}/doomed.bin");

    let response = client
        .put(&url)
        .body(pattern(0, 1000))
        .send()
        .await
        .expect("PUT");
    assert_eq!(response.status(), 200);

    let response = client.delete(&url).send().await.expect("DELETE");
    assert_eq!(response.status(), 204);

    let response = client.get(&url).send().await.expect("GET after DELETE");
    assert_eq!(response.status(), 404);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>NoSuchKey</Code>"), "got: {text}");

    // Deleting a key that never existed is still success, as on S3.
    let response = client
        .delete(format!("{base}/{BUCKET}/never-existed"))
        .send()
        .await
        .expect("DELETE missing");
    assert_eq!(response.status(), 204);
}

#[tokio::test]
async fn batch_delete_removes_all_named_keys() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    for key in ["batch/a", "batch/b", "batch/keep"] {
        let response = client
            .put(format!("{base}/{BUCKET}/{key}"))
            .body(pattern(0, 100))
            .send()
            .await
            .expect("seed PUT");
        assert_eq!(response.status(), 200);
    }

    // Batch: two existing keys plus one that never existed (also success,
    // per S3 delete idempotency).
    let response = client
        .post(format!("{base}/{BUCKET}?delete"))
        .header("content-type", "application/xml")
        .body(
            "<Delete>\
               <Object><Key>batch/a</Key></Object>\
               <Object><Key>batch/b</Key></Object>\
               <Object><Key>batch/ghost</Key></Object>\
             </Delete>",
        )
        .send()
        .await
        .expect("POST ?delete");
    assert_eq!(response.status(), 200);
    let text = response.text().await.expect("body");
    for key in ["batch/a", "batch/b", "batch/ghost"] {
        assert!(
            text.contains(&format!("<Key>{key}</Key>")),
            "expected {key} in <Deleted> results, got: {text}"
        );
    }
    assert!(!text.contains("<Error>"), "unexpected errors: {text}");

    for (key, expected) in [("batch/a", 404), ("batch/b", 404), ("batch/keep", 200)] {
        let response = client
            .get(format!("{base}/{BUCKET}/{key}"))
            .send()
            .await
            .expect("GET after batch delete");
        assert_eq!(response.status(), expected, "{key}");
    }
}

#[tokio::test]
async fn copy_then_get_returns_source_bytes() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    let body = pattern(0, 50_000);

    let response = client
        .put(format!("{base}/{BUCKET}/src.bin"))
        .body(body.clone())
        .send()
        .await
        .expect("PUT source");
    assert_eq!(response.status(), 200);

    let response = client
        .put(format!("{base}/{BUCKET}/dest.bin"))
        .header("x-amz-copy-source", format!("{BUCKET}/src.bin"))
        .send()
        .await
        .expect("COPY");
    assert_eq!(response.status(), 200);
    let text = response.text().await.expect("body");
    assert!(
        text.contains("CopyObjectResult") && text.contains("<ETag>"),
        "expected CopyObjectResult XML with ETag, got: {text}"
    );

    let response = client
        .get(format!("{base}/{BUCKET}/dest.bin"))
        .send()
        .await
        .expect("GET dest");
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.expect("body"), body);

    // Copying a missing source is NoSuchKey.
    let response = client
        .put(format!("{base}/{BUCKET}/dest2.bin"))
        .header("x-amz-copy-source", format!("{BUCKET}/no-such-source"))
        .send()
        .await
        .expect("COPY missing source");
    assert_eq!(response.status(), 404);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>NoSuchKey</Code>"), "got: {text}");
}

#[tokio::test]
async fn cross_bucket_copy_is_rejected_not_supported() {
    // Same-bucket copy only: origin clients are bucket-scoped, so a copy whose
    // source bucket differs from the destination bucket is refused with a clear
    // not-supported error (NotImplemented, 501) — never silently done wrong.
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    let body = pattern(0, 1_000);

    let response = client
        .put(format!("{base}/{BUCKET}/src.bin"))
        .body(body.clone())
        .send()
        .await
        .expect("PUT source");
    assert_eq!(response.status(), 200);

    // Destination bucket differs from the copy-source bucket.
    let response = client
        .put(format!("{base}/other-bucket/dest.bin"))
        .header("x-amz-copy-source", format!("{BUCKET}/src.bin"))
        .send()
        .await
        .expect("cross-bucket COPY");
    assert_eq!(response.status(), 501);
    let text = response.text().await.expect("body");
    assert!(
        text.contains("<Code>NotImplemented</Code>"),
        "expected NotImplemented for a cross-bucket copy, got: {text}"
    );
}

#[tokio::test]
async fn multipart_lifecycle_assembles_exact_bytes() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/{BUCKET}/mp/table.parquet");

    // Create: the upload ID comes from the backend and round-trips verbatim.
    let response = client
        .post(format!("{url}?uploads"))
        .send()
        .await
        .expect("CreateMultipartUpload");
    assert_eq!(response.status(), 200);
    let text = response.text().await.expect("body");
    let upload_id = xml_tag(&text, "UploadId");
    assert!(!upload_id.is_empty());

    // Two parts with distinct contents; sizes chosen so a swapped or
    // truncated part cannot go unnoticed.
    let part1 = pattern(0, 6 * 1024 * 1024);
    let part2 = pattern(6 * 1024 * 1024, 1_234_567);
    let mut etags = Vec::new();
    for (number, part) in [(1, part1.clone()), (2, part2.clone())] {
        let response = client
            .put(format!("{url}?partNumber={number}&uploadId={upload_id}"))
            .body(part)
            .send()
            .await
            .expect("UploadPart");
        assert_eq!(response.status(), 200, "part {number}");
        etags.push(
            response
                .headers()
                .get("etag")
                .expect("part ETag")
                .to_str()
                .expect("ascii")
                .to_owned(),
        );
    }

    // ListParts reflects what was uploaded, ascending.
    let response = client
        .get(format!("{url}?uploadId={upload_id}"))
        .send()
        .await
        .expect("ListParts");
    assert_eq!(response.status(), 200);
    let text = response.text().await.expect("body");
    assert!(text.contains("<PartNumber>1</PartNumber>"), "got: {text}");
    assert!(text.contains("<PartNumber>2</PartNumber>"), "got: {text}");
    assert!(
        text.contains(&format!("<Size>{}</Size>", part1.len())),
        "part 1 size missing: {text}"
    );

    // Not visible until completed.
    let response = client.get(&url).send().await.expect("GET before complete");
    assert_eq!(response.status(), 404);

    // Complete with the client-held manifest; the assembled object must be
    // byte-identical to the concatenated parts.
    let manifest = format!(
        "<CompleteMultipartUpload>\
           <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\
           <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part>\
         </CompleteMultipartUpload>",
        etags[0], etags[1]
    );
    let response = client
        .post(format!("{url}?uploadId={upload_id}"))
        .header("content-type", "application/xml")
        .body(manifest)
        .send()
        .await
        .expect("CompleteMultipartUpload");
    assert_eq!(response.status(), 200);
    let text = response.text().await.expect("body");
    assert!(
        text.contains("CompleteMultipartUploadResult"),
        "got: {text}"
    );

    let response = client.get(&url).send().await.expect("GET after complete");
    assert_eq!(response.status(), 200);
    let got = response.bytes().await.expect("body");
    let mut expected = part1.to_vec();
    expected.extend_from_slice(&part2);
    assert_eq!(got.len(), expected.len(), "assembled size differs");
    assert_eq!(&got[..], &expected[..], "assembled bytes differ");

    // The completed upload ID is gone: ListParts against it is NoSuchUpload.
    let response = client
        .get(format!("{url}?uploadId={upload_id}"))
        .send()
        .await
        .expect("ListParts after complete");
    assert_eq!(response.status(), 404);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>NoSuchUpload</Code>"), "got: {text}");
}

#[tokio::test]
async fn abort_discards_parts_and_forgets_the_upload() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/{BUCKET}/mp/aborted.bin");

    let response = client
        .post(format!("{url}?uploads"))
        .send()
        .await
        .expect("CreateMultipartUpload");
    let upload_id = xml_tag(&response.text().await.expect("body"), "UploadId");

    let response = client
        .put(format!("{url}?partNumber=1&uploadId={upload_id}"))
        .body(pattern(0, 100_000))
        .send()
        .await
        .expect("UploadPart");
    let etag = response
        .headers()
        .get("etag")
        .expect("part ETag")
        .to_str()
        .expect("ascii")
        .to_owned();

    let response = client
        .delete(format!("{url}?uploadId={upload_id}"))
        .send()
        .await
        .expect("AbortMultipartUpload");
    assert_eq!(response.status(), 204);

    // The object never became visible.
    let response = client.get(&url).send().await.expect("GET after abort");
    assert_eq!(response.status(), 404);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>NoSuchKey</Code>"), "got: {text}");

    // The parts are gone with the upload: listing and completing are both
    // NoSuchUpload now.
    let response = client
        .get(format!("{url}?uploadId={upload_id}"))
        .send()
        .await
        .expect("ListParts after abort");
    assert_eq!(response.status(), 404);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>NoSuchUpload</Code>"), "got: {text}");

    let response = client
        .post(format!("{url}?uploadId={upload_id}"))
        .header("content-type", "application/xml")
        .body(format!(
            "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>\
             </CompleteMultipartUpload>"
        ))
        .send()
        .await
        .expect("Complete after abort");
    assert_eq!(response.status(), 404);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>NoSuchUpload</Code>"), "got: {text}");
}

#[tokio::test]
async fn wrong_upload_id_returns_no_such_upload_xml() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/{BUCKET}/mp/bogus.bin");

    let response = client
        .put(format!("{url}?partNumber=1&uploadId=not-a-real-upload"))
        .body(pattern(0, 100))
        .send()
        .await
        .expect("UploadPart with bogus ID");
    assert_eq!(response.status(), 404);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>NoSuchUpload</Code>"), "got: {text}");
}

/// Recording [`Invalidation`]: remembers every key it was called with, so
/// tests can assert exactly when the hook fired.
#[derive(Default)]
struct RecordingInvalidation {
    /// Keys from every `invalidate` call, in order.
    calls: Mutex<Vec<CacheKey>>,
}

#[async_trait::async_trait]
impl Invalidation for RecordingInvalidation {
    /// Records the keys and succeeds.
    async fn invalidate(&self, keys: &[CacheKey]) -> Result<(), String> {
        self.calls
            .lock()
            .expect("recording lock")
            .extend_from_slice(keys);
        Ok(())
    }
}

/// [`ObjectWrite`] whose every operation fails at the backend — the fake for
/// proving nothing acks (and nothing invalidates) when the backend refuses a
/// write. Bodies are drained first, mirroring a backend that dies mid-PUT.
struct FailingWrite;

impl FailingWrite {
    /// The one error every operation returns.
    fn refuse<T>() -> Result<T, WriteError> {
        Err(WriteError::Backend("backend is down".to_owned()))
    }
}

impl ObjectWrite for FailingWrite {
    /// Consumes the body, then refuses.
    async fn put(
        &self,
        _key: &CacheKey,
        _metadata: WriteMetadata,
        mut body: WriteBodyStream,
    ) -> Result<PutOutcome, WriteError> {
        while let Some(chunk) = body.next().await {
            chunk?;
        }
        Self::refuse()
    }

    /// Refuses.
    async fn delete(&self, _key: &CacheKey) -> Result<(), WriteError> {
        Self::refuse()
    }

    /// Refuses.
    async fn delete_batch(
        &self,
        _keys: &[CacheKey],
    ) -> Result<Vec<Result<(), WriteError>>, WriteError> {
        Self::refuse()
    }

    /// Refuses.
    async fn copy(
        &self,
        _source: &CacheKey,
        _dest: &CacheKey,
        _metadata: Option<WriteMetadata>,
    ) -> Result<CopyOutcome, WriteError> {
        Self::refuse()
    }

    /// Refuses.
    async fn create_multipart(
        &self,
        _key: &CacheKey,
        _metadata: WriteMetadata,
    ) -> Result<MultipartCreation, WriteError> {
        Self::refuse()
    }

    /// Refuses.
    async fn upload_part(
        &self,
        _key: &CacheKey,
        _upload_id: &str,
        _part_number: u16,
        _checksum: WriteChecksum,
        _body: WriteBodyStream,
    ) -> Result<PartUpload, WriteError> {
        Self::refuse()
    }

    /// Refuses.
    async fn complete_multipart(
        &self,
        _key: &CacheKey,
        _upload_id: &str,
        _parts: Vec<CompletedPartRef>,
        _object_checksum: WriteChecksum,
    ) -> Result<PutOutcome, WriteError> {
        Self::refuse()
    }

    /// Refuses.
    async fn abort_multipart(&self, _key: &CacheKey, _upload_id: &str) -> Result<(), WriteError> {
        Self::refuse()
    }

    /// Refuses.
    async fn list_parts(
        &self,
        _key: &CacheKey,
        _upload_id: &str,
    ) -> Result<Vec<PartInfo>, WriteError> {
        Self::refuse()
    }
}

/// The #21 ordering invariant, failure half: when the backend refuses the
/// PUT, the client gets an error (no ack) and the invalidation hook never
/// fires — the backend still holds the old bytes, so cached state is still
/// valid and must not be dropped.
#[tokio::test]
async fn failed_backend_put_yields_error_and_no_invalidation() {
    let recording = Arc::new(RecordingInvalidation::default());
    let store = Arc::new(InMemory::new());
    let base = serve(verglas_s3::router(
        "default",
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        FailingWrite,
        Arc::new(PassthroughList::new(BackendStore::single(
            "default", BUCKET, store,
        ))),
        recording.clone(),
        None,
    ))
    .await;

    let response = reqwest::Client::new()
        .put(format!("{base}/{BUCKET}/unlucky.bin"))
        .body(pattern(0, 10_000))
        .send()
        .await
        .expect("PUT against failing backend");
    assert_eq!(response.status(), 500, "client must not receive an ack");
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>InternalError</Code>"), "got: {text}");
    assert!(
        recording.calls.lock().expect("recording lock").is_empty(),
        "invalidation must not fire for a write the backend refused"
    );
}

/// The #21 ordering invariant, success half: a durable PUT invalidates
/// exactly the written key before the client sees the ack.
#[tokio::test]
async fn successful_put_invalidates_exactly_the_written_key() {
    let recording = Arc::new(RecordingInvalidation::default());
    let store = Arc::new(InMemory::new());
    let base = serve(verglas_s3::router(
        "default",
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single("default", BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default", BUCKET, store,
        ))),
        recording.clone(),
        None,
    ))
    .await;

    let response = reqwest::Client::new()
        .put(format!("{base}/{BUCKET}/lucky.bin"))
        .body(pattern(0, 10_000))
        .send()
        .await
        .expect("PUT");
    assert_eq!(response.status(), 200);
    // The ack has been received, so the invalidation (ordered before it)
    // must already be visible.
    assert_eq!(
        recording.calls.lock().expect("recording lock").as_slice(),
        &[CacheKey {
            storage_binding_id: "default".to_owned(),
            bucket: BUCKET.to_owned(),
            key: "lucky.bin".to_owned(),
        }],
        "exactly the written key must have been invalidated before the ack"
    );
}

/// Invalidation is ordered before the ack: if it fails, the client must get
/// an error even though the backend write went through — acking would let a
/// post-ack read hit a stale cache entry.
struct FailingInvalidation;

#[async_trait::async_trait]
impl Invalidation for FailingInvalidation {
    /// Always fails.
    async fn invalidate(&self, _keys: &[CacheKey]) -> Result<(), String> {
        Err("invalidation broker unreachable".to_owned())
    }
}

#[tokio::test]
async fn failed_invalidation_blocks_the_ack() {
    let store = Arc::new(InMemory::new());
    let base = serve(verglas_s3::router(
        "default",
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single("default", BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default", BUCKET, store,
        ))),
        Arc::new(FailingInvalidation),
        None,
    ))
    .await;

    let response = reqwest::Client::new()
        .put(format!("{base}/{BUCKET}/blocked.bin"))
        .body(pattern(0, 1000))
        .send()
        .await
        .expect("PUT with failing invalidation");
    assert_eq!(
        response.status(),
        500,
        "a write whose invalidation failed must not be acked"
    );
}

/// [`ObjectWrite`] that drains PUT bodies chunk by chunk, tracking how many
/// bytes it has consumed and how far the producer (the HTTP client) had run
/// ahead at each step — the mirror of the read path's counting producer.
struct CountingWrite {
    /// Bytes the client-side producer has generated so far.
    produced: Arc<AtomicU64>,
    /// Bytes this writer has consumed off the request body.
    consumed: Arc<AtomicU64>,
    /// Largest observed producer lead (produced - consumed) in bytes.
    max_lead: Arc<AtomicU64>,
}

impl ObjectWrite for CountingWrite {
    /// Drains the body one chunk at a time, recording the producer's lead
    /// after every chunk; never accumulates the bytes.
    async fn put(
        &self,
        _key: &CacheKey,
        _metadata: WriteMetadata,
        mut body: WriteBodyStream,
    ) -> Result<PutOutcome, WriteError> {
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            let consumed = self
                .consumed
                .fetch_add(chunk.len() as u64, Ordering::SeqCst)
                + chunk.len() as u64;
            let lead = self
                .produced
                .load(Ordering::SeqCst)
                .saturating_sub(consumed);
            self.max_lead.fetch_max(lead, Ordering::SeqCst);
        }
        Ok(PutOutcome {
            e_tag: Some("\"counting\"".to_owned()),
            ..PutOutcome::default()
        })
    }

    /// Unused in this test.
    async fn delete(&self, _key: &CacheKey) -> Result<(), WriteError> {
        FailingWrite::refuse()
    }

    /// Unused in this test.
    async fn delete_batch(
        &self,
        _keys: &[CacheKey],
    ) -> Result<Vec<Result<(), WriteError>>, WriteError> {
        FailingWrite::refuse()
    }

    /// Unused in this test.
    async fn copy(
        &self,
        _source: &CacheKey,
        _dest: &CacheKey,
        _metadata: Option<WriteMetadata>,
    ) -> Result<CopyOutcome, WriteError> {
        FailingWrite::refuse()
    }

    /// Unused in this test.
    async fn create_multipart(
        &self,
        _key: &CacheKey,
        _metadata: WriteMetadata,
    ) -> Result<MultipartCreation, WriteError> {
        FailingWrite::refuse()
    }

    /// Unused in this test.
    async fn upload_part(
        &self,
        _key: &CacheKey,
        _upload_id: &str,
        _part_number: u16,
        _checksum: WriteChecksum,
        _body: WriteBodyStream,
    ) -> Result<PartUpload, WriteError> {
        FailingWrite::refuse()
    }

    /// Unused in this test.
    async fn complete_multipart(
        &self,
        _key: &CacheKey,
        _upload_id: &str,
        _parts: Vec<CompletedPartRef>,
        _object_checksum: WriteChecksum,
    ) -> Result<PutOutcome, WriteError> {
        FailingWrite::refuse()
    }

    /// Unused in this test.
    async fn abort_multipart(&self, _key: &CacheKey, _upload_id: &str) -> Result<(), WriteError> {
        FailingWrite::refuse()
    }

    /// Unused in this test.
    async fn list_parts(
        &self,
        _key: &CacheKey,
        _upload_id: &str,
    ) -> Result<Vec<PartInfo>, WriteError> {
        FailingWrite::refuse()
    }
}

/// Chunk size the synthetic client body yields.
const SYNTH_CHUNK: u64 = 1024 * 1024;

/// Bounded-memory proof for PUT, the reverse of the read path's test: a
/// 64 MiB body is produced lazily on the client side (counting every byte
/// generated) and consumed chunk by chunk at the interface; the producer may
/// never run more than a fixed slack ahead of the interface's consumption. The
/// slack (16 MiB) covers client write buffers plus kernel loopback socket
/// buffers; if the front-end collected the body anywhere before handing it
/// to the writer, the producer would run the full 64 MiB ahead and the
/// assertion would fail.
#[tokio::test]
async fn streaming_put_body_stays_bounded_ahead_of_the_writer() {
    const SIZE: u64 = 64 * 1024 * 1024;
    const SLACK: u64 = 16 * 1024 * 1024;

    let produced = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let max_lead = Arc::new(AtomicU64::new(0));
    let base = serve(verglas_s3::router(
        "default",
        PassthroughRead::new(BackendStore::single(
            "default",
            BUCKET,
            Arc::new(InMemory::new()),
        )),
        CountingWrite {
            produced: Arc::clone(&produced),
            consumed: Arc::clone(&consumed),
            max_lead: Arc::clone(&max_lead),
        },
        Arc::new(PassthroughList::new(BackendStore::single(
            "default",
            BUCKET,
            Arc::new(InMemory::new()),
        ))),
        Arc::new(NoopInvalidation),
        None,
    ))
    .await;

    let producer = Arc::clone(&produced);
    let body_stream = futures::stream::unfold(0u64, move |offset| {
        let producer = Arc::clone(&producer);
        async move {
            if offset >= SIZE {
                return None;
            }
            let len = SYNTH_CHUNK.min(SIZE - offset);
            producer.fetch_add(len, Ordering::SeqCst);
            Some((
                Ok::<Bytes, std::io::Error>(pattern(offset, len)),
                offset + len,
            ))
        }
    });

    let response = reqwest::Client::new()
        .put(format!("{base}/{BUCKET}/huge-upload.bin"))
        .header("content-length", SIZE)
        .body(reqwest::Body::wrap_stream(body_stream))
        .send()
        .await
        .expect("streaming PUT");
    assert_eq!(response.status(), 200);
    assert_eq!(
        consumed.load(Ordering::SeqCst),
        SIZE,
        "writer must consume the whole body"
    );
    assert_eq!(produced.load(Ordering::SeqCst), SIZE);
    let lead = max_lead.load(Ordering::SeqCst);
    assert!(
        lead <= SLACK,
        "producer ran {lead} bytes ahead of the writer (> {SLACK}); the body is being buffered"
    );
}

// --- #21 ordering, extended to every mutating op ------------------------------
//
// The success/failure ordering (durable → invalidate → ack) is proven for PUT
// above; DELETE, CopyObject, and CompleteMultipartUpload run the identical
// sequence in the front-end, so each gets the same two guards: a backend
// failure must ack nothing and invalidate nothing, and an invalidation failure
// must block the ack even though the backend write already landed. (The
// randomized cross-op property that a *post-ack* read never sees pre-write
// bytes lives in tests/write_ordering.rs.)

/// Boots the front-end with the real passthrough writer over `store` and the
/// given invalidation hook (reader/lister are passthrough over the same store),
/// so a durable write genuinely reaches the backend before invalidation runs.
async fn serve_with(store: Arc<InMemory>, invalidation: Arc<dyn Invalidation>) -> String {
    serve(verglas_s3::router(
        "default",
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single("default", BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default", BUCKET, store,
        ))),
        invalidation,
        None,
    ))
    .await
}

/// Writes `bytes` straight into the backend store, bypassing the front-end —
/// so a copy source can exist without the front-end's invalidation having run.
async fn seed_store(store: &InMemory, key: &str, bytes: Bytes) {
    store
        .put(&Path::from(key), PutPayload::from(bytes))
        .await
        .expect("seed store");
}

/// Creates a one-part multipart upload for `key` over HTTP and returns
/// `(upload_id, part_etag)` ready for a completion manifest.
async fn create_and_upload_one_part(
    client: &reqwest::Client,
    base: &str,
    key: &str,
    body: Bytes,
) -> (String, String) {
    let url = format!("{base}/{BUCKET}/{key}");
    let response = client
        .post(format!("{url}?uploads"))
        .send()
        .await
        .expect("CreateMultipartUpload");
    assert_eq!(response.status(), 200);
    let upload_id = xml_tag(&response.text().await.expect("body"), "UploadId");
    let response = client
        .put(format!("{url}?partNumber=1&uploadId={upload_id}"))
        .body(body)
        .send()
        .await
        .expect("UploadPart");
    assert_eq!(response.status(), 200);
    let e_tag = response
        .headers()
        .get("etag")
        .expect("part ETag")
        .to_str()
        .expect("ascii")
        .to_owned();
    (upload_id, e_tag)
}

/// The single written key, as the recording hook sees it.
fn only_key(key: &str) -> Vec<CacheKey> {
    vec![CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: BUCKET.to_owned(),
        key: key.to_owned(),
    }]
}

/// DELETE, backend-failure half: a refused delete acks nothing and invalidates
/// nothing (the backend still holds the object, so cached state is still valid).
#[tokio::test]
async fn failed_backend_delete_yields_error_and_no_invalidation() {
    let recording = Arc::new(RecordingInvalidation::default());
    let store = Arc::new(InMemory::new());
    let base = serve(verglas_s3::router(
        "default",
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        FailingWrite,
        Arc::new(PassthroughList::new(BackendStore::single(
            "default", BUCKET, store,
        ))),
        recording.clone(),
        None,
    ))
    .await;

    let response = reqwest::Client::new()
        .delete(format!("{base}/{BUCKET}/doomed.bin"))
        .send()
        .await
        .expect("DELETE against failing backend");
    assert_eq!(response.status(), 500, "client must not receive an ack");
    assert!(
        recording.calls.lock().expect("lock").is_empty(),
        "invalidation must not fire for a delete the backend refused"
    );
}

/// CopyObject, backend-failure half: a refused copy acks nothing and
/// invalidates nothing.
#[tokio::test]
async fn failed_backend_copy_yields_error_and_no_invalidation() {
    let recording = Arc::new(RecordingInvalidation::default());
    let store = Arc::new(InMemory::new());
    let base = serve(verglas_s3::router(
        "default",
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        FailingWrite,
        Arc::new(PassthroughList::new(BackendStore::single(
            "default", BUCKET, store,
        ))),
        recording.clone(),
        None,
    ))
    .await;

    let response = reqwest::Client::new()
        .put(format!("{base}/{BUCKET}/dest.bin"))
        .header("x-amz-copy-source", format!("{BUCKET}/src.bin"))
        .send()
        .await
        .expect("COPY against failing backend");
    assert_eq!(response.status(), 500, "client must not receive an ack");
    assert!(
        recording.calls.lock().expect("lock").is_empty(),
        "invalidation must not fire for a copy the backend refused"
    );
}

/// DELETE, invalidation-failure half: the backend delete succeeds (deleting a
/// missing key is idempotent success), but a failed invalidation must still
/// block the ack — otherwise a post-ack read could hit a stale entry.
#[tokio::test]
async fn failed_invalidation_blocks_the_ack_on_delete() {
    let store = Arc::new(InMemory::new());
    let base = serve_with(store, Arc::new(FailingInvalidation)).await;

    let response = reqwest::Client::new()
        .delete(format!("{base}/{BUCKET}/gone.bin"))
        .send()
        .await
        .expect("DELETE with failing invalidation");
    assert_eq!(
        response.status(),
        500,
        "a delete whose invalidation failed must not be acked"
    );
}

/// CopyObject, invalidation-failure half: the backend copy lands, but a failed
/// invalidation blocks the ack.
#[tokio::test]
async fn failed_invalidation_blocks_the_ack_on_copy() {
    let store = Arc::new(InMemory::new());
    seed_store(&store, "src.bin", pattern(0, 4_000)).await;
    let base = serve_with(store, Arc::new(FailingInvalidation)).await;

    let response = reqwest::Client::new()
        .put(format!("{base}/{BUCKET}/dest.bin"))
        .header("x-amz-copy-source", format!("{BUCKET}/src.bin"))
        .send()
        .await
        .expect("COPY with failing invalidation");
    assert_eq!(
        response.status(),
        500,
        "a copy whose invalidation failed must not be acked"
    );
}

/// CompleteMultipartUpload, invalidation-failure half: the object is assembled
/// and durable at the backend, but a failed invalidation blocks the ack — the
/// completion is the moment a multipart object becomes visible, so it is a
/// write for ordering purposes.
#[tokio::test]
async fn failed_invalidation_blocks_the_ack_on_complete_multipart() {
    let store = Arc::new(InMemory::new());
    let base = serve_with(store, Arc::new(FailingInvalidation)).await;
    let client = reqwest::Client::new();
    let (upload_id, e_tag) =
        create_and_upload_one_part(&client, &base, "mp/table.parquet", pattern(0, 100_000)).await;

    let response = client
        .post(format!(
            "{base}/{BUCKET}/mp/table.parquet?uploadId={upload_id}"
        ))
        .header("content-type", "application/xml")
        .body(format!(
            "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>{e_tag}</ETag></Part>\
             </CompleteMultipartUpload>"
        ))
        .send()
        .await
        .expect("Complete with failing invalidation");
    assert_eq!(
        response.status(),
        500,
        "a completed multipart whose invalidation failed must not be acked"
    );
}

/// DELETE, success half: a durable delete invalidates exactly the deleted key
/// before the client sees the 204.
#[tokio::test]
async fn successful_delete_invalidates_the_key() {
    let recording = Arc::new(RecordingInvalidation::default());
    let store = Arc::new(InMemory::new());
    let base = serve_with(store, recording.clone()).await;

    let response = reqwest::Client::new()
        .delete(format!("{base}/{BUCKET}/target.bin"))
        .send()
        .await
        .expect("DELETE");
    assert_eq!(response.status(), 204);
    assert_eq!(
        recording.calls.lock().expect("lock").as_slice(),
        only_key("target.bin").as_slice(),
        "exactly the deleted key must be invalidated before the ack"
    );
}

/// CopyObject, success half: a durable copy invalidates the destination key
/// (and only the destination — the source is unchanged) before the ack.
#[tokio::test]
async fn successful_copy_invalidates_only_the_destination() {
    let recording = Arc::new(RecordingInvalidation::default());
    let store = Arc::new(InMemory::new());
    seed_store(&store, "src.bin", pattern(0, 4_000)).await;
    let base = serve_with(store, recording.clone()).await;

    let response = reqwest::Client::new()
        .put(format!("{base}/{BUCKET}/dest.bin"))
        .header("x-amz-copy-source", format!("{BUCKET}/src.bin"))
        .send()
        .await
        .expect("COPY");
    assert_eq!(response.status(), 200);
    assert_eq!(
        recording.calls.lock().expect("lock").as_slice(),
        only_key("dest.bin").as_slice(),
        "only the copy destination must be invalidated before the ack"
    );
}

/// CompleteMultipartUpload, success half: create and upload_part fire no
/// invalidation (nothing is visible yet); the completion invalidates exactly
/// the assembled key before the ack.
#[tokio::test]
async fn successful_complete_multipart_invalidates_the_key() {
    let recording = Arc::new(RecordingInvalidation::default());
    let store = Arc::new(InMemory::new());
    let base = serve_with(store, recording.clone()).await;
    let client = reqwest::Client::new();
    let (upload_id, e_tag) =
        create_and_upload_one_part(&client, &base, "mp/done.parquet", pattern(0, 100_000)).await;
    assert!(
        recording.calls.lock().expect("lock").is_empty(),
        "create/upload must not invalidate — nothing is visible before completion"
    );

    let response = client
        .post(format!(
            "{base}/{BUCKET}/mp/done.parquet?uploadId={upload_id}"
        ))
        .header("content-type", "application/xml")
        .body(format!(
            "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>{e_tag}</ETag></Part>\
             </CompleteMultipartUpload>"
        ))
        .send()
        .await
        .expect("CompleteMultipartUpload");
    assert_eq!(response.status(), 200);
    assert_eq!(
        recording.calls.lock().expect("lock").as_slice(),
        only_key("mp/done.parquet").as_slice(),
        "exactly the completed key must be invalidated before the ack"
    );
}

// --- #143 object metadata / #146 error classification / #155 edge validation --
//
// These exercise the front-end + passthrough end-to-end over the in-memory
// origin (which round-trips the same `Attribute` set real S3 does), so the
// metadata channel, the CopyObject MetadataDirective, and the edge validations
// are proven without a network. The origin-error *classification* half of #146
// (a wrong part ETag / too-small part) is a unit test in the passthrough module
// itself — the in-memory store cannot synthesize those origin codes.

use base64::Engine as _;
use md5::Digest as _;

/// One response header as an owned `String`, or `None` when absent.
fn header(response: &reqwest::Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .map(|v| v.to_str().expect("ascii header").to_owned())
}

/// #143: user metadata and the system headers a PUT carries must round-trip
/// on both GET and HEAD — before the fix only Content-Type survived.
#[tokio::test]
async fn put_object_metadata_round_trips_on_get_and_head() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/{BUCKET}/meta.bin");
    let response = client
        .put(&url)
        .header("content-type", "text/plain")
        .header("cache-control", "max-age=42")
        .header("content-encoding", "gzip")
        .header("content-disposition", "attachment; filename=x")
        .header("content-language", "en-US")
        .header("x-amz-meta-alpha", "one")
        .header("x-amz-meta-beta", "two")
        .body(pattern(0, 1000))
        .send()
        .await
        .expect("PUT with metadata");
    assert_eq!(response.status(), 200);

    for method in ["GET", "HEAD"] {
        let response = if method == "GET" {
            client.get(&url).send().await
        } else {
            client.head(&url).send().await
        }
        .expect("read back");
        assert_eq!(response.status(), 200, "{method}");
        assert_eq!(
            header(&response, "content-type").as_deref(),
            Some("text/plain"),
            "{method} content-type"
        );
        assert_eq!(
            header(&response, "cache-control").as_deref(),
            Some("max-age=42"),
            "{method} cache-control"
        );
        assert_eq!(
            header(&response, "content-encoding").as_deref(),
            Some("gzip"),
            "{method} content-encoding"
        );
        assert_eq!(
            header(&response, "content-disposition").as_deref(),
            Some("attachment; filename=x"),
            "{method} content-disposition"
        );
        assert_eq!(
            header(&response, "content-language").as_deref(),
            Some("en-US"),
            "{method} content-language"
        );
        assert_eq!(
            header(&response, "x-amz-meta-alpha").as_deref(),
            Some("one"),
            "{method} meta alpha"
        );
        assert_eq!(
            header(&response, "x-amz-meta-beta").as_deref(),
            Some("two"),
            "{method} meta beta"
        );
    }
}

/// #143: a plain CopyObject (default `MetadataDirective: COPY`) retains the
/// source object's stored metadata on the destination.
#[tokio::test]
async fn copy_object_retains_metadata_by_default() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    client
        .put(format!("{base}/{BUCKET}/src"))
        .header("content-type", "audio/ogg")
        .header("x-amz-meta-key1", "value1")
        .body(pattern(0, 3))
        .send()
        .await
        .expect("PUT src");

    let response = client
        .put(format!("{base}/{BUCKET}/dst"))
        .header("x-amz-copy-source", format!("{BUCKET}/src"))
        .send()
        .await
        .expect("COPY");
    assert_eq!(response.status(), 200);

    let response = client
        .get(format!("{base}/{BUCKET}/dst"))
        .send()
        .await
        .expect("GET dst");
    assert_eq!(
        header(&response, "content-type").as_deref(),
        Some("audio/ogg")
    );
    assert_eq!(
        header(&response, "x-amz-meta-key1").as_deref(),
        Some("value1")
    );
}

/// #143: CopyObject with `MetadataDirective: REPLACE` writes the request's
/// metadata onto the destination instead of the source's.
#[tokio::test]
async fn copy_object_replaces_metadata_with_directive() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    client
        .put(format!("{base}/{BUCKET}/src"))
        .header("content-type", "audio/ogg")
        .header("x-amz-meta-old", "gone")
        .body(pattern(0, 4096))
        .send()
        .await
        .expect("PUT src");

    let response = client
        .put(format!("{base}/{BUCKET}/dst"))
        .header("x-amz-copy-source", format!("{BUCKET}/src"))
        .header("x-amz-metadata-directive", "REPLACE")
        .header("content-type", "audio/mpeg")
        .header("x-amz-meta-new", "fresh")
        .send()
        .await
        .expect("COPY REPLACE");
    assert_eq!(response.status(), 200);

    let response = client
        .get(format!("{base}/{BUCKET}/dst"))
        .send()
        .await
        .expect("GET dst");
    assert_eq!(
        header(&response, "content-type").as_deref(),
        Some("audio/mpeg")
    );
    assert_eq!(
        header(&response, "x-amz-meta-new").as_deref(),
        Some("fresh")
    );
    assert_eq!(
        header(&response, "x-amz-meta-old"),
        None,
        "the source's metadata must not survive a REPLACE copy"
    );
    // Content is still the source's bytes.
    assert_eq!(response.bytes().await.expect("body").len(), 4096);
}

/// #146: copying an object onto itself without replacing metadata is
/// `InvalidRequest` (400), not the origin's generic 500.
#[tokio::test]
async fn copy_object_to_itself_without_replace_is_invalid_request() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    client
        .put(format!("{base}/{BUCKET}/self"))
        .body(pattern(0, 3))
        .send()
        .await
        .expect("PUT");

    let response = client
        .put(format!("{base}/{BUCKET}/self"))
        .header("x-amz-copy-source", format!("{BUCKET}/self"))
        .send()
        .await
        .expect("COPY to self");
    assert_eq!(response.status(), 400);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>InvalidRequest</Code>"), "got: {text}");
}

/// #143/#146: copying an object onto itself *is* allowed with a REPLACE
/// metadata change, and the new metadata takes effect.
#[tokio::test]
async fn copy_object_to_itself_with_replace_metadata_succeeds() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    client
        .put(format!("{base}/{BUCKET}/self"))
        .body(pattern(0, 3))
        .send()
        .await
        .expect("PUT");

    let response = client
        .put(format!("{base}/{BUCKET}/self"))
        .header("x-amz-copy-source", format!("{BUCKET}/self"))
        .header("x-amz-metadata-directive", "REPLACE")
        .header("x-amz-meta-foo", "bar")
        .send()
        .await
        .expect("COPY to self REPLACE");
    assert_eq!(response.status(), 200);

    let response = client
        .head(format!("{base}/{BUCKET}/self"))
        .send()
        .await
        .expect("HEAD self");
    assert_eq!(header(&response, "x-amz-meta-foo").as_deref(), Some("bar"));
}

/// #143: multipart metadata (set at CreateMultipartUpload) survives to the
/// completed object.
#[tokio::test]
async fn create_multipart_metadata_round_trips_after_complete() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/{BUCKET}/mp/meta.bin");

    let response = client
        .post(format!("{url}?uploads"))
        .header("content-type", "application/x-parquet")
        .header("x-amz-meta-run", "42")
        .send()
        .await
        .expect("CreateMultipartUpload");
    let upload_id = xml_tag(&response.text().await.expect("body"), "UploadId");

    let response = client
        .put(format!("{url}?partNumber=1&uploadId={upload_id}"))
        .body(pattern(0, 100_000))
        .send()
        .await
        .expect("UploadPart");
    let e_tag = response
        .headers()
        .get("etag")
        .expect("part ETag")
        .to_str()
        .expect("ascii")
        .to_owned();

    let response = client
        .post(format!("{url}?uploadId={upload_id}"))
        .header("content-type", "application/xml")
        .body(format!(
            "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>{e_tag}</ETag></Part>\
             </CompleteMultipartUpload>"
        ))
        .send()
        .await
        .expect("Complete");
    assert_eq!(response.status(), 200);

    let response = client.head(&url).send().await.expect("HEAD");
    assert_eq!(
        header(&response, "content-type").as_deref(),
        Some("application/x-parquet")
    );
    assert_eq!(header(&response, "x-amz-meta-run").as_deref(), Some("42"));
}

/// #155: a `Content-MD5` matching the body is accepted; a malformed one is
/// `InvalidDigest`; a well-formed one that does not match the body is
/// `BadDigest`.
#[tokio::test]
async fn content_md5_is_validated_at_the_edge() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/{BUCKET}/digest.bin");
    let body = pattern(0, 1024);
    let good = base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(&body));

    // Correct digest: accepted.
    let response = client
        .put(&url)
        .header("content-md5", &good)
        .body(body.clone())
        .send()
        .await
        .expect("PUT good md5");
    assert_eq!(
        response.status(),
        200,
        "correct Content-MD5 must be accepted"
    );

    // Well-formed 128-bit digest that does not match: BadDigest.
    let wrong = base64::engine::general_purpose::STANDARD.encode([0u8; 16]);
    let response = client
        .put(&url)
        .header("content-md5", &wrong)
        .body(body.clone())
        .send()
        .await
        .expect("PUT wrong md5");
    assert_eq!(response.status(), 400);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>BadDigest</Code>"), "got: {text}");

    // Not a 128-bit digest (11 bytes): InvalidDigest.
    let response = client
        .put(&url)
        .header("content-md5", "YWJyYWNhZGFicmE=")
        .body(body.clone())
        .send()
        .await
        .expect("PUT short md5");
    assert_eq!(response.status(), 400);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>InvalidDigest</Code>"), "got: {text}");
    // (An *empty* Content-MD5 is also InvalidDigest — decoding "" yields zero
    // bytes, not a 128-bit digest — but reqwest drops an empty-valued header
    // before it reaches the server, so that case is covered by the conformance
    // suite's boto client rather than here.)
}

/// #155: a DeleteObjects batch over the 1000-key cap is rejected at the edge
/// as MalformedXML (400), rather than delegated to the origin.
#[tokio::test]
async fn delete_objects_over_the_key_cap_is_malformed_xml() {
    let base = serve_memory().await;
    let client = reqwest::Client::new();

    let mut body = String::from("<Delete>");
    for i in 0..1001 {
        body.push_str(&format!("<Object><Key>k{i}</Key></Object>"));
    }
    body.push_str("</Delete>");

    let response = client
        .post(format!("{base}/{BUCKET}?delete"))
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .expect("DeleteObjects over cap");
    assert_eq!(response.status(), 400);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>MalformedXML</Code>"), "got: {text}");

    // Exactly 1000 keys is still accepted.
    let mut body = String::from("<Delete>");
    for i in 0..1000 {
        body.push_str(&format!("<Object><Key>k{i}</Key></Object>"));
    }
    body.push_str("</Delete>");
    let response = client
        .post(format!("{base}/{BUCKET}?delete"))
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .expect("DeleteObjects at cap");
    assert_eq!(
        response.status(),
        200,
        "a batch of exactly 1000 keys is legal"
    );
}
