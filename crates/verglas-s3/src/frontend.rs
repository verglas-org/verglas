//! The s3s protocol layer: implements the read surface
//! (`GetObject`/`HeadObject`) on top of the [`ObjectRead`] interface and the write
//! surface (`PutObject`, `DeleteObject`(s), `CopyObject`, the multipart
//! lifecycle) on top of the [`ObjectWrite`] interface, exposing the whole thing as
//! an axum router the server can bind to `listen.s3_port`. s3s owns request
//! parsing, error XML rendering, and status codes; this module owns the
//! mapping between S3 semantics and the interfaces — including the write path's
//! ordering invariant: backend durable, then invalidation, then (and only
//! then) the client ack.

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::error_handling::HandleError;
use axum::middleware::map_response;
use futures::StreamExt;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Checksum, ChecksumAlgorithm,
    ChecksumType, CommonPrefix, CompleteMultipartUploadInput, CompleteMultipartUploadOutput,
    CopyObjectInput, CopyObjectOutput, CopyObjectResult, CopyPartResult, CopySource,
    CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteObjectInput, DeleteObjectOutput,
    DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, EncodingType, GetObjectAttributesInput,
    GetObjectAttributesOutput, GetObjectAttributesParts, GetObjectInput, GetObjectOutput,
    HeadObjectInput, HeadObjectOutput, ListBucketsInput, ListBucketsOutput,
    ListMultipartUploadsInput, ListMultipartUploadsOutput, ListObjectsInput, ListObjectsOutput,
    ListObjectsV2Input, ListObjectsV2Output, ListPartsInput, ListPartsOutput, MetadataDirective,
    MultipartUpload, Object, ObjectPart, ObjectStorageClass, Owner, Part, PutObjectInput,
    PutObjectOutput, Range as DtoRange, StorageClass, StreamingBlob, Timestamp,
    UploadPartCopyInput, UploadPartCopyOutput, UploadPartInput, UploadPartOutput,
};
use s3s::service::S3ServiceBuilder;
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use verglas_core::CacheKey;

use crate::auth::StaticAuth;
use crate::cache_key_for;
use crate::middleware::{UnquoteAttributesEtag, unquote_attributes_etag};
use crate::preconditions::{Outcome, Preconditions, evaluate};
use verglas_core::list::{ListError, ListObject, ListPage, ListRequest, ObjectList};
use verglas_core::metrics::{DEFAULT_TENANT, NodeMetrics, RequestOp};
use verglas_core::read::{
    AttributesRequest, BodyStream, Checksums, DirectGet, DirectMeta, DirectReadOptions,
    ObjectAttributes, ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange, ServedTier,
    TierCell,
};
use verglas_core::trace::RequestId;
use verglas_core::write::{
    CompletedPartRef, Invalidation, MultipartUploadsRequest, ObjectWrite, WriteBodyStream,
    WriteChecksum, WriteError, WriteMetadata,
};

/// The S3 front-end: a thin protocol adapter over an [`ObjectRead`] source
/// and an [`ObjectWrite`] sink. Today both are the passthrough; issue #12
/// swaps a cache in for reads without this type changing, while writes stay
/// passthrough (read-through-only rule) and only ever touch the cache via
/// the [`Invalidation`] hook.
pub struct VerglasS3<R, W> {
    /// Immutable storage binding assigned to this database endpoint.
    storage_binding_id: String,
    /// Where bytes and metadata come from.
    reader: R,
    /// Where writes go — always through to the backend bucket.
    writer: W,
    /// Where listings come from — always direct to the backend bucket, never
    /// cached (issue #8's freshness rule), so it is a separate interface from the
    /// (cacheable) reader rather than another `ObjectRead` method.
    lister: Arc<dyn ObjectList>,
    /// Fired with the affected keys after a write is backend-durable and
    /// before the client is acked (the hook #12/#14 fill in).
    invalidation: Arc<dyn Invalidation>,
    /// Node-level Prometheus metrics (#46): each request records its duration
    /// and outcome here. `None` in tests that do not exercise metrics; the
    /// server always wires one.
    metrics: Option<Arc<NodeMetrics>>,
    /// Bounds live S3 response bytes while allowing small Parquet ranges much
    /// more concurrency than large response bodies (#254).
    response_body_budget: ResponseBodyBudget,
}

impl<R: ObjectRead, W: ObjectWrite> VerglasS3<R, W> {
    /// Wraps a byte source, a write sink, and a listing source in the protocol
    /// adapter, with no metrics wired (unit-test path).
    pub fn new(
        storage_binding_id: impl Into<String>,
        reader: R,
        writer: W,
        lister: Arc<dyn ObjectList>,
        invalidation: Arc<dyn Invalidation>,
    ) -> Self {
        VerglasS3 {
            storage_binding_id: storage_binding_id.into(),
            reader,
            writer,
            lister,
            invalidation,
            metrics: None,
            response_body_budget: ResponseBodyBudget::production(),
        }
    }

    /// Like [`VerglasS3::new`] but records request metrics into `metrics` (#46) —
    /// the server path.
    pub fn with_metrics(
        storage_binding_id: impl Into<String>,
        reader: R,
        writer: W,
        lister: Arc<dyn ObjectList>,
        invalidation: Arc<dyn Invalidation>,
        metrics: Arc<NodeMetrics>,
    ) -> Self {
        VerglasS3 {
            storage_binding_id: storage_binding_id.into(),
            reader,
            writer,
            lister,
            invalidation,
            metrics: Some(metrics),
            response_body_budget: ResponseBodyBudget::production(),
        }
    }

    /// Records one completed request against the node metrics, if wired (#46).
    /// `status` is the HTTP status the response carries; `tier` is the serving
    /// tier (meaningful for GET, `Passthrough` for the metadata/write ops that
    /// never touch a cache tier); `started` bounds the duration.
    fn record(&self, op: RequestOp, tier: ServedTier, status: u16, started: Instant) {
        let elapsed = started.elapsed();
        if let Some(metrics) = &self.metrics {
            metrics.observe_request(op, tier, status, DEFAULT_TENANT, elapsed.as_secs_f64());
        }
        emit_completion(op, tier, status, elapsed);
    }

    /// Runs the invalidation hook for `keys`. A failure blocks the client
    /// ack: the request errors instead, because acking a write whose stale
    /// cache entries might still be served would break issue #21's
    /// invariant (post-ack reads never see pre-write bytes).
    async fn invalidate(&self, keys: &[CacheKey]) -> S3Result<()> {
        self.invalidation.invalidate(keys).await.map_err(|message| {
            S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("cache invalidation failed: {message}"),
            )
        })
    }
}

/// Total response-body budget charged in 64 KiB units: 64 MiB. This is separate
/// from the cache and origin-fill budgets; it bounds only live socket egress.
const RESPONSE_BODY_BUDGET_UNITS: usize = 1024;

/// Granularity of the live-response budget. Parquet footer and column ranges
/// below this size pay one unit instead of one full-response slot.
const RESPONSE_BODY_BUDGET_QUANTUM_BYTES: u64 = 64 * 1024;

/// Maximum charge for one response: 4 MiB. Sixteen large responses may stream
/// together, preserving #254's proven safe worst case, while smaller ranges use
/// only their proportional share of the 64 MiB total budget.
const MAX_RESPONSE_BODY_CHARGE_UNITS: u32 = 64;

/// Shared weighted permits for response bodies actively streaming to clients.
/// Tokio's semaphore is FIFO-fair; charging by declared response length lets
/// high-fan-out small ranges proceed together without allowing large bodies to
/// accumulate unbounded socket work.
#[derive(Clone)]
struct ResponseBodyBudget {
    /// Budget units retained by active response body streams.
    units: Arc<Semaphore>,
    /// Number of declared response bytes represented by one permit.
    quantum_bytes: u64,
    /// Largest permit charge one response may retain.
    max_units_per_response: u32,
}

impl ResponseBodyBudget {
    /// Creates the production egress budget.
    fn production() -> Self {
        Self::new(
            RESPONSE_BODY_BUDGET_UNITS,
            RESPONSE_BODY_BUDGET_QUANTUM_BYTES,
            MAX_RESPONSE_BODY_CHARGE_UNITS,
        )
    }

    /// Creates a weighted budget from permit count, byte quantum, and per-body
    /// cap. Callers provide non-zero values; the private production constants
    /// and unit tests enforce that invariant.
    fn new(total_units: usize, quantum_bytes: u64, max_units_per_response: u32) -> Self {
        debug_assert!(total_units > 0);
        debug_assert!(quantum_bytes > 0);
        debug_assert!(max_units_per_response > 0);
        debug_assert!(max_units_per_response as usize <= total_units);
        Self {
            units: Arc::new(Semaphore::new(total_units)),
            quantum_bytes,
            max_units_per_response,
        }
    }

    /// Computes the permit charge for a declared response length, rounding a
    /// partial quantum upward and capping very large bodies.
    fn charge_units(&self, content_length: u64) -> u32 {
        let rounded = content_length.saturating_add(self.quantum_bytes - 1) / self.quantum_bytes;
        let bounded = rounded.max(1).min(u64::from(self.max_units_per_response));
        u32::try_from(bounded).unwrap_or(self.max_units_per_response)
    }

    /// Waits for the weighted permit charge for one declared response. The
    /// semaphore is private and never closed; closure remains an S3 error if
    /// that invariant is ever broken.
    async fn acquire_for(&self, content_length: u64) -> S3Result<OwnedSemaphorePermit> {
        self.units
            .clone()
            .acquire_many_owned(self.charge_units(content_length))
            .await
            .map_err(|_| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    "response body budget semaphore was unexpectedly closed",
                )
            })
    }

    /// Reports the uncharged budget units. This exists solely for the weighted
    /// budget regression tests.
    #[cfg(test)]
    fn available(&self) -> usize {
        self.units.available_permits()
    }
}

/// Retains a response's weighted egress permit for the lifetime of `body`,
/// including a client disconnect. The stream stays pull-based and unchanged;
/// this adds no per-chunk allocation, lock, copy, or aggregation.
fn retain_response_budget(body: BodyStream, permit: OwnedSemaphorePermit) -> BodyStream {
    futures::stream::unfold((body, permit), |(mut body, permit)| async move {
        body.next().await.map(|chunk| (chunk, (body, permit)))
    })
    .boxed()
}

/// Emits the single INFO completion line for one request (#61): op, key tier,
/// HTTP status, and duration, tagged with the current request id so the line
/// correlates with the front-end span, the peer hop, and any fill events. One
/// event per request; the fields are cheap and the macro compiles to a level
/// check when INFO is disabled, so the hot path pays nothing beyond that check.
/// The object key is never logged, only its serving tier — key redaction (#61).
fn emit_completion(op: RequestOp, tier: ServedTier, status: u16, elapsed: Duration) {
    tracing::info!(
        request_id = verglas_core::trace::current().as_ref().map(|r| r.as_str()),
        op = op.label(),
        tier = tier.label(),
        status,
        duration_ms = elapsed.as_secs_f64() * 1_000.0,
        "s3 request complete"
    );
}

/// Wraps a GET body stream so `on_done` runs exactly once when the stream ends
/// (or is dropped) — the point at which the request has actually served its
/// bytes and the serving tier (#46) is known. `metrics` is optional so the
/// no-metrics test path adds no wrapper cost; when absent the body is returned
/// untouched. The closure sees the [`TierCell`], read after the body drains.
fn observe_on_body_end(
    body: BodyStream,
    metrics: Option<Arc<NodeMetrics>>,
    op: RequestOp,
    tier: TierCell,
    status: u16,
    started: Instant,
) -> BodyStream {
    let Some(metrics) = metrics else {
        return body;
    };
    // The state carries a guard that fires on drop, so a client disconnect
    // mid-stream still records the request (with the tier resolved so far).
    // `request_id` is captured here, in the request future where the trace id is
    // still current — the guard's Drop can run later, off the request task, once
    // hyper finishes draining the body (#61).
    struct Guard {
        metrics: Arc<NodeMetrics>,
        op: RequestOp,
        tier: TierCell,
        status: u16,
        started: Instant,
        request_id: Option<RequestId>,
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            let tier = self.tier.get();
            let elapsed = self.started.elapsed();
            self.metrics.observe_request(
                self.op,
                tier,
                self.status,
                DEFAULT_TENANT,
                elapsed.as_secs_f64(),
            );
            // The completion line for a GET fires only once the body has
            // actually served its bytes and the tier is known — that is here,
            // not when the handler returns the stream.
            tracing::info!(
                request_id = self.request_id.as_ref().map(|r| r.as_str()),
                op = self.op.label(),
                tier = tier.label(),
                status = self.status,
                duration_ms = elapsed.as_secs_f64() * 1_000.0,
                "s3 request complete"
            );
        }
    }
    let guard = Guard {
        metrics,
        op,
        tier,
        status,
        started,
        request_id: verglas_core::trace::current(),
    };
    futures::stream::unfold((body, guard), |(mut body, guard)| async move {
        // The guard lives in the unfold state: when the stream ends or is
        // dropped, the state drops and the guard's Drop records the request.
        body.next().await.map(|chunk| (chunk, (body, guard)))
    })
    .boxed()
}

/// Translates s3s's parsed `Range` header into the interface's range form.
fn to_read_range(range: Option<DtoRange>) -> ReadRange {
    match range {
        None => ReadRange::Full,
        Some(DtoRange::Int { first, last: None }) => ReadRange::From(first),
        Some(DtoRange::Int {
            first,
            last: Some(last),
        }) => ReadRange::Bounded(first, last),
        Some(DtoRange::Suffix { length }) => ReadRange::Suffix(length),
    }
}

/// Maps read-interface errors onto the exact S3 error codes (and therefore status
/// codes and XML bodies, both rendered by s3s).
fn to_s3_error(error: ReadError) -> S3Error {
    match error {
        ReadError::NoSuchBucket => S3Error::with_message(
            S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        ),
        ReadError::AccessDenied => {
            S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied")
        }
        ReadError::NoSuchKey => {
            S3Error::with_message(S3ErrorCode::NoSuchKey, "The specified key does not exist.")
        }
        ReadError::InvalidRange => S3Error::with_message(
            S3ErrorCode::InvalidRange,
            "The requested range is not satisfiable",
        ),
        ReadError::InvalidPart => S3Error::with_message(
            S3ErrorCode::InvalidPart,
            "The requested part number is not valid for this object",
        ),
        ReadError::Backend(message) => S3Error::with_message(S3ErrorCode::InternalError, message),
    }
}

/// Maps list-interface errors onto the exact S3 error codes (and therefore status
/// codes and XML bodies, both rendered by s3s).
fn to_s3_list_error(error: ListError) -> S3Error {
    match error {
        ListError::NoSuchBucket => S3Error::with_message(
            S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        ),
        ListError::AccessDenied => {
            S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied")
        }
        ListError::Backend(message) => S3Error::with_message(S3ErrorCode::InternalError, message),
    }
}

/// S3's default and maximum page size for LIST. A request may ask for fewer,
/// but never more — real S3 caps at 1000 regardless of the requested value.
const MAX_LIST_KEYS: i32 = 1000;

/// S3's hard cap on the number of keys a single DeleteObjects request may name;
/// a larger batch is rejected as MalformedXML (issue #155).
const MAX_DELETE_KEYS: usize = 1000;

/// Clamps a client's `max-keys` into S3's `0..=1000`, defaulting to 1000 when
/// absent, so the interface always receives a sane ceiling.
fn clamp_max_keys(max_keys: Option<i32>) -> usize {
    max_keys.unwrap_or(MAX_LIST_KEYS).clamp(0, MAX_LIST_KEYS) as usize
}

/// Obfuscates a resume cursor (a real key) into a v2 continuation token. S3's
/// tokens are documented as opaque and "not a real key", so hex-encoding the
/// key keeps clients from depending on its contents while letting us decode it
/// back exactly — special characters and all — with no extra dependency.
fn encode_token(cursor: &str) -> String {
    let mut out = String::with_capacity(cursor.len() * 2);
    for byte in cursor.as_bytes() {
        out.push(char::from_digit((byte >> 4) as u32, 16).unwrap_or('0'));
        out.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap_or('0'));
    }
    out
}

/// Decodes a v2 continuation token produced by [`encode_token`] back into the
/// resume cursor. A malformed token (not our hex, or not valid UTF-8) is an
/// `InvalidArgument`, matching S3's rejection of a bad continuation token.
fn decode_token(token: &str) -> S3Result<String> {
    let bytes = token.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(invalid_token());
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16).ok_or_else(invalid_token)?;
        let lo = (pair[1] as char).to_digit(16).ok_or_else(invalid_token)?;
        out.push((hi << 4 | lo) as u8);
    }
    String::from_utf8(out).map_err(|_| invalid_token())
}

/// The error S3 returns for a continuation token it did not issue.
fn invalid_token() -> S3Error {
    S3Error::with_message(
        S3ErrorCode::InvalidArgument,
        "The continuation token provided is incorrect",
    )
}

/// Percent-encodes a listed value for `encoding-type=url` responses, matching
/// S3: everything except the RFC 3986 unreserved set and `/` is encoded. Only
/// invoked when the client asked for URL encoding, so unencoded responses keep
/// their keys verbatim.
fn url_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push(
                    char::from_digit((byte >> 4) as u32, 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
                out.push(
                    char::from_digit((byte & 0x0f) as u32, 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
            }
        }
    }
    out
}

/// Whether the request asked for `encoding-type=url`.
fn wants_url_encoding(encoding_type: &Option<EncodingType>) -> bool {
    encoding_type
        .as_ref()
        .is_some_and(|e| e.as_str() == EncodingType::URL)
}

/// Applies the response encoding to one value: URL-encoded when the client
/// asked, verbatim otherwise.
fn encode_value(value: String, url: bool) -> String {
    if url { url_encode(&value) } else { value }
}

/// The single static account Verglas serves. There is no per-object owner model
/// (issue #7's one-keypair auth), so a listing reports this fixed owner whenever
/// a v2 client asks for it (`fetch-owner=true`), matching S3's response shape.
const OWNER_ID: &str = "verglas";

/// Display name reported alongside [`OWNER_ID`] in the `<Owner>` element.
const OWNER_DISPLAY_NAME: &str = "verglas";

/// The fixed `<Owner>` element for the static account.
fn static_owner() -> Owner {
    Owner {
        id: Some(OWNER_ID.to_owned()),
        display_name: Some(OWNER_DISPLAY_NAME.to_owned()),
    }
}

/// Builds one `<Contents>` entry from a list-interface object, applying response
/// encoding to the key and normalizing the ETag to S3's quoted shape. A
/// storage class of `STANDARD` is reported (as S3 does) even though the origin
/// listing does not carry one. `with_owner` attaches the static `<Owner>` (v2
/// `fetch-owner=true`); it is omitted otherwise, as S3 does.
fn to_dto_object(object: ListObject, url: bool, with_owner: bool) -> Object {
    Object {
        key: Some(encode_value(object.key, url)),
        size: Some(object.size as i64),
        e_tag: object.e_tag.map(to_etag),
        last_modified: object.last_modified.map(Timestamp::from),
        storage_class: Some(ObjectStorageClass::from_static(
            ObjectStorageClass::STANDARD,
        )),
        owner: with_owner.then(static_owner),
        ..Object::default()
    }
}

/// Builds the `<CommonPrefixes>` list from the list interface's rolled-up prefixes,
/// applying response encoding. `None` when there are none, so the element is
/// omitted rather than emitted empty (matching S3).
fn to_common_prefixes(prefixes: Vec<String>, url: bool) -> Option<Vec<CommonPrefix>> {
    if prefixes.is_empty() {
        return None;
    }
    Some(
        prefixes
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(encode_value(prefix, url)),
            })
            .collect(),
    )
}

/// Maps write-interface errors onto the exact S3 error codes (and therefore
/// status codes and XML bodies, both rendered by s3s).
fn to_s3_write_error(error: WriteError) -> S3Error {
    match error {
        WriteError::NoSuchBucket => S3Error::with_message(
            S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        ),
        WriteError::AccessDenied => {
            S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied")
        }
        WriteError::CrossBucketCopy => S3Error::with_message(
            S3ErrorCode::NotImplemented,
            "Cross-bucket CopyObject is not supported; source and destination \
             must be the same bucket",
        ),
        WriteError::NoSuchKey => {
            S3Error::with_message(S3ErrorCode::NoSuchKey, "The specified key does not exist.")
        }
        WriteError::NoSuchUpload => S3Error::with_message(
            S3ErrorCode::NoSuchUpload,
            "The specified upload does not exist. The upload ID may be invalid, \
             or the upload may have been aborted or completed.",
        ),
        WriteError::InvalidPart(message) => {
            S3Error::with_message(S3ErrorCode::InvalidPart, message)
        }
        WriteError::InvalidRange => S3Error::with_message(
            S3ErrorCode::InvalidRange,
            "The requested range is not satisfiable",
        ),
        WriteError::EntityTooSmall(message) => {
            S3Error::with_message(S3ErrorCode::EntityTooSmall, message)
        }
        WriteError::InvalidRequest(message) => {
            S3Error::with_message(S3ErrorCode::InvalidRequest, message)
        }
        WriteError::Unsupported(message) => {
            S3Error::with_message(S3ErrorCode::NotImplemented, message)
        }
        WriteError::Body(message) => S3Error::with_message(
            S3ErrorCode::IncompleteBody,
            format!("The request body was not fully received: {message}"),
        ),
        WriteError::Backend(message) => S3Error::with_message(S3ErrorCode::InternalError, message),
    }
}

/// Decodes a client `Content-MD5` header into the 16 raw digest bytes it must
/// be. Real S3 rejects a header that is not valid base64, or that is not
/// exactly a 128-bit digest, with `InvalidDigest` (400) before the object is
/// written; Verglas validates it at the edge rather than delegating to the
/// origin (issue #155).
fn decode_content_md5(raw: &str) -> S3Result<[u8; 16]> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(raw.trim())
        .map_err(|_| invalid_digest())?;
    bytes.try_into().map_err(|_| invalid_digest())
}

/// The error S3 returns for a malformed `Content-MD5` header.
fn invalid_digest() -> S3Error {
    S3Error::with_message(
        S3ErrorCode::InvalidDigest,
        "The Content-MD5 you specified is not valid.",
    )
}

/// The error S3 returns when the body's MD5 does not match the client's
/// `Content-MD5`.
fn bad_digest() -> S3Error {
    S3Error::with_message(
        S3ErrorCode::BadDigest,
        "The Content-MD5 you specified did not match what we received.",
    )
}

/// Wraps a write body so its MD5 is computed incrementally as chunks flow to
/// the backend — bounded memory, no buffering — for `Content-MD5` validation.
/// The shared hasher is finalized after the write completes; a mismatch is
/// `BadDigest` (issue #155). Returns the wrapped stream and the handle to read
/// the digest from.
fn tee_md5(body: WriteBodyStream) -> (WriteBodyStream, std::sync::Arc<std::sync::Mutex<md5::Md5>>) {
    use md5::Digest;
    let hasher = std::sync::Arc::new(std::sync::Mutex::new(md5::Md5::new()));
    let sink = hasher.clone();
    let teed = body
        .map(move |chunk| {
            if let Ok(bytes) = &chunk {
                sink.lock().unwrap_or_else(|e| e.into_inner()).update(bytes);
            }
            chunk
        })
        .boxed();
    (teed, hasher)
}

/// Adapts an s3s request body into the interface's stream form. A missing body is
/// an empty object; nothing is buffered — chunks flow through as the client
/// sends them.
fn to_write_body(body: Option<StreamingBlob>) -> WriteBodyStream {
    match body {
        Some(blob) => blob
            .map(|chunk| chunk.map_err(|e| WriteError::Body(e.to_string())))
            .boxed(),
        None => futures::stream::empty().boxed(),
    }
}

/// Validates a wire part number into the interface's range (S3 allows 1..=10000).
fn to_part_number(raw: i32) -> S3Result<u16> {
    u16::try_from(raw)
        .ok()
        .filter(|n| (1..=10_000).contains(n))
        .ok_or_else(|| {
            S3Error::with_message(
                S3ErrorCode::InvalidArgument,
                "Part number must be an integer between 1 and 10000, inclusive",
            )
        })
}

/// Normalizes a backend entity tag for the wire. Real S3 reports ETags
/// already quoted; stores that report a bare value get strong-quoted so the
/// header always matches S3's shape.
fn to_etag(raw: String) -> s3s::dto::ETag {
    s3s::dto::ETag::from_str(&raw).unwrap_or(s3s::dto::ETag::Strong(raw))
}

/// Borrows the four conditional-request headers off a GET/HEAD input into the
/// [`Preconditions`] the evaluator reads. GET and HEAD carry an identical
/// condition-header set, so both handlers build it the same way.
macro_rules! preconditions_of {
    ($input:expr) => {
        Preconditions {
            if_match: $input.if_match.as_ref(),
            if_none_match: $input.if_none_match.as_ref(),
            if_modified_since: $input.if_modified_since.as_ref(),
            if_unmodified_since: $input.if_unmodified_since.as_ref(),
        }
    };
}

/// The `304 Not Modified` reply for a conditional read whose `If-None-Match` or
/// `If-Modified-Since` matched (issue #10). It carries the object's `ETag` so
/// the client can confirm the copy it holds; the body a 304 must not carry is
/// dropped by the response middleware. Modelled as an `S3Error` because that is
/// the only s3s path that honours a non-2xx status on a read.
fn not_modified_error(meta: &ObjectMeta) -> S3Error {
    let mut error = S3Error::with_message(S3ErrorCode::NotModified, "Not Modified");
    if let Some(raw) = &meta.e_tag
        && let Ok(value) = to_etag(raw.clone()).to_http_header()
    {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(axum::http::header::ETAG, value);
        error.set_headers(headers);
    }
    error
}

/// The `412 PreconditionFailed` reply for a conditional read whose `If-Match`
/// or `If-Unmodified-Since` failed (issue #10). No bytes are served.
fn precondition_failed_error() -> S3Error {
    S3Error::with_message(
        S3ErrorCode::PreconditionFailed,
        "At least one of the preconditions you specified did not hold.",
    )
}

/// The `Content-Range` header value for a served range: `bytes first-last/size`.
fn format_content_range(range: &std::ops::Range<u64>, size: u64) -> String {
    format!(
        "bytes {}-{}/{size}",
        range.start,
        range.end.saturating_sub(1)
    )
}

/// Copies whole-object metadata into the header fields shared by GET and HEAD
/// responses, keeping the two paths byte-for-byte consistent. A macro rather
/// than a function because `GetObjectOutput` and `HeadObjectOutput` are
/// distinct s3s types with the same field set; this writes every field an S3
/// read reports back, including the system headers and user metadata issue
/// #143 now threads through the write path.
macro_rules! apply_object_meta {
    ($output:expr, $meta:expr) => {{
        let meta: ObjectMeta = $meta;
        $output.e_tag = meta.e_tag.map(to_etag);
        $output.last_modified = meta.last_modified.map(Timestamp::from);
        $output.content_type = meta.content_type;
        $output.cache_control = meta.cache_control;
        $output.content_disposition = meta.content_disposition;
        $output.content_encoding = meta.content_encoding;
        $output.content_language = meta.content_language;
        $output.expires = meta.expires.as_deref().and_then(parse_http_date);
        $output.metadata =
            (!meta.user_metadata.is_empty()).then(|| meta.user_metadata.into_iter().collect());
    }};
}

/// Copies the forwarded origin checksums (issue #154) into a GET/HEAD output's
/// checksum fields. A macro because `GetObjectOutput` and `HeadObjectOutput`
/// are distinct s3s types with the same field set.
macro_rules! apply_checksums {
    ($output:expr, $checksums:expr) => {{
        let checksums: Checksums = $checksums;
        $output.checksum_crc32 = checksums.crc32;
        $output.checksum_crc32c = checksums.crc32c;
        $output.checksum_crc64nvme = checksums.crc64nvme;
        $output.checksum_sha1 = checksums.sha1;
        $output.checksum_sha256 = checksums.sha256;
        $output.checksum_type = checksums.checksum_type.map(ChecksumType::from);
    }};
}

/// Copies the forwarded origin checksums into an output that has the five
/// checksum value fields but NO `checksum_type` (issue #208): `UploadPartOutput`
/// reports the part's checksum without a type. A separate macro from
/// [`apply_checksums`] for exactly that reason.
macro_rules! apply_part_checksums {
    ($output:expr, $checksums:expr) => {{
        let checksums: Checksums = $checksums;
        $output.checksum_crc32 = checksums.crc32;
        $output.checksum_crc32c = checksums.crc32c;
        $output.checksum_crc64nvme = checksums.crc64nvme;
        $output.checksum_sha1 = checksums.sha1;
        $output.checksum_sha256 = checksums.sha256;
    }};
}

/// Builds the checksum request set (issue #154) from a PutObject's checksum
/// headers: the algorithm selector plus any precomputed value the client sent
/// for the origin to verify. Verglas forwards these and never computes one.
fn to_write_checksum(input: &PutObjectInput) -> WriteChecksum {
    WriteChecksum {
        algorithm: input
            .checksum_algorithm
            .as_ref()
            .map(|a| a.as_str().to_owned()),
        // A plain PutObject carries no checksum TYPE (that is a multipart
        // combination selector); leave it unset.
        checksum_type: None,
        crc32: input.checksum_crc32.clone(),
        crc32c: input.checksum_crc32c.clone(),
        crc64nvme: input.checksum_crc64nvme.clone(),
        sha1: input.checksum_sha1.clone(),
        sha256: input.checksum_sha256.clone(),
    }
}

/// Builds the passthrough (uncached) read options from a GET/HEAD's
/// version-ID, part-number, and checksum-mode inputs (issues #153/#154/#156).
/// A present part number is validated into S3's `1..=10000` range.
fn to_direct_read_options(
    version_id: Option<String>,
    part_number: Option<i32>,
    checksum_mode: Option<&s3s::dto::ChecksumMode>,
) -> S3Result<DirectReadOptions> {
    let part_number = match part_number {
        Some(raw) => Some(to_part_number(raw)?),
        None => None,
    };
    let checksum_mode = checksum_mode
        .map(|m| m.as_str().eq_ignore_ascii_case("ENABLED"))
        .unwrap_or(false);
    Ok(DirectReadOptions {
        version_id,
        part_number,
        checksum_mode,
    })
}

/// True when `etag` is a multipart ETag: `<hex>-<partcount>`. S3 encodes
/// multipart-ness in the ETag suffix, so this is how GetObjectAttributes decides
/// whether to report an `ObjectParts` section (issue #208). Tolerates
/// surrounding quotes; the value must have a `-` followed by at least one digit,
/// all digits to the end.
fn is_multipart_etag(etag: &str) -> bool {
    let value = etag.trim_matches('"');
    match value.rsplit_once('-') {
        Some((_, suffix)) => !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()),
        None => false,
    }
}

/// Converts the interface's forwarded checksums into an s3s `Checksum` block for
/// GetObjectAttributes (issue #154). `None` when the object carries none.
fn to_dto_checksum(checksums: Checksums) -> Option<Checksum> {
    if checksums.crc32.is_none()
        && checksums.crc32c.is_none()
        && checksums.crc64nvme.is_none()
        && checksums.sha1.is_none()
        && checksums.sha256.is_none()
    {
        return None;
    }
    Some(Checksum {
        checksum_crc32: checksums.crc32,
        checksum_crc32c: checksums.crc32c,
        checksum_crc64nvme: checksums.crc64nvme,
        checksum_sha1: checksums.sha1,
        checksum_sha256: checksums.sha256,
        checksum_type: checksums.checksum_type.map(ChecksumType::from),
    })
}

/// Formats an s3s [`Timestamp`] as the RFC 1123 HTTP-date string the `Expires`
/// header carries, so the raw write path can forward it verbatim (issue #189).
fn format_http_date(ts: Timestamp) -> Option<String> {
    let mut buf = Vec::new();
    ts.format(s3s::dto::TimestampFormat::HttpDate, &mut buf)
        .ok()?;
    String::from_utf8(buf).ok()
}

/// Parses an HTTP-date `Expires` string (as the origin stored and returned it)
/// back into an s3s [`Timestamp`] for the GET/HEAD response (issue #189). An
/// unparseable value is dropped rather than failing the read.
fn parse_http_date(raw: &str) -> Option<Timestamp> {
    Timestamp::parse(s3s::dto::TimestampFormat::HttpDate, raw).ok()
}

/// Assembles the write-path metadata (system headers + user `x-amz-meta-*`) an
/// S3 write request carries, so the backend stores it and a later GET/HEAD
/// reports it back (issue #143). Shared by PutObject, CreateMultipartUpload,
/// and CopyObject-with-REPLACE, which expose the same header set.
fn write_metadata(
    content_type: Option<String>,
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    expires: Option<Timestamp>,
    metadata: Option<s3s::dto::Metadata>,
) -> WriteMetadata {
    WriteMetadata {
        content_type,
        cache_control,
        content_disposition,
        content_encoding: strip_aws_chunked(content_encoding),
        content_language,
        expires: expires.and_then(format_http_date),
        // The checksum request headers (issue #154) are attached by the caller
        // that has them (PutObject); the shared metadata builder leaves them
        // empty so CreateMultipartUpload / CopyObject stay checksum-free.
        checksum: WriteChecksum::default(),
        user_metadata: metadata
            .map(|m| m.into_iter().collect())
            .unwrap_or_default(),
    }
}

/// Strips `aws-chunked` tokens from a `Content-Encoding` value, as S3 does:
/// `aws-chunked` is a transfer artifact of SigV4 streaming, not a content
/// coding, so it must never be stored or reported back (issue #143). Returns
/// `None` when nothing but `aws-chunked` remains.
fn strip_aws_chunked(encoding: Option<String>) -> Option<String> {
    let raw = encoding?;
    let kept: Vec<&str> = raw
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty() && !token.eq_ignore_ascii_case("aws-chunked"))
        .collect();
    if kept.is_empty() {
        None
    } else {
        Some(kept.join(", "))
    }
}

// ORDERING INVARIANT (issue #21) — load-bearing correctness code.
//
// Every mutating handler below follows the same sequence:
//   1. stream the client's bytes to the backend;
//   2. the backend confirms the write durable (the interface call returns `Ok` —
//      the `ObjectWrite` contract);
//   3. invalidate the affected logical keys;
//   4. only then ack the client (return `Ok`, which s3s renders as 2xx).
//
// Crash/failure windows and why each is safe:
//   - dying (or failing) before 2: the client got no ack and S3 writes are
//     atomic, so the backend and every cache entry still agree — safe;
//   - dying between 2 and 4: the backend has the new bytes but the client
//     got no ack (it must retry); the cache was either already invalidated
//     or is covered by the mutable-key TTL backstop (#14) — the same
//     argument that covers writes going direct to S3 behind our back;
//   - invalidation failing after 2: the request errors (no ack), landing in
//     the previous window.
//
// Never invalidate before the backend confirms (a concurrent read could
// re-fill the cache from pre-write backend state and then receive the ack's
// bytes late), and never ack before invalidation completes (a post-ack read
// could hit a stale entry). No write is ever admitted into the cache here —
// the cache fills on the read path only (issue #9's read-through-only rule).
#[async_trait::async_trait]
impl<R: ObjectRead, W: ObjectWrite> S3 for VerglasS3<R, W> {
    /// GetObject: full and ranged reads. A request with any `Range` header
    /// gets `Content-Range` set, which makes s3s respond `206 Partial
    /// Content`; the body is streamed straight off the interface, never buffered.
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        // Metrics (#46): time the whole request; the GET body observation fires
        // when the streamed body drains (its serving tier is known only then),
        // while the short-circuit error paths record at return.
        let started = Instant::now();
        let input = req.input;
        let key = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        let ranged = input.range.is_some();
        // 206 for a ranged read, 200 for a full read (the tier rides the body).
        let ok_status: u16 = if ranged { 206 } else { 200 };

        // Conditional requests (issue #10): when any condition header is
        // present, resolve the object's metadata through the read interface
        // FIRST and judge the preconditions on it. A 304/412 short-circuits
        // before any body GET, so no bytes are served; because `head` resolves
        // the key→ETag mapping the TTL-aware way (#14), a cached object costs
        // zero backend traffic here. Unconditional reads skip this entirely.
        let pre = preconditions_of!(input);
        if pre.present() {
            let meta = self.reader.head(&key).await.map_err(|e| {
                self.record(RequestOp::Get, ServedTier::Passthrough, 500, started);
                to_s3_error(e)
            })?;
            match evaluate(&pre, &meta) {
                Outcome::NotModified => {
                    self.record(RequestOp::Get, ServedTier::Dram, 304, started);
                    return Err(not_modified_error(&meta));
                }
                Outcome::Failed => {
                    self.record(RequestOp::Get, ServedTier::Dram, 412, started);
                    return Err(precondition_failed_error());
                }
                Outcome::Proceed => {}
            }
        }

        // A version ID, part number, or checksum mode (issues #153/#154/#156)
        // names an immutable, version- or part-scoped view the cache does not
        // model: forward it straight to the origin and echo the extras back.
        let options = to_direct_read_options(
            input.version_id,
            input.part_number,
            input.checksum_mode.as_ref(),
        )?;
        if options.is_direct() {
            let DirectGet {
                meta:
                    DirectMeta {
                        meta,
                        version_id,
                        parts_count,
                        checksums,
                    },
                range,
                body,
            } = self
                .reader
                .get_direct(&key, to_read_range(input.range), options)
                .await
                .map_err(to_s3_error)?;
            let content_length = range.end - range.start;
            let response_budget = self
                .response_body_budget
                .acquire_for(content_length)
                .await?;
            // A direct read is a straight-through-origin serve (#46).
            let tier = TierCell::new();
            tier.set(ServedTier::Passthrough);
            let body = retain_response_budget(
                observe_on_body_end(
                    body,
                    self.metrics.clone(),
                    RequestOp::Get,
                    tier,
                    ok_status,
                    started,
                ),
                response_budget,
            );
            let mut output = GetObjectOutput {
                accept_ranges: Some("bytes".to_owned()),
                content_length: Some(content_length as i64),
                content_range: ranged.then(|| format_content_range(&range, meta.size)),
                body: Some(StreamingBlob::wrap(sync_wrapper::SyncStream::new(body))),
                version_id,
                parts_count,
                ..GetObjectOutput::default()
            };
            apply_object_meta!(output, meta);
            apply_checksums!(output, checksums);
            return Ok(S3Response::new(output));
        }
        let ObjectGet {
            meta,
            range,
            body,
            served_from,
        } = self
            .reader
            .get(&key, to_read_range(input.range))
            .await
            .map_err(to_s3_error)?;
        let content_length = range.end - range.start;
        let response_budget = self
            .response_body_budget
            .acquire_for(content_length)
            .await?;

        // The serving tier is resolved as the body streams; observe when it ends.
        let body = retain_response_budget(
            observe_on_body_end(
                body,
                self.metrics.clone(),
                RequestOp::Get,
                served_from,
                ok_status,
                started,
            ),
            response_budget,
        );
        let mut output = GetObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            content_length: Some(content_length as i64),
            content_range: ranged.then(|| format_content_range(&range, meta.size)),
            // SyncStream only adds the `Sync` bound s3s wants; polling still
            // pulls chunks one at a time from the interface — no buffering.
            body: Some(StreamingBlob::wrap(sync_wrapper::SyncStream::new(body))),
            ..GetObjectOutput::default()
        };
        apply_object_meta!(output, meta);
        Ok(S3Response::new(output))
    }

    /// ListBuckets: returns an empty bucket list. The server serves a configured
    /// bucket set (a single bucket and/or glob patterns, #235) addressed
    /// directly; a glob set cannot be enumerated ahead of time, and the server
    /// does not enumerate the origin estate. Answering with an empty list (rather
    /// than a not-supported error) keeps clients that probe ListBuckets on
    /// connect working; enumerating the served set is a follow-up.
    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(Vec::new()),
            ..ListBucketsOutput::default()
        }))
    }

    /// ListObjectsV2: the modern listing dialect. `prefix`, `delimiter`,
    /// `continuation-token`, `start-after`, and `max-keys` all forward to the
    /// backend bucket (never cached — issue #8). The continuation token is our
    /// own obfuscation of the resume cursor; `start-after` is used only when no
    /// token is present, matching S3's precedence. Common prefixes, truncation,
    /// and `KeyCount` come straight off the interface page.
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let started = Instant::now();
        let input = req.input;
        let url = wants_url_encoding(&input.encoding_type);
        let max = clamp_max_keys(input.max_keys);
        // An empty `delimiter=` is no delimiter at all: S3 does no roll-up and
        // omits the `<Delimiter>` echo (issue #145).
        let delimiter = input.delimiter.clone().filter(|d| !d.is_empty());
        // A continuation token overrides start-after (S3's rule); decode it
        // back to the raw resume cursor.
        let start_after = match &input.continuation_token {
            Some(token) => Some(decode_token(token)?),
            None => input.start_after.clone(),
        };
        let request = ListRequest {
            storage_binding_id: self.storage_binding_id.clone(),
            bucket: input.bucket.clone(),
            prefix: input.prefix.clone().unwrap_or_default(),
            delimiter: delimiter.clone(),
            start_after,
            max_keys: max,
        };
        let page = self.lister.list(request).await.map_err(to_s3_list_error)?;
        // `max-keys=0` is a valid empty page and is never truncated, even when
        // the bucket has more keys (issue #145).
        let is_truncated = max != 0 && page.is_truncated;
        let key_count = (page.objects.len() + page.common_prefixes.len()) as i32;
        let next_continuation_token = is_truncated
            .then(|| page.next_start_after.as_deref().map(encode_token))
            .flatten();
        let with_owner = input.fetch_owner.unwrap_or(false);
        let ListPage {
            objects,
            common_prefixes,
            ..
        } = page;
        let response = S3Response::new(ListObjectsV2Output {
            name: Some(input.bucket),
            prefix: input.prefix.map(|p| encode_value(p, url)),
            delimiter: delimiter.map(|d| encode_value(d, url)),
            max_keys: Some(max as i32),
            key_count: Some(key_count),
            is_truncated: Some(is_truncated),
            continuation_token: input.continuation_token,
            next_continuation_token,
            start_after: input.start_after.map(|s| encode_value(s, url)),
            contents: (!objects.is_empty()).then(|| {
                objects
                    .into_iter()
                    .map(|o| to_dto_object(o, url, with_owner))
                    .collect()
            }),
            common_prefixes: to_common_prefixes(common_prefixes, url),
            encoding_type: url.then(|| EncodingType::from_static(EncodingType::URL)),
            ..ListObjectsV2Output::default()
        });
        // Listings always forward to the origin (never cached, #8).
        self.record(RequestOp::List, ServedTier::Passthrough, 200, started);
        Ok(response)
    }

    /// ListObjects (v1): the legacy dialect, still used by some clients. Same
    /// backend passthrough as v2, but paginated by `marker` — which is a plain
    /// key, not an obfuscated token. `NextMarker` is returned only when a
    /// delimiter is set (as S3 does); without one, clients resume from the last
    /// returned key, which equals our cursor.
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let started = Instant::now();
        let input = req.input;
        let url = wants_url_encoding(&input.encoding_type);
        let max = clamp_max_keys(input.max_keys);
        // An empty `delimiter=` is no delimiter at all (issue #145).
        let delimiter = input.delimiter.clone().filter(|d| !d.is_empty());
        let request = ListRequest {
            storage_binding_id: self.storage_binding_id.clone(),
            bucket: input.bucket.clone(),
            prefix: input.prefix.clone().unwrap_or_default(),
            delimiter: delimiter.clone(),
            start_after: input.marker.clone(),
            max_keys: max,
        };
        let page = self.lister.list(request).await.map_err(to_s3_list_error)?;
        // `max-keys=0` is a valid empty page and is never truncated (issue #145).
        let is_truncated = max != 0 && page.is_truncated;
        // S3 only emits NextMarker for delimited listings; otherwise the client
        // continues from the last key it received.
        let next_marker = is_truncated
            .then(|| page.next_start_after.clone())
            .flatten()
            .filter(|_| delimiter.is_some())
            .map(|m| encode_value(m, url));
        let ListPage {
            objects,
            common_prefixes,
            ..
        } = page;
        let response = S3Response::new(ListObjectsOutput {
            name: Some(input.bucket),
            // v1's top-level `<Prefix>` echo is returned verbatim: unlike v2,
            // botocore does not url-decode it, so a control-character prefix must
            // round-trip unencoded even under `encoding-type=url` (issue #145).
            prefix: input.prefix,
            delimiter: delimiter.map(|d| encode_value(d, url)),
            // S3 always includes `<Marker>` in a v1 response — the empty string
            // when the client sent none (issue #145).
            marker: Some(
                input
                    .marker
                    .map(|m| encode_value(m, url))
                    .unwrap_or_default(),
            ),
            max_keys: Some(max as i32),
            is_truncated: Some(is_truncated),
            next_marker,
            contents: (!objects.is_empty()).then(|| {
                objects
                    .into_iter()
                    .map(|o| to_dto_object(o, url, false))
                    .collect()
            }),
            common_prefixes: to_common_prefixes(common_prefixes, url),
            encoding_type: url.then(|| EncodingType::from_static(EncodingType::URL)),
            ..ListObjectsOutput::default()
        });
        self.record(RequestOp::List, ServedTier::Passthrough, 200, started);
        Ok(response)
    }

    /// HeadObject: metadata only, with the same header values a GET of the
    /// same object would carry. Ranged HEADs are served as whole-object
    /// HEADs for now — engines only range GETs.
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        // Metrics (#46): a HEAD has no streamed body, so it records at return.
        let started = Instant::now();
        let input = req.input;
        let key = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        // Version-ID / part-number / checksum-mode HEADs (issues #153/#154/#156)
        // go straight to the origin, exactly like the GET twin.
        let options = to_direct_read_options(
            input.version_id,
            input.part_number,
            input.checksum_mode.as_ref(),
        )?;
        if options.is_direct() {
            let DirectMeta {
                meta,
                version_id,
                parts_count,
                checksums,
            } = self
                .reader
                .head_direct(&key, options)
                .await
                .map_err(to_s3_error)?;
            let mut output = HeadObjectOutput {
                accept_ranges: Some("bytes".to_owned()),
                content_length: Some(meta.size as i64),
                version_id,
                parts_count,
                ..HeadObjectOutput::default()
            };
            apply_object_meta!(output, meta);
            apply_checksums!(output, checksums);
            self.record(RequestOp::Head, ServedTier::Passthrough, 200, started);
            return Ok(S3Response::new(output));
        }
        let meta = self.reader.head(&key).await.map_err(to_s3_error)?;

        // Conditional requests (issue #10): HEAD honours the same four
        // condition headers as GET, judged on the metadata already in hand, so
        // no extra backend traffic. A 304/412 short-circuits the response.
        let pre = preconditions_of!(input);
        if pre.present() {
            match evaluate(&pre, &meta) {
                Outcome::NotModified => {
                    self.record(RequestOp::Head, ServedTier::Dram, 304, started);
                    return Err(not_modified_error(&meta));
                }
                Outcome::Failed => {
                    self.record(RequestOp::Head, ServedTier::Dram, 412, started);
                    return Err(precondition_failed_error());
                }
                Outcome::Proceed => {}
            }
        }

        let mut output = HeadObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            content_length: Some(meta.size as i64),
            ..HeadObjectOutput::default()
        };
        apply_object_meta!(output, meta);
        // HEAD metadata is served warm from the pinned store / mapping cache.
        self.record(RequestOp::Head, ServedTier::Dram, 200, started);
        Ok(S3Response::new(output))
    }

    /// PutObject: the body streams through to the backend untouched. The
    /// backend's ETag is returned so the client's ETag matches later reads.
    /// See the ORDERING INVARIANT comment on this impl.
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let started = Instant::now();
        // s3s drops a present-but-empty header to `None`, but S3 still rejects
        // an empty `Content-MD5` as InvalidDigest — so consult the raw header.
        let md5_header_present = req.headers.contains_key("content-md5");
        let input = req.input;
        let key = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        // Content-MD5 (#155): reject a malformed digest before writing
        // (InvalidDigest); a well-formed digest is checked against the body
        // as it streams and enforced after the write (BadDigest below).
        let expected_md5 = match input.content_md5.as_deref() {
            Some(raw) => Some(decode_content_md5(raw)?),
            // Header present but s3s parsed nothing from it (empty value).
            None if md5_header_present => return Err(invalid_digest()),
            None => None,
        };
        // Capture the checksum request headers before the metadata builder
        // moves the rest of the input (issue #154).
        let checksum = to_write_checksum(&input);
        let mut metadata = write_metadata(
            input.content_type,
            input.cache_control,
            input.content_disposition,
            input.content_encoding,
            input.content_language,
            input.expires,
            input.metadata,
        );
        metadata.checksum = checksum;
        let (body, hasher) = match expected_md5 {
            Some(_) => {
                let (teed, hasher) = tee_md5(to_write_body(input.body));
                (teed, Some(hasher))
            }
            None => (to_write_body(input.body), None),
        };
        // 1+2: backend-durable before anything else.
        let outcome = self
            .writer
            .put(&key, metadata, body)
            .await
            .map_err(to_s3_write_error)?;
        // 3: invalidate; 4: ack. On a failed PUT we never reach here: S3
        // PUTs are atomic, so a failure leaves the backend (and any cache
        // entry) on the old bytes — nothing to invalidate, no ack. The
        // backend now holds the new bytes, so invalidate before the digest
        // verdict too: a BadDigest still changed backend state, and cached
        // state must not lag it (the bytes are the client's real body, never
        // corrupt — Verglas only reports that the *claimed* digest was wrong).
        self.invalidate(std::slice::from_ref(&key)).await?;
        if let (Some(expected), Some(hasher)) = (expected_md5, hasher) {
            use md5::Digest;
            let digest = hasher
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
                .finalize();
            if digest.as_slice() != expected {
                return Err(bad_digest());
            }
        }
        // Echo the origin's checksum (issue #154) so a client that asked the
        // origin to verify one sees it confirmed on the PUT response.
        let checksums = outcome.checksums;
        let mut output = PutObjectOutput {
            e_tag: outcome.e_tag.map(to_etag),
            version_id: outcome.version_id,
            ..PutObjectOutput::default()
        };
        apply_checksums!(output, checksums);
        // A write is always a straight-through-origin operation (#46).
        self.record(RequestOp::Put, ServedTier::Passthrough, 200, started);
        Ok(S3Response::new(output))
    }

    /// DeleteObject: forward, invalidate, ack (see the ORDERING INVARIANT
    /// comment). Deleting a missing key is success, as on S3 (204).
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        let key = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        self.writer.delete(&key).await.map_err(to_s3_write_error)?;
        self.invalidate(std::slice::from_ref(&key)).await?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    /// DeleteObjects (batch): forwards each delete and reports per-key
    /// results in the response body, exactly like S3 (the request itself
    /// succeeds even when individual keys fail). Every requested key is
    /// invalidated regardless of its per-key result: a "failed" delete may
    /// still have removed the object at the origin (e.g. a lost response),
    /// and over-invalidation is always safe.
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        // S3 caps a single DeleteObjects request at 1000 keys; a larger batch
        // is MalformedXML (400), enforced at the edge rather than delegated to
        // the origin (issue #155).
        if input.delete.objects.len() > MAX_DELETE_KEYS {
            return Err(S3Error::with_message(
                S3ErrorCode::MalformedXML,
                "The XML you provided was not well-formed or did not validate \
                 against our published schema",
            ));
        }
        let quiet = input.delete.quiet.unwrap_or(false);
        let keys: Vec<CacheKey> = input
            .delete
            .objects
            .iter()
            .map(|object| cache_key_for(&self.storage_binding_id, &input.bucket, &object.key))
            .collect();
        let results = self
            .writer
            .delete_batch(&keys)
            .await
            .map_err(to_s3_write_error)?;
        // Backend outcome known for the whole batch → invalidate → ack.
        self.invalidate(&keys).await?;

        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for (object, result) in input.delete.objects.into_iter().zip(results) {
            match result {
                // Quiet mode reports only failures, as on S3.
                Ok(()) if quiet => {}
                Ok(()) => deleted.push(DeletedObject {
                    key: Some(object.key),
                    ..DeletedObject::default()
                }),
                Err(error) => {
                    let s3_error = to_s3_write_error(error);
                    errors.push(s3s::dto::Error {
                        code: Some(s3_error.code().as_str().to_owned()),
                        key: Some(object.key),
                        message: s3_error.message().map(str::to_owned),
                        version_id: None,
                    });
                }
            }
        }
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: (!deleted.is_empty()).then_some(deleted),
            errors: (!errors.is_empty()).then_some(errors),
            ..DeleteObjectsOutput::default()
        }))
    }

    /// CopyObject: server-side copy within the configured bucket, then
    /// invalidation of the destination key (the source is unchanged), then
    /// the ack (see the ORDERING INVARIANT comment).
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        let CopySource::Bucket {
            bucket: source_bucket,
            key: source_key,
            version_id,
        } = input.copy_source
        else {
            return Err(S3Error::with_message(
                S3ErrorCode::NotImplemented,
                "access point copy sources are not supported",
            ));
        };
        if version_id.is_some() {
            return Err(S3Error::with_message(
                S3ErrorCode::NotImplemented,
                "versioned copy sources are not supported",
            ));
        }
        let source = cache_key_for(&self.storage_binding_id, &source_bucket, &source_key);
        let dest = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        // MetadataDirective (#143): the default is COPY (retain the source's
        // stored metadata); REPLACE writes the request's metadata onto the
        // destination instead.
        let replace = input
            .metadata_directive
            .as_ref()
            .is_some_and(|d| d.as_str() == MetadataDirective::REPLACE);
        // Copying an object onto itself is legal only when its metadata is
        // being replaced; otherwise S3 answers InvalidRequest (#146). Catch it
        // here rather than let the origin's generic error flatten to a 500.
        if source == dest && !replace {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "This copy request is illegal because it is trying to copy an \
                 object to itself without changing the object's metadata, \
                 storage class, website redirect location or encryption \
                 attributes.",
            ));
        }
        let metadata = replace.then(|| {
            write_metadata(
                input.content_type,
                input.cache_control,
                input.content_disposition,
                input.content_encoding,
                input.content_language,
                input.expires,
                input.metadata,
            )
        });
        let outcome = self
            .writer
            .copy(&source, &dest, metadata)
            .await
            .map_err(to_s3_write_error)?;
        self.invalidate(std::slice::from_ref(&dest)).await?;
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: outcome.e_tag.map(to_etag),
                last_modified: outcome.last_modified.map(Timestamp::from),
                ..CopyObjectResult::default()
            }),
            ..CopyObjectOutput::default()
        }))
    }

    /// CreateMultipartUpload: forwarded; the backend's upload ID goes to the
    /// client verbatim so the rest of the lifecycle is stateless. No
    /// invalidation — nothing is visible until completion.
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let key = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        let mut metadata = write_metadata(
            input.content_type,
            input.cache_control,
            input.content_disposition,
            input.content_encoding,
            input.content_language,
            input.expires,
            input.metadata,
        );
        // Forward the checksum SELECTION (algorithm + type) so the origin
        // computes the object checksum from the parts (issue #208). This routes
        // the whole upload through the raw path — object_store can't carry it.
        metadata.checksum = WriteChecksum {
            algorithm: input
                .checksum_algorithm
                .as_ref()
                .map(|a| a.as_str().to_owned()),
            checksum_type: input.checksum_type.as_ref().map(|t| t.as_str().to_owned()),
            ..WriteChecksum::default()
        };
        let creation = self
            .writer
            .create_multipart(&key, metadata)
            .await
            .map_err(to_s3_write_error)?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(creation.upload_id),
            checksum_algorithm: creation.checksum_algorithm.map(ChecksumAlgorithm::from),
            checksum_type: creation.checksum_type.map(ChecksumType::from),
            ..CreateMultipartUploadOutput::default()
        }))
    }

    /// UploadPart: the part body streams through to the backend; the
    /// backend's part ETag goes back to the client for its completion
    /// manifest. No invalidation — parts are invisible until completion.
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let input = req.input;
        let part_number = to_part_number(input.part_number)?;
        let key = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        // Forward any per-part checksum the client asserted (issue #208); the
        // origin verifies it. A present checksum routes the part over the raw
        // path, matching the raw create.
        let checksum = WriteChecksum {
            algorithm: input
                .checksum_algorithm
                .as_ref()
                .map(|a| a.as_str().to_owned()),
            crc32: input.checksum_crc32.clone(),
            crc32c: input.checksum_crc32c.clone(),
            crc64nvme: input.checksum_crc64nvme.clone(),
            sha1: input.checksum_sha1.clone(),
            sha256: input.checksum_sha256.clone(),
            ..WriteChecksum::default()
        };
        let part = self
            .writer
            .upload_part(
                &key,
                &input.upload_id,
                part_number,
                checksum,
                to_write_body(input.body),
            )
            .await
            .map_err(to_s3_write_error)?;
        let mut output = UploadPartOutput {
            e_tag: Some(to_etag(part.e_tag)),
            ..UploadPartOutput::default()
        };
        apply_part_checksums!(output, part.checksums);
        Ok(S3Response::new(output))
    }

    /// CompleteMultipartUpload: this is the moment a multipart object
    /// becomes visible, so it is a write for ordering purposes — backend
    /// assembly confirmed durable, then invalidation of the key, then the
    /// ack (see the ORDERING INVARIANT comment).
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        let key = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        let manifest = input
            .multipart_upload
            .and_then(|upload| upload.parts)
            .unwrap_or_default();
        if manifest.is_empty() {
            // Real S3 rejects a completion without any <Part> as malformed.
            return Err(S3Error::with_message(
                S3ErrorCode::MalformedXML,
                "You must specify at least one part",
            ));
        }
        let parts = manifest
            .into_iter()
            .map(|part| {
                let part_number = to_part_number(part.part_number.ok_or_else(|| {
                    S3Error::with_message(S3ErrorCode::MalformedXML, "part is missing PartNumber")
                })?)?;
                let e_tag = part.e_tag.ok_or_else(|| {
                    S3Error::with_message(S3ErrorCode::MalformedXML, "part is missing ETag")
                })?;
                // Carry the part's asserted checksums into the completion so the
                // origin can combine them into the object checksum (issue #208).
                Ok(CompletedPartRef {
                    part_number,
                    e_tag: e_tag.value().to_owned(),
                    checksums: Checksums {
                        crc32: part.checksum_crc32,
                        crc32c: part.checksum_crc32c,
                        crc64nvme: part.checksum_crc64nvme,
                        sha1: part.checksum_sha1,
                        sha256: part.checksum_sha256,
                        checksum_type: None,
                    },
                })
            })
            .collect::<S3Result<Vec<_>>>()?;
        // The object-level checksum the client asserts for the assembled object.
        let object_checksum = WriteChecksum {
            checksum_type: input.checksum_type.as_ref().map(|t| t.as_str().to_owned()),
            crc32: input.checksum_crc32.clone(),
            crc32c: input.checksum_crc32c.clone(),
            crc64nvme: input.checksum_crc64nvme.clone(),
            sha1: input.checksum_sha1.clone(),
            sha256: input.checksum_sha256.clone(),
            ..WriteChecksum::default()
        };
        let outcome = self
            .writer
            .complete_multipart(&key, &input.upload_id, parts, object_checksum)
            .await
            .map_err(to_s3_write_error)?;
        self.invalidate(std::slice::from_ref(&key)).await?;
        let mut output = CompleteMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            e_tag: outcome.e_tag.clone().map(to_etag),
            version_id: outcome.version_id.clone(),
            ..CompleteMultipartUploadOutput::default()
        };
        apply_checksums!(output, outcome.checksums);
        Ok(S3Response::new(output))
    }

    /// AbortMultipartUpload: forwarded; the backend discards the parts. No
    /// invalidation — an aborted upload never became visible.
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let input = req.input;
        let key = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        self.writer
            .abort_multipart(&key, &input.upload_id)
            .await
            .map_err(to_s3_write_error)?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    /// ListParts: read-only view of an in-flight upload. Marker/max-parts
    /// pagination is applied here so the wire shape matches S3 even though
    /// the interface returns the full list.
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let input = req.input;
        let key = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        let parts = self
            .writer
            .list_parts(&key, &input.upload_id)
            .await
            .map_err(to_s3_write_error)?;

        let marker = input.part_number_marker.unwrap_or(0);
        // S3's default and maximum page size for ListParts.
        let max_parts = input.max_parts.unwrap_or(1000).clamp(0, 1000) as usize;
        let mut page: Vec<Part> = parts
            .into_iter()
            .filter(|part| i32::from(part.part_number) > marker)
            .map(|part| Part {
                e_tag: part.e_tag.map(to_etag),
                last_modified: part.last_modified.map(Timestamp::from),
                part_number: Some(i32::from(part.part_number)),
                size: Some(part.size as i64),
                ..Part::default()
            })
            .collect();
        let is_truncated = page.len() > max_parts;
        page.truncate(max_parts);
        let next_marker = is_truncated
            .then(|| page.last().and_then(|p| p.part_number))
            .flatten();
        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(input.upload_id),
            part_number_marker: input.part_number_marker,
            next_part_number_marker: next_marker,
            max_parts: input.max_parts,
            is_truncated: Some(is_truncated),
            parts: (!page.is_empty()).then_some(page),
            ..ListPartsOutput::default()
        }))
    }

    /// GetObjectAttributes (issue #154): pure passthrough. The attributes —
    /// ETag, size, storage class, checksums, and the multipart part list — are
    /// the origin's, forwarded verbatim; Verglas computes none of them and the
    /// cache is never consulted.
    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        let input = req.input;
        let key = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        let request = AttributesRequest {
            object_attributes: input
                .object_attributes
                .iter()
                .map(|a| a.as_str().to_owned())
                .collect(),
            max_parts: input.max_parts,
            part_number_marker: input.part_number_marker,
            version_id: input.version_id,
        };
        let ObjectAttributes {
            e_tag,
            object_size,
            storage_class,
            last_modified,
            version_id,
            checksums,
            object_parts,
        } = self
            .reader
            .object_attributes(&key, request)
            .await
            .map_err(to_s3_error)?;
        // AWS reports `ObjectParts` only for objects uploaded via multipart;
        // for a single-PUT object it omits the element entirely. MinIO instead
        // reports a one-part `ObjectParts` for every object, so drop it when the
        // ETag is not a multipart ETag (a multipart ETag ends in `-<count>`).
        // The ETag is S3's own record of multipart-ness, so this matches AWS
        // without Verglas inspecting the object.
        let is_multipart = e_tag.as_deref().is_some_and(is_multipart_etag);
        let object_parts = object_parts.filter(|_| is_multipart);
        let object_parts = object_parts.map(|parts| GetObjectAttributesParts {
            is_truncated: Some(parts.is_truncated),
            max_parts: parts.max_parts,
            next_part_number_marker: parts.next_part_number_marker,
            part_number_marker: parts.part_number_marker,
            total_parts_count: parts.total_parts_count,
            parts: Some(
                parts
                    .parts
                    .into_iter()
                    .map(|p| {
                        let mut part = ObjectPart {
                            part_number: Some(p.part_number),
                            size: Some(p.size as i64),
                            ..ObjectPart::default()
                        };
                        part.checksum_crc32 = p.checksums.crc32;
                        part.checksum_crc32c = p.checksums.crc32c;
                        part.checksum_crc64nvme = p.checksums.crc64nvme;
                        part.checksum_sha1 = p.checksums.sha1;
                        part.checksum_sha256 = p.checksums.sha256;
                        part
                    })
                    .collect(),
            ),
        });
        let mut response = S3Response::new(GetObjectAttributesOutput {
            e_tag: e_tag.map(to_etag),
            object_size: object_size.map(|s| s as i64),
            storage_class: storage_class.map(StorageClass::from),
            last_modified: last_modified.map(Timestamp::from),
            version_id,
            checksum: to_dto_checksum(checksums),
            object_parts,
            ..GetObjectAttributesOutput::default()
        });
        // s3s always quotes the XML `<ETag>`, but this one body reports it
        // unquoted (issue #209). Tag the response so the router's transform
        // strips the quotes; s3s copies these extensions to the HTTP response.
        response.extensions.insert(UnquoteAttributesEtag);
        Ok(response)
    }

    /// UploadPartCopy (issue #153): server-side copies a source object (or a
    /// byte range of it) into one part of an in-flight multipart upload. Same
    /// bucket only, mirroring CopyObject; a cross-bucket or versioned source is
    /// `NotImplemented`. No invalidation — the part is invisible until the
    /// upload completes.
    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let input = req.input;
        let CopySource::Bucket {
            bucket: source_bucket,
            key: source_key,
            version_id,
        } = input.copy_source
        else {
            return Err(S3Error::with_message(
                S3ErrorCode::NotImplemented,
                "access point copy sources are not supported",
            ));
        };
        if version_id.is_some() {
            return Err(S3Error::with_message(
                S3ErrorCode::NotImplemented,
                "versioned copy sources are not supported",
            ));
        }
        // Cross-bucket UploadPartCopy IS supported (unlike CopyObject): the copy
        // is server-side at the origin, driven by an `x-amz-copy-source` header
        // the destination's raw client sends, so no cross-client GET+PUT is
        // needed. The source and destination may name different buckets.
        let part_number = to_part_number(input.part_number)?;
        let source = cache_key_for(&self.storage_binding_id, &source_bucket, &source_key);
        let dest = cache_key_for(&self.storage_binding_id, &input.bucket, &input.key);
        let outcome = self
            .writer
            .upload_part_copy(
                &source,
                &dest,
                &input.upload_id,
                part_number,
                input.copy_source_range,
            )
            .await
            .map_err(to_s3_write_error)?;
        Ok(S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: outcome.e_tag.map(to_etag),
                last_modified: outcome.last_modified.map(Timestamp::from),
                ..CopyPartResult::default()
            }),
            ..UploadPartCopyOutput::default()
        }))
    }

    /// ListMultipartUploads (issue #153): read-only passthrough to the origin.
    /// Filters, delimiter roll-up, and the key/upload-ID markers are forwarded
    /// verbatim; the result is never cached.
    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let input = req.input;
        let url = wants_url_encoding(&input.encoding_type);
        // S3 caps a ListMultipartUploads page at 1000 uploads, like LIST.
        let max_uploads = input
            .max_uploads
            .unwrap_or(MAX_LIST_KEYS)
            .clamp(0, MAX_LIST_KEYS);
        let delimiter = input.delimiter.clone().filter(|d| !d.is_empty());
        let request = MultipartUploadsRequest {
            storage_binding_id: self.storage_binding_id.clone(),
            bucket: input.bucket.clone(),
            prefix: input.prefix.clone(),
            delimiter: delimiter.clone(),
            key_marker: input.key_marker.clone(),
            upload_id_marker: input.upload_id_marker.clone(),
            max_uploads: max_uploads as usize,
        };
        let page = self
            .writer
            .list_multipart_uploads(request)
            .await
            .map_err(to_s3_write_error)?;
        let uploads: Vec<MultipartUpload> = page
            .uploads
            .into_iter()
            .map(|u| MultipartUpload {
                key: Some(encode_value(u.key, url)),
                upload_id: Some(u.upload_id),
                storage_class: u.storage_class.map(StorageClass::from),
                initiated: u.initiated.map(Timestamp::from),
                owner: Some(static_owner()),
                initiator: None,
                ..MultipartUpload::default()
            })
            .collect();
        Ok(S3Response::new(ListMultipartUploadsOutput {
            bucket: Some(input.bucket),
            prefix: input.prefix.map(|p| encode_value(p, url)),
            delimiter: delimiter.map(|d| encode_value(d, url)),
            key_marker: input.key_marker,
            upload_id_marker: input.upload_id_marker,
            max_uploads: Some(max_uploads),
            is_truncated: Some(page.is_truncated),
            next_key_marker: page.next_key_marker.map(|m| encode_value(m, url)),
            next_upload_id_marker: page.next_upload_id_marker,
            uploads: (!uploads.is_empty()).then_some(uploads),
            common_prefixes: to_common_prefixes(page.common_prefixes, url),
            encoding_type: url.then(|| EncodingType::from_static(EncodingType::URL)),
            ..ListMultipartUploadsOutput::default()
        }))
    }
}

/// Converts transport-level failures (not S3 application errors, which s3s
/// already rendered as error XML) into a bare 500.
async fn handle_http_error(_error: s3s::HttpError) -> axum::http::StatusCode {
    axum::http::StatusCode::INTERNAL_SERVER_ERROR
}

/// Builds the complete S3 endpoint as an axum router, path-style only: s3s
/// service over the given [`ObjectRead`] source and [`ObjectWrite`] sink (writes
/// fire `invalidation` before acking). When `credentials` is `Some`, every
/// request must carry a valid SigV4 signature (header or presigned query string)
/// matching one of the configured static keys; unsigned requests get
/// `AccessDenied`. With `None`, auth is disabled (unit tests only).
pub fn router<R: ObjectRead, W: ObjectWrite>(
    storage_binding_id: impl Into<String>,
    reader: R,
    writer: W,
    lister: Arc<dyn ObjectList>,
    invalidation: Arc<dyn Invalidation>,
    credentials: Option<(String, String)>,
) -> Router {
    router_with_passthrough(
        storage_binding_id,
        reader,
        writer,
        lister,
        invalidation,
        credentials,
        None,
        None,
        None,
    )
}

/// Builds the endpoint like [`router`] with two optional additions:
///
/// - `passthrough` (issue #152): when `Some`, allowlisted bucket-config
///   operations (HeadBucket, GetBucketLocation) are forwarded to the origin
///   resolved through `stores` before s3s's typed dispatch would 501 them.
///   `None` keeps s3s's default 501 for unmodeled operations.
/// - `base_domain` (issue #11): when `Some`, virtual-hosted-style requests are
///   accepted, whose bucket is the leading Host label under that domain
///   (`bucket.<domain>`); path-style still works, because s3s only extracts a
///   bucket from the Host when it is a proper subdomain of `base_domain`. `None`
///   is path-style only. A domain that fails to parse is logged and the endpoint
///   stays path-style rather than failing to bind.
#[allow(clippy::too_many_arguments)]
pub fn router_with_passthrough<R: ObjectRead, W: ObjectWrite>(
    storage_binding_id: impl Into<String>,
    reader: R,
    writer: W,
    lister: Arc<dyn ObjectList>,
    invalidation: Arc<dyn Invalidation>,
    credentials: Option<(String, String)>,
    passthrough: Option<Arc<dyn verglas_backend::BackendStores>>,
    base_domain: Option<&str>,
    metrics: Option<Arc<NodeMetrics>>,
) -> Router {
    let storage_binding_id = storage_binding_id.into();
    let service = {
        let handler = match metrics {
            Some(metrics) => VerglasS3::with_metrics(
                storage_binding_id.clone(),
                reader,
                writer,
                lister,
                invalidation,
                metrics,
            ),
            None => VerglasS3::new(
                storage_binding_id.clone(),
                reader,
                writer,
                lister,
                invalidation,
            ),
        };
        let mut builder = S3ServiceBuilder::new(handler);
        if let Some((access_key_id, secret_access_key)) = credentials {
            builder.set_auth(StaticAuth::from_single(access_key_id, secret_access_key));
            // Default `S3Access`: authenticated requests proceed, anonymous
            // requests are rejected with AccessDenied before the handler runs.
        }
        if let Some(stores) = passthrough {
            // Checked before typed dispatch: allowlisted bucket-config ops are
            // forwarded to the origin instead of 501ing (issue #152).
            builder.set_route(crate::passthrough_route::BucketConfigPassthrough::new(
                storage_binding_id.clone(),
                stores,
            ));
        }
        if let Some(domain) = base_domain {
            match s3s::host::SingleDomain::new(domain) {
                Ok(host) => builder.set_host(host),
                Err(error) => tracing::error!(
                    domain,
                    %error,
                    "listen.domain did not parse as a base domain; serving path-style only"
                ),
            }
        }
        builder.build()
    };
    let service = tower::ServiceBuilder::new()
        // Unquotes the GetObjectAttributes `<ETag>` (issue #209); a no-op on
        // every response the handler did not tag. Runs closest to s3s so it
        // sees the marker extension before the outer trace layer (which only
        // touches error bodies) shapes the response.
        .layer(map_response(unquote_attributes_etag))
        .service(HandleError::new(service, handle_http_error));
    // `trace_request` runs outermost (#61): it mints the request id, scopes it
    // for the whole request future, opens the root span, and stamps the id and
    // S3 error enrichment onto the response the client sees.
    Router::new()
        .fallback_service(service)
        .layer(axum::middleware::from_fn(crate::middleware::trace_request))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #143: `aws-chunked` is a SigV4 transfer artifact and is stripped from
    /// `Content-Encoding`; genuine codings (and their order/spacing) survive,
    /// and an encoding of only `aws-chunked` becomes absent.
    #[test]
    fn strip_aws_chunked_matches_s3() {
        assert_eq!(strip_aws_chunked(None), None);
        assert_eq!(
            strip_aws_chunked(Some("gzip".into())).as_deref(),
            Some("gzip")
        );
        assert_eq!(
            strip_aws_chunked(Some("deflate, gzip".into())).as_deref(),
            Some("deflate, gzip")
        );
        assert_eq!(
            strip_aws_chunked(Some("gzip, aws-chunked".into())).as_deref(),
            Some("gzip")
        );
        assert_eq!(
            strip_aws_chunked(Some("aws-chunked, gzip".into())).as_deref(),
            Some("gzip")
        );
        assert_eq!(strip_aws_chunked(Some("aws-chunked".into())), None);
        assert_eq!(
            strip_aws_chunked(Some("aws-chunked, aws-chunked".into())),
            None
        );
    }

    /// #11: a base domain that s3s rejects does not panic or fail to build — the
    /// router still constructs (path-style only), because serving path-style is
    /// strictly better than not serving. A well-formed domain installs the host
    /// parser. Both paths build a router; the addressing behavior itself is
    /// covered by the `addressing` integration test.
    #[test]
    fn router_with_domain_tolerates_a_malformed_domain() {
        use crate::passthrough::{PassthroughList, PassthroughWrite};
        use verglas_backend::BackendStore;

        let store = std::sync::Arc::new(object_store::memory::InMemory::new());
        let build = |domain: Option<&str>| {
            router_with_passthrough(
                "default",
                crate::PassthroughRead::new(BackendStore::single(
                    "default",
                    "test-bucket",
                    store.clone(),
                )),
                PassthroughWrite::new(BackendStore::single(
                    "default",
                    "test-bucket",
                    store.clone(),
                )),
                Arc::new(PassthroughList::new(BackendStore::single(
                    "default",
                    "test-bucket",
                    store.clone(),
                ))),
                Arc::new(crate::NoopInvalidation),
                None,
                None,
                domain,
                None,
            )
        };
        // A slash makes it not a bare domain; s3s's SingleDomain rejects it, and
        // the router falls back to path-style rather than failing.
        let _ = build(Some("not a domain/with slash"));
        // A well-formed domain installs the host parser.
        let _ = build(Some("s3.example.com"));
    }

    /// #208: a multipart ETag (`<hex>-<count>`) is recognised; a plain
    /// single-PUT ETag is not, so its `ObjectParts` section is suppressed.
    #[test]
    fn multipart_etag_is_recognised_by_suffix() {
        assert!(is_multipart_etag("b2add96cc9702bbf4efb0ccdfc6b7747-3"));
        assert!(is_multipart_etag("\"b2add96cc9702bbf4efb0ccdfc6b7747-12\""));
        assert!(!is_multipart_etag("acbd18db4cc2f85cedef654fccc4a4d8"));
        assert!(!is_multipart_etag("\"acbd18db4cc2f85cedef654fccc4a4d8\""));
        // A trailing dash with no count, or a non-numeric suffix, is not
        // multipart.
        assert!(!is_multipart_etag("abc-"));
        assert!(!is_multipart_etag("abc-xyz"));
    }

    /// #155: a well-formed base64 128-bit digest decodes; anything else (short,
    /// empty, non-base64) is rejected as InvalidDigest.
    #[test]
    fn content_md5_decode_validates_length_and_encoding() {
        // 16 zero bytes, base64-encoded.
        assert_eq!(
            decode_content_md5("AAAAAAAAAAAAAAAAAAAAAA==").expect("valid"),
            [0u8; 16]
        );
        for bad in ["", "YWJyYWNhZGFicmE=", "not*base64", "AAAA"] {
            let err = decode_content_md5(bad).expect_err("must reject");
            assert_eq!(
                err.code().as_str(),
                S3ErrorCode::InvalidDigest.as_str(),
                "{bad} must be InvalidDigest"
            );
        }
    }

    /// #254: small Parquet ranges consume one unit of a byte-weighted response
    /// budget rather than one-sixteenth of all egress capacity. More than
    /// sixteen small responses may stay live without removing the hard bound.
    #[tokio::test]
    async fn response_budget_weights_ranges_instead_of_counting_bodies() {
        const LIVE_BODIES: usize = 32;
        const LIVE_BODY_UNITS: u32 = 32;
        let budget = ResponseBodyBudget::new(LIVE_BODIES, 1, LIVE_BODY_UNITS);
        let mut held_bodies = Vec::with_capacity(LIVE_BODIES);

        for byte in 0..LIVE_BODIES as u8 {
            let body =
                futures::stream::once(async move { Ok(bytes::Bytes::from(vec![byte])) }).boxed();
            held_bodies.push(retain_response_budget(
                body,
                budget
                    .acquire_for(1)
                    .await
                    .expect("the private semaphore stays open"),
            ));
        }

        assert_eq!(
            budget.available(),
            0,
            "each one-byte range consumes one budget unit while its body is live"
        );

        let waiting = budget.acquire_for(1);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), waiting)
                .await
                .is_err(),
            "the next response must wait while the egress budget is exhausted"
        );

        drop(held_bodies.pop());
        assert!(
            budget.acquire_for(1).await.is_ok(),
            "dropping one response must wake the next budget waiter"
        );
    }

    /// #254: one huge response cannot monopolize the weighted egress budget.
    /// Its charge is capped, leaving capacity for another response to progress.
    #[tokio::test]
    async fn large_response_charge_is_capped() {
        let budget = ResponseBodyBudget::new(8, 1, 4);
        let first = budget
            .acquire_for(u64::MAX)
            .await
            .expect("the private semaphore stays open");
        assert_eq!(budget.available(), 4);
        let second = budget
            .acquire_for(u64::MAX)
            .await
            .expect("the capped second response fits");
        assert_eq!(budget.available(), 0);

        let waiting = budget.acquire_for(1);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), waiting)
                .await
                .is_err(),
            "the total response budget remains a hard ceiling"
        );

        drop(first);
        assert!(budget.acquire_for(1).await.is_ok());
        drop(second);
    }

    /// #254: weighting rounds a partial quantum upward so every non-empty body
    /// is charged and a boundary byte cannot escape the configured ceiling.
    #[tokio::test]
    async fn response_budget_rounds_up_partial_quanta() {
        let budget = ResponseBodyBudget::new(4, 4, 4);
        let one = budget
            .acquire_for(1)
            .await
            .expect("one byte consumes one quantum");
        assert_eq!(budget.available(), 3);
        let five = budget
            .acquire_for(5)
            .await
            .expect("five bytes consume two quanta");
        assert_eq!(budget.available(), 1);

        drop(one);
        drop(five);
    }

    /// #254: retaining the weighted permit does not alter chunk boundaries or
    /// bytes; the protocol still delivers exactly the declared content length.
    #[tokio::test]
    async fn response_budget_preserves_stream_bytes() {
        let budget = ResponseBodyBudget::new(4, 1, 4);
        let body = futures::stream::iter([
            Ok(bytes::Bytes::from_static(b"first")),
            Ok(bytes::Bytes::from_static(b"second")),
        ])
        .boxed();
        let mut held = retain_response_budget(
            body,
            budget
                .acquire_for(11)
                .await
                .expect("the capped response charge fits"),
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = held.next().await {
            bytes.extend_from_slice(&chunk.expect("body chunk succeeds"));
        }

        assert_eq!(bytes, b"firstsecond");
        assert_eq!(bytes.len(), 11);
    }
}
