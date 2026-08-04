//! The `ObjectWrite` interface between the S3 front-end and the backend bucket,
//! plus the [`Invalidation`] hook the write path fires before acking. Writes
//! are passthrough by design — Verglas is never the only copy of any byte —
//! so unlike [`ObjectRead`](crate::read::ObjectRead) this interface is not where a
//! cache drops in: the real backend client (#18) replaces the passthrough,
//! and the cache only ever appears behind [`Invalidation`] (#12/#14).

use std::collections::BTreeMap;
use std::future::Future;
use std::time::SystemTime;

use crate::CacheKey;
use crate::read::Checksums;
use bytes::Bytes;
use futures::stream::BoxStream;

/// Object metadata carried through the write path alongside the body: the S3
/// *system* headers that describe an object plus the user metadata map
/// (`x-amz-meta-*`). This is the channel issue #143 opened — before it, only
/// `content_type` reached the backend and every other header (and all user
/// metadata) was dropped on PUT / CreateMultipartUpload / CopyObject-REPLACE.
///
/// The set mirrors what an S3 `PutObject` stores and a later GET/HEAD reports
/// back (see [`ObjectMeta`](crate::read::ObjectMeta), its read-path twin).
/// Values are stored verbatim; the protocol layer owns wire parsing and the
/// backend owns forwarding whichever of these its store supports.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteMetadata {
    /// MIME type (`Content-Type`).
    pub content_type: Option<String>,
    /// Caching directive (`Cache-Control`).
    pub cache_control: Option<String>,
    /// Presentation hint (`Content-Disposition`).
    pub content_disposition: Option<String>,
    /// Content codings applied to the body (`Content-Encoding`).
    pub content_encoding: Option<String>,
    /// Natural language of the body (`Content-Language`).
    pub content_language: Option<String>,
    /// Expiry directive (`Expires`), issue #189. Carried as the raw HTTP-date
    /// string; `object_store` 0.14 models no `Expires` attribute, so a PUT that
    /// sets it is routed through the raw-request write path.
    pub expires: Option<String>,
    /// Checksum algorithm and any precomputed checksum values the client asked
    /// the origin to verify (issue #154). `object_store` cannot carry these, so
    /// a write that sets one is routed through the raw-request write path; the
    /// origin does the verification, Verglas never computes a checksum.
    pub checksum: WriteChecksum,
    /// User-defined metadata (`x-amz-meta-<key>`), keys without the prefix.
    /// Ordered so a copy's stored metadata is deterministic.
    pub user_metadata: BTreeMap<String, String>,
}

/// Checksum request parameters carried on a write (issue #154): the algorithm
/// selector plus any explicit precomputed checksum values. All optional; the
/// origin verifies them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteChecksum {
    /// The checksum algorithm (`x-amz-sdk-checksum-algorithm`), e.g. `SHA256`.
    pub algorithm: Option<String>,
    /// The checksum type (`x-amz-checksum-type`, `COMPOSITE` or `FULL_OBJECT`).
    /// Meaningful on CreateMultipartUpload, which picks how part checksums
    /// combine into the object checksum (issue #208).
    pub checksum_type: Option<String>,
    /// `x-amz-checksum-crc32`.
    pub crc32: Option<String>,
    /// `x-amz-checksum-crc32c`.
    pub crc32c: Option<String>,
    /// `x-amz-checksum-crc64nvme`.
    pub crc64nvme: Option<String>,
    /// `x-amz-checksum-sha1`.
    pub sha1: Option<String>,
    /// `x-amz-checksum-sha256`.
    pub sha256: Option<String>,
}

impl WriteChecksum {
    /// True when the client requested no checksum, so the write can stay on the
    /// typed path.
    pub fn is_empty(&self) -> bool {
        self.algorithm.is_none()
            && self.checksum_type.is_none()
            && self.crc32.is_none()
            && self.crc32c.is_none()
            && self.crc64nvme.is_none()
            && self.sha1.is_none()
            && self.sha256.is_none()
    }
}

/// Streaming write body: chunks exactly as the client sends them.
/// Implementations must consume this incrementally — buffering a whole
/// object is forbidden, bodies are multi-GB.
pub type WriteBodyStream = BoxStream<'static, Result<Bytes, WriteError>>;

/// The backend's answer to a durable whole-object write (PUT or
/// CompleteMultipartUpload): the entity tag the origin assigned, which the
/// client must see so its ETag matches later reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutOutcome {
    /// Entity tag exactly as the backend reports it (quoting normalized by
    /// the front-end, same as the read path).
    pub e_tag: Option<String>,
    /// The checksum the origin echoed for the stored object (issue #154),
    /// forwarded verbatim so the client's PUT response carries it.
    pub checksums: Checksums,
    /// The version ID the origin assigned, if the bucket is versioned.
    pub version_id: Option<String>,
}

/// The backend's answer to a server-side copy: metadata of the freshly
/// written destination object, as S3's `CopyObjectResult` XML requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOutcome {
    /// Entity tag of the destination object.
    pub e_tag: Option<String>,
    /// Last-modified time of the destination object.
    pub last_modified: Option<SystemTime>,
}

/// One entry of a client's CompleteMultipartUpload manifest: the part number
/// it assigned, the ETag it got back from [`ObjectWrite::upload_part`], and any
/// per-part checksums the client is asserting (issue #208). The origin verifies
/// both the ETag and the checksums; Verglas forwards them, never computing one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedPartRef {
    /// Client-assigned part number (S3 allows 1..=10000).
    pub part_number: u16,
    /// ETag returned when the part was uploaded; the backend verifies it.
    pub e_tag: String,
    /// The part's checksums from the client's manifest, forwarded into the
    /// completion so the origin can combine them into the object checksum.
    pub checksums: Checksums,
}

/// The origin's answer to CreateMultipartUpload: the upload ID plus the checksum
/// algorithm and type the origin recorded for the upload (issue #208), echoed
/// so the client's create response carries them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultipartCreation {
    /// The origin's upload ID, forwarded verbatim.
    pub upload_id: String,
    /// The checksum algorithm the origin will apply to parts, if any.
    pub checksum_algorithm: Option<String>,
    /// The checksum type (`COMPOSITE`/`FULL_OBJECT`) the origin recorded, if any.
    pub checksum_type: Option<String>,
}

/// The origin's answer to UploadPart: the part's ETag plus any checksum the
/// origin echoed for it (issue #208), forwarded into the client's response so
/// its completion manifest can assert the same values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartUpload {
    /// The ETag the origin assigned the part.
    pub e_tag: String,
    /// The checksums the origin echoed for the part, if the upload is checksummed.
    pub checksums: Checksums,
}

/// A ListMultipartUploads request (issue #153): the optional filters and
/// pagination cursors, plus the page size cap.
#[derive(Debug, Clone, Default)]
pub struct MultipartUploadsRequest {
    /// The bucket to list uploads in.
    pub bucket: String,
    /// Restrict to uploads whose key starts with this prefix.
    pub prefix: Option<String>,
    /// Roll up keys sharing a run up to this delimiter into common prefixes.
    pub delimiter: Option<String>,
    /// Resume listing after this key.
    pub key_marker: Option<String>,
    /// Resume listing after this upload ID (paired with `key_marker`).
    pub upload_id_marker: Option<String>,
    /// Maximum uploads to return on the page.
    pub max_uploads: usize,
}

/// One in-flight multipart upload as ListMultipartUploads reports it (issue
/// #153).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadEntry {
    /// The object key the upload targets.
    pub key: String,
    /// The origin's upload ID.
    pub upload_id: String,
    /// The upload's storage class, if the origin reported one.
    pub storage_class: Option<String>,
    /// When the upload was initiated, if the origin reported it.
    pub initiated: Option<SystemTime>,
}

/// One page of a ListMultipartUploads result (issue #153).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultipartUploadsPage {
    /// The uploads on this page.
    pub uploads: Vec<UploadEntry>,
    /// Delimiter-rolled-up common prefixes on this page.
    pub common_prefixes: Vec<String>,
    /// Whether more uploads remain past this page.
    pub is_truncated: bool,
    /// The key marker to resume from when truncated.
    pub next_key_marker: Option<String>,
    /// The upload-ID marker to resume from when truncated.
    pub next_upload_id_marker: Option<String>,
}

/// Metadata of one uploaded part, as ListParts reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartInfo {
    /// Client-assigned part number.
    pub part_number: u16,
    /// ETag the backend assigned to the part.
    pub e_tag: Option<String>,
    /// Size of the part in bytes.
    pub size: u64,
    /// When the part was uploaded.
    pub last_modified: Option<SystemTime>,
}

/// Write-path errors. As with [`ReadError`](crate::read::ReadError) the
/// variants map one-to-one onto S3 error codes, so implementations decide
/// semantics and the protocol layer only translates.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// The bucket does not exist (`NoSuchBucket`, 404): either the request named
    /// a bucket other than the one this server serves, or the origin reports the
    /// configured bucket is gone.
    #[error("bucket does not exist")]
    NoSuchBucket,
    /// The origin refused the request's credentials (`AccessDenied`, 403),
    /// passed through from the origin unchanged.
    #[error("access denied by origin")]
    AccessDenied,
    /// A CopyObject named a different source and destination bucket. Same-bucket
    /// copies are server-side; cross-bucket copy is not supported in v1
    /// (`NotImplemented`, 501) because origin clients are bucket-scoped.
    #[error("cross-bucket copy is not supported")]
    CrossBucketCopy,
    /// A copy source object does not exist (`NoSuchKey`, 404). Plain deletes
    /// of a missing key are *not* errors — S3 deletes are idempotent.
    #[error("key does not exist")]
    NoSuchKey,
    /// The multipart upload ID is unknown — never created, already
    /// completed, or already aborted (`NoSuchUpload`, 404).
    #[error("multipart upload does not exist")]
    NoSuchUpload,
    /// A part in a CompleteMultipartUpload manifest is invalid: bad part
    /// number, missing ETag, or an ETag the backend rejects (`InvalidPart`,
    /// 400).
    #[error("invalid part: {0}")]
    InvalidPart(String),
    /// An UploadPartCopy named a `copy-source-range` outside the source object
    /// (`InvalidRange`, 400/416). Issue #153.
    #[error("copy source range is not satisfiable")]
    InvalidRange,
    /// A non-final part in a completed multipart upload was smaller than S3's
    /// minimum part size (`EntityTooSmall`, 400). Classified from the origin's
    /// own answer, not judged by Verglas.
    #[error("multipart part too small: {0}")]
    EntityTooSmall(String),
    /// The request was well-formed but semantically illegal — most commonly a
    /// CopyObject whose source and destination are identical without a
    /// metadata change (`InvalidRequest`, 400).
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// The request is legal S3 but cannot be served without corrupting data —
    /// e.g. a multipart upload naming a key (or carrying an `Expires` header)
    /// the typed backend client would silently mangle, with no raw multipart
    /// path yet (`NotImplemented`, 501). Failing loudly beats storing wrong
    /// bytes (issue #189).
    #[error("not supported: {0}")]
    Unsupported(String),
    /// The client's body stream failed before the write finished
    /// (`IncompleteBody`, 400). The backend write was aborted; nothing
    /// became visible.
    #[error("client body failed: {0}")]
    Body(String),
    /// Any other backend failure (`InternalError`, 500). Carries the
    /// underlying message for logs.
    #[error("backend write failed: {0}")]
    Backend(String),
}

/// Sink for object writes behind the S3 front-end.
///
/// Contract every implementation must uphold (issue #21's ordering depends
/// on it): a method returning `Ok` means the backend has confirmed the write
/// **durable** — never resolve early, never buffer-and-ack. Bodies must be
/// streamed to the backend in bounded memory. Multipart upload IDs are the
/// backend's own IDs, round-tripped to the client untouched, so an upload
/// started through one path can be completed through another.
///
/// This is a separate trait from [`ObjectRead`](crate::read::ObjectRead) on
/// purpose: reads gain a cache in front of the backend (#12) while writes
/// stay passthrough forever (read-through-only rule), so the two interfaces get
/// different implementations behind the same front-end.
pub trait ObjectWrite: Send + Sync + 'static {
    /// Streams `body` to the backend as the new content of `key`, returning
    /// only once the backend confirms the object durable. `metadata` (system
    /// headers + user `x-amz-meta-*`) is stored with the object so later reads
    /// report it back (issue #143).
    fn put(
        &self,
        key: &CacheKey,
        metadata: WriteMetadata,
        body: WriteBodyStream,
    ) -> impl Future<Output = Result<PutOutcome, WriteError>> + Send;

    /// Deletes the object at `key`. Deleting a key that does not exist is
    /// success, exactly as on S3.
    fn delete(&self, key: &CacheKey) -> impl Future<Output = Result<(), WriteError>> + Send;

    /// Deletes a batch of keys, returning one result per key in input order.
    /// The outer error is for failures affecting the whole batch (wrong
    /// bucket); per-key failures land in the inner results.
    fn delete_batch(
        &self,
        keys: &[CacheKey],
    ) -> impl Future<Output = Result<Vec<Result<(), WriteError>>, WriteError>> + Send;

    /// Server-side copies `source` to `dest` within the backend bucket,
    /// returning the destination's metadata once it is durable. `metadata`
    /// selects the S3 `MetadataDirective` (issue #143): `None` is `COPY` — the
    /// source's stored metadata is retained — and `Some(md)` is `REPLACE`,
    /// writing `md` onto the destination instead of the source's.
    fn copy(
        &self,
        source: &CacheKey,
        dest: &CacheKey,
        metadata: Option<WriteMetadata>,
    ) -> impl Future<Output = Result<CopyOutcome, WriteError>> + Send;

    /// Starts a multipart upload for `key`, returning the backend's upload ID
    /// plus any checksum algorithm/type the origin recorded for it (issue #208).
    /// The client echoes the upload ID on every subsequent call. `metadata` is
    /// stored with the upload — including a requested checksum algorithm — so
    /// the completed object reports it back (issues #143/#208).
    fn create_multipart(
        &self,
        key: &CacheKey,
        metadata: WriteMetadata,
    ) -> impl Future<Output = Result<MultipartCreation, WriteError>> + Send;

    /// Streams one part to the backend, forwarding any per-part checksum the
    /// client asserted (issue #208) and returning the ETag the backend assigned
    /// plus the checksum it echoed (the client echoes both in its completion
    /// manifest).
    fn upload_part(
        &self,
        key: &CacheKey,
        upload_id: &str,
        part_number: u16,
        checksum: WriteChecksum,
        body: WriteBodyStream,
    ) -> impl Future<Output = Result<PartUpload, WriteError>> + Send;

    /// Completes a multipart upload from the client's part manifest, forwarding
    /// the object-level checksum the client asserted (issue #208). `Ok` means the
    /// assembled object is durable and visible on the backend; the returned
    /// [`PutOutcome`] carries the origin's composite checksum, if any.
    fn complete_multipart(
        &self,
        key: &CacheKey,
        upload_id: &str,
        parts: Vec<CompletedPartRef>,
        object_checksum: WriteChecksum,
    ) -> impl Future<Output = Result<PutOutcome, WriteError>> + Send;

    /// Aborts a multipart upload, discarding its parts on the backend.
    fn abort_multipart(
        &self,
        key: &CacheKey,
        upload_id: &str,
    ) -> impl Future<Output = Result<(), WriteError>> + Send;

    /// Lists the parts uploaded so far for an in-flight upload, ascending by
    /// part number.
    fn list_parts(
        &self,
        key: &CacheKey,
        upload_id: &str,
    ) -> impl Future<Output = Result<Vec<PartInfo>, WriteError>> + Send;

    /// Server-side copies `source` (optionally just the `copy_range` byte
    /// window of it) into part `part_number` of the in-flight upload
    /// `upload_id` on `dest` (UploadPartCopy, issue #153). Same-bucket only in
    /// v1, mirroring [`ObjectWrite::copy`]; the front-end rejects a cross-bucket
    /// or versioned source before calling. Returns the new part's ETag and
    /// last-modified time. The default is `Unsupported` for sinks that do not
    /// model it (test mocks).
    fn upload_part_copy(
        &self,
        _source: &CacheKey,
        _dest: &CacheKey,
        _upload_id: &str,
        _part_number: u16,
        _copy_range: Option<String>,
    ) -> impl Future<Output = Result<CopyOutcome, WriteError>> + Send {
        async {
            Err(WriteError::Unsupported(
                "UploadPartCopy not supported".to_owned(),
            ))
        }
    }

    /// Lists the in-flight multipart uploads in a bucket (ListMultipartUploads,
    /// issue #153). Read-only passthrough to the origin. The default is
    /// `Unsupported` for sinks that do not model it (test mocks).
    fn list_multipart_uploads(
        &self,
        _request: MultipartUploadsRequest,
    ) -> impl Future<Output = Result<MultipartUploadsPage, WriteError>> + Send {
        async {
            Err(WriteError::Unsupported(
                "ListMultipartUploads not supported".to_owned(),
            ))
        }
    }
}

/// Cache-invalidation hook, fired by the front-end with the logical keys a
/// durable write affected. Ordering is the front-end's job (issue #21):
/// backend durable → `invalidate` → ack; an implementation only has to
/// guarantee that when its future resolves `Ok`, no subsequent read of those
/// keys can be served from pre-write cached state.
///
/// This is the hook the cache engine (#12) and the key→ETag mapping (#14)
/// fill in. Until they exist there is nothing to invalidate and
/// [`NoopInvalidation`] is the only implementation.
#[async_trait::async_trait]
pub trait Invalidation: Send + Sync + 'static {
    /// Drops (or marks stale) any cached state for `keys`. An `Err` blocks
    /// the client ack: the front-end fails the request rather than ack a
    /// write whose stale entries might still be served.
    async fn invalidate(&self, keys: &[CacheKey]) -> Result<(), String>;
}

/// No-op [`Invalidation`] for the pre-cache world: there is no cached state
/// yet, so invalidating nothing is trivially correct.
pub struct NoopInvalidation;

#[async_trait::async_trait]
impl Invalidation for NoopInvalidation {
    /// Nothing is cached yet; succeed immediately.
    async fn invalidate(&self, _keys: &[CacheKey]) -> Result<(), String> {
        Ok(())
    }
}
