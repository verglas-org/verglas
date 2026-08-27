//! The `ObjectRead` interface between the S3 front-end and whatever produces bytes.
//! The front-end (issue #6) only ever reads through this trait, so the cache
//! (issue #12) and the real backend client (issue #18) drop in behind it
//! without the protocol layer changing. Defined here in `verglas-core` so
//! producers (`verglas-cache`'s engine, `verglas-s3`'s passthrough) and the
//! consumer (the protocol layer) share it without depending on each other.

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::time::SystemTime;

use crate::CacheKey;
use bytes::Bytes;
use futures::stream::BoxStream;

/// A requested byte range, mirroring the three HTTP `Range: bytes=` forms.
///
/// Offsets use HTTP semantics: `Bounded` is inclusive on both ends, exactly
/// as parsed off the wire. Resolution against the object's actual size
/// (clamping a too-long `Bounded`/`Suffix` to the object) is the
/// implementation's job, so a cache can resolve from local metadata while the
/// passthrough lets the origin resolve it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadRange {
    /// The entire object (no `Range` header).
    Full,
    /// `bytes=first-`: from `first` to the end of the object.
    From(u64),
    /// `bytes=first-last`: both ends inclusive.
    Bounded(u64, u64),
    /// `bytes=-length`: the last `length` bytes of the object.
    Suffix(u64),
}

/// Whole-object metadata, as the origin reports it.
///
/// `size` is always the size of the *entire object*, never of a served
/// range — the front-end needs the total to render `Content-Range`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Total object size in bytes.
    pub size: u64,
    /// Entity tag exactly as the origin reports it (may or may not be
    /// quoted; the front-end normalizes for the wire).
    pub e_tag: Option<String>,
    /// Last modification time of the object.
    pub last_modified: Option<SystemTime>,
    /// MIME type stored with the object, when the origin knows one.
    pub content_type: Option<String>,
    /// Caching directive stored with the object (`Cache-Control`), issue #143.
    pub cache_control: Option<String>,
    /// Presentation hint stored with the object (`Content-Disposition`).
    pub content_disposition: Option<String>,
    /// Content codings applied to the body (`Content-Encoding`).
    pub content_encoding: Option<String>,
    /// Natural language of the body (`Content-Language`).
    pub content_language: Option<String>,
    /// Expiry directive stored with the object (`Expires`), issue #189. Carried
    /// as the raw HTTP-date string the origin reports, because `object_store`
    /// 0.14 models no `Expires` attribute — the raw-request read path fills it.
    pub expires: Option<String>,
    /// User-defined metadata (`x-amz-meta-<key>`), keys without the prefix.
    pub user_metadata: BTreeMap<String, String>,
}

/// Streaming object body. Chunks arrive as the producer yields them; nothing
/// in the read path may collect this into one buffer (objects are multi-GB).
pub type BodyStream = BoxStream<'static, Result<Bytes, ReadError>>;

/// Outcome of revalidating a cached key→ETag mapping against the origin with a
/// conditional metadata request (`If-None-Match: <etag>`) — the mutable-mapping
/// refresh of issue #14. Only the mapping (the sole staleness-prone cache
/// state) is ever revalidated; blocks are ETag-keyed and so can never be stale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Revalidation {
    /// The origin's current version still matches the presented ETag (HTTP
    /// 304): the mapping holds, so only its TTL needs refreshing. No bytes are
    /// transferred and every cached block of this version stays valid.
    Unchanged,
    /// The object changed: the origin now serves a different version, whose
    /// fresh whole-object metadata (the new key→ETag mapping) rides along.
    /// Boxed: `ObjectMeta` carries the full header set (#143/#189) and would
    /// otherwise dwarf the other variants.
    Changed(Box<ObjectMeta>),
    /// The object (or its bucket) no longer exists: the mapping must be dropped
    /// and the caller re-resolves, surfacing the origin's own 404.
    Vanished,
}

/// Which rung of the cache served a read, for the per-tier request-duration
/// histogram (#46). The cache engine records the tier that produced the
/// first served block; the S3 front-end reads it after the body drains to
/// label `verglas_request_duration_seconds{tier}`.
///
/// The tiers mirror the miss ladder. `Passthrough` is the direct-to-origin
/// reader (no cache in the path) and the non-cacheable/escape-hatch reads the
/// cache forwards straight to the origin — distinct from `Backend`, which is a
/// cache *fill* that also admitted the block. `Meta` (the pinned metadata
/// store, #50) is folded into `Dram` for the histogram label because both are
/// DRAM-resident warm serves and the metadata split is already visible in the
/// cache counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServedTier {
    /// Served from the local DRAM tier (block memory tier or the pinned
    /// metadata store).
    Dram,
    /// Served from the local disk (NVMe) tier.
    Nvme,
    /// Served from a backend cache-fill (a miss that fetched and admitted).
    Backend,
    /// Served straight through the direct-to-origin passthrough (no cache in
    /// the path, or a non-cacheable/escape-hatch read).
    Passthrough,
}

impl ServedTier {
    /// The lowercase label used on the `tier` dimension of the metrics. Stable:
    /// dashboards and the metric reference depend on these exact strings.
    pub fn label(self) -> &'static str {
        match self {
            ServedTier::Dram => "dram",
            ServedTier::Nvme => "nvme",
            ServedTier::Backend => "backend",
            ServedTier::Passthrough => "passthrough",
        }
    }

    /// Encodes the tier as the `u8` a [`TierCell`] stores.
    fn to_u8(self) -> u8 {
        match self {
            ServedTier::Dram => 0,
            ServedTier::Nvme => 1,
            ServedTier::Backend => 2,
            ServedTier::Passthrough => 3,
        }
    }

    /// Decodes a tier from the `u8` a [`TierCell`] stores; any unknown value
    /// (the cell was never written — an empty body served no block) reads as
    /// `Dram`, the warm default.
    fn from_u8(v: u8) -> Self {
        match v {
            1 => ServedTier::Nvme,
            2 => ServedTier::Backend,
            3 => ServedTier::Passthrough,
            _ => ServedTier::Dram,
        }
    }
}

/// A one-slot, lock-free cell the cache engine writes the serving tier into as
/// the body streams, and the S3 front-end reads once the body has drained
/// (#46). Shared behind an `Arc` so the lazy body stream and the front-end see
/// the same slot; a single relaxed atomic store on the first served block sets
/// it (never a lock, never an allocation — the hot-path invariant holds).
#[derive(Debug, Clone)]
pub struct TierCell(std::sync::Arc<std::sync::atomic::AtomicU8>);

impl TierCell {
    /// A cell that reads `Dram` until the engine records a tier — the warm
    /// default, so an empty-body read (no block served) is not miscounted.
    pub fn new() -> Self {
        TierCell(std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0)))
    }

    /// Records the tier that served a block. Called once, on the first served
    /// block, from the streaming body — a single relaxed store.
    pub fn set(&self, tier: ServedTier) {
        self.0
            .store(tier.to_u8(), std::sync::atomic::Ordering::Relaxed);
    }

    /// The recorded serving tier, read by the front-end after the body drains.
    pub fn get(&self) -> ServedTier {
        ServedTier::from_u8(self.0.load(std::sync::atomic::Ordering::Relaxed))
    }
}

impl Default for TierCell {
    /// Same as [`TierCell::new`].
    fn default() -> Self {
        TierCell::new()
    }
}

/// A successful ranged read: metadata, the range actually served, and the
/// body stream for exactly that range.
pub struct ObjectGet {
    /// Whole-object metadata (`meta.size` is the full size).
    pub meta: ObjectMeta,
    /// Absolute half-open byte range served, already resolved and clamped
    /// against `meta.size`. Equals `0..meta.size` for a `Full` read.
    pub range: Range<u64>,
    /// The bytes of `range`, streamed.
    pub body: BodyStream,
    /// The tier that served this read, resolved as the body streams (#46). The
    /// front-end reads it after the body drains to label the request-duration
    /// histogram. Defaults to `Dram` when nothing wrote it (an empty body, or a
    /// reader that does not model tiers — the passthrough sets it explicitly).
    pub served_from: TierCell,
}

/// The modern S3 checksum values (issue #154), each a Base64 string exactly as
/// the origin reports it. Verglas only ever forwards these — it never computes
/// a checksum of its own, so serving origin bytes under origin ETags stays
/// honest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Checksums {
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
    /// `x-amz-checksum-type` (`COMPOSITE` or `FULL_OBJECT`).
    pub checksum_type: Option<String>,
}

/// The extra S3 parameters a GET/HEAD may carry that force a passthrough
/// (uncached) read straight to the origin: a version ID (issue #156), a part
/// number (issue #153), or checksum mode (issue #154). These name an immutable,
/// version- or part-scoped view the cache does not model, so any of them
/// bypasses the cache — the escape-hatch reads always resolve at the origin.
#[derive(Debug, Clone, Default)]
pub struct DirectReadOptions {
    /// `?versionId=<id>` — read one specific immutable object version.
    pub version_id: Option<String>,
    /// `?partNumber=<n>` — read just this part of a multipart object.
    pub part_number: Option<u16>,
    /// `x-amz-checksum-mode: ENABLED` — return the origin's stored checksums.
    pub checksum_mode: bool,
}

impl DirectReadOptions {
    /// True when any option is set, so a GET/HEAD must take the passthrough
    /// route rather than the cache.
    pub fn is_direct(&self) -> bool {
        self.version_id.is_some() || self.part_number.is_some() || self.checksum_mode
    }
}

/// Whole-object metadata for a direct read plus the version-, part-, and
/// checksum-scoped extras the escape-hatch reads surface (issues #153/#154/#156).
pub struct DirectMeta {
    /// The object metadata, exactly as a plain HEAD would report it.
    pub meta: ObjectMeta,
    /// The version ID the read resolved to, echoed back to the client.
    pub version_id: Option<String>,
    /// For a `partNumber` read: the object's total part count.
    pub parts_count: Option<i32>,
    /// The origin's stored checksums, present only when the read asked for them.
    pub checksums: Checksums,
}

/// A direct (uncached) ranged read: its metadata-with-extras, the range the
/// origin served, and the body stream.
pub struct DirectGet {
    /// Metadata plus version/part/checksum extras.
    pub meta: DirectMeta,
    /// Absolute half-open byte range served by the origin.
    pub range: Range<u64>,
    /// The bytes of `range`, streamed.
    pub body: BodyStream,
}

/// A GetObjectAttributes request (issue #154): which attributes the client
/// asked for, the parts-pagination cursor, and an optional version pin.
#[derive(Debug, Clone, Default)]
pub struct AttributesRequest {
    /// The `x-amz-object-attributes` list (`ETag`, `Checksum`, `ObjectParts`,
    /// `StorageClass`, `ObjectSize`).
    pub object_attributes: Vec<String>,
    /// `MaxParts` for the `ObjectParts` section.
    pub max_parts: Option<i32>,
    /// `PartNumberMarker` the parts section resumes from.
    pub part_number_marker: Option<i32>,
    /// `?versionId=<id>` — attributes of one specific version.
    pub version_id: Option<String>,
}

/// One part of a multipart object as GetObjectAttributes reports it (issue
/// #154).
pub struct ObjectPartInfo {
    /// The 1-based part number.
    pub part_number: i32,
    /// The part's size in bytes.
    pub size: u64,
    /// The part's stored checksums, forwarded verbatim (usually absent).
    pub checksums: Checksums,
}

/// The parts section of a GetObjectAttributes result (issue #154): a page of
/// parts plus the origin's pagination markers. Absent for a non-multipart
/// object.
pub struct ObjectPartsPage {
    /// The parts on this page.
    pub parts: Vec<ObjectPartInfo>,
    /// Total number of parts across all pages.
    pub total_parts_count: Option<i32>,
    /// The `MaxParts` the origin applied.
    pub max_parts: Option<i32>,
    /// The `PartNumberMarker` this page resumed from.
    pub part_number_marker: Option<i32>,
    /// The marker to resume from when truncated.
    pub next_part_number_marker: Option<i32>,
    /// Whether more parts remain past this page.
    pub is_truncated: bool,
}

/// A GetObjectAttributes result (issue #154), every field forwarded from the
/// origin — Verglas computes none of these itself.
pub struct ObjectAttributes {
    /// The object's `ETag` (unquoted, as the attributes API reports it).
    pub e_tag: Option<String>,
    /// The object's total size in bytes.
    pub object_size: Option<u64>,
    /// The object's storage class.
    pub storage_class: Option<String>,
    /// The object's last-modified time.
    pub last_modified: Option<SystemTime>,
    /// The resolved version ID, if the bucket is versioned.
    pub version_id: Option<String>,
    /// The object-level checksums, if any.
    pub checksums: Checksums,
    /// The parts section, present only for a multipart object.
    pub object_parts: Option<ObjectPartsPage>,
}

/// Read-path errors. The variants map one-to-one onto the S3 error codes the
/// front-end must emit, so implementations decide semantics and the protocol
/// layer only translates.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// The bucket does not exist (`NoSuchBucket`, 404): either the request named
    /// a bucket other than the one this server serves, or the origin reports the
    /// configured bucket is gone.
    #[error("bucket does not exist")]
    NoSuchBucket,
    /// The origin refused the request's credentials (`AccessDenied`, 403),
    /// passed through from the origin unchanged.
    #[error("access denied by origin")]
    AccessDenied,
    /// The object does not exist in the bucket (`NoSuchKey`, 404).
    #[error("key does not exist")]
    NoSuchKey,
    /// The range starts at or beyond the end of the object (`InvalidRange`,
    /// 416). Emitted by implementations that resolve ranges locally (the
    /// cache, #12); the passthrough delegates range validation to the origin.
    #[error("requested range is not satisfiable")]
    InvalidRange,
    /// A `partNumber` read named an invalid part — out of range, or part >1 of a
    /// non-multipart object (`InvalidPart`, 400). Issue #153.
    #[error("invalid part number")]
    InvalidPart,
    /// Any other backend failure (`InternalError`, 500). Carries the
    /// underlying message for logs; never fabricates bytes.
    #[error("backend read failed: {0}")]
    Backend(String),
}

/// Source of object bytes and metadata for the S3 front-end.
///
/// Implementations must be range-aware (serve exactly the resolved range,
/// never the whole object when a range was asked for) and must stream: the
/// returned [`BodyStream`] may not be backed by a full-object buffer unless
/// the object was already resident in memory.
///
/// Keys are logical [`CacheKey`]s so the cache (#12) can look up by the same
/// identity it admits under; the passthrough treats them as bucket + key.
pub trait ObjectRead: Send + Sync + 'static {
    /// Reads `range` of the object at `key`, returning whole-object metadata,
    /// the resolved absolute range, and a stream of exactly that range.
    fn get(
        &self,
        key: &CacheKey,
        range: ReadRange,
    ) -> impl Future<Output = Result<ObjectGet, ReadError>> + Send;

    /// Reads only the metadata of the object at `key`. Must agree with what
    /// [`ObjectRead::get`] would report for the same object (HEAD/GET parity).
    fn head(&self, key: &CacheKey) -> impl Future<Output = Result<ObjectMeta, ReadError>> + Send;

    /// Revalidates the cached mapping for `key`, whose current version is
    /// `etag`, with a conditional metadata request (see [`Revalidation`]) — the
    /// mutable-mapping refresh of issue #14.
    ///
    /// The default issues an unconditional [`ObjectRead::head`] and compares
    /// ETags: correct against any backend, but it transfers the metadata on
    /// every call. A real origin client (the S3 passthrough) overrides it with a
    /// native `If-None-Match` request so an unchanged object costs a bare 304
    /// and no bytes — which is the whole point of revalidating rather than
    /// re-resolving. A missing object maps to [`Revalidation::Vanished`].
    fn revalidate(
        &self,
        key: &CacheKey,
        etag: &str,
    ) -> impl Future<Output = Result<Revalidation, ReadError>> + Send {
        async move {
            match self.head(key).await {
                Ok(meta) if meta.e_tag.as_deref() == Some(etag) => Ok(Revalidation::Unchanged),
                Ok(meta) => Ok(Revalidation::Changed(Box::new(meta))),
                Err(ReadError::NoSuchKey | ReadError::NoSuchBucket) => Ok(Revalidation::Vanished),
                Err(other) => Err(other),
            }
        }
    }

    /// A passthrough (uncached) GET carrying the escape-hatch parameters
    /// (version ID, part number, checksum mode) — issues #153/#154/#156. These
    /// name an immutable, version- or part-scoped view the cache does not model,
    /// so the read goes straight to the origin. Only the passthrough and the
    /// cache engine (which forwards to its backend) implement this; the default
    /// exists so in-memory test readers need not.
    fn get_direct(
        &self,
        _key: &CacheKey,
        _range: ReadRange,
        _options: DirectReadOptions,
    ) -> impl Future<Output = Result<DirectGet, ReadError>> + Send {
        async {
            Err(ReadError::Backend(
                "direct read not supported by this reader".to_owned(),
            ))
        }
    }

    /// The metadata-only twin of [`ObjectRead::get_direct`] (a passthrough
    /// HEAD with the escape-hatch parameters).
    fn head_direct(
        &self,
        _key: &CacheKey,
        _options: DirectReadOptions,
    ) -> impl Future<Output = Result<DirectMeta, ReadError>> + Send {
        async {
            Err(ReadError::Backend(
                "direct head not supported by this reader".to_owned(),
            ))
        }
    }

    /// Fetches the origin's GetObjectAttributes for `key` (issue #154). Pure
    /// passthrough: attributes come from the origin and the cache is never
    /// touched. Default errors for readers that do not model it.
    fn object_attributes(
        &self,
        _key: &CacheKey,
        _request: AttributesRequest,
    ) -> impl Future<Output = Result<ObjectAttributes, ReadError>> + Send {
        async {
            Err(ReadError::Backend(
                "attributes not supported by this reader".to_owned(),
            ))
        }
    }
}

/// Sharing a reader is just sharing the `Arc`: a background producer (warming
/// #168, prefetch #51) holds an `Arc<Engine>` and cheaply clones it per task,
/// so the concrete reader need not be `Clone` itself.
impl<T: ObjectRead + ?Sized> ObjectRead for std::sync::Arc<T> {
    /// Delegates to the pointee.
    fn get(
        &self,
        key: &CacheKey,
        range: ReadRange,
    ) -> impl Future<Output = Result<ObjectGet, ReadError>> + Send {
        (**self).get(key, range)
    }

    /// Delegates to the pointee.
    fn head(&self, key: &CacheKey) -> impl Future<Output = Result<ObjectMeta, ReadError>> + Send {
        (**self).head(key)
    }

    /// Delegates to the pointee.
    fn get_direct(
        &self,
        key: &CacheKey,
        range: ReadRange,
        options: DirectReadOptions,
    ) -> impl Future<Output = Result<DirectGet, ReadError>> + Send {
        (**self).get_direct(key, range, options)
    }

    /// Delegates to the pointee.
    fn head_direct(
        &self,
        key: &CacheKey,
        options: DirectReadOptions,
    ) -> impl Future<Output = Result<DirectMeta, ReadError>> + Send {
        (**self).head_direct(key, options)
    }

    /// Delegates to the pointee.
    fn object_attributes(
        &self,
        key: &CacheKey,
        request: AttributesRequest,
    ) -> impl Future<Output = Result<ObjectAttributes, ReadError>> + Send {
        (**self).object_attributes(key, request)
    }
}
