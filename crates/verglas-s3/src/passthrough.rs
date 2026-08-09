//! Passthrough [`ObjectRead`] and [`ObjectWrite`]: reads go straight to the
//! origin bucket through an [`object_store`] client with no caching, and
//! writes stream straight through to it (writes stay passthrough forever —
//! Verglas is never the only copy of any byte). Both sides route by the
//! request's immutable storage binding and bucket: they resolve the origin client from the [`BackendStores`]
//! seam (`verglas-backend`), which serves a configured set of buckets (#235).
//! A request for a bucket in the set is served (its client built lazily on first
//! request); a request for any other bucket is rejected with `NoSuchBucket`, and
//! a key that does not exist in a served bucket surfaces the origin's own error.
//! The
//! read side is the temporary filler behind the `ObjectRead` interface — the cache
//! engine (issue #12) sits in front of it without the front-end changing.
//!
//! # Raw-request routing (issue #189)
//!
//! `object_store`'s typed `Path` silently corrupts trailing-slash and
//! empty-segment keys on both write and list, and its 0.14 attribute model has
//! no `Expires` in either direction. Where the origin is a real S3 endpoint
//! ([`BackendStores::raw_for`] answers `Some`), the passthrough therefore
//! routes through the byte-exact [`ResilientRawS3`] client (the raw client behind
//! the bucket's SAME concurrency budget, breaker, and retry policy as the
//! typed stack):
//! reads (GET/HEAD/revalidate) and LIST always — the typed client cannot read
//! back the full header surface or list pathological keys — and writes exactly
//! when the typed client would drop data (a key `Path` cannot represent
//! byte-for-byte, or an `Expires` header). Losslessly-representable writes stay
//! on the typed client, which carries the bucket's limiter/retry/breaker
//! stack. A store over an injected typed store (unit tests) has no raw
//! surface and keeps the typed paths throughout.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use bytes::Bytes;
use futures::StreamExt;
use object_store::multipart::PartId;
use object_store::path::Path;
use object_store::{
    Attribute, Attributes, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMultipartOptions,
    PutOptions, PutPayload,
};
use verglas_backend::{
    BackendError, BackendStores, MultipartObjectStore, RawAttributes, RawChecksums,
    RawCompletedPart, RawError, RawObjectMeta, RawRange, RawReadExtras, RawWriteHeaders,
    ResilientRawS3,
};
use verglas_core::CacheKey;

use verglas_core::list::{ListError, ListObject, ListPage, ListRequest, ObjectList};
use verglas_core::read::{
    AttributesRequest, Checksums, DirectGet, DirectMeta, DirectReadOptions, ObjectAttributes,
    ObjectGet, ObjectMeta, ObjectPartInfo, ObjectPartsPage, ObjectRead, ReadError, ReadRange,
    Revalidation, ServedTier, TierCell,
};
use verglas_core::write::{
    CompletedPartRef, CopyOutcome, MultipartCreation, MultipartUploadsPage,
    MultipartUploadsRequest, ObjectWrite, PartInfo, PartUpload, PutOutcome, UploadEntry,
    WriteBodyStream, WriteChecksum, WriteError, WriteMetadata,
};

/// A tier cell pre-set to `Passthrough`: the passthrough reader has no cache in
/// its path, so every read it serves is attributed to the passthrough tier for
/// the request-duration histogram (#46).
fn passthrough_tier() -> TierCell {
    let cell = TierCell::new();
    cell.set(ServedTier::Passthrough);
    cell
}

/// Maps a backend resolution failure onto the read interface's error space. A
/// request for a bucket other than the one this server serves is
/// `NoSuchBucket`; a construction failure is an internal backend error.
fn read_backend_error(error: BackendError) -> ReadError {
    match error {
        BackendError::NoSuchBucket { .. } => ReadError::NoSuchBucket,
        other => ReadError::Backend(other.to_string()),
    }
}

/// Maps a backend resolution failure onto the write interface's error space
/// (the write twin of [`read_backend_error`]).
fn write_backend_error(error: BackendError) -> WriteError {
    match error {
        BackendError::NoSuchBucket { .. } => WriteError::NoSuchBucket,
        other => WriteError::Backend(other.to_string()),
    }
}

/// Maps a backend resolution failure onto the list interface's error space
/// (the list twin of [`read_backend_error`]).
fn list_backend_error(error: BackendError) -> ListError {
    match error {
        BackendError::NoSuchBucket { .. } => ListError::NoSuchBucket,
        other => ListError::Backend(other.to_string()),
    }
}

/// True when `key` survives `object_store`'s typed `Path` byte-for-byte —
/// specifically `Path::from`, which is what every typed call site constructs.
/// `Path::from` silently strips trailing slashes and empty segments AND
/// percent-encodes a whole character set (`# % ~ < > [ ] { } ^ | \ " * ?`,
/// controls, `.`/`..` segments), so a PUT of `1999#` lands at the origin as
/// `1999%23` — wrong bytes at the namespace level. Any such key is raw-only
/// (issue #189).
fn path_faithful(key: &str) -> bool {
    Path::from(key).as_ref() == key
}

/// Maps a raw-client failure onto the read interface's error space (the raw
/// twin of [`map_store_error`]).
fn map_raw_read_error(error: RawError) -> ReadError {
    match error {
        RawError::NoSuchKey => ReadError::NoSuchKey,
        RawError::NoSuchBucket => ReadError::NoSuchBucket,
        RawError::AccessDenied => ReadError::AccessDenied,
        RawError::InvalidRange => ReadError::InvalidRange,
        RawError::InvalidPart => ReadError::InvalidPart,
        RawError::InvalidArgument => {
            ReadError::Backend("origin rejected the request argument".to_owned())
        }
        // 304 outside a conditional request cannot happen; classify as backend.
        RawError::NotModified => ReadError::Backend("unexpected 304".to_owned()),
        RawError::Credentials(message) => {
            ReadError::Backend(format!("origin credentials unavailable: {message}"))
        }
        RawError::Backend(message) => ReadError::Backend(message),
    }
}

/// Maps a raw read error for a `partNumber` read (issue #153). An out-of-range
/// part number is an `InvalidPart` (400) on S3, but some origins (MinIO) answer
/// it as a `416 InvalidRange`; when the read carried a part number, translate
/// that range error into the part error the client expects.
fn map_part_read_error(error: RawError, is_part_read: bool) -> ReadError {
    match map_raw_read_error(error) {
        ReadError::InvalidRange if is_part_read => ReadError::InvalidPart,
        other => other,
    }
}

/// Maps a raw-client failure onto the write interface's error space (the raw
/// twin of [`map_write_error`]).
fn map_raw_write_error(error: RawError) -> WriteError {
    match error {
        RawError::NoSuchKey => WriteError::NoSuchKey,
        RawError::NoSuchBucket => WriteError::NoSuchBucket,
        RawError::AccessDenied => WriteError::AccessDenied,
        RawError::InvalidPart => WriteError::InvalidPart("invalid part number".to_owned()),
        RawError::InvalidRange => WriteError::InvalidRange,
        RawError::InvalidArgument => {
            WriteError::InvalidRequest("the origin rejected a request argument".to_owned())
        }
        RawError::NotModified => WriteError::Backend("unexpected conditional failure".to_owned()),
        RawError::Credentials(message) => {
            WriteError::Backend(format!("origin credentials unavailable: {message}"))
        }
        RawError::Backend(message) => WriteError::Backend(message),
    }
}

/// Converts the raw client's header-faithful metadata into the interface form.
/// The `Expires` string rides through verbatim — the raw path is the only one
/// that can carry it (issue #189).
fn raw_to_object_meta(meta: RawObjectMeta) -> ObjectMeta {
    ObjectMeta {
        size: meta.size,
        e_tag: meta.e_tag,
        last_modified: meta.last_modified,
        content_type: meta.content_type,
        cache_control: meta.cache_control,
        content_disposition: meta.content_disposition,
        content_encoding: meta.content_encoding,
        content_language: meta.content_language,
        expires: meta.expires,
        user_metadata: meta.user_metadata,
    }
}

/// Converts the raw checksum set into the interface form, forwarding every
/// value verbatim (issue #154).
fn raw_to_checksums(checksums: RawChecksums) -> Checksums {
    Checksums {
        crc32: checksums.crc32,
        crc32c: checksums.crc32c,
        crc64nvme: checksums.crc64nvme,
        sha1: checksums.sha1,
        sha256: checksums.sha256,
        checksum_type: checksums.checksum_type,
    }
}

/// Splits raw metadata into the interface's [`DirectMeta`]: the version ID,
/// part count, and checksums the escape-hatch reads surface (issues
/// #153/#154/#156) plus the plain object metadata.
fn raw_to_direct_meta(meta: RawObjectMeta) -> DirectMeta {
    let version_id = meta.version_id.clone();
    let parts_count = meta.parts_count;
    let checksums = raw_to_checksums(meta.checksums.clone());
    DirectMeta {
        meta: raw_to_object_meta(meta),
        version_id,
        parts_count,
        checksums,
    }
}

/// Converts a raw GetObjectAttributes result into the interface form (issue
/// #154). Every field is forwarded from the origin; Verglas computes none.
fn raw_to_attributes(attributes: RawAttributes) -> ObjectAttributes {
    ObjectAttributes {
        e_tag: attributes.e_tag,
        object_size: attributes.object_size,
        storage_class: attributes.storage_class,
        last_modified: attributes.last_modified,
        version_id: attributes.version_id,
        checksums: raw_to_checksums(attributes.checksums),
        object_parts: attributes.object_parts.map(|parts| ObjectPartsPage {
            parts: parts
                .parts
                .into_iter()
                .map(|p| ObjectPartInfo {
                    part_number: p.part_number,
                    size: p.size,
                    checksums: raw_to_checksums(p.checksums),
                })
                .collect(),
            total_parts_count: parts.total_parts_count,
            max_parts: parts.max_parts,
            part_number_marker: parts.part_number_marker,
            next_part_number_marker: parts.next_part_number_marker,
            is_truncated: parts.is_truncated,
        }),
    }
}

/// Builds the raw client's read extras from the interface's direct-read
/// options (version ID, part number, checksum mode).
fn to_raw_read_extras(options: &DirectReadOptions) -> RawReadExtras {
    RawReadExtras {
        version_id: options.version_id.clone(),
        part_number: options.part_number,
        checksum_mode: options.checksum_mode,
    }
}

/// Converts write-path metadata into the raw client's header set, `Expires`
/// and the checksum headers included (issue #154).
fn to_raw_write_headers(metadata: &WriteMetadata) -> RawWriteHeaders {
    RawWriteHeaders {
        content_type: metadata.content_type.clone(),
        cache_control: metadata.cache_control.clone(),
        content_disposition: metadata.content_disposition.clone(),
        content_encoding: metadata.content_encoding.clone(),
        content_language: metadata.content_language.clone(),
        expires: metadata.expires.clone(),
        checksum: to_raw_checksum(&metadata.checksum),
        user_metadata: metadata.user_metadata.clone(),
    }
}

/// Converts an interface [`WriteChecksum`] into the raw client's checksum set
/// (issue #154/#208). A straight field copy; the raw layer decides which of
/// these ride as request headers for each operation.
fn to_raw_checksum(checksum: &WriteChecksum) -> verglas_backend::RawWriteChecksum {
    verglas_backend::RawWriteChecksum {
        algorithm: checksum.algorithm.clone(),
        checksum_type: checksum.checksum_type.clone(),
        crc32: checksum.crc32.clone(),
        crc32c: checksum.crc32c.clone(),
        crc64nvme: checksum.crc64nvme.clone(),
        sha1: checksum.sha1.clone(),
        sha256: checksum.sha256.clone(),
    }
}

/// Rebuilds the raw write-header set from a source object's own metadata, for
/// a raw copy that must retain it (S3's `MetadataDirective: COPY`).
fn raw_headers_from_meta(meta: &RawObjectMeta) -> RawWriteHeaders {
    RawWriteHeaders {
        content_type: meta.content_type.clone(),
        cache_control: meta.cache_control.clone(),
        content_disposition: meta.content_disposition.clone(),
        content_encoding: meta.content_encoding.clone(),
        content_language: meta.content_language.clone(),
        expires: meta.expires.clone(),
        checksum: verglas_backend::RawWriteChecksum::default(),
        user_metadata: meta.user_metadata.clone(),
    }
}

/// Translates the interface range into the raw client's request form (same
/// half-open `Bounded` conversion as [`to_get_range`]).
fn to_raw_range(range: ReadRange) -> RawRange {
    match range {
        ReadRange::Full => RawRange::Full,
        ReadRange::From(first) => RawRange::Offset(first),
        ReadRange::Bounded(first, last) => RawRange::Bounded(first, last.saturating_add(1)),
        ReadRange::Suffix(length) => RawRange::Suffix(length),
    }
}

/// Reads through to the origin, routing by the request's bucket. Production
/// wires the single configured S3 client from
/// `verglas_backend::BackendStore`; tests inject an in-memory-backed store.
pub struct PassthroughRead {
    /// The backend seam; the store for a request's bucket is resolved here.
    /// The server serves a configured bucket set (a bucket outside it is
    /// `NoSuchBucket`).
    stores: Arc<dyn BackendStores>,
}

impl PassthroughRead {
    /// Wraps the backend store. Every read routes to it for the request's
    /// bucket.
    pub fn new(stores: Arc<dyn BackendStores>) -> Self {
        PassthroughRead { stores }
    }

    /// Resolves (building on first sight) the origin store for `bucket`.
    fn store_for(&self, key: &CacheKey) -> Result<Arc<dyn MultipartObjectStore>, ReadError> {
        self.stores
            .store_for(&key.storage_binding_id, &key.bucket)
            .map_err(read_backend_error)
    }

    /// Resolves `bucket`'s byte-exact raw client when the origin is a real S3
    /// endpoint; `None` for injected typed stores (issue #189).
    fn raw_for(&self, key: &CacheKey) -> Result<Option<Arc<ResilientRawS3>>, ReadError> {
        self.stores
            .raw_for(&key.storage_binding_id, &key.bucket)
            .map_err(read_backend_error)
    }

    /// Resolves the raw client the escape-hatch reads (version ID, part number,
    /// checksum mode, attributes — issues #153/#154/#156) REQUIRE: these
    /// parameters have no `object_store` typed equivalent, so an origin with no
    /// raw path cannot serve them. Absence fails the request loudly rather than
    /// silently ignoring the parameter.
    fn raw_required(&self, key: &CacheKey) -> Result<Arc<ResilientRawS3>, ReadError> {
        self.raw_for(key)?.ok_or_else(|| {
            ReadError::Backend(
                "this origin has no raw request path; version/part/checksum reads are unsupported"
                    .to_owned(),
            )
        })
    }
}

/// Translates our HTTP-shaped range into `object_store`'s request form.
/// `Bounded` goes from inclusive-end (HTTP) to half-open (`object_store`).
fn to_get_range(range: ReadRange) -> Option<GetRange> {
    match range {
        ReadRange::Full => None,
        ReadRange::From(first) => Some(GetRange::Offset(first)),
        ReadRange::Bounded(first, last) => Some(GetRange::Bounded(first..last.saturating_add(1))),
        ReadRange::Suffix(length) => Some(GetRange::Suffix(length)),
    }
}

/// Maps `object_store` failures onto the interface's error space. A missing object
/// is `NoSuchKey`; the origin's own `NoSuchBucket` and `AccessDenied` are
/// recognized by their S3 error text and passed through so a served bucket that
/// has gone (or one this node's credentials cannot read) gets the origin's own
/// answer, not a generic 500. Anything else
/// (including a range the origin rejected) surfaces as a backend failure rather
/// than guessing.
fn map_store_error(error: object_store::Error) -> ReadError {
    // Check the origin's S3 code first: a missing *bucket* often arrives as a
    // 404 that `object_store` classifies as `NotFound`, so keying only on the
    // variant would flatten NoSuchBucket into NoSuchKey.
    match origin_s3_code(&error) {
        Some(OriginCode::NoSuchBucket) => ReadError::NoSuchBucket,
        Some(OriginCode::AccessDenied) => ReadError::AccessDenied,
        None => match error {
            object_store::Error::NotFound { .. } => ReadError::NoSuchKey,
            other => ReadError::Backend(other.to_string()),
        },
    }
}

/// The origin S3 error codes the passthrough recognizes and forwards unchanged.
enum OriginCode {
    /// The origin has no such bucket.
    NoSuchBucket,
    /// The origin refused the node's credentials.
    AccessDenied,
}

/// Sniffs an `object_store` error for an origin S3 error code Verglas forwards.
/// `object_store` folds S3 API errors into `Generic`/`Client` variants whose
/// text carries the S3 `<Code>`, so matching on the rendered message is the
/// portable way to recover it (the same approach the multipart mapping uses).
fn origin_s3_code(error: &object_store::Error) -> Option<OriginCode> {
    let text = error.to_string();
    if text.contains("NoSuchBucket") {
        Some(OriginCode::NoSuchBucket)
    } else if text.contains("AccessDenied") {
        Some(OriginCode::AccessDenied)
    } else {
        None
    }
}

/// Builds interface metadata from an `object_store` result. `meta.size` from
/// `object_store` is always the full object size, even for ranged GETs. The
/// system-header attributes and every `x-amz-meta-*` pair the origin returned
/// are surfaced so a GET/HEAD reports back what a PUT stored (issue #143).
fn to_object_meta(
    meta: &object_store::ObjectMeta,
    attributes: &object_store::Attributes,
) -> ObjectMeta {
    let system = |attr| attributes.get(attr).map(|v| v.as_ref().to_owned());
    let mut user_metadata = BTreeMap::new();
    for (attr, value) in attributes {
        if let Attribute::Metadata(key) = attr {
            user_metadata.insert(key.as_ref().to_owned(), value.as_ref().to_owned());
        }
    }
    ObjectMeta {
        size: meta.size,
        e_tag: meta.e_tag.clone(),
        last_modified: Some(SystemTime::from(meta.last_modified)),
        content_type: system(&Attribute::ContentType),
        cache_control: system(&Attribute::CacheControl),
        content_disposition: system(&Attribute::ContentDisposition),
        content_encoding: system(&Attribute::ContentEncoding),
        content_language: system(&Attribute::ContentLanguage),
        // `object_store` 0.14 models no `Expires` attribute; the raw-request
        // read path (issue #189) fills it when present.
        expires: None,
        user_metadata,
    }
}

impl ObjectRead for PassthroughRead {
    /// One origin GET per request: range resolution and clamping happen at
    /// the origin, and the body is plumbed through as a stream untouched.
    /// Against a real S3 origin the request goes over the raw client, so the
    /// key is addressed byte-exactly and the full header set — `Expires`
    /// included, which `object_store` cannot read — comes back (issue #189).
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        if let Some(raw) = self.raw_for(key)? {
            let got = raw
                .get(&key.key, to_raw_range(range))
                .await
                .map_err(map_raw_read_error)?;
            return Ok(ObjectGet {
                meta: raw_to_object_meta(got.meta),
                range: got.range,
                body: got.body.map(|r| r.map_err(map_raw_read_error)).boxed(),
                // Direct-to-origin: no cache tier in the path (#46).
                served_from: passthrough_tier(),
            });
        }
        let store = self.store_for(key)?;
        let options = GetOptions {
            range: to_get_range(range),
            ..GetOptions::default()
        };
        let result = store
            .get_opts(&Path::from(key.key.as_str()), options)
            .await
            .map_err(map_store_error)?;
        let meta = to_object_meta(&result.meta, &result.attributes);
        let range = result.range.clone();
        let body = result
            .into_stream()
            .map(|r| r.map_err(map_store_error))
            .boxed();
        Ok(ObjectGet {
            meta,
            range,
            body,
            served_from: passthrough_tier(),
        })
    }

    /// Metadata-only read. Against a real S3 origin this is a raw HEAD, the
    /// only request shape that reports the complete header set (`Expires`,
    /// issue #189) for any key byte-exactly; typed stores answer through
    /// their attribute model.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        if let Some(raw) = self.raw_for(key)? {
            let meta = raw.head(&key.key).await.map_err(map_raw_read_error)?;
            return Ok(raw_to_object_meta(meta));
        }
        let store = self.store_for(key)?;
        let options = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        let result = store
            .get_opts(&Path::from(key.key.as_str()), options)
            .await
            .map_err(map_store_error)?;
        Ok(to_object_meta(&result.meta, &result.attributes))
    }

    /// Native conditional revalidation (#14): a HEAD carrying
    /// `If-None-Match: <etag>`. The origin answers `304 Not Modified` (no body,
    /// no metadata transfer) when the version still holds, which is what makes
    /// revalidating an unchanged mutable object nearly free — the whole reason
    /// the engine revalidates rather than re-resolving. A changed object returns
    /// `200` with the new metadata; a vanished one returns the origin's 404.
    /// Same raw-first routing as [`PassthroughRead::head`].
    async fn revalidate(&self, key: &CacheKey, etag: &str) -> Result<Revalidation, ReadError> {
        if let Some(raw) = self.raw_for(key)? {
            return match raw.head_if_none_match(&key.key, etag).await {
                Ok(meta) => Ok(Revalidation::Changed(Box::new(raw_to_object_meta(meta)))),
                Err(RawError::NotModified) => Ok(Revalidation::Unchanged),
                Err(RawError::NoSuchKey | RawError::NoSuchBucket) => Ok(Revalidation::Vanished),
                Err(other) => Err(map_raw_read_error(other)),
            };
        }
        let store = self.store_for(key)?;
        let options = GetOptions {
            head: true,
            if_none_match: Some(etag.to_owned()),
            ..GetOptions::default()
        };
        match store.get_opts(&Path::from(key.key.as_str()), options).await {
            Ok(result) => Ok(Revalidation::Changed(Box::new(to_object_meta(
                &result.meta,
                &result.attributes,
            )))),
            // The origin's 304: the presented ETag is still current.
            Err(object_store::Error::NotModified { .. }) => Ok(Revalidation::Unchanged),
            // A missing object/bucket becomes `Vanished` so the caller drops the
            // mapping and re-resolves; any other error propagates.
            Err(other) => match map_store_error(other) {
                ReadError::NoSuchKey | ReadError::NoSuchBucket => Ok(Revalidation::Vanished),
                mapped => Err(mapped),
            },
        }
    }

    /// A version-, part-, or checksum-scoped GET (issues #153/#154/#156),
    /// forwarded straight to the origin over the raw client so the exact
    /// version/part is addressed and the full header surface (`x-amz-version-id`,
    /// `x-amz-mp-parts-count`, `x-amz-checksum-*`) comes back. These parameters
    /// have no `object_store` typed equivalent, so an origin with no raw path
    /// (injected test stores) cannot serve them.
    async fn get_direct(
        &self,
        key: &CacheKey,
        range: ReadRange,
        options: DirectReadOptions,
    ) -> Result<DirectGet, ReadError> {
        let raw = self.raw_required(key)?;
        let extras = to_raw_read_extras(&options);
        let got = raw
            .get_ext(&key.key, to_raw_range(range), &extras)
            .await
            .map_err(|e| map_part_read_error(e, options.part_number.is_some()))?;
        Ok(DirectGet {
            meta: raw_to_direct_meta(got.meta),
            range: got.range,
            body: got.body.map(|r| r.map_err(map_raw_read_error)).boxed(),
        })
    }

    /// The metadata-only twin of [`PassthroughRead::get_direct`] (a raw HEAD
    /// with the escape-hatch parameters attached).
    async fn head_direct(
        &self,
        key: &CacheKey,
        options: DirectReadOptions,
    ) -> Result<DirectMeta, ReadError> {
        let raw = self.raw_required(key)?;
        let extras = to_raw_read_extras(&options);
        let meta = raw
            .head_ext(&key.key, &extras)
            .await
            .map_err(|e| map_part_read_error(e, options.part_number.is_some()))?;
        Ok(raw_to_direct_meta(meta))
    }

    /// GetObjectAttributes (issue #154): forwarded to the origin's attributes
    /// API over the raw client and returned verbatim. Verglas computes no
    /// attribute of its own — the checksums and part list are the origin's.
    async fn object_attributes(
        &self,
        key: &CacheKey,
        request: AttributesRequest,
    ) -> Result<ObjectAttributes, ReadError> {
        let raw = self.raw_required(key)?;
        let attributes = raw
            .get_object_attributes(
                &key.key,
                &request.object_attributes,
                request.max_parts,
                request.part_number_marker,
                request.version_id.as_deref(),
            )
            .await
            .map_err(map_raw_read_error)?;
        Ok(raw_to_attributes(attributes))
    }
}

/// Flush threshold for streamed PUT bodies: bodies that fit under it go to
/// the backend as one atomic PUT; longer ones stream through the backend's
/// multipart API in parts of EXACTLY this size (trailing part excepted).
/// Comfortably above S3's 5 MiB minimum-part-size floor. Exactness is a hard
/// requirement, not tidiness: R2 rejects a multipart completion whose
/// non-trailing parts differ in length (the 2026-08-02 pageserver incident),
/// so the split must never let a part absorb chunk overshoot.
const PUT_PART_SIZE: usize = 8 * 1024 * 1024;

/// What the passthrough remembers about one part it streamed to the backend,
/// so ListParts can be answered (object_store has no ListParts call).
#[derive(Debug, Clone)]
struct TrackedPart {
    /// ETag the backend assigned the part.
    e_tag: String,
    /// Part size in bytes.
    size: u64,
    /// When this node streamed the part.
    last_modified: SystemTime,
}

/// The passthrough's record of one in-flight multipart upload it created.
#[derive(Debug, Default)]
struct TrackedUpload {
    /// Bucket the upload was created in; ListParts with the right ID against a
    /// different bucket must miss (two buckets can hold the same object key).
    bucket: String,
    /// Object key the upload was created for; ListParts with the right ID
    /// but the wrong key must miss, as on S3.
    key: String,
    /// True when the upload was created over the raw path (checksummed, or a
    /// raw-only key). Every later call for it MUST use the same path — a
    /// checksummed upload's parts are stored raw and object_store cannot see or
    /// complete them (issue #208).
    via_raw: bool,
    /// The checksum type (`COMPOSITE`/`FULL_OBJECT`) the client chose at create.
    /// The origin needs it restated on CompleteMultipartUpload for a FULL_OBJECT
    /// upload, but the client sends it only on create — so it is recorded here
    /// and supplied on completion when the completion request omits it (#208).
    checksum_type: Option<String>,
    /// Parts observed so far, by client part number (latest retry wins).
    parts: BTreeMap<u16, TrackedPart>,
}

/// Writes through to the origin, routing by the request's bucket. Every
/// operation returns only after the backend has confirmed it durable — the
/// front-end's ack ordering (issue #21) leans on that.
pub struct PassthroughWrite {
    /// The backend seam; the store for a request's bucket is resolved here.
    /// The server serves a configured bucket set (a bucket outside it is
    /// `NoSuchBucket`). Clients expose the low-level multipart API so backend
    /// upload IDs round-trip to the client.
    stores: Arc<dyn BackendStores>,
    /// In-flight uploads created through this process, kept only to answer
    /// ListParts (`object_store` cannot forward it). Every other multipart
    /// operation is stateless: upload IDs are the backend's own, so
    /// UploadPart/Complete/Abort work even for uploads this map has never
    /// seen. M1 is a cluster of one, so an upload ID missing here means the
    /// upload does not exist — except across a server restart, where
    /// ListParts (only) forgets live uploads until the backend client forwards
    /// it natively. Keyed by upload ID alone; the tracked entry carries the
    /// bucket so an ID cannot be replayed against the wrong bucket.
    uploads: Mutex<HashMap<String, TrackedUpload>>,
}

impl PassthroughWrite {
    /// Wraps the backend store. Every write routes to it for the request's
    /// bucket.
    pub fn new(stores: Arc<dyn BackendStores>) -> Self {
        PassthroughWrite {
            stores,
            uploads: Mutex::new(HashMap::new()),
        }
    }

    /// Resolves the origin store for `bucket`. A bucket other than the one this
    /// server serves is `NoSuchBucket`.
    fn store_for(&self, key: &CacheKey) -> Result<Arc<dyn MultipartObjectStore>, WriteError> {
        self.stores
            .store_for(&key.storage_binding_id, &key.bucket)
            .map_err(write_backend_error)
    }

    /// Resolves the raw client a data-dropping-if-typed write REQUIRES: a key
    /// `Path` cannot represent, or an `Expires` header. Absence of the raw
    /// surface fails the request loudly — degrading to the typed client would
    /// store wrong bytes at the namespace/header level (issue #189).
    fn raw_required(&self, key: &CacheKey, why: &str) -> Result<Arc<ResilientRawS3>, WriteError> {
        self.stores
            .raw_for(&key.storage_binding_id, &key.bucket)
            .map_err(write_backend_error)?
            .ok_or_else(|| {
                WriteError::Unsupported(format!(
                    "this origin has no raw request path, and the typed client would corrupt {why}"
                ))
            })
    }

    /// Locks the upload-tracking map (poisoning is unreachable: no code
    /// panics while holding the lock, so recover the data if it ever is).
    fn uploads(&self) -> std::sync::MutexGuard<'_, HashMap<String, TrackedUpload>> {
        self.uploads.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Whether the tracked upload `upload_id` for `key` was created over the raw
    /// path, so UploadPart/Complete route the same way even when their own
    /// arguments carry no checksum (issue #208). An upload this node never
    /// tracked (e.g. after a restart) falls back to the key-faithfulness
    /// predicate — the same default the pre-checksum code used.
    fn upload_via_raw(&self, key: &CacheKey, upload_id: &str) -> bool {
        self.uploads()
            .get(upload_id)
            .filter(|u| u.bucket == key.bucket && u.key == key.key)
            .map(|u| u.via_raw)
            .unwrap_or_else(|| !path_faithful(&key.key))
    }

    /// The checksum type recorded when the upload `upload_id` for `key` was
    /// created, if this node tracked it (issue #208). The completion restates it
    /// so a FULL_OBJECT upload's object checksum is verified correctly; the
    /// client sends the type only on create.
    fn upload_checksum_type(&self, key: &CacheKey, upload_id: &str) -> Option<String> {
        self.uploads()
            .get(upload_id)
            .filter(|u| u.bucket == key.bucket && u.key == key.key)
            .and_then(|u| u.checksum_type.clone())
    }
}

/// Sorts a completion manifest by part number and enforces contiguity from 1
/// (both the typed and raw completion APIs are positional), returning the parts
/// in part order — ETags and any per-part checksums intact (issue #208).
/// Non-contiguous numbers are rejected rather than silently renumbered —
/// renumbering would make the origin verify the wrong ETags. Real S3 clients
/// number parts 1..=N.
fn contiguous_parts(mut parts: Vec<CompletedPartRef>) -> Result<Vec<CompletedPartRef>, WriteError> {
    parts.sort_by_key(|p| p.part_number);
    for (idx, part) in parts.iter().enumerate() {
        if usize::from(part.part_number) != idx + 1 {
            return Err(WriteError::InvalidPart(format!(
                "part numbers must be contiguous from 1; found part {} at position {}",
                part.part_number,
                idx + 1
            )));
        }
    }
    Ok(parts)
}

/// Streams `body` to the origin over the raw client in bounded memory,
/// preserving the key's exact bytes and every metadata header (`Expires`
/// included): a body that fits one part buffer becomes a single raw PUT (S3
/// demands a known `Content-Length`, so the slice is materialized first);
/// anything longer streams through the raw multipart trio in parts of exactly
/// [`PUT_PART_SIZE`] (R2's uniform-length rule), aborted on any failure so
/// nothing partial becomes visible — the raw mirror of
/// [`stream_body_to_store`].
async fn raw_put(
    raw: &ResilientRawS3,
    key: &str,
    headers: RawWriteHeaders,
    mut body: WriteBodyStream,
) -> Result<PutOutcome, WriteError> {
    let mut slicer = PartSlicer::new(&mut body);
    let first = slicer.next_part(PUT_PART_SIZE).await?;
    if first.ended {
        let outcome = raw
            .put(key, headers, first.chunks)
            .await
            .map_err(map_raw_write_error)?;
        return Ok(PutOutcome {
            e_tag: outcome.e_tag,
            checksums: raw_to_checksums(outcome.checksums),
            version_id: outcome.version_id,
        });
    }

    // Large object: raw multipart, parts strictly one at a time so memory
    // stays bounded at ~one part. This is the internal streaming path for a
    // plain (uncached) PUT — no client checksums are involved here, so every
    // part and the completion carry empty checksum sets.
    let upload_id = raw
        .create_multipart(key, headers)
        .await
        .map_err(map_raw_write_error)?
        .upload_id;
    let mut part_etags = Vec::new();
    let mut slice = first;
    loop {
        let ended = slice.ended;
        match raw
            .put_part(
                key,
                &upload_id,
                part_etags.len() + 1,
                verglas_backend::RawWriteChecksum::default(),
                slice.chunks,
            )
            .await
        {
            Ok(part) => part_etags.push(part.e_tag),
            Err(error) => {
                // Best-effort cleanup; the origin's lifecycle rules are the
                // backstop for parts an abort could not reach.
                let _ = raw.abort_multipart(key, &upload_id).await;
                return Err(map_raw_write_error(error));
            }
        }
        if ended {
            break;
        }
        slice = match slicer.next_part(PUT_PART_SIZE).await {
            Ok(slice) => slice,
            Err(error) => {
                let _ = raw.abort_multipart(key, &upload_id).await;
                return Err(error);
            }
        };
        // The body ended exactly on the previous part boundary; there is no
        // final short part to send.
        if slice.len == 0 {
            break;
        }
    }
    let raw_parts = part_etags
        .into_iter()
        .map(|e_tag| RawCompletedPart {
            e_tag,
            checksums: RawChecksums::default(),
        })
        .collect();
    let result = raw
        .complete_multipart(
            key,
            &upload_id,
            raw_parts,
            verglas_backend::RawWriteChecksum::default(),
        )
        .await
        .map_err(map_raw_write_error)?;
    Ok(PutOutcome {
        e_tag: result.e_tag,
        checksums: raw_to_checksums(result.checksums),
        version_id: result.version_id,
    })
}

/// Recovers the origin's S3 `<Code>` from an `object_store` write/multipart
/// error and maps it to the matching client-facing [`WriteError`], so a 4xx
/// the origin raised reaches the client as a 4xx instead of being flattened
/// to `500 InternalError` (issue #146). `object_store` folds S3 API errors
/// into `Generic`/`Client` variants whose rendered text carries the `<Code>`,
/// so matching the message is the portable way to recover it (the same
/// approach [`origin_s3_code`] uses on the read side). Returns `None` when the
/// message names no recognized client-error code, leaving the caller to
/// classify by `object_store` variant.
fn origin_write_code(error: &object_store::Error) -> Option<WriteError> {
    let text = error.to_string();
    // Ordered most-specific first: `EntityTooSmall` before the generic
    // part/request codes so a too-small final part is not mis-labeled.
    if text.contains("NoSuchBucket") {
        Some(WriteError::NoSuchBucket)
    } else if text.contains("AccessDenied") {
        Some(WriteError::AccessDenied)
    } else if text.contains("EntityTooSmall") {
        Some(WriteError::EntityTooSmall(text))
    } else if text.contains("InvalidPart") {
        Some(WriteError::InvalidPart(text))
    } else if text.contains("InvalidRequest") {
        Some(WriteError::InvalidRequest(text))
    } else if text.contains("NoSuchUpload") {
        Some(WriteError::NoSuchUpload)
    } else {
        None
    }
}

/// Maps `object_store` failures on whole-object writes onto the interface's error
/// space. The origin's own 4xx codes are recovered first (issue #146); a bare
/// `NotFound` is `NoSuchKey`; anything else is a backend failure.
fn map_write_error(error: object_store::Error) -> WriteError {
    if let Some(mapped) = origin_write_code(&error) {
        return mapped;
    }
    match error {
        object_store::Error::NotFound { .. } => WriteError::NoSuchKey,
        other => WriteError::Backend(other.to_string()),
    }
}

/// Maps `object_store` failures on multipart operations. The origin's client
/// errors are recovered first — a wrong part ETag (`InvalidPart`) or a
/// too-small non-final part (`EntityTooSmall`) must reach the client as a 400,
/// not a 500 (issue #146). Otherwise "not found" means the upload ID (real S3
/// answers an unknown ID with 404); the in-memory store reports the same
/// condition as a generic error with a fixed message, matched here too so
/// tests see the `NoSuchUpload` wire shape real S3 produces.
fn map_multipart_error(error: object_store::Error) -> WriteError {
    if let Some(mapped) = origin_write_code(&error) {
        return mapped;
    }
    match error {
        object_store::Error::NotFound { .. } => WriteError::NoSuchUpload,
        other if other.to_string().contains("MultipartUpload not found") => {
            WriteError::NoSuchUpload
        }
        other => WriteError::Backend(other.to_string()),
    }
}

/// Builds the `object_store` attribute set for a write from [`WriteMetadata`]:
/// each system header the store understands (`Content-Type`, `Cache-Control`,
/// `Content-Encoding`, `Content-Disposition`, `Content-Language`) plus every
/// user-metadata pair as `x-amz-meta-<key>`. The backend's S3 client renders
/// these as the matching request headers; a store that does not model an
/// attribute simply drops it (issue #143).
fn write_attributes(metadata: &WriteMetadata) -> Attributes {
    let mut attributes = Attributes::new();
    if let Some(value) = &metadata.content_type {
        attributes.insert(Attribute::ContentType, value.clone().into());
    }
    if let Some(value) = &metadata.cache_control {
        attributes.insert(Attribute::CacheControl, value.clone().into());
    }
    if let Some(value) = &metadata.content_disposition {
        attributes.insert(Attribute::ContentDisposition, value.clone().into());
    }
    if let Some(value) = &metadata.content_encoding {
        attributes.insert(Attribute::ContentEncoding, value.clone().into());
    }
    if let Some(value) = &metadata.content_language {
        attributes.insert(Attribute::ContentLanguage, value.clone().into());
    }
    for (key, value) in &metadata.user_metadata {
        attributes.insert(
            Attribute::Metadata(key.clone().into()),
            value.clone().into(),
        );
    }
    attributes
}

/// One buffered slice of a client body: the chunks as received, their total
/// length, and whether the stream ended inside this slice.
struct BufferedSlice {
    /// Chunks in arrival order (never coalesced — no copying; at most the
    /// boundary chunk is zero-copy split).
    chunks: Vec<Bytes>,
    /// Total bytes across `chunks`.
    len: usize,
    /// True if the body finished before `target` bytes arrived.
    ended: bool,
}

/// Slices a client body into exact-length parts for the internal multipart
/// split. R2 requires every non-trailing part of one upload to be
/// byte-identical in length (the 2026-08-02 InvalidPart incident), so a part
/// must never overshoot its target by "whatever the boundary chunk brought":
/// the boundary chunk is split at the target (a zero-copy `Bytes::split_off`)
/// and its tail carried into the next part. Memory stays bounded at ~one part
/// plus the carried tail — the same ceiling the old overshooting fill had.
struct PartSlicer<'a> {
    /// The client body being sliced.
    body: &'a mut WriteBodyStream,
    /// Tail of the chunk that crossed the previous part boundary, emitted
    /// first in the next part.
    carry: Option<Bytes>,
}

impl<'a> PartSlicer<'a> {
    /// Starts slicing `body` with nothing carried over.
    fn new(body: &'a mut WriteBodyStream) -> Self {
        PartSlicer { body, carry: None }
    }

    /// Buffers the next part: exactly `target` bytes, or fewer with `ended`
    /// set when the body finishes first. A later call after `ended` returns an
    /// empty ended slice, which callers treat as "no trailing part".
    async fn next_part(&mut self, target: usize) -> Result<BufferedSlice, WriteError> {
        let mut chunks = Vec::new();
        let mut len = 0usize;
        if let Some(chunk) = self.carry.take() {
            len += chunk.len();
            chunks.push(chunk);
        }
        while len < target {
            match self.body.next().await {
                Some(Ok(chunk)) => {
                    len += chunk.len();
                    chunks.push(chunk);
                }
                Some(Err(error)) => return Err(error),
                None => {
                    return Ok(BufferedSlice {
                        chunks,
                        len,
                        ended: true,
                    });
                }
            }
        }
        // Trim the overshoot back to the exact target. The boundary chunk
        // always contributes at least one byte to this part (the loop only
        // pulled it because the part was still short), so the kept head is
        // never empty.
        if len > target
            && let Some(last) = chunks.last_mut()
        {
            let tail = last.split_off(last.len() - (len - target));
            self.carry = Some(tail);
            len = target;
        }
        Ok(BufferedSlice {
            chunks,
            len,
            ended: false,
        })
    }
}

/// Streams `body` into `store` at `path`, carrying `attributes`, in bounded
/// memory: a body that fits in one part buffer becomes a single atomic PUT;
/// anything longer streams through the origin's multipart API in parts of
/// exactly [`PUT_PART_SIZE`] (R2's uniform-length rule), aborted on any
/// failure so nothing partial becomes visible. Returns only once the origin
/// confirms the object durable. Shared by [`PassthroughWrite::put`] and the
/// copy-with-REPLACE path, which re-streams a source object through a fresh
/// write to attach new metadata.
async fn stream_body_to_store(
    store: &Arc<dyn MultipartObjectStore>,
    path: &Path,
    attributes: Attributes,
    body: &mut WriteBodyStream,
) -> Result<PutOutcome, WriteError> {
    let mut slicer = PartSlicer::new(body);
    let first = slicer.next_part(PUT_PART_SIZE).await?;
    if first.ended {
        // Small object: one atomic PUT, the origin's ETag comes back on the
        // PutResult.
        let options = PutOptions {
            attributes,
            ..PutOptions::default()
        };
        let result = store
            .put_opts(path, PutPayload::from_iter(first.chunks), options)
            .await
            .map_err(map_write_error)?;
        return Ok(PutOutcome {
            e_tag: result.e_tag,
            // The origin's version ID for the write (issue #156), when the
            // bucket is versioned; `None` otherwise.
            version_id: result.version,
            ..PutOutcome::default()
        });
    }

    // Large object: stream through the origin's multipart machinery. Parts are
    // uploaded strictly one at a time — slower than fanning out, but memory
    // stays bounded at ~one part.
    let options = PutMultipartOptions {
        attributes,
        ..PutMultipartOptions::default()
    };
    let mut upload = store
        .put_multipart_opts(path, options)
        .await
        .map_err(map_write_error)?;
    let mut slice = first;
    loop {
        let ended = slice.ended;
        if let Err(error) = upload.put_part(PutPayload::from_iter(slice.chunks)).await {
            // Best-effort cleanup; the origin's lifecycle rules are the
            // backstop for parts an abort could not reach.
            let _ = upload.abort().await;
            return Err(map_write_error(error));
        }
        if ended {
            break;
        }
        slice = match slicer.next_part(PUT_PART_SIZE).await {
            Ok(slice) => slice,
            Err(error) => {
                let _ = upload.abort().await;
                return Err(error);
            }
        };
        // The body ended exactly on the previous part boundary; there is no
        // final short part to send.
        if slice.len == 0 {
            break;
        }
    }
    let result = upload.complete().await.map_err(map_write_error)?;
    Ok(PutOutcome {
        e_tag: result.e_tag,
        version_id: result.version,
        ..PutOutcome::default()
    })
}

impl ObjectWrite for PassthroughWrite {
    /// Streams the body to the origin in bounded memory, carrying the object's
    /// metadata (system headers + user `x-amz-meta-*`, issue #143) so later
    /// reads report it back. Returns only once the origin has confirmed the
    /// object durable. A write the typed client would corrupt — a key `Path`
    /// mangles, or an `Expires` header it cannot carry — goes over the raw
    /// client instead (issue #189); everything else keeps the typed path and
    /// its resilience stack.
    async fn put(
        &self,
        key: &CacheKey,
        metadata: WriteMetadata,
        mut body: WriteBodyStream,
    ) -> Result<PutOutcome, WriteError> {
        // A checksum the origin must verify (issue #154) also forces the raw
        // path: the typed client cannot carry `x-amz-checksum-*` headers, and
        // silently dropping them would let a corrupt body pass as valid.
        if !path_faithful(&key.key) || metadata.expires.is_some() || !metadata.checksum.is_empty() {
            let raw = self.raw_required(key, "the key, an Expires header, or a checksum")?;
            return raw_put(&raw, &key.key, to_raw_write_headers(&metadata), body).await;
        }
        let store = self.store_for(key)?;
        let path = Path::from(key.key.as_str());
        stream_body_to_store(&store, &path, write_attributes(&metadata), &mut body).await
    }

    /// Forwards the delete. A missing key is success — S3 deletes are
    /// idempotent and answer 204 either way. A key the typed `Path` would
    /// mangle is deleted over the raw client, so the EXACT key is removed
    /// (issue #189) — deleting the mangled twin would be a wrong-namespace
    /// write.
    async fn delete(&self, key: &CacheKey) -> Result<(), WriteError> {
        if !path_faithful(&key.key) {
            let raw = self.raw_required(key, "the key")?;
            return match raw.delete(&key.key).await {
                Ok(()) | Err(RawError::NoSuchKey) => Ok(()),
                Err(other) => Err(map_raw_write_error(other)),
            };
        }
        let store = self.store_for(key)?;
        match store.delete(&Path::from(key.key.as_str())).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(other) => Err(WriteError::Backend(other.to_string())),
        }
    }

    /// Deletes sequentially, one origin call per key, collecting per-key
    /// results. Sequential is deliberate for now: batch sizes are ≤1000 and
    /// correctness reporting is simpler; #18's client can parallelize.
    async fn delete_batch(
        &self,
        keys: &[CacheKey],
    ) -> Result<Vec<Result<(), WriteError>>, WriteError> {
        // Each key routes to its own bucket's client (keys in one batch may
        // name different buckets); per-key failures land in the inner results.
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.delete(key).await);
        }
        Ok(results)
    }

    /// Forwards a server-side copy, then reads back the destination's
    /// metadata (one extra HEAD — `object_store::copy` reports nothing, and
    /// S3's CopyObjectResult XML needs the new ETag and mtime). Same-bucket
    /// only in v1: origin clients are bucket-scoped, so a cross-bucket copy
    /// (which would be a GET+PUT across two clients) is refused with a clear
    /// not-supported error rather than silently doing the wrong thing.
    ///
    /// `metadata` carries the S3 `MetadataDirective` (issue #143): `None`
    /// (`COPY`) is a true server-side copy that retains the source's stored
    /// metadata; `Some(md)` (`REPLACE`) has no server-side equivalent in
    /// `object_store` (its copy cannot set metadata), so the source is
    /// re-streamed through a fresh PUT that attaches `md`. Both are
    /// same-bucket, so one client serves the read and the write.
    async fn copy(
        &self,
        source: &CacheKey,
        dest: &CacheKey,
        metadata: Option<WriteMetadata>,
    ) -> Result<CopyOutcome, WriteError> {
        if source.bucket != dest.bucket {
            return Err(WriteError::CrossBucketCopy);
        }
        // A copy the typed client would corrupt — either key unrepresentable,
        // or a REPLACE carrying `Expires` — is served raw: GET the source and
        // re-stream it into the destination (retaining the source's own
        // headers for the COPY directive), then HEAD the result (issue #189).
        // Not server-side, but byte-identical — slow is acceptable, wrong is
        // never.
        if !path_faithful(&source.key)
            || !path_faithful(&dest.key)
            || metadata
                .as_ref()
                .is_some_and(|metadata| metadata.expires.is_some())
        {
            let raw = self.raw_required(dest, "a copy key or the Expires header")?;
            let got = raw
                .get(&source.key, RawRange::Full)
                .await
                .map_err(map_raw_write_error)?;
            let headers = match &metadata {
                Some(replacement) => to_raw_write_headers(replacement),
                None => raw_headers_from_meta(&got.meta),
            };
            let body: WriteBodyStream = got
                .body
                .map(|chunk| chunk.map_err(|e| WriteError::Backend(e.to_string())))
                .boxed();
            raw_put(&raw, &dest.key, headers, body).await?;
            let head = raw.head(&dest.key).await.map_err(map_raw_write_error)?;
            return Ok(CopyOutcome {
                e_tag: head.e_tag,
                last_modified: head.last_modified,
            });
        }
        let store = self.store_for(dest)?;
        let from = Path::from(source.key.as_str());
        let to = Path::from(dest.key.as_str());
        match metadata {
            // COPY directive: server-side copy retains the source metadata.
            None => store.copy(&from, &to).await.map_err(map_write_error)?,
            // REPLACE directive: re-stream the source through a PUT carrying
            // the new metadata. A missing source surfaces as NoSuchKey.
            Some(md) => {
                let source_result = store
                    .get_opts(&from, GetOptions::default())
                    .await
                    .map_err(map_write_error)?;
                let mut body: WriteBodyStream = source_result
                    .into_stream()
                    .map(|chunk| chunk.map_err(|e| WriteError::Backend(e.to_string())))
                    .boxed();
                stream_body_to_store(&store, &to, write_attributes(&md), &mut body).await?;
            }
        }
        let head = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        let result = store.get_opts(&to, head).await.map_err(map_write_error)?;
        Ok(CopyOutcome {
            e_tag: result.meta.e_tag.clone(),
            last_modified: Some(SystemTime::from(result.meta.last_modified)),
        })
    }

    /// Creates the upload at the origin and returns the origin's own upload
    /// ID, so every later call can be forwarded statelessly. The object's
    /// metadata (issue #143) is attached to the upload so the completed object
    /// reports it back. The upload is also recorded locally, purely so
    /// ListParts can be answered.
    ///
    /// A creation the typed client would corrupt — a key `Path` cannot
    /// represent byte-exactly, or an `Expires` header its attribute model
    /// cannot carry — goes over the raw client instead (issue #189). Upload
    /// IDs are the origin's own either way, so later lifecycle calls route by
    /// the same key-faithfulness predicate and interoperate.
    async fn create_multipart(
        &self,
        key: &CacheKey,
        metadata: WriteMetadata,
    ) -> Result<MultipartCreation, WriteError> {
        // A checksummed upload MUST take the raw path for the whole lifecycle:
        // `object_store`'s typed multipart API cannot carry checksum headers, so
        // per-part and composite checksums would be silently dropped (issue #208).
        let via_raw =
            !path_faithful(&key.key) || metadata.expires.is_some() || !metadata.checksum.is_empty();
        // The client names the checksum type only on create; the origin needs it
        // restated on a FULL_OBJECT completion, so record it now (issue #208).
        let checksum_type = metadata.checksum.checksum_type.clone();
        let creation = if via_raw {
            let raw = self.raw_required(key, "the key, the Expires header, or a checksum")?;
            let created = raw
                .create_multipart(&key.key, to_raw_write_headers(&metadata))
                .await
                .map_err(map_raw_write_error)?;
            MultipartCreation {
                upload_id: created.upload_id,
                checksum_algorithm: created.checksum_algorithm,
                checksum_type: created.checksum_type,
            }
        } else {
            let store = self.store_for(key)?;
            let options = PutMultipartOptions {
                attributes: write_attributes(&metadata),
                ..PutMultipartOptions::default()
            };
            let upload_id = store
                .create_multipart_opts(&Path::from(key.key.as_str()), options)
                .await
                .map_err(map_write_error)?;
            MultipartCreation {
                upload_id,
                ..MultipartCreation::default()
            }
        };
        self.uploads().insert(
            creation.upload_id.clone(),
            TrackedUpload {
                bucket: key.bucket.clone(),
                key: key.key.clone(),
                via_raw,
                checksum_type,
                parts: BTreeMap::new(),
            },
        );
        Ok(creation)
    }

    /// Streams one part to the origin as EXACTLY one backend part — same part
    /// number, same bytes. Never re-chunked, merged, or split: R2 requires
    /// every non-trailing part of an upload to be byte-identical in length, so
    /// the client's part boundaries are the only correct ones. The part body
    /// is buffered (a part is bounded by the client's part size — Iceberg
    /// writers use tens of MiB) because both the typed low-level multipart
    /// call and the raw part PUT (which needs a `Content-Length`) take a
    /// materialized payload; #18's client can stream parts through. Parts of a
    /// raw-only key go over the raw client, byte-exact (issue #189).
    async fn upload_part(
        &self,
        key: &CacheKey,
        upload_id: &str,
        part_number: u16,
        checksum: WriteChecksum,
        mut body: WriteBodyStream,
    ) -> Result<PartUpload, WriteError> {
        // `usize::MAX` buffers the whole part in one slice; the slicer never
        // splits it, so the client's part arrives at the backend intact.
        let mut slice = PartSlicer::new(&mut body).next_part(usize::MAX).await?;
        debug_assert!(slice.ended);
        let size = slice.len as u64;
        let chunks = std::mem::take(&mut slice.chunks);
        // S3 part numbers are 1-based; object_store part indexes are 0-based.
        // The front-end validates the 1..=10000 range; guard anyway so a
        // misuse of the interface cannot underflow.
        let part_idx = usize::from(
            part_number
                .checked_sub(1)
                .ok_or_else(|| WriteError::InvalidPart("part numbers start at 1".to_owned()))?,
        );
        // Route this part exactly as the upload was created. A checksummed
        // upload was created raw and its parts must go raw too — even a part
        // whose own request omitted a checksum value (boto3 computes it and
        // sends the header, but a manifest-only part might not), because the
        // whole upload's parts live on one path (issue #208).
        let part = if self.upload_via_raw(key, upload_id) || !checksum.is_empty() {
            let raw = self.raw_required(key, "the key or a checksum")?;
            let uploaded = raw
                .put_part(
                    &key.key,
                    upload_id,
                    usize::from(part_number),
                    to_raw_checksum(&checksum),
                    chunks,
                )
                .await
                .map_err(map_raw_write_error)?;
            PartUpload {
                e_tag: uploaded.e_tag,
                checksums: raw_to_checksums(uploaded.checksums),
            }
        } else {
            let store = self.store_for(key)?;
            let content_id = store
                .put_part(
                    &Path::from(key.key.as_str()),
                    &upload_id.to_owned(),
                    part_idx,
                    PutPayload::from_iter(chunks),
                )
                .await
                .map_err(map_multipart_error)?
                .content_id;
            PartUpload {
                e_tag: content_id,
                checksums: Checksums::default(),
            }
        };
        let mut uploads = self.uploads();
        if let Some(upload) = uploads.get_mut(upload_id)
            && upload.bucket == key.bucket
            && upload.key == key.key
        {
            upload.parts.insert(
                part_number,
                TrackedPart {
                    e_tag: part.e_tag.clone(),
                    size,
                    last_modified: SystemTime::now(),
                },
            );
        }
        Ok(part)
    }

    /// Forwards the completion manifest. Both completion APIs are positional
    /// (part `i` of the vec must be part number `i+1`), so non-contiguous part
    /// numbers are rejected by [`contiguous_parts`] instead of being silently
    /// renumbered — renumbering would make the origin verify the wrong ETags.
    /// Real S3 clients number parts 1..=N. A checksummed completion (per-part
    /// or object-level, issue #208), or a raw-only key (issue #189), completes
    /// over the raw client so the checksums and exact bytes survive; the origin
    /// combines the part checksums and echoes the composite back.
    async fn complete_multipart(
        &self,
        key: &CacheKey,
        upload_id: &str,
        parts: Vec<CompletedPartRef>,
        object_checksum: WriteChecksum,
    ) -> Result<PutOutcome, WriteError> {
        let parts = contiguous_parts(parts)?;
        // A FULL_OBJECT upload's completion MUST carry `x-amz-checksum-type`, but
        // the client names the type only on create. Restate the type recorded at
        // create when the completion request left it out (issue #208).
        let mut object_checksum = object_checksum;
        if object_checksum.checksum_type.is_none() {
            object_checksum.checksum_type = self.upload_checksum_type(key, upload_id);
        }
        // Complete on the same path the upload was created (issue #208): a
        // checksummed upload's parts live raw, so its completion must be raw
        // even when the manifest itself carries no checksums.
        let outcome = if self.upload_via_raw(key, upload_id) || !object_checksum.is_empty() {
            let raw = self.raw_required(key, "the key or a checksum")?;
            let raw_parts = parts
                .into_iter()
                .map(|p| RawCompletedPart {
                    e_tag: p.e_tag,
                    checksums: RawChecksums {
                        crc32: p.checksums.crc32,
                        crc32c: p.checksums.crc32c,
                        crc64nvme: p.checksums.crc64nvme,
                        sha1: p.checksums.sha1,
                        sha256: p.checksums.sha256,
                        checksum_type: p.checksums.checksum_type,
                    },
                })
                .collect();
            let done = raw
                .complete_multipart(
                    &key.key,
                    upload_id,
                    raw_parts,
                    to_raw_checksum(&object_checksum),
                )
                .await
                .map_err(map_raw_write_error)?;
            PutOutcome {
                e_tag: done.e_tag,
                checksums: raw_to_checksums(done.checksums),
                version_id: done.version_id,
            }
        } else {
            let store = self.store_for(key)?;
            let part_ids = parts
                .into_iter()
                .map(|p| PartId {
                    content_id: p.e_tag,
                })
                .collect();
            let e_tag = store
                .complete_multipart(
                    &Path::from(key.key.as_str()),
                    &upload_id.to_owned(),
                    part_ids,
                )
                .await
                .map_err(map_multipart_error)?
                .e_tag;
            PutOutcome {
                e_tag,
                ..PutOutcome::default()
            }
        };
        // Only forget the upload once the origin confirmed assembly; a
        // failed completion leaves the upload (and ListParts) alive, as on
        // S3.
        self.uploads().remove(upload_id);
        Ok(outcome)
    }

    /// Forwards the abort; the origin discards the parts. Local tracking is
    /// dropped only on origin-confirmed success. Raw-only keys abort over the
    /// raw client (issue #189).
    async fn abort_multipart(&self, key: &CacheKey, upload_id: &str) -> Result<(), WriteError> {
        if !path_faithful(&key.key) {
            let raw = self.raw_required(key, "the key")?;
            raw.abort_multipart(&key.key, upload_id)
                .await
                .map_err(map_raw_write_error)?;
            self.uploads().remove(upload_id);
            return Ok(());
        }
        let store = self.store_for(key)?;
        store
            .abort_multipart(&Path::from(key.key.as_str()), &upload_id.to_owned())
            .await
            .map_err(map_multipart_error)?;
        self.uploads().remove(upload_id);
        Ok(())
    }

    /// Answers ListParts from this node's own records — every part of a live
    /// upload was streamed through this process (see the `uploads` field for
    /// the restart caveat), and `object_store` cannot forward the call. An
    /// unknown ID, or the right ID under the wrong bucket/key, is
    /// `NoSuchUpload`, matching S3.
    async fn list_parts(
        &self,
        key: &CacheKey,
        upload_id: &str,
    ) -> Result<Vec<PartInfo>, WriteError> {
        let uploads = self.uploads();
        let upload = uploads
            .get(upload_id)
            .filter(|u| u.bucket == key.bucket && u.key == key.key)
            .ok_or(WriteError::NoSuchUpload)?;
        Ok(upload
            .parts
            .iter()
            .map(|(part_number, part)| PartInfo {
                part_number: *part_number,
                e_tag: Some(part.e_tag.clone()),
                size: part.size,
                last_modified: Some(part.last_modified),
            })
            .collect())
    }

    /// UploadPartCopy (issue #153): server-side copies `source` (optionally the
    /// `copy_range` window) into part `part_number` of the upload on `dest`,
    /// over the raw client so the copy-source header carries the exact key.
    /// Same-bucket only — the front-end rejects a cross-bucket source before
    /// this runs, and origin clients are bucket-scoped. The new part is tracked
    /// locally so a later ListParts sees it; its size comes from the range when
    /// given, else from a HEAD of the source.
    async fn upload_part_copy(
        &self,
        source: &CacheKey,
        dest: &CacheKey,
        upload_id: &str,
        part_number: u16,
        copy_range: Option<String>,
    ) -> Result<CopyOutcome, WriteError> {
        let raw = self.raw_required(dest, "an UploadPartCopy")?;
        let copy_source = format!(
            "/{}/{}",
            source.bucket,
            verglas_backend::encode_key(&source.key)
        );
        let (e_tag, last_modified) = raw
            .upload_part_copy(
                &dest.key,
                upload_id,
                part_number,
                &copy_source,
                copy_range.as_deref(),
            )
            .await
            .map_err(map_raw_write_error)?;
        // Track the new part so ListParts (local, best-effort) reflects it.
        // The copied part's size is the range length, or the whole source (read
        // from the SOURCE bucket's client, which may differ from the dest).
        let size = match parse_copy_range_len(copy_range.as_deref()) {
            Some(len) => len,
            None => match self
                .stores
                .raw_for(&source.storage_binding_id, &source.bucket)
            {
                Ok(Some(src_raw)) => src_raw
                    .head(&source.key)
                    .await
                    .map(|m| m.size)
                    .unwrap_or_default(),
                _ => 0,
            },
        };
        if let Some(etag) = &e_tag {
            let mut uploads = self.uploads();
            if let Some(upload) = uploads.get_mut(upload_id)
                && upload.bucket == dest.bucket
                && upload.key == dest.key
            {
                upload.parts.insert(
                    part_number,
                    TrackedPart {
                        e_tag: etag.clone(),
                        size,
                        last_modified: SystemTime::now(),
                    },
                );
            }
        }
        Ok(CopyOutcome {
            e_tag,
            last_modified,
        })
    }

    /// ListMultipartUploads (issue #153): read-only passthrough to the origin
    /// over the raw client (object_store has no such call), returning uploads
    /// and rolled-up common prefixes with byte-exact keys. An origin with no
    /// raw path (injected test stores) cannot serve it.
    async fn list_multipart_uploads(
        &self,
        request: MultipartUploadsRequest,
    ) -> Result<MultipartUploadsPage, WriteError> {
        let raw = self
            .stores
            .raw_for(&request.storage_binding_id, &request.bucket)
            .map_err(write_backend_error)?
            .ok_or_else(|| {
                WriteError::Unsupported(
                    "this origin has no raw request path for ListMultipartUploads".to_owned(),
                )
            })?;
        let page = raw
            .list_multipart_uploads(
                request.prefix.as_deref(),
                request.delimiter.as_deref(),
                request.key_marker.as_deref(),
                request.upload_id_marker.as_deref(),
                request.max_uploads.max(1),
            )
            .await
            .map_err(map_raw_write_error)?;
        Ok(MultipartUploadsPage {
            uploads: page
                .uploads
                .into_iter()
                .map(|u| UploadEntry {
                    key: u.key,
                    upload_id: u.upload_id,
                    storage_class: u.storage_class,
                    initiated: u.initiated,
                })
                .collect(),
            common_prefixes: page.common_prefixes,
            is_truncated: page.is_truncated,
            next_key_marker: page.next_key_marker,
            next_upload_id_marker: page.next_upload_id_marker,
        })
    }
}

/// Parses the byte length of a `bytes=a-b` copy-source range, or `None` when
/// absent or unparseable (issue #153).
fn parse_copy_range_len(range: Option<&str>) -> Option<u64> {
    let spec = range?.trim().strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    let start: u64 = start.trim().parse().ok()?;
    let end: u64 = end.trim().parse().ok()?;
    (end >= start).then(|| end - start + 1)
}

/// Maps `object_store` list failures onto the interface's error space. As on the
/// read/write sides the origin's own `NoSuchBucket`/`AccessDenied` are
/// recognized and forwarded; everything else is a backend failure.
fn map_list_error(error: object_store::Error) -> ListError {
    match origin_s3_code(&error) {
        Some(OriginCode::NoSuchBucket) => ListError::NoSuchBucket,
        Some(OriginCode::AccessDenied) => ListError::AccessDenied,
        None => ListError::Backend(error.to_string()),
    }
}

/// Maps raw-client list failures onto the interface's error space (the raw
/// twin of [`map_list_error`]). A 404 on a listing means the bucket.
fn map_raw_list_error(error: RawError) -> ListError {
    match error {
        RawError::NoSuchBucket | RawError::NoSuchKey => ListError::NoSuchBucket,
        RawError::AccessDenied => ListError::AccessDenied,
        RawError::Credentials(message) => {
            ListError::Backend(format!("origin credentials unavailable: {message}"))
        }
        other => ListError::Backend(other.to_string()),
    }
}

/// What one listed key resolves to under the request's delimiter: either a
/// standalone object or the common prefix it rolls up into.
enum ListEntry {
    /// A key returned in `<Contents>` verbatim.
    Object,
    /// The key rolls up to this common prefix (`prefix` + the run up to and
    /// including the first delimiter after it).
    CommonPrefix(String),
}

/// The path-segment-aligned ancestor of a raw S3 prefix — everything up to and
/// including its last `/`, or `None` when the prefix names no directory.
///
/// `object_store`'s `list(prefix)` filters on *path segments*, but S3's prefix
/// is a raw string prefix (so `part-` must match `part-0001`, and `a/b` must
/// match `a/bc`). Listing this ancestor returns a superset that the caller then
/// filters by the raw prefix, reproducing S3 exactly. A prefix with no `/`
/// lists from the bucket root.
fn segment_ancestor(prefix: &str) -> Option<String> {
    prefix.rfind('/').map(|idx| prefix[..=idx].to_owned())
}

/// Classifies a key against the raw `prefix` and optional `delimiter`. The key
/// is guaranteed to start with `prefix`, so slicing at `prefix.len()` lands on
/// a char boundary; the delimiter search is byte-based but the split point is
/// always a boundary too (it is where the delimiter matched), so unicode keys
/// are handled correctly.
fn classify(key: &str, prefix: &str, delimiter: Option<&str>) -> ListEntry {
    match delimiter {
        Some(delim) if !delim.is_empty() => {
            let remainder = &key[prefix.len()..];
            match remainder.find(delim) {
                Some(idx) => {
                    let end = prefix.len() + idx + delim.len();
                    ListEntry::CommonPrefix(key[..end].to_owned())
                }
                None => ListEntry::Object,
            }
        }
        _ => ListEntry::Object,
    }
}

/// Lists through to the origin, routing by the request's bucket. Listings are
/// **never cached** (issue #8): every page is a fresh backend read, so a file
/// written and immediately listed always appears. Pagination, the raw-prefix
/// filter, and common-prefix roll-up are computed here on top of
/// `object_store`'s ordered listing stream.
pub struct PassthroughList {
    /// The backend seam; the store for a request's bucket is resolved here.
    /// The server serves a configured bucket set (a bucket outside it is
    /// `NoSuchBucket`).
    stores: Arc<dyn BackendStores>,
}

impl PassthroughList {
    /// Wraps the backend store. Every listing routes to it for the request's
    /// bucket.
    pub fn new(stores: Arc<dyn BackendStores>) -> Self {
        PassthroughList { stores }
    }

    /// Resolves the origin store for `bucket`. A bucket other than the one this
    /// server serves is `NoSuchBucket`; a construction failure is a backend
    /// error.
    fn store_for(&self, request: &ListRequest) -> Result<Arc<dyn MultipartObjectStore>, ListError> {
        self.stores
            .store_for(&request.storage_binding_id, &request.bucket)
            .map_err(list_backend_error)
    }

    /// Builds one bounded page over the raw client's ListObjectsV2 (issue
    /// #189): the origin filters by the RAW S3 prefix directly (no
    /// segment-ancestor superset needed) and returns keys percent-decoded to
    /// their exact bytes — the only way trailing-slash / empty-segment keys
    /// survive a listing. Identical pagination, roll-up, and resume-cursor
    /// semantics to the typed scan (#144), fed by raw pages instead of the
    /// typed stream.
    async fn list_raw(
        &self,
        raw: Arc<ResilientRawS3>,
        request: ListRequest,
    ) -> Result<ListPage, ListError> {
        let prefix = request.prefix.as_str();
        let delimiter = request.delimiter.as_deref();
        let max = request.max_keys;
        let start = request.start_after.clone().unwrap_or_default();

        let mut objects: Vec<ListObject> = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut count = 0usize;
        // The common prefix currently being rolled up; keys under it after the
        // first are skipped, which is what makes a whole group count as one.
        let mut current_group: Option<String> = None;
        // The representative of the last entry actually emitted — the
        // exclusive cursor the next page resumes from (issue #144).
        let mut resume_cursor: Option<String> = None;
        let mut is_truncated = false;

        // A common-prefix cursor's group was fully covered by the previous
        // page's CommonPrefix; `start-after` is byte-exclusive, so its keys
        // stream back and must be skipped, not re-rolled (issue #144).
        let resume_group: Option<String> = match delimiter {
            Some(d) if !d.is_empty() && !start.is_empty() && start.ends_with(d) => {
                Some(start.clone())
            }
            _ => None,
        };

        let mut continuation: Option<String> = None;
        'pages: loop {
            let page = raw
                .list_v2(
                    prefix,
                    (!start.is_empty()).then_some(start.as_str()),
                    continuation.as_deref(),
                    1000,
                )
                .await
                .map_err(map_raw_list_error)?;
            let next_token = page.next_continuation_token;
            for entry in page.entries {
                let key = entry.key;
                // The origin already filtered by the raw prefix; defensive.
                if !key.starts_with(prefix) {
                    continue;
                }
                // The group the resume cursor already returned as a
                // CommonPrefix.
                if let Some(group) = &resume_group
                    && key.starts_with(group.as_str())
                {
                    continue;
                }
                let classified = classify(&key, prefix, delimiter);
                // Same group as the one already emitted this page: fold it in.
                if let (ListEntry::CommonPrefix(cp), Some(group)) = (&classified, &current_group)
                    && cp == group
                {
                    continue;
                }
                // A new distinct entry that does not fit belongs to the next
                // page.
                if count == max {
                    is_truncated = true;
                    break 'pages;
                }
                match classified {
                    ListEntry::CommonPrefix(cp) => {
                        resume_cursor = Some(cp.clone());
                        common_prefixes.push(cp.clone());
                        current_group = Some(cp);
                    }
                    ListEntry::Object => {
                        resume_cursor = Some(key.clone());
                        objects.push(ListObject {
                            key,
                            size: entry.size,
                            e_tag: entry.e_tag,
                            last_modified: entry.last_modified,
                        });
                        current_group = None;
                    }
                }
                count += 1;
            }
            match next_token {
                Some(token) => continuation = Some(token),
                None => break,
            }
        }

        let next_start_after = is_truncated.then(|| resume_cursor.unwrap_or(start));
        Ok(ListPage {
            objects,
            common_prefixes,
            is_truncated,
            next_start_after,
        })
    }
}

#[async_trait::async_trait]
impl ObjectList for PassthroughList {
    /// Builds one bounded page. Walks the origin's ordered listing (resumed
    /// strictly after `start_after` via `list_with_offset`, which S3 pushes
    /// down), filters to the raw S3 prefix, rolls up common prefixes when a
    /// delimiter is set, and stops at `max_keys` entries (objects and prefixes
    /// counted together).
    ///
    /// The resume cursor is the *last entry emitted*: a raw object key, or —
    /// when the page ended on a rolled-up group — that group's common prefix
    /// (which ends with the delimiter), exactly as S3 hands back `NextMarker` /
    /// `NextContinuationToken` (issue #144). Because `list_with_offset` is a
    /// plain byte-exclusive bound, a common-prefix cursor like `boo/` still sorts
    /// *before* keys inside the group (`boo/bar`), so resuming re-reads them; the
    /// scan therefore skips any key under the resume cursor's group rather than
    /// re-rolling it into the same prefix a second time.
    async fn list(&self, request: ListRequest) -> Result<ListPage, ListError> {
        // A real S3 origin is listed over the raw client so keys come back
        // byte-exactly (issue #189); the typed stream cannot represent
        // trailing-slash / empty-segment keys at all.
        if let Some(raw) = self
            .stores
            .raw_for(&request.storage_binding_id, &request.bucket)
            .map_err(list_backend_error)?
        {
            return self.list_raw(raw, request).await;
        }
        let store = self.store_for(&request)?;

        let ancestor_path = segment_ancestor(&request.prefix).map(|a| Path::from(a.as_str()));
        // `list_with_offset` is exclusive; an empty offset admits every key.
        let start = request.start_after.clone().unwrap_or_default();
        let offset_path = Path::from(start.as_str());
        let mut stream = store.list_with_offset(ancestor_path.as_ref(), &offset_path);

        let prefix = request.prefix.as_str();
        let delimiter = request.delimiter.as_deref();
        let max = request.max_keys;

        let mut objects: Vec<ListObject> = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut count = 0usize;
        // The common prefix currently being rolled up; keys under it after the
        // first are skipped, which is what makes a whole group count as one.
        let mut current_group: Option<String> = None;
        // The representative of the last entry actually emitted — a raw object
        // key, or the common prefix a rolled-up group was returned under. This
        // is the exclusive cursor the next page resumes from (issue #144).
        let mut resume_cursor: Option<String> = None;
        let mut is_truncated = false;

        // When the cursor is a common prefix (it ends with the delimiter), every
        // key beneath it was already covered by the CommonPrefix the previous
        // page returned. `list_with_offset` is byte-exclusive, so those keys
        // still stream back here — skip the whole group instead of re-rolling it
        // into the same prefix again (issue #144).
        let resume_group: Option<String> = match delimiter {
            Some(d) if !d.is_empty() && !start.is_empty() && start.ends_with(d) => {
                Some(start.clone())
            }
            _ => None,
        };

        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(map_list_error)?;
            let key = meta.location.as_ref().to_owned();

            // Raw-prefix filter. Keys arrive ascending, so a key past the
            // prefix block ends the scan; a key before it (possible only under
            // a shared ancestor directory) is skipped.
            if !key.starts_with(prefix) {
                if key.as_str() > prefix {
                    break;
                }
                continue;
            }

            // The group the resume cursor already returned as a CommonPrefix.
            if let Some(group) = &resume_group
                && key.starts_with(group.as_str())
            {
                continue;
            }

            let entry = classify(&key, prefix, delimiter);
            // Same group as the one already emitted this page: fold it in.
            if let (ListEntry::CommonPrefix(cp), Some(group)) = (&entry, &current_group)
                && cp == group
            {
                continue;
            }

            // A new distinct entry that does not fit belongs to the next page.
            if count == max {
                is_truncated = true;
                break;
            }

            match entry {
                ListEntry::CommonPrefix(cp) => {
                    resume_cursor = Some(cp.clone());
                    common_prefixes.push(cp.clone());
                    current_group = Some(cp);
                }
                ListEntry::Object => {
                    resume_cursor = Some(key.clone());
                    objects.push(ListObject {
                        key,
                        size: meta.size,
                        e_tag: meta.e_tag.clone(),
                        last_modified: Some(SystemTime::from(meta.last_modified)),
                    });
                    current_group = None;
                }
            }
            count += 1;
        }

        let next_start_after = is_truncated.then(|| resume_cursor.unwrap_or(start));
        Ok(ListPage {
            objects,
            common_prefixes,
            is_truncated,
            next_start_after,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #153: an UploadPartCopy `copy-source-range` yields the inclusive byte
    /// count; a missing or malformed range yields `None`.
    #[test]
    fn copy_range_len_parses_inclusive_bytes() {
        assert_eq!(parse_copy_range_len(Some("bytes=0-9")), Some(10));
        assert_eq!(parse_copy_range_len(Some("bytes=5-5")), Some(1));
        assert_eq!(parse_copy_range_len(None), None);
        assert_eq!(parse_copy_range_len(Some("0-9")), None);
        assert_eq!(parse_copy_range_len(Some("bytes=9-0")), None);
    }

    /// Builds an `object_store` generic error whose message carries an S3
    /// `<Code>`, the shape the library folds origin API errors into.
    fn origin_error(code: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: format!("Error performing request: response 400 <Code>{code}</Code>").into(),
        }
    }

    /// #146: a wrong part ETag on CompleteMultipartUpload is `InvalidPart`
    /// (400), not flattened to a backend 500.
    #[test]
    fn multipart_wrong_etag_maps_to_invalid_part() {
        assert!(matches!(
            map_multipart_error(origin_error("InvalidPart")),
            WriteError::InvalidPart(_)
        ));
    }

    /// #146: a too-small non-final part is `EntityTooSmall` (400).
    #[test]
    fn multipart_small_part_maps_to_entity_too_small() {
        assert!(matches!(
            map_multipart_error(origin_error("EntityTooSmall")),
            WriteError::EntityTooSmall(_)
        ));
    }

    /// #146: an origin `InvalidRequest` on the copy path is preserved as a
    /// 400, not flattened to a backend 500.
    #[test]
    fn copy_invalid_request_is_preserved() {
        assert!(matches!(
            map_write_error(origin_error("InvalidRequest")),
            WriteError::InvalidRequest(_)
        ));
    }

    /// An origin error naming no recognized code still classifies unknown
    /// multipart failures as `NoSuchUpload` / whole-object ones as `Backend`.
    #[test]
    fn unrecognized_codes_fall_through() {
        assert!(matches!(
            map_write_error(origin_error("SomethingElse")),
            WriteError::Backend(_)
        ));
    }
}

#[cfg(test)]
mod raw_routing_tests {
    use super::*;

    /// #189: the faithfulness predicate must flag every shape `Path::from`
    /// mangles — trailing slashes, empty segments, control characters, the
    /// RFC1738-ish encode set, relative segments, a leading slash, and any
    /// non-ASCII byte (`percent_encode` always encodes those) — and accept
    /// ordinary ASCII keys (spaces included).
    #[test]
    fn path_faithfulness_matches_path_from_semantics() {
        for bad in [
            "asdf/",
            "0/",
            "a//b",
            "/lead",
            "x\u{1}y",
            "1999#",
            "pct%25",
            "sta*r",
            "que?ry",
            "qu\"ote",
            "til~de",
            "a/./b",
            "a/../b",
            "uni-ключ",
        ] {
            assert!(!path_faithful(bad), "{bad:?} must be raw-only");
        }
        for good in [
            "plain",
            "a/b/c",
            "sp ace",
            "1999+",
            "dot.file",
            "under_score",
        ] {
            assert!(path_faithful(good), "{good:?} must stay on the typed path");
        }
    }

    /// #189: raw read errors map one-to-one onto the read interface's error
    /// space (`NotModified` outside a conditional request is a backend error).
    #[test]
    fn raw_read_errors_map_to_read_errors() {
        assert!(matches!(
            map_raw_read_error(RawError::NoSuchKey),
            ReadError::NoSuchKey
        ));
        assert!(matches!(
            map_raw_read_error(RawError::NoSuchBucket),
            ReadError::NoSuchBucket
        ));
        assert!(matches!(
            map_raw_read_error(RawError::AccessDenied),
            ReadError::AccessDenied
        ));
        assert!(matches!(
            map_raw_read_error(RawError::InvalidRange),
            ReadError::InvalidRange
        ));
        assert!(matches!(
            map_raw_read_error(RawError::NotModified),
            ReadError::Backend(_)
        ));
        assert!(matches!(
            map_raw_read_error(RawError::Backend("x".into())),
            ReadError::Backend(_)
        ));
        assert!(matches!(
            map_raw_read_error(RawError::Credentials("x".into())),
            ReadError::Backend(_)
        ));
    }

    /// #189: raw write errors map onto the write interface's error space.
    #[test]
    fn raw_write_errors_map_to_write_errors() {
        assert!(matches!(
            map_raw_write_error(RawError::NoSuchKey),
            WriteError::NoSuchKey
        ));
        assert!(matches!(
            map_raw_write_error(RawError::NoSuchBucket),
            WriteError::NoSuchBucket
        ));
        assert!(matches!(
            map_raw_write_error(RawError::AccessDenied),
            WriteError::AccessDenied
        ));
        assert!(matches!(
            map_raw_write_error(RawError::NotModified),
            WriteError::Backend(_)
        ));
        assert!(matches!(
            map_raw_write_error(RawError::Backend("x".into())),
            WriteError::Backend(_)
        ));
        assert!(matches!(
            map_raw_write_error(RawError::Credentials("x".into())),
            WriteError::Backend(_)
        ));
    }

    /// #189: raw list errors — a 404 on a listing means the bucket.
    #[test]
    fn raw_list_errors_map_to_list_errors() {
        assert!(matches!(
            map_raw_list_error(RawError::NoSuchBucket),
            ListError::NoSuchBucket
        ));
        assert!(matches!(
            map_raw_list_error(RawError::NoSuchKey),
            ListError::NoSuchBucket
        ));
        assert!(matches!(
            map_raw_list_error(RawError::AccessDenied),
            ListError::AccessDenied
        ));
        assert!(matches!(
            map_raw_list_error(RawError::Backend("x".into())),
            ListError::Backend(_)
        ));
        assert!(matches!(
            map_raw_list_error(RawError::Credentials("x".into())),
            ListError::Backend(_)
        ));
    }

    /// #189: the interface range converts to the raw request form with the
    /// same inclusive-to-half-open `Bounded` translation as the typed path.
    #[test]
    fn read_ranges_translate_to_raw_ranges() {
        assert!(matches!(to_raw_range(ReadRange::Full), RawRange::Full));
        assert!(matches!(
            to_raw_range(ReadRange::From(7)),
            RawRange::Offset(7)
        ));
        assert!(matches!(
            to_raw_range(ReadRange::Bounded(2, 9)),
            RawRange::Bounded(2, 10)
        ));
        assert!(matches!(
            to_raw_range(ReadRange::Suffix(5)),
            RawRange::Suffix(5)
        ));
    }

    /// #189: `WriteMetadata` (including `Expires` and user metadata) converts
    /// field-for-field into the raw write-header set, and a source object's
    /// own metadata rebuilds the same set for a retained copy.
    #[test]
    fn write_metadata_and_source_meta_convert_to_raw_headers() {
        let mut user_metadata = BTreeMap::new();
        user_metadata.insert("k".to_owned(), "v".to_owned());
        let metadata = WriteMetadata {
            content_type: Some("t".into()),
            cache_control: Some("cc".into()),
            content_disposition: Some("cd".into()),
            content_encoding: Some("ce".into()),
            content_language: Some("cl".into()),
            expires: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            user_metadata: user_metadata.clone(),
            ..WriteMetadata::default()
        };
        let headers = to_raw_write_headers(&metadata);
        assert_eq!(headers.content_type.as_deref(), Some("t"));
        assert_eq!(headers.cache_control.as_deref(), Some("cc"));
        assert_eq!(headers.content_disposition.as_deref(), Some("cd"));
        assert_eq!(headers.content_encoding.as_deref(), Some("ce"));
        assert_eq!(headers.content_language.as_deref(), Some("cl"));
        assert_eq!(
            headers.expires.as_deref(),
            Some("Wed, 21 Oct 2015 07:28:00 GMT")
        );
        assert_eq!(headers.user_metadata, user_metadata);

        let meta = RawObjectMeta {
            size: 1,
            e_tag: Some("\"e\"".into()),
            last_modified: None,
            content_type: Some("t".into()),
            cache_control: Some("cc".into()),
            content_disposition: Some("cd".into()),
            content_encoding: Some("ce".into()),
            content_language: Some("cl".into()),
            expires: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            version_id: None,
            parts_count: None,
            checksums: RawChecksums::default(),
            user_metadata: user_metadata.clone(),
        };
        let retained = raw_headers_from_meta(&meta);
        assert_eq!(retained.content_type.as_deref(), Some("t"));
        assert_eq!(
            retained.expires.as_deref(),
            Some("Wed, 21 Oct 2015 07:28:00 GMT")
        );
        assert_eq!(retained.user_metadata, user_metadata);

        let object_meta = raw_to_object_meta(meta);
        assert_eq!(object_meta.size, 1);
        assert_eq!(object_meta.e_tag.as_deref(), Some("\"e\""));
        assert_eq!(
            object_meta.expires.as_deref(),
            Some("Wed, 21 Oct 2015 07:28:00 GMT")
        );
        assert_eq!(object_meta.user_metadata, user_metadata);
    }

    /// The completion manifest is sorted and validated for contiguity from 1;
    /// a gap is `InvalidPart`, never silently renumbered. Per-part checksums
    /// ride along untouched (issue #208).
    #[test]
    fn completion_manifest_contiguity_is_enforced() {
        let parts = vec![
            CompletedPartRef {
                part_number: 2,
                e_tag: "b".into(),
                checksums: Checksums::default(),
            },
            CompletedPartRef {
                part_number: 1,
                e_tag: "a".into(),
                checksums: Checksums {
                    sha256: Some("part-1-sha".into()),
                    ..Checksums::default()
                },
            },
        ];
        let sorted = contiguous_parts(parts).expect("contiguous");
        assert_eq!(
            sorted.iter().map(|p| p.e_tag.clone()).collect::<Vec<_>>(),
            vec!["a".to_owned(), "b".to_owned()]
        );
        // The sort must keep each part's own checksum with it.
        assert_eq!(sorted[0].checksums.sha256.as_deref(), Some("part-1-sha"));
        assert_eq!(sorted[1].checksums.sha256, None);
        let gapped = vec![
            CompletedPartRef {
                part_number: 1,
                e_tag: "a".into(),
                checksums: Checksums::default(),
            },
            CompletedPartRef {
                part_number: 3,
                e_tag: "c".into(),
                checksums: Checksums::default(),
            },
        ];
        assert!(matches!(
            contiguous_parts(gapped),
            Err(WriteError::InvalidPart(_))
        ));
    }
}
