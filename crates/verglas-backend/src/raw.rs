//! A thin, SigV4-signed raw-HTTP S3 client that trades `object_store`'s typed
//! conveniences for byte-exact fidelity.
//!
//! `object_store`'s [`object_store::path::Path`] normalises keys: it corrupts
//! trailing slashes, collapses empty segments, and rejects control characters,
//! and its 0.14 model has no place to carry an `Expires` header. This module is
//! the escape hatch. It builds the request path by percent-encoding every byte
//! of the key except the RFC 3986 unreserved set (and a literal `/`), signs the
//! request with SigV4 using the `UNSIGNED-PAYLOAD` variant so bodies stream in
//! bounded memory, and reflects every metadata header — including `Expires` and
//! `x-amz-meta-*` — verbatim. The signing algorithm is adapted from the proven
//! integration-test helper in `verglas-s3/tests/support/sigv4.rs`.
//!
//! Addressing is path-style (`{endpoint}/{bucket}/{key}`), which MinIO and the
//! dev origin use. Virtual-hosted-style is not modelled here.

use std::collections::BTreeMap;
use std::ops::Range;
use std::time::SystemTime;

use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use hmac::{Hmac, Mac};
use object_store::aws::AwsCredentialProvider;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use reqwest::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH,
    CONTENT_RANGE, CONTENT_TYPE, ETAG, EXPIRES, HeaderMap, HeaderName, HeaderValue, LAST_MODIFIED,
    RANGE,
};
use reqwest::{Client, Method, Response, StatusCode, Url};
use sha2::{Digest, Sha256};

use verglas_core::config::Backend;

use crate::credentials::s3_credentials;
use crate::{BackendError, BackendSettings};

/// Bytes kept literal in a percent-encoded object key: the RFC 3986 unreserved
/// set (`A-Za-z0-9-._~`) plus `/`, which S3 preserves as a path separator.
/// Every other byte — spaces, control characters, reserved punctuation, and all
/// non-ASCII bytes — is percent-encoded, so the key travels byte-for-byte.
const PATH_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

/// Bytes kept literal in a query-string component: the RFC 3986 unreserved set
/// only. Unlike a path, `/` is encoded here, matching SigV4's canonical-query
/// rules so the signature covers the exact bytes on the wire.
const QUERY_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// The payload-hash placeholder that lets a streamed body skip the up-front
/// SHA-256 SigV4 would otherwise demand — the whole point of this client.
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// Percent-encodes every byte of `key` except the RFC 3986 unreserved set and a
/// literal `/`. This single-encoding is load-bearing: it is used identically for
/// the request URL and the SigV4 canonical URI so the origin receives — and
/// stores — the key byte-for-byte. `asdf/` stays `asdf/`, `a//b` keeps its empty
/// segment, `\n` becomes `%0A`, and a space becomes `%20`.
pub fn encode_key(key: &str) -> String {
    utf8_percent_encode(key, PATH_ENCODE_SET).to_string()
}

/// Percent-encodes a query key or value per SigV4 canonical-query rules
/// (unreserved bytes literal, everything else — including `/` — encoded).
fn encode_query(value: &str) -> String {
    utf8_percent_encode(value, QUERY_ENCODE_SET).to_string()
}

/// Object metadata reflected verbatim from the origin's response headers. Header
/// values the origin omits are `None`; `Expires` is kept as its raw HTTP-date
/// string because `object_store` 0.14 cannot model it at all.
#[derive(Debug)]
pub struct RawObjectMeta {
    /// The full object size in bytes: `Content-Length` for a HEAD or full GET;
    /// for a ranged GET, [`RawS3::get`] corrects it to the total from
    /// `Content-Range`, so callers can always render `Content-Range` themselves.
    pub size: u64,
    /// The origin's `ETag`, quotes included.
    pub e_tag: Option<String>,
    /// `Last-Modified` parsed from its RFC 1123 form; `None` if absent or
    /// unparseable.
    pub last_modified: Option<SystemTime>,
    /// `Content-Type` as sent by the origin.
    pub content_type: Option<String>,
    /// `Cache-Control` as sent by the origin.
    pub cache_control: Option<String>,
    /// `Content-Disposition` as sent by the origin.
    pub content_disposition: Option<String>,
    /// `Content-Encoding` as sent by the origin.
    pub content_encoding: Option<String>,
    /// `Content-Language` as sent by the origin.
    pub content_language: Option<String>,
    /// The raw `Expires` HTTP-date string as returned by the origin.
    pub expires: Option<String>,
    /// The version ID the origin assigned this object (`x-amz-version-id`),
    /// present only on a versioned bucket (issue #156).
    pub version_id: Option<String>,
    /// Total number of parts a multipart object has, reflected from the
    /// origin's `x-amz-mp-parts-count` on a `partNumber` read (issue #153).
    pub parts_count: Option<i32>,
    /// The origin's stored checksums, forwarded verbatim and never recomputed
    /// (issue #154). Populated on a read only when the request asked for them
    /// (`x-amz-checksum-mode: ENABLED`) and the object carries one.
    pub checksums: RawChecksums,
    /// User metadata: every `x-amz-meta-*` header, keyed by the name with the
    /// prefix stripped and lowercased.
    pub user_metadata: BTreeMap<String, String>,
}

/// The modern S3 checksum values (issue #154), each a Base64 string exactly as
/// the origin reports it. Verglas forwards these; it never computes a checksum
/// itself, so the cache serving origin bytes under origin ETags stays honest.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RawChecksums {
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

impl RawChecksums {
    /// Reads every `x-amz-checksum-*` header the origin returned into the set.
    fn from_headers(headers: &HeaderMap) -> Self {
        let get = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        RawChecksums {
            crc32: get("x-amz-checksum-crc32"),
            crc32c: get("x-amz-checksum-crc32c"),
            crc64nvme: get("x-amz-checksum-crc64nvme"),
            sha1: get("x-amz-checksum-sha1"),
            sha256: get("x-amz-checksum-sha256"),
            checksum_type: get("x-amz-checksum-type"),
        }
    }

    /// True when no checksum value is present, so callers can skip emitting an
    /// empty checksum block.
    pub fn is_empty(&self) -> bool {
        self.crc32.is_none()
            && self.crc32c.is_none()
            && self.crc64nvme.is_none()
            && self.sha1.is_none()
            && self.sha256.is_none()
    }

    /// Appends each present checksum as a `<ChecksumXXX>value</ChecksumXXX>`
    /// element inside a completion manifest `<Part>` (issue #208). The
    /// `checksum_type` is not a per-part element, so it is skipped here.
    fn append_manifest_elements(&self, out: &mut String) {
        let mut push = |tag: &str, value: &Option<String>| {
            if let Some(v) = value {
                out.push_str(&format!("<{tag}>{}</{tag}>", xml_escape_text(v)));
            }
        };
        push("ChecksumCRC32", &self.crc32);
        push("ChecksumCRC32C", &self.crc32c);
        push("ChecksumCRC64NVME", &self.crc64nvme);
        push("ChecksumSHA1", &self.sha1);
        push("ChecksumSHA256", &self.sha256);
    }
}

/// Metadata headers to attach to a PUT. All are optional; each present field is
/// both sent as a request header and covered by the SigV4 signature. Cloneable
/// so the resilience layer can re-issue an attempt.
#[derive(Clone, Default)]
pub struct RawWriteHeaders {
    /// `Content-Type` to store with the object.
    pub content_type: Option<String>,
    /// `Cache-Control` to store with the object.
    pub cache_control: Option<String>,
    /// `Content-Disposition` to store with the object.
    pub content_disposition: Option<String>,
    /// `Content-Encoding` to store with the object.
    pub content_encoding: Option<String>,
    /// `Content-Language` to store with the object.
    pub content_language: Option<String>,
    /// The `Expires` HTTP-date string to send (`object_store` cannot carry it).
    pub expires: Option<String>,
    /// The checksum algorithm and any client-supplied checksum values to
    /// forward on the write (issue #154). The origin validates them; Verglas
    /// never computes a checksum of its own.
    pub checksum: RawWriteChecksum,
    /// User metadata, emitted as `x-amz-meta-<key>` headers.
    pub user_metadata: BTreeMap<String, String>,
}

/// Checksum request headers to forward on a write (issue #154): the SDK
/// algorithm selector plus any explicit precomputed checksum values the client
/// supplied. All optional; the origin does the verification.
#[derive(Clone, Default)]
pub struct RawWriteChecksum {
    /// `x-amz-sdk-checksum-algorithm` (e.g. `SHA256`, `CRC32`).
    pub algorithm: Option<String>,
    /// `x-amz-checksum-type` (`COMPOSITE`/`FULL_OBJECT`) — only meaningful on
    /// CreateMultipartUpload, which is the sole caller that reads it (issue #208).
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

impl RawWriteChecksum {
    /// Appends every present checksum header as a signed `(name, value)` pair.
    fn append_pairs(&self, pairs: &mut Vec<(String, String)>) {
        if let Some(v) = &self.algorithm {
            pairs.push(("x-amz-sdk-checksum-algorithm".to_owned(), v.clone()));
        }
        if let Some(v) = &self.crc32 {
            pairs.push(("x-amz-checksum-crc32".to_owned(), v.clone()));
        }
        if let Some(v) = &self.crc32c {
            pairs.push(("x-amz-checksum-crc32c".to_owned(), v.clone()));
        }
        if let Some(v) = &self.crc64nvme {
            pairs.push(("x-amz-checksum-crc64nvme".to_owned(), v.clone()));
        }
        if let Some(v) = &self.sha1 {
            pairs.push(("x-amz-checksum-sha1".to_owned(), v.clone()));
        }
        if let Some(v) = &self.sha256 {
            pairs.push(("x-amz-checksum-sha256".to_owned(), v.clone()));
        }
    }
}

/// A byte range to request. `Bounded` uses a half-open, exclusive end to match
/// Rust's [`Range`]; it is translated to S3's inclusive `Range` header. Copyable
/// so the resilience layer can re-issue an attempt.
#[derive(Clone, Copy)]
pub enum RawRange {
    /// The whole object.
    Full,
    /// From the given offset to the end.
    Offset(u64),
    /// A half-open `[start, end)` window.
    Bounded(u64, u64),
    /// The final N bytes.
    Suffix(u64),
}

/// Extra read parameters carried on a GET/HEAD beyond the byte range: a
/// version ID (issue #156), a part number (issue #153), and whether to ask the
/// origin to return its stored checksums (issue #154). All default to absent,
/// so a plain read attaches nothing.
#[derive(Clone, Default)]
pub struct RawReadExtras {
    /// `?versionId=<id>` — pin the read to one immutable object version.
    pub version_id: Option<String>,
    /// `?partNumber=<n>` — read just this part of a multipart object.
    pub part_number: Option<u16>,
    /// Send `x-amz-checksum-mode: ENABLED` so the origin returns its stored
    /// checksums in the response headers.
    pub checksum_mode: bool,
}

impl RawReadExtras {
    /// Builds the SigV4-canonical query string (sorted, encoded) for the
    /// version-ID and part-number params; empty when neither is set.
    fn canonical_query(&self) -> String {
        let mut params: Vec<(String, String)> = Vec::new();
        if let Some(pn) = self.part_number {
            params.push(("partNumber".to_owned(), pn.to_string()));
        }
        if let Some(vid) = &self.version_id {
            params.push(("versionId".to_owned(), vid.clone()));
        }
        let mut encoded: Vec<(String, String)> = params
            .iter()
            .map(|(k, v)| (encode_query(k), encode_query(v)))
            .collect();
        encoded.sort_by(|a, b| a.0.cmp(&b.0));
        encoded
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    }

    /// The extra signed request headers: the checksum-mode header when asked.
    fn header_pairs(&self) -> Vec<(String, String)> {
        let mut headers = Vec::new();
        if self.checksum_mode {
            headers.push(("x-amz-checksum-mode".to_owned(), "ENABLED".to_owned()));
        }
        headers
    }
}

/// The result of a GET: parsed metadata, the resolved byte range served, and the
/// body as a bounded-memory stream (never buffered whole).
pub struct RawGet {
    /// Metadata parsed from the response headers.
    pub meta: RawObjectMeta,
    /// The byte range the origin actually served (from `Content-Range`, or
    /// `0..size` for a full response).
    pub range: Range<u64>,
    /// The response body, streamed chunk by chunk.
    pub body: BoxStream<'static, Result<Bytes, RawError>>,
}

/// The outcome of CreateMultipartUpload: the origin's upload ID plus the
/// checksum algorithm/type it recorded for the upload (issue #208).
pub struct RawMultipartCreation {
    /// The origin's upload ID.
    pub upload_id: String,
    /// The checksum algorithm the origin recorded (from `<ChecksumAlgorithm>`).
    pub checksum_algorithm: Option<String>,
    /// The checksum type the origin recorded (from `<ChecksumType>`).
    pub checksum_type: Option<String>,
}

/// The outcome of UploadPart: the origin's part `ETag` plus any checksum it
/// echoed for the part (issue #208).
pub struct RawPartUpload {
    /// The part `ETag`, quotes included.
    pub e_tag: String,
    /// The checksums the origin echoed for the part.
    pub checksums: RawChecksums,
}

/// One part in a completion manifest: its ETag plus the per-part checksums the
/// client asserted (issue #208). Part order in the vec is the part order.
pub struct RawCompletedPart {
    /// The part's ETag, as returned by UploadPart.
    pub e_tag: String,
    /// The part's checksums, emitted inside the manifest `<Part>`.
    pub checksums: RawChecksums,
}

/// The outcome of a PUT: the origin's `ETag` for the stored object, plus any
/// checksum the origin echoed back (issue #154) and the assigned version ID.
pub struct RawPutOutcome {
    /// The `ETag` the origin returned, quotes included.
    pub e_tag: Option<String>,
    /// Checksums the origin returned for the stored object, forwarded verbatim.
    pub checksums: RawChecksums,
    /// The version ID the origin assigned (`x-amz-version-id`), if any.
    pub version_id: Option<String>,
}

/// A verbatim forwarded response for the unmodeled-operation passthrough
/// (issue #152): the origin's status, its response headers (minus hop-by-hop
/// framing headers), and the buffered response body. The status is returned
/// as-is — a 404/403 is a legitimate origin answer the front-end streams back,
/// never mapped to a [`RawError`].
pub struct RawForwardResponse {
    /// The origin's HTTP status code.
    pub status: u16,
    /// The origin's response headers, hop-by-hop framing headers removed so the
    /// front-end can re-frame the buffered body itself.
    pub headers: Vec<(String, String)>,
    /// The full response body, buffered. Bucket-config responses are small
    /// control-plane documents, so buffering is bounded by design.
    pub body: Bytes,
}

/// One object in a ListObjectsV2 page, with its key already percent-decoded to
/// its exact bytes.
pub struct RawListEntry {
    /// The object key, percent-decoded from the `encoding-type=url` response.
    pub key: String,
    /// The object's size in bytes.
    pub size: u64,
    /// The object's `ETag`, quotes included.
    pub e_tag: Option<String>,
    /// The object's last-modified time (parsed from RFC 3339).
    pub last_modified: Option<SystemTime>,
}

/// One page of a ListObjectsV2 result plus the continuation token for the next
/// page, if the listing was truncated.
pub struct RawListPage {
    /// The objects on this page.
    pub entries: Vec<RawListEntry>,
    /// The token to pass as `continuation` for the next page, or `None` when the
    /// listing is complete.
    pub next_continuation_token: Option<String>,
}

/// One in-flight multipart upload as ListMultipartUploads reports it (issue
/// #153): its key (percent-decoded) and the upload ID, plus the storage class
/// and initiation time the origin returned.
pub struct RawUpload {
    /// The object key the upload targets, decoded to its exact bytes.
    pub key: String,
    /// The origin's upload ID.
    pub upload_id: String,
    /// The origin's storage class for the upload, if reported.
    pub storage_class: Option<String>,
    /// When the upload was initiated (parsed from RFC 3339), if reported.
    pub initiated: Option<SystemTime>,
}

/// One page of a ListMultipartUploads result: the uploads and rolled-up common
/// prefixes on this page, plus the truncation markers to resume from (issue
/// #153). Keys and prefixes are percent-decoded to their exact bytes.
pub struct RawUploadsPage {
    /// The uploads on this page.
    pub uploads: Vec<RawUpload>,
    /// Delimiter-rolled-up common prefixes on this page.
    pub common_prefixes: Vec<String>,
    /// Whether more uploads remain past this page.
    pub is_truncated: bool,
    /// The key marker to resume from when truncated.
    pub next_key_marker: Option<String>,
    /// The upload-ID marker to resume from when truncated.
    pub next_upload_id_marker: Option<String>,
}

/// One part of a multipart object as GetObjectAttributes reports it (issue
/// #154): the part number, its size, and any part-level checksum the origin
/// stored.
pub struct RawObjectPart {
    /// The 1-based part number.
    pub part_number: i32,
    /// The part's size in bytes.
    pub size: u64,
    /// The part's checksums, forwarded verbatim (usually absent).
    pub checksums: RawChecksums,
}

/// The parts section of a GetObjectAttributes response (issue #154): the parts
/// on this page plus the pagination markers the origin returned. Absent
/// entirely for a non-multipart object.
pub struct RawObjectParts {
    /// The parts on this page.
    pub parts: Vec<RawObjectPart>,
    /// Total number of parts across all pages.
    pub total_parts_count: Option<i32>,
    /// The `MaxParts` the origin applied to this page.
    pub max_parts: Option<i32>,
    /// The `PartNumberMarker` this page resumed from.
    pub part_number_marker: Option<i32>,
    /// The marker to resume from when truncated.
    pub next_part_number_marker: Option<i32>,
    /// Whether more parts remain past this page.
    pub is_truncated: bool,
}

/// A GetObjectAttributes result (issue #154): the object-level attributes the
/// origin reported, all forwarded verbatim. Verglas computes none of these.
pub struct RawAttributes {
    /// The object's `ETag` (without quotes, as the attributes API reports it).
    pub e_tag: Option<String>,
    /// The object's total size in bytes.
    pub object_size: Option<u64>,
    /// The object's storage class.
    pub storage_class: Option<String>,
    /// The object's last-modified time, from the `Last-Modified` response
    /// header.
    pub last_modified: Option<SystemTime>,
    /// The version ID the read resolved to, if the bucket is versioned.
    pub version_id: Option<String>,
    /// The object-level checksums, if any.
    pub checksums: RawChecksums,
    /// The parts section, present only for a multipart object when `ObjectParts`
    /// was requested.
    pub object_parts: Option<RawObjectParts>,
}

/// A raw-client operation failure. The S3-shaped variants are mapped from the
/// HTTP status and error body; everything else is `Backend`.
#[derive(Debug, thiserror::Error)]
pub enum RawError {
    /// The key does not exist (404 without a `NoSuchBucket` code).
    #[error("key does not exist")]
    NoSuchKey,
    /// The bucket does not exist (404 with a `NoSuchBucket` code).
    #[error("bucket does not exist")]
    NoSuchBucket,
    /// The credentials were rejected (403).
    #[error("access denied by origin")]
    AccessDenied,
    /// A conditional request was not modified (304).
    #[error("not modified")]
    NotModified,
    /// The requested range is not satisfiable (416).
    #[error("requested range not satisfiable")]
    InvalidRange,
    /// The requested part number is invalid — out of range, or a part read of a
    /// non-multipart object (`InvalidPart`, 400) — issue #153.
    #[error("invalid part number")]
    InvalidPart,
    /// The origin rejected a request argument (`InvalidArgument`, 400) — e.g. an
    /// UploadPartCopy source range outside the source object. Forwarded as a
    /// 400 rather than flattened to a 500 (issue #146).
    #[error("invalid request argument")]
    InvalidArgument,
    /// Any other failure — transport error, unexpected status, or malformed
    /// response — carrying an operator-facing description.
    #[error("raw backend request failed: {0}")]
    Backend(String),
    /// The credential provider could not supply a current origin identity.
    #[error("origin credentials unavailable: {0}")]
    Credentials(String),
}

/// A SigV4-signed, path-style S3 client bound to one bucket. Holds a single
/// reused [`Client`]; clone-free across calls.
pub struct RawS3 {
    /// The shared HTTP client, reused across every request.
    client: Client,
    /// The origin base URL with no trailing slash (e.g. `http://127.0.0.1:9000`).
    endpoint: String,
    /// The `Host` header value (authority, with port when non-default) signed on
    /// every request.
    host: String,
    /// The signing region.
    region: String,
    /// The bucket this client addresses.
    bucket: String,
    /// The refreshable provider consulted before every origin signature.
    credentials: AwsCredentialProvider,
}

/// Builds a [`BackendError::Build`] carrying `msg` for a construction failure.
fn build_error(bucket: &str, msg: impl Into<String>) -> BackendError {
    BackendError::Build {
        bucket: bucket.to_owned(),
        source: object_store::Error::Generic {
            store: "RawS3",
            source: msg.into().into(),
        },
    }
}

impl RawS3 {
    /// Resolves the client purely from the AWS environment (no `[backend]`
    /// config overrides): endpoint/region from `AWS_*`, defaulting region to
    /// `us-east-1` and the endpoint to the regional AWS endpoint. Origin
    /// credentials come from the AWS default provider chain when each request
    /// is signed. The registry's config path uses [`RawS3::from_settings`] so
    /// `[backend]` endpoint and region win.
    pub fn from_env(bucket: &str) -> Result<Self, BackendError> {
        let settings =
            BackendSettings::resolve(&Backend::default(), &|name| std::env::var(name).ok())
                .map_err(|e| build_error(bucket, e.to_string()))?;
        Self::from_settings(bucket, &settings, s3_credentials(&Backend::default()))
    }

    /// Builds the raw client for `bucket` from already-resolved
    /// [`BackendSettings`] (#221): the config-over-env endpoint and region plus
    /// a refreshing provider for origin credentials. This is the path the
    /// registry uses so dynamic credentials reach the byte-exact client as well
    /// as the typed one. Validates the endpoint URL and derives the signed
    /// `Host` authority.
    pub fn from_settings(
        bucket: &str,
        settings: &BackendSettings,
        credentials: AwsCredentialProvider,
    ) -> Result<Self, BackendError> {
        let url = Url::parse(&settings.endpoint).map_err(|e| {
            build_error(
                bucket,
                format!("invalid endpoint `{}`: {e}", settings.endpoint),
            )
        })?;
        match url.scheme() {
            "https" => {}
            "http" => {
                if !settings.allow_http {
                    return Err(build_error(
                        bucket,
                        "endpoint uses http but backend.allow_http / AWS_ALLOW_HTTP is not set",
                    ));
                }
            }
            other => {
                return Err(build_error(
                    bucket,
                    format!("endpoint scheme `{other}` is not http or https"),
                ));
            }
        }
        let host_str = url
            .host_str()
            .ok_or_else(|| build_error(bucket, "endpoint has no host"))?;
        let host = match url.port() {
            Some(port) => format!("{host_str}:{port}"),
            None => host_str.to_owned(),
        };

        let client = Client::builder()
            .build()
            .map_err(|e| build_error(bucket, format!("cannot build HTTP client: {e}")))?;

        Ok(RawS3 {
            client,
            endpoint: settings.endpoint.clone(),
            host,
            region: settings.region.clone(),
            bucket: bucket.to_owned(),
            credentials,
        })
    }

    /// HEADs `key` and returns its metadata. Sends no body and no `Range`.
    pub async fn head(&self, key: &str) -> Result<RawObjectMeta, RawError> {
        self.head_conditional(key, None, &RawReadExtras::default())
            .await
    }

    /// HEADs `key` with the extra read parameters (version ID, part number,
    /// checksum mode) attached (issues #153, #154, #156).
    pub async fn head_ext(
        &self,
        key: &str,
        extras: &RawReadExtras,
    ) -> Result<RawObjectMeta, RawError> {
        self.head_conditional(key, None, extras).await
    }

    /// Conditional HEAD carrying `If-None-Match: <etag>`: the origin's 304 (the
    /// version still holds) surfaces as [`RawError::NotModified`] with no
    /// metadata transferred — the cheap revalidation the cache engine relies on
    /// (#14). A changed object returns its fresh metadata.
    pub async fn head_if_none_match(
        &self,
        key: &str,
        etag: &str,
    ) -> Result<RawObjectMeta, RawError> {
        self.head_conditional(key, Some(etag), &RawReadExtras::default())
            .await
    }

    /// The shared HEAD request: plain when `if_none_match` is `None`,
    /// conditional (and signed as such) when an ETag is presented. `extras`
    /// carries any version-ID / part-number query params and the checksum-mode
    /// header.
    async fn head_conditional(
        &self,
        key: &str,
        if_none_match: Option<&str>,
        extras: &RawReadExtras,
    ) -> Result<RawObjectMeta, RawError> {
        let uri = self.object_uri(key);
        let query = extras.canonical_query();
        let url = self.request_url(&uri, &query)?;
        let mut headers = extras.header_pairs();
        if let Some(etag) = if_none_match {
            headers.push(("if-none-match".to_owned(), etag.to_owned()));
        }
        let attach = self.signed_headers("HEAD", &uri, &query, headers).await?;
        let request = self.apply_headers(self.client.request(Method::HEAD, url), attach)?;
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let response = ensure_success(response).await?;
        Ok(meta_from_headers(response.headers()))
    }

    /// GETs `key` over the requested `range` and returns the metadata, the
    /// resolved served range, and the body as a bounded-memory stream.
    pub async fn get(&self, key: &str, range: RawRange) -> Result<RawGet, RawError> {
        self.get_ext(key, range, &RawReadExtras::default()).await
    }

    /// GETs `key` over `range` with the extra read parameters (version ID, part
    /// number, checksum mode) attached (issues #153, #154, #156).
    pub async fn get_ext(
        &self,
        key: &str,
        range: RawRange,
        extras: &RawReadExtras,
    ) -> Result<RawGet, RawError> {
        let uri = self.object_uri(key);
        let query = extras.canonical_query();
        let url = self.request_url(&uri, &query)?;
        let attach = self
            .signed_headers("GET", &uri, &query, extras.header_pairs())
            .await?;
        let mut request = self.apply_headers(self.client.request(Method::GET, url), attach)?;
        if let Some(value) = range_header(&range) {
            let value = HeaderValue::from_str(&value)
                .map_err(|e| RawError::Backend(format!("invalid range header: {e}")))?;
            request = request.header(RANGE, value);
        }
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let response = ensure_success(response).await?;
        let headers = response.headers().clone();
        let mut meta = meta_from_headers(&headers);
        let content_length = meta.size;
        let (resolved, total) = parse_content_range(
            headers.get(CONTENT_RANGE).and_then(|v| v.to_str().ok()),
            content_length,
        );
        // A ranged response's Content-Length is the slice; the full object
        // size callers need for `Content-Range` rendering is the total.
        if let Some(total) = total {
            meta.size = total;
        }
        let body = response
            .bytes_stream()
            .map_err(|e| RawError::Backend(e.to_string()))
            .boxed();
        Ok(RawGet {
            meta,
            range: resolved,
            body,
        })
    }

    /// PUTs `chunks` (one buffered slice, its exact length known) to `key`,
    /// signing every attached metadata header. S3 requires `Content-Length` on
    /// PUT, so the raw client takes a bounded, materialized slice — callers
    /// stream larger bodies through the raw multipart trio instead. Returns
    /// the origin's `ETag`.
    pub async fn put(
        &self,
        key: &str,
        headers: RawWriteHeaders,
        chunks: Vec<Bytes>,
    ) -> Result<RawPutOutcome, RawError> {
        let uri = self.object_uri(key);
        let url = self.build_url(&uri)?;
        let attach = self
            .signed_headers("PUT", &uri, "", write_header_pairs(headers))
            .await?;
        let request = self.apply_headers(self.client.request(Method::PUT, url), attach)?;
        // A materialized body makes reqwest emit the Content-Length S3
        // demands (411 without it).
        let request = request.body(reqwest::Body::from(concat_chunks(chunks)));
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let response = ensure_success(response).await?;
        let headers = response.headers();
        let e_tag = str_header(headers, &ETAG);
        let checksums = RawChecksums::from_headers(headers);
        let version_id = str_header_named(headers, "x-amz-version-id");
        Ok(RawPutOutcome {
            e_tag,
            checksums,
            version_id,
        })
    }

    /// Starts a multipart upload for `key` carrying the metadata headers, and
    /// returns the origin's upload ID plus any checksum algorithm/type it
    /// recorded (issue #208). With [`RawS3::put_part`] and
    /// [`RawS3::complete_multipart`] this is how bodies too large for one
    /// buffered PUT stream through the raw path in bounded memory.
    ///
    /// A create carries the checksum SELECTION headers — `x-amz-checksum-algorithm`
    /// and `x-amz-checksum-type` — not the per-value headers a part or a plain PUT
    /// carries; those are supplied on [`RawS3::put_part`] instead.
    pub async fn create_multipart(
        &self,
        key: &str,
        headers: RawWriteHeaders,
    ) -> Result<RawMultipartCreation, RawError> {
        let uri = self.object_uri(key);
        let query = "uploads=";
        let url = self.build_query_url(&uri, query)?;
        let mut pairs = write_header_pairs(headers.clone());
        // CreateMultipartUpload names the algorithm and combination type, not a
        // value. `write_header_pairs` emitted the value headers (harmless/empty
        // here); add the create-only selection headers.
        if let Some(algo) = &headers.checksum.algorithm {
            pairs.push(("x-amz-checksum-algorithm".to_owned(), algo.clone()));
        }
        if let Some(kind) = &headers.checksum.checksum_type {
            pairs.push(("x-amz-checksum-type".to_owned(), kind.clone()));
        }
        let attach = self.signed_headers("POST", &uri, query, pairs).await?;
        let request = self.apply_headers(self.client.request(Method::POST, url), attach)?;
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let response = ensure_success(response).await?;
        // S3 reports the recorded algorithm/type as response HEADERS (MinIO does
        // not put them in the initiate body); read the headers first and fall
        // back to the body elements for origins that use them instead.
        let header_algorithm = str_header_named(response.headers(), "x-amz-checksum-algorithm");
        let header_type = str_header_named(response.headers(), "x-amz-checksum-type");
        let xml = response
            .text()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let upload_id = tag_text(&xml, "UploadId")
            .ok_or_else(|| RawError::Backend(format!("no UploadId in response: {xml}")))?;
        Ok(RawMultipartCreation {
            upload_id,
            checksum_algorithm: header_algorithm.or_else(|| tag_text(&xml, "ChecksumAlgorithm")),
            checksum_type: header_type.or_else(|| tag_text(&xml, "ChecksumType")),
        })
    }

    /// Uploads one part (1-based `part_number`) of a raw multipart upload,
    /// forwarding any per-part checksum the client asserted and returning the
    /// ETag the origin assigned plus the checksum it echoed (issue #208).
    pub async fn put_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: usize,
        checksum: RawWriteChecksum,
        chunks: Vec<Bytes>,
    ) -> Result<RawPartUpload, RawError> {
        let uri = self.object_uri(key);
        let query = format!(
            "partNumber={part_number}&uploadId={}",
            encode_query(upload_id)
        );
        let url = self.build_query_url(&uri, &query)?;
        let mut pairs = Vec::new();
        checksum.append_pairs(&mut pairs);
        let attach = self.signed_headers("PUT", &uri, &query, pairs).await?;
        let request = self.apply_headers(self.client.request(Method::PUT, url), attach)?;
        let request = request.body(reqwest::Body::from(concat_chunks(chunks)));
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let response = ensure_success(response).await?;
        let checksums = RawChecksums::from_headers(response.headers());
        let e_tag = str_header(response.headers(), &ETAG)
            .ok_or_else(|| RawError::Backend("origin returned no part ETag".to_owned()))?;
        Ok(RawPartUpload { e_tag, checksums })
    }

    /// Completes a raw multipart upload from the part manifest in part order
    /// (part `i` of the vec is part number `i+1`). Each part carries its ETag
    /// and any per-part checksums the client asserted; `object_checksum` carries
    /// the object-level checksum the client asserts for the assembled object
    /// (issue #208). Returns the assembled object's ETag and echoed checksums.
    pub async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<RawCompletedPart>,
        object_checksum: RawWriteChecksum,
    ) -> Result<RawPutOutcome, RawError> {
        let uri = self.object_uri(key);
        let query = format!("uploadId={}", encode_query(upload_id));
        let url = self.build_query_url(&uri, &query)?;
        let mut manifest = String::from("<CompleteMultipartUpload>");
        for (index, part) in parts.iter().enumerate() {
            manifest.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag>",
                index + 1,
                xml_escape_text(&part.e_tag)
            ));
            part.checksums.append_manifest_elements(&mut manifest);
            manifest.push_str("</Part>");
        }
        manifest.push_str("</CompleteMultipartUpload>");
        // The object-level checksum the client asserts rides as request headers,
        // exactly as AWS/MinIO expect; the origin verifies against the combined
        // part checksums.
        let mut pairs = Vec::new();
        object_checksum.append_pairs(&mut pairs);
        // A FULL_OBJECT upload MUST restate `x-amz-checksum-type` on completion:
        // MinIO rejects a full-object checksum with `InvalidArgument` ("checksum
        // type mismatch") when the header is absent, defaulting the upload to
        // COMPOSITE. `append_pairs` omits the type because a plain PUT and a part
        // must not carry it; the completion is the one operation that needs it
        // (issue #208).
        if let Some(kind) = &object_checksum.checksum_type {
            pairs.push(("x-amz-checksum-type".to_owned(), kind.clone()));
        }
        let attach = self.signed_headers("POST", &uri, &query, pairs).await?;
        let request = self.apply_headers(self.client.request(Method::POST, url), attach)?;
        let request = request.body(manifest);
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let response = ensure_success(response).await?;
        let version_id = str_header_named(response.headers(), "x-amz-version-id");
        // MinIO reports the checksum TYPE as a response header, not in the
        // completion body (issue #208); capture it before the body is consumed.
        let header_checksum_type = str_header_named(response.headers(), "x-amz-checksum-type");
        let xml = response
            .text()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        // S3 can answer a completion failure inside a 200 body.
        if let Some(code) = error_code(&xml) {
            return Err(RawError::Backend(format!(
                "multipart completion failed: {code}"
            )));
        }
        // The completion body carries the object-level checksum for a
        // checksummed upload (issue #154/#208); forward whichever the origin set.
        // The type comes from the header when the body omits it.
        let checksums = RawChecksums {
            crc32: tag_text(&xml, "ChecksumCRC32"),
            crc32c: tag_text(&xml, "ChecksumCRC32C"),
            crc64nvme: tag_text(&xml, "ChecksumCRC64NVME"),
            sha1: tag_text(&xml, "ChecksumSHA1"),
            sha256: tag_text(&xml, "ChecksumSHA256"),
            checksum_type: tag_text(&xml, "ChecksumType").or(header_checksum_type),
        };
        Ok(RawPutOutcome {
            e_tag: tag_text(&xml, "ETag"),
            checksums,
            version_id,
        })
    }

    /// Aborts a raw multipart upload; the origin discards its parts.
    pub async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<(), RawError> {
        let uri = self.object_uri(key);
        let query = format!("uploadId={}", encode_query(upload_id));
        let url = self.build_query_url(&uri, &query)?;
        let attach = self
            .signed_headers("DELETE", &uri, &query, Vec::new())
            .await?;
        let request = self.apply_headers(self.client.request(Method::DELETE, url), attach)?;
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        ensure_success(response).await?;
        Ok(())
    }

    /// DELETEs `key`. A missing key is not an error on S3 (DELETE is idempotent).
    pub async fn delete(&self, key: &str) -> Result<(), RawError> {
        let uri = self.object_uri(key);
        let url = self.build_url(&uri)?;
        let attach = self.signed_headers("DELETE", &uri, "", Vec::new()).await?;
        let request = self.apply_headers(self.client.request(Method::DELETE, url), attach)?;
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        ensure_success(response).await?;
        Ok(())
    }

    /// Runs a ListObjectsV2 request with `encoding-type=url` and percent-decodes
    /// the returned keys so they are byte-exact. An empty `prefix` lists the
    /// whole bucket; `start_after` and `continuation` are optional cursors;
    /// `max_keys` caps the page (coerced to at least 1).
    pub async fn list_v2(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        continuation: Option<&str>,
        max_keys: usize,
    ) -> Result<RawListPage, RawError> {
        let max_keys = max_keys.max(1);
        let mut params: Vec<(String, String)> = vec![
            ("list-type".to_owned(), "2".to_owned()),
            ("max-keys".to_owned(), max_keys.to_string()),
            ("encoding-type".to_owned(), "url".to_owned()),
        ];
        if !prefix.is_empty() {
            params.push(("prefix".to_owned(), prefix.to_owned()));
        }
        if let Some(value) = start_after {
            params.push(("start-after".to_owned(), value.to_owned()));
        }
        if let Some(value) = continuation {
            params.push(("continuation-token".to_owned(), value.to_owned()));
        }

        let mut encoded: Vec<(String, String)> = params
            .iter()
            .map(|(k, v)| (encode_query(k), encode_query(v)))
            .collect();
        encoded.sort_by(|a, b| a.0.cmp(&b.0));
        let query = encoded
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        let canonical_uri = format!("/{}", self.bucket);
        let url_string = format!("{}/{}?{}", self.endpoint, self.bucket, query);
        let url = Url::parse(&url_string)
            .map_err(|e| RawError::Backend(format!("invalid list url `{url_string}`: {e}")))?;

        let attach = self
            .signed_headers("GET", &canonical_uri, &query, Vec::new())
            .await?;
        let request = self.apply_headers(self.client.request(Method::GET, url), attach)?;
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let response = ensure_success(response).await?;
        let xml = response
            .text()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        Ok(parse_list(&xml))
    }

    /// Forwards a bucket-level request (`{endpoint}/{bucket}[?query]`) verbatim,
    /// re-signed with the node credentials, and returns the origin's response
    /// untouched (status, headers, buffered body) for the unmodeled-operation
    /// passthrough (issue #152). `query` must already be in SigV4 canonical form
    /// (sorted, percent-encoded); pass `""` for none.
    ///
    /// The allowlisted callers today are HeadBucket and GetBucketLocation —
    /// bucket-config reads that carry no request body and answer with small
    /// control-plane documents; this method therefore sends no body and no extra
    /// request headers. A request body would be the extension point for
    /// forwarding config PUTs, and is deliberately not built yet. Unlike the
    /// typed methods, the status is never mapped to a [`RawError`]: the origin's
    /// own 200/403/404 is the answer.
    pub async fn forward_bucket(
        &self,
        method: &str,
        query: &str,
    ) -> Result<RawForwardResponse, RawError> {
        let canonical_uri = format!("/{}", self.bucket);
        let url = if query.is_empty() {
            self.build_url(&canonical_uri)?
        } else {
            self.build_query_url(&canonical_uri, query)?
        };
        let http_method = Method::from_bytes(method.as_bytes())
            .map_err(|e| RawError::Backend(format!("invalid method `{method}`: {e}")))?;
        let attach = self
            .signed_headers(method, &canonical_uri, query, Vec::new())
            .await?;
        let request = self.apply_headers(self.client.request(http_method, url), attach)?;
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter(|(name, _)| !is_hop_by_hop(name.as_str()))
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|v| (name.as_str().to_owned(), v.to_owned()))
            })
            .collect();
        let body = response
            .bytes()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        Ok(RawForwardResponse {
            status,
            headers,
            body,
        })
    }

    /// The canonical URI / URL path for an object: `/{bucket}/{encoded-key}`.
    fn object_uri(&self, key: &str) -> String {
        format!("/{}/{}", self.bucket, encode_key(key))
    }

    /// Parses `{endpoint}{encoded_path}?{canonical_query}` into a [`Url`],
    /// with the same byte-exactness guard as [`RawS3::build_url`]. The query
    /// string must already be in SigV4 canonical form (sorted, encoded) so the
    /// wire bytes match the signed bytes.
    fn build_query_url(&self, encoded_path: &str, canonical_query: &str) -> Result<Url, RawError> {
        let raw = format!("{}{}?{}", self.endpoint, encoded_path, canonical_query);
        let url =
            Url::parse(&raw).map_err(|e| RawError::Backend(format!("invalid url `{raw}`: {e}")))?;
        if url.path() != encoded_path {
            return Err(RawError::Backend(format!(
                "url path `{}` differs from encoded `{encoded_path}`",
                url.path()
            )));
        }
        Ok(url)
    }

    /// Parses `{endpoint}{encoded_path}` into a [`Url`], verifying reqwest did
    /// not re-encode the path — the byte-exactness the whole client exists for.
    fn build_url(&self, encoded_path: &str) -> Result<Url, RawError> {
        let raw = format!("{}{}", self.endpoint, encoded_path);
        let url =
            Url::parse(&raw).map_err(|e| RawError::Backend(format!("invalid url `{raw}`: {e}")))?;
        debug_assert_eq!(url.path(), encoded_path, "url path re-encoded by parser");
        if url.path() != encoded_path {
            return Err(RawError::Backend(format!(
                "url path `{}` differs from encoded `{encoded_path}`",
                url.path()
            )));
        }
        Ok(url)
    }

    /// Builds the request URL for `encoded_path`, appending `canonical_query`
    /// only when it is non-empty — a bare `?` is never sent.
    fn request_url(&self, encoded_path: &str, canonical_query: &str) -> Result<Url, RawError> {
        if canonical_query.is_empty() {
            self.build_url(encoded_path)
        } else {
            self.build_query_url(encoded_path, canonical_query)
        }
    }

    /// Server-side copies a source object (or a byte range of it) into part
    /// `part_number` of the multipart upload `upload_id` on `dest_key` (issue
    /// #153). `copy_source` is the raw `/{bucket}/{key}` value; `copy_range` is
    /// an optional `bytes=a-b` window. Returns the part's ETag and last-modified
    /// time from the `<CopyPartResult>` body. Same-bucket only — the caller
    /// enforces that before calling.
    pub async fn upload_part_copy(
        &self,
        dest_key: &str,
        upload_id: &str,
        part_number: u16,
        copy_source: &str,
        copy_range: Option<&str>,
    ) -> Result<(Option<String>, Option<SystemTime>), RawError> {
        let uri = self.object_uri(dest_key);
        let query = format!(
            "partNumber={part_number}&uploadId={}",
            encode_query(upload_id)
        );
        let url = self.build_query_url(&uri, &query)?;
        let mut headers = vec![("x-amz-copy-source".to_owned(), copy_source.to_owned())];
        if let Some(range) = copy_range {
            headers.push(("x-amz-copy-source-range".to_owned(), range.to_owned()));
        }
        let attach = self.signed_headers("PUT", &uri, &query, headers).await?;
        let request = self.apply_headers(self.client.request(Method::PUT, url), attach)?;
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let response = ensure_success(response).await?;
        let xml = response
            .text()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        // S3 can answer a copy failure inside a 200 body.
        if let Some(code) = error_code(&xml) {
            return Err(map_body_code(&code));
        }
        let e_tag = tag_text(&xml, "ETag");
        let last_modified = tag_text(&xml, "LastModified").and_then(|s| parse_rfc3339(&s));
        Ok((e_tag, last_modified))
    }

    /// Lists the in-flight multipart uploads in the bucket (issue #153),
    /// percent-decoding keys and common prefixes to their exact bytes. All
    /// filters/cursors are optional; `max_uploads` caps the page (coerced to at
    /// least 1).
    pub async fn list_multipart_uploads(
        &self,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max_uploads: usize,
    ) -> Result<RawUploadsPage, RawError> {
        let max_uploads = max_uploads.max(1);
        let mut params: Vec<(String, String)> = vec![
            ("uploads".to_owned(), String::new()),
            ("max-uploads".to_owned(), max_uploads.to_string()),
            ("encoding-type".to_owned(), "url".to_owned()),
        ];
        if let Some(v) = prefix.filter(|v| !v.is_empty()) {
            params.push(("prefix".to_owned(), v.to_owned()));
        }
        if let Some(v) = delimiter.filter(|v| !v.is_empty()) {
            params.push(("delimiter".to_owned(), v.to_owned()));
        }
        if let Some(v) = key_marker.filter(|v| !v.is_empty()) {
            params.push(("key-marker".to_owned(), v.to_owned()));
        }
        if let Some(v) = upload_id_marker.filter(|v| !v.is_empty()) {
            params.push(("upload-id-marker".to_owned(), v.to_owned()));
        }
        let mut encoded: Vec<(String, String)> = params
            .iter()
            .map(|(k, v)| (encode_query(k), encode_query(v)))
            .collect();
        encoded.sort_by(|a, b| a.0.cmp(&b.0));
        let query = encoded
            .iter()
            // SigV4 canonicalizes an empty-value param as `key=`, so emit the
            // trailing `=` on the wire too or the origin recomputes a different
            // signature (issues #153/#154).
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        let canonical_uri = format!("/{}", self.bucket);
        let url_string = format!("{}/{}?{}", self.endpoint, self.bucket, query);
        let url = Url::parse(&url_string)
            .map_err(|e| RawError::Backend(format!("invalid uploads url `{url_string}`: {e}")))?;
        let attach = self
            .signed_headers("GET", &canonical_uri, &query, Vec::new())
            .await?;
        let request = self.apply_headers(self.client.request(Method::GET, url), attach)?;
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let response = ensure_success(response).await?;
        let xml = response
            .text()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        Ok(parse_uploads(&xml))
    }

    /// Fetches the origin's GetObjectAttributes for `key` (issue #154):
    /// `?attributes` with the `x-amz-object-attributes` header listing the
    /// attributes requested. `max_parts`/`part_number_marker` paginate the
    /// parts section; `version_id` pins a version. Everything is forwarded
    /// verbatim — Verglas computes no attribute of its own.
    pub async fn get_object_attributes(
        &self,
        key: &str,
        object_attributes: &[String],
        max_parts: Option<i32>,
        part_number_marker: Option<i32>,
        version_id: Option<&str>,
    ) -> Result<RawAttributes, RawError> {
        let uri = self.object_uri(key);
        let mut params: Vec<(String, String)> = vec![("attributes".to_owned(), String::new())];
        if let Some(vid) = version_id {
            params.push(("versionId".to_owned(), vid.to_owned()));
        }
        let mut encoded: Vec<(String, String)> = params
            .iter()
            .map(|(k, v)| (encode_query(k), encode_query(v)))
            .collect();
        encoded.sort_by(|a, b| a.0.cmp(&b.0));
        let query = encoded
            .iter()
            // SigV4 canonicalizes an empty-value param as `key=`, so emit the
            // trailing `=` on the wire too or the origin recomputes a different
            // signature (issues #153/#154).
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let url_string = format!("{}{}?{}", self.endpoint, uri, query);
        let url = Url::parse(&url_string).map_err(|e| {
            RawError::Backend(format!("invalid attributes url `{url_string}`: {e}"))
        })?;

        let mut headers = vec![(
            "x-amz-object-attributes".to_owned(),
            object_attributes.join(","),
        )];
        if let Some(max) = max_parts {
            headers.push(("x-amz-max-parts".to_owned(), max.to_string()));
        }
        if let Some(marker) = part_number_marker {
            headers.push(("x-amz-part-number-marker".to_owned(), marker.to_string()));
        }
        let attach = self.signed_headers("GET", &uri, &query, headers).await?;
        let request = self.apply_headers(self.client.request(Method::GET, url), attach)?;
        let response = request
            .send()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        let response = ensure_success(response).await?;
        let last_modified =
            str_header_named(response.headers(), "last-modified").and_then(|s| parse_http_date(&s));
        let version_id = str_header_named(response.headers(), "x-amz-version-id");
        let xml = response
            .text()
            .await
            .map_err(|e| RawError::Backend(e.to_string()))?;
        Ok(parse_attributes(&xml, last_modified, version_id))
    }

    /// Attaches the signed header list (including `Authorization`) to a request.
    fn apply_headers(
        &self,
        mut request: reqwest::RequestBuilder,
        attach: Vec<(String, String)>,
    ) -> Result<reqwest::RequestBuilder, RawError> {
        for (name, value) in attach {
            let header_name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| RawError::Backend(format!("invalid header name `{name}`: {e}")))?;
            let header_value = HeaderValue::from_str(&value).map_err(|e| {
                RawError::Backend(format!("invalid header value for `{name}`: {e}"))
            })?;
            request = request.header(header_name, header_value);
        }
        Ok(request)
    }

    /// Computes the SigV4 `UNSIGNED-PAYLOAD` signature over `method`,
    /// `canonical_uri`, `canonical_query`, and `meta_headers`, and returns the
    /// full list of headers to attach (the metadata headers, `x-amz-date`,
    /// `x-amz-content-sha256`, `x-amz-security-token` when present, and
    /// `Authorization`). `Host` is signed but left for reqwest to send.
    async fn signed_headers(
        &self,
        method: &str,
        canonical_uri: &str,
        canonical_query: &str,
        meta_headers: Vec<(String, String)>,
    ) -> Result<Vec<(String, String)>, RawError> {
        let credentials = self
            .credentials
            .get_credential()
            .await
            .map_err(|error| RawError::Credentials(error.to_string()))?;
        let (amz_date, date_stamp) = amz_dates();

        let mut pairs: Vec<(String, String)> = vec![
            ("host".to_owned(), self.host.clone()),
            (
                "x-amz-content-sha256".to_owned(),
                UNSIGNED_PAYLOAD.to_owned(),
            ),
            ("x-amz-date".to_owned(), amz_date.clone()),
        ];
        if let Some(token) = &credentials.token {
            pairs.push(("x-amz-security-token".to_owned(), token.clone()));
        }
        for (name, value) in &meta_headers {
            pairs.push((name.clone(), value.trim().to_owned()));
        }
        pairs.sort_by(|a, b| a.0.cmp(&b.0));

        let signed_headers = pairs
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers = pairs
            .iter()
            .map(|(name, value)| format!("{name}:{value}"))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";

        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{UNSIGNED_PAYLOAD}"
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{date_stamp}/{}/s3/aws4_request\n{}",
            self.region,
            hex_sha256(canonical_request.as_bytes())
        );
        let signature = sigv4_signature(
            &credentials.secret_key,
            &date_stamp,
            &self.region,
            "s3",
            &string_to_sign,
        )?;
        let credential = format!(
            "{}/{date_stamp}/{}/s3/aws4_request",
            credentials.key_id, self.region
        );
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={credential}, SignedHeaders={signed_headers}, Signature={signature}"
        );

        let mut attach: Vec<(String, String)> = pairs
            .into_iter()
            .filter(|(name, _)| name != "host")
            .collect();
        attach.push(("authorization".to_owned(), authorization));
        Ok(attach)
    }
}

/// Flattens [`RawWriteHeaders`] into the signed `(name, value)` request-header
/// pairs a metadata-carrying write sends: each present system header plus
/// every `x-amz-meta-*` pair.
fn write_header_pairs(headers: RawWriteHeaders) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    if let Some(v) = headers.content_type {
        pairs.push(("content-type".to_owned(), v));
    }
    if let Some(v) = headers.cache_control {
        pairs.push(("cache-control".to_owned(), v));
    }
    if let Some(v) = headers.content_disposition {
        pairs.push(("content-disposition".to_owned(), v));
    }
    if let Some(v) = headers.content_encoding {
        pairs.push(("content-encoding".to_owned(), v));
    }
    if let Some(v) = headers.content_language {
        pairs.push(("content-language".to_owned(), v));
    }
    if let Some(v) = headers.expires {
        pairs.push(("expires".to_owned(), v));
    }
    headers.checksum.append_pairs(&mut pairs);
    for (k, v) in headers.user_metadata {
        pairs.push((format!("x-amz-meta-{}", k.to_ascii_lowercase()), v));
    }
    pairs
}

/// Joins buffered body chunks into one contiguous [`Bytes`] (no copy for the
/// zero/one-chunk cases). Bounded by the caller's slice size, never a whole
/// object.
fn concat_chunks(mut chunks: Vec<Bytes>) -> Bytes {
    match chunks.len() {
        0 => Bytes::new(),
        1 => chunks.swap_remove(0),
        _ => {
            let total: usize = chunks.iter().map(Bytes::len).sum();
            let mut joined = Vec::with_capacity(total);
            for chunk in chunks {
                joined.extend_from_slice(&chunk);
            }
            Bytes::from(joined)
        }
    }
}

/// Escapes text placed inside an XML element body (the completion manifest's
/// ETags).
fn xml_escape_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Whether a response header is hop-by-hop framing the passthrough must not
/// copy onto the re-framed response (issue #152). The body is buffered and
/// re-emitted, so its length and transfer coding are recomputed by the
/// front-end; `connection`/`keep-alive` describe the origin connection, not the
/// client's.
fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "transfer-encoding"
            | "content-length"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "upgrade"
    )
}

/// Reads a header as an owned UTF-8 string, or `None` if absent / non-UTF-8.
fn str_header(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
}

/// Parses response headers into [`RawObjectMeta`], collecting every
/// `x-amz-meta-*` header (prefix stripped, already-lowercase name) into
/// `user_metadata`.
fn meta_from_headers(headers: &HeaderMap) -> RawObjectMeta {
    let size = headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    let mut user_metadata = BTreeMap::new();
    for (name, value) in headers.iter() {
        if let Some(key) = name.as_str().strip_prefix("x-amz-meta-")
            && let Ok(value) = value.to_str()
        {
            user_metadata.insert(key.to_owned(), value.to_owned());
        }
    }

    RawObjectMeta {
        size,
        e_tag: str_header(headers, &ETAG),
        last_modified: str_header(headers, &LAST_MODIFIED).and_then(|s| parse_http_date(&s)),
        content_type: str_header(headers, &CONTENT_TYPE),
        cache_control: str_header(headers, &CACHE_CONTROL),
        content_disposition: str_header(headers, &CONTENT_DISPOSITION),
        content_encoding: str_header(headers, &CONTENT_ENCODING),
        content_language: str_header(headers, &CONTENT_LANGUAGE),
        expires: str_header(headers, &EXPIRES),
        version_id: str_header_named(headers, "x-amz-version-id"),
        parts_count: str_header_named(headers, "x-amz-mp-parts-count")
            .and_then(|s| s.parse::<i32>().ok()),
        checksums: RawChecksums::from_headers(headers),
        user_metadata,
    }
}

/// Reads a header by its lowercase name as an owned UTF-8 string, or `None`.
fn str_header_named(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

/// Translates a [`RawRange`] into an S3 `Range` header value, converting the
/// half-open exclusive end of `Bounded` to S3's inclusive last byte.
fn range_header(range: &RawRange) -> Option<String> {
    match range {
        RawRange::Full => None,
        RawRange::Offset(start) => Some(format!("bytes={start}-")),
        RawRange::Bounded(start, end) => Some(format!("bytes={start}-{}", end.saturating_sub(1))),
        RawRange::Suffix(length) => Some(format!("bytes=-{length}")),
    }
}

/// Parses a `Content-Range` header (`bytes start-end/total`) into the resolved
/// half-open byte range plus the full object size, falling back to
/// `(0..content_length, None)` when absent or unparseable.
fn parse_content_range(value: Option<&str>, content_length: u64) -> (Range<u64>, Option<u64>) {
    if let Some(value) = value
        && let Some(rest) = value.trim().strip_prefix("bytes ")
        && let Some((span, total)) = rest.split_once('/')
        && let Some((start, end)) = span.split_once('-')
        && let (Ok(start), Ok(end)) = (start.trim().parse::<u64>(), end.trim().parse::<u64>())
    {
        return (start..end.saturating_add(1), total.trim().parse().ok());
    }
    (0..content_length, None)
}

/// Returns `(x-amz-date, date-stamp)` — `YYYYMMDDTHHMMSSZ` and `YYYYMMDD` — for
/// the current UTC instant.
fn amz_dates() -> (String, String) {
    let now: DateTime<Utc> = Utc::now();
    (
        now.format("%Y%m%dT%H%M%SZ").to_string(),
        now.format("%Y%m%d").to_string(),
    )
}

/// Parses an RFC 1123 HTTP date (`Wed, 21 Oct 2015 07:28:00 GMT`) to a
/// [`SystemTime`], returning `None` on a parse failure.
fn parse_http_date(value: &str) -> Option<SystemTime> {
    let naive = NaiveDateTime::parse_from_str(value, "%a, %d %b %Y %H:%M:%S GMT").ok()?;
    Some(SystemTime::from(naive.and_utc()))
}

/// Parses an RFC 3339 timestamp (ListObjectsV2 `LastModified`) to a
/// [`SystemTime`], returning `None` on a parse failure.
fn parse_rfc3339(value: &str) -> Option<SystemTime> {
    let parsed = DateTime::parse_from_rfc3339(value).ok()?;
    Some(SystemTime::from(parsed.with_timezone(&Utc)))
}

/// Hex-encoded SHA-256 digest of `bytes`.
fn hex_sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// HMAC-SHA256 of `data` under `key`, as a raw 32-byte tag. HMAC accepts any key
/// length, so the `Result` is threaded only to avoid an `unwrap`.
fn hmac_sign(key: &[u8], data: &[u8]) -> Result<Vec<u8>, RawError> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)
        .map_err(|e| RawError::Backend(format!("hmac init: {e}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

/// Derives the SigV4 signing key from the secret and scope, then signs
/// `string_to_sign`, returning the hex signature.
fn sigv4_signature(
    secret_key: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> Result<String, RawError> {
    let k_secret = format!("AWS4{secret_key}");
    let k_date = hmac_sign(k_secret.as_bytes(), date_stamp.as_bytes())?;
    let k_region = hmac_sign(&k_date, region.as_bytes())?;
    let k_service = hmac_sign(&k_region, service.as_bytes())?;
    let k_signing = hmac_sign(&k_service, b"aws4_request")?;
    Ok(hex::encode(hmac_sign(
        &k_signing,
        string_to_sign.as_bytes(),
    )?))
}

/// Extracts the `<Code>` element from an S3 error XML body.
fn error_code(xml: &str) -> Option<String> {
    let start = xml.find("<Code>")? + "<Code>".len();
    let end = xml[start..].find("</Code>")? + start;
    Some(xml[start..end].to_owned())
}

/// Returns the response unchanged on a 2xx status; otherwise maps the status and
/// body to a [`RawError`].
async fn ensure_success(response: Response) -> Result<Response, RawError> {
    if response.status().is_success() {
        return Ok(response);
    }
    Err(map_error(response).await)
}

/// Maps a non-success response to a [`RawError`], reading the S3 error `<Code>`
/// from the body to distinguish `NoSuchBucket` from `NoSuchKey` on a 404.
async fn map_error(response: Response) -> RawError {
    let status = response.status();
    if status == StatusCode::NOT_MODIFIED {
        return RawError::NotModified;
    }
    if status == StatusCode::FORBIDDEN {
        return RawError::AccessDenied;
    }
    if status == StatusCode::RANGE_NOT_SATISFIABLE {
        return RawError::InvalidRange;
    }
    let body = response.text().await.unwrap_or_default();
    if status == StatusCode::NOT_FOUND {
        match error_code(&body).as_deref() {
            Some("NoSuchBucket") => RawError::NoSuchBucket,
            _ => RawError::NoSuchKey,
        }
    } else if let Some(code) = error_code(&body) {
        // A 4xx carries an S3 `<Code>` the front-end must preserve — an
        // out-of-range part (`InvalidPart`) or copy range (`InvalidRange`)
        // reaches the client as a 400/416, not a flattened 500 (issues #153).
        match code.as_str() {
            "InvalidPart" => RawError::InvalidPart,
            "InvalidRange" => RawError::InvalidRange,
            "InvalidArgument" => RawError::InvalidArgument,
            _ => RawError::Backend(format!("{status}: {body}")),
        }
    } else {
        RawError::Backend(format!("{status}: {body}"))
    }
}

/// Maps an S3 error `<Code>` recovered from a 200-status body (the copy/complete
/// operations answer some failures that way) to the matching [`RawError`].
fn map_body_code(code: &str) -> RawError {
    match code {
        "NoSuchKey" => RawError::NoSuchKey,
        "NoSuchBucket" => RawError::NoSuchBucket,
        "AccessDenied" => RawError::AccessDenied,
        "InvalidPart" => RawError::InvalidPart,
        "InvalidRange" => RawError::InvalidRange,
        "InvalidArgument" => RawError::InvalidArgument,
        other => RawError::Backend(format!("origin error: {other}")),
    }
}

/// Extracts the first `<tag>…</tag>` element's inner text from `xml`, with XML
/// entities unescaped.
fn tag_text(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml_unescape(&xml[start..end]))
}

/// Unescapes the XML entities S3 emits (`&amp; &lt; &gt; &quot; &apos;` and
/// numeric `&#N;` / `&#xH;`), leaving unrecognised `&` sequences intact.
fn xml_unescape(input: &str) -> String {
    if !input.contains('&') {
        return input.to_owned();
    }
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < input.len() {
        if bytes[i] == b'&' {
            if let Some(offset) = input[i..].find(';') {
                let entity = &input[i + 1..i + offset];
                let decoded = match entity {
                    "amp" => Some('&'),
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    "quot" => Some('"'),
                    "apos" => Some('\''),
                    _ => {
                        if let Some(hexits) = entity
                            .strip_prefix("#x")
                            .or_else(|| entity.strip_prefix("#X"))
                        {
                            u32::from_str_radix(hexits, 16)
                                .ok()
                                .and_then(char::from_u32)
                        } else if let Some(digits) = entity.strip_prefix('#') {
                            digits.parse::<u32>().ok().and_then(char::from_u32)
                        } else {
                            None
                        }
                    }
                };
                if let Some(ch) = decoded {
                    out.push(ch);
                    i += offset + 1;
                    continue;
                }
            }
            out.push('&');
            i += 1;
        } else {
            let ch = input[i..].chars().next().unwrap_or('\u{FFFD}');
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

/// Parses a ListObjectsV2 XML body into a [`RawListPage`], percent-decoding each
/// `<Key>` (the response used `encoding-type=url`) so keys are byte-exact.
fn parse_list(xml: &str) -> RawListPage {
    let is_truncated = tag_text(xml, "IsTruncated")
        .map(|v| v == "true")
        .unwrap_or(false);
    let next_continuation_token = if is_truncated {
        tag_text(xml, "NextContinuationToken")
    } else {
        None
    };

    let mut entries = Vec::new();
    let mut rest = xml;
    while let Some(index) = rest.find("<Contents>") {
        let after = &rest[index + "<Contents>".len()..];
        let end = match after.find("</Contents>") {
            Some(end) => end,
            None => break,
        };
        let block = &after[..end];
        let key_encoded = tag_text(block, "Key").unwrap_or_default();
        // AWS-style `encoding-type=url` encodes a space as `+` (a literal `+`
        // arrives as `%2B`), so map `+` to space BEFORE percent-decoding —
        // python's `unquote_plus`, which is what boto3 applies to these keys.
        let key = percent_decode_str(&key_encoded.replace('+', " "))
            .decode_utf8_lossy()
            .into_owned();
        let size = tag_text(block, "Size")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let e_tag = tag_text(block, "ETag");
        let last_modified = tag_text(block, "LastModified").and_then(|s| parse_rfc3339(&s));
        entries.push(RawListEntry {
            key,
            size,
            e_tag,
            last_modified,
        });
        rest = &after[end + "</Contents>".len()..];
    }

    RawListPage {
        entries,
        next_continuation_token,
    }
}

/// Returns the inner text of the first `<tag>…</tag>` block in `xml` (element
/// content, not entity-unescaped — for nested-block extraction), or `None`.
fn inner_block<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

/// Percent-decodes an `encoding-type=url` key or prefix to its exact bytes
/// (`+` → space first, matching python's `unquote_plus`, as [`parse_list`] does).
fn decode_url_value(encoded: &str) -> String {
    percent_decode_str(&encoded.replace('+', " "))
        .decode_utf8_lossy()
        .into_owned()
}

/// Reads the checksum values out of a `<Checksum>`/`<Part>` sub-block.
fn checksums_from_block(block: &str) -> RawChecksums {
    RawChecksums {
        crc32: tag_text(block, "ChecksumCRC32"),
        crc32c: tag_text(block, "ChecksumCRC32C"),
        crc64nvme: tag_text(block, "ChecksumCRC64NVME"),
        sha1: tag_text(block, "ChecksumSHA1"),
        sha256: tag_text(block, "ChecksumSHA256"),
        checksum_type: tag_text(block, "ChecksumType"),
    }
}

/// Parses a ListMultipartUploads XML body into a [`RawUploadsPage`] (issue
/// #153), percent-decoding each `<Key>` and `<Prefix>` (the response used
/// `encoding-type=url`) so they are byte-exact.
fn parse_uploads(xml: &str) -> RawUploadsPage {
    let is_truncated = tag_text(xml, "IsTruncated")
        .map(|v| v == "true")
        .unwrap_or(false);
    let next_key_marker = tag_text(xml, "NextKeyMarker").map(|s| decode_url_value(&s));
    let next_upload_id_marker = tag_text(xml, "NextUploadIdMarker");

    let mut uploads = Vec::new();
    let mut rest = xml;
    while let Some(index) = rest.find("<Upload>") {
        let after = &rest[index + "<Upload>".len()..];
        let end = match after.find("</Upload>") {
            Some(end) => end,
            None => break,
        };
        let block = &after[..end];
        let key = tag_text(block, "Key")
            .map(|s| decode_url_value(&s))
            .unwrap_or_default();
        let upload_id = tag_text(block, "UploadId").unwrap_or_default();
        let storage_class = tag_text(block, "StorageClass");
        let initiated = tag_text(block, "Initiated").and_then(|s| parse_rfc3339(&s));
        uploads.push(RawUpload {
            key,
            upload_id,
            storage_class,
            initiated,
        });
        rest = &after[end + "</Upload>".len()..];
    }

    let mut common_prefixes = Vec::new();
    let mut rest = xml;
    while let Some(index) = rest.find("<CommonPrefixes>") {
        let after = &rest[index + "<CommonPrefixes>".len()..];
        let end = match after.find("</CommonPrefixes>") {
            Some(end) => end,
            None => break,
        };
        let block = &after[..end];
        if let Some(prefix) = tag_text(block, "Prefix") {
            common_prefixes.push(decode_url_value(&prefix));
        }
        rest = &after[end + "</CommonPrefixes>".len()..];
    }

    RawUploadsPage {
        uploads,
        common_prefixes,
        is_truncated,
        next_key_marker,
        next_upload_id_marker,
    }
}

/// Parses a GetObjectAttributes XML body into a [`RawAttributes`] (issue #154).
/// `last_modified`/`version_id` come from response headers the body omits.
fn parse_attributes(
    xml: &str,
    last_modified: Option<SystemTime>,
    version_id: Option<String>,
) -> RawAttributes {
    let e_tag = tag_text(xml, "ETag");
    let object_size = tag_text(xml, "ObjectSize").and_then(|s| s.parse::<u64>().ok());
    let storage_class = tag_text(xml, "StorageClass");
    let checksums = inner_block(xml, "Checksum")
        .map(checksums_from_block)
        .unwrap_or_default();

    let object_parts = inner_block(xml, "ObjectParts").map(|block| {
        // AWS's own docs disagree on the element name — some origins emit
        // `<TotalPartsCount>`, others (MinIO) `<PartsCount>`; accept either.
        let total_parts_count = tag_text(block, "TotalPartsCount")
            .or_else(|| tag_text(block, "PartsCount"))
            .and_then(|s| s.parse().ok());
        let max_parts = tag_text(block, "MaxParts").and_then(|s| s.parse().ok());
        let part_number_marker = tag_text(block, "PartNumberMarker").and_then(|s| s.parse().ok());
        let next_part_number_marker =
            tag_text(block, "NextPartNumberMarker").and_then(|s| s.parse().ok());
        let is_truncated = tag_text(block, "IsTruncated")
            .map(|v| v == "true")
            .unwrap_or(false);
        let mut parts = Vec::new();
        let mut rest = block;
        while let Some(index) = rest.find("<Part>") {
            let after = &rest[index + "<Part>".len()..];
            let end = match after.find("</Part>") {
                Some(end) => end,
                None => break,
            };
            let part_block = &after[..end];
            let part_number = tag_text(part_block, "PartNumber")
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);
            let size = tag_text(part_block, "Size")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            parts.push(RawObjectPart {
                part_number,
                size,
                checksums: checksums_from_block(part_block),
            });
            rest = &after[end + "</Part>".len()..];
        }
        RawObjectParts {
            parts,
            total_parts_count,
            max_parts,
            part_number_marker,
            next_part_number_marker,
            is_truncated,
        }
    });

    RawAttributes {
        e_tag,
        object_size,
        storage_class,
        last_modified,
        version_id,
        checksums,
        object_parts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #153/#154/#156: read extras render a SigV4-canonical query — sorted,
    /// with `partNumber` before `versionId`, and an empty string when none set.
    #[test]
    fn read_extras_canonical_query() {
        assert_eq!(RawReadExtras::default().canonical_query(), "");
        let extras = RawReadExtras {
            version_id: Some("v1".to_owned()),
            part_number: Some(3),
            checksum_mode: true,
        };
        assert_eq!(extras.canonical_query(), "partNumber=3&versionId=v1");
        // checksum-mode is a header, not a query param.
        assert_eq!(
            extras.header_pairs(),
            vec![("x-amz-checksum-mode".to_owned(), "ENABLED".to_owned())]
        );
    }

    /// #153: an S3 error `<Code>` recovered from a 200 body maps to the right
    /// typed error.
    #[test]
    fn body_code_maps_invalid_part_and_range() {
        assert!(matches!(
            map_body_code("InvalidPart"),
            RawError::InvalidPart
        ));
        assert!(matches!(
            map_body_code("InvalidRange"),
            RawError::InvalidRange
        ));
        assert!(matches!(map_body_code("NoSuchKey"), RawError::NoSuchKey));
        assert!(matches!(map_body_code("Weird"), RawError::Backend(_)));
    }

    /// #153: ListMultipartUploads XML parses uploads (keys url-decoded), common
    /// prefixes, and the truncation markers.
    #[test]
    fn parse_uploads_extracts_uploads_and_markers() {
        let xml = r#"<?xml version="1.0"?>
        <ListMultipartUploadsResult>
          <IsTruncated>true</IsTruncated>
          <NextKeyMarker>a%2Fb</NextKeyMarker>
          <NextUploadIdMarker>uZZ</NextUploadIdMarker>
          <Upload><Key>my+key</Key><UploadId>u1</UploadId>
            <StorageClass>STANDARD</StorageClass>
            <Initiated>2024-01-02T03:04:05.000Z</Initiated></Upload>
          <Upload><Key>other</Key><UploadId>u2</UploadId></Upload>
          <CommonPrefixes><Prefix>pre%2F</Prefix></CommonPrefixes>
        </ListMultipartUploadsResult>"#;
        let page = parse_uploads(xml);
        assert_eq!(page.uploads.len(), 2);
        // `+` decodes to space (unquote_plus), `%2B` would be a literal `+`.
        assert_eq!(page.uploads[0].key, "my key");
        assert_eq!(page.uploads[0].upload_id, "u1");
        assert_eq!(page.uploads[0].storage_class.as_deref(), Some("STANDARD"));
        assert!(page.uploads[0].initiated.is_some());
        assert_eq!(page.uploads[1].key, "other");
        assert_eq!(page.common_prefixes, vec!["pre/".to_owned()]);
        assert!(page.is_truncated);
        assert_eq!(page.next_key_marker.as_deref(), Some("a/b"));
        assert_eq!(page.next_upload_id_marker.as_deref(), Some("uZZ"));
    }

    /// #154: GetObjectAttributes XML parses the object-level fields, the
    /// checksum block, and the paginated parts section.
    #[test]
    fn parse_attributes_object_and_parts() {
        let xml = r#"<?xml version="1.0"?>
        <GetObjectAttributesOutput>
          <ETag>abc123</ETag>
          <ObjectSize>31457280</ObjectSize>
          <StorageClass>STANDARD</StorageClass>
          <Checksum><ChecksumSHA256>Zm9v</ChecksumSHA256>
            <ChecksumType>COMPOSITE</ChecksumType></Checksum>
          <ObjectParts>
            <TotalPartsCount>6</TotalPartsCount>
            <MaxParts>1</MaxParts>
            <PartNumberMarker>3</PartNumberMarker>
            <NextPartNumberMarker>4</NextPartNumberMarker>
            <IsTruncated>true</IsTruncated>
            <Part><PartNumber>4</PartNumber><Size>5242880</Size></Part>
          </ObjectParts>
        </GetObjectAttributesOutput>"#;
        let attrs = parse_attributes(xml, None, Some("v9".to_owned()));
        assert_eq!(attrs.e_tag.as_deref(), Some("abc123"));
        assert_eq!(attrs.object_size, Some(31457280));
        assert_eq!(attrs.storage_class.as_deref(), Some("STANDARD"));
        assert_eq!(attrs.version_id.as_deref(), Some("v9"));
        assert_eq!(attrs.checksums.sha256.as_deref(), Some("Zm9v"));
        assert_eq!(attrs.checksums.checksum_type.as_deref(), Some("COMPOSITE"));
        let parts = attrs.object_parts.expect("multipart");
        assert_eq!(parts.total_parts_count, Some(6));
        assert_eq!(parts.max_parts, Some(1));
        assert_eq!(parts.part_number_marker, Some(3));
        assert_eq!(parts.next_part_number_marker, Some(4));
        assert!(parts.is_truncated);
        assert_eq!(parts.parts.len(), 1);
        assert_eq!(parts.parts[0].part_number, 4);
        assert_eq!(parts.parts[0].size, 5242880);
    }

    /// #154: a non-multipart object has no `<ObjectParts>`, so the parts section
    /// is absent (the test-suite asserts `ObjectParts` is not in the response).
    #[test]
    fn parse_attributes_non_multipart_has_no_parts() {
        let xml = "<GetObjectAttributesOutput><ETag>e</ETag>\
                   <ObjectSize>3</ObjectSize>\
                   <StorageClass>STANDARD</StorageClass></GetObjectAttributesOutput>";
        let attrs = parse_attributes(xml, None, None);
        assert!(attrs.object_parts.is_none());
        assert!(attrs.checksums.is_empty());
    }
}
