//! Byte-exact round-trip tests for the raw S3 client (`raw.rs`).
//!
//! A deterministic in-process mock S3 origin (axum, signature-ignoring) stores
//! objects keyed by the EXACT decoded bytes of the request path. The tests drive
//! [`RawS3`] against it to prove that pathological keys — trailing slashes, empty
//! segments, control characters, spaces, reserved punctuation — survive PUT,
//! HEAD, and ListObjectsV2 byte-for-byte, that the `Expires` header round-trips,
//! that user metadata and content-type round-trip, that pagination works, that
//! the raw multipart trio assembles parts in order while carrying metadata, that
//! conditional HEAD / ranged GET / error statuses map to the right [`RawError`]
//! variants, and that AWS's `+`-for-space list encoding decodes byte-exact.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use futures::stream::StreamExt;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use verglas_backend::{
    RawChecksums, RawCompletedPart, RawError, RawGet, RawRange, RawS3, RawWriteChecksum,
    RawWriteHeaders,
};

/// The bucket every test addresses.
const BUCKET: &str = "bucket";

/// A stored object: its exact bytes plus the metadata headers the origin echoes.
#[derive(Clone, Default)]
struct StoredObject {
    /// The object body as received.
    body: Vec<u8>,
    /// The synthetic ETag assigned at store time (quotes included).
    etag: String,
    /// The stored `Content-Type`, if the PUT sent one.
    content_type: Option<String>,
    /// The stored `Cache-Control`, if the PUT sent one.
    cache_control: Option<String>,
    /// The stored raw `Expires` header, if the PUT sent one.
    expires: Option<String>,
    /// The stored `x-amz-meta-*` headers (name without prefix).
    user_meta: BTreeMap<String, String>,
    /// The object-level SHA256 checksum the completion asserted, if any (#208).
    checksum_sha256: Option<String>,
}

/// An in-flight multipart upload: the destination key, the metadata headers
/// remembered from the initiate request, and the parts received so far.
#[derive(Clone, Default)]
struct Upload {
    /// The exact decoded key the completed object will be stored under.
    key: String,
    /// `Content-Type` remembered from the initiate request.
    content_type: Option<String>,
    /// `Cache-Control` remembered from the initiate request.
    cache_control: Option<String>,
    /// Raw `Expires` header remembered from the initiate request.
    expires: Option<String>,
    /// `x-amz-meta-*` headers remembered from the initiate request.
    user_meta: BTreeMap<String, String>,
    /// The checksum algorithm the initiate request selected, if any (#208).
    checksum_algorithm: Option<String>,
    /// The checksum type the initiate request selected, if any (#208).
    checksum_type: Option<String>,
    /// Part bodies keyed by part number; the BTreeMap keeps them in part order.
    parts: BTreeMap<usize, Vec<u8>>,
    /// Per-part SHA256 checksums the parts asserted, keyed by part number (#208).
    part_checksums: BTreeMap<usize, String>,
}

/// The mock origin's whole mutable state: stored objects keyed by exact key
/// bytes, in-flight multipart uploads keyed by upload id, and the id counter.
#[derive(Default)]
struct MockState {
    /// Completed objects, keyed by the exact decoded key bytes.
    objects: BTreeMap<String, StoredObject>,
    /// In-flight multipart uploads, keyed by upload id.
    uploads: BTreeMap<String, Upload>,
    /// Monotonic counter used to mint upload ids.
    next_upload_id: u64,
}

/// Shared, thread-safe mock-origin state.
type Store = std::sync::Arc<Mutex<MockState>>;

/// Percent-encodes a key for `encoding-type=url` list responses the way AWS
/// does: a space becomes `+`, a literal `+` stays `%2B`, the unreserved set
/// stays literal, and everything else is percent-encoded. This pins the
/// client's unquote-plus list decoding.
fn encode_list_key(key: &str) -> String {
    utf8_percent_encode(key, NON_ALPHANUMERIC)
        .to_string()
        // keep the unreserved set literal so the client decodes to exact bytes
        .replace("%2D", "-")
        .replace("%2E", ".")
        .replace("%5F", "_")
        .replace("%7E", "~")
        // AWS parity: a space is encoded as `+` (a literal `+` is `%2B` above)
        .replace("%20", "+")
}

/// Minimal XML-escaping for text placed in element bodies.
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Percent-decodes a URL path component to its exact bytes.
fn decode(component: &str) -> String {
    percent_decode_str(component)
        .decode_utf8_lossy()
        .into_owned()
}

/// Parses a raw query string into decoded key/value pairs.
fn parse_query(query: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        map.insert(decode(key), decode(value));
    }
    map
}

/// The single handler: rejects the synthetic `missing-bucket` / `denied-bucket`
/// buckets before any store lookup, dispatches ListObjectsV2 and the multipart
/// sub-resource requests by query, then falls through to plain object methods.
async fn handler(State(store): State<Store>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let method = parts.method.clone();
    let path = parts.uri.path().to_owned();
    let query = parts.uri.query().unwrap_or("").to_owned();
    let params = parse_query(&query);

    let after_bucket = path.strip_prefix('/').unwrap_or(&path);
    let (bucket, key_encoded) = match after_bucket.split_once('/') {
        Some((bucket, key)) => (bucket.to_owned(), key.to_owned()),
        None => (after_bucket.to_owned(), String::new()),
    };
    let key = decode(&key_encoded);

    // Synthetic error buckets are decided by path inspection alone, BEFORE any
    // store lookup, so they answer every object method uniformly.
    if bucket == "missing-bucket" {
        return no_such_bucket();
    }
    if bucket == "denied-bucket" {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::empty())
            .expect("denied response");
    }

    if method == Method::GET && query.contains("list-type=2") {
        return list_response(&store, &query);
    }

    // Bucket-level control-plane requests (empty key): the surface the #152
    // passthrough forwards. HEAD answers 200 for an existing bucket (with an
    // origin-specific header to prove verbatim forwarding); GET ?location
    // answers a LocationConstraint document.
    if key_encoded.is_empty() && bucket != "missing-bucket" {
        if method == Method::HEAD {
            return Response::builder()
                .status(StatusCode::OK)
                .header("x-amz-bucket-region", "us-west-2")
                .body(Body::empty())
                .expect("head bucket response");
        }
        if method == Method::GET && params.contains_key("location") {
            let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                <LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                us-west-2</LocationConstraint>";
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(Body::from(xml))
                .expect("location response");
        }
    }

    // Multipart sub-resource requests are distinguished by their query.
    if method == Method::POST && params.contains_key("uploads") {
        return initiate_multipart_response(&store, &key, &parts.headers);
    }
    if method == Method::PUT
        && let (Some(part_number), Some(upload_id)) =
            (params.get("partNumber"), params.get("uploadId"))
    {
        let part_number = part_number.parse::<usize>().unwrap_or(0);
        let part_checksum = header_value(&parts.headers, "x-amz-checksum-sha256");
        let bytes = to_bytes(body, 10 * 1024 * 1024)
            .await
            .expect("read part body");
        return put_part_response(
            &store,
            upload_id,
            part_number,
            part_checksum,
            bytes.to_vec(),
        );
    }
    if method == Method::POST
        && let Some(upload_id) = params.get("uploadId")
    {
        let object_checksum = header_value(&parts.headers, "x-amz-checksum-sha256");
        let completion_type = header_value(&parts.headers, "x-amz-checksum-type");
        let manifest = to_bytes(body, 10 * 1024 * 1024)
            .await
            .expect("read completion manifest");
        return complete_multipart_response(
            &store,
            upload_id,
            object_checksum,
            completion_type,
            &String::from_utf8_lossy(&manifest),
        );
    }
    if method == Method::DELETE
        && let Some(upload_id) = params.get("uploadId")
    {
        store.lock().expect("store lock").uploads.remove(upload_id);
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("abort response");
    }

    match method {
        Method::PUT => {
            let etag = format!("\"etag-{}\"", key.len());
            let mut object = StoredObject {
                etag: etag.clone(),
                ..StoredObject::default()
            };
            object.content_type = header_value(&parts.headers, "content-type");
            object.cache_control = header_value(&parts.headers, "cache-control");
            object.expires = header_value(&parts.headers, "expires");
            for (name, value) in parts.headers.iter() {
                if let Some(meta) = name.as_str().strip_prefix("x-amz-meta-")
                    && let Ok(value) = value.to_str()
                {
                    object.user_meta.insert(meta.to_owned(), value.to_owned());
                }
            }
            let bytes = to_bytes(body, 10 * 1024 * 1024)
                .await
                .expect("read put body");
            object.body = bytes.to_vec();
            store
                .lock()
                .expect("store lock")
                .objects
                .insert(key, object);
            Response::builder()
                .status(StatusCode::OK)
                .header("etag", etag)
                .body(Body::empty())
                .expect("put response")
        }
        Method::HEAD => {
            let guard = store.lock().expect("store lock");
            match guard.objects.get(&key) {
                Some(object) => {
                    // Honour `If-None-Match`: an unchanged ETag answers 304.
                    if header_value(&parts.headers, "if-none-match").as_deref()
                        == Some(object.etag.as_str())
                    {
                        return Response::builder()
                            .status(StatusCode::NOT_MODIFIED)
                            .body(Body::empty())
                            .expect("not modified response");
                    }
                    object_head_response(object)
                }
                None => not_found(),
            }
        }
        Method::GET => {
            let guard = store.lock().expect("store lock");
            match guard.objects.get(&key) {
                Some(object) => object_get_response(object, header_value(&parts.headers, "range")),
                None => not_found(),
            }
        }
        Method::DELETE => {
            store.lock().expect("store lock").objects.remove(&key);
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("delete response")
        }
        _ => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty())
            .expect("method not allowed"),
    }
}

/// Allocates an upload id, remembers the destination key and the metadata
/// headers of the initiate request, and returns the S3-shaped initiate XML.
fn initiate_multipart_response(
    store: &Store,
    key: &str,
    headers: &axum::http::HeaderMap,
) -> Response {
    let mut upload = Upload {
        key: key.to_owned(),
        content_type: header_value(headers, "content-type"),
        cache_control: header_value(headers, "cache-control"),
        expires: header_value(headers, "expires"),
        checksum_algorithm: header_value(headers, "x-amz-checksum-algorithm"),
        checksum_type: header_value(headers, "x-amz-checksum-type"),
        ..Upload::default()
    };
    for (name, value) in headers.iter() {
        if let Some(meta) = name.as_str().strip_prefix("x-amz-meta-")
            && let Ok(value) = value.to_str()
        {
            upload.user_meta.insert(meta.to_owned(), value.to_owned());
        }
    }
    // Echo the checksum algorithm/type back in the initiate XML, as S3 does.
    let mut extra = String::new();
    if let Some(algo) = &upload.checksum_algorithm {
        extra.push_str(&format!("<ChecksumAlgorithm>{algo}</ChecksumAlgorithm>"));
    }
    if let Some(kind) = &upload.checksum_type {
        extra.push_str(&format!("<ChecksumType>{kind}</ChecksumType>"));
    }
    let mut guard = store.lock().expect("store lock");
    guard.next_upload_id += 1;
    let upload_id = format!("upload-{}", guard.next_upload_id);
    guard.uploads.insert(upload_id.clone(), upload);
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(format!(
            "<InitiateMultipartUploadResult><UploadId>{upload_id}</UploadId>{extra}</InitiateMultipartUploadResult>"
        )))
        .expect("initiate response")
}

/// Stores one part's bytes under `(upload_id, part_number)`, remembers any
/// per-part checksum, and answers with a deterministic per-part ETag header,
/// echoing the checksum back the way S3 does (issue #208).
fn put_part_response(
    store: &Store,
    upload_id: &str,
    part_number: usize,
    part_checksum: Option<String>,
    bytes: Vec<u8>,
) -> Response {
    let mut guard = store.lock().expect("store lock");
    let Some(upload) = guard.uploads.get_mut(upload_id) else {
        return not_found();
    };
    let etag = format!("\"part-{part_number}-{}\"", bytes.len());
    upload.parts.insert(part_number, bytes);
    if let Some(checksum) = &part_checksum {
        upload.part_checksums.insert(part_number, checksum.clone());
    }
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("etag", etag);
    if let Some(checksum) = part_checksum {
        builder = builder.header("x-amz-checksum-sha256", checksum);
    }
    builder.body(Body::empty()).expect("part response")
}

/// Assembles the upload's parts in part-number order into a stored object at
/// the remembered key with the remembered metadata headers, drops the upload,
/// and answers with the S3-shaped completion XML (ETag XML-escaped). When the
/// completion asserted an object checksum it is stored and echoed back, the way
/// S3 does for a checksummed multipart upload (issue #208).
fn complete_multipart_response(
    store: &Store,
    upload_id: &str,
    object_checksum: Option<String>,
    completion_type: Option<String>,
    manifest: &str,
) -> Response {
    let mut guard = store.lock().expect("store lock");
    let Some(upload) = guard.uploads.remove(upload_id) else {
        return not_found();
    };
    // Mirror MinIO: a FULL_OBJECT upload's completion MUST restate
    // `x-amz-checksum-type`; without it the origin rejects the request with
    // `400 InvalidArgument`. The client sends the type only on create, so the
    // passthrough must remember and resend it (issue #208).
    if upload.checksum_type.as_deref() == Some("FULL_OBJECT")
        && completion_type.as_deref() != Some("FULL_OBJECT")
    {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("content-type", "application/xml")
            .body(Body::from(
                "<Error><Code>InvalidArgument</Code><Message>checksum type mismatch</Message></Error>",
            ))
            .expect("full-object type error");
    }
    // Every part the client asserted a checksum for must carry that same
    // checksum in the manifest — this is what proves the completion forwarded
    // the per-part checksums (issue #208).
    for (part_number, checksum) in &upload.part_checksums {
        assert!(
            manifest.contains(&format!("<ChecksumSHA256>{checksum}</ChecksumSHA256>")),
            "completion manifest must carry part {part_number}'s checksum"
        );
    }
    let mut body = Vec::new();
    for part in upload.parts.values() {
        body.extend_from_slice(part);
    }
    let object = StoredObject {
        body,
        etag: "\"mp-etag\"".to_owned(),
        content_type: upload.content_type,
        cache_control: upload.cache_control,
        expires: upload.expires,
        user_meta: upload.user_meta,
        checksum_sha256: object_checksum.clone(),
    };
    guard.objects.insert(upload.key, object);
    let checksum_xml = object_checksum
        .map(|c| format!("<ChecksumSHA256>{c}</ChecksumSHA256>"))
        .unwrap_or_default();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(format!(
            "<CompleteMultipartUploadResult><ETag>&quot;mp-etag&quot;</ETag>{checksum_xml}</CompleteMultipartUploadResult>"
        )))
        .expect("complete response")
}

/// Reads a request header as an owned string.
fn header_value(headers: &axum::http::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
}

/// Applies the stored metadata headers common to HEAD and GET responses.
fn with_object_headers(
    mut builder: axum::http::response::Builder,
    object: &StoredObject,
) -> axum::http::response::Builder {
    builder = builder
        .header("etag", &object.etag)
        .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT");
    if let Some(value) = &object.content_type {
        builder = builder.header("content-type", value);
    }
    if let Some(value) = &object.cache_control {
        builder = builder.header("cache-control", value);
    }
    if let Some(value) = &object.expires {
        builder = builder.header("expires", value);
    }
    for (name, value) in &object.user_meta {
        builder = builder.header(format!("x-amz-meta-{name}"), value);
    }
    if let Some(value) = &object.checksum_sha256 {
        builder = builder.header("x-amz-checksum-sha256", value);
    }
    builder
}

/// Builds a HEAD response: stored headers plus an explicit `Content-Length`.
fn object_head_response(object: &StoredObject) -> Response {
    let builder = Response::builder()
        .status(StatusCode::OK)
        .header("content-length", object.body.len().to_string());
    with_object_headers(builder, object)
        .body(Body::empty())
        .expect("head response")
}

/// Builds a GET response. Without a `Range` header the full body is returned;
/// with `bytes=a-b`, `bytes=a-`, or `bytes=-n` a 206 carrying the slice and a
/// `Content-Range: bytes a-b/total` is returned, and an unsatisfiable range
/// (start at or past the end) answers 416 with an empty body.
fn object_get_response(object: &StoredObject, range: Option<String>) -> Response {
    let len = object.body.len() as u64;
    if let Some(spec) = range.as_deref().and_then(|v| v.strip_prefix("bytes=")) {
        let (start, end) = if let Some(suffix) = spec.strip_prefix('-') {
            let n = suffix.parse::<u64>().unwrap_or(0);
            (len.saturating_sub(n), len)
        } else {
            let (first, last) = spec.split_once('-').unwrap_or((spec, ""));
            let start = first.parse::<u64>().unwrap_or(0);
            let end = if last.is_empty() {
                len
            } else {
                last.parse::<u64>().unwrap_or(0).saturating_add(1).min(len)
            };
            (start, end)
        };
        if start >= len {
            return Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .body(Body::empty())
                .expect("range not satisfiable response");
        }
        let slice = object.body[start as usize..end as usize].to_vec();
        let builder = Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("content-range", format!("bytes {start}-{}/{len}", end - 1));
        return with_object_headers(builder, object)
            .body(Body::from(slice))
            .expect("ranged get response");
    }
    let builder = Response::builder().status(StatusCode::OK);
    with_object_headers(builder, object)
        .body(Body::from(object.body.clone()))
        .expect("get response")
}

/// A 404 with an S3-shaped `NoSuchKey` error body.
fn not_found() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from(
            "<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>",
        ))
        .expect("not found response")
}

/// A 404 with an S3-shaped `NoSuchBucket` error body.
fn no_such_bucket() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from(
            "<?xml version=\"1.0\"?><Error><Code>NoSuchBucket</Code></Error>",
        ))
        .expect("no such bucket response")
}

/// Builds a ListObjectsV2 XML response honouring `prefix`, `start-after`,
/// `continuation-token`, `max-keys`, and `encoding-type=url`.
fn list_response(store: &Store, query: &str) -> Response {
    let params = parse_query(query);
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let cursor = params
        .get("continuation-token")
        .or_else(|| params.get("start-after"))
        .cloned();
    let max_keys: usize = params
        .get("max-keys")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let guard = store.lock().expect("store lock");
    let mut matched: Vec<(String, usize, String)> = guard
        .objects
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .filter(|(key, _)| cursor.as_ref().is_none_or(|c| *key > c))
        .map(|(key, object)| (key.clone(), object.body.len(), object.etag.clone()))
        .collect();

    let truncated = matched.len() > max_keys;
    matched.truncate(max_keys);
    let next_token = if truncated {
        matched.last().map(|(key, _, _)| key.clone())
    } else {
        None
    };

    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>");
    xml.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
    for (key, size, etag) in &matched {
        xml.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>2015-10-21T07:28:00.000Z</LastModified><ETag>{}</ETag><Size>{}</Size></Contents>",
            xml_escape(&encode_list_key(key)),
            xml_escape(etag),
            size
        ));
    }
    if let Some(token) = &next_token {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(token)
        ));
    }
    xml.push_str("</ListBucketResult>");

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(xml))
        .expect("list response")
}

/// Starts the mock origin once on a dedicated runtime thread (decoupled from the
/// per-test runtimes) and sets the `AWS_*` env once. Returns the shared store.
fn setup() -> Store {
    static STORE: OnceLock<Store> = OnceLock::new();
    STORE
        .get_or_init(|| {
            let store: Store = std::sync::Arc::new(Mutex::new(MockState::default()));
            let server_store = store.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build runtime");
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind listener");
                    let addr = listener.local_addr().expect("local addr");
                    tx.send(addr).expect("send addr");
                    let app = Router::new().fallback(handler).with_state(server_store);
                    axum::serve(listener, app).await.expect("serve");
                });
            });
            let addr = rx.recv().expect("recv addr");
            // SAFETY: env is set exactly once, before any test reads it, guarded
            // by the OnceLock; no other thread mutates the environment.
            unsafe {
                std::env::set_var("AWS_ENDPOINT", format!("http://{addr}"));
                std::env::set_var("AWS_ACCESS_KEY_ID", "test-access-key");
                std::env::set_var("AWS_SECRET_ACCESS_KEY", "test-secret-key");
                std::env::set_var("AWS_ALLOW_HTTP", "true");
                std::env::set_var("AWS_REGION", "us-east-1");
            }
            store
        })
        .clone()
}

/// Wraps `bytes` as a single-chunk PUT body slice.
fn body_chunks(bytes: &[u8]) -> Vec<Bytes> {
    vec![Bytes::copy_from_slice(bytes)]
}

/// Drains a [`RawGet`] body stream into one contiguous byte vector.
async fn collect_body(got: RawGet) -> Vec<u8> {
    got.body
        .fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk.expect("body chunk"));
            acc
        })
        .await
}

/// The pathological keys that must survive byte-for-byte.
fn pathological_keys() -> Vec<String> {
    vec![
        "asdf/".to_owned(),
        "0/".to_owned(),
        "a//b".to_owned(),
        "dir/".to_owned(),
        "x\u{0001}y".to_owned(),
        "sp ace".to_owned(),
        "1999#".to_owned(),
        "quote\"x".to_owned(),
    ]
}

/// Pathological keys must survive PUT, HEAD, and ListObjectsV2 byte-for-byte.
#[tokio::test]
async fn pathological_keys_round_trip_byte_exact() {
    let store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");

    for (index, key) in pathological_keys().into_iter().enumerate() {
        let payload = format!("body-{index}").into_bytes();
        client
            .put(&key, RawWriteHeaders::default(), body_chunks(&payload))
            .await
            .expect("put pathological key");

        // The mock stored the EXACT key bytes decoded from the request path.
        {
            let guard = store.lock().expect("store lock");
            assert!(
                guard.objects.contains_key(&key),
                "mock did not store exact key {key:?}"
            );
            assert_eq!(
                guard.objects.get(&key).expect("stored object").body,
                payload,
                "stored body mismatch for {key:?}"
            );
        }

        // HEAD returns the object for the exact key.
        let meta = client.head(&key).await.expect("head pathological key");
        assert_eq!(meta.size, payload.len() as u64, "size mismatch for {key:?}");
    }

    // ListObjectsV2 returns every key percent-decoded to its exact bytes.
    let page = client
        .list_v2("", None, None, 1000)
        .await
        .expect("list keys");
    let listed: Vec<String> = page.entries.iter().map(|e| e.key.clone()).collect();
    for key in pathological_keys() {
        assert!(
            listed.contains(&key),
            "list did not return exact key {key:?}; got {listed:?}"
        );
    }
}

/// The `Expires` header must round-trip verbatim through PUT and HEAD.
#[tokio::test]
async fn expires_header_round_trips() {
    let _store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");
    let key = "expires-object";
    let expires = "Wed, 21 Oct 2015 07:28:00 GMT";

    client
        .put(
            key,
            RawWriteHeaders {
                expires: Some(expires.to_owned()),
                ..RawWriteHeaders::default()
            },
            body_chunks(b"payload"),
        )
        .await
        .expect("put with expires");

    let meta = client.head(key).await.expect("head with expires");
    assert_eq!(
        meta.expires.as_deref(),
        Some(expires),
        "expires header must round-trip verbatim"
    );
}

/// `x-amz-meta-*` and `Content-Type` must round-trip through PUT and HEAD.
#[tokio::test]
async fn user_metadata_and_content_type_round_trip() {
    let _store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");
    let key = "meta-object";

    let mut user_metadata = BTreeMap::new();
    user_metadata.insert("owner".to_owned(), "alice".to_owned());
    user_metadata.insert("purpose".to_owned(), "test".to_owned());

    client
        .put(
            key,
            RawWriteHeaders {
                content_type: Some("application/json".to_owned()),
                user_metadata: user_metadata.clone(),
                ..RawWriteHeaders::default()
            },
            body_chunks(b"{}"),
        )
        .await
        .expect("put with metadata");

    let meta = client.head(key).await.expect("head with metadata");
    assert_eq!(
        meta.content_type.as_deref(),
        Some("application/json"),
        "content-type must round-trip"
    );
    assert_eq!(
        meta.user_metadata.get("owner").map(String::as_str),
        Some("alice"),
        "user metadata `owner` must round-trip"
    );
    assert_eq!(
        meta.user_metadata.get("purpose").map(String::as_str),
        Some("test"),
        "user metadata `purpose` must round-trip"
    );
}

/// A full GET must stream back the exact stored body with the full range.
#[tokio::test]
async fn get_streams_stored_body() {
    let _store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");
    let key = "get-object";
    let payload = b"streamed-body-contents";

    client
        .put(key, RawWriteHeaders::default(), body_chunks(payload))
        .await
        .expect("put for get");

    let got = client.get(key, RawRange::Full).await.expect("get object");
    assert_eq!(got.meta.size, payload.len() as u64, "get size mismatch");
    assert_eq!(
        got.range,
        0..payload.len() as u64,
        "resolved range mismatch"
    );

    let collected = collect_body(got).await;
    assert_eq!(collected.as_slice(), payload, "streamed body mismatch");
}

/// HEAD on an absent key must map to [`RawError::NoSuchKey`].
#[tokio::test]
async fn head_missing_key_is_no_such_key() {
    let _store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");
    let result = client.head("definitely-absent-key").await;
    assert!(
        matches!(result, Err(RawError::NoSuchKey)),
        "missing key HEAD must map to NoSuchKey, got {result:?}"
    );
}

/// ListObjectsV2 must paginate through continuation tokens to completion.
#[tokio::test]
async fn list_v2_paginates_with_continuation_token() {
    let _store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");

    // Distinct prefix so this test's keys are isolated from other tests.
    let keys = ["page/a", "page/b", "page/c", "page/d", "page/e"];
    for key in keys {
        client
            .put(key, RawWriteHeaders::default(), body_chunks(b"x"))
            .await
            .expect("put page key");
    }

    let first = client
        .list_v2("page/", None, None, 2)
        .await
        .expect("first page");
    assert_eq!(first.entries.len(), 2, "first page size");
    assert_eq!(first.entries[0].key, "page/a");
    assert_eq!(first.entries[1].key, "page/b");
    let token = first
        .next_continuation_token
        .clone()
        .expect("first page must be truncated");

    let second = client
        .list_v2("page/", None, Some(&token), 2)
        .await
        .expect("second page");
    assert_eq!(second.entries.len(), 2, "second page size");
    assert_eq!(second.entries[0].key, "page/c");
    assert_eq!(second.entries[1].key, "page/d");
    let token = second
        .next_continuation_token
        .clone()
        .expect("second page must be truncated");

    let third = client
        .list_v2("page/", None, Some(&token), 2)
        .await
        .expect("third page");
    assert_eq!(third.entries.len(), 1, "final page size");
    assert_eq!(third.entries[0].key, "page/e");
    assert!(
        third.next_continuation_token.is_none(),
        "final page must not be truncated"
    );
}

/// The raw multipart trio must assemble parts in part-number order at the exact
/// (pathological) key and carry the initiate request's metadata headers onto
/// the completed object; the completion must surface the origin's ETag.
#[tokio::test]
async fn multipart_round_trip_assembles_and_carries_headers() {
    let store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");
    let key = "mp dir/";
    let expires = "Wed, 21 Oct 2015 07:28:00 GMT";

    let creation = client
        .create_multipart(
            key,
            RawWriteHeaders {
                expires: Some(expires.to_owned()),
                content_type: Some("application/x-t".to_owned()),
                ..RawWriteHeaders::default()
            },
        )
        .await
        .expect("create multipart");
    let upload_id = creation.upload_id;

    let part_one = b"first-part-payload-".to_vec();
    let part_two = b"second-part-payload".to_vec();
    let part_one_up = client
        .put_part(
            key,
            &upload_id,
            1,
            RawWriteChecksum::default(),
            body_chunks(&part_one),
        )
        .await
        .expect("put part 1");
    let part_two_up = client
        .put_part(
            key,
            &upload_id,
            2,
            RawWriteChecksum::default(),
            body_chunks(&part_two),
        )
        .await
        .expect("put part 2");
    assert_ne!(
        part_one_up.e_tag, part_two_up.e_tag,
        "parts must get distinct ETags"
    );

    let outcome = client
        .complete_multipart(
            key,
            &upload_id,
            vec![
                RawCompletedPart {
                    e_tag: part_one_up.e_tag,
                    checksums: RawChecksums::default(),
                },
                RawCompletedPart {
                    e_tag: part_two_up.e_tag,
                    checksums: RawChecksums::default(),
                },
            ],
            RawWriteChecksum::default(),
        )
        .await
        .expect("complete multipart");
    assert_eq!(
        outcome.e_tag.as_deref(),
        Some("\"mp-etag\""),
        "completion must surface the origin's (XML-unescaped) ETag"
    );

    // The mock assembled the parts in order at the EXACT key, and the initiate
    // request's metadata headers landed on the completed object.
    let guard = store.lock().expect("store lock");
    let object = guard
        .objects
        .get(key)
        .expect("mock must hold the assembled object at the exact key");
    let mut expected = part_one.clone();
    expected.extend_from_slice(&part_two);
    assert_eq!(
        object.body, expected,
        "assembled body must be part1 + part2"
    );
    assert_eq!(
        object.content_type.as_deref(),
        Some("application/x-t"),
        "content-type from create_multipart must be stored"
    );
    assert_eq!(
        object.expires.as_deref(),
        Some(expires),
        "expires from create_multipart must be stored"
    );
    assert!(
        !guard.uploads.contains_key(&upload_id),
        "completed upload must be dropped from the mock"
    );
}

/// Aborting a multipart upload must discard the upload and its parts without
/// materializing any object at the key.
#[tokio::test]
async fn multipart_abort_discards() {
    let store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");
    let key = "mp-aborted-object";

    let upload_id = client
        .create_multipart(key, RawWriteHeaders::default())
        .await
        .expect("create multipart for abort")
        .upload_id;
    client
        .put_part(
            key,
            &upload_id,
            1,
            RawWriteChecksum::default(),
            body_chunks(b"doomed-part"),
        )
        .await
        .expect("put part before abort");

    client
        .abort_multipart(key, &upload_id)
        .await
        .expect("abort multipart");

    let guard = store.lock().expect("store lock");
    assert!(
        !guard.objects.contains_key(key),
        "aborted upload must not materialize an object"
    );
    assert!(
        !guard.uploads.contains_key(&upload_id),
        "aborted upload must be dropped from the mock"
    );
}

/// #208: a checksummed multipart upload must forward the checksum SELECTION on
/// create, each PART's checksum on upload and back in the response, each part's
/// checksum into the completion MANIFEST, and the object-level checksum on
/// complete — with the origin echoing the composite back. Verglas computes
/// none of these; it only forwards.
#[tokio::test]
async fn multipart_checksums_forward_never_recomputed() {
    let _store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");
    let key = "checksummed-multipart";

    let creation = client
        .create_multipart(
            key,
            RawWriteHeaders {
                checksum: RawWriteChecksum {
                    algorithm: Some("SHA256".to_owned()),
                    checksum_type: Some("COMPOSITE".to_owned()),
                    ..RawWriteChecksum::default()
                },
                ..RawWriteHeaders::default()
            },
        )
        .await
        .expect("create checksummed multipart");
    assert_eq!(
        creation.checksum_algorithm.as_deref(),
        Some("SHA256"),
        "create must surface the origin's ChecksumAlgorithm"
    );
    assert_eq!(
        creation.checksum_type.as_deref(),
        Some("COMPOSITE"),
        "create must surface the origin's ChecksumType"
    );
    let upload_id = creation.upload_id;

    let part_sha = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";
    let part = client
        .put_part(
            key,
            &upload_id,
            1,
            RawWriteChecksum {
                sha256: Some(part_sha.to_owned()),
                ..RawWriteChecksum::default()
            },
            body_chunks(b"one-part-payload"),
        )
        .await
        .expect("put checksummed part");
    assert_eq!(
        part.checksums.sha256.as_deref(),
        Some(part_sha),
        "the part upload must echo the origin's part checksum"
    );

    let composite = "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1";
    let outcome = client
        .complete_multipart(
            key,
            &upload_id,
            vec![RawCompletedPart {
                e_tag: part.e_tag,
                checksums: RawChecksums {
                    sha256: Some(part_sha.to_owned()),
                    ..RawChecksums::default()
                },
            }],
            RawWriteChecksum {
                sha256: Some(composite.to_owned()),
                ..RawWriteChecksum::default()
            },
        )
        .await
        .expect("complete checksummed multipart");
    assert_eq!(
        outcome.checksums.sha256.as_deref(),
        Some(composite),
        "completion must surface the origin's composite object checksum"
    );
}

/// A FULL_OBJECT upload's completion must carry `x-amz-checksum-type`, which the
/// client names only on create. The mock origin rejects a FULL_OBJECT completion
/// that omits the header (as MinIO does), so this passes only because
/// `complete_multipart` restates the object-level checksum type (issue #208).
#[tokio::test]
async fn full_object_completion_forwards_checksum_type() {
    let _store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");
    let key = "full-object-multipart";

    let creation = client
        .create_multipart(
            key,
            RawWriteHeaders {
                checksum: RawWriteChecksum {
                    algorithm: Some("CRC32".to_owned()),
                    checksum_type: Some("FULL_OBJECT".to_owned()),
                    ..RawWriteChecksum::default()
                },
                ..RawWriteHeaders::default()
            },
        )
        .await
        .expect("create full-object multipart");
    let upload_id = creation.upload_id;

    let part = client
        .put_part(
            key,
            &upload_id,
            1,
            RawWriteChecksum {
                crc32: Some("JRTCyQ==".to_owned()),
                ..RawWriteChecksum::default()
            },
            body_chunks(b"full-object-part"),
        )
        .await
        .expect("put full-object part");

    // The completion carries only the object-level checksum VALUE (as a client
    // does); the raw client must add `x-amz-checksum-type: FULL_OBJECT` itself.
    client
        .complete_multipart(
            key,
            &upload_id,
            vec![RawCompletedPart {
                e_tag: part.e_tag,
                checksums: RawChecksums::default(),
            }],
            RawWriteChecksum {
                crc32: Some("WgDhBQ==".to_owned()),
                checksum_type: Some("FULL_OBJECT".to_owned()),
                ..RawWriteChecksum::default()
            },
        )
        .await
        .expect("full-object completion must forward the checksum type");
}

/// A conditional HEAD with the current ETag must map the origin's 304 to
/// [`RawError::NotModified`]; a stale ETag must return fresh metadata.
#[tokio::test]
async fn head_if_none_match_304_maps_to_not_modified() {
    let _store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");
    let key = "inm-object";
    let payload = b"conditional-body";

    client
        .put(key, RawWriteHeaders::default(), body_chunks(payload))
        .await
        .expect("put for conditional head");
    let etag = client
        .head(key)
        .await
        .expect("head for etag")
        .e_tag
        .expect("stored object must have an etag");

    let unchanged = client.head_if_none_match(key, &etag).await;
    assert!(
        matches!(unchanged, Err(RawError::NotModified)),
        "matching etag must map to NotModified, got {unchanged:?}"
    );

    let changed = client
        .head_if_none_match(key, "\"some-other-etag\"")
        .await
        .expect("non-matching etag must return metadata");
    assert_eq!(
        changed.size,
        payload.len() as u64,
        "fresh metadata must carry the object size"
    );
    assert_eq!(
        changed.e_tag.as_deref(),
        Some(etag.as_str()),
        "fresh metadata must carry the current etag"
    );
}

/// Ranged GETs must resolve the served range from `Content-Range` and report
/// the FULL object size (the total), not the slice length.
#[tokio::test]
async fn ranged_get_resolves_range_and_total() {
    let _store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");
    let key = "ranged-object";
    let payload: Vec<u8> = (0u8..100).collect();

    client
        .put(key, RawWriteHeaders::default(), body_chunks(&payload))
        .await
        .expect("put ranged object");

    // Bounded [10, 20): exactly bytes 10..20, total size from Content-Range.
    let got = client
        .get(key, RawRange::Bounded(10, 20))
        .await
        .expect("bounded get");
    assert_eq!(got.range, 10..20, "bounded range must resolve to 10..20");
    assert_eq!(
        got.meta.size, 100,
        "meta.size must be the Content-Range total"
    );
    let body = collect_body(got).await;
    assert_eq!(body, payload[10..20], "bounded body must be bytes 10..20");

    // Suffix(5): the final five bytes.
    let got = client
        .get(key, RawRange::Suffix(5))
        .await
        .expect("suffix get");
    assert_eq!(got.range, 95..100, "suffix range must resolve to 95..100");
    assert_eq!(got.meta.size, 100, "suffix meta.size must be the total");
    let body = collect_body(got).await;
    assert_eq!(
        body,
        payload[95..100],
        "suffix body must be the last 5 bytes"
    );

    // Offset(90): from byte 90 to the end.
    let got = client
        .get(key, RawRange::Offset(90))
        .await
        .expect("offset get");
    assert_eq!(got.range, 90..100, "offset range must resolve to 90..100");
    assert_eq!(got.meta.size, 100, "offset meta.size must be the total");
    let body = collect_body(got).await;
    assert_eq!(body, payload[90..100], "offset body must be bytes 90..100");
}

/// A range starting at or past the end of the object must map the origin's 416
/// to [`RawError::InvalidRange`].
#[tokio::test]
async fn unsatisfiable_range_is_invalid_range() {
    let _store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");
    let key = "tiny-object";

    client
        .put(key, RawWriteHeaders::default(), body_chunks(b"ten bytes!"))
        .await
        .expect("put tiny object");

    let result = client.get(key, RawRange::Offset(1000)).await;
    assert!(
        matches!(result, Err(RawError::InvalidRange)),
        "offset past the end must map to InvalidRange, got an Ok or other error: {:?}",
        result.as_ref().err()
    );
}

/// A 404 carrying a `NoSuchBucket` error code must map to
/// [`RawError::NoSuchBucket`], not `NoSuchKey`. Uses GET because a HEAD
/// response carries no body, so the error `<Code>` is only readable on GET —
/// exactly as with real S3, whose HeadObject 404s are code-less.
#[tokio::test]
async fn missing_bucket_maps_to_no_such_bucket() {
    let _store = setup();
    let client = RawS3::from_env("missing-bucket").expect("build missing-bucket client");
    let result = client.get("any-key", RawRange::Full).await;
    assert!(
        matches!(result, Err(RawError::NoSuchBucket)),
        "404 with NoSuchBucket code must map to NoSuchBucket, got an Ok or other error: {:?}",
        result.as_ref().err()
    );
}

/// A 403 must map to [`RawError::AccessDenied`].
#[tokio::test]
async fn denied_bucket_maps_to_access_denied() {
    let _store = setup();
    let client = RawS3::from_env("denied-bucket").expect("build denied-bucket client");
    let result = client.head("any-key").await;
    assert!(
        matches!(result, Err(RawError::AccessDenied)),
        "403 must map to AccessDenied, got {result:?}"
    );
}

/// Keys containing a space (AWS-encoded as `+` under `encoding-type=url`) and a
/// literal `+` (encoded as `%2B`) must both round-trip byte-exact through
/// `list_v2`'s unquote-plus decoding.
#[tokio::test]
async fn list_decodes_aws_plus_encoded_spaces() {
    let _store = setup();
    let client = RawS3::from_env(BUCKET).expect("build client");

    // Distinct prefix so this test's keys are isolated from other tests.
    let keys = ["lsp/lit+eral+plus", "lsp/pl us space"];
    for key in keys {
        client
            .put(key, RawWriteHeaders::default(), body_chunks(b"x"))
            .await
            .expect("put plus/space key");
    }

    let page = client
        .list_v2("lsp/", None, None, 1000)
        .await
        .expect("list plus/space keys");
    let listed: Vec<String> = page.entries.iter().map(|e| e.key.clone()).collect();
    assert_eq!(
        listed,
        keys.iter().map(|k| (*k).to_owned()).collect::<Vec<_>>(),
        "space and literal-plus keys must round-trip byte-exact through list_v2"
    );
}

/// #152: the verbatim bucket-level forward returns the origin's HeadBucket
/// (200, with an origin header) and GetBucketLocation (200, LocationConstraint
/// body) untouched, and never maps the status to a RawError.
#[tokio::test]
async fn forward_bucket_head_and_location_pass_through() {
    let _store = setup();
    let client = RawS3::from_env("bucket").expect("build client");

    let head = client
        .forward_bucket("HEAD", "")
        .await
        .expect("forward head bucket");
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty(), "HEAD carries no body");
    assert!(
        head.headers.iter().any(
            |(name, value)| name.eq_ignore_ascii_case("x-amz-bucket-region")
                && value == "us-west-2"
        ),
        "origin response headers forwarded verbatim"
    );
    assert!(
        !head
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("content-length")),
        "hop-by-hop framing headers are stripped"
    );

    let location = client
        .forward_bucket("GET", "location=")
        .await
        .expect("forward get location");
    assert_eq!(location.status, 200);
    assert!(
        String::from_utf8_lossy(&location.body).contains("<LocationConstraint"),
        "GetBucketLocation body forwarded verbatim"
    );
}

/// #152: a forwarded request to a missing bucket returns the origin's 404
/// verbatim as an `Ok` response (the status is the answer, not an error).
#[tokio::test]
async fn forward_bucket_missing_returns_origin_404() {
    let _store = setup();
    let client = RawS3::from_env("missing-bucket").expect("build client");
    let head = client
        .forward_bucket("HEAD", "")
        .await
        .expect("forward returns the origin status, not an error");
    assert_eq!(head.status, 404);
}
