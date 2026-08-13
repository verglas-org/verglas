//! Shared harness for the backend fill tests.
//!
//! Two test binaries exercise the SAME streamed-fill contract through the same
//! assertion helper: `tests/fills.rs` drives it against the in-process stub
//! origin below (the default `cargo test` path — no MinIO, no docker, no
//! network beyond loopback), and `tests/minio.rs` drives it against a real
//! S3-compatible store when explicitly asked to (`--ignored` +
//! `VERGLAS_MINIO_URL`).
//!
//! The stub origin follows the crate's established mock pattern (the
//! signature-ignoring axum origin in `tests/raw_roundtrip.rs`), trimmed to the
//! object methods the typed fill path uses: PUT, HEAD, ranged/full GET, and
//! DELETE, path-style addressed.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, GetResultPayload, ObjectStoreExt, PutPayload};
use percent_encoding::percent_decode_str;
use verglas_backend::MultipartObjectStore;

/// A deterministic payload: byte `i` is `i % 251` (prime, so no chunk
/// alignment can mask an offset bug).
pub fn pattern(len: usize) -> Bytes {
    (0..len)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// The streamed-fill contract (issue #18 acceptance criteria), asserted against
/// whichever origin built `store`: a multi-megabyte PUT round-trips, HEAD
/// reports the full size, a bounded ranged GET streams exactly the requested
/// slice, a full GET streams the whole object byte-identically, and DELETE
/// cleans up.
pub async fn assert_streamed_fill(store: Arc<dyn MultipartObjectStore>) {
    // Write an object larger than one part so the fill is genuinely streamed.
    let key = Path::from(format!("issue-18/fill-{}.bin", std::process::id()));
    let body = pattern(3 * 1024 * 1024);
    store
        .put(&key, PutPayload::from_bytes(body.clone()))
        .await
        .expect("put through the limited client");

    // HEAD reports the full size (ETag resolution path, #14).
    let meta = store.head(&key).await.expect("head");
    assert_eq!(meta.size, body.len() as u64);

    // Ranged fill returns exactly the requested slice, streamed (not buffered
    // whole): assert the payload is a Stream and the bytes match.
    let want = &body[1000..1000 + 4096];
    let result = store
        .get_opts(
            &key,
            GetOptions {
                range: Some(GetRange::Bounded(1000..1000 + 4096)),
                ..GetOptions::default()
            },
        )
        .await
        .expect("ranged get");
    assert!(
        matches!(result.payload, GetResultPayload::Stream(_)),
        "fills must stream, not buffer the whole object"
    );
    let got = result.bytes().await.expect("collect range");
    assert_eq!(got.as_ref(), want, "ranged fill must be byte-identical");

    // Full streamed read round-trips the whole object.
    let full = store
        .get(&key)
        .await
        .expect("full get")
        .bytes()
        .await
        .expect("collect full");
    assert_eq!(full, body, "full fill must be byte-identical");

    store.delete(&key).await.expect("cleanup");
}

/// Stored objects keyed by their exact decoded key (bucket stripped).
type StubStore = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

/// Starts the in-process stub origin on an ephemeral loopback port, on its own
/// thread with a current-thread runtime (decoupled from the test's runtime),
/// and returns its base URL. The stub ignores request signatures; auth is not
/// under test here.
pub fn spawn_stub_origin() -> String {
    let store: StubStore = Arc::new(Mutex::new(BTreeMap::new()));
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
            let app = Router::new().fallback(handler).with_state(store);
            axum::serve(listener, app).await.expect("serve");
        });
    });
    let addr = rx.recv().expect("recv addr");
    format!("http://{addr}")
}

/// The stub's single handler: strips the leading `/bucket/` (path-style
/// addressing) and dispatches the plain object methods.
async fn handler(State(store): State<StubStore>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_owned();
    let after_bucket = path.strip_prefix('/').unwrap_or(&path);
    let key_encoded = after_bucket.split_once('/').map(|(_, k)| k).unwrap_or("");
    let key = percent_decode_str(key_encoded)
        .decode_utf8_lossy()
        .into_owned();

    // Bulk delete (`POST /bucket?delete`): the typed client deletes through
    // DeleteObjects, not per-key DELETE. Parse the `<Key>` elements out of the
    // manifest, remove them, and answer the S3-shaped `DeleteResult`.
    if parts.method == Method::POST && parts.uri.query().unwrap_or("").contains("delete") {
        let manifest = to_bytes(body, 10 * 1024 * 1024)
            .await
            .expect("read delete manifest");
        let manifest = String::from_utf8_lossy(&manifest).into_owned();
        let mut deleted = String::new();
        let mut guard = store.lock().expect("store lock");
        let mut rest = manifest.as_str();
        while let Some(open) = rest.find("<Key>") {
            let after = &rest[open + "<Key>".len()..];
            let Some(close) = after.find("</Key>") else {
                break;
            };
            let key = &after[..close];
            guard.remove(key);
            deleted.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
            rest = &after[close + "</Key>".len()..];
        }
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult>{deleted}</DeleteResult>"
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/xml")
            .body(Body::from(xml))
            .expect("delete result response");
    }

    match parts.method {
        Method::PUT => {
            let bytes = to_bytes(body, 10 * 1024 * 1024)
                .await
                .expect("read put body");
            let etag = format!("\"stub-{}\"", bytes.len());
            store
                .lock()
                .expect("store lock")
                .insert(key, bytes.to_vec());
            Response::builder()
                .status(StatusCode::OK)
                .header("etag", etag)
                .body(Body::empty())
                .expect("put response")
        }
        Method::HEAD => {
            let guard = store.lock().expect("store lock");
            match guard.get(&key) {
                Some(object) => object_headers(object)
                    .header("content-length", object.len().to_string())
                    .body(Body::empty())
                    .expect("head response"),
                None => not_found(),
            }
        }
        Method::GET => {
            let guard = store.lock().expect("store lock");
            match guard.get(&key) {
                Some(object) => get_response(object, header_value(&parts.headers, "range")),
                None => not_found(),
            }
        }
        Method::DELETE => {
            store.lock().expect("store lock").remove(&key);
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

/// Reads a request header as an owned string.
fn header_value(headers: &axum::http::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
}

/// A response builder carrying the metadata headers the typed client reads
/// (ETag and Last-Modified) with the stub's fixed status.
fn object_headers(object: &[u8]) -> axum::http::response::Builder {
    Response::builder()
        .status(StatusCode::OK)
        .header("etag", format!("\"stub-{}\"", object.len()))
        .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
}

/// Builds a GET response. Without a `Range` header the full body is returned;
/// with `bytes=a-b`, `bytes=a-`, or `bytes=-n` a 206 carrying the slice and a
/// `Content-Range: bytes a-b/total` is returned, and an unsatisfiable range
/// (start at or past the end) answers 416 with an empty body.
fn get_response(object: &[u8], range: Option<String>) -> Response {
    let len = object.len() as u64;
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
        let slice = object[start as usize..end as usize].to_vec();
        return object_headers(object)
            .status(StatusCode::PARTIAL_CONTENT)
            .header("content-range", format!("bytes {start}-{}/{len}", end - 1))
            .body(Body::from(slice))
            .expect("ranged get response");
    }
    object_headers(object)
        .body(Body::from(object.to_vec()))
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
