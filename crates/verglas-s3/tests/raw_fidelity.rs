//! Proxy round-trip fidelity tests (issue #189): keys and headers must pass
//! through the server byte-exactly where `object_store`'s typed abstractions
//! drop data. A signature-ignoring mock S3 origin stores objects keyed by the
//! EXACT decoded bytes of the request path; the front-end is served over a
//! production-shaped `BackendStore::from_config` (typed + raw clients, both
//! resolved from env). The tests pin: pathological key names (trailing slash,
//! empty segments, control characters) survive PUT / GET / LIST / DELETE
//! byte-for-byte; the `Expires` header is stored by PUT and reported back by
//! GET and HEAD; and ordinary keys keep flowing through the typed client.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::Response;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use verglas_core::config::Backend;
use verglas_s3::{
    BackendStore, NoopInvalidation, PassthroughList, PassthroughRead, PassthroughWrite,
};

/// Bytes kept literal when a test encodes a key into a proxy URL path: the
/// RFC 3986 unreserved set plus `/`, matching what a real S3 client sends.
const PATH_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

/// A stored object at the mock origin: exact body bytes plus the metadata
/// headers the origin echoes back on reads.
#[derive(Clone, Default)]
struct StoredObject {
    /// The object body as received.
    body: Vec<u8>,
    /// Synthetic ETag assigned at store time (quotes included).
    etag: String,
    /// Stored `Content-Type`, when the PUT sent one.
    content_type: Option<String>,
    /// Stored raw `Expires` header, when the PUT sent one.
    expires: Option<String>,
    /// Stored `x-amz-meta-*` headers (names without the prefix).
    user_meta: BTreeMap<String, String>,
}

/// Shared, thread-safe origin state keyed by exact key bytes (bucket-agnostic:
/// the mock strips the leading bucket segment, so tests isolate by key).
type Store = Arc<Mutex<BTreeMap<String, StoredObject>>>;

/// Percent-decodes a URL component to its exact bytes.
fn decode(component: &str) -> String {
    percent_decode_str(component)
        .decode_utf8_lossy()
        .into_owned()
}

/// Percent-encodes a key for the proxy URL path exactly as a real S3 client
/// does (unreserved set and `/` literal, everything else encoded).
fn encode_path_key(key: &str) -> String {
    utf8_percent_encode(key, PATH_ENCODE_SET).to_string()
}

/// Percent-encodes a key for an `encoding-type=url` list response.
fn encode_list_key(key: &str) -> String {
    utf8_percent_encode(key, NON_ALPHANUMERIC)
        .to_string()
        .replace("%2D", "-")
        .replace("%2E", ".")
        .replace("%5F", "_")
        .replace("%7E", "~")
}

/// Minimal XML escaping for element text.
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
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

/// Reads a request header as an owned string.
fn header_value(headers: &axum::http::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
}

/// One in-flight mock multipart upload: the exact destination key, the
/// metadata headers from creation, and the parts received so far.
#[derive(Default)]
struct MockUpload {
    /// The exact destination key.
    key: String,
    /// `Content-Type` from the creation request.
    content_type: Option<String>,
    /// `Expires` from the creation request.
    expires: Option<String>,
    /// Part bytes by part number.
    parts: BTreeMap<usize, Vec<u8>>,
}

/// The mock origin's in-flight multipart uploads, shared across tests (upload
/// IDs are unique, so tests cannot collide).
fn uploads() -> Arc<Mutex<BTreeMap<String, MockUpload>>> {
    static UPLOADS: OnceLock<Arc<Mutex<BTreeMap<String, MockUpload>>>> = OnceLock::new();
    UPLOADS
        .get_or_init(|| Arc::new(Mutex::new(BTreeMap::new())))
        .clone()
}

/// Handles the mock origin's multipart surface: create (`?uploads`), part
/// upload (`?partNumber&uploadId`), completion (POST `?uploadId`), and abort
/// (DELETE `?uploadId`). Returns `None` for non-multipart requests.
async fn multipart_response(
    store: &Store,
    method: &Method,
    key: &str,
    params: &BTreeMap<String, String>,
    headers: &axum::http::HeaderMap,
    body: Vec<u8>,
) -> Option<Response> {
    let uploads = uploads();
    if *method == Method::POST && params.contains_key("uploads") {
        let mut guard = uploads.lock().unwrap_or_else(|e| e.into_inner());
        let upload_id = format!("upload-{}", guard.len() + 1);
        guard.insert(
            upload_id.clone(),
            MockUpload {
                key: key.to_owned(),
                content_type: header_value(headers, "content-type"),
                expires: header_value(headers, "expires"),
                parts: BTreeMap::new(),
            },
        );
        return Some(
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(format!(
                    "<InitiateMultipartUploadResult><UploadId>{upload_id}</UploadId></InitiateMultipartUploadResult>"
                )))
                .expect("create response"),
        );
    }
    let upload_id = params.get("uploadId")?.clone();
    if *method == Method::PUT {
        let part_number: usize = params.get("partNumber")?.parse().ok()?;
        let mut guard = uploads.lock().unwrap_or_else(|e| e.into_inner());
        let upload = guard.get_mut(&upload_id)?;
        upload.parts.insert(part_number, body);
        return Some(
            Response::builder()
                .status(StatusCode::OK)
                .header("etag", format!("\"part-{part_number}\""))
                .body(Body::empty())
                .expect("part response"),
        );
    }
    if *method == Method::POST {
        let mut guard = uploads.lock().unwrap_or_else(|e| e.into_inner());
        // Cloudflare R2's rule (2026-08-02 incident): every part except the
        // last must be byte-identical in length, or the completion fails with
        // 400 InvalidPart. The mock enforces it so any split that alters part
        // boundaries fails here exactly as it does against R2.
        {
            let upload = guard.get(&upload_id)?;
            let lens: Vec<usize> = upload.parts.values().map(Vec::len).collect();
            if let Some((_, non_trailing)) = lens.split_last()
                && non_trailing.iter().any(|len| *len != non_trailing[0])
            {
                return Some(
                    Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(
                            "<?xml version=\"1.0\"?><Error><Code>InvalidPart</Code>\
                             <Message>All non-trailing parts must have the same length.</Message>\
                             </Error>",
                        ))
                        .expect("invalid part response"),
                );
            }
        }
        let upload = guard.remove(&upload_id)?;
        let mut assembled = Vec::new();
        for part in upload.parts.values() {
            assembled.extend_from_slice(part);
        }
        let object = StoredObject {
            etag: "\"mp-etag\"".to_owned(),
            content_type: upload.content_type,
            expires: upload.expires,
            body: assembled,
            ..StoredObject::default()
        };
        store
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(upload.key, object);
        return Some(
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(
                    "<CompleteMultipartUploadResult><ETag>&quot;mp-etag&quot;</ETag></CompleteMultipartUploadResult>",
                ))
                .expect("complete response"),
        );
    }
    if *method == Method::DELETE {
        uploads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&upload_id);
        return Some(
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("abort response"),
        );
    }
    None
}

/// The mock origin's single handler: dispatches by method, detecting
/// ListObjectsV2 by its `list-type=2` query and the multipart lifecycle by
/// its `uploads`/`uploadId` query.
async fn handler(State(store): State<Store>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let method = parts.method.clone();
    let path = parts.uri.path().to_owned();
    let query = parts.uri.query().unwrap_or("").to_owned();

    let after_bucket = path.strip_prefix('/').unwrap_or(&path);
    let key_encoded = match after_bucket.split_once('/') {
        Some((_bucket, key)) => key.to_owned(),
        None => String::new(),
    };
    let key = decode(&key_encoded);

    if method == Method::GET && query.contains("list-type=2") {
        return list_response(&store, &query);
    }

    let params = parse_query(&query);
    if params.contains_key("uploads") || params.contains_key("uploadId") {
        let bytes = to_bytes(body, 64 * 1024 * 1024)
            .await
            .expect("multipart body")
            .to_vec();
        if let Some(response) =
            multipart_response(&store, &method, &key, &params, &parts.headers, bytes).await
        {
            return response;
        }
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(
                "<?xml version=\"1.0\"?><Error><Code>NoSuchUpload</Code></Error>",
            ))
            .expect("no such upload");
    }

    match method {
        Method::PUT => {
            let etag = format!("\"etag-{}\"", key.len());
            let mut object = StoredObject {
                etag: etag.clone(),
                content_type: header_value(&parts.headers, "content-type"),
                expires: header_value(&parts.headers, "expires"),
                ..StoredObject::default()
            };
            for (name, value) in parts.headers.iter() {
                if let Some(meta) = name.as_str().strip_prefix("x-amz-meta-")
                    && let Ok(value) = value.to_str()
                {
                    object.user_meta.insert(meta.to_owned(), value.to_owned());
                }
            }
            let bytes = to_bytes(body, 64 * 1024 * 1024).await.expect("put body");
            object.body = bytes.to_vec();
            store
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key, object);
            Response::builder()
                .status(StatusCode::OK)
                .header("etag", etag)
                .body(Body::empty())
                .expect("put response")
        }
        Method::HEAD | Method::GET => {
            let guard = store.lock().unwrap_or_else(|e| e.into_inner());
            match guard.get(&key) {
                // A conditional read whose ETag still holds: the origin's 304.
                Some(object)
                    if header_value(&parts.headers, "if-none-match").as_deref()
                        == Some(object.etag.as_str()) =>
                {
                    Response::builder()
                        .status(StatusCode::NOT_MODIFIED)
                        .body(Body::empty())
                        .expect("not modified")
                }
                Some(object) => {
                    let mut builder = Response::builder()
                        .status(StatusCode::OK)
                        .header("etag", &object.etag)
                        .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                        .header("content-length", object.body.len().to_string());
                    if let Some(value) = &object.content_type {
                        builder = builder.header("content-type", value);
                    }
                    if let Some(value) = &object.expires {
                        builder = builder.header("expires", value);
                    }
                    for (name, value) in &object.user_meta {
                        builder = builder.header(format!("x-amz-meta-{name}"), value);
                    }
                    let body = if method == Method::HEAD {
                        Body::empty()
                    } else {
                        Body::from(object.body.clone())
                    };
                    builder.body(body).expect("object response")
                }
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from(
                        "<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>",
                    ))
                    .expect("not found"),
            }
        }
        Method::DELETE => {
            store.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
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
    let url_encoded = params.get("encoding-type").map(String::as_str) == Some("url");

    let guard = store.lock().unwrap_or_else(|e| e.into_inner());
    let mut matched: Vec<(String, usize, String)> = guard
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .filter(|(key, _)| cursor.as_ref().is_none_or(|c| *key > c))
        .map(|(key, object)| (key.clone(), object.body.len(), object.etag.clone()))
        .collect();

    let truncated = matched.len() > max_keys;
    matched.truncate(max_keys);
    let next_token = truncated
        .then(|| matched.last().map(|(key, _, _)| key.clone()))
        .flatten();

    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>");
    xml.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
    for (key, size, etag) in &matched {
        let rendered = if url_encoded {
            encode_list_key(key)
        } else {
            key.clone()
        };
        xml.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>2015-10-21T07:28:00.000Z</LastModified><ETag>{}</ETag><Size>{}</Size></Contents>",
            xml_escape(&rendered),
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

/// Starts the mock origin once (dedicated runtime thread) and points the
/// `AWS_*` env at it, so both the typed `object_store` client and the raw
/// client the registry builds talk to it. Returns the shared origin state.
fn origin() -> Store {
    static STORE: OnceLock<Store> = OnceLock::new();
    STORE
        .get_or_init(|| {
            let store: Store = Arc::new(Mutex::new(BTreeMap::new()));
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
                        .expect("bind origin");
                    let addr = listener.local_addr().expect("origin addr");
                    tx.send(addr).expect("send addr");
                    let app = Router::new().fallback(handler).with_state(server_store);
                    axum::serve(listener, app).await.expect("serve origin");
                });
            });
            let addr = rx.recv().expect("recv addr");
            // SAFETY: set exactly once before any client reads it, guarded by
            // the OnceLock; nothing else in this binary mutates the env.
            unsafe {
                std::env::set_var("AWS_ENDPOINT", format!("http://{addr}"));
                std::env::set_var("AWS_ACCESS_KEY_ID", "test-access-key");
                std::env::set_var("AWS_SECRET_ACCESS_KEY", "test-secret-key");
                std::env::set_var("AWS_ALLOW_HTTP", "true");
                std::env::set_var("AWS_REGION", "us-east-1");
                std::env::set_var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false");
            }
            store
        })
        .clone()
}

/// Serves the front-end over a production-shaped registry (typed + raw
/// clients from env) on an ephemeral port; returns the proxy base URL.
async fn serve_proxy() -> String {
    let registry = BackendStore::from_config(
        "default",
        &Backend {
            bucket: Some("fidelity-bucket".to_owned()),
            ..Backend::default()
        },
    );
    let app = verglas_s3::router(
        "default",
        PassthroughRead::new(registry.clone()),
        PassthroughWrite::new(registry.clone()),
        Arc::new(PassthroughList::new(registry)),
        Arc::new(NoopInvalidation),
        None,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy");
    let addr = listener.local_addr().expect("proxy addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve proxy");
    });
    format!("http://{addr}")
}

/// Extracts every `<Key>` element text from a LIST XML body.
fn xml_keys(xml: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Key>") {
        let after = &rest[start + "<Key>".len()..];
        let Some(end) = after.find("</Key>") else {
            break;
        };
        out.push(after[..end].to_owned());
        rest = &after[end + "</Key>".len()..];
    }
    out
}

/// The pathological key names the proxy must preserve byte-for-byte: trailing
/// slashes (directory markers), empty segments, control characters, spaces,
/// reserved punctuation, and a leading slash.
fn pathological_keys() -> Vec<String> {
    vec![
        "raw/asdf/".to_owned(),
        "raw/0/".to_owned(),
        "raw/a//b".to_owned(),
        "raw/x\u{0001}y".to_owned(),
        "raw/sp ace/".to_owned(),
        "raw/1999#/".to_owned(),
    ]
}

/// Byte-exact key preservation through the proxy: PUT stores the exact key at
/// the origin, GET reads it back, LIST returns it verbatim, DELETE removes it.
#[tokio::test]
async fn pathological_keys_round_trip_byte_exact_through_proxy() {
    let store = origin();
    let base = serve_proxy().await;
    let client = reqwest::Client::new();

    for (index, key) in pathological_keys().into_iter().enumerate() {
        let payload = format!("body-{index}");
        let url = format!("{base}/fidelity-bucket/{}", encode_path_key(&key));

        // PUT through the proxy: the origin must store the EXACT key bytes.
        let put = client
            .put(&url)
            .body(payload.clone())
            .send()
            .await
            .expect("proxy put");
        assert!(
            put.status().is_success(),
            "proxy PUT of {key:?} failed: {}",
            put.status()
        );
        {
            let guard = store.lock().unwrap_or_else(|e| e.into_inner());
            assert!(
                guard.contains_key(&key),
                "origin stored a corrupted key for {key:?}; stored keys: {:?}",
                guard.keys().collect::<Vec<_>>()
            );
            assert_eq!(
                guard.get(&key).expect("stored").body,
                payload.as_bytes(),
                "stored body mismatch for {key:?}"
            );
        }

        // GET through the proxy addresses the same exact key.
        let got = client.get(&url).send().await.expect("proxy get");
        assert_eq!(
            got.status(),
            reqwest::StatusCode::OK,
            "proxy GET of {key:?}"
        );
        assert_eq!(
            got.bytes().await.expect("get body").as_ref(),
            payload.as_bytes(),
            "GET body mismatch for {key:?}"
        );
    }

    // LIST v2 through the proxy returns every key byte-exactly (URL encoding
    // requested so control characters survive the XML transport).
    let list_url = format!("{base}/fidelity-bucket?list-type=2&prefix=raw%2F&encoding-type=url");
    let xml = client
        .get(&list_url)
        .send()
        .await
        .expect("proxy list")
        .text()
        .await
        .expect("list body");
    let listed: Vec<String> = xml_keys(&xml).iter().map(|k| decode(k)).collect();
    for key in pathological_keys() {
        assert!(
            listed.contains(&key),
            "proxy LIST dropped or corrupted {key:?}; got {listed:?}"
        );
    }

    // DELETE through the proxy removes the exact key.
    for key in pathological_keys() {
        let url = format!("{base}/fidelity-bucket/{}", encode_path_key(&key));
        let deleted = client.delete(&url).send().await.expect("proxy delete");
        assert!(
            deleted.status().is_success(),
            "proxy DELETE of {key:?}: {}",
            deleted.status()
        );
        let guard = store.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            !guard.contains_key(&key),
            "origin still holds {key:?} after proxy DELETE"
        );
    }
}

/// The `Expires` header round-trips: PUT stores it at the origin and both
/// GET and HEAD report it back (issue #189, the residual #143 header).
#[tokio::test]
async fn expires_round_trips_through_proxy() {
    let store = origin();
    let base = serve_proxy().await;
    let client = reqwest::Client::new();
    let expires = "Wed, 21 Oct 2015 07:28:00 GMT";
    let url = format!("{base}/fidelity-bucket/expires-key");

    let put = client
        .put(&url)
        .header("expires", expires)
        .body("payload")
        .send()
        .await
        .expect("proxy put");
    assert!(put.status().is_success(), "proxy PUT: {}", put.status());
    {
        let guard = store.lock().unwrap_or_else(|e| e.into_inner());
        let stored = guard.get("expires-key").expect("stored object");
        assert_eq!(
            stored.expires.as_deref(),
            Some(expires),
            "PUT must forward the Expires header to the origin"
        );
    }

    let head = client.head(&url).send().await.expect("proxy head");
    assert_eq!(head.status(), reqwest::StatusCode::OK);
    assert_eq!(
        head.headers().get("expires").and_then(|v| v.to_str().ok()),
        Some(expires),
        "HEAD must report the stored Expires back"
    );

    let get = client.get(&url).send().await.expect("proxy get");
    assert_eq!(get.status(), reqwest::StatusCode::OK);
    assert_eq!(
        get.headers().get("expires").and_then(|v| v.to_str().ok()),
        Some(expires),
        "GET must report the stored Expires back (HEAD/GET parity)"
    );
}

/// Ordinary keys without `Expires` keep flowing (through the typed client)
/// and read back their bytes and content type.
#[tokio::test]
async fn normal_keys_round_trip_through_proxy() {
    let _store = origin();
    let base = serve_proxy().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/fidelity-bucket/plain/normal-key.txt");

    let put = client
        .put(&url)
        .header("content-type", "text/plain")
        .body("normal-body")
        .send()
        .await
        .expect("proxy put");
    assert!(put.status().is_success(), "proxy PUT: {}", put.status());

    let got = client.get(&url).send().await.expect("proxy get");
    assert_eq!(got.status(), reqwest::StatusCode::OK);
    assert_eq!(
        got.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/plain"),
        "content type must round-trip"
    );
    assert_eq!(
        got.bytes().await.expect("get body").as_ref(),
        b"normal-body",
        "body must round-trip"
    );
}

/// Extracts the first `<tag>...</tag>` text from an XML body.
fn xml_value(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_owned())
}

/// The multipart lifecycle round-trips a pathological key byte-exactly: the
/// proxy routes create/part/complete over the raw client (issue #189), so the
/// assembled object lands at the EXACT key with its creation headers.
#[tokio::test]
async fn multipart_round_trips_pathological_key_through_proxy() {
    let store = origin();
    let base = serve_proxy().await;
    let client = reqwest::Client::new();
    let key = "raw/mp dir/";
    let expires = "Wed, 21 Oct 2015 07:28:00 GMT";
    let url = format!("{base}/fidelity-bucket/{}", encode_path_key(key));

    let created = client
        .post(format!("{url}?uploads"))
        .header("expires", expires)
        .header("content-type", "application/x-mp")
        .send()
        .await
        .expect("proxy create multipart");
    assert!(created.status().is_success(), "{}", created.status());
    let create_xml = created.text().await.expect("create body");
    let upload_id = xml_value(&create_xml, "UploadId").expect("upload id in response");

    let part = client
        .put(format!("{url}?partNumber=1&uploadId={upload_id}"))
        .body("part-one-bytes")
        .send()
        .await
        .expect("proxy upload part");
    assert!(part.status().is_success(), "{}", part.status());
    let part_etag = part
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .expect("part etag")
        .to_owned();

    let manifest = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{part_etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let completed = client
        .post(format!("{url}?uploadId={upload_id}"))
        .body(manifest)
        .send()
        .await
        .expect("proxy complete multipart");
    assert!(completed.status().is_success(), "{}", completed.status());

    let guard = store.lock().unwrap_or_else(|e| e.into_inner());
    let stored = guard
        .get(key)
        .unwrap_or_else(|| panic!("origin stored a corrupted multipart key for {key:?}"));
    assert_eq!(stored.body, b"part-one-bytes", "assembled body mismatch");
    assert_eq!(
        stored.expires.as_deref(),
        Some(expires),
        "CreateMultipartUpload must carry Expires over the raw path"
    );
    assert_eq!(
        stored.content_type.as_deref(),
        Some("application/x-mp"),
        "creation content-type must be stored"
    );
}

/// A copy between pathological keys re-streams over the raw client and
/// RETAINS the source's headers (S3's default COPY directive).
#[tokio::test]
async fn copy_retains_headers_for_pathological_keys() {
    let store = origin();
    let base = serve_proxy().await;
    let client = reqwest::Client::new();
    let source = "raw/copy src/";
    let dest = "raw/copy dst/";
    let expires = "Wed, 21 Oct 2015 07:28:00 GMT";

    let put = client
        .put(format!(
            "{base}/fidelity-bucket/{}",
            encode_path_key(source)
        ))
        .header("expires", expires)
        .header("content-type", "application/x-src")
        .body("copy-source-bytes")
        .send()
        .await
        .expect("proxy put source");
    assert!(put.status().is_success(), "{}", put.status());

    let copied = client
        .put(format!("{base}/fidelity-bucket/{}", encode_path_key(dest)))
        .header(
            "x-amz-copy-source",
            format!("fidelity-bucket/{}", encode_path_key(source)),
        )
        .send()
        .await
        .expect("proxy copy");
    assert!(copied.status().is_success(), "copy: {}", copied.status());

    let guard = store.lock().unwrap_or_else(|e| e.into_inner());
    let stored = guard
        .get(dest)
        .unwrap_or_else(|| panic!("origin has no copy destination {dest:?}"));
    assert_eq!(stored.body, b"copy-source-bytes", "copied body mismatch");
    assert_eq!(
        stored.expires.as_deref(),
        Some(expires),
        "COPY must retain the source's Expires"
    );
    assert_eq!(
        stored.content_type.as_deref(),
        Some("application/x-src"),
        "COPY must retain the source's content type"
    );
}

/// A copy with `MetadataDirective: REPLACE` carrying `Expires` routes raw and
/// stores the REPLACEMENT headers on the destination.
#[tokio::test]
async fn copy_replace_stores_new_expires() {
    let store = origin();
    let base = serve_proxy().await;
    let client = reqwest::Client::new();
    let new_expires = "Thu, 22 Oct 2015 08:00:00 GMT";

    let put = client
        .put(format!("{base}/fidelity-bucket/replace-src"))
        .header("content-type", "application/x-old")
        .body("replace-bytes")
        .send()
        .await
        .expect("proxy put source");
    assert!(put.status().is_success(), "{}", put.status());

    let copied = client
        .put(format!("{base}/fidelity-bucket/replace-dst"))
        .header("x-amz-copy-source", "fidelity-bucket/replace-src")
        .header("x-amz-metadata-directive", "REPLACE")
        .header("expires", new_expires)
        .header("content-type", "application/x-new")
        .send()
        .await
        .expect("proxy copy replace");
    assert!(copied.status().is_success(), "copy: {}", copied.status());

    let guard = store.lock().unwrap_or_else(|e| e.into_inner());
    let stored = guard.get("replace-dst").expect("destination stored");
    assert_eq!(stored.body, b"replace-bytes", "copied body mismatch");
    assert_eq!(
        stored.expires.as_deref(),
        Some(new_expires),
        "REPLACE must store the new Expires"
    );
    assert_eq!(
        stored.content_type.as_deref(),
        Some("application/x-new"),
        "REPLACE must store the new content type"
    );
}

/// A raw-only body larger than one part buffer streams through the raw
/// multipart trio in bounded memory and lands byte-exact.
#[tokio::test]
async fn large_raw_put_streams_through_raw_multipart() {
    let store = origin();
    let base = serve_proxy().await;
    let client = reqwest::Client::new();
    let key = "raw/big dir/";
    // Over PUT_PART_SIZE (8 MiB) so the raw multipart path engages; byte i is
    // i % 251 (prime) so no chunk alignment can mask an offset bug.
    let payload: Vec<u8> = (0..9 * 1024 * 1024).map(|i| (i % 251) as u8).collect();

    let put = client
        .put(format!("{base}/fidelity-bucket/{}", encode_path_key(key)))
        .body(payload.clone())
        .send()
        .await
        .expect("proxy large put");
    assert!(put.status().is_success(), "{}", put.status());

    let guard = store.lock().unwrap_or_else(|e| e.into_inner());
    let stored = guard
        .get(key)
        .unwrap_or_else(|| panic!("origin has no large object at {key:?}"));
    assert_eq!(stored.body.len(), payload.len(), "length mismatch");
    assert_eq!(stored.body, payload, "large body must land byte-exact");
}

/// REPRODUCTION (R2 InvalidPart, 2026-08-02), raw-path twin of the typed
/// `streamed_put_splits_into_uniform_parts` test: the raw multipart split of a
/// large streamed body must produce byte-identical non-trailing parts no
/// matter how the client's chunks arrive — the mock origin enforces R2's rule
/// at completion, so an overshooting split fails this write outright.
#[tokio::test]
async fn large_raw_put_splits_into_uniform_parts() {
    use futures::StreamExt;
    use verglas_core::CacheKey;
    use verglas_core::write::{ObjectWrite, WriteMetadata};

    let store = origin();
    let registry = BackendStore::from_config(
        "default",
        &Backend {
            bucket: Some("fidelity-bucket".to_owned()),
            ..Backend::default()
        },
    );
    let writer = PassthroughWrite::new(registry);
    // Raw-only key (trailing slash), so the write takes the raw client.
    let key = "raw/uniform dir/";

    // ~21 MiB delivered in irregular, non-repeating chunk sizes: under an
    // overshooting split, part 1 and part 2 differ in length and R2 (and the
    // mock) reject the completion.
    let chunk_lens: Vec<usize> = (0..14).map(|i| 1_000_000 + i * 77_777).collect();
    let total: usize = chunk_lens.iter().sum();
    let payload: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
    let mut chunks = Vec::with_capacity(chunk_lens.len());
    let mut offset = 0;
    for len in chunk_lens {
        chunks.push(Ok(bytes::Bytes::copy_from_slice(
            &payload[offset..offset + len],
        )));
        offset += len;
    }
    let body = futures::stream::iter(chunks).boxed();

    writer
        .put(
            &CacheKey {
                storage_binding_id: "default".to_owned(),
                bucket: "fidelity-bucket".to_owned(),
                key: key.to_owned(),
            },
            WriteMetadata::default(),
            body,
        )
        .await
        .expect("streamed raw PUT must satisfy R2's uniform-part rule");

    let guard = store.lock().unwrap_or_else(|e| e.into_inner());
    let stored = guard
        .get(key)
        .unwrap_or_else(|| panic!("origin has no object at {key:?}"));
    assert_eq!(stored.body, payload, "body must land byte-exact");
}

/// A raw-requiring write against a registry with no raw surface fails loudly
/// (501 NotImplemented) rather than degrading to the lossy typed path.
#[tokio::test]
async fn expires_put_without_raw_surface_fails_loudly() {
    use object_store::memory::InMemory;
    let app = verglas_s3::router(
        "default",
        PassthroughRead::new(BackendStore::single(
            "default",
            "fidelity-bucket",
            Arc::new(InMemory::new()),
        )),
        PassthroughWrite::new(BackendStore::single(
            "default",
            "fidelity-bucket",
            Arc::new(InMemory::new()),
        )),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default",
            "fidelity-bucket",
            Arc::new(InMemory::new()),
        ))),
        Arc::new(verglas_s3::NoopInvalidation),
        None,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy");
    let addr = listener.local_addr().expect("proxy addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve proxy");
    });

    let client = reqwest::Client::new();
    let response = client
        .put(format!("http://{addr}/fidelity-bucket/expires-key"))
        .header("expires", "Wed, 21 Oct 2015 07:28:00 GMT")
        .body("x")
        .send()
        .await
        .expect("proxy put");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::NOT_IMPLEMENTED,
        "an Expires-carrying PUT with no raw surface must 501, never drop the header"
    );
    let body = response.text().await.expect("error body");
    assert!(
        body.contains("NotImplemented"),
        "error code must be NotImplemented; got {body}"
    );
}

/// The passthrough reader's revalidation goes over the raw conditional HEAD:
/// a matching ETag is `Unchanged` (the origin's 304), a different one is
/// `Changed` with fresh metadata, and a vanished key is `Vanished`.
#[tokio::test]
async fn revalidate_routes_over_raw_conditional_head() {
    use verglas_core::read::{ObjectRead, Revalidation};
    let _store = origin();
    let registry: Arc<BackendStore> = BackendStore::from_config(
        "default",
        &Backend {
            bucket: Some("fidelity-bucket".to_owned()),
            ..Backend::default()
        },
    );
    let reader = PassthroughRead::new(registry);
    let proxy = serve_proxy().await;
    let client = reqwest::Client::new();

    let url = format!("{proxy}/fidelity-bucket/revalidate-key");
    let put = client
        .put(&url)
        .body("revalidate-body")
        .send()
        .await
        .expect("seed object");
    assert!(put.status().is_success());
    let etag = client
        .head(&url)
        .send()
        .await
        .expect("head")
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .expect("etag")
        .to_owned();

    let key = verglas_s3::cache_key_for("default", "fidelity-bucket", "revalidate-key");
    let unchanged = reader.revalidate(&key, &etag).await.expect("revalidate");
    assert!(
        matches!(unchanged, Revalidation::Unchanged),
        "matching etag must be Unchanged, got {unchanged:?}"
    );

    let changed = reader
        .revalidate(&key, "\"stale-etag\"")
        .await
        .expect("revalidate changed");
    match changed {
        Revalidation::Changed(meta) => {
            assert_eq!(meta.size, "revalidate-body".len() as u64, "fresh meta size")
        }
        other => panic!("expected Changed, got {other:?}"),
    }

    let missing = verglas_s3::cache_key_for("default", "fidelity-bucket", "never-existed");
    let vanished = reader
        .revalidate(&missing, "\"any\"")
        .await
        .expect("revalidate vanished");
    assert!(
        matches!(vanished, Revalidation::Vanished),
        "missing key must be Vanished, got {vanished:?}"
    );
}

/// A GET of a missing key through the proxy surfaces the origin's own 404
/// (`NoSuchKey`), mapped off the raw client.
#[tokio::test]
async fn missing_key_maps_to_no_such_key_through_proxy() {
    let _store = origin();
    let base = serve_proxy().await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{base}/fidelity-bucket/definitely-not-there"))
        .send()
        .await
        .expect("proxy get");
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    let body = response.text().await.expect("error body");
    assert!(
        body.contains("NoSuchKey"),
        "error code must be NoSuchKey; got {body}"
    );
}
