//! Versioned-read fidelity (issue #156): a `GET`/`HEAD` carrying `?versionId=`
//! forwards the parameter to the origin and serves that exact immutable version
//! byte-for-byte, cold and warm, without any versioning machinery of Verglas's
//! own. A version's bytes are immutable, so a second read returns the same bytes
//! as the first (versioned reads bypass the cache — serve-only in v1).
//!
//! The origin is a signature-ignoring mock that keeps a version history per key
//! and honours `?versionId=` on reads and `x-amz-version-id` on writes, exactly
//! the surface a versioned bucket exposes. Requests flow through the whole
//! Verglas front-end (s3s protocol layer → passthrough → raw client) over real
//! HTTP, so the test exercises the same path a client would.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::Response;
use verglas_core::config::Backend;
use verglas_s3::{
    BackendStore, NoopInvalidation, PassthroughList, PassthroughRead, PassthroughWrite,
};

/// One stored version of an object: its assigned version ID and exact bytes.
#[derive(Clone)]
struct Version {
    /// The version ID the origin assigned at write time.
    id: String,
    /// The exact object bytes for this version.
    body: Vec<u8>,
}

/// The mock origin's per-key version history, oldest first (the last entry is
/// the current object). Keyed by exact object key.
type Store = Arc<Mutex<BTreeMap<String, Vec<Version>>>>;

/// Parses a raw query string into decoded key/value pairs.
fn parse_query(query: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        map.insert(key.to_owned(), value.to_owned());
    }
    map
}

/// The mock origin handler: PUT appends a new version and echoes its
/// `x-amz-version-id`; GET/HEAD serve the requested `?versionId=` (or the
/// current version when absent), reflecting `x-amz-version-id` back.
async fn handler(State(store): State<Store>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let method = parts.method.clone();
    let path = parts.uri.path().to_owned();
    let query = parts.uri.query().unwrap_or("").to_owned();
    let params = parse_query(&query);

    // Path is `/{bucket}/{key}`; the mock is bucket-agnostic (keys are unique).
    let after_bucket = path.strip_prefix('/').unwrap_or(&path);
    let key = match after_bucket.split_once('/') {
        Some((_bucket, key)) => key.to_owned(),
        None => String::new(),
    };

    match method {
        Method::PUT => {
            let bytes = to_bytes(body, 16 * 1024 * 1024).await.expect("put body");
            let mut guard = store.lock().unwrap_or_else(|e| e.into_inner());
            let history = guard.entry(key).or_default();
            let id = format!("ver-{}", history.len() + 1);
            history.push(Version {
                id: id.clone(),
                body: bytes.to_vec(),
            });
            Response::builder()
                .status(StatusCode::OK)
                .header("etag", "\"versioned-etag\"")
                .header("x-amz-version-id", id)
                .body(Body::empty())
                .expect("put response")
        }
        Method::HEAD | Method::GET => {
            let guard = store.lock().unwrap_or_else(|e| e.into_inner());
            let requested = params.get("versionId").cloned();
            let version = guard.get(&key).and_then(|history| match &requested {
                Some(id) => history.iter().find(|v| &v.id == id),
                None => history.last(),
            });
            match version {
                Some(version) => {
                    let builder = Response::builder()
                        .status(StatusCode::OK)
                        .header("etag", "\"versioned-etag\"")
                        .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                        .header("content-length", version.body.len().to_string())
                        .header("x-amz-version-id", &version.id);
                    let body = if method == Method::HEAD {
                        Body::empty()
                    } else {
                        Body::from(version.body.clone())
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
        _ => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty())
            .expect("method not allowed"),
    }
}

/// Starts the mock origin once and points the `AWS_*` env at it, so the raw
/// client the registry builds talks to it.
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
            // SAFETY: set exactly once before any client reads it, guarded by the
            // OnceLock; nothing else in this binary mutates the env.
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

/// Serves the front-end over a production-shaped registry (raw client from env)
/// on an ephemeral port; returns the proxy base URL.
async fn serve_proxy() -> String {
    let registry = BackendStore::from_config(
        "default",
        &Backend {
            bucket: Some("versioned-bucket".to_owned()),
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

/// PUTs a version through the proxy and returns the version ID the origin
/// assigned (from `x-amz-version-id`).
async fn put_version(client: &reqwest::Client, url: &str, body: &str) -> String {
    let put = client
        .put(url)
        .body(body.to_owned())
        .send()
        .await
        .expect("proxy put");
    assert!(put.status().is_success(), "PUT failed: {}", put.status());
    put.headers()
        .get("x-amz-version-id")
        .and_then(|v| v.to_str().ok())
        .expect("version id on write")
        .to_owned()
}

/// #156: `GET ?versionId=` through Verglas returns the exact bytes of that
/// version, byte-identical to the origin's stored bytes, cold and warm; a plain
/// GET returns the current version; and the response echoes `x-amz-version-id`.
#[tokio::test]
async fn versioned_get_serves_exact_version_cold_and_warm() {
    let store = origin();
    let base = serve_proxy().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/versioned-bucket/testobj");

    // Two immutable versions of the same key with different bytes.
    let v1_body = "alpha-version-one";
    let v2_body = "bravo-version-two-longer";
    let v1 = put_version(&client, &url, v1_body).await;
    let v2 = put_version(&client, &url, v2_body).await;
    assert_ne!(v1, v2, "each write gets a distinct version id");

    // The origin's own stored bytes for each version — the direct reference the
    // proxy read must match byte-for-byte.
    let (stored_v1, stored_v2) = {
        let guard = store.lock().unwrap_or_else(|e| e.into_inner());
        let history = guard.get("testobj").expect("history");
        (
            history
                .iter()
                .find(|v| v.id == v1)
                .expect("v1")
                .body
                .clone(),
            history
                .iter()
                .find(|v| v.id == v2)
                .expect("v2")
                .body
                .clone(),
        )
    };

    // Cold and warm read of the OLD version: byte-identical both times, and to
    // the origin's stored bytes (versioned reads bypass the cache, so warm is
    // by construction the same passthrough as cold).
    for pass in ["cold", "warm"] {
        let resp = client
            .get(format!("{url}?versionId={v1}"))
            .send()
            .await
            .expect("versioned get");
        assert_eq!(resp.status(), StatusCode::OK, "{pass} status");
        assert_eq!(
            resp.headers()
                .get("x-amz-version-id")
                .and_then(|v| v.to_str().ok()),
            Some(v1.as_str()),
            "{pass} version id echoed"
        );
        let body = resp.bytes().await.expect("body").to_vec();
        assert_eq!(body, stored_v1, "{pass} v1 bytes byte-identical to origin");
        assert_eq!(body, v1_body.as_bytes(), "{pass} v1 bytes are the v1 body");
    }

    // The newer version by ID, and the current object with no version.
    let get_v2 = client
        .get(format!("{url}?versionId={v2}"))
        .send()
        .await
        .expect("versioned get v2");
    assert_eq!(get_v2.bytes().await.expect("v2 body").to_vec(), stored_v2);

    // A plain GET (no versionId) resolves to the current version's bytes. Its
    // response is served through the ordinary cached read path, which does not
    // surface a version ID — only the explicit versionId reads do (issue #156);
    // the current-version identity is proven by the bytes.
    let current = client.get(&url).send().await.expect("plain get");
    assert_eq!(
        current.bytes().await.expect("current body").to_vec(),
        v2_body.as_bytes(),
        "a plain GET resolves to the current version's bytes"
    );
}

/// #156: `HEAD ?versionId=` reports the metadata of that exact version (size and
/// echoed version id), without transferring a body.
#[tokio::test]
async fn versioned_head_reports_version_metadata() {
    let _store = origin();
    let base = serve_proxy().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/versioned-bucket/headobj");

    let v1 = put_version(&client, &url, "one").await;
    let v2 = put_version(&client, &url, "two-two-two").await;

    let head_v1 = client
        .head(format!("{url}?versionId={v1}"))
        .send()
        .await
        .expect("versioned head");
    assert_eq!(head_v1.status(), StatusCode::OK);
    assert_eq!(
        head_v1
            .headers()
            .get("content-length")
            .expect("content-length"),
        "3",
        "HEAD of v1 reports v1's size"
    );
    assert_eq!(
        head_v1
            .headers()
            .get("x-amz-version-id")
            .expect("version id"),
        v1.as_str()
    );
    assert!(head_v1.bytes().await.expect("empty").is_empty());

    let head_v2 = client
        .head(format!("{url}?versionId={v2}"))
        .send()
        .await
        .expect("versioned head v2");
    assert_eq!(
        head_v2
            .headers()
            .get("content-length")
            .expect("content-length"),
        "11",
        "HEAD of v2 reports v2's size"
    );
}
