//! Integration tests for the private REST catalog watcher's response decoding.
//! Some managed REST catalogs return `Content-Encoding: gzip` bodies; the
//! watcher must decode them deterministically under concurrency.

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::json;
use verglas_tables::catalog::{CatalogSource, RestCatalogSource, TableIdent};

/// Whether the mock compresses its bodies with gzip.
struct GzipMock {
    /// When true, every load-table body is gzip-encoded with a
    /// `Content-Encoding: gzip` header, mimicking a gzip-encoding managed catalog.
    compress: bool,
}

/// gzip-encodes a byte slice into a single member.
fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).expect("gzip write");
    encoder.finish().expect("gzip finish")
}

/// A load-table body for one table, echoing the table name so a concurrent
/// caller can prove it got its own response and not a neighbour's garbled one.
fn load_table_json(ns: &str, name: &str) -> Vec<u8> {
    let body = json!({
        "metadata-location": format!("s3://lake/{ns}/{name}/metadata/v1.json"),
        "metadata": {
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": format!("s3://lake/{ns}/{name}"),
            "current-snapshot-id": 1,
            "properties": {"table.name": name}
        }
    });
    serde_json::to_vec(&body).expect("serialize load-table body")
}

/// `GET /v1/demo/namespaces/{ns}/tables/{table}` — gzip-encoded (or plain)
/// load-table JSON. When compressing, the body carries `Content-Encoding: gzip`
/// exactly as such catalogs frame it.
async fn load_table(
    State(mock): State<Arc<GzipMock>>,
    Path((ns, name)): Path<(String, String)>,
) -> Response {
    if name == "missing" {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NOT_FOUND;
        response.headers_mut().insert(
            header::CONTENT_ENCODING,
            header::HeaderValue::from_static("gzip"),
        );
        return response;
    }
    let json = load_table_json(&ns, &name);
    if mock.compress {
        let compressed = gzip(&json);
        let mut response = Response::new(Body::from(compressed));
        response.headers_mut().insert(
            header::CONTENT_ENCODING,
            header::HeaderValue::from_static("gzip"),
        );
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        response
    } else {
        (
            [(header::CONTENT_TYPE, "application/json")],
            Body::from(json),
        )
            .into_response()
    }
}

/// Boots a mock catalog that either gzips or serves plain bodies.
async fn spawn_mock(compress: bool) -> SocketAddr {
    let mock = Arc::new(GzipMock { compress });
    let app = Router::new()
        .route(
            "/v1/config",
            get(|| async { axum::Json(json!({"overrides": {"prefix": "demo"}})) }),
        )
        .route("/v1/demo/namespaces/{ns}/tables/{table}", get(load_table))
        .with_state(mock);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gzip mock");
    let addr = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock serves");
    });
    addr
}

/// Builds a private watcher source pointed at the mock.
fn source_for(addr: SocketAddr) -> RestCatalogSource {
    let config: verglas_core::config::Catalog = toml::de::from_str(&format!(
        "uri = \"http://{addr}\"\npoll_interval_secs = 30\n"
    ))
    .expect("catalog config parses");
    RestCatalogSource::from_config(&config).expect("source builds")
}

/// A gzip-compressed upstream load-table response is decoded before the private
/// watcher parses the pointer fields it needs.
#[tokio::test(flavor = "multi_thread")]
async fn gzip_upstream_is_decoded_for_watcher() {
    let addr = spawn_mock(true).await;
    let source = source_for(addr);
    let response = source
        .table_pointer(&TableIdent::new(&["rlean"], "custom_points"))
        .await
        .expect("watcher request succeeds");
    assert_eq!(response.current_snapshot_id, Some(1));
    assert!(response.metadata_location.contains("custom_points"));
}

/// The reported race: N concurrent proxied load-tables against a compressing
/// upstream must ALL return clean parsed JSON, across repeated runs. A shared
/// decoder or buffer would garble ~5/6 of these; a per-request decode is
/// deterministically green.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_gzip_requests_all_decode_cleanly() {
    let addr = spawn_mock(true).await;
    let source = source_for(addr);

    for _ in 0..20 {
        let mut handles = Vec::new();
        for i in 0..24 {
            let source = source.clone();
            handles.push(tokio::spawn(async move {
                let name = format!("t{i}");
                let response = source
                    .table_pointer(&TableIdent::new(&["rlean"], &name))
                    .await
                    .expect("watcher request succeeds");
                assert!(response.metadata_location.contains(&name));
            }));
        }
        for handle in handles {
            handle.await.expect("task joins");
        }
    }
}

/// No regression against an identity (uncompressed) upstream — the S3 Tables
/// path. A plain JSON body passes through unchanged and still parses.
#[tokio::test(flavor = "multi_thread")]
async fn identity_upstream_is_parsed_unchanged() {
    let addr = spawn_mock(false).await;
    let source = source_for(addr);
    let response = source
        .table_pointer(&TableIdent::new(&["rlean"], "custom_points"))
        .await
        .expect("watcher request succeeds");
    assert_eq!(response.current_snapshot_id, Some(1));
    assert!(response.metadata_location.contains("custom_points"));
}

/// A catalog may advertise gzip on an empty 404 response. There is no gzip
/// member to decode; the watcher must preserve it as a catalog status error.
#[tokio::test(flavor = "multi_thread")]
async fn empty_gzip_404_passes_through() {
    let addr = spawn_mock(true).await;
    let source = source_for(addr);
    let error = source
        .table_pointer(&TableIdent::new(&["rlean"], "missing"))
        .await
        .expect_err("missing table must remain a catalog error");
    assert!(error.to_string().contains("404"));
}
