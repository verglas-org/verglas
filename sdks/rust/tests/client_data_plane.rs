//! Connection and execution-plane contract tests for the Rust SDK.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::{
    Json, Router,
    routing::{get, post},
};
use futures::{StreamExt, stream};
use serde_json::json;
use tokio::net::TcpListener;
use verglas_core::admin::{ACCESS_PATH, LocalAccess};
use verglas_sdk::{Client, ClientError, ConnectOptions};
use verglas_sdk::{KvPutOptions, KvReadTier};

/// The Rust KV handle sends raw bytes and preserves server-owned versions and cursors.
#[tokio::test]
async fn kv_handle_is_a_thin_raw_byte_client() {
    async fn put(headers: HeaderMap, body: Bytes) -> impl IntoResponse {
        assert_eq!(headers["x-verglas-ttl-seconds"], "300");
        assert_eq!(headers["if-none-match"], "*");
        assert_eq!(body, Bytes::from_static(b"blue"));
        (
            StatusCode::CREATED,
            [("etag", "\"4\""), ("x-verglas-idempotent", "false")],
        )
    }
    async fn get_kv() -> impl IntoResponse {
        (
            [
                ("etag", "\"4\""),
                ("content-type", "application/octet-stream"),
                ("x-verglas-modified-at-ms", "11"),
                ("x-verglas-meta-kind", "demo"),
            ],
            Bytes::from_static(b"blue"),
        )
    }
    async fn delete_kv(headers: HeaderMap) -> impl IntoResponse {
        assert_eq!(headers["if-match"], "\"4\"");
        Json(json!({"removed": true}))
    }
    async fn list_kv() -> impl IntoResponse {
        Json(json!({
            "entries": [{"key":"user/a", "version":"\"4\"", "modified_at_ms":11, "metadata":{}}],
            "next_cursor":"opaque"
        }))
    }
    let app = Router::new()
        .route("/v1/kv/workshop.blueprints", get(list_kv))
        .route(
            "/v1/kv/workshop.blueprints/{key}",
            get(get_kv).put(put).delete(delete_kv),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let client = Client::connect(
        ConnectOptions::new(endpoint.clone())
            .with_query_uri(endpoint)
            .with_catalog_uri("http://127.0.0.1:1")
            .with_s3_endpoint("http://127.0.0.1:8333")
            .with_token("scoped"),
    )
    .await
    .expect("client");
    let cache = client.kv("workshop.blueprints").expect("namespace");
    let put = cache
        .put(
            "featured",
            Bytes::from_static(b"blue"),
            KvPutOptions {
                ttl_seconds: Some(300),
                create_only: true,
                ..KvPutOptions::default()
            },
        )
        .await
        .expect("put");
    assert_eq!(put.version, "\"4\"");
    let value = cache.get("featured").await.expect("get").expect("live");
    assert_eq!(value.bytes, Bytes::from_static(b"blue"));
    assert_eq!(value.tier, KvReadTier::Unspecified);
    assert!(
        cache
            .delete("featured", Some(&value.version))
            .await
            .expect("delete")
            .removed
    );
    let page = cache
        .list("user/", 25, Some("opaque-in"))
        .await
        .expect("list");
    assert_eq!(page.entries[0].key, "user/a");
    assert_eq!(page.next_cursor.as_deref(), Some("opaque"));
}

/// A client authenticates once, discovers the real catalog plus the server's S3
/// cache endpoint, and keeps the two data-plane destinations separate.
#[tokio::test]
async fn connect_separates_catalog_from_server_cache() {
    let query_uri = "http://127.0.0.1:8334";
    let access = LocalAccess {
        s3_endpoint: "http://127.0.0.1:8333".to_owned(),
        query_uri: query_uri.to_owned(),
        catalog_uri: Some("https://catalog.example.test".to_owned()),
        warehouse: Some("s3://warehouse/tenant".to_owned()),
        region: "auto".to_owned(),
        bucket: Some("warehouse".to_owned()),
        access_key_id: Some("VGKEY".to_owned()),
    };
    let app = Router::new().route(
        ACCESS_PATH,
        get(move |headers: HeaderMap| async move {
            assert_eq!(
                headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok()),
                Some("Bearer scoped-access-token")
            );
            Json(access)
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
    let endpoint = format!("http://{}", listener.local_addr().expect("mock address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("mock server") });

    let client = Client::connect(ConnectOptions::new(endpoint).with_token("scoped-access-token"))
        .await
        .expect("connect client");
    assert_eq!(client.catalog_uri(), "https://catalog.example.test");
    assert_eq!(client.query_uri(), query_uri);
    assert_eq!(client.s3_endpoint(), Some("http://127.0.0.1:8333"));
}

/// Fully injected container coordinates never require the server admin port.
#[tokio::test]
async fn container_environment_shape_connects_without_admin_service() {
    let client = Client::connect(
        ConnectOptions::new("http://127.0.0.1:1")
            .with_query_uri("http://verglas:8334")
            .with_catalog_uri("https://catalog.example.test")
            .with_warehouse("s3://warehouse/tenant")
            .with_s3_endpoint("http://verglas:8333")
            .with_token("catalog-token"),
    )
    .await
    .expect("connect without admin service");
    assert_eq!(client.catalog_uri(), "https://catalog.example.test");
    assert_eq!(client.s3_endpoint(), Some("http://verglas:8333"));
}

#[derive(Clone, Default)]
struct Captured {
    paths: Arc<Mutex<Vec<String>>>,
}

/// Query and logical table writes use the server execution gateway. They never
/// instantiate an Iceberg query engine or table writer inside the SDK.
#[tokio::test]
async fn query_and_append_use_server_execution_roles() {
    let captured = Captured::default();
    let app = Router::new()
        .route("/v1/write/{name}", post(write_arrow))
        .route("/v1/databases/analytics/query", post(query_arrow))
        .with_state(captured.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gateway");
    let endpoint = format!("http://{}", listener.local_addr().expect("gateway address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("gateway server") });

    let client = Client::connect(
        ConnectOptions::new(endpoint.clone())
            .with_query_uri(endpoint)
            .with_catalog_uri("http://127.0.0.1:1")
            .with_s3_endpoint("http://127.0.0.1:8333")
            .with_token("sdk-token"),
    )
    .await
    .expect("client");

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
        .expect("batch");
    let append = client
        .append_stream(
            "sdk.events",
            stream::iter(vec![Ok::<_, ClientError>(batch.clone())]),
            "run-1",
        )
        .await
        .expect("append");
    assert_eq!(append.rows_committed, 2);

    let mut query = client
        .query_stream("analytics", "select id from sdk.events")
        .await
        .expect("query");
    assert_eq!(query.next().await.expect("result").expect("batch"), batch);
    assert!(query.next().await.is_none());
    assert_eq!(
        captured.paths.lock().expect("paths").as_slice(),
        ["write:sdk.events", "query"]
    );
}

/// Query routing refuses names that cannot identify a registered database.
#[tokio::test]
async fn query_stream_rejects_invalid_database_names() {
    let client = Client::connect(
        ConnectOptions::new("http://127.0.0.1:1")
            .with_query_uri("http://127.0.0.1:1")
            .with_catalog_uri("http://127.0.0.1:1")
            .with_s3_endpoint("http://127.0.0.1:8333"),
    )
    .await
    .expect("client");

    for database in [
        "",
        "9analytics",
        "-analytics",
        "analytics/team",
        "analytics.db",
    ] {
        let result = client.query_stream(database, "SELECT 1").await;
        assert!(
            matches!(result, Err(ClientError::Configuration(message)) if message.contains("database name")),
            "{database:?} must be rejected"
        );
    }
}

#[derive(Clone, Default)]
struct CatalogState {
    created: Arc<AtomicBool>,
}

/// Table metadata and creation go directly to the Iceberg REST catalog.
#[tokio::test]
async fn ensure_table_uses_catalog_rest_without_server_table_routes() {
    let state = CatalogState::default();
    let app = Router::new()
        .route("/v1/config", get(|| async { Json(json!({})) }))
        .route(
            "/v1/namespaces",
            post(|| async { (axum::http::StatusCode::OK, Json(json!({}))) }),
        )
        .route("/v1/namespaces/sdk/tables/events", get(load_catalog_table))
        .route("/v1/namespaces/sdk/tables", post(create_catalog_table))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind catalog");
    let catalog_uri = format!("http://{}", listener.local_addr().expect("catalog address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("catalog server") });
    let client = Client::connect(
        ConnectOptions::new("http://127.0.0.1:1")
            .with_query_uri("http://127.0.0.1:1")
            .with_catalog_uri(catalog_uri)
            .with_s3_endpoint("http://127.0.0.1:8333"),
    )
    .await
    .expect("client");
    let definition = verglas_sdk::TableDefinition {
        schema: vec![verglas_sdk::ColumnSpec::required("id", "int64")],
        partitions: vec![verglas_sdk::PartitionSpec::identity("id")],
    };
    assert_eq!(
        client
            .ensure_table("sdk.events", &definition)
            .await
            .expect("create"),
        verglas_sdk::EnsureTable::Created
    );
    assert!(state.created.load(Ordering::SeqCst));
    assert_eq!(
        client
            .ensure_table("sdk.events", &definition)
            .await
            .expect("existing"),
        verglas_sdk::EnsureTable::Existing
    );
}

/// Returns the table after the create request has been observed.
async fn load_catalog_table(State(state): State<CatalogState>) -> axum::response::Response {
    if !state.created.load(Ordering::SeqCst) {
        return (axum::http::StatusCode::NOT_FOUND, Json(json!({}))).into_response();
    }
    (
        axum::http::StatusCode::OK,
        Json(json!({
            "metadata": {
                "current-schema-id": 0,
                "schemas": [{
                    "type": "struct",
                    "schema-id": 0,
                    "identifier-field-ids": [],
                    "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
                }],
                "default-spec-id": 0,
                "partition-specs": [{
                    "spec-id": 0,
                    "fields": [{
                        "source-id": 1,
                        "field-id": 1000,
                        "name": "id-identity",
                        "transform": "identity"
                    }]
                }]
            }
        })),
    )
        .into_response()
}

/// Validates the standard REST create-table request and marks it visible.
async fn create_catalog_table(
    State(state): State<CatalogState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    assert_eq!(body["name"], "events");
    assert_eq!(body["schema"]["fields"][0]["type"], "long");
    assert_eq!(body["partition-spec"]["fields"][0]["source-id"], 1);
    assert_eq!(body["partition-spec"]["fields"][0]["name"], "id_identity");
    state.created.store(true, Ordering::SeqCst);
    (axum::http::StatusCode::OK, Json(json!({})))
}

/// Accepts one bounded Arrow stream and acknowledges the worker commit.
async fn write_arrow(
    State(captured): State<Captured>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    assert_eq!(
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer sdk-token")
    );
    let rows: usize = arrow_ipc::reader::StreamReader::try_new(body.as_ref(), None)
        .expect("Arrow request")
        .map(|batch| batch.expect("batch").num_rows())
        .sum();
    captured
        .paths
        .lock()
        .expect("paths")
        .push(format!("write:{name}"));
    Json(json!({
        "snapshotId": "10",
        "rowsCommitted": rows,
        "watermark": "10",
        "idempotent": false
    }))
}

/// Returns an Arrow IPC stream exactly as the isolated query role does.
async fn query_arrow(State(captured): State<Captured>, headers: HeaderMap) -> impl IntoResponse {
    assert_eq!(
        headers.get("accept").and_then(|value| value.to_str().ok()),
        Some(verglas_sdk::ARROW_STREAM_CONTENT_TYPE)
    );
    captured
        .paths
        .lock()
        .expect("paths")
        .push("query".to_owned());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
        .expect("batch");
    let mut bytes = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).expect("Arrow writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish stream");
    }
    (
        [("content-type", verglas_sdk::ARROW_STREAM_CONTENT_TYPE)],
        Body::from(bytes),
    )
}
