//! End-to-end contract tests for the Rust data-plane client.

use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use futures::{StreamExt, stream};
use serde_json::json;
use tokio::net::TcpListener;
use verglas_sdk::{
    Client, ColumnSpec, ConnectOptions, EnsureTable, PartitionSpec, TableDefinition,
};

#[derive(Clone, Default)]
struct Captured {
    authorizations: Arc<Mutex<Vec<String>>>,
    append_content_types: Arc<Mutex<Vec<String>>>,
}

/// Verifies the shared authenticated table data-plane contract end to end.
#[tokio::test]
async fn ensure_append_and_query_use_one_authenticated_streaming_contract() {
    let captured = Captured::default();
    let app = Router::new()
        .route("/v1/tables/{name}", post(ensure_table))
        .route("/v1/tables/{name}/commit", post(commit_arrow))
        .route("/v1/query", post(query_arrow))
        .with_state(captured.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
    let endpoint = format!("http://{}", listener.local_addr().expect("mock address"));
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock server");
    });

    let client = Client::connect(
        ConnectOptions::new(endpoint)
            .with_token("sdk-token")
            .with_connect_timeout(std::time::Duration::from_secs(1)),
    )
    .expect("connect client");
    let definition = TableDefinition {
        schema: vec![
            ColumnSpec::required("id", "int64"),
            ColumnSpec::nullable("value", "utf8"),
        ],
        partitions: vec![PartitionSpec::identity("id")],
    };
    let ensured = client
        .ensure_table("sdk.events", &definition)
        .await
        .expect("ensure table");
    assert_eq!(ensured, EnsureTable::Created);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .expect("batch");
    let append = client
        .append_stream("sdk.events", stream::iter(vec![Ok(batch)]), "run-1")
        .await
        .expect("append stream");
    assert_eq!(append.rows_committed, 2);
    assert_eq!(append.commits, 1);

    let mut query = client
        .query_stream("select id, value from sdk.events order by id")
        .await
        .expect("query stream");
    let result = query.next().await.expect("query batch").expect("batch ok");
    assert_eq!(result.num_rows(), 2);
    assert!(query.next().await.is_none());

    assert_eq!(
        captured
            .authorizations
            .lock()
            .expect("auth lock")
            .as_slice(),
        ["Bearer sdk-token", "Bearer sdk-token", "Bearer sdk-token"]
    );
    assert_eq!(
        captured
            .append_content_types
            .lock()
            .expect("content type lock")
            .as_slice(),
        ["application/vnd.apache.arrow.stream"]
    );
}

/// Existing tables must match every schema and partition field exactly.
#[tokio::test]
async fn ensure_table_rejects_definition_drift() {
    let app = Router::new().route(
        "/v1/tables/{name}",
        post(|| async {
            (
                StatusCode::CONFLICT,
                Json(json!({
                    "created": false,
                    "definition": {
                        "schema": [{"name":"id", "type":"utf8", "nullable":false}],
                        "partitions": []
                    }
                })),
            )
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
    let endpoint = format!("http://{}", listener.local_addr().expect("mock address"));
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock server");
    });
    let client = Client::connect(ConnectOptions::new(endpoint)).expect("client");
    let expected = TableDefinition {
        schema: vec![ColumnSpec::required("id", "int64")],
        partitions: vec![],
    };
    let error = client
        .ensure_table("sdk.events", &expected)
        .await
        .expect_err("definition drift must fail");
    assert!(error.to_string().contains("definition mismatch"));
}

/// Validates and acknowledges an idempotent ensure-table request.
async fn ensure_table(
    State(captured): State<Captured>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    capture_auth(&captured, &headers);
    assert_eq!(name, "sdk.events");
    assert_eq!(body["schema"][0]["name"], "id");
    (
        StatusCode::CREATED,
        Json(json!({
            "created": true,
            "definition": body
        })),
    )
}

/// Decodes and acknowledges a streamed Arrow append.
async fn commit_arrow(
    State(captured): State<Captured>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    capture_auth(&captured, &headers);
    assert_eq!(name, "sdk.events");
    captured
        .append_content_types
        .lock()
        .expect("content type lock")
        .push(
            headers
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string(),
        );
    let reader =
        arrow_ipc::reader::StreamReader::try_new(body.as_ref(), None).expect("decode append IPC");
    let rows: usize = reader
        .map(|batch| batch.expect("append batch").num_rows())
        .sum();
    Json(json!({
        "snapshotId":"10",
        "rowsCommitted":rows,
        "watermark":"10",
        "idempotent":false
    }))
}

/// Returns a query result using the Arrow IPC streaming representation.
async fn query_arrow(State(captured): State<Captured>, headers: HeaderMap) -> impl IntoResponse {
    capture_auth(&captured, &headers);
    assert_eq!(
        headers.get("accept").and_then(|value| value.to_str().ok()),
        Some("application/vnd.apache.arrow.stream")
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .expect("query batch");
    let mut bytes = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).expect("query writer");
        writer.write(&batch).expect("write query batch");
        writer.finish().expect("finish query stream");
    }
    let chunks = bytes
        .chunks(3)
        .map(|chunk| Ok::<_, std::convert::Infallible>(Bytes::copy_from_slice(chunk)))
        .collect::<Vec<_>>();
    (
        [("content-type", "application/vnd.apache.arrow.stream")],
        Body::from_stream(stream::iter(chunks)),
    )
}

/// Records bearer authentication for later assertions.
fn capture_auth(captured: &Captured, headers: &HeaderMap) {
    captured.authorizations.lock().expect("auth lock").push(
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string(),
    );
}
