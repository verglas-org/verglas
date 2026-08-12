//! HTTP surface for the isolated logical write role.
//!
//! The role accepts one bounded Arrow IPC stream per request and publishes one
//! idempotent Iceberg append. Object files are written through the cache S3
//! endpoint by the catalog connection held by the production committer.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use verglas_api::table::CommitResponse;

/// Maximum Arrow payload accepted by one bounded write request.
const WRITE_BODY_LIMIT_BYTES: usize = 32 * 1024 * 1024;

/// Logical table commit implementation behind the HTTP role.
#[async_trait]
pub trait BatchCommitter: Send + Sync {
    /// Commits the batches to `table` with replay protection.
    async fn commit(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
        idempotency_key: Option<String>,
    ) -> Result<CommitResponse, String>;

    /// Creates or appends a table from one bounded source-file payload.
    async fn ingest(
        &self,
        table: &str,
        mode: &str,
        format: &str,
        partition_by: Option<&str>,
        body: Bytes,
        idempotency_key: Option<String>,
    ) -> Result<Value, String>;
}

/// Publishes only snapshots that the authoritative catalog already acknowledged.
#[async_trait]
pub trait CommitPublisher: Send + Sync {
    /// Durably publishes one table/snapshot identity before the write response is acknowledged.
    async fn publish(&self, table: &str, snapshot_id: &str) -> Result<(), String>;
}

/// State shared by all write-role handlers.
#[derive(Clone)]
pub struct AppState {
    committer: Arc<dyn BatchCommitter>,
    publisher: Arc<dyn CommitPublisher>,
}

impl AppState {
    /// Creates role state around one production or test committer.
    pub fn new(committer: Arc<dyn BatchCommitter>, publisher: Arc<dyn CommitPublisher>) -> Self {
        Self {
            committer,
            publisher,
        }
    }
}

/// Builds the isolated role's health and write routes.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/write/{name}", post(write_arrow))
        .route("/v1/ingest/{name}", post(ingest_file))
        .layer(axum::extract::DefaultBodyLimit::max(WRITE_BODY_LIMIT_BYTES))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct IngestQuery {
    mode: String,
    format: String,
    partition_by: Option<String>,
}

/// Relays bounded source bytes to the writer that owns schema inference and
/// Iceberg file creation.
async fn ingest_file(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<IngestQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    match state
        .committer
        .ingest(
            &name,
            &query.mode,
            &query.format,
            query.partition_by.as_deref(),
            body,
            idempotency_key,
        )
        .await
    {
        Ok(response) => {
            let snapshot_id = response.get("snapshot_id").and_then(Value::as_i64);
            if let Some(snapshot_id) = snapshot_id
                && let Err(error) = state
                    .publisher
                    .publish(&name, &snapshot_id.to_string())
                    .await
            {
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("snapshot committed but event publication failed: {error}"),
                )
                    .into_response();
            }
            Json(response).into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, error).into_response(),
    }
}

/// Decodes one bounded Arrow stream and commits it exactly once.
async fn write_arrow(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !content_type.starts_with("application/vnd.apache.arrow.stream") {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "write requests require application/vnd.apache.arrow.stream",
        )
            .into_response();
    }
    let reader = match arrow_ipc::reader::StreamReader::try_new(Cursor::new(body), None) {
        Ok(reader) => reader,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let batches = match reader.collect::<Result<Vec<_>, _>>() {
        Ok(batches) => batches,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    match state
        .committer
        .commit(&name, batches, idempotency_key)
        .await
    {
        Ok(response) => match state.publisher.publish(&name, &response.snapshot_id).await {
            Ok(()) => Json(response).into_response(),
            Err(error) => (
                StatusCode::BAD_GATEWAY,
                format!("snapshot committed but event publication failed: {error}"),
            )
                .into_response(),
        },
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}
