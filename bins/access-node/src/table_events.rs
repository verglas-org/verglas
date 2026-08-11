//! Routes acknowledged table commits through one explicit durable queue.
//! Subscribers select exact database/table topics and retain queue receipts.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Extension, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use verglas_queue::{QueueManager, QueueMessage, Receipt};
use verglas_rest::data_plane::AuthenticatedDatabaseId;
use verglas_rest::queue::{QueueProxy, QueueProxyResponse};

#[derive(Clone)]
struct TableEventsState {
    queues: Arc<dyn QueueManager>,
    proxy: Arc<dyn QueueProxy>,
    tenant_id: Arc<str>,
    queue_name: Arc<str>,
}

/// Builds database-scoped commit publication and subscription routes.
pub(crate) fn router(
    queues: Arc<dyn QueueManager>,
    proxy: Arc<dyn QueueProxy>,
    tenant_id: String,
    queue_name: String,
) -> Router {
    Router::new()
        .route("/v1/databases/{database}/commits/{table}", post(publish))
        .route("/v1/databases/{database}/subscribe", post(subscribe))
        .route("/v1/databases/{database}/subscriptions/ack", post(ack))
        .with_state(TableEventsState {
            queues,
            proxy,
            tenant_id: Arc::from(tenant_id),
            queue_name: Arc::from(queue_name),
        })
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PublishBody {
    snapshot_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SubscribeBody {
    group: String,
    owner: String,
    tables: Vec<String>,
    max: Option<u32>,
    lease_seconds: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AckBody {
    group: String,
    receipt: Receipt,
}

/// Persists one event only after the writer has an acknowledged snapshot id.
async fn publish(
    State(state): State<TableEventsState>,
    Extension(database_id): Extension<AuthenticatedDatabaseId>,
    Path((_database, table)): Path<(String, String)>,
    Json(body): Json<PublishBody>,
) -> Response {
    if table.trim().is_empty() || body.snapshot_id.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "table and snapshotId are required").into_response();
    }
    let topic = table_topic(database_id.as_str(), &table);
    let message = QueueMessage {
        id: format!(
            "iceberg:{}:{table}:{}",
            database_id.as_str(),
            body.snapshot_id
        ),
        topic,
        payload: serde_json::json!({
            "database": database_id.as_str(),
            "table": table,
            "snapshotId": body.snapshot_id,
            "committedAt": "",
        }),
    };
    let body = match serde_json::to_vec(&serde_json::json!({ "messages": [message] })) {
        Ok(body) => Bytes::from(body),
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };
    proxy(&state, "enqueue", body).await
}

/// Opens one push subscription over exact table topics.
async fn subscribe(
    State(state): State<TableEventsState>,
    Extension(database_id): Extension<AuthenticatedDatabaseId>,
    Path(_database): Path<String>,
    Json(body): Json<SubscribeBody>,
) -> Response {
    if body.tables.is_empty() || body.tables.iter().any(|table| table.trim().is_empty()) {
        return (StatusCode::BAD_REQUEST, "at least one table is required").into_response();
    }
    let topics = body
        .tables
        .iter()
        .map(|table| table_topic(database_id.as_str(), table))
        .collect::<Vec<_>>();
    let body = match serde_json::to_vec(&serde_json::json!({
        "group": body.group,
        "owner": body.owner,
        "topics": topics,
        "max": body.max,
        "leaseSeconds": body.lease_seconds,
    })) {
        Ok(body) => Bytes::from(body),
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };
    proxy(&state, "subscribe", body).await
}

/// Acknowledges a delivered table event under its original consumer group.
async fn ack(
    State(state): State<TableEventsState>,
    Extension(_database_id): Extension<AuthenticatedDatabaseId>,
    Path(_database): Path<String>,
    Json(body): Json<AckBody>,
) -> Response {
    let body = match serde_json::to_vec(&body) {
        Ok(body) => Bytes::from(body),
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };
    proxy(&state, "ack", body).await
}

/// Resolves the configured explicit queue and preserves its streaming response.
async fn proxy(state: &TableEventsState, operation: &str, body: Bytes) -> Response {
    let queue = match state
        .queues
        .get_queue(&state.tenant_id, &state.queue_name)
        .await
    {
        Ok(queue) => queue,
        Err(error) => return (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response(),
    };
    match state.proxy.request(&queue, operation, body).await {
        Ok(response) => upstream_response(response),
        Err(error) => (StatusCode::BAD_GATEWAY, error).into_response(),
    }
}

/// Converts one queue-container response without buffering a subscription body.
fn upstream_response(upstream: QueueProxyResponse) -> Response {
    let status = StatusCode::from_u16(upstream.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut response = Response::new(upstream.body);
    *response.status_mut() = status;
    if let Ok(value) = upstream.content_type.parse() {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}

/// Creates the exact topic shared by publishers and database-scoped subscribers.
fn table_topic(database: &str, table: &str) -> String {
    format!("database/{database}/table/{table}")
}
