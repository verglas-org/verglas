//! Serves one independently provisioned queue over its private HTTP boundary.
//! All durable state lives in the queue-owned PostgreSQL database.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use verglas_queue::{AckRequest, PollRequest, QueueError, QueueStore, Receipt};

/// Shared queue service state fixed at process startup.
#[derive(Clone)]
struct ServiceState {
    store: Arc<dyn QueueStore>,
    token: Arc<str>,
}

/// Creates the private data-plane router for one queue deployment.
pub fn router(store: Arc<dyn QueueStore>, token: String) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/enqueue", post(enqueue))
        .route("/v1/poll", post(poll))
        .route("/v1/ack", post(ack))
        .with_state(ServiceState {
            store,
            token: Arc::from(token),
        })
}

/// Reports process readiness after the PostgreSQL store has connected.
async fn health() -> StatusCode {
    StatusCode::OK
}

/// Bounded append body accepted by the queue container.
#[derive(Debug, Deserialize)]
struct EnqueueBody {
    messages: Vec<Value>,
}

/// Stable positions assigned to one append batch.
#[derive(Debug, Serialize)]
struct EnqueueResponse {
    positions: Vec<i64>,
}

/// Consumer-owned claim request; service time is never accepted from callers.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PollBody {
    group: String,
    owner: String,
    max: u32,
    lease_seconds: u64,
}

/// Consumer acknowledgement body carrying the exact fenced receipt.
#[derive(Debug, Deserialize)]
struct AckBody {
    group: String,
    receipt: Receipt,
}

/// Appends messages only after authenticating the private queue credential.
async fn enqueue(
    State(state): State<ServiceState>,
    headers: HeaderMap,
    Json(body): Json<EnqueueBody>,
) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match state.store.enqueue(&body.messages).await {
        Ok(positions) => Json(EnqueueResponse { positions }).into_response(),
        Err(error) => queue_error(error),
    }
}

/// Claims an exclusive bounded batch using server-owned time.
async fn poll(
    State(state): State<ServiceState>,
    headers: HeaderMap,
    Json(body): Json<PollBody>,
) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let request = PollRequest {
        group: body.group,
        owner: body.owner,
        max: body.max,
        now: Utc::now(),
        lease_seconds: body.lease_seconds,
    };
    match state.store.poll(&request).await {
        Ok(deliveries) => Json(serde_json::json!({ "deliveries": deliveries })).into_response(),
        Err(error) => queue_error(error),
    }
}

/// Acknowledges exactly one live delivery generation using server-owned time.
async fn ack(
    State(state): State<ServiceState>,
    headers: HeaderMap,
    Json(body): Json<AckBody>,
) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let request = AckRequest {
        group: body.group,
        receipt: body.receipt,
        now: Utc::now(),
    };
    match state.store.ack(&request).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => queue_error(error),
    }
}

/// Validates the queue-specific private bearer without accepting alternate schemes.
fn authorized(headers: &HeaderMap, token: &str) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|value| value == token)
}

/// Maps queue failures to bounded HTTP errors without exposing database details.
fn queue_error(error: QueueError) -> Response {
    let status = match error {
        QueueError::Invalid(_) => StatusCode::BAD_REQUEST,
        QueueError::StaleReceipt { .. } => StatusCode::CONFLICT,
        QueueError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string()).into_response()
}
