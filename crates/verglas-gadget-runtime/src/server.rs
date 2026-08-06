//! Exposes the authenticated Gadget registry and browser bundle over HTTP.

use std::sync::{Arc, Mutex, MutexGuard};

use axum::body::{Body, to_bytes};
use axum::extract::ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, put};
use axum::{Json, Router};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

use crate::{
    GadgetBundle, HostConfig, ProcessSupervisor, RegisterOutcome, RuntimeCatalog, RuntimeConfig,
    RuntimeError, supervisor::gadget_capability_token,
};

const MAX_CAPABILITY_REQUEST_BYTES: usize = 4 * 1024 * 1024;

/// Trusted upstream configuration held only by the Gadget runtime control process.
#[derive(Debug, Clone)]
pub struct DataPlaneConfig {
    /// Base URL of the local or cloud Verglas data API.
    pub endpoint: String,
    /// Upstream bearer credential; never copied into a Gadget child process.
    pub token: String,
    /// Loopback base URL through which child capabilities reach this runtime.
    pub capability_base_url: String,
}

/// Shared state for the runtime's bounded HTTP handlers.
struct ServiceState {
    catalog: Mutex<RuntimeCatalog>,
    token: String,
    supervisor: Option<Arc<ProcessSupervisor>>,
    data_plane: Option<DataPlaneConfig>,
    http: reqwest::Client,
}

/// An authenticated HTTP service around one runtime catalog.
pub struct RuntimeService {
    state: Arc<ServiceState>,
}

impl RuntimeService {
    /// Creates a runtime service with one explicit bearer token.
    pub fn new(config: RuntimeConfig, token: String) -> Result<Self, RuntimeError> {
        if token.is_empty() {
            return Err(RuntimeError::EmptyRuntimeToken);
        }
        Ok(Self {
            state: Arc::new(ServiceState {
                catalog: Mutex::new(RuntimeCatalog::new(config)?),
                token,
                supervisor: None,
                data_plane: None,
                http: reqwest::Client::new(),
            }),
        })
    }

    /// Creates a service that lazily starts child hosts for Gadget RPC sessions.
    pub fn with_host(
        config: RuntimeConfig,
        token: String,
        host: HostConfig,
        data_plane: DataPlaneConfig,
    ) -> Result<Self, RuntimeError> {
        if token.is_empty() {
            return Err(RuntimeError::EmptyRuntimeToken);
        }
        if data_plane.endpoint.is_empty()
            || data_plane.token.is_empty()
            || data_plane.capability_base_url.is_empty()
        {
            return Err(RuntimeError::EmptyDataPlaneConfig);
        }
        let supervisor = ProcessSupervisor::new(
            host,
            data_plane
                .capability_base_url
                .trim_end_matches('/')
                .to_owned(),
            token.clone(),
        );
        Ok(Self {
            state: Arc::new(ServiceState {
                catalog: Mutex::new(RuntimeCatalog::new(config)?),
                token,
                supervisor: Some(Arc::new(supervisor)),
                data_plane: Some(data_plane),
                http: reqwest::Client::new(),
            }),
        })
    }

    /// Builds the complete runtime HTTP router.
    pub fn router(self) -> Router {
        Router::new()
            .route("/healthz", get(health))
            .route("/v1/gadgets", get(list_gadgets))
            .route(
                "/v1/gadgets/{id}",
                put(register_gadget).delete(delete_gadget),
            )
            .route("/v1/gadgets/{id}/client.js", get(client_module))
            .route("/v1/gadgets/{id}/rpc", get(gadget_rpc))
            .route("/v1/gadgets/{id}/data/{*path}", any(proxy_data))
            .with_state(self.state)
    }
}

/// Proxies one Gadget-scoped SDK call while retaining the upstream credential here.
async fn proxy_data(
    State(state): State<Arc<ServiceState>>,
    Path((id, path)): Path<(String, String)>,
    request: Request<Body>,
) -> Response {
    let expected = gadget_capability_token(&state.token, &id);
    if !authorized(request.headers(), &expected) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let registered = match lock_catalog(&state) {
        Ok(catalog) => catalog.get(&id).is_some(),
        Err(status) => return status.into_response(),
    };
    if !registered {
        return StatusCode::NOT_FOUND.into_response();
    }
    if !allowed_data_path(&path) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(data_plane) = &state.data_plane else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let (parts, body) = request.into_parts();
    let body = match to_bytes(body, MAX_CAPABILITY_REQUEST_BYTES).await {
        Ok(body) => body,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let mut upstream_url = format!(
        "{}/{}",
        data_plane.endpoint.trim_end_matches('/'),
        path.trim_start_matches('/'),
    );
    if let Some(query) = parts.uri.query() {
        upstream_url.push('?');
        upstream_url.push_str(query);
    }
    let mut upstream = state
        .http
        .request(parts.method, upstream_url)
        .bearer_auth(&data_plane.token)
        .body(body);
    for name in [axum::http::header::ACCEPT, CONTENT_TYPE] {
        if let Some(value) = parts.headers.get(&name) {
            upstream = upstream.header(name, value);
        }
    }
    let response = match upstream.send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(gadget_id = %id, %error, "Verglas data capability request failed");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };
    let status = response.status();
    let content_type = response.headers().get(CONTENT_TYPE).cloned();
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(gadget_id = %id, %error, "Verglas data capability response failed");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };
    let mut result = Response::builder().status(status);
    if let Some(content_type) = content_type {
        result = result.header(CONTENT_TYPE, content_type);
    }
    result
        .body(Body::from(bytes))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn allowed_data_path(path: &str) -> bool {
    [
        "admin/access",
        "catalog/v1/",
        "v1/query",
        "v1/write/",
        "v1/tables/",
        "v1/queues/",
        "v1/graphs/",
        "v1/kv/",
    ]
    .iter()
    .any(|prefix| path == *prefix || path.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::allowed_data_path;

    #[test]
    fn data_capability_covers_sdk_routes_but_not_control_routes() {
        for path in [
            "admin/access",
            "catalog/v1/config",
            "catalog/v1/warehouse/namespaces",
            "v1/query",
            "v1/write/company.events",
            "v1/tables/company.events/rows",
            "v1/queues/jobs/enqueue",
            "v1/graphs/company/query",
            "v1/kv/gadget.workspace/key",
        ] {
            assert!(
                allowed_data_path(path),
                "SDK path should be allowed: {path}"
            );
        }
        for path in ["admin/workers", "v1/workers/source/run", "v1/secrets"] {
            assert!(
                !allowed_data_path(path),
                "control path should be denied: {path}"
            );
        }
    }
}

/// Registration response returned for created and selected revisions.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RegisterResponse {
    outcome: &'static str,
    digest: String,
    previous_version: Option<String>,
}

/// Returns process liveness without exposing runtime configuration.
async fn health() -> StatusCode {
    StatusCode::OK
}

/// Lists selected Gadget revisions in stable identity order.
async fn list_gadgets(State(state): State<Arc<ServiceState>>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match lock_catalog(&state) {
        Ok(catalog) => {
            let records = catalog.list().into_iter().cloned().collect::<Vec<_>>();
            Json(records).into_response()
        }
        Err(status) => status.into_response(),
    }
}

/// Registers or selects one immutable Gadget revision.
async fn register_gadget(
    State(state): State<Arc<ServiceState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(bundle): Json<GadgetBundle>,
) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let outcome = match lock_catalog(&state) {
        Ok(mut catalog) => match catalog.register(&id, bundle) {
            Ok(outcome) => outcome,
            Err(error) => return runtime_error_response(error),
        },
        Err(status) => return status.into_response(),
    };
    let replaces_child = matches!(outcome, RegisterOutcome::Replaced { .. });
    let (status, body) = match outcome {
        RegisterOutcome::Created { digest } => (
            StatusCode::CREATED,
            RegisterResponse {
                outcome: "created",
                digest,
                previous_version: None,
            },
        ),
        RegisterOutcome::Unchanged { digest } => (
            StatusCode::OK,
            RegisterResponse {
                outcome: "unchanged",
                digest,
                previous_version: None,
            },
        ),
        RegisterOutcome::Replaced {
            previous_version,
            digest,
        } => (
            StatusCode::OK,
            RegisterResponse {
                outcome: "replaced",
                digest,
                previous_version: Some(previous_version),
            },
        ),
    };
    if replaces_child
        && let Some(supervisor) = &state.supervisor
        && let Err(error) = supervisor.stop(&id).await
    {
        tracing::warn!(gadget_id = %id, %error, "failed to stop replaced Gadget child");
    }
    (status, Json(body)).into_response()
}

/// Removes one selected Gadget revision.
async fn delete_gadget(
    State(state): State<Arc<ServiceState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let removed = match lock_catalog(&state) {
        Ok(mut catalog) => match catalog.remove(&id) {
            Ok(removed) => removed,
            Err(error) => return runtime_error_response(error),
        },
        Err(status) => return status.into_response(),
    };
    match removed {
        true => {
            if let Some(supervisor) = &state.supervisor
                && let Err(error) = supervisor.stop(&id).await
            {
                tracing::warn!(gadget_id = %id, %error, "failed to stop deleted Gadget child");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            StatusCode::NO_CONTENT.into_response()
        }
        false => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Authenticates and forwards one Cap'n Web connection to its Gadget child.
async fn gadget_rpc(
    State(state): State<Arc<ServiceState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(supervisor) = state.supervisor.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let (record, bundle) = match lock_catalog(&state) {
        Ok(catalog) => {
            let Some(record) = catalog.get(&id).cloned() else {
                return StatusCode::NOT_FOUND.into_response();
            };
            let Some(bundle) = catalog.bundle(&id).cloned() else {
                return StatusCode::NOT_FOUND.into_response();
            };
            (record, bundle)
        }
        Err(status) => return status.into_response(),
    };
    let endpoint = match supervisor.ensure(&id, &record.digest, &bundle).await {
        Ok(endpoint) => endpoint,
        Err(error) => {
            tracing::error!(gadget_id = %id, %error, "failed to start Gadget child");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };
    upgrade.on_upgrade(move |socket| proxy_rpc(socket, endpoint, id))
}

/// Relays bounded WebSocket frames between the authenticated caller and child.
async fn proxy_rpc(socket: WebSocket, endpoint: std::net::SocketAddr, gadget_id: String) {
    let upstream = tokio_tungstenite::connect_async(format!("ws://{endpoint}/api")).await;
    let (upstream, _) = match upstream {
        Ok(connection) => connection,
        Err(error) => {
            tracing::warn!(%gadget_id, %error, "failed to connect to Gadget child RPC");
            return;
        }
    };
    let (mut client_write, mut client_read) = socket.split();
    let (mut upstream_write, mut upstream_read) = upstream.split();

    loop {
        tokio::select! {
            client = client_read.next() => {
                let Some(Ok(message)) = client else { break; };
                let Some(message) = to_upstream(message) else { break; };
                if upstream_write.send(message).await.is_err() { break; }
            }
            upstream = upstream_read.next() => {
                let Some(Ok(message)) = upstream else { break; };
                let Some(message) = to_client(message) else { break; };
                if client_write.send(message).await.is_err() { break; }
            }
        }
    }
}

/// Converts one client frame into the child transport representation.
fn to_upstream(message: AxumMessage) -> Option<TungsteniteMessage> {
    match message {
        AxumMessage::Text(text) => Some(TungsteniteMessage::Text(text.to_string().into())),
        AxumMessage::Binary(bytes) => Some(TungsteniteMessage::Binary(bytes)),
        AxumMessage::Ping(bytes) => Some(TungsteniteMessage::Ping(bytes)),
        AxumMessage::Pong(bytes) => Some(TungsteniteMessage::Pong(bytes)),
        AxumMessage::Close(_) => None,
    }
}

/// Converts one child frame into the caller transport representation.
fn to_client(message: TungsteniteMessage) -> Option<AxumMessage> {
    match message {
        TungsteniteMessage::Text(text) => Some(AxumMessage::Text(text.to_string().into())),
        TungsteniteMessage::Binary(bytes) => Some(AxumMessage::Binary(bytes)),
        TungsteniteMessage::Ping(bytes) => Some(AxumMessage::Ping(bytes)),
        TungsteniteMessage::Pong(bytes) => Some(AxumMessage::Pong(bytes)),
        TungsteniteMessage::Close(_) | TungsteniteMessage::Frame(_) => None,
    }
}

/// Returns the selected browser module for the OS-owned iframe.
async fn client_module(
    State(state): State<Arc<ServiceState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match lock_catalog(&state) {
        Ok(catalog) => match catalog.bundle(&id) {
            Some(bundle) => (
                StatusCode::OK,
                [(CONTENT_TYPE, "text/javascript; charset=utf-8")],
                bundle.client_module.clone(),
            )
                .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
        Err(status) => status.into_response(),
    }
}

/// Locks the catalog and converts poisoning into an honest server failure.
fn lock_catalog(state: &ServiceState) -> Result<MutexGuard<'_, RuntimeCatalog>, StatusCode> {
    state
        .catalog
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// Checks a bearer credential without early-returning on the first byte mismatch.
fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    let Some(value) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some(actual) = value.strip_prefix("Bearer ") else {
        return false;
    };
    constant_time_eq(actual.as_bytes(), expected.as_bytes())
}

/// Compares equal-length byte strings in time independent of their contents.
fn constant_time_eq(actual: &[u8], expected: &[u8]) -> bool {
    if actual.len() != expected.len() {
        return false;
    }
    actual
        .iter()
        .zip(expected)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

/// Maps bounded public runtime failures to HTTP responses.
fn runtime_error_response(error: RuntimeError) -> Response {
    let status = match error {
        RuntimeError::TargetMismatch { .. } => StatusCode::FORBIDDEN,
        RuntimeError::Capacity { .. } => StatusCode::TOO_MANY_REQUESTS,
        RuntimeError::RevisionConflict { .. } => StatusCode::CONFLICT,
        RuntimeError::InvalidCapacity
        | RuntimeError::InvalidGadgetId { .. }
        | RuntimeError::InvalidBundlePath { .. }
        | RuntimeError::ReservedBundlePath { .. }
        | RuntimeError::MissingServerModule
        | RuntimeError::InvalidVersion { .. }
        | RuntimeError::BundleTooLarge { .. } => StatusCode::BAD_REQUEST,
        RuntimeError::BundleEncoding(_)
        | RuntimeError::EmptyRuntimeToken
        | RuntimeError::EmptyDataPlaneConfig => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string()).into_response()
}
