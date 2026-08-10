//! Manages explicit queue resources while data operations remain in queue containers.
//! Unknown names fail closed; these routes never create filesystem state.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use verglas_queue::{
    CreateQueueRequest, PlanError, QueueManager, QueueRepositoryError, QueueServiceError, QueueView,
};

use crate::data_plane::AuthenticatedPrincipal;

/// Object-safe queue resource service.
pub type QueueRuntime = Arc<dyn QueueManager>;

/// Shared authorization lifecycle paired with queue mutations.
pub type QueueAuthorizationRuntime = Arc<dyn QueueAuthorization>;

/// Authorization resource operations required by the queue API.
#[async_trait]
pub trait QueueAuthorization: Send + Sync {
    /// Creates `queue/{name}` and grants the authenticated creator ownership.
    async fn create_queue_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        queue: &str,
    ) -> Result<(), QueueAuthorizationError>;

    /// Deletes `queue/{name}` after the route's modify check succeeds.
    async fn delete_queue_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        queue: &str,
    ) -> Result<(), QueueAuthorizationError>;
}

/// Bounded lifecycle failure that never exposes authorization credentials.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("queue authorization lifecycle failed: {message}")]
pub struct QueueAuthorizationError {
    message: String,
}

impl QueueAuthorizationError {
    /// Creates a safe operator-facing failure message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Tenant-fixed queue resource API state.
#[derive(Clone)]
struct QueueApi {
    service: QueueRuntime,
    authorization: QueueAuthorizationRuntime,
    tenant_id: Arc<str>,
}

/// Bounded response returned from one private queue container.
pub struct QueueProxyResponse {
    /// Upstream queue-service status.
    pub status: u16,
    /// Bounded JSON or text response bytes.
    pub body: Bytes,
}

/// Private queue-container transport implemented by the local provisioner.
#[async_trait]
pub trait QueueProxy: Send + Sync {
    /// Forwards one authenticated operation to the declared queue container.
    async fn request(
        &self,
        queue: &QueueView,
        operation: &str,
        body: Bytes,
    ) -> Result<QueueProxyResponse, String>;
}

/// State used only by queue message operations.
#[derive(Clone)]
struct QueueDataApi {
    service: QueueRuntime,
    proxy: Arc<dyn QueueProxy>,
    tenant_id: Arc<str>,
}

/// Stable collection envelope returned by queue discovery.
#[derive(Debug, Serialize)]
struct QueueListResponse {
    queues: Vec<QueueView>,
}

/// Mounts explicit queue lifecycle routes for one tenant.
pub fn router(
    service: QueueRuntime,
    authorization: QueueAuthorizationRuntime,
    tenant_id: String,
) -> Router {
    Router::new()
        .route("/v1/queues", get(list_queues).post(create_queue))
        .route("/v1/queues/{name}", get(get_queue).delete(delete_queue))
        .with_state(QueueApi {
            service,
            authorization,
            tenant_id: Arc::from(tenant_id),
        })
}

/// Mounts queue message operations that resolve a declaration before proxying.
pub fn data_router(service: QueueRuntime, proxy: Arc<dyn QueueProxy>, tenant_id: String) -> Router {
    Router::new()
        .route("/v1/queues/{name}/enqueue", axum::routing::post(enqueue))
        .route("/v1/queues/{name}/poll", axum::routing::post(poll))
        .route("/v1/queues/{name}/ack", axum::routing::post(ack))
        .with_state(QueueDataApi {
            service,
            proxy,
            tenant_id: Arc::from(tenant_id),
        })
}

/// Lists only queues that were explicitly created.
async fn list_queues(State(api): State<QueueApi>) -> Response {
    match api.service.list_queues(&api.tenant_id).await {
        Ok(queues) => Json(QueueListResponse { queues }).into_response(),
        Err(error) => service_error(error),
    }
}

/// Returns one declared queue without exposing its private database credentials.
async fn get_queue(State(api): State<QueueApi>, Path(name): Path<String>) -> Response {
    match api.service.get_queue(&api.tenant_id, &name).await {
        Ok(queue) => Json(queue).into_response(),
        Err(error) => service_error(error),
    }
}

/// Creates both dedicated runtimes from one validated declaration.
async fn create_queue(
    State(api): State<QueueApi>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<CreateQueueRequest>,
) -> Response {
    if principal.tenant_id != api.tenant_id.as_ref() {
        return StatusCode::FORBIDDEN.into_response();
    }
    let plan = match request.plan(api.tenant_id.as_ref()) {
        Ok(plan) => plan,
        Err(error) => return plan_error(error),
    };
    let name = plan.name().to_owned();
    let queue = match api.service.create_queue(plan).await {
        Ok(queue) => queue,
        Err(error) => return service_error(error),
    };
    if let Err(error) = api
        .authorization
        .create_queue_resource(&principal, &name)
        .await
    {
        return match api.service.delete_queue(&api.tenant_id, &name).await {
            Ok(()) => authorization_error(error),
            Err(rollback) => authorization_error(QueueAuthorizationError::new(format!(
                "{error}; queue rollback failed: {rollback}"
            ))),
        };
    }
    (StatusCode::CREATED, Json(queue)).into_response()
}

/// Deletes the queue container and Neon deployment before its declaration.
async fn delete_queue(
    State(api): State<QueueApi>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(name): Path<String>,
) -> Response {
    if principal.tenant_id != api.tenant_id.as_ref() {
        return StatusCode::FORBIDDEN.into_response();
    }
    let deletion = api.service.delete_queue(&api.tenant_id, &name).await;
    if let Err(error) = &deletion
        && !matches!(error, QueueServiceError::NotFound { .. })
    {
        return service_error(error.clone());
    }
    match api
        .authorization
        .delete_queue_resource(&principal, &name)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => authorization_error(error),
    }
}

/// Maps authorization failures to a fail-closed dependency response.
fn authorization_error(error: QueueAuthorizationError) -> Response {
    (
        StatusCode::BAD_GATEWAY,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}

/// Maps declaration validation to a bounded client error.
fn plan_error(error: PlanError) -> Response {
    (StatusCode::BAD_REQUEST, error.to_string()).into_response()
}

/// Maps lifecycle failures without returning PostgreSQL or runtime credentials.
fn service_error(error: QueueServiceError) -> Response {
    let status = match &error {
        QueueServiceError::NotFound { .. } => StatusCode::NOT_FOUND,
        QueueServiceError::Repository(QueueRepositoryError::Duplicate { .. }) => {
            StatusCode::CONFLICT
        }
        QueueServiceError::Repository(QueueRepositoryError::Backend(_))
        | QueueServiceError::Provisioning(_) => StatusCode::BAD_GATEWAY,
    };
    (status, error.to_string()).into_response()
}

/// Proxies one append after proving the named queue was explicitly declared.
async fn enqueue(
    State(api): State<QueueDataApi>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    proxy_operation(api, name, "enqueue", body).await
}

/// Proxies one lease claim after proving the named queue was explicitly declared.
async fn poll(State(api): State<QueueDataApi>, Path(name): Path<String>, body: Bytes) -> Response {
    proxy_operation(api, name, "poll", body).await
}

/// Proxies one fenced acknowledgement after proving the named queue was declared.
async fn ack(State(api): State<QueueDataApi>, Path(name): Path<String>, body: Bytes) -> Response {
    proxy_operation(api, name, "ack", body).await
}

/// Resolves one durable queue and relays a bounded private-container response.
async fn proxy_operation(
    api: QueueDataApi,
    name: String,
    operation: &'static str,
    body: Bytes,
) -> Response {
    let queue = match api.service.get_queue(&api.tenant_id, &name).await {
        Ok(queue) => queue,
        Err(error) => return service_error(error),
    };
    match api.proxy.request(&queue, operation, body).await {
        Ok(upstream) => {
            let status = StatusCode::from_u16(upstream.status).unwrap_or(StatusCode::BAD_GATEWAY);
            (
                status,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                upstream.body,
            )
                .into_response()
        }
        Err(error) => (StatusCode::BAD_GATEWAY, error).into_response(),
    }
}
