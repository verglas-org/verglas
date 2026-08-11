//! Tenant-local database resource management API.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use verglas_database::{
    CreateDatabaseRequest, DatabaseKind, DatabaseManager, DatabaseServiceError, DatabaseView,
    PlanError,
};

use crate::data_plane::AuthenticatedPrincipal;

/// Shared database service independent of its persistence implementation.
pub type DatabaseRuntime = Arc<dyn DatabaseManager>;

/// Shared authorization lifecycle paired with database resource mutations.
pub type DatabaseAuthorizationRuntime = Arc<dyn DatabaseAuthorization>;

/// Authorization resource operations required by the database API.
#[async_trait]
pub trait DatabaseAuthorization: Send + Sync {
    /// Idempotently creates `database/{name}`, grants its creator `own`, and
    /// grants only the selected engine's service its required action set.
    async fn create_database_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        database: &str,
        kind: DatabaseKind,
    ) -> Result<(), DatabaseAuthorizationError>;

    /// Idempotently deletes `database/{name}` and grants rooted on it.
    async fn delete_database_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        database: &str,
    ) -> Result<(), DatabaseAuthorizationError>;
}

/// Bounded lifecycle failure that never exposes authorization credentials.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("database authorization lifecycle failed: {message}")]
pub struct DatabaseAuthorizationError {
    message: String,
}

impl DatabaseAuthorizationError {
    /// Creates a safe operator-facing failure message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Bounded collection envelope returned by database discovery.
#[derive(Debug, Serialize)]
struct DatabaseListResponse {
    databases: Vec<DatabaseView>,
}

/// State fixed to the tenant-local access deployment.
#[derive(Clone)]
struct DatabaseApi {
    service: DatabaseRuntime,
    authorization: DatabaseAuthorizationRuntime,
    tenant_id: Arc<str>,
}

/// Mounts the database API for exactly one configured tenant.
pub fn router(
    service: DatabaseRuntime,
    authorization: DatabaseAuthorizationRuntime,
    tenant_id: String,
) -> Router {
    Router::new()
        .route("/v1/databases", get(list_databases).post(create_database))
        .route(
            "/v1/databases/{name}",
            get(get_database).delete(delete_database),
        )
        .with_state(DatabaseApi {
            service,
            authorization,
            tenant_id: Arc::from(tenant_id),
        })
}

/// Lists public definitions for the configured tenant's databases.
async fn list_databases(State(runtime): State<DatabaseApi>) -> Response {
    match runtime.service.list_databases(&runtime.tenant_id).await {
        Ok(databases) => Json(DatabaseListResponse { databases }).into_response(),
        Err(error) => database_error(error),
    }
}

/// Returns one public database definition by tenant-local name.
async fn get_database(State(runtime): State<DatabaseApi>, Path(name): Path<String>) -> Response {
    match runtime
        .service
        .get_database(&runtime.tenant_id, &name)
        .await
    {
        Ok(database) => Json(database).into_response(),
        Err(error) => database_error(error),
    }
}

/// Deletes one database owned by the configured tenant.
async fn delete_database(
    State(runtime): State<DatabaseApi>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(name): Path<String>,
) -> Response {
    if !require_tenant(&runtime, &principal) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let deletion = runtime
        .service
        .delete_database(&runtime.tenant_id, &name)
        .await;
    if let Err(error) = &deletion
        && !matches!(error, DatabaseServiceError::NotFound { .. })
    {
        return database_error(error.clone());
    }
    match runtime
        .authorization
        .delete_database_resource(&principal, &name)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => authorization_error(error),
    }
}

/// Validates, resolves scoped secret IDs, and persists one database resource.
async fn create_database(
    State(runtime): State<DatabaseApi>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<CreateDatabaseRequest>,
) -> Response {
    if !require_tenant(&runtime, &principal) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let plan = match request.plan(runtime.tenant_id.as_ref()) {
        Ok(plan) => plan,
        Err(error) => return plan_error(error),
    };
    let database_name = plan.name().to_owned();
    let database_kind = plan.kind();
    if let Err(error) = runtime
        .authorization
        .create_database_resource(&principal, &database_name, database_kind)
        .await
    {
        return authorization_error(error);
    }
    let database = match runtime.service.create_database(plan).await {
        Ok(database) => database,
        Err(error) => {
            return match runtime
                .authorization
                .delete_database_resource(&principal, &database_name)
                .await
            {
                Ok(()) => database_error(error),
                Err(rollback) => authorization_error(DatabaseAuthorizationError::new(format!(
                    "{error}; authorization rollback failed: {rollback}"
                ))),
            };
        }
    };
    (StatusCode::CREATED, Json(database)).into_response()
}

/// Rejects a verified identity from any tenant other than this deployment.
fn require_tenant(runtime: &DatabaseApi, principal: &AuthenticatedPrincipal) -> bool {
    principal.tenant_id == runtime.tenant_id.as_ref()
}

/// Maps authorization lifecycle failures to a fail-closed dependency response.
fn authorization_error(error: DatabaseAuthorizationError) -> Response {
    (
        StatusCode::BAD_GATEWAY,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}

/// Maps declaration failures to bounded client errors.
fn plan_error(error: PlanError) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}

/// Maps resource and secret failures without exposing secret material.
fn database_error(error: DatabaseServiceError) -> Response {
    let status = match &error {
        DatabaseServiceError::Duplicate { .. } => StatusCode::CONFLICT,
        DatabaseServiceError::NotFound { .. } => StatusCode::NOT_FOUND,
        DatabaseServiceError::Secret(_) => StatusCode::FORBIDDEN,
        DatabaseServiceError::Repository(_) | DatabaseServiceError::Provisioning(_) => {
            StatusCode::BAD_GATEWAY
        }
    };
    (
        status,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}
