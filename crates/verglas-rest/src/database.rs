//! Tenant-local database resource creation API.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use verglas_database::{CreateDatabaseRequest, DatabaseCreator, DatabaseServiceError, PlanError};

/// Shared database service independent of its persistence implementation.
pub type DatabaseRuntime = Arc<dyn DatabaseCreator>;

/// State fixed to the tenant-local access deployment.
#[derive(Clone)]
struct DatabaseApi {
    service: DatabaseRuntime,
    tenant_id: Arc<str>,
}

/// Mounts the database API for exactly one configured tenant.
pub fn router(service: DatabaseRuntime, tenant_id: String) -> Router {
    Router::new()
        .route("/v1/databases", post(create_database))
        .with_state(DatabaseApi {
            service,
            tenant_id: Arc::from(tenant_id),
        })
}

/// Validates, resolves scoped secret IDs, and persists one database resource.
async fn create_database(
    State(runtime): State<DatabaseApi>,
    Json(request): Json<CreateDatabaseRequest>,
) -> Response {
    let plan = match request.plan(runtime.tenant_id.as_ref()) {
        Ok(plan) => plan,
        Err(error) => return plan_error(error),
    };
    match runtime.service.create_database(plan).await {
        Ok(database) => (StatusCode::CREATED, Json(database)).into_response(),
        Err(error) => database_error(error),
    }
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
        DatabaseServiceError::Secret(_) => StatusCode::FORBIDDEN,
        DatabaseServiceError::Repository(_) => StatusCode::BAD_GATEWAY,
    };
    (
        status,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}
