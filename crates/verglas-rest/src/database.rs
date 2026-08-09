//! Tenant-local database resource management API.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use verglas_database::{
    CreateDatabaseRequest, DatabaseManager, DatabaseServiceError, DatabaseView, PlanError,
};

/// Shared database service independent of its persistence implementation.
pub type DatabaseRuntime = Arc<dyn DatabaseManager>;

/// Bounded collection envelope returned by database discovery.
#[derive(Debug, Serialize)]
struct DatabaseListResponse {
    databases: Vec<DatabaseView>,
}

/// State fixed to the tenant-local access deployment.
#[derive(Clone)]
struct DatabaseApi {
    service: DatabaseRuntime,
    tenant_id: Arc<str>,
}

/// Mounts the database API for exactly one configured tenant.
pub fn router(service: DatabaseRuntime, tenant_id: String) -> Router {
    Router::new()
        .route("/v1/databases", get(list_databases).post(create_database))
        .route(
            "/v1/databases/{name}",
            get(get_database).delete(delete_database),
        )
        .with_state(DatabaseApi {
            service,
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
async fn delete_database(State(runtime): State<DatabaseApi>, Path(name): Path<String>) -> Response {
    match runtime
        .service
        .delete_database(&runtime.tenant_id, &name)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => database_error(error),
    }
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
        DatabaseServiceError::NotFound { .. } => StatusCode::NOT_FOUND,
        DatabaseServiceError::Secret(_) => StatusCode::FORBIDDEN,
        DatabaseServiceError::Repository(_) => StatusCode::BAD_GATEWAY,
    };
    (
        status,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}
