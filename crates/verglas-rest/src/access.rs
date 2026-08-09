//! HTTP administration and decision surface for universal Verglas authorization.
//!
//! The router depends only on the backend-neutral [`Authorizer`] contract, so
//! the local composition and a standalone cloud microVM serve identical bytes.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use verglas_authz::{
    AccessCheck, Authorizer, AuthzError, Grant, GrantDelegation, GrantRevocation, Principal,
    Resource,
};

/// Shared authorization backend mounted by local and standalone servers.
pub type AccessRuntime = Arc<dyn Authorizer>;

/// Builds the complete access administration and decision API.
pub fn router(authorizer: AccessRuntime) -> Router {
    Router::new()
        .route(
            "/v1/access/principals",
            post(create_principal).get(list_principals),
        )
        .route(
            "/v1/access/principals/{id}",
            get(get_principal).delete(delete_principal),
        )
        .route(
            "/v1/access/resources",
            post(create_resource).get(list_resources),
        )
        .route(
            "/v1/access/resources/{id}",
            get(get_resource).delete(delete_resource),
        )
        .route("/v1/access/grants", post(create_grant).get(list_grants))
        .route("/v1/access/delegations", post(delegate_grant))
        .route("/v1/access/revocations", post(revoke_grant))
        .route("/v1/access/grants/{id}", delete(delete_grant))
        .route("/v1/access/check", post(check_access))
        .with_state(authorizer)
}

/// Tenant selection required by every list, get, and delete operation.
#[derive(Debug, Deserialize)]
struct TenantQuery {
    tenant_id: String,
}

/// Stable empty success response for idempotent-looking HTTP clients.
#[derive(Debug, Serialize)]
struct Deleted {
    deleted: bool,
}

/// Creates one principal.
async fn create_principal(
    State(authorizer): State<AccessRuntime>,
    Json(principal): Json<Principal>,
) -> Response {
    result(
        StatusCode::CREATED,
        authorizer.create_principal(principal).await,
    )
}

/// Returns one tenant-scoped principal.
async fn get_principal(
    State(authorizer): State<AccessRuntime>,
    Query(query): Query<TenantQuery>,
    Path(id): Path<String>,
) -> Response {
    result(
        StatusCode::OK,
        authorizer.get_principal(&query.tenant_id, &id).await,
    )
}

/// Lists principals in one tenant.
async fn list_principals(
    State(authorizer): State<AccessRuntime>,
    Query(query): Query<TenantQuery>,
) -> Response {
    result(
        StatusCode::OK,
        authorizer.list_principals(&query.tenant_id).await,
    )
}

/// Deletes one principal and its grants.
async fn delete_principal(
    State(authorizer): State<AccessRuntime>,
    Query(query): Query<TenantQuery>,
    Path(id): Path<String>,
) -> Response {
    deleted(authorizer.delete_principal(&query.tenant_id, &id).await)
}

/// Creates one resource.
async fn create_resource(
    State(authorizer): State<AccessRuntime>,
    Json(resource): Json<Resource>,
) -> Response {
    result(
        StatusCode::CREATED,
        authorizer.create_resource(resource).await,
    )
}

/// Returns one tenant-scoped resource.
async fn get_resource(
    State(authorizer): State<AccessRuntime>,
    Query(query): Query<TenantQuery>,
    Path(id): Path<String>,
) -> Response {
    result(
        StatusCode::OK,
        authorizer.get_resource(&query.tenant_id, &id).await,
    )
}

/// Lists resources in one tenant.
async fn list_resources(
    State(authorizer): State<AccessRuntime>,
    Query(query): Query<TenantQuery>,
) -> Response {
    result(
        StatusCode::OK,
        authorizer.list_resources(&query.tenant_id).await,
    )
}

/// Deletes one childless resource and its originating grants.
async fn delete_resource(
    State(authorizer): State<AccessRuntime>,
    Query(query): Query<TenantQuery>,
    Path(id): Path<String>,
) -> Response {
    deleted(authorizer.delete_resource(&query.tenant_id, &id).await)
}

/// Creates one explicit grant.
async fn create_grant(
    State(authorizer): State<AccessRuntime>,
    Json(grant): Json<Grant>,
) -> Response {
    result(StatusCode::CREATED, authorizer.create_grant(grant).await)
}

/// Creates a grant under an existing principal's bounded delegation authority.
async fn delegate_grant(
    State(authorizer): State<AccessRuntime>,
    Json(delegation): Json<GrantDelegation>,
) -> Response {
    result(
        StatusCode::CREATED,
        authorizer.delegate_grant(delegation).await,
    )
}

/// Revokes a grant under the actor's bounded grant-management authority.
async fn revoke_grant(
    State(authorizer): State<AccessRuntime>,
    Json(revocation): Json<GrantRevocation>,
) -> Response {
    deleted(authorizer.revoke_grant(revocation).await)
}

/// Lists grants in one tenant.
async fn list_grants(
    State(authorizer): State<AccessRuntime>,
    Query(query): Query<TenantQuery>,
) -> Response {
    result(
        StatusCode::OK,
        authorizer.list_grants(&query.tenant_id).await,
    )
}

/// Deletes one grant.
async fn delete_grant(
    State(authorizer): State<AccessRuntime>,
    Query(query): Query<TenantQuery>,
    Path(id): Path<String>,
) -> Response {
    deleted(authorizer.delete_grant(&query.tenant_id, &id).await)
}

/// Evaluates one action and returns its explanation.
async fn check_access(
    State(authorizer): State<AccessRuntime>,
    Json(check): Json<AccessCheck>,
) -> Response {
    result(StatusCode::OK, authorizer.check(check).await)
}

/// Serializes one successful typed result or maps its stable authorization error.
fn result<T: Serialize>(status: StatusCode, value: Result<T, AuthzError>) -> Response {
    match value {
        Ok(value) => (status, Json(value)).into_response(),
        Err(error) => error_response(error),
    }
}

/// Serializes a successful delete without inventing a nullable resource body.
fn deleted(value: Result<(), AuthzError>) -> Response {
    result(StatusCode::OK, value.map(|()| Deleted { deleted: true }))
}

/// Maps backend-neutral failures to stable HTTP statuses and bounded JSON.
fn error_response(error: AuthzError) -> Response {
    let status = match &error {
        AuthzError::Invalid(_) | AuthzError::Token(_) => StatusCode::BAD_REQUEST,
        AuthzError::NotFound(_) => StatusCode::NOT_FOUND,
        AuthzError::Conflict(_) => StatusCode::CONFLICT,
        AuthzError::Forbidden(_) => StatusCode::FORBIDDEN,
        AuthzError::Backend(_) => StatusCode::BAD_GATEWAY,
    };
    (
        status,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}
