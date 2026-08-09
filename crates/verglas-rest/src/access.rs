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
    ReplaceSecret, ResolveSecret, Resource, SecretError, SecretKind, SecretService,
};

/// Shared authorization backend mounted by local and standalone servers.
pub type AccessRuntime = Arc<dyn Authorizer>;

/// Shared encrypted-secret service mounted only by the standalone access process.
pub type SecretRuntime = Arc<SecretService>;

/// Tenant-local secret HTTP state owned by one access process.
#[derive(Clone)]
struct SecretHttpRuntime {
    secrets: SecretRuntime,
    tenant_id: Arc<str>,
    creator_principal_id: Arc<str>,
}

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

/// Adds encrypted secret lifecycle and authorized runtime-resolution routes.
pub fn router_with_secrets(
    authorizer: AccessRuntime,
    secrets: SecretRuntime,
    tenant_id: impl Into<Arc<str>>,
    creator_principal_id: impl Into<Arc<str>>,
) -> Router {
    router(authorizer).merge(secret_router(SecretHttpRuntime {
        secrets,
        tenant_id: tenant_id.into(),
        creator_principal_id: creator_principal_id.into(),
    }))
}

/// Builds the secret-specific routes with an isolated state type.
fn secret_router(runtime: SecretHttpRuntime) -> Router {
    Router::new()
        .route("/v1/secrets", post(create_secret).get(list_secrets))
        .route("/v1/access/secrets/resolve", post(resolve_secret))
        .route("/v1/secrets/{id}", get(get_secret).put(replace_secret))
        .with_state(runtime)
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

/// JSON create body whose value is consumed immediately by the encryption boundary.
#[derive(Deserialize)]
struct CreateSecretBody {
    name: String,
    #[serde(rename = "type")]
    kind: SecretKind,
    scope: String,
    value: String,
}

/// JSON replacement body bound to an acting principal.
#[derive(Deserialize)]
struct ReplaceSecretBody {
    principal_id: String,
    value: String,
}

/// JSON resolution body for one provider URI.
#[derive(Debug, Deserialize)]
struct ResolveSecretBody {
    tenant_id: String,
    principal_id: String,
    kind: SecretKind,
    uri: String,
}

/// Authorized material returned only by the explicit resolution endpoint.
#[derive(Debug, Serialize)]
struct ResolvedSecretBody {
    resource_id: String,
    version: u64,
    scope: String,
    value: String,
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

/// Creates one authorization resource and its encrypted first value.
async fn create_secret(
    State(runtime): State<SecretHttpRuntime>,
    Json(body): Json<CreateSecretBody>,
) -> Response {
    secret_result(
        StatusCode::CREATED,
        runtime
            .secrets
            .create(verglas_authz::CreateSecret::new(
                runtime.tenant_id.as_ref(),
                runtime.creator_principal_id.as_ref(),
                body.name,
                body.kind,
                body.scope,
                body.value.as_bytes(),
            ))
            .await,
    )
}

/// Returns one secret's public metadata without loading its value.
async fn get_secret(State(runtime): State<SecretHttpRuntime>, Path(id): Path<String>) -> Response {
    secret_result(
        StatusCode::OK,
        runtime.secrets.get(runtime.tenant_id.as_ref(), &id).await,
    )
}

/// Lists public secret metadata without loading any values.
async fn list_secrets(State(runtime): State<SecretHttpRuntime>) -> Response {
    secret_result(
        StatusCode::OK,
        runtime.secrets.list(runtime.tenant_id.as_ref()).await,
    )
}

/// Replaces a secret's encrypted value while preserving its resource identity.
async fn replace_secret(
    State(runtime): State<SecretHttpRuntime>,
    Path(id): Path<String>,
    Json(body): Json<ReplaceSecretBody>,
) -> Response {
    secret_result(
        StatusCode::OK,
        runtime
            .secrets
            .replace(ReplaceSecret::new(
                runtime.tenant_id.as_ref(),
                body.principal_id,
                id,
                body.value.as_bytes(),
            ))
            .await,
    )
}

/// Resolves and decrypts the longest authorized provider scope for a trusted runtime.
async fn resolve_secret(
    State(runtime): State<SecretHttpRuntime>,
    Json(body): Json<ResolveSecretBody>,
) -> Response {
    if body.tenant_id != runtime.tenant_id.as_ref() {
        return secret_error_response(SecretError::Forbidden(
            "requested tenant does not match this access service".to_owned(),
        ));
    }
    let resolved = runtime
        .secrets
        .resolve(ResolveSecret::new(
            body.tenant_id,
            body.principal_id,
            body.kind,
            body.uri,
        ))
        .await
        .and_then(|resolved| {
            String::from_utf8(resolved.expose().to_vec())
                .map(|value| ResolvedSecretBody {
                    resource_id: resolved.resource_id,
                    version: resolved.version,
                    scope: resolved.scope,
                    value,
                })
                .map_err(|_| SecretError::Backend("secret value is not UTF-8".to_owned()))
        });
    secret_result(StatusCode::OK, resolved)
}

/// Serializes one successful typed result or maps its stable authorization error.
fn result<T: Serialize>(status: StatusCode, value: Result<T, AuthzError>) -> Response {
    match value {
        Ok(value) => (status, Json(value)).into_response(),
        Err(error) => error_response(error),
    }
}

/// Serializes one secret result or maps its bounded failure category.
fn secret_result<T: Serialize>(status: StatusCode, value: Result<T, SecretError>) -> Response {
    match value {
        Ok(value) => (status, Json(value)).into_response(),
        Err(error) => secret_error_response(error),
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

/// Maps secret failures without including values or ciphertext in error bodies.
fn secret_error_response(error: SecretError) -> Response {
    let status = match &error {
        SecretError::Invalid(_) => StatusCode::BAD_REQUEST,
        SecretError::NotFound(_) => StatusCode::NOT_FOUND,
        SecretError::Conflict(_) => StatusCode::CONFLICT,
        SecretError::Forbidden(_) => StatusCode::FORBIDDEN,
        SecretError::Backend(_) => StatusCode::BAD_GATEWAY,
    };
    (
        status,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}
