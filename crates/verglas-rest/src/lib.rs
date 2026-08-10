//! Self-hosted HTTP composition for independently reusable Verglas services.
//! This router is the deployment-specific surface that mounts cache, admin,
//! catalog proxy, and worker APIs in one process.

use axum::extract::{Extension, OriginalUri, Path, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{
    Json, Router,
    routing::{any, get},
};
use bytes::Bytes;
use serde::Deserialize;
use verglas_catalog::{CatalogGateway, CatalogRuntimeRegistry, DatabaseId};

use crate::data_plane::AuthenticatedBearer;

pub mod access;
pub mod admin;
pub mod dashboard;
pub mod data_plane;
pub mod database;
pub mod follow;
pub mod kv;
pub mod logging;
pub mod namespace;
pub mod platform;
pub mod query_worker;
pub mod queue;
pub mod write_worker;

/// REST service version reported by the admin API.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Builds the S3 surface, including its optional execution API route.
pub use verglas_s3::router_with_passthrough as compose_s3;

/// Mounts the shallow catalog proxy at `/catalog` onto an existing query API.
pub fn compose_query_and_catalog(query: Router, catalog: CatalogGateway) -> Router {
    query.merge(
        Router::new()
            .route("/catalog/_verglas/generation", get(catalog_generation))
            .route("/catalog/{*path}", any(catalog_request))
            .with_state(catalog),
    )
}

/// Mounts database-scoped catalog routes backed by a dynamic tenant registry.
pub fn compose_database_catalogs(query: Router, catalogs: CatalogRuntimeRegistry) -> Router {
    query.merge(
        Router::new()
            .route(
                "/v1/databases/{database}/catalog/_verglas/generation",
                get(database_catalog_generation),
            )
            .route(
                "/v1/databases/{database}/catalog/{*path}",
                any(database_catalog_request),
            )
            .with_state(catalogs),
    )
}

/// Route parameters for a database-scoped catalog request.
#[derive(Debug, Deserialize)]
struct DatabaseCatalogPath {
    database: String,
    #[allow(dead_code)]
    path: String,
}

/// Returns one database catalog's prepared-response generation.
async fn database_catalog_generation(
    State(catalogs): State<CatalogRuntimeRegistry>,
    Path(database): Path<String>,
) -> Response {
    let Ok(database) = DatabaseId::new(database) else {
        return (StatusCode::BAD_REQUEST, "invalid database id").into_response();
    };
    let Some(catalog) = catalogs.get(&database) else {
        return (StatusCode::NOT_FOUND, "database catalog not found").into_response();
    };
    Json(serde_json::json!({ "generation": catalog.generation() })).into_response()
}

/// Routes one database's Iceberg REST request to its bound live gateway.
async fn database_catalog_request(
    State(catalogs): State<CatalogRuntimeRegistry>,
    Path(path): Path<DatabaseCatalogPath>,
    verified_bearer: Option<Extension<AuthenticatedBearer>>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(Extension(verified_bearer)) = verified_bearer else {
        return (StatusCode::UNAUTHORIZED, "catalog authentication required").into_response();
    };
    let database = path.database;
    let Ok(database_id) = DatabaseId::new(database.clone()) else {
        return (StatusCode::BAD_REQUEST, "invalid database id").into_response();
    };
    let Some(catalog) = catalogs.get(&database_id) else {
        return (StatusCode::NOT_FOUND, "database catalog not found").into_response();
    };
    let Some(path_and_query) = uri.path_and_query().map(|value| value.as_str()) else {
        return (StatusCode::BAD_REQUEST, "catalog request has no path").into_response();
    };
    let mount = format!("/v1/databases/{database}/catalog");
    let Some(upstream_path) = path_and_query.strip_prefix(&mount) else {
        return (
            StatusCode::BAD_REQUEST,
            "catalog request is outside its database mount",
        )
            .into_response();
    };
    authenticated_catalog_response(
        catalog,
        method,
        upstream_path,
        headers,
        body,
        verified_bearer,
    )
    .await
}

/// Cache-owned catalog generation for query-worker session fencing.
async fn catalog_generation(State(catalog): State<CatalogGateway>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "generation": catalog.generation() }))
}

/// Forwards one mounted catalog request while preserving status and headers.
async fn catalog_request(
    State(catalog): State<CatalogGateway>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(path_and_query) = uri.path_and_query().map(|value| value.as_str()) else {
        return (StatusCode::BAD_REQUEST, "catalog request has no path").into_response();
    };
    let Some(upstream_path) = path_and_query.strip_prefix("/catalog") else {
        return (
            StatusCode::BAD_REQUEST,
            "catalog request is outside its mount",
        )
            .into_response();
    };
    catalog_response(catalog, method, upstream_path, headers, body).await
}

/// Converts a shallow gateway result into an Axum response.
async fn catalog_response(
    catalog: CatalogGateway,
    method: Method,
    upstream_path: &str,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match catalog.request(method, upstream_path, headers, body).await {
        Ok(result) => {
            let status = StatusCode::from_u16(result.status).unwrap_or(StatusCode::BAD_GATEWAY);
            let mut response = Response::new(axum::body::Body::from(result.body));
            *response.status_mut() = status;
            *response.headers_mut() = result.headers;
            response
        }
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            format!("catalog gateway error: {error}"),
        )
            .into_response(),
    }
}

/// Converts an authenticated shallow gateway result into an Axum response.
async fn authenticated_catalog_response(
    catalog: CatalogGateway,
    method: Method,
    upstream_path: &str,
    headers: HeaderMap,
    body: Bytes,
    verified_bearer: AuthenticatedBearer,
) -> Response {
    match catalog
        .authenticated_request(
            method,
            upstream_path,
            headers,
            body,
            verified_bearer.header_value().clone(),
        )
        .await
    {
        Ok(result) => {
            let status = StatusCode::from_u16(result.status).unwrap_or(StatusCode::BAD_GATEWAY);
            let mut response = Response::new(axum::body::Body::from(result.body));
            *response.status_mut() = status;
            *response.headers_mut() = result.headers;
            response
        }
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            format!("catalog gateway error: {error}"),
        )
            .into_response(),
    }
}
