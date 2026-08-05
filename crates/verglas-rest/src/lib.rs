//! On-prem HTTP composition for independently reusable Verglas services.
//! Cloud roles use the underlying crates directly and do not depend on this
//! deployment-specific router.

use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::any};
use bytes::Bytes;
use verglas_catalog::CatalogGateway;

pub mod admin;
pub mod dashboard;
pub mod follow;
pub mod kv;
pub mod logging;
pub mod platform;
pub mod query_worker;
pub mod write_worker;

/// REST service version reported by the admin API.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Builds the S3 surface, including its optional execution API route.
pub use verglas_s3::router_with_passthrough as compose_s3;

/// Mounts the shallow catalog proxy at `/catalog` onto an existing query API.
pub fn compose_query_and_catalog(query: Router, catalog: CatalogGateway) -> Router {
    query.merge(
        Router::new()
            .route("/catalog/{*path}", any(catalog_request))
            .with_state(catalog),
    )
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
