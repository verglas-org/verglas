//! Tenant-local bridge from the public access listener to the cache data plane.
//!
//! The caller's bearer has already been checked by [`verglas_rest::data_plane::protect`]
//! before these handlers run. The bridge preserves that bearer so the cache listener
//! independently validates the same principal and current policy before serving data.

use axum::Router;
use axum::body::Body;
use axum::extract::{Extension, Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use reqwest::Url;
use verglas_rest::data_plane::AuthenticatedDatabaseId;

#[derive(Clone)]
struct DataPlaneProxy {
    endpoint: Url,
    http: reqwest::Client,
}

impl DataPlaneProxy {
    fn new(endpoint: &str) -> Result<Self, String> {
        let mut endpoint = Url::parse(endpoint)
            .map_err(|error| format!("invalid data-plane endpoint: {error}"))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err("data-plane endpoint must use http or https".to_owned());
        }
        endpoint.set_path("/");
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        Ok(Self {
            endpoint,
            http: reqwest::Client::new(),
        })
    }

    fn target(&self, request: &Request) -> Result<Url, String> {
        let path_and_query = request
            .uri()
            .path_and_query()
            .map_or("/", |value| value.as_str());
        let path = request.uri().path();
        let segments = path.trim_matches('/').split('/').collect::<Vec<_>>();
        if let ["v1", "databases", _, "catalog", rest @ ..] = segments.as_slice() {
            let suffix = rest.join("/");
            let mut target = if suffix.is_empty() {
                self.endpoint.join("catalog")
            } else {
                self.endpoint.join(&format!("catalog/{suffix}"))
            }
            .map_err(|error| format!("invalid catalog request target: {error}"))?;
            target.set_query(request.uri().query());
            return Ok(target);
        }
        self.endpoint
            .join(path_and_query)
            .map_err(|error| format!("invalid data-plane request target: {error}"))
    }
}

/// Routes whose authorization question is defined by the shared data-plane policy.
pub(crate) fn router(endpoint: &str) -> Result<Router, String> {
    let proxy = DataPlaneProxy::new(endpoint)?;
    Ok(Router::new()
        .route("/v1/databases/{database}/query", any(relay))
        .route("/v1/databases/{database}/catalog/{*path}", any(relay))
        .route("/v1/databases/{database}/tables", any(relay))
        .route("/v1/databases/{database}/tables/{*path}", any(relay))
        .route("/v1/databases/{database}/write/{name}", any(relay))
        .route("/v1/databases/{database}/ingest/{name}", any(relay))
        .route("/v1/databases/{database}/graphs/{*path}", any(relay))
        .with_state(proxy))
}

async fn relay(
    State(proxy): State<DataPlaneProxy>,
    database_id: Option<Extension<AuthenticatedDatabaseId>>,
    request: Request,
) -> Result<Response, Response> {
    let target = proxy.target(&request).map_err(internal_error)?;
    let is_catalog = request.uri().path().contains("/catalog/");
    let method = request.method().clone();
    let headers = request.headers().clone();
    let body = axum::body::to_bytes(request.into_body(), 64 * 1024 * 1024)
        .await
        .map_err(|error| internal_error(format!("could not read data-plane request: {error}")))?;
    let mut upstream = proxy.http.request(method, target).body(body);
    for name in [
        header::AUTHORIZATION,
        header::ACCEPT,
        header::CONTENT_TYPE,
        header::CONTENT_ENCODING,
        header::IF_MATCH,
        header::IF_NONE_MATCH,
    ] {
        if let Some(value) = headers.get(&name) {
            upstream = upstream.header(name, value);
        }
    }
    if is_catalog {
        let Some(Extension(database_id)) = database_id else {
            return Err(internal_error("catalog database identity was not resolved"));
        };
        upstream = upstream.header("x-verglas-database-id", database_id.as_str());
    }
    let upstream = upstream
        .send()
        .await
        .map_err(|error| internal_error(format!("tenant data plane is unavailable: {error}")))?;
    let status = upstream.status();
    let response_headers = upstream.headers().clone();
    let mut response = Response::new(Body::from_stream(upstream.bytes_stream()));
    *response.status_mut() = status;
    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_ENCODING,
        header::ETAG,
        header::CACHE_CONTROL,
        header::LOCATION,
    ] {
        if let Some(value) = response_headers.get(&name) {
            response.headers_mut().insert(name, value.clone());
        }
    }
    Ok(response)
}

fn internal_error(error: impl std::fmt::Display) -> Response {
    (StatusCode::BAD_GATEWAY, format!("{error}\n")).into_response()
}

#[cfg(test)]
mod tests {
    use super::DataPlaneProxy;
    use axum::body::Body;
    use axum::http::Request;

    #[test]
    fn preserves_database_path_and_query_on_fixed_origin() {
        let proxy = DataPlaneProxy::new("http://10.42.0.10:8334").expect("proxy");
        let request = Request::builder()
            .uri("/v1/databases/default/catalog/v1/config?warehouse=default")
            .body(Body::empty())
            .expect("request");
        assert_eq!(
            proxy.target(&request).expect("target").as_str(),
            "http://10.42.0.10:8334/catalog/v1/config?warehouse=default",
        );
    }

    #[test]
    fn rejects_non_http_data_plane_origins() {
        assert!(DataPlaneProxy::new("file:///tmp/cache").is_err());
    }
}
