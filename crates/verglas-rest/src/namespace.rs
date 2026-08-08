//! Reflected Integration namespace gateway for the local Verglas endpoint.

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures::StreamExt;
use thiserror::Error;

/// Private connection from the Verglas server to its container runtime manager.
#[derive(Clone)]
pub struct NamespaceGateway {
    endpoint: reqwest::Url,
    token: String,
    client: reqwest::Client,
}

impl NamespaceGateway {
    /// Validates and stores one private runtime-manager endpoint and credential.
    pub fn new(
        endpoint: impl AsRef<str>,
        token: impl Into<String>,
    ) -> Result<Self, NamespaceGatewayError> {
        let endpoint = reqwest::Url::parse(endpoint.as_ref())
            .map_err(|error| NamespaceGatewayError::Endpoint(error.to_string()))?;
        let token = token.into();
        if token.is_empty() {
            return Err(NamespaceGatewayError::MissingToken);
        }
        Ok(Self {
            endpoint,
            token,
            client: reqwest::Client::new(),
        })
    }

    /// Builds a runtime-manager URL from encoded path segments.
    fn url(&self, segments: &[&str]) -> Result<reqwest::Url, NamespaceGatewayError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| NamespaceGatewayError::InvalidEndpoint)?
            .clear()
            .extend(segments);
        Ok(url)
    }
}

/// Namespace gateway configuration and upstream transport failures.
#[derive(Debug, Error)]
pub enum NamespaceGatewayError {
    /// The runtime-manager endpoint was not a valid URL.
    #[error("invalid namespace runtime endpoint: {0}")]
    Endpoint(String),
    /// The configured URL cannot be used as a hierarchical base URL.
    #[error("namespace runtime endpoint cannot be a base URL")]
    InvalidEndpoint,
    /// The private runtime-manager bearer token was absent.
    #[error("namespace runtime token is required")]
    MissingToken,
    /// The runtime manager could not be reached or stopped streaming.
    #[error("namespace runtime request failed: {0}")]
    Transport(#[from] reqwest::Error),
}

impl IntoResponse for NamespaceGatewayError {
    /// Converts configuration and upstream failures to bounded gateway responses.
    fn into_response(self) -> Response {
        let status = match self {
            Self::Endpoint(_) | Self::InvalidEndpoint | Self::MissingToken => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::Transport(_) => StatusCode::BAD_GATEWAY,
        };
        (status, self.to_string()).into_response()
    }
}

/// Builds the reflected namespace routes mounted on the primary Verglas endpoint.
pub fn router(gateway: NamespaceGateway) -> Router {
    Router::new()
        .route("/v1/namespaces", get(list))
        .route("/v1/namespaces/{namespace}", get(show))
        .route("/v1/namespaces/{namespace}/invoke/{method}", post(invoke))
        .with_state(gateway)
}

/// Relays the visible Integration manifest collection.
async fn list(State(gateway): State<NamespaceGateway>) -> Result<Response, NamespaceGatewayError> {
    relay(
        &gateway,
        Method::GET,
        &["v1", "namespaces"],
        &HeaderMap::new(),
        Bytes::new(),
    )
    .await
}

/// Relays one reflected Integration manifest.
async fn show(
    State(gateway): State<NamespaceGateway>,
    Path(namespace): Path<String>,
) -> Result<Response, NamespaceGatewayError> {
    relay(
        &gateway,
        Method::GET,
        &["v1", "namespaces", &namespace],
        &HeaderMap::new(),
        Bytes::new(),
    )
    .await
}

/// Relays one bounded or streaming Integration API invocation.
async fn invoke(
    State(gateway): State<NamespaceGateway>,
    Path((namespace, method)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, NamespaceGatewayError> {
    relay(
        &gateway,
        Method::POST,
        &["v1", "namespaces", &namespace, "invoke", &method],
        &headers,
        body,
    )
    .await
}

/// Sends one private authenticated request and streams its response unchanged.
async fn relay(
    gateway: &NamespaceGateway,
    method: Method,
    segments: &[&str],
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response, NamespaceGatewayError> {
    let mut request = gateway
        .client
        .request(method, gateway.url(segments)?)
        .bearer_auth(&gateway.token)
        .body(body);
    for name in [header::CONTENT_TYPE, header::ACCEPT] {
        if let Some(value) = headers.get(&name) {
            request = request.header(name, value);
        }
    }
    let upstream = request.send().await?;
    let status = upstream.status();
    let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let stream = upstream
        .bytes_stream()
        .map(|chunk| chunk.map_err(std::io::Error::other));
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    Ok(response)
}
