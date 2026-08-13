//! Pools workload traffic across every endpoint in one dedicated cache ring.

use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, Response, StatusCode};
use axum::routing::any;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

/// A stable object-to-ingress assignment over every member of one cache ring.
///
/// Rendezvous hashing keeps every operation for an object on one ingress while
/// avoiding the single-node write concentration caused by a fixed S3 endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointPool {
    endpoints: Vec<String>,
}

/// Why a cache-ring endpoint pool cannot be constructed.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EndpointPoolError {
    /// A workload must be assigned at least one cache-ring endpoint.
    #[error("the endpoint pool is empty")]
    Empty,
    /// Each ring member must appear exactly once in the pool.
    #[error("duplicate endpoint in pool: {0}")]
    Duplicate(String),
}

impl EndpointPool {
    /// Builds a pool from the complete endpoint set assigned to one workload.
    pub fn new<I, S>(endpoints: I) -> Result<Self, EndpointPoolError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let endpoints = endpoints.into_iter().map(Into::into).collect::<Vec<_>>();
        if endpoints.is_empty() {
            return Err(EndpointPoolError::Empty);
        }

        let mut unique = HashSet::with_capacity(endpoints.len());
        for endpoint in &endpoints {
            if !unique.insert(endpoint.as_str()) {
                return Err(EndpointPoolError::Duplicate(endpoint.clone()));
            }
        }

        Ok(Self { endpoints })
    }

    /// Selects the stable ingress for an object path, ignoring operation query parameters.
    pub fn endpoint_for_path(&self, path_and_query: &str) -> &str {
        &self.endpoints[self.index_for_path(path_and_query)]
    }

    /// Selects the stable member index for an object path.
    pub fn index_for_path(&self, path_and_query: &str) -> usize {
        let object_path = path_and_query
            .split_once('?')
            .map_or(path_and_query, |(path, _)| path);
        self.endpoints
            .iter()
            .enumerate()
            .max_by_key(|(_, endpoint)| rendezvous_score(object_path, endpoint))
            .map_or(0, |(index, _)| index)
    }
}

/// Computes the rendezvous score for one object and candidate ring ingress.
fn rendezvous_score(object_path: &str, endpoint: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(object_path.as_bytes());
    hasher.update([0]);
    hasher.update(endpoint.as_bytes());
    hasher.finalize().into()
}

/// Shared state for the S3-compatible ring gateway.
#[derive(Clone)]
struct S3Gateway {
    /// Stable object placement over the complete workload ring.
    pool: EndpointPool,
    /// One HTTP client whose per-origin pools retain connections to all members.
    client: reqwest::Client,
}

/// Builds an S3-compatible router that forwards each object to its ring ingress.
///
/// The URI path, query, method, headers, and streaming body are preserved. Since
/// endpoint selection excludes the query string, create/upload/complete calls
/// for one multipart object cannot split across cache nodes.
pub fn s3_router(pool: EndpointPool) -> Router {
    Router::new()
        .fallback(any(forward_s3_request))
        .with_state(Arc::new(S3Gateway {
            pool,
            client: reqwest::Client::new(),
        }))
}

/// Streams one S3 request to its selected ring member and streams the response back.
async fn forward_s3_request(
    State(state): State<Arc<S3Gateway>>,
    request: Request<Body>,
) -> Response<Body> {
    match forward_s3_request_inner(&state, request).await {
        Ok(response) => response,
        Err(error) => Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Body::from(error.to_string()))
            .unwrap_or_else(|_| Response::new(Body::empty())),
    }
}

/// Performs the fallible portion of S3 request forwarding.
async fn forward_s3_request_inner(
    state: &S3Gateway,
    request: Request<Body>,
) -> Result<Response<Body>, GatewayError> {
    let (parts, body) = request.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or(parts.uri.path(), |value| value.as_str());
    let endpoint = state.pool.endpoint_for_path(path_and_query);
    let target = format!("{}{}", endpoint.trim_end_matches('/'), path_and_query);
    let upstream = state
        .client
        .request(parts.method, target)
        .headers(parts.headers)
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .send()
        .await?;
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let mut response = Response::new(Body::from_stream(upstream.bytes_stream()));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(response)
}

/// Why the local ring gateway could not complete an upstream request.
#[derive(Debug, Error)]
enum GatewayError {
    /// The selected cache ingress could not complete the HTTP exchange.
    #[error("cache-ring S3 request failed: {0}")]
    Upstream(#[from] reqwest::Error),
}

/// Maximum time spent proving one private cache endpoint reachable before the
/// next ring member is tried. Stable Fly private addresses normally connect in
/// under a millisecond; this bound keeps one stopped member off the cold path.
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_millis(50);

/// Rotating endpoint state for one logical safekeeper transport.
struct TcpEndpointPool {
    /// Every safekeeper ingress assigned to the database timeline.
    endpoints: Arc<Vec<String>>,
    /// Starting member for the next client session.
    next: AtomicUsize,
}

impl TcpEndpointPool {
    /// Builds a non-empty pool of safekeeper socket addresses.
    fn new(endpoints: Vec<String>) -> io::Result<Self> {
        if endpoints.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the safekeeper endpoint pool is empty",
            ));
        }
        let unique = endpoints.iter().collect::<HashSet<_>>();
        if unique.len() != endpoints.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the safekeeper endpoint pool contains a duplicate",
            ));
        }
        Ok(Self {
            endpoints: Arc::new(endpoints),
            next: AtomicUsize::new(0),
        })
    }

    /// Connects one client session to one active ingress, rotating its first choice.
    async fn connect(&self) -> io::Result<tokio::net::TcpStream> {
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.endpoints.len();
        let mut last_error = None;
        for offset in 0..self.endpoints.len() {
            let endpoint = &self.endpoints[(start + offset) % self.endpoints.len()];
            match tokio::time::timeout(
                TCP_CONNECT_TIMEOUT,
                tokio::net::TcpStream::connect(endpoint),
            )
            .await
            {
                Ok(Ok(stream)) => return Ok(stream),
                Ok(Err(error)) => last_error = Some(error),
                Err(_) => {
                    last_error = Some(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("safekeeper connect to {endpoint} timed out"),
                    ));
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "no safekeeper endpoint is reachable",
            )
        }))
    }
}

/// Serves one logical Neon safekeeper address over all assigned cache endpoints.
///
/// Each Neon TCP session is attached to exactly one ingress, so endpoint count
/// never becomes an additional WAL durability quorum. New and reconnected
/// sessions rotate their first choice and skip endpoints that do not connect
/// within the private-network deadline.
pub async fn serve_tcp_pool(
    listener: tokio::net::TcpListener,
    endpoints: Vec<String>,
) -> io::Result<()> {
    let pool = Arc::new(TcpEndpointPool::new(endpoints)?);
    loop {
        let (mut client, _) = listener.accept().await?;
        let pool = Arc::clone(&pool);
        tokio::spawn(async move {
            let Ok(mut upstream) = pool.connect().await else {
                let _ = client.shutdown().await;
                return;
            };
            let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            let _ = client.shutdown().await;
            let _ = upstream.shutdown().await;
        });
    }
}
