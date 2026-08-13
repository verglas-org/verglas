//! Runs the workload-local S3 gateway over every endpoint in one Verglas cache ring.

use std::net::SocketAddr;

use verglas_ring_proxy::{EndpointPool, s3_router};

/// Loads the mandatory ring coordinates and serves the S3-compatible gateway.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw_endpoints = std::env::var("VERGLAS_RING_S3_ENDPOINTS")?;
    let endpoints = raw_endpoints
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let pool = EndpointPool::new(endpoints)?;
    let listen = std::env::var("VERGLAS_RING_S3_LISTEN")
        .unwrap_or_else(|_| "[::]:8333".to_owned())
        .parse::<SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, s3_router(pool)).await?;
    Ok(())
}
