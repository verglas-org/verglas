//! Runs one logical Neon safekeeper address over every cache endpoint assigned to a database.

use std::net::SocketAddr;

use verglas_ring_proxy::serve_tcp_pool;

/// Loads the mandatory safekeeper ring and serves raw PostgreSQL protocol sessions.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoints = std::env::var("VERGLAS_RING_SAFEKEEPER_ENDPOINTS")?
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let listen = std::env::var("VERGLAS_RING_SAFEKEEPER_LISTEN")
        .unwrap_or_else(|_| "[::]:5454".to_owned())
        .parse::<SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(listen).await?;
    serve_tcp_pool(listener, endpoints).await?;
    Ok(())
}
