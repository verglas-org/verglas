//! `verglas-query-node` — the standalone Verglas tenant query server.
//!
//! One job: serve `POST /v1/query` over `verglas-iceberg`'s embedded
//! DataFusion catalog. It reads the tenant catalog and the ring's S3 endpoint
//! the same coordinates a production tenant query machine receives — and
//! nothing else: no cache, no S3 frontend, no cluster/consensus/writeback.
//! Config arrives entirely as environment variables (see [`config`]),
//! production parity with how tenant machines receive their coordinates.

mod config;
mod server;

use std::sync::Arc;

use config::QueryNodeConfig;

/// The server version, from the package manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() {
    let config = match QueryNodeConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("verglas-query-node: {error}");
            std::process::exit(1);
        }
    };

    let connection = config.connection();
    let catalog = match verglas_iceberg::catalog::open_catalog(&connection).await {
        Ok(catalog) => catalog,
        Err(error) => {
            eprintln!(
                "verglas-query-node: failed to open catalog {}: {error}",
                connection.catalog_uri
            );
            std::process::exit(1);
        }
    };

    let listener = match tokio::net::TcpListener::bind(&config.listen_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!(
                "verglas-query-node: failed to bind {}: {error}",
                config.listen_addr
            );
            std::process::exit(1);
        }
    };
    let local_addr = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| config.listen_addr.clone());

    let state = server::AppState::new(
        Arc::clone(&catalog),
        connection.clone(),
        config.memory_limit_bytes,
        config.query_timeout,
    );
    eprintln!(
        "verglas-query-node {VERSION} serving POST /v1/query on http://{local_addr} (catalog={}, warehouse={:?}, memory_limit_bytes={}, query_timeout_secs={})",
        connection.catalog_uri,
        connection.warehouse,
        config.memory_limit_bytes,
        config.query_timeout.as_secs(),
    );

    if let Err(error) = axum::serve(listener, server::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        eprintln!("verglas-query-node: server error: {error}");
        std::process::exit(1);
    }
}

/// Waits for Ctrl-C before tearing down the listener.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
