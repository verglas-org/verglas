//! Starts one queue container over its dedicated managed Neon database.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tokio::net::TcpListener;
use verglas_queue::PgQueue;

/// Queue-container process configuration supplied only by the provisioner.
#[derive(Debug, Parser)]
#[command(name = "verglas-queue-service", version)]
struct Args {
    /// Dedicated queue database connection string.
    #[arg(long, env = "VERGLAS_QUEUE_DATABASE_URL", hide_env_values = true)]
    database_url: String,
    /// Private bearer shared with the tenant queue gateway.
    #[arg(long, env = "VERGLAS_QUEUE_TOKEN", hide_env_values = true)]
    token: String,
    /// Private listener reached through the container network.
    #[arg(long, env = "VERGLAS_QUEUE_LISTEN", default_value = "0.0.0.0:8370")]
    listen: SocketAddr,
}

/// Connects durable storage before reporting HTTP readiness.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let store = Arc::new(PgQueue::connect(&args.database_url).await?);
    let listener = TcpListener::bind(args.listen).await?;
    axum::serve(listener, verglas_queue_service::router(store, args.token)).await?;
    Ok(())
}
