//! Runs the standalone Linear integration Vessel.

use std::net::SocketAddr;

use clap::Parser;
use tokio::net::TcpListener;

/// Linear integration server configuration.
#[derive(Debug, Parser)]
#[command(name = "verglas-integration-linear", version)]
struct Args {
    /// HTTP address listened to inside the Vessel container.
    #[arg(
        long,
        env = "VERGLAS_INTEGRATION_LISTEN",
        default_value = "0.0.0.0:8371"
    )]
    listen: SocketAddr,
}

/// Starts the Linear integration HTTP server.
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("verglas-integration-linear: {error}");
        std::process::exit(1);
    }
}

/// Performs fallible listener setup and serving.
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let listener = TcpListener::bind(args.listen).await?;
    axum::serve(listener, verglas_integration_linear::router()).await?;
    Ok(())
}
