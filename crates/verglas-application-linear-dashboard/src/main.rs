//! Runs the Linear dashboard Application Vessel.

use std::net::SocketAddr;

use clap::Parser;
use tokio::net::TcpListener;

/// Linear dashboard server configuration.
#[derive(Debug, Parser)]
#[command(name = "verglas-application-linear-dashboard", version)]
struct Args {
    /// HTTP address listened to inside the Application container.
    #[arg(
        long,
        env = "VERGLAS_APPLICATION_LISTEN",
        default_value = "0.0.0.0:8372"
    )]
    listen: SocketAddr,
    /// Private-network origin of the Linear integration Vessel.
    #[arg(
        long,
        env = "VERGLAS_LINEAR_INTEGRATION_URL",
        default_value = "http://verglas-vessel-linear:8371"
    )]
    integration_url: String,
}

/// Starts the full-stack dashboard server.
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("verglas-application-linear-dashboard: {error}");
        std::process::exit(1);
    }
}

/// Performs fallible listener setup and serving.
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let listener = TcpListener::bind(args.listen).await?;
    axum::serve(
        listener,
        verglas_application_linear_dashboard::router(args.integration_url),
    )
    .await?;
    Ok(())
}
