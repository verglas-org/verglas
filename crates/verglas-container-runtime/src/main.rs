//! Runs the trusted open-source Docker container runtime manager.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use tokio::net::TcpListener;
use verglas_container_runtime::{DockerRuntime, RuntimeService, ensure_local_postgres_tls};

/// Local Docker Engine runtime manager configuration.
#[derive(Debug, Parser)]
#[command(name = "verglas-container-runtime", version)]
struct Args {
    /// HTTP address used by the local Verglas control plane.
    #[arg(
        long,
        env = "VERGLAS_CONTAINER_RUNTIME_LISTEN",
        default_value = "127.0.0.1:8360"
    )]
    listen: SocketAddr,
    /// Required bearer token protecting Docker lifecycle operations.
    #[arg(long, env = "VERGLAS_CONTAINER_RUNTIME_TOKEN")]
    token: String,
    /// Persistent desired-state document restored after manager restart.
    #[arg(
        long,
        env = "VERGLAS_CONTAINER_RUNTIME_STATE",
        default_value = "/var/lib/verglas-container-runtime/deployments.json"
    )]
    state: PathBuf,
    /// Existing Docker network attached to managed local containers by default.
    #[arg(
        long,
        env = "VERGLAS_CONTAINER_RUNTIME_NETWORK",
        default_value = "verglas-runtime"
    )]
    network: String,
}

/// Connects to Docker, restores desired state, and serves the local API.
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("verglas-container-runtime: {error}");
        std::process::exit(1);
    }
}

/// Performs fallible runtime manager startup.
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    ensure_local_postgres_tls(&args.state)?;
    let runtime = DockerRuntime::connect_local()?;
    let service = RuntimeService::open(runtime, args.token, args.state, args.network).await?;
    service.recover().await?;
    let listener = TcpListener::bind(args.listen).await?;
    service.serve(listener).await?;
    Ok(())
}
