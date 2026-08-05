//! Starts the standalone multi-Gadget local or single-Gadget cloud runtime.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use verglas_gadget_runtime::{HostConfig, RuntimeConfig, RuntimeService};

/// Standalone Gadget runtime configuration.
#[derive(Debug, Parser)]
#[command(name = "verglas-gadget-runtime", version)]
struct Args {
    /// Address serving the authenticated registry and Gadget RPC routes.
    #[arg(
        long,
        env = "VERGLAS_GADGET_RUNTIME_LISTEN",
        default_value = "0.0.0.0:8350"
    )]
    listen: SocketAddr,
    /// Bearer token required by every control, client-module, and RPC route.
    #[arg(long, env = "VERGLAS_GADGET_RUNTIME_TOKEN")]
    runtime_token: String,
    /// Hard number of Gadgets one local runtime may register.
    #[arg(long, env = "VERGLAS_GADGET_MAX_GADGETS", default_value_t = 64)]
    max_gadgets: usize,
    /// Optional cloud identity that constrains this runtime to one Gadget.
    #[arg(long, env = "VERGLAS_GADGET_ID")]
    target_gadget: Option<String>,
    /// JavaScript runtime used to start the private Gadget host.
    #[arg(long, env = "VERGLAS_GADGET_HOST_COMMAND", default_value = "bun")]
    host_command: PathBuf,
    /// Cap'n Web host module executed once per selected Gadget.
    #[arg(
        long,
        env = "VERGLAS_GADGET_HOST_SCRIPT",
        default_value = "/opt/verglas-gadget-runtime/host.mjs"
    )]
    host_script: PathBuf,
    /// Maximum seconds for a child host to bind its private listener.
    #[arg(long, env = "VERGLAS_GADGET_STARTUP_SECS", default_value_t = 15)]
    startup_seconds: u64,
    /// Verglas REST base used by deployment-scoped Gadget KV storage.
    #[arg(long, env = "VERGLAS_GADGET_KV_ENDPOINT")]
    kv_endpoint: Option<String>,
    /// Scoped token used only by the child host's captured KV transport.
    #[arg(long, env = "VERGLAS_GADGET_KV_TOKEN")]
    kv_token: Option<String>,
}

/// Parses configuration, binds the service, and runs until process shutdown.
async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()?;

    let config = match args.target_gadget {
        Some(target) => RuntimeConfig::single(target),
        None => RuntimeConfig::local(args.max_gadgets),
    };
    let mut environment = BTreeMap::new();
    match (args.kv_endpoint, args.kv_token) {
        (Some(endpoint), Some(token)) => {
            environment.insert("VERGLAS_GADGET_KV_ENDPOINT".to_owned(), endpoint);
            environment.insert("VERGLAS_GADGET_KV_TOKEN".to_owned(), token);
        }
        (None, None) => {}
        _ => {
            return Err(
                "VERGLAS_GADGET_KV_ENDPOINT and VERGLAS_GADGET_KV_TOKEN must be set together"
                    .into(),
            );
        }
    }
    let host = HostConfig {
        command: args.host_command,
        arguments: vec![args.host_script.to_string_lossy().into_owned()],
        startup_timeout: Duration::from_secs(args.startup_seconds),
        environment,
    };
    let app = RuntimeService::with_host(config, args.runtime_token, host)?.router();
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(address = %args.listen, "Verglas Gadget runtime listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Resolves when the process receives its supported termination signal.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        match terminate {
            Ok(mut terminate) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "failed to listen for interrupt signal");
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to listen for termination signal");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Reports startup failure without exposing configuration values.
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("verglas-gadget-runtime: {error}");
        std::process::exit(1);
    }
}
