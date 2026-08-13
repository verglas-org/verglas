//! Runs the Verglas-owned, CRaft-backed Iceberg catalog service.

#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use lakekeeper::{CONFIG, tokio};
use tracing_subscriber::{EnvFilter, filter::LevelFilter};

mod serve;

/// Command-line configuration for the catalog service.
#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// Supported catalog operations.
#[derive(Debug, Subcommand)]
enum Commands {
    /// Serve one `CRaft`-backed Iceberg warehouse without `PostgreSQL`.
    ServeCraft {
        /// Ordered, comma-separated Verglas catalog ingress URLs.
        #[clap(long, env = "VERGLAS_CATALOG_ENDPOINTS", value_delimiter = ',')]
        endpoints: Vec<String>,
        /// Tenant that owns the `CRaft` catalog groups.
        #[clap(long, env = "VERGLAS_CATALOG_TENANT")]
        tenant: String,
        /// Hosted warehouse group name.
        #[clap(long, env = "VERGLAS_CATALOG_WAREHOUSE")]
        warehouse: String,
        /// Lakekeeper S3 storage profile JSON using the AWS credential chain.
        #[clap(long, env = "VERGLAS_METADATA_S3_PROFILE")]
        metadata_s3_profile: String,
    },
}

/// Parses configuration and runs the requested catalog operation.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(true)
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    match Cli::parse().command {
        Commands::ServeCraft {
            endpoints,
            tenant,
            warehouse,
            metadata_s3_profile,
        } => {
            serve::serve_craft(
                std::net::SocketAddr::from((CONFIG.bind_ip, CONFIG.listen_port)),
                endpoints,
                tenant,
                warehouse,
                metadata_s3_profile,
            )
            .await
        }
    }
}
