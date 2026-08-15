//! `verglas` — the Verglas CLI, a PURE CLIENT.
//!
//! Cloud is the default control plane (`https://api.verglas.dev`). Self-hosted
//! OSS servers are selected with `VERGLAS_ENDPOINT`. Workers are Cloud-only.
//! `drain` always targets this machine's loopback admin port.

mod admin_client;
mod backend;
mod cli;
mod commands;
mod credentials;
mod dashboard_spec;
mod output;
mod worker_spec;

use clap::Parser;
use cli::{Cli, Command};

/// Runs the parsed CLI command against the configured admin endpoint.
async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let token = cli.resolved_token()?;
    let credentials_path = cli.resolved_credentials_path()?;
    let endpoint = crate::backend::resolved_endpoint();
    match cli.command {
        Command::Drain(args) => {
            commands::drain::run(
                verglas_core::admin::DEFAULT_ENDPOINT,
                token.as_deref(),
                &args,
                cli.json,
            )
            .await
        }
        Command::Status => commands::status::run(&endpoint, token.as_deref(), cli.json).await,
        Command::Table(command) => commands::table::run(command, token.as_deref(), cli.json).await,
        Command::Dashboard(command) => {
            commands::dashboard::run(command, &endpoint, token.as_deref(), cli.json).await
        }
        Command::Workers(command) => {
            commands::workers::run(command, &endpoint, token.as_deref(), cli.json).await
        }
        Command::Lakehouse(command) => {
            commands::lakehouse::run(command, &cli.access_endpoint, token.as_deref(), cli.json)
                .await
        }
        Command::Secret(command) => {
            commands::secret::run(command, &cli.access_endpoint, token.as_deref(), cli.json).await
        }
        Command::Token(command) => {
            commands::token::run(
                command,
                &cli.access_endpoint,
                token.as_deref(),
                &credentials_path,
                cli.json,
            )
            .await
        }
        Command::Graph(command) => commands::graph::run(command, &cli.s3_endpoint, cli.json).await,
        Command::Vector(command) => {
            commands::vector::run(command, &cli.s3_endpoint, cli.json).await
        }
    }
}

/// Entry point. Parses and dispatches inside the tokio runtime.
fn main() {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime builds");
    let cli = Cli::parse();
    if let Err(error) = runtime.block_on(run(cli)) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}
