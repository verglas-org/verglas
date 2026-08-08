//! `verglas` — the Verglas CLI, a PURE CLIENT.
//!
//! The developer and operator entry point. The CLI is never required to be
//! co-located with a server: every data-plane and registry verb (`table`,
//! `query`, `graph`, `workers`, `index`) targets whatever `--server-endpoint` /
//! `VERGLAS_ENDPOINT` names — localhost by default, but equally a remote
//! self-hosted server on another host. Which server OWNS a declaration is a
//! property of the endpoint, not of where the CLI runs.
//!
//! `crate::backend` is the single place that resolves the server client a
//! command speaks to.
//!
//! `drain` is local by definition: it drains the server addressed by the
//! configured endpoint. Running an edge server in Docker is for cache and read
//! latency, never a requirement to use the platform.

mod admin_client;
mod backend;
mod cli;
mod commands;
mod output;
mod worker_spec;

use clap::Parser;
use cli::{Cli, Command};

/// Runs the parsed CLI command against the configured admin endpoint.
async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Drain(args) => commands::drain::run(&cli.endpoint, &args, cli.json).await,
        Command::Status => commands::status::run(&cli.endpoint, cli.json).await,
        Command::Table(command) => commands::table::run(command, &cli.endpoint, cli.json).await,
        Command::Graph(command) => commands::graph::run(command, &cli.endpoint, cli.json).await,
        Command::Query(args) => commands::query::run(args, &cli.endpoint, cli.json).await,
        Command::Index(command) => {
            commands::table::run_index_registry(command, &cli.endpoint, cli.json).await
        }
        Command::Dashboard(command) => {
            commands::dashboard::run(command, &cli.endpoint, cli.json).await
        }
        Command::Workers(command) => {
            commands::workers::run(command, &cli.endpoint, cli.json).await
        }
        Command::Kv(command) => {
            commands::kv::run(command, &cli.endpoint, cli.token.as_deref()).await
        }
        Command::Vessel(command) => commands::vessel::run(command, cli.json).await,
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
