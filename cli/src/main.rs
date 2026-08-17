//! `verglas` — the Verglas CLI, a PURE CLIENT.
//!
//! Cloud is the default control plane (`https://api.verglas.dev`). Self-hosted
//! OSS servers are selected with `VERGLAS_ENDPOINT`. Workers are Cloud-only.
//! Self-hosters drain a node through its admin API directly; the CLI has no
//! drain verb.

mod admin_client;
mod auth;
mod backend;
mod cli;
mod commands;
mod connection_profile;
mod credentials;
mod dashboard_spec;
mod output;
mod worker_spec;

use clap::Parser;
use cli::{Cli, Command};

/// Runs the parsed CLI command against the configured admin endpoint. `login`
/// and `logout` never resolve an existing bearer (a stale or absent session
/// must not block them); every other command resolves its bearer through
/// `auth::resolved_bearer`, the CLI's single choke point for `VERGLAS_TOKEN`,
/// the scoped-token store, and the WorkOS session.
async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let credentials_path = cli.resolved_credentials_path()?;
    let endpoint = crate::backend::resolved_endpoint();
    match cli.command {
        Command::Login(args) => {
            commands::connection::login(&Cli::access_endpoint(), args.api_key.as_deref(), args.no_browser)
                .await
        }
        Command::Logout => commands::connection::logout(),
        Command::Status => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            commands::status::run(&endpoint, token.as_deref(), cli.json).await
        }
        Command::Table(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            commands::table::run(command, token.as_deref(), cli.json).await
        }
        Command::Dashboard(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            commands::dashboard::run(command, &endpoint, token.as_deref(), cli.json).await
        }
        Command::Workers(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            commands::workers::run(command, &endpoint, token.as_deref(), cli.json).await
        }
        Command::Lakehouse(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            commands::lakehouse::run(command, &Cli::access_endpoint(), token.as_deref(), cli.json)
                .await
        }
        Command::Secret(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            commands::secret::run(command, &Cli::access_endpoint(), token.as_deref(), cli.json)
                .await
        }
        Command::Token(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            commands::token::run(
                command,
                &Cli::access_endpoint(),
                token.as_deref(),
                &credentials_path,
                cli.json,
            )
            .await
        }
        Command::Graph(command) => {
            commands::graph::run(command, &semantic_endpoint(), cli.json).await
        }
        Command::Vector(command) => {
            commands::vector::run(command, &semantic_endpoint(), cli.json).await
        }
    }
}

/// The semantic S3 endpoint for `graph`/`vector`: `VERGLAS_S3_ENDPOINT` wins;
/// then the `[connection]` profile written by `verglas login`; then the local
/// loopback listener.
fn semantic_endpoint() -> String {
    if let Ok(url) = std::env::var("VERGLAS_S3_ENDPOINT")
        && !url.trim().is_empty()
    {
        return url;
    }
    connection_profile::resolve_from_environment(&connection_profile::environment())
        .map(|connection| connection.semantic_uri)
        .unwrap_or_else(|_| "http://127.0.0.1:8333".to_owned())
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
