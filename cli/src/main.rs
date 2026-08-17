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
/// the scoped-token store, and the durable control-plane token — never
/// WorkOS, which is spent once at login and never contacted again. Cloud
/// commands (everything except `login`/`logout`/`graph`/`vector`) also give
/// `auth` a chance to opportunistically renew a stale durable token after a
/// successful call; see `auth::maybe_renew_durable_token`.
async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let credentials_path = cli.resolved_credentials_path()?;
    let endpoint = crate::backend::resolved_endpoint();
    match cli.command {
        Command::Login(args) => {
            commands::connection::login(
                &Cli::access_endpoint(),
                args.api_key.as_deref(),
                args.no_browser,
            )
            .await
        }
        Command::Logout => commands::connection::logout(),
        Command::Status => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            after_success(commands::status::run(&endpoint, token.as_deref(), cli.json).await).await
        }
        Command::Table(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            after_success(commands::table::run(command, token.as_deref(), cli.json).await).await
        }
        Command::Dashboard(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            after_success(
                commands::dashboard::run(command, &endpoint, token.as_deref(), cli.json).await,
            )
            .await
        }
        Command::Workers(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            after_success(
                commands::workers::run(command, &endpoint, token.as_deref(), cli.json).await,
            )
            .await
        }
        Command::Lakehouse(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            after_success(
                commands::lakehouse::run(
                    command,
                    &Cli::access_endpoint(),
                    token.as_deref(),
                    cli.json,
                )
                .await,
            )
            .await
        }
        Command::Token(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            after_success(
                commands::token::run(command, &Cli::access_endpoint(), token.as_deref(), cli.json)
                    .await,
            )
            .await
        }
        Command::Lakehouse(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            after_success(
                commands::lakehouse::run(command, &Cli::access_endpoint(), token.as_deref(), cli.json)
                    .await,
            )
            .await
        }
        Command::Secret(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            after_success(
                commands::secret::run(command, &Cli::access_endpoint(), token.as_deref(), cli.json)
                    .await,
            )
            .await
        }
        Command::Token(command) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            after_success(
                commands::token::run(
                    command,
                    &Cli::access_endpoint(),
                    token.as_deref(),
                    &credentials_path,
                    cli.json,
                )
                .await,
            )
            .await
        }
        Command::Ingest(args) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            after_success(commands::data::ingest(&endpoint, token.as_deref(), args, cli.json).await)
                .await
        }
        Command::Sql(args) => {
            let token = auth::resolved_bearer(&credentials_path).await?;
            after_success(commands::data::sql(&endpoint, token.as_deref(), args, cli.json).await)
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

/// Gives `auth` a chance to opportunistically renew a stale durable token
/// after a cloud command succeeds. A no-op when the command failed, when
/// there is no durable token file, or when it is not yet stale; renewal
/// itself is entirely best-effort and never changes `result`.
async fn after_success<T, E>(result: Result<T, E>) -> Result<T, E> {
    if result.is_ok() {
        auth::maybe_renew_durable_token().await;
    }
    result
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
