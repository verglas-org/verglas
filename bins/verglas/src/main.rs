//! `verglas` — the Verglas CLI, a PURE CLIENT.
//!
//! The developer and operator entry point. The CLI is never required to be
//! co-located with a daemon: every data-plane and registry verb (`table`,
//! `query`, `graph`, `workers`, `index`, `tables`) targets whatever
//! `--daemon-endpoint` / `VERGLAS_ENDPOINT` names — localhost by default, but
//! equally a remote daemon on another host or a cloud node. Which daemon OWNS a
//! declaration is a property of the endpoint, not of where the CLI runs, so a
//! host can declare a sink against a remote daemon without running one locally.
//! `crate::backend` is the single place that resolves per-command which backend
//! (the daemon endpoint, or the control plane) a command speaks to.
//!
//! Only the service-lifecycle verbs (`init`, `start`, `stop`, `status`, `logs`,
//! `dev`, `drain`) are local by definition — they install, run, or drain the
//! daemon on THIS machine. Running a local/edge daemon is for cache and read
//! latency, never a requirement to use the platform.
//!
//! `verglas login` enriches the platform primitives' `list`/`show` output with
//! the tenant's cloud items through the control-plane client; the verbs keep
//! working without a login.

mod admin_client;
mod backend;
mod cli;
mod commands;
mod controlplane;
mod output;
mod service;
mod worker_spec;

use clap::Parser;
use cli::{Cli, Command};

/// Runs the parsed CLI command against the configured admin endpoint.
async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Login(args) => commands::login::run(args).await,
        Command::Dev(args) => commands::dev::run(args).await,
        Command::Drain(args) => commands::drain::run(&cli.endpoint, &args, cli.json).await,
        Command::Init(args) => commands::lifecycle::init(args).await,
        Command::Start(args) => commands::lifecycle::start(args).await,
        Command::Stop(args) => commands::lifecycle::stop(args).await,
        Command::Restart(args) => commands::lifecycle::restart(args).await,
        Command::Status(args) => commands::lifecycle::status(args, cli.json).await,
        Command::Logs(args) => commands::lifecycle::logs(args).await,
        Command::Table(command) => commands::table::run(command, &cli.endpoint, cli.json).await,
        Command::Graph(command) => commands::graph::run(command, &cli.endpoint, cli.json).await,
        Command::Query(args) => commands::query::run(args, &cli.endpoint, cli.json).await,
        Command::Index(command) => {
            commands::table::run_index_registry(command, &cli.endpoint, cli.json).await
        }
        Command::Workers(command) => {
            commands::cloud::run_workers(command, &cli.endpoint, cli.json).await
        }
        Command::Containers(command) => commands::cloud::run_containers(command, cli.json).await,
        Command::Db(command) => commands::cloud::run_db(command, cli.json).await,
        Command::Volumes(command) => commands::cloud::run_volumes(command, cli.json).await,
        Command::Secrets(command) => commands::cloud::run_secrets(command, cli.json).await,
        Command::Skills(command) => match command {
            cli::SkillsCommand::Install(args) => commands::skills::run(args, cli.json).await,
        },
    }
}

/// Entry point. Applies the agent settings from the one config file BEFORE
/// the tokio runtime exists (env mutation must precede any threads), then
/// parses and dispatches inside the runtime.
fn main() {
    // The one config: agent settings from ~/.verglas/config.toml (env vars
    // still override for dev). Before argument parsing so clap's env-backed
    // args see the derived values, and before the runtime spawns threads.
    verglas_core::config::apply_agent_env();
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime builds");
    let cli = Cli::parse();
    if let Err(error) = runtime.block_on(run(cli)) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}
