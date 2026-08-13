//! `verglas-query` — the standalone Verglas query role.
//!
//! Stateless SQL over Iceberg tables (`crates/verglas-iceberg`'s embedded
//! DataFusion engine), reading every table's data through a configured cache
//! S3 endpoint — never a direct object-store path. There is no cache tier, no
//! S3 frontend, no source/sink/jobs code, and no catalog write path in this
//! binary.
//!
//! This binary is meant to be launched on demand by an external orchestrator,
//! or directly for local use, and killed after use: it
//! self-exits after an idle period with no request, and shuts down cleanly on
//! SIGTERM or SIGINT either way. Its memory need is *predicted*, not just
//! grown reactively: given the SQL it's about to serve (`--for-query`), it
//! estimates the query's memory need from the query plan and Iceberg table
//! statistics before requesting its initial grant (`sizing::request_for_query`),
//! then grows that grant, grow-only, if a later request needs more
//! (`sizing::ensure_covers`). The grant request/grow/release calls go through
//! `verglas_core::grant::MemoryGrantHost` — the engine-level contract
//! every worker kind gets, not something query-specific. Standalone/dev runs
//! with no host agent attached use `LocalGrantHost`, which grants whatever is
//! asked and enforces nothing.
//!
//! Two invocation modes:
//!
//! - `verglas-query --config <path> [--for-query <sql>]` — serves
//!   `POST /v1/query`, `POST /v1/query/estimate`, and `GET /healthz` until
//!   idle or signaled.
//! - `verglas-query --config <path> estimate --sql <sql> [--at-table <t>
//!   --at-reference <r>]` — answers one estimate as JSON on stdout and exits,
//!   without starting a server. This is how a launcher sizes a worker's
//!   initial grant *before* deciding to start it, without a catalog
//!   connection of its own already open.

use std::sync::Arc;

use verglas_core::grant::{LocalGrantHost, MemoryGrantHost};

use verglas_query::VERSION;
use verglas_query::config::QueryConfig;
use verglas_query::{logging, serve};

/// Parsed command-line invocation.
enum Invocation {
    /// `--version` / `-V`: print and exit.
    Version,
    /// Serve the query surface, optionally sized from `for_query`.
    Serve {
        config_path: String,
        for_query: Option<String>,
        ports_file: Option<String>,
    },
    /// Answer one estimate on stdout and exit, without serving.
    Estimate {
        config_path: String,
        sql: String,
        at_table: Option<String>,
        at_reference: Option<String>,
    },
}

/// Parses argv by hand, matching the style `verglas-cache-node` uses (no
/// clap): `--config <path>` is required in every mode; `estimate` is a
/// subcommand that must come first; `--for-query`/`--sql`/`--at-table`/
/// `--at-reference` are flags.
fn parse_args() -> Result<Invocation, String> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--version" || a == "-V") {
        return Ok(Invocation::Version);
    }

    let estimate_mode = matches!(args.first().map(String::as_str), Some("estimate"));
    if estimate_mode {
        args.remove(0);
    }

    let mut config_path = None;
    let mut for_query = None;
    let mut ports_file = None;
    let mut sql = None;
    let mut at_table = None;
    let mut at_reference = None;

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => config_path = Some(require_value(&mut iter, "--config")?),
            "--for-query" => for_query = Some(require_value(&mut iter, "--for-query")?),
            // Same convention `verglas-cache-node --ports-file` uses (issue #194): bind
            // an ephemeral port, then append `<role> <ip:port>` lines so a
            // parent that spawned this worker learns the real port without a
            // probe-then-bind race.
            "--ports-file" => ports_file = Some(require_value(&mut iter, "--ports-file")?),
            "--sql" => sql = Some(require_value(&mut iter, "--sql")?),
            "--at-table" => at_table = Some(require_value(&mut iter, "--at-table")?),
            "--at-reference" => at_reference = Some(require_value(&mut iter, "--at-reference")?),
            other => return Err(format!("unrecognized argument `{other}`")),
        }
    }
    let config_path = config_path.ok_or_else(|| "--config <path> is required".to_owned())?;

    if estimate_mode {
        let sql = sql.ok_or_else(|| "estimate mode requires --sql <statement>".to_owned())?;
        Ok(Invocation::Estimate {
            config_path,
            sql,
            at_table,
            at_reference,
        })
    } else {
        Ok(Invocation::Serve {
            config_path,
            for_query,
            ports_file,
        })
    }
}

/// Takes the next argv value or reports which flag needed one.
fn require_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    iter.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

#[tokio::main]
async fn main() {
    let invocation = match parse_args() {
        Ok(invocation) => invocation,
        Err(error) => {
            eprintln!("verglas-query: {error}");
            std::process::exit(1);
        }
    };

    if matches!(invocation, Invocation::Version) {
        println!("verglas-query {VERSION}");
        return;
    }

    // Pin the process rustls CryptoProvider so an https cache/catalog endpoint
    // resolves TLS through a chosen provider rather than panicking when the
    // dep graph carries more than one.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let config_path = match &invocation {
        Invocation::Serve { config_path, .. } | Invocation::Estimate { config_path, .. } => {
            config_path.clone()
        }
        Invocation::Version => unreachable!(),
    };
    let config = match QueryConfig::load(std::path::Path::new(&config_path)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("verglas-query: {error}");
            std::process::exit(1);
        }
    };

    let grant_host: Arc<dyn MemoryGrantHost> = Arc::new(LocalGrantHost);

    match invocation {
        Invocation::Estimate {
            sql,
            at_table,
            at_reference,
            ..
        } => {
            // No serving, no logging subscriber needed — this mode is a
            // one-shot CLI call whose only output is the JSON result.
            let connection = match build_connection_for_estimate(&config) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("verglas-query: {e}");
                    std::process::exit(1);
                }
            };
            let catalog = match verglas_iceberg::catalog::open_catalog(&connection).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("verglas-query: cannot open catalog: {e}");
                    std::process::exit(1);
                }
            };
            let at = serve::time_travel_from_flags(at_table, at_reference);
            match verglas_iceberg::estimate(catalog, &sql, at).await {
                Ok(estimate) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&estimate).expect("serializable")
                    );
                }
                Err(e) => {
                    eprintln!("verglas-query: {e}");
                    std::process::exit(1);
                }
            }
        }
        Invocation::Serve {
            for_query,
            ports_file,
            ..
        } => {
            logging::install(config.log.format, &config.log.level);
            println!("verglas-query {VERSION} config ok: {}", config.summary());
            let ports_file = ports_file.map(std::path::PathBuf::from);
            if let Err(error) = serve::run(&config, grant_host, for_query, ports_file).await {
                eprintln!("verglas-query failed: {error}");
                std::process::exit(1);
            }
        }
        Invocation::Version => unreachable!(),
    }
}

/// Builds the connection for the `estimate` CLI mode — the same connection
/// `serve::run` builds, exposed here since that mode never calls `serve::run`
/// at all (it never starts a server).
fn build_connection_for_estimate(
    config: &QueryConfig,
) -> Result<verglas_iceberg::Connection, String> {
    serve::connection_for(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `--config` is required in every mode.
    #[test]
    fn parse_args_requires_config() {
        let err = require_value(&mut std::iter::empty(), "--config").expect_err("no value given");
        assert!(err.contains("--config"));
    }
}
