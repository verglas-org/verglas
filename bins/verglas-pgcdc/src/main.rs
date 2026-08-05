//! The CDC worker binary: env-in / result-file-out, one drain tick per run.
//!
//! This is the subprocess worker the fleet spawns for a zero-ETL deployment. It
//! follows the same contract `verglas_harness::worker::run_worker` and the TS
//! SDK's `endpoint-run.ts` share: the parent sets the run's environment, the
//! child runs once and writes a [`RunResult`] JSON to `RESULT_PATH`, and exits 0
//! on success or 1 on failure. There is no framed stdio protocol.
//!
//! One run: resolve the environment into a [`CdcEnv`], connect the Postgres pool
//! and open the Iceberg catalog (all data-file IO routed through the injected
//! cache endpoint — never direct R2), run exactly one
//! [`verglas_pgcdc::runner::drain_tick`], and report the rows appended.
//!
//! ## Watermark
//!
//! The confirmed LSN is durable in the replication slot itself — the drain
//! advances the slot only after the Iceberg append commits, so the slot is the
//! authoritative cursor and a crash before the result write simply redelivers on
//! the next tick. Mirroring the confirmed LSN to the server's durable watermark
//! endpoint (`GET/PUT /v1/watermark`, keyed by `DEPLOYMENT`) is a
//! TODO(watermark-http-mirror): it is an observability convenience, not a
//! correctness requirement, and is out of scope for green v1.

use std::process::ExitCode;

use verglas_pgcdc::iceberg_sink::SinkConfig;
use verglas_pgcdc::runner::{IcebergSink, PgConn, RunnerConfig, drain_tick};
use verglas_sdk::worker::{
    CloudEvent, DEFAULT_RESULT_PATH, ENV_DEPLOYMENT, ENV_RESULT_PATH, ENV_TARGET, ENV_TOKEN,
    RunResult,
};

/// The environment the control plane injects for a CDC worker (the launch env
/// built in verglas-cloud's `pg_cdc.ts`), beyond the shared `VERGLAS_*` /
/// `DEPLOYMENT` / `TARGET` worker bindings. One contract, no alternates: these
/// exact names are what the fleet launch carries (the PG password arrives
/// sealed to the box and is unsealed into `VERGLAS_CDC_PG_PASSWORD` by the
/// host agent before the guest starts).
mod env {
    /// Source-database host (the tenant PG VM's guest IP, resolved on the box).
    pub const PG_HOST: &str = "VERGLAS_CDC_PG_HOST";
    /// Source-database port (defaults to 5432).
    pub const PG_PORT: &str = "VERGLAS_CDC_PG_PORT";
    /// Source-database name (the tenant database this job captures).
    pub const PG_DATABASE: &str = "VERGLAS_CDC_PG_DATABASE";
    /// Source-database user (the dedicated CDC replication role).
    pub const PG_USER: &str = "VERGLAS_CDC_PG_USER";
    /// Source-database password (unsealed on the box; never logged).
    pub const PG_PASSWORD: &str = "VERGLAS_CDC_PG_PASSWORD";
    /// The replication slot name (defaults to `verglas_cdc`).
    pub const SLOT: &str = "VERGLAS_CDC_SLOT";
    /// The publication name (defaults to `verglas_cdc`).
    pub const PUBLICATION: &str = "VERGLAS_CDC_PUBLICATION";
    /// The tenant's Iceberg REST catalog (catalogd in the PG stack VM, :8181).
    pub const CATALOG_ENDPOINT: &str = "VERGLAS_CDC_CATALOG_ENDPOINT";
    /// The tenant cache S3 endpoint ALL Iceberg data-file IO routes through
    /// (:8333). Required: there is no direct-R2 branch.
    pub const CACHE_S3_ENDPOINT: &str = "VERGLAS_CDC_CACHE_S3_ENDPOINT";
    /// The catalog warehouse identifier (optional; catalogd defaults it).
    pub const WAREHOUSE: &str = "VERGLAS_CDC_WAREHOUSE";
    /// The S3 signing region (defaults to `us-east-1`).
    pub const S3_REGION: &str = "VERGLAS_CDC_S3_REGION";
    /// The S3 access key id for data-file IO through the cache.
    pub const S3_ACCESS_KEY_ID: &str = "VERGLAS_CDC_S3_ACCESS_KEY_ID";
    /// The S3 secret access key for data-file IO through the cache.
    pub const S3_SECRET_ACCESS_KEY: &str = "VERGLAS_CDC_S3_SECRET_ACCESS_KEY";
}

/// The source-database connection parts. Kept discrete (not a DSN string) so a
/// password with URL-special characters never needs escaping — the connection
/// is built with `PgConnectOptions`, not string splicing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgParts {
    /// The tenant PG host.
    pub host: String,
    /// The Postgres wire port.
    pub port: u16,
    /// The database this job captures.
    pub database: String,
    /// The dedicated CDC role.
    pub user: String,
    /// The role's password. Never logged.
    pub password: String,
}

/// The resolved CDC run environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcEnv {
    /// The deployment name (the watermark key and the run log pipeline).
    pub deployment: String,
    /// The deployment-configured target (informational for CDC — the tables are
    /// derived from the publication, not from `TARGET`).
    pub target: Option<String>,
    /// The source-database connection parts.
    pub pg: PgParts,
    /// The replication slot to drain.
    pub slot: String,
    /// The publication the slot decodes against.
    pub publication: String,
    /// The Iceberg sink connection config (catalog + cache endpoint + creds).
    pub sink: SinkConfig,
    /// The result-file path to write the [`RunResult`] to.
    pub result_path: String,
}

impl CdcEnv {
    /// Resolves the run environment from a `getenv` closure. Returns an error
    /// naming the first missing required variable. Testable without touching the
    /// process environment.
    pub fn from_env<F: Fn(&str) -> Option<String>>(getenv: F) -> Result<CdcEnv, String> {
        let deployment = getenv(ENV_DEPLOYMENT).ok_or("DEPLOYMENT is required")?;
        let pg = PgParts {
            host: getenv(env::PG_HOST).ok_or("VERGLAS_CDC_PG_HOST is required")?,
            port: match getenv(env::PG_PORT) {
                Some(p) => p
                    .parse::<u16>()
                    .map_err(|_| format!("VERGLAS_CDC_PG_PORT is not a port: {p}"))?,
                None => 5432,
            },
            database: getenv(env::PG_DATABASE).ok_or("VERGLAS_CDC_PG_DATABASE is required")?,
            user: getenv(env::PG_USER).ok_or("VERGLAS_CDC_PG_USER is required")?,
            password: getenv(env::PG_PASSWORD).unwrap_or_default(),
        };

        let default_cfg = RunnerConfig::default();
        let sink = SinkConfig {
            catalog_uri: getenv(env::CATALOG_ENDPOINT)
                .ok_or("VERGLAS_CDC_CATALOG_ENDPOINT is required")?,
            // The shared worker token authenticates against the tenant catalogd.
            token: getenv(ENV_TOKEN),
            warehouse: getenv(env::WAREHOUSE),
            // The cache endpoint: all Iceberg IO routes through it, never R2.
            // Required — a CDC run without a cache endpoint must not start.
            s3_endpoint: Some(
                getenv(env::CACHE_S3_ENDPOINT)
                    .ok_or("VERGLAS_CDC_CACHE_S3_ENDPOINT is required")?,
            ),
            region: getenv(env::S3_REGION).unwrap_or_else(|| "us-east-1".to_owned()),
            access_key_id: getenv(env::S3_ACCESS_KEY_ID),
            secret_access_key: getenv(env::S3_SECRET_ACCESS_KEY),
        };

        Ok(CdcEnv {
            deployment,
            target: getenv(ENV_TARGET),
            pg,
            slot: getenv(env::SLOT).unwrap_or(default_cfg.slot),
            publication: getenv(env::PUBLICATION).unwrap_or(default_cfg.publication),
            sink,
            result_path: getenv(ENV_RESULT_PATH).unwrap_or_else(|| DEFAULT_RESULT_PATH.to_owned()),
        })
    }
}

/// Runs one drain tick and returns the rows appended.
async fn run(cdc: &CdcEnv) -> Result<u64, String> {
    let cfg = RunnerConfig {
        slot: cdc.slot.clone(),
        publication: cdc.publication.clone(),
        ..RunnerConfig::default()
    };

    let options = sqlx::postgres::PgConnectOptions::new()
        .host(&cdc.pg.host)
        .port(cdc.pg.port)
        .database(&cdc.pg.database)
        .username(&cdc.pg.user)
        .password(&cdc.pg.password);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .map_err(|e| format!("connect postgres: {e}"))?;

    let catalog = verglas_pgcdc::iceberg_sink::open_catalog(&cdc.sink)
        .await
        .map_err(|e| format!("open catalog: {e}"))?;

    let pg = PgConn {
        pool,
        cfg: cfg.clone(),
    };
    let sink = IcebergSink {
        catalog,
        slot: cfg.slot.clone(),
    };

    let status = drain_tick(&pg, &sink, &cfg)
        .await
        .map_err(|e| format!("drain tick: {e}"))?;

    // TODO(watermark-http-mirror): PUT status.confirmed_lsn to the server's
    // /v1/watermark keyed by cdc.deployment. The slot is the durable cursor;
    // this mirror is observability only.
    Ok(status.tables.iter().map(|t| t.rows_appended).sum())
}

fn main() -> ExitCode {
    let cdc = match CdcEnv::from_env(|k| std::env::var(k).ok()) {
        Ok(cdc) => cdc,
        Err(e) => {
            let result = RunResult::failed(e);
            let _ = write_result(DEFAULT_RESULT_PATH, &result);
            eprintln!("verglas-pgcdc: {}", result.error.unwrap_or_default());
            return ExitCode::FAILURE;
        }
    };

    // Informational: the trigger this run was invoked under (CDC is cron-driven).
    let event = match CloudEvent::from_env(|k| std::env::var(k).ok()) {
        Ok(event) => event,
        Err(error) => {
            let result = RunResult::failed(error);
            let _ = write_result(&cdc.result_path, &result);
            return ExitCode::FAILURE;
        }
    };
    eprintln!(
        "verglas-pgcdc: deployment={} event_type={}",
        cdc.deployment, event.event_type
    );

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            let result = RunResult::failed(format!("tokio runtime: {e}"));
            let _ = write_result(&cdc.result_path, &result);
            return ExitCode::FAILURE;
        }
    };

    let (result, code) = match runtime.block_on(run(&cdc)) {
        Ok(rows) => (RunResult::ok(rows), ExitCode::SUCCESS),
        Err(message) => {
            eprintln!("verglas-pgcdc: {message}");
            (RunResult::failed(message), ExitCode::FAILURE)
        }
    };
    if let Err(e) = write_result(&cdc.result_path, &result) {
        eprintln!("verglas-pgcdc: writing result file: {e}");
        return ExitCode::FAILURE;
    }
    code
}

/// Writes the [`RunResult`] JSON to `path`, creating parent directories.
fn write_result(path: &str, result: &RunResult) -> std::io::Result<()> {
    if let Some(parent) = std::path::Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string(result)?;
    std::fs::write(path, json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn getenv_from<'a>(map: &'a HashMap<&'a str, &'a str>) -> impl Fn(&str) -> Option<String> + 'a {
        move |k: &str| map.get(k).map(|s| s.to_string())
    }

    /// The full launch env the control plane injects (pg_cdc.ts contract).
    fn full_env() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            (ENV_DEPLOYMENT, "cdc-worker"),
            (ENV_TOKEN, "tok"),
            (env::PG_HOST, "10.0.0.7"),
            (env::PG_PORT, "5432"),
            (env::PG_DATABASE, "app"),
            (env::PG_USER, "vgcdc_ab12"),
            (env::PG_PASSWORD, "s3cr:et/with@specials"),
            (env::SLOT, "verglas_cdc"),
            (env::PUBLICATION, "verglas_cdc"),
            (env::CATALOG_ENDPOINT, "http://10.0.0.7:8181"),
            (env::CACHE_S3_ENDPOINT, "http://10.0.0.9:8333"),
        ])
    }

    #[test]
    fn resolves_the_control_plane_launch_env() {
        let map = full_env();
        let cdc = CdcEnv::from_env(getenv_from(&map)).expect("env");
        assert_eq!(cdc.deployment, "cdc-worker");
        assert_eq!(cdc.pg.host, "10.0.0.7");
        assert_eq!(cdc.pg.port, 5432);
        assert_eq!(cdc.pg.database, "app");
        assert_eq!(cdc.pg.user, "vgcdc_ab12");
        // Discrete parts: a password with URL-special characters survives as-is.
        assert_eq!(cdc.pg.password, "s3cr:et/with@specials");
        assert_eq!(cdc.slot, "verglas_cdc");
        assert_eq!(cdc.publication, "verglas_cdc");
        assert_eq!(cdc.sink.catalog_uri, "http://10.0.0.7:8181");
        assert_eq!(cdc.sink.token.as_deref(), Some("tok"));
        assert_eq!(
            cdc.sink.s3_endpoint.as_deref(),
            Some("http://10.0.0.9:8333")
        );
        assert_eq!(cdc.result_path, DEFAULT_RESULT_PATH);
    }

    #[test]
    fn slot_and_publication_default_when_unset() {
        let mut map = full_env();
        map.remove(env::SLOT);
        map.remove(env::PUBLICATION);
        map.remove(env::PG_PORT);
        let cdc = CdcEnv::from_env(getenv_from(&map)).expect("env");
        assert_eq!(cdc.slot, "verglas_cdc");
        assert_eq!(cdc.publication, "verglas_cdc");
        assert_eq!(cdc.pg.port, 5432);
    }

    #[test]
    fn missing_deployment_is_an_error() {
        let mut map = full_env();
        map.remove(ENV_DEPLOYMENT);
        assert!(CdcEnv::from_env(getenv_from(&map)).is_err());
    }

    #[test]
    fn missing_catalog_endpoint_is_an_error() {
        let mut map = full_env();
        map.remove(env::CATALOG_ENDPOINT);
        assert!(CdcEnv::from_env(getenv_from(&map)).is_err());
    }

    #[test]
    fn missing_cache_endpoint_is_an_error() {
        // Cache-or-nothing: without the cache S3 endpoint the run must refuse
        // to start rather than fall through to any direct backend.
        let mut map = full_env();
        map.remove(env::CACHE_S3_ENDPOINT);
        let err = CdcEnv::from_env(getenv_from(&map)).expect_err("must fail");
        assert!(err.contains("VERGLAS_CDC_CACHE_S3_ENDPOINT"));
    }

    #[test]
    fn bad_port_is_an_error() {
        let mut map = full_env();
        map.insert(env::PG_PORT, "not-a-port");
        assert!(CdcEnv::from_env(getenv_from(&map)).is_err());
    }
}
