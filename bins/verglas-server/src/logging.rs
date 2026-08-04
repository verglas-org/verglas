//! The server's `tracing` subscriber (#61, building on #241/#247).
//!
//! One subscriber is installed for the process before any background work
//! starts. Its output format (`json` for log pipelines, `pretty` for
//! `verglas dev`) and default verbosity come from the `[log]` config section;
//! `RUST_LOG` overrides the level at startup and `VERGLAS_LOG_FORMAT` overrides
//! the format (the escape hatch `verglas dev` uses to force pretty output
//! without editing the generated config). The filter sits behind a reload
//! handle so the admin API can raise it to `DEBUG` for a live investigation and
//! lower it again — no restart.
//!
//! # Logging never blocks a fill
//!
//! Per-request logging must not stall serving (a standing invariant). Writing
//! to `stderr` synchronously can block if the log consumer is slow or stalled,
//! so the subscriber writes through a background worker with a bounded,
//! drop-on-full queue ([`tracing_appender::non_blocking`]): the serving task
//! hands the line off without waiting, and a stalled consumer drops log lines
//! rather than back-pressuring a request. This is also the "sampling knob for
//! very hot deployments" the issue calls for — under extreme log volume the
//! queue sheds load instead of the server stalling.

use std::io::stderr;
use std::sync::OnceLock;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, Registry, fmt, prelude::*, reload};
use verglas_core::config::LogFormat;

/// Env var overriding [`crate::logging`]'s configured format. `verglas dev` sets
/// it to `pretty` so a local run is human-readable regardless of the config.
const LOG_FORMAT_ENV: &str = "VERGLAS_LOG_FORMAT";

/// The reload handle for the process's log filter, set once at install time. The
/// admin API reads it to swap the filter at runtime.
static RELOAD: OnceLock<reload::Handle<EnvFilter, Registry>> = OnceLock::new();

/// Keeps the non-blocking writer's background worker alive for the process. The
/// worker flushes and stops when this guard drops, so it is parked here for the
/// server's lifetime rather than dropped at the end of [`install`].
static WRITER_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

/// The format the subscriber uses, after the `VERGLAS_LOG_FORMAT` override is
/// applied to the configured value.
fn effective_format(configured: LogFormat) -> LogFormat {
    match std::env::var(LOG_FORMAT_ENV).ok().as_deref() {
        Some("pretty") => LogFormat::Pretty,
        Some("json") => LogFormat::Json,
        _ => configured,
    }
}

/// Builds the startup filter: `RUST_LOG` when set (so the well-known env var
/// still works), otherwise the configured level. An unparseable value in either
/// falls back to `info` so a typo never silences the server (#241).
fn startup_filter(level: &str) -> EnvFilter {
    if let Ok(from_env) = EnvFilter::try_from_default_env() {
        return from_env;
    }
    EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Installs the process subscriber from the `[log]` config (or the built-in
/// defaults when the server runs config-less). Idempotent by way of
/// `try_init`: a second call (a test that already installed one) is ignored.
pub fn install(format: LogFormat, level: &str) {
    let (filter, handle) = reload::Layer::new(startup_filter(level));
    let _ = RELOAD.set(handle);
    // Non-blocking writer over stderr: the serving task never waits on the log
    // sink, and a stalled consumer drops lines instead of stalling a fill.
    let (writer, guard) = tracing_appender::non_blocking(stderr());
    let _ = WRITER_GUARD.set(guard);
    let registry = Registry::default().with(filter);
    let installed = match effective_format(format) {
        // Flattened JSON: the event's fields and the active span's fields
        // (request_id, op, …) sit at the top level of one object per line, so
        // `jq '.request_id'` works without reaching into a nested span array.
        LogFormat::Json => registry
            .with(
                fmt::layer()
                    .json()
                    .flatten_event(true)
                    .with_current_span(true)
                    .with_span_list(false)
                    .with_writer(writer),
            )
            .try_init(),
        LogFormat::Pretty => registry.with(fmt::layer().with_writer(writer)).try_init(),
    };
    if let Err(error) = installed {
        eprintln!("verglas-server: failed to install tracing subscriber: {error}");
    }
}

/// Applies a new verbosity filter at runtime (the admin `POST /admin/log`
/// path). Returns the applied filter string on success, or an actionable error
/// when the filter does not parse or the subscriber was never installed.
pub fn reload_level(level: &str) -> Result<String, String> {
    let filter =
        EnvFilter::try_new(level).map_err(|e| format!("invalid log filter `{level}`: {e}"))?;
    let handle = RELOAD
        .get()
        .ok_or_else(|| "log subscriber is not installed".to_owned())?;
    handle
        .reload(filter)
        .map_err(|e| format!("failed to apply log filter: {e}"))?;
    Ok(level.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `VERGLAS_LOG_FORMAT` overrides the configured format both ways; an unset
    /// or unrecognised value keeps the configured one.
    #[test]
    fn env_overrides_configured_format() {
        // Serialize on the process-global env var so parallel tests do not race.
        // SAFETY: single-threaded within this test; no other thread reads the var.
        unsafe { std::env::remove_var(LOG_FORMAT_ENV) };
        assert_eq!(effective_format(LogFormat::Json), LogFormat::Json);
        unsafe { std::env::set_var(LOG_FORMAT_ENV, "pretty") };
        assert_eq!(effective_format(LogFormat::Json), LogFormat::Pretty);
        unsafe { std::env::set_var(LOG_FORMAT_ENV, "json") };
        assert_eq!(effective_format(LogFormat::Pretty), LogFormat::Json);
        unsafe { std::env::set_var(LOG_FORMAT_ENV, "garbage") };
        assert_eq!(effective_format(LogFormat::Pretty), LogFormat::Pretty);
        unsafe { std::env::remove_var(LOG_FORMAT_ENV) };
    }

    /// A filter with an unparseable level directive is rejected by name before
    /// it can reach the handle, so a typo is a clear error not a silent no-op.
    #[test]
    fn reload_rejects_a_bad_filter() {
        let error = reload_level("verglas_cache=notalevel").expect_err("bad filter rejected");
        assert!(
            error.contains("invalid log filter"),
            "the rejection names the bad filter: {error}"
        );
    }
}
