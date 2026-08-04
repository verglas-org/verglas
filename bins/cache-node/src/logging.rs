//! The cache node's `tracing` subscriber.
//!
//! Copied down from `bins/verglas-server/src/logging.rs`, dropping the runtime reload
//! handle: the cache node has no `/admin/log` route to hot-reload the filter, so
//! the subscriber is installed once from the `[log]` config and left alone. Same
//! output shapes as verglas-server (`json` for pipelines, `pretty` for local dev), the
//! same `RUST_LOG`/`VERGLAS_LOG_FORMAT` overrides, and the same non-blocking
//! writer so a stalled log consumer drops lines rather than back-pressuring a
//! serving task (a standing invariant: logging never blocks a fill).

use std::io::stderr;
use std::sync::OnceLock;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use verglas_core::config::LogFormat;

/// Env var overriding the configured format, matching verglas-server so a `VERGLAS_LOG_FORMAT=pretty`
/// works the same way against either binary.
const LOG_FORMAT_ENV: &str = "VERGLAS_LOG_FORMAT";

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

/// Builds the startup filter: `RUST_LOG` when set, otherwise the configured
/// level. An unparseable value in either falls back to `info` so a typo never
/// silences the server.
fn startup_filter(level: &str) -> EnvFilter {
    if let Ok(from_env) = EnvFilter::try_from_default_env() {
        return from_env;
    }
    EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Installs the process subscriber from the `[log]` config. Idempotent via
/// `try_init`: a second call (a test that already installed one) is ignored.
pub fn install(format: LogFormat, level: &str) {
    let (writer, guard) = tracing_appender::non_blocking(stderr());
    let _ = WRITER_GUARD.set(guard);
    let registry = tracing_subscriber::registry().with(startup_filter(level));
    let installed = match effective_format(format) {
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
        eprintln!("verglas-cache-node: failed to install tracing subscriber: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `VERGLAS_LOG_FORMAT` overrides the configured format both ways; an unset
    /// or unrecognised value keeps the configured one.
    #[test]
    fn env_overrides_configured_format() {
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
}
