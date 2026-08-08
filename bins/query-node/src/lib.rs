//! The `verglas-query` library surface: the pieces of the query role that are
//! exercised directly by integration tests and shared with the binary.
//!
//! Mirrors `bins/verglas-server`'s lib+bin split for the same reason: `main.rs` owns
//! process startup and argv parsing, this library exposes the HTTP router,
//! config, sizing, and connection-building logic so tests can drive them
//! without spawning a process (though `tests/` also spawns the real binary for
//! the parts that need a real subprocess — see its module docs).
//!
//! - [`admin`]: the admin HTTP router (`/v1/query`, `/v1/query/estimate`,
//!   `/healthz`) and its request/response shapes.
//! - [`config`]: the `[listen]`/`[log]`/`[cache]`/`[metadata]` TOML schema.
//! - [`serve`]: startup wiring — opens the catalog, requests the initial
//!   grant, serves the router, shuts down on idle or a signal.
//! - [`sizing`]: turns a plan-based memory estimate into a grant request/grow
//!   call against `verglas_sdk::grant::MemoryGrantHost`.
//! - [`logging`]: the process-global tracing subscriber.

pub mod admin;
pub mod config;
pub mod logging;
pub mod serve;
pub mod sizing;

/// The binary version, from the package manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
