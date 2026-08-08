//! The `verglas-server` library surface: the pieces of the server that are exercised
//! directly by integration tests and shared between the binary and its tests.
//!
//! The server binary (`main.rs`) owns process startup, the cache engine, and the
//! listeners; this library exposes the request-handling and platform-execution
//! layers so they can be tested without spawning a process:
//!
//! - [`admin`]: the admin HTTP router and its route handlers, including the
//!   `verglas_sys` registry and platform queue routes.
//! - [`platform`]: optional REST and catalog trigger ingress into the external
//!   scheduler service.
//! - [`logging`]: the process-global tracing subscriber and its reloadable
//!   level filter.
//! - [`query_worker`]: dispatches `POST /v1/query` to a standalone
//!   `verglas-query` worker when `[query_worker]` is configured. Opt-in; when
//!   configured it is the sole query engine (no embedded fallback on
//!   dispatch failure). When unset, `/v1/query` stays on the embedded engine.

pub use verglas_rest::{admin, follow, logging, platform, query_worker, write_worker};

/// The server version, from the package manifest. Reported by `/admin/version`
/// and stamped on operator log lines.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
