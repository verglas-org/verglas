//! The `verglasd` library surface: the pieces of the daemon that are exercised
//! directly by integration tests and shared between the binary and its tests.
//!
//! The daemon binary (`main.rs`) owns process startup, the cache engine, and the
//! listeners; this library exposes the request-handling and platform-execution
//! layers so they can be tested without spawning a process:
//!
//! - [`admin`]: the admin HTTP router and its route handlers, including the
//!   `verglas_sys` registry, watermark, and platform queue routes.
//! - [`platform`]: the daemon-hosted harness executors (#328) — the
//!   [`platform::SysWatermarkStore`] over `verglas_sys.watermarks` and the
//!   registry-driven supervisor that runs each active local deployment's
//!   executor.
//! - [`logging`]: the process-global tracing subscriber and its reloadable
//!   level filter.
//! - [`shadow`]: the adapter bridging `verglas_vector`'s `ShadowBlobStore` seam
//!   onto the cache-managed shadow store in `verglas_cache` (#95), so the vector
//!   index's Puffin blobs land in the real NVMe-resident store.
//! - [`query_worker`]: dispatches `POST /v1/query` to a standalone
//!   `verglas-query` worker when `[query_worker]` is configured. Opt-in; when
//!   configured it is the sole query engine (no embedded fallback on
//!   dispatch failure). When unset, `/v1/query` stays on the embedded engine.

pub mod admin;
pub mod follow;
pub mod logging;
pub mod node_report;
pub mod platform;
pub mod query_worker;
pub mod shadow;
pub mod write_worker;

/// The daemon version, from the package manifest. Reported by `/admin/version`
/// and stamped on operator log lines.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
