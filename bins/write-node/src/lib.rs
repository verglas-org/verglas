//! The isolated `verglas-write` role library surface.

pub mod admin;
pub mod config;
pub mod serve;

/// The binary version from the package manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
