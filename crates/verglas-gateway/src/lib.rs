//! OSS Durable Object gateway for wrangler-style manifests.
//!
//! The library owns HTTP and WebSocket connections, launches one resident DO
//! through celld, and exchanges frozen NDJSON events over its private socket.

mod connection;
mod error;
mod gateway;
mod manifest;
mod protocol;
mod spawn;

pub use error::GatewayError;
pub use gateway::Gateway;
pub use manifest::{Binding, Manifest, ManifestError};
pub use spawn::{CelldSpawner, DoSpawner, SpawnRequest};
