//! Reusable host adapters shared by the isolated Durable Object runtime binaries.
//!
//! The binaries own process supervision and endpoint serving; this library owns
//! the in-process capability adapters passed into the Worker component.

pub mod event_endpoint;
pub mod worker_storage;

pub use event_endpoint::{EventDispatcher, EventEndpoint, EventEndpointError};
pub use worker_storage::TursoWorkerStorage;
