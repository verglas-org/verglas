//! Reusable host adapters shared by the isolated Durable Object runtime binaries.
//!
//! The binaries own process supervision and endpoint serving; this library owns
//! the in-process capability adapters passed into the Worker component.

pub mod catalog_commit;
pub mod event_endpoint;
pub mod host_config;
pub mod origin_storage;
pub mod worker_storage;

pub use catalog_commit::{
    CatalogCommitService, CatalogCommitServiceConfig, IcebergCatalogCommitService,
    MAX_CATALOG_COMMIT_BODY_BYTES,
};
pub use event_endpoint::{EventDispatcher, EventEndpoint, EventEndpointError};
pub use host_config::{
    CatalogHostConfig, CatalogHostConfigError, CatalogOriginConfig, DurableHostState, SinkFence,
};
pub use origin_storage::{OriginStorageConfig, OriginStorageError, OriginStorageFactory};
pub use worker_storage::{BindingStreamAppender, TursoWorkerStorage};
