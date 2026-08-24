//! Durable Object Worker ABI, event gating, and component artifact storage.
//!
//! The crate keeps Wasmtime bindings isolated from the pure event and artifact
//! infrastructure used by the per-object host.

pub mod abi;
pub mod artifact;
pub mod gate;
pub mod pool;
pub mod runtime;

pub use abi::{
    DurableObject, HostError, MAX_ATTACHMENT_SIZE, SocketId, WitHandlerError, WorkerBindings,
    WorkerHost, WorkerSockets, WorkerStorage, bindings,
};
pub use artifact::{ArtifactError, ArtifactStore, ComponentDigest, CwasmCache, DirArtifactStore};
pub use gate::{EventGate, EventPermit, StagingSockets};
pub use pool::{DoRouter, PoolError, WorkerPool};
pub use runtime::{PendingEvent, Request, Response, RuntimeError, WorkerRuntime};
