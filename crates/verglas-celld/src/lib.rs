//! Tenant cell supervision and Durable Object placement.
//!
//! One `celld-host` process uses this library to supervise many single-group
//! `verglasd` children while preserving hard resource ceilings and balanced
//! leader placement across the tenant's three hosts.

mod control;
mod lifecycle;
mod management;
mod placement;
mod supervisor;

pub use control::{ControlError, ControlServer, SupervisorHandle};
pub use lifecycle::{ChildLifecycle, ChildState, LifecycleError, SuspendFence};
pub use management::ManagementApi;
pub use placement::{
    DoLoad, GroupPlacement, HostCapacity, HostId, HostLoad, PlacementError, PlacementPlanner,
    ReplicaPlacement, ReplicaRole,
};
pub use supervisor::{
    ChildCommand, ChildDescriptor, ChildSpec, ExitedChild, HostSupervisor, ManagedCasConfig,
    SupervisorError, WorkerDurability, WorkerResourceLimits,
};
