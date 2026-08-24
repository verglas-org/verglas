//! Tenant cell supervision and Durable Object placement.
//!
//! One `celld-host` process uses this library to supervise many single-group
//! `verglasd` children while preserving hard resource ceilings and balanced
//! leader placement across the tenant's three hosts.

mod alarm;
mod control;
mod fly;
mod lifecycle;
mod management;
mod placement;
mod provision;
mod supervisor;

pub use alarm::{AlarmError, AlarmFuture, AlarmSchedule, AlarmWake};
pub use control::{ControlError, ControlServer, SharedControlServer};
pub use fly::{FlyAuthTokenSource, FlyMachineSize, FlyMachinesConfig, FlyMachinesProvisioner};
pub use lifecycle::{ChildLifecycle, ChildState, LifecycleError, SuspendFence};
pub use management::ManagementApi;
pub use placement::{
    DoLoad, GroupPlacement, HostCapacity, HostId, HostLoad, PlacementError, PlacementPlanner,
    ReplicaPlacement, ReplicaRole,
};
pub use provision::{
    ChildCommand, ChildDescriptor, ChildSpec, LocalProcessProvisioner, ProvisionError,
    ProvisionFuture, ProvisionHandle, ProvisionRequest, ProvisionedChild, Provisioner,
    WorkerComponent, WorkerDurability, WorkerResourceLimits,
};
pub use supervisor::{ExitedChild, HostSupervisor, SupervisorError};
