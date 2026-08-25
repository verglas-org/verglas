//! Tenant-cell supervision for one Durable Object Worker per owner.
//!
//! Celld starts and fences resident processes. Placement is an external
//! responsibility; celld has no replica election or local CAS fallback.

mod alarm;
mod control;
mod lifecycle;
mod management;
mod placement;
mod provision;
mod supervisor;

pub use alarm::{AlarmError, AlarmFuture, AlarmSchedule, AlarmWake};
pub use control::{ControlError, ControlServer, SharedControlServer};
pub use lifecycle::{ChildLifecycle, ChildState, LifecycleError, SuspendFence};
pub use management::ManagementApi;
pub use placement::HostId;
pub use provision::{
    ChildCommand, ChildDescriptor, ChildSpec, HostServiceBinding, LocalProcessProvisioner,
    ProvisionError, ProvisionFuture, ProvisionHandle, ProvisionRequest, ProvisionedChild,
    Provisioner, WorkerComponent, WorkerResourceLimits,
};
pub use supervisor::{ExitedChild, HostSupervisor, SupervisorError};
