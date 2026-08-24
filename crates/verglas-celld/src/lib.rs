//! Tenant-cell supervision for one Turso-backed Durable Object Worker per owner.
//!
//! Celld starts and fences resident processes. Cloud placement and the external
//! lease-validating Turso sync ingress own placement authority; celld has no
//! replica election or local CAS fallback.

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
pub use placement::HostId;
pub use provision::{
    ChildCommand, ChildDescriptor, ChildSpec, LocalProcessProvisioner, ProvisionError,
    ProvisionFuture, ProvisionHandle, ProvisionRequest, ProvisionedChild, Provisioner, TursoConfig,
    WorkerComponent, WorkerResourceLimits,
};
pub use supervisor::{ExitedChild, HostSupervisor, SupervisorError};
