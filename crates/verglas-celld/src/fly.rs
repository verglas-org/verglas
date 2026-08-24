//! Fail-closed Fly.io Machines provisioner configuration and extension point.
//!
//! A real `spawn` will call authenticated Machines create/start REST with the held
//! lease, paths, size, and region. `await_ready` will wait for the private Worker
//! socket; `kill` and `wait` will issue stop REST and observe completion. A future
//! suspend/resume extension will archive and checkpoint before suspend REST, resume
//! the machine snapshot on wake-on-request, restore the archive tail, and release
//! the route fence as required by `docs/architecture/do-workers.mdx`; this skeleton
//! returns `ProvisionError::Unsupported` for every operation until those guarantees
//! exist.

use std::process::ExitStatus;

use crate::{ProvisionError, ProvisionFuture, ProvisionRequest, ProvisionedChild, Provisioner};

/// Source descriptor for the token used by Fly Machines API calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlyAuthTokenSource {
    /// Read the token from one named environment variable.
    EnvironmentVariable(String),
}

/// Typed machine size fields retained for the future Machines create request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlyMachineSize {
    cpu_kind: String,
    vcpus: u16,
    memory_mib: u64,
}

impl FlyMachineSize {
    /// Creates the CPU kind, virtual CPU count, and memory reservation.
    pub fn new(cpu_kind: impl Into<String>, vcpus: u16, memory_mib: u64) -> Self {
        Self {
            cpu_kind: cpu_kind.into(),
            vcpus,
            memory_mib,
        }
    }

    /// Returns the Fly CPU kind.
    pub fn cpu_kind(&self) -> &str {
        &self.cpu_kind
    }

    /// Returns the requested virtual CPU count.
    pub fn vcpus(&self) -> u16 {
        self.vcpus
    }

    /// Returns the requested memory reservation in MiB.
    pub fn memory_mib(&self) -> u64 {
        self.memory_mib
    }
}

/// Configuration needed for a Fly Machines provisioner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlyMachinesConfig {
    api_base_url: String,
    app_name: String,
    auth_token_source: FlyAuthTokenSource,
    machine_size: FlyMachineSize,
    region: String,
}

impl FlyMachinesConfig {
    /// Creates typed API, application, authentication, size, and region fields.
    pub fn new(
        api_base_url: impl Into<String>,
        app_name: impl Into<String>,
        auth_token_source: FlyAuthTokenSource,
        machine_size: FlyMachineSize,
        region: impl Into<String>,
    ) -> Self {
        Self {
            api_base_url: api_base_url.into(),
            app_name: app_name.into(),
            auth_token_source,
            machine_size,
            region: region.into(),
        }
    }

    /// Returns the Machines API base URL.
    pub fn api_base_url(&self) -> &str {
        &self.api_base_url
    }

    /// Returns the Fly application name.
    pub fn app_name(&self) -> &str {
        &self.app_name
    }

    /// Returns the configured token source without resolving secrets.
    pub fn auth_token_source(&self) -> &FlyAuthTokenSource {
        &self.auth_token_source
    }

    /// Returns the requested machine size.
    pub fn machine_size(&self) -> &FlyMachineSize {
        &self.machine_size
    }

    /// Returns the requested Fly region.
    pub fn region(&self) -> &str {
        &self.region
    }
}

/// Fly Machines provisioner that fails closed until REST lifecycle semantics land.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlyMachinesProvisioner {
    config: FlyMachinesConfig,
}

impl FlyMachinesProvisioner {
    /// Creates a fail-closed provisioner with typed Machines configuration.
    pub fn new(config: FlyMachinesConfig) -> Self {
        Self { config }
    }

    /// Returns the immutable configuration planned for Machines requests.
    pub fn config(&self) -> &FlyMachinesConfig {
        &self.config
    }
}

impl Provisioner for FlyMachinesProvisioner {
    /// Rejects machine creation until the authenticated Machines REST call exists.
    fn spawn<'a>(&'a self, _request: ProvisionRequest) -> ProvisionFuture<'a, ProvisionedChild> {
        Box::pin(async {
            Err(ProvisionError::Unsupported {
                operation: "Fly Machines create/start",
            })
        })
    }

    /// Rejects readiness observation until machine socket monitoring exists.
    fn await_ready<'a>(&'a self, _child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()> {
        Box::pin(async {
            Err(ProvisionError::Unsupported {
                operation: "Fly Machines await-ready",
            })
        })
    }

    /// Rejects exit reaping until Machines lifecycle observation exists.
    fn try_wait(
        &self,
        _child: &mut ProvisionedChild,
    ) -> Result<Option<ExitStatus>, ProvisionError> {
        Err(ProvisionError::Unsupported {
            operation: "Fly Machines reap",
        })
    }

    /// Rejects machine stopping until the authenticated REST call exists.
    fn kill<'a>(&'a self, _child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()> {
        Box::pin(async {
            Err(ProvisionError::Unsupported {
                operation: "Fly Machines stop/suspend",
            })
        })
    }

    /// Rejects machine wait until stop completion observation exists.
    fn wait<'a>(&'a self, _child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ExitStatus> {
        Box::pin(async {
            Err(ProvisionError::Unsupported {
                operation: "Fly Machines wait",
            })
        })
    }
}
