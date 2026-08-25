//! Substrate-agnostic supervision and fenced routing for one Turso Worker.
//!
//! Celld keeps exactly one active process owner for each Durable Object. Lease
//! validation and placement are cloud responsibilities, including the external
//! sync ingress that validates the current placement before forwarding Turso
//! pushes. No in-process CAS fallback exists here.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::alarm::{AlarmError, AlarmSchedule};
use crate::provision::{
    ChildCommand, ChildDescriptor, ChildSpec, LocalProcessProvisioner, ProvisionError,
    ProvisionRequest, ProvisionedChild, Provisioner,
};
use crate::{ChildLifecycle, ChildState, HostId, LifecycleError, SuspendFence};

/// One child observed exiting since the previous supervisor poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitedChild {
    do_id: String,
    status: std::process::ExitStatus,
}

impl ExitedChild {
    /// Returns the Durable Object whose process exited.
    pub fn do_id(&self) -> &str {
        &self.do_id
    }

    /// Returns the operating-system exit status.
    pub fn status(&self) -> std::process::ExitStatus {
        self.status
    }
}

/// A process, lifecycle, or substrate operation that failed closed.
#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    /// A lifecycle fence rejected the requested transition.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    /// The committed alarm deadline could not be converted into a timer delay.
    #[error(transparent)]
    Alarm(#[from] AlarmError),
    /// Alarm operations require an injected wake schedule.
    #[error("alarm schedule is not configured")]
    AlarmNotConfigured,
    /// Process or filesystem setup failed.
    #[error("child process I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// The DO identity could escape or alias its isolated directory.
    #[error("invalid Durable Object identity: {0}")]
    InvalidDoId(String),
    /// A launch field violates the one-path Turso contract.
    #[error("invalid Turso Worker launch: {0}")]
    InvalidLaunch(String),
    /// A component digest is not 64 hexadecimal characters.
    #[error("invalid component digest: {0}")]
    InvalidComponentDigest(String),
    /// A host capability declaration is outside the exact runtime contract.
    #[error("invalid host service declaration: {binding} -> {service}")]
    InvalidHostService {
        /// Environment binding named by the declaration.
        binding: String,
        /// Infrastructure service named by the declaration.
        service: String,
    },
    /// This host already supervises the DO.
    #[error("Durable Object {0} is already supervised on this host")]
    Duplicate(String),
    /// The requested DO is not assigned to this host.
    #[error("Durable Object {0} is not supervised on this host")]
    Unknown(String),
    /// The process exited during launch rather than becoming supervised.
    #[error("Durable Object {do_id} exited during launch with {status}")]
    Exited {
        /// DO whose child failed.
        do_id: String,
        /// Operating-system exit status.
        status: std::process::ExitStatus,
    },
    /// The child did not bind its exclusive event socket before the launch deadline.
    #[error("Durable Object {0} did not become event-socket-ready")]
    ReadinessTimeout(String),
    /// The child socket path was occupied by a non-socket filesystem object.
    #[error("Durable Object {0} produced an invalid Unix event socket path")]
    InvalidSocket(String),
    /// The lifecycle fence forbids routing this request to the child.
    #[error("Durable Object {0} is not eligible for this route")]
    RouteFenced(String),
    /// The selected compute substrate has not implemented one operation.
    #[error("provisioner operation unsupported: {0}")]
    UnsupportedProvisioner(&'static str),
}

impl From<ProvisionError> for SupervisorError {
    /// Preserves stable supervisor errors while translating substrate failures.
    fn from(error: ProvisionError) -> Self {
        match error {
            ProvisionError::Unsupported { operation } => Self::UnsupportedProvisioner(operation),
            ProvisionError::Io(error) => Self::Io(error),
            ProvisionError::Exited { do_id, status } => Self::Exited { do_id, status },
            ProvisionError::InvalidSocket(do_id) => Self::InvalidSocket(do_id),
            ProvisionError::ReadinessTimeout(do_id) => Self::ReadinessTimeout(do_id),
        }
    }
}

/// One tenant host's registry of isolated Turso Worker children.
pub struct HostSupervisor {
    host_id: HostId,
    root: PathBuf,
    command: ChildCommand,
    provisioner: Arc<dyn Provisioner>,
    alarm_schedule: Option<AlarmSchedule>,
    children: HashMap<String, ManagedChild>,
}

/// A supervised child and its lifecycle fence.
struct ManagedChild {
    spec: ChildSpec,
    lifecycle: ChildLifecycle,
    handle: Option<ProvisionedChild>,
    descriptor: ChildDescriptor,
}

impl HostSupervisor {
    /// Creates one host supervisor using the default local process substrate.
    pub fn new(host_id: HostId, root: impl AsRef<Path>, command: ChildCommand) -> Self {
        Self::with_provisioner(
            host_id,
            root,
            command,
            Arc::new(LocalProcessProvisioner::new()),
        )
    }

    /// Creates one host supervisor with an explicitly selected compute substrate.
    pub fn with_provisioner(
        host_id: HostId,
        root: impl AsRef<Path>,
        command: ChildCommand,
        provisioner: Arc<dyn Provisioner>,
    ) -> Self {
        Self {
            host_id,
            root: root.as_ref().to_path_buf(),
            command,
            provisioner,
            alarm_schedule: None,
            children: HashMap::new(),
        }
    }

    /// Adds the injected alarm schedule used by the routing layer's wake path.
    pub fn with_alarm_schedule(mut self, alarm_schedule: AlarmSchedule) -> Self {
        self.alarm_schedule = Some(alarm_schedule);
        self
    }

    /// Arms a derived deadline after the corresponding committed event.
    pub fn arm_alarm(
        &mut self,
        do_id: impl Into<String>,
        deadline_ms: u64,
    ) -> Result<(), SupervisorError> {
        let alarm_schedule = self
            .alarm_schedule
            .as_mut()
            .ok_or(SupervisorError::AlarmNotConfigured)?;
        alarm_schedule.arm(do_id, deadline_ms)?;
        Ok(())
    }

    /// Cancels a derived deadline without changing committed alarm state.
    pub fn disarm_alarm(&mut self, do_id: &str) -> Result<(), SupervisorError> {
        let alarm_schedule = self
            .alarm_schedule
            .as_mut()
            .ok_or(SupervisorError::AlarmNotConfigured)?;
        alarm_schedule.disarm(do_id);
        Ok(())
    }

    /// Launches one isolated Turso Worker and records its lifecycle fence.
    pub async fn spawn(&mut self, spec: ChildSpec) -> Result<ChildDescriptor, SupervisorError> {
        if self.children.contains_key(spec.do_id()) {
            return Err(SupervisorError::Duplicate(spec.do_id().to_owned()));
        }
        let child = self.launch(&spec).await?;
        let descriptor = child.descriptor().clone();
        self.children.insert(
            spec.do_id().to_owned(),
            ManagedChild {
                spec,
                lifecycle: ChildLifecycle::running(),
                handle: Some(child),
                descriptor: descriptor.clone(),
            },
        );
        Ok(descriptor)
    }

    /// Stops one Worker only after push, outbox drain, and clean event shutdown.
    pub async fn suspend(
        &mut self,
        do_id: &str,
        fence: SuspendFence,
    ) -> Result<(), SupervisorError> {
        let provisioner = Arc::clone(&self.provisioner);
        let managed = self
            .children
            .get_mut(do_id)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        let mut lifecycle = managed.lifecycle;
        lifecycle.suspend(fence)?;
        if let Some(mut process) = managed.handle.take() {
            provisioner
                .kill(&mut process)
                .await
                .map_err(SupervisorError::from)?;
            let _ = provisioner
                .wait(&mut process)
                .await
                .map_err(SupervisorError::from)?;
        }
        managed.lifecycle = lifecycle;
        Ok(())
    }

    /// Launches a suspended Worker without making it routable before readiness.
    pub async fn start_restore(&mut self, do_id: &str) -> Result<ChildDescriptor, SupervisorError> {
        let managed = self
            .children
            .get(do_id)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        let spec = managed.spec.clone();
        let mut lifecycle = managed.lifecycle;
        lifecycle.begin_restore()?;
        let child = self.launch(&spec).await?;
        let descriptor = child.descriptor().clone();
        let managed = self
            .children
            .get_mut(do_id)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        managed.lifecycle = lifecycle;
        managed.handle = Some(child);
        managed.descriptor = descriptor.clone();
        Ok(descriptor)
    }

    /// Makes a restored process routable after its event socket is ready.
    pub fn finish_restore(&mut self, do_id: &str) -> Result<(), SupervisorError> {
        let managed = self
            .children
            .get_mut(do_id)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        managed.lifecycle.finish_restore()?;
        Ok(())
    }

    /// Routes a serialized event only to the one running Worker owner.
    pub fn route_stateful(&mut self, do_id: &str) -> Result<PathBuf, SupervisorError> {
        self.poll_exited()?;
        let managed = self
            .children
            .get(do_id)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        if !managed.lifecycle.may_execute_event() || managed.handle.is_none() {
            return Err(SupervisorError::RouteFenced(do_id.to_owned()));
        }
        Ok(managed.descriptor.socket_path().to_path_buf())
    }

    /// Returns one supervised Worker's current lifecycle state.
    pub fn state(&self, do_id: &str) -> Option<ChildState> {
        self.children
            .get(do_id)
            .map(|child| child.lifecycle.state())
    }

    /// Returns the live child process identifier, if running or restoring.
    pub fn pid(&self, do_id: &str) -> Option<u32> {
        self.children
            .get(do_id)
            .and_then(|child| child.handle.as_ref())
            .map(ProvisionedChild::pid)
    }

    /// Reaps exited children immediately and fences routing behind recovery.
    pub fn poll_exited(&mut self) -> Result<Vec<ExitedChild>, SupervisorError> {
        let provisioner = Arc::clone(&self.provisioner);
        let mut exited = Vec::new();
        for managed in self.children.values_mut() {
            let status = match managed.handle.as_mut() {
                Some(process) => provisioner
                    .try_wait(process)
                    .map_err(SupervisorError::from)?,
                None => None,
            };
            if let Some(status) = status {
                managed.handle = None;
                if managed.lifecycle.state() == ChildState::Running {
                    managed.lifecycle.begin_crash_recovery()?;
                }
                exited.push(ExitedChild {
                    do_id: managed.spec.do_id().to_owned(),
                    status,
                });
            }
        }
        exited.sort_by(|left, right| left.do_id.cmp(&right.do_id));
        Ok(exited)
    }

    /// Stops every remaining child before the host process exits.
    pub async fn shutdown(&mut self) -> Result<(), SupervisorError> {
        let provisioner = Arc::clone(&self.provisioner);
        for managed in self.children.values_mut() {
            if let Some(mut process) = managed.handle.take() {
                provisioner
                    .kill(&mut process)
                    .await
                    .map_err(SupervisorError::from)?;
                let _ = provisioner
                    .wait(&mut process)
                    .await
                    .map_err(SupervisorError::from)?;
            }
        }
        Ok(())
    }

    /// Builds explicit substrate paths and waits for the provisioner's readiness fence.
    async fn launch(&self, spec: &ChildSpec) -> Result<ProvisionedChild, SupervisorError> {
        let request = ProvisionRequest::from_child(&self.command, &self.host_id, &self.root, spec)?;
        let mut child = self
            .provisioner
            .spawn(request)
            .await
            .map_err(SupervisorError::from)?;
        self.provisioner
            .await_ready(&mut child)
            .await
            .map_err(SupervisorError::from)?;
        Ok(child)
    }
}
