//! Substrate-agnostic child lifecycle supervision and fenced Unix-socket routing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::alarm::{AlarmError, AlarmSchedule};
use crate::provision::{
    ChildCommand, ChildDescriptor, ChildSpec, LocalProcessProvisioner, ProvisionError,
    ProvisionRequest, ProvisionedChild, Provisioner,
};
use crate::{
    ChildLifecycle, ChildState, HostId, LifecycleError, ReplicaRole, SuspendFence, WorkerDurability,
};

/// One child observed exiting since the previous supervisor poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitedChild {
    do_id: String,
    status: std::process::ExitStatus,
}

impl ExitedChild {
    /// Returns the Durable Object whose replica exited.
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
    /// Worker durability arguments violate the child role or recovery fence.
    #[error("invalid worker durability: {0}")]
    InvalidDurability(String),
    /// A component digest is not 64 hexadecimal characters.
    #[error("invalid component digest: {0}")]
    InvalidComponentDigest(String),
    /// This host already supervises the DO replica.
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
    /// The child did not bind its exclusive Unix socket before the launch deadline.
    #[error("Durable Object {0} did not become socket-ready")]
    ReadinessTimeout(String),
    /// The child socket path was occupied by a non-socket filesystem object.
    #[error("Durable Object {0} produced an invalid Unix socket path")]
    InvalidSocket(String),
    /// Recovery completion disagrees with the role used to launch the process.
    #[error("Durable Object {0} restore role does not match its child process")]
    RoleMismatch(String),
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

/// One tenant host's registry of isolated single-Raft-group children.
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

    /// Selects one logical DO child, preferring the stateful Worker over its replica.
    fn key_for(&self, do_id: &str, role: Option<ReplicaRole>) -> Option<String> {
        self.children
            .iter()
            .filter(|(_, managed)| {
                managed.spec.do_id() == do_id
                    && role.is_none_or(|required| managed.spec.role() == required)
            })
            .min_by(|(left_key, left), (right_key, right)| {
                child_role_rank(left.spec.role())
                    .cmp(&child_role_rank(right.spec.role()))
                    .then_with(|| left_key.cmp(right_key))
            })
            .map(|(key, _)| key.clone())
    }

    /// Launches one isolated replica and records its lifecycle fence.
    pub async fn spawn(&mut self, spec: ChildSpec) -> Result<ChildDescriptor, SupervisorError> {
        if self.children.contains_key(spec.supervision_key()) {
            return Err(SupervisorError::Duplicate(spec.do_id().to_owned()));
        }
        let child = self.launch(&spec).await?;
        let descriptor = child.descriptor().clone();
        let lifecycle = ChildLifecycle::running(spec.role(), spec.applied());
        self.children.insert(
            spec.supervision_key().to_owned(),
            ManagedChild {
                spec,
                lifecycle,
                handle: Some(child),
                descriptor: descriptor.clone(),
            },
        );
        Ok(descriptor)
    }

    /// Drains, checkpoints, covers, cleans, and then stops one Worker replica.
    pub async fn suspend_orchestrated(&mut self, do_id: &str) -> Result<(), SupervisorError> {
        let key = self
            .key_for(do_id, Some(ReplicaRole::Leader))
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        let (worker_path, replica_path, generation, token, applied) = {
            let managed = self
                .children
                .get(&key)
                .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
            let Some(WorkerDurability::Replica {
                socket,
                lease_token,
                generation,
                ..
            }) = managed.spec.durability()
            else {
                return Err(SupervisorError::InvalidDurability(
                    "orchestrated suspend requires replica durability".to_owned(),
                ));
            };
            (
                managed.descriptor.socket_path().to_path_buf(),
                socket.clone(),
                *generation,
                lease_token.clone(),
                managed.spec.applied(),
            )
        };
        endpoint_command(&worker_path, "DRAIN").await?;
        endpoint_command(&worker_path, "CHECKPOINT").await?;
        let token = hex::encode(token);
        let identity = hex::encode(do_id);
        endpoint_command(
            &replica_path,
            &format!("REPLICA_COVER {generation} {token} {applied} {applied} {identity}"),
        )
        .await?;
        endpoint_command(
            &replica_path,
            &format!("REPLICA_CLEAN {generation} {token} {applied}"),
        )
        .await?;
        self.suspend(do_id, SuspendFence::new(applied, applied, applied))
            .await
    }

    /// Stops one replica only after archive and checkpoint fences cover applied state.
    pub async fn suspend(
        &mut self,
        do_id: &str,
        fence: SuspendFence,
    ) -> Result<(), SupervisorError> {
        let provisioner = Arc::clone(&self.provisioner);
        let key = self
            .key_for(do_id, None)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        let managed = self
            .children
            .get_mut(&key)
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

    /// Launches a suspended replica in restore mode without making it routable.
    pub async fn start_restore(
        &mut self,
        do_id: &str,
        required: u64,
        role: ReplicaRole,
    ) -> Result<ChildDescriptor, SupervisorError> {
        let key = self
            .key_for(do_id, None)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        let managed = self
            .children
            .get(&key)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        let mut spec = managed.spec.clone();
        spec.set_role(role);
        let mut lifecycle = managed.lifecycle;
        lifecycle.begin_restore(required)?;
        let child = self.launch(&spec).await?;
        let descriptor = child.descriptor().clone();
        let managed = self
            .children
            .get_mut(&key)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        managed.lifecycle = lifecycle;
        managed.spec = spec;
        managed.handle = Some(child);
        managed.descriptor = descriptor.clone();
        Ok(descriptor)
    }

    /// Makes a restored process routable after it reaches the ingress fence.
    ///
    /// The routing layer must re-read committed alarm state and call
    /// [`Self::arm_alarm`] after this fence; an in-memory deadline is not authoritative.
    pub fn finish_restore(
        &mut self,
        do_id: &str,
        role: ReplicaRole,
        restored: u64,
    ) -> Result<(), SupervisorError> {
        let key = self
            .key_for(do_id, None)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        let managed = self
            .children
            .get_mut(&key)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        if managed.spec.role() != role {
            return Err(SupervisorError::RoleMismatch(do_id.to_owned()));
        }
        managed.lifecycle.finish_restore(role, restored)?;
        managed.spec.set_role(role);
        managed.spec.set_applied(restored);
        Ok(())
    }

    /// Routes a stateful Worker event only to the running local leader.
    pub fn route_stateful(&mut self, do_id: &str) -> Result<PathBuf, SupervisorError> {
        self.poll_exited()?;
        let key = self
            .key_for(do_id, Some(ReplicaRole::Leader))
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        let managed = self
            .children
            .get(&key)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        if !managed.lifecycle.may_execute_stateful_event() || managed.handle.is_none() {
            return Err(SupervisorError::RouteFenced(do_id.to_owned()));
        }
        Ok(managed.descriptor.socket_path().to_path_buf())
    }

    /// Routes a fenced snapshot read to any sufficiently applied running replica.
    pub fn route_snapshot(
        &mut self,
        do_id: &str,
        requested: u64,
    ) -> Result<PathBuf, SupervisorError> {
        self.poll_exited()?;
        let key = self
            .key_for(do_id, None)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        let managed = self
            .children
            .get(&key)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        if !managed.lifecycle.may_serve_snapshot(requested) || managed.handle.is_none() {
            return Err(SupervisorError::RouteFenced(do_id.to_owned()));
        }
        Ok(managed.descriptor.socket_path().to_path_buf())
    }

    /// Returns one supervised replica's current lifecycle state.
    pub fn state(&self, do_id: &str) -> Option<ChildState> {
        self.key_for(do_id, None)
            .and_then(|key| self.children.get(&key))
            .map(|child| child.lifecycle.state())
    }

    /// Returns the live child process identifier, if running or restoring.
    pub fn pid(&self, do_id: &str) -> Option<u32> {
        self.key_for(do_id, Some(ReplicaRole::Leader))
            .or_else(|| self.key_for(do_id, None))
            .and_then(|key| self.children.get(&key))
            .and_then(|child| child.handle.as_ref())
            .map(ProvisionedChild::pid)
    }

    /// Reaps exited children immediately and fences every route behind recovery.
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
                if matches!(managed.lifecycle.state(), ChildState::Running(_)) {
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
        let request = ProvisionRequest::from_child(&self.command, &self.host_id, &self.root, spec);
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

/// Sends one lifecycle command and requires an explicit successful response.
async fn endpoint_command(path: &Path, command: &str) -> Result<(), SupervisorError> {
    let mut stream = UnixStream::connect(path)
        .await
        .map_err(SupervisorError::Io)?;
    stream
        .write_all(format!("{command}\n").as_bytes())
        .await
        .map_err(SupervisorError::Io)?;
    let mut response = String::new();
    BufReader::new(stream)
        .read_line(&mut response)
        .await
        .map_err(SupervisorError::Io)?;
    if response.starts_with("OK") {
        Ok(())
    } else {
        Err(SupervisorError::InvalidDurability(format!(
            "endpoint command {command} failed: {}",
            response.trim()
        )))
    }
}

/// Orders the stateful Worker before its paired durability replica.
fn child_role_rank(role: ReplicaRole) -> u8 {
    match role {
        ReplicaRole::Leader => 0,
        ReplicaRole::Follower => 1,
    }
}
