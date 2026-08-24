//! Compute-substrate provisioning contracts and the local-process implementation.
//!
//! The supervisor owns lifecycle fences while this module owns process handles,
//! readiness observation, termination, and exit reaping. Suspend and resume are
//! intentionally not trait operations yet: the supervisor has no caller that can
//! provide the machine snapshot contract, so the future Fly implementation is
//! documented in `fly` rather than represented by speculative methods.

use std::future::Future;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};

use crate::supervisor::SupervisorError;
use crate::{HostId, ReplicaRole};

/// Component instantiation can take minutes on cold local Wasmtime caches.
const CHILD_READINESS_TIMEOUT: Duration = Duration::from_secs(180);

/// A failure returned by a compute provisioner operation.
#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    /// The selected substrate does not implement this operation yet.
    #[error("provisioner operation unsupported: {operation}")]
    Unsupported {
        /// Stable operation name for a fail-closed error.
        operation: &'static str,
    },
    /// A local process or filesystem operation failed.
    #[error("child process I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// The process exited before binding its private socket.
    #[error("Durable Object {do_id} exited during launch with {status}")]
    Exited {
        /// Durable Object whose child failed during launch.
        do_id: String,
        /// Operating-system exit status.
        status: ExitStatus,
    },
    /// The child socket path was occupied by a non-socket filesystem object.
    #[error("Durable Object {0} produced an invalid Unix socket path")]
    InvalidSocket(String),
    /// The child did not bind its private socket before the launch deadline.
    #[error("Durable Object {0} did not become socket-ready")]
    ReadinessTimeout(String),
}

/// A boxed asynchronous result used by object-safe provisioner methods.
pub type ProvisionFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProvisionError>> + Send + 'a>>;

/// Substrate-owned process or machine handle operations.
pub trait ProvisionHandle: Send {
    /// Reaps an exited handle without waiting for a running handle.
    fn try_wait(&mut self) -> Result<Option<ExitStatus>, ProvisionError>;

    /// Sends the substrate's termination signal to the handle.
    fn kill<'a>(&'a mut self) -> ProvisionFuture<'a, ()>;

    /// Waits until the substrate reports the handle's exit status.
    fn wait<'a>(&'a mut self) -> ProvisionFuture<'a, ExitStatus>;
}

/// Descriptor and opaque handle returned by a successful substrate spawn.
pub struct ProvisionedChild {
    do_id: String,
    descriptor: ChildDescriptor,
    handle: Box<dyn ProvisionHandle>,
}

impl ProvisionedChild {
    /// Creates a child result that can be retained by the host supervisor.
    pub fn new(
        do_id: impl Into<String>,
        descriptor: ChildDescriptor,
        handle: Box<dyn ProvisionHandle>,
    ) -> Self {
        Self {
            do_id: do_id.into(),
            descriptor,
            handle,
        }
    }

    /// Returns the durable object identity used for launch errors.
    pub fn do_id(&self) -> &str {
        &self.do_id
    }

    /// Returns the stable substrate descriptor.
    pub fn descriptor(&self) -> &ChildDescriptor {
        &self.descriptor
    }

    /// Returns the operating-system process identifier recorded by the substrate.
    pub fn pid(&self) -> u32 {
        self.descriptor.pid()
    }

    /// Returns the private Worker socket path.
    pub fn socket_path(&self) -> &Path {
        self.descriptor.socket_path()
    }

    /// Returns the child data directory.
    pub fn data_dir(&self) -> &Path {
        self.descriptor.data_dir()
    }

    /// Borrows the opaque handle for provisioner operation dispatch.
    pub(crate) fn handle_mut(&mut self) -> &mut dyn ProvisionHandle {
        self.handle.as_mut()
    }
}

/// Program, paths, role, and held lease identity supplied to a provisioner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionRequest {
    program: PathBuf,
    args: Vec<String>,
    host_id: String,
    do_id: String,
    replica_id: u64,
    role: ReplicaRole,
    socket_path: PathBuf,
    data_dir: PathBuf,
    durability: Option<WorkerDurability>,
    component: Option<WorkerComponent>,
}

/// Tenant component identity and event ingress for one Worker child.
///
/// Present only on leader-role children: replicas never execute tenant code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerComponent {
    /// Lowercase SHA-256 hex identity of the component artifact.
    digest: String,
    /// Directory holding digest-named component artifacts.
    dir: PathBuf,
    /// Private Unix socket where the child serves the DO event protocol.
    event_socket: PathBuf,
}

impl WorkerComponent {
    /// Validates the digest shape and creates the component launch identity.
    pub fn new(
        digest: impl Into<String>,
        dir: impl Into<PathBuf>,
        event_socket: impl Into<PathBuf>,
    ) -> Result<Self, SupervisorError> {
        let digest = digest.into();
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(SupervisorError::InvalidComponentDigest(digest));
        }
        Ok(Self {
            digest,
            dir: dir.into(),
            event_socket: event_socket.into(),
        })
    }

    /// Returns the artifact digest in hex.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Returns the artifact directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the event-protocol socket path the child must bind.
    pub fn event_socket(&self) -> &Path {
        &self.event_socket
    }
}

impl ProvisionRequest {
    /// Creates a substrate request with explicit launch paths and lease identity.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        program: impl Into<PathBuf>,
        args: Vec<String>,
        host_id: impl Into<String>,
        do_id: impl Into<String>,
        replica_id: u64,
        role: ReplicaRole,
        socket_path: impl Into<PathBuf>,
        data_dir: impl Into<PathBuf>,
        durability: Option<WorkerDurability>,
    ) -> Self {
        Self {
            program: program.into(),
            args,
            host_id: host_id.into(),
            do_id: do_id.into(),
            replica_id,
            role,
            socket_path: socket_path.into(),
            data_dir: data_dir.into(),
            durability,
            component: None,
        }
    }

    /// Attaches the tenant component identity forwarded to the child.
    pub fn with_component(mut self, component: WorkerComponent) -> Self {
        self.component = Some(component);
        self
    }

    /// Returns the tenant component launch identity, if configured.
    pub fn component(&self) -> Option<&WorkerComponent> {
        self.component.as_ref()
    }

    /// Returns the executable selected for a local or remote launch.
    pub fn program(&self) -> &Path {
        &self.program
    }

    /// Returns fixed executable arguments that precede supervisor arguments.
    pub fn args(&self) -> &[String] {
        &self.args
    }

    /// Returns the host identity carried into the child environment.
    pub fn host_id(&self) -> &str {
        &self.host_id
    }

    /// Returns the Durable Object identity.
    pub fn do_id(&self) -> &str {
        &self.do_id
    }

    /// Returns the per-host replica identity.
    pub fn replica_id(&self) -> u64 {
        self.replica_id
    }

    /// Returns the role passed to the child runtime.
    pub fn role(&self) -> ReplicaRole {
        self.role
    }

    /// Returns the private Worker socket path.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Returns the isolated child data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns the already-held worker durability authority, if configured.
    pub fn durability(&self) -> Option<&WorkerDurability> {
        self.durability.as_ref()
    }

    /// Builds a request from the supervisor's stable child specification.
    pub(crate) fn from_child(
        command: &ChildCommand,
        host_id: &HostId,
        root: &Path,
        spec: &ChildSpec,
    ) -> Self {
        let data_dir = root
            .join(&spec.supervision_key)
            .join(spec.replica_id.to_string());
        let socket_path = data_dir.join("worker.sock");
        let mut request = Self::new(
            command.program.clone(),
            command.args.clone(),
            host_id.as_str(),
            spec.do_id.clone(),
            spec.replica_id,
            spec.role,
            socket_path,
            data_dir,
            spec.durability.clone(),
        );
        if let Some(component) = &spec.component {
            request = request.with_component(component.clone());
        }
        request
    }
}

/// Executable and fixed arguments used to launch every `verglasd` child.
#[derive(Debug, Clone)]
pub struct ChildCommand {
    program: PathBuf,
    args: Vec<String>,
}

impl ChildCommand {
    /// Creates a child command from one executable path.
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
        }
    }

    /// Appends one fixed executable argument.
    pub fn arg(mut self, argument: impl Into<String>) -> Self {
        self.args.push(argument.into());
        self
    }

    /// Returns the configured child executable.
    pub fn program(&self) -> &Path {
        &self.program
    }

    /// Returns the configured fixed arguments.
    pub fn args(&self) -> &[String] {
        &self.args
    }
}

/// Per-worker durability authority passed through without cloud composition logic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerDurability {
    /// One externally durable replica service and an already-held lease.
    Replica {
        /// Private replica service socket.
        socket: PathBuf,
        /// Opaque ownership token.
        lease_token: String,
        /// Monotonic ownership generation.
        generation: u64,
        /// Sequence the worker must recover before binding.
        start_sequence: u64,
        /// Managed compacted archive root; absent when offload is disabled.
        offload_dir: Option<PathBuf>,
    },
}

/// Validates one identity before it is used in a host-local path or process argument.
fn validate_identity(identity: String) -> Result<String, SupervisorError> {
    if identity.is_empty()
        || !identity
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || identity == "."
        || identity == ".."
    {
        return Err(SupervisorError::InvalidDoId(identity));
    }
    Ok(identity)
}

/// Durable identity and initial role of one host-local DO replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildSpec {
    do_id: String,
    /// Host-local key that separates the replica pager from the Worker pager.
    supervision_key: String,
    replica_id: u64,
    role: ReplicaRole,
    applied: u64,
    durability: Option<WorkerDurability>,
    component: Option<WorkerComponent>,
}

impl ChildSpec {
    /// Validates a filesystem-safe DO identity and creates its launch specification.
    pub fn new(
        do_id: impl Into<String>,
        replica_id: u64,
        role: ReplicaRole,
        applied: u64,
    ) -> Result<Self, SupervisorError> {
        let do_id = validate_identity(do_id.into())?;
        Ok(Self {
            supervision_key: do_id.clone(),
            do_id,
            replica_id,
            role,
            applied,
            durability: None,
            component: None,
        })
    }

    /// Gives a paired replica its own host-local pager and supervision slot.
    pub(crate) fn with_supervision_key(
        mut self,
        supervision_key: impl Into<String>,
    ) -> Result<Self, SupervisorError> {
        self.supervision_key = validate_identity(supervision_key.into())?;
        Ok(self)
    }

    /// Attaches the tenant component the leader child must load and serve.
    pub fn with_component(mut self, component: WorkerComponent) -> Result<Self, SupervisorError> {
        if self.role != ReplicaRole::Leader {
            return Err(SupervisorError::InvalidDurability(
                "replica-only child cannot execute tenant components".to_owned(),
            ));
        }
        self.component = Some(component);
        Ok(self)
    }

    /// Attaches the already-provisioned durability authority for one worker.
    pub fn with_durability(
        mut self,
        durability: WorkerDurability,
    ) -> Result<Self, SupervisorError> {
        if self.role != ReplicaRole::Leader {
            return Err(SupervisorError::InvalidDurability(
                "replica-only child cannot own worker durability".to_owned(),
            ));
        }
        match &durability {
            WorkerDurability::Replica {
                lease_token,
                start_sequence,
                ..
            } if lease_token.is_empty() || *start_sequence != self.applied => {
                return Err(SupervisorError::InvalidDurability(
                    "replica lease token must be nonempty and start at applied sequence".to_owned(),
                ));
            }
            WorkerDurability::Replica { .. } => {}
        }
        self.durability = Some(durability);
        Ok(self)
    }

    /// Returns the Durable Object identity.
    pub(crate) fn do_id(&self) -> &str {
        &self.do_id
    }

    /// Returns the host-local key used for this child process and pager.
    pub(crate) fn supervision_key(&self) -> &str {
        &self.supervision_key
    }

    /// Returns the initial replica role used by lifecycle fencing.
    pub(crate) fn role(&self) -> ReplicaRole {
        self.role
    }

    /// Returns the applied sequence used by lifecycle fencing.
    pub(crate) fn applied(&self) -> u64 {
        self.applied
    }

    /// Updates the role after a restore election has completed.
    pub(crate) fn set_role(&mut self, role: ReplicaRole) {
        self.role = role;
    }

    /// Updates the applied sequence after a restore fence has completed.
    pub(crate) fn set_applied(&mut self, applied: u64) {
        self.applied = applied;
    }
}

/// Stable process and isolation paths returned after a successful spawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildDescriptor {
    pid: u32,
    socket_path: PathBuf,
    data_dir: PathBuf,
}

impl ChildDescriptor {
    /// Creates a substrate descriptor for a process or machine endpoint.
    pub fn new(pid: u32, socket_path: impl Into<PathBuf>, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            pid,
            socket_path: socket_path.into(),
            data_dir: data_dir.into(),
        }
    }

    /// Returns the operating-system process identifier.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Returns the child-exclusive Worker Unix socket path.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Returns the child-exclusive SQLite, WAL, fragment, and checkpoint directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

/// Compute substrate operations required by the host lifecycle supervisor.
///
/// Suspension and resumption are deliberately not methods yet. The current
/// supervisor has only the fenced kill-and-wait and spawn callers; a future
/// machine-snapshot contract belongs here once those lifecycle callers exist.
pub trait Provisioner: Send + Sync {
    /// Starts one child or machine with its lease identity and isolation paths.
    fn spawn<'a>(&'a self, request: ProvisionRequest) -> ProvisionFuture<'a, ProvisionedChild>;

    /// Waits until the child has bound its private socket before publication.
    fn await_ready<'a>(&'a self, child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()>;

    /// Reaps an exited child without waiting for a running child.
    fn try_wait(&self, child: &mut ProvisionedChild) -> Result<Option<ExitStatus>, ProvisionError>;

    /// Sends the substrate termination operation after lifecycle fencing.
    fn kill<'a>(&'a self, child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()>;

    /// Waits for the substrate termination operation to be reaped.
    fn wait<'a>(&'a self, child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ExitStatus>;
}

/// Local child-process substrate used by development, tests, and `celld-host`.
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalProcessProvisioner;

impl LocalProcessProvisioner {
    /// Creates the local process provisioner without external state.
    pub const fn new() -> Self {
        Self
    }
}

/// A Tokio child process retained behind the provisioner handle seam.
struct LocalProcessHandle {
    process: Child,
}

impl ProvisionHandle for LocalProcessHandle {
    /// Reaps an exited local child without waiting for a running child.
    fn try_wait(&mut self) -> Result<Option<ExitStatus>, ProvisionError> {
        self.process.try_wait().map_err(ProvisionError::from)
    }

    /// Sends SIGKILL to the local child.
    fn kill<'a>(&'a mut self) -> ProvisionFuture<'a, ()> {
        Box::pin(async move { self.process.kill().await.map_err(ProvisionError::from) })
    }

    /// Waits for the local child after termination.
    fn wait<'a>(&'a mut self) -> ProvisionFuture<'a, ExitStatus> {
        Box::pin(async move { self.process.wait().await.map_err(ProvisionError::from) })
    }
}

impl Provisioner for LocalProcessProvisioner {
    /// Creates one isolated local process and returns its opaque handle.
    fn spawn<'a>(&'a self, request: ProvisionRequest) -> ProvisionFuture<'a, ProvisionedChild> {
        Box::pin(async move {
            tokio::fs::create_dir_all(request.data_dir()).await?;
            match tokio::fs::remove_file(request.socket_path()).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(ProvisionError::from(error)),
            }
            let role = match request.role() {
                ReplicaRole::Leader => "worker",
                ReplicaRole::Follower => "replica",
            };
            let mut command = Command::new(request.program());
            command
                .args(request.args())
                .arg("--do-id")
                .arg(request.do_id())
                .arg("--replica-id")
                .arg(request.replica_id().to_string())
                .arg("--role")
                .arg(role)
                .arg("--socket")
                .arg(request.socket_path())
                .arg("--data-dir")
                .arg(request.data_dir())
                .env("VERGLAS_CELL_HOST", request.host_id())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            if let Some(WorkerDurability::Replica {
                socket,
                lease_token,
                generation,
                start_sequence,
                offload_dir,
            }) = request.durability()
            {
                command
                    .arg("--replica-socket")
                    .arg(socket)
                    .arg("--lease-token")
                    .arg(lease_token)
                    .arg("--lease-generation")
                    .arg(generation.to_string())
                    .arg("--start-sequence")
                    .arg(start_sequence.to_string());
                if let Some(offload_dir) = offload_dir {
                    command.arg("--offload-dir").arg(offload_dir);
                }
            }
            if let Some(component) = request.component() {
                match tokio::fs::remove_file(component.event_socket()).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(ProvisionError::from(error)),
                }
                command
                    .arg("--component-digest")
                    .arg(component.digest())
                    .arg("--component-dir")
                    .arg(component.dir())
                    .arg("--event-socket")
                    .arg(component.event_socket());
            }
            let process = command.spawn()?;
            let pid = process.id().ok_or_else(|| {
                ProvisionError::Io(std::io::Error::other("spawned process has no pid"))
            })?;
            let descriptor = ChildDescriptor::new(
                pid,
                request.socket_path().to_path_buf(),
                request.data_dir().to_path_buf(),
            );
            Ok(ProvisionedChild::new(
                request.do_id().to_owned(),
                descriptor,
                Box::new(LocalProcessHandle { process }),
            ))
        })
    }

    /// Waits for a local child socket and fails closed on early exit or bad paths.
    fn await_ready<'a>(&'a self, child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()> {
        Box::pin(async move {
            let deadline = Instant::now() + CHILD_READINESS_TIMEOUT;
            loop {
                if let Some(status) = self.try_wait(child)? {
                    return Err(ProvisionError::Exited {
                        do_id: child.do_id().to_owned(),
                        status,
                    });
                }
                match tokio::fs::symlink_metadata(child.socket_path()).await {
                    Ok(metadata) if metadata.file_type().is_socket() => return Ok(()),
                    Ok(_) => {
                        let _ = self.kill(child).await;
                        return Err(ProvisionError::InvalidSocket(child.do_id().to_owned()));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(ProvisionError::from(error)),
                }
                if Instant::now() >= deadline {
                    let _ = self.kill(child).await;
                    return Err(ProvisionError::ReadinessTimeout(child.do_id().to_owned()));
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    }

    /// Reaps a local process without waiting for a running process.
    fn try_wait(&self, child: &mut ProvisionedChild) -> Result<Option<ExitStatus>, ProvisionError> {
        child.handle_mut().try_wait()
    }

    /// Kills a local process through its opaque handle.
    fn kill<'a>(&'a self, child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()> {
        Box::pin(async move { child.handle_mut().kill().await })
    }

    /// Waits for a local process to finish after termination.
    fn wait<'a>(&'a self, child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ExitStatus> {
        Box::pin(async move { child.handle_mut().wait().await })
    }
}
