//! Concrete `celld-host` child-process supervision and fenced Unix-socket routing.

use std::collections::HashMap;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};

use crate::{ChildLifecycle, ChildState, HostId, LifecycleError, ReplicaRole, SuspendFence};

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
}

/// Explicit managed object-store connection fields for one CAS worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedCasConfig {
    /// S3-compatible HTTP endpoint, including its scheme and authority.
    pub endpoint: String,
    /// Managed bucket containing the DO head and immutable objects.
    pub bucket: String,
    /// Verglas-owned object prefix within the managed bucket.
    pub prefix: String,
    /// AWS signing region used by the object-store client.
    pub region: String,
    /// Access key used to sign managed object requests.
    pub access_key_id: String,
    /// Secret key used to sign managed object requests.
    pub secret_access_key: String,
}

impl ManagedCasConfig {
    /// Creates one explicit managed object-store configuration.
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            prefix: prefix.into(),
            region: region.into(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        }
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
    /// A lease-fenced managed S3 CAS head and immutable transaction stream.
    ManagedCas {
        /// Explicit managed object-store connection settings.
        store: ManagedCasConfig,
        /// Opaque ownership token held by the launcher.
        lease_token: String,
        /// Monotonic ownership generation held by the launcher.
        generation: u64,
        /// Sequence the worker must recover before binding.
        start_sequence: u64,
        /// ETag of the head version held by the launcher, when supplied.
        lease_etag: Option<String>,
        /// Version ID of the head version held by the launcher, when supplied.
        lease_version: Option<String>,
    },
}

/// Hard operating-system ceilings applied before a worker process runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerResourceLimits {
    memory_ceiling_bytes: u64,
    open_files_ceiling: u64,
}

impl WorkerResourceLimits {
    /// Creates validated memory and descriptor ceilings for one child process.
    pub fn new(
        memory_ceiling_bytes: u64,
        open_files_ceiling: u64,
    ) -> Result<Self, SupervisorError> {
        if memory_ceiling_bytes == 0 || open_files_ceiling < 3 {
            return Err(SupervisorError::InvalidResourceLimits);
        }
        Ok(Self {
            memory_ceiling_bytes,
            open_files_ceiling,
        })
    }

    /// Returns the address-space ceiling in bytes.
    pub fn memory_ceiling_bytes(self) -> u64 {
        self.memory_ceiling_bytes
    }

    /// Returns the maximum number of simultaneously open file descriptors.
    pub fn open_files_ceiling(self) -> u64 {
        self.open_files_ceiling
    }
}

impl Default for WorkerResourceLimits {
    fn default() -> Self {
        Self {
            memory_ceiling_bytes: 4 * 1024 * 1024 * 1024,
            open_files_ceiling: 1024,
        }
    }
}

/// Durable identity and initial role of one host-local DO replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildSpec {
    do_id: String,
    replica_id: u64,
    role: ReplicaRole,
    applied: u64,
    durability: Option<WorkerDurability>,
    resource_limits: WorkerResourceLimits,
}

impl ChildSpec {
    /// Validates a filesystem-safe DO identity and creates its launch specification.
    pub fn new(
        do_id: impl Into<String>,
        replica_id: u64,
        role: ReplicaRole,
        applied: u64,
    ) -> Result<Self, SupervisorError> {
        let do_id = do_id.into();
        if do_id.is_empty()
            || !do_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || do_id == "."
            || do_id == ".."
        {
            return Err(SupervisorError::InvalidDoId(do_id));
        }
        Ok(Self {
            do_id,
            replica_id,
            role,
            applied,
            durability: None,
            resource_limits: WorkerResourceLimits::default(),
        })
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
            WorkerDurability::ManagedCas {
                store,
                lease_token,
                lease_etag,
                lease_version,
                start_sequence,
                ..
            } if store.endpoint.is_empty()
                || store.bucket.is_empty()
                || store.region.is_empty()
                || store.access_key_id.is_empty()
                || store.secret_access_key.is_empty()
                || lease_token.is_empty()
                || (lease_etag.is_none() && lease_version.is_none())
                || *start_sequence != self.applied =>
            {
                return Err(SupervisorError::InvalidDurability(
                    "managed CAS requires store credentials, one held head version, and matching start sequence"
                        .to_owned(),
                ));
            }
            WorkerDurability::ManagedCas { .. } => {}
        }
        self.durability = Some(durability);
        Ok(self)
    }

    /// Applies validated hard resource ceilings to this child launch.
    pub fn with_resource_limits(mut self, resource_limits: WorkerResourceLimits) -> Self {
        self.resource_limits = resource_limits;
        self
    }

    /// Returns the hard resource ceilings that will be applied at spawn.
    pub fn resource_limits(&self) -> WorkerResourceLimits {
        self.resource_limits
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

/// A process or lifecycle operation that failed closed.
#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    /// A lifecycle fence rejected the requested transition.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    /// Process or filesystem setup failed.
    #[error("child process I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// The DO identity could escape or alias its isolated directory.
    #[error("invalid Durable Object identity: {0}")]
    InvalidDoId(String),
    /// Worker durability arguments violate the child role or recovery fence.
    #[error("invalid worker durability: {0}")]
    InvalidDurability(String),
    /// Resource ceilings are zero or leave no descriptors for the worker runtime.
    #[error("invalid worker resource limits")]
    InvalidResourceLimits,
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
    /// A coordinated drain, checkpoint, coverage, or clean command failed.
    #[error("Durable Object orchestration failed: {0}")]
    Orchestration(String),
}

struct ManagedChild {
    spec: ChildSpec,
    lifecycle: ChildLifecycle,
    process: Option<Child>,
    descriptor: ChildDescriptor,
}

/// Sends one bounded lifecycle command to a child endpoint.
async fn endpoint_command(path: &Path, command: &str) -> Result<String, SupervisorError> {
    let operation = async {
        let mut stream = UnixStream::connect(path).await?;
        stream.write_all(format!("{command}\n").as_bytes()).await?;
        let mut response = String::new();
        BufReader::new(stream).read_line(&mut response).await?;
        if response.is_empty() {
            return Err(SupervisorError::Orchestration(format!(
                "{command} returned no response"
            )));
        }
        let response = response.trim_end_matches(['\r', '\n']);
        if let Some(error) = response.strip_prefix("ERR ") {
            return Err(SupervisorError::Orchestration(error.to_owned()));
        }
        if response == "OK" {
            return Ok(String::new());
        }
        response
            .strip_prefix("OK ")
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                SupervisorError::Orchestration(format!(
                    "{command} returned malformed response {response}"
                ))
            })
    };
    tokio::time::timeout(Duration::from_secs(10), operation)
        .await
        .map_err(|_| SupervisorError::Orchestration(format!("{command} timed out")))?
}

/// Parses a nonnegative lifecycle sequence returned by a child command.
fn command_sequence(command: &str, payload: &str) -> Result<u64, SupervisorError> {
    payload.parse::<u64>().map_err(|_| {
        SupervisorError::Orchestration(format!("{command} returned invalid sequence {payload}"))
    })
}

/// Parses the worker or replica status watermark tuple.
fn command_status(payload: &str, expected_role: &str) -> Result<(u64, u64, u64), SupervisorError> {
    let fields = payload.split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() != 4 || fields[0] != expected_role {
        return Err(SupervisorError::Orchestration(format!(
            "STATUS returned unexpected payload {payload}"
        )));
    }
    let applied = command_sequence("STATUS applied", fields[1])?;
    let archived = command_sequence("STATUS archived", fields[2])?;
    let checkpointed = command_sequence("STATUS checkpointed", fields[3])?;
    Ok((applied, archived, checkpointed))
}

/// One tenant host's registry of isolated single-Raft-group child processes.
pub struct HostSupervisor {
    host_id: HostId,
    root: PathBuf,
    command: ChildCommand,
    children: HashMap<String, ManagedChild>,
}

impl HostSupervisor {
    /// Creates one host supervisor rooted at an exclusive local data directory.
    pub fn new(host_id: HostId, root: impl AsRef<Path>, command: ChildCommand) -> Self {
        Self {
            host_id,
            root: root.as_ref().to_path_buf(),
            command,
            children: HashMap::new(),
        }
    }

    /// Launches one isolated replica process and records its lifecycle fence.
    pub async fn spawn(&mut self, spec: ChildSpec) -> Result<ChildDescriptor, SupervisorError> {
        if self.children.contains_key(&spec.do_id) {
            return Err(SupervisorError::Duplicate(spec.do_id));
        }
        let (process, descriptor) = self.launch(&spec).await?;
        let lifecycle = ChildLifecycle::running(spec.role, spec.applied);
        self.children.insert(
            spec.do_id.clone(),
            ManagedChild {
                spec,
                lifecycle,
                process: Some(process),
                descriptor: descriptor.clone(),
            },
        );
        Ok(descriptor)
    }

    /// Stops one replica only after archive and checkpoint fences cover applied state.
    pub async fn suspend(
        &mut self,
        do_id: &str,
        fence: SuspendFence,
    ) -> Result<(), SupervisorError> {
        let managed = self
            .children
            .get_mut(do_id)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        let mut lifecycle = managed.lifecycle;
        lifecycle.suspend(fence)?;
        if let Some(mut process) = managed.process.take() {
            process.kill().await?;
            let _ = process.wait().await?;
        }
        managed.lifecycle = lifecycle;
        Ok(())
    }

    /// Drains, checkpoints, covers, cleans, and terminates one replica-backed worker.
    pub async fn suspend_orchestrated(&mut self, do_id: &str) -> Result<(), SupervisorError> {
        let (worker_socket, replica_socket, lease_generation, lease_token) = {
            let managed = self
                .children
                .get_mut(do_id)
                .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
            managed.lifecycle.begin_suspend()?;
            let durability = managed.spec.durability.as_ref().ok_or_else(|| {
                SupervisorError::InvalidDurability(
                    "orchestrated suspension requires replica durability".to_owned(),
                )
            });
            let durability = match durability {
                Ok(durability) => durability,
                Err(error) => {
                    managed.lifecycle.rollback_suspend()?;
                    return Err(error);
                }
            };
            let WorkerDurability::Replica {
                socket,
                lease_token,
                generation,
                ..
            } = durability
            else {
                managed.lifecycle.rollback_suspend()?;
                return Err(SupervisorError::InvalidDurability(
                    "orchestrated suspension requires replica durability".to_owned(),
                ));
            };
            (
                managed.descriptor.socket_path.clone(),
                socket.clone(),
                *generation,
                lease_token.clone(),
            )
        };

        let orchestration = async {
            let drained =
                command_sequence("DRAIN", &endpoint_command(&worker_socket, "DRAIN").await?)?;
            let checkpointed = command_sequence(
                "CHECKPOINT",
                &endpoint_command(&worker_socket, "CHECKPOINT").await?,
            )?;
            let (applied, archived, worker_checkpointed) =
                command_status(&endpoint_command(&worker_socket, "STATUS").await?, "worker")?;
            if drained != archived || checkpointed != worker_checkpointed {
                return Err(SupervisorError::Orchestration(
                    "worker status did not confirm drain and checkpoint coverage".to_owned(),
                ));
            }
            if archived < applied || worker_checkpointed < applied {
                return Err(SupervisorError::Orchestration(
                    "worker checkpoint coverage is behind applied state".to_owned(),
                ));
            }
            let token = hex::encode(lease_token.as_bytes());
            let identity = hex::encode(format!("checkpoint/{checkpointed}").as_bytes());
            endpoint_command(
                &replica_socket,
                &format!(
                    "REPLICA_COVER {lease_generation} {token} {archived} {checkpointed} {identity}"
                ),
            )
            .await?;
            let (_, replica_archived, replica_checkpointed) = command_status(
                &endpoint_command(&replica_socket, "STATUS").await?,
                "replica",
            )?;
            if replica_archived < archived || replica_checkpointed < checkpointed {
                return Err(SupervisorError::Orchestration(
                    "replica did not record complete checkpoint coverage".to_owned(),
                ));
            }
            endpoint_command(
                &replica_socket,
                &format!("REPLICA_CLEAN {lease_generation} {token} {checkpointed}"),
            )
            .await?;
            Ok(SuspendFence::new(applied, archived, checkpointed))
        }
        .await;

        let fence = match orchestration {
            Ok(fence) => fence,
            Err(error) => {
                let managed = self
                    .children
                    .get_mut(do_id)
                    .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
                managed.lifecycle.rollback_suspend()?;
                return Err(error);
            }
        };
        let mut process = {
            let managed = self
                .children
                .get_mut(do_id)
                .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
            match managed.lifecycle.finish_suspend(fence) {
                Ok(()) => managed.process.take(),
                Err(error) => {
                    managed.lifecycle.rollback_suspend()?;
                    return Err(error.into());
                }
            }
        };
        if let Some(process) = process.as_mut() {
            process.kill().await?;
            let _ = process.wait().await?;
        }
        Ok(())
    }

    /// Launches a suspended replica in restore mode without making it routable.
    pub async fn start_restore(
        &mut self,
        do_id: &str,
        required: u64,
        role: ReplicaRole,
    ) -> Result<ChildDescriptor, SupervisorError> {
        let managed = self
            .children
            .get(do_id)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        let mut spec = managed.spec.clone();
        spec.role = role;
        let mut lifecycle = managed.lifecycle;
        lifecycle.begin_restore(required)?;
        let (process, descriptor) = self.launch(&spec).await?;
        let managed = self
            .children
            .get_mut(do_id)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        managed.lifecycle = lifecycle;
        managed.spec = spec;
        managed.process = Some(process);
        managed.descriptor = descriptor.clone();
        Ok(descriptor)
    }

    /// Makes a restored process routable after it reaches the ingress fence.
    pub fn finish_restore(
        &mut self,
        do_id: &str,
        role: ReplicaRole,
        restored: u64,
    ) -> Result<(), SupervisorError> {
        let managed = self
            .children
            .get_mut(do_id)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        if managed.spec.role != role {
            return Err(SupervisorError::RoleMismatch(do_id.to_owned()));
        }
        managed.lifecycle.finish_restore(role, restored)?;
        managed.spec.role = role;
        managed.spec.applied = restored;
        Ok(())
    }

    /// Routes a stateful Worker event only to the running local leader.
    pub fn route_stateful(&mut self, do_id: &str) -> Result<PathBuf, SupervisorError> {
        self.poll_exited()?;
        let managed = self
            .children
            .get(do_id)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        if !managed.lifecycle.may_execute_stateful_event() || managed.process.is_none() {
            return Err(SupervisorError::RouteFenced(do_id.to_owned()));
        }
        Ok(managed.descriptor.socket_path.clone())
    }

    /// Routes a fenced snapshot read to any sufficiently applied running replica.
    pub fn route_snapshot(
        &mut self,
        do_id: &str,
        requested: u64,
    ) -> Result<PathBuf, SupervisorError> {
        self.poll_exited()?;
        let managed = self
            .children
            .get(do_id)
            .ok_or_else(|| SupervisorError::Unknown(do_id.to_owned()))?;
        if !managed.lifecycle.may_serve_snapshot(requested) || managed.process.is_none() {
            return Err(SupervisorError::RouteFenced(do_id.to_owned()));
        }
        Ok(managed.descriptor.socket_path.clone())
    }

    /// Returns one supervised replica's current lifecycle state.
    pub fn state(&self, do_id: &str) -> Option<ChildState> {
        self.children
            .get(do_id)
            .map(|child| child.lifecycle.state())
    }

    /// Returns the live child process identifier, if running or restoring.
    pub fn pid(&self, do_id: &str) -> Option<u32> {
        self.children
            .get(do_id)
            .and_then(|child| child.process.as_ref())
            .and_then(Child::id)
    }

    /// Reaps exited processes immediately and fences every route behind recovery.
    pub fn poll_exited(&mut self) -> Result<Vec<ExitedChild>, SupervisorError> {
        let mut exited = Vec::new();
        for (do_id, managed) in &mut self.children {
            let status = match managed.process.as_mut() {
                Some(process) => process.try_wait()?,
                None => None,
            };
            if let Some(status) = status {
                managed.process = None;
                if matches!(managed.lifecycle.state(), ChildState::Running(_)) {
                    managed.lifecycle.begin_crash_recovery()?;
                }
                exited.push(ExitedChild {
                    do_id: do_id.clone(),
                    status,
                });
            }
        }
        exited.sort_by(|left, right| left.do_id.cmp(&right.do_id));
        Ok(exited)
    }

    /// Stops every remaining child before the host process exits.
    pub async fn shutdown(&mut self) -> Result<(), SupervisorError> {
        for managed in self.children.values_mut() {
            if let Some(mut process) = managed.process.take() {
                process.kill().await?;
                let _ = process.wait().await?;
            }
        }
        Ok(())
    }

    /// Applies one hard soft-and-hard Unix resource limit in the pre-exec child.
    fn set_child_limit(resource: libc::c_int, ceiling: u64) -> std::io::Result<()> {
        let mut current = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if unsafe { libc::getrlimit(resource, &mut current) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let ceiling = ceiling as libc::rlim_t;
        if current.rlim_max != libc::RLIM_INFINITY && ceiling > current.rlim_max {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "worker resource ceiling exceeds inherited hard limit",
            ));
        }
        let soft_limit = libc::rlimit {
            rlim_cur: ceiling,
            rlim_max: current.rlim_max,
        };
        if unsafe { libc::setrlimit(resource, &soft_limit) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let hard_limit = libc::rlimit {
            rlim_cur: ceiling,
            rlim_max: ceiling,
        };
        if unsafe { libc::setrlimit(resource, &hard_limit) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Creates isolated paths and starts one configured child process.
    async fn launch(&self, spec: &ChildSpec) -> Result<(Child, ChildDescriptor), SupervisorError> {
        let data_dir = self
            .root
            .join(&spec.do_id)
            .join(spec.replica_id.to_string());
        tokio::fs::create_dir_all(&data_dir).await?;
        let socket_path = data_dir.join("worker.sock");
        match tokio::fs::remove_file(&socket_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let role = match spec.role {
            ReplicaRole::Leader => "worker",
            ReplicaRole::Follower => "replica",
        };
        let mut command = Command::new(&self.command.program);
        command
            .args(&self.command.args)
            .arg("--do-id")
            .arg(&spec.do_id)
            .arg("--replica-id")
            .arg(spec.replica_id.to_string())
            .arg("--role")
            .arg(role)
            .arg("--socket")
            .arg(&socket_path)
            .arg("--data-dir")
            .arg(&data_dir)
            .env("VERGLAS_CELL_HOST", self.host_id.as_str())
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
        }) = &spec.durability
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
        if let Some(WorkerDurability::ManagedCas {
            store,
            lease_token,
            generation,
            start_sequence,
            lease_etag,
            lease_version,
        }) = &spec.durability
        {
            command
                .arg("--cas-endpoint")
                .arg(&store.endpoint)
                .arg("--cas-bucket")
                .arg(&store.bucket)
                .arg("--cas-prefix")
                .arg(&store.prefix)
                .arg("--cas-region")
                .arg(&store.region)
                .arg("--cas-access-key-id")
                .arg(&store.access_key_id)
                .arg("--cas-secret-access-key")
                .arg(&store.secret_access_key)
                .arg("--lease-token")
                .arg(lease_token)
                .arg("--lease-generation")
                .arg(generation.to_string())
                .arg("--start-sequence")
                .arg(start_sequence.to_string());
            if let Some(lease_etag) = lease_etag {
                command.arg("--lease-etag").arg(lease_etag);
            }
            if let Some(lease_version) = lease_version {
                command.arg("--lease-version").arg(lease_version);
            }
        }
        let limits = spec.resource_limits;
        // Extension point: move these ceilings into the future WASM/microVM runtime.
        unsafe {
            command.pre_exec(move || {
                #[cfg(not(target_os = "macos"))]
                Self::set_child_limit(libc::RLIMIT_AS, limits.memory_ceiling_bytes)?;
                // macOS rejects lowering its inherited unlimited memory rlimits;
                // its future microVM boundary is the memory-enforcement extension point.
                Self::set_child_limit(libc::RLIMIT_NOFILE, limits.open_files_ceiling)
            });
        }
        let mut process = command.spawn()?;
        if let Some(status) = process.try_wait()? {
            return Err(SupervisorError::Exited {
                do_id: spec.do_id.clone(),
                status,
            });
        }
        let pid = process.id().ok_or_else(|| {
            SupervisorError::Io(std::io::Error::other("spawned process has no pid"))
        })?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = process.try_wait()? {
                return Err(SupervisorError::Exited {
                    do_id: spec.do_id.clone(),
                    status,
                });
            }
            match tokio::fs::symlink_metadata(&socket_path).await {
                Ok(metadata) if metadata.file_type().is_socket() => break,
                Ok(_) => {
                    let _ = process.kill().await;
                    return Err(SupervisorError::InvalidSocket(spec.do_id.clone()));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            if Instant::now() >= deadline {
                let _ = process.kill().await;
                return Err(SupervisorError::ReadinessTimeout(spec.do_id.clone()));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok((
            process,
            ChildDescriptor {
                pid,
                socket_path,
                data_dir,
            },
        ))
    }
}
