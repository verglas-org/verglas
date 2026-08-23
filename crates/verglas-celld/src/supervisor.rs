//! Concrete `celld-host` child-process supervision and fenced Unix-socket routing.

use std::collections::HashMap;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

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

/// Durable identity and initial role of one host-local DO replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildSpec {
    do_id: String,
    replica_id: u64,
    role: ReplicaRole,
    applied: u64,
    durability: Option<WorkerDurability>,
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
        }
        self.durability = Some(durability);
        Ok(self)
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
}

struct ManagedChild {
    spec: ChildSpec,
    lifecycle: ChildLifecycle,
    process: Option<Child>,
    descriptor: ChildDescriptor,
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
        let deadline = Instant::now() + Duration::from_secs(2);
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
