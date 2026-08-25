//! Process provisioning for one Turso-backed Durable Object Worker.
//!
//! The local implementation forwards the known runtime CLI contract and passes
//! the operator-owned Catalog startup path only for the exact host capability
//! declaration. Cloud placement and the external lease-validating Turso sync
//! ingress remain cloud responsibilities; celld never invents a second ownership
//! or CAS protocol.

use std::future::Future;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};

use crate::HostId;
use crate::supervisor::SupervisorError;

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
    /// A Catalog child requested privileged runtime startup without operator configuration.
    #[error("Catalog runtime host config path is not configured")]
    CatalogHostConfigNotConfigured,
    /// The process exited before binding its private event socket.
    #[error("Durable Object {do_id} exited during launch with {status}")]
    Exited {
        /// Durable Object whose child failed during launch.
        do_id: String,
        /// Operating-system exit status.
        status: ExitStatus,
    },
    /// The child socket path was occupied by a non-socket filesystem object.
    #[error("Durable Object {0} produced an invalid Unix event socket path")]
    InvalidSocket(String),
    /// The child did not bind its private socket before the launch deadline.
    #[error("Durable Object {0} did not become event-socket-ready")]
    ReadinessTimeout(String),
}

/// A boxed asynchronous result used by object-safe provisioner methods.
pub type ProvisionFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProvisionError>> + Send + 'a>>;

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

    /// Returns the Durable Object identity used for launch errors.
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

    /// Returns the private Worker event socket path.
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

/// Substrate-owned process or machine handle operations.
pub trait ProvisionHandle: Send + Sync {
    /// Reaps an exited handle without waiting for a running handle.
    fn try_wait(&mut self) -> Result<Option<ExitStatus>, ProvisionError>;

    /// Sends the substrate's termination signal to the handle.
    fn kill<'a>(&'a mut self) -> ProvisionFuture<'a, ()>;

    /// Waits until the substrate reports the handle's exit status.
    fn wait<'a>(&'a mut self) -> ProvisionFuture<'a, ExitStatus>;
}

/// Hard process ceilings applied before a Worker child runs any code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResourceLimits {
    memory_bytes: u64,
    open_files: u64,
}

impl WorkerResourceLimits {
    /// Validates and creates one process ceiling configuration.
    pub fn new(memory_bytes: u64, open_files: u64) -> Result<Self, SupervisorError> {
        if memory_bytes == 0 || open_files == 0 {
            return Err(SupervisorError::InvalidLaunch(
                "worker resource limits must be nonzero".to_owned(),
            ));
        }
        Ok(Self {
            memory_bytes,
            open_files,
        })
    }

    /// Returns the memory ceiling used on platforms with address-space limits.
    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    /// Returns the descriptor ceiling.
    pub fn open_files(&self) -> u64 {
        self.open_files
    }
}

impl Default for WorkerResourceLimits {
    /// Returns the prototype's hard default process ceilings.
    fn default() -> Self {
        Self {
            memory_bytes: 4 * 1024 * 1024 * 1024,
            open_files: 1024,
        }
    }
}

/// Remote Turso database and token-file identity for one Durable Object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TursoConfig {
    remote_url: String,
    token_file: PathBuf,
}

impl TursoConfig {
    /// Validates and creates one explicit Turso deployment configuration.
    pub fn new(
        remote_url: impl Into<String>,
        token_file: impl Into<PathBuf>,
    ) -> Result<Self, SupervisorError> {
        let remote_url = remote_url.into();
        let token_file = token_file.into();
        if remote_url.is_empty() {
            return Err(SupervisorError::InvalidLaunch(
                "Turso remote URL cannot be empty".to_owned(),
            ));
        }
        if token_file.as_os_str().is_empty() {
            return Err(SupervisorError::InvalidLaunch(
                "Turso token file cannot be empty".to_owned(),
            ));
        }
        if remote_url.chars().any(char::is_whitespace) {
            return Err(SupervisorError::InvalidLaunch(
                "Turso remote URL cannot contain whitespace".to_owned(),
            ));
        }
        Ok(Self {
            remote_url,
            token_file,
        })
    }

    /// Returns the explicit remote Turso URL.
    pub fn remote_url(&self) -> &str {
        &self.remote_url
    }

    /// Returns the token-file path passed to `verglas-runtime`.
    pub fn token_file(&self) -> &Path {
        &self.token_file
    }
}

/// Tenant component identity and event ingress for one Worker child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerComponent {
    /// Lowercase SHA-256 hex identity of the component artifact.
    digest: String,
    /// Directory holding digest-named component artifacts.
    dir: PathBuf,
    /// Optional Wasmtime compiled component cache directory.
    cwasm_cache_dir: Option<PathBuf>,
    /// Private Unix socket where the child serves the DO event protocol.
    event_socket: PathBuf,
}

impl WorkerComponent {
    /// Validates the digest and creates the component launch identity.
    pub fn new(
        digest: impl Into<String>,
        dir: impl Into<PathBuf>,
        cwasm_cache_dir: Option<PathBuf>,
        event_socket: impl Into<PathBuf>,
    ) -> Result<Self, SupervisorError> {
        let digest = digest.into();
        let dir = dir.into();
        let event_socket = event_socket.into();
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(SupervisorError::InvalidComponentDigest(digest));
        }
        if dir.as_os_str().is_empty() || event_socket.as_os_str().is_empty() {
            return Err(SupervisorError::InvalidLaunch(
                "component directory and event socket cannot be empty".to_owned(),
            ));
        }
        Ok(Self {
            digest,
            dir,
            cwasm_cache_dir,
            event_socket,
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

    /// Returns the optional compiled component cache directory.
    pub fn cwasm_cache_dir(&self) -> Option<&Path> {
        self.cwasm_cache_dir.as_deref()
    }

    /// Returns the event-protocol socket path the child must bind.
    pub fn event_socket(&self) -> &Path {
        &self.event_socket
    }
}

/// The one privileged service declaration accepted by the host runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostServiceBinding {
    binding: String,
    service: String,
}

impl HostServiceBinding {
    /// Validates the exact Catalog-to-runtime capability declaration.
    pub fn new(
        binding: impl Into<String>,
        service: impl Into<String>,
    ) -> Result<Self, SupervisorError> {
        let binding = binding.into();
        let service = service.into();
        if binding != "ICEBERG_COMMIT" || service != "verglas-runtime" {
            return Err(SupervisorError::InvalidHostService { binding, service });
        }
        Ok(Self { binding, service })
    }

    /// Returns the guest environment binding name.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Returns the infrastructure runtime service name.
    pub fn service(&self) -> &str {
        &self.service
    }
}

/// Program, exact Turso arguments, and declared host capability supplied to a provisioner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionRequest {
    program: PathBuf,
    args: Vec<String>,
    host_id: String,
    do_id: String,
    data_dir: PathBuf,
    turso: TursoConfig,
    component: WorkerComponent,
    host_service: Option<HostServiceBinding>,
    resource_limits: WorkerResourceLimits,
}

impl ProvisionRequest {
    /// Creates a substrate request with the one-path Turso launch contract.
    pub fn new(
        program: impl Into<PathBuf>,
        args: Vec<String>,
        host_id: impl Into<String>,
        do_id: impl Into<String>,
        data_dir: impl Into<PathBuf>,
        turso: TursoConfig,
        component: WorkerComponent,
    ) -> Self {
        Self {
            program: program.into(),
            args,
            host_id: host_id.into(),
            do_id: do_id.into(),
            data_dir: data_dir.into(),
            turso,
            component,
            host_service: None,
            resource_limits: WorkerResourceLimits::default(),
        }
    }

    /// Attaches the exact privileged service declaration for the child runtime.
    pub fn with_host_service(mut self, host_service: HostServiceBinding) -> Self {
        self.host_service = Some(host_service);
        self
    }

    /// Attaches explicit process ceilings for the local child.
    pub fn with_resource_limits(mut self, resource_limits: WorkerResourceLimits) -> Self {
        self.resource_limits = resource_limits;
        self
    }

    /// Returns the process ceilings forwarded to the local provisioner.
    pub fn resource_limits(&self) -> &WorkerResourceLimits {
        &self.resource_limits
    }

    /// Returns the executable selected for a local or remote launch.
    pub fn program(&self) -> &Path {
        &self.program
    }

    /// Returns fixed executable arguments that precede runtime arguments.
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

    /// Returns the local Turso data root.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns the explicit Turso configuration.
    pub fn turso(&self) -> &TursoConfig {
        &self.turso
    }

    /// Returns the exact privileged service declaration for the child runtime.
    pub fn host_service(&self) -> Option<&HostServiceBinding> {
        self.host_service.as_ref()
    }

    /// Returns the verified component launch identity.
    pub fn component(&self) -> &WorkerComponent {
        &self.component
    }

    /// Builds a request from the supervisor's stable child specification.
    pub(crate) fn from_child(
        command: &ChildCommand,
        host_id: &HostId,
        root: &Path,
        spec: &ChildSpec,
    ) -> Result<Self, SupervisorError> {
        spec.validate()?;
        let data_dir = spec
            .data_dir
            .clone()
            .unwrap_or_else(|| root.join(spec.do_id()));
        let request = Self::new(
            command.program.clone(),
            command.args.clone(),
            host_id.as_str(),
            spec.do_id.clone(),
            data_dir,
            spec.turso.clone().ok_or_else(|| {
                SupervisorError::InvalidLaunch("Turso deployment is required".to_owned())
            })?,
            spec.component.clone().ok_or_else(|| {
                SupervisorError::InvalidLaunch("component and event socket are required".to_owned())
            })?,
        )
        .with_resource_limits(spec.resource_limits.clone());
        Ok(match spec.host_service.clone() {
            Some(host_service) => request.with_host_service(host_service),
            None => request,
        })
    }
}

/// Executable and fixed arguments used to launch every `verglas-runtime` child.
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

/// Durable identity and one-path Turso launch configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildSpec {
    do_id: String,
    data_dir: Option<PathBuf>,
    turso: Option<TursoConfig>,
    component: Option<WorkerComponent>,
    host_service: Option<HostServiceBinding>,
    resource_limits: WorkerResourceLimits,
}

impl ChildSpec {
    /// Validates a filesystem-safe DO identity and creates its launch specification.
    pub fn new(do_id: impl Into<String>) -> Result<Self, SupervisorError> {
        let do_id = validate_identity(do_id.into())?;
        Ok(Self {
            do_id,
            data_dir: None,
            turso: None,
            component: None,
            host_service: None,
            resource_limits: WorkerResourceLimits::default(),
        })
    }

    /// Attaches the local data root passed to `verglas-runtime`.
    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Result<Self, SupervisorError> {
        let data_dir = data_dir.into();
        if data_dir.as_os_str().is_empty() {
            return Err(SupervisorError::InvalidLaunch(
                "local data root cannot be empty".to_owned(),
            ));
        }
        self.data_dir = Some(data_dir);
        Ok(self)
    }

    /// Attaches the explicit Turso remote URL and token-file path.
    pub fn with_turso(mut self, turso: TursoConfig) -> Self {
        self.turso = Some(turso);
        self
    }

    /// Attaches the verified tenant component and event socket.
    pub fn with_component(mut self, component: WorkerComponent) -> Self {
        self.component = Some(component);
        self
    }

    /// Attaches the exact privileged service declaration for this child.
    pub fn with_host_service(mut self, host_service: HostServiceBinding) -> Self {
        self.host_service = Some(host_service);
        self
    }

    /// Attaches explicit process ceilings for this child.
    pub fn with_resource_limits(mut self, resource_limits: WorkerResourceLimits) -> Self {
        self.resource_limits = resource_limits;
        self
    }

    /// Returns the Durable Object identity.
    pub(crate) fn do_id(&self) -> &str {
        &self.do_id
    }

    /// Returns the exact privileged service declaration for this child.
    pub fn host_service(&self) -> Option<&HostServiceBinding> {
        self.host_service.as_ref()
    }

    /// Returns whether the spec has all required Turso launch values.
    pub(crate) fn validate(&self) -> Result<(), SupervisorError> {
        if self.turso.is_none() {
            return Err(SupervisorError::InvalidLaunch(
                "Turso deployment is required".to_owned(),
            ));
        }
        if self.component.is_none() {
            return Err(SupervisorError::InvalidLaunch(
                "component and event socket are required".to_owned(),
            ));
        }
        Ok(())
    }
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

/// Stable process and isolation paths returned after a successful spawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildDescriptor {
    pid: u32,
    socket_path: PathBuf,
    data_dir: PathBuf,
}

impl ChildDescriptor {
    /// Creates a substrate descriptor for a process endpoint.
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

    /// Returns the child-exclusive Worker event socket.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Returns the child-exclusive Turso data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

/// Compute substrate operations required by the host lifecycle supervisor.
pub trait Provisioner: Send + Sync {
    /// Starts one child or machine with its Turso and component launch paths.
    fn spawn<'a>(&'a self, request: ProvisionRequest) -> ProvisionFuture<'a, ProvisionedChild>;

    /// Waits until the child has bound its private event socket before publication.
    fn await_ready<'a>(&'a self, child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()>;

    /// Reaps an exited child without waiting for a running child.
    fn try_wait(&self, child: &mut ProvisionedChild) -> Result<Option<ExitStatus>, ProvisionError>;

    /// Sends the substrate termination operation after lifecycle fencing.
    fn kill<'a>(&'a self, child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()>;

    /// Waits for the substrate termination operation to be reaped.
    fn wait<'a>(&'a self, child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ExitStatus>;
}

/// Local child-process substrate used by development, tests, and `verglas-runtime`.
#[derive(Debug, Clone, Default)]
pub struct LocalProcessProvisioner {
    /// Optional host-owned startup configuration exposed only to Catalog children.
    catalog_host_config: Option<PathBuf>,
}

impl LocalProcessProvisioner {
    /// Creates the local process provisioner without Catalog startup configuration.
    pub const fn new() -> Self {
        Self {
            catalog_host_config: None,
        }
    }

    /// Configures the operator-owned path passed to Catalog runtime children.
    pub fn with_catalog_host_config(mut self, path: impl Into<PathBuf>) -> Self {
        self.catalog_host_config = Some(path.into());
        self
    }

    /// Returns the configured operator-owned Catalog startup path, if any.
    pub fn catalog_host_config(&self) -> Option<&Path> {
        self.catalog_host_config.as_deref()
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
    /// Creates one isolated local process with the exact runtime CLI arguments.
    fn spawn<'a>(&'a self, request: ProvisionRequest) -> ProvisionFuture<'a, ProvisionedChild> {
        Box::pin(async move {
            let catalog_host_config = match request.host_service() {
                Some(host_service)
                    if host_service.binding() == "ICEBERG_COMMIT"
                        && host_service.service() == "verglas-runtime" =>
                {
                    let path = self
                        .catalog_host_config
                        .clone()
                        .ok_or(ProvisionError::CatalogHostConfigNotConfigured)?;
                    if path.as_os_str().is_empty() {
                        return Err(ProvisionError::CatalogHostConfigNotConfigured);
                    }
                    Some(path)
                }
                _ => None,
            };
            tokio::fs::create_dir_all(request.data_dir()).await?;
            let event_socket = request.component().event_socket();
            match tokio::fs::remove_file(event_socket).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(ProvisionError::from(error)),
            }
            // The privileged startup path is selected only for the exact Catalog binding.
            let mut command = Command::new(request.program());
            command
                .args(request.args())
                .arg("--do-id")
                .arg(request.do_id())
                .arg("--data-dir")
                .arg(request.data_dir())
                .arg("--turso-url")
                .arg(request.turso().remote_url())
                .arg("--turso-token-file")
                .arg(request.turso().token_file())
                .arg("--component-digest")
                .arg(request.component().digest())
                .arg("--component-dir")
                .arg(request.component().dir())
                .env("VERGLAS_CELL_HOST", request.host_id())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            if let Some(cache_dir) = request.component().cwasm_cache_dir() {
                command.arg("--cwasm-cache-dir").arg(cache_dir);
            }
            if let Some(catalog_host_config) = catalog_host_config {
                command
                    .arg("--catalog-host-config")
                    .arg(catalog_host_config);
            }
            command.arg("--event-socket").arg(event_socket);
            let resource_limits = request.resource_limits().clone();
            // SAFETY: The closure only calls async-signal-safe setrlimit before exec.
            unsafe {
                command.pre_exec(move || {
                    let descriptor_limit = libc::rlimit {
                        rlim_cur: resource_limits.open_files as libc::rlim_t,
                        rlim_max: resource_limits.open_files as libc::rlim_t,
                    };
                    if libc::setrlimit(libc::RLIMIT_NOFILE, &descriptor_limit) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    #[cfg(not(target_os = "macos"))]
                    {
                        let memory_limit = libc::rlimit {
                            rlim_cur: resource_limits.memory_bytes as libc::rlim_t,
                            rlim_max: resource_limits.memory_bytes as libc::rlim_t,
                        };
                        if libc::setrlimit(libc::RLIMIT_AS, &memory_limit) != 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                    }
                    Ok(())
                });
            }
            let process = command.spawn()?;
            let pid = process.id().ok_or_else(|| {
                ProvisionError::Io(std::io::Error::other("spawned process has no pid"))
            })?;
            let descriptor = ChildDescriptor::new(
                pid,
                event_socket.to_path_buf(),
                request.data_dir().to_path_buf(),
            );
            Ok(ProvisionedChild::new(
                request.do_id().to_owned(),
                descriptor,
                Box::new(LocalProcessHandle { process }),
            ))
        })
    }

    /// Waits for a local event socket and fails closed on early exit or bad paths.
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
