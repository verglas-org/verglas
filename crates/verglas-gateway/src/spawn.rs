//! Celld control-plane spawning and bounded Turso event-socket readiness.
//!
//! The gateway sends one complete `SPAWN_WORKER` request, including the exact
//! declared host capability when present. It never starts a replica, managed CAS
//! worker, or alternate storage path. The event socket is the only returned
//! endpoint and becomes routable only after socket readiness.

use std::cmp::min;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::error::GatewayError;
use crate::manifest::HostServiceBinding;

const EVENT_SOCKET_TIMEOUT: Duration = Duration::from_secs(2);
const EVENT_SOCKET_INITIAL_DELAY: Duration = Duration::from_millis(5);
const EVENT_SOCKET_MAX_DELAY: Duration = Duration::from_millis(50);

/// Inputs required to launch one resident Turso Durable Object process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpawnRequest {
    do_id: String,
    binding: String,
    name: String,
    component_digest: String,
    component_dir: PathBuf,
    cwasm_cache_dir: Option<PathBuf>,
    host_service: Option<HostServiceBinding>,
    data_root: PathBuf,
    turso_url: String,
    turso_token_file: PathBuf,
}

impl SpawnRequest {
    /// Creates one incomplete request from a routed manifest binding.
    ///
    /// Production callers must attach Turso credentials with [`Self::with_turso`]
    /// before giving the request to [`DoSpawner::spawn`]. Keeping the request
    /// incomplete makes missing deployment configuration a hard error rather
    /// than an implicit local-storage path.
    pub fn new(
        do_id: String,
        binding: String,
        name: String,
        component_digest: String,
        component_dir: PathBuf,
        data_root: PathBuf,
    ) -> Self {
        Self {
            do_id,
            binding,
            name,
            component_digest,
            component_dir,
            cwasm_cache_dir: None,
            host_service: None,
            data_root,
            turso_url: String::new(),
            turso_token_file: PathBuf::new(),
        }
    }

    /// Attaches explicit remote Turso URL and token-file deployment credentials.
    pub fn with_turso(
        mut self,
        turso_url: impl Into<String>,
        turso_token_file: impl Into<PathBuf>,
    ) -> Self {
        self.turso_url = turso_url.into();
        self.turso_token_file = turso_token_file.into();
        self
    }

    /// Attaches an optional Wasmtime compiled component cache directory.
    pub fn with_cwasm_cache_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.cwasm_cache_dir = Some(path.into());
        self
    }

    /// Returns the celld-safe Durable Object identity.
    pub fn do_id(&self) -> &str {
        &self.do_id
    }

    /// Returns the manifest binding that selected this object.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Returns the URL object name that selected this object.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the immutable component digest to pass to `verglas-runtime`.
    pub fn component_digest(&self) -> &str {
        &self.component_digest
    }

    /// Returns the immutable component artifact directory.
    pub fn component_dir(&self) -> &Path {
        &self.component_dir
    }

    /// Returns the optional compiled component cache directory.
    pub fn cwasm_cache_dir(&self) -> Option<&Path> {
        self.cwasm_cache_dir.as_deref()
    }

    /// Attaches the exact privileged service declaration selected by the manifest.
    pub fn with_host_service(mut self, host_service: HostServiceBinding) -> Self {
        self.host_service = Some(host_service);
        self
    }

    /// Returns the exact privileged service declaration selected by the manifest.
    pub fn host_service(&self) -> Option<&HostServiceBinding> {
        self.host_service.as_ref()
    }

    /// Returns the process data root requested by the gateway.
    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    /// Returns the remote Turso URL selected by manifest deployment config.
    pub fn turso_url(&self) -> &str {
        &self.turso_url
    }

    /// Returns the token-file path selected by manifest deployment config.
    pub fn turso_token_file(&self) -> &Path {
        &self.turso_token_file
    }

    /// Returns the local Turso database directory for this object.
    fn data_dir(&self) -> PathBuf {
        self.data_root.join(&self.do_id)
    }

    /// Returns the event socket path for this object.
    fn event_socket(&self) -> PathBuf {
        self.data_dir().join("events.sock")
    }
}

/// Launches a Durable Object and returns its private event socket path.
#[async_trait]
pub trait DoSpawner: Send + Sync {
    /// Performs one spawn without implementing idle or restart management.
    async fn spawn(&self, request: SpawnRequest) -> Result<PathBuf, GatewayError>;
}

/// Local Unix control client for the one-path Worker spawn command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CelldSpawner {
    control_socket: PathBuf,
}

impl CelldSpawner {
    /// Creates a spawner that sends commands to one host-local control socket.
    pub fn new(control_socket: impl Into<PathBuf>) -> Self {
        Self {
            control_socket: control_socket.into(),
        }
    }

    /// Sends one command and returns the payload after a strict `OK` response.
    async fn send_control_command(
        &self,
        command: &str,
        operation: &'static str,
    ) -> Result<String, GatewayError> {
        let mut stream = UnixStream::connect(&self.control_socket)
            .await
            .map_err(|error| GatewayError::control_io(operation, error))?;
        stream
            .write_all(format!("{command}\n").as_bytes())
            .await
            .map_err(|error| GatewayError::control_io(operation, error))?;
        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .await
            .map_err(|error| GatewayError::control_io(operation, error))?;
        let response = response.trim();
        if let Some(payload) = response.strip_prefix("OK ") {
            return Ok(payload.to_owned());
        }
        if response == "OK" {
            return Ok(String::new());
        }
        Err(GatewayError::SpawnRejected {
            message: format!("{operation} failed: {response}"),
        })
    }

    /// Sends the complete worker launch contract to celld.
    async fn spawn_worker(
        &self,
        request: &SpawnRequest,
        event_socket: &Path,
    ) -> Result<(), GatewayError> {
        if request.turso_url().is_empty() {
            return Err(GatewayError::SpawnRejected {
                message: "Turso remote URL is required".to_owned(),
            });
        }
        if request.turso_token_file().as_os_str().is_empty() {
            return Err(GatewayError::SpawnRejected {
                message: "Turso token-file path is required".to_owned(),
            });
        }
        if request.component_digest().len() != 64
            || !request
                .component_digest()
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(GatewayError::SpawnRejected {
                message: "component digest must be 64 hexadecimal characters".to_owned(),
            });
        }
        let cache = request
            .cwasm_cache_dir()
            .map_or_else(|| "-".to_owned(), |path| path.display().to_string());
        let (host_binding, host_service) = request
            .host_service()
            .map_or(("-", "-"), |service| (service.binding(), service.service()));
        let command = format!(
            "SPAWN_WORKER {} {} {} {} {} {} {} {} {} {}",
            request.do_id(),
            request.data_dir().display(),
            request.turso_url(),
            request.turso_token_file().display(),
            request.component_digest(),
            request.component_dir().display(),
            cache,
            event_socket.display(),
            host_binding,
            host_service,
        );
        self.send_control_command(&command, "SPAWN_WORKER")
            .await
            .map(|_| ())
    }

    /// Waits for a real Unix event socket with bounded backoff.
    async fn wait_for_event_socket(&self, event_socket: &Path) -> Result<(), GatewayError> {
        let deadline = Instant::now() + EVENT_SOCKET_TIMEOUT;
        let mut delay = EVENT_SOCKET_INITIAL_DELAY;
        loop {
            match tokio::fs::symlink_metadata(event_socket).await {
                Ok(metadata) if metadata.file_type().is_socket() => return Ok(()),
                Ok(_) => {
                    return Err(GatewayError::SpawnRejected {
                        message: format!(
                            "event socket path is not a Unix socket: {}",
                            event_socket.display()
                        ),
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(GatewayError::event_io("inspect event socket", error)),
            }
            if Instant::now() >= deadline {
                return Err(GatewayError::SpawnRejected {
                    message: format!(
                        "event socket did not become ready: {}",
                        event_socket.display()
                    ),
                });
            }
            tokio::time::sleep(delay).await;
            delay = min(delay.saturating_mul(2), EVENT_SOCKET_MAX_DELAY);
        }
    }
}

#[async_trait]
impl DoSpawner for CelldSpawner {
    /// Starts one Turso Worker and waits until its event socket is a Unix socket.
    async fn spawn(&self, request: SpawnRequest) -> Result<PathBuf, GatewayError> {
        if request.do_id().is_empty() {
            return Err(GatewayError::SpawnRejected {
                message: "Durable Object id is required".to_owned(),
            });
        }
        let event_socket = request.event_socket();
        let parent = event_socket
            .parent()
            .ok_or_else(|| GatewayError::SpawnRejected {
                message: format!(
                    "event socket has no parent directory: {}",
                    event_socket.display()
                ),
            })?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| GatewayError::event_io("create event socket directory", error))?;
        self.spawn_worker(&request, &event_socket).await?;
        self.wait_for_event_socket(&event_socket).await?;
        Ok(event_socket)
    }
}
