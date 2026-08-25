//! Strict local control protocol for one Turso Worker process per Durable Object.
//!
//! The only spawn command carries the complete runtime launch contract and the
//! optional exact host capability declaration. Old replica, managed-CAS, lease,
//! generation, and checkpoint commands are absent.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::Mutex;

use crate::{
    ChildSpec, HostServiceBinding, HostSupervisor, SupervisorError, SuspendFence, TursoConfig,
    WorkerComponent,
};

const MAX_COMMAND_BYTES: usize = 8 * 1024;
const SPAWN_WORKER_FIELDS: usize = 11;

/// A control-socket bind or request-processing failure.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    /// Unix socket or stream I/O failed.
    #[error("control socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

/// Unix-socket control endpoint owning one host supervisor.
pub struct ControlServer {
    path: PathBuf,
    listener: UnixListener,
    supervisor: HostSupervisor,
}

impl ControlServer {
    /// Binds a fresh host-local control socket after removing a stale socket path.
    pub async fn bind(
        path: impl AsRef<Path>,
        supervisor: HostSupervisor,
    ) -> Result<Self, ControlError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = UnixListener::bind(&path)?;
        Ok(Self {
            path,
            listener,
            supervisor,
        })
    }

    /// Binds a control socket while sharing supervision with another host API.
    pub async fn bind_shared(
        path: impl AsRef<Path>,
        supervisor: Arc<Mutex<HostSupervisor>>,
    ) -> Result<SharedControlServer, ControlError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = UnixListener::bind(&path)?;
        Ok(SharedControlServer {
            path,
            listener,
            supervisor,
        })
    }

    /// Accepts and executes exactly one newline-delimited control request.
    pub async fn serve_once(&mut self) -> Result<(), ControlError> {
        let (stream, _) = self.listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut command = Vec::new();
        let mut limited = BufReader::new(read_half).take((MAX_COMMAND_BYTES + 1) as u64);
        limited.read_until(b'\n', &mut command).await?;
        let response = if command.len() > MAX_COMMAND_BYTES {
            "ERR invalid command: exceeds 8192 bytes\n".to_owned()
        } else {
            match std::str::from_utf8(&command) {
                Ok(line) => self.execute(line.trim()).await,
                Err(_) => "ERR invalid command: command is not UTF-8\n".to_owned(),
            }
        };
        write_half.write_all(response.as_bytes()).await?;
        write_half.shutdown().await?;
        Ok(())
    }

    /// Serves control requests until the task is cancelled.
    pub async fn run(&mut self) -> Result<(), ControlError> {
        loop {
            self.serve_once().await?;
        }
    }

    /// Returns mutable access for orderly process shutdown and embedding.
    pub fn supervisor_mut(&mut self) -> &mut HostSupervisor {
        &mut self.supervisor
    }

    /// Returns the bound host-local control socket path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Parses and executes one strict control command.
    async fn execute(&mut self, line: &str) -> String {
        match execute_inner(&mut self.supervisor, line).await {
            Ok(payload) if payload.is_empty() => "OK\n".to_owned(),
            Ok(payload) => format!("OK {payload}\n"),
            Err(error) => format!("ERR {}\n", one_line(&error)),
        }
    }
}

/// Applies one validated command to a supervisor.
async fn execute_inner(supervisor: &mut HostSupervisor, line: &str) -> Result<String, String> {
    let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
    let Some(command) = fields.first().copied() else {
        return Err("invalid command: empty request".to_owned());
    };
    match command {
        "SPAWN_WORKER" if fields.len() == SPAWN_WORKER_FIELDS => {
            let data_dir = nonempty_path(fields[2], "data root")?;
            let turso = TursoConfig::new(fields[3], nonempty_path(fields[4], "token file")?)
                .map_err(|error| format!("invalid command: {error}"))?;
            let cache_dir = (fields[7] != "-").then(|| PathBuf::from(fields[7]));
            let component = WorkerComponent::new(
                fields[5],
                nonempty_path(fields[6], "component directory")?,
                cache_dir,
                nonempty_path(fields[8], "event socket")?,
            )
            .map_err(|error| format!("invalid command: {error}"))?;
            let host_service = parse_host_service(fields[9], fields[10])?;
            let spec = ChildSpec::new(fields[1])
                .map_err(|error| error.to_string())?
                .with_data_dir(data_dir)
                .map_err(|error| error.to_string())?
                .with_turso(turso)
                .with_component(component);
            let spec = match host_service {
                Some(host_service) => spec.with_host_service(host_service),
                None => spec,
            };
            let descriptor = supervisor
                .spawn(spec)
                .await
                .map_err(|error| error.to_string())?;
            Ok(descriptor.socket_path().display().to_string())
        }
        "SUSPEND" if fields.len() == 5 => {
            supervisor
                .suspend(
                    fields[1],
                    SuspendFence::new(
                        parse_confirmation(fields[2], "push confirmation")?,
                        parse_confirmation(fields[3], "outbox drain confirmation")?,
                        parse_confirmation(fields[4], "event shutdown confirmation")?,
                    ),
                )
                .await
                .map_err(|error| error.to_string())?;
            Ok(String::new())
        }
        "PID" if fields.len() == 2 => supervisor
            .pid(fields[1])
            .map(|pid| pid.to_string())
            .ok_or_else(|| format!("Durable Object {} has no running process", fields[1])),
        "ROUTE_STATEFUL" if fields.len() == 2 => supervisor
            .route_stateful(fields[1])
            .map(|path| path.display().to_string())
            .map_err(|error| error.to_string()),
        _ => Err(format!("invalid command: {command}")),
    }
}

/// Unix-socket control endpoint sharing one supervisor with the management API.
pub struct SharedControlServer {
    path: PathBuf,
    listener: UnixListener,
    supervisor: Arc<Mutex<HostSupervisor>>,
}

impl SharedControlServer {
    /// Accepts and executes exactly one newline-delimited control request.
    pub async fn serve_once(&mut self) -> Result<(), ControlError> {
        let (stream, _) = self.listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut command = Vec::new();
        let mut limited = BufReader::new(read_half).take((MAX_COMMAND_BYTES + 1) as u64);
        limited.read_until(b'\n', &mut command).await?;
        let response = if command.len() > MAX_COMMAND_BYTES {
            "ERR invalid command: exceeds 8192 bytes\n".to_owned()
        } else {
            match std::str::from_utf8(&command) {
                Ok(line) => self.execute(line.trim()).await,
                Err(_) => "ERR invalid command: command is not UTF-8\n".to_owned(),
            }
        };
        write_half.write_all(response.as_bytes()).await?;
        write_half.shutdown().await?;
        Ok(())
    }

    /// Serves control requests until the task is cancelled.
    pub async fn run(&mut self) -> Result<(), ControlError> {
        loop {
            self.serve_once().await?;
        }
    }

    /// Returns the bound host-local control socket path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Shuts down all children through the shared supervisor.
    pub async fn shutdown(&self) -> Result<(), SupervisorError> {
        self.supervisor.lock().await.shutdown().await
    }

    /// Parses and executes one strict control command under the shared lock.
    async fn execute(&self, line: &str) -> String {
        let mut supervisor = self.supervisor.lock().await;
        match execute_inner(&mut supervisor, line).await {
            Ok(payload) if payload.is_empty() => "OK\n".to_owned(),
            Ok(payload) => format!("OK {payload}\n"),
            Err(error) => format!("ERR {}\n", one_line(&error)),
        }
    }
}

impl Drop for SharedControlServer {
    /// Removes the shared control socket name when the endpoint is dropped.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl Drop for ControlServer {
    /// Removes the control socket name when the host endpoint is dropped.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Parses the optional exact runtime host service pair.
fn parse_host_service(binding: &str, service: &str) -> Result<Option<HostServiceBinding>, String> {
    if binding == "-" && service == "-" {
        return Ok(None);
    }
    if binding == "-" || service == "-" {
        return Err(
            "invalid command: host service binding and service must be supplied together"
                .to_owned(),
        );
    }
    HostServiceBinding::new(binding, service)
        .map(Some)
        .map_err(|error| format!("invalid command: {error}"))
}

/// Rejects empty paths before they enter process arguments.
fn nonempty_path(value: &str, field: &str) -> Result<PathBuf, String> {
    if value.is_empty() || value == "-" {
        return Err(format!("invalid command: {field} cannot be empty"));
    }
    Ok(PathBuf::from(value))
}

/// Parses one explicit yes/no confirmation field.
fn parse_confirmation(value: &str, field: &str) -> Result<bool, String> {
    match value {
        "yes" => Ok(true),
        "no" => Ok(false),
        _ => Err(format!("invalid command: {field} must be yes or no")),
    }
}

/// Ensures one protocol error cannot inject a second response line.
fn one_line(error: &str) -> String {
    error.replace(['\r', '\n'], " ")
}

impl From<SupervisorError> for ControlError {
    /// Wraps an unexpected supervisor I/O error for server embeddings.
    fn from(error: SupervisorError) -> Self {
        Self::Io(std::io::Error::other(error.to_string()))
    }
}
