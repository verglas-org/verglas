//! Local Unix control protocol used by placement agents to drive `celld-host`.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

use crate::{
    ChildSpec, HostSupervisor, ManagedCasConfig, ReplicaRole, SupervisorError, SuspendFence,
    WorkerDurability,
};

const MAX_COMMAND_BYTES: usize = 8 * 1024;

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
        match self.execute_inner(line).await {
            Ok(payload) if payload.is_empty() => "OK\n".to_owned(),
            Ok(payload) => format!("OK {payload}\n"),
            Err(error) => format!("ERR {}\n", one_line(&error)),
        }
    }

    /// Applies one validated command to the owned supervisor.
    async fn execute_inner(&mut self, line: &str) -> Result<String, String> {
        let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
        let Some(command) = fields.first().copied() else {
            return Err("invalid command: empty request".to_owned());
        };
        match command {
            "SPAWN" if fields.len() == 5 => {
                let role = parse_role(fields[3])?;
                let spec = ChildSpec::new(
                    fields[1],
                    parse_u64(fields[2], "replica id")?,
                    role,
                    parse_u64(fields[4], "applied sequence")?,
                )
                .map_err(|error| error.to_string())?;
                let descriptor = self
                    .supervisor
                    .spawn(spec)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(descriptor.socket_path().display().to_string())
            }
            "SPAWN_WORKER" if fields.len() == 9 => {
                let token = hex::decode(fields[5])
                    .map_err(|error| format!("invalid command: lease token: {error}"))?;
                let token = String::from_utf8(token)
                    .map_err(|error| format!("invalid command: lease token: {error}"))?;
                let spec = ChildSpec::new(
                    fields[1],
                    parse_u64(fields[2], "replica id")?,
                    ReplicaRole::Leader,
                    parse_u64(fields[3], "applied sequence")?,
                )
                .map_err(|error| error.to_string())?
                .with_durability(WorkerDurability::Replica {
                    socket: PathBuf::from(fields[4]),
                    lease_token: token,
                    generation: parse_u64(fields[6], "lease generation")?,
                    start_sequence: parse_u64(fields[7], "start sequence")?,
                    offload_dir: (fields[8] != "-").then(|| PathBuf::from(fields[8])),
                })
                .map_err(|error| error.to_string())?;
                let descriptor = self
                    .supervisor
                    .spawn(spec)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(descriptor.socket_path().display().to_string())
            }
            "SPAWN_CAS_WORKER" if fields.len() == 15 => {
                let token = decode_hex_string(fields[10], "lease token")?;
                let lease_etag = decode_optional_hex(fields[13], "lease ETag")?;
                let lease_version = decode_optional_hex(fields[14], "lease version")?;
                let spec = ChildSpec::new(
                    fields[1],
                    parse_u64(fields[2], "replica id")?,
                    ReplicaRole::Leader,
                    parse_u64(fields[3], "applied sequence")?,
                )
                .map_err(|error| error.to_string())?
                .with_durability(WorkerDurability::ManagedCas {
                    store: ManagedCasConfig::new(
                        fields[4], fields[5], fields[6], fields[7], fields[8], fields[9],
                    ),
                    lease_token: token,
                    generation: parse_u64(fields[11], "lease generation")?,
                    start_sequence: parse_u64(fields[12], "start sequence")?,
                    lease_etag,
                    lease_version,
                })
                .map_err(|error| error.to_string())?;
                let descriptor = self
                    .supervisor
                    .spawn(spec)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(descriptor.socket_path().display().to_string())
            }
            "SUSPEND" if fields.len() == 5 => {
                self.supervisor
                    .suspend(
                        fields[1],
                        SuspendFence::new(
                            parse_u64(fields[2], "applied sequence")?,
                            parse_u64(fields[3], "archive sequence")?,
                            parse_u64(fields[4], "checkpoint sequence")?,
                        ),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(String::new())
            }
            "SUSPEND_ORCHESTRATED" if fields.len() == 2 => {
                self.supervisor
                    .suspend_orchestrated(fields[1])
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(String::new())
            }
            "RESTORE" if fields.len() == 4 => {
                let descriptor = self
                    .supervisor
                    .start_restore(
                        fields[1],
                        parse_u64(fields[2], "required sequence")?,
                        parse_role(fields[3])?,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(descriptor.socket_path().display().to_string())
            }
            "FINISH_RESTORE" if fields.len() == 4 => {
                self.supervisor
                    .finish_restore(
                        fields[1],
                        parse_role(fields[2])?,
                        parse_u64(fields[3], "restored sequence")?,
                    )
                    .map_err(|error| error.to_string())?;
                Ok(String::new())
            }
            "PID" if fields.len() == 2 => self
                .supervisor
                .pid(fields[1])
                .map(|pid| pid.to_string())
                .ok_or_else(|| format!("Durable Object {} has no running process", fields[1])),
            "ROUTE_STATEFUL" if fields.len() == 2 => self
                .supervisor
                .route_stateful(fields[1])
                .map(|path| path.display().to_string())
                .map_err(|error| error.to_string()),
            "ROUTE_SNAPSHOT" if fields.len() == 3 => self
                .supervisor
                .route_snapshot(fields[1], parse_u64(fields[2], "snapshot fence")?)
                .map(|path| path.display().to_string())
                .map_err(|error| error.to_string()),
            _ => Err(format!("invalid command: {command}")),
        }
    }
}

impl Drop for ControlServer {
    /// Removes the control socket name when the host endpoint is dropped.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Decodes one required hexadecimal string field.
fn decode_hex_string(value: &str, field: &str) -> Result<String, String> {
    let bytes = hex::decode(value).map_err(|error| format!("invalid command: {field}: {error}"))?;
    String::from_utf8(bytes).map_err(|error| format!("invalid command: {field}: {error}"))
}

/// Decodes an optional hexadecimal field represented by a dash.
fn decode_optional_hex(value: &str, field: &str) -> Result<Option<String>, String> {
    if value == "-" {
        return Ok(None);
    }
    decode_hex_string(value, field).map(Some)
}

/// Parses one unsigned protocol field with a stable error message.
fn parse_u64(value: &str, field: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("invalid command: {field} must be an unsigned integer"))
}

/// Parses the only two Raft replica roles accepted by the host.
fn parse_role(value: &str) -> Result<ReplicaRole, String> {
    match value {
        "leader" => Ok(ReplicaRole::Leader),
        "follower" => Ok(ReplicaRole::Follower),
        _ => Err("invalid command: role must be leader or follower".to_owned()),
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
