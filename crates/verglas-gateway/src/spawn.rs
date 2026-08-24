//! Celld control-plane spawning and bounded event-socket readiness.
//!
//! The gateway starts a private replica authority through the strict celld
//! control protocol, then starts the component-bearing Worker with an event
//! socket path chosen under the configured data root. The returned celld OK
//! payload is the Worker control socket and is deliberately not used for events.

use std::cmp::min;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::error::GatewayError;

const EVENT_SOCKET_TIMEOUT: Duration = Duration::from_secs(2);
const EVENT_SOCKET_INITIAL_DELAY: Duration = Duration::from_millis(5);
const EVENT_SOCKET_MAX_DELAY: Duration = Duration::from_millis(50);

/// Inputs required to launch one resident Durable Object process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpawnRequest {
    do_id: String,
    binding: String,
    name: String,
    component_digest: String,
    component_dir: PathBuf,
    data_root: PathBuf,
}

impl SpawnRequest {
    /// Creates one spawn request from a routed manifest binding.
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
            data_root,
        }
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

    /// Returns the immutable component digest to pass to `verglasd`.
    pub fn component_digest(&self) -> &str {
        &self.component_digest
    }

    /// Returns the immutable component artifact directory.
    pub fn component_dir(&self) -> &Path {
        &self.component_dir
    }

    /// Returns the process data root requested by the gateway CLI.
    pub fn data_root(&self) -> &Path {
        &self.data_root
    }
}

/// Launches a Durable Object and returns its private event socket path.
#[async_trait]
pub trait DoSpawner: Send + Sync {
    /// Performs one spawn without implementing idle or restart management.
    async fn spawn(&self, request: SpawnRequest) -> Result<PathBuf, GatewayError>;
}

/// Local Unix control client for the celld replica and Worker spawn commands.
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

    /// Returns the host-local control socket path.
    pub fn control_socket(&self) -> &Path {
        &self.control_socket
    }

    /// Returns the event socket path owned by one gateway-routed object.
    fn event_socket_path(&self, request: &SpawnRequest) -> PathBuf {
        request
            .data_root()
            .join(request.do_id())
            .join("events.sock")
    }

    /// Sends the minimal replica command needed by the OSS single-node POC.
    async fn spawn_replica(&self, request: &SpawnRequest) -> Result<PathBuf, GatewayError> {
        // The replica endpoint validates every committed envelope's DO identity,
        // so its supervised identity must match the Worker rather than gain a suffix.
        let command = format!("SPAWN {} 1 follower 0\n", request.do_id());
        self.send_control_command(&command, "SPAWN replica").await
    }

    /// Reads the replica's applied sequence before handing it to a Worker restart.
    async fn replica_sequence(&self, replica_socket: &Path) -> Result<u64, GatewayError> {
        let mut stream = UnixStream::connect(replica_socket)
            .await
            .map_err(|error| GatewayError::control_io("connect replica status", error))?;
        stream
            .write_all(b"STATUS\n")
            .await
            .map_err(|error| GatewayError::control_io("write replica status", error))?;
        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .await
            .map_err(|error| GatewayError::control_io("read replica status", error))?;
        let fields = response.split_ascii_whitespace().collect::<Vec<_>>();
        if fields.len() != 5 || fields[0] != "OK" || fields[1] != "replica" {
            return Err(GatewayError::SpawnRejected {
                message: format!(
                    "replica status returned an invalid response: {}",
                    response.trim()
                ),
            });
        }
        fields[2]
            .parse::<u64>()
            .map_err(|_| GatewayError::SpawnRejected {
                message: format!(
                    "replica status applied sequence is not an unsigned integer: {}",
                    fields[2]
                ),
            })
    }

    /// Sends the component-bearing worker command with the held local lease shape.
    async fn spawn_worker(
        &self,
        request: &SpawnRequest,
        replica_socket: &Path,
        start_sequence: u64,
        event_socket: &Path,
    ) -> Result<(), GatewayError> {
        let lease_token = format!("lease-{}", request.do_id());
        let command = format!(
            "SPAWN_WORKER {} 1 {start_sequence} {} {} 11 {start_sequence} - {} {} {}\n",
            request.do_id(),
            replica_socket.display(),
            hex::encode(lease_token),
            request.component_digest(),
            request.component_dir().display(),
            event_socket.display(),
        );
        let _worker_socket = self
            .send_control_command(&command, "SPAWN_WORKER worker")
            .await?;
        Ok(())
    }

    /// Sends one line to celld and parses its returned child control socket path.
    async fn send_control_command(
        &self,
        command: &str,
        operation: &'static str,
    ) -> Result<PathBuf, GatewayError> {
        let mut stream = UnixStream::connect(&self.control_socket)
            .await
            .map_err(|error| GatewayError::control_io("connect", error))?;
        stream
            .write_all(command.as_bytes())
            .await
            .map_err(|error| GatewayError::control_io(operation, error))?;
        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .await
            .map_err(|error| GatewayError::control_io(operation, error))?;
        let response = response.trim_end_matches(['\r', '\n']);
        let Some(path) = response.strip_prefix("OK ") else {
            let message = response
                .strip_prefix("ERR ")
                .map_or("control socket returned an invalid response", |message| {
                    message
                });
            return Err(GatewayError::SpawnRejected {
                message: message.to_owned(),
            });
        };
        if path.is_empty() {
            return Err(GatewayError::SpawnRejected {
                message: format!("{operation} returned an empty Worker socket path"),
            });
        }
        Ok(PathBuf::from(path))
    }

    /// Waits for celld's separately supplied event socket to become a Unix socket.
    async fn wait_for_event_socket(&self, path: &Path) -> Result<(), GatewayError> {
        let deadline = Instant::now() + EVENT_SOCKET_TIMEOUT;
        let mut delay = EVENT_SOCKET_INITIAL_DELAY;
        loop {
            match tokio::fs::symlink_metadata(path).await {
                Ok(metadata) if metadata.file_type().is_socket() => return Ok(()),
                Ok(_) => {
                    return Err(GatewayError::SpawnRejected {
                        message: format!(
                            "event socket path is not a Unix socket: {}",
                            path.display()
                        ),
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(GatewayError::event_io("inspect event socket", error)),
            }
            if Instant::now() >= deadline {
                return Err(GatewayError::SpawnRejected {
                    message: format!(
                        "event socket did not bind before the {}ms deadline: {}",
                        EVENT_SOCKET_TIMEOUT.as_millis(),
                        path.display()
                    ),
                });
            }
            tokio::time::sleep(delay).await;
            delay = min(delay + delay, EVENT_SOCKET_MAX_DELAY);
        }
    }
}

#[async_trait]
impl DoSpawner for CelldSpawner {
    /// Starts the replica, starts the component Worker, and waits for its event socket.
    async fn spawn(&self, request: SpawnRequest) -> Result<PathBuf, GatewayError> {
        let event_socket = self.event_socket_path(&request);
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
        let replica_socket = self.spawn_replica(&request).await?;
        let start_sequence = self.replica_sequence(&replica_socket).await?;
        self.spawn_worker(&request, &replica_socket, start_sequence, &event_socket)
            .await?;
        self.wait_for_event_socket(&event_socket).await?;
        Ok(event_socket)
    }
}
