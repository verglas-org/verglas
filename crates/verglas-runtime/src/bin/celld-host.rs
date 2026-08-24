//! Runnable tenant-cell host daemon exposing the local supervision control socket.

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Mutex;
use verglas_celld::{ChildCommand, ControlServer, HostId, HostSupervisor, ManagementApi};

struct Config {
    host_id: String,
    root: PathBuf,
    child_program: PathBuf,
    control_socket: Option<PathBuf>,
    management_bind: Option<SocketAddr>,
}

impl Config {
    /// Parses the strict prototype command line without compatibility aliases.
    fn parse() -> Result<Self, String> {
        let mut arguments = std::env::args().skip(1);
        let mut host_id = None;
        let mut root = None;
        let mut child_program = None;
        let mut control_socket = None;
        let mut management_bind = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--host-id" => host_id = Some(next_value(&mut arguments, "--host-id")?),
                "--root" => root = Some(PathBuf::from(next_value(&mut arguments, "--root")?)),
                "--child" => {
                    child_program = Some(PathBuf::from(next_value(&mut arguments, "--child")?));
                }
                "--control" => {
                    control_socket = Some(PathBuf::from(next_value(&mut arguments, "--control")?));
                }
                "--management-bind" => {
                    let value = next_value(&mut arguments, "--management-bind")?;
                    management_bind = Some(value.parse::<SocketAddr>().map_err(|error| {
                        format!("invalid --management-bind address {value}: {error}")
                    })?);
                }
                "--help" => return Err(usage().to_owned()),
                other => return Err(format!("unknown argument {other}\n{}", usage())),
            }
        }
        Ok(Self {
            host_id: host_id.ok_or_else(|| format!("missing --host-id\n{}", usage()))?,
            root: root.ok_or_else(|| format!("missing --root\n{}", usage()))?,
            child_program: child_program.ok_or_else(|| format!("missing --child\n{}", usage()))?,
            control_socket,
            management_bind,
        })
    }

    /// Resolves the explicit control socket or the root-local default.
    fn control_path(&self) -> PathBuf {
        self.control_socket
            .clone()
            .unwrap_or_else(|| self.root.join("celld.sock"))
    }
}

/// Reads one required option value.
fn next_value(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("missing value for {option}"))
}

/// Returns the one supported command-line shape.
fn usage() -> &'static str {
    "usage: celld-host --host-id ID --root PATH --child VERGLASD [--control SOCKET] [--management-bind IP:PORT]"
}

/// Runs the control endpoint until termination or a fatal socket error.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = match Config::parse() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            return Err("invalid celld-host arguments".into());
        }
    };
    let control_path = config.control_path();
    let supervisor = Arc::new(Mutex::new(HostSupervisor::new(
        HostId::new(config.host_id),
        &config.root,
        ChildCommand::new(config.child_program),
    )));
    let mut server = ControlServer::bind_shared(&control_path, supervisor.clone()).await?;
    eprintln!("celld-host control socket: {}", server.path().display());
    if let Some(bind_address) = config.management_bind {
        let listener = TcpListener::bind(bind_address).await?;
        let management = ManagementApi::new(&config.root, supervisor).router();
        eprintln!("celld-host management API: http://{bind_address}");
        tokio::select! {
            result = server.run() => result?,
            result = axum::serve(listener, management) => result?,
            signal = tokio::signal::ctrl_c() => signal?,
        }
    } else {
        tokio::select! {
            result = server.run() => result?,
            signal = tokio::signal::ctrl_c() => signal?,
        }
    }
    server.supervisor_mut().shutdown().await?;
    Ok(())
}
