//! Runnable tenant-cell host daemon exposing the local supervision control socket.

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Mutex;
use verglasd::{
    ChildCommand, ControlServer, HostId, HostSupervisor, LocalProcessProvisioner, ManagementApi,
};

struct Config {
    host_id: String,
    root: PathBuf,
    child_program: PathBuf,
    control_socket: Option<PathBuf>,
    management_bind: Option<SocketAddr>,
    /// Optional operator-owned startup configuration for Catalog runtime children.
    catalog_host_config: Option<PathBuf>,
    /// Per-object S3-CAS/Foyer configuration passed to every runtime child.
    storage_host_config: Option<PathBuf>,
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
        let mut catalog_host_config = None;
        let mut storage_host_config = None;
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
                "--catalog-host-config" => {
                    catalog_host_config = Some(PathBuf::from(next_value(
                        &mut arguments,
                        "--catalog-host-config",
                    )?));
                }
                "--storage-host-config" => {
                    storage_host_config = Some(PathBuf::from(next_value(
                        &mut arguments,
                        "--storage-host-config",
                    )?));
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
            catalog_host_config,
            storage_host_config,
        })
    }

    /// Resolves the explicit control socket or the root-local default.
    fn control_path(&self) -> PathBuf {
        self.control_socket
            .clone()
            .unwrap_or_else(|| self.root.join("verglasd.sock"))
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
    "usage: verglasd --host-id ID --root PATH --child VERGLAS_RUNTIME [--control SOCKET] [--management-bind IP:PORT] [--storage-host-config PATH] [--catalog-host-config PATH]"
}

/// Runs the control endpoint until termination or a fatal socket error.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = match Config::parse() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            return Err("invalid verglasd arguments".into());
        }
    };
    let control_path = config.control_path();
    let mut provisioner = LocalProcessProvisioner::new();
    if let Some(path) = config.storage_host_config {
        provisioner = provisioner.with_storage_host_config(path);
    }
    if let Some(path) = config.catalog_host_config {
        provisioner = provisioner.with_catalog_host_config(path);
    }
    let supervisor = Arc::new(Mutex::new(HostSupervisor::with_provisioner(
        HostId::new(config.host_id),
        &config.root,
        ChildCommand::new(config.child_program),
        Arc::new(provisioner),
    )));
    let mut server = ControlServer::bind_shared(&control_path, supervisor.clone()).await?;
    eprintln!("verglasd control socket: {}", server.path().display());
    if let Some(bind_address) = config.management_bind {
        let listener = TcpListener::bind(bind_address).await?;
        let management = ManagementApi::new(&config.root, supervisor).router();
        eprintln!("verglasd management API: http://{bind_address}");
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
    server.shutdown().await?;
    Ok(())
}
