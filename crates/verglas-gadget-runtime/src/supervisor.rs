//! Materializes immutable bundles and supervises one JavaScript host per Gadget.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use sha2::Digest;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::GadgetBundle;

/// Prefix emitted by a healthy child after binding its private listener.
const READY_PREFIX: &str = "VERGLAS_GADGET_READY=";

/// Compatibility module imported by existing Gadget server modules.
const CLOUDFLARE_WORKERS_SHIM: &str = r#"
const capnweb = globalThis.__verglasCapnWeb;
if (!capnweb) throw new Error("Verglas Gadget host did not install Cap'n Web");
export const RpcTarget = capnweb.RpcTarget;
export const RpcStub = capnweb.RpcStub;
export class DurableObject extends RpcTarget {
  constructor(ctx, env) {
    super();
    this.ctx = ctx;
    this.env = env;
  }
}
export class WorkerEntrypoint extends RpcTarget {
  constructor(ctx, env) {
    super();
    this.ctx = ctx;
    this.env = env;
  }
}
export function restore() {
  throw new Error("ctx.restore() is not available before persistent capability storage lands");
}
"#;

/// Child-host command and bounded startup policy.
#[derive(Debug, Clone)]
pub struct HostConfig {
    /// Executable that hosts one materialized Gadget bundle.
    pub command: PathBuf,
    /// Arguments placed before the materialized bundle directory.
    pub arguments: Vec<String>,
    /// Maximum time allowed for the child to announce readiness.
    pub startup_timeout: Duration,
    /// Environment supplied to every child host.
    pub environment: BTreeMap<String, String>,
}

/// Failures while materializing or supervising Gadget child processes.
#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    /// Runtime source could not be written to its isolated directory.
    #[error("materialize Gadget `{id}`: {message}")]
    Materialize {
        /// Gadget whose bundle failed.
        id: String,
        /// Filesystem failure without source bytes.
        message: String,
    },
    /// The configured host process could not be launched.
    #[error("start Gadget `{id}` host: {message}")]
    Start {
        /// Gadget whose host failed.
        id: String,
        /// Process failure.
        message: String,
    },
    /// The child exited or emitted an invalid readiness record.
    #[error("Gadget `{id}` host readiness: {message}")]
    Readiness {
        /// Gadget whose host did not become ready.
        id: String,
        /// Bounded readiness failure.
        message: String,
    },
    /// A child could not be terminated cleanly.
    #[error("stop Gadget `{id}` host: {message}")]
    Stop {
        /// Gadget whose host could not be stopped.
        id: String,
        /// Process failure.
        message: String,
    },
    /// The supervisor lock was poisoned by a failed task.
    #[error("Gadget supervisor state is unavailable")]
    State,
}

/// One live child and the temporary bundle directory that owns its source.
struct RunningHost {
    digest: String,
    endpoint: SocketAddr,
    child: Child,
    _bundle_root: TempDir,
}

/// Multiplexes independently replaceable Gadget child hosts in one container.
pub struct ProcessSupervisor {
    config: HostConfig,
    capability_base_url: String,
    capability_seed: String,
    hosts: Mutex<HashMap<String, RunningHost>>,
}

impl ProcessSupervisor {
    /// Creates an empty child supervisor.
    pub fn new(config: HostConfig, capability_base_url: String, capability_seed: String) -> Self {
        Self {
            config,
            capability_base_url,
            capability_seed,
            hosts: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the existing healthy child or launches the selected revision.
    pub async fn ensure(
        &self,
        id: &str,
        digest: &str,
        bundle: &GadgetBundle,
    ) -> Result<SocketAddr, SupervisorError> {
        let mut hosts = self.hosts.lock().await;
        if let Some(host) = hosts.get_mut(id) {
            let running = host
                .child
                .try_wait()
                .map_err(|_| SupervisorError::State)?
                .is_none();
            if running && host.digest == digest {
                return Ok(host.endpoint);
            }
        }
        if let Some(mut previous) = hosts.remove(id) {
            stop_child(id, &mut previous.child).await?;
        }
        let host = self.start(id, digest, bundle).await?;
        let endpoint = host.endpoint;
        hosts.insert(id.to_owned(), host);
        Ok(endpoint)
    }

    /// Stops and removes one Gadget child if it is active.
    pub async fn stop(&self, id: &str) -> Result<bool, SupervisorError> {
        let mut hosts = self.hosts.lock().await;
        let Some(mut host) = hosts.remove(id) else {
            return Ok(false);
        };
        stop_child(id, &mut host.child).await?;
        Ok(true)
    }

    /// Returns active child identities in stable order for diagnostics.
    pub async fn active(&self) -> Vec<String> {
        let mut ids = self.hosts.lock().await.keys().cloned().collect::<Vec<_>>();
        ids.sort();
        ids
    }

    /// Materializes and starts one private child host.
    async fn start(
        &self,
        id: &str,
        digest: &str,
        bundle: &GadgetBundle,
    ) -> Result<RunningHost, SupervisorError> {
        let root = tempfile::tempdir().map_err(|error| SupervisorError::Materialize {
            id: id.to_owned(),
            message: error.to_string(),
        })?;
        materialize_bundle(id, root.path(), bundle)?;

        let mut command = Command::new(&self.config.command);
        command
            .env_clear()
            .args(&self.config.arguments)
            .arg(root.path())
            .envs(&self.config.environment)
            .env("VERGLAS_GADGET_ID", id)
            .env("VERGLAS_GADGET_VERSION", &bundle.version)
            .env("VERGLAS_GADGET_DIGEST", digest)
            .env(
                "VERGLAS_GADGET_CAPABILITY_ENDPOINT",
                format!("{}/v1/gadgets/{id}/data", self.capability_base_url),
            )
            .env(
                "VERGLAS_GADGET_CAPABILITY_TOKEN",
                gadget_capability_token(&self.capability_seed, id),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| SupervisorError::Start {
            id: id.to_owned(),
            message: error.to_string(),
        })?;
        let stdout = child.stdout.take().ok_or_else(|| SupervisorError::Start {
            id: id.to_owned(),
            message: "child stdout was not piped".to_owned(),
        })?;
        let mut lines = BufReader::new(stdout).lines();
        let ready = tokio::time::timeout(self.config.startup_timeout, lines.next_line())
            .await
            .map_err(|_| SupervisorError::Readiness {
                id: id.to_owned(),
                message: "startup timed out".to_owned(),
            })?
            .map_err(|error| SupervisorError::Readiness {
                id: id.to_owned(),
                message: error.to_string(),
            })?
            .ok_or_else(|| SupervisorError::Readiness {
                id: id.to_owned(),
                message: "child exited before announcing readiness".to_owned(),
            })?;
        let endpoint = ready
            .strip_prefix(READY_PREFIX)
            .ok_or_else(|| SupervisorError::Readiness {
                id: id.to_owned(),
                message: format!("unexpected child output `{ready}`"),
            })?
            .parse::<SocketAddr>()
            .map_err(|error| SupervisorError::Readiness {
                id: id.to_owned(),
                message: format!("invalid endpoint: {error}"),
            })?;
        tokio::spawn(async move {
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(target: "verglas_gadget", "{line}");
            }
        });
        Ok(RunningHost {
            digest: digest.to_owned(),
            endpoint,
            child,
            _bundle_root: root,
        })
    }
}

/// Derives one non-transferable data capability without exposing the runtime or upstream token.
pub(crate) fn gadget_capability_token(seed: &str, gadget_id: &str) -> String {
    let mut digest = sha2::Sha256::new();
    digest.update(b"verglas-gadget-data-capability\0");
    digest.update(gadget_id.as_bytes());
    digest.update(b"\0");
    digest.update(seed.as_bytes());
    hex::encode(digest.finalize())
}

/// Writes one immutable revision into a private temporary directory.
fn materialize_bundle(
    id: &str,
    root: &std::path::Path,
    bundle: &GadgetBundle,
) -> Result<(), SupervisorError> {
    let transformed = bundle
        .server_module
        .replace("\"cloudflare:workers\"", "\"./cloudflare-workers.mjs\"")
        .replace("'cloudflare:workers'", "'./cloudflare-workers.mjs'");
    write_bundle_file(id, &root.join("server.js"), transformed.as_bytes())?;
    write_bundle_file(id, &root.join("client.js"), bundle.client_module.as_bytes())?;
    write_bundle_file(
        id,
        &root.join("cloudflare-workers.mjs"),
        CLOUDFLARE_WORKERS_SHIM.as_bytes(),
    )?;
    for (name, contents) in &bundle.files {
        let path = root.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| SupervisorError::Materialize {
                id: id.to_owned(),
                message: error.to_string(),
            })?;
        }
        write_bundle_file(id, &path, contents.as_bytes())?;
    }
    Ok(())
}

/// Writes one source file while redacting its contents from failures.
fn write_bundle_file(
    id: &str,
    path: &std::path::Path,
    contents: &[u8],
) -> Result<(), SupervisorError> {
    std::fs::write(path, contents).map_err(|error| SupervisorError::Materialize {
        id: id.to_owned(),
        message: format!("{}: {error}", path.display()),
    })
}

/// Terminates one owned child and waits for process reaping.
async fn stop_child(id: &str, child: &mut Child) -> Result<(), SupervisorError> {
    if child
        .try_wait()
        .map_err(|error| SupervisorError::Stop {
            id: id.to_owned(),
            message: error.to_string(),
        })?
        .is_some()
    {
        return Ok(());
    }
    child.kill().await.map_err(|error| SupervisorError::Stop {
        id: id.to_owned(),
        message: error.to_string(),
    })?;
    child.wait().await.map_err(|error| SupervisorError::Stop {
        id: id.to_owned(),
        message: error.to_string(),
    })?;
    Ok(())
}
