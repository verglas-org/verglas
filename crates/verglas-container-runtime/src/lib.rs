//! Places open-source Verglas workloads on an operator-owned Docker Engine.
//!
//! The Docker daemon is part of the trusted local control plane. Container
//! specifications cannot forward its socket or client configuration into a
//! workload. Verglas-owned labels provide the authority for every mutation.

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use bollard::Docker;
use bollard::errors::Error as BollardError;
use bollard::models::{
    ContainerCreateBody, HostConfig, NetworkCreateRequest, PortBinding as DockerPortBinding,
};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, ListContainersOptionsBuilder,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod service;

pub use service::{RuntimeService, ServiceError};

/// Label marking a container as owned by the Verglas runtime.
pub const LABEL_MANAGED: &str = "io.verglas.managed";

/// Label holding the stable Verglas deployment identity.
pub const LABEL_DEPLOYMENT: &str = "io.verglas.deployment";

/// Label holding the digest of the immutable runtime specification.
pub const LABEL_SPEC_DIGEST: &str = "io.verglas.spec-sha256";

const CONTAINER_NAME_PREFIX: &str = "verglas-";

/// One host path exposed inside a managed workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindMount {
    /// Path on the Docker Engine host.
    pub source: String,
    /// Absolute path visible inside the workload.
    pub target: String,
}

/// One TCP port published from a managed container to the local host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedPort {
    /// TCP port listened to inside the container.
    pub container_port: u16,
    /// TCP port published on the Docker Engine host.
    pub host_port: u16,
}

/// Immutable declaration for one locally placed container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerSpec {
    /// Stable deployment identity used to derive the Docker container name.
    pub deployment_id: String,
    /// OCI image reference supplied to the Docker Engine.
    pub image: String,
    /// Optional command overriding the image command.
    #[serde(default)]
    pub command: Vec<String>,
    /// Optional executable overriding the image entrypoint.
    #[serde(default)]
    pub entrypoint: Vec<String>,
    /// Workload environment sorted for deterministic hashing.
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    /// Explicit host bind mounts sorted in declaration order.
    #[serde(default)]
    pub bind_mounts: Vec<BindMount>,
    /// Docker network shared with declared local dependencies.
    #[serde(default)]
    pub network: Option<String>,
    /// TCP ports explicitly published to the Docker Engine host.
    #[serde(default)]
    pub published_ports: Vec<PublishedPort>,
}

impl ContainerSpec {
    /// Creates a minimal immutable deployment specification.
    pub fn new(deployment_id: impl Into<String>, image: impl Into<String>) -> Self {
        Self {
            deployment_id: deployment_id.into(),
            image: image.into(),
            command: Vec::new(),
            entrypoint: Vec::new(),
            environment: BTreeMap::new(),
            bind_mounts: Vec::new(),
            network: None,
            published_ports: Vec::new(),
        }
    }

    /// Replaces the image command with the supplied argument sequence.
    pub fn with_command<I, S>(mut self, command: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.command = command.into_iter().map(Into::into).collect();
        self
    }

    /// Replaces the image entrypoint with the supplied executable sequence.
    pub fn with_entrypoint<I, S>(mut self, entrypoint: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.entrypoint = entrypoint.into_iter().map(Into::into).collect();
        self
    }

    /// Adds or replaces one workload environment entry.
    pub fn with_environment(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.environment.insert(key.into(), value.into());
        self
    }

    /// Adds one explicit host bind mount.
    pub fn with_bind_mount(mut self, source: impl Into<String>, target: impl Into<String>) -> Self {
        self.bind_mounts.push(BindMount {
            source: source.into(),
            target: target.into(),
        });
        self
    }

    /// Attaches the workload to one existing Docker network.
    pub fn with_network(mut self, network: impl Into<String>) -> Self {
        self.network = Some(network.into());
        self
    }

    /// Publishes one container TCP port on a fixed local host port.
    pub fn with_published_port(mut self, container_port: u16, host_port: u16) -> Self {
        self.published_ports.push(PublishedPort {
            container_port,
            host_port,
        });
        self
    }

    /// Validates identity, image, and the Docker authority boundary.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.deployment_id.is_empty()
            || !self
                .deployment_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(RuntimeError::InvalidDeploymentId {
                deployment_id: self.deployment_id.clone(),
            });
        }
        if self.image.trim().is_empty() {
            return Err(RuntimeError::MissingImage);
        }
        for key in self.environment.keys() {
            if key.starts_with("DOCKER_") {
                return Err(RuntimeError::DockerAuthority {
                    detail: format!("environment key {key}"),
                });
            }
        }
        for mount in &self.bind_mounts {
            if is_docker_socket(&mount.source) || is_docker_socket(&mount.target) {
                return Err(RuntimeError::DockerAuthority {
                    detail: format!("bind mount {}:{}", mount.source, mount.target),
                });
            }
        }
        if self
            .network
            .as_ref()
            .is_some_and(|network| network.is_empty())
        {
            return Err(RuntimeError::InvalidNetwork);
        }
        if self
            .published_ports
            .iter()
            .any(|port| port.container_port == 0 || port.host_port == 0)
        {
            return Err(RuntimeError::InvalidPort);
        }
        Ok(())
    }

    /// Computes the stable SHA-256 digest of the immutable declaration.
    pub fn digest(&self) -> Result<String, RuntimeError> {
        self.validate()?;
        let encoded = serde_json::to_vec(self)
            .map_err(|error| RuntimeError::SpecificationEncoding(error.to_string()))?;
        Ok(hex::encode(Sha256::digest(encoded)))
    }

    /// Builds the complete ownership label set for this declaration.
    pub fn labels(&self) -> Result<BTreeMap<String, String>, RuntimeError> {
        Ok(BTreeMap::from([
            (LABEL_MANAGED.to_owned(), "true".to_owned()),
            (LABEL_DEPLOYMENT.to_owned(), self.deployment_id.clone()),
            (LABEL_SPEC_DIGEST.to_owned(), self.digest()?),
        ]))
    }

    /// Returns the deterministic Docker container name for this deployment.
    fn container_name(&self) -> String {
        format!("{CONTAINER_NAME_PREFIX}{}", self.deployment_id)
    }

    /// Normalizes this declaration into an engine create request.
    fn create_request(&self) -> Result<EngineCreateRequest, RuntimeError> {
        self.validate()?;
        Ok(EngineCreateRequest {
            name: self.container_name(),
            image: self.image.clone(),
            command: self.command.clone(),
            entrypoint: self.entrypoint.clone(),
            environment: self.environment.clone(),
            bind_mounts: self.bind_mounts.clone(),
            network: self.network.clone(),
            published_ports: self.published_ports.clone(),
            labels: self.labels()?,
        })
    }
}

/// Normalized lifecycle state independent of Docker response spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedState {
    /// The workload process is running.
    Running,
    /// The container exists but its workload process is not running.
    Stopped,
}

/// Public observation of one Verglas-managed deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedContainer {
    /// Docker Engine container identifier.
    pub id: String,
    /// Stable Verglas deployment identity.
    pub deployment_id: String,
    /// Normalized lifecycle state.
    pub state: ObservedState,
}

/// Result of reconciling one desired container declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileOutcome {
    /// A missing container was created and started.
    Created,
    /// An unchanged stopped container was started.
    Started,
    /// A changed owned container was replaced and started.
    Replaced,
    /// The matching container was already running.
    Unchanged,
}

/// Failures produced by local container placement.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// A deployment identity cannot be mapped safely to a Docker name.
    #[error("invalid deployment id: {deployment_id}")]
    InvalidDeploymentId {
        /// Rejected deployment identity.
        deployment_id: String,
    },
    /// An OCI image reference was not supplied.
    #[error("container image must not be empty")]
    MissingImage,
    /// An explicitly selected Docker network was empty.
    #[error("container network must not be empty")]
    InvalidNetwork,
    /// A published TCP port used the reserved zero value.
    #[error("published container and host ports must be non-zero")]
    InvalidPort,
    /// A workload attempted to receive Docker daemon authority.
    #[error("workload cannot receive Docker authority through {detail}")]
    DockerAuthority {
        /// Rejected environment entry or mount.
        detail: String,
    },
    /// A same-named container is not owned by Verglas.
    #[error("container {name} exists without Verglas ownership labels")]
    UnmanagedCollision {
        /// Colliding Docker container name.
        name: String,
    },
    /// A managed container omitted its stable deployment identity label.
    #[error("managed container {name} has no deployment identity label")]
    MissingDeploymentLabel {
        /// Docker container name with incomplete labels.
        name: String,
    },
    /// Serialization of an immutable declaration failed.
    #[error("failed to encode container specification: {0}")]
    SpecificationEncoding(String),
    /// The Docker Engine rejected or could not execute an operation.
    #[error("Docker Engine operation failed: {0}")]
    Engine(String),
}

/// Trusted local Docker Engine placement adapter.
pub struct DockerRuntime {
    core: DockerRuntimeCore<BollardDockerApi>,
}

impl DockerRuntime {
    /// Connects to the configured local Docker socket using Docker defaults.
    pub fn connect_local() -> Result<Self, RuntimeError> {
        let docker = Docker::connect_with_local_defaults()
            .map_err(|error| RuntimeError::Engine(error.to_string()))?;
        Ok(Self {
            core: DockerRuntimeCore::new(BollardDockerApi { docker }),
        })
    }

    /// Reconciles a desired immutable container declaration.
    pub async fn reconcile(
        &self,
        specification: &ContainerSpec,
    ) -> Result<ReconcileOutcome, RuntimeError> {
        self.core.reconcile(specification).await
    }

    /// Inspects one managed deployment, returning None when it does not exist.
    pub async fn inspect(
        &self,
        deployment_id: &str,
    ) -> Result<Option<ManagedContainer>, RuntimeError> {
        self.core.inspect(deployment_id).await
    }

    /// Stops one managed deployment and reports whether state changed.
    pub async fn stop(&self, deployment_id: &str) -> Result<bool, RuntimeError> {
        self.core.stop(deployment_id).await
    }

    /// Removes one managed deployment and reports whether it existed.
    pub async fn remove(&self, deployment_id: &str) -> Result<bool, RuntimeError> {
        self.core.remove(deployment_id).await
    }

    /// Lists only containers carrying valid Verglas ownership labels.
    pub async fn list(&self) -> Result<Vec<ManagedContainer>, RuntimeError> {
        self.core.list().await
    }

    /// Ensures one labelled shared Docker network exists.
    pub async fn ensure_network(&self, name: &str) -> Result<bool, RuntimeError> {
        self.core.ensure_network(name).await
    }
}

#[derive(Debug, Clone)]
struct EngineContainer {
    id: String,
    name: String,
    labels: BTreeMap<String, String>,
    state: ObservedState,
}

#[derive(Debug, Clone)]
struct EngineCreateRequest {
    name: String,
    image: String,
    command: Vec<String>,
    entrypoint: Vec<String>,
    environment: BTreeMap<String, String>,
    bind_mounts: Vec<BindMount>,
    network: Option<String>,
    published_ports: Vec<PublishedPort>,
    labels: BTreeMap<String, String>,
}

#[async_trait]
trait DockerApi: Send + Sync {
    /// Finds a container by its exact engine name.
    async fn inspect(&self, name: &str) -> Result<Option<EngineContainer>, RuntimeError>;

    /// Pulls the image when necessary and creates one stopped container.
    async fn create(&self, request: EngineCreateRequest) -> Result<(), RuntimeError>;

    /// Starts one existing stopped container.
    async fn start(&self, name: &str) -> Result<(), RuntimeError>;

    /// Stops one existing running container.
    async fn stop(&self, name: &str) -> Result<(), RuntimeError>;

    /// Removes one existing stopped container.
    async fn remove(&self, name: &str) -> Result<(), RuntimeError>;

    /// Lists all containers visible to the engine client.
    async fn list(&self) -> Result<Vec<EngineContainer>, RuntimeError>;

    /// Finds a Docker network and returns its labels.
    async fn inspect_network(
        &self,
        name: &str,
    ) -> Result<Option<BTreeMap<String, String>>, RuntimeError>;

    /// Creates one labelled bridge network.
    async fn create_network(
        &self,
        name: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), RuntimeError>;
}

struct DockerRuntimeCore<A> {
    api: A,
}

impl<A> DockerRuntimeCore<A>
where
    A: DockerApi,
{
    /// Creates a reconciler around one Docker API implementation.
    fn new(api: A) -> Self {
        Self { api }
    }

    /// Reconciles one immutable container specification.
    async fn reconcile(
        &self,
        specification: &ContainerSpec,
    ) -> Result<ReconcileOutcome, RuntimeError> {
        let request = specification.create_request()?;
        let existing = self.api.inspect(&request.name).await?;
        let Some(existing) = existing else {
            self.api.create(request.clone()).await?;
            self.api.start(&request.name).await?;
            return Ok(ReconcileOutcome::Created);
        };
        ensure_owned(&existing)?;

        let desired_digest = request.labels.get(LABEL_SPEC_DIGEST).map(String::as_str);
        let observed_digest = existing.labels.get(LABEL_SPEC_DIGEST).map(String::as_str);
        if desired_digest != observed_digest {
            if existing.state == ObservedState::Running {
                self.api.stop(&existing.name).await?;
            }
            self.api.remove(&existing.name).await?;
            self.api.create(request.clone()).await?;
            self.api.start(&request.name).await?;
            return Ok(ReconcileOutcome::Replaced);
        }
        if existing.state == ObservedState::Stopped {
            self.api.start(&existing.name).await?;
            return Ok(ReconcileOutcome::Started);
        }
        Ok(ReconcileOutcome::Unchanged)
    }

    /// Inspects one managed deployment by stable identity.
    async fn inspect(&self, deployment_id: &str) -> Result<Option<ManagedContainer>, RuntimeError> {
        let name = container_name(deployment_id)?;
        self.api
            .inspect(&name)
            .await?
            .map(|container| normalize_managed(&container))
            .transpose()
    }

    /// Stops one managed deployment idempotently.
    async fn stop(&self, deployment_id: &str) -> Result<bool, RuntimeError> {
        let name = container_name(deployment_id)?;
        let Some(container) = self.api.inspect(&name).await? else {
            return Ok(false);
        };
        ensure_owned(&container)?;
        if container.state == ObservedState::Stopped {
            return Ok(false);
        }
        self.api.stop(&name).await?;
        Ok(true)
    }

    /// Removes one managed deployment idempotently.
    async fn remove(&self, deployment_id: &str) -> Result<bool, RuntimeError> {
        let name = container_name(deployment_id)?;
        let Some(container) = self.api.inspect(&name).await? else {
            return Ok(false);
        };
        ensure_owned(&container)?;
        if container.state == ObservedState::Running {
            self.api.stop(&name).await?;
        }
        self.api.remove(&name).await?;
        Ok(true)
    }

    /// Lists and normalizes all valid Verglas-owned containers.
    async fn list(&self) -> Result<Vec<ManagedContainer>, RuntimeError> {
        self.api
            .list()
            .await?
            .into_iter()
            .filter(is_owned)
            .map(|container| normalize_managed(&container))
            .collect()
    }

    /// Creates a missing shared network and fails closed on a foreign collision.
    async fn ensure_network(&self, name: &str) -> Result<bool, RuntimeError> {
        if name.is_empty() {
            return Err(RuntimeError::InvalidNetwork);
        }
        if let Some(labels) = self.api.inspect_network(name).await? {
            if labels.get(LABEL_MANAGED).map(String::as_str) == Some("true") {
                return Ok(false);
            }
            return Err(RuntimeError::UnmanagedCollision {
                name: name.to_owned(),
            });
        }
        self.api
            .create_network(
                name,
                BTreeMap::from([(LABEL_MANAGED.to_owned(), "true".to_owned())]),
            )
            .await?;
        Ok(true)
    }
}

#[derive(Clone)]
struct BollardDockerApi {
    docker: Docker,
}

#[async_trait]
impl DockerApi for BollardDockerApi {
    /// Inspects one Docker container and maps a missing name to None.
    async fn inspect(&self, name: &str) -> Result<Option<EngineContainer>, RuntimeError> {
        match self.docker.inspect_container(name, None).await {
            Ok(response) => {
                let labels = response
                    .config
                    .and_then(|config| config.labels)
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                Ok(Some(EngineContainer {
                    id: response.id.unwrap_or_default(),
                    name: response
                        .name
                        .unwrap_or_else(|| name.to_owned())
                        .trim_start_matches('/')
                        .to_owned(),
                    labels,
                    state: if response
                        .state
                        .and_then(|state| state.running)
                        .unwrap_or(false)
                    {
                        ObservedState::Running
                    } else {
                        ObservedState::Stopped
                    },
                }))
            }
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(engine_error(error)),
        }
    }

    /// Pulls the declared image and creates one stopped Docker container.
    async fn create(&self, request: EngineCreateRequest) -> Result<(), RuntimeError> {
        if let Err(error) = self.docker.inspect_image(&request.image).await {
            if !is_not_found(&error) {
                return Err(engine_error(error));
            }
            self.docker
                .create_image(
                    Some(
                        CreateImageOptionsBuilder::new()
                            .from_image(&request.image)
                            .build(),
                    ),
                    None,
                    None,
                )
                .try_collect::<Vec<_>>()
                .await
                .map_err(engine_error)?;
        }
        let environment = request
            .environment
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>();
        let binds = request
            .bind_mounts
            .into_iter()
            .map(|mount| format!("{}:{}", mount.source, mount.target))
            .collect::<Vec<_>>();
        let exposed_ports = request
            .published_ports
            .iter()
            .map(|port| format!("{}/tcp", port.container_port))
            .collect::<Vec<_>>();
        let port_bindings = request
            .published_ports
            .into_iter()
            .map(|port| {
                (
                    format!("{}/tcp", port.container_port),
                    Some(vec![DockerPortBinding {
                        host_ip: Some("127.0.0.1".to_owned()),
                        host_port: Some(port.host_port.to_string()),
                    }]),
                )
            })
            .collect::<HashMap<_, _>>();
        let host_config =
            if binds.is_empty() && request.network.is_none() && port_bindings.is_empty() {
                None
            } else {
                Some(HostConfig {
                    binds: (!binds.is_empty()).then_some(binds),
                    network_mode: request.network,
                    port_bindings: (!port_bindings.is_empty()).then_some(port_bindings),
                    ..Default::default()
                })
            };
        let body = ContainerCreateBody {
            image: Some(request.image),
            cmd: (!request.command.is_empty()).then_some(request.command),
            entrypoint: (!request.entrypoint.is_empty()).then_some(request.entrypoint),
            env: (!environment.is_empty()).then_some(environment),
            labels: Some(request.labels.into_iter().collect::<HashMap<_, _>>()),
            exposed_ports: (!exposed_ports.is_empty()).then_some(exposed_ports),
            host_config,
            ..Default::default()
        };
        self.docker
            .create_container(
                Some(
                    CreateContainerOptionsBuilder::new()
                        .name(&request.name)
                        .build(),
                ),
                body,
            )
            .await
            .map_err(engine_error)?;
        Ok(())
    }

    /// Starts one Docker container.
    async fn start(&self, name: &str) -> Result<(), RuntimeError> {
        self.docker
            .start_container(name, None)
            .await
            .map_err(engine_error)
    }

    /// Stops one Docker container.
    async fn stop(&self, name: &str) -> Result<(), RuntimeError> {
        self.docker
            .stop_container(name, None)
            .await
            .map_err(engine_error)
    }

    /// Removes one Docker container.
    async fn remove(&self, name: &str) -> Result<(), RuntimeError> {
        self.docker
            .remove_container(name, None)
            .await
            .map_err(engine_error)
    }

    /// Lists every Docker container, including stopped containers.
    async fn list(&self) -> Result<Vec<EngineContainer>, RuntimeError> {
        let summaries = self
            .docker
            .list_containers(Some(ListContainersOptionsBuilder::new().all(true).build()))
            .await
            .map_err(engine_error)?;
        Ok(summaries
            .into_iter()
            .map(|summary| EngineContainer {
                id: summary.id.unwrap_or_default(),
                name: summary
                    .names
                    .and_then(|names| names.into_iter().next())
                    .unwrap_or_default()
                    .trim_start_matches('/')
                    .to_owned(),
                labels: summary.labels.unwrap_or_default().into_iter().collect(),
                state: if summary.state.map(|state| state.to_string()) == Some("running".to_owned())
                {
                    ObservedState::Running
                } else {
                    ObservedState::Stopped
                },
            })
            .collect())
    }

    /// Inspects one Docker network and maps a missing name to None.
    async fn inspect_network(
        &self,
        name: &str,
    ) -> Result<Option<BTreeMap<String, String>>, RuntimeError> {
        match self.docker.inspect_network(name, None).await {
            Ok(network) => Ok(Some(
                network.labels.unwrap_or_default().into_iter().collect(),
            )),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(engine_error(error)),
        }
    }

    /// Creates one labelled Docker bridge network.
    async fn create_network(
        &self,
        name: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), RuntimeError> {
        self.docker
            .create_network(NetworkCreateRequest {
                name: name.to_owned(),
                labels: Some(labels.into_iter().collect()),
                ..Default::default()
            })
            .await
            .map_err(engine_error)?;
        Ok(())
    }
}

/// Rejects host paths and targets that convey the Docker control socket.
fn is_docker_socket(path: &str) -> bool {
    path.rsplit('/').next() == Some("docker.sock")
}

/// Derives a Docker container name after validating the deployment identity.
fn container_name(deployment_id: &str) -> Result<String, RuntimeError> {
    let specification = ContainerSpec::new(deployment_id, "identity-validation");
    specification.validate()?;
    Ok(specification.container_name())
}

/// Returns whether a container carries the exact Verglas ownership marker.
fn is_owned(container: &EngineContainer) -> bool {
    container.labels.get(LABEL_MANAGED).map(String::as_str) == Some("true")
}

/// Enforces ownership before any mutation of an existing Docker container.
fn ensure_owned(container: &EngineContainer) -> Result<(), RuntimeError> {
    if is_owned(container) {
        Ok(())
    } else {
        Err(RuntimeError::UnmanagedCollision {
            name: container.name.clone(),
        })
    }
}

/// Converts one valid owned engine record into the public observed vocabulary.
fn normalize_managed(container: &EngineContainer) -> Result<ManagedContainer, RuntimeError> {
    ensure_owned(container)?;
    let deployment_id = container
        .labels
        .get(LABEL_DEPLOYMENT)
        .cloned()
        .ok_or_else(|| RuntimeError::MissingDeploymentLabel {
            name: container.name.clone(),
        })?;
    Ok(ManagedContainer {
        id: container.id.clone(),
        deployment_id,
        state: container.state,
    })
}

/// Identifies Docker's name-not-found response without matching error strings.
fn is_not_found(error: &BollardError) -> bool {
    matches!(
        error,
        BollardError::DockerResponseServerError {
            status_code: 404,
            ..
        }
    )
}

/// Erases Bollard implementation details from the crate's public error type.
fn engine_error(error: BollardError) -> RuntimeError {
    RuntimeError::Engine(error.to_string())
}

#[cfg(test)]
mod tests;
