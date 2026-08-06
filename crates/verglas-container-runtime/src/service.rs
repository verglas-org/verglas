//! Authenticated local desired-state API for Docker container placement.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path as AxumPath, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{any, get, post, put};
use axum::{Json, Router};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};

use crate::{
    ContainerSpec, DockerRuntime, ManagedContainer, ObservedState, ReconcileOutcome, RuntimeError,
    VesselRole, VesselSpec,
};

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const MAX_PROXY_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_PROXY_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Failures from the local runtime manager service.
#[derive(Debug, Error)]
pub enum ServiceError {
    /// The request did not carry the configured local bearer token.
    #[error("unauthorized")]
    Unauthorized,
    /// The URL deployment identity did not match the submitted declaration.
    #[error("deployment path {path} does not match body identity {body}")]
    IdentityMismatch {
        /// Deployment identity from the URL.
        path: String,
        /// Deployment identity from the JSON body.
        body: String,
    },
    /// Bootstrap services cannot be submitted to their own runtime manager.
    #[error("bootstrap service {deployment_id} cannot be a managed deployment")]
    BootstrapTarget {
        /// Rejected bootstrap deployment identity.
        deployment_id: String,
    },
    /// Reading or atomically writing the desired-state file failed.
    #[error("desired-state storage failed: {0}")]
    Storage(#[from] std::io::Error),
    /// Decoding the desired-state file failed.
    #[error("desired-state document is invalid: {0}")]
    Decode(#[from] serde_json::Error),
    /// Docker placement failed.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// A private Vessel HTTP call failed before returning a response.
    #[error("Vessel HTTP request failed: {0}")]
    VesselRequest(String),
    /// A private Vessel response exceeded the manager's bounded relay contract.
    #[error("Vessel HTTP response exceeds {MAX_PROXY_RESPONSE_BYTES} bytes")]
    VesselResponseTooLarge,
    /// A public local application path named a non-Application Vessel.
    #[error("Vessel {0} is not an Application")]
    NotApplication(String),
}

impl IntoResponse for ServiceError {
    /// Converts service failures to bounded local HTTP responses.
    fn into_response(self) -> Response {
        let status = match self {
            ServiceError::Unauthorized => StatusCode::UNAUTHORIZED,
            ServiceError::IdentityMismatch { .. }
            | ServiceError::BootstrapTarget { .. }
            | ServiceError::Runtime(
                RuntimeError::InvalidDeploymentId { .. }
                | RuntimeError::MissingImage
                | RuntimeError::InvalidNetwork
                | RuntimeError::InvalidPort
                | RuntimeError::InvalidHealthPath
                | RuntimeError::DockerAuthority { .. },
            ) => StatusCode::BAD_REQUEST,
            ServiceError::Runtime(RuntimeError::UnmanagedCollision { .. }) => StatusCode::CONFLICT,
            ServiceError::VesselRequest(_) | ServiceError::VesselResponseTooLarge => {
                StatusCode::BAD_GATEWAY
            }
            ServiceError::NotApplication(_) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}

/// Long-running trusted manager for local desired container state.
pub struct RuntimeService {
    state: Arc<ServiceState>,
}

impl RuntimeService {
    /// Opens persisted desired state and prepares the authenticated HTTP API.
    pub async fn open(
        runtime: DockerRuntime,
        token: impl Into<String>,
        state_path: impl Into<PathBuf>,
        default_network: impl Into<String>,
    ) -> Result<Self, ServiceError> {
        let state_path = state_path.into();
        let default_network = default_network.into();
        runtime.ensure_network(&default_network).await?;
        let desired = load_desired(&state_path).await?;
        Ok(Self {
            state: Arc::new(ServiceState {
                runtime,
                token: token.into(),
                state_path,
                default_network,
                desired: RwLock::new(desired),
                operation: Mutex::new(()),
            }),
        })
    }

    /// Reconciles every persisted declaration before accepting requests.
    pub async fn recover(&self) -> Result<(), ServiceError> {
        self.state.reconcile_all().await
    }

    /// Returns the HTTP router for embedding in a local process or test.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/health", get(health))
            .route("/apps/{name}", get(application_redirect))
            .route("/apps/{name}/", get(application_root))
            .route("/apps/{name}/{*path}", get(application_proxy))
            .route("/v1/containers", get(list_containers))
            .route("/v1/vessels", get(list_vessels))
            .route(
                "/v1/vessels/{name}",
                get(get_vessel).put(put_vessel).delete(delete_vessel),
            )
            .route("/v1/vessels/{name}/http/{*path}", any(proxy_vessel))
            .route(
                "/v1/containers/{deployment_id}",
                put(put_container).delete(delete_container),
            )
            .route("/v1/containers/{deployment_id}/stop", post(stop_container))
            .route(
                "/v1/containers/{deployment_id}/resume",
                post(resume_container),
            )
            .layer(DefaultBodyLimit::max(MAX_PROXY_REQUEST_BYTES))
            .with_state(Arc::clone(&self.state))
    }

    /// Serves requests and continuously repairs persisted desired state.
    pub async fn serve(self, listener: TcpListener) -> Result<(), ServiceError> {
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
            loop {
                interval.tick().await;
                if let Err(error) = state.reconcile_all().await {
                    eprintln!("verglas-container-runtime: reconciliation failed: {error}");
                }
            }
        });
        axum::serve(listener, self.router())
            .await
            .map_err(ServiceError::Storage)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DesiredDeployment {
    specification: ContainerSpec,
    running: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DesiredState {
    #[serde(default)]
    containers: BTreeMap<String, DesiredDeployment>,
    #[serde(default)]
    vessels: BTreeMap<String, VesselSpec>,
}

struct ServiceState {
    runtime: DockerRuntime,
    token: String,
    state_path: PathBuf,
    default_network: String,
    desired: RwLock<DesiredState>,
    operation: Mutex<()>,
}

impl ServiceState {
    /// Applies every persisted running or stopped declaration to Docker.
    async fn reconcile_all(&self) -> Result<(), ServiceError> {
        let _operation = self.operation.lock().await;
        let desired = self.desired.read().await.clone();
        for deployment in desired.containers.values() {
            if deployment.running {
                self.runtime.reconcile(&deployment.specification).await?;
            } else {
                self.runtime
                    .stop(&deployment.specification.deployment_id)
                    .await?;
            }
        }
        for vessel in desired.vessels.values() {
            self.runtime
                .reconcile(&self.normalize(vessel.container_spec()?))
                .await?;
        }
        Ok(())
    }

    /// Writes the complete desired-state map with an atomic rename.
    async fn persist(&self) -> Result<(), ServiceError> {
        let desired = self.desired.read().await;
        let encoded = serde_json::to_vec_pretty(&*desired)?;
        if let Some(parent) = self.state_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let temporary = self.state_path.with_extension("json.tmp");
        tokio::fs::write(&temporary, encoded).await?;
        tokio::fs::rename(temporary, &self.state_path).await?;
        Ok(())
    }

    /// Applies the shared runtime network when the caller omitted one.
    fn normalize(&self, mut specification: ContainerSpec) -> ContainerSpec {
        if specification.network.is_none() {
            specification.network = Some(self.default_network.clone());
        }
        specification
    }
}

/// Returns process health without requiring container-management authority.
async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Lists the Docker observations for every Verglas-owned container.
async fn list_containers(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagedContainer>>, ServiceError> {
    authorize(&headers, &state.token)?;
    Ok(Json(state.runtime.list().await?))
}

/// Creates or replaces one desired running deployment.
async fn put_container(
    State(state): State<Arc<ServiceState>>,
    AxumPath(deployment_id): AxumPath<String>,
    headers: HeaderMap,
    Json(specification): Json<ContainerSpec>,
) -> Result<Json<ReconcileOutcome>, ServiceError> {
    authorize(&headers, &state.token)?;
    validate_target(&deployment_id, &specification)?;
    let specification = state.normalize(specification);
    let _operation = state.operation.lock().await;
    let outcome = state.runtime.reconcile(&specification).await?;
    state.desired.write().await.containers.insert(
        deployment_id,
        DesiredDeployment {
            specification,
            running: true,
        },
    );
    state.persist().await?;
    Ok(Json(outcome))
}

/// Removes one desired deployment and only its matching owned container.
async fn delete_container(
    State(state): State<Arc<ServiceState>>,
    AxumPath(deployment_id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ServiceError> {
    authorize(&headers, &state.token)?;
    let _operation = state.operation.lock().await;
    state.runtime.remove(&deployment_id).await?;
    state
        .desired
        .write()
        .await
        .containers
        .remove(&deployment_id);
    state.persist().await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Records a stopped desired state and stops the owned container idempotently.
async fn stop_container(
    State(state): State<Arc<ServiceState>>,
    AxumPath(deployment_id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ServiceError> {
    authorize(&headers, &state.token)?;
    let _operation = state.operation.lock().await;
    if let Some(deployment) = state
        .desired
        .write()
        .await
        .containers
        .get_mut(&deployment_id)
    {
        deployment.running = false;
    }
    state.persist().await?;
    state.runtime.stop(&deployment_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Records a running desired state and reconciles the stored declaration.
async fn resume_container(
    State(state): State<Arc<ServiceState>>,
    AxumPath(deployment_id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<ReconcileOutcome>, ServiceError> {
    authorize(&headers, &state.token)?;
    let _operation = state.operation.lock().await;
    let specification = {
        let mut desired = state.desired.write().await;
        let deployment = desired.containers.get_mut(&deployment_id).ok_or_else(|| {
            RuntimeError::InvalidDeploymentId {
                deployment_id: deployment_id.clone(),
            }
        })?;
        deployment.running = true;
        deployment.specification.clone()
    };
    let outcome = state.runtime.reconcile(&specification).await?;
    state.persist().await?;
    Ok(Json(outcome))
}

/// Public runtime observation for a Vessel without secret-bearing configuration.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VesselView {
    name: String,
    role: VesselRole,
    image: String,
    state: Option<ObservedState>,
    health: VesselHealth,
}

/// Result of the optional private-network Vessel health probe.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum VesselHealth {
    Ready,
    Unhealthy,
    Unknown,
}

/// Lists desired Vessels and their current Docker state.
async fn list_vessels(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<VesselView>>, ServiceError> {
    authorize(&headers, &state.token)?;
    let vessels = state
        .desired
        .read()
        .await
        .vessels
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let mut views = Vec::with_capacity(vessels.len());
    for vessel in vessels {
        views.push(vessel_view(&state, vessel).await?);
    }
    Ok(Json(views))
}

/// Returns one desired Vessel and its current Docker state.
async fn get_vessel(
    State(state): State<Arc<ServiceState>>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<VesselView>, ServiceError> {
    authorize(&headers, &state.token)?;
    let vessel = state
        .desired
        .read()
        .await
        .vessels
        .get(&name)
        .cloned()
        .ok_or(RuntimeError::InvalidDeploymentId {
            deployment_id: name,
        })?;
    Ok(Json(vessel_view(&state, vessel).await?))
}

/// Creates or replaces one desired Vessel and its single container.
async fn put_vessel(
    State(state): State<Arc<ServiceState>>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    Json(vessel): Json<VesselSpec>,
) -> Result<Json<ReconcileOutcome>, ServiceError> {
    authorize(&headers, &state.token)?;
    if name != vessel.name {
        return Err(ServiceError::IdentityMismatch {
            path: name,
            body: vessel.name,
        });
    }
    let specification = state.normalize(vessel.container_spec()?);
    let _operation = state.operation.lock().await;
    let outcome = state.runtime.reconcile(&specification).await?;
    state
        .desired
        .write()
        .await
        .vessels
        .insert(vessel.name.clone(), vessel);
    state.persist().await?;
    Ok(Json(outcome))
}

/// Removes one desired Vessel and its owned container.
async fn delete_vessel(
    State(state): State<Arc<ServiceState>>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ServiceError> {
    authorize(&headers, &state.token)?;
    let _operation = state.operation.lock().await;
    state.runtime.remove(&format!("vessel-{name}")).await?;
    state.desired.write().await.vessels.remove(&name);
    state.persist().await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Proxies one authenticated request to a Vessel over the private Docker network.
async fn proxy_vessel(
    State(state): State<Arc<ServiceState>>,
    AxumPath((name, path)): AxumPath<(String, String)>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ServiceError> {
    authorize(&headers, &state.token)?;
    forward_vessel(&state, &name, &path, method, &headers, body).await
}

/// Redirects a local Application URL to its slash-terminated asset base.
async fn application_redirect(AxumPath(name): AxumPath<String>) -> Redirect {
    Redirect::temporary(&format!("/apps/{name}/"))
}

/// Serves the root document of one local Application Vessel.
async fn application_root(
    State(state): State<Arc<ServiceState>>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Response, ServiceError> {
    application_proxy(State(state), AxumPath((name, String::new())), headers).await
}

/// Serves a local Application Vessel without exposing Integration HTTP surfaces.
async fn application_proxy(
    State(state): State<Arc<ServiceState>>,
    AxumPath((name, path)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ServiceError> {
    let vessel = state
        .desired
        .read()
        .await
        .vessels
        .get(&name)
        .cloned()
        .ok_or_else(|| ServiceError::NotApplication(name.clone()))?;
    if vessel.role != VesselRole::Application {
        return Err(ServiceError::NotApplication(name));
    }
    forward_vessel(
        &state,
        &vessel.name,
        &path,
        Method::GET,
        &headers,
        Bytes::new(),
    )
    .await
}

/// Relays one bounded request to a declared Vessel's private HTTP endpoint.
async fn forward_vessel(
    state: &ServiceState,
    name: &str,
    path: &str,
    method: Method,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response, ServiceError> {
    let vessel = state
        .desired
        .read()
        .await
        .vessels
        .get(name)
        .cloned()
        .ok_or_else(|| RuntimeError::InvalidDeploymentId {
            deployment_id: name.to_owned(),
        })?;
    let url = format!("http://verglas-vessel-{name}:{}/{path}", vessel.http.port);
    let response = reqwest::Client::new()
        .request(method, url)
        .header(
            header::CONTENT_TYPE,
            headers
                .get(header::CONTENT_TYPE)
                .cloned()
                .unwrap_or_else(|| "application/json".parse().expect("static content type")),
        )
        .body(body)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|error| ServiceError::VesselRequest(error.to_string()))?;
    let status = response.status();
    let content_type = response.headers().get(header::CONTENT_TYPE).cloned();
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| ServiceError::VesselRequest(error.to_string()))?;
        if bytes.len().saturating_add(chunk.len()) > MAX_PROXY_RESPONSE_BYTES {
            return Err(ServiceError::VesselResponseTooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    let mut result = (status, bytes).into_response();
    if let Some(content_type) = content_type {
        result
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    Ok(result)
}

/// Joins a desired Vessel with its normalized Docker observation.
async fn vessel_view(state: &ServiceState, vessel: VesselSpec) -> Result<VesselView, ServiceError> {
    let observed = state
        .runtime
        .inspect(&format!("vessel-{}", vessel.name))
        .await?;
    let health = match (&observed, &vessel.http.health_path) {
        (Some(container), Some(path)) if container.state == ObservedState::Running => {
            let url = format!(
                "http://verglas-vessel-{}:{}{}",
                vessel.name, vessel.http.port, path
            );
            match reqwest::Client::new()
                .get(url)
                .timeout(Duration::from_secs(2))
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => VesselHealth::Ready,
                _ => VesselHealth::Unhealthy,
            }
        }
        (Some(_), Some(_)) => VesselHealth::Unhealthy,
        _ => VesselHealth::Unknown,
    };
    Ok(VesselView {
        name: vessel.name,
        role: vessel.role,
        image: vessel.image,
        state: observed.map(|container| container.state),
        health,
    })
}

/// Compares the bearer credential without accepting alternate authority forms.
fn authorize(headers: &HeaderMap, token: &str) -> Result<(), ServiceError> {
    let expected = format!("Bearer {token}");
    if headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some(expected.as_str())
    {
        Ok(())
    } else {
        Err(ServiceError::Unauthorized)
    }
}

/// Prevents path/body confusion and recursive bootstrap placement.
fn validate_target(path: &str, specification: &ContainerSpec) -> Result<(), ServiceError> {
    if path != specification.deployment_id {
        return Err(ServiceError::IdentityMismatch {
            path: path.to_owned(),
            body: specification.deployment_id.clone(),
        });
    }
    if is_bootstrap_target(path) {
        return Err(ServiceError::BootstrapTarget {
            deployment_id: path.to_owned(),
        });
    }
    Ok(())
}

/// Returns whether an identity names one of the two Compose bootstrap services.
fn is_bootstrap_target(deployment_id: &str) -> bool {
    matches!(
        deployment_id,
        "server" | "verglas-server" | "container-runtime" | "verglas-container-runtime"
    )
}

/// Loads the last complete desired-state document or starts empty.
async fn load_desired(path: &Path) -> Result<DesiredState, ServiceError> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(DesiredState::default()),
        Err(error) => Err(ServiceError::Storage(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::{is_bootstrap_target, load_desired};

    /// The manager and the data-plane server cannot recursively manage themselves.
    #[test]
    fn bootstrap_targets_are_reserved() {
        assert!(is_bootstrap_target("verglas-server"));
        assert!(is_bootstrap_target("verglas-container-runtime"));
        assert!(!is_bootstrap_target("optional-workspace-1"));
    }

    /// A missing desired-state file initializes an empty registry.
    #[tokio::test]
    async fn missing_state_file_loads_empty() {
        let path = std::env::temp_dir().join(format!(
            "verglas-runtime-missing-state-{}.json",
            std::process::id()
        ));
        let desired = load_desired(&path).await.expect("load missing state");
        assert!(desired.containers.is_empty());
        assert!(desired.vessels.is_empty());
    }
}
