//! Authenticated local desired-state API for Docker container placement.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path as AxumPath, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{any, get, post, put};
use axum::{Json, Router};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};

use crate::{
    ContainerSpec, DockerRuntime, ManagedContainer, ObservedState, ReconcileOutcome, RuntimeError,
    VesselProjectSpec, VesselRole, VesselSpec,
};

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const MAX_PROXY_REQUEST_BYTES: usize = 3 * 1024 * 1024;
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
    /// A local application preview path named a non-Application Vessel.
    #[error("Vessel {0} is not an Application")]
    NotApplication(String),
    /// A reflected namespace did not identify an Integration Vessel.
    #[error("namespace {0} is not a registered Integration")]
    NotIntegration(String),
    /// An Integration returned a malformed or mismatched reflection manifest.
    #[error("Integration {name} returned an invalid namespace manifest: {detail}")]
    InvalidNamespaceManifest {
        /// Stable Integration Vessel name.
        name: String,
        /// Bounded validation failure.
        detail: String,
    },
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
                | RuntimeError::InvalidProjectPath { .. }
                | RuntimeError::MissingProjectFile { .. }
                | RuntimeError::InvalidPackageJson(_)
                | RuntimeError::MissingStartScript
                | RuntimeError::ProjectTooLarge
                | RuntimeError::DockerAuthority { .. },
            ) => StatusCode::BAD_REQUEST,
            ServiceError::Runtime(RuntimeError::UnmanagedCollision { .. }) => StatusCode::CONFLICT,
            ServiceError::VesselRequest(_) | ServiceError::VesselResponseTooLarge => {
                StatusCode::BAD_GATEWAY
            }
            ServiceError::NotApplication(_) | ServiceError::NotIntegration(_) => {
                StatusCode::NOT_FOUND
            }
            ServiceError::InvalidNamespaceManifest { .. } => StatusCode::BAD_GATEWAY,
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
            .route("/v1/namespaces", get(list_namespaces))
            .route("/v1/namespaces/{namespace}", get(get_namespace))
            .route(
                "/v1/namespaces/{namespace}/invoke/{method}",
                post(invoke_namespace),
            )
            .route(
                "/v1/vessels/{name}",
                get(get_vessel).put(put_vessel).delete(delete_vessel),
            )
            .route("/v1/vessels/{name}/project", put(put_vessel_project))
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

/// Result of building and reconciling one standalone Vessel project.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VesselProjectView {
    name: String,
    image: String,
    outcome: ReconcileOutcome,
}

/// Builds a standalone TypeScript project and starts its immutable Vessel image.
async fn put_vessel_project(
    State(state): State<Arc<ServiceState>>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    Json(project): Json<VesselProjectSpec>,
) -> Result<Json<VesselProjectView>, ServiceError> {
    authorize(&headers, &state.token)?;
    if name != project.name {
        return Err(ServiceError::IdentityMismatch {
            path: name,
            body: project.name,
        });
    }
    let _operation = state.operation.lock().await;
    let build = state.runtime.build_project(&project).await?;
    let vessel = project.vessel_spec(build.image.clone());
    let specification = state.normalize(vessel.container_spec()?);
    let outcome = state.runtime.reconcile(&specification).await?;
    state
        .desired
        .write()
        .await
        .vessels
        .insert(vessel.name.clone(), vessel);
    state.persist().await?;
    Ok(Json(VesselProjectView {
        name: project.name,
        image: build.image,
        outcome,
    }))
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

/// Discovers the reflected manifests published by every Integration Vessel.
async fn list_namespaces(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<Value>>, ServiceError> {
    authorize(&headers, &state.token)?;
    let names = state
        .desired
        .read()
        .await
        .vessels
        .values()
        .filter(|vessel| vessel.role == VesselRole::Integration)
        .map(|vessel| vessel.name.clone())
        .collect::<Vec<_>>();
    let mut manifests = Vec::with_capacity(names.len());
    for name in names {
        manifests.push(load_namespace_manifest(&state, &name).await?);
    }
    Ok(Json(manifests))
}

/// Returns one Integration Vessel's self-published reflection manifest.
async fn get_namespace(
    State(state): State<Arc<ServiceState>>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, ServiceError> {
    authorize(&headers, &state.token)?;
    require_integration(&state, &namespace).await?;
    Ok(Json(load_namespace_manifest(&state, &namespace).await?))
}

/// Streams one reflected Integration invocation through its private container.
async fn invoke_namespace(
    State(state): State<Arc<ServiceState>>,
    AxumPath((namespace, method)): AxumPath<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ServiceError> {
    authorize(&headers, &state.token)?;
    require_integration(&state, &namespace).await?;
    forward_vessel_streaming(
        &state,
        &namespace,
        &format!("v1/namespace/invoke/{method}"),
        Method::POST,
        &headers,
        body,
    )
    .await
}

/// Ensures a public namespace resolves only to an Integration Vessel.
async fn require_integration(state: &ServiceState, name: &str) -> Result<(), ServiceError> {
    let role = state
        .desired
        .read()
        .await
        .vessels
        .get(name)
        .map(|vessel| vessel.role);
    if role == Some(VesselRole::Integration) {
        Ok(())
    } else {
        Err(ServiceError::NotIntegration(name.to_owned()))
    }
}

/// Reads and validates one bounded manifest from an Integration container.
async fn load_namespace_manifest(state: &ServiceState, name: &str) -> Result<Value, ServiceError> {
    let response = forward_vessel(
        state,
        name,
        "v1/namespace",
        Method::GET,
        &HeaderMap::new(),
        Bytes::new(),
    )
    .await?;
    if !response.status().is_success() {
        return Err(ServiceError::InvalidNamespaceManifest {
            name: name.to_owned(),
            detail: format!("HTTP {}", response.status()),
        });
    }
    let body = axum::body::to_bytes(response.into_body(), MAX_PROXY_RESPONSE_BYTES)
        .await
        .map_err(|error| ServiceError::InvalidNamespaceManifest {
            name: name.to_owned(),
            detail: error.to_string(),
        })?;
    let manifest: Value =
        serde_json::from_slice(&body).map_err(|error| ServiceError::InvalidNamespaceManifest {
            name: name.to_owned(),
            detail: error.to_string(),
        })?;
    validate_namespace_manifest(name, manifest)
}

/// Enforces the stable one-Vessel/one-namespace identity mapping.
fn validate_namespace_manifest(name: &str, manifest: Value) -> Result<Value, ServiceError> {
    if manifest.get("namespace").and_then(Value::as_str) != Some(name) {
        return Err(ServiceError::InvalidNamespaceManifest {
            name: name.to_owned(),
            detail: "namespace must match the Vessel name".to_owned(),
        });
    }
    if !manifest.get("methods").is_some_and(Value::is_object) {
        return Err(ServiceError::InvalidNamespaceManifest {
            name: name.to_owned(),
            detail: "methods must be an object".to_owned(),
        });
    }
    Ok(manifest)
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

/// Serves a local Application preview without exposing Integration HTTP surfaces.
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
    let response = send_vessel(state, name, path, method, headers, body).await?;
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

/// Relays an Integration stream without buffering it in the manager.
async fn forward_vessel_streaming(
    state: &ServiceState,
    name: &str,
    path: &str,
    method: Method,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response, ServiceError> {
    let response = send_vessel(state, name, path, method, headers, body).await?;
    let status = response.status();
    let content_type = response.headers().get(header::CONTENT_TYPE).cloned();
    let mut result = Response::new(Body::from_stream(response.bytes_stream()));
    *result.status_mut() = status;
    if let Some(content_type) = content_type {
        result
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    Ok(result)
}

/// Sends one request to a declared Vessel on the private runtime network.
async fn send_vessel(
    state: &ServiceState,
    name: &str,
    path: &str,
    method: Method,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<reqwest::Response, ServiceError> {
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
    let mut request = reqwest::Client::new()
        .request(method, url)
        .body(body)
        .timeout(Duration::from_secs(30));
    for name in [header::CONTENT_TYPE, header::ACCEPT] {
        if let Some(value) = headers.get(&name) {
            request = request.header(name, value);
        }
    }
    request
        .send()
        .await
        .map_err(|error| ServiceError::VesselRequest(error.to_string()))
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
    use serde_json::json;

    use super::{is_bootstrap_target, load_desired, validate_namespace_manifest};

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

    /// Integration containers cannot claim another Vessel's public namespace.
    #[test]
    fn namespace_manifest_must_match_vessel_identity() {
        let valid = json!({"namespace":"crm","methods":{}});
        assert!(validate_namespace_manifest("crm", valid).is_ok());

        let mismatch = json!({"namespace":"billing","methods":{}});
        let error = validate_namespace_manifest("crm", mismatch)
            .expect_err("mismatched namespace must fail closed");
        assert!(error.to_string().contains("namespace must match"));
    }
}
