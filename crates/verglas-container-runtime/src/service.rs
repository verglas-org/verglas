//! Authenticated local desired-state API for Docker container placement.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};

use crate::{ContainerSpec, DockerRuntime, ManagedContainer, ReconcileOutcome, RuntimeError};

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);

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
                | RuntimeError::DockerAuthority { .. },
            ) => StatusCode::BAD_REQUEST,
            ServiceError::Runtime(RuntimeError::UnmanagedCollision { .. }) => StatusCode::CONFLICT,
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
            .route("/v1/containers", get(list_containers))
            .route(
                "/v1/containers/{deployment_id}",
                put(put_container).delete(delete_container),
            )
            .route("/v1/containers/{deployment_id}/stop", post(stop_container))
            .route(
                "/v1/containers/{deployment_id}/resume",
                post(resume_container),
            )
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

type DesiredState = BTreeMap<String, DesiredDeployment>;

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
        for deployment in desired.values() {
            if deployment.running {
                self.runtime.reconcile(&deployment.specification).await?;
            } else {
                self.runtime
                    .stop(&deployment.specification.deployment_id)
                    .await?;
            }
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
    state.desired.write().await.insert(
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
    state.desired.write().await.remove(&deployment_id);
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
    if let Some(deployment) = state.desired.write().await.get_mut(&deployment_id) {
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
        let deployment =
            desired
                .get_mut(&deployment_id)
                .ok_or_else(|| RuntimeError::InvalidDeploymentId {
                    deployment_id: deployment_id.clone(),
                })?;
        deployment.running = true;
        deployment.specification.clone()
    };
    let outcome = state.runtime.reconcile(&specification).await?;
    state.persist().await?;
    Ok(Json(outcome))
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
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
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
        assert!(desired.is_empty());
    }
}
