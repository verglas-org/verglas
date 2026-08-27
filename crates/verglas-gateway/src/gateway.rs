//! Axum HTTP and WebSocket routing for Workers and resident Durable Objects.
//!
//! Public routes execute the stateless Worker tier first. Durable Objects are
//! resolved only through manifest bindings, while `/do/...` remains an explicit
//! internal/debug route for inspecting a resident object's event socket.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::extract::{FromRequestParts, Path as RoutePath, Request, State, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router, serve};
use base64::Engine;
use bytes::Bytes;
use flate2::read::GzDecoder;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};
use verglas_do_wasm::{
    ComponentDigest, CwasmCache, DoRouter, HostError, Request as WorkerRequest,
    Response as WorkerResponse, WorkerPool,
};

use crate::connection::{DoCallHandler, DoConnection, FetchEvent};
use crate::error::GatewayError;
use crate::manifest::{ArtifactProduct, Binding, Manifest, PipelineBinding, SystemBinding};
use crate::protocol::{FetchResponse, WsOutbound};
use crate::spawn::{DoSpawner, SpawnRequest, VerglasdSpawner};

/// Executes one stateless Worker fetch with a gateway-owned DO router.
#[async_trait]
pub trait WorkerExecutor: Send + Sync {
    /// Runs the Worker fetch without granting it Durable Object storage.
    async fn fetch(
        &self,
        request: WorkerRequest,
        router: Arc<dyn DoRouter>,
    ) -> Result<WorkerResponse, GatewayError>;

    /// Runs one stateless scheduled event without granting Durable Object storage.
    async fn scheduled(
        &self,
        _scheduled_epoch_millis: u64,
        _cron: String,
        _router: Arc<dyn DoRouter>,
    ) -> Result<(), GatewayError> {
        Err(GatewayError::WorkerUnavailable {
            message: "scheduled Worker execution is not configured".to_owned(),
        })
    }
}

/// Worker-pool adapter used by production gateways and real-stack tests.
pub struct WorkerPoolExecutor {
    pool: Arc<WorkerPool>,
}

impl WorkerPoolExecutor {
    /// Wraps one compiled stateless Worker pool for gateway dispatch.
    pub fn new(pool: Arc<WorkerPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl WorkerExecutor for WorkerPoolExecutor {
    /// Dispatches one request through a fresh WorkerPool store and instance.
    async fn fetch(
        &self,
        request: WorkerRequest,
        router: Arc<dyn DoRouter>,
    ) -> Result<WorkerResponse, GatewayError> {
        self.pool
            .dispatch_fetch(router, request)
            .await
            .map_err(|error| GatewayError::WorkerPool {
                message: error.to_string(),
            })
    }

    /// Dispatches one scheduled event through a fresh WorkerPool store and instance.
    async fn scheduled(
        &self,
        scheduled_epoch_millis: u64,
        cron: String,
        router: Arc<dyn DoRouter>,
    ) -> Result<(), GatewayError> {
        self.pool
            .scheduled(router, scheduled_epoch_millis, cron)
            .await
            .map_err(|error| GatewayError::WorkerPool {
                message: error.to_string(),
            })
    }
}

/// A production gateway whose public ingress is Worker-first.
#[derive(Clone)]
pub struct Gateway {
    state: Arc<GatewayState>,
}

/// Shared routing state held by every axum request handler.
struct GatewayState {
    manifest: Manifest,
    deployment: RwLock<ActiveDeployment>,
    data_root: PathBuf,
    spawner: Arc<dyn DoSpawner>,
    connections: Mutex<HashMap<DoKey, Arc<DoConnection>>>,
    pending_websockets: Mutex<HashMap<u64, PendingWebSocket>>,
    hibernating_websockets: Mutex<HashMap<u64, Arc<DoConnection>>>,
    next_websocket: AtomicU64,
    activity: Option<ActivityReporter>,
    ingress_token: Option<String>,
    schedule_token: Option<String>,
}

/// The manifest and compiled stateless Worker swapped as one atomic unit.
struct ActiveDeployment {
    manifest: Manifest,
    worker: Arc<dyn WorkerExecutor>,
}

#[derive(Clone)]
struct ActivityReporter {
    events: tokio::sync::mpsc::Sender<ActivityEvent>,
    active: Arc<AtomicUsize>,
    worker: Arc<RwLock<String>>,
}

#[derive(Clone, Copy)]
enum ActivityEvent {
    Touch,
    CpuPressure,
}

impl ActivityReporter {
    fn begin(&self) -> ActivityLease {
        self.active.fetch_add(1, Ordering::AcqRel);
        let _ = self.events.try_send(ActivityEvent::Touch);
        ActivityLease {
            reporter: Some(self.clone()),
        }
    }

    async fn report_cpu_pressure(&self) {
        let _ = self.events.send(ActivityEvent::CpuPressure).await;
    }
}

/// Keeps the Worker's idle lease alive until the request has fully completed.
struct ActivityLease {
    reporter: Option<ActivityReporter>,
}

impl Drop for ActivityLease {
    fn drop(&mut self) {
        let Some(reporter) = &self.reporter else {
            return;
        };
        reporter.active.fetch_sub(1, Ordering::AcqRel);
        // Renew once more at completion, making the ten-second deadline start
        // after the final active request rather than after it began.
        let _ = reporter.events.try_send(ActivityEvent::Touch);
    }
}

/// The manifest binding and object name that identify one resident actor.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DoKey {
    binding: String,
    name: String,
}

/// The local object connection selected while a Worker accepts an upgrade.
#[derive(Clone)]
struct PendingWebSocket {
    connection: Arc<DoConnection>,
    key: DoKey,
}

/// Durable routing metadata needed to rebuild a logical socket after restart.
#[derive(Clone, Deserialize, Serialize)]
struct HibernatingWebSocketRecord {
    binding: String,
    name: String,
    callback_url: String,
}

/// One edge-owned WebSocket upgrade delivered as a short HTTP event.
#[derive(Deserialize)]
struct HibernatingWebSocketOpen {
    socket_id: u64,
    callback_url: String,
    request: HibernatingWebSocketRequest,
}

/// Original client request fields supplied by the Cloudflare session object.
#[derive(Deserialize)]
struct HibernatingWebSocketRequest {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body_b64: String,
}

/// One client message delivered after Cloudflare wakes the session object.
#[derive(Deserialize)]
struct HibernatingWebSocketMessage {
    socket_id: u64,
    binding: String,
    name: String,
    callback_url: String,
    text: bool,
    data_b64: String,
}

/// One client close delivered after Cloudflare wakes the session object.
#[derive(Deserialize)]
struct HibernatingWebSocketClose {
    socket_id: u64,
    binding: String,
    name: String,
    callback_url: String,
    code: u16,
    reason: String,
}

/// A failed production Worker load that reports the error without a fallback.
struct FailedWorker {
    error: GatewayError,
}

#[async_trait]
impl WorkerExecutor for FailedWorker {
    /// Returns the immutable startup failure for every public Worker request.
    async fn fetch(
        &self,
        _request: WorkerRequest,
        _router: Arc<dyn DoRouter>,
    ) -> Result<WorkerResponse, GatewayError> {
        Err(self.error.clone())
    }

    /// Returns the immutable startup failure for every scheduled event.
    async fn scheduled(
        &self,
        _scheduled_epoch_millis: u64,
        _cron: String,
        _router: Arc<dyn DoRouter>,
    ) -> Result<(), GatewayError> {
        Err(self.error.clone())
    }
}

impl Gateway {
    /// Creates a gateway using verglasd and loads the Worker component from the manifest.
    pub fn new(
        manifest: &Manifest,
        control_socket: impl Into<PathBuf>,
        data_root: impl Into<PathBuf>,
    ) -> Self {
        let spawner = Arc::new(VerglasdSpawner::new(control_socket.into()));
        let worker = match load_worker_executor(manifest) {
            Ok(worker) => worker,
            Err(error) => Arc::new(FailedWorker { error }),
        };
        Self::with_parts(
            manifest,
            data_root,
            spawner,
            worker,
            std::env::var("VERGLAS_INGRESS_TOKEN").ok(),
            std::env::var("VERGLAS_ACTIVITY_TOKEN").ok(),
        )
    }

    /// Creates a gateway with an injected spawner and no Worker component.
    pub fn with_spawner(
        manifest: &Manifest,
        data_root: impl Into<PathBuf>,
        spawner: Arc<dyn DoSpawner>,
    ) -> Self {
        Self::with_parts(
            manifest,
            data_root,
            spawner,
            Arc::new(FailedWorker {
                error: GatewayError::WorkerUnavailable {
                    message: "no Worker executor was injected".to_owned(),
                },
            }),
            std::env::var("VERGLAS_INGRESS_TOKEN").ok(),
            std::env::var("VERGLAS_ACTIVITY_TOKEN").ok(),
        )
    }

    /// Creates a gateway with an injected Worker executor for fake-driven tests.
    pub fn with_worker_executor(
        manifest: &Manifest,
        data_root: impl Into<PathBuf>,
        spawner: Arc<dyn DoSpawner>,
        worker: Arc<dyn WorkerExecutor>,
    ) -> Self {
        Self::with_parts(
            manifest,
            data_root,
            spawner,
            worker,
            std::env::var("VERGLAS_INGRESS_TOKEN").ok(),
            std::env::var("VERGLAS_ACTIVITY_TOKEN").ok(),
        )
    }

    /// Builds a gateway with explicit control-plane credentials for deterministic tests.
    pub fn with_worker_executor_tokens(
        manifest: &Manifest,
        data_root: impl Into<PathBuf>,
        spawner: Arc<dyn DoSpawner>,
        worker: Arc<dyn WorkerExecutor>,
        ingress_token: Option<String>,
        schedule_token: Option<String>,
    ) -> Self {
        Self::with_parts(
            manifest,
            data_root,
            spawner,
            worker,
            ingress_token,
            schedule_token,
        )
    }

    /// Creates a gateway around the sibling crate's compiled WorkerPool.
    pub fn with_worker_pool(
        manifest: &Manifest,
        data_root: impl Into<PathBuf>,
        spawner: Arc<dyn DoSpawner>,
        pool: Arc<WorkerPool>,
    ) -> Self {
        Self::with_worker_executor(
            manifest,
            data_root,
            spawner,
            Arc::new(WorkerPoolExecutor::new(pool)),
        )
    }

    /// Builds shared state while preserving one connection per object key.
    fn with_parts(
        manifest: &Manifest,
        data_root: impl Into<PathBuf>,
        spawner: Arc<dyn DoSpawner>,
        worker: Arc<dyn WorkerExecutor>,
        ingress_token: Option<String>,
        schedule_token: Option<String>,
    ) -> Self {
        Self {
            state: Arc::new(GatewayState {
                manifest: manifest.clone(),
                deployment: RwLock::new(ActiveDeployment {
                    manifest: manifest.clone(),
                    worker,
                }),
                data_root: data_root.into(),
                spawner,
                connections: Mutex::new(HashMap::new()),
                pending_websockets: Mutex::new(HashMap::new()),
                hibernating_websockets: Mutex::new(HashMap::new()),
                next_websocket: AtomicU64::new(1),
                activity: activity_reporter_from_env(),
                ingress_token,
                schedule_token,
            }),
        }
    }

    /// Returns the axum router with public Worker routes before internal DO routes.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/__verglas/assign", post(assign_worker_handler))
            .route(
                "/__verglas/scheduled",
                axum::routing::post(scheduled_handler),
            )
            .route(
                "/__verglas/websocket/open",
                post(hibernating_websocket_open_handler),
            )
            .route(
                "/__verglas/websocket/message",
                post(hibernating_websocket_message_handler),
            )
            .route(
                "/__verglas/websocket/close",
                post(hibernating_websocket_close_handler),
            )
            .route("/", any(public_handler))
            .route("/{*path}", any(public_handler))
            .route("/do/{binding}/{name}/ws", get(websocket_handler))
            .route("/do/{binding}/{name}", any(http_root_handler))
            .route("/do/{binding}/{name}/{*path}", any(http_path_handler))
            .with_state(Arc::clone(&self.state))
            .layer(middleware::from_fn_with_state(
                Arc::clone(&self.state),
                resource_pressure_middleware,
            ))
    }

    /// Serves the gateway router on one already-bound TCP listener.
    pub async fn serve(self, listener: TcpListener) -> Result<(), GatewayError> {
        serve(listener, self.router())
            .await
            .map_err(|error| GatewayError::control_io("serve HTTP", error))
    }

    /// Checks a route binding without spawning or connecting to its object.
    pub fn resolve_binding(&self, binding: &str, _name: &str) -> Result<&Binding, GatewayError> {
        self.state
            .manifest
            .binding(binding)
            .ok_or_else(|| GatewayError::UnknownBinding {
                binding: binding.to_owned(),
            })
    }

    /// Resolves a pipeline binding only when the request names its stream object.
    pub fn resolve_pipeline(
        &self,
        binding: &str,
        stream: &str,
    ) -> Result<&PipelineBinding, GatewayError> {
        let pipeline =
            self.state
                .manifest
                .pipeline(binding)
                .ok_or_else(|| GatewayError::UnknownBinding {
                    binding: binding.to_owned(),
                })?;
        if pipeline.stream() != stream {
            return Err(GatewayError::UnknownObject {
                binding: binding.to_owned(),
                name: stream.to_owned(),
            });
        }
        Ok(pipeline)
    }

    /// Resolves one explicit Pipeline, Sink, or Catalog service binding.
    pub fn resolve_service(
        &self,
        binding: &str,
        object: &str,
    ) -> Result<&SystemBinding, GatewayError> {
        let service = self
            .state
            .manifest
            .services()
            .iter()
            .find(|service| service.binding() == binding)
            .ok_or_else(|| GatewayError::UnknownBinding {
                binding: binding.to_owned(),
            })?;
        if service.object() != object {
            return Err(GatewayError::UnknownObject {
                binding: binding.to_owned(),
                name: object.to_owned(),
            });
        }
        Ok(service)
    }

    /// Finds or creates the one resident event actor for a route key.
    async fn connection_for(
        state: &Arc<GatewayState>,
        binding: &str,
        name: &str,
    ) -> Result<Arc<DoConnection>, GatewayError> {
        let manifest = state.deployment.read().await.manifest.clone();
        let product = manifest
            .product_for_binding(binding, name)
            .map_err(|error| match error {
                crate::manifest::ManifestError::UnknownBinding { binding } => {
                    GatewayError::UnknownBinding { binding }
                }
                crate::manifest::ManifestError::WrongBindingObject {
                    binding, actual, ..
                } => GatewayError::UnknownObject {
                    binding,
                    name: actual,
                },
                error => GatewayError::SpawnRejected {
                    message: error.to_string(),
                },
            })?;
        let artifact = manifest.artifact_for_product(product).map_err(|error| {
            GatewayError::SpawnRejected {
                message: error.to_string(),
            }
        })?;
        let do_id = do_identity(binding, name)?;
        let key = DoKey {
            binding: binding.to_owned(),
            name: name.to_owned(),
        };
        let mut connections = state.connections.lock().await;
        if let Some(connection) = connections.get(&key) {
            return Ok(Arc::clone(connection));
        }
        let mut request = SpawnRequest::new(
            do_id,
            binding.to_owned(),
            name.to_owned(),
            artifact.digest().to_owned(),
            artifact.component_dir().to_path_buf(),
            state.data_root.clone(),
        );
        if let Some(cache_dir) = artifact.cwasm_cache_dir() {
            request = request.with_cwasm_cache_dir(cache_dir.to_path_buf());
        }
        if product == ArtifactProduct::Catalog
            && let Some(host_service) = manifest.host_services().first().cloned()
        {
            request = request.with_host_service(host_service);
        }
        let event_socket = state.spawner.spawn(request).await?;
        let do_call = Arc::new(GatewayDoRouter {
            state: Arc::clone(state),
            source: Some(key.clone()),
        });
        let connection = Arc::new(DoConnection::connect(event_socket, do_call).await?);
        connections.insert(key, Arc::clone(&connection));
        Ok(connection)
    }

    /// Allocates one gateway-wide pending WebSocket identity.
    fn next_websocket(state: &GatewayState) -> Result<u64, GatewayError> {
        state
            .next_websocket
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| GatewayError::Protocol {
                message: "pending WebSocket identity exhausted".to_owned(),
            })
    }
}

/// Runs one authenticated control-plane cron event through the stateless Worker export.
async fn scheduled_handler(
    State(state): State<Arc<GatewayState>>,
    request: Request,
) -> Result<Response, GatewayError> {
    require_ingress(&state, request.headers())?;
    let presented = request
        .headers()
        .get("x-verglas-scheduled-token")
        .and_then(|value| value.to_str().ok());
    if presented != state.schedule_token.as_deref() || state.schedule_token.is_none() {
        return Err(GatewayError::UnauthorizedScheduled);
    }
    let body = to_bytes(request.into_body(), 16 * 1024)
        .await
        .map_err(|error| GatewayError::InvalidHttp {
            message: error.to_string(),
        })?;
    let event: serde_json::Value =
        serde_json::from_slice(&body).map_err(|error| GatewayError::InvalidHttp {
            message: error.to_string(),
        })?;
    let scheduled_epoch_millis = event
        .get("scheduled_time")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| GatewayError::InvalidHttp {
            message: "scheduled_time must be a non-negative integer".to_owned(),
        })?;
    let cron = event
        .get("cron")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| GatewayError::InvalidHttp {
            message: "cron must be a non-empty string".to_owned(),
        })?
        .to_owned();
    let _activity = begin_activity(&state);
    let router = Arc::new(GatewayDoRouter {
        state: Arc::clone(&state),
        source: None,
    });
    let worker = Arc::clone(&state.deployment.read().await.worker);
    worker
        .scheduled(scheduled_epoch_millis, cron, router)
        .await?;
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    Ok(response)
}

/// Opens one logical guest socket while Cloudflare owns the client transport.
async fn hibernating_websocket_open_handler(
    State(state): State<Arc<GatewayState>>,
    request: Request,
) -> Result<Response, GatewayError> {
    require_websocket_control(&state, request.headers())?;
    let _activity = begin_activity(&state);
    let body = to_bytes(request.into_body(), 2 * 1024 * 1024)
        .await
        .map_err(|error| GatewayError::InvalidHttp {
            message: error.to_string(),
        })?;
    let open: HibernatingWebSocketOpen =
        serde_json::from_slice(&body).map_err(|error| GatewayError::InvalidHttp {
            message: error.to_string(),
        })?;
    validate_websocket_callback(&open.callback_url)?;
    let event = FetchEvent {
        method: open.request.method,
        url: open.request.url,
        headers: open.request.headers,
        body: base64::engine::general_purpose::STANDARD
            .decode(open.request.body_b64)
            .map_err(|error| GatewayError::InvalidHttp {
                message: format!("WebSocket request body is not base64: {error}"),
            })?,
        ws: Some(open.socket_id),
    };
    let router = Arc::new(GatewayDoRouter {
        state: Arc::clone(&state),
        source: None,
    });
    let worker = Arc::clone(&state.deployment.read().await.worker);
    let response = worker.fetch(worker_request(&event), router).await?;
    if response.status != 101 || response.accept_ws != Some(open.socket_id) {
        state
            .pending_websockets
            .lock()
            .await
            .remove(&open.socket_id);
        return Err(GatewayError::Protocol {
            message: format!(
                "Worker did not accept hibernating WebSocket {}",
                open.socket_id
            ),
        });
    }
    let pending = state
        .pending_websockets
        .lock()
        .await
        .remove(&open.socket_id)
        .ok_or_else(|| GatewayError::Protocol {
            message: format!(
                "Worker accepted hibernating WebSocket {} without a DO connection",
                open.socket_id
            ),
        })?;
    let effects = pending
        .connection
        .open_websocket_with_id(open.socket_id)
        .await?;
    let record = HibernatingWebSocketRecord {
        binding: pending.key.binding,
        name: pending.key.name,
        callback_url: open.callback_url,
    };
    write_hibernating_websocket_record(&state, open.socket_id, &record).await?;
    state
        .hibernating_websockets
        .lock()
        .await
        .insert(open.socket_id, Arc::clone(&pending.connection));
    spawn_hibernating_effect_forwarder(
        Arc::clone(&state),
        open.socket_id,
        record.clone(),
        Arc::clone(&pending.connection),
        effects,
    );
    Ok(Json(serde_json::json!({
        "binding": record.binding,
        "name": record.name,
    }))
    .into_response())
}

/// Delivers one client frame and waits for the guest event transaction to commit.
async fn hibernating_websocket_message_handler(
    State(state): State<Arc<GatewayState>>,
    request: Request,
) -> Result<Response, GatewayError> {
    require_websocket_control(&state, request.headers())?;
    let _activity = begin_activity(&state);
    let body = to_bytes(request.into_body(), 2 * 1024 * 1024)
        .await
        .map_err(|error| GatewayError::InvalidHttp {
            message: error.to_string(),
        })?;
    let message: HibernatingWebSocketMessage =
        serde_json::from_slice(&body).map_err(|error| GatewayError::InvalidHttp {
            message: error.to_string(),
        })?;
    let data = base64::engine::general_purpose::STANDARD
        .decode(message.data_b64)
        .map_err(|error| GatewayError::InvalidHttp {
            message: format!("WebSocket message body is not base64: {error}"),
        })?;
    let connection = ensure_hibernating_websocket(
        &state,
        message.socket_id,
        HibernatingWebSocketRecord {
            binding: message.binding,
            name: message.name,
            callback_url: message.callback_url,
        },
    )
    .await?;
    match connection
        .websocket_message(message.socket_id, message.text, data)
        .await
    {
        Ok(()) | Err(GatewayError::WorkerError { .. }) => {}
        Err(error) => return Err(error),
    }
    Ok(no_content_response())
}

/// Delivers one client close and removes its restart routing metadata.
async fn hibernating_websocket_close_handler(
    State(state): State<Arc<GatewayState>>,
    request: Request,
) -> Result<Response, GatewayError> {
    require_websocket_control(&state, request.headers())?;
    let _activity = begin_activity(&state);
    let body = to_bytes(request.into_body(), 16 * 1024)
        .await
        .map_err(|error| GatewayError::InvalidHttp {
            message: error.to_string(),
        })?;
    let close: HibernatingWebSocketClose =
        serde_json::from_slice(&body).map_err(|error| GatewayError::InvalidHttp {
            message: error.to_string(),
        })?;
    let connection = ensure_hibernating_websocket(
        &state,
        close.socket_id,
        HibernatingWebSocketRecord {
            binding: close.binding,
            name: close.name,
            callback_url: close.callback_url,
        },
    )
    .await?;
    connection
        .websocket_close(close.socket_id, close.code, close.reason)
        .await?;
    connection.remove_websocket(close.socket_id).await;
    state
        .hibernating_websockets
        .lock()
        .await
        .remove(&close.socket_id);
    remove_hibernating_websocket_record(&state, close.socket_id).await?;
    Ok(no_content_response())
}

/// Requires both the Fly mesh identity and the edge WebSocket credential.
fn require_websocket_control(
    state: &GatewayState,
    headers: &HeaderMap,
) -> Result<(), GatewayError> {
    require_ingress(state, headers)?;
    let presented = headers
        .get("x-verglas-websocket-token")
        .and_then(|value| value.to_str().ok());
    if state.schedule_token.is_some() && presented == state.schedule_token.as_deref() {
        Ok(())
    } else {
        Err(GatewayError::UnauthorizedWebSocket)
    }
}

/// Rejects callbacks outside HTTPS, except loopback endpoints used by local tests.
fn validate_websocket_callback(callback: &str) -> Result<(), GatewayError> {
    let url = reqwest::Url::parse(callback).map_err(|error| GatewayError::InvalidHttp {
        message: format!("invalid WebSocket callback URL: {error}"),
    })?;
    let loopback = url
        .host_str()
        .and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .is_some_and(|address| address.is_loopback());
    if url.scheme() == "https" || (url.scheme() == "http" && loopback) {
        Ok(())
    } else {
        Err(GatewayError::InvalidHttp {
            message: "WebSocket callback URL must use HTTPS".to_owned(),
        })
    }
}

/// Restores a logical socket from disk when a Fly process has restarted.
async fn ensure_hibernating_websocket(
    state: &Arc<GatewayState>,
    socket_id: u64,
    edge_record: HibernatingWebSocketRecord,
) -> Result<Arc<DoConnection>, GatewayError> {
    if let Some(connection) = state
        .hibernating_websockets
        .lock()
        .await
        .get(&socket_id)
        .cloned()
    {
        return Ok(connection);
    }
    let record = match read_hibernating_websocket_record(state, socket_id).await? {
        Some(record) => record,
        None => {
            write_hibernating_websocket_record(state, socket_id, &edge_record).await?;
            edge_record
        }
    };
    let connection = Gateway::connection_for(state, &record.binding, &record.name).await?;
    let effects = connection.open_websocket_with_id(socket_id).await?;
    state
        .hibernating_websockets
        .lock()
        .await
        .insert(socket_id, Arc::clone(&connection));
    spawn_hibernating_effect_forwarder(
        Arc::clone(state),
        socket_id,
        record,
        Arc::clone(&connection),
        effects,
    );
    Ok(connection)
}

/// Forwards committed guest effects to the owning Cloudflare session object.
fn spawn_hibernating_effect_forwarder(
    state: Arc<GatewayState>,
    socket_id: u64,
    record: HibernatingWebSocketRecord,
    connection: Arc<DoConnection>,
    mut effects: tokio::sync::mpsc::UnboundedReceiver<WsOutbound>,
) {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        while let Some(effect) = effects.recv().await {
            let (body, closes) = match effect {
                WsOutbound::Message { text, data } => (
                    serde_json::json!({
                        "socket_id": socket_id,
                        "type": "message",
                        "text": text,
                        "data_b64": base64::engine::general_purpose::STANDARD.encode(data),
                    }),
                    false,
                ),
                WsOutbound::Close { code, reason } => (
                    serde_json::json!({
                        "socket_id": socket_id,
                        "type": "close",
                        "code": code,
                        "reason": reason,
                    }),
                    true,
                ),
            };
            let mut delivered = false;
            for delay in [0_u64, 100, 250, 500] {
                if delay != 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                let mut request = client.post(&record.callback_url).json(&body);
                if let Some(token) = state.schedule_token.as_deref() {
                    request = request.header("x-verglas-websocket-token", token);
                }
                if request
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    delivered = true;
                    break;
                }
            }
            if !delivered || closes {
                break;
            }
        }
        connection.remove_websocket(socket_id).await;
        state.hibernating_websockets.lock().await.remove(&socket_id);
        let _ = remove_hibernating_websocket_record(&state, socket_id).await;
    });
}

/// Returns the stable per-socket metadata path under the gateway data root.
fn hibernating_websocket_record_path(state: &GatewayState, socket_id: u64) -> PathBuf {
    state
        .data_root
        .join("websocket-sessions")
        .join(format!("{socket_id}.json"))
}

/// Persists socket routing before acknowledging the edge open event.
async fn write_hibernating_websocket_record(
    state: &GatewayState,
    socket_id: u64,
    record: &HibernatingWebSocketRecord,
) -> Result<(), GatewayError> {
    let path = hibernating_websocket_record_path(state, socket_id);
    let parent = path.parent().ok_or_else(|| GatewayError::InvalidHttp {
        message: "WebSocket record path has no parent".to_owned(),
    })?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| GatewayError::control_io("create WebSocket record directory", error))?;
    let bytes = serde_json::to_vec(record).map_err(|error| GatewayError::InvalidHttp {
        message: format!("encode WebSocket record: {error}"),
    })?;
    tokio::fs::write(&path, bytes)
        .await
        .map_err(|error| GatewayError::control_io("write WebSocket record", error))
}

/// Loads socket routing left by the process that originally accepted it.
async fn read_hibernating_websocket_record(
    state: &GatewayState,
    socket_id: u64,
) -> Result<Option<HibernatingWebSocketRecord>, GatewayError> {
    let bytes = match tokio::fs::read(hibernating_websocket_record_path(state, socket_id)).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(GatewayError::control_io("read WebSocket record", error)),
    };
    let record = serde_json::from_slice(&bytes).map_err(|error| GatewayError::Protocol {
        message: format!("invalid persisted WebSocket record: {error}"),
    })?;
    Ok(Some(record))
}

/// Deletes routing after the client close transaction commits.
async fn remove_hibernating_websocket_record(
    state: &GatewayState,
    socket_id: u64,
) -> Result<(), GatewayError> {
    match tokio::fs::remove_file(hibernating_websocket_record_path(state, socket_id)).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(GatewayError::control_io("remove WebSocket record", error)),
    }
}

/// Builds an empty successful response for private event delivery routes.
fn no_content_response() -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    response
}

fn cpu_throttled_usec() -> Option<u64> {
    for (path, field, divisor) in [
        ("/sys/fs/cgroup/cpu.stat", "throttled_usec", 1_u64),
        (
            "/sys/fs/cgroup/cpu,cpuacct/cpu.stat",
            "throttled_time",
            1_000_u64,
        ),
    ] {
        let Ok(stat) = std::fs::read_to_string(path) else {
            continue;
        };
        if let Some(value) = stat.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            match (fields.next(), fields.next()) {
                (Some(name), Some(value)) if name == field => value.parse::<u64>().ok(),
                _ => None,
            }
        }) {
            return Some(value / divisor);
        }
    }
    None
}

fn cgroup_cpu_usage_usec() -> Option<u64> {
    if let Ok(stat) = std::fs::read_to_string("/sys/fs/cgroup/cpu.stat") {
        if let Some(value) = stat.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            match (fields.next(), fields.next()) {
                (Some("usage_usec"), Some(value)) => value.parse::<u64>().ok(),
                _ => None,
            }
        }) {
            return Some(value);
        }
    }
    std::fs::read_to_string("/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage")
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|nanoseconds| nanoseconds / 1_000)
}

fn process_cpu_usec() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the pointed-to rusage on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized usage.
    let usage = unsafe { usage.assume_init() };
    let user = u64::try_from(usage.ru_utime.tv_sec)
        .ok()?
        .saturating_mul(1_000_000)
        + u64::try_from(usage.ru_utime.tv_usec).ok()?;
    let system = u64::try_from(usage.ru_stime.tv_sec)
        .ok()?
        .saturating_mul(1_000_000)
        + u64::try_from(usage.ru_stime.tv_usec).ok()?;
    Some(user.saturating_add(system))
}

fn resource_pressure_detected(
    wall_usec: u64,
    workload_cpu_usec: Option<(u64, u64)>,
    throttled_usec: Option<(u64, u64)>,
) -> bool {
    let threshold = (wall_usec / 5).max(10_000);
    let cgroup_pressure = matches!(
        throttled_usec,
        Some((start, end)) if end.saturating_sub(start) >= threshold
    );
    // Fly enforces shared-CPU bandwidth outside the guest Firecracker VM, so
    // its guest cgroup exposes no quota or throttling counter. A CPU-bound
    // request then consumes meaningful process CPU but takes far more wall
    // time than CPU time. This second signal detects that externally imposed
    // scheduling delay without classifying mostly-I/O waits as CPU pressure.
    let workload_pressure = workload_cpu_usec.is_some_and(|(start, end)| {
        let cpu_usec = end.saturating_sub(start);
        let externally_delayed =
            cpu_usec >= 100_000 && wall_usec >= 250_000 && cpu_usec.saturating_mul(2) <= wall_usec;
        let sustained = cpu_usec >= 1_000_000;
        externally_delayed || sustained
    });
    cgroup_pressure || workload_pressure
}

/** Marks a response when the VM's CPU cgroup was throttled while serving it.
 * The edge consumes this internal header and persists a vertical promotion. */
async fn resource_pressure_middleware(
    State(state): State<Arc<GatewayState>>,
    request: Request,
    next: Next,
) -> Response {
    let before_throttled = cpu_throttled_usec();
    // A Worker executes in a child runtime process. Account the whole guest
    // cgroup when available, with the gateway process as a portable fallback.
    let before_cpu = cgroup_cpu_usage_usec().or_else(process_cpu_usec);
    let started = Instant::now();
    let mut response = next.run(request).await;
    let wall_usec = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    if resource_pressure_detected(
        wall_usec,
        before_cpu.zip(cgroup_cpu_usage_usec().or_else(process_cpu_usec)),
        before_throttled.zip(cpu_throttled_usec()),
    ) {
        if let Some(reporter) = &state.activity {
            reporter.report_cpu_pressure().await;
        }
        response.headers_mut().insert(
            HeaderName::from_static("x-verglas-resource-pressure"),
            HeaderValue::from_static("cpu"),
        );
    }
    response
}

fn activity_reporter_from_env() -> Option<ActivityReporter> {
    let url = std::env::var("VERGLAS_ACTIVITY_URL").ok()?;
    let token = std::env::var("VERGLAS_ACTIVITY_TOKEN").ok()?;
    let tenant = std::env::var("VERGLAS_TENANT_ID").ok()?;
    let worker = Arc::new(RwLock::new(std::env::var("VERGLAS_WORKER_NAME").ok()?));
    let callback_worker = Arc::clone(&worker);
    let pressure_url = url.replace("/v1/internal/tenant-touch", "/v1/internal/worker-pressure");
    let (events, mut receiver) = tokio::sync::mpsc::channel(8);
    let active = Arc::new(AtomicUsize::new(0));
    let active_requests = Arc::clone(&active);
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut heartbeat = tokio::time::interval(Duration::from_secs(2));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let event = tokio::select! {
                event = receiver.recv() => {
                    match event {
                        Some(event) => event,
                        None => break,
                    }
                }
                _ = heartbeat.tick(), if active_requests.load(Ordering::Acquire) > 0 => ActivityEvent::Touch,
            };
            let pressure = matches!(event, ActivityEvent::CpuPressure);
            let worker_name = callback_worker.read().await.clone();
            let body = if pressure {
                serde_json::json!({
                    "tenant_id": tenant,
                    "worker_name": worker_name,
                    "signal": "cpu",
                })
            } else {
                serde_json::json!({
                    "tenant_id": tenant,
                    "worker_name": worker_name,
                })
            };
            let _ = client
                .post(if pressure { &pressure_url } else { &url })
                .header("x-verglas-cloud-internal", &token)
                .json(&body)
                .send()
                .await;
        }
    });
    // The startup edge is activity too. Arming the lease here means the
    // ten-second idle window starts only after artifacts are downloaded and
    // the gateway is constructed, rather than while Fly is still pulling the
    // image or the cell is booting.
    let _ = events.try_send(ActivityEvent::Touch);
    Some(ActivityReporter {
        events,
        active,
        worker,
    })
}

fn begin_activity(state: &GatewayState) -> ActivityLease {
    state
        .activity
        .as_ref()
        .map_or(ActivityLease { reporter: None }, ActivityReporter::begin)
}

fn require_ingress(state: &GatewayState, headers: &HeaderMap) -> Result<(), GatewayError> {
    let Some(expected) = state.ingress_token.as_deref() else {
        return Ok(());
    };
    let presented = headers
        .get("x-verglas-worker-ingress")
        .and_then(|value| value.to_str().ok());
    if presented == Some(expected) {
        Ok(())
    } else {
        Err(GatewayError::UnauthorizedIngress)
    }
}

/// Routes Worker and DO binding calls into resident event actors.
struct GatewayDoRouter {
    state: Arc<GatewayState>,
    source: Option<DoKey>,
}

impl GatewayDoRouter {
    /// Forwards one fetch to a target object and tracks accepted WebSockets.
    async fn route(
        &self,
        binding: String,
        object: String,
        event: FetchEvent,
    ) -> Result<FetchResponse, GatewayError> {
        let manifest = self.state.deployment.read().await.manifest.clone();
        if let Some(origin) = manifest
            .origin_for_binding(&binding, &object)
            .map_err(|error| GatewayError::SpawnRejected {
                message: error.to_string(),
            })?
        {
            return remote_fetch(
                origin,
                &binding,
                &object,
                event,
                self.state.ingress_token.as_deref(),
            )
            .await;
        }
        if let Some(source) = &self.source
            && source.binding == binding
            && source.name == object
        {
            return Err(GatewayError::SelfCallDeadlock {
                source_binding: source.binding.clone(),
                source_object: source.name.clone(),
                target_binding: binding,
                target_object: object,
            });
        }
        let connection = Gateway::connection_for(&self.state, &binding, &object).await?;
        let response = connection.fetch(event.clone()).await?;
        if let Some(accepted) = response.accept_ws {
            let Some(pending) = event.ws else {
                return Err(GatewayError::Protocol {
                    message: format!("DO accepted WebSocket {accepted} without a pending request"),
                });
            };
            if pending != accepted {
                return Err(GatewayError::Protocol {
                    message: format!(
                        "DO accepted WebSocket {accepted}, but pending request was {pending}"
                    ),
                });
            }
            self.state.pending_websockets.lock().await.insert(
                accepted,
                PendingWebSocket {
                    connection,
                    key: DoKey {
                        binding,
                        name: object,
                    },
                },
            );
        }
        Ok(response)
    }
}

/// Proxies the flattened DO fetch contract to a Worker in another microVM.
async fn remote_fetch(
    origin: &str,
    binding: &str,
    object: &str,
    event: FetchEvent,
    ingress_token: Option<&str>,
) -> Result<FetchResponse, GatewayError> {
    if event.ws.is_some() {
        return Err(GatewayError::RemoteWorker {
            message: "cross-microVM WebSocket bindings are not supported".to_owned(),
        });
    }
    let request_uri = event
        .url
        .parse::<Uri>()
        .map_err(|error| GatewayError::InvalidHttp {
            message: format!("remote binding request URI is invalid: {error}"),
        })?;
    let target = remote_target(origin, binding, object, &request_uri);
    let method = reqwest::Method::from_bytes(event.method.as_bytes()).map_err(|error| {
        GatewayError::InvalidHttp {
            message: format!("remote binding method is invalid: {error}"),
        }
    })?;
    let client = reqwest::Client::new();
    let mut request = client.request(method, target).body(event.body);
    if let Some(token) = ingress_token {
        request = request.header("x-verglas-worker-ingress", token);
    }
    for (name, value) in event.headers {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "host" | "connection" | "content-length" | "transfer-encoding"
        ) {
            continue;
        }
        request = request.header(&name, &value);
    }
    let response = request
        .send()
        .await
        .map_err(|error| GatewayError::RemoteWorker {
            message: error.to_string(),
        })?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            Ok((
                name.as_str().to_owned(),
                value
                    .to_str()
                    .map_err(|error| GatewayError::InvalidHttp {
                        message: format!("remote response header is invalid: {error}"),
                    })?
                    .to_owned(),
            ))
        })
        .collect::<Result<Vec<_>, GatewayError>>()?;
    let body = response
        .bytes()
        .await
        .map_err(|error| GatewayError::RemoteWorker {
            message: error.to_string(),
        })?
        .to_vec();
    Ok(FetchResponse {
        status,
        headers,
        body,
        accept_ws: None,
    })
}

/// Maps a binding fetch URL onto the internal route without adding a trailing
/// slash that axum intentionally treats as a different route.
fn remote_target(origin: &str, binding: &str, object: &str, request_uri: &Uri) -> String {
    let base = format!(
        "{origin}/do/{}/{}",
        percent_encode_segment(binding),
        percent_encode_segment(object),
    );
    let path = request_uri.path();
    let suffix = if path == "/" { "" } else { path };
    match request_uri.query() {
        Some(query) => format!("{base}{suffix}?{query}"),
        None => format!("{base}{suffix}"),
    }
}

fn percent_encode_segment(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

#[async_trait]
impl DoRouter for GatewayDoRouter {
    /// Resolves a Worker binding and forwards its flattened stub fetch.
    async fn do_fetch(
        &self,
        binding: String,
        object: String,
        request: WorkerRequest,
    ) -> Result<WorkerResponse, HostError> {
        let event = FetchEvent {
            method: request.method,
            url: request.uri,
            headers: request.headers,
            body: request.body,
            ws: request.ws,
        };
        self.route(binding, object, event)
            .await
            .map(worker_response)
            .map_err(|error| HostError::backend(error.to_string()))
    }
}

#[async_trait]
impl DoCallHandler for GatewayDoRouter {
    /// Services a DO-originated call without allowing a source self-deadlock.
    async fn call(
        &self,
        binding: String,
        object: String,
        event: FetchEvent,
    ) -> Result<FetchResponse, GatewayError> {
        self.route(binding, object, event).await
    }
}

/// Converts the gateway response record to the WorkerPool response record.
fn worker_response(response: FetchResponse) -> WorkerResponse {
    WorkerResponse {
        status: response.status,
        headers: response.headers,
        body: response.body,
        accept_ws: response.accept_ws,
    }
}

/// Loads the immutable Worker component named by the generated manifest.
fn load_worker_executor(manifest: &Manifest) -> Result<Arc<dyn WorkerExecutor>, GatewayError> {
    let artifact = manifest
        .artifact_for_product(crate::manifest::ArtifactProduct::Worker)
        .map_err(|error| GatewayError::WorkerUnavailable {
            message: error.to_string(),
        })?;
    let path = artifact
        .component_dir()
        .join(format!("{}.wasm", artifact.digest()));
    let bytes = std::fs::read(&path).map_err(|error| GatewayError::WorkerUnavailable {
        message: format!("read Worker component {}: {error}", path.display()),
    })?;
    let actual_digest = hex::encode(Sha256::digest(&bytes));
    if actual_digest != artifact.digest() {
        return Err(GatewayError::WorkerUnavailable {
            message: format!(
                "Worker component digest mismatch: expected {}, got {actual_digest}",
                artifact.digest()
            ),
        });
    }
    let digest = ComponentDigest::from_hex(artifact.digest()).map_err(|error| {
        GatewayError::WorkerUnavailable {
            message: format!("invalid Worker component digest: {error}"),
        }
    })?;
    let cache = artifact.cwasm_cache_dir().map(CwasmCache::new);
    let pool = WorkerPool::load_with_cache(
        wasmtime::Config::new(),
        cache.as_ref().map(|cache| (cache, digest)),
        &bytes,
    )
    .map_err(|error| GatewayError::WorkerUnavailable {
        message: format!("load Worker component {}: {error}", path.display()),
    })?;
    Ok(Arc::new(WorkerPoolExecutor::new(Arc::new(pool))))
}

/// One immutable cloud artifact assigned to a resumed regional reserve.
#[derive(Deserialize)]
struct WorkerAssignment {
    worker_name: String,
    digest: String,
    component_url: String,
    manifest_url: String,
    cwasm_url: String,
    cwasm_filename: String,
}

/// Timings returned to the control plane so deployment latency stays observable.
#[derive(Serialize)]
struct WorkerAssignmentResult {
    digest: String,
    download_ms: u128,
    activate_ms: u128,
}

const MAX_COMPONENT_BYTES: usize = 64 * 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;
const MAX_CWASM_GZIP_BYTES: usize = 128 * 1024 * 1024;
const MAX_CWASM_BYTES: u64 = 512 * 1024 * 1024;

/// Downloads one authenticated immutable artifact with a strict size ceiling.
async fn download_assignment_artifact(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    maximum: usize,
) -> Result<Vec<u8>, GatewayError> {
    let parsed = reqwest::Url::parse(url).map_err(|error| GatewayError::InvalidHttp {
        message: format!("invalid assignment artifact URL: {error}"),
    })?;
    if parsed.scheme() != "https" {
        return Err(GatewayError::InvalidHttp {
            message: "assignment artifact URLs must use HTTPS".to_owned(),
        });
    }
    let response = client
        .get(parsed)
        .header("x-verglas-worker-ingress", token)
        .send()
        .await
        .map_err(|error| GatewayError::WorkerUnavailable {
            message: format!("download assigned Worker artifact: {error}"),
        })?;
    if !response.status().is_success() {
        return Err(GatewayError::WorkerUnavailable {
            message: format!(
                "download assigned Worker artifact returned HTTP {}",
                response.status()
            ),
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(GatewayError::InvalidHttp {
            message: "assigned Worker artifact exceeds its size limit".to_owned(),
        });
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|error| GatewayError::WorkerUnavailable {
            message: format!("read assigned Worker artifact: {error}"),
        })?;
    if bytes.len() > maximum {
        return Err(GatewayError::InvalidHttp {
            message: "assigned Worker artifact exceeds its size limit".to_owned(),
        });
    }
    Ok(bytes.to_vec())
}

/// Writes an immutable artifact through a sibling temporary file.
async fn write_assignment_file(path: &Path, bytes: &[u8]) -> Result<(), GatewayError> {
    let parent = path
        .parent()
        .ok_or_else(|| GatewayError::WorkerUnavailable {
            message: format!("assigned artifact has no parent: {}", path.display()),
        })?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| GatewayError::control_io("create assignment directory", error))?;
    let partial = path.with_extension("partial");
    tokio::fs::write(&partial, bytes)
        .await
        .map_err(|error| GatewayError::control_io("write assigned artifact", error))?;
    tokio::fs::rename(&partial, path)
        .await
        .map_err(|error| GatewayError::control_io("publish assigned artifact", error))
}

/// Resumes a generic Machine without changing its Fly config, then atomically
/// swaps in the already-compiled Worker selected by the control plane.
async fn assign_worker_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(assignment): Json<WorkerAssignment>,
) -> Result<Json<WorkerAssignmentResult>, GatewayError> {
    require_ingress(&state, &headers)?;
    if assignment.worker_name.is_empty()
        || assignment.worker_name.len() > 128
        || !assignment
            .worker_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(GatewayError::InvalidHttp {
            message: "invalid assigned Worker name".to_owned(),
        });
    }
    if assignment.digest.len() != 64
        || !assignment
            .digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(GatewayError::InvalidHttp {
            message: "assigned Worker digest must be lowercase SHA-256".to_owned(),
        });
    }
    let expected_cwasm_prefix = format!("{}-", assignment.digest);
    let cwasm_fingerprint = assignment
        .cwasm_filename
        .strip_prefix(&expected_cwasm_prefix)
        .and_then(|value| value.strip_suffix(".cwasm"));
    if !cwasm_fingerprint.is_some_and(|value| {
        value.len() == 16
            && value
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    }) {
        return Err(GatewayError::InvalidHttp {
            message: "assigned native artifact filename does not match its digest".to_owned(),
        });
    }
    let token = state
        .ingress_token
        .as_deref()
        .ok_or_else(|| GatewayError::InvalidHttp {
            message: "in-place assignment requires an ingress token".to_owned(),
        })?;
    let download_started = Instant::now();
    let client = reqwest::Client::new();
    let (component, manifest_source, cwasm_gzip) = tokio::try_join!(
        download_assignment_artifact(
            &client,
            &assignment.component_url,
            token,
            MAX_COMPONENT_BYTES,
        ),
        download_assignment_artifact(&client, &assignment.manifest_url, token, MAX_MANIFEST_BYTES,),
        download_assignment_artifact(&client, &assignment.cwasm_url, token, MAX_CWASM_GZIP_BYTES,),
    )?;
    let download_ms = download_started.elapsed().as_millis();
    let actual_digest = hex::encode(Sha256::digest(&component));
    if actual_digest != assignment.digest {
        return Err(GatewayError::WorkerUnavailable {
            message: format!(
                "assigned Worker component digest mismatch: expected {}, got {actual_digest}",
                assignment.digest
            ),
        });
    }
    let mut cwasm = Vec::new();
    GzDecoder::new(cwasm_gzip.as_slice())
        .take(MAX_CWASM_BYTES + 1)
        .read_to_end(&mut cwasm)
        .map_err(|error| GatewayError::WorkerUnavailable {
            message: format!("decompress assigned native artifact: {error}"),
        })?;
    if cwasm.is_empty() || cwasm.len() as u64 > MAX_CWASM_BYTES {
        return Err(GatewayError::WorkerUnavailable {
            message: "assigned native artifact is empty or exceeds its size limit".to_owned(),
        });
    }

    let activate_started = Instant::now();
    let artifact_root = state.data_root.join("worker-artifact");
    let component_root = artifact_root.join("components");
    let cwasm_root = artifact_root.join("cwasm");
    let component_path = component_root.join(format!("{}.wasm", assignment.digest));
    let cwasm_path = cwasm_root.join(&assignment.cwasm_filename);
    tokio::try_join!(
        write_assignment_file(&component_path, &component),
        write_assignment_file(&cwasm_path, &cwasm),
    )?;

    let mut manifest_text =
        String::from_utf8(manifest_source).map_err(|error| GatewayError::InvalidHttp {
            message: format!("assigned Worker manifest is not UTF-8: {error}"),
        })?;
    for product in ["COUNTER", "STREAM", "PIPELINE", "SINK", "CATALOG"] {
        let replacement = std::env::var(format!("VERGLAS_{product}_APP")).map_err(|_| {
            GatewayError::WorkerUnavailable {
                message: format!("VERGLAS_{product}_APP is not configured"),
            }
        })?;
        manifest_text = manifest_text.replace(&format!("__{product}_APP__"), &replacement);
    }
    let mut manifest_json: serde_json::Value =
        serde_json::from_str(&manifest_text).map_err(|error| GatewayError::InvalidHttp {
            message: format!("assigned Worker manifest is invalid JSON: {error}"),
        })?;
    let root = manifest_json
        .as_object_mut()
        .ok_or_else(|| GatewayError::InvalidHttp {
            message: "assigned Worker manifest root must be an object".to_owned(),
        })?;
    let artifacts = root
        .get_mut("artifacts")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| GatewayError::InvalidHttp {
            message: "assigned Worker manifest requires artifacts".to_owned(),
        })?;
    let descriptor = serde_json::json!({
        "digest": assignment.digest,
        "component_dir": component_root,
        "cwasm_cache_dir": cwasm_root,
    });
    artifacts.insert("worker".to_owned(), descriptor.clone());
    if artifacts.contains_key("durable_object") {
        artifacts.insert("durable_object".to_owned(), descriptor);
    }
    root.insert(
        "data_root".to_owned(),
        serde_json::Value::String(state.data_root.to_string_lossy().into_owned()),
    );
    let manifest_text =
        serde_json::to_string(&manifest_json).map_err(|error| GatewayError::InvalidHttp {
            message: format!("encode assigned Worker manifest: {error}"),
        })?;
    let manifest = Manifest::parse(&manifest_text).map_err(|error| GatewayError::InvalidHttp {
        message: format!("assigned Worker manifest is invalid: {error}"),
    })?;
    let worker = load_worker_executor(&manifest)?;
    {
        let mut active = state.deployment.write().await;
        active.manifest = manifest;
        active.worker = worker;
    }
    state.connections.lock().await.clear();
    if let Some(activity) = &state.activity {
        *activity.worker.write().await = assignment.worker_name;
        let _ = activity.events.try_send(ActivityEvent::Touch);
    }
    Ok(Json(WorkerAssignmentResult {
        digest: assignment.digest,
        download_ms,
        activate_ms: activate_started.elapsed().as_millis(),
    }))
}

/// Handles a public request by running the Worker tier before any DO.
async fn public_handler(
    State(state): State<Arc<GatewayState>>,
    request: Request,
) -> Result<Response, GatewayError> {
    require_ingress(&state, request.headers())?;
    let _activity = begin_activity(&state);
    let (mut parts, body) = request.into_parts();
    let ws_upgrade = if is_websocket_upgrade(&parts.headers) {
        Some(
            WebSocketUpgrade::from_request_parts(&mut parts, &())
                .await
                .map_err(|error| GatewayError::InvalidHttp {
                    message: format!("invalid WebSocket upgrade: {error:?}"),
                })?,
        )
    } else {
        None
    };
    let body = to_bytes(body, usize::MAX)
        .await
        .map_err(|error| GatewayError::InvalidHttp {
            message: format!("request body could not be read: {error}"),
        })?;
    let pending_ws = ws_upgrade
        .as_ref()
        .map(|_| Gateway::next_websocket(&state))
        .transpose()?;
    let event = FetchEvent {
        method: parts.method.to_string(),
        url: request_url(&parts.uri),
        headers: request_headers(&parts.headers)?,
        body: body.to_vec(),
        ws: pending_ws,
    };
    let router = Arc::new(GatewayDoRouter {
        state: Arc::clone(&state),
        source: None,
    });
    let worker = Arc::clone(&state.deployment.read().await.worker);
    let response = match worker.fetch(worker_request(&event), router).await {
        Ok(response) => response,
        Err(error) => {
            if let Some(ws) = pending_ws {
                state.pending_websockets.lock().await.remove(&ws);
            }
            return Err(error);
        }
    };
    let response = FetchResponse {
        status: response.status,
        headers: response.headers,
        body: response.body,
        accept_ws: response.accept_ws,
    };
    if let Some(pending) = pending_ws {
        match response.accept_ws {
            None => {
                state.pending_websockets.lock().await.remove(&pending);
                return http_response(response.status, response.headers, response.body);
            }
            Some(accepted) if accepted == pending => {}
            Some(accepted) => {
                state.pending_websockets.lock().await.remove(&pending);
                return Err(GatewayError::Protocol {
                    message: format!(
                        "Worker accepted WebSocket {accepted}, but pending request was {pending}"
                    ),
                });
            }
        }
        let registered = state
            .pending_websockets
            .lock()
            .await
            .remove(&pending)
            .ok_or_else(|| GatewayError::Protocol {
                message: format!("Worker accepted WebSocket {pending} without a DO connection"),
            })?;
        let upgrade = ws_upgrade.ok_or_else(|| GatewayError::Protocol {
            message: "Worker accepted a WebSocket without an upgrade request".to_owned(),
        })?;
        let connection = registered.connection;
        let effects = connection.open_websocket_with_id(pending).await?;
        return Ok(upgrade.on_upgrade(move |socket| {
            websocket_session_registered(socket, connection, pending, effects)
        }));
    }
    if response.accept_ws.is_some() {
        return Err(GatewayError::Protocol {
            message: "Worker returned accept_ws without a pending WebSocket".to_owned(),
        });
    }
    http_response(response.status, response.headers, response.body)
}

/// Handles the internal route without a trailing path component.
async fn http_root_handler(
    State(state): State<Arc<GatewayState>>,
    RoutePath((binding, name)): RoutePath<(String, String)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, GatewayError> {
    forward_http(state, binding, name, method, uri, headers, body).await
}

/// Handles the internal route with a wildcard path component.
async fn http_path_handler(
    State(state): State<Arc<GatewayState>>,
    RoutePath((binding, name, _path)): RoutePath<(String, String, String)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, GatewayError> {
    forward_http(state, binding, name, method, uri, headers, body).await
}

/// Converts an internal route request into a fetch frame and maps its response.
async fn forward_http(
    state: Arc<GatewayState>,
    binding: String,
    name: String,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, GatewayError> {
    require_ingress(&state, &headers)?;
    let _activity = begin_activity(&state);
    let request_headers = request_headers(&headers)?;
    let prefix = format!("/do/{binding}/{name}");
    let suffix = uri
        .path()
        .strip_prefix(&prefix)
        .ok_or_else(|| GatewayError::InvalidHttp {
            message: format!("request path does not match route prefix {prefix}"),
        })?;
    let path = if suffix.is_empty() { "/" } else { suffix };
    let url = match uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    };
    let event = FetchEvent {
        method: method.to_string(),
        url,
        headers: request_headers,
        body: body.to_vec(),
        ws: None,
    };
    let connection = Gateway::connection_for(&state, &binding, &name).await?;
    let response = connection.fetch(event).await?;
    http_response(response.status, response.headers, response.body)
}

/// Performs the internal/debug WebSocket upgrade with automatic acceptance.
async fn websocket_handler(
    State(state): State<Arc<GatewayState>>,
    RoutePath((binding, name)): RoutePath<(String, String)>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, GatewayError> {
    let connection = Gateway::connection_for(&state, &binding, &name).await?;
    Ok(upgrade.on_upgrade(move |socket| websocket_session(socket, connection)))
}

/// Opens a debug WebSocket identity after the internal route has upgraded.
async fn websocket_session(socket: axum::extract::ws::WebSocket, connection: Arc<DoConnection>) {
    let (ws, effects) = match connection.open_websocket().await {
        Ok(value) => value,
        Err(_) => return,
    };
    websocket_session_registered(socket, connection, ws, effects).await;
}

/// Relays one registered WebSocket and keeps Worker errors nonfatal to the session.
async fn websocket_session_registered(
    mut socket: axum::extract::ws::WebSocket,
    connection: Arc<DoConnection>,
    ws: u64,
    mut effects: tokio::sync::mpsc::UnboundedReceiver<WsOutbound>,
) {
    let mut close_event = (1000_u16, String::new());
    let mut worker_closed = false;
    loop {
        tokio::select! {
            effect = effects.recv() => {
                let Some(effect) = effect else { break; };
                let closes = matches!(&effect, WsOutbound::Close { .. });
                let result = match effect {
                    WsOutbound::Message { text, data } => {
                        let message = if text {
                            match String::from_utf8(data) {
                                Ok(value) => axum::extract::ws::Message::Text(value.into()),
                                Err(_) => break,
                            }
                        } else {
                            axum::extract::ws::Message::Binary(data.into())
                        };
                        socket.send(message).await
                    }
                    WsOutbound::Close { code, reason } => {
                        worker_closed = true;
                        let frame = axum::extract::ws::CloseFrame {
                            code,
                            reason: reason.into(),
                        };
                        socket.send(axum::extract::ws::Message::Close(Some(frame))).await
                    }
                };
                if result.is_err() || closes {
                    break;
                }
            }
            incoming = socket.next() => {
                let Some(incoming) = incoming else { break; };
                let Ok(message) = incoming else { break; };
                match message {
                    axum::extract::ws::Message::Text(text) => {
                        let _ = connection.websocket_message(ws, true, text.as_bytes().to_vec()).await;
                    }
                    axum::extract::ws::Message::Binary(data) => {
                        let _ = connection.websocket_message(ws, false, data.to_vec()).await;
                    }
                    axum::extract::ws::Message::Ping(data) => {
                        if socket.send(axum::extract::ws::Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    axum::extract::ws::Message::Pong(_) => {}
                    axum::extract::ws::Message::Close(frame) => {
                        close_event = frame.as_ref().map_or((1000, String::new()), |frame| {
                            (frame.code, frame.reason.to_string())
                        });
                        let _ = socket.send(axum::extract::ws::Message::Close(frame)).await;
                        break;
                    }
                }
            }
        }
    }
    if !worker_closed {
        let _ = connection
            .websocket_close(ws, close_event.0, close_event.1)
            .await;
    }
    connection.remove_websocket(ws).await;
}

/// Detects the WebSocket handshake shape before asking axum to validate it.
fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    headers
        .get("upgrade")
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
        && headers.get("connection").is_some_and(|value| {
            value.to_str().ok().is_some_and(|value| {
                value
                    .split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case("upgrade"))
            })
        })
}

/// Converts one HTTP request into the component Worker request record.
fn worker_request(event: &FetchEvent) -> WorkerRequest {
    WorkerRequest {
        method: event.method.clone(),
        uri: event.url.clone(),
        headers: event.headers.clone(),
        body: event.body.clone(),
        ws: event.ws,
    }
}

/// Returns the path and query string expected by the WIT request record.
fn request_url(uri: &Uri) -> String {
    uri.path_and_query()
        .map_or_else(|| uri.path().to_owned(), ToString::to_string)
}

/// Converts every request header to the protocol's ordered string representation.
fn request_headers(headers: &HeaderMap) -> Result<Vec<(String, String)>, GatewayError> {
    headers
        .iter()
        .map(|(name, value)| {
            let value = value.to_str().map_err(|error| GatewayError::InvalidHttp {
                message: format!("request header {} is not visible UTF-8: {error}", name),
            })?;
            Ok((name.to_string(), value.to_owned()))
        })
        .collect()
}

/// Builds an HTTP response from a validated fetch-result frame.
fn http_response(
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> Result<Response, GatewayError> {
    let status = StatusCode::from_u16(status).map_err(|error| GatewayError::InvalidHttp {
        message: format!("Worker returned invalid status {status}: {error}"),
    })?;
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    for (name, value) in headers {
        let name =
            HeaderName::from_bytes(name.as_bytes()).map_err(|error| GatewayError::InvalidHttp {
                message: format!("Worker returned invalid response header name: {error}"),
            })?;
        let value = HeaderValue::from_str(&value).map_err(|error| GatewayError::InvalidHttp {
            message: format!("Worker returned invalid response header value: {error}"),
        })?;
        response.headers_mut().append(name, value);
    }
    Ok(response)
}

/// Builds a verglasd-safe identity from a binding and URL object name.
fn do_identity(binding: &str, name: &str) -> Result<String, GatewayError> {
    let identity = format!("{binding}--{name}");
    if identity.is_empty()
        || !identity
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || identity == "."
        || identity == ".."
    {
        return Err(GatewayError::InvalidIdentity { identity });
    }
    Ok(identity)
}

#[cfg(test)]
mod activity_tests {
    use super::*;

    #[test]
    fn activity_leases_track_overlapping_requests_until_the_last_drop() {
        let (events, _receiver) = tokio::sync::mpsc::channel(1);
        let active = Arc::new(AtomicUsize::new(0));
        let reporter = ActivityReporter {
            events,
            active: Arc::clone(&active),
            worker: Arc::new(RwLock::new("worker".to_owned())),
        };

        let first = reporter.begin();
        let second = reporter.begin();
        assert_eq!(active.load(Ordering::Acquire), 2);
        drop(first);
        assert_eq!(active.load(Ordering::Acquire), 1);
        drop(second);
        assert_eq!(active.load(Ordering::Acquire), 0);
    }

    #[test]
    fn remote_root_and_path_urls_match_axum_routes() {
        let root = "/".parse::<Uri>().expect("root URI");
        let root_query = "/?view=current".parse::<Uri>().expect("root query URI");
        let path = "/incr?step=2".parse::<Uri>().expect("path URI");
        assert_eq!(
            remote_target("http://counter.flycast", "COUNTER", "global", &root),
            "http://counter.flycast/do/COUNTER/global",
        );
        assert_eq!(
            remote_target("http://counter.flycast", "COUNTER", "global", &root_query),
            "http://counter.flycast/do/COUNTER/global?view=current",
        );
        assert_eq!(
            remote_target("http://counter.flycast", "COUNTER", "global", &path),
            "http://counter.flycast/do/COUNTER/global/incr?step=2",
        );
    }

    #[test]
    fn resource_pressure_accepts_external_scheduler_delay_but_not_io_wait() {
        assert!(resource_pressure_detected(
            1_600_000,
            Some((0, 100_000)),
            Some((0, 0)),
        ));
        assert!(!resource_pressure_detected(
            1_600_000,
            Some((0, 20_000)),
            Some((0, 0)),
        ));
        assert!(!resource_pressure_detected(
            200_000,
            Some((0, 180_000)),
            Some((0, 0)),
        ));
        assert!(resource_pressure_detected(
            10_000_000,
            Some((0, 9_000_000)),
            Some((0, 0)),
        ));
    }
}
