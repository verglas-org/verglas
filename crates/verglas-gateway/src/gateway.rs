//! Axum HTTP and WebSocket routing for Workers and resident Durable Objects.
//!
//! Public routes execute the stateless Worker tier first. Durable Objects are
//! resolved only through manifest bindings, while `/do/...` remains an explicit
//! internal/debug route for inspecting a resident object's event socket.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::extract::{FromRequestParts, Path as RoutePath, Request, State, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::response::Response;
use axum::routing::{any, get};
use axum::{Router, serve};
use bytes::Bytes;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use verglas_do_wasm::{
    DoRouter, HostError, Request as WorkerRequest, Response as WorkerResponse, WorkerPool,
};

use crate::connection::{DoCallHandler, DoConnection, FetchEvent};
use crate::error::GatewayError;
use crate::manifest::{ArtifactProduct, Binding, Manifest, PipelineBinding, SystemBinding};
use crate::protocol::{FetchResponse, WsOutbound};
use crate::spawn::{CelldSpawner, DoSpawner, SpawnRequest};

/// Executes one stateless Worker fetch with a gateway-owned DO router.
#[async_trait]
pub trait WorkerExecutor: Send + Sync {
    /// Runs the Worker fetch without granting it Durable Object storage.
    async fn fetch(
        &self,
        request: WorkerRequest,
        router: Arc<dyn DoRouter>,
    ) -> Result<WorkerResponse, GatewayError>;
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
}

/// A production gateway whose public ingress is Worker-first.
#[derive(Clone)]
pub struct Gateway {
    state: Arc<GatewayState>,
}

/// Shared routing state held by every axum request handler.
struct GatewayState {
    manifest: Manifest,
    data_root: PathBuf,
    spawner: Arc<dyn DoSpawner>,
    worker: Arc<dyn WorkerExecutor>,
    connections: Mutex<HashMap<DoKey, Arc<DoConnection>>>,
    pending_websockets: Mutex<HashMap<u64, Arc<DoConnection>>>,
    next_websocket: AtomicU64,
    activity: Option<ActivityReporter>,
    ingress_token: Option<String>,
}

#[derive(Clone)]
struct ActivityReporter {
    events: tokio::sync::mpsc::Sender<()>,
    active: Arc<AtomicUsize>,
}

impl ActivityReporter {
    fn begin(&self) -> ActivityLease {
        self.active.fetch_add(1, Ordering::AcqRel);
        let _ = self.events.try_send(());
        ActivityLease {
            reporter: Some(self.clone()),
        }
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
        let _ = reporter.events.try_send(());
    }
}

/// The manifest binding and object name that identify one resident actor.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DoKey {
    binding: String,
    name: String,
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
}

impl Gateway {
    /// Creates a gateway using celld and loads the Worker component from the manifest.
    pub fn new(
        manifest: &Manifest,
        control_socket: impl Into<PathBuf>,
        data_root: impl Into<PathBuf>,
    ) -> Self {
        let spawner = Arc::new(CelldSpawner::new(control_socket.into()));
        let worker = match load_worker_executor(manifest) {
            Ok(worker) => worker,
            Err(error) => Arc::new(FailedWorker { error }),
        };
        Self::with_parts(manifest, data_root, spawner, worker)
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
        )
    }

    /// Creates a gateway with an injected Worker executor for fake-driven tests.
    pub fn with_worker_executor(
        manifest: &Manifest,
        data_root: impl Into<PathBuf>,
        spawner: Arc<dyn DoSpawner>,
        worker: Arc<dyn WorkerExecutor>,
    ) -> Self {
        Self::with_parts(manifest, data_root, spawner, worker)
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
    ) -> Self {
        Self {
            state: Arc::new(GatewayState {
                manifest: manifest.clone(),
                data_root: data_root.into(),
                spawner,
                worker,
                connections: Mutex::new(HashMap::new()),
                pending_websockets: Mutex::new(HashMap::new()),
                next_websocket: AtomicU64::new(1),
                activity: activity_reporter_from_env(),
                ingress_token: std::env::var("VERGLAS_INGRESS_TOKEN").ok(),
            }),
        }
    }

    /// Returns the axum router with public Worker routes before internal DO routes.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/", any(public_handler))
            .route("/{*path}", any(public_handler))
            .route("/do/{binding}/{name}/ws", get(websocket_handler))
            .route("/do/{binding}/{name}", any(http_root_handler))
            .route("/do/{binding}/{name}/{*path}", any(http_path_handler))
            .with_state(Arc::clone(&self.state))
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
        let product =
            state
                .manifest
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
        let artifact = state
            .manifest
            .artifact_for_product(product)
            .map_err(|error| GatewayError::SpawnRejected {
                message: error.to_string(),
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
            && let Some(host_service) = state.manifest.host_services().first().cloned()
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

fn activity_reporter_from_env() -> Option<ActivityReporter> {
    let url = std::env::var("VERGLAS_ACTIVITY_URL").ok()?;
    let token = std::env::var("VERGLAS_ACTIVITY_TOKEN").ok()?;
    let tenant = std::env::var("VERGLAS_TENANT_ID").ok()?;
    let worker = std::env::var("VERGLAS_WORKER_NAME").ok()?;
    let (events, mut receiver) = tokio::sync::mpsc::channel(1);
    let active = Arc::new(AtomicUsize::new(0));
    let active_requests = Arc::clone(&active);
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut heartbeat = tokio::time::interval(Duration::from_secs(2));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                event = receiver.recv() => {
                    if event.is_none() {
                        break;
                    }
                }
                _ = heartbeat.tick(), if active_requests.load(Ordering::Acquire) > 0 => {}
            }
            let _ = client
                .post(&url)
                .header("x-verglas-cloud-internal", &token)
                .json(&serde_json::json!({
                    "tenant_id": tenant,
                    "worker_name": worker,
                }))
                .send()
                .await;
        }
    });
    Some(ActivityReporter { events, active })
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
        if let Some(origin) = self
            .state
            .manifest
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
            self.state
                .pending_websockets
                .lock()
                .await
                .insert(accepted, connection);
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
    let suffix = request_uri
        .path_and_query()
        .map_or("/", |value| value.as_str());
    let target = format!(
        "{origin}/do/{}/{}{suffix}",
        percent_encode_segment(binding),
        percent_encode_segment(object),
    );
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
    let pool = WorkerPool::load(wasmtime::Config::new(), &bytes).map_err(|error| {
        GatewayError::WorkerUnavailable {
            message: format!("load Worker component {}: {error}", path.display()),
        }
    })?;
    Ok(Arc::new(WorkerPoolExecutor::new(Arc::new(pool))))
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
    let response = match state.worker.fetch(worker_request(&event), router).await {
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
        let connection = state
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
                        if connection.websocket_message(ws, true, text.as_bytes().to_vec()).await.is_err() {
                            break;
                        }
                    }
                    axum::extract::ws::Message::Binary(data) => {
                        if connection.websocket_message(ws, false, data.to_vec()).await.is_err() {
                            break;
                        }
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

/// Builds a celld-safe identity from a binding and URL object name.
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
        };

        let first = reporter.begin();
        let second = reporter.begin();
        assert_eq!(active.load(Ordering::Acquire), 2);
        drop(first);
        assert_eq!(active.load(Ordering::Acquire), 1);
        drop(second);
        assert_eq!(active.load(Ordering::Acquire), 0);
    }
}
