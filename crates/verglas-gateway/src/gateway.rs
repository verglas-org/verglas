//! Axum HTTP and WebSocket routing for resident Durable Objects.
//!
//! The gateway owns client connections and creates exactly one event actor for
//! each `(binding, name)` key. It intentionally never suspends, restarts, or
//! replaces a resident object during the process lifetime.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path as RoutePath, State, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::response::Response;
use axum::routing::{any, get};
use axum::{Router, serve};
use bytes::Bytes;
use futures::StreamExt;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::connection::{DoConnection, FetchEvent};
use crate::error::GatewayError;
use crate::manifest::{Binding, Manifest};
use crate::protocol::WsOutbound;
use crate::spawn::{CelldSpawner, DoSpawner, SpawnRequest};

/// A resident OSS Durable Object HTTP/WebSocket gateway.
#[derive(Clone)]
pub struct Gateway {
    state: Arc<GatewayState>,
}

/// Shared routing state held by every axum request handler.
struct GatewayState {
    manifest: Manifest,
    data_root: PathBuf,
    spawner: Arc<dyn DoSpawner>,
    connections: Mutex<HashMap<DoKey, Arc<DoConnection>>>,
}

/// The manifest binding and object name that identify one resident actor.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DoKey {
    binding: String,
    name: String,
}

impl Gateway {
    /// Creates a gateway using the local celld control socket as its spawner.
    pub fn new(
        manifest: &Manifest,
        control_socket: impl Into<PathBuf>,
        data_root: impl Into<PathBuf>,
    ) -> Self {
        let spawner = Arc::new(CelldSpawner::new(control_socket.into()));
        Self::with_spawner(manifest, data_root, spawner)
    }

    /// Creates a gateway with an injected spawner for tests or another substrate.
    pub fn with_spawner(
        manifest: &Manifest,
        data_root: impl Into<PathBuf>,
        spawner: Arc<dyn DoSpawner>,
    ) -> Self {
        Self {
            state: Arc::new(GatewayState {
                manifest: manifest.clone(),
                data_root: data_root.into(),
                spawner,
                connections: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Returns the axum router exposing HTTP and WebSocket DO routes.
    pub fn router(&self) -> Router {
        Router::new()
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

    /// Finds or creates the one resident event actor for a route key.
    async fn connection_for(
        state: &Arc<GatewayState>,
        binding: &str,
        name: &str,
    ) -> Result<Arc<DoConnection>, GatewayError> {
        if state.manifest.binding(binding).is_none() {
            return Err(GatewayError::UnknownBinding {
                binding: binding.to_owned(),
            });
        }
        let do_id = do_identity(binding, name)?;
        let key = DoKey {
            binding: binding.to_owned(),
            name: name.to_owned(),
        };
        let mut connections = state.connections.lock().await;
        if let Some(connection) = connections.get(&key) {
            return Ok(Arc::clone(connection));
        }
        let request = SpawnRequest::new(
            do_id,
            binding.to_owned(),
            name.to_owned(),
            state.manifest.component_digest().to_owned(),
            state.manifest.component_dir().to_path_buf(),
            state.data_root.clone(),
        );
        let request = match state.manifest.managed_cas() {
            Some(cas) => request.with_managed_cas(cas.clone()),
            None => request,
        };
        let event_socket = state.spawner.spawn(request).await?;
        let connection = Arc::new(DoConnection::connect(event_socket).await?);
        connections.insert(key, Arc::clone(&connection));
        Ok(connection)
    }
}

/// Handles the route without a trailing path component.
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

/// Handles the route with a wildcard path component.
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

/// Converts one axum request into a fetch frame and maps its terminal response.
async fn forward_http(
    state: Arc<GatewayState>,
    binding: String,
    name: String,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, GatewayError> {
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
    };
    let connection = Gateway::connection_for(&state, &binding, &name).await?;
    let response = connection.fetch(event).await?;
    http_response(response.status, response.headers, response.body)
}

/// Performs the gateway-owned WebSocket upgrade after spawning the DO.
async fn websocket_handler(
    State(state): State<Arc<GatewayState>>,
    RoutePath((binding, name)): RoutePath<(String, String)>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, GatewayError> {
    let connection = Gateway::connection_for(&state, &binding, &name).await?;
    Ok(upgrade.on_upgrade(move |socket| websocket_session(socket, connection)))
}

/// Relays one accepted WebSocket and keeps Worker errors nonfatal to the session.
async fn websocket_session(
    mut socket: axum::extract::ws::WebSocket,
    connection: Arc<DoConnection>,
) {
    let (ws, mut effects) = match connection.open_websocket().await {
        Ok(value) => value,
        Err(_) => return,
    };
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
                        if connection
                            .websocket_message(ws, true, text.as_bytes().to_vec())
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    axum::extract::ws::Message::Binary(data) => {
                        if connection
                            .websocket_message(ws, false, data.to_vec())
                            .await
                            .is_err()
                        {
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
