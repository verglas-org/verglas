//! NDJSON event endpoint for one resident Durable Object Worker.
//!
//! This module owns frame decoding, transaction-scoped Worker dispatch, commit-gated
//! socket output, and advisory alarm delivery. Durable state is supplied by one
//! Turso store; this socket carries only the gateway event protocol.
//! Cross-object `do-call` subrequests are emitted immediately and are deliberately
//! not output-gated: they are irreversible side effects like Cloudflare subrequests,
//! while only WebSocket sends and closes wait for the event commit.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use verglas_do_turso::TursoStore;
use verglas_do_wasm::{
    EventGate, HostError, PendingEvent, Request, Response, RuntimeError, SocketId, WorkerBindings,
    WorkerRuntime, WorkerSockets, WorkerStorage,
};

use crate::worker_storage::{BindingStreamAppender, TursoWorkerStorage};

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Errors that prevent the event endpoint from serving its socket.
#[derive(Debug, Error)]
pub enum EventEndpointError {
    /// Reports Unix socket or stream I/O failure.
    #[error("event endpoint I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Reports a durable Turso failure while starting or delivering an event.
    #[error("event endpoint Turso failure: {0}")]
    Store(#[source] verglas_do_turso::Error),
    /// Reports a Worker component invocation failure.
    #[error("event endpoint Worker failure: {0}")]
    Runtime(#[source] RuntimeError),
    /// Reports a host capability failure while releasing committed effects.
    #[error("event endpoint host failure: {0}")]
    Host(#[source] HostError),
    /// Reports malformed NDJSON that cannot be associated with an event id.
    #[error("event endpoint protocol failure: {0}")]
    Protocol(String),
}

/// Dispatch seam used by the endpoint so protocol tests can use a scripted handler.
#[async_trait]
pub trait EventDispatcher: Send + Sync {
    /// Runs component initialization once before the first gateway event.
    async fn dispatch_init(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
    ) -> Result<PendingEvent<()>, RuntimeError>;

    /// Dispatches one HTTP fetch request.
    async fn dispatch_fetch(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
        request: Request,
    ) -> Result<PendingEvent<Response>, RuntimeError>;

    /// Dispatches one durable alarm event.
    async fn dispatch_alarm(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
        scheduled_millis: u64,
    ) -> Result<PendingEvent<()>, RuntimeError>;

    /// Dispatches one WebSocket message event.
    async fn dispatch_websocket_message(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
        socket: SocketId,
        message: Vec<u8>,
    ) -> Result<PendingEvent<()>, RuntimeError>;

    /// Dispatches one WebSocket close event.
    #[allow(clippy::too_many_arguments)]
    async fn dispatch_websocket_close(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
        socket: SocketId,
        code: u16,
        reason: String,
    ) -> Result<PendingEvent<()>, RuntimeError>;
}

#[async_trait]
impl EventDispatcher for WorkerRuntime {
    /// Runs the real Worker component initialization export.
    async fn dispatch_init(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        WorkerRuntime::dispatch_init(self, gate, storage, sockets, bindings).await
    }

    /// Runs the real Worker component fetch export.
    async fn dispatch_fetch(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
        request: Request,
    ) -> Result<PendingEvent<Response>, RuntimeError> {
        WorkerRuntime::dispatch_fetch(self, gate, storage, sockets, bindings, request).await
    }

    /// Runs the real Worker component alarm export.
    async fn dispatch_alarm(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
        scheduled_millis: u64,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        WorkerRuntime::dispatch_alarm(self, gate, storage, sockets, bindings, scheduled_millis)
            .await
    }

    /// Runs the real Worker component WebSocket message export.
    async fn dispatch_websocket_message(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
        socket: SocketId,
        message: Vec<u8>,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        WorkerRuntime::dispatch_websocket_message(
            self, gate, storage, sockets, bindings, socket, message,
        )
        .await
    }

    /// Runs the real Worker component WebSocket close export.
    async fn dispatch_websocket_close(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
        socket: SocketId,
        code: u16,
        reason: String,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        WorkerRuntime::dispatch_websocket_close(
            self, gate, storage, sockets, bindings, socket, code, reason,
        )
        .await
    }
}

/// One typed gateway failure returned for a cross-object call.
#[derive(Debug, Deserialize)]
struct DoCallErrorFrame {
    /// Stable machine-readable gateway failure code.
    code: String,
    /// Human-readable gateway failure description.
    message: String,
}

/// One gateway-to-Worker NDJSON input frame.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum InboundFrame {
    /// Delivers one HTTP request.
    #[serde(rename = "fetch")]
    Fetch {
        /// Gateway event identity.
        id: u64,
        /// HTTP method.
        method: String,
        /// Request URL path.
        url: String,
        /// Ordered request headers.
        headers: Vec<(String, String)>,
        /// Base64 request body.
        body_b64: String,
        /// Pending WebSocket identity supplied with an upgrade request.
        #[serde(default)]
        ws: Option<SocketId>,
    },
    /// Answers one cross-Durable-Object call emitted during a Worker event.
    #[serde(rename = "do-call-result")]
    DoCallResult {
        /// Call identity echoed from the outbound request.
        id: u64,
        /// Successful response status, omitted on an error.
        status: Option<u16>,
        /// Successful ordered response headers, omitted on an error.
        headers: Option<Vec<(String, String)>>,
        /// Successful base64 response body, omitted on an error.
        body_b64: Option<String>,
        /// Accepted pending WebSocket identity, when present.
        accept_ws: Option<SocketId>,
        /// Typed gateway failure, omitted on success.
        error: Option<DoCallErrorFrame>,
    },
    /// Registers a gateway-accepted WebSocket.
    #[serde(rename = "ws-open")]
    WsOpen {
        /// Gateway WebSocket identity.
        ws: SocketId,
    },
    /// Delivers one WebSocket message.
    #[serde(rename = "ws-message")]
    WsMessage {
        /// Gateway event identity.
        id: u64,
        /// Gateway WebSocket identity.
        ws: SocketId,
        /// Whether the incoming payload was text.
        text: bool,
        /// Base64 message body.
        data_b64: String,
    },
    /// Delivers one WebSocket close.
    #[serde(rename = "ws-close")]
    WsClose {
        /// Gateway event identity.
        id: u64,
        /// Gateway WebSocket identity.
        ws: SocketId,
        /// WebSocket close code.
        code: u16,
        /// WebSocket close reason.
        reason: String,
    },
}

/// One Worker-to-gateway NDJSON output frame.
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum OutboundFrame {
    /// Releases one staged WebSocket message.
    #[serde(rename = "ws-send")]
    WsSend {
        /// Target WebSocket.
        ws: SocketId,
        /// Whether the payload is text.
        text: bool,
        /// Base64 message body.
        data_b64: String,
    },
    /// Releases one staged WebSocket close.
    #[serde(rename = "ws-close-out")]
    WsCloseOut {
        /// Target WebSocket.
        ws: SocketId,
        /// Close code.
        code: u16,
        /// Close reason.
        reason: String,
    },
    /// Requests one cross-Durable-Object fetch from the gateway immediately.
    /// This frame is not held by the event output gate because subrequests are
    /// irreversible side effects; only socket output remains commit-gated.
    #[serde(rename = "do-call")]
    DoCall {
        /// Call identity echoed in the result frame.
        id: u64,
        /// Manifest binding to resolve.
        binding: String,
        /// Named object within the binding.
        object: String,
        /// HTTP method for the target fetch.
        method: String,
        /// Request URL path and query string.
        url: String,
        /// Ordered request headers.
        headers: Vec<(String, String)>,
        /// Base64 request body.
        body_b64: String,
        /// Pending WebSocket identity propagated through the call.
        #[serde(skip_serializing_if = "Option::is_none")]
        ws: Option<SocketId>,
    },
    /// Returns one fetch result.
    #[serde(rename = "fetch-result")]
    FetchResult {
        /// Gateway event identity.
        id: u64,
        /// HTTP status.
        status: u16,
        /// Ordered response headers.
        headers: Vec<(String, String)>,
        /// Base64 response body.
        body_b64: String,
        /// Pending WebSocket identity accepted by the guest, when present.
        #[serde(skip_serializing_if = "Option::is_none")]
        accept_ws: Option<SocketId>,
    },
    /// Terminates a WebSocket message or close event.
    #[serde(rename = "done")]
    Done {
        /// Gateway event identity.
        id: u64,
    },
    /// Reports one failed event.
    #[serde(rename = "error")]
    Error {
        /// Gateway event identity.
        id: u64,
        /// Stable failure message.
        message: String,
    },
}

/// Maximum number of cross-object calls one resident event socket may await.
const MAX_IN_FLIGHT_DO_CALLS: usize = 16;

/// Pending cross-object calls and their hard concurrency budget.
struct PendingDoCalls {
    /// Monotonic identity assigned to each outbound call.
    next_id: AtomicU64,
    /// Hard ceiling on calls awaiting gateway responses.
    permits: Arc<Semaphore>,
    /// Result channels indexed by outbound call identity.
    waiters: Mutex<BTreeMap<u64, oneshot::Sender<Result<Response, HostError>>>>,
}

impl PendingDoCalls {
    /// Creates an empty pending-call registry with the fixed in-flight ceiling.
    fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT_DO_CALLS)),
            waiters: Mutex::new(BTreeMap::new()),
        }
    }

    /// Reserves one in-flight slot and allocates a call identity and result channel.
    async fn register(
        &self,
    ) -> Result<
        (
            u64,
            OwnedSemaphorePermit,
            oneshot::Receiver<Result<Response, HostError>>,
        ),
        HostError,
    > {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| HostError::backend("do-call concurrency budget is closed"))?;
        let id = self
            .next_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(1)
            })
            .map_err(|_| HostError::backend("do-call identity exhausted"))?;
        let (sender, receiver) = oneshot::channel();
        let mut waiters = self.waiters.lock().await;
        if waiters.insert(id, sender).is_some() {
            return Err(HostError::backend(format!(
                "duplicate do-call identity {id}"
            )));
        }
        Ok((id, permit, receiver))
    }

    /// Removes one pending call after its outbound frame could not be sent.
    async fn remove(&self, id: u64) {
        self.waiters.lock().await.remove(&id);
    }

    /// Resolves one pending call or reports an unknown result identity.
    async fn resolve(
        &self,
        id: u64,
        result: Result<Response, HostError>,
    ) -> Result<(), EventEndpointError> {
        let sender = self.waiters.lock().await.remove(&id).ok_or_else(|| {
            EventEndpointError::Protocol(format!("unknown do-call result id {id}"))
        })?;
        let _ = sender.send(result);
        Ok(())
    }
}

/// Worker binding host that emits do-call frames and awaits their results.
#[derive(Clone)]
struct DoCallRouter {
    /// Per-event-socket pending call registry.
    pending: Arc<PendingDoCalls>,
    /// Endpoint writer channel serviced while dispatch is awaiting a call.
    outbound: mpsc::Sender<OutboundFrame>,
}

impl DoCallRouter {
    /// Creates a router attached to one event socket's writer channel.
    fn new(outbound: mpsc::Sender<OutboundFrame>) -> Self {
        Self {
            pending: Arc::new(PendingDoCalls::new()),
            outbound,
        }
    }

    /// Converts one successful result frame's optional fields into a response.
    fn decode_success(
        status: Option<u16>,
        headers: Option<Vec<(String, String)>>,
        body_b64: Option<String>,
        accept_ws: Option<SocketId>,
    ) -> Result<Response, HostError> {
        let status = status.ok_or_else(|| HostError::backend("do-call result omitted status"))?;
        let headers =
            headers.ok_or_else(|| HostError::backend("do-call result omitted headers"))?;
        let body_b64 = body_b64.ok_or_else(|| HostError::backend("do-call result omitted body"))?;
        let body = base64::engine::general_purpose::STANDARD
            .decode(body_b64)
            .map_err(|error| HostError::backend(format!("do-call body is not base64: {error}")))?;
        Ok(Response {
            status,
            headers,
            body,
            accept_ws,
        })
    }
}

#[async_trait]
impl WorkerBindings for DoCallRouter {
    /// Emits one ungated do-call and awaits the gateway's response frame.
    async fn do_fetch(
        &self,
        binding: String,
        object: String,
        request: Request,
    ) -> Result<Response, HostError> {
        let (id, permit, receiver) = self.pending.register().await?;
        let frame = OutboundFrame::DoCall {
            id,
            binding,
            object,
            method: request.method,
            url: request.uri,
            headers: request.headers,
            body_b64: base64::engine::general_purpose::STANDARD.encode(request.body),
            ws: request.ws,
        };
        if self.outbound.send(frame).await.is_err() {
            self.pending.remove(id).await;
            drop(permit);
            return Err(HostError::backend("event socket closed during do-call"));
        }
        let result = receiver
            .await
            .map_err(|_| HostError::backend("event socket closed during do-call"))?;
        drop(permit);
        result
    }
}

/// Host binding used by startup and detached alarm events without a gateway peer.
struct NoBindings;

#[async_trait]
impl WorkerBindings for NoBindings {
    /// Rejects a cross-object call when no event socket can carry its frame.
    async fn do_fetch(
        &self,
        _binding: String,
        _object: String,
        _request: Request,
    ) -> Result<Response, HostError> {
        Err(HostError::Unsupported {
            operation: "do-fetch without a gateway event socket",
        })
    }
}

/// Creates the honest no-peer binding capability for detached events.
fn no_bindings() -> Arc<dyn WorkerBindings> {
    Arc::new(NoBindings)
}

/// Commit-gated output and accepted WebSocket registry for one event socket.
#[derive(Default)]
struct GatewaySocketSink {
    /// Effects queued by committed event permits in stage order.
    effects: Mutex<Vec<OutboundFrame>>,
    /// Gateway-accepted WebSocket identities.
    sockets: Mutex<BTreeSet<SocketId>>,
}

impl GatewaySocketSink {
    /// Registers one gateway-accepted WebSocket before any guest event.
    async fn register(&self, socket: SocketId) -> Result<(), HostError> {
        let mut sockets = self.sockets.lock().await;
        if !sockets.insert(socket) {
            return Err(HostError::backend(format!(
                "WebSocket {socket} is already open"
            )));
        }
        Ok(())
    }

    /// Takes committed effects while preserving their stage order.
    async fn take_effects(&self) -> Vec<OutboundFrame> {
        std::mem::take(&mut *self.effects.lock().await)
    }

    /// Rejects effects aimed at a socket that the gateway did not register.
    async fn ensure_open(&self, socket: SocketId) -> Result<(), HostError> {
        if self.sockets.lock().await.contains(&socket) {
            Ok(())
        } else {
            Err(HostError::backend(format!(
                "WebSocket {socket} is not open"
            )))
        }
    }
}

#[async_trait]
impl WorkerSockets for GatewaySocketSink {
    /// Queues a committed WebSocket message for the endpoint writer.
    async fn send(&self, socket: SocketId, message: Vec<u8>) -> Result<(), HostError> {
        self.ensure_open(socket).await?;
        self.effects.lock().await.push(OutboundFrame::WsSend {
            ws: socket,
            text: true,
            data_b64: base64::engine::general_purpose::STANDARD.encode(message),
        });
        Ok(())
    }

    /// Queues a committed WebSocket close and removes the accepted identity.
    async fn close(&self, socket: SocketId, code: u16, reason: String) -> Result<(), HostError> {
        self.ensure_open(socket).await?;
        self.effects.lock().await.push(OutboundFrame::WsCloseOut {
            ws: socket,
            code,
            reason,
        });
        self.sockets.lock().await.remove(&socket);
        Ok(())
    }

    /// Rejects attachment writes on the output-only committed sink.
    async fn set_attachment(&self, _socket: SocketId, _value: Vec<u8>) -> Result<(), HostError> {
        Err(HostError::Unsupported {
            operation: "socket attachment without an event transaction",
        })
    }

    /// Rejects attachment reads on the output-only committed sink.
    async fn get_attachment(&self, _socket: SocketId) -> Result<Option<Vec<u8>>, HostError> {
        Err(HostError::Unsupported {
            operation: "socket attachment without an event transaction",
        })
    }

    /// Rejects attachment enumeration on the output-only committed sink.
    async fn attached(&self) -> Result<Vec<SocketId>, HostError> {
        Err(HostError::Unsupported {
            operation: "socket attachment without an event transaction",
        })
    }
}

/// Event-scoped socket capability that shares the event's storage transaction.
struct EventSockets {
    /// Commit-gated gateway output sink.
    sink: Arc<GatewaySocketSink>,
    /// Event transaction used for attachment persistence.
    storage: Arc<TursoWorkerStorage>,
}

#[async_trait]
impl WorkerSockets for EventSockets {
    /// Stages a message in the current event permit through the gate wrapper.
    async fn send(&self, socket: SocketId, message: Vec<u8>) -> Result<(), HostError> {
        self.sink.send(socket, message).await
    }

    /// Stages a close in the current event permit through the gate wrapper.
    async fn close(&self, socket: SocketId, code: u16, reason: String) -> Result<(), HostError> {
        self.sink.close(socket, code, reason).await
    }

    /// Stages an attachment in the current event transaction.
    async fn set_attachment(&self, socket: SocketId, value: Vec<u8>) -> Result<(), HostError> {
        self.storage.set_attachment(socket, value).await
    }

    /// Reads an attachment from the current event transaction view.
    async fn get_attachment(&self, socket: SocketId) -> Result<Option<Vec<u8>>, HostError> {
        self.storage.get_attachment(socket).await
    }

    /// Lists attachments from the current event transaction view.
    async fn attached(&self) -> Result<Vec<SocketId>, HostError> {
        self.storage.attached_sockets().await
    }
}

/// Resident NDJSON event endpoint for one Worker process.
pub struct EventEndpoint {
    /// Socket path owned by this endpoint.
    path: PathBuf,
    /// Bound Unix listener.
    listener: UnixListener,
    /// Turso database and remote durability boundary for event transactions.
    store: Arc<TursoStore>,
    /// Real runtime or a scripted test dispatcher.
    dispatcher: Arc<dyn EventDispatcher>,
    /// Serialized event gate shared by every accepted connection.
    gate: EventGate,
    /// Output sink shared by permits and the connection writer.
    sink: Arc<GatewaySocketSink>,
    /// Committed alarm deadline currently armed.
    alarm_deadline: Option<u64>,
}

impl EventEndpoint {
    /// Binds an event socket after the Turso constructor has validated its schema.
    pub async fn bind(
        path: impl AsRef<Path>,
        store: Arc<TursoStore>,
        dispatcher: Arc<dyn EventDispatcher>,
    ) -> Result<Self, EventEndpointError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = UnixListener::bind(&path)?;
        let sink = Arc::new(GatewaySocketSink::default());
        let gate = EventGate::new(Arc::clone(&sink) as Arc<dyn WorkerSockets>);
        Ok(Self {
            path,
            listener,
            store,
            dispatcher,
            gate,
            sink,
            alarm_deadline: None,
        })
    }

    /// Returns the event socket path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Runs initialization once, arms the committed alarm, and serves connections.
    pub async fn run(&mut self) -> Result<(), EventEndpointError> {
        self.initialize().await?;
        self.refresh_alarm().await?;
        loop {
            if let Some(deadline) = self.alarm_deadline {
                let mut sleep = Box::pin(tokio::time::sleep(duration_until(deadline)));
                tokio::select! {
                    result = self.listener.accept() => {
                        let (stream, _) = result?;
                        self.handle_connection(stream).await?;
                    }
                    () = sleep.as_mut() => {
                        self.alarm_deadline = None;
                        let _ = self.execute_alarm(deadline, no_bindings()).await?;
                        self.refresh_alarm().await?;
                    }
                }
            } else {
                let (stream, _) = self.listener.accept().await?;
                self.handle_connection(stream).await?;
            }
        }
    }

    /// Commits the component initialization transaction before accepting events.
    async fn initialize(&self) -> Result<(), EventEndpointError> {
        let (storage, sockets) = self.event_capabilities().await?;
        let pending = match self
            .dispatcher
            .dispatch_init(
                &self.gate,
                Arc::clone(&storage) as Arc<dyn WorkerStorage>,
                Arc::clone(&sockets) as Arc<dyn WorkerSockets>,
                no_bindings(),
            )
            .await
        {
            Ok(pending) => pending,
            Err(error) => {
                storage.rollback().await.map_err(EventEndpointError::Host)?;
                return Err(EventEndpointError::Runtime(error));
            }
        };
        let ((), permit) = pending.into_parts();
        storage.commit().await.map_err(EventEndpointError::Host)?;
        permit.commit().await.map_err(EventEndpointError::Host)?;
        let _ = self.sink.take_effects().await;
        Ok(())
    }

    /// Accepts one persistent gateway stream and clears its router on disconnect.
    async fn handle_connection(&mut self, stream: UnixStream) -> Result<(), EventEndpointError> {
        let result = self.handle_connection_inner(stream).await;
        self.store.clear_runtime_stream_appender().await;
        result
    }

    /// Services one gateway stream while its connection-local router is live.
    async fn handle_connection_inner(
        &mut self,
        stream: UnixStream,
    ) -> Result<(), EventEndpointError> {
        let (read_half, mut write_half) = stream.into_split();
        self.write_effects(&mut write_half).await?;
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(32);
        let router = Arc::new(DoCallRouter::new(outbound_sender));
        self.store
            .set_runtime_stream_appender(Arc::new(BindingStreamAppender::new(router.clone())))
            .await;
        let mut queued = VecDeque::new();
        loop {
            let frame = if let Some(frame) = queued.pop_front() {
                frame
            } else if let Some(deadline) = self.alarm_deadline {
                let mut sleep = Box::pin(tokio::time::sleep(duration_until(deadline)));
                tokio::select! {
                    result = read_frame(&mut reader, &mut line) => match result? {
                        Some(frame) => frame,
                        None => return Ok(()),
                    },
                    () = sleep.as_mut() => {
                        self.alarm_deadline = None;
                        let binding_router: Arc<dyn WorkerBindings> = router.clone();
                        let operation = self.execute_alarm(deadline, binding_router);
                        let Some(frames) = wait_for_pending(
                            operation,
                            &mut reader,
                            &mut write_half,
                            &router,
                            &mut outbound_receiver,
                            &mut queued,
                        ).await? else {
                            return Ok(());
                        };
                        write_frames(&mut write_half, &frames).await?;
                        self.refresh_alarm().await?;
                        continue;
                    }
                }
            } else {
                match read_frame(&mut reader, &mut line).await? {
                    Some(frame) => frame,
                    None => return Ok(()),
                }
            };

            if let InboundFrame::DoCallResult {
                id,
                status,
                headers,
                body_b64,
                accept_ws,
                error,
            } = frame
            {
                router
                    .pending
                    .resolve(
                        id,
                        decode_do_call_result(status, headers, body_b64, accept_ws, error),
                    )
                    .await?;
                continue;
            }
            let operation = self.handle_frame(frame, Arc::clone(&router));
            let Some(frames) = wait_for_pending(
                operation,
                &mut reader,
                &mut write_half,
                &router,
                &mut outbound_receiver,
                &mut queued,
            )
            .await?
            else {
                return Ok(());
            };
            write_frames(&mut write_half, &frames).await?;
        }
    }

    /// Dispatches one protocol frame and returns terminal output in event order.
    async fn handle_frame(
        &mut self,
        frame: InboundFrame,
        router: Arc<DoCallRouter>,
    ) -> Result<Vec<OutboundFrame>, EventEndpointError> {
        let frames = match frame {
            InboundFrame::DoCallResult { .. } => {
                return Err(EventEndpointError::Protocol(
                    "do-call result was not awaited".to_owned(),
                ));
            }
            InboundFrame::WsOpen { ws } => {
                self.sink
                    .register(ws)
                    .await
                    .map_err(EventEndpointError::Host)?;
                Vec::new()
            }
            InboundFrame::Fetch {
                id,
                method,
                url,
                headers,
                body_b64,
                ws,
            } => {
                let body = match base64::engine::general_purpose::STANDARD.decode(body_b64) {
                    Ok(body) => body,
                    Err(error) => {
                        let frames = vec![OutboundFrame::Error {
                            id,
                            message: format!("invalid fetch body: {error}"),
                        }];
                        self.refresh_alarm().await?;
                        return Ok(frames);
                    }
                };
                let request = Request {
                    method,
                    uri: url,
                    headers,
                    body,
                    ws,
                };
                match self.dispatch_fetch(request, router).await {
                    Ok(response) => {
                        let mut frames = self.sink.take_effects().await;
                        frames.push(OutboundFrame::FetchResult {
                            id,
                            status: response.status,
                            headers: response.headers,
                            body_b64: base64::engine::general_purpose::STANDARD
                                .encode(response.body),
                            accept_ws: response.accept_ws,
                        });
                        frames
                    }
                    Err(error) => vec![OutboundFrame::Error {
                        id,
                        message: error.to_string(),
                    }],
                }
            }
            InboundFrame::WsMessage {
                id,
                ws,
                text: _text,
                data_b64,
            } => match base64::engine::general_purpose::STANDARD.decode(data_b64) {
                Ok(message) => match self.dispatch_ws_message(ws, message, router).await {
                    Ok(()) => {
                        let mut frames = self.sink.take_effects().await;
                        frames.push(OutboundFrame::Done { id });
                        frames
                    }
                    Err(message) => vec![OutboundFrame::Error { id, message }],
                },
                Err(error) => vec![OutboundFrame::Error {
                    id,
                    message: format!("invalid WebSocket body: {error}"),
                }],
            },
            InboundFrame::WsClose {
                id,
                ws,
                code,
                reason,
            } => match self.dispatch_ws_close(ws, code, reason, router).await {
                Ok(()) => {
                    let mut frames = self.sink.take_effects().await;
                    frames.push(OutboundFrame::Done { id });
                    frames
                }
                Err(message) => vec![OutboundFrame::Error { id, message }],
            },
        };
        self.refresh_alarm().await?;
        Ok(frames)
    }

    /// Delivers one fetch through a fresh event transaction.
    async fn dispatch_fetch(
        &self,
        request: Request,
        router: Arc<DoCallRouter>,
    ) -> Result<Response, EventEndpointError> {
        let (storage, sockets) = self.event_capabilities().await?;
        let pending = match self
            .dispatcher
            .dispatch_fetch(
                &self.gate,
                Arc::clone(&storage) as Arc<dyn WorkerStorage>,
                Arc::clone(&sockets) as Arc<dyn WorkerSockets>,
                router,
                request,
            )
            .await
        {
            Ok(pending) => pending,
            Err(error) => {
                storage.rollback().await.map_err(EventEndpointError::Host)?;
                return Err(EventEndpointError::Runtime(error));
            }
        };
        let (response, permit) = pending.into_parts();
        storage.commit().await.map_err(EventEndpointError::Host)?;
        permit.commit().await.map_err(EventEndpointError::Host)?;
        Ok(response)
    }

    /// Delivers one WebSocket message through a fresh event transaction.
    async fn dispatch_ws_message(
        &self,
        socket: SocketId,
        message: Vec<u8>,
        router: Arc<DoCallRouter>,
    ) -> Result<(), String> {
        let (storage, sockets) = self
            .event_capabilities()
            .await
            .map_err(|error| error.to_string())?;
        let pending = match self
            .dispatcher
            .dispatch_websocket_message(
                &self.gate,
                Arc::clone(&storage) as Arc<dyn WorkerStorage>,
                Arc::clone(&sockets) as Arc<dyn WorkerSockets>,
                router,
                socket,
                message,
            )
            .await
        {
            Ok(pending) => pending,
            Err(error) => {
                storage
                    .rollback()
                    .await
                    .map_err(|rollback| rollback.to_string())?;
                return Err(error.to_string());
            }
        };
        let ((), permit) = pending.into_parts();
        storage.commit().await.map_err(|error| error.to_string())?;
        permit.commit().await.map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Delivers one WebSocket close through a fresh event transaction.
    async fn dispatch_ws_close(
        &self,
        socket: SocketId,
        code: u16,
        reason: String,
        router: Arc<DoCallRouter>,
    ) -> Result<(), String> {
        let (storage, sockets) = self
            .event_capabilities()
            .await
            .map_err(|error| error.to_string())?;
        let pending = match self
            .dispatcher
            .dispatch_websocket_close(
                &self.gate,
                Arc::clone(&storage) as Arc<dyn WorkerStorage>,
                Arc::clone(&sockets) as Arc<dyn WorkerSockets>,
                router,
                socket,
                code,
                reason,
            )
            .await
        {
            Ok(pending) => pending,
            Err(error) => {
                storage
                    .rollback()
                    .await
                    .map_err(|rollback| rollback.to_string())?;
                return Err(error.to_string());
            }
        };
        let ((), permit) = pending.into_parts();
        storage.commit().await.map_err(|error| error.to_string())?;
        permit.commit().await.map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Executes one advisory alarm and returns committed socket effects.
    async fn execute_alarm(
        &self,
        scheduled: u64,
        router: Arc<dyn WorkerBindings>,
    ) -> Result<Vec<OutboundFrame>, EventEndpointError> {
        if self
            .store
            .alarm()
            .await
            .map_err(EventEndpointError::Store)?
            != Some(scheduled)
        {
            return Ok(Vec::new());
        }
        let (storage, sockets) = self.event_capabilities().await?;
        let pending = self
            .dispatcher
            .dispatch_alarm(
                &self.gate,
                Arc::clone(&storage) as Arc<dyn WorkerStorage>,
                Arc::clone(&sockets) as Arc<dyn WorkerSockets>,
                router,
                scheduled,
            )
            .await;
        match pending {
            Ok(pending) => {
                let ((), permit) = pending.into_parts();
                match storage.commit().await {
                    Ok(_) => {
                        if let Err(error) = permit.commit().await {
                            eprintln!("alarm output commit failed: {error}");
                            return Ok(Vec::new());
                        }
                        Ok(self.sink.take_effects().await)
                    }
                    Err(error) => {
                        eprintln!("alarm transaction commit failed: {error}");
                        Ok(Vec::new())
                    }
                }
            }
            Err(error) => {
                // Alarm frames have no gateway id, so there is no terminal error
                // frame to emit. Roll back before retaining the alarm deadline.
                storage.rollback().await.map_err(EventEndpointError::Host)?;
                eprintln!("alarm handler failed: {error}");
                Ok(Vec::new())
            }
        }
    }

    /// Creates one event transaction and its event-scoped host capabilities.
    async fn event_capabilities(
        &self,
    ) -> Result<(Arc<TursoWorkerStorage>, Arc<EventSockets>), EventEndpointError> {
        let storage = Arc::new(
            TursoWorkerStorage::begin(Arc::clone(&self.store))
                .await
                .map_err(EventEndpointError::Host)?,
        );
        let sockets = Arc::new(EventSockets {
            sink: Arc::clone(&self.sink),
            storage: Arc::clone(&storage),
        });
        Ok((storage, sockets))
    }

    /// Refreshes the one timer from committed alarm state.
    async fn refresh_alarm(&mut self) -> Result<(), EventEndpointError> {
        self.alarm_deadline = self
            .store
            .alarm()
            .await
            .map_err(EventEndpointError::Store)?;
        Ok(())
    }

    /// Writes every committed socket effect before a terminal frame.
    async fn write_effects<W: AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
    ) -> Result<(), EventEndpointError> {
        for effect in self.sink.take_effects().await {
            self.write_frame(writer, &effect).await?;
        }
        Ok(())
    }

    /// Serializes one output frame as exactly one NDJSON line.
    async fn write_frame<W: AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        frame: &OutboundFrame,
    ) -> Result<(), EventEndpointError> {
        let mut bytes = serde_json::to_vec(frame)
            .map_err(|error| EventEndpointError::Protocol(error.to_string()))?;
        bytes.push(b'\n');
        writer.write_all(&bytes).await?;
        writer.flush().await?;
        Ok(())
    }
}

/// Reads and decodes one bounded NDJSON frame from the event socket.
async fn read_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    line: &mut String,
) -> Result<Option<InboundFrame>, EventEndpointError> {
    let read = reader.read_line(line).await?;
    if read == 0 {
        return Ok(None);
    }
    if line.len() > MAX_FRAME_BYTES {
        return Err(EventEndpointError::Protocol(
            "event frame exceeds endpoint limit".to_owned(),
        ));
    }
    let frame = serde_json::from_str::<InboundFrame>(line.trim_end())
        .map_err(|error| EventEndpointError::Protocol(error.to_string()))?;
    line.clear();
    Ok(Some(frame))
}

/// Converts one inbound call-result frame into a host response or hard gateway error.
fn decode_do_call_result(
    status: Option<u16>,
    headers: Option<Vec<(String, String)>>,
    body_b64: Option<String>,
    accept_ws: Option<SocketId>,
    error: Option<DoCallErrorFrame>,
) -> Result<Response, HostError> {
    if let Some(error) = error {
        return Err(HostError::backend(format!(
            "gateway do-call {}: {}",
            error.code, error.message
        )));
    }
    DoCallRouter::decode_success(status, headers, body_b64, accept_ws)
}

/// Services call frames while one event dispatch awaits its result.
async fn wait_for_pending<R, W, F>(
    operation: F,
    reader: &mut BufReader<R>,
    writer: &mut W,
    router: &DoCallRouter,
    outbound: &mut mpsc::Receiver<OutboundFrame>,
    queued: &mut VecDeque<InboundFrame>,
) -> Result<Option<Vec<OutboundFrame>>, EventEndpointError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    F: Future<Output = Result<Vec<OutboundFrame>, EventEndpointError>>,
{
    let mut operation = Box::pin(operation);
    let mut line = String::new();
    loop {
        tokio::select! {
            result = &mut operation => return result.map(Some),
            frame = outbound.recv() => {
                let frame = frame.ok_or_else(|| {
                    EventEndpointError::Protocol("do-call writer channel closed".to_owned())
                })?;
                write_frame_to(writer, &frame).await?;
            }
            result = read_frame(reader, &mut line) => {
                let Some(frame) = result? else {
                    return Ok(None);
                };
                match frame {
                    InboundFrame::DoCallResult {
                        id,
                        status,
                        headers,
                        body_b64,
                        accept_ws,
                        error,
                    } => {
                        router
                            .pending
                            .resolve(
                                id,
                                decode_do_call_result(
                                    status,
                                    headers,
                                    body_b64,
                                    accept_ws,
                                    error,
                                ),
                            )
                            .await?;
                    }
                    frame => queued.push_back(frame),
                }
            }
        }
    }
}

/// Writes one or more frames while preserving their event order.
async fn write_frames<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frames: &[OutboundFrame],
) -> Result<(), EventEndpointError> {
    for frame in frames {
        write_frame_to(writer, frame).await?;
    }
    Ok(())
}

/// Serializes one output frame as exactly one NDJSON line.
async fn write_frame_to<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &OutboundFrame,
) -> Result<(), EventEndpointError> {
    let mut bytes = serde_json::to_vec(frame)
        .map_err(|error| EventEndpointError::Protocol(error.to_string()))?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

impl Drop for EventEndpoint {
    /// Removes the event socket name when the resident process exits.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Converts a committed epoch-millisecond deadline into a nonnegative delay.
fn duration_until(deadline: u64) -> Duration {
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => u64::try_from(duration.as_millis()).map_or(u64::MAX, |millis| millis),
        Err(_) => 0,
    };
    Duration::from_millis(deadline.saturating_sub(now))
}
