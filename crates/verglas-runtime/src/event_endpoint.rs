//! NDJSON event endpoint for one resident Durable Object Worker.
//!
//! This module owns frame decoding, transaction-scoped Worker dispatch, commit-gated
//! socket output, and advisory alarm delivery. Replica control traffic remains in
//! `verglas-do-engine`; this socket carries only the gateway event protocol.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use verglas_do_engine::{
    DoEngine, DoStorage, Error as EngineError, IsolationLevel, SnapshotFence, WorkerStateView,
    ensure_worker_tables,
};
use verglas_do_wasm::{
    EventGate, HostError, PendingEvent, Request, Response, RuntimeError, SocketId, WorkerRuntime,
    WorkerSockets, WorkerStorage,
};

use crate::worker_storage::EngineWorkerStorage;

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Errors that prevent the event endpoint from serving its socket.
#[derive(Debug, Error)]
pub enum EventEndpointError {
    /// Reports Unix socket or stream I/O failure.
    #[error("event endpoint I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Reports a durable engine failure while starting or delivering an event.
    #[error("event endpoint engine failure: {0}")]
    Engine(#[source] EngineError),
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
    ) -> Result<PendingEvent<()>, RuntimeError>;

    /// Dispatches one HTTP fetch request.
    async fn dispatch_fetch(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        request: Request,
    ) -> Result<PendingEvent<Response>, RuntimeError>;

    /// Dispatches one durable alarm event.
    async fn dispatch_alarm(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        scheduled_millis: u64,
    ) -> Result<PendingEvent<()>, RuntimeError>;

    /// Dispatches one WebSocket message event.
    async fn dispatch_websocket_message(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        socket: SocketId,
        message: Vec<u8>,
    ) -> Result<PendingEvent<()>, RuntimeError>;

    /// Dispatches one WebSocket close event.
    async fn dispatch_websocket_close(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
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
    ) -> Result<PendingEvent<()>, RuntimeError> {
        WorkerRuntime::dispatch_init(self, gate, storage, sockets).await
    }

    /// Runs the real Worker component fetch export.
    async fn dispatch_fetch(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        request: Request,
    ) -> Result<PendingEvent<Response>, RuntimeError> {
        WorkerRuntime::dispatch_fetch(self, gate, storage, sockets, request).await
    }

    /// Runs the real Worker component alarm export.
    async fn dispatch_alarm(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        scheduled_millis: u64,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        WorkerRuntime::dispatch_alarm(self, gate, storage, sockets, scheduled_millis).await
    }

    /// Runs the real Worker component WebSocket message export.
    async fn dispatch_websocket_message(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        socket: SocketId,
        message: Vec<u8>,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        WorkerRuntime::dispatch_websocket_message(self, gate, storage, sockets, socket, message)
            .await
    }

    /// Runs the real Worker component WebSocket close export.
    async fn dispatch_websocket_close(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        socket: SocketId,
        code: u16,
        reason: String,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        WorkerRuntime::dispatch_websocket_close(self, gate, storage, sockets, socket, code, reason)
            .await
    }
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
    storage: Arc<EngineWorkerStorage>,
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
    /// Engine and commit authority for event transactions.
    engine: Arc<DoEngine>,
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
    /// Binds an event socket and ensures reserved Worker tables exist.
    pub async fn bind(
        path: impl AsRef<Path>,
        engine: Arc<DoEngine>,
        dispatcher: Arc<dyn EventDispatcher>,
    ) -> Result<Self, EventEndpointError> {
        ensure_worker_tables(engine.as_ref())
            .await
            .map_err(EventEndpointError::Engine)?;
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
            engine,
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
                        self.execute_alarm(deadline).await?;
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
        let pending = self
            .dispatcher
            .dispatch_init(
                &self.gate,
                Arc::clone(&storage) as Arc<dyn WorkerStorage>,
                Arc::clone(&sockets) as Arc<dyn WorkerSockets>,
            )
            .await
            .map_err(EventEndpointError::Runtime)?;
        let ((), permit) = pending.into_parts();
        storage.commit().await.map_err(EventEndpointError::Host)?;
        permit.commit().await.map_err(EventEndpointError::Host)?;
        let _ = self.sink.take_effects().await;
        Ok(())
    }

    /// Accepts one persistent gateway stream and interleaves input with alarms.
    async fn handle_connection(&mut self, stream: UnixStream) -> Result<(), EventEndpointError> {
        let (read_half, mut write_half) = stream.into_split();
        self.write_effects(&mut write_half).await?;
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();
        loop {
            if let Some(deadline) = self.alarm_deadline {
                let delay = duration_until(deadline);
                let mut sleep = Box::pin(tokio::time::sleep(delay));
                tokio::select! {
                    result = reader.read_line(&mut line) => {
                        if !self.handle_read_result(result?, &mut line, &mut write_half).await? {
                            return Ok(());
                        }
                    }
                    () = sleep.as_mut() => {
                        self.alarm_deadline = None;
                        self.deliver_alarm(deadline, &mut write_half).await?;
                        self.refresh_alarm().await?;
                    }
                }
            } else {
                let read = reader.read_line(&mut line).await?;
                if !self
                    .handle_read_result(read, &mut line, &mut write_half)
                    .await?
                {
                    return Ok(());
                }
            }
        }
    }

    /// Parses and handles one completed input line, returning false on EOF.
    async fn handle_read_result<W: AsyncWrite + Unpin>(
        &mut self,
        read: usize,
        line: &mut String,
        writer: &mut W,
    ) -> Result<bool, EventEndpointError> {
        if read == 0 {
            return Ok(false);
        }
        if line.len() > MAX_FRAME_BYTES {
            return Err(EventEndpointError::Protocol(
                "event frame exceeds endpoint limit".to_owned(),
            ));
        }
        let frame = serde_json::from_str::<InboundFrame>(line.trim_end())
            .map_err(|error| EventEndpointError::Protocol(error.to_string()))?;
        self.handle_frame(frame, writer).await?;
        line.clear();
        Ok(true)
    }

    /// Dispatches one protocol frame and writes its terminal response in order.
    async fn handle_frame<W: AsyncWrite + Unpin>(
        &mut self,
        frame: InboundFrame,
        writer: &mut W,
    ) -> Result<(), EventEndpointError> {
        match frame {
            InboundFrame::WsOpen { ws } => {
                self.sink
                    .register(ws)
                    .await
                    .map_err(EventEndpointError::Host)?;
            }
            InboundFrame::Fetch {
                id,
                method,
                url,
                headers,
                body_b64,
            } => {
                let body = match base64::engine::general_purpose::STANDARD.decode(body_b64) {
                    Ok(body) => body,
                    Err(error) => {
                        self.write_frame(
                            writer,
                            &OutboundFrame::Error {
                                id,
                                message: format!("invalid fetch body: {error}"),
                            },
                        )
                        .await?;
                        return Ok(());
                    }
                };
                let request = Request {
                    method,
                    uri: url,
                    headers,
                    body,
                };
                let result = self
                    .dispatch_fetch(request)
                    .await
                    .map_err(|error| error.to_string());
                match result {
                    Ok(response) => {
                        self.write_effects(writer).await?;
                        self.write_frame(
                            writer,
                            &OutboundFrame::FetchResult {
                                id,
                                status: response.status,
                                headers: response.headers,
                                body_b64: base64::engine::general_purpose::STANDARD
                                    .encode(response.body),
                            },
                        )
                        .await?;
                    }
                    Err(message) => {
                        self.write_frame(writer, &OutboundFrame::Error { id, message })
                            .await?;
                    }
                }
            }
            InboundFrame::WsMessage {
                id,
                ws,
                text: _text,
                data_b64,
            } => {
                let message = match base64::engine::general_purpose::STANDARD.decode(data_b64) {
                    Ok(message) => message,
                    Err(error) => {
                        self.write_frame(
                            writer,
                            &OutboundFrame::Error {
                                id,
                                message: format!("invalid WebSocket body: {error}"),
                            },
                        )
                        .await?;
                        return Ok(());
                    }
                };
                match self.dispatch_ws_message(ws, message).await {
                    Ok(()) => {
                        self.write_effects(writer).await?;
                        self.write_frame(writer, &OutboundFrame::Done { id })
                            .await?;
                    }
                    Err(message) => {
                        self.write_frame(writer, &OutboundFrame::Error { id, message })
                            .await?;
                    }
                }
            }
            InboundFrame::WsClose {
                id,
                ws,
                code,
                reason,
            } => match self.dispatch_ws_close(ws, code, reason).await {
                Ok(()) => {
                    self.write_effects(writer).await?;
                    self.write_frame(writer, &OutboundFrame::Done { id })
                        .await?;
                }
                Err(message) => {
                    self.write_frame(writer, &OutboundFrame::Error { id, message })
                        .await?;
                }
            },
        }
        self.refresh_alarm().await?;
        Ok(())
    }

    /// Delivers one fetch through a fresh event transaction.
    async fn dispatch_fetch(&self, request: Request) -> Result<Response, EventEndpointError> {
        let (storage, sockets) = self.event_capabilities().await?;
        let pending = self
            .dispatcher
            .dispatch_fetch(
                &self.gate,
                Arc::clone(&storage) as Arc<dyn WorkerStorage>,
                Arc::clone(&sockets) as Arc<dyn WorkerSockets>,
                request,
            )
            .await
            .map_err(EventEndpointError::Runtime)?;
        let (response, permit) = pending.into_parts();
        storage.commit().await.map_err(EventEndpointError::Host)?;
        permit.commit().await.map_err(EventEndpointError::Host)?;
        Ok(response)
    }

    /// Delivers one WebSocket message through a fresh event transaction.
    async fn dispatch_ws_message(&self, socket: SocketId, message: Vec<u8>) -> Result<(), String> {
        let (storage, sockets) = self
            .event_capabilities()
            .await
            .map_err(|error| error.to_string())?;
        let pending = self
            .dispatcher
            .dispatch_websocket_message(
                &self.gate,
                Arc::clone(&storage) as Arc<dyn WorkerStorage>,
                Arc::clone(&sockets) as Arc<dyn WorkerSockets>,
                socket,
                message,
            )
            .await
            .map_err(|error| error.to_string())?;
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
    ) -> Result<(), String> {
        let (storage, sockets) = self
            .event_capabilities()
            .await
            .map_err(|error| error.to_string())?;
        let pending = self
            .dispatcher
            .dispatch_websocket_close(
                &self.gate,
                Arc::clone(&storage) as Arc<dyn WorkerStorage>,
                Arc::clone(&sockets) as Arc<dyn WorkerSockets>,
                socket,
                code,
                reason,
            )
            .await
            .map_err(|error| error.to_string())?;
        let ((), permit) = pending.into_parts();
        storage.commit().await.map_err(|error| error.to_string())?;
        permit.commit().await.map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Delivers an advisory alarm only if its deadline is still committed.
    async fn deliver_alarm<W: AsyncWrite + Unpin>(
        &self,
        scheduled: u64,
        writer: &mut W,
    ) -> Result<(), EventEndpointError> {
        self.execute_alarm(scheduled).await?;
        self.write_effects(writer).await
    }

    /// Executes one advisory alarm without requiring an attached gateway writer.
    async fn execute_alarm(&self, scheduled: u64) -> Result<(), EventEndpointError> {
        if WorkerStateView::new(self.engine.as_ref())
            .alarm()
            .await
            .map_err(EventEndpointError::Engine)?
            != Some(scheduled)
        {
            return Ok(());
        }
        let (storage, sockets) = self.event_capabilities().await?;
        let pending = self
            .dispatcher
            .dispatch_alarm(
                &self.gate,
                Arc::clone(&storage) as Arc<dyn WorkerStorage>,
                Arc::clone(&sockets) as Arc<dyn WorkerSockets>,
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
                        }
                    }
                    Err(error) => {
                        eprintln!("alarm transaction commit failed: {error}");
                    }
                }
            }
            Err(error) => {
                // Alarm frames have no gateway id, so there is no terminal error
                // frame to emit. The dispatch error already aborted its permit.
                eprintln!("alarm handler failed: {error}");
            }
        }
        Ok(())
    }

    /// Creates one event transaction and its event-scoped host capabilities.
    async fn event_capabilities(
        &self,
    ) -> Result<(Arc<EngineWorkerStorage>, Arc<EventSockets>), EventEndpointError> {
        let transaction = self
            .engine
            .begin(IsolationLevel::Snapshot)
            .await
            .map_err(EventEndpointError::Engine)?;
        let snapshot = SnapshotFence::at(transaction.envelope().base_commit_sequence());
        let storage = Arc::new(EngineWorkerStorage::new_with_snapshot(
            Arc::clone(&self.engine),
            transaction,
            snapshot,
        ));
        let sockets = Arc::new(EventSockets {
            sink: Arc::clone(&self.sink),
            storage: Arc::clone(&storage),
        });
        Ok((storage, sockets))
    }

    /// Refreshes the one timer from committed alarm state.
    async fn refresh_alarm(&mut self) -> Result<(), EventEndpointError> {
        self.alarm_deadline = WorkerStateView::new(self.engine.as_ref())
            .alarm()
            .await
            .map_err(EventEndpointError::Engine)?;
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
