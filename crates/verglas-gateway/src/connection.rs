//! Resident per-Durable-Object event-socket actors.
//!
//! Each actor serializes writes, reads worker frames in wire order, and routes
//! commit-gated WebSocket effects to the live client channel for that object.

use std::collections::HashMap;
use std::path::Path;

use base64::Engine;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};

use crate::error::GatewayError;
use crate::protocol::{FetchResponse, GatewayFrame, WorkerFrame, WsOutbound};

/// One HTTP fetch request after conversion to protocol fields.
#[derive(Debug)]
pub(crate) struct FetchEvent {
    /// HTTP method text.
    pub method: String,
    /// Request path and query string.
    pub url: String,
    /// Ordered request headers.
    pub headers: Vec<(String, String)>,
    /// Raw request body.
    pub body: Vec<u8>,
}

/// One live connection to a resident Durable Object.
pub(crate) struct DoConnection {
    commands: mpsc::Sender<Command>,
}

impl DoConnection {
    /// Connects to a spawned event socket and starts its resident reader actor.
    pub(crate) async fn connect(path: impl AsRef<Path>) -> Result<Self, GatewayError> {
        let stream = UnixStream::connect(path.as_ref())
            .await
            .map_err(|error| GatewayError::event_io("connect", error))?;
        let (reader, writer) = tokio::io::split(stream);
        let (commands, receiver) = mpsc::channel(128);
        let actor = EventActor::new(reader, writer, receiver);
        tokio::spawn(async move {
            actor.run().await;
        });
        Ok(Self { commands })
    }

    /// Sends one fetch event and waits for its terminal fetch-result or error.
    pub(crate) async fn fetch(&self, event: FetchEvent) -> Result<FetchResponse, GatewayError> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::Fetch { event, respond_to })
            .await
            .map_err(|_| GatewayError::Disconnected)?;
        response.await.map_err(|_| GatewayError::Disconnected)?
    }

    /// Registers one WebSocket and waits until its ws-open frame is written.
    pub(crate) async fn open_websocket(
        &self,
    ) -> Result<(u64, mpsc::UnboundedReceiver<WsOutbound>), GatewayError> {
        let (effects, receiver) = mpsc::unbounded_channel();
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::OpenWebSocket {
                effects,
                respond_to,
            })
            .await
            .map_err(|_| GatewayError::Disconnected)?;
        let ws = response.await.map_err(|_| GatewayError::Disconnected)??;
        Ok((ws, receiver))
    }

    /// Sends one client WebSocket message and waits only for frame admission.
    pub(crate) async fn websocket_message(
        &self,
        ws: u64,
        text: bool,
        data: Vec<u8>,
    ) -> Result<(), GatewayError> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::WebSocketMessage {
                ws,
                text,
                data,
                respond_to,
            })
            .await
            .map_err(|_| GatewayError::Disconnected)?;
        response.await.map_err(|_| GatewayError::Disconnected)?
    }

    /// Sends a client WebSocket close event and waits until its frame is written.
    pub(crate) async fn websocket_close(
        &self,
        ws: u64,
        code: u16,
        reason: String,
    ) -> Result<(), GatewayError> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::WebSocketClose {
                ws,
                code,
                reason,
                respond_to,
            })
            .await
            .map_err(|_| GatewayError::Disconnected)?;
        response.await.map_err(|_| GatewayError::Disconnected)?
    }

    /// Removes a disconnected client from the effect routing map.
    pub(crate) async fn remove_websocket(&self, ws: u64) {
        let _ = self.commands.send(Command::RemoveWebSocket { ws }).await;
    }
}

/// A command serialized by one event-socket actor.
enum Command {
    /// Writes one fetch frame and tracks its response.
    Fetch {
        /// Event fields to encode.
        event: FetchEvent,
        /// Terminal response channel.
        respond_to: oneshot::Sender<Result<FetchResponse, GatewayError>>,
    },
    /// Writes ws-open and registers an effect channel.
    OpenWebSocket {
        /// Client effect channel owned by the WebSocket task.
        effects: mpsc::UnboundedSender<WsOutbound>,
        /// Channel receiving the assigned identity.
        respond_to: oneshot::Sender<Result<u64, GatewayError>>,
    },
    /// Writes one ws-message and tracks its terminal frame.
    WebSocketMessage {
        /// Gateway-owned WebSocket identity.
        ws: u64,
        /// Whether the payload is text.
        text: bool,
        /// Raw message bytes.
        data: Vec<u8>,
        /// Admission acknowledgement channel, independent of the event terminal frame.
        respond_to: oneshot::Sender<Result<(), GatewayError>>,
    },
    /// Writes one ws-close event without requiring a client response.
    WebSocketClose {
        /// Gateway-owned WebSocket identity.
        ws: u64,
        /// Client close code.
        code: u16,
        /// Client close reason.
        reason: String,
        /// Channel acknowledged after the frame is written.
        respond_to: oneshot::Sender<Result<(), GatewayError>>,
    },
    /// Removes one disconnected WebSocket identity.
    RemoveWebSocket {
        /// Gateway-owned WebSocket identity.
        ws: u64,
    },
}

/// A terminal event waiter retained until the worker emits its terminal frame.
enum PendingEvent {
    /// HTTP fetch response waiter.
    Fetch(oneshot::Sender<Result<FetchResponse, GatewayError>>),
    /// WebSocket event whose client does not wait for the terminal frame.
    Ignore,
}

/// Owns one split Unix event socket and all pending event identities.
struct EventActor {
    reader: BufReader<ReadHalf<UnixStream>>,
    writer: WriteHalf<UnixStream>,
    commands: mpsc::Receiver<Command>,
    next_event_id: u64,
    next_ws_id: u64,
    pending: HashMap<u64, PendingEvent>,
    websockets: HashMap<u64, mpsc::UnboundedSender<WsOutbound>>,
}

impl EventActor {
    /// Creates an actor with independent monotonic event and WebSocket counters.
    fn new(
        reader: ReadHalf<UnixStream>,
        writer: WriteHalf<UnixStream>,
        commands: mpsc::Receiver<Command>,
    ) -> Self {
        Self {
            reader: BufReader::new(reader),
            writer,
            commands,
            next_event_id: 1,
            next_ws_id: 1,
            pending: HashMap::new(),
            websockets: HashMap::new(),
        }
    }

    /// Runs until the event socket closes or one protocol violation fails it closed.
    async fn run(mut self) {
        let mut line = String::new();
        loop {
            tokio::select! {
                command = self.commands.recv() => {
                    let Some(command) = command else { break; };
                    if self.handle_command(command).await.is_err() {
                        break;
                    }
                }
                result = self.reader.read_line(&mut line) => {
                    match result {
                        Ok(0) => break,
                        Ok(_) => {
                            let frame = match serde_json::from_str::<WorkerFrame>(line.trim_end()) {
                                Ok(frame) => frame,
                                Err(error) => {
                                    self.fail_all(GatewayError::Protocol { message: error.to_string() });
                                    break;
                                }
                            };
                            if self.handle_worker_frame(frame).is_err() {
                                break;
                            }
                            line.clear();
                        }
                        Err(error) => {
                            self.fail_all(GatewayError::event_io("read", error));
                            break;
                        }
                    }
                }
            }
        }
        self.fail_all(GatewayError::Disconnected);
    }

    /// Applies one gateway command and writes its frame before acknowledging it.
    async fn handle_command(&mut self, command: Command) -> Result<(), GatewayError> {
        match command {
            Command::Fetch { event, respond_to } => {
                let id = self.next_event()?;
                let frame = GatewayFrame::Fetch {
                    id,
                    method: event.method,
                    url: event.url,
                    headers: event.headers,
                    body_b64: base64::engine::general_purpose::STANDARD.encode(event.body),
                };
                self.pending.insert(id, PendingEvent::Fetch(respond_to));
                if let Err(error) = self.write_frame(&frame).await {
                    self.pending.remove(&id);
                    return Err(error);
                }
            }
            Command::OpenWebSocket {
                effects,
                respond_to,
            } => {
                let ws = self.next_websocket()?;
                let frame = GatewayFrame::WsOpen { ws };
                self.websockets.insert(ws, effects);
                if let Err(error) = self.write_frame(&frame).await {
                    self.websockets.remove(&ws);
                    let _ = respond_to.send(Err(error.clone()));
                    return Err(error);
                }
                let _ = respond_to.send(Ok(ws));
            }
            Command::WebSocketMessage {
                ws,
                text,
                data,
                respond_to,
            } => {
                if !self.websockets.contains_key(&ws) {
                    let _ = respond_to.send(Err(GatewayError::Protocol {
                        message: format!("unknown WebSocket {ws}"),
                    }));
                    return Ok(());
                }
                let id = self.next_event()?;
                let frame = GatewayFrame::WsMessage {
                    id,
                    ws,
                    text,
                    data_b64: base64::engine::general_purpose::STANDARD.encode(data),
                };
                self.pending.insert(id, PendingEvent::Ignore);
                if let Err(error) = self.write_frame(&frame).await {
                    self.pending.remove(&id);
                    let _ = respond_to.send(Err(error.clone()));
                    return Err(error);
                }
                let _ = respond_to.send(Ok(()));
            }
            Command::WebSocketClose {
                ws,
                code,
                reason,
                respond_to,
            } => {
                if !self.websockets.contains_key(&ws) {
                    let _ = respond_to.send(Err(GatewayError::Protocol {
                        message: format!("unknown WebSocket {ws}"),
                    }));
                    return Ok(());
                }
                let id = self.next_event()?;
                let frame = GatewayFrame::WsClose {
                    id,
                    ws,
                    code,
                    reason,
                };
                self.pending.insert(id, PendingEvent::Ignore);
                if let Err(error) = self.write_frame(&frame).await {
                    self.pending.remove(&id);
                    let _ = respond_to.send(Err(error.clone()));
                    return Err(error);
                }
                let _ = respond_to.send(Ok(()));
            }
            Command::RemoveWebSocket { ws } => {
                self.websockets.remove(&ws);
            }
        }
        Ok(())
    }

    /// Applies one worker frame without reordering effects around its terminal frame.
    fn handle_worker_frame(&mut self, frame: WorkerFrame) -> Result<(), GatewayError> {
        match frame {
            WorkerFrame::WsSend { ws, text, data_b64 } => {
                let data = base64::engine::general_purpose::STANDARD
                    .decode(data_b64)
                    .map_err(|error| GatewayError::Protocol {
                        message: format!("ws-send body is not base64: {error}"),
                    })?;
                let Some(effects) = self.websockets.get(&ws) else {
                    return Ok(());
                };
                if effects.send(WsOutbound::Message { text, data }).is_err() {
                    self.websockets.remove(&ws);
                }
            }
            WorkerFrame::WsCloseOut { ws, code, reason } => {
                let Some(effects) = self.websockets.remove(&ws) else {
                    return Ok(());
                };
                let _ = effects.send(WsOutbound::Close { code, reason });
            }
            WorkerFrame::FetchResult {
                id,
                status,
                headers,
                body_b64,
            } => {
                let body = base64::engine::general_purpose::STANDARD
                    .decode(body_b64)
                    .map_err(|error| GatewayError::Protocol {
                        message: format!("fetch-result body is not base64: {error}"),
                    })?;
                let Some(PendingEvent::Fetch(respond_to)) = self.pending.remove(&id) else {
                    return Err(GatewayError::Protocol {
                        message: format!("fetch-result references unknown fetch event {id}"),
                    });
                };
                let _ = respond_to.send(Ok(FetchResponse {
                    status,
                    headers,
                    body,
                }));
            }
            WorkerFrame::Done { id } => {
                let Some(pending) = self.pending.remove(&id) else {
                    return Err(GatewayError::Protocol {
                        message: format!("done references unknown event {id}"),
                    });
                };
                if !matches!(pending, PendingEvent::Ignore) {
                    return Err(GatewayError::Protocol {
                        message: format!("done references a non-WebSocket event {id}"),
                    });
                }
            }
            WorkerFrame::Error { id, message } => {
                let Some(pending) = self.pending.remove(&id) else {
                    return Err(GatewayError::Protocol {
                        message: format!("error references unknown event {id}"),
                    });
                };
                let error = GatewayError::WorkerError { message };
                match pending {
                    PendingEvent::Fetch(respond_to) => {
                        let _ = respond_to.send(Err(error));
                    }
                    PendingEvent::Ignore => {}
                }
            }
        }
        Ok(())
    }

    /// Allocates the next event identity without allowing wraparound.
    fn next_event(&mut self) -> Result<u64, GatewayError> {
        let id = self.next_event_id;
        self.next_event_id =
            self.next_event_id
                .checked_add(1)
                .ok_or_else(|| GatewayError::Protocol {
                    message: "event identity exhausted".to_owned(),
                })?;
        Ok(id)
    }

    /// Allocates the next WebSocket identity without allowing wraparound.
    fn next_websocket(&mut self) -> Result<u64, GatewayError> {
        let id = self.next_ws_id;
        self.next_ws_id = self
            .next_ws_id
            .checked_add(1)
            .ok_or_else(|| GatewayError::Protocol {
                message: "WebSocket identity exhausted".to_owned(),
            })?;
        Ok(id)
    }

    /// Serializes one frame and appends exactly one NDJSON line.
    async fn write_frame<T: serde::Serialize>(&mut self, frame: &T) -> Result<(), GatewayError> {
        let mut bytes = serde_json::to_vec(frame).map_err(|error| GatewayError::Protocol {
            message: format!("cannot encode gateway frame: {error}"),
        })?;
        bytes.push(b'\n');
        self.writer
            .write_all(&bytes)
            .await
            .map_err(|error| GatewayError::event_io("write", error))
    }

    /// Fails every pending event and closes every effect channel after actor shutdown.
    fn fail_all(&mut self, error: GatewayError) {
        for pending in self.pending.drain().map(|(_, pending)| pending) {
            match pending {
                PendingEvent::Fetch(respond_to) => {
                    let _ = respond_to.send(Err(error.clone()));
                }
                PendingEvent::Ignore => {}
            }
        }
        self.websockets.clear();
    }
}
