//! Protocol acceptance tests for the resident NDJSON Worker event endpoint.
//!
//! These tests use the endpoint's scripted `EventDispatcher` seam because
//! hand-written canonical WIT handler exports are not practical here. Turso
//! transactions, output gating, Unix framing, alarms, do-call routing, and
//! event serialization remain real.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use verglas_do_turso::{OutboxRecord, StreamAppender, TursoStore};
use verglas_do_wasm::{
    EventGate, EventPermit, HostError, PendingEvent, Request, Response, RuntimeError, SocketId,
    WorkerBindings, WorkerSockets, WorkerStorage,
};
use verglas_runtime::{EventDispatcher, EventEndpoint};

/// Test appender that records successful delivery attempts without a Stream fallback.
#[derive(Default)]
struct CountingAppender {
    /// Number of batches presented for durable acknowledgement.
    calls: AtomicU64,
}

#[async_trait]
impl StreamAppender for CountingAppender {
    /// Records one append attempt and acknowledges it.
    async fn append(&self, _records: Vec<OutboxRecord>) -> verglas_do_turso::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Test appender that fails before the Stream acknowledgement boundary.
struct FailingAppender;

#[async_trait]
impl StreamAppender for FailingAppender {
    /// Rejects every append so the event remains unpublished and gated.
    async fn append(&self, _records: Vec<OutboxRecord>) -> verglas_do_turso::Result<()> {
        Err(verglas_do_turso::Error::StreamAppend(
            "test Stream unavailable".to_owned(),
        ))
    }
}

/// Scripted event handler used only to exercise the real endpoint protocol seam.
#[derive(Default)]
struct ScriptedDispatcher {
    /// Optional startup alarm used by the self-delivery acceptance test.
    alarm_deadline: Option<u64>,
    /// Counts alarm handler invocations when alarm mode is enabled.
    alarm_calls: Option<Arc<AtomicU64>>,
}

impl ScriptedDispatcher {
    /// Converts a host failure into the runtime's handler error shape.
    fn host_failure(error: HostError) -> RuntimeError {
        RuntimeError::Handler {
            message: error.to_string(),
        }
    }

    /// Creates one event permit and the staged socket view it owns.
    async fn begin(
        gate: &EventGate,
        sockets: Arc<dyn WorkerSockets>,
    ) -> (EventPermit, Arc<dyn WorkerSockets>) {
        let permit = gate.begin_event().await;
        let staged = permit.staging_sockets(sockets);
        (permit, staged)
    }
}

#[async_trait]
impl EventDispatcher for ScriptedDispatcher {
    /// Performs optional initialization work while proving the init commit path.
    async fn dispatch_init(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        _bindings: Arc<dyn WorkerBindings>,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        let (permit, _staged_sockets) = Self::begin(gate, sockets).await;
        if let Some(deadline) = self.alarm_deadline {
            storage
                .set_alarm(deadline)
                .await
                .map_err(Self::host_failure)?;
        }
        Ok(PendingEvent {
            outcome: (),
            permit,
        })
    }

    /// Handles fetches and stages a durable marker for successful requests.
    async fn dispatch_fetch(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        _bindings: Arc<dyn WorkerBindings>,
        request: Request,
    ) -> Result<PendingEvent<Response>, RuntimeError> {
        let (permit, staged_sockets) = Self::begin(gate, sockets).await;
        if request.uri == "/publish" || request.uri == "/publish-fail" {
            storage
                .stream_send(
                    "STREAM".to_owned(),
                    "stream-id".to_owned(),
                    r#"[{"value":1}]"#.to_owned(),
                )
                .await
                .map_err(Self::host_failure)?;
            staged_sockets
                .send(7, b"published".to_vec())
                .await
                .map_err(Self::host_failure)?;
            if request.uri == "/publish-fail" {
                permit.abort();
                return Err(RuntimeError::Handler {
                    message: "scripted publication failure".to_owned(),
                });
            }
        }
        if request.uri == "/fail" {
            storage
                .put("fetch-failure".to_owned(), b"must-not-commit".to_vec())
                .await
                .map_err(Self::host_failure)?;
            permit.abort();
            return Err(RuntimeError::Handler {
                message: "scripted fetch failure".to_owned(),
            });
        }
        storage
            .put("fetch-success".to_owned(), request.body)
            .await
            .map_err(Self::host_failure)?;
        Ok(PendingEvent {
            outcome: Response {
                status: 200,
                headers: vec![("content-type".to_owned(), "text/plain".to_owned())],
                body: request.uri.clone().into_bytes(),
                accept_ws: (request.uri == "/accept-ws")
                    .then_some(request.ws)
                    .flatten(),
            },
            permit,
        })
    }

    /// Stages one message and deliberately fails a named message after staging output.
    async fn dispatch_websocket_message(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        _bindings: Arc<dyn WorkerBindings>,
        socket: u64,
        message: Vec<u8>,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        let (permit, staged_sockets) = Self::begin(gate, sockets).await;
        let text = String::from_utf8(message.clone()).map_err(|error| RuntimeError::Handler {
            message: error.to_string(),
        })?;
        storage
            .put(format!("message:{text}"), message.clone())
            .await
            .map_err(Self::host_failure)?;
        staged_sockets
            .send(socket, message)
            .await
            .map_err(Self::host_failure)?;
        if text == "fail" {
            permit.abort();
            return Err(RuntimeError::Handler {
                message: "scripted message failure".to_owned(),
            });
        }
        Ok(PendingEvent {
            outcome: (),
            permit,
        })
    }

    /// Handles the alarm and clears it when alarm mode is enabled.
    async fn dispatch_alarm(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        _bindings: Arc<dyn WorkerBindings>,
        _scheduled_millis: u64,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        let (permit, _staged_sockets) = Self::begin(gate, sockets).await;
        if let Some(calls) = &self.alarm_calls {
            calls.fetch_add(1, Ordering::SeqCst);
            storage.delete_alarm().await.map_err(Self::host_failure)?;
        }
        Ok(PendingEvent {
            outcome: (),
            permit,
        })
    }

    /// Runs no close behavior in this endpoint framing test.
    async fn dispatch_websocket_close(
        &self,
        gate: &EventGate,
        _storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        _bindings: Arc<dyn WorkerBindings>,
        _socket: u64,
        _code: u16,
        _reason: String,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        let (permit, _staged_sockets) = Self::begin(gate, sockets).await;
        Ok(PendingEvent {
            outcome: (),
            permit,
        })
    }
}

/// Starts a real Unix event endpoint with the scripted handler seam.
async fn start_endpoint_with_dispatcher(
    directory: &tempfile::TempDir,
    dispatcher: Arc<dyn EventDispatcher>,
) -> Result<
    (
        Arc<TursoStore>,
        tokio::task::JoinHandle<Result<(), verglas_runtime::EventEndpointError>>,
    ),
    Box<dyn Error>,
> {
    let store =
        Arc::new(TursoStore::open(directory.path().join("worker.db"), "endpoint-test").await?);
    let path = directory.path().join("events.sock");
    let mut endpoint = EventEndpoint::bind(&path, Arc::clone(&store), dispatcher).await?;
    let task = tokio::spawn(async move { endpoint.run().await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !path.exists() {
        if tokio::time::Instant::now() >= deadline {
            task.abort();
            return Err("event socket did not bind".into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok((store, task))
}

/// Starts the endpoint with the ordinary scripted dispatcher.
async fn start_endpoint(
    directory: &tempfile::TempDir,
) -> Result<
    (
        Arc<TursoStore>,
        tokio::task::JoinHandle<Result<(), verglas_runtime::EventEndpointError>>,
    ),
    Box<dyn Error>,
> {
    start_endpoint_with_dispatcher(directory, Arc::new(ScriptedDispatcher::default())).await
}

/// Starts an endpoint with an explicit Stream appender failure or probe.
async fn start_endpoint_with_appender(
    directory: &tempfile::TempDir,
    appender: Arc<dyn StreamAppender>,
) -> Result<
    (
        Arc<TursoStore>,
        tokio::task::JoinHandle<Result<(), verglas_runtime::EventEndpointError>>,
    ),
    Box<dyn Error>,
> {
    let store =
        Arc::new(TursoStore::open(directory.path().join("worker.db"), "endpoint-test").await?);
    store.set_stream_appender(appender).await;
    let path = directory.path().join("events.sock");
    let mut endpoint = EventEndpoint::bind(
        &path,
        Arc::clone(&store),
        Arc::new(ScriptedDispatcher::default()),
    )
    .await?;
    let task = tokio::spawn(async move { endpoint.run().await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !path.exists() {
        if tokio::time::Instant::now() >= deadline {
            task.abort();
            return Err("event socket did not bind".into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok((store, task))
}

/// Opens the endpoint and returns writable and readable stream halves.
async fn connect(
    directory: &tempfile::TempDir,
) -> Result<
    (
        tokio::io::WriteHalf<UnixStream>,
        BufReader<tokio::io::ReadHalf<UnixStream>>,
    ),
    Box<dyn Error>,
> {
    let stream = UnixStream::connect(directory.path().join("events.sock")).await?;
    let (read, write) = tokio::io::split(stream);
    Ok((write, BufReader::new(read)))
}

/// Sends an input frame that intentionally has no protocol response.
async fn send_frame(
    writer: &mut tokio::io::WriteHalf<UnixStream>,
    frame: Value,
) -> Result<(), Box<dyn Error>> {
    writer.write_all(format!("{frame}\n").as_bytes()).await?;
    Ok(())
}

/// Sends a JSON input frame and reads one JSON output frame.
async fn round_trip(
    writer: &mut tokio::io::WriteHalf<UnixStream>,
    reader: &mut BufReader<tokio::io::ReadHalf<UnixStream>>,
    frame: Value,
) -> Result<Value, Box<dyn Error>> {
    writer.write_all(format!("{frame}\n").as_bytes()).await?;
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    Ok(serde_json::from_str(line.trim())?)
}

/// Fetches return fetch-result frames over a real Unix socket.
#[tokio::test]
async fn fetch_returns_fetch_result() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let (_store, task) = start_endpoint(&directory).await?;
    let (mut writer, mut reader) = connect(&directory).await?;
    let result = round_trip(
        &mut writer,
        &mut reader,
        json!({
            "type": "fetch",
            "id": 1,
            "method": "POST",
            "url": "/counter/incr",
            "headers": [],
            "body_b64": "aGk="
        }),
    )
    .await?;
    assert_eq!(result["type"], "fetch-result");
    assert_eq!(result["id"], 1);
    assert_eq!(result["status"], 200);
    task.abort();
    Ok(())
}

/// Guest acceptance is carried on fetch-result, while ordinary responses omit it.
#[tokio::test]
async fn fetch_upgrade_acceptance_is_forwarded_without_local_state() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let (_store, task) = start_endpoint(&directory).await?;
    let (mut writer, mut reader) = connect(&directory).await?;
    let accepted = round_trip(
        &mut writer,
        &mut reader,
        json!({
            "type": "fetch", "id": 2, "method": "GET", "url": "/accept-ws",
            "headers": [], "body_b64": "", "ws": 17
        }),
    )
    .await?;
    assert_eq!(accepted["accept_ws"], 17);
    let ordinary = round_trip(
        &mut writer,
        &mut reader,
        json!({
            "type": "fetch", "id": 3, "method": "GET", "url": "/ordinary",
            "headers": [], "body_b64": "", "ws": 18
        }),
    )
    .await?;
    assert!(ordinary.get("accept_ws").is_none());
    task.abort();
    Ok(())
}

/// Successful WebSocket effects follow Turso commit, while failed handlers emit none.
#[tokio::test]
async fn websocket_effects_follow_turso_commit_and_failures_rollback() -> Result<(), Box<dyn Error>>
{
    let directory = tempfile::tempdir()?;
    let (store, task) = start_endpoint(&directory).await?;
    let (mut writer, mut reader) = connect(&directory).await?;
    send_frame(&mut writer, json!({ "type": "ws-open", "ws": 7 })).await?;
    writer
        .write_all(
            format!(
                "{}\n",
                json!({
                    "type": "ws-message", "id": 2, "ws": 7, "text": true, "data_b64": "b2s="
                })
            )
            .as_bytes(),
        )
        .await?;
    let send = read_json(&mut reader).await?;
    assert_eq!(send["type"], "ws-send");
    let committed = store.begin_event().await?;
    assert_eq!(committed.get_kv("message:ok").await?, Some(b"ok".to_vec()));
    committed.rollback().await?;
    let done = read_json(&mut reader).await?;
    assert_eq!(done, json!({ "type": "done", "id": 2 }));

    let failed = round_trip(
        &mut writer,
        &mut reader,
        json!({
            "type": "ws-message", "id": 3, "ws": 7, "text": true, "data_b64": "ZmFpbA=="
        }),
    )
    .await?;
    assert_eq!(failed["type"], "error");
    assert_eq!(failed["id"], 3);
    let failed_view = store.begin_event().await?;
    assert_eq!(failed_view.get_kv("message:fail").await?, None);
    failed_view.rollback().await?;
    task.abort();
    Ok(())
}

/// Handler failure before source commit releases neither Stream append nor socket output.
#[tokio::test]
async fn publication_before_commit_has_no_append_or_effect() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let appender = Arc::new(CountingAppender::default());
    let (store, task) = start_endpoint_with_appender(&directory, appender.clone()).await?;
    let (mut writer, mut reader) = connect(&directory).await?;
    send_frame(&mut writer, json!({ "type": "ws-open", "ws": 7 })).await?;
    let failure = round_trip(
        &mut writer,
        &mut reader,
        json!({
            "type": "fetch", "id": 30, "method": "POST", "url": "/publish-fail",
            "headers": [], "body_b64": ""
        }),
    )
    .await?;
    assert_eq!(failure["type"], "error");
    assert_eq!(appender.calls.load(Ordering::SeqCst), 0);
    let event = store.begin_event().await?;
    event.rollback().await?;
    assert!(store.pending_outbox(10).await?.is_empty());
    task.abort();
    Ok(())
}

/// A live endpoint injects the real binding appender and releases effects only after its ACK.
#[tokio::test]
async fn runtime_appender_routes_stream_ack_before_effects() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let (_store, task) = start_endpoint(&directory).await?;
    let (mut writer, mut reader) = connect(&directory).await?;
    send_frame(&mut writer, json!({ "type": "ws-open", "ws": 7 })).await?;
    writer
        .write_all(
            format!(
                "{}\n",
                json!({
                    "type": "fetch", "id": 33, "method": "POST", "url": "/publish",
                    "headers": [], "body_b64": ""
                })
            )
            .as_bytes(),
        )
        .await?;

    let append = read_json(&mut reader).await?;
    assert_eq!(append["type"], "do-call");
    assert_eq!(append["binding"], "STREAM");
    assert_eq!(append["object"], "stream-id");
    assert_eq!(append["method"], "POST");
    assert_eq!(append["url"], "https://verglas.internal/stream/append");
    assert_eq!(append["body_b64"], "W3sidmFsdWUiOjF9XQ==");
    let headers = append["headers"]
        .as_array()
        .ok_or("append headers were not an array")?;
    let identity = headers.iter().find_map(|header| {
        let values = header.as_array()?;
        if values.first()?.as_str()? == "x-verglas-producer-event-id" {
            values.get(1)?.as_str().map(str::to_owned)
        } else {
            None
        }
    });
    assert_eq!(identity.as_deref(), Some("[\"endpoint-test:2:0\"]"));

    let withheld = tokio::time::timeout(Duration::from_millis(25), read_json(&mut reader)).await;
    assert!(withheld.is_err(), "effects escaped before the Stream ACK");
    writer
        .write_all(
            format!(
                "{}\n",
                json!({
                    "type": "do-call-result", "id": append["id"], "status": 202,
                    "headers": [], "body_b64": ""
                })
            )
            .as_bytes(),
        )
        .await?;
    let effect = read_json(&mut reader).await?;
    assert_eq!(effect["type"], "ws-send");
    let result = read_json(&mut reader).await?;
    assert_eq!(result["type"], "fetch-result");
    assert_eq!(result["id"], 33);
    task.abort();
    Ok(())
}

/// A failed Stream acknowledgement keeps effects gated and blocks the next event.
#[tokio::test]
async fn publication_ack_failure_blocks_effects_and_next_event() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let (store, task) = start_endpoint_with_appender(&directory, Arc::new(FailingAppender)).await?;
    let (mut writer, mut reader) = connect(&directory).await?;
    send_frame(&mut writer, json!({ "type": "ws-open", "ws": 7 })).await?;
    let failure = round_trip(
        &mut writer,
        &mut reader,
        json!({
            "type": "fetch", "id": 31, "method": "POST", "url": "/publish",
            "headers": [], "body_b64": ""
        }),
    )
    .await?;
    assert_eq!(failure["type"], "error");
    let next = round_trip(
        &mut writer,
        &mut reader,
        json!({
            "type": "fetch", "id": 32, "method": "GET", "url": "/next",
            "headers": [], "body_b64": ""
        }),
    )
    .await?;
    assert_eq!(next["type"], "error");
    assert!(matches!(
        store.begin_event().await,
        Err(verglas_do_turso::Error::OutboxInFlight)
    ));
    task.abort();
    Ok(())
}

/// Pipelined events retain strict event order and never interleave effects.
#[tokio::test]
async fn pipelined_events_are_serialized() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let (_store, task) = start_endpoint(&directory).await?;
    let (mut writer, mut reader) = connect(&directory).await?;
    send_frame(&mut writer, json!({ "type": "ws-open", "ws": 7 })).await?;
    let first = json!({
        "type": "ws-message", "id": 10, "ws": 7, "text": true, "data_b64": "b25l"
    });
    let second = json!({
        "type": "ws-message", "id": 11, "ws": 7, "text": true, "data_b64": "dHdv"
    });
    writer
        .write_all(format!("{first}\n{second}\n").as_bytes())
        .await?;
    let mut values = Vec::new();
    for _ in 0..4 {
        values.push(read_json(&mut reader).await?);
    }
    assert_eq!(values[0]["type"], "ws-send");
    assert_eq!(values[0]["data_b64"], "b25l");
    assert_eq!(values[1], json!({ "type": "done", "id": 10 }));
    assert_eq!(values[2]["type"], "ws-send");
    assert_eq!(values[2]["data_b64"], "dHdv");
    assert_eq!(values[3], json!({ "type": "done", "id": 11 }));
    task.abort();
    Ok(())
}

/// A committed short alarm deadline self-delivers through the event gate.
#[tokio::test]
async fn alarm_deadline_self_delivers_and_clears() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    let deadline = u64::try_from(now.as_millis())? + 30;
    let calls = Arc::new(AtomicU64::new(0));
    let dispatcher = Arc::new(ScriptedDispatcher {
        alarm_deadline: Some(deadline),
        alarm_calls: Some(Arc::clone(&calls)),
    });
    let (store, task) = start_endpoint_with_dispatcher(&directory, dispatcher).await?;
    let (_writer, _reader) = connect(&directory).await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.alarm().await?, None);
    task.abort();
    Ok(())
}

/// Dispatcher that performs one cross-Durable-Object call during fetch.
struct DoCallDispatcher;

impl DoCallDispatcher {
    /// Creates a successful pending event without host effects.
    async fn empty_event(gate: &EventGate) -> PendingEvent<()> {
        PendingEvent {
            outcome: (),
            permit: gate.begin_event().await,
        }
    }

    /// Converts a failed binding call into the guest handler error shape.
    fn binding_failure(error: HostError) -> RuntimeError {
        RuntimeError::Handler {
            message: error.to_string(),
        }
    }
}

#[async_trait]
impl EventDispatcher for DoCallDispatcher {
    /// Runs an empty initialization event.
    async fn dispatch_init(
        &self,
        gate: &EventGate,
        _storage: Arc<dyn WorkerStorage>,
        _sockets: Arc<dyn WorkerSockets>,
        _bindings: Arc<dyn WorkerBindings>,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        Ok(Self::empty_event(gate).await)
    }

    /// Calls the gateway binding router and returns its response to the handler.
    async fn dispatch_fetch(
        &self,
        gate: &EventGate,
        _storage: Arc<dyn WorkerStorage>,
        _sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
        _request: Request,
    ) -> Result<PendingEvent<Response>, RuntimeError> {
        let permit = gate.begin_event().await;
        let target = Request {
            method: "GET".to_owned(),
            uri: "/target".to_owned(),
            headers: vec![("x-call".to_owned(), "yes".to_owned())],
            body: b"call-body".to_vec(),
            ws: None,
        };
        let response = bindings
            .do_fetch("COUNTER".to_owned(), "alice".to_owned(), target)
            .await
            .map_err(Self::binding_failure)?;
        Ok(PendingEvent {
            outcome: response,
            permit,
        })
    }

    /// Runs an empty alarm event because this dispatcher only tests fetch calls.
    async fn dispatch_alarm(
        &self,
        gate: &EventGate,
        _storage: Arc<dyn WorkerStorage>,
        _sockets: Arc<dyn WorkerSockets>,
        _bindings: Arc<dyn WorkerBindings>,
        _scheduled_millis: u64,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        Ok(Self::empty_event(gate).await)
    }

    /// Runs an empty WebSocket message event because this dispatcher only tests fetch calls.
    async fn dispatch_websocket_message(
        &self,
        gate: &EventGate,
        _storage: Arc<dyn WorkerStorage>,
        _sockets: Arc<dyn WorkerSockets>,
        _bindings: Arc<dyn WorkerBindings>,
        _socket: SocketId,
        _message: Vec<u8>,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        Ok(Self::empty_event(gate).await)
    }

    /// Runs an empty WebSocket close event because this dispatcher only tests fetch calls.
    async fn dispatch_websocket_close(
        &self,
        gate: &EventGate,
        _storage: Arc<dyn WorkerStorage>,
        _sockets: Arc<dyn WorkerSockets>,
        _bindings: Arc<dyn WorkerBindings>,
        _socket: SocketId,
        _code: u16,
        _reason: String,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        Ok(Self::empty_event(gate).await)
    }
}

/// A scripted peer can answer a DO call while the fetch handler is suspended.
#[tokio::test]
async fn do_call_result_reaches_handler_before_fetch_result() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let (_store, task) =
        start_endpoint_with_dispatcher(&directory, Arc::new(DoCallDispatcher)).await?;
    let (mut writer, mut reader) = connect(&directory).await?;
    send_frame(
        &mut writer,
        json!({
            "type": "fetch", "id": 20, "method": "GET", "url": "/outer",
            "headers": [], "body_b64": ""
        }),
    )
    .await?;
    let call = read_json(&mut reader).await?;
    assert_eq!(call["type"], "do-call");
    assert_eq!(call["id"], 1);
    assert_eq!(call["binding"], "COUNTER");
    writer
        .write_all(
            format!(
                "{}\n",
                json!({
                    "type": "do-call-result", "id": 1, "status": 201,
                    "headers": [["x-target", "ok"]], "body_b64": "ZnJvbS10YXJnZXQ="
                })
            )
            .as_bytes(),
        )
        .await?;
    let result = read_json(&mut reader).await?;
    assert_eq!(result["type"], "fetch-result");
    assert_eq!(result["id"], 20);
    assert_eq!(result["status"], 201);
    assert_eq!(result["body_b64"], "ZnJvbS10YXJnZXQ=");
    task.abort();
    Ok(())
}

/// A gateway error response becomes a handler error and never a fetch result.
#[tokio::test]
async fn do_call_gateway_error_reaches_handler_as_error() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let (_store, task) =
        start_endpoint_with_dispatcher(&directory, Arc::new(DoCallDispatcher)).await?;
    let (mut writer, mut reader) = connect(&directory).await?;
    send_frame(
        &mut writer,
        json!({
            "type": "fetch", "id": 21, "method": "GET", "url": "/outer",
            "headers": [], "body_b64": ""
        }),
    )
    .await?;
    let call = read_json(&mut reader).await?;
    writer
        .write_all(
            format!(
                "{}\n",
                json!({
                    "type": "do-call-result", "id": call["id"],
                    "error": {"code": "target-error", "message": "target failed"}
                })
            )
            .as_bytes(),
        )
        .await?;
    let result = read_json(&mut reader).await?;
    assert_eq!(result["type"], "error");
    assert_eq!(result["id"], 21);
    assert!(result["message"].as_str().is_some_and(|message| {
        message.contains("target-error") || message.contains("target failed")
    }));
    task.abort();
    Ok(())
}

/// A result for an id with no pending call is a protocol failure.
#[tokio::test]
async fn unknown_do_call_result_id_is_protocol_error() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let (_store, task) =
        start_endpoint_with_dispatcher(&directory, Arc::new(DoCallDispatcher)).await?;
    let (mut writer, _reader) = connect(&directory).await?;
    send_frame(
        &mut writer,
        json!({
            "type": "do-call-result", "id": 999, "status": 200,
            "headers": [], "body_b64": ""
        }),
    )
    .await?;
    let outcome = tokio::time::timeout(Duration::from_secs(1), task).await??;
    let error = outcome.expect_err("unknown call id must fail");
    assert!(error.to_string().contains("unknown do-call"));
    Ok(())
}

/// Reads one JSON frame from a connected event peer.
async fn read_json(
    reader: &mut BufReader<tokio::io::ReadHalf<UnixStream>>,
) -> Result<Value, Box<dyn Error>> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    Ok(serde_json::from_str(line.trim())?)
}
