//! Protocol acceptance tests for the resident NDJSON Worker event endpoint.
//!
//! These tests use the endpoint's scripted `EventDispatcher` seam because hand-written
//! canonical WIT handler exports are not practical in this checkout. Storage, gate,
//! Unix framing, commit ordering, failure rollback, and event serialization are real.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, TransactionEnvelope, WorkerStateView,
};
use verglas_do_wasm::{
    EventGate, EventPermit, HostError, PendingEvent, Request, Response, RuntimeError,
    WorkerSockets, WorkerStorage,
};
use verglas_runtime::{EventDispatcher, EventEndpoint};

/// Deterministic commit authority for endpoint ordering assertions.
#[derive(Default)]
struct CountingAuthority {
    /// Next commit sequence.
    sequence: AtomicU64,
}

#[async_trait]
impl CommitAuthority for CountingAuthority {
    /// Assigns the next contiguous sequence to each committed event envelope.
    async fn commit(
        &self,
        envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(CommitReceipt::new(sequence, envelope.transaction_id()))
    }
}

/// Scripted event handler used only to exercise the real endpoint protocol seam.
#[derive(Default)]
struct ScriptedDispatcher {
    /// Records event order inside the scripted handler.
    order: Mutex<Vec<String>>,
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
    /// Performs no initialization work while proving the endpoint's init commit path.
    async fn dispatch_init(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
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
        request: Request,
    ) -> Result<PendingEvent<Response>, RuntimeError> {
        let (permit, _staged_sockets) = Self::begin(gate, sockets).await;
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
        self.order.lock().await.push(request.uri.clone());
        Ok(PendingEvent {
            outcome: Response {
                status: 200,
                headers: vec![("content-type".to_owned(), "text/plain".to_owned())],
                body: request.uri.into_bytes(),
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
        socket: u64,
        message: Vec<u8>,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        let (permit, staged_sockets) = Self::begin(gate, sockets).await;
        let text = String::from_utf8(message.clone()).map_err(|error| RuntimeError::Handler {
            message: error.to_string(),
        })?;
        self.order.lock().await.push(text.clone());
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

    /// Runs no alarm behavior in this endpoint framing test.
    async fn dispatch_alarm(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
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
) -> (
    Arc<DoEngine>,
    tokio::task::JoinHandle<Result<(), verglas_runtime::EventEndpointError>>,
) {
    let engine = Arc::new(DoEngine::new(
        "endpoint-test",
        Arc::new(CountingAuthority::default()),
    ));
    let path = directory.path().join("events.sock");
    let mut endpoint = EventEndpoint::bind(&path, Arc::clone(&engine), dispatcher)
        .await
        .expect("bind event endpoint");
    let task = tokio::spawn(async move { endpoint.run().await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !path.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "event socket did not bind"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    (engine, task)
}

/// Starts the endpoint with the ordinary scripted dispatcher.
async fn start_endpoint(
    directory: &tempfile::TempDir,
) -> (
    Arc<DoEngine>,
    tokio::task::JoinHandle<Result<(), verglas_runtime::EventEndpointError>>,
) {
    start_endpoint_with_dispatcher(directory, Arc::new(ScriptedDispatcher::default())).await
}

/// Opens the endpoint and returns a line reader plus writable stream halves.
async fn connect(
    directory: &tempfile::TempDir,
) -> (
    tokio::io::WriteHalf<UnixStream>,
    BufReader<tokio::io::ReadHalf<UnixStream>>,
) {
    let stream = UnixStream::connect(directory.path().join("events.sock"))
        .await
        .expect("connect event socket");
    let (read, write) = tokio::io::split(stream);
    (write, BufReader::new(read))
}

/// Sends an input frame that intentionally has no protocol response.
async fn send_frame(writer: &mut tokio::io::WriteHalf<UnixStream>, frame: Value) {
    writer
        .write_all(format!("{}\n", frame).as_bytes())
        .await
        .expect("write event frame");
}

/// Sends a JSON input frame and reads one JSON output frame.
async fn round_trip(
    writer: &mut tokio::io::WriteHalf<UnixStream>,
    reader: &mut BufReader<tokio::io::ReadHalf<UnixStream>>,
    frame: Value,
) -> Value {
    writer
        .write_all(format!("{}\n", frame).as_bytes())
        .await
        .expect("write event frame");
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read event frame");
    serde_json::from_str(line.trim()).expect("decode event frame")
}

/// Fetches return fetch-result frames over a real Unix socket.
#[tokio::test]
async fn fetch_returns_fetch_result() {
    let directory = tempfile::tempdir().expect("endpoint directory");
    let (_engine, task) = start_endpoint(&directory).await;
    let (mut writer, mut reader) = connect(&directory).await;
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
    .await;
    assert_eq!(result["type"], "fetch-result");
    assert_eq!(result["id"], 1);
    assert_eq!(result["status"], 200);
    task.abort();
}

/// Successful WebSocket effects precede done, while failed handlers emit no send.
#[tokio::test]
async fn websocket_effects_commit_before_done_and_failures_release_nothing() {
    let directory = tempfile::tempdir().expect("endpoint directory");
    let (engine, task) = start_endpoint(&directory).await;
    let (mut writer, mut reader) = connect(&directory).await;
    send_frame(&mut writer, json!({ "type": "ws-open", "ws": 7 })).await;
    writer
        .write_all(
            format!(
                "{}\n",
                json!({
                    "type": "ws-message",
                    "id": 2,
                    "ws": 7,
                    "text": true,
                    "data_b64": "b2s="
                })
            )
            .as_bytes(),
        )
        .await
        .expect("write websocket message");
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read send");
    let send: Value = serde_json::from_str(line.trim()).expect("decode send");
    assert_eq!(send["type"], "ws-send");
    line.clear();
    reader.read_line(&mut line).await.expect("read done");
    let done: Value = serde_json::from_str(line.trim()).expect("decode done");
    assert_eq!(done, json!({ "type": "done", "id": 2 }));
    assert_eq!(
        WorkerStateView::new(engine.as_ref())
            .kv_get("message:ok")
            .await
            .expect("committed marker"),
        Some(b"ok".to_vec())
    );

    let failed = round_trip(
        &mut writer,
        &mut reader,
        json!({
            "type": "ws-message",
            "id": 3,
            "ws": 7,
            "text": true,
            "data_b64": "ZmFpbA=="
        }),
    )
    .await;
    assert_eq!(failed["type"], "error");
    assert_eq!(failed["id"], 3);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut line))
            .await
            .is_err(),
        "failed handler emitted an unexpected staged frame"
    );
    assert_eq!(
        WorkerStateView::new(engine.as_ref())
            .kv_get("message:fail")
            .await
            .expect("failed marker read"),
        None
    );
    task.abort();
}

/// Pipelined events retain strict event order and never interleave effects.
#[tokio::test]
async fn pipelined_events_are_serialized() {
    let directory = tempfile::tempdir().expect("endpoint directory");
    let (_engine, task) = start_endpoint(&directory).await;
    let (mut writer, mut reader) = connect(&directory).await;
    send_frame(&mut writer, json!({ "type": "ws-open", "ws": 7 })).await;
    let first = json!({
        "type": "ws-message", "id": 10, "ws": 7, "text": true, "data_b64": "b25l"
    });
    let second = json!({
        "type": "ws-message", "id": 11, "ws": 7, "text": true, "data_b64": "dHdv"
    });
    writer
        .write_all(format!("{first}\n{second}\n").as_bytes())
        .await
        .expect("write pipelined frames");
    let mut values = Vec::new();
    for _ in 0..4 {
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .expect("read pipeline frame");
        values.push(serde_json::from_str::<Value>(line.trim()).expect("decode pipeline frame"));
    }
    assert_eq!(values[0]["type"], "ws-send");
    assert_eq!(values[0]["data_b64"], "b25l");
    assert_eq!(values[1], json!({ "type": "done", "id": 10 }));
    assert_eq!(values[2]["type"], "ws-send");
    assert_eq!(values[2]["data_b64"], "dHdv");
    assert_eq!(values[3], json!({ "type": "done", "id": 11 }));
    task.abort();
}

/// A committed short alarm deadline self-delivers through the event gate.
#[tokio::test]
async fn alarm_deadline_self_delivers_and_clears() {
    let directory = tempfile::tempdir().expect("endpoint directory");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_millis();
    let deadline = u64::try_from(now).expect("millis") + 30;
    let calls = Arc::new(AtomicU64::new(0));
    let dispatcher = Arc::new(ScriptedDispatcher {
        order: Mutex::new(Vec::new()),
        alarm_deadline: Some(deadline),
        alarm_calls: Some(Arc::clone(&calls)),
    });
    let (engine, task) = start_endpoint_with_dispatcher(&directory, dispatcher).await;
    let (_writer, _reader) = connect(&directory).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        WorkerStateView::new(engine.as_ref())
            .alarm()
            .await
            .expect("alarm state"),
        None
    );
    task.abort();
}
