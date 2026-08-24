//! Fake-driven Cloudflare ingress, cross-DO call, and guest WebSocket tests.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use verglas_do_wasm::{DoRouter, Request as WorkerRequest, Response as WorkerResponse};
use verglas_gateway::{DoSpawner, Gateway, GatewayError, Manifest, SpawnRequest, WorkerExecutor};

const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// Routes a public Worker fetch through the fake Worker's DO binding call.
#[tokio::test]
async fn public_route_runs_worker_then_do_fetch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let fixture = Fixture::new().await?;
    let path = fixture.socket_path("alice");
    let event = spawn_fetch_server(path.clone(), 200, "from-do", None, None).await?;
    let worker = Arc::new(BindingWorker);
    let gateway = fixture.gateway(worker);
    let address = start_gateway(gateway).await?;

    let response = reqwest::get(format!("http://{address}/public?x=1")).await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await?, "from-do");
    event.await??;
    Ok(())
}

/// Services one DO do-call by spawning and forwarding to its target object.
#[tokio::test]
async fn do_call_is_forwarded_and_answered() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let fixture = Fixture::new().await?;
    let source_path = fixture.socket_path("alice");
    let target_path = fixture.socket_path("bob");
    let target = spawn_fetch_server(target_path, 200, "target", None, None).await?;
    let source = spawn_do_call_server(source_path, "bob", false).await?;
    let gateway = fixture.gateway(Arc::new(BindingWorker));
    let address = start_gateway(gateway).await?;

    let response = reqwest::get(format!("http://{address}/do/COUNTER/alice/source")).await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await?, "source");
    source.await??;
    target.await??;
    Ok(())
}

/// Rejects a DO self-call before opening another serialized connection.
#[tokio::test]
async fn do_call_self_call_has_typed_deadlock_error()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let fixture = Fixture::new().await?;
    let source = spawn_do_call_server(fixture.socket_path("alice"), "alice", true).await?;
    let gateway = fixture.gateway(Arc::new(BindingWorker));
    let address = start_gateway(gateway).await?;

    let response = reqwest::get(format!("http://{address}/do/COUNTER/alice/source")).await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await?, "source");
    source.await??;
    Ok(())
}

/// Completes a public WebSocket only when the DO returns accept_ws.
#[tokio::test]
async fn public_websocket_accept_is_guest_driven()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let fixture = Fixture::new().await?;
    let event =
        spawn_fetch_server(fixture.socket_path("alice"), 101, "", Some(0), Some("ws")).await?;
    let gateway = fixture.gateway(Arc::new(BindingWorker));
    let address = start_gateway(gateway).await?;

    let (mut socket, _) = connect_async(format!("ws://{address}/socket")).await?;
    socket.send(Message::Text("hello".into())).await?;
    assert_eq!(
        socket.next().await.transpose()?,
        Some(Message::Text("echo".into()))
    );
    socket.close(None).await?;
    event.await??;
    Ok(())
}

/// Worker executor fake that performs the binding call expected from a module Worker.
struct BindingWorker;

#[async_trait]
impl WorkerExecutor for BindingWorker {
    /// Calls the configured Durable Object binding and returns its response unchanged.
    async fn fetch(
        &self,
        request: WorkerRequest,
        router: Arc<dyn DoRouter>,
    ) -> Result<WorkerResponse, GatewayError> {
        router
            .do_fetch("COUNTER".to_owned(), "alice".to_owned(), request)
            .await
            .map_err(|error| GatewayError::WorkerError {
                message: error.to_string(),
            })
    }
}

/// Builds one valid generated gateway manifest while keeping Worker fields minimal.
fn manifest(path: &Path) -> Manifest {
    Manifest::parse(&format!(
        r#"{{"name":"counter","main":"worker.js","durable_objects":{{"bindings":[{{"name":"COUNTER","class_name":"Counter"}}]}},"component_digest":"{DIGEST}","component_dir":"components","data_root":"{}"}}"#,
        path.join("state").display()
    ))
    .expect("valid test manifest")
}

/// Isolates Unix sockets and lets each test inject a Worker executor.
struct Fixture {
    directory: TempDir,
    spawner: Arc<TestSpawner>,
}

impl Fixture {
    /// Creates an empty fake spawn table rooted in a temporary directory.
    async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let directory = tempfile::tempdir()?;
        Ok(Self {
            spawner: Arc::new(TestSpawner {
                directory: directory.path().to_path_buf(),
            }),
            directory,
        })
    }

    /// Returns the event socket path for one object name.
    fn socket_path(&self, name: &str) -> PathBuf {
        self.directory.path().join(format!("{name}.sock"))
    }

    /// Builds a gateway with the fake Worker executor and fake celld spawner.
    fn gateway(&self, worker: Arc<dyn WorkerExecutor>) -> Gateway {
        Gateway::with_worker_executor(
            &manifest(self.directory.path()),
            self.directory.path().join("state"),
            Arc::clone(&self.spawner) as Arc<dyn DoSpawner>,
            worker,
        )
    }
}

/// Returns one fake spawn path based on the requested Durable Object name.
struct TestSpawner {
    directory: PathBuf,
}

#[async_trait]
impl DoSpawner for TestSpawner {
    /// Returns the deterministic socket path used by the test's Unix listener.
    async fn spawn(&self, request: SpawnRequest) -> Result<PathBuf, GatewayError> {
        Ok(self.directory.join(format!("{}.sock", request.name())))
    }
}

/// Starts the gateway on an ephemeral local TCP address.
async fn start_gateway(
    gateway: Gateway,
) -> Result<std::net::SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, gateway.router()).await });
    Ok(address)
}

/// Serves one fetch event and optionally accepts a pending WebSocket.
async fn spawn_fetch_server(
    path: PathBuf,
    status: u16,
    body: &str,
    accept_ws: Option<u64>,
    websocket: Option<&str>,
) -> Result<
    tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let listener = UnixListener::bind(&path)?;
    let body = body.to_owned();
    let websocket = websocket.map(str::to_owned);
    Ok(tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let fetch: Value = serde_json::from_str(&lines.next_line().await?.ok_or("missing fetch")?)?;
        assert_eq!(fetch["type"], "fetch");
        if let Some(expected_ws) = accept_ws {
            let ws = if expected_ws == 0 {
                fetch["ws"].as_u64().ok_or("missing pending ws")?
            } else {
                expected_ws
            };
            assert_eq!(fetch["ws"].as_u64(), Some(ws));
            write_half
                .write_all(format!("{}\n", json!({
                    "type": "fetch-result", "id": fetch["id"], "status": status,
                    "headers": [], "body_b64": base64::engine::general_purpose::STANDARD.encode(body),
                    "accept_ws": ws,
                })).as_bytes())
                .await?;
            let open: Value =
                serde_json::from_str(&lines.next_line().await?.ok_or("missing ws-open")?)?;
            assert_eq!(open["type"], "ws-open");
            assert_eq!(open["ws"], ws);
            let message: Value =
                serde_json::from_str(&lines.next_line().await?.ok_or("missing ws-message")?)?;
            assert_eq!(message["type"], "ws-message");
            write_half
                .write_all(
                    format!(
                        "{}\n{}\n",
                        json!({
                            "type": "ws-send", "ws": ws, "text": true,
                            "data_b64": base64::engine::general_purpose::STANDARD.encode("echo"),
                        }),
                        json!({"type": "done", "id": message["id"]})
                    )
                    .as_bytes(),
                )
                .await?;
        } else {
            write_half
                .write_all(format!("{}\n", json!({
                    "type": "fetch-result", "id": fetch["id"], "status": status,
                    "headers": [], "body_b64": base64::engine::general_purpose::STANDARD.encode(body),
                })).as_bytes())
                .await?;
        }
        let _ = websocket;
        Ok(())
    }))
}

/// Serves one source fetch, issues a DO call, and completes the source event.
async fn spawn_do_call_server(
    path: PathBuf,
    target: &str,
    self_call: bool,
) -> Result<
    tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let listener = UnixListener::bind(&path)?;
    let target = target.to_owned();
    Ok(tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let fetch: Value =
            serde_json::from_str(&lines.next_line().await?.ok_or("missing source fetch")?)?;
        let object = target;
        write_half
            .write_all(
                format!(
                    "{}\n",
                    json!({
                        "type": "do-call", "id": 77, "binding": "COUNTER", "object": object,
                        "method": "GET", "url": "/target", "headers": [], "body_b64": "",
                    })
                )
                .as_bytes(),
            )
            .await?;
        let result: Value =
            serde_json::from_str(&lines.next_line().await?.ok_or("missing do-call result")?)?;
        assert_eq!(result["type"], "do-call-result");
        if self_call {
            assert_eq!(result["error"]["code"], "self-call-deadlock");
            assert!(
                result["error"]["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("serialized"))
            );
        } else {
            assert_eq!(result["status"], 200);
            assert_eq!(
                base64::engine::general_purpose::STANDARD
                    .decode(result["body_b64"].as_str().ok_or("missing result body")?)?,
                b"target"
            );
        }
        write_half
            .write_all(format!("{}\n", json!({
                "type": "fetch-result", "id": fetch["id"], "status": 200,
                "headers": [], "body_b64": base64::engine::general_purpose::STANDARD.encode("source"),
            })).as_bytes())
            .await?;
        Ok(())
    }))
}
