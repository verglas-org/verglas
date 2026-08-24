//! HTTP and WebSocket protocol tests against fake celld and verglasd sockets.

use base64::Engine;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use verglas_gateway::{CelldSpawner, DoSpawner, Gateway, GatewayError, Manifest, SpawnRequest};

const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// Sends the fresh replica and component-bearing worker commands before event bind.
#[tokio::test]
async fn celld_spawner_sends_exact_commands_and_retries_event_bind()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let data_root = directory.path().join("state");
    let control_path = directory.path().join("celld.sock");
    let event_path = data_root.join("COUNTER--alice").join("events.sock");
    let replica_path = directory.path().join("replica.sock");
    let worker_path = directory.path().join("worker.sock");
    let replica_listener = UnixListener::bind(&replica_path)?;
    let replica_task = tokio::spawn(async move {
        let (stream, _) = replica_listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let status = lines.next_line().await?.ok_or("missing replica status")?;
        assert_eq!(status, "STATUS");
        write_half.write_all(b"OK replica 7 0 0\n").await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let manifest = Manifest::parse(&format!(
        "{{\"name\":\"counter\",\"main\":\"src/index.ts\",\"durable_objects\":{{\"bindings\":[{{\"name\":\"COUNTER\",\"class_name\":\"Counter\"}}]}},\"component_digest\":\"{DIGEST}\",\"component_dir\":\"components\",\"data_root\":\"state\"}}"
    ))?;
    let control_listener = UnixListener::bind(&control_path)?;
    let control_event_path = event_path.clone();
    let control_task = tokio::spawn(async move {
        let (stream, _) = control_listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let replica_command = lines.next_line().await?.ok_or("missing replica command")?;
        assert_eq!(replica_command, "SPAWN COUNTER--alice 1 follower 0");
        write_half
            .write_all(format!("OK {}\n", replica_path.display()).as_bytes())
            .await?;
        let (stream, _) = control_listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let worker_command = lines.next_line().await?.ok_or("missing worker command")?;
        let token = hex::encode("lease-COUNTER--alice");
        assert_eq!(
            worker_command,
            format!(
                "SPAWN_WORKER COUNTER--alice 1 7 {} {} 11 7 - {DIGEST} components {}",
                replica_path.display(),
                token,
                control_event_path.display()
            )
        );
        write_half
            .write_all(format!("OK {}\n", worker_path.display()).as_bytes())
            .await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let delayed_event_path = event_path.clone();
    let event_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let parent = delayed_event_path.parent().ok_or("event parent")?;
        tokio::fs::create_dir_all(parent).await?;
        let listener = UnixListener::bind(&delayed_event_path)?;
        let (stream, _) = listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let frame: Value = serde_json::from_str(&lines.next_line().await?.ok_or("missing fetch")?)?;
        assert_eq!(frame["type"], "fetch");
        write_half
            .write_all(
                format!(
                    "{}\n",
                    json!({"type": "fetch-result", "id": 1, "status": 200, "headers": [], "body_b64": base64::engine::general_purpose::STANDARD.encode("ok")})
                )
                .as_bytes(),
            )
            .await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let gateway = Gateway::new(&manifest, &control_path, &data_root);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, gateway.router()).await });
    let started = Instant::now();
    let response = reqwest::get(format!("http://{address}/do/COUNTER/alice/value")).await?;
    assert!(started.elapsed() >= std::time::Duration::from_millis(30));
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await?, "ok");
    control_task.await??;
    replica_task.await??;
    event_task.await??;
    server.abort();
    let _ = server.await;
    Ok(())
}

/// Sends the exact managed-CAS component worker command without a replica spawn.
#[tokio::test]
async fn celld_spawner_sends_exact_managed_cas_command()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let data_root = directory.path().join("state");
    let control_path = directory.path().join("celld.sock");
    let event_path = data_root.join("COUNTER--alice").join("events.sock");
    let worker_path = directory.path().join("worker.sock");
    let control_event_path = event_path.clone();
    let control_listener = UnixListener::bind(&control_path)?;
    let control_task = tokio::spawn(async move {
        let (stream, _) = control_listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let command = lines
            .next_line()
            .await?
            .ok_or("missing CAS worker command")?;
        assert_eq!(
            command,
            format!(
                "SPAWN_CAS_WORKER COUNTER--alice 1 7 http://127.0.0.1:8333 objects verglas us-east-1 access secret {} 11 7 {} - {DIGEST} components {}",
                hex::encode("opaque token"),
                hex::encode("etag-7"),
                control_event_path.display()
            )
        );
        write_half
            .write_all(format!("OK {}\n", worker_path.display()).as_bytes())
            .await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let event_path_for_task = event_path.clone();
    let event_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        let parent = event_path_for_task.parent().ok_or("event parent")?;
        tokio::fs::create_dir_all(parent).await?;
        let listener = UnixListener::bind(&event_path_for_task)?;
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        drop(listener);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let manifest = Manifest::parse(&format!(
        r#"{{
            "name": "counter", "main": "worker.js",
            "durable_objects": {{"bindings": [{{"name": "COUNTER", "class_name": "Counter"}}]}},
            "component_digest": "{DIGEST}", "component_dir": "components", "data_root": "state",
            "managed_cas": {{
                "endpoint": "http://127.0.0.1:8333", "bucket": "objects", "prefix": "verglas",
                "region": "us-east-1", "access_key_id": "access", "secret_access_key": "secret",
                "lease_token": "opaque token", "lease_generation": 11, "start_sequence": 7,
                "lease_etag": "etag-7"
            }}
        }}"#
    ))?;
    let cas = manifest
        .managed_cas()
        .cloned()
        .ok_or("missing CAS config")?;
    let request = SpawnRequest::new(
        "COUNTER--alice".to_owned(),
        "COUNTER".to_owned(),
        "alice".to_owned(),
        DIGEST.to_owned(),
        PathBuf::from("components"),
        data_root,
    )
    .with_managed_cas(cas);
    let spawner = CelldSpawner::new(control_path);
    let returned_event = spawner.spawn(request).await?;
    assert_eq!(returned_event, event_path);
    control_task.await??;
    event_task.await??;
    Ok(())
}

/// Routes one HTTP fetch through the fake event endpoint and preserves response data.
#[tokio::test]
async fn http_fetch_round_trip() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut fixture = Fixture::new().await?;
    let event = fixture.event_path();
    let listener_path = event.clone();
    let event_task = tokio::spawn(async move {
        let parent = listener_path.parent().ok_or("event parent")?;
        tokio::fs::create_dir_all(parent).await?;
        let listener = UnixListener::bind(&listener_path)?;
        let (stream, _) = listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let line = lines.next_line().await?.ok_or("missing fetch frame")?;
        let frame: Value = serde_json::from_str(&line)?;
        assert_eq!(frame["type"], "fetch");
        assert_eq!(frame["id"], 1);
        assert_eq!(frame["method"], "POST");
        assert_eq!(frame["url"], "/incr?step=2");
        assert!(
            frame["headers"]
                .as_array()
                .is_some_and(|headers| headers.iter().any(|header| header[0] == "x-test"))
        );
        assert_eq!(
            frame["body_b64"],
            base64::engine::general_purpose::STANDARD.encode("body")
        );
        write_half
            .write_all(
                format!(
                    "{}\n",
                    json!({
                        "type": "fetch-result",
                        "id": 1,
                        "status": 201,
                        "headers": [["content-type", "text/plain"], ["x-result", "yes"]],
                        "body_b64": base64::engine::general_purpose::STANDARD.encode("created")
                    })
                )
                .as_bytes(),
            )
            .await?;
        let second_line = lines
            .next_line()
            .await?
            .ok_or("missing second fetch frame")?;
        let second: Value = serde_json::from_str(&second_line)?;
        assert_eq!(second["type"], "fetch");
        assert_eq!(second["id"], 2);
        assert_eq!(second["method"], "GET");
        assert_eq!(second["url"], "/second");
        write_half
            .write_all(
                format!(
                    "{}\n",
                    json!({
                        "type": "fetch-result",
                        "id": 2,
                        "status": 204,
                        "headers": [],
                        "body_b64": ""
                    })
                )
                .as_bytes(),
            )
            .await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let control_task = fixture.control_for(&event).await?;
    let address = fixture.start_gateway().await?;

    let response = reqwest::Client::new()
        .post(format!("http://{address}/do/COUNTER/alice/incr?step=2"))
        .header("x-test", "value")
        .body("body")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    assert_eq!(response.headers()["content-type"], "text/plain");
    assert_eq!(response.headers()["x-result"], "yes");
    assert_eq!(response.text().await?, "created");

    let second = reqwest::get(format!("http://{address}/do/COUNTER/alice/second")).await?;
    assert_eq!(second.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(second.text().await?, "");

    event_task.await??;
    control_task.await??;
    fixture.shutdown().await;
    Ok(())
}

/// Applies commit-gated WebSocket sends in frame order and keeps errors nonfatal.
#[tokio::test]
async fn websocket_effects_and_errors_preserve_session()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut fixture = Fixture::new().await?;
    let event = fixture.event_path();
    let listener_path = event.clone();
    let event_task = tokio::spawn(async move {
        let parent = listener_path.parent().ok_or("event parent")?;
        tokio::fs::create_dir_all(parent).await?;
        let listener = UnixListener::bind(&listener_path)?;
        let (stream, _) = listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let open: Value = serde_json::from_str(&lines.next_line().await?.ok_or("missing open")?)?;
        assert_eq!(open["type"], "ws-open");
        let ws = open["ws"].as_u64().ok_or("missing ws id")?;

        let first: Value = serde_json::from_str(&lines.next_line().await?.ok_or("missing first")?)?;
        assert_eq!(first["type"], "ws-message");
        let first_id = first["id"].as_u64().ok_or("missing first id")?;
        write_half
            .write_all(
                format!(
                    "{}\n",
                    json!({"type": "error", "id": first_id, "message": "abort"})
                )
                .as_bytes(),
            )
            .await?;

        let second: Value =
            serde_json::from_str(&lines.next_line().await?.ok_or("missing second")?)?;
        assert_eq!(second["type"], "ws-message");
        assert_eq!(second["ws"], ws);
        let second_id = second["id"].as_u64().ok_or("missing second id")?;
        write_half
            .write_all(
                format!(
                    "{}\n{}\n",
                    json!({
                        "type": "ws-send",
                        "ws": ws,
                        "text": true,
                        "data_b64": base64::engine::general_purpose::STANDARD.encode("ok")
                    }),
                    json!({"type": "done", "id": second_id})
                )
                .as_bytes(),
            )
            .await?;
        while let Some(line) = lines.next_line().await? {
            if serde_json::from_str::<Value>(&line)?["type"] == "ws-close" {
                break;
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let control_task = fixture.control_for(&event).await?;
    let address = fixture.start_gateway().await?;
    let (mut socket, _) = connect_async(format!("ws://{address}/do/COUNTER/alice/ws")).await?;
    socket.send(Message::Text("bad".into())).await?;
    socket.send(Message::Text("good".into())).await?;

    let received = socket.next().await.ok_or("missing websocket send")??;
    assert_eq!(received, Message::Text("ok".into()));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), socket.next())
            .await
            .is_err()
    );
    socket.close(None).await?;

    event_task.await??;
    control_task.await??;
    fixture.shutdown().await;
    Ok(())
}

/// Delivers two pipelined WebSocket effects in the event order emitted by verglasd.
#[tokio::test]
async fn websocket_pipelined_effect_order_is_authoritative()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut fixture = Fixture::new().await?;
    let event = fixture.event_path();
    let listener_path = event.clone();
    let event_task = tokio::spawn(async move {
        let parent = listener_path.parent().ok_or("event parent")?;
        tokio::fs::create_dir_all(parent).await?;
        let listener = UnixListener::bind(&listener_path)?;
        let (stream, _) = listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let open: Value = serde_json::from_str(&lines.next_line().await?.ok_or("missing open")?)?;
        let ws = open["ws"].as_u64().ok_or("missing ws")?;
        let first: Value = serde_json::from_str(&lines.next_line().await?.ok_or("missing first")?)?;
        let second: Value =
            serde_json::from_str(&lines.next_line().await?.ok_or("missing second")?)?;
        let first_id = first["id"].as_u64().ok_or("missing first id")?;
        let second_id = second["id"].as_u64().ok_or("missing second id")?;
        assert_eq!(first["ws"], ws);
        assert_eq!(second["ws"], ws);
        write_half
            .write_all(
                format!(
                    "{}\n{}\n{}\n{}\n",
                    json!({"type": "ws-send", "ws": ws, "text": true, "data_b64": base64::engine::general_purpose::STANDARD.encode("one")}),
                    json!({"type": "done", "id": first_id}),
                    json!({"type": "ws-send", "ws": ws, "text": true, "data_b64": base64::engine::general_purpose::STANDARD.encode("two")}),
                    json!({"type": "done", "id": second_id})
                )
                .as_bytes(),
            )
            .await?;
        while let Some(line) = lines.next_line().await? {
            if serde_json::from_str::<Value>(&line)?["type"] == "ws-close" {
                break;
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let control_task = fixture.control_for(&event).await?;
    let address = fixture.start_gateway().await?;
    let (mut socket, _) = connect_async(format!("ws://{address}/do/COUNTER/alice/ws")).await?;
    socket.send(Message::Text("one".into())).await?;
    socket.send(Message::Text("two".into())).await?;
    assert_eq!(
        socket.next().await.ok_or("missing first")??,
        Message::Text("one".into())
    );
    assert_eq!(
        socket.next().await.ok_or("missing second")??,
        Message::Text("two".into())
    );
    socket.close(None).await?;

    event_task.await??;
    control_task.await??;
    fixture.shutdown().await;
    Ok(())
}

/// Returns 404 for an unmatched route and exposes unknown bindings as typed errors.
#[tokio::test]
async fn unknown_route_and_binding_are_rejected()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut fixture = Fixture::new().await?;
    let address = fixture.start_gateway().await?;
    let response = reqwest::get(format!("http://{address}/not-a-do-route")).await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    let error = fixture.gateway().resolve_binding("MISSING", "alice");
    assert!(matches!(error, Err(GatewayError::UnknownBinding { .. })));
    fixture.shutdown().await;
    Ok(())
}

struct Fixture {
    directory: TempDir,
    control_path: PathBuf,
    gateway: Gateway,
    server_task: Option<JoinHandle<Result<(), std::io::Error>>>,
}

impl Fixture {
    /// Creates a valid gateway manifest and isolated Unix-socket paths.
    async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let directory = tempfile::tempdir()?;
        let manifest = Manifest::parse(&format!(
            "{{\"name\":\"counter\",\"main\":\"src/index.ts\",\"durable_objects\":{{\"bindings\":[{{\"name\":\"COUNTER\",\"class_name\":\"Counter\"}}]}},\"component_digest\":\"{DIGEST}\",\"component_dir\":\"components\",\"data_root\":\"state\"}}"
        ))?;
        let control_path = directory.path().join("celld.sock");
        let gateway = Gateway::new(&manifest, &control_path, directory.path().join("state"));
        Ok(Self {
            directory,
            control_path,
            gateway,
            server_task: None,
        })
    }

    /// Returns the private event socket path used by the fake worker.
    fn event_path(&self) -> PathBuf {
        self.directory
            .path()
            .join("state")
            .join("COUNTER--alice")
            .join("events.sock")
    }

    /// Starts a fake celld control endpoint that returns the fake event socket.
    async fn control_for(
        &self,
        event_path: &Path,
    ) -> Result<
        JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let listener = UnixListener::bind(&self.control_path)?;
        let event_path = event_path.to_path_buf();
        let replica_path = self.directory.path().join("replica.sock");
        let worker_path = self.directory.path().join("worker.sock");
        let replica_listener = UnixListener::bind(&replica_path)?;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let (read_half, mut write_half) = stream.into_split();
            let mut lines = BufReader::new(read_half).lines();
            let replica_command = lines.next_line().await?.ok_or("missing replica command")?;
            assert_eq!(replica_command, "SPAWN COUNTER--alice 1 follower 0");
            write_half
                .write_all(format!("OK {}\n", replica_path.display()).as_bytes())
                .await?;
            let (stream, _) = replica_listener.accept().await?;
            let (read_half, mut write_half) = stream.into_split();
            let mut lines = BufReader::new(read_half).lines();
            assert_eq!(
                lines.next_line().await?.ok_or("missing replica status")?,
                "STATUS"
            );
            write_half.write_all(b"OK replica 0 0 0\n").await?;
            let (stream, _) = listener.accept().await?;
            let (read_half, mut write_half) = stream.into_split();
            let mut lines = BufReader::new(read_half).lines();
            let worker_command = lines.next_line().await?.ok_or("missing worker command")?;
            let token = hex::encode("lease-COUNTER--alice");
            assert_eq!(
                worker_command,
                format!(
                    "SPAWN_WORKER COUNTER--alice 1 0 {} {} 11 0 - {DIGEST} components {}",
                    replica_path.display(),
                    token,
                    event_path.display()
                )
            );
            write_half
                .write_all(format!("OK {}\n", worker_path.display()).as_bytes())
                .await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        Ok(task)
    }

    /// Starts the real axum server on an ephemeral TCP port.
    async fn start_gateway(
        &mut self,
    ) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let router = self.gateway.router();
        self.server_task = Some(tokio::spawn(
            async move { axum::serve(listener, router).await },
        ));
        Ok(address)
    }

    /// Borrows the gateway for direct typed-error assertions.
    fn gateway(&self) -> &Gateway {
        &self.gateway
    }

    /// Stops the ephemeral server and keeps the fixture alive through assertions.
    async fn shutdown(self) {
        if let Some(task) = self.server_task {
            task.abort();
            let _ = task.await;
        }
    }
}
