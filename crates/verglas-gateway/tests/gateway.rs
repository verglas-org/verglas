//! HTTP, WebSocket, and exact control forwarding tests.

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
use verglas_gateway::{DoSpawner, Gateway, GatewayError, Manifest, SpawnRequest, VerglasdSpawner};

const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn manifest_source() -> String {
    format!(
        r#"{{"name":"counter","main":"src/index.ts","durable_objects":{{"bindings":[{{"name":"COUNTER","class_name":"Counter"}}]}},"artifacts":{{"worker":{{"digest":"{DIGEST}","component_dir":"components"}},"durable_object":{{"digest":"{DIGEST}","component_dir":"components"}}}},"data_root":"state"}}"#
    )
}

/// Sends one complete Worker command and retries until the event socket binds.
#[tokio::test]
async fn verglasd_spawner_sends_exact_worker_command_and_waits_for_event_bind()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let data_root = directory.path().join("state");
    let control_path = directory.path().join("verglasd.sock");
    let event_path = data_root.join("COUNTER--alice").join("events.sock");
    let listener = UnixListener::bind(&control_path)?;
    let expected_event = event_path.clone();
    let expected_data_root = data_root.clone();
    let control_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let command = lines.next_line().await?.ok_or("missing worker command")?;
        assert_eq!(
            command,
            format!(
                "SPAWN_WORKER COUNTER--alice {} {DIGEST} components - {} - -",
                expected_data_root.join("COUNTER--alice").display(),
                expected_event.display()
            )
        );
        write_half
            .write_all(format!("OK {}\n", expected_event.display()).as_bytes())
            .await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let delayed_event = event_path.clone();
    let event_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let parent = delayed_event.parent().ok_or("event parent")?;
        tokio::fs::create_dir_all(parent).await?;
        let listener = UnixListener::bind(&delayed_event)?;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        drop(listener);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let request = SpawnRequest::new(
        "COUNTER--alice".to_owned(),
        "COUNTER".to_owned(),
        "alice".to_owned(),
        DIGEST.to_owned(),
        PathBuf::from("components"),
        data_root,
    );
    let started = Instant::now();
    let returned = VerglasdSpawner::new(control_path).spawn(request).await?;
    assert_eq!(returned, event_path);
    assert!(started.elapsed() >= std::time::Duration::from_millis(40));
    control_task.await??;
    event_task.await??;
    Ok(())
}

/// Routes two HTTP fetches through one fake Turso event endpoint.
#[tokio::test]
async fn http_fetch_round_trip() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut fixture = Fixture::new()?;
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
        write_half
            .write_all(
                format!(
                    "{}\n",
                    json!({"type":"fetch-result","id":1,"status":201,"headers":[["content-type","text/plain"]],"body_b64":base64::engine::general_purpose::STANDARD.encode("created")})
                )
                .as_bytes(),
            )
            .await?;
        let second: Value =
            serde_json::from_str(&lines.next_line().await?.ok_or("missing second")?)?;
        assert_eq!(second["id"], 2);
        write_half
            .write_all(
                format!(
                    "{}\n",
                    json!({"type":"fetch-result","id":2,"status":204,"headers":[],"body_b64":""})
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
        .body("body")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    assert_eq!(response.text().await?, "created");
    let second = reqwest::get(format!("http://{address}/do/COUNTER/alice/second")).await?;
    assert_eq!(second.status(), reqwest::StatusCode::NO_CONTENT);
    event_task.await??;
    control_task.await??;
    fixture.shutdown().await;
    Ok(())
}

/// Applies commit-gated WebSocket sends in frame order and keeps errors nonfatal.
#[tokio::test]
async fn websocket_effects_and_errors_preserve_session()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut fixture = Fixture::new()?;
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
        let first_id = first["id"].as_u64().ok_or("missing first id")?;
        write_half
            .write_all(
                format!(
                    "{}\n",
                    json!({"type":"error","id":first_id,"message":"abort"})
                )
                .as_bytes(),
            )
            .await?;
        let second: Value =
            serde_json::from_str(&lines.next_line().await?.ok_or("missing second")?)?;
        let second_id = second["id"].as_u64().ok_or("missing second id")?;
        write_half
            .write_all(
                format!(
                    "{}\n{}\n",
                    json!({"type":"ws-send","ws":ws,"text":true,"data_b64":base64::engine::general_purpose::STANDARD.encode("ok")}),
                    json!({"type":"done","id":second_id})
                )
                .as_bytes(),
            )
            .await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let control_task = fixture.control_for(&event).await?;
    let address = fixture.start_gateway().await?;
    let (mut socket, _) = connect_async(format!("ws://{address}/do/COUNTER/alice/ws")).await?;
    socket.send(Message::Text("bad".into())).await?;
    socket.send(Message::Text("good".into())).await?;
    assert_eq!(
        socket.next().await.ok_or("missing websocket send")??,
        Message::Text("ok".into())
    );
    socket.close(None).await?;
    event_task.await??;
    control_task.await??;
    fixture.shutdown().await;
    Ok(())
}

/// Unknown namespace bindings remain distinct from pipeline bindings.
#[tokio::test]
async fn unknown_route_and_binding_are_rejected()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut fixture = Fixture::new()?;
    let address = fixture.start_gateway().await?;
    let response = reqwest::get(format!("http://{address}/do/MISSING/alice")).await?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    assert!(matches!(
        fixture.gateway().resolve_binding("MISSING", "alice"),
        Err(GatewayError::UnknownBinding { .. })
    ));
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
    fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let directory = tempfile::tempdir()?;
        let manifest = Manifest::parse(&manifest_source())?;
        let control_path = directory.path().join("verglasd.sock");
        let gateway = Gateway::new(&manifest, &control_path, directory.path().join("state"));
        Ok(Self {
            directory,
            control_path,
            gateway,
            server_task: None,
        })
    }

    /// Returns the private event socket path used by the fake Worker.
    fn event_path(&self) -> PathBuf {
        self.directory
            .path()
            .join("state")
            .join("COUNTER--alice")
            .join("events.sock")
    }

    /// Starts a fake verglasd control endpoint that validates the one-path command.
    async fn control_for(
        &self,
        event_path: &Path,
    ) -> Result<
        JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let listener = UnixListener::bind(&self.control_path)?;
        let event_path = event_path.to_path_buf();
        let data_root = self.directory.path().join("state");
        Ok(tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let (read_half, mut write_half) = stream.into_split();
            let mut lines = BufReader::new(read_half).lines();
            let command = lines.next_line().await?.ok_or("missing worker command")?;
            assert_eq!(
                command,
                format!(
                    "SPAWN_WORKER COUNTER--alice {} {DIGEST} components - {} - -",
                    data_root.join("COUNTER--alice").display(),
                    event_path.display()
                )
            );
            write_half
                .write_all(format!("OK {}\n", event_path.display()).as_bytes())
                .await?;
            Ok(())
        }))
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
