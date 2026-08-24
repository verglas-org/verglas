//! Gateway-level AC1 proof using the real celld host, verglasd, and JS component.

use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpListener;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use verglas_gateway::{Gateway, Manifest};

/// Child process guard that prevents an assertion failure from orphaning celld.
struct ManagedChild(Child);

impl Deref for ManagedChild {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ManagedChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for ManagedChild {
    /// Kills a child left behind by a failed integration assertion.
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Proves committed Worker effects and handler errors at the real gateway boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_stack_websocket_effects_are_commit_gated_and_errors_are_nonfatal() {
    let root = tempfile::tempdir().expect("integration root");
    let project = root.path().join("worker");
    let components = root.path().join("components");
    let data_root = root.path().join("state");
    std::fs::create_dir_all(&project).expect("project directory");
    std::fs::create_dir_all(&components).expect("component directory");
    write_worker_project(&project, &components, &data_root);
    build_component(&project, &components);

    let manifest_path = project.join("gateway.json");
    let manifest = Manifest::from_path(&manifest_path).expect("built gateway manifest");
    let digest = manifest.component_digest().to_owned();
    assert!(components.join(format!("{digest}.wasm")).is_file());

    let repository = repository_root();
    let celld = repository.join("target/debug/celld-host");
    let verglasd = repository.join("target/debug/verglasd");
    assert!(
        celld.is_file(),
        "build target/debug/celld-host before this test"
    );
    assert!(
        verglasd.is_file(),
        "build target/debug/verglasd before this test"
    );
    let control = root.path().join("celld.sock");
    let mut host = ManagedChild(
        Command::new(&celld)
            .arg("--host-id")
            .arg("ac1-cell")
            .arg("--root")
            .arg(&data_root)
            .arg("--child")
            .arg(&verglasd)
            .arg("--control")
            .arg(&control)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn real celld-host"),
    );
    wait_for_socket(&mut host, &control).await;

    let gateway = Gateway::new(&manifest, &control, &data_root);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("gateway listener");
    let address = listener.local_addr().expect("gateway address");
    let server = tokio::spawn(async move { axum::serve(listener, gateway.router()).await });

    // CC2 proof: the public route executes the Worker export, which performs
    // the DO binding fetch before this response reaches the client.
    let worker_state = reqwest::get(format!("http://{address}/state"))
        .await
        .expect("public Worker fetch")
        .text()
        .await
        .expect("public Worker response body");
    assert_eq!(worker_state, "");

    let ws_url = format!("ws://{address}/do/COUNTER/ac1/ws");
    let (mut websocket, _) = tokio::time::timeout(Duration::from_secs(240), connect_async(ws_url))
        .await
        .expect("component startup exceeded 240 seconds")
        .expect("connect real gateway WebSocket");

    websocket
        .send(Message::Text("committed".into()))
        .await
        .expect("send successful event");
    let committed = tokio::time::timeout(Duration::from_secs(10), websocket.next())
        .await
        .expect("successful event did not produce a WebSocket effect")
        .expect("gateway WebSocket closed after successful event")
        .expect("successful event WebSocket read")
        .into_text()
        .expect("successful effect is text");
    assert_eq!(committed, "committed");

    // The worker's storage write is visible as soon as its effect is delivered.
    // This couples the external effect to the event commit and terminal-frame path.
    let state = reqwest::get(format!("http://{address}/state"))
        .await
        .expect("state fetch")
        .text()
        .await
        .expect("state body");
    assert_eq!(state, "committed");

    websocket
        .send(Message::Text("error".into()))
        .await
        .expect("send failing event");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), websocket.next())
            .await
            .is_err(),
        "handler-error event delivered a staged WebSocket effect"
    );

    websocket
        .send(Message::Text("alive".into()))
        .await
        .expect("send event after handler error");
    let alive = tokio::time::timeout(Duration::from_secs(10), websocket.next())
        .await
        .expect("session did not remain alive after handler error")
        .expect("gateway WebSocket closed after handler error")
        .expect("post-error WebSocket read")
        .into_text()
        .expect("post-error effect is text");
    assert_eq!(alive, "alive");

    websocket
        .close(None)
        .await
        .expect("close gateway WebSocket");
    server.abort();
    let _ = server.await;
    stop_host(&mut host);
}

/// Writes a minimal real Worker that stages storage and socket effects.
fn write_worker_project(project: &Path, components: &Path, data_root: &Path) {
    std::fs::write(
        project.join("wrangler.jsonc"),
        r#"{
          "name": "ac1-worker",
          "main": "worker.js",
          "compatibility_date": "2024-01-01",
          "durable_objects": { "bindings": [{ "name": "COUNTER", "class_name": "Counter" }] }
        }
        "#,
    )
    .expect("wrangler manifest");
    std::fs::write(
        project.join("worker.js"),
        r#"import { DurableObject } from 'cloudflare:workers';

        export default {
          fetch(request, env) {
            const id = env.COUNTER.idFromName('ac1');
            return env.COUNTER.get(id).fetch(request);
          }
        };

        export class Counter extends DurableObject {
          async fetch() {
            const value = await this.ctx.storage.get('last');
            return new Response(value ?? '', { headers: { 'content-type': 'text/plain' } });
          }

          async webSocketMessage(socket, message) {
            const value = new TextDecoder().decode(message);
            if (value === 'error') {
              await this.ctx.storage.put('last', 'error');
              socket.send('must-not-deliver');
              throw new Error('intentional handler failure');
            }
            await this.ctx.storage.put('last', value);
            socket.send(value);
          }
        }
        "#,
    )
    .expect("Worker source");
    std::fs::write(
        project.join("gateway.json"),
        serde_json::to_vec_pretty(&json!({
            "name": "ac1-worker",
            "main": "worker.js",
            "compatibility_date": "2024-01-01",
            "compatibility_flags": ["nodejs_compat"],
            "durable_objects": { "bindings": [{ "name": "COUNTER", "class_name": "Counter" }] },
            "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Counter"] }],
            "vars": { "AC1": "worker-first" },
            "component_digest": "0".repeat(64),
            "component_dir": components,
            "data_root": data_root,
        }))
        .expect("gateway JSON"),
    )
    .expect("gateway manifest");
}

/// Runs the prescribed JS builder against the WIT v2 `service` world.
fn build_component(project: &Path, components: &Path) {
    // The builder invokes componentize with --world-name service. Keep this
    // fixture on the frozen WIT v2 world rather than restoring durable-object.
    let script = repository_root().join("sdks/worker-js/bin/build.mjs");
    let output = Command::new("node")
        .arg(script)
        .arg(project)
        .arg("--out")
        .arg(components)
        .arg("--gateway")
        .arg(project.join("gateway.json"))
        .output()
        .unwrap_or_else(|error| panic!("node is required to build AC1 component: {error}"));
    assert!(
        output.status.success(),
        "worker-js builder failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Returns the checkout root containing the SDK and built runtime binaries.
fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("repository root")
        .to_path_buf()
}

/// Waits for a real Unix control socket to become available.
async fn wait_for_socket(child: &mut Child, path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        if let Some(status) = child.try_wait().expect("inspect celld-host") {
            panic!(
                "celld-host exited before binding {}: {status}",
                path.display()
            );
        }
        assert!(Instant::now() < deadline, "{} did not bind", path.display());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Gracefully stops celld so its supervisor drains and kills real children.
fn stop_host(host: &mut ManagedChild) {
    let status = Command::new("kill")
        .arg("-INT")
        .arg(host.id().to_string())
        .status()
        .expect("signal celld-host");
    assert!(status.success());
    host.wait().expect("wait celld-host");
}
