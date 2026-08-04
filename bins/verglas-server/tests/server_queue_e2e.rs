//! End-to-end proof that a real `verglas-server` process serves the platform queue
//! routes (#328): spawn a throwaway server bound to a loopback port with a temp
//! cache dir, drive `enqueue`/`poll`/`ack` over HTTP, and tear the server down
//! by its captured PID only — no broad kill, no installed server touched.
//!
//! The queue routes need no cache engine or catalog, so they answer as soon as
//! the admin listener binds, even while recovery is still gated.

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Spawns a throwaway server with a temp cache dir on an ephemeral admin port,
/// returning the child (its PID is the only thing torn down) and its endpoint.
fn spawn_throwaway() -> (Child, String, tempfile::TempDir) {
    let admin = std::net::TcpListener::bind("127.0.0.1:0").expect("bind admin");
    let admin_addr = admin.local_addr().expect("admin addr");
    drop(admin);

    let dir = tempfile::tempdir().expect("cache tempdir");
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n[backend]\nbucket = \"throwaway-lake\"\n",
        dir.path().display()
    );
    let cfg = dir.path().join("verglas.toml");
    std::fs::File::create(&cfg)
        .expect("create config")
        .write_all(toml.as_bytes())
        .expect("write config");

    let server = Command::new(env!("CARGO_BIN_EXE_verglas-server"))
        .arg("--config")
        .arg(&cfg)
        .env("VERGLAS_ADMIN_ADDR", admin_addr.to_string())
        .env("VERGLAS_S3_ADDR", "127.0.0.1:0")
        // Test opt-out: skip the origin probe so the server comes up without a
        // reachable backend (the queue routes need neither).
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn verglas-server");

    (server, format!("http://{admin_addr}"), dir)
}

/// Waits until the admin version probe answers (the listener is up).
fn wait_for_admin(endpoint: &str) {
    for _ in 0..100 {
        if reqwest::blocking::get(format!("{endpoint}/admin/version"))
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("admin API did not become ready at {endpoint}");
}

/// The server serves the queue routes end to end: enqueue three rows, poll them
/// for a group, ack, then poll again to see the watermark advanced.
#[test]
fn server_serves_queue_routes() {
    let (mut server, endpoint, _dir) = spawn_throwaway();
    wait_for_admin(&endpoint);
    let client = reqwest::blocking::Client::new();

    // Enqueue.
    let resp = client
        .post(format!("{endpoint}/v1/queues/e2e/enqueue"))
        .json(&serde_json::json!({ "rows": [{ "id": 1 }, { "id": 2 }, { "id": 3 }] }))
        .send()
        .expect("enqueue");
    assert!(resp.status().is_success(), "enqueue ok");
    let body: serde_json::Value = resp.json().expect("enqueue json");
    assert_eq!(body["enqueued"], 3);
    assert_eq!(body["endPosition"], 3);

    // Poll a fresh group: all three.
    let body: serde_json::Value = client
        .get(format!("{endpoint}/v1/queues/e2e/poll"))
        .query(&[("group", "g"), ("max", "10")])
        .send()
        .expect("poll")
        .json()
        .expect("poll json");
    assert_eq!(body["records"].as_array().expect("records").len(), 3);

    // Ack through 3.
    let body: serde_json::Value = client
        .post(format!("{endpoint}/v1/queues/e2e/ack"))
        .json(&serde_json::json!({ "group": "g", "position": 3 }))
        .send()
        .expect("ack")
        .json()
        .expect("ack json");
    assert_eq!(body["watermark"], 3);

    // Poll again: caught up.
    let body: serde_json::Value = client
        .get(format!("{endpoint}/v1/queues/e2e/poll"))
        .query(&[("group", "g")])
        .send()
        .expect("poll2")
        .json()
        .expect("poll2 json");
    assert_eq!(body["records"].as_array().expect("records").len(), 0);

    // Tear down by PID only.
    let _ = server.kill();
    let _ = server.wait();
}
