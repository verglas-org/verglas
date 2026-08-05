//! Process-level contract for the always-on native KV engine: no KV config,
//! acknowledged writes survive SIGKILL, and object-cache purge cannot touch KV.

use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use reqwest::StatusCode;

/// Reserves an unused loopback address for a child listener.
fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
}

/// Starts a server over the same persistent data directory with no KV section.
fn spawn(
    config: &std::path::Path,
    admin: SocketAddr,
    s3: SocketAddr,
    log: &std::path::Path,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_verglas-server"))
        .arg("--config")
        .arg(config)
        .env("VERGLAS_ADMIN_ADDR", admin.to_string())
        .env("VERGLAS_S3_ADDR", s3.to_string())
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(log).expect("create server log"),
        ))
        .spawn()
        .expect("spawn server")
}

/// Waits for the server recovery gate to report ready.
async fn ready(client: &reqwest::Client, admin: SocketAddr) {
    for _ in 0..200 {
        if client
            .get(format!("http://{admin}/admin/healthz"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not become ready");
}

/// Always-on KV data survives abrupt termination and ignores object-cache purge.
#[tokio::test]
async fn acknowledged_kv_survives_sigkill_and_object_purge_without_config() {
    let scratch = tempfile::tempdir().expect("tempdir");
    let cache = scratch.path().join("cache");
    std::fs::create_dir_all(&cache).expect("cache dir");
    let credentials = scratch.path().join("credentials");
    std::fs::write(
        &credentials,
        "[default]\naws_access_key_id = local-tenant\naws_secret_access_key = local-secret\n",
    )
    .expect("credentials");
    let config = scratch.path().join("verglas.toml");
    std::fs::write(
        &config,
        format!(
            "[cache]\ndir = \"{}\"\ncapacity_bytes = \"128MB\"\ndram_bytes = \"80MB\"\n\n[backend]\nbucket = \"unused\"\nendpoint = \"http://127.0.0.1:9\"\nallow_http = true\n\n[auth]\ncredentials_file = \"{}\"\n",
            cache.display(),
            credentials.display(),
        ),
    )
    .expect("config");
    assert!(
        !std::fs::read_to_string(&config)
            .expect("config")
            .contains("[kv]")
    );

    let client = reqwest::Client::new();
    let admin = free_addr();
    let s3 = free_addr();
    let first_log = scratch.path().join("first.log");
    let mut first = spawn(&config, admin, s3, &first_log);
    ready(&client, admin).await;
    let endpoint = format!("http://{admin}/v1/kv/workshop.blueprints/featured");
    assert_eq!(
        client
            .put(&endpoint)
            .body("blue")
            .send()
            .await
            .expect("unauthenticated")
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = client
        .put(&endpoint)
        .bearer_auth("local-secret")
        .header("x-verglas-ttl-seconds", "300")
        .body("blue")
        .send()
        .await
        .expect("put");
    assert_eq!(response.status(), StatusCode::CREATED);
    first.kill().expect("SIGKILL child");
    let _ = first.wait();
    let log = std::fs::read_to_string(&first_log).expect("read first log");
    for secret in ["local-secret", "workshop.blueprints", "featured", "blue"] {
        assert!(
            !log.contains(secret),
            "server log exposed KV input: {secret}"
        );
    }

    let admin = free_addr();
    let s3 = free_addr();
    let mut second = spawn(&config, admin, s3, &scratch.path().join("second.log"));
    ready(&client, admin).await;
    let endpoint = format!("http://{admin}/v1/kv/workshop.blueprints/featured");
    let response = client
        .get(&endpoint)
        .bearer_auth("local-secret")
        .send()
        .await
        .expect("get after restart");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.expect("body"), "blue");

    let purge = client
        .post(format!("http://{admin}/cache/purge"))
        .send()
        .await
        .expect("purge");
    assert_eq!(purge.status(), StatusCode::OK);
    let response = client
        .get(&endpoint)
        .bearer_auth("local-secret")
        .send()
        .await
        .expect("get after purge");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.expect("body"), "blue");
    second.kill().expect("stop child");
    let _ = second.wait();
}
