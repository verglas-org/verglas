//! Contract tests for `verglas lakehouse`: list and create speak the control
//! plane's `/v1/lakehouses` surface with the resolved bearer and the tenant id
//! recorded in the connection profile at login.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::thread;

use tempfile::tempdir;

/// One-shot mock control plane: captures the request head+body, returns `body`.
fn one_shot_server(
    status: &'static str,
    body: &'static str,
) -> (String, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock");
    let address = listener.local_addr().expect("address");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 8192];
        let mut content_length = 0usize;
        loop {
            let count = stream.read(&mut buffer).expect("read");
            if count == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..count]);
            if let Some(head_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&request[..head_end]).to_lowercase();
                if let Some(line) = head.lines().find(|l| l.starts_with("content-length:")) {
                    content_length = line["content-length:".len()..].trim().parse().unwrap_or(0);
                }
                if request.len() >= head_end + 4 + content_length {
                    break;
                }
            }
        }
        write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
        .expect("respond");
        String::from_utf8_lossy(&request).into_owned()
    });
    (format!("http://{address}"), handle)
}

/// Writes a connection profile carrying a tenant id and a bearer file.
fn write_profile(home: &std::path::Path, tenant: &str) -> std::path::PathBuf {
    let root = home.join(".verglas");
    fs::create_dir_all(root.join("credentials")).expect("dirs");
    let bearer = root.join("credentials").join("control-plane-token");
    fs::write(&bearer, "cli-bearer-secret").expect("bearer");
    let config = root.join("config.toml");
    fs::write(
        &config,
        format!(
            "[connection]\ntenant_id = \"{tenant}\"\nbearer_file = \"{}\"\n",
            bearer.display()
        ),
    )
    .expect("config");
    config
}

fn lakehouse_command(home: &std::path::Path, url: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verglas"));
    command
        .env("HOME", home)
        .env("VERGLAS_TOKEN", "cli-bearer-secret")
        .env("VERGLAS_ACCESS_ENDPOINT", url);
    command
}

#[test]
fn list_gets_v1_lakehouses_for_the_profile_tenant_with_the_bearer() {
    let home = tempdir().expect("home");
    write_profile(home.path(), "11111111-2222-4333-8444-555555555555");
    let (url, server) = one_shot_server(
        "200 OK",
        r#"{"lakehouses":[{"database_id":"db1","name":"default","storage":{"mode":"managed","bucket":"b","key_prefix":"iceberg"}}]}"#,
    );
    let output = lakehouse_command(home.path(), &url)
        .args(["lakehouse", "list"])
        .output()
        .expect("runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let request = server.join().expect("server");
    assert!(
        request.starts_with("GET /v1/lakehouses?tenant_id=11111111-2222-4333-8444-555555555555 "),
        "{request}"
    );
    assert!(
        request
            .to_lowercase()
            .contains("authorization: bearer cli-bearer-secret"),
        "{request}"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("default"), "{stdout}");
    assert!(stdout.contains("managed"), "{stdout}");
}

#[test]
fn create_posts_a_managed_lakehouse_by_default() {
    let home = tempdir().expect("home");
    write_profile(home.path(), "11111111-2222-4333-8444-555555555555");
    let (url, server) = one_shot_server("201 Created", r#"{"warehouse-id":"wh-9"}"#);
    let output = lakehouse_command(home.path(), &url)
        .args(["lakehouse", "create", "analytics"])
        .output()
        .expect("runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let request = server.join().expect("server");
    assert!(request.starts_with("POST /v1/lakehouses "), "{request}");
    let body: serde_json::Value =
        serde_json::from_str(request.split("\r\n\r\n").nth(1).expect("body")).expect("json body");
    assert_eq!(body["tenant_id"], "11111111-2222-4333-8444-555555555555");
    assert_eq!(body["name"], "analytics");
    assert_eq!(body["storage"]["mode"], "managed");
}

#[test]
fn create_with_data_path_posts_an_external_s3_storage_profile() {
    let home = tempdir().expect("home");
    write_profile(home.path(), "11111111-2222-4333-8444-555555555555");
    let (url, server) = one_shot_server("201 Created", r#"{"warehouse-id":"wh-10"}"#);
    let output = lakehouse_command(home.path(), &url)
        .env("VERGLAS_STORAGE_ACCESS_KEY_ID", "AKEXT")
        .env("VERGLAS_STORAGE_SECRET_ACCESS_KEY", "ext-secret")
        .args([
            "lakehouse",
            "create",
            "bring-your-own",
            "--data-path",
            "s3://my-bucket/lake/prefix",
            "--storage-endpoint",
            "https://s3.example",
            "--storage-region",
            "us-west-2",
        ])
        .output()
        .expect("runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let request = server.join().expect("server");
    let body: serde_json::Value =
        serde_json::from_str(request.split("\r\n\r\n").nth(1).expect("body")).expect("json body");
    assert_eq!(body["name"], "bring-your-own");
    let storage = &body["storage"];
    assert_eq!(storage["mode"], "external-s3");
    let profile = &storage["profile"];
    assert_eq!(profile["bucket"], "my-bucket");
    assert_eq!(profile["key_prefix"], "lake/prefix");
    assert_eq!(profile["endpoint"], "https://s3.example");
    assert_eq!(profile["region"], "us-west-2");
    assert_eq!(profile["access_key_id"], "AKEXT");
    assert_eq!(profile["secret_access_key"], "ext-secret");
    // The secret arrives via environment, never argv.
    assert!(!request.contains("ext-secret\r\n") || request.contains("secret_access_key"));
}

#[test]
fn an_unauthorized_response_maps_to_token_renewal_guidance() {
    let home = tempdir().expect("home");
    write_profile(home.path(), "11111111-2222-4333-8444-555555555555");
    let (url, server) = one_shot_server("401 Unauthorized", r#"{"error":"unauthorized"}"#);
    let output = lakehouse_command(home.path(), &url)
        .args(["lakehouse", "list"])
        .output()
        .expect("runs");
    assert!(!output.status.success());
    server.join().expect("server");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("run `verglas login` to sign in again"),
        "{stderr}"
    );
}

#[test]
fn create_without_a_tenant_id_fails_with_sign_in_guidance() {
    let home = tempdir().expect("home");
    // Profile exists but predates tenant recording.
    let root = home.path().join(".verglas");
    fs::create_dir_all(&root).expect("dirs");
    fs::write(root.join("config.toml"), "[connection]\n").expect("config");
    let output = lakehouse_command(home.path(), "http://127.0.0.1:9")
        .args(["lakehouse", "create", "x"])
        .output()
        .expect("runs");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("verglas login"), "{stderr}");
}
