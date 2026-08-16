//! Contract tests for the shared Verglas login profile consumed by integrations.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::thread;

use serde_json::Value;
use tempfile::tempdir;

const CLOUD_API_KEY: &str = "cloud-api-key-secret";
const ENDPOINT_SECRET: &str = "endpoint-signing-secret";
const CATALOG_TOKEN: &str = "catalog-query-bearer-secret";

fn clean_command(home: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verglas"));
    command
        .env("HOME", home)
        .env_remove("VERGLAS_ENDPOINT")
        .env_remove("VERGLAS_TOKEN")
        .env_remove("VERGLAS_ACCESS_KEY_ID")
        .env_remove("VERGLAS_SECRET_ACCESS_KEY")
        .env_remove("VERGLAS_REGION");
    command
}

fn one_shot_provision_server() -> (String, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock control plane");
    let address = listener.local_addr().expect("mock address");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept provision request");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let count = stream.read(&mut buffer).expect("read request");
            if count == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..count]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let body = format!(
            r#"{{"s3_url":"https://tenant.s3.example","catalog_url":"https://tenant.catalog.example","query_url":"https://tenant.query.example","slug":"acme","s3_access_key_id":"VGACME","s3_secret_access_key":"{ENDPOINT_SECRET}","catalog_token":"{CATALOG_TOKEN}","tier":"starter"}}"#,
        );
        write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", body.len(), body).expect("write response");
        String::from_utf8(request).expect("request utf8")
    });
    (format!("http://{address}"), handle)
}

#[test]
fn login_writes_a_secret_free_shared_profile_and_connection_resolves_it() {
    let home = tempdir().expect("temporary home");
    let (url, request) = one_shot_provision_server();
    let output = clean_command(home.path())
        .args(["login", "--url", &url, "--api-key", CLOUD_API_KEY])
        .output()
        .expect("login runs");
    assert!(
        output.status.success(),
        "login failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let request = request.join().expect("mock server exits");
    assert!(request.starts_with("POST /v1/provision "), "{request}");
    assert!(
        request
            .to_ascii_lowercase()
            .contains(&format!("authorization: bearer {CLOUD_API_KEY}"))
    );

    let root = home.path().join(".verglas");
    let config = fs::read_to_string(root.join("config.toml")).expect("shared config");
    assert!(config.contains("[connection]"), "{config}");
    assert!(config.contains("https://tenant.query.example"), "{config}");
    assert!(config.contains("https://tenant.s3.example"), "{config}");
    for secret in [CLOUD_API_KEY, ENDPOINT_SECRET, CATALOG_TOKEN] {
        assert!(!config.contains(secret), "config leaked a secret");
        assert!(
            !String::from_utf8_lossy(&output.stdout).contains(secret),
            "stdout leaked a secret"
        );
    }

    let resolved = clean_command(home.path())
        .args(["connection", "--json", "--include-secrets"])
        .output()
        .expect("connection resolution runs");
    assert!(
        resolved.status.success(),
        "resolution failed: {}",
        String::from_utf8_lossy(&resolved.stderr)
    );
    let profile: Value = serde_json::from_slice(&resolved.stdout).expect("connection json");
    assert_eq!(profile["query_uri"], "https://tenant.query.example");
    assert_eq!(profile["semantic_uri"], "https://tenant.s3.example");
    assert_eq!(profile["access_key_id"], "VGACME");
    assert_eq!(profile["secret_access_key"], ENDPOINT_SECRET);
    assert_eq!(profile["bearer_token"], CATALOG_TOKEN);

    #[cfg(unix)]
    for entry in fs::read_dir(root.join("credentials")).expect("credential directory") {
        use std::os::unix::fs::PermissionsExt;
        let mode = entry
            .expect("credential entry")
            .metadata()
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "credential files must be owner-only");
    }
}

#[test]
fn manually_configured_self_host_profile_does_not_require_a_bearer_token() {
    let home = tempdir().expect("temporary home");
    let root = home.path().join(".verglas");
    let credentials = root.join("credentials");
    fs::create_dir_all(&credentials).expect("credential directory");
    fs::write(
        credentials.join("endpoint.ini"),
        "[default]\naws_access_key_id = LOCAL\naws_secret_access_key = local-secret\n",
    )
    .expect("endpoint credentials");
    fs::write(
        root.join("config.toml"),
        r#"
[connection]
query_uri = "http://127.0.0.1:8334"
semantic_uri = "http://127.0.0.1:8333"
region = "us-east-1"
credentials_file = "~/.verglas/credentials/endpoint.ini"
"#,
    )
    .expect("manual config");

    let output = clean_command(home.path())
        .args(["connection", "--json", "--include-secrets"])
        .output()
        .expect("connection resolution runs");
    assert!(
        output.status.success(),
        "resolution failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let profile: Value = serde_json::from_slice(&output.stdout).expect("connection json");
    assert_eq!(profile["access_key_id"], "LOCAL");
    assert_eq!(profile["secret_access_key"], "local-secret");
    assert!(profile.get("bearer_token").is_none() || profile["bearer_token"].is_null());
}
