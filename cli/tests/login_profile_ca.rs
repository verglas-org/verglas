//! Coverage for the optional `ca_certificate` provisioning field (R6-D):
//! when the control plane's `POST /v1/provision` response includes it, the
//! CLI must persist it to `~/.verglas/credentials/ca.pem` (owner-only),
//! record `ca_file` in the `[connection]` profile, and expose `ca_file`
//! through `verglas connection --json`. This is additive to the frozen
//! `login_profile*.rs` / `login_browser.rs` tests, whose mock servers omit
//! the field and must keep writing no file and no profile key.

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
const CA_CERTIFICATE: &str =
    "-----BEGIN CERTIFICATE-----\nMIIBfakeCAcertificatecontents\n-----END CERTIFICATE-----\n";

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

fn one_shot_provision_server_with_ca() -> (String, thread::JoinHandle<String>) {
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
        let escaped_ca = CA_CERTIFICATE.replace('\n', "\\n");
        let body = format!(
            r#"{{"s3_url":"https://tenant.s3.example","catalog_url":"https://tenant.catalog.example","query_url":"https://tenant.query.example","slug":"acme","s3_access_key_id":"VGACME","s3_secret_access_key":"{ENDPOINT_SECRET}","catalog_token":"{CATALOG_TOKEN}","tier":"starter","ca_certificate":"{escaped_ca}"}}"#,
        );
        write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", body.len(), body).expect("write response");
        String::from_utf8(request).expect("request utf8")
    });
    (format!("http://{address}"), handle)
}

#[test]
fn login_persists_ca_certificate_when_present_and_exposes_it_via_connection_json() {
    let home = tempdir().expect("temporary home");
    let (url, request) = one_shot_provision_server_with_ca();
    let output = clean_command(home.path())
        .env("VERGLAS_ACCESS_ENDPOINT", &url)
        .args(["login", "--api-key", CLOUD_API_KEY])
        .output()
        .expect("login runs");
    assert!(
        output.status.success(),
        "login failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    request.join().expect("mock server exits");

    let root = home.path().join(".verglas");
    let ca_path = root.join("credentials").join("ca.pem");
    assert!(ca_path.is_file(), "ca.pem was not written");
    let contents = fs::read_to_string(&ca_path).expect("ca.pem contents");
    assert_eq!(contents, CA_CERTIFICATE);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&ca_path)
            .expect("ca.pem metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "ca.pem must be owner-only");
    }

    let config = fs::read_to_string(root.join("config.toml")).expect("shared config");
    assert!(config.contains("ca_file"), "{config}");
    assert!(
        !config.contains(CA_CERTIFICATE),
        "config must reference the ca file by path, not embed its contents"
    );

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
    assert_eq!(
        profile["ca_file"].as_str().expect("ca_file present"),
        ca_path.to_string_lossy()
    );
}
