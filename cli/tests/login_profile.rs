//! Contract tests for the shared Verglas login profile consumed by integrations.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::thread;

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
            r#"{{"s3_url":"https://tenant.s3.example","catalog_url":"https://tenant.catalog.example","query_url":"https://tenant.query.example","slug":"acme","s3_access_key_id":"VGACME","s3_secret_access_key":"{ENDPOINT_SECRET}","catalog_token":"{CATALOG_TOKEN}","tier":"starter","tenant_id":"11111111-2222-4333-8444-555555555555","api_token":"vgt_durable-machine-token"}}"#,
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
        .env("VERGLAS_ACCESS_ENDPOINT", &url)
        .args(["login", "--api-key", CLOUD_API_KEY])
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
    assert!(config.contains("https://tenant.s3.example"), "{config}");
    for secret in [CLOUD_API_KEY, ENDPOINT_SECRET, CATALOG_TOKEN] {
        assert!(!config.contains(secret), "config leaked a secret");
        assert!(
            !String::from_utf8_lossy(&output.stdout).contains(secret),
            "stdout leaked a secret"
        );
    }

    let profile: toml::Value =
        toml::from_str(&fs::read_to_string(root.join("config.toml")).expect("config"))
            .expect("config parses");
    let connection = &profile["connection"];
    assert_eq!(
        connection["semantic_uri"].as_str(),
        Some("https://tenant.s3.example")
    );
    // The tenant id from provisioning is recorded so `verglas lakehouse` can
    // address the control plane's tenant-scoped routes.
    assert_eq!(
        connection["tenant_id"].as_str(),
        Some("11111111-2222-4333-8444-555555555555")
    );

    // The durable machine token from provisioning lands in the endpoint-keyed
    // credential store, so tenant-scoped commands resolve a bearer with no
    // VERGLAS_TOKEN in the environment.
    let store = fs::read_to_string(
        home.path()
            .join(".config")
            .join("verglas")
            .join("credentials.json"),
    )
    .expect("credential store");
    assert!(store.contains("vgt_durable-machine-token"), "{store}");
    for secret in [CLOUD_API_KEY, ENDPOINT_SECRET, CATALOG_TOKEN] {
        assert!(!store.contains(secret), "store leaked an unrelated secret");
    }
    let endpoint_ini =
        fs::read_to_string(connection["credentials_file"].as_str().expect("creds path"))
            .expect("endpoint credentials file");
    assert!(endpoint_ini.contains("VGACME"), "{endpoint_ini}");
    assert!(endpoint_ini.contains(ENDPOINT_SECRET), "{endpoint_ini}");
    let bearer = fs::read_to_string(connection["bearer_file"].as_str().expect("bearer path"))
        .expect("bearer file");
    assert_eq!(bearer, CATALOG_TOKEN);

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
