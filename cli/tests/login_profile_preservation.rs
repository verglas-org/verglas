//! Regression coverage for non-destructive shared-profile updates.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::thread;

use tempfile::tempdir;

fn provision_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock");
    let address = listener.local_addr().expect("address");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
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
        let body = r#"{"s3_url":"https://s3.example","catalog_url":"https://catalog.example","query_url":"https://query.example","slug":"acme","s3_access_key_id":"KEY","s3_secret_access_key":"secret","catalog_token":"token","tier":"starter"}"#;
        write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", body.len(), body).expect("respond");
    });
    (format!("http://{address}"), handle)
}

#[test]
fn login_updates_only_connection_in_an_explicit_config_path() {
    let home = tempdir().expect("home");
    let config_dir = home.path().join("a-custom-config-directory");
    fs::create_dir_all(&config_dir).expect("config directory");
    let config = config_dir.join("shared.toml");
    fs::write(
        &config,
        r#"
[catalog]
uri = "https://preexisting.catalog.example"
warehouse = "existing"

[unrelated]
keep = "this value"

[connection]
custom_extension = "retain-me"
query_uri = "https://old.query.example"
"#,
    )
    .expect("initial config");
    let (url, server) = provision_server();
    let output = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .env("HOME", home.path())
        .env("VERGLAS_CONFIG", &config)
        .args(["login", "--url", &url, "--api-key", "an-api-key"])
        .output()
        .expect("login runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    server.join().expect("server exits");

    let text = fs::read_to_string(&config).expect("updated config");
    let value: toml::Value = toml::from_str(&text).expect("valid TOML");
    assert_eq!(
        value["catalog"]["uri"].as_str(),
        Some("https://preexisting.catalog.example")
    );
    assert_eq!(value["catalog"]["warehouse"].as_str(), Some("existing"));
    assert_eq!(value["unrelated"]["keep"].as_str(), Some("this value"));
    assert_eq!(
        value["connection"]["custom_extension"].as_str(),
        Some("retain-me")
    );
    assert_eq!(
        value["connection"]["query_uri"].as_str(),
        Some("https://query.example")
    );
    assert!(config_dir.join("credentials/endpoint.ini").is_file());
}

#[cfg(unix)]
#[test]
fn connection_hardens_manually_referenced_secret_files() {
    use std::os::unix::fs::PermissionsExt as _;

    let home = tempdir().expect("home");
    let config = home.path().join("profile.toml");
    let endpoint = home.path().join("endpoint.ini");
    let bearer = home.path().join("bearer-token");
    fs::write(
        &endpoint,
        "[default]\naws_access_key_id = LOCAL\naws_secret_access_key = local-secret\n",
    )
    .expect("endpoint");
    fs::write(&bearer, "local-bearer").expect("bearer");
    fs::set_permissions(&endpoint, fs::Permissions::from_mode(0o644)).expect("endpoint mode");
    fs::set_permissions(&bearer, fs::Permissions::from_mode(0o644)).expect("bearer mode");
    fs::write(
        &config,
        format!(
            "[connection]\nquery_uri = \"http://127.0.0.1:8334\"\nsemantic_uri = \"http://127.0.0.1:8333\"\ncredentials_file = \"{}\"\nbearer_file = \"{}\"\n",
            endpoint.display(), bearer.display()
        ),
    )
    .expect("config");
    let output = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .env("HOME", home.path())
        .env("VERGLAS_CONFIG", &config)
        .args(["connection", "--json", "--include-secrets"])
        .output()
        .expect("connection runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::metadata(endpoint)
            .expect("endpoint metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(bearer)
            .expect("bearer metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}
