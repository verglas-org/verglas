//! Contract tests for `verglas token` (scoped access tokens) and the CLI's
//! transparent machine-token renewal: an actively used login never expires.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::thread;

use tempfile::tempdir;

/// Mock control plane serving `count` sequential one-request connections.
/// Returns every captured request head+body, in order.
fn serving(
    count: usize,
    responses: Vec<(&'static str, &'static str)>,
) -> (String, thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock");
    let address = listener.local_addr().expect("address");
    let handle = thread::spawn(move || {
        let mut captured = Vec::new();
        for (status, body) in responses.into_iter().take(count) {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 8192];
            let mut content_length = 0usize;
            loop {
                let read = stream.read(&mut buffer).expect("read");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if let Some(head_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&request[..head_end]).to_lowercase();
                    if let Some(line) = head.lines().find(|l| l.starts_with("content-length:")) {
                        content_length =
                            line["content-length:".len()..].trim().parse().unwrap_or(0);
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
            captured.push(String::from_utf8_lossy(&request).into_owned());
        }
        captured
    });
    (format!("http://{address}"), handle)
}

/// Writes the endpoint-keyed credential store with a token of the given age.
fn write_store(home: &std::path::Path, endpoint: &str, token: &str, age_days: u64) {
    let issued = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
        - age_days * 24 * 3600;
    let dir = home.join(".config").join("verglas");
    fs::create_dir_all(&dir).expect("store dir");
    let store = dir.join("credentials.json");
    fs::write(
        &store,
        format!(
            r#"{{"tokens":{{"{}":{{"token":"{token}","token_id":"provisioned","issued_at":{issued}}}}}}}"#,
            endpoint.trim_end_matches('/')
        ),
    )
    .expect("store");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(&store, fs::Permissions::from_mode(0o600)).expect("mode");
    }
}

fn command(home: &std::path::Path, url: &str) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_verglas"));
    cmd.env("HOME", home)
        .env_remove("VERGLAS_TOKEN")
        .env("VERGLAS_ACCESS_ENDPOINT", url);
    cmd
}

#[test]
fn token_create_posts_name_and_scopes_and_prints_the_token_once() {
    let home = tempdir().expect("home");
    let (url, server) = serving(
        1,
        vec![(
            "201 Created",
            r#"{"token":"vgs_scoped-secret","token_id":"tok_1"}"#,
        )],
    );
    write_store(home.path(), &url, "vgt_machine", 1);
    let output = command(home.path(), &url)
        .args([
            "token",
            "create",
            "producer",
            "--scope",
            "ingest:app_logs",
            "--scope",
            "sql:read",
        ])
        .output()
        .expect("runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.join().expect("server");
    assert!(
        requests[0]
            .starts_with("POST /v0/tokens?name=producer&scope=ingest%3Aapp_logs&scope=sql%3Aread "),
        "{}",
        requests[0]
    );
    assert!(
        requests[0]
            .to_lowercase()
            .contains("authorization: bearer vgt_machine"),
        "{}",
        requests[0]
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("vgs_scoped-secret"), "{stdout}");
}

#[test]
fn token_list_shows_names_and_scopes_never_values() {
    let home = tempdir().expect("home");
    let (url, server) = serving(
        1,
        vec![(
            "200 OK",
            r#"{"tokens":[{"id":"tok_1","name":"producer","token":"vgs_pre…","scopes":["ingest:app_logs"],"created_at":"2026-08-17T00:00:00Z"}]}"#,
        )],
    );
    write_store(home.path(), &url, "vgt_machine", 1);
    let output = command(home.path(), &url)
        .args(["token", "list"])
        .output()
        .expect("runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.join().expect("server");
    assert!(
        requests[0].starts_with("GET /v0/tokens "),
        "{}",
        requests[0]
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("tok_1"), "{stdout}");
    assert!(stdout.contains("producer"), "{stdout}");
    assert!(stdout.contains("ingest:app_logs"), "{stdout}");
    assert!(
        !stdout.contains("vgs_pre"),
        "even truncated token values stay out of list output: {stdout}"
    );
}

#[test]
fn token_revoke_deletes_by_id() {
    let home = tempdir().expect("home");
    let (url, server) = serving(1, vec![("200 OK", r#"{"ok":true}"#)]);
    write_store(home.path(), &url, "vgt_machine", 1);
    let output = command(home.path(), &url)
        .args(["token", "revoke", "tok_1"])
        .output()
        .expect("runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.join().expect("server");
    assert!(
        requests[0].starts_with("DELETE /v0/tokens/tok_1 "),
        "{}",
        requests[0]
    );
}

#[test]
fn a_token_past_half_life_renews_transparently_before_the_command() {
    let home = tempdir().expect("home");
    let (url, server) = serving(
        2,
        vec![
            (
                "200 OK",
                r#"{"api_token":"vgt_renewed","expires_in":2592000}"#,
            ),
            ("200 OK", r#"{"tokens":[]}"#),
        ],
    );
    // 16 days old: past the 15-day half-life, still valid.
    write_store(home.path(), &url, "vgt_aging", 16);
    let output = command(home.path(), &url)
        .args(["token", "list"])
        .output()
        .expect("runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.join().expect("server");
    // First the renewal, presented with the aging token…
    assert!(
        requests[0].starts_with("POST /v1/tokens/renew "),
        "{}",
        requests[0]
    );
    assert!(
        requests[0]
            .to_lowercase()
            .contains("authorization: bearer vgt_aging"),
        "{}",
        requests[0]
    );
    // …then the command itself, already using the renewed token.
    assert!(
        requests[1]
            .to_lowercase()
            .contains("authorization: bearer vgt_renewed"),
        "{}",
        requests[1]
    );
    // The store now holds the renewed token.
    let store = fs::read_to_string(
        home.path()
            .join(".config")
            .join("verglas")
            .join("credentials.json"),
    )
    .expect("store");
    assert!(store.contains("vgt_renewed"), "{store}");
    assert!(!store.contains("vgt_aging"), "{store}");
}

#[test]
fn a_fresh_token_does_not_renew() {
    let home = tempdir().expect("home");
    let (url, server) = serving(1, vec![("200 OK", r#"{"tokens":[]}"#)]);
    write_store(home.path(), &url, "vgt_fresh", 2);
    let output = command(home.path(), &url)
        .args(["token", "list"])
        .output()
        .expect("runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.join().expect("server");
    assert!(
        requests[0].starts_with("GET /v0/tokens "),
        "{}",
        requests[0]
    );
}
