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

/// Seeds the durable control-plane token `verglas login` writes, which
/// `auth::resolved_bearer` falls back to. HOME-based, so it does not depend
/// on XDG_CONFIG_HOME (which CI runners set and developer machines do not).
fn write_store(home: &std::path::Path, _endpoint: &str, token: &str, _age_days: u64) {
    let dir = home.join(".verglas").join("credentials");
    fs::create_dir_all(&dir).expect("credentials dir");
    let path = dir.join("control-plane-token");
    fs::write(&path, token).expect("durable token");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("mode");
    }
}

fn command(home: &std::path::Path, url: &str) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_verglas"));
    cmd.env("HOME", home)
        .env_remove("VERGLAS_TOKEN")
        // CI runners set XDG_CONFIG_HOME; leaving it through would point the
        // credential store outside the test's temporary HOME.
        .env_remove("XDG_CONFIG_HOME")
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
            r#"{"token":"vgs_scoped-secret","token_id":"tok_1","name":"producer","scopes":["ingest:app_logs","sql:read"]}"#,
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
fn token_create_json_round_trips_every_granted_scope() {
    // Regression for the control plane once honoring only the first ?scope=
    // param: the response's scopes must reflect the full granted set.
    let home = tempdir().expect("home");
    let (url, server) = serving(
        1,
        vec![(
            "201 Created",
            r#"{"token":"vgs_scoped-secret","token_id":"tok_1","name":"producer","scopes":["ingest:app_logs","sql:read"]}"#,
        )],
    );
    write_store(home.path(), &url, "vgt_machine", 1);
    let output = command(home.path(), &url)
        .args([
            "--json",
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
    server.join().expect("server");
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json output");
    let scopes: Vec<&str> = parsed["scopes"]
        .as_array()
        .expect("scopes array")
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect();
    assert_eq!(scopes, ["ingest:app_logs", "sql:read"]);
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
