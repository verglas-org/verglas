//! Frozen acceptance for the browser-based `verglas login` flow: the CLI
//! listens on a loopback callback, hands the user to the dashboard authorize
//! page, and exchanges the returned one-time code at POST /v1/provision with
//! the exact bearer contract the api-key path uses.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use tempfile::tempdir;

const CODE: &str = "vgc_browser-acceptance-code-0123456789";
const ENDPOINT_SECRET: &str = "endpoint-signing-secret";
const CATALOG_TOKEN: &str = "catalog-query-bearer-secret";

fn provision_server() -> (String, thread::JoinHandle<String>) {
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
            r#"{{"s3_url":"https://tenant.fly.dev:8443","catalog_url":"https://tenant.fly.dev","query_url":"https://tenant.fly.dev","slug":"acme","s3_access_key_id":"VG0123456789ABCDEF01","s3_secret_access_key":"{ENDPOINT_SECRET}","catalog_token":"{CATALOG_TOKEN}","tier":"free"}}"#,
        );
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .expect("write response");
        String::from_utf8(request).expect("request utf8")
    });
    (format!("http://{address}"), handle)
}

fn http_get(address: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(address).expect("connect callback listener");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nhost: {address}\r\nconnection: close\r\n\r\n"
    )
    .expect("write callback request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read callback response");
    response
}

#[test]
fn no_browser_login_completes_through_the_loopback_callback() {
    let home = tempdir().expect("temporary home");
    let (url, provision) = provision_server();

    let mut child = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .env("HOME", home.path())
        .env_remove("VERGLAS_ENDPOINT")
        .env_remove("VERGLAS_TOKEN")
        .env_remove("VERGLAS_ACCESS_KEY_ID")
        .env_remove("VERGLAS_SECRET_ACCESS_KEY")
        .env_remove("VERGLAS_REGION")
        .env("VERGLAS_ACCESS_ENDPOINT", &url)
        .env("VERGLAS_DASHBOARD_URL", "https://dashboard.invalid")
        .args(["login", "--no-browser"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("login starts");

    // The CLI prints the authorize URL (state + loopback port) before blocking
    // on the callback.
    let stderr = child.stderr.take().expect("stderr piped");
    let mut authorize_url = String::new();
    let mut kept_lines = Vec::new();
    for line in BufReader::new(stderr).lines() {
        let line = line.expect("stderr line");
        kept_lines.push(line.clone());
        if let Some(index) = line.find("https://dashboard.invalid/cli/authorize?") {
            authorize_url = line[index..]
                .split_whitespace()
                .next()
                .expect("authorize url token")
                .to_owned();
            break;
        }
    }
    assert!(
        !authorize_url.is_empty(),
        "login never printed the authorize URL: {kept_lines:?}"
    );

    let query = authorize_url.split_once('?').expect("query string").1;
    let mut state = None;
    let mut port = None;
    for pair in query.split('&') {
        match pair.split_once('=') {
            Some(("state", value)) => state = Some(value.to_owned()),
            Some(("port", value)) => port = Some(value.to_owned()),
            _ => {}
        }
    }
    let state = state.expect("state parameter");
    let port = port.expect("port parameter");
    assert!(state.len() >= 22, "state too short: {state}");
    let callback = format!("127.0.0.1:{port}");

    // Wrong state and stray paths must not abort the pending login.
    let rejected = http_get(
        &callback,
        &format!("/callback?code={CODE}&state=not-the-state"),
    );
    assert!(
        !rejected.starts_with("HTTP/1.1 2"),
        "mismatched state must be rejected: {rejected}"
    );
    let stray = http_get(&callback, "/favicon.ico");
    assert!(
        !stray.starts_with("HTTP/1.1 2"),
        "stray paths must not complete the login: {stray}"
    );

    let accepted = http_get(&callback, &format!("/callback?code={CODE}&state={state}"));
    assert!(
        accepted.starts_with("HTTP/1.1 2"),
        "matching callback must succeed: {accepted}"
    );

    let status = {
        let mut waited = Duration::ZERO;
        loop {
            if let Some(status) = child.try_wait().expect("poll login") {
                break status;
            }
            assert!(waited < Duration::from_secs(30), "login did not exit");
            thread::sleep(Duration::from_millis(50));
            waited += Duration::from_millis(50);
        }
    };
    assert!(status.success(), "login exited nonzero");

    let request = provision.join().expect("mock control plane exits");
    assert!(request.starts_with("POST /v1/provision "), "{request}");
    assert!(
        request
            .to_ascii_lowercase()
            .contains(&format!("authorization: bearer {CODE}")),
        "exchange must present the one-time code as the bearer: {request}"
    );

    let root = home.path().join(".verglas");
    let config = fs::read_to_string(root.join("config.toml")).expect("shared config");
    assert!(config.contains("[connection]"), "{config}");
    assert!(config.contains("https://tenant.fly.dev:8443"), "{config}");
    for secret in [CODE, ENDPOINT_SECRET, CATALOG_TOKEN] {
        assert!(!config.contains(secret), "config leaked a secret");
    }
    assert!(
        !root
            .join("credentials")
            .join("control-plane-token")
            .exists(),
        "single-use codes must not be persisted"
    );
    assert!(root.join("credentials").join("endpoint.ini").exists());
}
