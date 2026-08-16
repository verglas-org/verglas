//! Browser-based `verglas login`: bind a loopback callback listener, hand the
//! user to the dashboard authorize page, and collect the one-time code the
//! dashboard redirects back with.
//!
//! The listener is a small explicit state machine: it starts `Listening`,
//! stays there through any number of `Rejected` attempts (wrong state, stray
//! paths such as a browser's favicon probe), and ends at `Matched` the moment
//! a callback presents the exact `state` this run minted. A 120s deadline
//! (enforced via socket-level timeouts on both `accept` and each connection's
//! reads) bounds the whole wait so a login can never hang the terminal
//! forever.

use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;

/// Overall wall-clock budget for the loopback callback to arrive.
const LOGIN_DEADLINE: Duration = Duration::from_secs(120);
/// Per-connection budget for a client to finish sending its request line and
/// headers, bounded independently of the overall deadline so one slow or
/// malicious connection cannot eat the whole login window by trickling bytes.
const CONNECTION_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Hard cap on header bytes read from one connection, guarding against an
/// unbounded read loop from a client that never sends a blank line.
const MAX_HEADER_BYTES: usize = 16 * 1024;

const STATE_LEN: usize = 32;
const STATE_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

const SUCCESS_BODY: &str = "<!doctype html><html><head><title>Verglas CLI</title></head>\
<body><h1>You're signed in.</h1><p>The Verglas CLI has received your authorization. \
You can close this tab and return to your terminal.</p></body></html>";

/// Failures specific to the loopback browser-login flow.
#[derive(Debug, Error)]
pub enum BrowserLoginError {
    /// The CSRF `state` nonce could not be generated from OS randomness.
    #[error("could not generate a random login state: {0}")]
    Random(getrandom::Error),
    /// `--dashboard-url` is not a valid absolute URL.
    #[error("invalid --dashboard-url `{0}`")]
    InvalidDashboardUrl(String),
    /// The loopback listener could not be bound.
    #[error("could not bind a loopback callback listener on 127.0.0.1: {0}")]
    Bind(std::io::Error),
    /// The loopback listener could not accept a connection.
    #[error("could not accept a browser callback connection: {0}")]
    Accept(std::io::Error),
    /// No matching callback arrived before the overall deadline elapsed.
    #[error("timed out after {}s waiting for the browser sign-in to complete", LOGIN_DEADLINE.as_secs())]
    Timeout,
}

/// Runs the full loopback flow: mint a state, print (and optionally open) the
/// dashboard authorize URL, then block until a matching `/callback` arrives or
/// the deadline elapses. Returns the one-time code to exchange at
/// `POST /v1/provision`.
pub async fn await_authorization_code(
    dashboard_url: &str,
    open_browser: bool,
) -> Result<String, BrowserLoginError> {
    let dashboard_url = dashboard_url.trim_end_matches('/');
    if reqwest::Url::parse(dashboard_url).is_err() {
        return Err(BrowserLoginError::InvalidDashboardUrl(
            dashboard_url.to_owned(),
        ));
    }

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(BrowserLoginError::Bind)?;
    let port = listener
        .local_addr()
        .map_err(BrowserLoginError::Bind)?
        .port();

    let state = random_state()?;
    let authorize_url = format!("{dashboard_url}/cli/authorize?state={state}&port={port}");

    note("Open this URL to finish signing in to Verglas:");
    note(format_args!("  {authorize_url}"));
    if open_browser && let Err(error) = open_system_browser(&authorize_url) {
        note(format_args!(
            "warning: could not open a browser automatically ({error}); use the URL above"
        ));
    }
    note(format_args!(
        "Waiting up to {}s for the browser callback...",
        LOGIN_DEADLINE.as_secs()
    ));

    let deadline = Instant::now() + LOGIN_DEADLINE;
    let mut rejected_attempts: u32 = 0;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(BrowserLoginError::Timeout);
        }
        let stream = match tokio::time::timeout(remaining, listener.accept()).await {
            Ok(Ok((stream, _))) => stream,
            Ok(Err(source)) => return Err(BrowserLoginError::Accept(source)),
            Err(_) => return Err(BrowserLoginError::Timeout),
        };
        match handle_connection(stream, &state, deadline).await {
            Some(code) => {
                note("Received a matching callback; exchanging the one-time code...");
                return Ok(code);
            }
            None => {
                rejected_attempts += 1;
                note(format_args!(
                    "Ignored an unmatched callback request (attempt {rejected_attempts}); still listening..."
                ));
            }
        }
    }
}

/// Writes one progress line to stderr, best-effort. `println!`/`eprintln!`
/// panic if the write fails, but the reader on the other end of stderr (a
/// test harness, `| head`, a closed terminal) may legitimately stop reading
/// before this long-lived listener is done producing output; a broken pipe
/// here must never crash a login that is otherwise proceeding correctly.
fn note(message: impl std::fmt::Display) {
    use std::io::Write as _;
    let _ = writeln!(std::io::stderr(), "{message}");
}

/// Serves one connection to completion, responding and returning the code on
/// a matched callback, or responding with a non-2xx status and returning
/// `None` for anything else (wrong state, stray path, malformed request).
/// Connection-level I/O failures are logged and also treated as a rejected
/// attempt: a single bad actor must never abort the pending login.
async fn handle_connection(
    stream: TcpStream,
    expected_state: &str,
    deadline: Instant,
) -> Option<String> {
    let mut reader = BufReader::new(stream);
    let read_deadline = deadline.min(Instant::now() + CONNECTION_READ_TIMEOUT);

    let request_line = match read_line_bounded(&mut reader, read_deadline).await {
        Ok(Some(line)) => line,
        _ => return None,
    };
    let Some((method, target)) = parse_request_line(&request_line) else {
        respond(
            reader.get_mut(),
            400,
            "Bad Request",
            "malformed request line",
        )
        .await;
        return None;
    };
    // Drain and discard headers up to the blank line so the client's write
    // completes cleanly; bounded by both the byte cap and the deadline.
    if drain_headers(&mut reader, read_deadline).await.is_err() {
        respond(reader.get_mut(), 400, "Bad Request", "malformed headers").await;
        return None;
    }

    if method != "GET" {
        respond(
            reader.get_mut(),
            405,
            "Method Not Allowed",
            "only GET is accepted",
        )
        .await;
        return None;
    }

    match evaluate_callback(target, expected_state) {
        CallbackOutcome::Matched { code } => {
            respond(reader.get_mut(), 200, "OK", SUCCESS_BODY).await;
            Some(code)
        }
        CallbackOutcome::NotCallback => {
            respond(reader.get_mut(), 404, "Not Found", "not found").await;
            None
        }
        CallbackOutcome::StateMismatch => {
            respond(reader.get_mut(), 400, "Bad Request", "state mismatch").await;
            None
        }
        CallbackOutcome::MissingCode => {
            respond(
                reader.get_mut(),
                400,
                "Bad Request",
                "missing authorization code",
            )
            .await;
            None
        }
    }
}

/// Classification of one `/callback` request against the login's pending
/// state. The listening -> rejected -> matched progression above is expressed
/// through this pure, independently testable evaluation plus the caller's
/// loop: every non-`Matched` outcome keeps the listener in `Listening`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CallbackOutcome {
    /// `/callback` with the expected `state` and a `code` present.
    Matched { code: String },
    /// Any request whose path is not `/callback` (favicon, health probes...).
    NotCallback,
    /// `/callback` with a missing or non-matching `state`.
    StateMismatch,
    /// `/callback` with the expected `state` but no `code`.
    MissingCode,
}

/// Parses the request-target as a URL (canonical percent-decoding via the
/// `url` crate already pulled in through `reqwest::Url`) and classifies it.
fn evaluate_callback(request_target: &str, expected_state: &str) -> CallbackOutcome {
    let Ok(url) = reqwest::Url::parse(&format!("http://127.0.0.1{request_target}")) else {
        return CallbackOutcome::NotCallback;
    };
    if url.path() != "/callback" {
        return CallbackOutcome::NotCallback;
    }
    let mut code = None;
    let mut state = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "code" => code = Some(value.into_owned()),
            "state" => state = Some(value.into_owned()),
            _ => {}
        }
    }
    if state.as_deref() != Some(expected_state) {
        return CallbackOutcome::StateMismatch;
    }
    match code {
        Some(code) => CallbackOutcome::Matched { code },
        None => CallbackOutcome::MissingCode,
    }
}

/// Splits an HTTP/1.1 request line into its method and request-target.
fn parse_request_line(line: &str) -> Option<(&str, &str)> {
    let mut parts = line.trim_end().splitn(3, ' ');
    let method = parts.next()?;
    let target = parts.next()?;
    parts.next()?; // HTTP version, required but unused
    Some((method, target))
}

/// Reads one CRLF-terminated line, bounded by `deadline` and `MAX_HEADER_BYTES`.
/// Returns `Ok(None)` on a clean EOF before any bytes arrive.
async fn read_line_bounded(
    reader: &mut BufReader<TcpStream>,
    deadline: Instant,
) -> std::io::Result<Option<String>> {
    let mut line = String::new();
    let remaining = deadline.saturating_duration_since(Instant::now());
    let read = tokio::time::timeout(remaining, reader.read_line(&mut line))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read timed out"))??;
    if read == 0 {
        return Ok(None);
    }
    if line.len() > MAX_HEADER_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "request line too large",
        ));
    }
    Ok(Some(line))
}

/// Reads and discards header lines until the blank line that ends them.
async fn drain_headers(
    reader: &mut BufReader<TcpStream>,
    deadline: Instant,
) -> std::io::Result<()> {
    let mut total = 0usize;
    loop {
        match read_line_bounded(reader, deadline).await? {
            None => return Ok(()), // client closed after the request line; nothing more to drain
            Some(line) => {
                total += line.len();
                if total > MAX_HEADER_BYTES {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "headers too large",
                    ));
                }
                if line == "\r\n" || line == "\n" || line.is_empty() {
                    return Ok(());
                }
            }
        }
    }
}

/// Writes a minimal, connection-closing HTTP/1.1 response. Best-effort: a
/// write failure here does not change whether the callback counted.
async fn respond(stream: &mut TcpStream, status: u16, reason: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: text/html; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

/// Mints an unpredictable, URL-safe `state` nonce (>=22 chars required by the
/// authorize contract; 32 gives comfortable margin) from OS randomness via
/// rejection sampling, so every output character is uniformly distributed
/// over the 62-symbol alphabet with no modulo bias.
fn random_state() -> Result<String, BrowserLoginError> {
    let mut state = String::with_capacity(STATE_LEN);
    let mut buffer = [0_u8; 64];
    // 256 % 62 == 8, so bytes >= 248 are rejected to keep the mapping uniform.
    const REJECTION_THRESHOLD: u8 = 248;
    while state.len() < STATE_LEN {
        getrandom::getrandom(&mut buffer).map_err(BrowserLoginError::Random)?;
        for &byte in &buffer {
            if state.len() == STATE_LEN {
                break;
            }
            if byte >= REJECTION_THRESHOLD {
                continue;
            }
            state.push(STATE_ALPHABET[(byte % 62) as usize] as char);
        }
    }
    Ok(state)
}

/// Best-effort system browser launch, per-OS, with no dedicated crate: `open`
/// on macOS, `xdg-open` on Linux, and `cmd /C start` on Windows. The URL is
/// always printed regardless, so a launch failure never strands the user.
fn open_system_browser(url: &str) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open").arg(url).spawn()?;
    }
    #[cfg(target_os = "linux")]
    {
        std::process::Command::new("xdg-open").arg(url).spawn()?;
    }
    #[cfg(target_os = "windows")]
    {
        std::process::Command::new("cmd")
            .args(["/C", "start", "", url])
            .spawn()?;
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no known system browser launcher for this platform",
        ));
    }
    #[allow(unreachable_code)]
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluate_callback_matches_the_expected_state_and_code() {
        let outcome = evaluate_callback("/callback?code=abc123&state=xyz", "xyz");
        assert_eq!(
            outcome,
            CallbackOutcome::Matched {
                code: "abc123".to_owned()
            }
        );
    }

    #[test]
    fn evaluate_callback_percent_decodes_the_code_canonically() {
        let outcome = evaluate_callback("/callback?code=vgc_a%2Bb&state=xyz", "xyz");
        assert_eq!(
            outcome,
            CallbackOutcome::Matched {
                code: "vgc_a+b".to_owned()
            }
        );
    }

    #[test]
    fn evaluate_callback_rejects_a_mismatched_state() {
        let outcome = evaluate_callback("/callback?code=abc123&state=not-it", "xyz");
        assert_eq!(outcome, CallbackOutcome::StateMismatch);
    }

    #[test]
    fn evaluate_callback_rejects_a_missing_state() {
        let outcome = evaluate_callback("/callback?code=abc123", "xyz");
        assert_eq!(outcome, CallbackOutcome::StateMismatch);
    }

    #[test]
    fn evaluate_callback_rejects_a_missing_code_even_with_matching_state() {
        let outcome = evaluate_callback("/callback?state=xyz", "xyz");
        assert_eq!(outcome, CallbackOutcome::MissingCode);
    }

    #[test]
    fn evaluate_callback_ignores_stray_paths() {
        assert_eq!(
            evaluate_callback("/favicon.ico", "xyz"),
            CallbackOutcome::NotCallback
        );
        assert_eq!(evaluate_callback("/", "xyz"), CallbackOutcome::NotCallback);
        assert_eq!(
            evaluate_callback("/callback/nested?state=xyz&code=abc", "xyz"),
            CallbackOutcome::NotCallback
        );
    }

    #[test]
    fn evaluate_callback_rejects_a_malformed_request_target() {
        assert_eq!(
            evaluate_callback("not a valid target \u{0}", "xyz"),
            CallbackOutcome::NotCallback
        );
    }

    #[test]
    fn parse_request_line_extracts_method_and_target() {
        assert_eq!(
            parse_request_line("GET /callback?code=a&state=b HTTP/1.1\r\n"),
            Some(("GET", "/callback?code=a&state=b"))
        );
    }

    #[test]
    fn parse_request_line_rejects_a_truncated_line() {
        assert_eq!(parse_request_line("GET\r\n"), None);
        assert_eq!(parse_request_line(""), None);
    }

    #[test]
    fn random_state_is_long_enough_url_safe_and_not_repeated() {
        let first = random_state().expect("random state generates");
        let second = random_state().expect("random state generates");
        assert!(first.len() >= 22, "state too short: {first}");
        assert!(
            first.chars().all(|c| c.is_ascii_alphanumeric()),
            "state must be url-safe: {first}"
        );
        assert_ne!(first, second, "two draws must not collide");
    }
}
