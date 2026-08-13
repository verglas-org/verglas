//! The startup probe fails fast when a configured backend cannot be reached or
//! authenticated (#233). The server calls [`BackendStore::probe`] before it
//! serves; a credential or reachability failure must surface as an error so the
//! process exits with a clear message rather than serving an empty store.

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::any;
use verglas_backend::BackendStore;
use verglas_core::config::{Backend, BackendProvider, BreakerPolicy, RetryPolicy};

/// A mock origin that answers every request with one fixed status code.
async fn fixed_status(State(status): State<StatusCode>) -> StatusCode {
    status
}

/// Starts a local origin that answers every request with `status` and returns
/// its base URL and the server task.
async fn start_origin(status: StatusCode) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new().fallback(any(fixed_status)).with_state(status);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind origin fixture");
    let endpoint = format!("http://{}", listener.local_addr().expect("fixture address"));
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("origin fixture serves requests");
    });
    (endpoint, server)
}

/// Writes an AWS-format credentials file to a fresh temp path and returns it.
fn write_creds() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "verglas-probe-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("creds");
    std::fs::write(
        &path,
        "[default]\naws_access_key_id = AK\naws_secret_access_key = secret\n",
    )
    .expect("write creds");
    path
}

/// A backend pointed at `endpoint` for bucket `lake`, with a credentials file so
/// the credential chain is `credentials-file` and retries are effectively off
/// (the probe must not spin under a fast test).
fn probe_backend(endpoint: String) -> Backend {
    Backend {
        provider: BackendProvider::S3,
        bucket: Some("lake".to_owned()),
        endpoint: Some(endpoint),
        region: Some("us-east-1".to_owned()),
        allow_http: true,
        credentials_file: Some(write_creds().display().to_string()),
        retry: RetryPolicy {
            max_retries: 0,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
            budget_ms: 50,
        },
        ..Backend::default()
    }
}

/// An origin that rejects credentials (403 on every request) must fail the
/// startup probe, and the error must name the bucket and the credential chain
/// that was tried so the operator can act.
#[tokio::test]
async fn probe_fails_when_the_origin_rejects_credentials() {
    let (endpoint, server) = start_origin(StatusCode::FORBIDDEN).await;
    let store = BackendStore::from_config("default", &probe_backend(endpoint));
    let error = store
        .probe()
        .await
        .expect_err("an origin that rejects credentials must fail the startup probe");
    let message = error.to_string();
    assert!(
        message.contains("lake"),
        "error names the bucket: {message}"
    );
    assert!(
        message.contains("credentials-file"),
        "error names the credential chain tried: {message}"
    );
    server.abort();
}

/// A reachable, authenticated origin passes the probe even when the probe key is
/// absent (a 404 proves the origin answered and the credentials were accepted —
/// an empty bucket is healthy, not a failure).
#[tokio::test]
async fn probe_passes_when_the_origin_answers() {
    let (endpoint, server) = start_origin(StatusCode::NOT_FOUND).await;
    let store = BackendStore::from_config("default", &probe_backend(endpoint));
    store
        .probe()
        .await
        .expect("a reachable authenticated origin passes the probe");
    server.abort();
}

/// A glob-only config has no single named bucket to HEAD, so the probe is a
/// no-op success — it never builds a client for a glob pattern (the factory
/// here panics if the probe touches it) and never blocks such a server.
#[tokio::test]
async fn probe_is_a_noop_without_a_single_bucket() {
    let store = BackendStore::with_glob_factory(
        "default",
        None,
        vec!["*--table-s3".to_owned()],
        64,
        RetryPolicy::default(),
        BreakerPolicy::default(),
        |_bucket| panic!("the probe must not build a client for a glob-only config"),
    );
    store
        .probe()
        .await
        .expect("a glob-only config has no single bucket to probe");
}
