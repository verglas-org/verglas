//! The raw request path must sit behind the SAME per-bucket resilience stack
//! as the typed client (issue #189 review): one concurrency budget (#129) and
//! one circuit breaker + retry policy (#20) per bucket, shared between the two
//! clients — not parallel copies. These tests drive a production-shaped
//! `BackendStore::from_config` against a deterministic mock origin and pin:
//! raw and typed operations draw from one semaphore (a streamed raw GET holds
//! its permit for the body's life), an open breaker fast-fails raw operations
//! without touching the origin, and a transient origin failure is retried
//! under the shared policy.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::Response;
use object_store::path::Path;
use verglas_backend::{BackendStore, BackendStores, RawRange, RawWriteHeaders};
use verglas_core::config::{Backend, BreakerPolicy, RetryPolicy};

/// Shared mock-origin state: per-key origin hit counts and per-key remaining
/// forced failures (a `flaky/` key fails with 500 until its budget drains).
#[derive(Default)]
struct Origin {
    /// Origin requests observed, by key.
    hits: HashMap<String, usize>,
    /// Remaining 500s to serve, by key.
    failures_left: HashMap<String, usize>,
    /// Stored object bodies, by key.
    objects: HashMap<String, Vec<u8>>,
}

/// The shared origin state handle.
type State_ = Arc<Mutex<Origin>>;

/// Total origin hits for `key`.
fn hits(state: &State_, key: &str) -> usize {
    *state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .hits
        .get(key)
        .unwrap_or(&0)
}

/// Arms `key` to answer 500 for its next `n` requests.
fn arm_failures(state: &State_, key: &str, n: usize) {
    state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .failures_left
        .insert(key.to_owned(), n);
}

/// Seeds an object directly into the origin.
fn seed(state: &State_, key: &str, body: &[u8]) {
    state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .objects
        .insert(key.to_owned(), body.to_vec());
}

/// The mock origin: counts hits per key, serves armed 500s first, then plain
/// PUT/GET/HEAD semantics. `boom/` keys always answer 500.
async fn handler(State(state): State<State_>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_owned();
    let after_bucket = path.strip_prefix('/').unwrap_or(&path);
    let key = match after_bucket.split_once('/') {
        Some((_bucket, key)) => percent_encoding::percent_decode_str(key)
            .decode_utf8_lossy()
            .into_owned(),
        None => String::new(),
    };

    {
        let mut guard = state.lock().unwrap_or_else(|e| e.into_inner());
        *guard.hits.entry(key.clone()).or_insert(0) += 1;
        if key.starts_with("boom/") {
            return status_only(StatusCode::INTERNAL_SERVER_ERROR);
        }
        if let Some(left) = guard.failures_left.get_mut(&key)
            && *left > 0
        {
            *left -= 1;
            return status_only(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    match parts.method {
        Method::PUT => {
            let bytes = axum::body::to_bytes(body, 16 * 1024 * 1024)
                .await
                .expect("put body");
            state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .objects
                .insert(key, bytes.to_vec());
            Response::builder()
                .status(StatusCode::OK)
                .header("etag", "\"e\"")
                .body(Body::empty())
                .expect("put response")
        }
        Method::GET | Method::HEAD => {
            let object = state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .objects
                .get(&key)
                .cloned();
            match object {
                Some(bytes) => {
                    let builder = Response::builder()
                        .status(StatusCode::OK)
                        .header("etag", "\"e\"")
                        .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                        .header("content-length", bytes.len().to_string());
                    let body = if parts.method == Method::HEAD {
                        Body::empty()
                    } else {
                        Body::from(bytes)
                    };
                    builder.body(body).expect("object response")
                }
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("<Error><Code>NoSuchKey</Code></Error>"))
                    .expect("not found"),
            }
        }
        _ => status_only(StatusCode::METHOD_NOT_ALLOWED),
    }
}

/// A bare status-code response.
fn status_only(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .body(Body::empty())
        .expect("status response")
}

/// Boots the mock origin once and points the AWS env at it.
fn origin() -> State_ {
    static STATE: OnceLock<State_> = OnceLock::new();
    STATE
        .get_or_init(|| {
            let state: State_ = Arc::new(Mutex::new(Origin::default()));
            let server_state = state.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build runtime");
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind origin");
                    let addr = listener.local_addr().expect("origin addr");
                    tx.send(addr).expect("send addr");
                    let app = Router::new().fallback(handler).with_state(server_state);
                    axum::serve(listener, app).await.expect("serve origin");
                });
            });
            let addr = rx.recv().expect("recv addr");
            // SAFETY: set exactly once before any client reads it.
            unsafe {
                std::env::set_var("AWS_ENDPOINT", format!("http://{addr}"));
                std::env::set_var("AWS_ACCESS_KEY_ID", "k");
                std::env::set_var("AWS_SECRET_ACCESS_KEY", "s");
                std::env::set_var("AWS_ALLOW_HTTP", "true");
                std::env::set_var("AWS_REGION", "us-east-1");
                std::env::set_var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false");
            }
            state
        })
        .clone()
}

/// A no-retry, tiny-backoff policy so failures surface immediately.
fn no_retry() -> RetryPolicy {
    RetryPolicy {
        max_retries: 0,
        initial_backoff_ms: 1,
        max_backoff_ms: 1,
        budget_ms: 1_000,
    }
}

/// A breaker that trips on the very first unhealthy outcome and stays open.
fn hair_trigger_breaker() -> BreakerPolicy {
    BreakerPolicy {
        failure_rate: 0.5,
        min_samples: 1,
        cooldown_ms: 60_000,
        half_open_max_probes: 1,
    }
}

/// Raw and typed operations must draw from ONE per-bucket concurrency budget:
/// with a budget of 1, a raw GET whose body is still open holds the only
/// permit, so a typed request must wait — and proceed once the stream drops.
#[tokio::test]
async fn raw_and_typed_ops_share_one_concurrency_budget() {
    let state = origin();
    seed(&state, "shared/one", b"0123456789");
    let backend = Backend {
        max_concurrent_requests: 1,
        retry: no_retry(),
        bucket: Some("shared-bucket".to_owned()),
        ..Backend::default()
    };
    let registry = BackendStore::from_config(&backend);
    let raw = registry
        .raw_for("shared-bucket")
        .expect("raw_for")
        .expect("raw client");
    let typed = registry.store_for("shared-bucket").expect("typed store");

    // The raw GET holds the bucket's only permit while its body is open.
    let held = raw
        .get("shared/one", RawRange::Full)
        .await
        .expect("raw get");

    // A typed request against the SAME bucket must block on the shared budget.
    let waited = tokio::time::timeout(
        Duration::from_millis(300),
        typed.get_opts(
            &Path::from("shared/one"),
            object_store::GetOptions::default(),
        ),
    )
    .await;
    assert!(
        waited.is_err(),
        "typed op must wait while a raw stream holds the shared permit"
    );

    // Dropping the raw body releases the permit; the typed request proceeds.
    drop(held);
    let unblocked = tokio::time::timeout(
        Duration::from_millis(2_000),
        typed.get_opts(
            &Path::from("shared/one"),
            object_store::GetOptions::default(),
        ),
    )
    .await;
    assert!(
        unblocked.is_ok_and(|r| r.is_ok()),
        "typed op must proceed once the raw stream releases its permit"
    );
}

/// With a budget of N, the N+1th raw operation waits until one of the N open
/// raw GET streams drops its permit.
#[tokio::test]
async fn n_plus_first_raw_get_waits_for_a_permit() {
    let state = origin();
    seed(&state, "queue/a", b"aaaa");
    seed(&state, "queue/b", b"bbbb");
    let backend = Backend {
        max_concurrent_requests: 2,
        retry: no_retry(),
        bucket: Some("queue-bucket".to_owned()),
        ..Backend::default()
    };
    let registry = BackendStore::from_config(&backend);
    let raw = registry
        .raw_for("queue-bucket")
        .expect("raw_for")
        .expect("raw client");

    let first = raw.get("queue/a", RawRange::Full).await.expect("get a");
    let second = raw.get("queue/b", RawRange::Full).await.expect("get b");

    let waited = tokio::time::timeout(Duration::from_millis(300), raw.head("queue/a")).await;
    assert!(
        waited.is_err(),
        "the N+1th raw op must wait while N raw streams hold every permit"
    );

    drop(first);
    let unblocked = tokio::time::timeout(Duration::from_millis(2_000), raw.head("queue/a")).await;
    assert!(
        unblocked.is_ok_and(|r| r.is_ok()),
        "the queued raw op must proceed once a permit frees"
    );
    drop(second);
}

/// An open breaker fast-fails raw operations without touching the origin: the
/// first (unretried) failure trips the hair-trigger breaker, and the next raw
/// call is shed — origin hit count unchanged, shed error named.
#[tokio::test]
async fn breaker_open_fast_fails_raw_ops() {
    let state = origin();
    let backend = Backend {
        retry: no_retry(),
        breaker: hair_trigger_breaker(),
        bucket: Some("breaker-bucket".to_owned()),
        ..Backend::default()
    };
    let registry = BackendStore::from_config(&backend);
    let raw = registry
        .raw_for("breaker-bucket")
        .expect("raw_for")
        .expect("raw client");

    // Trip: one transient failure, no retries, hair-trigger threshold.
    let tripped = raw.head("boom/key").await;
    assert!(tripped.is_err(), "the arming request must fail");
    let hits_after_trip = hits(&state, "boom/key");

    // Shed: the breaker is open, so the origin must not be touched.
    let shed = raw.head("boom/key").await;
    let message = match shed {
        Err(error) => error.to_string(),
        Ok(_) => panic!("an open breaker must shed the request"),
    };
    assert!(
        message.contains("circuit breaker open"),
        "shed error must name the open breaker; got: {message}"
    );
    assert_eq!(
        hits(&state, "boom/key"),
        hits_after_trip,
        "a shed raw request must never reach the origin"
    );
}

/// A transient origin failure is retried under the shared policy: two armed
/// 500s then success means exactly three origin hits and an Ok result.
#[tokio::test]
async fn raw_retry_then_succeed_on_a_flaky_origin() {
    let state = origin();
    seed(&state, "flaky/key", b"payload");
    arm_failures(&state, "flaky/key", 2);
    let backend = Backend {
        retry: RetryPolicy {
            max_retries: 3,
            initial_backoff_ms: 1,
            max_backoff_ms: 2,
            budget_ms: 10_000,
        },
        bucket: Some("retry-bucket".to_owned()),
        ..Backend::default()
    };
    let registry = BackendStore::from_config(&backend);
    let raw = registry
        .raw_for("retry-bucket")
        .expect("raw_for")
        .expect("raw client");

    let meta = raw.head("flaky/key").await.expect("retried head");
    assert_eq!(meta.size, "payload".len() as u64, "meta after retries");
    assert_eq!(
        hits(&state, "flaky/key"),
        3,
        "two 500s + one success = exactly three origin attempts"
    );
}

/// Raw writes are retried too (the buffered slice makes a PUT re-issuable):
/// an armed 500 then success stores the object.
#[tokio::test]
async fn raw_put_retries_and_lands() {
    let state = origin();
    arm_failures(&state, "flaky/put", 1);
    let backend = Backend {
        retry: RetryPolicy {
            max_retries: 2,
            initial_backoff_ms: 1,
            max_backoff_ms: 2,
            budget_ms: 10_000,
        },
        bucket: Some("retry-put-bucket".to_owned()),
        ..Backend::default()
    };
    let registry = BackendStore::from_config(&backend);
    let raw = registry
        .raw_for("retry-put-bucket")
        .expect("raw_for")
        .expect("raw client");

    raw.put(
        "flaky/put",
        RawWriteHeaders::default(),
        vec![bytes::Bytes::from_static(b"retried-bytes")],
    )
    .await
    .expect("retried put");
    let stored = state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .objects
        .get("flaky/put")
        .cloned();
    assert_eq!(
        stored.as_deref(),
        Some(b"retried-bytes".as_ref()),
        "the retried PUT must land the exact bytes"
    );
    assert_eq!(hits(&state, "flaky/put"), 2, "one 500 + one success");
}
