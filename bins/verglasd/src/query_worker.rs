//! Dispatches `POST /v1/query` to a standalone `verglas-query` worker,
//! spawned on demand and killed after use, instead of running it embedded.
//!
//! Opt-in via `[query_worker]` in config (unset by default). When configured,
//! this dispatcher is the sole engine for `/v1/query`: a failure to spawn or
//! reach the worker is a hard error. When unset, the daemon serves queries
//! through the embedded engine over the loopback catalog — there is no dual
//! path that retries the other engine.
//!
//! # Streaming, not buffering
//!
//! The worker's own `/v1/query` streams its response body as batches are
//! produced (`bins/query-node/src/admin.rs`), so this dispatcher pipes that
//! stream straight through to the daemon's own caller instead of parsing a
//! complete `QueryReport` and re-serializing it — buffering here would
//! defeat the entire point (peak memory tracking the query's operator
//! working set, not the result size) one hop later. [`QueryWorkerDispatcher::dispatch`]
//! returns an axum [`Response`] built directly from the worker's HTTP
//! response: its status and body stream are relayed as-is, chunk by chunk,
//! with nothing buffered in between. The spawned child stays alive for
//! exactly as long as that body stream is being read — it is moved into the
//! stream's own state and killed (`kill_on_drop`) the moment the stream ends,
//! whether that's a clean finish or the caller disconnecting early.
//!
//! # Why this is its own small dispatcher
//!
//! Rather than routed through the existing workers/jobs framework
//! (`verglas_harness::worker`, `bins/verglasd/src/platform.rs`): that
//! framework's one execution model is subprocess env-in/result-file-out
//! (`WorkerExec`/`RunResult`), shaped for a scheduled or triggered job that
//! writes rows to a table and reports a count — not a synchronous, streamed
//! request that returns typed tabular results. Registering the query role as
//! a `verglas_sys.workers` deployment (so it shows up in `GET /v1/workers`
//! like other workers) is a natural follow-up, not done here.
//!
//! Sizing: the worker is launched with `--for-query <sql>`, so it estimates
//! this exact query's memory need from the query plan and Iceberg table
//! statistics before requesting its own initial grant
//! (`verglas-query`'s `sizing::request_for_query`) — the daemon does not
//! duplicate that estimate here.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use futures::StreamExt;
use tokio::sync::Mutex;
use verglas_iceberg::TimeTravel;

/// Dispatch counter for unique per-launch ports-file names — this process's
/// pid plus a monotonic counter is unique enough without pulling in a UUID
/// dependency for one temp-file name.
static DISPATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// How long a spawned worker gets to report its port before dispatch gives up
/// and the caller falls back to the embedded path.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);

/// Spawns a `verglas-query` worker for one request and returns a `Response`
/// built by relaying the worker's own response — status and streamed body —
/// straight through. Serialized — one dispatch runs at a time, held for the
/// full lifetime of the relayed stream (a `Mutex`, not a pool) — a deliberate
/// v1 scope limit: this launches a fresh process per dispatched query, which
/// is the "launched on demand, killed after use" model the workers framework
/// targets, not yet a warm pool. A later PR can widen this to a small pool of
/// standing workers without changing the wire contract.
pub struct QueryWorkerDispatcher {
    binary: PathBuf,
    config_path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl QueryWorkerDispatcher {
    /// `binary` is the `verglas-query` executable; `config_path` is a TOML
    /// file (rendered once at daemon startup, see `main.rs`) pointing it at
    /// this daemon's own loopback cache/catalog surface with the same signing
    /// keypair the embedded path uses.
    pub fn new(binary: PathBuf, config_path: PathBuf) -> Self {
        QueryWorkerDispatcher {
            binary,
            config_path,
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// Runs `sql` on a freshly launched query worker and returns a `Response`
    /// relaying its own response as-is. `Err` means the spawn/startup/initial
    /// connect failed *before* any bytes came back — a hard error for the
    /// caller when this dispatcher is configured. Anything the worker itself
    /// answered (success or error) comes back as `Ok` and is relayed
    /// verbatim.
    pub async fn dispatch(&self, sql: &str, at: Option<TimeTravel>) -> Result<Response, String> {
        // Owned (not borrowed from `self`), so it can move into the streamed
        // response body below and outlive this call.
        let guard = self.lock.clone().lock_owned().await;

        let dispatch_id = DISPATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let ports_file = std::env::temp_dir().join(format!(
            "verglas-query-worker-{}-{dispatch_id}.ports",
            std::process::id()
        ));
        let mut child = tokio::process::Command::new(&self.binary)
            .arg("--config")
            .arg(&self.config_path)
            .arg("--ports-file")
            .arg(&ports_file)
            .arg("--for-query")
            .arg(sql)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", self.binary.display()))?;

        let port = wait_for_port(&mut child, &ports_file).await;
        let _ = std::fs::remove_file(&ports_file);
        let port = match port {
            Ok(port) => port,
            Err(e) => {
                let _ = child.start_kill();
                return Err(e);
            }
        };

        let response = post_query_streaming(port, sql, at).await;
        let response = match response {
            Ok(response) => response,
            Err(e) => {
                let _ = child.start_kill();
                return Err(e);
            }
        };

        // From here on, the worker answered — relay it verbatim, keeping the
        // child (and the dispatch lock) alive for exactly as long as the
        // caller is still reading the body.
        relay_response(response, (guard, child))
    }
}

/// Builds an axum [`Response`] by relaying `response`'s status and body
/// stream chunk-for-chunk — nothing buffered in between — while `lifetime`
/// stays alive. In production `lifetime` is the dispatch lock's owned guard
/// plus the spawned worker's `Child` (`kill_on_drop`); both drop the instant
/// the relayed stream ends, whether that's a clean finish or the caller
/// disconnecting early. Split out from [`QueryWorkerDispatcher::dispatch`] so
/// the streaming/relay logic itself — the part this exists to prove never
/// buffers a whole body — is testable against a real HTTP response without
/// spawning a subprocess.
pub(crate) fn relay_response(
    response: reqwest::Response,
    lifetime: impl Send + 'static,
) -> Result<Response, String> {
    let status = StatusCode::from_u16(response.status().as_u16())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| HeaderValue::from_str(v).ok());

    let mut upstream = response.bytes_stream();
    let relayed = async_stream::stream! {
        let _lifetime = lifetime;
        while let Some(chunk) = upstream.next().await {
            match chunk {
                Ok(bytes) => yield Ok::<_, std::io::Error>(bytes),
                Err(e) => {
                    tracing::warn!("query worker stream read failed, truncating the relayed response: {e}");
                    return;
                }
            }
        }
    };

    let mut builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    builder
        .body(Body::from_stream(relayed))
        .map_err(|e| format!("could not build the relayed response: {e}"))
}

/// Polls `ports_file` for the `admin <ip:port>` line `verglas-query` appends
/// once it binds (the same convention `verglasd --ports-file` writes), or
/// bails early if the child exits first.
async fn wait_for_port(
    child: &mut tokio::process::Child,
    ports_file: &std::path::Path,
) -> Result<u16, String> {
    let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!(
                "query worker exited before reporting a port ({status})"
            ));
        }
        if let Ok(text) = std::fs::read_to_string(ports_file) {
            for line in text.lines() {
                if let Some(addr) = line.strip_prefix("admin ") {
                    let addr = addr.trim();
                    if let Some(port) = addr.rsplit(':').next().and_then(|p| p.parse::<u16>().ok())
                    {
                        return Ok(port);
                    }
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for the query worker to report its port".to_owned());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The wire request `POST /v1/query` (and this worker's own admin route)
/// accepts — the same shape on both sides of the dispatch.
#[derive(serde::Serialize)]
struct QueryRequest {
    sql: String,
    at: Option<QueryAt>,
}

#[derive(serde::Serialize)]
struct QueryAt {
    reference: String,
    table: String,
}

/// Posts `sql` to the worker's `/v1/query` and returns the raw, not-yet-read
/// `reqwest::Response` — the caller streams its body through rather than
/// buffering it here. Only a request-level failure (connection refused,
/// timeout) is an `Err`; a non-2xx HTTP status is still `Ok` (relayed as-is).
async fn post_query_streaming(
    port: u16,
    sql: &str,
    at: Option<TimeTravel>,
) -> Result<reqwest::Response, String> {
    let body = QueryRequest {
        sql: sql.to_owned(),
        at: at.map(|t| QueryAt {
            reference: t.reference,
            table: t.table,
        }),
    };
    let client = reqwest::Client::new();
    client
        .post(format!("http://127.0.0.1:{port}/v1/query"))
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("query worker request failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixture "worker" that streams a large body over many small, delayed
    /// chunks — enough to force genuinely separate TCP writes rather than
    /// something Nagle/buffering could coalesce into one read. Returns its
    /// bound address and the exact bytes it will send, so the test can
    /// compare the relayed body against ground truth.
    async fn spawn_streaming_fixture() -> (std::net::SocketAddr, Vec<u8>) {
        let chunk = b"0123456789".repeat(100); // 1000 bytes per chunk
        let chunk_count = 50; // 50,000 bytes total — large enough to matter
        let expected: Vec<u8> = chunk.repeat(chunk_count);

        let app = axum::Router::new().route(
            "/stream",
            axum::routing::get(move || {
                let chunk = chunk.clone();
                async move {
                    let body_stream = async_stream::stream! {
                        for _ in 0..chunk_count {
                            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                            yield Ok::<_, std::io::Error>(bytes::Bytes::from(chunk.clone()));
                        }
                    };
                    axum::response::Response::builder()
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from_stream(body_stream))
                        .expect("building a response with a stream body cannot fail")
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (addr, expected)
    }

    /// `relay_response` reproduces the source response's bytes exactly, and —
    /// the property this exists to prove — the relayed body reaches a real
    /// client over more than one chunk, with no single chunk holding most of
    /// the total. That is only possible if the relay reads and re-emits each
    /// upstream chunk as it arrives rather than buffering the whole body
    /// first: a buffered relay would necessarily collapse everything into one
    /// write once the (already fully read) body was ready to send.
    #[tokio::test]
    async fn relay_response_streams_without_buffering() {
        let (fixture_addr, expected) = spawn_streaming_fixture().await;

        let client = reqwest::Client::new();
        let upstream_response = client
            .get(format!("http://{fixture_addr}/stream"))
            .send()
            .await
            .expect("fixture request");

        let relayed = relay_response(upstream_response, ()).expect("relay");
        assert!(relayed.status().is_success());

        // Poll the relayed `Body`'s own data stream directly — this preserves
        // exactly the chunk boundaries `relay_response` yields, the same
        // boundaries a real client downstream of `verglasd`'s admin router
        // would observe on the wire, without needing a second real TCP hop
        // just to observe them.
        let mut stream = relayed.into_body().into_data_stream();
        let mut chunk_count = 0usize;
        let mut largest_chunk = 0usize;
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("chunk");
            chunk_count += 1;
            largest_chunk = largest_chunk.max(chunk.len());
            body.extend_from_slice(&chunk);
        }

        assert_eq!(
            body, expected,
            "relayed bytes must match the source exactly"
        );
        assert!(
            chunk_count > 1,
            "a 50,000-byte relayed body should arrive over more than one chunk, got {chunk_count}"
        );
        assert!(
            largest_chunk < body.len() / 2,
            "no single relayed chunk ({largest_chunk} bytes) should hold most of the \
             {}-byte body — that would mean the relay buffered it first",
            body.len()
        );
    }
}
