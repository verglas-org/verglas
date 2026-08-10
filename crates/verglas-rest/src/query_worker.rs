//! Dispatches database-scoped SQL to a standalone `verglas-query` worker,
//! spawned on demand and killed after use, instead of running it embedded.
//!
//! Opt-in via `[query_worker]` in config (unset by default). When configured,
//! this dispatcher is the sole engine for `/v1/databases/{database}/query`: a failure to spawn or
//! reach the worker is a hard error. When unset, the server serves queries
//! through the embedded engine over the private upstream catalog — there is no dual
//! path that retries the other engine.
//!
//! # Streaming, not buffering
//!
//! The worker's own `/v1/query` streams its response body as batches are
//! produced (`bins/query-node/src/admin.rs`), so this dispatcher pipes that
//! stream straight through to the server's own caller instead of parsing a
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
//! (`verglas_harness::worker`, `bins/verglas-server/src/platform.rs`): that
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
//! (`verglas-query`'s `sizing::request_for_query`) — the server does not
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
use verglas_catalog::{CatalogRuntimeRegistry, DatabaseId};
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
    runtime: QueryWorkerRuntimeConfig,
    catalogs: CatalogRuntimeRegistry,
    lock: Arc<Mutex<()>>,
}

/// Static settings used to render one query-worker config per Lakehouse database.
#[derive(Debug, Clone)]
pub struct QueryWorkerRuntimeConfig {
    /// Directory that receives database-specific TOML files.
    pub config_dir: PathBuf,
    /// Cache-routed S3 endpoint used for every object read.
    pub cache_s3_endpoint: String,
    /// Region passed to the S3 client.
    pub region: String,
    /// Restricted credentials file for the cache S3 endpoint.
    pub credentials_file: PathBuf,
    /// Admin origin containing `/v1/databases/{database}/catalog`.
    pub admin_origin: String,
}

impl QueryWorkerRuntimeConfig {
    /// Renders the isolated worker configuration for one validated live database.
    fn render(&self, database: &DatabaseId) -> Result<PathBuf, String> {
        std::fs::create_dir_all(&self.config_dir)
            .map_err(|error| format!("create {}: {error}", self.config_dir.display()))?;
        let config_path = self.config_dir.join(format!("{}.toml", database.as_str()));
        let rendered = format!(
            "[listen]\nadmin_port = 0\n\n\
             [cache]\ns3_endpoint = \"{}\"\nregion = \"{}\"\ncredentials_file = \"{}\"\n\n\
             [metadata]\nuri = \"{}/v1/databases/{}/catalog\"\n",
            self.cache_s3_endpoint,
            self.region,
            self.credentials_file.display(),
            self.admin_origin.trim_end_matches('/'),
            database.as_str(),
        );
        std::fs::write(&config_path, rendered)
            .map_err(|error| format!("write query role config: {error}"))?;
        Ok(config_path)
    }
}

impl QueryWorkerDispatcher {
    /// Creates a dispatcher using the live Lakehouse registry as its activation boundary.
    pub fn new(
        binary: PathBuf,
        runtime: QueryWorkerRuntimeConfig,
        catalogs: CatalogRuntimeRegistry,
    ) -> Self {
        QueryWorkerDispatcher {
            binary,
            runtime,
            catalogs,
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// Reports whether a Lakehouse database currently has a live catalog runtime.
    pub fn has_database(&self, database: &DatabaseId) -> bool {
        self.catalogs.get(database).is_some()
    }

    /// Runs `sql` on a freshly launched query worker and returns a `Response`
    /// relaying its own response as-is. `Err` means the spawn/startup/initial
    /// connect failed *before* any bytes came back — a hard error for the
    /// caller when this dispatcher is configured. Anything the worker itself
    /// answered (success or error) comes back as `Ok` and is relayed
    /// verbatim.
    pub async fn dispatch(
        &self,
        database: &DatabaseId,
        sql: &str,
        at: Option<TimeTravel>,
        accept_arrow: bool,
        bearer_token: &str,
    ) -> Result<Response, String> {
        if bearer_token.is_empty() {
            return Err("query run bearer must not be empty".to_owned());
        }
        if !self.has_database(database) {
            return Err(format!(
                "database `{}` has no Lakehouse query runtime",
                database.as_str()
            ));
        }
        // Owned (not borrowed from `self`), so it can move into the streamed
        // response body below and outlive this call.
        let guard = self.lock.clone().lock_owned().await;
        let config_path = self.runtime.render(database)?;

        let dispatch_id = DISPATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let ports_file = std::env::temp_dir().join(format!(
            "verglas-query-worker-{}-{dispatch_id}.ports",
            std::process::id()
        ));
        let mut child = self
            .worker_command(&config_path, &ports_file, sql, bearer_token)
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

        let response = post_query_streaming(port, sql, at, accept_arrow).await;
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

    /// Builds one ephemeral child command with the scoped bearer only in its inherited environment.
    fn worker_command(
        &self,
        config_path: &std::path::Path,
        ports_file: &std::path::Path,
        sql: &str,
        bearer_token: &str,
    ) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(&self.binary);
        command
            .arg("--config")
            .arg(config_path)
            .arg("--ports-file")
            .arg(ports_file)
            .arg("--for-query")
            .arg(sql)
            .env(verglas_core::RUN_BEARER_TOKEN_ENV, bearer_token);
        command
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
/// once it binds (the same convention `verglas-server --ports-file` writes), or
/// bails early if the child exits first.
pub(crate) async fn wait_for_port(
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
    accept_arrow: bool,
) -> Result<reqwest::Response, String> {
    let body = QueryRequest {
        sql: sql.to_owned(),
        at: at.map(|t| QueryAt {
            reference: t.reference,
            table: t.table,
        }),
    };
    let mut request = reqwest::Client::new().post(format!("http://127.0.0.1:{port}/v1/query"));
    if accept_arrow {
        request = request.header(
            reqwest::header::ACCEPT,
            verglas_sdk::ARROW_STREAM_CONTENT_TYPE,
        );
    }
    request
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("query worker request failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Per-database configs target the corresponding catalog mount and never a singleton route.
    #[test]
    fn runtime_config_is_database_scoped() {
        let dir = tempfile::tempdir().expect("config dir");
        let runtime = QueryWorkerRuntimeConfig {
            config_dir: dir.path().to_owned(),
            cache_s3_endpoint: "http://127.0.0.1:8333".to_owned(),
            region: "auto".to_owned(),
            credentials_file: dir.path().join("credentials"),
            admin_origin: "http://127.0.0.1:8334".to_owned(),
        };
        let database = DatabaseId::new("analytics").expect("database id");

        let path = runtime.render(&database).expect("render config");
        let config = std::fs::read_to_string(path).expect("read config");

        assert!(config.contains("uri = \"http://127.0.0.1:8334/v1/databases/analytics/catalog\""));
        assert!(!config.contains("8334/catalog\""));
    }

    /// A non-Lakehouse database is rejected before attempting to spawn a process.
    #[tokio::test]
    async fn dispatch_requires_a_live_database_catalog() {
        let dir = tempfile::tempdir().expect("config dir");
        let dispatcher = QueryWorkerDispatcher::new(
            PathBuf::from("/binary/that/must/not/be/spawned"),
            QueryWorkerRuntimeConfig {
                config_dir: dir.path().to_owned(),
                cache_s3_endpoint: "http://127.0.0.1:8333".to_owned(),
                region: "auto".to_owned(),
                credentials_file: dir.path().join("credentials"),
                admin_origin: "http://127.0.0.1:8334".to_owned(),
            },
            CatalogRuntimeRegistry::default(),
        );
        let database = DatabaseId::new("postgres_only").expect("database id");

        let error = dispatcher
            .dispatch(&database, "SELECT 1", None, false, "scoped-test-token")
            .await
            .expect_err("missing Lakehouse must fail");

        assert_eq!(
            error,
            "database `postgres_only` has no Lakehouse query runtime"
        );
    }

    /// The scoped bearer is child-only environment state, never serialized or exposed in argv.
    #[test]
    fn worker_command_keeps_bearer_out_of_config_and_arguments() {
        let dir = tempfile::tempdir().expect("config dir");
        let runtime = QueryWorkerRuntimeConfig {
            config_dir: dir.path().join("databases"),
            cache_s3_endpoint: "http://127.0.0.1:8333".to_owned(),
            region: "auto".to_owned(),
            credentials_file: dir.path().join("credentials"),
            admin_origin: "http://127.0.0.1:8334".to_owned(),
        };
        let database = DatabaseId::new("analytics").expect("database id");
        let config_path = runtime.render(&database).expect("render config");
        let dispatcher = QueryWorkerDispatcher::new(
            PathBuf::from("verglas-query"),
            runtime,
            CatalogRuntimeRegistry::default(),
        );
        let token = "scoped-token-that-must-not-persist";
        let command =
            dispatcher.worker_command(&config_path, &dir.path().join("ports"), "SELECT 1", token);
        let command = command.as_std();
        let arguments = command
            .get_args()
            .map(|value| value.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        let inherited = command
            .get_envs()
            .find(|(name, _)| *name == std::ffi::OsStr::new(verglas_core::RUN_BEARER_TOKEN_ENV))
            .and_then(|(_, value)| value)
            .map(|value| value.to_string_lossy().into_owned());
        let serialized = std::fs::read_to_string(config_path).expect("read config");

        assert_eq!(inherited.as_deref(), Some(token));
        assert!(!arguments.contains(token));
        assert!(!serialized.contains(token));
        assert!(!serialized.contains(verglas_core::RUN_BEARER_TOKEN_ENV));
    }

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
        // boundaries a real client downstream of `verglas-server`'s admin router
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
