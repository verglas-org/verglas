//! The query role's HTTP surface: `POST /v1/query`, `POST /v1/query/estimate`,
//! and `GET /healthz`.
//!
//! `/v1/query`'s response is a *stream*, not a buffered body: it is built from
//! `verglas_iceberg::query_stream`, which returns Arrow `RecordBatch`es as
//! DataFusion's execution produces them rather than collecting the whole
//! result first (see that function's doc comment). This handler writes one
//! JSON chunk per batch as it arrives — `{"columns":[...],"rows":[` first,
//! then each batch's rows, then `],"row_count":N}` once the stream is
//! exhausted — so the *shape* on the wire is exactly the same
//! `{columns, rows, row_count}` object `verglas-server`'s embedded `/v1/query`
//! returns in one shot, byte-for-byte reconstructable by any client that
//! reads to EOF and parses the whole body as JSON, but nothing on the server
//! side ever holds the whole result in memory to produce it. Peak worker
//! memory during a query therefore tracks its operator working set (a join's
//! hash table, a sort's buffer) — the thing `crates/verglas-iceberg/src/estimate.rs`
//! actually estimates — not the result size. A result small enough to fit in
//! one TCP write behaves identically to a buffered response; the point is
//! that nothing *requires* full materialization for a large one.
//!
//! The request wire shape (`{sql, at: {reference, table}}`) is byte-for-byte
//! the same as `verglas-server`'s embedded `/v1/query`, so a client (or the
//! server's own dispatcher, streaming this response straight through to its
//! own caller) does not need to know which of the two is answering.

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use futures::StreamExt;
use iceberg::Catalog;
use serde::Deserialize;
use tokio::sync::{Mutex, RwLock};
use verglas_iceberg::AgentError;
use verglas_sdk::grant::{MemoryGrant, MemoryGrantHost};

use crate::sizing;

/// Shared state every handler reads: the catalog this instance queries
/// against, the memory grant it currently holds and the host to grow it
/// through, and the last-activity clock the idle-shutdown loop watches.
#[derive(Clone)]
pub struct AppState {
    /// The Iceberg catalog, opened once at startup against the configured
    /// cache-owned `[metadata]` endpoint and reading data only through the configured
    /// `[cache].s3_endpoint`.
    pub catalog: Arc<dyn Catalog>,
    /// Long-lived DataFusion catalog session, replaced only when the cache
    /// watcher exposes a new prepared-catalog generation.
    pub prepared_catalog: PreparedQueryCatalog,
    /// The grant host: local/no-op standalone, or a real enforcing host when
    /// one is wired in.
    pub grant_host: Arc<dyn MemoryGrantHost>,
    /// The grant currently held.
    pub grant: Arc<Mutex<MemoryGrant>>,
    /// Unix seconds of the last request served — the idle-shutdown loop reads
    /// this, every handler that touches the query surface updates it.
    pub last_activity: Arc<AtomicU64>,
    /// Whether this runtime can use an estimate to grow its memory grant.
    /// Fixed-memory microVMs disable it to avoid repeating catalog I/O that
    /// cannot change their already-assigned memory.
    pub estimate_on_request: bool,
}

/// A generation-fenced, reusable DataFusion catalog session.
#[derive(Clone)]
pub struct PreparedQueryCatalog {
    catalog: Arc<dyn Catalog>,
    generation_url: Option<String>,
    http: reqwest::Client,
    memory_limit_bytes: usize,
    current: Arc<RwLock<PreparedGeneration>>,
}

struct PreparedGeneration {
    generation: u64,
    catalog: verglas_iceberg::PreparedCatalog,
}

#[derive(Deserialize)]
struct CatalogGeneration {
    generation: u64,
}

impl PreparedQueryCatalog {
    /// Prepares the initial catalog once. Tests and embedded callers can omit a
    /// generation URL; fleet query workers point it at the cache metadata mount.
    pub async fn open(
        catalog: Arc<dyn Catalog>,
        metadata_uri: Option<&str>,
        memory_limit_bytes: usize,
    ) -> Result<Self, AgentError> {
        let http = reqwest::Client::new();
        let generation_url =
            metadata_uri.map(|uri| format!("{}/_verglas/generation", uri.trim_end_matches('/')));
        let (generation, prepared) = match &generation_url {
            Some(url) => loop {
                let before = fetch_generation(&http, url).await?;
                let prepared = verglas_iceberg::PreparedCatalog::open_with_memory_limit(
                    catalog.clone(),
                    memory_limit_bytes,
                )
                .await?;
                let after = fetch_generation(&http, url).await?;
                if before == after {
                    break (after, prepared);
                }
            },
            None => (
                0,
                verglas_iceberg::PreparedCatalog::open_with_memory_limit(
                    catalog.clone(),
                    memory_limit_bytes,
                )
                .await?,
            ),
        };
        Ok(Self {
            catalog,
            generation_url,
            http,
            memory_limit_bytes,
            current: Arc::new(RwLock::new(PreparedGeneration {
                generation,
                catalog: prepared,
            })),
        })
    }

    /// Uses the current prepared session, rebuilding only after a catalog-feed
    /// change. Time-travel remains request-specific because it deliberately pins
    /// a historical snapshot rather than the current catalog generation.
    async fn query_stream(
        &self,
        sql: &str,
        at: Option<verglas_iceberg::TimeTravel>,
    ) -> Result<verglas_iceberg::QueryExecution, AgentError> {
        if at.is_some() {
            return verglas_iceberg::query_stream(self.catalog.clone(), sql, at).await;
        }
        self.refresh_if_changed().await?;
        let prepared = self.current.read().await.catalog.clone();
        prepared.query_stream(sql).await
    }

    async fn refresh_if_changed(&self) -> Result<(), AgentError> {
        let Some(url) = &self.generation_url else {
            return Ok(());
        };
        let observed = fetch_generation(&self.http, url).await?;
        if self.current.read().await.generation == observed {
            return Ok(());
        }

        // Hold the write lock through reconstruction so concurrent requests do
        // not all rebuild the same generation. Re-check after taking it because
        // another request may have completed the refresh while this one waited.
        let mut current = self.current.write().await;
        let observed = fetch_generation(&self.http, url).await?;
        if current.generation == observed {
            return Ok(());
        }
        let prepared = verglas_iceberg::PreparedCatalog::open_with_memory_limit(
            self.catalog.clone(),
            self.memory_limit_bytes,
        )
        .await?;
        let completed_generation = fetch_generation(&self.http, url).await?;
        if completed_generation != observed {
            return Err(AgentError::Query(
                "catalog changed while rebuilding the query session; retry the query".to_owned(),
            ));
        }
        *current = PreparedGeneration {
            generation: completed_generation,
            catalog: prepared,
        };
        Ok(())
    }
}

async fn fetch_generation(http: &reqwest::Client, url: &str) -> Result<u64, AgentError> {
    let response = http
        .get(url)
        .send()
        .await
        .map_err(|error| AgentError::Query(format!("read catalog generation: {error}")))?
        .error_for_status()
        .map_err(|error| AgentError::Query(format!("read catalog generation: {error}")))?;
    response
        .json::<CatalogGeneration>()
        .await
        .map(|value| value.generation)
        .map_err(|error| AgentError::Query(format!("decode catalog generation: {error}")))
}

impl AppState {
    /// Stamps `last_activity` to now.
    fn touch(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_activity.store(now, Ordering::Relaxed);
    }
}

/// The whole admin router: readiness plus the query surface.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/query", post(query_sql))
        .route("/v1/query/estimate", post(query_estimate))
        .with_state(state)
}

/// `GET /healthz`: this binary has no recovery phase to wait on (it opens its
/// catalog before serving at all), so once it answers it is ready.
async fn healthz() -> &'static str {
    "ok"
}

/// The body of `POST /v1/query` and `POST /v1/query/estimate`: identical to
/// `verglas-server`'s embedded `/v1/query` request shape.
#[derive(Debug, Deserialize)]
struct QueryRequest {
    /// The SQL to run (or, for `/estimate`, to plan without running).
    sql: String,
    /// Optional time travel: pins `table` to snapshot-id-or-timestamp
    /// `reference`.
    at: Option<QueryAt>,
}

/// The time-travel pin of a [`QueryRequest`].
#[derive(Debug, Deserialize)]
struct QueryAt {
    /// A snapshot id or an RFC 3339 timestamp.
    reference: String,
    /// The table to pin.
    table: String,
}

impl QueryRequest {
    fn time_travel(&self) -> Option<verglas_iceberg::TimeTravel> {
        self.at.as_ref().map(|at| verglas_iceberg::TimeTravel {
            reference: at.reference.clone(),
            table: at.table.clone(),
        })
    }
}

/// `POST /v1/query`: streams SQL results as `{columns, rows, row_count}`, one
/// JSON chunk per `RecordBatch` as DataFusion produces it — see the module
/// doc comment for why, and for exactly what stays byte-compatible with the
/// embedded server's buffered response.
///
/// Before running, this estimates the query's memory need and grows the held
/// grant to cover it if the current grant falls short (grow-only, the escape
/// hatch — the primary sizing happens once at launch, `sizing::request_for_query`,
/// from whatever query triggered the launch; this is the *operator* working
/// set, not the result size — a large result never grows the grant by
/// itself). A request whose SQL cannot be planned is a 400 — the same point
/// `execute_stream` fails at, before any response bytes go out, so the status
/// code is still meaningful. A failure *during* streaming (a real runtime
/// error, not a planning error) cannot change an already-sent 200; it
/// truncates the body and is logged server-side instead — an unavoidable
/// property of not buffering the whole result first, not a bug.
async fn query_sql(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Response {
    state.touch();
    let at = request.time_travel();

    if state.estimate_on_request
        && let Ok(estimate) =
            verglas_iceberg::estimate(state.catalog.clone(), &request.sql, at.clone()).await
    {
        let mut current = state.grant.lock().await;
        *current = sizing::ensure_covers(
            state.grant_host.as_ref(),
            *current,
            estimate.estimated_total_bytes,
        )
        .await;
    }

    let execution = match state.prepared_catalog.query_stream(&request.sql, at).await {
        Ok(v) => v,
        Err(error) => return query_error(error),
    };

    if accepts_arrow(&headers) {
        return arrow_query_response(execution);
    }
    let verglas_iceberg::QueryExecution {
        columns,
        mut batches,
    } = execution;

    let columns_json = serde_json::to_string(&columns).unwrap_or_else(|_| "[]".to_owned());
    let body = async_stream::stream! {
        yield Ok::<_, std::io::Error>(Bytes::from(format!(
            "{{\"columns\":{columns_json},\"rows\":["
        )));
        let mut row_count: usize = 0;
        while let Some(item) = batches.next().await {
            let batch = match item {
                Ok(batch) => batch,
                Err(error) => {
                    tracing::error!(
                        "query execution failed mid-stream, truncating the response: {error}"
                    );
                    return;
                }
            };
            match verglas_iceberg::batch_to_json_rows_fragment(&batch) {
                Ok((bytes, n)) if n > 0 => {
                    if row_count > 0 {
                        yield Ok(Bytes::from_static(b","));
                    }
                    row_count += n;
                    yield Ok(Bytes::from(bytes));
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(
                        "failed to encode a query result batch, truncating the response: {error}"
                    );
                    return;
                }
            }
        }
        yield Ok(Bytes::from(format!("],\"row_count\":{row_count}}}")));
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_stream(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// True when the caller requests Arrow IPC rather than JSON rows.
fn accepts_arrow(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("application/vnd.apache.arrow.stream"))
}

/// Streams the query batches as one Arrow IPC stream without collecting them.
fn arrow_query_response(execution: verglas_iceberg::QueryExecution) -> Response {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let output = ChannelWriter(buffer.clone());
        let schema = execution.batches.schema();
        let mut batches = execution.batches;
        let mut writer = match arrow_ipc::writer::StreamWriter::try_new(output, &schema) {
            Ok(writer) => writer,
            Err(error) => {
                let _ = sender.send(Err(io::Error::other(error.to_string()))).await;
                return;
            }
        };
        if !send_arrow_fragment(&sender, &buffer).await {
            return;
        }
        while let Some(batch) = batches.next().await {
            match batch {
                Ok(batch) => {
                    if let Err(error) = writer.write(&batch) {
                        let _ = sender.send(Err(io::Error::other(error.to_string()))).await;
                        return;
                    }
                    if !send_arrow_fragment(&sender, &buffer).await {
                        return;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(io::Error::other(error.to_string()))).await;
                    return;
                }
            }
        }
        if let Err(error) = writer.finish() {
            let _ = sender.send(Err(io::Error::other(error.to_string()))).await;
            return;
        }
        let _ = send_arrow_fragment(&sender, &buffer).await;
    });
    let stream = futures::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Sends bytes produced since the previous batch through the bounded channel.
async fn send_arrow_fragment(
    sender: &tokio::sync::mpsc::Sender<Result<Bytes, io::Error>>,
    buffer: &std::sync::Mutex<Vec<u8>>,
) -> bool {
    let bytes = match buffer.lock() {
        Ok(mut buffer) => std::mem::take(&mut *buffer),
        Err(_) => return false,
    };
    bytes.is_empty() || sender.send(Ok(Bytes::from(bytes))).await.is_ok()
}

/// Blocking writer used only by Arrow's synchronous stream encoder.
struct ChannelWriter(Arc<std::sync::Mutex<Vec<u8>>>);

impl Write for ChannelWriter {
    /// Appends encoded bytes to the current response fragment.
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("Arrow response buffer poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    /// The async sender controls response flushing.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// `POST /v1/query/estimate`: answers the plan-based memory estimate for
/// `sql` without executing it — how the launcher (or an operator) sizes a
/// grant before running the query for real.
async fn query_estimate(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Response {
    state.touch();
    let at = request.time_travel();
    match verglas_iceberg::estimate(state.catalog.clone(), &request.sql, at).await {
        Ok(estimate) => Json(estimate).into_response(),
        Err(error) => query_error(error),
    }
}

/// Maps a query/estimate failure to a status code: a statement the engine
/// cannot plan or execute is the caller's bad input (400); anything else
/// (catalog reachability, an Iceberg error) is a 500.
fn query_error(error: AgentError) -> Response {
    use axum::http::StatusCode;
    match error {
        AgentError::Query(_) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
        other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()).into_response(),
    }
}
