//! Admin HTTP API served by `verglas-server` on a separate port from the S3 endpoint.
//!
//! This is the private control surface — bound to loopback, never the S3
//! data-plane port. Beyond the version and health probes it carries the
//! `POST /cache/purge` operation (issue #138), which is wired only when a
//! cache engine exists (i.e. a full server, not the config-less smoke mode).
//!
//! # Serve-gating (#16)
//!
//! The admin listener comes up *before* the cache engine finishes disk
//! recovery, so `/admin/healthz` can answer `starting` (503) — rather than
//! connection-refused — while a load balancer polls it. A [`Health`] gate,
//! flipped to ready once recovery completes, drives that. The engine-dependent
//! routes (purge, stats, members) are wired through deferred [`OnceLock`] slots
//! filled at the same moment, so they exist on the same router but answer
//! `503 Service Unavailable` until the engine is ready.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, Query, RawQuery, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{
    Json, Router,
    routing::{any, get, post, put},
};
use std::future::Future;
use std::pin::Pin;

use iceberg::Catalog;
use serde::Deserialize;
use serde_json::json;
use verglas_graph::{
    Direction, Edge, Graph, GraphError, IndexBuildMode, Neighbor, Node, Path as GraphPath, Reached,
    ReaderBackend, Subgraph, TraversalFilter, TripletReceipt,
};
use verglas_iceberg::tables_api;
use verglas_iceberg::{AgentError, parse_table_ident};
use verglas_sdk::graph as graph_wire;
use verglas_sdk::vector as vector_wire;
use verglas_vector::error::VectorError;
use verglas_vector::maintenance::{MaintenanceConfig, MaintenanceReport};
use verglas_vector::service::{SearchOptions, VectorService};
use verglas_vector::{IndexKey, Metric, VamanaParams};

use verglas_cache::CachePurger;
use verglas_core::admin::{
    ACCESS_PATH, DRAIN_PATH, DrainAck, DrainRequest, HEALTHZ_PATH, HealthzInfo, LOG_PATH,
    LocalAccess, LogLevelInfo, LogLevelRequest, MEMBERS_PATH, METRICS_PATH, MembersInfo,
    PURGE_PATH, STATS_PATH, StatsInfo, TABLE_METRICS_PATH, VERSION_PATH, VersionInfo,
};
use verglas_core::metrics::EXPOSITION_CONTENT_TYPE;

/// Readiness gate for serve-gating (#16). Starts reporting `starting` and flips
/// to `ok` once the cache engine's disk recovery completes, so a load balancer
/// polling `/admin/healthz` never routes to a node that would cold-miss
/// everything. Cheap to share: one atomic behind an `Arc`.
#[derive(Clone)]
pub struct Health(Arc<AtomicBool>);

impl Health {
    /// A gate that reports `starting` until [`Health::mark_ready`] is called —
    /// the full server uses this while its engine recovers.
    pub fn starting() -> Self {
        Health(Arc::new(AtomicBool::new(false)))
    }

    /// A gate that is already ready. The config-less admin-only mode (CLI smoke
    /// flows) has no engine to recover, so it is ready the instant it binds.
    pub fn ready() -> Self {
        Health(Arc::new(AtomicBool::new(true)))
    }

    /// Marks recovery complete; subsequent probes report `ok` and route.
    pub fn mark_ready(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether recovery has completed.
    fn is_ready(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// A cache purger wired lazily once recovery completes (#16 serve-gating). Empty
/// while the engine is still building; the purge route answers 503 until set.
pub type PurgerSlot = Arc<OnceLock<Arc<dyn CachePurger>>>;

/// A stats source wired lazily once recovery completes; see [`PurgerSlot`].
pub type StatsSlot = Arc<OnceLock<StatsSource>>;

/// A metrics source wired lazily once recovery completes; see [`PurgerSlot`].
pub type MetricsSlot = Arc<OnceLock<MetricsSource>>;

/// A membership source wired lazily once recovery completes; see [`PurgerSlot`].
pub type MembersSlot = Arc<OnceLock<MembersSource>>;

/// Produces a live [`StatsInfo`] snapshot (cache config + read-path counters).
/// The server supplies one closure over its engine handle and config; the admin
/// surface owns nothing of the engine's type. `None` when the server booted
/// without a config (the config-less admin-only mode used by smoke flows), so
/// there is no engine to report on.
pub type StatsSource = Arc<dyn Fn() -> StatsInfo + Send + Sync>;

/// Produces the Prometheus text exposition for `GET /metrics` (issue #46): the
/// live request families encoded from the shared registry, plus the node-level
/// counters and gauges read from the running engine and backend at scrape time.
/// The server supplies one closure over its metrics handle, engine, and backend;
/// the admin surface owns none of those types. `None` in the config-less
/// admin-only mode (no engine to report on).
pub type MetricsSource = Arc<dyn Fn() -> String + Send + Sync>;

/// Produces the per-table telemetry report for `GET /v1/metering/tables` (#60):
/// one row per table with recorded traffic (hit rate, cached/served bytes,
/// requests avoided, latency saved) plus fleet totals, read from the rollup at
/// call time. The server supplies one closure over its telemetry hub and mapper;
/// the admin surface owns neither type. `None` (route absent) when there is no
/// mapper to attribute reads (prefetch off / config-less mode).
pub type TablesReportSource = Arc<dyn Fn() -> verglas_core::telemetry::TablesReport + Send + Sync>;

/// A per-table telemetry source wired lazily once recovery completes.
pub type TablesReportSlot = Arc<OnceLock<TablesReportSource>>;

/// Produces a live [`MembersInfo`] snapshot (this node's gossip membership view
/// and ring epoch, issue #27). The server supplies one closure over its cluster
/// agent; the admin surface owns nothing of the gossip layer's types. `None`
/// when the server runs single-node (no `[cluster]`), so the route is absent.
pub type MembersSource = Arc<dyn Fn() -> MembersInfo + Send + Sync>;

/// A drain source wired lazily once recovery completes; see [`PurgerSlot`].
pub type DrainSlot = Arc<OnceLock<DrainHandler>>;

/// Initiates a graceful drain (issue #31): gossips this node `draining` and
/// schedules its exit, returning the ack. The server supplies one async closure
/// over its cluster agent; the admin surface owns nothing of the gossip layer.
/// `None` when the server runs single-node (no `[cluster]`), so the route is
/// absent — a cluster of one has nowhere to drain to.
pub type DrainHandler =
    Arc<dyn Fn(DrainRequest) -> Pin<Box<dyn Future<Output = DrainAck> + Send>> + Send + Sync>;

/// The catalog handle the SDK table routes commit and read through, wired
/// lazily after recovery like the other engine slots. It is an `Arc<dyn Catalog>`
/// over the server's own loopback gateway; the routes answer 503 until it is
/// filled.
pub type TablesSlot = Arc<OnceLock<Arc<dyn Catalog>>>;

/// Optional on-prem Rill integration. The runtime carries the deferred table
/// catalog slot and reaches Rill only over its private network API.
pub type DashboardSlot = Arc<crate::dashboard::DashboardRuntime>;

/// The catalog handle the `graph` verb-family routes (`/v1/graphs/...`) drive
/// the graph-over-Iceberg engine through. A graph is not a new storage
/// primitive — it is a namespace holding two plain Iceberg tables plus the
/// Puffin adjacency index — so it shares the same private catalog handle as
/// [`TablesSlot`], filled at the same moment after recovery, and answers 503
/// until then.
pub type GraphsSlot = Arc<OnceLock<Arc<dyn Catalog>>>;

/// The runtime backing the vector-index routes (`/v1/tables|graphs/{..}/
/// indexes`): the private catalog handle plus the disposable decoded-index
/// cache. Durable indexes are snapshot-bound Puffin statistics attachments in
/// the customer table.
pub struct VectorRuntime {
    /// The private upstream catalog used to read and commit index attachments.
    pub catalog: Arc<dyn Catalog>,
    /// The index service with a disposable decoded-index serving cache.
    pub service: Arc<VectorService>,
}

/// The deferred handle the vector-index routes drive, wired after recovery like
/// [`TablesSlot`]; the routes answer 503 until it is filled.
pub type VectorSlot = Arc<OnceLock<Arc<VectorRuntime>>>;

/// The `verglas_sys` registry handle the registry and watermark routes write
/// and read through (#322), wired lazily after recovery like [`TablesSlot`].
/// Registry writes go through the server's engine handle only — this slot is
/// the single local write authority for `verglas_sys`.
pub type SysSlot = Arc<OnceLock<Arc<verglas_platform::SystemCatalog>>>;

/// The queue root directory the queue routes append to and read from
/// (`/v1/queues/<name>/{enqueue,poll,ack}`, #328). Unlike the engine slots this
/// is a plain path, not a deferred [`OnceLock`]: a segment log needs no cache
/// recovery, so the routes answer as soon as the server owns a writable state
/// dir. The TS SDK queue verb targets these routes directly.
pub type QueueDir = Arc<std::path::PathBuf>;

/// The scheduler ingress handle the manual and HTTP routes enqueue through,
/// wired after recovery opens the deployment registry.
pub type PlatformSlot = Arc<OnceLock<Arc<crate::platform::SchedulerIngress>>>;

/// The engine-backed surfaces the admin router serves, each optional (absent =
/// the route is not mounted) and most deferred behind a [`OnceLock`] slot
/// (mounted but 503 until recovery fills it). One struct instead of a
/// positional list so call sites name what they wire.
#[derive(Default)]
pub struct Slots {
    /// Reflected Integration namespace gateway backed by the local runtime manager.
    pub namespaces: Option<crate::namespace::NamespaceGateway>,
    /// Always-on native KV routes, authenticated at the route boundary.
    pub kv: Option<crate::kv::KvRuntime>,
    /// Cache purge (`POST /cache/purge`).
    pub purger: Option<PurgerSlot>,
    /// Cache stats probe (`GET /cache/stats`).
    pub stats: Option<StatsSlot>,
    /// Prometheus exposition (`GET /metrics`).
    pub metrics: Option<MetricsSlot>,
    /// Per-table telemetry report (`GET /v1/metering/tables`, #60).
    pub table_metrics: Option<TablesReportSlot>,
    /// Gossip membership probe (`GET /members`).
    pub members: Option<MembersSlot>,
    /// Drain control (`POST /admin/drain`).
    pub drain: Option<DrainSlot>,
    /// Shallow on-prem Iceberg REST proxy sharing cache state with polling.
    pub catalog: Option<verglas_catalog::CatalogGateway>,
    /// The local-access snapshot (`GET /admin/access`).
    pub access: Option<LocalAccess>,
    /// The internal catalog handle used by compaction and engine subsystems.
    pub tables: Option<TablesSlot>,
    /// Rill-backed dashboard routes, absent when `[analytics.rill]` is unset.
    pub dashboards: Option<DashboardSlot>,
    /// The `graph` verb-family routes (`/v1/graphs/...`), backed by
    /// `verglas-graph` over the same private catalog as the table routes.
    pub graphs: Option<GraphsSlot>,
    /// The vector-index routes (`/v1/tables|graphs/{..}/indexes...`), backed by
    /// `verglas-vector` over snapshot-bound Iceberg attachments.
    pub vector: Option<VectorSlot>,
    /// The `verglas_sys` registry routes (`/v1/workers`).
    pub sys: Option<SysSlot>,
    /// The platform queue routes (`/v1/queues/<name>/{enqueue,poll,ack}`),
    /// backed by [`verglas_harness::queue`] under this queue root.
    pub queues: Option<QueueDir>,
    /// Manual and dynamically routed HTTP ingress into the scheduler queue.
    pub platform: Option<PlatformSlot>,
    /// The standalone query worker dispatcher (`[query_worker]` configured).
    /// When present it is the sole engine for `/v1/query`; dispatch failure is
    /// a hard error, not an embedded-engine fallback.
    pub query_worker: Option<Arc<crate::query_worker::QueryWorkerDispatcher>>,
    /// The standalone logical write worker dispatcher.
    pub write_worker: Option<Arc<crate::write_worker::WriteWorkerDispatcher>>,
}

/// Builds the admin router with the version and health probes, plus the cache
/// purge endpoint (issue #138), the cache stats probe (issue #141), and the
/// gossip membership probe (issue #27) — each present only when the server has
/// the corresponding subsystem (its slot is `Some`). The config-less smoke/CLI
/// path passes `None` for all and gets probes only.
///
/// `health` drives `/admin/healthz`: `starting` (503) until [`Health::mark_ready`]
/// flips it, then `ok` (200). The engine-dependent routes take deferred
/// [`OnceLock`] slots inside `slots` so the router can be built and served
/// before the engine finishes recovering; they answer 503 until their slot is
/// filled (#16).
pub fn router(server_version: &'static str, health: Health, slots: Slots) -> Router {
    let Slots {
        namespaces,
        kv,
        purger,
        stats,
        metrics,
        table_metrics,
        members,
        drain,
        catalog,
        access,
        tables,
        dashboards,
        graphs,
        vector,
        sys,
        queues,
        platform,
        query_worker,
        write_worker,
    } = slots;
    let mut app = Router::new()
        .route(
            VERSION_PATH,
            get({
                let version = server_version;
                move || async move { Json(VersionInfo::for_server(version)) }
            }),
        )
        // Runtime log-level control (#61). Not slot-gated: the reload handle is
        // process-global, so this answers whether or not the engine is ready.
        .route(LOG_PATH, post(set_log_level))
        .merge(health_router(health));
    if let Some(namespaces) = namespaces {
        app = app.merge(crate::namespace::router(namespaces));
    }
    if let Some(kv) = kv {
        app = app.merge(crate::kv::router(kv));
    }
    if let Some(access) = access {
        // Local-access probe (#287): a fixed snapshot of this server's
        // connection details, resolved once at startup. Not engine-dependent, so
        // it needs no deferred slot — it answers the moment the admin listener
        // binds, which is what the zero-config CLI verbs poll.
        app = app.route(
            ACCESS_PATH,
            get(move || {
                let access = access.clone();
                async move { Json(access) }
            }),
        );
    }
    if let Some(purger) = purger {
        app = app.merge(purge_router(purger));
    }
    if let Some(stats) = stats {
        app = app.route(
            STATS_PATH,
            get(move || {
                let stats = stats.clone();
                async move {
                    match stats.get() {
                        Some(stats) => Json(stats()).into_response(),
                        None => recovering(),
                    }
                }
            }),
        );
    }
    if let Some(metrics) = metrics {
        app = app.route(
            METRICS_PATH,
            get(move || {
                let metrics = metrics.clone();
                async move {
                    match metrics.get() {
                        Some(render) => (
                            [(axum::http::header::CONTENT_TYPE, EXPOSITION_CONTENT_TYPE)],
                            render(),
                        )
                            .into_response(),
                        None => recovering(),
                    }
                }
            }),
        );
    }
    if let Some(table_metrics) = table_metrics {
        app = app.route(
            TABLE_METRICS_PATH,
            get(move || {
                let table_metrics = table_metrics.clone();
                async move {
                    match table_metrics.get() {
                        Some(source) => Json(source()).into_response(),
                        None => recovering(),
                    }
                }
            }),
        );
    }
    if let Some(members) = members {
        app = app.route(
            MEMBERS_PATH,
            get(move || {
                let members = members.clone();
                async move {
                    match members.get() {
                        Some(members) => Json(members()).into_response(),
                        None => recovering(),
                    }
                }
            }),
        );
    }
    if let Some(drain) = drain {
        app = app.route(
            DRAIN_PATH,
            post(move |body: Option<Json<DrainRequest>>| {
                let drain = drain.clone();
                async move {
                    let Some(handler) = drain.get() else {
                        return recovering();
                    };
                    // A body is optional: `POST /admin/drain` with none takes
                    // the server's configured drain timeout.
                    let request = body.map(|Json(r)| r).unwrap_or_default();
                    Json(handler(request).await).into_response()
                }
            }),
        );
    }
    if let Some(tables) = tables {
        app = app.merge(compact_router(tables));
    }
    if let Some(dashboards) = dashboards {
        app = app.merge(dashboard_router(dashboards));
    }
    app = app.merge(v1_serving_router(query_worker, write_worker));
    if let Some(graphs) = graphs {
        app = app.merge(graphs_router(graphs));
    }
    if let Some(vector) = vector {
        app = app.merge(vector_router(vector));
    }
    if let Some(sys) = sys {
        app = app.merge(sys_router(sys));
    }
    if let Some(queues) = queues {
        app = app.merge(queue_router(queues));
    }
    if let Some(platform) = platform {
        app = app.merge(platform_router(platform));
    }
    if let Some(catalog) = catalog {
        app = crate::compose_query_and_catalog(app, catalog);
    }
    app
}

/// Mounts the optional Rill dashboard resource API.
fn dashboard_router(dashboards: DashboardSlot) -> Router {
    Router::new()
        .route("/v1/dashboards", post(dashboard_create).get(dashboard_list))
        .route(
            "/v1/dashboards/{name}",
            get(dashboard_show).delete(dashboard_delete),
        )
        .with_state(dashboards)
}

/// Creates or refreshes a Rill dashboard for one catalog-resolved table.
async fn dashboard_create(
    State(dashboards): State<DashboardSlot>,
    Json(request): Json<crate::dashboard::CreateDashboardRequest>,
) -> Response {
    match dashboards.create(request).await {
        Ok(info) => Json(info).into_response(),
        Err(error) => dashboard_error(error),
    }
}

/// Lists Verglas-owned Rill dashboards.
async fn dashboard_list(State(dashboards): State<DashboardSlot>) -> Response {
    match dashboards.list().await {
        Ok(list) => Json(list).into_response(),
        Err(error) => dashboard_error(error),
    }
}

/// Shows one Verglas-owned Rill dashboard.
async fn dashboard_show(
    State(dashboards): State<DashboardSlot>,
    Path(name): Path<String>,
) -> Response {
    match dashboards.show(&name).await {
        Ok(info) => Json(info).into_response(),
        Err(error) => dashboard_error(error),
    }
}

/// Deletes only the Rill files owned by the named Verglas dashboard.
async fn dashboard_delete(
    State(dashboards): State<DashboardSlot>,
    Path(name): Path<String>,
) -> Response {
    match dashboards.delete(&name).await {
        Ok(deleted) => Json(deleted).into_response(),
        Err(error) => dashboard_error(error),
    }
}

/// Maps the dashboard contract to stable HTTP statuses.
fn dashboard_error(error: crate::dashboard::DashboardError) -> Response {
    let status = match &error {
        crate::dashboard::DashboardError::Invalid(_) => StatusCode::BAD_REQUEST,
        crate::dashboard::DashboardError::NotFound(_) => StatusCode::NOT_FOUND,
        crate::dashboard::DashboardError::Ownership(_) => StatusCode::CONFLICT,
        crate::dashboard::DashboardError::Catalog(message)
            if message == "cache engine is still recovering" =>
        {
            StatusCode::SERVICE_UNAVAILABLE
        }
        crate::dashboard::DashboardError::Catalog(_)
        | crate::dashboard::DashboardError::Rill(_) => StatusCode::BAD_GATEWAY,
    };
    (status, error.to_string()).into_response()
}

/// The health sub-router, isolated so its [`Health`] state does not leak into
/// the probe routes' state type.
fn health_router(health: Health) -> Router {
    Router::new()
        .route(HEALTHZ_PATH, get(healthz))
        .with_state(health)
}

/// The purge sub-router, isolated so its [`PurgerSlot`] state does not leak into
/// the probe routes' state type.
fn purge_router(purger: PurgerSlot) -> Router {
    Router::new()
        .route(PURGE_PATH, post(purge))
        .with_state(purger)
}

/// Bounded request-body limit used by execution and index surfaces.
const TABLES_BODY_LIMIT_BYTES: usize = 32 * 1024 * 1024;

/// The execution gateway served on both server listeners. Catalog metadata is
/// resolved directly by clients; only isolated query and logical-write roles
/// are dispatched through this surface.
pub fn v1_serving_router(
    query_worker: Option<Arc<crate::query_worker::QueryWorkerDispatcher>>,
    write_worker: Option<Arc<crate::write_worker::WriteWorkerDispatcher>>,
) -> Router {
    query_router(query_worker).merge(write_router(write_worker))
}

/// The manual compaction route (`POST /admin/compact`): run one compaction pass
/// over the private catalog on demand. The open-source server ships compaction
/// as an opt-in mechanism — this is the manual trigger; recurring policy belongs
/// to a container-backed worker. Shares the [`TablesSlot`] private catalog and
/// answers 503 until it is wired after recovery.
fn compact_router(tables: TablesSlot) -> Router {
    Router::new()
        .route("/admin/compact", post(compact_now))
        .with_state(tables)
}

/// `POST /admin/compact`: runs a single compaction pass over every table in the
/// private catalog and returns the pass report (tables examined, per-table files
/// rewritten, snapshots committed) as JSON. Bin-packs small files under the
/// platform target and REPLACE-commits with conflict retry, off the serve path.
/// Answers 503 until the catalog handle is wired; 500 with the plain message if
/// the pass fails to list namespaces or tables.
async fn compact_now(State(tables): State<TablesSlot>) -> Response {
    let Some(catalog) = tables.get() else {
        return recovering();
    };
    match verglas_iceberg::run_compaction(
        catalog.as_ref(),
        verglas_iceberg::CompactionOptions::default(),
    )
    .await
    {
        Ok(report) => Json(report).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

/// The isolated query-role dispatcher. No embedded execution path exists.
#[derive(Clone)]
struct QueryState {
    query_worker: Option<Arc<crate::query_worker::QueryWorkerDispatcher>>,
}

/// Mounts the query gateway even when no worker is configured, returning a
/// clear service-unavailable response instead of linking an embedded fallback.
fn query_router(query_worker: Option<Arc<crate::query_worker::QueryWorkerDispatcher>>) -> Router {
    Router::new()
        .route("/v1/query", post(query_sql))
        .with_state(QueryState { query_worker })
}

/// The body of `POST /v1/query`: the SQL statement and an optional time-travel
/// pin for one table.
#[derive(Debug, Deserialize)]
struct QueryRequest {
    /// The SQL to run.
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

/// Dispatches SQL to `verglas-query` and relays its streamed response.
async fn query_sql(
    State(state): State<QueryState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Response {
    let Some(dispatcher) = &state.query_worker else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "query worker is not configured",
        )
            .into_response();
    };
    let time_travel = request.at.map(|at| verglas_iceberg::TimeTravel {
        reference: at.reference,
        table: at.table,
    });
    match dispatcher
        .dispatch(&request.sql, time_travel, accepts_arrow(&headers))
        .await
    {
        Ok(response) => response,
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            format!("query worker unavailable: {error}"),
        )
            .into_response(),
    }
}

/// True when a request selects the Arrow IPC representation.
fn accepts_arrow(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains(verglas_sdk::ARROW_STREAM_CONTENT_TYPE))
}

/// State for the isolated logical write gateway.
#[derive(Clone)]
struct WriteState {
    write_worker: Option<Arc<crate::write_worker::WriteWorkerDispatcher>>,
}

/// Mounts the logical write gateway with no embedded table-writer fallback.
fn write_router(write_worker: Option<Arc<crate::write_worker::WriteWorkerDispatcher>>) -> Router {
    Router::new()
        .route("/v1/write/{name}", post(write_dispatch))
        .route("/v1/ingest/{name}", post(ingest_dispatch))
        .layer(axum::extract::DefaultBodyLimit::max(
            TABLES_BODY_LIMIT_BYTES,
        ))
        .with_state(WriteState { write_worker })
}

/// Relays a bounded CSV, JSONL, or Parquet ingest to `verglas-write`.
async fn ingest_dispatch(
    State(state): State<WriteState>,
    Path(name): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(dispatcher) = &state.write_worker else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "write worker is not configured",
        )
            .into_response();
    };
    let Some(query) = query else {
        return (
            StatusCode::BAD_REQUEST,
            "ingest query parameters are required",
        )
            .into_response();
    };
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    match dispatcher
        .dispatch_ingest(&name, &query, body, idempotency_key)
        .await
    {
        Ok(response) => response,
        Err(error) => (StatusCode::BAD_GATEWAY, error).into_response(),
    }
}

/// Relays one bounded Arrow write to `verglas-write`.
async fn write_dispatch(
    State(state): State<WriteState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(dispatcher) = &state.write_worker else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "write worker is not configured",
        )
            .into_response();
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !content_type.starts_with(verglas_sdk::ARROW_STREAM_CONTENT_TYPE) {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "write requests require application/vnd.apache.arrow.stream",
        )
            .into_response();
    }
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    match dispatcher.dispatch(&name, body, idempotency_key).await {
        Ok(response) => response,
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            format!("write worker unavailable: {error}"),
        )
            .into_response(),
    }
}

/// The `graph` verb-family sub-router (`/v1/graphs/...`): create a graph, insert
/// nodes and edges, build the Puffin adjacency index, run a traversal query, and
/// show a graph's backing tables and index state. A graph is not a new storage
/// primitive — it is a namespace holding two plain Iceberg tables
/// (`<namespace>.nodes`, `<namespace>.edges`) plus the index — so these routes
/// share the same private catalog handle as the table routes and answer 503
/// until it is wired after recovery. The insert routes take the same 32 MiB body
/// ceiling as the table commit route so a batch lands in one commit.
fn graphs_router(graphs: GraphsSlot) -> Router {
    Router::new()
        .route(
            "/v1/graphs/{namespace}",
            post(graphs_create).get(graphs_show),
        )
        .route("/v1/graphs/{namespace}/nodes", post(graphs_insert_nodes))
        .route("/v1/graphs/{namespace}/edges", post(graphs_insert_edges))
        .route("/v1/graphs/{namespace}/index", post(graphs_build_index))
        .route("/v1/graphs/{namespace}/query", post(graphs_query))
        .layer(axum::extract::DefaultBodyLimit::max(
            TABLES_BODY_LIMIT_BYTES,
        ))
        .with_state(graphs)
}

/// Why a graph route could not open its engine handle: the catalog slot is not
/// wired yet (503), or the namespace did not parse (a graph-engine error). Kept
/// small so the route helper returns a compact `Result` rather than a full
/// `Response` in the error arm.
enum GraphOpenError {
    /// The private catalog handle is not wired yet — answer 503.
    Recovering,
    /// The namespace could not be opened as a graph.
    Open(GraphError),
}

impl GraphOpenError {
    /// Renders the open failure as the HTTP response the route returns.
    fn into_response(self) -> Response {
        match self {
            GraphOpenError::Recovering => recovering(),
            GraphOpenError::Open(error) => graph_error(error),
        }
    }
}

/// Opens the graph engine handle over the private catalog for `namespace`.
/// Every graph route starts here so the slot-not-wired and bad-namespace paths
/// are handled in one place.
fn open_graph(graphs: &GraphsSlot, namespace: &str) -> Result<Graph, GraphOpenError> {
    let Some(catalog) = graphs.get() else {
        return Err(GraphOpenError::Recovering);
    };
    Graph::open(catalog.clone(), namespace).map_err(GraphOpenError::Open)
}

/// `POST /v1/graphs/{namespace}`: creates the graph by ensuring its two backing
/// Iceberg tables exist (idempotent). Returns the namespace and the two table
/// names, which are plain tables queryable on their own. Answers 503 until the
/// catalog handle is wired.
async fn graphs_create(
    State(graphs): State<GraphsSlot>,
    Path(namespace): Path<String>,
) -> Response {
    let graph = match open_graph(&graphs, &namespace) {
        Ok(graph) => graph,
        Err(error) => return error.into_response(),
    };
    match graph.ensure_tables().await {
        Ok(()) => Json(graph_wire::GraphCreateReport {
            namespace: namespace.clone(),
            nodes_table: format!("{namespace}.nodes"),
            edges_table: format!("{namespace}.edges"),
        })
        .into_response(),
        Err(error) => graph_error(error),
    }
}

/// `POST /v1/graphs/{namespace}/nodes`: appends a batch of nodes and returns the
/// new nodes-table snapshot id. Answers 503 until the catalog handle is wired.
async fn graphs_insert_nodes(
    State(graphs): State<GraphsSlot>,
    Path(namespace): Path<String>,
    Json(request): Json<graph_wire::InsertNodesRequest>,
) -> Response {
    let graph = match open_graph(&graphs, &namespace) {
        Ok(graph) => graph,
        Err(error) => return error.into_response(),
    };
    let nodes: Vec<Node> = request.nodes.iter().map(node_from_input).collect();
    let count = nodes.len();
    match graph.insert_nodes(&nodes).await {
        Ok(snapshot_id) => Json(graph_wire::InsertReport { snapshot_id, count }).into_response(),
        Err(error) => graph_error(error),
    }
}

/// `POST /v1/graphs/{namespace}/edges`: appends a batch of edges (triplets) and
/// returns the new edges-table snapshot id — the snapshot an index build binds
/// to. Answers 503 until the catalog handle is wired.
async fn graphs_insert_edges(
    State(graphs): State<GraphsSlot>,
    Path(namespace): Path<String>,
    Json(request): Json<graph_wire::InsertEdgesRequest>,
) -> Response {
    let graph = match open_graph(&graphs, &namespace) {
        Ok(graph) => graph,
        Err(error) => return error.into_response(),
    };
    let edges: Vec<Edge> = request.edges.iter().map(edge_from_input).collect();
    let count = edges.len();
    match graph.insert_edges(&edges).await {
        Ok(snapshot_id) => Json(graph_wire::InsertReport { snapshot_id, count }).into_response(),
        Err(error) => graph_error(error),
    }
}

/// `POST /v1/graphs/{namespace}/index`: builds or refreshes the adjacency index
/// as of the requested edge snapshot (the current tip when absent) and binds it
/// as a Puffin blob. Returns the build report, or `built: false` when the edge
/// table has no data to index. Answers 503 until the catalog handle is wired.
async fn graphs_build_index(
    State(graphs): State<GraphsSlot>,
    Path(namespace): Path<String>,
    body: Option<Json<graph_wire::BuildIndexRequest>>,
) -> Response {
    let graph = match open_graph(&graphs, &namespace) {
        Ok(graph) => graph,
        Err(error) => return error.into_response(),
    };
    let at = body.and_then(|Json(request)| request.at);
    match graph.build_index(at).await {
        Ok(Some(report)) => Json(graph_wire::IndexReport {
            built: true,
            snapshot_id: Some(report.snapshot_id),
            node_count: report.node_count,
            edge_count: report.edge_count,
            blob_path: Some(report.blob_path),
            blob_bytes: Some(report.blob_bytes),
            mode: Some(index_mode_label(report.mode)),
        })
        .into_response(),
        Ok(None) => Json(graph_wire::IndexReport {
            built: false,
            snapshot_id: None,
            node_count: 0,
            edge_count: 0,
            blob_path: None,
            blob_bytes: None,
            mode: None,
        })
        .into_response(),
        Err(error) => graph_error(error),
    }
}

/// `POST /v1/graphs/{namespace}/query`: runs one traversal (`neighbors`,
/// `kHop`, `neighborhood`, or `paths`) over a reader opened as of `asOf` (the
/// current tip when absent). The reader prefers the bound index and falls back
/// to a scan of the plain tables; the response reports which path served it so
/// the turn-off equivalence is observable. A query missing the bound its `op`
/// requires (`k`, `maxHops`, or `dst`) is a 400. Answers 503 until the catalog
/// handle is wired.
async fn graphs_query(
    State(graphs): State<GraphsSlot>,
    Path(namespace): Path<String>,
    Json(request): Json<graph_wire::GraphQueryRequest>,
) -> Response {
    let graph = match open_graph(&graphs, &namespace) {
        Ok(graph) => graph,
        Err(error) => return error.into_response(),
    };
    let reader = match graph.reader(request.as_of).await {
        Ok(reader) => reader,
        Err(error) => return graph_error(error),
    };
    let direction = direction_from_wire(request.direction);
    let filter = TraversalFilter {
        predicate: request.filter.predicate.clone(),
        min_confidence: request.filter.min_confidence,
    };
    let mut response = graph_wire::GraphQueryResponse {
        op: request.op,
        backend: backend_to_wire(reader.backend()),
        snapshot_id: reader.snapshot_id(),
        neighbors: None,
        reached: None,
        subgraph: None,
        paths: None,
    };
    match request.op {
        graph_wire::GraphOp::Neighbors => {
            let out = reader.get_neighbors(&request.start, direction, &filter);
            response.neighbors = Some(out.iter().map(neighbor_to_wire).collect());
        }
        graph_wire::GraphOp::KHop => {
            let Some(k) = request.k else {
                return bad_request("`k` is required for a kHop query");
            };
            let out = reader.k_hop(&request.start, k, direction, &filter);
            response.reached = Some(out.iter().map(reached_to_wire).collect());
        }
        graph_wire::GraphOp::Neighborhood => {
            let Some(k) = request.k else {
                return bad_request("`k` is required for a neighborhood query");
            };
            let sub = reader.neighborhood(&request.start, k, direction, &filter);
            response.subgraph = Some(subgraph_to_wire(&sub));
        }
        graph_wire::GraphOp::Paths => {
            let Some(dst) = request.dst.as_deref() else {
                return bad_request("`dst` is required for a paths query");
            };
            let Some(max_hops) = request.max_hops else {
                return bad_request("`maxHops` is required for a paths query");
            };
            let out = reader.paths(&request.start, dst, max_hops, direction, &filter);
            response.paths = Some(out.iter().map(path_to_wire).collect());
        }
    }
    Json(response).into_response()
}

/// `GET /v1/graphs/{namespace}`: shows the graph's two backing tables, their
/// live row counts, and whether an adjacency index is bound to the current edge
/// snapshot. Answers 503 until the catalog handle is wired; a graph whose tables
/// do not exist is a 404.
async fn graphs_show(State(graphs): State<GraphsSlot>, Path(namespace): Path<String>) -> Response {
    let Some(catalog) = graphs.get() else {
        return recovering();
    };
    let graph = match Graph::open(catalog.clone(), &namespace) {
        Ok(graph) => graph,
        Err(error) => return graph_error(error),
    };
    let nodes_ident = graph.nodes_ident().clone();
    let edges_ident = graph.edges_ident().clone();
    let node_count = match tables_api::snapshot(catalog.as_ref(), &nodes_ident).await {
        Ok(snapshot) => snapshot.record_count,
        Err(error) => return table_error(error),
    };
    let edge_count = match tables_api::snapshot(catalog.as_ref(), &edges_ident).await {
        Ok(snapshot) => snapshot.record_count,
        Err(error) => return table_error(error),
    };
    let snapshot_id = match graph.current_edges_snapshot().await {
        Ok(id) => id,
        Err(error) => return graph_error(error),
    };
    // An index is "bound" when a reader over the current edge snapshot is served
    // by the index rather than the scan fallback.
    let indexed = match graph.reader(None).await {
        Ok(reader) => reader.backend() == ReaderBackend::Index,
        Err(error) => return graph_error(error),
    };
    Json(graph_wire::GraphShowReport {
        namespace: namespace.clone(),
        nodes_table: format!("{namespace}.nodes"),
        edges_table: format!("{namespace}.edges"),
        node_count: Some(node_count),
        edge_count: Some(edge_count),
        indexed,
        snapshot_id,
    })
    .into_response()
}

/// Converts a wire node input to the engine `Node`.
fn node_from_input(input: &graph_wire::NodeInput) -> Node {
    Node {
        id: input.id.clone(),
        labels: input.labels.clone(),
        properties: input.properties.clone(),
        agent_id: input.agent_id.clone(),
        namespace: input.namespace.clone(),
    }
}

/// Converts a wire edge input to the engine `Edge`, generating a fresh edge id
/// when the input omits one (via `Edge::new`) and copying the rest.
fn edge_from_input(input: &graph_wire::EdgeInput) -> Edge {
    let mut edge = Edge::new(
        input.src_id.clone(),
        input.predicate.clone(),
        input.dst_id.clone(),
        input.provenance.clone(),
    );
    if let Some(id) = &input.edge_id {
        edge.edge_id = id.clone();
    }
    edge.confidence = input.confidence;
    edge.supersedes = input.supersedes.clone();
    edge.valid_from = input.valid_from;
    edge.agent_id = input.agent_id.clone();
    edge.namespace = input.namespace.clone();
    edge.properties = input.properties.clone();
    edge
}

/// Maps the wire direction to the engine `Direction`.
fn direction_from_wire(direction: graph_wire::GraphDirection) -> Direction {
    match direction {
        graph_wire::GraphDirection::Out => Direction::Out,
        graph_wire::GraphDirection::In => Direction::In,
        graph_wire::GraphDirection::Both => Direction::Both,
    }
}

/// Maps the engine `Direction` back to the wire direction.
fn direction_to_wire(direction: Direction) -> graph_wire::GraphDirection {
    match direction {
        Direction::Out => graph_wire::GraphDirection::Out,
        Direction::In => graph_wire::GraphDirection::In,
        Direction::Both => graph_wire::GraphDirection::Both,
    }
}

/// Maps the reader backend to the wire backend tag.
fn backend_to_wire(backend: ReaderBackend) -> graph_wire::GraphBackend {
    match backend {
        ReaderBackend::Index => graph_wire::GraphBackend::Index,
        ReaderBackend::Scan => graph_wire::GraphBackend::Scan,
    }
}

/// The wire label for an index build mode (`full` today).
fn index_mode_label(mode: IndexBuildMode) -> String {
    match mode {
        IndexBuildMode::Full => "full".to_owned(),
    }
}

/// Converts an engine `Neighbor` to its wire view.
fn neighbor_to_wire(neighbor: &Neighbor) -> graph_wire::NeighborView {
    graph_wire::NeighborView {
        node_id: neighbor.node_id.clone(),
        predicate: neighbor.predicate.clone(),
        confidence: neighbor.confidence,
        edge_id: neighbor.edge_id.clone(),
        provenance: neighbor.provenance.clone(),
        direction: direction_to_wire(neighbor.direction),
    }
}

/// Converts an engine `Reached` to its wire view.
fn reached_to_wire(reached: &Reached) -> graph_wire::ReachedView {
    graph_wire::ReachedView {
        node_id: reached.node_id.clone(),
        hops: reached.hops,
        path_confidence: reached.path_confidence,
    }
}

/// Converts an engine `TripletReceipt` to its wire view.
fn triplet_to_wire(triplet: &TripletReceipt) -> graph_wire::TripletView {
    graph_wire::TripletView {
        src_id: triplet.src_id.clone(),
        predicate: triplet.predicate.clone(),
        dst_id: triplet.dst_id.clone(),
        confidence: triplet.confidence,
        edge_id: triplet.edge_id.clone(),
        provenance: triplet.provenance.clone(),
    }
}

/// Converts an engine `Subgraph` to its wire view.
fn subgraph_to_wire(subgraph: &Subgraph) -> graph_wire::SubgraphView {
    graph_wire::SubgraphView {
        nodes: subgraph.nodes.iter().map(reached_to_wire).collect(),
        edges: subgraph.edges.iter().map(triplet_to_wire).collect(),
    }
}

/// Converts an engine `Path` to its wire view.
fn path_to_wire(path: &GraphPath) -> graph_wire::PathView {
    graph_wire::PathView {
        nodes: path.nodes.clone(),
        edges: path.edges.iter().map(triplet_to_wire).collect(),
        confidence: path.confidence,
    }
}

/// A 400 response naming a bad-request reason (a graph query missing the bound
/// its op requires).
fn bad_request(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, message.to_owned()).into_response()
}

// ---------------------------------------------------------------------------
// Vector-index routes (`/v1/tables|graphs/{..}/indexes...`).
//
// Declaring an index builds and attaches it to the source snapshot. Searching
// requires an exact attachment for the current snapshot.
// ---------------------------------------------------------------------------

/// The vector-index sub-router. Table and graph indexes share the same service;
/// they differ only in how the source table identity is formed.
fn vector_router(vector: VectorSlot) -> Router {
    Router::new()
        .route(
            "/v1/tables/{name}/indexes",
            post(tables_index_declare).get(tables_index_list),
        )
        .route(
            "/v1/tables/{name}/indexes/{field}/search",
            post(tables_index_search),
        )
        .route(
            "/v1/tables/{name}/indexes/{field}/refresh",
            post(tables_index_refresh),
        )
        .route(
            "/v1/graphs/{namespace}/indexes",
            post(graphs_index_declare).get(graphs_index_list),
        )
        .route(
            "/v1/graphs/{namespace}/indexes/{field}/search",
            post(graphs_index_search),
        )
        .layer(axum::extract::DefaultBodyLimit::max(
            TABLES_BODY_LIMIT_BYTES,
        ))
        .with_state(vector)
}

/// `POST /v1/tables/{name}/indexes`: declare an index on a table field and run
/// the initial build.
async fn tables_index_declare(
    State(vector): State<VectorSlot>,
    Path(name): Path<String>,
    Json(request): Json<vector_wire::DeclareIndexRequest>,
) -> Response {
    let Some(rt) = vector.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    let key = IndexKey::table(&name, &request.field);
    declare_index(rt, ident, key, request).await
}

/// `GET /v1/tables/{name}/indexes`: list indexes declared on this table.
async fn tables_index_list(State(vector): State<VectorSlot>, Path(name): Path<String>) -> Response {
    let Some(rt) = vector.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    list_indexes(rt, ident, &format!("tbl:{name}")).await
}

/// `POST /v1/tables/{name}/indexes/{field}/search`: ANN search over a table
/// field. The exact current snapshot must carry the attachment.
async fn tables_index_search(
    State(vector): State<VectorSlot>,
    Path((name, field)): Path<(String, String)>,
    Json(request): Json<vector_wire::SearchRequest>,
) -> Response {
    let Some(rt) = vector.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    let key = IndexKey::table(&name, &field);
    search_index(rt, ident, key, request).await
}

/// `POST /v1/tables/{name}/indexes/{field}/refresh`: run one maintenance pass
/// for an already-declared table index, rebuilding its blob from the table's
/// current snapshot. The prior attachment carries the refresh configuration.
async fn tables_index_refresh(
    State(vector): State<VectorSlot>,
    Path((name, field)): Path<(String, String)>,
) -> Response {
    let Some(rt) = vector.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    let key = IndexKey::table(&name, &field);
    refresh_index(rt, ident, key).await
}

/// Shared refresh path: run the maintenance pass and return the newly attached
/// snapshot-bound index.
async fn refresh_index(rt: &VectorRuntime, ident: iceberg::TableIdent, key: IndexKey) -> Response {
    let report = match rt.service.refresh(rt.catalog.as_ref(), &ident, &key).await {
        Ok(report) => report,
        Err(error) => return vector_error(error),
    };
    let metric = report.as_ref().map_or(Metric::Cosine, |built| built.metric);
    Json(report_to_wire(key.target, key.field, metric, report)).into_response()
}

/// `POST /v1/graphs/{namespace}/indexes`: declare an index on a graph node-table
/// field. A graph's nodes live in `<namespace>.nodes`.
async fn graphs_index_declare(
    State(vector): State<VectorSlot>,
    Path(namespace): Path<String>,
    Json(request): Json<vector_wire::DeclareIndexRequest>,
) -> Response {
    let Some(rt) = vector.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&format!("{namespace}.nodes")) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    let key = IndexKey::graph(&namespace, &request.field);
    declare_index(rt, ident, key, request).await
}

/// `GET /v1/graphs/{namespace}/indexes`: list indexes declared on this graph.
async fn graphs_index_list(
    State(vector): State<VectorSlot>,
    Path(namespace): Path<String>,
) -> Response {
    let Some(rt) = vector.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&format!("{namespace}.nodes")) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    list_indexes(rt, ident, &format!("graph:{namespace}")).await
}

/// `POST /v1/graphs/{namespace}/indexes/{field}/search`: ANN search over a graph
/// node-table field.
async fn graphs_index_search(
    State(vector): State<VectorSlot>,
    Path((namespace, field)): Path<(String, String)>,
    Json(request): Json<vector_wire::SearchRequest>,
) -> Response {
    let Some(rt) = vector.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&format!("{namespace}.nodes")) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    let key = IndexKey::graph(&namespace, &field);
    search_index(rt, ident, key, request).await
}

/// Shared declare path for tables and graphs.
///
/// Declaring runs the initial build and commits its Puffin attachment through
/// the table's catalog.
async fn declare_index(
    rt: &VectorRuntime,
    ident: iceberg::TableIdent,
    key: IndexKey,
    request: vector_wire::DeclareIndexRequest,
) -> Response {
    let config = match config_from_request(&key.field, &request) {
        Ok(config) => config,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let metric = config.metric;
    let target = key.target.clone();
    let field = key.field.clone();
    let report = match rt
        .service
        .declare(rt.catalog.as_ref(), &ident, key.clone(), config.clone())
        .await
    {
        Ok(report) => report,
        Err(error) => return vector_error(error),
    };

    Json(report_to_wire(target, field, metric, report)).into_response()
}

/// Shared search path for tables and graphs.
async fn search_index(
    rt: &VectorRuntime,
    ident: iceberg::TableIdent,
    key: IndexKey,
    request: vector_wire::SearchRequest,
) -> Response {
    let opts = SearchOptions {
        k: request.k,
        l: request.l.unwrap_or(request.k.max(64)),
    };
    match rt
        .service
        .search(rt.catalog.as_ref(), &ident, &key, &request.vector, &opts)
        .await
    {
        Ok(outcome) => Json(vector_wire::SearchResponse {
            source: "index".to_owned(),
            neighbors: outcome
                .neighbors
                .into_iter()
                .map(|n| vector_wire::SearchHit {
                    id: n.id,
                    distance: n.distance,
                })
                .collect(),
        })
        .into_response(),
        Err(error) => vector_error(error),
    }
}

/// Shared list path.
async fn list_indexes(rt: &VectorRuntime, ident: iceberg::TableIdent, target: &str) -> Response {
    match rt.service.list(rt.catalog.as_ref(), &ident, target).await {
        Ok(all) => {
            let indexes = all
                .into_iter()
                .map(|d| vector_wire::IndexInfo {
                    target: d.target,
                    field: d.field,
                    metric: d.metric.as_str().to_owned(),
                    reflected_snapshot: Some(d.reflected_snapshot),
                    live_count: Some(d.live_count),
                })
                .collect();
            Json(vector_wire::IndexListResponse { indexes }).into_response()
        }
        Err(error) => vector_error(error),
    }
}

/// Builds a maintenance config from a declare request. A bad metric returns a
/// 400 message.
fn config_from_request(
    field: &str,
    request: &vector_wire::DeclareIndexRequest,
) -> Result<MaintenanceConfig, String> {
    let metric = Metric::parse(&request.metric)
        .ok_or_else(|| format!("unknown metric '{}': use 'l2' or 'cosine'", request.metric))?;
    let id_field = request.id_field.clone().unwrap_or_else(|| "id".to_owned());
    let mut params = VamanaParams::default();
    if let Some(p) = &request.params {
        if let Some(r) = p.r {
            params.r = r;
        }
        if let Some(l) = p.l {
            params.l_build = l;
        }
        if let Some(alpha) = p.alpha {
            params.alpha = alpha;
        }
    }
    let mut config = MaintenanceConfig::new(id_field, field, metric);
    config.params = params;
    Ok(config)
}

/// Maps a maintenance report (or the no-rows-yet `None`) to the wire report.
fn report_to_wire(
    target: String,
    field: String,
    metric: Metric,
    report: Option<MaintenanceReport>,
) -> vector_wire::IndexReport {
    match report {
        Some(r) => vector_wire::IndexReport {
            target,
            field,
            metric: metric.as_str().to_owned(),
            reflected_snapshot: Some(r.reflected_snapshot),
            full_build: r.full_build,
            inserts: r.inserts,
            deletes: r.deletes,
            consolidated: r.consolidated,
            live_count: r.live_count,
            tombstones: r.tombstones,
            blob_location: Some(r.blob_location),
            blob_bytes: r.blob_bytes,
        },
        None => vector_wire::IndexReport {
            target,
            field,
            metric: metric.as_str().to_owned(),
            reflected_snapshot: None,
            full_build: true,
            inserts: 0,
            deletes: 0,
            consolidated: false,
            live_count: 0,
            tombstones: 0,
            blob_location: None,
            blob_bytes: 0,
        },
    }
}

/// Maps a vector-engine error to an HTTP status: a bad field/dimension/id is the
/// caller's input (400), a missing table is a 404, and everything else is a 500.
fn vector_error(error: VectorError) -> Response {
    let status = match &error {
        VectorError::DimMismatch { .. }
        | VectorError::InvalidDim(_)
        | VectorError::Field(_)
        | VectorError::NoIdColumn(_) => StatusCode::BAD_REQUEST,
        VectorError::TablesApi(AgentError::BadIdent(_))
        | VectorError::TablesApi(AgentError::SchemaMismatch { .. })
        | VectorError::TablesApi(AgentError::TableApi(_)) => StatusCode::BAD_REQUEST,
        VectorError::IndexNotFound { .. } => StatusCode::NOT_FOUND,
        VectorError::TablesApi(AgentError::Iceberg(inner)) | VectorError::Iceberg(inner)
            if inner.kind() == iceberg::ErrorKind::TableNotFound
                || inner.kind() == iceberg::ErrorKind::NamespaceNotFound =>
        {
            StatusCode::NOT_FOUND
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string()).into_response()
}

/// Maps a graph-engine error to an HTTP status: a malformed row or a bad
/// namespace is the caller's input (400), a missing table/namespace is a 404,
/// and everything else is a 500. A corrupt index never surfaces here — the
/// reader falls back to a scan.
fn graph_error(error: GraphError) -> Response {
    match error {
        GraphError::Engine(inner) => table_error(inner),
        GraphError::Iceberg(inner)
            if inner.kind() == iceberg::ErrorKind::TableNotFound
                || inner.kind() == iceberg::ErrorKind::NamespaceNotFound =>
        {
            (StatusCode::NOT_FOUND, inner.to_string()).into_response()
        }
        GraphError::MalformedRow { .. } => {
            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
        other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()).into_response(),
    }
}

/// The `verglas_sys` registry sub-router: register, list, show, and state
/// transitions for workers. Every route answers 503 until the registry handle
/// is wired after recovery. Registry writes go through this server-held handle
/// only — the CLI never writes `verglas_sys` directly.
fn sys_router(sys: SysSlot) -> Router {
    Router::new()
        .route("/v1/workers", post(worker_register).get(worker_list))
        .route("/v1/workers/{name}", get(worker_show))
        .route("/v1/workers/{name}/state", put(worker_set_state))
        .with_state(sys)
}

/// The platform queue sub-router (`/v1/queues/<name>/{enqueue,poll,ack}`, #328):
/// workers can exchange batches through a queue instead of a table, and the TS
/// SDK queue verb speaks these routes. Backed by [`verglas_harness::queue`] — a
/// per-queue durable segment log under the queue root — with at-least-once
/// delivery and consumer-group watermarks. The routes carry `serde_json::Value`
/// payloads (row-shaped JSON). Segment-log operations are synchronous file IO,
/// so each runs on the blocking pool, never on an async worker.
fn queue_router(queues: QueueDir) -> Router {
    Router::new()
        .route("/v1/queues/{name}/enqueue", post(queue_enqueue))
        .route("/v1/queues/{name}/poll", get(queue_poll))
        .route("/v1/queues/{name}/ack", post(queue_ack))
        .with_state(queues)
}

/// The body of `POST /v1/queues/{name}/enqueue`: the rows to append.
#[derive(Debug, Deserialize)]
struct QueueEnqueueBody {
    /// The row payloads to append, in order.
    rows: Vec<serde_json::Value>,
}

/// Query parameters for `GET /v1/queues/{name}/poll`: the consumer group and an
/// optional page bound.
#[derive(Debug, Deserialize)]
struct QueuePollQuery {
    /// The consumer group whose watermark bounds the read.
    group: String,
    /// The maximum records to return this poll (defaults to a page).
    max: Option<usize>,
}

/// The body of `POST /v1/queues/{name}/ack`: the group and the position to
/// advance its watermark to.
#[derive(Debug, Deserialize)]
struct QueueAckBody {
    /// The consumer group whose watermark advances.
    group: String,
    /// The position acked through (typically the last polled position + 1).
    position: u64,
}

/// The default poll page when the caller names no `max`.
const QUEUE_POLL_DEFAULT_MAX: usize = 256;

/// Maps a queue IO failure to a 500 with a plain message.
fn queue_error(error: std::io::Error) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("queue: {error}")).into_response()
}

/// `POST /v1/queues/{name}/enqueue`: appends the body's rows to the named queue,
/// returning how many landed and the queue's end position. Matches the TS SDK
/// `QueueEnqueueResult`.
async fn queue_enqueue(
    State(queues): State<QueueDir>,
    Path(name): Path<String>,
    Json(body): Json<QueueEnqueueBody>,
) -> Response {
    let root = verglas_harness::queue::queue_root(&queues);
    let result = tokio::task::spawn_blocking(move || -> std::io::Result<(usize, u64)> {
        let log = verglas_harness::queue::SegmentLog::<serde_json::Value>::open(&root, &name)?;
        let mut enqueued = 0usize;
        for row in &body.rows {
            if log.append(row)? {
                enqueued += 1;
            }
        }
        Ok((enqueued, log.end_position()?))
    })
    .await;
    match result {
        Ok(Ok((enqueued, end_position))) => Json(json!({
            "enqueued": enqueued,
            "endPosition": end_position,
        }))
        .into_response(),
        Ok(Err(e)) => queue_error(e),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("queue task: {e}"),
        )
            .into_response(),
    }
}

/// `GET /v1/queues/{name}/poll?group=&max=`: reads up to `max` records at or
/// after the group's watermark, plus the group's current watermark. Matches the
/// TS SDK `QueuePollResult`. Reads without an ack re-serve the same records.
async fn queue_poll(
    State(queues): State<QueueDir>,
    Path(name): Path<String>,
    Query(query): Query<QueuePollQuery>,
) -> Response {
    let root = verglas_harness::queue::queue_root(&queues);
    let max = query.max.unwrap_or(QUEUE_POLL_DEFAULT_MAX);
    let group = query.group;
    let result = tokio::task::spawn_blocking(
        move || -> std::io::Result<(Vec<(u64, serde_json::Value)>, u64)> {
            let log = verglas_harness::queue::SegmentLog::<serde_json::Value>::open(&root, &name)?;
            let watermark = log.watermark(&group)?;
            let records = log.read_from(watermark, max)?;
            Ok((records, watermark))
        },
    )
    .await;
    match result {
        Ok(Ok((records, watermark))) => {
            let records: Vec<serde_json::Value> = records
                .into_iter()
                .map(|(position, row)| json!({ "position": position, "row": row }))
                .collect();
            Json(json!({ "records": records, "watermark": watermark })).into_response()
        }
        Ok(Err(e)) => queue_error(e),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("queue task: {e}"),
        )
            .into_response(),
    }
}

/// `POST /v1/queues/{name}/ack`: advances the group's watermark to `position`
/// (monotone — a regressing position is ignored), returning the resulting
/// watermark. Matches the TS SDK `ack` reply.
async fn queue_ack(
    State(queues): State<QueueDir>,
    Path(name): Path<String>,
    Json(body): Json<QueueAckBody>,
) -> Response {
    let root = verglas_harness::queue::queue_root(&queues);
    let group = body.group;
    let position = body.position;
    let result = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
        let log = verglas_harness::queue::SegmentLog::<serde_json::Value>::open(&root, &name)?;
        log.ack(&group, position)?;
        log.watermark(&group)
    })
    .await;
    match result {
        Ok(Ok(watermark)) => Json(json!({ "watermark": watermark })).into_response(),
        Ok(Err(e)) => queue_error(e),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("queue task: {e}"),
        )
            .into_response(),
    }
}

/// Worker ingress routes: manual dispatch, named webhook compatibility, and
/// deployment-configured dynamic HTTP paths. Each creates a durable job object.
fn platform_router(platform: PlatformSlot) -> Router {
    Router::new()
        .route("/v1/events", post(worker_event))
        .route("/v1/workers/{name}/run", post(worker_run_now))
        .route("/v1/hooks/{name}", post(worker_webhook))
        .route("/v1/http/{*path}", any(worker_dynamic_http))
        .with_state(platform)
}

/// Accepts one structured CloudEvent and fans it out to exact worker
/// subscriptions. The CloudEvent's source and id remain the durable
/// idempotency identity after broker delivery.
async fn worker_event(
    State(platform): State<PlatformSlot>,
    Json(event): Json<verglas_sdk::worker::CloudEvent>,
) -> Response {
    let Some(ingress) = platform.get() else {
        return recovering();
    };
    match ingress.event(event, chrono::Utc::now()).await {
        Ok(outcomes) => {
            let jobs: Vec<serde_json::Value> = outcomes
                .into_iter()
                .map(|outcome| match outcome {
                    verglas_scheduler::EnqueueOutcome::Created(job_id) => {
                        json!({ "job_id": job_id, "created": true })
                    }
                    verglas_scheduler::EnqueueOutcome::Existing(job_id) => {
                        json!({ "job_id": job_id, "created": false })
                    }
                })
                .collect();
            (
                StatusCode::ACCEPTED,
                Json(json!({ "matched": jobs.len(), "jobs": jobs })),
            )
                .into_response()
        }
        Err(crate::platform::IngressError::Invalid(message)) => {
            (StatusCode::BAD_REQUEST, message).into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

/// Enqueues one manual invocation for a running worker.
async fn worker_run_now(
    State(platform): State<PlatformSlot>,
    Path(name): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    let request_id = match idempotency_key(&headers) {
        Ok(request_id) => request_id,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let Some(ingress) = platform.get() else {
        return recovering();
    };
    enqueue_response(ingress.manual(&name, request_id).await)
}

/// Enqueues an inbound request for one named webhook worker.
async fn worker_webhook(
    State(platform): State<PlatformSlot>,
    Path(name): Path<String>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_id = match idempotency_key(&headers) {
        Ok(request_id) => request_id,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let Some(ingress) = platform.get() else {
        return recovering();
    };
    enqueue_response(
        ingress
            .webhook(
                &name,
                request_id,
                verglas_sdk::worker::HttpCallback {
                    method: method.to_string(),
                    path: uri
                        .path_and_query()
                        .map_or_else(|| uri.path().to_owned(), ToString::to_string),
                    headers: callback_headers(&headers),
                    body: body.to_vec(),
                },
            )
            .await,
    )
}

/// Routes a dynamically configured HTTP path to its owning deployment.
async fn worker_dynamic_http(
    State(platform): State<PlatformSlot>,
    Path(path): Path<String>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_id = match idempotency_key(&headers) {
        Ok(request_id) => request_id,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let Some(ingress) = platform.get() else {
        return recovering();
    };
    enqueue_response(
        ingress
            .dynamic_http(
                &format!("/{path}"),
                request_id,
                verglas_sdk::worker::HttpCallback {
                    method: method.to_string(),
                    path: uri
                        .query()
                        .map_or_else(|| format!("/{path}"), |query| format!("/{path}?{query}")),
                    headers: callback_headers(&headers),
                    body: body.to_vec(),
                },
            )
            .await,
    )
}

/// Copies end-to-end request headers into the durable callback event.
fn callback_headers(headers: &HeaderMap) -> std::collections::BTreeMap<String, String> {
    const HOP_BY_HOP: &[&str] = &[
        "connection",
        "content-length",
        "host",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ];
    headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str();
            if HOP_BY_HOP.contains(&name) || name == "idempotency-key" {
                return None;
            }
            value
                .to_str()
                .ok()
                .map(|value| (name.to_owned(), value.to_owned()))
        })
        .collect()
}

/// Reads the caller-owned idempotency identity required by all HTTP ingress.
fn idempotency_key(headers: &axum::http::HeaderMap) -> Result<String, &'static str> {
    headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or("Idempotency-Key header is required")
}

/// Maps a create-only enqueue result onto accepted or idempotent-join JSON.
fn enqueue_response(
    result: Result<verglas_scheduler::EnqueueOutcome, crate::platform::IngressError>,
) -> Response {
    match result {
        Ok(verglas_scheduler::EnqueueOutcome::Created(job_id)) => (
            StatusCode::ACCEPTED,
            Json(json!({ "job_id": job_id, "created": true })),
        )
            .into_response(),
        Ok(verglas_scheduler::EnqueueOutcome::Existing(job_id)) => {
            Json(json!({ "job_id": job_id, "created": false })).into_response()
        }
        Err(crate::platform::IngressError::Invalid(message)) => {
            (StatusCode::NOT_FOUND, message).into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

/// Maps a registry failure to an HTTP response: an unknown name is a 404, a
/// decode failure of caller input is a 400 upstream of this call, and anything
/// else (catalog IO) is a 500 with the plain message.
fn platform_error(error: verglas_platform::PlatformError) -> Response {
    use verglas_platform::PlatformError;
    let status = match &error {
        PlatformError::NotFound { .. } => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string()).into_response()
}

/// Query parameters for `GET /v1/workers`: which lifecycle view to list.
#[derive(Debug, Deserialize)]
struct ListView {
    /// `active` (default) or `all`.
    view: Option<String>,
}

/// The body of the `PUT .../state` route: the target lifecycle state.
#[derive(Debug, Deserialize)]
struct StatePut {
    /// The state to append (`running`, `paused`, `archived`, ...).
    state: String,
}

/// Parses a `state` body value; the error is the message for the 400 the
/// caller builds (kept as a string so the `Err` stays small for clippy's
/// large-error lint).
fn parse_state(state: &str) -> Result<verglas_platform::SystemState, String> {
    verglas_platform::SystemState::parse(state).map_err(|e| e.to_string())
}

/// `POST /v1/workers`: declares a worker (a new append-only revision; a
/// re-register bumps the revision and preserves `created_at`).
async fn worker_register(
    State(sys): State<SysSlot>,
    Json(spec): Json<verglas_platform::WorkerSpec>,
) -> Response {
    let Some(sys) = sys.get() else {
        return recovering();
    };
    match sys.register_worker(spec).await {
        Ok(row) => Json(row).into_response(),
        Err(error) => platform_error(error),
    }
}

/// `GET /v1/workers?view=`: the current view of the worker registry — `active`
/// (default) or `all`.
async fn worker_list(State(sys): State<SysSlot>, Query(query): Query<ListView>) -> Response {
    let Some(sys) = sys.get() else {
        return recovering();
    };
    let result = match query.view.as_deref() {
        None | Some("active") => sys.list_active_workers().await,
        Some("all") => sys.list_workers().await,
        Some(other) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("unknown view `{other}`: expected active or all"),
            )
                .into_response();
        }
    };
    match result {
        Ok(rows) => Json(rows).into_response(),
        Err(error) => platform_error(error),
    }
}

/// `GET /v1/workers/{name}`: the current revision of one worker, or 404.
async fn worker_show(State(sys): State<SysSlot>, Path(name): Path<String>) -> Response {
    let Some(sys) = sys.get() else {
        return recovering();
    };
    match sys.get_worker(&name).await {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("no worker named {name}")).into_response(),
        Err(error) => platform_error(error),
    }
}

/// `PUT /v1/workers/{name}/state`: appends a revision flipping the worker's
/// lifecycle state (pause/resume/archive).
async fn worker_set_state(
    State(sys): State<SysSlot>,
    Path(name): Path<String>,
    Json(body): Json<StatePut>,
) -> Response {
    let Some(sys) = sys.get() else {
        return recovering();
    };
    let state = match parse_state(&body.state) {
        Ok(state) => state,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    match sys.set_worker_state(&name, state).await {
        Ok(row) => Json(row).into_response(),
        Err(error) => platform_error(error),
    }
}

/// Maps a table-API failure to an HTTP response. A bad identifier, a schema
/// mismatch, or a malformed request/cursor is the caller's fault (400); a missing
/// table is a 404; anything else is a 500 with the plain-English message.
fn table_error(error: AgentError) -> Response {
    let status = match &error {
        AgentError::BadIdent(_) | AgentError::SchemaMismatch { .. } | AgentError::TableApi(_) => {
            StatusCode::BAD_REQUEST
        }
        AgentError::Iceberg(inner)
            if inner.kind() == iceberg::ErrorKind::TableNotFound
                || inner.kind() == iceberg::ErrorKind::NamespaceNotFound =>
        {
            StatusCode::NOT_FOUND
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string()).into_response()
}

/// The 503 body every engine-dependent route returns until recovery completes.
fn recovering() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "verglas-server cache engine is still recovering",
    )
        .into_response()
}

/// Returns the server readiness payload: `ok`/200 once recovery is complete,
/// `starting`/503 while it runs (serve-gating, #16).
async fn healthz(State(health): State<Health>) -> Response {
    if health.is_ready() {
        (StatusCode::OK, Json(HealthzInfo::ok())).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthzInfo::starting()),
        )
            .into_response()
    }
}

/// Hot-reloads the process log filter (#61). A parseable filter is applied and
/// echoed back; an unparseable one is a 400 with the parser's message, so a
/// typo at the CLI is a clear rejection rather than a silent no-op. Loopback-only
/// by construction — reachable only through the admin listener.
async fn set_log_level(Json(request): Json<LogLevelRequest>) -> Response {
    match crate::logging::reload_level(&request.level) {
        Ok(level) => {
            tracing::info!(level = %level, "log level changed via admin API");
            (StatusCode::OK, Json(LogLevelInfo { level })).into_response()
        }
        Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
    }
}

/// Resets the cache to cold (issue #138) by bumping the cache generation
/// (#178): an O(1) logical clear that returns immediately, then logs one line
/// with the byte counts and returns them. Loopback-only by construction — this
/// handler is reachable only through the admin listener, never the S3 surface.
/// Answers 503 until the engine has finished recovering (#16).
async fn purge(State(purger): State<PurgerSlot>) -> Response {
    let Some(purger) = purger.get() else {
        return recovering();
    };
    let report = purger.purge().await;
    eprintln!(
        "verglas-server cache purge: generation now {}, mappings freed {} bytes, \
         {} DRAM bytes now reclaimable (physically resident until LRU reclaim)",
        report.generation, report.mapping_bytes_freed, report.reclaimable_bytes
    );
    Json(report).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VERSION;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;
    use verglas_backend::BackendStore;
    use verglas_cache::HybridCacheEngine;
    use verglas_core::admin::PurgeReport;
    use verglas_core::config::{ByteSize, Cache as CacheConfig};
    use verglas_s3::PassthroughRead;

    /// Builds a real cluster-of-one engine over an empty in-memory origin,
    /// erased to `Arc<dyn CachePurger>` — the same handle the server hands the
    /// admin router.
    async fn purger(dir: &std::path::Path) -> Arc<dyn CachePurger> {
        let store = std::sync::Arc::new(object_store::memory::InMemory::new());
        let backend = PassthroughRead::new(BackendStore::single("test-bucket", store));
        let config = CacheConfig {
            dir: dir.to_path_buf(),
            capacity_bytes: ByteSize(64 * 1024 * 1024),
            dram_bytes: ByteSize(128 * 1024 * 1024),
            ..CacheConfig::default()
        };
        let engine = HybridCacheEngine::single_node(backend, &config)
            .await
            .expect("build engine");
        Arc::new(engine)
    }

    /// A purger slot already filled with a real engine — the post-recovery
    /// state the server leaves the slot in.
    async fn ready_purger(dir: &std::path::Path) -> PurgerSlot {
        let slot: PurgerSlot = Arc::new(OnceLock::new());
        let _ = slot.set(purger(dir).await);
        slot
    }

    /// Durable HTTP events keep application headers but not transport framing
    /// or the scheduler's idempotency key.
    #[test]
    fn callback_headers_keep_only_end_to_end_values() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().expect("header"));
        headers.insert("connection", "keep-alive".parse().expect("header"));
        headers.insert("idempotency-key", "request-1".parse().expect("header"));
        assert_eq!(
            callback_headers(&headers),
            std::collections::BTreeMap::from([(
                "content-type".to_owned(),
                "application/json".to_owned(),
            )])
        );
    }

    /// Sends a request to the router and returns the response.
    async fn call(app: Router, method: &str, uri: &str) -> Response {
        app.oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router responds")
    }

    /// Without an on-prem catalog gateway, the catalog route is absent.
    #[tokio::test]
    async fn catalog_route_is_absent_without_a_gateway() {
        let absent = router(VERSION, Health::ready(), Slots::default());
        let response = call(absent, "GET", "/catalog/v1/config").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// `GET /admin/access` (issue #287) returns the local-access snapshot when
    /// one is configured, and the route is absent otherwise so an older/config-
    /// less server simply 404s (the CLI then falls back to flags/env). The served
    /// body carries the discovery fields and never a `secret_access_key`: the
    /// admin socket is unauthenticated and host-scoped, so a secret served here
    /// would leak lakehouse read/write access to any local process.
    #[tokio::test]
    async fn access_route_serves_the_snapshot_without_the_secret() {
        let access = LocalAccess {
            s3_endpoint: "http://127.0.0.1:8333".to_owned(),
            query_uri: "http://127.0.0.1:8334".to_owned(),
            catalog_uri: Some("https://catalog.example.test".to_owned()),
            warehouse: Some("s3://warehouse/tenant".to_owned()),
            region: "us-east-1".to_owned(),
            bucket: Some("warehouse".to_owned()),
            access_key_id: Some("VGKEY".to_owned()),
        };
        let configured = router(
            VERSION,
            Health::ready(),
            Slots {
                access: Some(access.clone()),
                ..Slots::default()
            },
        );
        let response = call(configured, "GET", "/admin/access").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let decoded: LocalAccess = serde_json::from_slice(&body).expect("access json");
        assert_eq!(decoded, access);

        // The raw served JSON must carry the discovery fields and no secret.
        let value: serde_json::Value = serde_json::from_slice(&body).expect("access json value");
        for key in [
            "s3_endpoint",
            "query_uri",
            "catalog_uri",
            "warehouse",
            "region",
            "bucket",
            "access_key_id",
        ] {
            assert!(
                value.get(key).is_some(),
                "served access body must carry `{key}`"
            );
        }
        assert!(
            value.get("secret_access_key").is_none(),
            "served access body must not carry a secret_access_key field"
        );

        let absent = router(VERSION, Health::ready(), Slots::default());
        let response = call(absent, "GET", "/admin/access").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// `POST /cache/purge` on the router routes to the handler, invokes the
    /// engine's purge, and returns the `PurgeReport` — a cold engine reports
    /// zero bytes freed with the disk tier cleared.
    #[tokio::test]
    async fn purge_route_resets_the_engine_and_reports_counts() {
        let dir = tempfile::tempdir().expect("temp dir");
        let app = router(
            VERSION,
            Health::ready(),
            Slots {
                purger: Some(ready_purger(dir.path()).await),
                ..Slots::default()
            },
        );

        let response = call(app, "POST", PURGE_PATH).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let report: PurgeReport = serde_json::from_slice(&body).expect("purge report");
        assert_eq!(report.mapping_bytes_freed, 0);
        assert_eq!(report.reclaimable_bytes, 0);
        assert_eq!(
            report.generation, 1,
            "the first purge bumps to generation 1"
        );
    }

    /// Without a purger slot (the config-less server), the purge route is absent
    /// — the endpoint exists only when a cache engine does.
    #[tokio::test]
    async fn purge_route_is_absent_without_an_engine() {
        let app = router(VERSION, Health::ready(), Slots::default());
        let response = call(app, "POST", PURGE_PATH).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// Serve-gating (#16): `/admin/healthz` reports `starting` (503) until the
    /// engine finishes recovery, then `ok` (200) once the gate is flipped — the
    /// exact signal a load balancer needs to hold traffic off a cold node.
    #[tokio::test]
    async fn healthz_reports_starting_until_recovery_then_ok() {
        let health = Health::starting();
        // While recovering: 503 + "starting".
        let app = router(VERSION, health.clone(), Slots::default());
        let response = call(app, "GET", HEALTHZ_PATH).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let info: HealthzInfo = serde_json::from_slice(&body).expect("healthz");
        assert_eq!(info.status, "starting");

        // Recovery completes: 200 + "ok".
        health.mark_ready();
        let app = router(VERSION, health, Slots::default());
        let response = call(app, "GET", HEALTHZ_PATH).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let info: HealthzInfo = serde_json::from_slice(&body).expect("healthz");
        assert_eq!(info.status, "ok");
    }

    /// A ready server's health probe answers `ok` even with the engine routes
    /// wired — the purge wiring does not disturb the base probes.
    #[tokio::test]
    async fn healthz_answers_ok_with_a_purger_present() {
        let dir = tempfile::tempdir().expect("temp dir");
        let app = router(
            VERSION,
            Health::ready(),
            Slots {
                purger: Some(ready_purger(dir.path()).await),
                ..Slots::default()
            },
        );
        let response = call(app, "GET", HEALTHZ_PATH).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let info: HealthzInfo = serde_json::from_slice(&body).expect("healthz");
        assert_eq!(info.status, "ok");
    }

    /// The engine-dependent routes exist while recovery runs but answer 503
    /// until their deferred slot is filled — the router can be served before the
    /// engine is ready (#16), and purge is never a route-not-found flap.
    #[tokio::test]
    async fn purge_route_is_present_but_503_while_the_slot_is_unfilled() {
        let empty: PurgerSlot = Arc::new(OnceLock::new());
        let app = router(
            VERSION,
            Health::starting(),
            Slots {
                purger: Some(empty),
                ..Slots::default()
            },
        );
        let response = call(app, "POST", PURGE_PATH).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// `POST /admin/drain` (issue #31) with a filled slot routes to the handler
    /// — invoked with the default request when the body is empty — and returns
    /// its `DrainAck`. The handler is a canned closure here (the real one, over
    /// the gossip agent, lives in `main`); this proves the wiring and the
    /// empty-body default.
    #[tokio::test]
    async fn drain_route_invokes_the_handler_and_returns_the_ack() {
        let slot: DrainSlot = Arc::new(OnceLock::new());
        let handler: DrainHandler = Arc::new(|req: DrainRequest| {
            Box::pin(async move {
                DrainAck {
                    node_id: "node-2".to_owned(),
                    state: "draining".to_owned(),
                    // Echo the timeout so the empty-body default is observable.
                    timeout_secs: req.timeout_secs.unwrap_or(600),
                }
            })
        });
        let _ = slot.set(handler);
        let app = router(
            VERSION,
            Health::ready(),
            Slots {
                drain: Some(slot),
                ..Slots::default()
            },
        );

        let response = call(app, "POST", DRAIN_PATH).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let ack: DrainAck = serde_json::from_slice(&body).expect("drain ack");
        assert_eq!(ack.node_id, "node-2");
        assert_eq!(ack.state, "draining");
        assert_eq!(ack.timeout_secs, 600, "an empty body takes the default");
    }

    /// Without a drain slot (a single-node server) the drain route is absent —
    /// a cluster of one has nowhere to drain to.
    #[tokio::test]
    async fn drain_route_is_absent_without_a_cluster() {
        let app = router(VERSION, Health::ready(), Slots::default());
        let response = call(app, "POST", DRAIN_PATH).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// The drain route exists while recovery runs but answers 503 until its
    /// deferred slot is filled (#16) — never a route-not-found flap.
    #[tokio::test]
    async fn drain_route_is_present_but_503_while_the_slot_is_unfilled() {
        let empty: DrainSlot = Arc::new(OnceLock::new());
        let app = router(
            VERSION,
            Health::starting(),
            Slots {
                drain: Some(empty),
                ..Slots::default()
            },
        );
        let response = call(app, "POST", DRAIN_PATH).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// A metrics slot filled with a canned renderer: the same post-recovery
    /// state the server leaves the slot in, but with a fixed body so the route
    /// wiring — not the render logic — is under test here.
    fn ready_metrics(body: &'static str) -> MetricsSlot {
        let slot: MetricsSlot = Arc::new(OnceLock::new());
        let _ = slot.set(Arc::new(move || body.to_owned()));
        slot
    }

    /// `GET /metrics` (issue #46) serves the Prometheus text exposition with the
    /// `text/plain; version=0.0.4` content type — the core acceptance surface.
    #[tokio::test]
    async fn metrics_route_serves_the_exposition_with_the_prometheus_content_type() {
        let body = "# TYPE verglas_cache_hits_total counter\nverglas_cache_hits_total 7\n";
        let app = router(
            VERSION,
            Health::ready(),
            Slots {
                metrics: Some(ready_metrics(body)),
                ..Slots::default()
            },
        );
        let response = call(app, "GET", METRICS_PATH).await;
        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        assert_eq!(content_type.as_deref(), Some(EXPOSITION_CONTENT_TYPE));
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let text = String::from_utf8(bytes.to_vec()).expect("utf8");
        assert!(text.contains("verglas_cache_hits_total 7"));
    }

    /// Without a metrics slot (the config-less server) the metrics route is
    /// absent — the endpoint exists only when a cache engine does.
    #[tokio::test]
    async fn metrics_route_is_absent_without_an_engine() {
        let app = router(VERSION, Health::ready(), Slots::default());
        let response = call(app, "GET", METRICS_PATH).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// The metrics route exists while recovery runs but answers 503 until its
    /// deferred slot is filled (#16) — never a route-not-found flap.
    #[tokio::test]
    async fn metrics_route_is_present_but_503_while_the_slot_is_unfilled() {
        let empty: MetricsSlot = Arc::new(OnceLock::new());
        let app = router(
            VERSION,
            Health::starting(),
            Slots {
                metrics: Some(empty),
                ..Slots::default()
            },
        );
        let response = call(app, "GET", METRICS_PATH).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Builds a filled tables slot: a memory catalog with `sdk.events` created
    /// from a one-row CSV seed, wrapped as the server wires it post-recovery.
    async fn tables_slot_with_table() -> TablesSlot {
        use iceberg::CatalogBuilder;
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};

        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                std::collections::HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.path().to_str().expect("utf8").to_string(),
                )]),
            )
            .await
            .expect("memory catalog");
        std::mem::forget(warehouse);
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);

        let ident = parse_table_ident("sdk.events").expect("ident");
        let dir = tempfile::tempdir().expect("src dir");
        let path = dir.path().join("seed.csv");
        std::fs::write(&path, "id,name\n1,seed\n").expect("write csv");
        verglas_iceberg::write::create_table(catalog.as_ref(), &ident, &path, None)
            .await
            .expect("create table");
        std::mem::forget(dir);

        let slot: TablesSlot = Arc::new(OnceLock::new());
        let _ = slot.set(catalog);
        slot
    }

    /// Dashboard routes are absent unless the optional Rill runtime is wired.
    #[tokio::test]
    async fn dashboard_routes_are_absent_without_rill_configuration() {
        let app = router(VERSION, Health::ready(), Slots::default());
        let response = call(app, "GET", "/v1/dashboards").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// Creating a dashboard resolves the table and writes Rill project resources.
    #[tokio::test]
    async fn dashboard_create_writes_rill_resources_for_the_catalog_table() {
        let rill = crate::dashboard::test_runtime(tables_slot_with_table().await).await;
        let recorder = rill.test_recorder();
        let app = router(
            VERSION,
            Health::ready(),
            Slots {
                dashboards: Some(Arc::new(rill)),
                ..Slots::default()
            },
        );
        let response = call_json(
            app.clone(),
            "POST",
            "/v1/dashboards",
            json!({"table": "sdk.events"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["name"], "sdk_events");
        assert_eq!(body["table"], "sdk.events");
        assert_eq!(body["url"], "http://127.0.0.1:9009/explore/sdk_events");

        let files = recorder.files();
        assert!(files.contains_key("rill.yaml"));
        assert!(files.contains_key("connectors/verglas.yaml"));
        assert!(files.contains_key("models/sdk_events.yaml"));
        assert!(files.contains_key("metrics/sdk_events.yaml"));
        assert!(files.contains_key("dashboards/sdk_events.yaml"));
        assert!(files["models/sdk_events.yaml"].contains("iceberg_scan"));
        assert!(
            files["models/sdk_events.yaml"].contains("create_secrets_from_connectors: verglas")
        );
        assert!(files["models/sdk_events.yaml"].contains("materialize: true"));
        assert!(files["dashboards/sdk_events.yaml"].contains("metrics_view: sdk_events"));

        // Repeating create refreshes the same owned files instead of creating
        // duplicates or failing on its own resources.
        let response = call_json(
            app.clone(),
            "POST",
            "/v1/dashboards",
            json!({"table": "sdk.events"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = call(app.clone(), "GET", "/v1/dashboards").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["dashboards"].as_array().expect("dashboards").len(), 1);

        let response = call(app.clone(), "GET", "/v1/dashboards/sdk_events").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["table"], "sdk.events");

        let response = call(app, "DELETE", "/v1/dashboards/sdk_events").await;
        assert_eq!(response.status(), StatusCode::OK);
        let files = recorder.files();
        assert!(files.contains_key("connectors/verglas.yaml"));
        assert!(!files.contains_key("models/sdk_events.yaml"));
        assert!(!files.contains_key("metrics/sdk_events.yaml"));
        assert!(!files.contains_key("dashboards/sdk_events.yaml"));
    }

    /// A generated name collision never overwrites a user-owned Rill file.
    #[tokio::test]
    async fn dashboard_create_refuses_an_unowned_rill_resource() {
        let rill = crate::dashboard::test_runtime(tables_slot_with_table().await).await;
        rill.test_recorder()
            .insert("models/clash.yaml", "type: model\nsql: SELECT 1\n");
        let app = router(
            VERSION,
            Health::ready(),
            Slots {
                dashboards: Some(Arc::new(rill)),
                ..Slots::default()
            },
        );
        let response = call_json(
            app,
            "POST",
            "/v1/dashboards",
            json!({"table": "sdk.events", "name": "clash"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    /// Sends a request with a JSON body and returns the response.
    async fn call_json(app: Router, method: &str, uri: &str, body: serde_json::Value) -> Response {
        app.oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&body).expect("serialize body"),
                ))
                .expect("request"),
        )
        .await
        .expect("router responds")
    }

    /// Reads a response body as JSON.
    async fn json_body(response: Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("json body")
    }

    /// A configured platform exposes standard structured CloudEvent ingress.
    /// The empty deferred slot returns 503, proving the route exists without
    /// requiring a live catalog and scheduler in this router test.
    #[tokio::test]
    async fn platform_mounts_structured_cloudevent_ingress() {
        let platform: PlatformSlot = Arc::new(OnceLock::new());
        let event = serde_json::json!({
            "specversion": "1.0",
            "id": "quote-1",
            "source": "urn:rabbitmq:market-data",
            "type": "com.yahoo.finance.quote",
            "subject": "SPY",
            "data": {"close": 632.08}
        });
        let response = call_json(
            router(
                VERSION,
                Health::ready(),
                Slots {
                    platform: Some(platform),
                    ..Slots::default()
                },
            ),
            "POST",
            "/v1/events",
            event,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// A tables route on a router whose slot is not yet filled answers 503, like
    /// the other engine-dependent routes before recovery.
    #[tokio::test]
    #[cfg(any())]
    async fn tables_routes_answer_503_until_the_catalog_is_wired() {
        let empty: TablesSlot = Arc::new(OnceLock::new());
        let app = router(
            VERSION,
            Health::ready(),
            Slots {
                tables: Some(empty),
                ..Slots::default()
            },
        );
        let response = call(app, "GET", "/v1/tables/sdk.events/snapshot").await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// The commit, snapshot, and rows routes serve the SDK contract with its
    /// camelCase shapes: commit appends and returns the new snapshot as the
    /// watermark, snapshot reports the live count, and rows pages with a cursor.
    #[tokio::test]
    #[cfg(any())]
    async fn commit_snapshot_and_rows_serve_the_sdk_shapes() {
        let slot = tables_slot_with_table().await;
        let router_of = || {
            router(
                VERSION,
                Health::ready(),
                Slots {
                    tables: Some(slot.clone()),
                    ..Slots::default()
                },
            )
        };

        // The seed row is visible.
        let body =
            json_body(call(router_of(), "GET", "/v1/tables/sdk.events/snapshot").await).await;
        assert!(body.get("snapshotId").is_some(), "camelCase snapshotId");
        assert!(body.get("watermark").is_some());
        assert_eq!(body["recordCount"], 1);

        // Commit two rows.
        let body = json_body(
            call_json(
                router_of(),
                "POST",
                "/v1/tables/sdk.events/commit",
                serde_json::json!({"rows": [{"id": 2, "name": "a"}, {"id": 3, "name": "b"}]}),
            )
            .await,
        )
        .await;
        assert_eq!(body["rowsCommitted"], 2);
        assert_eq!(body["idempotent"], false);
        assert!(body["snapshotId"].as_str().is_some());
        assert_eq!(body["watermark"], body["snapshotId"]);

        // The live count reflects the append.
        let body =
            json_body(call(router_of(), "GET", "/v1/tables/sdk.events/snapshot").await).await;
        assert_eq!(body["recordCount"], 3);

        // Rows pages with a cursor.
        let body =
            json_body(call(router_of(), "GET", "/v1/tables/sdk.events/rows?limit=2").await).await;
        assert_eq!(body["rows"].as_array().expect("rows array").len(), 2);
        assert!(body.get("nextCursor").is_some(), "more rows remain");
    }

    /// The extracted `/v1` serving router — the same surface the SigV4 S3 data
    /// port serves once the edge re-signs a forward — commits rows and reads
    /// them back over its own catalog slot, independent of the full admin
    /// router. This is the unit the S3 front-end drives via `ServingApi`.
    #[tokio::test]
    #[cfg(any())]
    async fn v1_serving_router_commits_a_table_end_to_end() {
        let slot = tables_slot_with_table().await;
        let router_of = || v1_serving_router(None, None);

        // Commit two rows through the serving router directly.
        let body = json_body(
            call_json(
                router_of(),
                "POST",
                "/v1/tables/sdk.events/commit",
                serde_json::json!({"rows": [{"id": 2, "name": "a"}, {"id": 3, "name": "b"}]}),
            )
            .await,
        )
        .await;
        assert_eq!(body["rowsCommitted"], 2);
        assert!(body["snapshotId"].as_str().is_some());

        // The append is visible through the same router's snapshot route, so the
        // tables and query sub-routers share one live catalog slot.
        let body =
            json_body(call(router_of(), "GET", "/v1/tables/sdk.events/snapshot").await).await;
        assert_eq!(body["recordCount"], 3);
    }

    /// The former table endpoint still decodes Arrow while callers transition
    /// to the isolated write role.
    #[tokio::test]
    #[cfg(any())]
    async fn rust_sdk_table_contract_streams_arrow_end_to_end() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let slot = tables_slot_with_table().await;
        let router_of = || {
            router(
                VERSION,
                Health::ready(),
                Slots {
                    tables: Some(slot.clone()),
                    ..Slots::default()
                },
            )
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["arrow"])),
            ],
        )
        .expect("append batch");
        let mut append_bytes = Vec::new();
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut append_bytes, &schema)
                .expect("append writer");
            writer.write(&batch).expect("append batch IPC");
            writer.finish().expect("finish append IPC");
        }
        let response = router_of()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/tables/sdk.events/commit")
                    .header("content-type", verglas_sdk::ARROW_STREAM_CONTENT_TYPE)
                    .header("idempotency-key", "arrow-1")
                    .body(Body::from(append_bytes))
                    .expect("append request"),
            )
            .await
            .expect("append response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json_body(response).await["rowsCommitted"], 1);
    }

    #[tokio::test]
    #[cfg(any())]
    async fn ensure_table_is_one_idempotent_post_without_a_definition_route() {
        let slot = tables_slot_with_table().await;
        let router_of = || {
            router(
                VERSION,
                Health::ready(),
                Slots {
                    tables: Some(slot.clone()),
                    ..Slots::default()
                },
            )
        };
        let exact = serde_json::json!({
            "schema": [
                {"name":"id", "type":"int64", "nullable":true},
                {"name":"name", "type":"utf8", "nullable":true}
            ],
            "partitions": []
        });
        let response = call_json(router_of(), "POST", "/v1/tables/sdk.events", exact).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!json_body(response).await["created"].as_bool().unwrap());

        let mismatch = serde_json::json!({
            "schema": [{"name":"id", "type":"utf8", "nullable":false}],
            "partitions": []
        });
        let response = call_json(router_of(), "POST", "/v1/tables/sdk.events", mismatch).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let response = call(router_of(), "GET", "/v1/tables/sdk.events/definition").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// A repeated idempotency key over HTTP replays the original result and the
    /// live count does not grow.
    #[tokio::test]
    #[cfg(any())]
    async fn commit_idempotency_key_replays_over_http() {
        let slot = tables_slot_with_table().await;
        let router_of = || {
            router(
                VERSION,
                Health::ready(),
                Slots {
                    tables: Some(slot.clone()),
                    ..Slots::default()
                },
            )
        };
        let request =
            serde_json::json!({"rows": [{"id": 9, "name": "once"}], "idempotencyKey": "k1"});

        let first = json_body(
            call_json(
                router_of(),
                "POST",
                "/v1/tables/sdk.events/commit",
                request.clone(),
            )
            .await,
        )
        .await;
        assert_eq!(first["idempotent"], false);

        let second = json_body(
            call_json(router_of(), "POST", "/v1/tables/sdk.events/commit", request).await,
        )
        .await;
        assert_eq!(second["idempotent"], true, "second commit is a replay");
        assert_eq!(second["snapshotId"], first["snapshotId"]);

        let snap =
            json_body(call(router_of(), "GET", "/v1/tables/sdk.events/snapshot").await).await;
        assert_eq!(snap["recordCount"], 2, "seed + one, not two");
    }

    /// A commit carrying a column the table does not have is a 400.
    #[tokio::test]
    #[cfg(any())]
    async fn commit_with_an_unknown_column_is_a_400() {
        let slot = tables_slot_with_table().await;
        let app = router(
            VERSION,
            Health::ready(),
            Slots {
                tables: Some(slot),
                ..Slots::default()
            },
        );
        let response = call_json(
            app,
            "POST",
            "/v1/tables/sdk.events/commit",
            serde_json::json!({"rows": [{"id": 1, "name": "x", "extra": "nope"}]}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// A ready router with only the given slots filled.
    fn slots_router(slots: Slots) -> Router {
        router(VERSION, Health::ready(), slots)
    }

    /// A catalog handle never enables embedded SQL execution.
    #[tokio::test]
    async fn query_route_requires_an_isolated_worker() {
        let slot = tables_slot_with_table().await;
        let app = slots_router(Slots {
            tables: Some(slot),
            ..Slots::default()
        });
        let response = call_json(
            app,
            "POST",
            "/v1/query",
            serde_json::json!({"sql": "SELECT id, name FROM sdk.events"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Proprietary table metadata routes stay absent; clients use the mounted
    /// standard Iceberg REST catalog instead.
    #[tokio::test]
    async fn catalog_table_proxy_is_not_mounted() {
        let app = slots_router(Slots {
            tables: Some(tables_slot_with_table().await),
            ..Slots::default()
        });
        assert_eq!(
            call(app.clone(), "GET", "/v1/tables").await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            call(app, "POST", "/v1/tables/sdk.events/commit")
                .await
                .status(),
            StatusCode::NOT_FOUND
        );
    }

    /// `POST /v1/query` answers 503 until the private catalog is wired — the
    /// same pattern as the tables routes.
    #[tokio::test]
    async fn query_route_answers_503_until_the_catalog_is_wired() {
        let empty: TablesSlot = Arc::new(OnceLock::new());
        let app = slots_router(Slots {
            tables: Some(empty),
            ..Slots::default()
        });
        let response = call_json(
            app,
            "POST",
            "/v1/query",
            serde_json::json!({"sql": "SELECT 1"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Sends a request with a raw (non-JSON) body and returns the response.
    /// The table inspect routes (#323) serve the CLI's list/show/history verbs:
    /// list with an optional namespace filter, describe with schema and
    /// counters, and the snapshot history — the same report shapes the embedded
    /// engine produced.
    #[tokio::test]
    #[cfg(any())]
    async fn table_inspect_routes_serve_list_show_and_history() {
        let slot = tables_slot_with_table().await;
        let router_of = || {
            slots_router(Slots {
                tables: Some(slot.clone()),
                ..Slots::default()
            })
        };

        let body = json_body(call(router_of(), "GET", "/v1/tables").await).await;
        assert_eq!(body["tables"][0]["namespace"], "sdk");
        assert_eq!(body["tables"][0]["name"], "events");

        // A namespace that does not exist is a 404, matching the engine's
        // NamespaceNotFound — the same failure the embedded verb surfaced.
        let response = call(router_of(), "GET", "/v1/tables?namespace=absent").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body =
            json_body(call(router_of(), "GET", "/v1/tables/sdk.events/describe").await).await;
        assert_eq!(body["table"], "sdk.events");
        assert_eq!(body["row_count"], 1);
        let columns: Vec<&str> = body["schema"]
            .as_array()
            .expect("schema")
            .iter()
            .map(|f| f["name"].as_str().expect("name"))
            .collect();
        assert_eq!(columns, vec!["id", "name"]);

        let body = json_body(call(router_of(), "GET", "/v1/tables/sdk.events/history").await).await;
        assert_eq!(body["table"], "sdk.events");
        assert_eq!(
            body["snapshots"].as_array().expect("snapshots").len(),
            1,
            "one create commit"
        );
    }

    /// `POST /v1/tables/{name}/ingest` (#323) is the CLI's create/append from a
    /// source file, moved server-side: the server infers the schema, writes the
    /// data files, and commits — the CLI only streams bytes. `mode=create`
    /// creates (optionally partitioned); `mode=append` appends schema-checked.
    #[tokio::test]
    #[cfg(any())]
    async fn table_ingest_route_creates_and_appends_from_file_bytes() {
        let slot = tables_slot_with_table().await;
        let router_of = || {
            slots_router(Slots {
                tables: Some(slot.clone()),
                ..Slots::default()
            })
        };

        // Create a fresh table from CSV bytes.
        let body = json_body(
            call_raw(
                router_of(),
                "POST",
                "/v1/tables/sdk.fresh/ingest?mode=create&format=csv",
                b"id,label\n1,a\n2,b\n".to_vec(),
            )
            .await,
        )
        .await;
        assert_eq!(body["operation"], "create");
        assert_eq!(body["records_added"], 2);

        // Append more rows to it.
        let body = json_body(
            call_raw(
                router_of(),
                "POST",
                "/v1/tables/sdk.fresh/ingest?mode=append&format=csv",
                b"id,label\n3,c\n".to_vec(),
            )
            .await,
        )
        .await;
        assert_eq!(body["operation"], "append");
        assert_eq!(body["records_added"], 1);

        // A mismatched column set on append is a 400 naming the column, and the
        // table is unchanged.
        let response = call_raw(
            router_of(),
            "POST",
            "/v1/tables/sdk.fresh/ingest?mode=append&format=csv",
            b"id,wrong\n4,d\n".to_vec(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // An unknown mode or format is a 400.
        let response = call_raw(
            router_of(),
            "POST",
            "/v1/tables/sdk.other/ingest?mode=upsert&format=csv",
            b"id\n1\n".to_vec(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let response = call_raw(
            router_of(),
            "POST",
            "/v1/tables/sdk.other/ingest?mode=create&format=xml",
            b"<no/>".to_vec(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// A partitioned ingest create records the partition column in the report
    /// and the describe route reflects it.
    #[tokio::test]
    #[cfg(any())]
    async fn table_ingest_create_supports_identity_partitioning() {
        let slot = tables_slot_with_table().await;
        let router_of = || {
            slots_router(Slots {
                tables: Some(slot.clone()),
                ..Slots::default()
            })
        };
        let body = json_body(
            call_raw(
                router_of(),
                "POST",
                "/v1/tables/sdk.parts/ingest?mode=create&format=csv&partition_by=day",
                b"day,v\n2026-07-24,1\n".to_vec(),
            )
            .await,
        )
        .await;
        assert_eq!(body["partition_by"], serde_json::json!(["day"]));
        let body = json_body(call(router_of(), "GET", "/v1/tables/sdk.parts/describe").await).await;
        assert_eq!(body["partition_by"], serde_json::json!(["day"]));
    }

    // ----- graph verb-family routes (`/v1/graphs/...`) -----

    /// A filled graphs slot over a fresh empty memory catalog — the post-recovery
    /// state the server leaves the slot in. The same handle type as the tables
    /// slot, so `/v1/tables` and `/v1/graphs` see one catalog.
    async fn graphs_slot_ready() -> GraphsSlot {
        use iceberg::CatalogBuilder;
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};

        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                std::collections::HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.path().to_str().expect("utf8").to_string(),
                )]),
            )
            .await
            .expect("memory catalog");
        std::mem::forget(warehouse);
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);
        let slot: GraphsSlot = Arc::new(OnceLock::new());
        let _ = slot.set(catalog);
        slot
    }

    /// A graph route on a router whose slot is not yet filled answers 503, like
    /// the other engine-dependent routes before recovery.
    #[tokio::test]
    async fn graph_routes_answer_503_until_the_catalog_is_wired() {
        let empty: GraphsSlot = Arc::new(OnceLock::new());
        let app = slots_router(Slots {
            graphs: Some(empty),
            ..Slots::default()
        });
        let response = call(app, "POST", "/v1/graphs/kg").await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Creating a graph ensures its two backing tables, which then show up as
    /// plain tables in `/v1/tables` — the "stored separately as plain tables"
    /// contract.
    #[tokio::test]
    #[cfg(any())]
    async fn create_graph_makes_two_plain_tables() {
        let slot = graphs_slot_ready().await;
        let tables: TablesSlot = slot.clone();
        let router_of = || {
            slots_router(Slots {
                graphs: Some(slot.clone()),
                tables: Some(tables.clone()),
                ..Slots::default()
            })
        };

        let body = json_body(call(router_of(), "POST", "/v1/graphs/kg").await).await;
        assert_eq!(body["namespace"], "kg");
        assert_eq!(body["nodesTable"], "kg.nodes");
        assert_eq!(body["edgesTable"], "kg.edges");

        // Both backing tables are listed by the plain table-list route.
        let list = json_body(call(router_of(), "GET", "/v1/tables?namespace=kg").await).await;
        let names: Vec<String> = list["tables"]
            .as_array()
            .expect("tables array")
            .iter()
            .map(|t| t["name"].as_str().expect("name").to_owned())
            .collect();
        assert!(names.contains(&"nodes".to_owned()), "nodes table listed");
        assert!(names.contains(&"edges".to_owned()), "edges table listed");
    }

    /// Seeds a small graph (a→b, b→c, c→d as `knows`; a→c as `met`) through the
    /// insert routes and returns the ready graphs slot. The router built from it
    /// serves every graph route over this one catalog.
    async fn seeded_graph_router() -> GraphsSlot {
        let slot = graphs_slot_ready().await;
        let router_of = || {
            slots_router(Slots {
                graphs: Some(slot.clone()),
                ..Slots::default()
            })
        };
        call(router_of(), "POST", "/v1/graphs/kg").await;
        call_json(
            router_of(),
            "POST",
            "/v1/graphs/kg/nodes",
            json!({"nodes": [{"id":"a"},{"id":"b"},{"id":"c"},{"id":"d"}]}),
        )
        .await;
        call_json(
            router_of(),
            "POST",
            "/v1/graphs/kg/edges",
            json!({"edges": [
                {"srcId":"a","predicate":"knows","dstId":"b","provenance":"m1"},
                {"srcId":"b","predicate":"knows","dstId":"c","provenance":"m2"},
                {"srcId":"c","predicate":"knows","dstId":"d","provenance":"m3"},
                {"srcId":"a","predicate":"met","dstId":"c","confidence":0.5,"provenance":"m4"}
            ]}),
        )
        .await;
        slot
    }

    /// The insert routes return the new snapshot id and the row count.
    #[tokio::test]
    async fn insert_nodes_and_edges_return_the_snapshot_and_count() {
        let slot = graphs_slot_ready().await;
        let router_of = || {
            slots_router(Slots {
                graphs: Some(slot.clone()),
                ..Slots::default()
            })
        };
        call(router_of(), "POST", "/v1/graphs/kg").await;

        let body = json_body(
            call_json(
                router_of(),
                "POST",
                "/v1/graphs/kg/nodes",
                json!({"nodes": [{"id":"a"},{"id":"b"}]}),
            )
            .await,
        )
        .await;
        assert_eq!(body["count"], 2);
        assert!(
            body["snapshotId"].as_i64().is_some(),
            "camelCase snapshotId"
        );

        let body = json_body(
            call_json(
                router_of(),
                "POST",
                "/v1/graphs/kg/edges",
                json!({"edges": [{"srcId":"a","predicate":"knows","dstId":"b","provenance":"m1"}]}),
            )
            .await,
        )
        .await;
        assert_eq!(body["count"], 1);
        assert!(body["snapshotId"].as_i64().is_some());
    }

    /// The `neighbors`, `kHop`, and `paths` queries return the expected results
    /// once the index is built. Reads the small seeded graph.
    #[tokio::test]
    async fn traversal_queries_return_expected_results() {
        let slot = seeded_graph_router().await;
        let router_of = || {
            slots_router(Slots {
                graphs: Some(slot.clone()),
                ..Slots::default()
            })
        };
        // Build the index so the query is served by it.
        let index = json_body(call(router_of(), "POST", "/v1/graphs/kg/index").await).await;
        assert_eq!(index["built"], true);
        assert_eq!(index["edgeCount"], 4);
        assert_eq!(index["mode"], "full");

        // neighbors(a, out) = {b via knows, c via met}.
        let body = json_body(
            call_json(
                router_of(),
                "POST",
                "/v1/graphs/kg/query",
                json!({"op":"neighbors","start":"a","direction":"out"}),
            )
            .await,
        )
        .await;
        assert_eq!(body["backend"], "index");
        let mut neighbors: Vec<String> = body["neighbors"]
            .as_array()
            .expect("neighbors array")
            .iter()
            .map(|n| n["nodeId"].as_str().expect("nodeId").to_owned())
            .collect();
        neighbors.sort();
        assert_eq!(neighbors, vec!["b".to_owned(), "c".to_owned()]);

        // kHop(a, 2, out) reaches b, c, d.
        let body = json_body(
            call_json(
                router_of(),
                "POST",
                "/v1/graphs/kg/query",
                json!({"op":"kHop","start":"a","k":2,"direction":"out"}),
            )
            .await,
        )
        .await;
        let mut reached: Vec<String> = body["reached"]
            .as_array()
            .expect("reached array")
            .iter()
            .map(|r| r["nodeId"].as_str().expect("nodeId").to_owned())
            .collect();
        reached.sort();
        assert_eq!(
            reached,
            vec!["b".to_owned(), "c".to_owned(), "d".to_owned()]
        );

        // paths(a -> d, maxHops 3) = a, c, d (the two-hop shortest path).
        let body = json_body(
            call_json(
                router_of(),
                "POST",
                "/v1/graphs/kg/query",
                json!({"op":"paths","start":"a","dst":"d","maxHops":3,"direction":"out"}),
            )
            .await,
        )
        .await;
        let paths = body["paths"].as_array().expect("paths array");
        assert_eq!(paths.len(), 1);
        let nodes: Vec<String> = paths[0]["nodes"]
            .as_array()
            .expect("nodes array")
            .iter()
            .map(|n| n.as_str().expect("node").to_owned())
            .collect();
        assert_eq!(nodes, vec!["a".to_owned(), "c".to_owned(), "d".to_owned()]);
    }

    /// The turn-off contract: a query returns identical results with and without
    /// a bound index. Before `POST .../index` the query is served by a scan;
    /// after, by the index; the reached set is the same.
    #[tokio::test]
    async fn queries_are_identical_with_and_without_the_index() {
        let slot = seeded_graph_router().await;
        let router_of = || {
            slots_router(Slots {
                graphs: Some(slot.clone()),
                ..Slots::default()
            })
        };
        let khop = |router: Router| async move {
            json_body(
                call_json(
                    router,
                    "POST",
                    "/v1/graphs/kg/query",
                    json!({"op":"kHop","start":"a","k":3,"direction":"out"}),
                )
                .await,
            )
            .await
        };

        // No index yet: scan backend.
        let before = khop(router_of()).await;
        assert_eq!(before["backend"], "scan");

        // Build the index, then the same query is served by it.
        call(router_of(), "POST", "/v1/graphs/kg/index").await;
        let after = khop(router_of()).await;
        assert_eq!(after["backend"], "index");

        // The reached results are identical.
        assert_eq!(before["reached"], after["reached"]);
    }

    /// A `kHop` query without `k`, and a `paths` query without `dst`/`maxHops`,
    /// are 400s — the request is missing the bound its op requires.
    #[tokio::test]
    async fn query_missing_required_bound_is_a_400() {
        let slot = seeded_graph_router().await;
        let router_of = || {
            slots_router(Slots {
                graphs: Some(slot.clone()),
                ..Slots::default()
            })
        };
        let response = call_json(
            router_of(),
            "POST",
            "/v1/graphs/kg/query",
            json!({"op":"kHop","start":"a"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = call_json(
            router_of(),
            "POST",
            "/v1/graphs/kg/query",
            json!({"op":"paths","start":"a"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// `GET /v1/graphs/{ns}` reports the backing tables, the live counts, and
    /// whether an index is bound to the current edge snapshot.
    #[tokio::test]
    async fn show_reports_counts_and_index_state() {
        let slot = seeded_graph_router().await;
        let router_of = || {
            slots_router(Slots {
                graphs: Some(slot.clone()),
                ..Slots::default()
            })
        };
        // Before an index build: not indexed, counts reflect the seed.
        let body = json_body(call(router_of(), "GET", "/v1/graphs/kg").await).await;
        assert_eq!(body["namespace"], "kg");
        assert_eq!(body["nodesTable"], "kg.nodes");
        assert_eq!(body["edgesTable"], "kg.edges");
        assert_eq!(body["nodeCount"], 4);
        assert_eq!(body["edgeCount"], 4);
        assert_eq!(body["indexed"], false);

        // After an index build: indexed is true.
        call(router_of(), "POST", "/v1/graphs/kg/index").await;
        let body = json_body(call(router_of(), "GET", "/v1/graphs/kg").await).await;
        assert_eq!(body["indexed"], true);
    }
}
