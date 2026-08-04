//! Admin HTTP API served by `verglasd` on a separate port from the S3 endpoint.
//!
//! This is the private control surface — bound to loopback, never the S3
//! data-plane port. Beyond the version and health probes it carries the
//! `POST /cache/purge` operation (issue #138), which is wired only when a
//! cache engine exists (i.e. a full daemon, not the config-less smoke mode).
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

use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{
    Json, Router,
    routing::{get, post, put},
};
use std::future::Future;
use std::io::{self, Cursor, Write};
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
use verglas_vector::service::{SearchOptions, SearchSource, VectorService};
use verglas_vector::store::IndexKey;
use verglas_vector::{Metric, VamanaParams};

use verglas_cache::CachePurger;
use verglas_core::admin::{
    ACCESS_PATH, DRAIN_PATH, DrainAck, DrainRequest, HEALTHZ_PATH, HealthzInfo, LOG_PATH,
    LocalAccess, LogLevelInfo, LogLevelRequest, MEMBERS_PATH, METRICS_PATH, MembersInfo,
    PURGE_PATH, STATS_PATH, StatsInfo, TABLE_METRICS_PATH, VERSION_PATH, VersionInfo,
};
use verglas_core::metrics::EXPOSITION_CONTENT_TYPE;
use verglas_tables::catalog::CatalogGateway;

/// Readiness gate for serve-gating (#16). Starts reporting `starting` and flips
/// to `ok` once the cache engine's disk recovery completes, so a load balancer
/// polling `/admin/healthz` never routes to a node that would cold-miss
/// everything. Cheap to share: one atomic behind an `Arc`.
#[derive(Clone)]
pub struct Health(Arc<AtomicBool>);

impl Health {
    /// A gate that reports `starting` until [`Health::mark_ready`] is called —
    /// the full daemon uses this while its engine recovers.
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
/// The daemon supplies one closure over its engine handle and config; the admin
/// surface owns nothing of the engine's type. `None` when the daemon booted
/// without a config (the config-less admin-only mode used by smoke flows), so
/// there is no engine to report on.
pub type StatsSource = Arc<dyn Fn() -> StatsInfo + Send + Sync>;

/// Produces the Prometheus text exposition for `GET /metrics` (issue #46): the
/// live request families encoded from the shared registry, plus the node-level
/// counters and gauges read from the running engine and backend at scrape time.
/// The daemon supplies one closure over its metrics handle, engine, and backend;
/// the admin surface owns none of those types. `None` in the config-less
/// admin-only mode (no engine to report on).
pub type MetricsSource = Arc<dyn Fn() -> String + Send + Sync>;

/// Produces the per-table telemetry report for `GET /v1/metering/tables` (#60):
/// one row per table with recorded traffic (hit rate, cached/served bytes,
/// requests avoided, latency saved) plus fleet totals, read from the rollup at
/// call time. The daemon supplies one closure over its telemetry hub and mapper;
/// the admin surface owns neither type. `None` (route absent) when there is no
/// mapper to attribute reads (prefetch off / config-less mode).
pub type TablesReportSource = Arc<dyn Fn() -> verglas_core::telemetry::TablesReport + Send + Sync>;

/// A per-table telemetry source wired lazily once recovery completes.
pub type TablesReportSlot = Arc<OnceLock<TablesReportSource>>;

/// Produces a live [`MembersInfo`] snapshot (this node's gossip membership view
/// and ring epoch, issue #27). The daemon supplies one closure over its cluster
/// agent; the admin surface owns nothing of the gossip layer's types. `None`
/// when the daemon runs single-node (no `[cluster]`), so the route is absent.
pub type MembersSource = Arc<dyn Fn() -> MembersInfo + Send + Sync>;

/// A drain source wired lazily once recovery completes; see [`PurgerSlot`].
pub type DrainSlot = Arc<OnceLock<DrainHandler>>;

/// Initiates a graceful drain (issue #31): gossips this node `draining` and
/// schedules its exit, returning the ack. The daemon supplies one async closure
/// over its cluster agent; the admin surface owns nothing of the gossip layer.
/// `None` when the daemon runs single-node (no `[cluster]`), so the route is
/// absent — a cluster of one has nowhere to drain to.
pub type DrainHandler =
    Arc<dyn Fn(DrainRequest) -> Pin<Box<dyn Future<Output = DrainAck> + Send>> + Send + Sync>;

/// The catalog handle the SDK table routes commit and read through, wired
/// lazily after recovery like the other engine slots. It is an `Arc<dyn Catalog>`
/// over the daemon's own loopback gateway; the routes answer 503 until it is
/// filled.
pub type TablesSlot = Arc<OnceLock<Arc<dyn Catalog>>>;

/// The catalog handle the `graph` verb-family routes (`/v1/graphs/...`) drive
/// the graph-over-Iceberg engine through. A graph is not a new storage
/// primitive — it is a namespace holding two plain Iceberg tables plus the
/// Puffin adjacency index — so it shares the same loopback catalog handle as
/// [`TablesSlot`], filled at the same moment after recovery, and answers 503
/// until then.
pub type GraphsSlot = Arc<OnceLock<Arc<dyn Catalog>>>;

/// The runtime backing the vector-index routes (`/v1/tables|graphs/{..}/
/// indexes`): the loopback catalog handle (to read source deltas/rows) plus the
/// index service (shadow store + registry + serving cache). A vector index is a
/// cluster-local derived artifact — the blob lives in the shadow store, never
/// the source table (#95, diverging from #91) — so this is a Verglas-owned
/// service, not a per-table catalog handle.
pub struct VectorRuntime {
    /// The loopback catalog the maintenance MV and brute-force fallback read
    /// source rows through.
    pub catalog: Arc<dyn Catalog>,
    /// The index service (declare/refresh/search) over the shadow store. Its
    /// in-memory registry is a projection of `sys`'s `verglas_sys.indexes` rows.
    pub service: Arc<VectorService>,
    /// The durable index registry (`verglas_sys.indexes`): the source of truth a
    /// declaration writes to and a reboot rehydrates from. Shared with the
    /// worker registry routes through [`SysSlot`].
    pub sys: Arc<verglas_platform::SystemCatalog>,
    /// The cluster id this daemon stamps on the index rows it writes and filters
    /// by on reboot (resolved once at start; hostname by default).
    pub cluster_id: String,
}

/// The deferred handle the vector-index routes drive, wired after recovery like
/// [`TablesSlot`]; the routes answer 503 until it is filled.
pub type VectorSlot = Arc<OnceLock<Arc<VectorRuntime>>>;

/// The `verglas_sys` registry handle the registry and watermark routes write
/// and read through (#322), wired lazily after recovery like [`TablesSlot`].
/// Registry writes go through the daemon's engine handle only — this slot is
/// the single local write authority for `verglas_sys`.
pub type SysSlot = Arc<OnceLock<Arc<verglas_platform::SystemCatalog>>>;

/// The queue root directory the queue routes append to and read from
/// (`/v1/queues/<name>/{enqueue,poll,ack}`, #328). Unlike the engine slots this
/// is a plain path, not a deferred [`OnceLock`]: a segment log needs no cache
/// recovery, so the routes answer as soon as the daemon owns a writable state
/// dir. The TS SDK queue verb targets these routes directly.
pub type QueueDir = Arc<std::path::PathBuf>;

/// The worker runtime handle the manual-run and webhook routes invoke, wired
/// lazily after recovery like [`TablesSlot`] — it runs workers over the loopback
/// catalog, so it exists only once recovery has opened it.
pub type PlatformSlot = Arc<OnceLock<Arc<crate::platform::WorkerSupervisor>>>;

/// The engine-backed surfaces the admin router serves, each optional (absent =
/// the route is not mounted) and most deferred behind a [`OnceLock`] slot
/// (mounted but 503 until recovery fills it). One struct instead of a
/// positional list so call sites name what they wire.
#[derive(Default)]
pub struct Slots {
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
    /// The loopback Iceberg REST gateway (`/catalog/...`).
    pub catalog: Option<CatalogGateway>,
    /// The local-access snapshot (`GET /admin/access`).
    pub access: Option<LocalAccess>,
    /// The SDK table routes and `/v1/query` (`/v1/tables/...`, `POST /v1/query`).
    pub tables: Option<TablesSlot>,
    /// The `graph` verb-family routes (`/v1/graphs/...`), backed by
    /// `verglas-graph` over the same loopback catalog as the table routes.
    pub graphs: Option<GraphsSlot>,
    /// The vector-index routes (`/v1/tables|graphs/{..}/indexes...`), backed by
    /// `verglas-vector` over the cluster-local shadow store.
    pub vector: Option<VectorSlot>,
    /// The `verglas_sys` registry and watermark routes (`/v1/workers`,
    /// `/v1/watermark`).
    pub sys: Option<SysSlot>,
    /// The platform queue routes (`/v1/queues/<name>/{enqueue,poll,ack}`),
    /// backed by [`verglas_harness::queue`] under this queue root.
    pub queues: Option<QueueDir>,
    /// The worker manual-run (`POST /v1/workers/<name>/run`) and webhook
    /// (`POST /v1/hooks/<name>`) routes, invoking the worker runtime.
    pub platform: Option<PlatformSlot>,
    /// The standalone query worker dispatcher (`[query_worker]` configured).
    /// When present it is the sole engine for `/v1/query`; dispatch failure is
    /// a hard error, not an embedded-engine fallback. Absent (the default)
    /// keeps `/v1/query` on the embedded engine.
    pub query_worker: Option<Arc<crate::query_worker::QueryWorkerDispatcher>>,
}

/// Builds the admin router with the version and health probes, plus the cache
/// purge endpoint (issue #138), the cache stats probe (issue #141), and the
/// gossip membership probe (issue #27) — each present only when the daemon has
/// the corresponding subsystem (its slot is `Some`). The config-less smoke/CLI
/// path passes `None` for all and gets probes only.
///
/// `health` drives `/admin/healthz`: `starting` (503) until [`Health::mark_ready`]
/// flips it, then `ok` (200). The engine-dependent routes take deferred
/// [`OnceLock`] slots inside `slots` so the router can be built and served
/// before the engine finishes recovering; they answer 503 until their slot is
/// filled (#16).
pub fn router(daemon_version: &'static str, health: Health, slots: Slots) -> Router {
    let Slots {
        purger,
        stats,
        metrics,
        table_metrics,
        members,
        drain,
        catalog,
        access,
        tables,
        graphs,
        vector,
        sys,
        queues,
        platform,
        query_worker,
    } = slots;
    let mut app = Router::new()
        .route(
            VERSION_PATH,
            get({
                let version = daemon_version;
                move || async move { Json(VersionInfo::for_daemon(version)) }
            }),
        )
        // Runtime log-level control (#61). Not slot-gated: the reload handle is
        // process-global, so this answers whether or not the engine is ready.
        .route(LOG_PATH, post(set_log_level))
        .merge(health_router(health));
    if let Some(access) = access {
        // Local-access probe (#287): a fixed snapshot of this daemon's
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
                    // the daemon's configured drain timeout.
                    let request = body.map(|Json(r)| r).unwrap_or_default();
                    Json(handler(request).await).into_response()
                }
            }),
        );
    }
    if let Some(tables) = tables {
        app = app.merge(compact_router(tables.clone()));
        app = app.merge(v1_serving_router(tables, query_worker));
    }
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
        app = app.merge(catalog_router(catalog));
    }
    app
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

/// The loopback Iceberg REST surface. The catch-all preserves the catalog
/// request target after the private `/catalog` mount point.
fn catalog_router(catalog: CatalogGateway) -> Router {
    Router::new()
        .route("/catalog/{*path}", axum::routing::any(catalog_request))
        .with_state(catalog)
}

/// Serves or forwards one Iceberg REST request through the shared catalog
/// gateway. Upstream transport failures map to 502; upstream HTTP statuses and
/// bodies pass through unchanged.
///
/// COMMIT BARRIER SEAM (#286). This is where a table commit is forwarded to the
/// catalog, and so where the write-back commit barrier belongs: before
/// forwarding a mutating commit (the `POST .../tables/{table}` updateTable and
/// the create/register mutations that publish metadata), await propagation of
/// the write-back data files the commit references, so the catalog never points
/// at files still buffered locally. The primitive is built and tested:
/// `verglas_writeback::JournalBarrier` over the coordinator's shared journal
/// (both the §6 EC-quorum and the #286 single-node backends record durability
/// there), exposed as `verglas_writeback::CommitBarrier`. Wiring is a small
/// follow-up: the write-back tier is built in the data-plane scope while this
/// router is assembled in the admin scope, so the barrier reaches here through a
/// deferred [`OnceLock`] slot (like [`PurgerSlot`] and the other engine-dependent
/// routes). With the slot filled, a mutating catalog request calls
/// `barrier.await_all_dirty(deadline)` (the conservative, exact-refs-not-parsed
/// form) and returns a clear 5xx on [`verglas_writeback::BarrierError::Timeout`]
/// so the commit fails and the table stays consistent. Left as a documented seam
/// rather than wired now: threading the deferred slot across the two scopes plus
/// its own real-catalog integration test is disproportionate to this PR, whose
/// core is the fast-ack path and the barrier primitive.
async fn catalog_request(
    State(catalog): State<CatalogGateway>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(path_and_query) = uri.path_and_query().map(|value| value.as_str()) else {
        return (StatusCode::BAD_REQUEST, "catalog request has no path").into_response();
    };
    let Some(upstream_path) = path_and_query.strip_prefix("/catalog") else {
        return (
            StatusCode::BAD_REQUEST,
            "catalog request is outside its mount",
        )
            .into_response();
    };
    match catalog.request(method, upstream_path, headers, body).await {
        Ok(result) => {
            let status = StatusCode::from_u16(result.status).unwrap_or(StatusCode::BAD_GATEWAY);
            let mut response = Response::new(axum::body::Body::from(result.body));
            *response.status_mut() = status;
            *response.headers_mut() = result.headers;
            response
        }
        Err(error) => {
            eprintln!("verglasd catalog gateway request failed: {error}");
            (
                StatusCode::BAD_GATEWAY,
                format!("catalog gateway error: {error}"),
            )
                .into_response()
        }
    }
}

/// The commit-body ceiling for the tables routes. Axum's default is 2 MiB — a
/// framework default, not a design choice — which forced bulk ingest into
/// hundreds of tiny commits per day (each one an Iceberg snapshot and a catalog
/// call). 32 MiB is deliberate: bodies are buffered in memory while the batch
/// becomes Parquet, so the ceiling stays bounded, but a day of minute bars now
/// lands in tens of commits instead of hundreds.
const TABLES_BODY_LIMIT_BYTES: usize = 32 * 1024 * 1024;

/// The `/v1` serving API: the tables sub-router and the query sub-router merged
/// into one state-erased [`Router`]. This is the surface the daemon serves both
/// on the loopback admin listener (inside [`router`]) and, re-signed by the
/// edge, on the SigV4-gated S3 data port. The manual-compaction route is admin
/// only and is not part of it. `query_worker` is the optional standalone-worker
/// dispatcher `/v1/query` tries before its embedded fallback.
pub fn v1_serving_router(
    tables: TablesSlot,
    query_worker: Option<Arc<crate::query_worker::QueryWorkerDispatcher>>,
) -> Router {
    tables_router(tables.clone()).merge(query_router(tables, query_worker))
}

/// The SDK table sub-router (`/v1/tables/...`): commit rows and read the current
/// snapshot, a page of rows, or the delta since a watermark. Isolated so its
/// [`TablesSlot`] state does not leak into the other routes' state type. Each
/// route answers 503 until the loopback catalog handle is wired after recovery
/// (#16). `{name}` is the `namespace.table` identifier.
fn tables_router(tables: TablesSlot) -> Router {
    Router::new()
        .route("/v1/tables", get(tables_list))
        .route("/v1/tables/{name}", post(tables_create))
        .route("/v1/tables/{name}/definition", get(tables_definition))
        .route("/v1/tables/{name}/commit", post(tables_commit))
        .route("/v1/tables/{name}/snapshot", get(tables_snapshot))
        .route("/v1/tables/{name}/rows", get(tables_rows))
        .route("/v1/tables/{name}/delta", get(tables_delta))
        .route("/v1/tables/{name}/describe", get(tables_describe))
        .route("/v1/tables/{name}/history", get(tables_history))
        .route("/v1/tables/{name}/ingest", post(tables_ingest))
        .layer(axum::extract::DefaultBodyLimit::max(
            TABLES_BODY_LIMIT_BYTES,
        ))
        .with_state(tables)
}

/// The manual compaction route (`POST /admin/compact`): run one compaction pass
/// over the loopback catalog on demand. The open-source daemon ships compaction
/// as an opt-in mechanism — this is the manual trigger; recurring policy belongs
/// to a container-backed worker. Shares the [`TablesSlot`] loopback catalog and
/// answers 503 until it is wired after recovery.
fn compact_router(tables: TablesSlot) -> Router {
    Router::new()
        .route("/admin/compact", post(compact_now))
        .with_state(tables)
}

/// `POST /admin/compact`: runs a single compaction pass over every table in the
/// loopback catalog and returns the pass report (tables examined, per-table files
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

/// Query parameters for `GET /v1/tables`: an optional dotted namespace filter.
#[derive(Debug, Deserialize)]
struct TablesListQuery {
    /// Restrict the listing to this namespace; absent lists every one.
    namespace: Option<String>,
}

/// `GET /v1/tables?namespace=`: every table, or the ones under one namespace —
/// the CLI's `table list` (#323). Answers 503 until the catalog handle is
/// wired.
async fn tables_list(
    State(tables): State<TablesSlot>,
    Query(query): Query<TablesListQuery>,
) -> Response {
    let Some(catalog) = tables.get() else {
        return recovering();
    };
    match verglas_iceberg::inspect::list(catalog.as_ref(), query.namespace.as_deref()).await {
        Ok(report) => Json(report).into_response(),
        Err(error) => table_error(error),
    }
}

/// `GET /v1/tables/{name}/describe`: schema, partitioning, and current-snapshot
/// counters — the CLI's `table show` (#323). Answers 503 until the catalog
/// handle is wired.
async fn tables_describe(State(tables): State<TablesSlot>, Path(name): Path<String>) -> Response {
    let Some(catalog) = tables.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    match verglas_iceberg::inspect::show(catalog.as_ref(), &ident).await {
        Ok(report) => Json(report).into_response(),
        Err(error) => table_error(error),
    }
}

/// `GET /v1/tables/{name}/history`: the snapshot log — the CLI's
/// `table history` (#323). Answers 503 until the catalog handle is wired.
async fn tables_history(State(tables): State<TablesSlot>, Path(name): Path<String>) -> Response {
    let Some(catalog) = tables.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    match verglas_iceberg::inspect::history(catalog.as_ref(), &ident).await {
        Ok(report) => Json(report).into_response(),
        Err(error) => table_error(error),
    }
}

/// Query parameters for `POST /v1/tables/{name}/ingest`: the operation mode,
/// the source format, and the optional identity-partition column for a create.
#[derive(Debug, Deserialize)]
struct IngestQuery {
    /// `create` (new table, schema inferred) or `append` (schema-checked).
    mode: String,
    /// The source format: `csv`, `jsonl`, or `parquet`.
    format: String,
    /// For `mode=create`: add an identity partition on this column.
    partition_by: Option<String>,
}

/// `POST /v1/tables/{name}/ingest?mode=&format=&partition_by=`: the CLI's
/// `table create`/`table append` moved server-side (#323) — the daemon is the
/// only local write authority, so the CLI streams the source file's bytes and
/// the engine work (schema inference, Parquet writing, the CAS commit) happens
/// here. The body is the raw file content; the format comes from the query
/// parameter because the daemon never sees the client's filename. Answers 503
/// until the catalog handle is wired.
async fn tables_ingest(
    State(tables): State<TablesSlot>,
    Path(name): Path<String>,
    Query(query): Query<IngestQuery>,
    body: Bytes,
) -> Response {
    let Some(catalog) = tables.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    // The engine's ingest reads a path and infers the format from its
    // extension, so the bytes land in a scratch file named for the declared
    // format. The scratch dir is dropped (deleted) when the handler returns.
    let extension = match query.format.as_str() {
        "csv" => "csv",
        "jsonl" => "jsonl",
        "parquet" => "parquet",
        other => {
            return (
                StatusCode::BAD_REQUEST,
                format!("unknown format `{other}`: expected csv, jsonl, or parquet"),
            )
                .into_response();
        }
    };
    let scratch = match tempfile::tempdir() {
        Ok(dir) => dir,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("ingest scratch dir: {e}"),
            )
                .into_response();
        }
    };
    let path = scratch.path().join(format!("ingest.{extension}"));
    if let Err(e) = tokio::fs::write(&path, &body).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("ingest scratch write: {e}"),
        )
            .into_response();
    }
    match query.mode.as_str() {
        "create" => {
            match verglas_iceberg::write::create_table(
                catalog.as_ref(),
                &ident,
                &path,
                query.partition_by.as_deref(),
            )
            .await
            {
                Ok(report) => Json(report).into_response(),
                Err(error) => table_error(error),
            }
        }
        "append" => match verglas_iceberg::write::append(catalog.as_ref(), &ident, &path).await {
            Ok(report) => Json(report).into_response(),
            Err(error) => table_error(error),
        },
        other => (
            StatusCode::BAD_REQUEST,
            format!("unknown mode `{other}`: expected create or append"),
        )
            .into_response(),
    }
}

/// `POST /v1/tables/{name}`: creates a table from an explicit schema and
/// partition spec (arbitrary column types, per-column nullability, identity or
/// month partition transforms). This is the create the SDK uses when schema
/// inference cannot express the columns a caller needs. Answers 503 until the
/// catalog handle is wired.
async fn tables_create(
    State(tables): State<TablesSlot>,
    Path(name): Path<String>,
    Json(request): Json<tables_api::CreateTableRequest>,
) -> Response {
    let Some(catalog) = tables.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    match tables_api::create_table(catalog.as_ref(), &ident, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => table_error(error),
    }
}

/// `GET /v1/tables/{name}/definition`: returns the exact schema and partition
/// transforms used by [`verglas_sdk::Client::ensure_table`].
async fn tables_definition(State(tables): State<TablesSlot>, Path(name): Path<String>) -> Response {
    let Some(catalog) = tables.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    match tables_api::definition(catalog.as_ref(), &ident).await {
        Ok(definition) => Json(definition).into_response(),
        Err(error) => table_error(error),
    }
}

/// Query parameters for `GET /v1/tables/{name}/rows`: an optional page size and
/// an opaque cursor from a previous page.
#[derive(Debug, Deserialize)]
struct RowsQuery {
    /// The maximum number of rows in the page.
    limit: Option<usize>,
    /// The cursor returned by the previous page, if any.
    cursor: Option<String>,
}

/// Query parameters for `GET /v1/tables/{name}/delta`: the watermark to read
/// after and an optional cap on the number of rows.
#[derive(Debug, Deserialize)]
struct DeltaQuery {
    /// The watermark (snapshot id) to return rows committed after.
    since: Option<String>,
    /// The maximum number of rows to return.
    limit: Option<usize>,
}

/// `POST /v1/tables/{name}/commit`: converts the JSON rows to Arrow against the
/// table's schema and appends them through the CAS write path, returning the new
/// snapshot as the watermark. A repeated idempotency key replays the original
/// result without writing. Answers 503 until the catalog handle is wired.
async fn tables_commit(
    State(tables): State<TablesSlot>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(catalog) = tables.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let result = if content_type.starts_with(verglas_sdk::ARROW_STREAM_CONTENT_TYPE) {
        let reader = match arrow_ipc::reader::StreamReader::try_new(Cursor::new(body), None) {
            Ok(reader) => reader,
            Err(error) => {
                return (StatusCode::BAD_REQUEST, error.to_string()).into_response();
            }
        };
        let batches = match reader.collect::<Result<Vec<_>, _>>() {
            Ok(batches) => batches,
            Err(error) => {
                return (StatusCode::BAD_REQUEST, error.to_string()).into_response();
            }
        };
        let idempotency_key = headers
            .get("idempotency-key")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        tables_api::commit_batches(catalog.as_ref(), &ident, batches, idempotency_key).await
    } else {
        let request = match serde_json::from_slice::<tables_api::CommitRequest>(&body) {
            Ok(request) => request,
            Err(error) => {
                return (StatusCode::BAD_REQUEST, error.to_string()).into_response();
            }
        };
        tables_api::commit(catalog.as_ref(), &ident, request).await
    };
    match result {
        Ok(response) => Json(response).into_response(),
        Err(error) => table_error(error),
    }
}

/// `GET /v1/tables/{name}/snapshot`: the current snapshot id, watermark, and
/// live row count. Answers 503 until the catalog handle is wired.
async fn tables_snapshot(State(tables): State<TablesSlot>, Path(name): Path<String>) -> Response {
    let Some(catalog) = tables.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    match tables_api::snapshot(catalog.as_ref(), &ident).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => table_error(error),
    }
}

/// `GET /v1/tables/{name}/rows?limit=&cursor=`: a page of the current snapshot's
/// rows, with the cursor for the next page. Answers 503 until the catalog handle
/// is wired.
async fn tables_rows(
    State(tables): State<TablesSlot>,
    Path(name): Path<String>,
    Query(query): Query<RowsQuery>,
) -> Response {
    let Some(catalog) = tables.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    match tables_api::rows(catalog.as_ref(), &ident, query.limit, query.cursor).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => table_error(error),
    }
}

/// `GET /v1/tables/{name}/delta?since=&limit=`: the rows committed after the
/// `since` watermark, plus the new tip. Answers 503 until the catalog handle is
/// wired.
async fn tables_delta(
    State(tables): State<TablesSlot>,
    Path(name): Path<String>,
    Query(query): Query<DeltaQuery>,
) -> Response {
    let Some(catalog) = tables.get() else {
        return recovering();
    };
    let ident = match parse_table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    match tables_api::delta(catalog.as_ref(), &ident, query.since, query.limit).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => table_error(error),
    }
}

/// The state `/v1/query` reads: the loopback catalog handle for the embedded
/// path (both its Arrow-IPC and buffered-JSON responses), and the optional
/// standalone-worker dispatcher tried before either.
#[derive(Clone)]
struct QueryState {
    tables: TablesSlot,
    query_worker: Option<Arc<crate::query_worker::QueryWorkerDispatcher>>,
}

/// The `/v1/query` sub-router: when a query worker is configured it is the sole
/// engine; otherwise queries run on the embedded engine over the same loopback
/// catalog handle as the tables routes. Answers 503 until the catalog handle is
/// wired (#322).
fn query_router(
    tables: TablesSlot,
    query_worker: Option<Arc<crate::query_worker::QueryWorkerDispatcher>>,
) -> Router {
    Router::new()
        .route("/v1/query", post(query_sql))
        .with_state(QueryState {
            tables,
            query_worker,
        })
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

/// `POST /v1/query`: runs SQL and returns a result. When a standalone query
/// worker is configured, this tries dispatching to it first
/// (`crate::query_worker::QueryWorkerDispatcher` spawns it on demand, sized
/// from this exact query, and kills it after); any dispatch failure — spawn,
/// startup timeout, a bad response — falls back to running the same SQL
/// through the embedded engine below, logged at `warn` so an operator can see
/// dispatch is unhealthy without the request itself failing. (Dispatch always
/// answers the worker's own `{columns, rows, row_count}` JSON shape; a
/// dispatched request does not currently honor an Arrow `Accept` preference —
/// see `query_worker.rs`.)
///
/// The embedded engine itself answers two ways: an Arrow IPC stream
/// (`accepts_arrow`, never collects the result) when the caller's `Accept`
/// header asks for it, otherwise the stable buffered `{columns, rows,
/// row_count}` JSON `QueryReport` every existing client expects. Answers 503
/// until the catalog handle is wired (the embedded fallback needs it, so
/// dispatch alone being configured is not enough to skip this gate). A
/// statement the engine cannot plan or execute is a 400 either way — the SQL
/// is the caller's input.
async fn query_sql(
    State(state): State<QueryState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Response {
    let Some(catalog) = state.tables.get() else {
        return recovering();
    };
    let time_travel = request.at.map(|at| verglas_iceberg::TimeTravel {
        reference: at.reference,
        table: at.table,
    });

    if let Some(dispatcher) = &state.query_worker {
        // When a query worker is configured it is the only engine. Success and
        // real query errors stream through; a failure to reach the worker is a
        // hard error — never an embedded-engine fallback.
        match dispatcher.dispatch(&request.sql, time_travel.clone()).await {
            Ok(response) => return response,
            Err(error) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("query worker unavailable: {error}"),
                )
                    .into_response();
            }
        }
    }

    if accepts_arrow(&headers) {
        return match verglas_iceberg::query::query_stream(
            catalog.clone(),
            &request.sql,
            time_travel,
        )
        .await
        {
            Ok(execution) => arrow_query_response(execution),
            Err(error @ AgentError::Query(_)) => {
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            Err(error) => table_error(error),
        };
    }
    match verglas_iceberg::query::query(catalog.clone(), &request.sql, time_travel).await {
        Ok(report) => Json(report).into_response(),
        Err(error @ AgentError::Query(_)) => {
            (StatusCode::BAD_REQUEST, error.to_string()).into_response()
        }
        Err(error) => table_error(error),
    }
}

/// True when a request selects the Arrow IPC representation.
fn accepts_arrow(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains(verglas_sdk::ARROW_STREAM_CONTENT_TYPE))
}

/// Streams a DataFusion execution as one Arrow IPC response without collecting
/// all record batches in memory.
fn arrow_query_response(execution: verglas_iceberg::query::QueryExecution) -> Response {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let output = ChannelWriter(buffer.clone());
        let schema = execution.batches.schema();
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
        let mut batches = execution.batches;
        while let Some(batch) = futures::StreamExt::next(&mut batches).await {
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
    let body_stream = futures::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    });
    (
        [(header::CONTENT_TYPE, verglas_sdk::ARROW_STREAM_CONTENT_TYPE)],
        Body::from_stream(body_stream),
    )
        .into_response()
}

/// Drains one encoder fragment into the bounded response channel.
async fn send_arrow_fragment(
    sender: &tokio::sync::mpsc::Sender<Result<Bytes, io::Error>>,
    buffer: &std::sync::Mutex<Vec<u8>>,
) -> bool {
    let bytes = {
        let mut buffer = buffer.lock().expect("Arrow response buffer poisoned");
        std::mem::take(&mut *buffer)
    };
    bytes.is_empty() || sender.send(Ok(Bytes::from(bytes))).await.is_ok()
}

/// `Write` adapter that accumulates one Arrow batch fragment before the async
/// task forwards it through the bounded HTTP body channel.
struct ChannelWriter(Arc<std::sync::Mutex<Vec<u8>>>);

impl Write for ChannelWriter {
    /// Appends encoded bytes to the current bounded batch fragment.
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("Arrow response buffer poisoned"))?
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    /// Arrow writes are delivered immediately, so flushing is a no-op.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The `graph` verb-family sub-router (`/v1/graphs/...`): create a graph, insert
/// nodes and edges, build the Puffin adjacency index, run a traversal query, and
/// show a graph's backing tables and index state. A graph is not a new storage
/// primitive — it is a namespace holding two plain Iceberg tables
/// (`<namespace>.nodes`, `<namespace>.edges`) plus the index — so these routes
/// share the same loopback catalog handle as the table routes and answer 503
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
    /// The loopback catalog handle is not wired yet — answer 503.
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

/// Opens the graph engine handle over the loopback catalog for `namespace`.
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
// Declaring an index provisions its maintenance MV (the initial full build)
// and registers it; searching serves from the local index, with a brute-force
// fallback over the column when no index exists (the turn-off path). These are
// NEW paths — they never collide with the memory recall routes.
// ---------------------------------------------------------------------------

/// The vector-index sub-router. Table and graph indexes share the same runtime
/// (shadow store + service); they differ only in how the source ident and blob
/// key are formed. The 32 MiB body limit covers a large query vector.
fn vector_router(vector: VectorSlot) -> Router {
    Router::new()
        .route("/v1/indexes", get(indexes_registry_list))
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
    declare_index(rt, ident, key, verglas_platform::TARGET_KIND_TABLE, request).await
}

/// `GET /v1/tables/{name}/indexes`: list indexes declared on this table.
async fn tables_index_list(State(vector): State<VectorSlot>, Path(name): Path<String>) -> Response {
    let Some(rt) = vector.get() else {
        return recovering();
    };
    list_indexes(rt, &format!("tbl:{name}")).await
}

/// `POST /v1/tables/{name}/indexes/{field}/search`: ANN search over a table
/// field (brute-force fallback when no index exists).
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
/// current snapshot. This is how a declared-but-unbuilt index (its source table
/// was empty at declare) gets built once rows land — the memory index over
/// `agent_memory.memories` is declared at boot and built here after the first
/// consolidation writes memories. Uses the config already in the service
/// registry (so the memory index keeps its uuid id encoding), not the request.
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

/// Shared refresh path: run the maintenance pass, then reflect the build into
/// the durable registry row (so `GET /v1/indexes` shows the new snapshot/blob).
/// The reboot re-serve does not depend on the row's `blob_ref` — the blob is
/// found by key in the shadow store — so a registry-update failure is logged,
/// not fatal.
async fn refresh_index(rt: &VectorRuntime, ident: iceberg::TableIdent, key: IndexKey) -> Response {
    let report = match rt.service.refresh(rt.catalog.as_ref(), &ident, &key).await {
        Ok(report) => report,
        Err(error) => return vector_error(error),
    };
    let metric = if let Some(r) = &report {
        // Reflect the fresh build into the durable row, preserving its identity.
        let name = verglas_platform::index_row_name(&rt.cluster_id, &key.target, &key.field);
        if let Ok(Some(existing)) = rt.sys.get_index(&name).await {
            let spec = verglas_platform::IndexSpec {
                name,
                target_kind: existing.target_kind,
                target: existing.target,
                field: existing.field,
                metric: existing.metric.clone(),
                params: existing.params,
                cluster_id: existing.cluster_id,
                reflected_snapshot: Some(r.reflected_snapshot),
                blob_ref: Some(r.blob_location.clone()),
                created_by: existing.created_by,
            };
            if let Err(error) = rt.sys.register_index(spec).await {
                tracing::warn!("vector: reflecting the memory index refresh failed: {error}");
            }
            Metric::parse(&existing.metric).unwrap_or(Metric::Cosine)
        } else {
            Metric::Cosine
        }
    } else {
        Metric::Cosine
    };
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
    declare_index(rt, ident, key, verglas_platform::TARGET_KIND_GRAPH, request).await
}

/// `GET /v1/graphs/{namespace}/indexes`: list indexes declared on this graph.
async fn graphs_index_list(
    State(vector): State<VectorSlot>,
    Path(namespace): Path<String>,
) -> Response {
    let Some(rt) = vector.get() else {
        return recovering();
    };
    list_indexes(rt, &format!("graph:{namespace}")).await
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
/// Declaring runs the initial build through the index service AND writes the
/// durable `verglas_sys.indexes` row (the source of truth a reboot rehydrates
/// from). The in-memory service registry is the projection; the durable row
/// carries the reflected snapshot and blob reference when the build lands one.
async fn declare_index(
    rt: &VectorRuntime,
    ident: iceberg::TableIdent,
    key: IndexKey,
    target_kind: &str,
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

    // Write the durable registry row: the in-memory build above is the serving
    // projection, this is the record a reboot rehydrates from. A registry write
    // failure is a 500 — the declare is not durable without it.
    let spec = verglas_platform::IndexSpec {
        name: verglas_platform::index_row_name(&rt.cluster_id, &target, &field),
        target_kind: target_kind.to_owned(),
        target: target.clone(),
        field: field.clone(),
        metric: metric.as_str().to_owned(),
        params: index_params_json(&config),
        cluster_id: rt.cluster_id.clone(),
        reflected_snapshot: report.as_ref().map(|r| r.reflected_snapshot),
        blob_ref: report.as_ref().map(|r| r.blob_location.clone()),
        created_by: "daemon".to_owned(),
    };
    if let Err(error) = rt.sys.register_index(spec).await {
        return platform_error(error);
    }
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
        default_metric: Metric::Cosine,
        default_id_field: "id".to_owned(),
    };
    match rt
        .service
        .search(rt.catalog.as_ref(), &ident, &key, &request.vector, &opts)
        .await
    {
        Ok(outcome) => Json(vector_wire::SearchResponse {
            source: match outcome.source {
                SearchSource::Index => "index".to_owned(),
                SearchSource::BruteForce => "bruteForce".to_owned(),
            },
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
async fn list_indexes(rt: &VectorRuntime, target: &str) -> Response {
    match rt.service.list().await {
        Ok(all) => {
            let indexes = all
                .into_iter()
                .filter(|d| d.target == target)
                .map(|d| vector_wire::IndexInfo {
                    target: d.target,
                    field: d.field,
                    metric: d.metric.as_str().to_owned(),
                    reflected_snapshot: d.reflected_snapshot,
                    live_count: d.live_count,
                })
                .collect();
            Json(vector_wire::IndexListResponse { indexes }).into_response()
        }
        Err(error) => vector_error(error),
    }
}

/// `GET /v1/indexes`: the durable index registry across every table, graph, and
/// cluster backing `verglas index list`. Unlike the per-table
/// `GET /v1/tables/{t}/indexes` (the
/// in-memory serving projection), this reads `verglas_sys.indexes` directly, so
/// it shows every declared index with its cluster id, state, and reflected
/// snapshot — including rows served by other clusters.
///
/// Phase 5 hand-off: surfacing this registry in verglas-cloud alongside tables
/// and graphs is a later step, and the Iceberg `verglas_sys.indexes` table is
/// already cloud-readable, so nothing in verglas-cloud is touched here.
async fn indexes_registry_list(State(vector): State<VectorSlot>) -> Response {
    let Some(rt) = vector.get() else {
        return recovering();
    };
    match rt.sys.list_indexes().await {
        Ok(rows) => {
            let mut indexes: Vec<vector_wire::RegistryIndexInfo> = rows
                .into_iter()
                .map(|r| vector_wire::RegistryIndexInfo {
                    name: r.name,
                    target_kind: r.target_kind,
                    target: r.target,
                    field: r.field,
                    metric: r.metric,
                    cluster_id: r.cluster_id,
                    state: r.state.as_str().to_owned(),
                    reflected_snapshot: r.reflected_snapshot,
                })
                .collect();
            indexes.sort_by(|a, b| a.name.cmp(&b.name));
            Json(vector_wire::RegistryIndexListResponse { indexes }).into_response()
        }
        Err(error) => platform_error(error),
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

/// Serializes a maintenance config's Vamana params and id column into the JSON
/// stored in the registry row's `params` column
/// (`{"r":64,"l":100,"alpha":1.2,"idField":"id"}`). The inverse of
/// [`config_from_row`], so a declare and a reboot rebuild the same config.
fn index_params_json(config: &MaintenanceConfig) -> String {
    serde_json::json!({
        "r": config.params.r,
        "l": config.params.l_build,
        "alpha": config.params.alpha,
        "idField": config.id_field,
        "idEncoding": config.id_encoding.as_str(),
    })
    .to_string()
}

/// Rebuilds a maintenance config from a durable registry row — the inverse of
/// [`index_params_json`] plus the row's metric and field. A bad metric or params
/// JSON is an error (the row is skipped on rehydration, logged).
fn config_from_row(row: &verglas_platform::IndexRow) -> Result<MaintenanceConfig, String> {
    let metric = Metric::parse(&row.metric)
        .ok_or_else(|| format!("index '{}' has unknown metric '{}'", row.name, row.metric))?;
    let parsed: serde_json::Value = serde_json::from_str(&row.params)
        .map_err(|e| format!("index '{}' has unreadable params: {e}", row.name))?;
    let id_field = parsed
        .get("idField")
        .and_then(|v| v.as_str())
        .unwrap_or("id")
        .to_owned();
    let id_encoding = verglas_vector::IdEncoding::parse(
        parsed
            .get("idEncoding")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
    );
    let mut params = VamanaParams::default();
    if let Some(r) = parsed.get("r").and_then(|v| v.as_u64()) {
        params.r = r as usize;
    }
    if let Some(l) = parsed.get("l").and_then(|v| v.as_u64()) {
        params.l_build = l as usize;
    }
    if let Some(alpha) = parsed.get("alpha").and_then(|v| v.as_f64()) {
        params.alpha = alpha as f32;
    }
    let mut config = MaintenanceConfig::new(id_field, row.field.clone(), metric);
    config.params = params;
    config.id_encoding = id_encoding;
    Ok(config)
}

/// The source table a registry row's index is built over: `tbl:ns.table` reads
/// `ns.table`; `graph:ns` reads the graph's `ns.nodes` node table.
fn ident_for_row(row: &verglas_platform::IndexRow) -> Result<iceberg::TableIdent, String> {
    let dotted = if let Some(rest) = row.target.strip_prefix("tbl:") {
        rest.to_owned()
    } else if let Some(ns) = row.target.strip_prefix("graph:") {
        format!("{ns}.nodes")
    } else {
        return Err(format!(
            "index '{}' has unknown target '{}'",
            row.name, row.target
        ));
    };
    parse_table_ident(&dotted).map_err(|e| format!("index '{}' target: {e:?}", row.name))
}

/// The [`IndexKey`] a registry row addresses in the shadow store: the row's
/// logical `target` (already `tbl:`/`graph:`-prefixed) plus its field.
fn key_for_row(row: &verglas_platform::IndexRow) -> IndexKey {
    IndexKey {
        target: row.target.clone(),
        field: row.field.clone(),
    }
}

/// Rehydrates this daemon's declared indexes from the durable registry on boot.
///
/// Reads the `Running` `verglas_sys.indexes` rows for this daemon's cluster id
/// and, per row, restores the maintenance config and either loads the present
/// shadow-store blob (serve immediately) or rebuilds a missing one via the
/// maintenance MV — recording the rebuild's snapshot/blob back to the registry.
/// Rows for OTHER cluster ids are left alone: they stay visible in the durable
/// registry (and in `GET /v1/indexes`) but are not served locally. A search on
/// this daemon for a foreign-cluster index takes the existing brute-force
/// fallback (the turn-off path), not a 404 — the smallest surface, and the
/// honest answer for a serving locality that holds no local blob.
///
/// A row this daemon cannot map (bad metric, params, or target) is logged and
/// skipped, never fatal — one malformed row must not stop the others from
/// serving.
pub async fn rehydrate_indexes(runtime: &VectorRuntime) {
    let rows = match runtime
        .sys
        .list_running_indexes_for_cluster(&runtime.cluster_id)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!("vector: reading the index registry failed: {e}");
            return;
        }
    };
    for row in rows {
        let config = match config_from_row(&row) {
            Ok(config) => config,
            Err(e) => {
                tracing::warn!("vector: skipping index rehydration: {e}");
                continue;
            }
        };
        let ident = match ident_for_row(&row) {
            Ok(ident) => ident,
            Err(e) => {
                tracing::warn!("vector: skipping index rehydration: {e}");
                continue;
            }
        };
        let key = key_for_row(&row);
        match runtime
            .service
            .rehydrate(runtime.catalog.as_ref(), &ident, key, config)
            .await
        {
            Ok(verglas_vector::service::RehydrateOutcome::ServedFromBlob {
                reflected_snapshot,
                live_count,
            }) => {
                tracing::info!(
                    "vector: rehydrated index {} from its blob (snapshot {reflected_snapshot}, {live_count} live)",
                    row.name
                );
            }
            Ok(verglas_vector::service::RehydrateOutcome::Rebuilt(report)) => {
                if let Some(report) = report {
                    // A rebuilt blob is a new durable revision so the next reboot
                    // rehydrates it instead of rebuilding again.
                    if let Err(e) = runtime
                        .sys
                        .set_index_build(&row.name, report.reflected_snapshot, report.blob_location)
                        .await
                    {
                        tracing::warn!(
                            "vector: rebuilt index {} but could not record it: {e}",
                            row.name
                        );
                    }
                    tracing::info!("vector: rebuilt index {} (no blob was present)", row.name);
                } else {
                    tracing::info!(
                        "vector: index {} has no source rows yet; will build when data lands",
                        row.name
                    );
                }
            }
            Err(e) => tracing::warn!("vector: rehydrating index {} failed: {e}", row.name),
        }
    }
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

/// The `verglas_sys` registry and watermark sub-router: register, list, show,
/// and state transitions for workers, plus the deployment watermark store. Every
/// route answers 503 until the registry handle is wired after recovery. Registry
/// writes go through this daemon-held handle only — the CLI never writes
/// `verglas_sys` directly.
fn sys_router(sys: SysSlot) -> Router {
    Router::new()
        .route("/v1/workers", post(worker_register).get(worker_list))
        .route("/v1/workers/{name}", get(worker_show))
        .route("/v1/workers/{name}/state", put(worker_set_state))
        .route("/v1/watermark", get(watermark_get).put(watermark_put))
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

/// The worker execution sub-router: run a worker now regardless of trigger
/// (`POST /v1/workers/<name>/run`, the manual trigger), or route an inbound
/// webhook to a webhook-triggered worker (`POST /v1/hooks/<name>`). Both invoke
/// the worker runtime over the loopback catalog; each answers 503 until recovery
/// wires the runtime.
fn platform_router(platform: PlatformSlot) -> Router {
    Router::new()
        .route("/v1/workers/{name}/run", post(worker_run_now))
        .route("/v1/hooks/{name}", post(worker_webhook))
        .with_state(platform)
}

/// Runs one worker now through the runtime, returning the rows it produced. The
/// manual trigger: it runs any `Running` worker regardless of its declared
/// triggers, as a cron run over no interval. 503 until the runtime is wired; 500
/// with the plain message on a run failure (an unknown or paused worker is a run
/// failure).
async fn worker_run_now(
    State(platform): State<PlatformSlot>,
    Path(name): Path<String>,
) -> Response {
    run_worker_route(
        &platform,
        &name,
        verglas_sdk::worker::TriggerEvent::Cron(Default::default()),
    )
    .await
}

/// Routes an inbound webhook to its worker: runs the named worker now with a
/// webhook trigger event. The daemon does not interpret the webhook body — a
/// webhook worker reads whatever it needs through its own client.
async fn worker_webhook(
    State(platform): State<PlatformSlot>,
    Path(name): Path<String>,
) -> Response {
    run_worker_route(&platform, &name, verglas_sdk::worker::TriggerEvent::Webhook).await
}

/// Shared body for the manual-run and webhook routes: run the named worker for
/// `event` through the runtime.
async fn run_worker_route(
    platform: &PlatformSlot,
    name: &str,
    event: verglas_sdk::worker::TriggerEvent,
) -> Response {
    let Some(runtime) = platform.get() else {
        return recovering();
    };
    match runtime.run_worker_now(name, event).await {
        Ok(rows) => Json(json!({ "worker": name, "rows_produced": rows })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
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

/// Query parameters for the local watermark routes: the deployment identifier.
/// The cloud endpoint scopes the watermark by the caller's bearer token; the
/// local daemon has no per-deployment tokens, so the deployment is named
/// explicitly. The wire shape of the bodies is identical to the cloud's.
#[derive(Debug, Deserialize)]
struct WatermarkQuery {
    /// The deployment whose watermark is read or written.
    deployment: Option<String>,
}

/// The body of `PUT /v1/watermark` — the same one-key shape the cloud endpoint
/// takes (client.ts `setWatermark`).
#[derive(Debug, Deserialize)]
struct WatermarkPut {
    /// The opaque watermark value to store.
    watermark: String,
}

/// `GET /v1/watermark?deployment=`: the deployment's durable cross-run
/// watermark, `{"watermark": null}` before the first set — the same wire shape
/// as the cloud endpoint (client.ts `watermark()`). 400 without a deployment.
async fn watermark_get(
    State(sys): State<SysSlot>,
    Query(query): Query<WatermarkQuery>,
) -> Response {
    let Some(sys) = sys.get() else {
        return recovering();
    };
    let Some(deployment) = query.deployment else {
        return (
            StatusCode::BAD_REQUEST,
            "the local watermark routes need a ?deployment= identifier \
             (the cloud endpoint infers it from the bearer token)",
        )
            .into_response();
    };
    match sys.get_watermark(&deployment).await {
        Ok(row) => Json(json!({
            "watermark": row.map(|r| r.watermark)
        }))
        .into_response(),
        Err(error) => platform_error(error),
    }
}

/// `PUT /v1/watermark?deployment=`: stores the deployment's durable watermark,
/// overwriting the previous value (a new append-only revision underneath).
/// Answers 204 with no body, like the cloud endpoint. 400 without a deployment.
async fn watermark_put(
    State(sys): State<SysSlot>,
    Query(query): Query<WatermarkQuery>,
    Json(body): Json<WatermarkPut>,
) -> Response {
    let Some(sys) = sys.get() else {
        return recovering();
    };
    let Some(deployment) = query.deployment else {
        return (
            StatusCode::BAD_REQUEST,
            "the local watermark routes need a ?deployment= identifier \
             (the cloud endpoint infers it from the bearer token)",
        )
            .into_response();
    };
    match sys.set_watermark(&deployment, body.watermark).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => platform_error(error),
    }
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
        "verglasd cache engine is still recovering",
    )
        .into_response()
}

/// Returns the daemon readiness payload: `ok`/200 once recovery is complete,
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
        "verglasd cache purge: generation now {}, mappings freed {} bytes, \
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
    use verglas_tables::catalog::CatalogGateway;

    /// Builds a real cluster-of-one engine over an empty in-memory origin,
    /// erased to `Arc<dyn CachePurger>` — the same handle the daemon hands the
    /// admin router.
    async fn purger(dir: &std::path::Path) -> Arc<dyn CachePurger> {
        let store = std::sync::Arc::new(object_store::memory::InMemory::new());
        let backend = PassthroughRead::new(BackendStore::single("test-bucket", store));
        let config = CacheConfig {
            dir: dir.to_path_buf(),
            capacity_bytes: ByteSize(64 * 1024 * 1024),
            dram_bytes: ByteSize(128 * 1024 * 1024),
            shadow_capacity_bytes: ByteSize(1024 * 1024),
            ..CacheConfig::default()
        };
        let engine = HybridCacheEngine::single_node(backend, &config)
            .await
            .expect("build engine");
        Arc::new(engine)
    }

    /// A purger slot already filled with a real engine — the post-recovery
    /// state the daemon leaves the slot in.
    async fn ready_purger(dir: &std::path::Path) -> PurgerSlot {
        let slot: PurgerSlot = Arc::new(OnceLock::new());
        let _ = slot.set(purger(dir).await);
        slot
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

    /// The loopback catalog route forwards through the configured gateway and
    /// is absent from config-less daemons.
    #[tokio::test]
    async fn catalog_gateway_route_is_present_only_when_configured() {
        let upstream = Router::new().route(
            "/iceberg/v1/config",
            get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind catalog fixture");
        let addr = listener.local_addr().expect("fixture addr");
        tokio::spawn(async move {
            axum::serve(listener, upstream)
                .await
                .expect("fixture serves");
        });
        let catalog: verglas_core::config::Catalog = toml::de::from_str(&format!(
            "uri = \"http://{addr}/iceberg\"\npoll_interval_secs = 30\n"
        ))
        .expect("catalog config");
        let gateway = CatalogGateway::from_config(&catalog).expect("gateway");

        let configured = router(
            VERSION,
            Health::ready(),
            Slots {
                catalog: Some(gateway),
                ..Slots::default()
            },
        );
        let response = call(configured, "GET", "/catalog/v1/config").await;
        assert_eq!(response.status(), StatusCode::OK);

        let absent = router(VERSION, Health::ready(), Slots::default());
        let response = call(absent, "GET", "/catalog/v1/config").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// `GET /admin/access` (issue #287) returns the local-access snapshot when
    /// one is configured, and the route is absent otherwise so an older/config-
    /// less daemon simply 404s (the CLI then falls back to flags/env). The served
    /// body carries the discovery fields and never a `secret_access_key`: the
    /// admin socket is unauthenticated and host-scoped, so a secret served here
    /// would leak lakehouse read/write access to any local process.
    #[tokio::test]
    async fn access_route_serves_the_snapshot_without_the_secret() {
        let access = LocalAccess {
            s3_endpoint: "http://127.0.0.1:8333".to_owned(),
            catalog_path: Some("/catalog".to_owned()),
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
            "catalog_path",
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

    /// Without a purger slot (the config-less daemon), the purge route is absent
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

    /// A ready daemon's health probe answers `ok` even with the engine routes
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

    /// Without a drain slot (a single-node daemon) the drain route is absent —
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
    /// state the daemon leaves the slot in, but with a fixed body so the route
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

    /// Without a metrics slot (the config-less daemon) the metrics route is
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
    /// from a one-row CSV seed, wrapped as the daemon wires it post-recovery.
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

    /// A tables route on a router whose slot is not yet filled answers 503, like
    /// the other engine-dependent routes before recovery.
    #[tokio::test]
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
    async fn v1_serving_router_commits_a_table_end_to_end() {
        let slot = tables_slot_with_table().await;
        let router_of = || v1_serving_router(slot.clone(), None);

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

    /// The Rust SDK representation returns exact definitions, accepts Arrow
    /// appends, and streams Arrow query results through the real daemon router.
    #[tokio::test]
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

        let definition =
            json_body(call(router_of(), "GET", "/v1/tables/sdk.events/definition").await).await;
        assert_eq!(definition["schema"][0]["name"], "id");
        assert_eq!(definition["schema"][0]["type"], "int64");
        assert_eq!(definition["partitions"], serde_json::json!([]));

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

        let response = router_of()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("content-type", "application/json")
                    .header("accept", verglas_sdk::ARROW_STREAM_CONTENT_TYPE)
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "sql": "select id, name from sdk.events order by id"
                        }))
                        .expect("query JSON"),
                    ))
                    .expect("query request"),
            )
            .await
            .expect("query response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some(verglas_sdk::ARROW_STREAM_CONTENT_TYPE)
        );
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("query body");
        let batches = arrow_ipc::reader::StreamReader::try_new(bytes.as_ref(), None)
            .expect("query IPC")
            .collect::<Result<Vec<_>, _>>()
            .expect("query batches");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    }

    /// A repeated idempotency key over HTTP replays the original result and the
    /// live count does not grow.
    #[tokio::test]
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

    /// Builds a filled registry slot: a `SystemCatalog` over a fresh memory
    /// catalog, wrapped as the daemon wires it post-recovery.
    async fn sys_slot() -> SysSlot {
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
        let sys = verglas_platform::SystemCatalog::new(catalog);
        let slot: SysSlot = Arc::new(OnceLock::new());
        let _ = slot.set(Arc::new(sys));
        slot
    }

    /// A ready router with only the given slots filled.
    fn slots_router(slots: Slots) -> Router {
        router(VERSION, Health::ready(), slots)
    }

    /// `POST /v1/query` runs SQL through the engine over the wired catalog and
    /// returns the stable `{columns, rows, row_count}` report shape.
    #[tokio::test]
    async fn query_route_runs_sql_over_the_engine() {
        let slot = tables_slot_with_table().await;
        let app = slots_router(Slots {
            tables: Some(slot),
            ..Slots::default()
        });
        let body = json_body(
            call_json(
                app,
                "POST",
                "/v1/query",
                serde_json::json!({"sql": "SELECT id, name FROM sdk.events"}),
            )
            .await,
        )
        .await;
        assert_eq!(body["columns"], serde_json::json!(["id", "name"]));
        assert_eq!(body["row_count"], 1);
        assert_eq!(body["rows"][0]["name"], "seed");
    }

    /// `POST /v1/query` answers 503 until the loopback catalog is wired — the
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

    /// An unplannable SQL statement is a 400 with the engine's message, not a
    /// 500 — the statement is the caller's input.
    #[tokio::test]
    async fn query_route_maps_a_bad_statement_to_400() {
        let slot = tables_slot_with_table().await;
        let app = slots_router(Slots {
            tables: Some(slot),
            ..Slots::default()
        });
        let response = call_json(
            app,
            "POST",
            "/v1/query",
            serde_json::json!({"sql": "SELECT nope FROM missing.table"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// `GET`/`PUT /v1/watermark` round-trips with exactly the cloud endpoint's
    /// wire shape (client.ts `watermark()`/`setWatermark()`): the GET body is one
    /// `watermark` key (null before the first set), the PUT body is one
    /// `watermark` key, and the PUT answers 204 with no body. Locally the
    /// deployment is named by the `?deployment=` parameter (the cloud identifies
    /// it by bearer token).
    #[tokio::test]
    async fn watermark_round_trip_matches_the_cloud_wire_shape() {
        let slot = sys_slot().await;
        let router_of = || {
            slots_router(Slots {
                sys: Some(slot.clone()),
                ..Slots::default()
            })
        };

        // Null before the first set.
        let response = call(router_of(), "GET", "/v1/watermark?deployment=dep-a").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body, serde_json::json!({"watermark": null}), "wire shape");

        // PUT stores it; 204, empty body.
        let response = call_json(
            router_of(),
            "PUT",
            "/v1/watermark?deployment=dep-a",
            serde_json::json!({"watermark": "snap-42"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // GET returns exactly {"watermark": "snap-42"}.
        let body =
            json_body(call(router_of(), "GET", "/v1/watermark?deployment=dep-a").await).await;
        assert_eq!(body, serde_json::json!({"watermark": "snap-42"}));

        // Another deployment still reads null.
        let body =
            json_body(call(router_of(), "GET", "/v1/watermark?deployment=dep-b").await).await;
        assert_eq!(body, serde_json::json!({"watermark": null}));
    }

    /// The local watermark routes require the `?deployment=` identifier — a
    /// request without one is a 400, never a guess.
    #[tokio::test]
    async fn watermark_without_a_deployment_is_a_400() {
        let slot = sys_slot().await;
        let response = call(
            slots_router(Slots {
                sys: Some(slot.clone()),
                ..Slots::default()
            }),
            "GET",
            "/v1/watermark",
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let response = call_json(
            slots_router(Slots {
                sys: Some(slot),
                ..Slots::default()
            }),
            "PUT",
            "/v1/watermark",
            serde_json::json!({"watermark": "w"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// Sends a request with a raw (non-JSON) body and returns the response.
    async fn call_raw(app: Router, method: &str, uri: &str, body: Vec<u8>) -> Response {
        app.oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/octet-stream")
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("router responds")
    }

    /// The table inspect routes (#323) serve the CLI's list/show/history verbs:
    /// list with an optional namespace filter, describe with schema and
    /// counters, and the snapshot history — the same report shapes the embedded
    /// engine produced.
    #[tokio::test]
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
    /// source file, moved server-side: the daemon infers the schema, writes the
    /// data files, and commits — the CLI only streams bytes. `mode=create`
    /// creates (optionally partitioned); `mode=append` appends schema-checked.
    #[tokio::test]
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
    /// state the daemon leaves the slot in. The same handle type as the tables
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
