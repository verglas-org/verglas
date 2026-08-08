//! The cache node's minimal admin HTTP surface.
//!
//! A private, loopback control surface on a separate port from the S3 data
//! plane, carrying exactly the four endpoints the fleet polls:
//!
//! - `GET /admin/healthz` — the fleet's health check (RUNBOOK contract). Reports
//!   `starting`/503 while the cache engine rebuilds its index from the on-disk
//!   tiers, then `ok`/200 once recovery completes, so a load balancer never
//!   routes to a node that would cold-miss everything (serve-gating, #16).
//! - `GET /admin/version` — the binary name and release.
//! - `GET /admin/stats` — the JSON read-path counter snapshot the benchmarks read.
//! - `GET /metrics` — the Prometheus exposition the metrics VM self-scrapes.
//!
//! Everything else verglas-server's admin router carries (purge, members, drain, the
//! table/graph/vector/platform/recall verb families) is deliberately absent:
//! the cache node proxies prepared catalog reads but owns no catalog semantics,
//! surfaces have nothing to answer. The `Health` gate and the deferred stats/
//! metrics slots mirror verglas-server's serve-gating shape (origin: `bins/verglas-server/src/admin.rs`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use axum::extract::{DefaultBodyLimit, OriginalUri};
use axum::http::{HeaderMap, Method, header};
use axum::response::{IntoResponse, Response};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{any, get, post},
};
use bytes::Bytes;
use verglas_catalog::CatalogGateway;
use verglas_tables::catalog::PushWatcher;

use verglas_core::admin::{
    HEALTHZ_PATH, HealthzInfo, METRICS_PATH, STATS_PATH, StatsInfo, VERSION_PATH, VersionInfo,
};
use verglas_core::metrics::EXPOSITION_CONTENT_TYPE;

/// Readiness gate for serve-gating (#16). Starts reporting `starting` and flips
/// to `ok` once the cache engine's disk recovery completes. Cheap to share: one
/// atomic behind an `Arc`.
#[derive(Clone)]
pub struct Health(Arc<AtomicBool>);

impl Health {
    /// A gate that reports `starting` until [`Health::mark_ready`] is called.
    pub fn starting() -> Self {
        Health(Arc::new(AtomicBool::new(false)))
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

/// Produces a live [`StatsInfo`] snapshot. The serve path supplies one closure
/// over its engine handle and config; the admin surface owns nothing of the
/// engine's type.
pub type StatsSource = Arc<dyn Fn() -> StatsInfo + Send + Sync>;

/// Produces the Prometheus text exposition for `GET /metrics`.
pub type MetricsSource = Arc<dyn Fn() -> String + Send + Sync>;

/// A stats source wired lazily once recovery completes (#16 serve-gating). Empty
/// while the engine is still building; the stats route answers 503 until set.
pub type StatsSlot = Arc<OnceLock<StatsSource>>;

/// A metrics source wired lazily once recovery completes; see [`StatsSlot`].
pub type MetricsSlot = Arc<OnceLock<MetricsSource>>;

/// Direct Lakekeeper mutation endpoint on the tenant network. The payload is a
/// CloudEvent for observability, but catalog state remains authoritative: one
/// authenticated signal schedules a coalesced pointer reconciliation.
pub const CATALOG_EVENTS_PATH: &str = "/admin/catalog/events";

#[derive(Clone)]
struct CatalogEventState {
    watcher: Arc<PushWatcher>,
    token: Arc<str>,
}

/// Mounts the authenticated, push-driven catalog refresh endpoint.
pub fn catalog_event_router(watcher: Arc<PushWatcher>, token: String) -> Router {
    Router::new()
        .route(CATALOG_EVENTS_PATH, post(catalog_event))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(CatalogEventState {
            watcher,
            token: Arc::from(token),
        })
}

async fn catalog_event(
    State(state): State<CatalogEventState>,
    headers: HeaderMap,
    _body: Bytes,
) -> Response {
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if !constant_time_equal(presented.as_bytes(), state.token.as_bytes()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if !state.watcher.request_refresh() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    StatusCode::ACCEPTED.into_response()
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let length = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..length {
        difference |= usize::from(*left.get(index).unwrap_or(&0) ^ *right.get(index).unwrap_or(&0));
    }
    difference == 0
}

/// The 503 body the engine-dependent routes return until recovery completes.
fn recovering() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "verglas-cache-node cache engine is still recovering",
    )
        .into_response()
}

/// Returns the readiness payload: `ok`/200 once recovery is complete,
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

/// Assembles the admin router. `version` names this binary in `/admin/version`;
/// the two slots are filled by the serve path the instant recovery completes, so
/// `/admin/stats` and `/metrics` answer 503 (not 404) until then, exactly like
/// verglas-server's deferred routes.
pub fn router(
    version: &'static str,
    health: Health,
    stats: StatsSlot,
    metrics: MetricsSlot,
) -> Router {
    Router::new()
        .route(
            VERSION_PATH,
            get(move || async move {
                Json(VersionInfo {
                    name: "verglas-cache-node".to_owned(),
                    version: version.to_owned(),
                })
            }),
        )
        .route(HEALTHZ_PATH, get(healthz))
        .with_state(health)
        .route(
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
        )
        .route(
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
        )
}

/// Mounts the cache-owned Iceberg REST metadata gateway at `/catalog`.
/// Successful GET responses are shared with the cache node's watcher; query
/// workers therefore consume already-observed metadata without Lakekeeper
/// credentials or an independent catalog cache.
pub fn catalog_router(catalog: CatalogGateway) -> Router {
    Router::new()
        .route("/catalog/_verglas/generation", get(catalog_generation))
        .route("/catalog/{*path}", any(catalog_request))
        .with_state(catalog)
}

/// Returns the cache-owned catalog generation. Query workers keep their
/// DataFusion catalog session while this value is unchanged and rebuild it
/// exactly once after the watcher observes a changed catalog response.
async fn catalog_generation(State(catalog): State<CatalogGateway>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "generation": catalog.generation() }))
}

/// Forwards one local metadata request through the cache node's shared catalog
/// gateway while preserving the upstream status, headers, and body.
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
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            format!("catalog gateway error: {error}"),
        )
            .into_response(),
    }
}
