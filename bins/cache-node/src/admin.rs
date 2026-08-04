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
//! Everything else verglasd's admin router carries (purge, members, drain, the
//! catalog/table/graph/vector/platform/recall verb families) is deliberately
//! absent: the cache node runs no catalog, no cluster, and no jobs, so those
//! surfaces have nothing to answer. The `Health` gate and the deferred stats/
//! metrics slots mirror verglasd's serve-gating shape (origin: `bins/verglasd/src/admin.rs`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use axum::response::{IntoResponse, Response};
use axum::{Json, Router, extract::State, http::StatusCode, routing::get};

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
/// verglasd's deferred routes.
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
