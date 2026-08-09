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
use axum::http::{HeaderMap, HeaderValue, Method, header};
use axum::response::{IntoResponse, Response};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{any, get, post},
};
use bytes::Bytes;
use serde::Deserialize;
use verglas_catalog::CatalogGateway;
use verglas_catalog::CatalogMutation;
use verglas_tables::catalog::PollingWatcher;
use verglas_write::catalog_log::CatalogLogError;

use verglas_core::admin::{
    HEALTHZ_PATH, HealthzInfo, METRICS_PATH, STATS_PATH, StatsInfo, VERSION_PATH, VersionInfo,
};
use verglas_core::metrics::EXPOSITION_CONTENT_TYPE;

use crate::catalog_consistency::{StrongCatalog, StrongCatalogError};

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
/// CloudEvent carrying the transactionally committed catalog pointer.
pub const CATALOG_EVENTS_PATH: &str = "/admin/catalog/events";

#[derive(Clone)]
struct StrongCatalogState {
    catalog: Arc<StrongCatalog>,
    token: Arc<str>,
}

#[derive(Clone)]
struct EventualCatalogState {
    watcher: Arc<PollingWatcher>,
    token: Arc<str>,
}

/// Mounts an authenticated hint that accelerates, but never replaces, polling.
pub fn eventual_catalog_event_router(watcher: Arc<PollingWatcher>, token: String) -> Router {
    Router::new()
        .route(CATALOG_EVENTS_PATH, post(eventual_catalog_event))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(EventualCatalogState {
            watcher,
            token: Arc::from(token),
        })
}

/// Accepts one eventual notification and wakes the periodic watcher.
async fn eventual_catalog_event(
    State(state): State<EventualCatalogState>,
    headers: HeaderMap,
) -> Response {
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if !constant_time_equal(presented.as_bytes(), state.token.as_bytes()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if headers
        .get("x-verglas-consistency")
        .and_then(|value| value.to_str().ok())
        != Some("eventual")
    {
        return (
            StatusCode::BAD_REQUEST,
            "x-verglas-consistency must be eventual",
        )
            .into_response();
    }
    state.watcher.request_refresh();
    StatusCode::ACCEPTED.into_response()
}

/// Mounts the fenced catalog gateway and authenticated quorum event endpoint.
pub fn strong_catalog_router(catalog: Arc<StrongCatalog>, token: String) -> Router {
    Router::new()
        .route(CATALOG_EVENTS_PATH, post(strong_catalog_event))
        .route(
            "/catalog/_verglas/generation",
            get(strong_catalog_generation),
        )
        .route("/catalog/{*path}", any(strong_catalog_request))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(StrongCatalogState {
            catalog,
            token: Arc::from(token),
        })
}

/// Quorum-appends and locally applies one ordered Lakekeeper mutation.
async fn strong_catalog_event(
    State(state): State<StrongCatalogState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if !constant_time_equal(presented.as_bytes(), state.token.as_bytes()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if headers
        .get("x-verglas-consistency")
        .and_then(|value| value.to_str().ok())
        != Some("strong")
    {
        return (
            StatusCode::BAD_REQUEST,
            "x-verglas-consistency must be strong",
        )
            .into_response();
    }
    let mutation = match decode_lakekeeper_mutation(&body) {
        Ok(mutation) => mutation,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    match state.catalog.append_and_apply(mutation).await {
        Ok(ack) => {
            let event_id = match HeaderValue::from_str(&ack.event_id) {
                Ok(value) => value,
                Err(_) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        "event id is not a valid header value",
                    )
                        .into_response();
                }
            };
            let sequence = match HeaderValue::from_str(&ack.sequence.to_string()) {
                Ok(value) => value,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            let mut response = StatusCode::OK.into_response();
            response
                .headers_mut()
                .insert("x-verglas-applied-event-id", event_id);
            response
                .headers_mut()
                .insert("x-verglas-applied-sequence", sequence);
            response
        }
        Err(StrongCatalogError::Log(
            CatalogLogError::StaleSequence { .. } | CatalogLogError::Conflict { .. },
        )) => StatusCode::CONFLICT.into_response(),
        Err(error) => {
            tracing::warn!(%error, "strong catalog mutation remains unapplied");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

#[derive(Deserialize)]
struct LakekeeperEvent {
    id: String,
    sequence: u64,
    #[serde(rename = "type")]
    operation: String,
    warehouseid: String,
    tableid: String,
    #[serde(default)]
    snapshotid: Option<i64>,
    data: LakekeeperEventData,
}

#[derive(Deserialize)]
struct LakekeeperEventData {
    #[serde(default)]
    before: Option<LakekeeperTableRow>,
    #[serde(default)]
    after: Option<LakekeeperTableRow>,
}

#[derive(Deserialize)]
struct LakekeeperTableRow {
    name: String,
    tabular_namespace_name: Vec<String>,
    #[serde(default)]
    metadata_location: Option<String>,
}

/// Decodes the durable Lakekeeper envelope into the ring's compact record.
fn decode_lakekeeper_mutation(body: &[u8]) -> Result<CatalogMutation, String> {
    let event: LakekeeperEvent =
        serde_json::from_slice(body).map_err(|error| format!("invalid catalog event: {error}"))?;
    let target = if event.operation == "dropTable" {
        event.data.before.as_ref()
    } else {
        event.data.after.as_ref()
    }
    .ok_or_else(|| format!("{} event has no target table row", event.operation))?;
    if event.operation != "dropTable" && target.metadata_location.is_none() {
        return Err(format!(
            "{} event has no committed metadata location",
            event.operation
        ));
    }
    let (previous_namespace, previous_table) = if event.operation == "renameTable" {
        let previous = event
            .data
            .before
            .as_ref()
            .ok_or_else(|| "renameTable event has no previous table row".to_owned())?;
        (
            Some(previous.tabular_namespace_name.clone()),
            Some(previous.name.clone()),
        )
    } else {
        (None, None)
    };
    Ok(CatalogMutation {
        sequence: event.sequence,
        event_id: event.id,
        warehouse_id: event.warehouseid,
        table_id: event.tableid,
        namespace: target.tabular_namespace_name.clone(),
        table: target.name.clone(),
        previous_namespace,
        previous_table,
        metadata_location: (event.operation != "dropTable")
            .then(|| target.metadata_location.clone())
            .flatten(),
        snapshot_id: event.snapshotid,
        operation: event.operation,
    })
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

/// Returns a fenced generation only after replaying the ring's committed tail.
async fn strong_catalog_generation(State(state): State<StrongCatalogState>) -> Response {
    if let Err(error) = state.catalog.catch_up().await {
        tracing::warn!(%error, "strong catalog generation fence unavailable");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    Json(serde_json::json!({
        "generation": state.catalog.generation(),
        "applied_sequence": state.catalog.applied_sequence(),
    }))
    .into_response()
}

/// Fences one strong metadata read against the EC committed tail.
async fn strong_catalog_request(
    State(state): State<StrongCatalogState>,
    uri: OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(error) = state.catalog.catch_up().await {
        tracing::warn!(%error, "strong catalog read fence unavailable");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    forward_catalog(state.catalog.gateway(), uri, method, headers, body).await
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
    uri: OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward_catalog(&catalog, uri, method, headers, body).await
}

/// Forwards one already-fenced local catalog request to the shared gateway.
async fn forward_catalog(
    catalog: &CatalogGateway,
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

#[cfg(test)]
mod catalog_consistency_tests {
    use super::decode_lakekeeper_mutation;

    /// The direct protocol preserves the durable sequence and both sides of a
    /// rename instead of reducing the body to an unqualified refresh signal.
    #[test]
    fn decodes_ordered_lakekeeper_rename() {
        let mutation = decode_lakekeeper_mutation(
            br#"{
                "id":"event-41",
                "sequence":41,
                "type":"renameTable",
                "warehouseid":"warehouse-1",
                "tableid":"table-1",
                "data":{
                    "before":{"name":"events","tabular_namespace_name":["old"],"metadata_location":"s3://lake/events/v1.json"},
                    "after":{"name":"renamed","tabular_namespace_name":["new"],"metadata_location":"s3://lake/events/v1.json"}
                }
            }"#,
        )
        .expect("valid mutation");
        assert_eq!(mutation.sequence, 41);
        assert_eq!(mutation.event_id, "event-41");
        assert_eq!(mutation.namespace, vec!["new"]);
        assert_eq!(mutation.table, "renamed");
        assert_eq!(mutation.previous_namespace, Some(vec!["old".into()]));
        assert_eq!(mutation.previous_table.as_deref(), Some("events"));
    }

    /// Strong delivery rejects a body without the PostgreSQL outbox sequence;
    /// it never silently treats the event as an eventual refresh.
    #[test]
    fn rejects_event_without_durable_sequence() {
        let error = decode_lakekeeper_mutation(
            br#"{"id":"event-1","type":"updateTable","warehouseid":"w","tableid":"t","data":{"after":{"name":"t","tabular_namespace_name":["db"],"metadata_location":"s3://m"}}}"#,
        )
        .expect_err("sequence is mandatory");
        assert!(error.to_string().contains("sequence"));
    }

    /// A staged Lakekeeper row is not a committed Iceberg table mutation and
    /// cannot silently behave like a drop in the strong state machine.
    #[test]
    fn rejects_non_drop_event_without_metadata_pointer() {
        let error = decode_lakekeeper_mutation(
            br#"{"id":"event-1","sequence":1,"type":"createTable","warehouseid":"w","tableid":"t","data":{"after":{"name":"t","tabular_namespace_name":["db"],"metadata_location":null}}}"#,
        )
        .expect_err("committed pointer is mandatory");
        assert!(error.contains("no committed metadata location"));
    }
}
