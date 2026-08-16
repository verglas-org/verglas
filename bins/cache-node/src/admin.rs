//! The cache node's minimal admin HTTP surface.
//!
//! A private, loopback control surface on a separate port from the S3 data
//! plane. Health, version, stats, and metrics remain operator probes; a fifth
//! authenticated surface lets a host atomically fence foreground work before
//! stopping an idle process while retaining its NVMe:
//!
//! - `GET /admin/healthz` — the fleet's health check (RUNBOOK contract). Reports
//!   `starting`/503 while the cache engine rebuilds its index from the on-disk
//!   tiers, then `ok`/200 once recovery completes, so a load balancer never
//!   routes to a node that would cold-miss everything (serve-gating, #16).
//! - `GET /admin/version` — the binary name and release.
//! - `GET /admin/stats` — the JSON read-path counter snapshot the benchmarks read.
//! - `GET /metrics` — the Prometheus exposition the metrics VM self-scrapes.
//! - `GET /admin/quiescence` and its fence subresources — bearer-authenticated
//!   foreground activity diagnostics, atomic admission close, and
//!   generation-checked reopen after a failed stop.
//!
//! Everything else verglas-cache-node's admin router carries (purge, members, drain, the
//! table/graph/vector/platform/recall verb families) is deliberately absent:
//! the cache node proxies prepared catalog reads but owns no catalog semantics,
//! surfaces have nothing to answer. The `Health` gate and the deferred stats/
//! metrics slots mirror verglas-cache-node's serve-gating shape (origin: `bins/cache-node/src/admin.rs`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use futures::future::BoxFuture;

use axum::extract::{DefaultBodyLimit, OriginalUri, Request};
use axum::http::{HeaderMap, Method, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{any, get, post},
};
use bytes::Bytes;
use futures::StreamExt;
use serde::Deserialize;
use verglas_catalog::CatalogGateway;
use verglas_tables::catalog::PollingWatcher;

use verglas_core::activity::{ActivityPlane, ActivityTracker};
use verglas_core::admin::{
    ACCESS_PATH, HEALTHZ_PATH, HealthzInfo, LocalAccess, METRICS_PATH, QUIESCENCE_PATH, STATS_PATH,
    StatsInfo, VERSION_PATH, VersionInfo,
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
/// CloudEvent carrying the transactionally committed catalog pointer.
pub const CATALOG_EVENTS_PATH: &str = "/admin/catalog/events";
/// Authenticated lifecycle endpoint that must finish authoritative drain before VM stop.
pub const AUTHORITATIVE_STOP_PATH: &str = "/admin/lifecycle/authoritative-stop";
/// Authenticated authoritative-voter replacement endpoint.
pub const MEMBERSHIP_REPLACE_PATH: &str = "/admin/lifecycle/membership/replace";
/// Authenticated control-plane binding from a Neon timeline to its WAL bucket.
pub const NEON_TIMELINE_BINDINGS_PATH: &str = "/admin/neon/timeline-bindings";

/// One explicit authoritative-voter replacement request.
#[derive(Deserialize)]
pub struct MembershipReplaceRequest {
    pub group: String,
    pub remove: u64,
    pub add: u64,
    pub node_id: String,
    pub address: std::net::SocketAddr,
}

/// One immutable durable-object binding requested before a Neon timeline opens.
#[derive(Deserialize)]
pub struct NeonTimelineBindingRequest {
    /// Tenant component of the consensus timeline group identity.
    pub tenant_id: String,
    /// Timeline component of the consensus timeline group identity.
    pub timeline_id: String,
    /// Tigris bucket authorized for only this timeline's WAL archive objects.
    pub bucket: String,
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
    if !constant_time_eq(presented, state.token.as_ref()) {
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

/// Authenticated host-agent access to the foreground activity tracker.
#[derive(Clone)]
pub struct Quiescence {
    /// Foreground operations shared by every cache-node protocol plane.
    tracker: ActivityTracker,
    /// Required bearer token, kept out of the response and logs.
    token: Arc<str>,
}

/// Async authoritative drain operation supplied by cache-node serving wiring.
pub type AuthoritativeStop = Arc<dyn Fn() -> BoxFuture<'static, Result<(), String>> + Send + Sync>;
/// Async replacement supplied by the consensus lifecycle owner.
pub type MembershipReplace =
    Arc<dyn Fn(MembershipReplaceRequest) -> BoxFuture<'static, Result<(), String>> + Send + Sync>;
/// Binds a consensus timeline to its immutable WAL archive bucket.
pub type NeonTimelineBinding =
    Arc<dyn Fn(NeonTimelineBindingRequest) -> BoxFuture<'static, Result<(), String>> + Send + Sync>;

#[derive(Clone)]
struct NeonTimelineBindingState {
    token: Arc<str>,
    bind: NeonTimelineBinding,
}

/// Mounts the bearer-authenticated durable WAL binding control surface.
pub fn neon_timeline_binding_router(token: String, bind: NeonTimelineBinding) -> Router {
    Router::new()
        .route(NEON_TIMELINE_BINDINGS_PATH, post(bind_neon_timeline))
        .with_state(NeonTimelineBindingState {
            token: Arc::from(token),
            bind,
        })
}

/// Validates and commits one durable archive binding before Neon ingress starts.
async fn bind_neon_timeline(
    State(state): State<NeonTimelineBindingState>,
    headers: HeaderMap,
    Json(request): Json<NeonTimelineBindingRequest>,
) -> Response {
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if !constant_time_eq(presented, state.token.as_ref()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if request.tenant_id.is_empty() || request.timeline_id.is_empty() || request.bucket.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "tenant_id, timeline_id, and bucket are required",
        )
            .into_response();
    }
    match (state.bind)(request).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::CONFLICT, error).into_response(),
    }
}

/// Mounts authenticated consensus membership replacement without a catalog fallback.
pub fn membership_router(quiescence: Quiescence, replace: MembershipReplace) -> Router {
    Router::new()
        .route(MEMBERSHIP_REPLACE_PATH, post(replace_membership))
        .with_state((quiescence, replace))
}

/// Executes the committed learner, repair, joint, and uniform transition.
async fn replace_membership(
    State((quiescence, replace)): State<(Quiescence, MembershipReplace)>,
    headers: HeaderMap,
    Json(request): Json<MembershipReplaceRequest>,
) -> Response {
    if let Err(response) = authenticate_host(&headers, &quiescence.token) {
        return *response;
    }
    if request.group.is_empty()
        || request.remove == 0
        || request.add == 0
        || request.add == request.remove
        || request.node_id.is_empty()
    {
        return (
            StatusCode::BAD_REQUEST,
            "request is not one voter replacement",
        )
            .into_response();
    }
    match replace(request).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::CONFLICT, error.to_string()).into_response(),
    }
}

impl Quiescence {
    /// Creates a fail-closed probe configuration from a non-empty token.
    pub fn new(tracker: ActivityTracker, token: impl Into<String>) -> Result<Self, &'static str> {
        let token = token.into();
        if token.is_empty() {
            return Err("host-agent token must not be empty");
        }
        Ok(Self {
            tracker,
            token: Arc::from(token),
        })
    }
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
/// verglas-cache-node's deferred routes.
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

/// Mounts the host-agent stop-fence probe without placing the probe itself in
/// foreground activity accounting.
pub fn quiescence_router(quiescence: Quiescence, stop: Option<AuthoritativeStop>) -> Router {
    let stop_quiescence = quiescence.clone();
    Router::new()
        .route(QUIESCENCE_PATH, get(quiescence_status))
        .route(
            &format!("{QUIESCENCE_PATH}/fence"),
            axum::routing::post(fence_admission),
        )
        .route(
            &format!("{QUIESCENCE_PATH}/fence/{{generation}}"),
            axum::routing::delete(unfence_admission),
        )
        .route(
            AUTHORITATIVE_STOP_PATH,
            post(move |headers| authoritative_stop(stop_quiescence.clone(), stop.clone(), headers)),
        )
        .with_state(quiescence)
}

/// Refuses a stop until the archive, catalog-export, and membership coordinator is wired.
///
/// A foreground fence alone cannot authorize stopping an authoritative CRaft voter.
async fn authoritative_stop(
    quiescence: Quiescence,
    stop: Option<AuthoritativeStop>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = authenticate_host(&headers, &quiescence.token) {
        return *response;
    }
    match stop {
        Some(stop) => match stop().await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(error) => (StatusCode::CONFLICT, error).into_response(),
        },
        None => (
            StatusCode::CONFLICT,
            "authoritative lifecycle coordinator is unavailable; retaining voter",
        )
            .into_response(),
    }
}

/// Returns foreground activity only to the configured host agent.
async fn quiescence_status(State(quiescence): State<Quiescence>, headers: HeaderMap) -> Response {
    if let Err(response) = authenticate_host(&headers, &quiescence.token) {
        return *response;
    }
    Json(quiescence.tracker.snapshot()).into_response()
}

/// Atomically closes foreground admission before reporting the current fence.
async fn fence_admission(State(quiescence): State<Quiescence>, headers: HeaderMap) -> Response {
    if let Err(response) = authenticate_host(&headers, &quiescence.token) {
        return *response;
    }
    let _generation = quiescence.tracker.fence();
    Json(quiescence.tracker.snapshot()).into_response()
}

/// Reopens foreground admission only for the current fence generation.
async fn unfence_admission(
    State(quiescence): State<Quiescence>,
    Path(generation): Path<u64>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = authenticate_host(&headers, &quiescence.token) {
        return *response;
    }
    if !quiescence.tracker.unfence(generation) {
        return (
            StatusCode::CONFLICT,
            "fence generation is stale or admission is already open",
        )
            .into_response();
    }
    Json(quiescence.tracker.snapshot()).into_response()
}

/// Applies foreground HTTP accounting to a data router. The guard is retained
/// inside the response stream so large S3 reads remain active through their
/// final byte and every error/drop path releases the counter.
pub fn track_http(router: Router, tracker: ActivityTracker, plane: ActivityPlane) -> Router {
    router.layer(axum::middleware::from_fn_with_state(
        (tracker, plane),
        track_http_request,
    ))
}

/// Holds one HTTP activity guard through the returned response-body stream.
async fn track_http_request(
    State((tracker, plane)): State<(ActivityTracker, ActivityPlane)>,
    request: Request,
    next: Next,
) -> Response {
    let Ok(guard) = tracker.try_begin(plane) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "verglas-cache-node is fenced for scale-to-zero",
        )
            .into_response();
    };
    let response = next.run(request).await;
    let (parts, body) = response.into_parts();
    let stream = futures::stream::unfold(
        (body.into_data_stream(), guard),
        |(mut body, guard)| async move { body.next().await.map(|item| (item, (body, guard))) },
    );
    Response::from_parts(parts, axum::body::Body::from_stream(stream))
}

/// Validates the host-agent bearer credential or returns a bounded 401.
fn authenticate_host(headers: &HeaderMap, expected: &str) -> Result<(), Box<Response>> {
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if supplied.is_some_and(|value| constant_time_eq(value, expected)) {
        Ok(())
    } else {
        Err(Box::new(
            (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                "host-agent bearer token required",
            )
                .into_response(),
        ))
    }
}

/// Compares equal-length credentials without an early byte mismatch exit.
fn constant_time_eq(supplied: &str, expected: &str) -> bool {
    if supplied.len() != expected.len() {
        return false;
    }
    supplied
        .as_bytes()
        .iter()
        .zip(expected.as_bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
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

/// Mounts non-secret client discovery for the local Table and semantic APIs.
/// The catalog URI always names this process's gateway, never the upstream
/// provider, so clients cannot bypass its credential boundary.
pub fn access_router(access: LocalAccess) -> Router {
    Router::new().route(
        ACCESS_PATH,
        get(move || {
            let access = access.clone();
            async move { Json(access) }
        }),
    )
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
    let authorization = headers.get(header::AUTHORIZATION).cloned();
    let database_id = headers
        .get("x-verglas-database-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let result = match (authorization, database_id) {
        (Some(authorization), Some(database_id)) => {
            catalog
                .authenticated_request_for_database(
                    method,
                    upstream_path,
                    headers,
                    body,
                    authorization,
                    &database_id,
                )
                .await
        }
        // The self-hosted SDK uses a bearer for its local API even though this
        // deployment has no database-scoped catalog identity. Drop that caller
        // bearer and let the gateway apply its configured provider authority.
        (_, None) => catalog.request(method, upstream_path, headers, body).await,
        (None, Some(_)) => {
            return (
                StatusCode::UNAUTHORIZED,
                "catalog bearer and database identity are required together",
            )
                .into_response();
        }
    };
    match result {
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
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, header};
    use tower::ServiceExt;
    use verglas_core::activity::{ActivityPlane, ActivityTracker};
    use verglas_core::admin::{QUIESCENCE_PATH, QuiescenceInfo};

    #[tokio::test]
    async fn membership_replacement_requires_host_token_and_valid_request() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let called = Arc::clone(&calls);
        let replace: MembershipReplace = Arc::new(move |_| {
            let called = Arc::clone(&called);
            Box::pin(async move {
                called.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        });
        let app = membership_router(
            Quiescence::new(ActivityTracker::new(), "host-secret").expect("token"),
            replace,
        );
        let body = r#"{"group":"timeline/a","remove":1,"add":2,"node_id":"node-b","address":"127.0.0.1:5500"}"#;
        let denied = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(MEMBERSHIP_REPLACE_PATH)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let invalid = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(MEMBERSHIP_REPLACE_PATH)
                    .header(header::AUTHORIZATION, "Bearer host-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"group":"","remove":1,"add":1,"node_id":"","address":"127.0.0.1:5500"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        let accepted = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(MEMBERSHIP_REPLACE_PATH)
                    .header(header::AUTHORIZATION, "Bearer host-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(accepted.status(), StatusCode::NO_CONTENT);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    /// Timeline archive binding accepts only the configured control-plane bearer.
    #[tokio::test]
    async fn neon_timeline_binding_uses_bearer_and_returns_conflicts() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let called = Arc::clone(&calls);
        let bind: NeonTimelineBinding = Arc::new(move |request| {
            let called = Arc::clone(&called);
            Box::pin(async move {
                called.fetch_add(1, Ordering::Relaxed);
                if request.bucket == "other-bucket" {
                    Err("timeline already uses its first bucket".to_owned())
                } else {
                    Ok(())
                }
            })
        });
        let app = neon_timeline_binding_router("control-secret".to_owned(), bind);
        let request =
            r#"{"tenant_id":"tenant-a","timeline_id":"timeline-a","bucket":"tenant-a-db"}"#;
        let denied = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(NEON_TIMELINE_BINDINGS_PATH)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let bound = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(NEON_TIMELINE_BINDINGS_PATH)
                    .header(header::AUTHORIZATION, "Bearer control-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(bound.status(), StatusCode::NO_CONTENT);
        let conflicting = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(NEON_TIMELINE_BINDINGS_PATH)
                    .header(header::AUTHORIZATION, "Bearer control-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"tenant_id":"tenant-a","timeline_id":"timeline-a","bucket":"other-bucket"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(conflicting.status(), StatusCode::CONFLICT);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    /// The scale-to-zero status is fail-closed and discloses activity only to
    /// the host agent holding the configured bearer credential.
    #[tokio::test]
    async fn quiescence_status_requires_the_host_agent_token() {
        let tracker = ActivityTracker::new();
        let app = quiescence_router(
            Quiescence::new(tracker, "host-secret").expect("token"),
            None,
        );

        for authorization in [None, Some("Bearer wrong-secret")] {
            let mut request = Request::builder().uri(QUIESCENCE_PATH);
            if let Some(value) = authorization {
                request = request.header(header::AUTHORIZATION, value);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).expect("request"))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri(QUIESCENCE_PATH)
                    .header(header::AUTHORIZATION, "Bearer host-secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let info: QuiescenceInfo = serde_json::from_slice(&body).expect("quiescence json");
        assert!(info.idle);
        assert_eq!(info.accepted, 0, "the probe must not count itself");
    }

    /// A foreground HTTP request remains in flight until its streaming body is
    /// consumed or dropped, not merely until the handler returns its headers.
    #[tokio::test]
    async fn tracked_http_body_holds_the_stop_fence_until_stream_end() {
        let tracker = ActivityTracker::new();
        let body_gate = Arc::new(tokio::sync::Notify::new());
        let gate = body_gate.clone();
        let app = track_http(
            Router::new().route(
                "/stream",
                get(move || {
                    let gate = gate.clone();
                    async move {
                        Body::from_stream(futures::stream::once(async move {
                            gate.notified().await;
                            Ok::<_, std::convert::Infallible>(Bytes::from_static(b"done"))
                        }))
                    }
                }),
            ),
            tracker.clone(),
            ActivityPlane::Http,
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(tracker.snapshot().inflight, 1);
        body_gate.notify_one();
        let _ = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("stream body");
        assert!(tracker.snapshot().idle);
    }

    /// The host atomically fences new admission, observes pre-fence work drain,
    /// and can reopen only with the current fence generation.
    #[tokio::test]
    async fn fence_rejects_new_http_and_stale_reopen() {
        let tracker = ActivityTracker::new();
        let data = track_http(
            Router::new().route("/data", get(|| async { "ok" })),
            tracker.clone(),
            ActivityPlane::Http,
        );
        let control = quiescence_router(
            Quiescence::new(tracker.clone(), "host-secret").expect("token"),
            None,
        );

        let fence = control
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{QUIESCENCE_PATH}/fence"))
                    .header(header::AUTHORIZATION, "Bearer host-secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("fence response");
        assert_eq!(fence.status(), StatusCode::OK);
        let fence_body = to_bytes(fence.into_body(), usize::MAX)
            .await
            .expect("fence body");
        let info: QuiescenceInfo = serde_json::from_slice(&fence_body).expect("json");
        assert!(!info.accepting);
        assert!(info.idle);

        let rejected = data
            .oneshot(
                Request::builder()
                    .uri("/data")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("data response");
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(tracker.snapshot().accepted, 0);

        let stale = control
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("{QUIESCENCE_PATH}/fence/0"))
                    .header(header::AUTHORIZATION, "Bearer host-secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("stale reopen response");
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        assert!(!tracker.snapshot().accepting);

        let reopened = control
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("{QUIESCENCE_PATH}/fence/{}", info.generation))
                    .header(header::AUTHORIZATION, "Bearer host-secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("reopen response");
        assert_eq!(reopened.status(), StatusCode::OK);
        assert!(tracker.snapshot().accepting);
    }
}
