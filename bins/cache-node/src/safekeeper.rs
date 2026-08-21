//! Neon WAL ingress over the cache fleet's universal coded Multi-Raft groups.

use axum::serve::ListenerExt as _;
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::Response,
    routing::post,
};
use object_store::ObjectStoreExt;
use sha2::{Digest, Sha256};
use verglas_backend::{BackendStore, BackendStores, MultipartObjectStore};
use verglas_catalog::{ManagedCatalogRequest, ManagedCatalogResponse};
use verglas_consensus::{CatalogExport, GroupError, GroupRequest, GroupResponse, ReplicationMode};
use verglas_core::activity::ActivityTracker;
use verglas_safekeeper::{
    AppendGeometry, ArchiveError, ArchiveObject, ArchiveTimeline, ImmutableSegmentStore,
    SegmentArchiver, SegmentRelease, WalRequest, WalResponse, read_archived_wal_prefix,
};

use crate::{VERSION, consensus::ConsensusPlane, ring::RingPlane};

/// Default HTTP/2 address for the Verglas Neon WAL protocol.
const DEFAULT_SAFEKEEPER_ADDR: &str = "0.0.0.0:5454";
const WAL_SEGMENT_BYTES: u64 = 16 * 1024 * 1024;
/// Hard ingress budget: one complete 16 MiB WAL segment and bounded frame headroom.
///
/// The public benchmark appends 8 MiB frames, while archive scheduling operates on
/// complete 16 MiB segments. This prevents the HTTP extractor from retaining an
/// unbounded request and leaves room for the canonical wire envelope and catalog
/// command metadata.
const INGRESS_BODY_LIMIT_BYTES: usize = 17 * 1024 * 1024;

#[derive(Clone)]
struct WalIngress {
    consensus: Arc<ConsensusPlane>,
    coded_holders: Vec<u64>,
    majority_holders: Vec<u64>,
    wal_registry: Arc<BackendStore>,
    archive_inflight: Arc<Mutex<BTreeSet<String>>>,
}

/// One immutable object namespace assigned to a single durability domain.
///
/// The concrete type deliberately travels with its prefix so callers cannot
/// pair a WAL store with a catalog prefix (or the reverse) at a call site.
#[derive(Clone)]
struct ArchiveDestination {
    store: Arc<dyn ImmutableSegmentStore>,
    prefix: String,
}

impl ArchiveDestination {
    /// Pairs one archive store with its namespace prefix.
    fn new(store: Arc<dyn ImmutableSegmentStore>, prefix: String) -> Self {
        Self { store, prefix }
    }
}

/// Immutable object namespace reserved for timeline WAL archive segments.
#[derive(Clone)]
pub(crate) struct WalArchiveDestination(ArchiveDestination);

impl WalArchiveDestination {
    /// Creates the WAL-only archive destination from the WAL config target.
    pub(crate) fn new(store: Arc<dyn ImmutableSegmentStore>, prefix: String) -> Self {
        Self(ArchiveDestination::new(store, prefix))
    }
}

/// Immutable object namespace reserved for warehouse catalog checkpoints.
#[derive(Clone)]
pub(crate) struct CatalogArchiveDestination(ArchiveDestination);

impl CatalogArchiveDestination {
    /// Creates the catalog-only archive destination from the catalog config target.
    pub(crate) fn new(store: Arc<dyn ImmutableSegmentStore>, prefix: String) -> Self {
        Self(ArchiveDestination::new(store, prefix))
    }
}

/// Separates the two authoritative object durability domains.
///
/// This is intentionally not a map keyed by group name: each field makes a
/// WAL/catalog routing mistake visible in type-checked call sites.
/// The catalog destination is absent on a cache-only node. Only `warehouse/`
/// groups read it, and those exist only when the catalog runs, so a WAL-only
/// ring needs no catalog archive configured.
#[derive(Clone)]
pub(crate) struct AuthoritativeArchives {
    pub(crate) wal_registry: Arc<BackendStore>,
    pub(crate) catalog: Option<CatalogArchiveDestination>,
}

/// Immutable WAL archive storage over the explicitly configured object bucket.
pub(crate) struct ObjectArchiveStore {
    store: Arc<dyn MultipartObjectStore>,
}

impl ObjectArchiveStore {
    /// Binds archive operations to one explicitly authorized backend bucket.
    pub(crate) fn new(store: Arc<dyn MultipartObjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ImmutableSegmentStore for ObjectArchiveStore {
    async fn upload(&self, object: &ArchiveObject, bytes: Bytes) -> Result<(), ArchiveError> {
        let path = object_store::path::Path::from(object.key.as_str());
        let options = object_store::PutOptions {
            mode: object_store::PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&path, object_store::PutPayload::from(bytes), options)
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(error) => Err(ArchiveError::ObjectStore(error.to_string())),
        }
    }

    async fn verify(&self, object: &ArchiveObject) -> Result<(), ArchiveError> {
        let bytes = self.read(object).await?;
        object.verify_bytes(&bytes)
    }

    async fn read(&self, object: &ArchiveObject) -> Result<Bytes, ArchiveError> {
        let path = object_store::path::Path::from(object.key.as_str());
        self.store
            .get(&path)
            .await
            .map_err(|error| ArchiveError::ObjectStore(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| ArchiveError::ObjectStore(error.to_string()))
    }
}

/// Routes archive reads and checkpoints through the current group leader.
struct RoutedArchiveTimeline {
    consensus: Arc<ConsensusPlane>,
    group: String,
    holders: Vec<u64>,
}

#[async_trait]
impl ArchiveTimeline for RoutedArchiveTimeline {
    async fn read_committed_wal(
        &self,
        from: u64,
        to: u64,
        minimum_index: u64,
    ) -> Result<Bytes, ArchiveError> {
        match self
            .consensus
            .submit_archive(
                &self.group,
                GroupRequest::ReadWal {
                    from,
                    to,
                    minimum_index,
                },
            )
            .await
            .map_err(|error| ArchiveError::ObjectStore(error.to_string()))?
        {
            GroupResponse::Bytes(bytes) => Ok(Bytes::from(bytes)),
            _ => Err(ArchiveError::ObjectStore(
                "archive read returned the wrong response".to_owned(),
            )),
        }
    }

    async fn commit_archive_checkpoint(
        &self,
        request: u128,
        end_lsn: u64,
        object: &ArchiveObject,
    ) -> Result<(), ArchiveError> {
        match self
            .consensus
            .submit_archive(
                &self.group,
                GroupRequest::ArchiveCheckpoint {
                    request_id: request,
                    end_lsn,
                    object: object.key.clone(),
                    holders: self.holders.clone(),
                },
            )
            .await
            .map_err(|error| ArchiveError::ObjectStore(error.to_string()))?
        {
            GroupResponse::Applied(_) => Ok(()),
            _ => Err(ArchiveError::ObjectStore(
                "archive checkpoint returned the wrong response".to_owned(),
            )),
        }
    }

    async fn release_through(&self, end_lsn: u64) -> Result<SegmentRelease, ArchiveError> {
        match self
            .consensus
            .submit_archive(&self.group, GroupRequest::WalState { minimum_index: 0 })
            .await
            .map_err(|error| ArchiveError::ObjectStore(error.to_string()))?
        {
            GroupResponse::WalState(state) if state.checkpointed_end() >= end_lsn => {
                Ok(SegmentRelease::checkpointed(end_lsn))
            }
            _ => Err(ArchiveError::ObjectStore(
                "WAL release is not checkpointed".to_owned(),
            )),
        }
    }
}

/// Validates an explicit WAL erasure geometry against the complete cache ring.
pub(crate) fn configured_geometry(
    nodes: usize,
    k: usize,
    m: usize,
    w: usize,
) -> Result<AppendGeometry, String> {
    let geometry = AppendGeometry::new(k, m, w).map_err(|error| error.to_string())?;
    if geometry.total() != nodes {
        return Err(format!(
            "safekeeper EC k={k}, m={m} creates {} fragments for {nodes} cache nodes",
            geometry.total()
        ));
    }
    Ok(geometry)
}

/// Reads one mandatory positive-or-zero integer from the process environment.
fn geometry_env(name: &str) -> Result<usize, std::io::Error> {
    let raw =
        std::env::var(name).map_err(|_| std::io::Error::other(format!("{name} is required")))?;
    raw.parse::<usize>()
        .map_err(|error| std::io::Error::other(format!("invalid {name}={raw}: {error}")))
}

/// Loads and validates the mandatory process-level WAL EC configuration.
pub(crate) fn geometry_from_env(nodes: usize) -> Result<AppendGeometry, std::io::Error> {
    configured_geometry(
        nodes,
        geometry_env("VERGLAS_SAFEKEEPER_EC_K")?,
        geometry_env("VERGLAS_SAFEKEEPER_EC_M")?,
        geometry_env("VERGLAS_SAFEKEEPER_EC_W")?,
    )
    .map_err(std::io::Error::other)
}

/// Serves the consensus-owned Neon WAL protocol until its listener fails.
pub async fn serve(
    ring: Arc<RingPlane>,
    layout: AppendGeometry,
    consensus: Arc<ConsensusPlane>,
    archives: AuthoritativeArchives,
    _activity: ActivityTracker,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::var("VERGLAS_SAFEKEEPER_ADDR")
        .unwrap_or_else(|_| DEFAULT_SAFEKEEPER_ADDR.to_owned())
        .parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let voters = ring.consensus_voters();
    let state = WalIngress {
        consensus,
        coded_holders: voters.iter().copied().take(layout.w).collect(),
        majority_holders: voters.iter().copied().take(voters.len() / 2 + 1).collect(),
        wal_registry: archives.wal_registry,
        archive_inflight: Arc::new(Mutex::new(BTreeSet::new())),
    };
    let app = Router::new()
        .route("/wal/v1/{tenant}/{timeline}", post(wal_request))
        .route("/catalog/v1/{tenant}/{warehouse}", post(catalog_request))
        .layer(DefaultBodyLimit::max(INGRESS_BODY_LIMIT_BYTES))
        .with_state(state);
    eprintln!(
        "verglas-cache-node {VERSION} Verglas Neon WAL ingress listening on http://{} (EC k={}, m={}, coded threshold={})",
        listener.local_addr()?,
        layout.k,
        layout.m,
        layout.w,
    );
    // Consensus rounds are latency-critical small writes: Nagle off.
    let listener = listener.tap_io(|io| {
        let _ = io.set_nodelay(true);
    });
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Waits for process termination before closing the public WAL ingress.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Routes catalog work through its tenant root before the warehouse group.
async fn catalog_request(
    State(state): State<WalIngress>,
    Path((tenant, warehouse)): Path<(String, String)>,
    Json(request): Json<ManagedCatalogRequest>,
) -> Result<Json<ManagedCatalogResponse>, (StatusCode, String)> {
    execute_catalog_request(&state, &tenant, &warehouse, request)
        .await
        .map(Json)
}

/// Serves catalog requests from inside the ring-node process.
///
/// The cloud topology runs one stateless catalog beside every ring node. Over
/// HTTP that catalog would be issuing requests to a consensus plane living in
/// its own address space; this binds it directly instead. It routes through
/// [`execute_catalog_request`], the same function the peer HTTP route uses,
/// so embedded and remote catalogs cannot drift in behaviour.
pub(crate) struct LocalCatalogTransport {
    ingress: WalIngress,
    tenant: String,
    warehouse: String,
}

impl LocalCatalogTransport {
    /// Binds one warehouse route to this node's own consensus plane.
    pub(crate) fn new(
        consensus: Arc<ConsensusPlane>,
        ring: &RingPlane,
        layout: AppendGeometry,
        wal_registry: Arc<BackendStore>,
        tenant: impl Into<String>,
        warehouse: impl Into<String>,
    ) -> Self {
        let voters = ring.consensus_voters();
        Self {
            ingress: WalIngress {
                consensus,
                coded_holders: voters.iter().copied().take(layout.w).collect(),
                majority_holders: voters.iter().copied().take(voters.len() / 2 + 1).collect(),
                wal_registry,
                archive_inflight: Arc::new(Mutex::new(BTreeSet::new())),
            },
            tenant: tenant.into(),
            warehouse: warehouse.into(),
        }
    }
}

#[async_trait]
impl verglas_catalog::ManagedCatalogTransport for LocalCatalogTransport {
    /// The single warehouse this embedded catalog serves.
    fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Executes in-process, preserving the HTTP route's conflict semantics.
    ///
    /// A catalog conflict stays final rather than becoming a retryable
    /// transport failure; anything else keeps its status so the caller's
    /// failover logic reads identically to the networked path.
    async fn execute(
        &self,
        request: &ManagedCatalogRequest,
    ) -> Result<ManagedCatalogResponse, verglas_catalog::ManagedCatalogError> {
        execute_catalog_request(
            &self.ingress,
            &self.tenant,
            &self.warehouse,
            request.clone(),
        )
        .await
        .map_err(|(status, detail)| {
            if status == StatusCode::CONFLICT {
                verglas_catalog::ManagedCatalogError::Conflict
            } else {
                // The typed error carries only a status, so log the reason
                // rather than discard it: a bare 503 says nothing about which
                // group failed or why.
                tracing::warn!(
                    target: "verglas_cache_node::catalog",
                    status = %status, detail = %detail,
                    tenant = %self.tenant, warehouse = %self.warehouse,
                    "embedded catalog request failed"
                );
                verglas_catalog::ManagedCatalogError::Rejected(status.as_u16())
            }
        })
    }
}

/// Executes one catalog request against this node's consensus plane.
///
/// Shared by the peer HTTP route above and by the in-process transport an
/// embedded catalog uses, so a catalog co-located with a ring node runs
/// exactly the same code as one reaching it over the network — including the
/// tenant-root warehouse registration and the same error classification.
async fn execute_catalog_request(
    state: &WalIngress,
    tenant: &str,
    warehouse: &str,
    request: ManagedCatalogRequest,
) -> Result<ManagedCatalogResponse, (StatusCode, String)> {
    let command = match request {
        ManagedCatalogRequest::Commit { request_id, batch } => GroupRequest::CommitCatalog {
            request_id,
            batch,
            holders: state.majority_holders.clone(),
        },
        ManagedCatalogRequest::Table { table } => GroupRequest::CatalogTable { table },
        ManagedCatalogRequest::Namespaces => GroupRequest::CatalogNamespaces,
        ManagedCatalogRequest::Tables => GroupRequest::CatalogTables,
        ManagedCatalogRequest::Record { entity, id } => GroupRequest::CatalogRecord { entity, id },
        ManagedCatalogRequest::Records { entity } => GroupRequest::CatalogRecords { entity },
    };
    if tenant.is_empty() || warehouse.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "tenant and warehouse are required".to_owned(),
        ));
    }
    let root = format!("tenant/{tenant}");
    let warehouse_group = format!("warehouse/{tenant}/{warehouse}");
    let digest = Sha256::digest(format!("{tenant}\0{warehouse}").as_bytes());
    let request_id = u128::from_be_bytes(digest[..16].try_into().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "warehouse identity is malformed".to_owned(),
        )
    })?);
    let root_response = state
        .consensus
        .submit(
            &root,
            GroupRequest::RegisterWarehouse {
                request_id,
                warehouse: warehouse.to_owned(),
                group: warehouse_group.clone(),
                holders: state.majority_holders.clone(),
            },
        )
        .await
        .map_err(catalog_submission_error)?;
    if root_response != GroupResponse::WarehouseGroup(Some(warehouse_group.clone())) {
        return Err((
            StatusCode::CONFLICT,
            "tenant root rejected warehouse route".to_owned(),
        ));
    }
    let response = state
        .consensus
        .submit(&warehouse_group, command)
        .await
        .map_err(catalog_submission_error)?;
    let response = match response {
        GroupResponse::Applied(response) => ManagedCatalogResponse::Applied(response),
        GroupResponse::CatalogTable(table) => ManagedCatalogResponse::Table(table),
        GroupResponse::CatalogNamespaces(namespaces) => {
            ManagedCatalogResponse::Namespaces(namespaces)
        }
        GroupResponse::CatalogTables(tables) => ManagedCatalogResponse::Tables(tables),
        GroupResponse::CatalogRecord(record) => ManagedCatalogResponse::Record(record),
        GroupResponse::CatalogRecords(records) => ManagedCatalogResponse::Records(records),
        GroupResponse::CatalogExport(_) | GroupResponse::WalArchiveBinding(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "wrong warehouse response".to_owned(),
            ));
        }
        GroupResponse::WarehouseGroup(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "wrong warehouse response".to_owned(),
            ));
        }
        GroupResponse::Bytes(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "wrong group response".to_owned(),
            ));
        }
        GroupResponse::WalState(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "wrong group response".to_owned(),
            ));
        }
        GroupResponse::ArchivedWalReadPlan(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "wrong group response".to_owned(),
            ));
        }
    };
    Ok(response)
}

/// Preserves availability failover while making authoritative catalog conflicts final HTTP 409 responses.
fn catalog_submission_error(
    error: Box<dyn std::error::Error + Send + Sync>,
) -> (StatusCode, String) {
    let status = match error.downcast_ref::<GroupError>() {
        Some(GroupError::CatalogConflict | GroupError::ConflictingRetry) => StatusCode::CONFLICT,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, error.to_string())
}

/// Converts one transport request into the universal timeline-group command.
async fn wal_request(
    State(state): State<WalIngress>,
    Path((tenant, timeline)): Path<(String, String)>,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let request =
        WalRequest::decode(&body).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let group = format!("timeline/{tenant}/{timeline}");
    if let WalRequest::ReadWal {
        from,
        to,
        minimum_index,
    } = request
    {
        return composed_wal_read(&state, &group, from, to, minimum_index).await;
    }
    if let WalRequest::Append {
        request_id,
        writer_epoch,
        start_lsn,
        payload,
    } = request.clone()
    {
        let coded = GroupRequest::AppendWal {
            request_id,
            writer_epoch,
            start_lsn,
            payload: payload.clone(),
            holders: state.coded_holders.clone(),
            mode: ReplicationMode::Coded,
        };
        let response = match state.consensus.submit(&group, coded).await {
            Ok(response) => response,
            Err(error) => {
                let coded_unavailable = matches!(
                    error.downcast_ref::<verglas_consensus::GroupError>(),
                    Some(verglas_consensus::GroupError::Payload(
                        verglas_consensus::PayloadError::InsufficientDurability { .. }
                            | verglas_consensus::PayloadError::Transport(_)
                    ))
                );
                if !coded_unavailable {
                    return Err((StatusCode::CONFLICT, error.to_string()));
                }
                state
                    .consensus
                    .submit(
                        &group,
                        GroupRequest::AppendWal {
                            request_id,
                            writer_epoch,
                            start_lsn,
                            payload,
                            holders: state.majority_holders.clone(),
                            mode: ReplicationMode::Complete,
                        },
                    )
                    .await
                    .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?
            }
        };
        if let GroupResponse::Applied(applied) = &response
            && applied.wal_end.is_some()
        {
            spawn_archive(state.clone(), group, applied.index);
        }
        return response_binary(response);
    }
    let holders = state.majority_holders.clone();
    let command = match request {
        WalRequest::OpenTimeline {
            request_id,
            start_lsn,
        } => GroupRequest::OpenTimeline {
            request_id,
            start_lsn,
            holders,
        },
        WalRequest::AcquireWriter { request_id, writer } => GroupRequest::AcquireWriter {
            request_id,
            writer,
            holders,
        },
        WalRequest::Append { .. } => unreachable!("append handled before command conversion"),
        WalRequest::ReadWal {
            from,
            to,
            minimum_index,
        } => GroupRequest::ReadWal {
            from,
            to,
            minimum_index,
        },
        WalRequest::Status { minimum_index } => GroupRequest::WalState { minimum_index },
        WalRequest::ReleaseWriter {
            request_id,
            writer_epoch,
        } => GroupRequest::ReleaseWriter {
            request_id,
            writer_epoch,
            holders,
        },
    };
    let response = state
        .consensus
        .submit(&group, command)
        .await
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?;
    match &response {
        GroupResponse::Applied(applied) => {
            if applied.wal_end.is_some() {
                spawn_archive(state.clone(), group, applied.index);
            }
        }
        GroupResponse::WalState(status) => {
            spawn_archive(state.clone(), group, status.applied_index())
        }
        _ => {}
    }
    response_binary(response)
}

/// Serves a baseline retained read or composes its archived prefix and retained tail.
async fn composed_wal_read(
    state: &WalIngress,
    group: &str,
    from: u64,
    to: u64,
    minimum_index: u64,
) -> Result<Response, (StatusCode, String)> {
    let response = state
        .consensus
        .submit(
            group,
            GroupRequest::ReadWal {
                from,
                to,
                minimum_index,
            },
        )
        .await
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?;
    let plan = match response {
        GroupResponse::Bytes(bytes) => return response_binary(GroupResponse::Bytes(bytes)),
        GroupResponse::ArchivedWalReadPlan(plan) => plan,
        _ => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "WAL read returned the wrong response".to_owned(),
            ));
        }
    };
    let archived_end = to.min(plan.checkpointed_end());
    let mut output = Vec::with_capacity((to - from) as usize);
    if from < archived_end {
        let destination = resolve_wal_archive(&state.consensus, &state.wal_registry, group)
            .await
            .map_err(|error| (StatusCode::CONFLICT, error))?;
        let prefix = read_archived_wal_prefix(
            destination.0.store.as_ref(),
            plan.archived_segments(),
            from,
            archived_end,
        )
        .await
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?;
        output.extend_from_slice(&prefix);
    }
    output.extend_from_slice(plan.retained_tail());
    response_binary(GroupResponse::Bytes(output))
}

/// Archives every newly complete PostgreSQL segment without delaying append acknowledgement.
fn spawn_archive(state: WalIngress, group: String, minimum_index: u64) {
    let inserted = state
        .archive_inflight
        .lock()
        .map(|mut groups| groups.insert(group.clone()))
        .unwrap_or(false);
    if !inserted {
        return;
    }
    tokio::spawn(async move {
        struct InflightGuard {
            group: String,
            groups: Arc<Mutex<BTreeSet<String>>>,
        }
        impl Drop for InflightGuard {
            fn drop(&mut self) {
                if let Ok(mut groups) = self.groups.lock() {
                    groups.remove(&self.group);
                }
            }
        }
        let _inflight = InflightGuard {
            group: group.clone(),
            groups: Arc::clone(&state.archive_inflight),
        };
        let destination =
            match resolve_wal_archive(&state.consensus, &state.wal_registry, &group).await {
                Ok(destination) => destination,
                Err(error) => {
                    tracing::error!(%error, %group, "resolve committed WAL archive binding");
                    return;
                }
            };
        let timeline = Arc::new(RoutedArchiveTimeline {
            consensus: Arc::clone(&state.consensus),
            group: group.clone(),
            holders: state.majority_holders.clone(),
        });
        let archiver = match SegmentArchiver::new(
            timeline,
            Arc::clone(&destination.0.store),
            format!("{}/{}", destination.0.prefix, group),
        ) {
            Ok(archiver) => archiver,
            Err(error) => {
                tracing::error!(%error, %group, "configure WAL segment archiver");
                return;
            }
        };
        let mut fence = minimum_index;
        let mut backoff = std::time::Duration::from_millis(250);
        loop {
            let status = match state
                .consensus
                .submit_archive(
                    &group,
                    GroupRequest::WalState {
                        minimum_index: fence,
                    },
                )
                .await
            {
                Ok(GroupResponse::WalState(status)) => status,
                Ok(_) => return,
                Err(error) => {
                    tracing::warn!(%error, %group, "read archive boundary");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                    continue;
                }
            };
            let start = status.checkpointed_end();
            let end = if start.is_multiple_of(WAL_SEGMENT_BYTES) {
                start.saturating_add(WAL_SEGMENT_BYTES)
            } else {
                start.saturating_add(WAL_SEGMENT_BYTES - start % WAL_SEGMENT_BYTES)
            };
            if end > status.committed_end() || end <= start {
                return;
            }
            let digest = Sha256::digest(format!("{group}\0{start}\0{end}").as_bytes());
            let request = match digest[..16].try_into() {
                Ok(bytes) => u128::from_be_bytes(bytes),
                Err(_) => return,
            };
            if let Err(error) = archiver
                .archive(request, start, end, status.applied_index())
                .await
            {
                tracing::warn!(%error, %group, start_lsn = start, end_lsn = end, "archive committed WAL segment");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                continue;
            }
            fence = status.applied_index();
            backoff = std::time::Duration::from_millis(250);
        }
    });
}

/// Resolves a timeline's consensus-committed archive bucket through the only
/// authorized backend binding. An unbound timeline has no archive fallback.
async fn resolve_wal_archive(
    consensus: &Arc<ConsensusPlane>,
    registry: &Arc<BackendStore>,
    group: &str,
) -> Result<WalArchiveDestination, String> {
    let bucket = match consensus
        .submit(group, GroupRequest::WalArchiveBinding)
        .await
        .map_err(|error| error.to_string())?
    {
        GroupResponse::WalArchiveBinding(Some(bucket)) => bucket,
        GroupResponse::WalArchiveBinding(None) => {
            return Err("timeline WAL archive bucket is not consensus-bound".to_owned());
        }
        _ => return Err("timeline WAL archive binding returned wrong response".to_owned()),
    };
    let store = registry
        .store_for(crate::serve::DEFAULT_STORAGE_BINDING_ID, &bucket)
        .map_err(|error| error.to_string())?;
    let store: Arc<dyn ImmutableSegmentStore> = Arc::new(ObjectArchiveStore::new(store));
    Ok(WalArchiveDestination::new(store, "_verglas/wal".to_owned()))
}

/// Flushes the final committed tail for one timeline during an authoritative stop.
///
/// This runs only after the admin lifecycle fence rejected new mutations. It
/// uses the normal archive checkpoint path, so successful return proves every
/// byte through the committed tail has a verified immutable representation.
pub(crate) async fn flush_timeline_tail(
    consensus: Arc<ConsensusPlane>,
    destination: &WalArchiveDestination,
    holders: Vec<u64>,
    group: String,
) -> Result<(), ArchiveError> {
    let status = match consensus
        .submit(&group, GroupRequest::WalState { minimum_index: 0 })
        .await
        .map_err(|error| ArchiveError::ObjectStore(error.to_string()))?
    {
        GroupResponse::WalState(status) => status,
        _ => {
            return Err(ArchiveError::ObjectStore(
                "timeline status returned wrong response".to_owned(),
            ));
        }
    };
    if status.checkpointed_end() == status.committed_end() {
        return Ok(());
    }
    let digest = Sha256::digest(format!("stop\0{group}\0{}", status.committed_end()).as_bytes());
    let request = u128::from_be_bytes(
        digest[..16]
            .try_into()
            .map_err(|_| ArchiveError::ObjectStore("archive identity malformed".to_owned()))?,
    );
    let timeline = Arc::new(RoutedArchiveTimeline {
        consensus,
        group: group.clone(),
        holders,
    });
    SegmentArchiver::new(
        timeline,
        Arc::clone(&destination.0.store),
        format!("{}/{group}", destination.0.prefix),
    )?
    .archive_tail(
        request,
        status.checkpointed_end(),
        status.committed_end(),
        status.applied_index(),
    )
    .await
    .map(|_| ())
}

/// Drains every locally hosted timeline tail after admission is fenced.
///
/// Warehouse exports are intentionally reported as unavailable until their
/// committed checkpoint command is installed; this keeps lifecycle stop closed.
pub(crate) async fn drain_authoritative_groups(
    consensus: Arc<ConsensusPlane>,
    archives: AuthoritativeArchives,
    holders: Vec<u64>,
) -> Result<(), String> {
    for group in consensus.hosted_groups().await {
        if group.starts_with("timeline/") {
            flush_timeline_tail(
                Arc::clone(&consensus),
                &resolve_wal_archive(&consensus, &archives.wal_registry, &group).await?,
                holders.clone(),
                group,
            )
            .await
            .map_err(|error| error.to_string())?;
        } else if group.starts_with("warehouse/") {
            match consensus
                .submit(&group, GroupRequest::ExportCatalog)
                .await
                .map_err(|error| error.to_string())?
            {
                GroupResponse::CatalogExport(export) => {
                    // A warehouse group cannot exist without the catalog, which
                    // is what configures this destination.
                    let destination = archives.catalog.as_ref().ok_or_else(|| {
                        format!("group {group} needs a catalog archive, which is not configured")
                    })?;
                    let (request, object) = persist_catalog_export(destination, &group, &export)
                        .await
                        .map_err(|error| error.to_string())?;
                    match consensus
                        .submit(
                            &group,
                            GroupRequest::CatalogCheckpoint {
                                request_id: request,
                                applied_index: export.applied_index,
                                object,
                                holders: holders.clone(),
                            },
                        )
                        .await
                        .map_err(|error| error.to_string())?
                    {
                        GroupResponse::Applied(_) => {}
                        _ => return Err("catalog checkpoint returned wrong response".to_owned()),
                    }
                }
                _ => return Err("catalog export returned wrong response".to_owned()),
            }
        }
    }
    Ok(())
}

/// Uploads and verifies one catalog export in the catalog-only durability domain.
///
/// The returned request identity and object key are passed to CRaft only after
/// immutable storage verifies the exact export bytes.
async fn persist_catalog_export(
    destination: &CatalogArchiveDestination,
    group: &str,
    export: &CatalogExport,
) -> Result<(u128, String), ArchiveError> {
    let hash: [u8; 32] = Sha256::digest(&export.bytes).into();
    let object = ArchiveObject {
        key: format!(
            "{}/{group}/catalog-{:016x}-{}",
            destination.0.prefix,
            export.applied_index,
            hex::encode(hash)
        ),
        hash,
        length: export.bytes.len() as u64,
    };
    destination
        .0
        .store
        .upload(&object, Bytes::copy_from_slice(&export.bytes))
        .await?;
    destination.0.store.verify(&object).await?;
    let request = u128::from_be_bytes(hash[..16].try_into().map_err(|_| {
        ArchiveError::ObjectStore("catalog checkpoint identity malformed".to_owned())
    })?);
    Ok((request, object.key))
}

/// Converts the universal engine response into the Neon WAL surface.
fn response_binary(response: GroupResponse) -> Result<Response, (StatusCode, String)> {
    let response = match response {
        GroupResponse::Applied(response) => WalResponse::Applied {
            index: response.index,
            writer_epoch: response.writer_epoch,
            wal_end: response.wal_end,
            archive_lsn: response.archive_lsn,
        },
        GroupResponse::Bytes(payload) => WalResponse::WalBytes { payload },
        GroupResponse::WalState(state) => WalResponse::Status {
            index: state.applied_index(),
            wal_end: state.committed_end(),
            archive_lsn: state.checkpointed_end(),
        },
        GroupResponse::CatalogTable(_)
        | GroupResponse::CatalogNamespaces(_)
        | GroupResponse::CatalogTables(_)
        | GroupResponse::CatalogRecord(_)
        | GroupResponse::CatalogRecords(_)
        | GroupResponse::CatalogExport(_)
        | GroupResponse::WalArchiveBinding(_)
        | GroupResponse::WarehouseGroup(_)
        | GroupResponse::ArchivedWalReadPlan(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "wrong group response".to_owned(),
            ));
        }
    };
    let body = response
        .encode()
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let mut response = Response::new(axum::body::Body::from(body));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A recording immutable store used to prove catalog exports choose only
    /// the catalog destination supplied to the persistence helper.
    #[derive(Default)]
    struct RecordingStore {
        objects: Mutex<Vec<(String, Vec<u8>)>>,
    }

    #[async_trait]
    impl ImmutableSegmentStore for RecordingStore {
        /// Records the immutable bytes a caller attempted to archive.
        async fn upload(&self, object: &ArchiveObject, bytes: Bytes) -> Result<(), ArchiveError> {
            self.objects
                .lock()
                .map_err(|_| ArchiveError::ObjectStore("recording store lock poisoned".to_owned()))?
                .push((object.key.clone(), bytes.to_vec()));
            Ok(())
        }

        /// Accepts the object because upload retained it in the recording fixture.
        async fn verify(&self, _object: &ArchiveObject) -> Result<(), ArchiveError> {
            Ok(())
        }

        /// The routing test never reads an archived object.
        async fn read(&self, _object: &ArchiveObject) -> Result<Bytes, ArchiveError> {
            Err(ArchiveError::ObjectStore(
                "recording store does not serve reads".to_owned(),
            ))
        }
    }

    /// Four-node geometry is explicit and must consume the configured ring.
    #[test]
    fn configured_geometry_accepts_four_node_ec() {
        assert_eq!(
            configured_geometry(4, 2, 2, 3).expect("valid four-node geometry"),
            AppendGeometry { k: 2, m: 2, w: 3 }
        );
    }

    /// A geometry that leaves nodes unused is rejected.
    #[test]
    fn configured_geometry_rejects_ring_mismatch() {
        let error = configured_geometry(4, 2, 1, 3).expect_err("three fragments for four nodes");
        assert!(error.contains("4 cache nodes"), "{error}");
    }

    /// Catalog checkpoints use their dedicated destination and never the WAL destination.
    #[tokio::test]
    async fn catalog_checkpoint_persists_only_to_catalog_archive() {
        let wal_store = Arc::new(RecordingStore::default());
        let catalog_store = Arc::new(RecordingStore::default());
        let catalog =
            CatalogArchiveDestination::new(catalog_store.clone(), "catalog-prefix".to_owned());
        let export = CatalogExport {
            applied_index: 42,
            bytes: b"canonical-catalog-export".to_vec(),
        };

        let (_request, object) = persist_catalog_export(&catalog, "warehouse/acme/main", &export)
            .await
            .expect("catalog export persists");

        assert!(object.starts_with("catalog-prefix/warehouse/acme/main/catalog-"));
        assert_eq!(
            catalog_store
                .objects
                .lock()
                .expect("catalog store lock")
                .as_slice(),
            [(object, b"canonical-catalog-export".to_vec())]
        );
        assert!(
            wal_store.objects.lock().expect("wal store lock").is_empty(),
            "the WAL archive must not receive catalog checkpoint bytes"
        );
    }
}
