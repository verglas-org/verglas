//! Neon WAL ingress over the cache fleet's universal coded Multi-Raft groups.

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::Response,
    routing::post,
};
use object_store::ObjectStoreExt;
use sha2::{Digest, Sha256};
use verglas_backend::MultipartObjectStore;
use verglas_catalog::{ManagedCatalogRequest, ManagedCatalogResponse};
use verglas_consensus::{GroupError, GroupRequest, GroupResponse, ReplicationMode};
use verglas_core::activity::ActivityTracker;
use verglas_safekeeper::{
    AppendGeometry, ArchiveError, ArchiveObject, ArchiveTimeline, ImmutableSegmentStore,
    SegmentArchiver, SegmentRelease, WalRequest, WalResponse,
};

use crate::{VERSION, consensus::ConsensusPlane, ring::RingPlane};

/// Default HTTP/2 address for the Verglas Neon WAL protocol.
const DEFAULT_SAFEKEEPER_ADDR: &str = "0.0.0.0:5454";
const WAL_SEGMENT_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone)]
struct WalIngress {
    consensus: Arc<ConsensusPlane>,
    coded_holders: Vec<u64>,
    majority_holders: Vec<u64>,
    archive_store: Arc<dyn ImmutableSegmentStore>,
    archive_prefix: String,
    archive_inflight: Arc<Mutex<BTreeSet<String>>>,
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
        let path = object_store::path::Path::from(object.key.as_str());
        let bytes = self
            .store
            .get(&path)
            .await
            .map_err(|error| ArchiveError::ObjectStore(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| ArchiveError::ObjectStore(error.to_string()))?;
        if bytes.len() as u64 != object.length
            || <[u8; 32]>::from(Sha256::digest(&bytes)) != object.hash
        {
            return Err(ArchiveError::ObjectStore(
                "visible archive object has the wrong identity".to_owned(),
            ));
        }
        Ok(())
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
            .submit(
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
            .submit(
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
            .submit(&self.group, GroupRequest::WalState { minimum_index: 0 })
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
    archive_store: Arc<dyn ImmutableSegmentStore>,
    archive_prefix: String,
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
        archive_store,
        archive_prefix,
        archive_inflight: Arc::new(Mutex::new(BTreeSet::new())),
    };
    let app = Router::new()
        .route("/wal/v1/{tenant}/{timeline}", post(wal_request))
        .route("/catalog/v1/{tenant}/{warehouse}", post(catalog_request))
        .with_state(state);
    eprintln!(
        "verglas-cache-node {VERSION} Verglas Neon WAL ingress listening on http://{} (EC k={}, m={}, coded threshold={})",
        listener.local_addr()?,
        layout.k,
        layout.m,
        layout.w,
    );
    axum::serve(listener, app).await?;
    Ok(())
}

/// Routes catalog work through its tenant root before the warehouse group.
async fn catalog_request(
    State(state): State<WalIngress>,
    Path((tenant, warehouse)): Path<(String, String)>,
    Json(request): Json<ManagedCatalogRequest>,
) -> Result<Json<ManagedCatalogResponse>, (StatusCode, String)> {
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
                warehouse: warehouse.clone(),
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
        GroupResponse::CatalogExport(_) => {
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
    };
    Ok(Json(response))
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
        let timeline = Arc::new(RoutedArchiveTimeline {
            consensus: Arc::clone(&state.consensus),
            group: group.clone(),
            holders: state.majority_holders.clone(),
        });
        let archiver = match SegmentArchiver::new(
            timeline,
            Arc::clone(&state.archive_store),
            format!("{}/{}", state.archive_prefix, group),
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
                .submit(
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

/// Flushes the final committed tail for one timeline during an authoritative stop.
///
/// This runs only after the admin lifecycle fence rejected new mutations. It
/// uses the normal archive checkpoint path, so successful return proves every
/// byte through the committed tail has a verified immutable representation.
pub(crate) async fn flush_timeline_tail(
    consensus: Arc<ConsensusPlane>,
    archive_store: Arc<dyn ImmutableSegmentStore>,
    archive_prefix: String,
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
    SegmentArchiver::new(timeline, archive_store, format!("{archive_prefix}/{group}"))?
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
    archive_store: Arc<dyn ImmutableSegmentStore>,
    archive_prefix: String,
    holders: Vec<u64>,
) -> Result<(), String> {
    for group in consensus.hosted_groups().await {
        if group.starts_with("timeline/") {
            flush_timeline_tail(
                Arc::clone(&consensus),
                Arc::clone(&archive_store),
                archive_prefix.clone(),
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
                    let hash: [u8; 32] = Sha256::digest(&export.bytes).into();
                    let object = ArchiveObject {
                        key: format!(
                            "{archive_prefix}/{group}/catalog-{:016x}-{}",
                            export.applied_index,
                            hex::encode(hash)
                        ),
                        hash,
                        length: export.bytes.len() as u64,
                    };
                    archive_store
                        .upload(&object, Bytes::from(export.bytes))
                        .await
                        .map_err(|error| error.to_string())?;
                    archive_store
                        .verify(&object)
                        .await
                        .map_err(|error| error.to_string())?;
                    let request = u128::from_be_bytes(
                        hash[..16]
                            .try_into()
                            .map_err(|_| "catalog checkpoint identity malformed".to_owned())?,
                    );
                    match consensus
                        .submit(
                            &group,
                            GroupRequest::CatalogCheckpoint {
                                request_id: request,
                                applied_index: export.applied_index,
                                object: object.key,
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
        | GroupResponse::WarehouseGroup(_) => {
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
}
