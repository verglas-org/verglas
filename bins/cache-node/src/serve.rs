//! The cache serving path: build the origin backend, probe it, build the hybrid
//! cache engine over a single-node rendezvous ring, run the disk-full guardrail
//! poll, and serve the SigV4 S3 endpoint with read-through and ring write-back.
//!
//! This is the CACHE-relevant subset of `bins/cache-node/src/main.rs::serve`,
//! reproduced without gossip, table lifecycle, or any admin surface beyond
//! health/stats/metrics. Ring members provide the shared durability plane.
//!
//! ## Read ownership and write durability
//!
//! - **Ring**: a [`RendezvousRing::single`] node owns every key, so the read
//!   path never consults a peer. This is byte-identical ownership to a verglas-cache-node
//!   server started without `[cluster]`, and it drops the whole `verglas-cluster`
//!   dependency (gossip, peer RPC, fragment store).
//! - **Peers**: [`NoopPeerFetch`] — there are no peers to fetch from.
//! - **Object write-back**: a configured fragment ring stages every S3 PUT as
//!   erasure-coded, fsynced fragments before acknowledgement. Dirty fragments
//!   remain outside normal cache eviction while origin propagation runs.
//! - **Block-device write-back**: the block tier (#382) is the exception that
//!   reaches the ring. When `VERGLAS_RING_PEERS` names a ring, a device FLUSH is
//!   erasure-coded across the boxes and acked on a quorum (draining to R2 in the
//!   background); see [`crate::ring`]. The embedded safekeeper shares that same
//!   fragment store and peer RPC. With no ring configured, object PUT and block
//!   FLUSH retain the synchronous origin barrier and no safekeeper starts.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sha2::{Digest, Sha256};
use verglas_backend::{BackendStore, BackendStores};
use verglas_block::{ObjectBackend, ObjectStoreBackend};
use verglas_cache::HybridCacheEngine;
use verglas_cluster::peer::PeerClient;
use verglas_core::activity::{ActivityPlane, ActivityTracker};
use verglas_core::admin::{CacheConfigInfo, CountersInfo, StatsInfo, WritebackStatsInfo};
use verglas_core::config::{CatalogConsistency, Config};
use verglas_core::metrics::NodeMetrics;
use verglas_core::node::NodeId;
use verglas_core::ring::RendezvousRing;
use verglas_s3::{PassthroughList, PassthroughRead, PassthroughWrite};
use verglas_writeback::{
    JournalStore, PrefixRule, WriteCoordinator, WritebackMetrics, WritebackPolicy, WritebackTier,
};

use crate::VERSION;
use crate::admin;

/// The node id the single-node cache uses. Matches verglas-cache-node's cluster-of-one id
/// so ownership is byte-identical to a pre-cluster server.
const SINGLE_NODE_ID: &str = "single";
/// Storage binding for the built-in managed lakehouse database.
pub(crate) const DEFAULT_STORAGE_BINDING_ID: &str = "default-storage";

/// The default NBD listen port for the block-device tier (#382). An attach
/// client connects a kernel NBD client here; the export name selects the
/// device. Overridable with `VERGLAS_BLOCK_ADDR` (same shape as
/// `VERGLAS_S3_ADDR`). Not a config knob — one fixed port, one listener.
///
/// The S3 service is the deliberate exception to "one listener": with the
/// `VERGLAS_S3_TLS_*` env trio set (see [`crate::tls`]), `VERGLAS_S3_ADDR`'s
/// plaintext listener stays up for 6PN-internal callers (lakekeeper,
/// inter-cache peers, gateway probes) *and* a second, TLS-terminated
/// listener serves the identical axum router on `VERGLAS_S3_TLS_ADDR` for
/// public-internet callers — replacing fly-proxy's per-request TLS
/// termination on that hot path. Still not a config knob: both addresses
/// come from env, never from `config.toml`.
const BLOCK_PORT: u16 = 8335;

/// The concrete engine the cache node runs: the hybrid cache over the
/// single-bucket passthrough backend, with a one-member rendezvous ring and the
/// no-op peer fetch. No `verglas-cluster` types appear here.
pub(crate) type CacheEngine = HybridCacheEngine<PassthroughRead, PeerClient, RendezvousRing>;
type WritebackSlot = Arc<std::sync::OnceLock<Arc<WritebackMetrics>>>;

/// Builds the all-object policy used by a tenant cache ring. Neon uploads both
/// immutable layers and mutable publication metadata through ordinary PUTs, so
/// every PUT must cross the same ring durability barrier.
fn object_writeback_policy(layout: verglas_safekeeper::AppendGeometry) -> WritebackPolicy {
    WritebackPolicy::new(vec![PrefixRule {
        prefix: String::new(),
        k: layout.k,
        m: layout.m,
        w: layout.w,
    }])
}

/// Whether the operator has opted out of the backend startup probe via
/// `VERGLAS_DEV_ALLOW_MISSING_ORIGIN`. Truthy values are `1`/`true`
/// (case-insensitive). A dev/test escape hatch — same name and semantics as
/// verglas-cache-node so the two binaries behave identically.
fn dev_allow_missing_origin() -> bool {
    std::env::var("VERGLAS_DEV_ALLOW_MISSING_ORIGIN")
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            value == "1" || value == "true"
        })
        .unwrap_or(false)
}

/// Reserves one quarter of configured origin concurrency for repeated aligned
/// cache tails, leaving the rest for user reads. Copied from verglas-cache-node so the
/// overfetch behaviour matches.
fn background_fill_limit(max_concurrent_requests: usize) -> usize {
    max_concurrent_requests / 4
}

/// Builds the `/admin/stats` source: reads the engine's live counters and DRAM
/// usage and stamps them next to the configured budgets. Write-back counters
/// become available after a ring-backed object writer starts.
fn stats_source(
    config: &Config,
    engine: CacheEngine,
    writeback: WritebackSlot,
) -> admin::StatsSource {
    let cache = CacheConfigInfo {
        dir: config.cache.dir.display().to_string(),
        capacity_bytes: config.cache.capacity_bytes.0,
        dram_bytes: config.cache.dram_bytes.0,
    };
    Arc::new(move || {
        let c = engine.counters().snapshot();
        StatsInfo {
            cache: cache.clone(),
            counters: CountersInfo {
                dram_hits: c.dram_hits,
                dram_misses: c.dram_misses,
                disk_hits: c.disk_hits,
                disk_misses: c.disk_misses,
                peer_hits: c.peer_hits,
                peer_misses: c.peer_misses,
                peer_errors: c.peer_errors,
                peer_served_blocks: c.peer_served_blocks,
                peer_served_bytes: c.peer_served_bytes,
                dram_bytes_served: c.dram_bytes_served,
                disk_bytes_served: c.disk_bytes_served,
                peer_bytes_served: c.peer_bytes_served,
                backend_bytes_served: c.backend_bytes_served,
                backend_fills: c.backend_fills,
                backend_fill_bytes: c.backend_fill_bytes,
                backend_heads: c.backend_heads,
                non_cacheable_passthroughs: c.non_cacheable_passthroughs,
                meta_hits: c.meta_hits,
                meta_misses: c.meta_misses,
                meta_bytes_served: c.meta_bytes_served,
                retired_bytes_pending: c.retired_bytes_pending,
                retired_bytes_reclaimed: c.retired_bytes_reclaimed,
                retired_files_reclaimed: c.retired_files_reclaimed,
            },
            dram_usage_bytes: engine.dram_usage(),
            dram_live_bytes: engine.dram_live_bytes(),
            dram_reclaimable_bytes: engine.dram_reclaimable_bytes(),
            warming: None,
            writeback: writeback.get().map(|metrics| {
                let snapshot = metrics.snapshot();
                WritebackStatsInfo {
                    acked_via_quorum: snapshot.acked_via_quorum,
                    acked_via_write_through: snapshot.acked_via_write_through,
                    mode_transitions: snapshot.mode_transitions,
                    propagated: snapshot.propagated,
                    propagation_failures: snapshot.propagation_failures,
                    fragments_repaired: snapshot.fragments_repaired,
                    fragments_scrubbed: snapshot.fragments_scrubbed,
                    corrupt_fragments_found: snapshot.corrupt_fragments_found,
                }
            }),
        }
    })
}

/// Builds the `GET /metrics` source: renders the Prometheus exposition on each
/// scrape from the shared request registry plus the engine and backend counters
/// read at scrape time. The CACHE-relevant subset of verglas-cache-node's `metrics_source`
/// (no per-table telemetry).
fn metrics_source(
    config: &Config,
    metrics: Arc<NodeMetrics>,
    engine: CacheEngine,
    backend: Arc<BackendStore>,
    writeback: WritebackSlot,
) -> admin::MetricsSource {
    use verglas_core::metrics::{MetricsSnapshot, TierSize, WritebackMetricsSnapshot, render};
    use verglas_core::read::ServedTier;

    let dram_capacity = config.cache.dram_bytes.0;
    let nvme_capacity = config.cache.capacity_bytes.0;
    Arc::new(move || {
        let c = engine.counters().snapshot();
        let cache_hits = c.dram_hits + c.disk_hits + c.peer_hits + c.meta_hits;
        let cache_misses = c.dram_misses + c.disk_misses + c.meta_misses;
        let breaker = backend.aggregate_breaker_snapshot();
        let backend_success = c.backend_fills + c.backend_heads + c.meta_fills;
        let snapshot = MetricsSnapshot {
            bytes_served: vec![
                (ServedTier::Dram, c.dram_bytes_served + c.meta_bytes_served),
                (ServedTier::Nvme, c.disk_bytes_served),
                (ServedTier::Peer, c.peer_bytes_served),
                (ServedTier::Backend, c.backend_bytes_served),
            ],
            cache_hits,
            cache_misses,
            cache_admitted: c.blocks_admitted,
            cache_rejected: c.blocks_rejected,
            cache_tiers: vec![
                (
                    ServedTier::Dram,
                    TierSize {
                        used: engine.dram_usage(),
                        capacity: dram_capacity,
                    },
                ),
                (
                    ServedTier::Nvme,
                    TierSize {
                        used: engine.disk_usage_bytes(),
                        capacity: nvme_capacity,
                    },
                ),
            ],
            fill_inflight: engine.inflight_len(),
            backend_requests: vec![("success", backend_success), ("shed", breaker.rejections)],
            backend_retries: backend.retries_total(),
            breaker_trips: breaker.trips,
            breaker_rejections: breaker.rejections,
            breaker_state: match breaker.state {
                verglas_backend::BreakerState::Closed => "closed",
                verglas_backend::BreakerState::Open => "open",
                verglas_backend::BreakerState::HalfOpen => "half_open",
            },
            writeback: writeback.get().map(|metrics| {
                let snapshot = metrics.snapshot();
                WritebackMetricsSnapshot {
                    fragments_repaired: snapshot.fragments_repaired,
                    fragments_scrubbed: snapshot.fragments_scrubbed,
                    corrupt_fragments_found: snapshot.corrupt_fragments_found,
                }
            }),
        };
        render(&metrics, &snapshot)
    })
}

/// Spawns the disk-full guardrail poll (#96). Once a second it reads the free
/// space under `cache.dir` and the engine's remaining physical growth room and
/// publishes one decision the fill path reads as a plain atomic: pause block
/// admission when the filesystem nears full so a full disk degrades the node to
/// origin fills rather than crashing it (budgets are hard ceilings). The
/// same decision publishes the unused physical budget to the fragment store.
/// Dirty fragments remain protected when the ceiling shrinks. Detached for the
/// process lifetime.
fn spawn_disk_monitor(
    config: &Config,
    caching_paused: Arc<AtomicBool>,
    engine: CacheEngine,
    object_ring: Option<Arc<crate::ring::RingPlane>>,
) -> tokio::task::JoinHandle<()> {
    use verglas_core::disk::{DiskParams, disk_decision, free_bytes};

    let dir = config.cache.dir.clone();
    let capacity = config.cache.capacity_bytes.0;
    // Keep a headroom reserve so admission stops before the disk is truly spent;
    // the gap to `high_water` is the hysteresis band. Floored at 64 MiB so a tiny
    // test budget still leaves a sane reserve. Same shape as verglas-cache-node's poll.
    let low_water = (capacity / 16).max(64 * 1024 * 1024);
    let high_water = low_water.saturating_mul(2);
    let params = DiskParams {
        low_water,
        high_water,
    };
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            ticker.tick().await;
            let was_paused = caching_paused.load(Ordering::Relaxed);
            let free = free_bytes(&dir);
            let room = engine.disk_growth_room_bytes();
            let frag_used = object_ring
                .as_ref()
                .map_or(0, |ring| ring.fragment_used_bytes());
            let state = disk_decision(free, room, frag_used, was_paused, &params);
            if let Some(ring) = &object_ring {
                ring.set_fragment_ceiling(state.fragment_max);
            }
            caching_paused.store(state.caching_paused, Ordering::Relaxed);
            if state.caching_paused != was_paused {
                if state.caching_paused {
                    tracing::warn!(
                        fs_free_bytes = free,
                        foyer_growth_room = room,
                        "cache admission paused: disk headroom below low water"
                    );
                } else {
                    tracing::info!(
                        fs_free_bytes = free,
                        foyer_growth_room = room,
                        "cache admission resumed"
                    );
                }
            }
        }
    })
}

/// Builds the cache engine and runs the admin and S3 listeners together.
///
/// Ordering mirrors verglas-cache-node's serve-gating (#16): the admin listener binds
/// first reporting `starting`, then the backend is probed and the engine
/// recovers its on-disk tiers, then the stats/metrics slots are filled and the
/// health gate flips to `ready`, then the S3 endpoint serves. Either listener
/// failing tears the process down — a half-alive cache is worse than a dead one.
pub async fn run(
    config: &Config,
    credentials: (String, String),
) -> Result<(), Box<dyn std::error::Error>> {
    // One backend store shared by the read and write passthroughs.
    let registry = BackendStore::from_config(DEFAULT_STORAGE_BINDING_ID, &config.backend);
    eprintln!(
        "verglas-cache-node {VERSION} backend resolved: {} (serving bucket set `{}`)",
        registry.describe(),
        registry.bucket_set()
    );

    // Origin reachability does not gate recovery. An acknowledged dirty object
    // may exist only on the ring, so this process must recover and serve it even
    // while the origin is unavailable. The probe runs as a persistent degraded-
    // state signal and never terminates the data plane.
    let _origin_probe = if dev_allow_missing_origin() {
        eprintln!(
            "verglas-cache-node {VERSION} WARNING: VERGLAS_DEV_ALLOW_MISSING_ORIGIN set — skipping the backend startup probe; the origin is NOT verified (dev/test only)"
        );
        None
    } else {
        Some(spawn_origin_probe(Arc::clone(&registry)))
    };

    let node_metrics = Arc::new(NodeMetrics::new()?);
    let activity = ActivityTracker::new();
    let quiescence = std::env::var("VERGLAS_HOST_AGENT_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
        .map(|token| admin::Quiescence::new(activity.clone(), token))
        .transpose()
        .map_err(std::io::Error::other)?;
    if quiescence.is_none() {
        tracing::warn!(
            "VERGLAS_HOST_AGENT_TOKEN is unset; authenticated scale-to-zero fencing is unavailable"
        );
    }
    let health = admin::Health::starting();
    let stats_slot: admin::StatsSlot = Arc::new(std::sync::OnceLock::new());
    let metrics_slot: admin::MetricsSlot = Arc::new(std::sync::OnceLock::new());
    let writeback_slot: WritebackSlot = Arc::new(std::sync::OnceLock::new());
    let page_cache_slot: crate::page_cache::PageCacheSlot = Arc::new(std::sync::OnceLock::new());

    // Bind the admin listener first so its port is owned before serving; it
    // answers `/admin/healthz` = starting/503 while the engine recovers below.
    let admin_addr = std::env::var("VERGLAS_ADMIN_ADDR")
        .unwrap_or_else(|_| format!("127.0.0.1:{}", config.listen.admin_port));
    let admin_listener = tokio::net::TcpListener::bind(&admin_addr).await?;
    eprintln!(
        "verglas-cache-node {VERSION} admin API listening on http://{}",
        admin_listener.local_addr()?
    );

    // Pre-bind the S3 listener too, so its port is owned before serving.
    let s3_addr = std::env::var("VERGLAS_S3_ADDR")
        .unwrap_or_else(|_| format!("0.0.0.0:{}", config.listen.s3_port));
    let s3_listener = tokio::net::TcpListener::bind(&s3_addr).await?;

    // The optional public S3 TLS listener (#R6-A): all-or-none env trio, no
    // fallback to plaintext-only serving on a partial or invalid config. See
    // `crate::tls` for the trust model and the "one fixed port, one
    // listener" exception this adds.
    let s3_tls_listener = crate::tls::from_env().await?;
    match &s3_tls_listener {
        Some(tls_listener) => eprintln!(
            "verglas-cache-node {VERSION} TLS S3 listener bound on {} (public passthrough)",
            tls_listener.local_addr()?
        ),
        None => eprintln!(
            "verglas-cache-node {VERSION} TLS S3 listener disabled: VERGLAS_S3_TLS_* is unset"
        ),
    }

    // Block-device tier (#382): the durable chunk store + attached-device
    // registry, the block-control route on the admin surface, and the NBD
    // listener. It backs onto the SAME single bucket the cache reads/writes
    // through. A glob-only/bucketless config (dev) has no single bucket to root
    // chunks in, so the block tier is off there.
    // The ring flush plane, held for the process lifetime when the node is on a
    // ring (fragment peer server + takeover loop). `None` on a single-node node.
    let mut ring_plane = None;
    let block_registry = match config.backend.bucket.as_deref() {
        Some(bucket) => {
            let store = registry.store_for(DEFAULT_STORAGE_BINDING_ID, bucket)?;
            let backend: Arc<dyn ObjectBackend> = Arc::new(ObjectStoreBackend::new(store));
            let device_registry =
                crate::blockdev::DeviceRegistry::open(&config.cache.dir, backend).await?;
            // Learn the ring and attach the flush write-back plane to the registry
            // before any device is ensured. With no ring configured this is a
            // no-op and FLUSH stays the synchronous origin barrier (#382).
            ring_plane = crate::ring::setup(
                &config.cache.dir,
                config.cache.capacity_bytes.0,
                &device_registry,
                Arc::clone(&page_cache_slot),
                activity.clone(),
            )
            .await?
            .map(Arc::new);
            let block_addr = std::env::var("VERGLAS_BLOCK_ADDR")
                .unwrap_or_else(|_| format!("0.0.0.0:{BLOCK_PORT}"));
            let block_listener = tokio::net::TcpListener::bind(&block_addr).await?;
            eprintln!(
                "verglas-cache-node {VERSION} block-device NBD listening on {} (backing bucket `{bucket}`)",
                block_listener.local_addr()?
            );
            Some((device_registry, block_listener))
        }
        None => {
            eprintln!(
                "verglas-cache-node {VERSION} block-device tier disabled: no single backend.bucket to root chunks in"
            );
            None
        }
    };

    // The Neon listener is another data plane of this same process. It is
    // present whenever this node belongs to the fragment ring and shares that
    // ring's transport/listener/store with block FLUSH.
    let object_ring = ring_plane.clone();
    let safekeeper_args = match (config.backend.bucket.clone(), ring_plane.as_ref()) {
        (Some(bucket), Some(ring)) => {
            let layout = crate::safekeeper::geometry_from_env(ring.node_count())?;
            let consensus = crate::consensus::ConsensusPlane::new(
                config.cache.dir.clone(),
                Arc::clone(ring),
                layout.k,
                layout.m,
            )
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
            let catalog_archive = config.catalog_archive.as_ref().ok_or_else(|| {
                std::io::Error::other(
                    "catalog_archive is required when the authoritative cache ring is enabled",
                )
            })?;
            let catalog_store =
                registry.store_for(DEFAULT_STORAGE_BINDING_ID, &catalog_archive.bucket)?;
            let catalog_store: Arc<dyn verglas_safekeeper::ImmutableSegmentStore> =
                Arc::new(crate::safekeeper::ObjectArchiveStore::new(catalog_store));
            let archives = crate::safekeeper::AuthoritativeArchives {
                wal_registry: Arc::clone(&registry),
                catalog: crate::safekeeper::CatalogArchiveDestination::new(
                    catalog_store,
                    catalog_archive.prefix.clone(),
                ),
            };
            let _ = bucket;
            Some((Arc::clone(ring), layout, consensus, archives))
        }
        _ => {
            eprintln!(
                "verglas-cache-node {VERSION} embedded safekeeper disabled: this node is not a configured fragment-ring member"
            );
            None
        }
    };
    let object_consensus = safekeeper_args
        .as_ref()
        .map(|(_, _, consensus, _)| Arc::clone(consensus));
    let shutdown_consensus = object_consensus.clone();

    // An external Iceberg REST endpoint is always an explicitly eventual
    // source. Managed catalog transactions use the native warehouse endpoint
    // below, where ConsensusPlane is the only ordering and durability authority.
    struct CatalogRuntime {
        gateway: Box<verglas_catalog::CatalogGateway>,
        watcher: Arc<verglas_tables::catalog::PollingWatcher>,
        token: Option<String>,
    }

    let catalog_runtime = match config.catalog.as_ref() {
        None => None,
        Some(catalog) => {
            use verglas_tables::catalog::{PollingWatcher, WatcherOptions};

            external_catalog_consistency(catalog.consistency)?;

            let gateway = verglas_catalog::CatalogGateway::from_config(catalog)?;
            let options = WatcherOptions::from_config(catalog);
            let runtime = CatalogRuntime {
                watcher: Arc::new(PollingWatcher::spawn(gateway.source(), options)),
                gateway: Box::new(gateway),
                token: std::env::var("VERGLAS_CATALOG_EVENT_TOKEN")
                    .ok()
                    .filter(|value| !value.is_empty()),
            };
            Some(runtime)
        }
    };

    let mut admin_app = admin::router(
        VERSION,
        health.clone(),
        stats_slot.clone(),
        metrics_slot.clone(),
    );
    let authoritative_stop = safekeeper_args
        .as_ref()
        .map(|(ring, _, consensus, archives)| {
            let consensus = Arc::clone(consensus);
            let archives = archives.clone();
            let holders = ring
                .consensus_voters()
                .into_iter()
                .take(ring.consensus_voters().len() / 2 + 1)
                .collect::<Vec<_>>();
            let stop: admin::AuthoritativeStop = Arc::new(move || {
                let consensus = Arc::clone(&consensus);
                let archives = archives.clone();
                let holders = holders.clone();
                let future: futures::future::BoxFuture<'static, Result<(), String>> =
                    Box::pin(async move {
                        crate::safekeeper::drain_authoritative_groups(
                            Arc::clone(&consensus),
                            archives,
                            holders,
                        )
                        .await?;
                        consensus
                            .relinquished_local_voter()
                            .await
                            .map_err(|error| error.to_string())
                    });
                future
            });
            stop
        });
    if let Some(quiescence) = quiescence {
        if let Some((ring, _, consensus, _)) = safekeeper_args.as_ref() {
            let consensus = Arc::clone(consensus);
            let ring = Arc::clone(ring);
            let replace: admin::MembershipReplace = Arc::new(move |request| {
                let consensus = Arc::clone(&consensus);
                let ring = Arc::clone(&ring);
                Box::pin(async move {
                    ring.validate_voter_identity(request.add, &request.node_id)
                        .map_err(str::to_owned)?;
                    let mut voters = consensus
                        .committed_voters(&request.group)
                        .await
                        .map_err(|error| error.to_string())?;
                    if !voters.remove(&request.remove) || voters.contains(&request.add) {
                        return Err("request is not one committed voter replacement".to_owned());
                    }
                    voters.insert(request.add);
                    consensus
                        .replace_voter(
                            &request.group,
                            request.remove,
                            request.add,
                            request.node_id,
                            request.address,
                            voters.into_iter().collect(),
                        )
                        .await
                        .map_err(|error| error.to_string())
                })
            });
            admin_app = admin_app.merge(admin::membership_router(quiescence.clone(), replace));
        }
        admin_app = admin_app.merge(admin::quiescence_router(quiescence, authoritative_stop));
    }
    if let (Some((ring, _, consensus, archives)), Ok(token)) = (
        safekeeper_args.as_ref(),
        std::env::var("VERGLAS_CATALOG_EVENT_TOKEN"),
    ) && !token.is_empty()
    {
        let consensus = Arc::clone(consensus);
        let registry = Arc::clone(&archives.wal_registry);
        let holders = ring
            .consensus_voters()
            .into_iter()
            .take(ring.consensus_voters().len() / 2 + 1)
            .collect::<Vec<_>>();
        let bind: admin::NeonTimelineBinding = Arc::new(move |request| {
            let consensus = Arc::clone(&consensus);
            let registry = Arc::clone(&registry);
            let holders = holders.clone();
            Box::pin(async move {
                registry
                    .store_for(DEFAULT_STORAGE_BINDING_ID, &request.bucket)
                    .map_err(|error| error.to_string())?;
                let group = format!("timeline/{}/{}", request.tenant_id, request.timeline_id);
                let digest = Sha256::digest(
                    format!(
                        "wal-archive-binding\0{}\0{}\0{}",
                        request.tenant_id, request.timeline_id, request.bucket
                    )
                    .as_bytes(),
                );
                let request_id = u128::from_be_bytes(
                    digest[..16]
                        .try_into()
                        .map_err(|_| "WAL archive binding identity malformed".to_owned())?,
                );
                match consensus
                    .submit(
                        &group,
                        verglas_consensus::GroupRequest::BindWalArchive {
                            request_id,
                            bucket: request.bucket,
                            holders,
                        },
                    )
                    .await
                    .map_err(|error| error.to_string())?
                {
                    verglas_consensus::GroupResponse::Applied(response)
                        if matches!(
                            response.outcome,
                            verglas_consensus::AppliedOutcome::Committed
                                | verglas_consensus::AppliedOutcome::Duplicate
                        ) =>
                    {
                        Ok(())
                    }
                    _ => Err("timeline WAL archive binding was not committed".to_owned()),
                }
            })
        });
        admin_app = admin_app.merge(admin::neon_timeline_binding_router(token, bind));
    }
    admin_app = admin_app.merge(admin::track_http(
        crate::page_cache::router(Arc::clone(&page_cache_slot)),
        activity.clone(),
        ActivityPlane::Http,
    ));
    if let Some((device_registry, _)) = &block_registry {
        admin_app = admin_app.merge(admin::track_http(
            crate::blockdev::control_router(device_registry.clone()),
            activity.clone(),
            ActivityPlane::Http,
        ));
    }
    if let Some(runtime) = &catalog_runtime {
        let mut catalog_app = admin::catalog_router(runtime.gateway.as_ref().clone());
        if let Some(token) = &runtime.token {
            catalog_app = catalog_app.merge(admin::eventual_catalog_event_router(
                Arc::clone(&runtime.watcher),
                token.clone(),
            ));
        }
        eprintln!(
            "verglas-cache-node {VERSION} external catalog consistency is eventual (polling)"
        );
        admin_app = admin_app.merge(admin::track_http(
            catalog_app,
            activity.clone(),
            ActivityPlane::Http,
        ));
    }
    let admin_fut = async move {
        axum::serve(admin_listener, admin_app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    };

    let data_plane = async {
        let backend = PassthroughRead::new(registry.clone());
        let (node, ring, peers) = match &object_ring {
            Some(ring) => (ring.node_id(), ring.read_ring(), ring.read_client()),
            None => {
                let node = NodeId::new(SINGLE_NODE_ID);
                (
                    node.clone(),
                    RendezvousRing::single(node),
                    PeerClient::disabled(),
                )
            }
        };

        // Disk recovery runs inside the engine build (foyer rescans its regions
        // and rebuilds the index). Time it for the operator log (#16).
        let recovery_start = std::time::Instant::now();
        let engine = HybridCacheEngine::new_with_background_fill_limit(
            backend,
            peers,
            ring,
            node,
            &config.cache,
            background_fill_limit(config.backend.max_concurrent_requests),
        )
        .await?;
        eprintln!(
            "verglas-cache-node {VERSION} cache recovery complete in {:?} — serving was gated on it (#16)",
            recovery_start.elapsed()
        );

        // Disk-full guardrail poll (#96). Held for the process lifetime.
        let _disk_monitor = spawn_disk_monitor(
            config,
            engine.caching_paused_handle(),
            engine.clone(),
            object_ring.clone(),
        );

        // Recovery is done: fill the engine-dependent admin routes, then flip the
        // health gate so a load balancer may start routing here.
        let _ = stats_slot.set(stats_source(
            config,
            engine.clone(),
            Arc::clone(&writeback_slot),
        ));
        let _ = metrics_slot.set(metrics_source(
            config,
            node_metrics.clone(),
            engine.clone(),
            registry.clone(),
            Arc::clone(&writeback_slot),
        ));
        let _ = page_cache_slot.set(engine.clone());
        health.mark_ready();

        serve_s3(S3Serve {
            config,
            s3_listener,
            s3_tls_listener,
            credentials,
            engine,
            registry,
            node_metrics,
            writeback: (object_ring, object_consensus, writeback_slot),
            activity: activity.clone(),
        })
        .await
    };

    // The NBD data plane. Present only when the block tier is enabled; otherwise
    // a never-resolving future so the join waits on the admin and S3 planes. A
    // listener-accept failure tears the process down like the other listeners.
    let nbd_activity = activity.clone();
    let nbd_fut = async move {
        match block_registry {
            Some((device_registry, block_listener)) => {
                crate::nbd::serve(block_listener, device_registry, nbd_activity)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
            }
            None => {
                std::future::pending::<()>().await;
                Ok(())
            }
        }
    };

    // The embedded PostgreSQL/Neon WAL plane. As with an absent NBD listener,
    // a node outside the fragment ring waits forever instead of inventing a
    // separate durability mode.
    let safekeeper_activity = activity.clone();
    let safekeeper_fut = async move {
        match safekeeper_args {
            Some((ring, layout, consensus, archives)) => {
                crate::safekeeper::serve(ring, layout, consensus, archives, safekeeper_activity)
                    .await
            }
            None => {
                std::future::pending::<()>().await;
                Ok(())
            }
        }
    };

    let result = tokio::try_join!(admin_fut, data_plane, nbd_fut, safekeeper_fut);
    if let Some(consensus) = shutdown_consensus {
        consensus
            .shutdown()
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
    }
    result.map(|_| ())
}

/// Probes the origin until it is reachable, then keeps checking after failures.
fn spawn_origin_probe(registry: Arc<BackendStore>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = std::time::Duration::from_secs(1);
        loop {
            match registry.probe().await {
                Ok(()) => {
                    eprintln!("verglas-cache-node {VERSION} backend probe passed");
                    backoff = std::time::Duration::from_secs(30);
                }
                Err(error) => {
                    eprintln!(
                        "verglas-cache-node {VERSION} backend unavailable; serving recovered cache and dirty ring data in degraded mode: {error}"
                    );
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                }
            }
            tokio::time::sleep(backoff).await;
        }
    })
}

/// Serves the SigV4 S3 endpoint until the process is interrupted. Reads are
/// served by the hybrid cache engine over the read passthrough, filling misses
/// from the origin. On a ring, writes acknowledge after fragment quorum and
/// propagate asynchronously; without a ring, writes acknowledge from the
/// origin. Both paths invalidate key-to-ETag mappings before acknowledgement.
/// Path-style always works; a configured `[listen].domain` adds virtual-hosted
/// addressing.
struct S3Serve<'a> {
    config: &'a Config,
    s3_listener: tokio::net::TcpListener,
    /// The TLS S3 listener, present only when the `VERGLAS_S3_TLS_*` env
    /// trio was set at boot. Serves the identical router built below.
    s3_tls_listener: Option<crate::tls::TlsListener>,
    credentials: (String, String),
    engine: CacheEngine,
    registry: Arc<BackendStore>,
    node_metrics: Arc<NodeMetrics>,
    writeback: (
        Option<Arc<crate::ring::RingPlane>>,
        Option<Arc<crate::consensus::ConsensusPlane>>,
        WritebackSlot,
    ),
    activity: ActivityTracker,
}

async fn serve_s3(context: S3Serve<'_>) -> Result<(), Box<dyn std::error::Error>> {
    let S3Serve {
        config,
        s3_listener,
        s3_tls_listener,
        credentials,
        engine,
        registry,
        node_metrics,
        writeback,
        activity,
    } = context;
    let (object_ring, object_consensus, writeback_slot) = writeback;
    // Semantic state is opened from the same customer catalog and routes table
    // FileIO back through this listener. No process-local registry survives
    // restart: the adapter reopens graphs from Iceberg for each operation.
    let semantic_api: Option<Arc<dyn verglas_s3::semantic::SemanticApi>> =
        match config.catalog.as_ref() {
            Some(catalog) => {
                let local_addr = s3_listener.local_addr()?;
                let connection = verglas_iceberg::Connection {
                    catalog_uri: catalog.uri.clone(),
                    token: catalog
                        .resolve_bearer_token()
                        .map_err(std::io::Error::other)?,
                    warehouse: catalog.warehouse.clone(),
                    s3_endpoint: Some(format!("http://{local_addr}")),
                    region: config
                        .backend
                        .region
                        .clone()
                        .unwrap_or_else(|| "us-east-1".to_owned()),
                    access_key_id: Some(credentials.0.clone()),
                    secret_access_key: Some(credentials.1.clone()),
                };
                Some(Arc::new(
                    verglas_s3::semantic::IcebergCatalogSemanticStore::new(
                        verglas_iceberg::catalog::open_catalog(&connection).await?,
                    ),
                ))
            }
            None => None,
        };
    // A successful vector/graph mutation optionally publishes a fire-and-
    // forget commit event to the control-plane queue ingress; this is a
    // best-effort notification side channel wired with the node's own
    // configured credential and tenant id, never the caller's. See the
    // trust-model note on `wrap_with_event_publisher` for what this can and
    // cannot be relied on for.
    let semantic_api =
        semantic_api.map(
            |api| match verglas_s3::semantic::EventPublishConfig::from_env() {
                Some(config) => verglas_s3::semantic::wrap_with_event_publisher(api, config),
                None => api,
            },
        );
    // Listings always pass straight through to the origin (never cached), so the
    // lister is wired to the backend registry directly, not the engine.
    let lister: Arc<dyn verglas_core::list::ObjectList> =
        Arc::new(PassthroughList::new(registry.clone()));
    // The engine doubles as the write path's invalidator (one Arc bump).
    let invalidation: Arc<dyn verglas_core::write::Invalidation> = Arc::new(engine.clone());
    let bucket_set = registry.bucket_set();
    let origin = Arc::new(PassthroughWrite::new(registry.clone()));

    let app = if let Some(ring) = object_ring {
        let layout = crate::safekeeper::geometry_from_env(ring.node_count())?;
        let policy = Arc::new(object_writeback_policy(layout));
        let (journal_store, migrated_journals) = JournalStore::open_for_binding(
            config.cache.dir.join("object-writeback"),
            DEFAULT_STORAGE_BINDING_ID,
        )?;
        if migrated_journals > 0 {
            eprintln!(
                "verglas-cache-node {VERSION} migrated {migrated_journals} legacy writeback journals to storage binding {DEFAULT_STORAGE_BINDING_ID}"
            );
        }
        let journals = Arc::new(journal_store);
        let metrics = Arc::new(WritebackMetrics::default());
        let _ = writeback_slot.set(Arc::clone(&metrics));
        let membership = ring.membership();
        let coordinator = Arc::new(WriteCoordinator::new(
            ring.transport(),
            Arc::clone(&membership),
            journals,
            metrics,
            Arc::clone(&origin),
            Arc::new(crate::consensus::ObjectConsensusCommitter::new(
                object_consensus.ok_or("ring write-back requires the consensus plane")?,
            )),
            std::time::Duration::from_millis(config.cache.writeback.ack_deadline_ms),
        ));
        let _repair = verglas_writeback::spawn_repair_loop(
            Arc::clone(&coordinator),
            membership,
            std::time::Duration::from_secs(2),
        );
        let _scrub = verglas_writeback::spawn_scrub_loop(
            Arc::clone(&coordinator),
            std::time::Duration::from_secs(config.cache.writeback.scrub_interval_secs),
        );
        let tier = WritebackTier::new(coordinator, engine, origin, policy);
        verglas_s3::router_with_passthrough(
            DEFAULT_STORAGE_BINDING_ID,
            tier.reader,
            tier.writer,
            lister,
            invalidation,
            Some(credentials.clone()),
            Some(registry as Arc<dyn verglas_backend::BackendStores>),
            config.listen.domain.as_deref(),
            Some(node_metrics),
        )
    } else {
        verglas_s3::router_with_passthrough(
            DEFAULT_STORAGE_BINDING_ID,
            engine,
            PassthroughWrite::new(registry.clone()),
            lister,
            invalidation,
            Some(credentials.clone()),
            Some(registry as Arc<dyn verglas_backend::BackendStores>),
            config.listen.domain.as_deref(),
            Some(node_metrics),
        )
    };
    // Semantic routes exist only with a configured customer catalog and use the
    // same configured keypair as S3 objects for their distinct SigV4 services.
    let app = match semantic_api {
        Some(api) => verglas_s3::semantic::router_with_sigv4(
            api,
            verglas_s3::semantic::SemanticCredentials::new(
                credentials.0.clone(),
                credentials.1.clone(),
            ),
        )
        .merge(app),
        None => app,
    };
    let app = admin::track_http(app, activity, ActivityPlane::Http);
    // SigV4 verification canonicalizes the `host` header the CLIENT signed,
    // which two legitimate hops rewrite or drop:
    //  - The Verglas Cloud edge router proxies `<slug>.s3.verglas.dev` to this
    //    node's Fly origin. A Cloudflare Worker cannot preserve a cross-zone
    //    Host header, so it forwards the signed one as `x-forwarded-host`.
    //    Trusting it is sound: a forged value still has to match a signature
    //    only the tenant's secret key can produce.
    //  - HTTP/2 requests carry the authority in the `:authority` pseudo-header
    //    and omit Host entirely (h2 clients, and fly-proxy forwarding h2).
    // Restore the signed host so both paths verify identically to direct
    // HTTP/1.1.
    let app = app.layer(axum::middleware::from_fn(
        |mut request: axum::extract::Request, next: axum::middleware::Next| async move {
            if let Some(forwarded) = request.headers().get("x-forwarded-host").cloned() {
                request
                    .headers_mut()
                    .insert(axum::http::header::HOST, forwarded);
            } else if !request.headers().contains_key(axum::http::header::HOST)
                && let Some(authority) = request.uri().authority().cloned()
                && let Ok(value) = axum::http::HeaderValue::from_str(authority.as_str())
            {
                request
                    .headers_mut()
                    .insert(axum::http::header::HOST, value);
            }
            next.run(request).await
        },
    ));

    let local_addr = s3_listener.local_addr()?;
    eprintln!(
        "verglas-cache-node {VERSION} serving S3 on http://{local_addr} (backend bucket set: {bucket_set})"
    );

    // Both listeners serve the SAME router. With no TLS trio configured this
    // is exactly the prior single-listener behavior; with it configured, the
    // TLS listener runs concurrently rather than after the plaintext one —
    // neither blocks the other's connections on the other's traffic.
    match s3_tls_listener {
        Some(tls_listener) => {
            let tls_addr = tls_listener.local_addr()?;
            eprintln!(
                "verglas-cache-node {VERSION} serving S3 (TLS) on https://{tls_addr} (public passthrough, backend bucket set: {bucket_set})"
            );
            let tls_app = app.clone();
            let (plaintext_result, tls_result) = tokio::join!(
                axum::serve(s3_listener, app).with_graceful_shutdown(shutdown_signal()),
                crate::tls::serve(tls_listener, tls_app, shutdown_signal()),
            );
            plaintext_result?;
            tls_result?;
        }
        None => {
            axum::serve(s3_listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await?;
        }
    }
    Ok(())
}

/// Waits for Ctrl-C before tearing down a listener.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::routing::get;
    use std::sync::atomic::AtomicUsize;
    use verglas_core::admin::{HEALTHZ_PATH, METRICS_PATH, STATS_PATH};

    /// Every object uploaded through a ring-backed tenant cache receives the
    /// same EC durability geometry as the embedded safekeeper.
    #[test]
    fn neon_object_writeback_covers_layers_and_index_publication() {
        let policy =
            object_writeback_policy(verglas_safekeeper::AppendGeometry { k: 2, m: 1, w: 3 });
        for key in [
            "tenants/t1/timelines/l1/000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF-00000001",
            "tenants/t1/timelines/l1/index_part.json-00000001",
        ] {
            assert_eq!(
                policy.geometry_for(&verglas_core::CacheKey {
                    storage_binding_id: DEFAULT_STORAGE_BINDING_ID.to_owned(),
                    bucket: "tenant".to_owned(),
                    key: key.to_owned(),
                }),
                Some((2, 1, 3))
            );
        }
    }

    /// Background fills consume only their configured quarter-share and are
    /// disabled when the origin budget is too small to reserve user capacity.
    #[test]
    fn background_fill_limit_reserves_foreground_capacity() {
        assert_eq!(background_fill_limit(1), 0);
        assert_eq!(background_fill_limit(3), 0);
        assert_eq!(background_fill_limit(4), 1);
        assert_eq!(background_fill_limit(64), 16);
    }

    /// Builds a real cache engine over a fake backend, builds the stats source
    /// this crate owns, mounts it on the admin router, and drives `/admin/stats`
    /// over a loopback socket. Proves the wiring `stats_source` -> `admin::router`
    /// reports the configured budgets — the piece a container smoke test
    /// exercises but never instruments.
    #[tokio::test]
    async fn admin_stats_reports_the_configured_budgets_in_process() {
        let dir = tempfile::tempdir().expect("temp dir");
        let toml = format!(
            "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\ndram_bytes = \"80MB\"\n[backend]\nbucket = \"test-bucket\"\n",
            dir.path().display()
        );
        let config = Config::from_toml_str(&toml).expect("valid config");

        let (engine, _registry) = build_test_engine(&config).await;
        let stats_slot: admin::StatsSlot = Arc::new(std::sync::OnceLock::new());
        let _ = stats_slot.set(stats_source(
            &config,
            engine,
            Arc::new(std::sync::OnceLock::new()),
        ));
        let metrics_slot: admin::MetricsSlot = Arc::new(std::sync::OnceLock::new());

        let health = admin::Health::starting();
        health.mark_ready();
        let addr = serve_router(admin::router(
            crate::VERSION,
            health,
            stats_slot,
            metrics_slot,
        ))
        .await;

        let body: verglas_core::admin::StatsInfo =
            reqwest::get(format!("http://{addr}{STATS_PATH}"))
                .await
                .expect("request")
                .json()
                .await
                .expect("stats json");
        assert_eq!(body.cache.dram_bytes, 80 * 1024 * 1024);
        assert_eq!(body.cache.capacity_bytes, 64 * 1024 * 1024);
        // A freshly built engine has served nothing, but the gauge is live.
        assert_eq!(body.counters.disk_hits, 0);
        // This isolated engine has no warmer or running write-back coordinator.
        assert!(body.warming.is_none());
        assert!(body.writeback.is_none());
    }

    /// `/admin/healthz` reports `starting`/503 until the slot-filling recovery
    /// step calls `mark_ready`, then `ok`/200 — the serve-gating (#16) contract
    /// operators and load balancers depend on. While starting, the engine-dependent
    /// `/admin/stats` and `/metrics` routes answer 503 (not 404), so a load
    /// balancer sees a clear not-ready signal rather than connection-refused.
    #[tokio::test]
    async fn healthz_and_engine_routes_gate_on_readiness() {
        let health = admin::Health::starting();
        let stats_slot: admin::StatsSlot = Arc::new(std::sync::OnceLock::new());
        let metrics_slot: admin::MetricsSlot = Arc::new(std::sync::OnceLock::new());
        let addr = serve_router(admin::router(
            crate::VERSION,
            health.clone(),
            stats_slot,
            metrics_slot,
        ))
        .await;

        // Starting: healthz is 503, and the unfilled engine routes are 503 too.
        let health_url = format!("http://{addr}{HEALTHZ_PATH}");
        let resp = reqwest::get(&health_url).await.expect("request");
        assert_eq!(resp.status().as_u16(), 503);
        assert_eq!(
            reqwest::get(format!("http://{addr}{STATS_PATH}"))
                .await
                .expect("request")
                .status()
                .as_u16(),
            503
        );
        assert_eq!(
            reqwest::get(format!("http://{addr}{METRICS_PATH}"))
                .await
                .expect("request")
                .status()
                .as_u16(),
            503
        );

        // Recovery completes: healthz flips to ok/200.
        health.mark_ready();
        let resp = reqwest::get(&health_url).await.expect("request");
        assert_eq!(resp.status().as_u16(), 200);
    }

    /// The cache node owns the upstream catalog response cache. Repeated query
    /// workers loading the same snapshot use its local gateway without another
    /// Lakekeeper request.
    #[tokio::test]
    async fn catalog_gateway_serves_repeated_metadata_from_cache() {
        let upstream_hits = Arc::new(AtomicUsize::new(0));
        let hits = upstream_hits.clone();
        let upstream = serve_router(Router::new().route(
            "/v1/config",
            get(move || {
                hits.fetch_add(1, Ordering::Relaxed);
                async { axum::Json(serde_json::json!({})) }
            }),
        ))
        .await;
        let catalog = verglas_core::config::Catalog {
            consistency: verglas_core::config::CatalogConsistency::Eventual,
            uri: format!("http://{upstream}"),
            poll_interval_secs: 30,
            include: Vec::new(),
            exclude: Vec::new(),
            credentials_file: None,
            credentials_profile: None,
            bearer_token: None,
            sigv4_region: None,
            sigv4_signing_name: None,
            warehouse: None,
        };
        let gateway = verglas_catalog::CatalogGateway::from_config(&catalog).expect("gateway");
        let addr = serve_router(admin::catalog_router(gateway)).await;

        for _ in 0..2 {
            let response = reqwest::get(format!("http://{addr}/catalog/v1/config"))
                .await
                .expect("metadata request");
            assert_eq!(response.status().as_u16(), 200);
        }
        assert_eq!(upstream_hits.load(Ordering::Relaxed), 1);
    }

    /// `/metrics` serves the Prometheus exposition with the
    /// `text/plain; version=0.0.4` content type once the slot is filled — the
    /// contract the metrics VM's self-scrape depends on.
    #[tokio::test]
    async fn metrics_serves_the_prometheus_exposition() {
        let dir = tempfile::tempdir().expect("temp dir");
        let toml = format!(
            "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\ndram_bytes = \"80MB\"\n[backend]\nbucket = \"test-bucket\"\n",
            dir.path().display()
        );
        let config = Config::from_toml_str(&toml).expect("valid config");
        let (engine, registry) = build_test_engine(&config).await;
        let node_metrics = Arc::new(NodeMetrics::new().expect("metrics"));

        let metrics_slot: admin::MetricsSlot = Arc::new(std::sync::OnceLock::new());
        let _ = metrics_slot.set(metrics_source(
            &config,
            node_metrics,
            engine,
            registry,
            Arc::new(std::sync::OnceLock::new()),
        ));
        let stats_slot: admin::StatsSlot = Arc::new(std::sync::OnceLock::new());
        let health = admin::Health::starting();
        health.mark_ready();
        let addr = serve_router(admin::router(
            crate::VERSION,
            health,
            stats_slot,
            metrics_slot,
        ))
        .await;

        let resp = reqwest::get(format!("http://{addr}{METRICS_PATH}"))
            .await
            .expect("request");
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some(verglas_core::metrics::EXPOSITION_CONTENT_TYPE)
        );
        let text = resp.text().await.expect("body");
        // The capacity gauge carries the configured NVMe ceiling.
        assert!(
            text.contains("verglas_cache_capacity_bytes"),
            "exposition carries the cache capacity family: {text}"
        );
    }

    /// Builds a cache engine over the config's backend (a fake bucket that is
    /// never reached — the tests never issue a read that would fill), the same
    /// single-node rendezvous ring + no-op peer the serve path uses.
    async fn build_test_engine(config: &Config) -> (CacheEngine, Arc<BackendStore>) {
        let registry = BackendStore::from_config(DEFAULT_STORAGE_BINDING_ID, &config.backend);
        let backend = PassthroughRead::new(registry.clone());
        let node = NodeId::new(SINGLE_NODE_ID);
        let ring = RendezvousRing::single(node.clone());
        let engine = HybridCacheEngine::new_with_background_fill_limit(
            backend,
            PeerClient::disabled(),
            ring,
            node,
            &config.cache,
            background_fill_limit(config.backend.max_concurrent_requests),
        )
        .await
        .expect("engine builds");
        (engine, registry)
    }

    /// Serves `app` on a loopback ephemeral port and returns the bound address.
    /// The server task is detached; the test process exit reaps it.
    async fn serve_router(app: axum::Router) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }
}
/// Rejects an external catalog configuration that claims managed consistency.
///
/// Managed warehouses use the native `warehouse/<id>` consensus groups instead
/// of an upstream REST gateway, so accepting `strong` here would create a
/// second authority or silently downgrade its semantics.
fn external_catalog_consistency(consistency: CatalogConsistency) -> Result<(), &'static str> {
    match consistency {
        CatalogConsistency::Eventual => Ok(()),
        CatalogConsistency::Strong => Err(
            "[catalog] configures an external catalog and must use consistency=eventual; managed warehouses use the native ConsensusPlane catalog endpoint",
        ),
    }
}

#[cfg(test)]
mod catalog_consistency_tests {
    use super::external_catalog_consistency;
    use verglas_core::config::CatalogConsistency;

    /// An upstream REST catalog cannot claim the managed consensus guarantee.
    #[test]
    fn external_strong_catalog_is_rejected_without_an_eventual_fallback() {
        let error = external_catalog_consistency(CatalogConsistency::Strong)
            .expect_err("external strong authority is forbidden");
        assert!(error.contains("must use consistency=eventual"));
    }

    /// Polling remains the explicit contract for a configured external catalog.
    #[test]
    fn external_eventual_catalog_remains_supported() {
        external_catalog_consistency(CatalogConsistency::Eventual)
            .expect("eventual external catalog is supported");
    }
}
