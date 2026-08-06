//! The cache serving path: build the origin backend, probe it, build the hybrid
//! cache engine over a single-node rendezvous ring, run the disk-full guardrail
//! poll, and serve the SigV4 S3 endpoint with read-through / write-through.
//!
//! This is the CACHE-relevant subset of `bins/verglas-server/src/main.rs::serve`,
//! reproduced without the cluster ring, gossip peers, write-back tier, catalog
//! watcher, table lifecycle, or any admin surface beyond health/stats/metrics.
//!
//! ## Why a cluster-of-one, no peers, no write-back
//!
//! - **Ring**: a [`RendezvousRing::single`] node owns every key, so the read
//!   path never consults a peer. This is byte-identical ownership to a verglas-server
//!   server started without `[cluster]`, and it drops the whole `verglas-cluster`
//!   dependency (gossip, peer RPC, fragment store).
//! - **Peers**: [`NoopPeerFetch`] — there are no peers to fetch from.
//! - **Object write-back**: the fleet cache image's boot script renders no
//!   `[cache.writeback]`, so the OBJECT tier is off by design; S3 PUTs pass
//!   straight through to the origin durably (write-through), exactly what a
//!   disabled write-back tier does in verglas-server.
//! - **Block-device write-back**: the block tier (#382) is the exception that
//!   reaches the ring. When `VERGLAS_RING_PEERS` names a ring, a device FLUSH is
//!   erasure-coded across the boxes and acked on a quorum (draining to R2 in the
//!   background); see [`crate::ring`]. The embedded safekeeper shares that same
//!   fragment store and peer RPC; the object read/serve path above is still a
//!   peerless cluster-of-one. With no ring configured block FLUSH stays the
//!   synchronous R2 barrier and no safekeeper listener starts.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use verglas_backend::{BackendStore, BackendStores};
use verglas_block::{ObjectBackend, ObjectStoreBackend};
use verglas_cache::HybridCacheEngine;
use verglas_core::admin::{CacheConfigInfo, CountersInfo, StatsInfo};
use verglas_core::config::Config;
use verglas_core::metrics::NodeMetrics;
use verglas_core::node::NodeId;
use verglas_core::peer::NoopPeerFetch;
use verglas_core::ring::RendezvousRing;
use verglas_s3::{PassthroughList, PassthroughRead, PassthroughWrite};

use crate::VERSION;
use crate::admin;

/// The node id the single-node cache uses. Matches verglas-server's cluster-of-one id
/// so ownership is byte-identical to a pre-cluster server.
const SINGLE_NODE_ID: &str = "single";

/// The default NBD listen port for the block-device tier (#382). The host agent
/// connects a microVM's kernel NBD client here; the export name selects the
/// device. Overridable with `VERGLAS_BLOCK_ADDR` (same shape as
/// `VERGLAS_S3_ADDR`). Not a config knob — one fixed port, one listener.
const BLOCK_PORT: u16 = 8335;

/// The concrete engine the cache node runs: the hybrid cache over the
/// single-bucket passthrough backend, with a one-member rendezvous ring and the
/// no-op peer fetch. No `verglas-cluster` types appear here.
type CacheEngine = HybridCacheEngine<PassthroughRead, NoopPeerFetch, RendezvousRing>;

/// Whether the operator has opted out of the backend startup probe via
/// `VERGLAS_DEV_ALLOW_MISSING_ORIGIN`. Truthy values are `1`/`true`
/// (case-insensitive). A dev/test escape hatch — same name and semantics as
/// verglas-server so the two binaries behave identically.
fn dev_allow_missing_origin() -> bool {
    std::env::var("VERGLAS_DEV_ALLOW_MISSING_ORIGIN")
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            value == "1" || value == "true"
        })
        .unwrap_or(false)
}

/// Reserves one quarter of configured origin concurrency for repeated aligned
/// cache tails, leaving the rest for user reads. Copied from verglas-server so the
/// overfetch behaviour matches.
fn background_fill_limit(max_concurrent_requests: usize) -> usize {
    max_concurrent_requests / 4
}

/// Builds the `/admin/stats` source: reads the engine's live counters and DRAM
/// usage and stamps them next to the configured budgets. Warming and write-back
/// are always `None` — the cache node runs neither.
fn stats_source(config: &Config, engine: CacheEngine) -> admin::StatsSource {
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
            writeback: None,
        }
    })
}

/// Builds the `GET /metrics` source: renders the Prometheus exposition on each
/// scrape from the shared request registry plus the engine and backend counters
/// read at scrape time. The CACHE-relevant subset of verglas-server's `metrics_source`
/// (no per-table telemetry, no write-back families).
fn metrics_source(
    config: &Config,
    metrics: Arc<NodeMetrics>,
    engine: CacheEngine,
    backend: Arc<BackendStore>,
) -> admin::MetricsSource {
    use verglas_core::metrics::{MetricsSnapshot, TierSize, render};
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
                        used: 0,
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
            writeback: None,
        };
        render(&metrics, &snapshot)
    })
}

/// Spawns the disk-full guardrail poll (#96). Once a second it reads the free
/// space under `cache.dir` and the engine's remaining physical growth room and
/// publishes one decision the fill path reads as a plain atomic: pause block
/// admission when the filesystem nears full so a full disk degrades the node to
/// origin fills rather than crashing it (budgets are hard ceilings). The
/// fragment-budget sharing verglas-server's poll also does is absent — there is no
/// write-back fragment store to grant budget to. Detached for the process
/// lifetime.
fn spawn_disk_monitor(
    config: &Config,
    caching_paused: Arc<AtomicBool>,
    engine: CacheEngine,
) -> tokio::task::JoinHandle<()> {
    use verglas_core::disk::{DiskParams, disk_decision, free_bytes};

    let dir = config.cache.dir.clone();
    let capacity = config.cache.capacity_bytes.0;
    // Keep a headroom reserve so admission stops before the disk is truly spent;
    // the gap to `high_water` is the hysteresis band. Floored at 64 MiB so a tiny
    // test budget still leaves a sane reserve. Same shape as verglas-server's poll.
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
            // No write-back fragments on a cache node, so nothing else holds the
            // shared budget (`frag_used = 0`).
            let state = disk_decision(free, room, 0, was_paused, &params);
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
/// Ordering mirrors verglas-server's serve-gating (#16): the admin listener binds
/// first reporting `starting`, then the backend is probed and the engine
/// recovers its on-disk tiers, then the stats/metrics slots are filled and the
/// health gate flips to `ready`, then the S3 endpoint serves. Either listener
/// failing tears the process down — a half-alive cache is worse than a dead one.
pub async fn run(
    config: &Config,
    credentials: (String, String),
) -> Result<(), Box<dyn std::error::Error>> {
    // One backend store shared by the read and write passthroughs.
    let registry = BackendStore::from_config(&config.backend);
    eprintln!(
        "verglas-cache-node {VERSION} backend resolved: {} (serving bucket set `{}`)",
        registry.describe(),
        registry.bucket_set()
    );

    // Startup probe (#233): a configured bucket that cannot be reached or
    // authenticated must fail startup, not serve an empty store. Dev/test that
    // runs without a reachable origin opts out via VERGLAS_DEV_ALLOW_MISSING_ORIGIN.
    if dev_allow_missing_origin() {
        eprintln!(
            "verglas-cache-node {VERSION} WARNING: VERGLAS_DEV_ALLOW_MISSING_ORIGIN set — skipping the backend startup probe; the origin is NOT verified (dev/test only)"
        );
    } else {
        registry.probe().await?;
        eprintln!("verglas-cache-node {VERSION} backend startup probe passed");
    }

    let node_metrics = Arc::new(NodeMetrics::new()?);
    let health = admin::Health::starting();
    let stats_slot: admin::StatsSlot = Arc::new(std::sync::OnceLock::new());
    let metrics_slot: admin::MetricsSlot = Arc::new(std::sync::OnceLock::new());

    // The cache node is the sole owner of upstream catalog credentials and
    // change tracking. The gateway and watcher share one response cache: every
    // successful poll/feed refresh prepares the exact Iceberg REST documents a
    // local query worker subsequently consumes from `/catalog`.
    let catalog_runtime = config
        .catalog
        .as_ref()
        .map(|catalog| -> Result<_, Box<dyn std::error::Error>> {
            use verglas_tables::catalog::{CatalogFeed, WatcherOptions, WsFeedConfig};

            let gateway = verglas_catalog::CatalogGateway::from_config(catalog)?;
            let feed_uri =
                std::env::var("VERGLAS_CATALOG_FEED_URI").unwrap_or_else(|_| catalog.uri.clone());
            let feed_token = std::env::var("VERGLAS_CATALOG_FEED_TOKEN")
                .ok()
                .or(catalog.resolve_bearer_token()?);
            let ws = if catalog.sigv4_enabled() {
                None
            } else {
                WsFeedConfig::from_catalog_uri(&feed_uri, feed_token)
            };
            let watcher =
                CatalogFeed::spawn(gateway.source(), WatcherOptions::from_config(catalog), ws);
            Ok((gateway, watcher))
        })
        .transpose()?;

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
            let store = registry.store_for(bucket)?;
            let backend: Arc<dyn ObjectBackend> = Arc::new(ObjectStoreBackend::new(store));
            let device_registry =
                crate::blockdev::DeviceRegistry::open(&config.cache.dir, backend).await?;
            // Learn the ring and attach the flush write-back plane to the registry
            // before any device is ensured. With no ring configured this is a
            // no-op and FLUSH stays the synchronous R2 barrier (#382).
            ring_plane = crate::ring::setup(&config.cache.dir, &device_registry)
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
    let safekeeper_args = match (config.backend.bucket.clone(), ring_plane) {
        (Some(bucket), Some(ring)) => Some((
            Arc::clone(&registry),
            bucket,
            config.cache.dir.clone(),
            ring,
        )),
        _ => {
            eprintln!(
                "verglas-cache-node {VERSION} embedded safekeeper disabled: this node is not a configured fragment-ring member"
            );
            None
        }
    };

    let mut admin_app = admin::router(
        VERSION,
        health.clone(),
        stats_slot.clone(),
        metrics_slot.clone(),
    );
    if let Some((device_registry, _)) = &block_registry {
        admin_app = admin_app.merge(crate::blockdev::control_router(device_registry.clone()));
    }
    if let Some((gateway, _watcher)) = &catalog_runtime {
        admin_app = admin_app.merge(admin::catalog_router(gateway.clone()));
    }
    let admin_fut = async move {
        axum::serve(admin_listener, admin_app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    };

    let data_plane = async {
        let backend = PassthroughRead::new(registry.clone());
        let node = NodeId::new(SINGLE_NODE_ID);
        let ring = RendezvousRing::single(node.clone());

        // Disk recovery runs inside the engine build (foyer rescans its regions
        // and rebuilds the index). Time it for the operator log (#16).
        let recovery_start = std::time::Instant::now();
        let engine = HybridCacheEngine::new_with_background_fill_limit(
            backend,
            NoopPeerFetch,
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
        let _disk_monitor =
            spawn_disk_monitor(config, engine.caching_paused_handle(), engine.clone());

        // Recovery is done: fill the engine-dependent admin routes, then flip the
        // health gate so a load balancer may start routing here.
        let _ = stats_slot.set(stats_source(config, engine.clone()));
        let _ = metrics_slot.set(metrics_source(
            config,
            node_metrics.clone(),
            engine.clone(),
            registry.clone(),
        ));
        health.mark_ready();

        serve_s3(
            config,
            s3_listener,
            credentials,
            engine,
            registry,
            node_metrics,
        )
        .await
    };

    // The NBD data plane. Present only when the block tier is enabled; otherwise
    // a never-resolving future so the join waits on the admin and S3 planes. A
    // listener-accept failure tears the process down like the other listeners.
    let nbd_fut = async move {
        match block_registry {
            Some((device_registry, block_listener)) => {
                crate::nbd::serve(block_listener, device_registry)
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
    let safekeeper_fut = async move {
        match safekeeper_args {
            Some((stores, bucket, cache_dir, ring)) => {
                crate::safekeeper::serve(stores, bucket, cache_dir, ring).await
            }
            None => {
                std::future::pending::<()>().await;
                Ok(())
            }
        }
    };

    tokio::try_join!(admin_fut, data_plane, nbd_fut, safekeeper_fut).map(|_| ())
}

/// Serves the SigV4 S3 endpoint until the process is interrupted. Reads are
/// served by the hybrid cache engine over the read passthrough, filling misses
/// from the origin; writes pass through durably and invalidate the engine's
/// key->ETag mappings before the client is acked. Path-style always works; a
/// configured `[listen].domain` adds virtual-hosted addressing.
async fn serve_s3(
    config: &Config,
    s3_listener: tokio::net::TcpListener,
    credentials: (String, String),
    engine: CacheEngine,
    registry: Arc<BackendStore>,
    node_metrics: Arc<NodeMetrics>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Listings always pass straight through to the origin (never cached), so the
    // lister is wired to the backend registry directly, not the engine.
    let lister: Arc<dyn verglas_core::list::ObjectList> =
        Arc::new(PassthroughList::new(registry.clone()));
    // The engine doubles as the write path's invalidator (one Arc bump).
    let invalidation: Arc<dyn verglas_core::write::Invalidation> = Arc::new(engine.clone());
    let writer = PassthroughWrite::new(registry.clone());
    let bucket_set = registry.bucket_set();

    let app = verglas_s3::router_with_passthrough(
        engine,
        writer,
        lister,
        invalidation,
        Some(credentials),
        Some(registry as Arc<dyn verglas_backend::BackendStores>),
        // The cache-node role does not serve the server's `/v1` API.
        None,
        config.listen.domain.as_deref(),
        Some(node_metrics),
    );

    let local_addr = s3_listener.local_addr()?;
    eprintln!(
        "verglas-cache-node {VERSION} serving S3 on http://{local_addr} (backend bucket set: {bucket_set})"
    );
    axum::serve(s3_listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
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
        let _ = stats_slot.set(stats_source(&config, engine));
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
        // The cache node never warms or writes back, so those are always absent.
        assert!(body.warming.is_none());
        assert!(body.writeback.is_none());
    }

    /// `/admin/healthz` reports `starting`/503 until the slot-filling recovery
    /// step calls `mark_ready`, then `ok`/200 — the serve-gating (#16) contract
    /// the fleet's health check depends on. While starting, the engine-dependent
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
        let _ = metrics_slot.set(metrics_source(&config, node_metrics, engine, registry));
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
        let registry = BackendStore::from_config(&config.backend);
        let backend = PassthroughRead::new(registry.clone());
        let node = NodeId::new(SINGLE_NODE_ID);
        let ring = RendezvousRing::single(node.clone());
        let engine = HybridCacheEngine::new_with_background_fill_limit(
            backend,
            NoopPeerFetch,
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
