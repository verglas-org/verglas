//! `verglas-server` — the Verglas cache server.
//!
//! Runs on NVMe nodes and speaks the S3 protocol, serving hot reads from the
//! local cache and reading through to the origin bucket on miss. The admin HTTP
//! API is the private control surface the `verglas` CLI talks to.

mod environment;

use verglas_server::{VERSION, admin, follow, logging, node_report, platform};

use std::sync::{Arc, OnceLock};

// Dependency edge reserved for the server's table-awareness wiring (M3).
use tower::util::ServiceExt as _;
use verglas_cache::{CachePurger, HybridCacheEngine};
use verglas_cluster::fragments::{FragmentKey, FragmentRecord};
use verglas_cluster::peer::{FragmentHandlers, LocalBlockFn};
use verglas_cluster::{
    AgentConfig, ClusterAgent, FragmentClient, LiveRing, LocalFragmentStore, PeerClient, PeerServer,
};
use verglas_core::admin::{
    CacheConfigInfo, CountersInfo, DEFAULT_ADMIN_ADDR, DrainAck, MemberInfo, MembersInfo,
    StatsInfo, WritebackStatsInfo,
};
use verglas_core::node::NodeId;
use verglas_s3::{PassthroughRead, PassthroughWrite};
use verglas_write::{
    AgentMembership, LiveMembership, PeerFragmentTransport, PrefixRule, SingleNodeMembership,
    WriteCoordinator, WritebackMetrics, WritebackPolicy, WritebackTier,
};

/// The concrete cache engine the server runs: the hybrid cache over the
/// single-bucket passthrough backend, routing ownership through the live gossip
/// ring and the peer-fetch client (#29). A single-node server runs the
/// degenerate one-member ring with a disabled peer client (it owns every key,
/// so the peer rung is never reached), so the engine type is the same whether
/// or not `[cluster]` is configured — no read-path code branches on it.
type ServerEngine = HybridCacheEngine<PassthroughRead, PeerClient, LiveRing>;

/// The node id every single-node (no `[cluster]`) server uses. Matches the
/// cache engine's cluster-of-one id so turning gossip off leaves ownership
/// byte-identical to a pre-cluster server: one member owns every key.
const SINGLE_NODE_ID: &str = "single";

/// Builds the `/admin/stats` source: a closure that reads the engine's live
/// counters and DRAM usage and stamps them alongside the configured cache
/// budgets. The configured budgets come from the validated config (the hard
/// ceilings); the counters and usage come from the running engine.
fn stats_source(
    config: &verglas_core::config::Config,
    engine: ServerEngine,
    warming: Option<Arc<verglas_tables::warming::WarmProgress>>,
    writeback: Option<Arc<WritebackMetrics>>,
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
            warming: warming.as_ref().map(|p| warming_info(&p.snapshot())),
            writeback: writeback.as_ref().map(|m| {
                let w = m.snapshot();
                WritebackStatsInfo {
                    acked_via_quorum: w.acked_via_quorum,
                    acked_via_write_through: w.acked_via_write_through,
                    mode_transitions: w.mode_transitions,
                    propagated: w.propagated,
                    propagation_failures: w.propagation_failures,
                    fragments_repaired: w.fragments_repaired,
                    fragments_scrubbed: w.fragments_scrubbed,
                    corrupt_fragments_found: w.corrupt_fragments_found,
                }
            }),
        }
    })
}

/// Builds the `GET /metrics` source (#46): a closure that renders the full
/// Prometheus exposition on each scrape. The live request families come from the
/// shared `metrics` registry the S3 front-end records into; the counters and
/// gauges are read from the running `engine` and `backend` at scrape time and
/// stamped into a [`MetricsSnapshot`]. The configured tier capacities come from
/// the validated config (the hard ceilings). Reads only — never on any hot path.
fn metrics_source(
    config: &verglas_core::config::Config,
    metrics: Arc<verglas_core::metrics::NodeMetrics>,
    storage: (ServerEngine, verglas_kv::Store),
    backend: Arc<verglas_backend::BackendStore>,
    writeback: Option<Arc<WritebackMetrics>>,
    telemetry: Option<Arc<verglas_core::telemetry::Telemetry>>,
    mapper: Option<Arc<verglas_tables::mapper::Mapper>>,
) -> admin::MetricsSource {
    use verglas_core::metrics::{MetricsSnapshot, TierSize, WritebackMetricsSnapshot, render};
    use verglas_core::read::ServedTier;

    let dram_capacity = config.cache.dram_bytes.0;
    let nvme_capacity = config.cache.capacity_bytes.0;
    let (engine, kv) = storage;
    Arc::new(move || {
        let c = engine.counters().snapshot();
        // Hits are every lookup a cache tier answered; misses are lookups that
        // had to fill. Peer errors are not misses (they degrade to a fill, which
        // the disk/backend miss already counts), so they are left out here.
        let cache_hits = c.dram_hits + c.disk_hits + c.peer_hits + c.meta_hits;
        let cache_misses = c.dram_misses + c.disk_misses + c.meta_misses;
        // Backend request outcomes: successful fills/heads vs breaker-shed
        // rejections, aggregated across every served bucket's own breaker (#235).
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
                        // The disk tier's live occupancy is not separately
                        // gauged in-process; report the configured ceiling as
                        // both until a disk-usage gauge lands. Capacity is the
                        // load-bearing number for the "how full" panels.
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
            // Write-back fragment-integrity counters (#220), present only when
            // the write-back tier is enabled.
            writeback: writeback.as_ref().map(|m| {
                let w = m.snapshot();
                WritebackMetricsSnapshot {
                    fragments_repaired: w.fragments_repaired,
                    fragments_scrubbed: w.fragments_scrubbed,
                    corrupt_fragments_found: w.corrupt_fragments_found,
                }
            }),
        };
        let mut text = render(&metrics, &snapshot);
        // Per-table families (#60): extend the #46 exposition with the derived
        // per-table series (requests avoided, latency saved, bytes served). Names
        // are resolved from the mapper at scrape time only. Raw counts only — no
        // dollars are ever baked into a counter.
        if let Some(telemetry) = &telemetry {
            telemetry.render_families(table_name_resolver(mapper.as_ref()), &mut text);
        }
        text.push_str(&kv.render_metrics());
        text
    })
}

/// Builds a `TableId` → dotted-name resolver from the live mapper for the
/// telemetry exposition and reports. `_unmapped`/`_overflow` are handled by the
/// rollup; this resolves only real ids, returning `None` (a `table_<id>`
/// fallback) for an id the current map no longer knows.
fn table_name_resolver(
    mapper: Option<&Arc<verglas_tables::mapper::Mapper>>,
) -> impl Fn(u32) -> Option<String> {
    let mapper = mapper.cloned();
    move |id| {
        let mapper = mapper.as_ref()?;
        let mapper_id = verglas_core::telemetry::decode_table_id(id)?;
        mapper
            .state()
            .table(verglas_tables::mapper::TableId(mapper_id))
            .map(|t| t.ident.dotted())
    }
}

/// Maps the warmer's live progress snapshot onto the admin-API DTO.
fn warming_info(
    p: &verglas_tables::warming::WarmProgressSnapshot,
) -> verglas_core::admin::WarmingInfo {
    verglas_core::admin::WarmingInfo {
        tables_started: p.tables_started,
        tables_completed: p.tables_completed,
        files_seen: p.files_seen,
        parquet_files: p.parquet_files,
        block_objects_warmed: p.block_objects_warmed,
        footers_warmed: p.footers_warmed,
        footer_bytes_warmed: p.footer_bytes_warmed,
        footer_gets: p.footer_gets,
        footer_refetches: p.footer_refetches,
        skipped_non_parquet: p.skipped_non_parquet,
        skipped_over_budget: p.skipped_over_budget,
        budget_alerts: p.budget_alerts,
    }
}

/// The serving-path hooks the lifecycle pipeline hands back to the server: the
/// warming progress ledger (for `/admin/stats`) and the heat-feed decorator's
/// sender + organic-yield gate (for wrapping the serving engine).
#[derive(Default)]
struct LifecycleHooks {
    /// Shared catalog watcher used by cache lifecycle and scheduler subscriptions.
    watcher: Option<Arc<verglas_tables::catalog::PollingWatcher>>,
    /// Live warming progress, when warming is on.
    warming: Option<Arc<verglas_tables::warming::WarmProgress>>,
    /// The request-path heat sample sender, when prefetch is on.
    heat_sender: Option<verglas_tables::lifecycle::heat::HeatSender>,
    /// The organic-traffic yield gate the serving reader occupies per read, when
    /// prefetch is on.
    yield_gate: Option<verglas_tables::lifecycle::prefetch::OrganicYield>,
    /// The per-table telemetry hub (#60), when prefetch is on (it needs the
    /// mapper to attribute reads to tables). Cloned into the serving decorator.
    telemetry: Option<Arc<verglas_core::telemetry::Telemetry>>,
    /// The live logical-key map, shared so the serving decorator can resolve a
    /// read's `TableId` and the admin surface can resolve ids back to names.
    mapper: Option<Arc<verglas_tables::mapper::Mapper>>,
}

/// Spawns the table-lifecycle pipeline (#168 warming and #51 prefetch/retire)
/// when a catalog is watched. One REST catalog watcher and one byte budget are
/// shared across both. Warming (when enabled) walks each table's metadata into
/// the pinned store on startup and every commit; prefetch (when enabled) keeps a
/// live logical-key map, feeds a heat ledger from the request path, and repairs
/// the cache after a compaction commit by prefetching the rewritten files' hot
/// chunks under one budgeted, organic-yielding executor. Returns the serving
/// hooks; all worker tasks are detached and live for the process.
fn spawn_lifecycle(
    config: &verglas_core::config::Config,
    engine: ServerEngine,
    catalog_source: Option<verglas_catalog::RestCatalogSource>,
) -> Result<LifecycleHooks, verglas_core::config::ConfigError> {
    let Some(catalog) = config.catalog.as_ref() else {
        return Ok(LifecycleHooks::default());
    };
    use verglas_tables::catalog::{PollingWatcher, WatcherOptions};
    use verglas_tables::warming::budget::TokenBucket;

    // One watcher, one byte budget shared by warming and prefetch. Resolving the
    // bearer token can fail (missing credentials file); config validation has
    // already caught that at load, so this only surfaces a late file change.
    let source = catalog_source.ok_or_else(|| {
        verglas_core::config::ConfigError::Invalid(
            "catalog.uri",
            "catalog gateway source is missing".to_owned(),
        )
    })?;
    let options = WatcherOptions::from_config(catalog);
    // Iceberg REST polling only. Hosted-catalog push notify is a cloud concern;
    // this process does not open a Verglas websocket to the catalog origin.
    let watcher = Arc::new(PollingWatcher::spawn(source, options));
    let w = &config.cache.warming;
    let budget = Arc::new(if w.byte_budget_bytes_per_sec.0 == 0 {
        TokenBucket::unlimited()
    } else {
        TokenBucket::new(w.byte_budget_bytes_per_sec.0, w.byte_budget_bytes_per_sec.0)
    });

    let mut hooks = LifecycleHooks {
        watcher: Some(watcher.clone()),
        ..LifecycleHooks::default()
    };
    if config.cache.warming.enabled {
        hooks.warming = Some(spawn_warming(
            config,
            engine.clone(),
            watcher.clone(),
            budget.clone(),
        ));
        eprintln!(
            "verglas-server {VERSION} eager metadata warming enabled (watching {})",
            catalog.uri
        );
    }
    if config.cache.prefetch.enabled {
        let prefetch = spawn_prefetch(config, engine, watcher, budget);
        hooks.heat_sender = Some(prefetch.heat_sender);
        hooks.yield_gate = Some(prefetch.yield_gate);
        hooks.telemetry = Some(prefetch.telemetry);
        hooks.mapper = Some(prefetch.mapper);
        eprintln!(
            "verglas-server {VERSION} snapshot-driven prefetch enabled (watching {})",
            catalog.uri
        );
    }
    Ok(hooks)
}

/// Bridges on-prem catalog polling events into durable scheduler invocations.
fn spawn_scheduler_catalog_events(
    watcher: Arc<verglas_tables::catalog::PollingWatcher>,
    ingress: Arc<platform::SchedulerIngress>,
) -> tokio::task::JoinHandle<()> {
    use verglas_tables::catalog::CatalogWatcher;
    let mut events = watcher.subscribe();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(change) => {
                    let Some(snapshot_id) = change.new_snapshot else {
                        continue;
                    };
                    let table = change.table.dotted();
                    let committed_at = chrono::Utc::now();
                    let mut event = verglas_sdk::worker::CloudEvent::new(
                        format!("{table}:{snapshot_id}"),
                        "urn:verglas:catalog:on-prem",
                        "org.apache.iceberg.snapshot.committed",
                    );
                    event.subject = Some(table.clone());
                    event.time = Some(committed_at.to_rfc3339());
                    event.datacontenttype = Some("application/json".to_owned());
                    event.data = Some(serde_json::json!({
                        "table": table,
                        "snapshotId": snapshot_id.to_string()
                    }));
                    if let Err(error) = ingress.event(event, committed_at).await {
                        tracing::warn!("scheduler catalog update enqueue failed: {error}");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        "scheduler catalog subscription lagged by {skipped} event(s); object idempotency keeps replay safe"
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    })
}

/// Spawns the eager warming coordinator (#168) over the shared watcher + budget.
fn spawn_warming(
    config: &verglas_core::config::Config,
    engine: ServerEngine,
    watcher: Arc<verglas_tables::catalog::PollingWatcher>,
    budget: Arc<verglas_tables::warming::budget::TokenBucket>,
) -> Arc<verglas_tables::warming::WarmProgress> {
    use verglas_tables::warming::{WarmConfig, WarmSource, Warmer, WarmingCoordinator};

    let w = &config.cache.warming;
    // Alert (and cap) if a table's footer working set would exceed the metadata
    // store's DRAM carve — the pathological-tiny-file guard from #50.
    let meta_budget_bytes = (config.cache.dram_bytes.0 as f64 * config.cache.meta_fraction) as u64;
    let warm_config = WarmConfig {
        concurrency: w.concurrency,
        footer_read_bytes: w.footer_read_bytes.0,
        meta_budget_bytes,
    };
    let reader: Arc<dyn WarmSource> = Arc::new(engine);
    let warmer = Arc::new(Warmer::new(reader, warm_config, budget));
    let progress = warmer.progress();
    WarmingCoordinator::new(warmer, watcher).spawn();
    progress
}

/// Spawns the #51 prefetch pipeline: the logical-key map + its updater, the heat
/// ledger + its request-path aggregator, the shared budgeted prefetch executor,
/// and the prefetch coordinator that repairs the cache on each compaction
/// commit. Returns the request-path heat sender, organic-yield gate, the
/// per-table telemetry hub, and the live mapper the serving decorator and admin
/// surface use.
struct PrefetchHandles {
    heat_sender: verglas_tables::lifecycle::heat::HeatSender,
    yield_gate: verglas_tables::lifecycle::prefetch::OrganicYield,
    telemetry: Arc<verglas_core::telemetry::Telemetry>,
    mapper: Arc<verglas_tables::mapper::Mapper>,
}

fn spawn_prefetch(
    config: &verglas_core::config::Config,
    engine: ServerEngine,
    watcher: Arc<verglas_tables::catalog::PollingWatcher>,
    budget: Arc<verglas_tables::warming::budget::TokenBucket>,
) -> PrefetchHandles {
    use verglas_tables::fetch::ObjectReadFetch;
    use verglas_tables::lifecycle::PrefetchCoordinator;
    use verglas_tables::lifecycle::heat::{self, EpochClock, HeatChannel, HeatLedger};
    use verglas_tables::lifecycle::prefetch::{PrefetchConfig, PrefetchExecutor};
    use verglas_tables::lifecycle::retire::RetirementScheduler;
    use verglas_tables::mapper::Mapper;
    use verglas_tables::mapper::updater::MapUpdater;
    use verglas_tables::warming::WarmSource;

    let p = &config.cache.prefetch;
    let prefetch_config = PrefetchConfig {
        enabled: true,
        concurrency: p.concurrency,
        footer_read_bytes: p.footer_read_bytes.0,
        heat_threshold: 0.0,
        organic_yield_k: p.organic_yield_k,
        max_queue: p.max_queue,
    };
    let clock = EpochClock::new(p.heat_epoch_secs);

    // The through-cache metadata reader (its reads warm the pinned meta store).
    let fetch = Arc::new(ObjectReadFetch::new(engine.clone()));

    // Live logical-key map, kept current by its own single-writer updater.
    let mapper = Arc::new(Mapper::new());
    MapUpdater::new(mapper.clone(), watcher.clone(), fetch.clone()).spawn();

    // Heat ledger fed from the request path via a drop-on-full channel and
    // folded by a background aggregator (classify() runs off the hot path).
    let ledger = Arc::new(HeatLedger::new(heat::DEFAULT_ALPHA, p.heat_table_cap));
    let mut channel = HeatChannel::new(p.heat_channel_capacity);
    let sender = channel.sender();
    if let Some(rx) = channel.take_receiver() {
        tokio::spawn(heat::run_aggregator(
            rx,
            mapper.clone(),
            ledger.clone(),
            clock,
        ));
    }

    // The shared budgeted executor and the coordinator that drives it. The
    // engine also doubles as the evict-first demotion sink (#198): a compaction
    // commit demotes its removed files here, and the coordinator's grace sweep
    // hard-evicts them once their snapshots expire (Arc bump — the reader and the
    // demoter are the same engine).
    let demoter: Arc<dyn verglas_cache::BlockDemoter> = Arc::new(engine.clone());
    let reader: Arc<dyn WarmSource> = Arc::new(engine);
    let executor = Arc::new(PrefetchExecutor::spawn(reader, budget, &prefetch_config));
    let yield_gate = executor.yield_gate();
    let retire = Arc::new(RetirementScheduler::new());
    // The retirement schedule and replay watermarks persist next to the cache
    // (#305): a restart re-demotes grace-pending objects instead of amnestying
    // them, and commits made while the server was down are replayed and
    // retired at the next catalog event.
    let state_path = config.cache.dir.join("retire-state.json");
    let coordinator = PrefetchCoordinator::new(
        watcher,
        fetch,
        mapper.clone(),
        ledger,
        executor,
        retire,
        Some(demoter),
        Some(state_path),
        prefetch_config,
        clock,
    );
    coordinator.restore(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
    );
    coordinator.spawn();

    // Per-table telemetry (#60): one hub the serving decorator records into, with
    // a detached task that drains the per-core tapes into the rollup ~1s. Fully
    // off the serving path — a slow rollup never touches serving, and the drain
    // cadence bounds how long an event waits before it is counted.
    let telemetry = Arc::new(verglas_core::telemetry::Telemetry::new());
    {
        let telemetry = telemetry.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                telemetry.roll_up();
            }
        });
    }

    PrefetchHandles {
        heat_sender: sender,
        yield_gate,
        telemetry,
        mapper,
    }
}

/// Parses `--config <path>` from the command line, loading and validating the
/// file when present. Exits non-zero with the loader's actionable message on
/// any failure — nothing may bind before the config is known good.
fn load_config_from_args() -> Option<LoadedServerConfig> {
    let environment_mode = std::env::args().any(|arg| arg == "--environment");
    let file_mode = std::env::args().any(|arg| arg == "--config");
    if environment_mode && file_mode {
        eprintln!("verglas-server: choose either --environment or --config, not both");
        std::process::exit(1);
    }
    if environment_mode {
        return match environment::EnvironmentConfig::load() {
            Ok(loaded) => Some(LoadedServerConfig {
                config: loaded.config,
                endpoint_credentials: Some(loaded.endpoint_credentials),
            }),
            Err(error) => {
                eprintln!("verglas-server: {error}");
                std::process::exit(1);
            }
        };
    }
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg != "--config" {
            continue;
        }
        let Some(path) = args.next() else {
            eprintln!("verglas-server: --config requires a path to a TOML config file");
            std::process::exit(1);
        };
        match verglas_core::config::Config::load(std::path::Path::new(&path)) {
            Ok(config) => {
                return Some(LoadedServerConfig {
                    config,
                    endpoint_credentials: None,
                });
            }
            Err(error) => {
                eprintln!("verglas-server: {error}");
                std::process::exit(1);
            }
        }
    }
    None
}

/// A startup configuration and, for Compose environment mode, endpoint
/// credentials supplied outside the serializable TOML schema.
struct LoadedServerConfig {
    /// The validated server settings.
    config: verglas_core::config::Config,
    /// Static endpoint credentials supplied by Compose.
    endpoint_credentials: Option<(String, String)>,
}

/// Reports each listener's kernel-assigned address to the file named by
/// `verglas-server --ports-file` (issue #194). When `verglas dev` (or a test) asks the
/// server to bind ephemeral ports (`:0`), it names a ports file the server
/// appends one `<role> <ip:port>` line to as each listener binds, so the parent
/// learns the real ports without probing them free first (the TOCTOU race #194
/// removes). Append-only, one line per listener, so a partial read never yields
/// a torn line. Write-only telemetry off every serving path.
struct PortsReport {
    /// The file the parent named; each report opens it in append mode.
    path: std::path::PathBuf,
}

impl PortsReport {
    /// Appends `role addr` for one bound listener. Best-effort: a report failure
    /// is logged and swallowed — it never fails the server, since serving does
    /// not depend on the parent learning the port.
    fn report(&self, role: &str, addr: std::net::SocketAddr) {
        use std::io::Write as _;
        let line = format!("{role} {addr}\n");
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            Ok(mut file) => {
                if let Err(e) = file.write_all(line.as_bytes()) {
                    eprintln!("verglas-server: could not write {role} port to ports file: {e}");
                }
            }
            Err(e) => eprintln!(
                "verglas-server: could not open ports file `{}`: {e}",
                self.path.display()
            ),
        }
    }
}

/// The process-wide ports reporter, set once from `--ports-file` before any
/// listener binds. A global sink keeps the report call out of every bind
/// function's signature; it is write-only and never read on a serving path.
static PORTS_REPORT: OnceLock<Option<PortsReport>> = OnceLock::new();

/// Records a bound listener's real address for the parent (issue #194). A no-op
/// when the server was launched without `--ports-file`.
fn report_port(role: &str, addr: std::net::SocketAddr) {
    if let Some(Some(report)) = PORTS_REPORT.get() {
        report.report(role, addr);
    }
}

/// Parses `--ports-file <path>` from the command line (issue #194). Returns the
/// path `verglas dev` named for the server to report its resolved ports to, or
/// `None` when the server binds fixed ports and needs no reporting.
fn ports_file_from_args() -> Option<std::path::PathBuf> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--ports-file" {
            return args.next().map(std::path::PathBuf::from);
        }
    }
    None
}

/// Reserves a kernel-assigned loopback UDP port for the gossip listener when
/// `verglas dev` requests ephemeral ports (issue #194). Unlike the S3 and admin
/// listeners — bound once and served on, so their port is fixed the instant it
/// is chosen — chitchat 0.11 binds its own gossip socket and fixes the
/// advertised address before that bind, so the resolved port must be known up
/// front. The reservation is released immediately and chitchat rebinds it
/// microseconds later in this same task; that in-process window is categorically
/// smaller than the parent-probe / child-bind-seconds-later race #194 removes,
/// and a lost race fails startup loudly rather than serving wrong bytes.
/// Extension point: a chitchat transport that accepts a pre-bound socket would
/// close even this window.
fn reserve_ephemeral_udp() -> std::io::Result<std::net::SocketAddr> {
    std::net::UdpSocket::bind("127.0.0.1:0")?.local_addr()
}

/// Reserves a kernel-assigned loopback TCP port for the peer-fetch listener when
/// `verglas dev` requests ephemeral ports (issue #194). The advertised peer
/// address must be gossiped before the peer server binds (the gossip agent
/// starts first), so the port is resolved here and the server binds it shortly
/// after — the same small in-process window as the gossip reservation.
fn reserve_ephemeral_tcp() -> std::io::Result<std::net::SocketAddr> {
    std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()
}

/// Generates a dev access keypair for servers started without `[auth]`. The
/// caller prints it once so an engine can still be pointed at this node.
fn generate_auth() -> (String, String) {
    use std::hash::{BuildHasher, RandomState};
    // RandomState is seeded from OS entropy; good enough for dev keys.
    let r = || RandomState::new().hash_one(0u64);
    (
        format!("VG{:016X}", r()),
        format!("{:016x}{:016x}", r(), r()),
    )
}

/// The static keypair the S3 endpoint accepts. When `[auth]` names a
/// credentials file, the keypair is read from it (AWS format, mode 0600) — the
/// secret never lives in `config.toml`. With no `[auth]` the server generates
/// an ephemeral pair and prints it once so an engine can still connect.
fn resolve_auth(config: &verglas_core::config::Config) -> Result<(String, String), String> {
    match &config.auth {
        Some(auth) => {
            let profile = auth
                .credentials_profile
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("default");
            let text = std::fs::read_to_string(&auth.credentials_file).map_err(|e| {
                format!(
                    "cannot read auth.credentials_file `{}`: {e}",
                    auth.credentials_file
                )
            })?;
            verglas_backend::read_aws_keypair(&text, profile).ok_or_else(|| {
                format!(
                    "auth.credentials_file `{}` has no complete `[{profile}]` profile (need aws_access_key_id and aws_secret_access_key)",
                    auth.credentials_file
                )
            })
        }
        None => {
            let (access_key_id, secret_access_key) = generate_auth();
            println!(
                "verglas-server generated access keys: access_key_id={access_key_id} secret_access_key={secret_access_key}"
            );
            Ok((access_key_id, secret_access_key))
        }
    }
}

/// Builds the [`LocalAccess`](verglas_core::admin::LocalAccess) snapshot the
/// `/admin/access` probe returns (issue #287) from the server's resolved config
/// and the endpoint access key id. The S3 endpoint is the loopback data port
/// (with the `https` scheme when the endpoint terminates TLS). The local query
/// and catalog proxy URIs are advertised so an authenticated SDK client uses
/// the composed on-prem service. Region falls back to `us-east-1` when the
/// backend leaves it unset.
///
/// Takes only the access key id, never the secret: the admin surface is
/// unauthenticated and host-scoped, so the paired secret must never travel over
/// it. The CLI reads the secret from the server's 0600 credentials file instead.
fn build_local_access(
    config: &verglas_core::config::Config,
    access_key_id: &str,
    s3_port: u16,
    admin_port: u16,
) -> verglas_core::admin::LocalAccess {
    let scheme = if config.listen.tls.is_some() {
        "https"
    } else {
        "http"
    };
    verglas_core::admin::LocalAccess {
        s3_endpoint: format!("{scheme}://127.0.0.1:{s3_port}"),
        query_uri: format!("{scheme}://127.0.0.1:{admin_port}"),
        catalog_uri: config
            .catalog
            .as_ref()
            .map(|_| format!("{scheme}://127.0.0.1:{admin_port}/catalog")),
        warehouse: config
            .catalog
            .as_ref()
            .and_then(|catalog| catalog.warehouse.clone()),
        region: config
            .backend
            .region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_owned()),
        bucket: config.backend.bucket.clone(),
        access_key_id: Some(access_key_id.to_owned()),
    }
}

/// Builds the private connection used for table-aware work: the configured
/// local catalog proxy plus the server's loopback S3 cache endpoint.
fn internal_connection(
    config: &verglas_core::config::Config,
    credentials: &(String, String),
    s3_port: u16,
    admin_port: u16,
) -> Result<verglas_iceberg::Connection, Box<dyn std::error::Error>> {
    let catalog = config
        .catalog
        .as_ref()
        .ok_or("internal catalog connection requested without [catalog]")?;
    if catalog.sigv4_enabled() {
        return Err(
            "private table/query catalog operations do not yet support SigV4 catalogs".into(),
        );
    }
    let scheme = if config.listen.tls.is_some() {
        "https"
    } else {
        "http"
    };
    Ok(verglas_iceberg::Connection {
        catalog_uri: format!("{scheme}://127.0.0.1:{admin_port}/catalog"),
        token: catalog.resolve_bearer_token()?,
        warehouse: catalog.warehouse.clone(),
        s3_endpoint: Some(format!("{scheme}://127.0.0.1:{s3_port}")),
        region: config
            .backend
            .region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_owned()),
        access_key_id: Some(credentials.0.clone()),
        secret_access_key: Some(credentials.1.clone()),
    })
}

/// Renders a `verglas-query` config (plus a credentials file carrying the same
/// endpoint keypair) pointing the worker at the server's S3 cache endpoint and
/// the server's local catalog proxy, and returns the dispatcher that spawns it on
/// demand — or `None` when `[query_worker]` is unset or the files could not
/// be rendered. `/v1/query` has no embedded engine; without a dispatcher it
/// returns service unavailable.
fn build_query_worker_dispatcher(
    config: &verglas_core::config::Config,
    credentials: &(String, String),
    resolved_s3_port: u16,
    resolved_admin_port: u16,
) -> Option<Arc<verglas_server::query_worker::QueryWorkerDispatcher>> {
    let query_worker = config.query_worker.as_ref()?;
    let config_path = match render_execution_worker_config(
        config,
        credentials,
        resolved_s3_port,
        resolved_admin_port,
        "query-worker",
    ) {
        Ok(path) => path,
        Err(error) => {
            eprintln!("verglas-server {VERSION} cannot configure query worker: {error}");
            return None;
        }
    };
    Some(Arc::new(
        verglas_server::query_worker::QueryWorkerDispatcher::new(
            std::path::PathBuf::from(&query_worker.binary),
            config_path,
        ),
    ))
}

/// Builds the isolated logical write-role dispatcher.
fn build_write_worker_dispatcher(
    config: &verglas_core::config::Config,
    credentials: &(String, String),
    resolved_s3_port: u16,
    resolved_admin_port: u16,
) -> Option<Arc<verglas_server::write_worker::WriteWorkerDispatcher>> {
    let write_worker = config.write_worker.as_ref()?;
    let config_path = match render_execution_worker_config(
        config,
        credentials,
        resolved_s3_port,
        resolved_admin_port,
        "write-worker",
    ) {
        Ok(path) => path,
        Err(error) => {
            eprintln!("verglas-server {VERSION} cannot configure write worker: {error}");
            return None;
        }
    };
    Some(Arc::new(
        verglas_server::write_worker::WriteWorkerDispatcher::new(
            std::path::PathBuf::from(&write_worker.binary),
            config_path,
        ),
    ))
}

/// Renders the common cache and catalog connection used by execution roles.
fn render_execution_worker_config(
    config: &verglas_core::config::Config,
    credentials: &(String, String),
    resolved_s3_port: u16,
    resolved_admin_port: u16,
    directory: &str,
) -> Result<std::path::PathBuf, String> {
    let dir = config.cache.dir.join(directory);
    std::fs::create_dir_all(&dir).map_err(|error| format!("create {}: {error}", dir.display()))?;
    let credentials_path = dir.join("credentials");
    std::fs::write(
        &credentials_path,
        format!(
            "[default]\naws_access_key_id = {}\naws_secret_access_key = {}\n",
            credentials.0, credentials.1
        ),
    )
    .map_err(|error| format!("write role credentials: {error}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&credentials_path, std::fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("restrict role credentials: {error}"))?;
    }
    let scheme = if config.listen.tls.is_some() {
        "https"
    } else {
        "http"
    };
    let region = config
        .backend
        .region
        .clone()
        .unwrap_or_else(|| "us-east-1".to_owned());
    let mut catalog = config
        .catalog
        .as_ref()
        .ok_or_else(|| "execution roles require [catalog]".to_owned())?
        .clone();
    catalog.uri = format!("{scheme}://127.0.0.1:{resolved_admin_port}/catalog");
    catalog.credentials_file = None;
    catalog.credentials_profile = None;
    catalog.bearer_token = None;
    catalog.sigv4_region = None;
    catalog.sigv4_signing_name = None;
    let catalog_toml =
        toml::to_string(&catalog).map_err(|error| format!("serialize role catalog: {error}"))?;
    let rendered = format!(
        "[listen]\nadmin_port = 0\n\n\
         [cache]\ns3_endpoint = \"{scheme}://127.0.0.1:{resolved_s3_port}\"\nregion = \"{region}\"\ncredentials_file = \"{credentials}\"\n\n\
         [catalog]\n{catalog_toml}",
        credentials = credentials_path.display(),
    );
    let config_path = dir.join("config.toml");
    std::fs::write(&config_path, rendered)
        .map_err(|error| format!("write role config: {error}"))?;
    Ok(config_path)
}

/// Opens the internal catalog handle used by compaction, graph, vector, and
/// worker subsystems through the local on-prem catalog proxy.
async fn build_tables_catalog(
    config: &verglas_core::config::Config,
    credentials: &(String, String),
    s3_port: u16,
    admin_port: u16,
) -> Result<Arc<dyn iceberg::Catalog>, Box<dyn std::error::Error>> {
    let connection = internal_connection(config, credentials, s3_port, admin_port)?;
    Ok(verglas_iceberg::catalog::open_catalog(&connection).await?)
}

/// The admin listener's bind address: explicit `VERGLAS_ADMIN_ADDR` (used by
/// `verglas dev` and tests, `127.0.0.1:0` for a kernel-assigned port, #194),
/// then the validated config's admin port (loopback — the admin API is a
/// private surface), then the compiled-in default.
fn admin_listen_addr(config: Option<&verglas_core::config::Config>) -> String {
    std::env::var("VERGLAS_ADMIN_ADDR").unwrap_or_else(|_| match config {
        Some(config) => format!("127.0.0.1:{}", config.listen.admin_port),
        None => DEFAULT_ADMIN_ADDR.to_owned(),
    })
}

/// Drives the server's execution gateway as a [`verglas_s3::ServingApi`], so
/// the SigV4-gated S3 data port answers the same query and write requests as
/// the loopback admin listener. The Cloudflare
/// edge re-signs a cache-pathed `/v1` request with the cache keypair and
/// forwards it to the data port; the s3s route validates the signature and hands
/// the buffered request here. This reconstructs an axum request, runs it through
/// the router once, and buffers the response. The router is a `Router<()>` built
/// from the configured isolated role dispatchers.
struct V1ServingApi {
    /// The state-erased query/write dispatch router.
    router: axum::Router,
}

#[async_trait::async_trait]
impl verglas_s3::ServingApi for V1ServingApi {
    async fn handle(&self, req: verglas_s3::ApiRequest) -> verglas_s3::ApiResponse {
        let verglas_s3::ApiRequest {
            tenant,
            method,
            uri,
            headers,
            body,
        } = req;
        let mut builder = axum::http::Request::builder().method(method).uri(uri);
        if let Some(destination) = builder.headers_mut() {
            *destination = headers;
        }
        let mut request = match builder.body(axum::body::Body::from(body)) {
            Ok(request) => request,
            Err(error) => {
                return v1_error_response(format!("building the /v1 request failed: {error}"));
            }
        };
        request
            .extensions_mut()
            .insert(verglas_rest::kv::AuthenticatedKvPrincipal { tenant });
        // The axum router is infallible (its Service error is `Infallible`), so
        // `oneshot` cannot return a transport error here.
        let response = match self.router.clone().oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        };
        let (parts, body) = response.into_parts();
        let body = match axum::body::to_bytes(body, usize::MAX).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return v1_error_response(format!("reading the /v1 response body failed: {error}"));
            }
        };
        verglas_s3::ApiResponse {
            status: parts.status,
            headers: parts.headers,
            body,
        }
    }
}

/// A bare 500 [`verglas_s3::ApiResponse`] for the rare failures reconstructing or
/// buffering a `/v1` round-trip (never the handler's own errors, which the
/// router renders normally).
fn v1_error_response(message: String) -> verglas_s3::ApiResponse {
    verglas_s3::ApiResponse {
        status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        headers: axum::http::HeaderMap::new(),
        body: axum::body::Bytes::from(message),
    }
}

/// Serves the admin API on the pre-bound `listener` until the process is
/// interrupted. The caller binds so the resolved port is known (and reported,
/// #194) before serving; the `purger` (present only when a cache engine exists)
/// enables the `POST /cache/purge` endpoint (issue #138).
async fn serve_admin(
    listener: tokio::net::TcpListener,
    health: admin::Health,
    slots: admin::Slots,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = admin::router(VERSION, health, slots);
    let local_addr = listener.local_addr()?;

    eprintln!("verglas-server {VERSION} admin API listening on http://{local_addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

/// Binds the S3 endpoint engines point at and serves until the process is
/// interrupted. Reads are served by the hybrid cache engine (issue #12) over
/// the read passthrough, filling misses from the origin bucket; writes pass
/// through durably by design and invalidate the cache engine's key→ETag
/// mappings before the client is acked (#9/#21 ordering).
#[allow(clippy::too_many_arguments)]
async fn serve_s3(
    config: &verglas_core::config::Config,
    s3_listener: Option<tokio::net::TcpListener>,
    credentials: (String, String),
    serving: verglas_tables::lifecycle::heat::HeatFeed<ServerEngine>,
    engine: ServerEngine,
    registry: Arc<verglas_backend::BackendStore>,
    node_id: NodeId,
    agent: Option<Arc<ClusterAgent>>,
    fragment_store: Option<LocalFragmentStore>,
    writeback_metrics: Option<Arc<WritebackMetrics>>,
    node_metrics: Arc<verglas_core::metrics::NodeMetrics>,
    serving_api: Option<Arc<dyn verglas_s3::ServingApi>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // The plain path serves on the pre-bound listener (bound and reported in
    // `serve`, #194). The TLS path (production, fixed ports) binds by address
    // here: explicit VERGLAS_S3_ADDR, then the validated config's S3 port on all
    // interfaces — engines connect from other hosts.
    let listen_addr = std::env::var("VERGLAS_S3_ADDR")
        .unwrap_or_else(|_| format!("0.0.0.0:{}", config.listen.s3_port));
    // Listings always pass straight through to the origin (never cached), so
    // the lister is wired to the backend registry directly, not the reader.
    let lister = Arc::new(verglas_s3::PassthroughList::new(registry.clone()));
    // The raw cache engine (not the heat-feed decorator) doubles as the write
    // path's invalidator (engine handles are one Arc bump).
    let invalidation = Arc::new(engine);

    // Feature-gate at construction, never per request: when the write-back tier
    // is enabled we wrap the read and write paths in its reader/writer; when it
    // is off the router gets exactly today's passthrough writer and serving
    // reader, so the disabled tier adds zero hot-path cost.
    match (fragment_store, writeback_metrics) {
        (Some(store), Some(metrics)) if config.cache.writeback.enabled => {
            let origin = Arc::new(PassthroughWrite::new(registry.clone()));
            let tier =
                build_writeback_tier(config, serving, origin, node_id, agent, store, metrics)?;
            run_s3(
                tier.reader,
                tier.writer,
                lister,
                invalidation,
                registry,
                credentials,
                s3_listener,
                &listen_addr,
                config.listen.domain.as_deref(),
                config.listen.tls.as_ref(),
                node_metrics.clone(),
                serving_api,
            )
            .await
        }
        _ => {
            let writer = PassthroughWrite::new(registry.clone());
            run_s3(
                serving,
                writer,
                lister,
                invalidation,
                registry,
                credentials,
                s3_listener,
                &listen_addr,
                config.listen.domain.as_deref(),
                config.listen.tls.as_ref(),
                node_metrics.clone(),
                serving_api,
            )
            .await
        }
    }
}

/// Binds the S3 listener and serves the assembled router until shutdown. Shared
/// by the write-back and passthrough write paths so only the wrapper types
/// differ between them.
#[allow(clippy::too_many_arguments)]
async fn run_s3<R, W>(
    reader: R,
    writer: W,
    lister: Arc<verglas_s3::PassthroughList>,
    invalidation: Arc<ServerEngine>,
    registry: Arc<verglas_backend::BackendStore>,
    credentials: (String, String),
    s3_listener: Option<tokio::net::TcpListener>,
    listen_addr: &str,
    domain: Option<&str>,
    tls: Option<&verglas_core::config::Tls>,
    metrics: Arc<verglas_core::metrics::NodeMetrics>,
    serving_api: Option<Arc<dyn verglas_s3::ServingApi>>,
) -> Result<(), Box<dyn std::error::Error>>
where
    R: verglas_core::read::ObjectRead,
    W: verglas_core::write::ObjectWrite,
{
    // The router forwards unmodeled bucket-config operations (HeadBucket,
    // GetBucketLocation) to the origin instead of returning 501 (#152), accepts
    // virtual-hosted-style addressing when a base domain is configured (#11),
    // and — when `serving_api` is present — answers the server's `/v1` API on
    // this SigV4-gated surface too. Path-style always works.
    let bucket = registry.bucket_set();
    let app = verglas_rest::compose_s3(
        reader,
        writer,
        lister,
        invalidation,
        Some(credentials),
        Some(registry),
        serving_api,
        domain,
        Some(metrics),
    );

    // TLS termination (#11): `[listen.tls]` serves HTTPS from the configured
    // cert and key (reloaded on SIGHUP); absent serves plain HTTP for local dev
    // on the pre-bound listener (its port already resolved and reported, #194).
    match tls {
        Some(tls) => serve_s3_tls(listen_addr, app, tls, &bucket).await,
        None => {
            let listener =
                s3_listener.ok_or("plain S3 path requires a pre-bound listener from `serve`")?;
            serve_s3_plain(listener, app, &bucket).await
        }
    }
}

/// Serves the S3 router as plain HTTP (local-dev default) on the pre-bound
/// `listener` until interrupted.
async fn serve_s3_plain(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    bucket_set: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let local_addr = listener.local_addr()?;

    eprintln!(
        "verglas-server {VERSION} serving S3 on http://{local_addr} (backend bucket set: {bucket_set})"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

/// Serves the S3 router over TLS from the configured cert and key.
async fn serve_s3_tls(
    listen_addr: &str,
    app: axum::Router,
    tls_config: &verglas_core::config::Tls,
    bucket_set: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr: std::net::SocketAddr = listen_addr.parse()?;
    tls::install_crypto_provider();
    let rustls_config = tls::load_config(tls_config).await?;
    tls::spawn_reload_on_sighup(
        rustls_config.clone(),
        tls_config.cert_path.clone(),
        tls_config.key_path.clone(),
    );

    // Ctrl-C triggers a graceful shutdown with a bounded drain window, mirroring
    // the plain listener's behavior.
    let handle = axum_server::Handle::new();
    {
        let handle = handle.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            handle.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
        });
    }

    eprintln!(
        "verglas-server {VERSION} serving S3 on https://{addr} (TLS terminated here; cert reloads on SIGHUP; backend bucket set: {bucket_set})"
    );

    axum_server::bind_rustls(addr, rustls_config)
        .handle(handle)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

/// Reserves one quarter of configured origin concurrency for repeated aligned
/// cache tails. The remaining three quarters stay available to user reads; very
/// small origin budgets disable partial-read overfetch entirely.
fn background_fill_limit(max_concurrent_requests: usize) -> usize {
    max_concurrent_requests / 4
}

mod tls;

/// Assembles the write-back tier (#180): the fragment transport (local store +
/// peer RPC), the live-membership view, the quorum coordinator over the origin
/// writer, and the reader/writer wrappers. Spawns the node-loss repair loop when
/// clustered. Replays any dirty journal from a previous run.
#[allow(clippy::too_many_arguments)]
fn build_writeback_tier(
    config: &verglas_core::config::Config,
    serving: verglas_tables::lifecycle::heat::HeatFeed<ServerEngine>,
    origin: Arc<PassthroughWrite>,
    node_id: NodeId,
    agent: Option<Arc<ClusterAgent>>,
    store: LocalFragmentStore,
    metrics: Arc<WritebackMetrics>,
) -> Result<
    WritebackTier<verglas_tables::lifecycle::heat::HeatFeed<ServerEngine>, PassthroughWrite>,
    Box<dyn std::error::Error>,
> {
    let wb = &config.cache.writeback;
    let policy = Arc::new(build_writeback_policy(wb));
    let journals = Arc::new(verglas_write::JournalStore::open(&config.cache.dir)?);
    let fragment_client = build_fragment_client(config, agent.as_ref());
    let transport = Arc::new(PeerFragmentTransport::new(
        node_id.clone(),
        store,
        fragment_client,
    ));
    let membership: Arc<dyn LiveMembership> = match &agent {
        Some(agent) => Arc::new(AgentMembership::new(agent.clone())),
        None => Arc::new(SingleNodeMembership::new(node_id)),
    };
    let coordinator = Arc::new(WriteCoordinator::new(
        transport,
        membership.clone(),
        journals,
        metrics,
        Arc::clone(&origin),
        std::time::Duration::from_millis(wb.ack_deadline_ms),
    ));
    let single_node = membership.is_single_node();
    // Repair only makes progress when membership can change; a single-node
    // server never opens the window.
    if agent.is_some() {
        let _repair = verglas_write::spawn_repair_loop(
            Arc::clone(&coordinator),
            membership,
            std::time::Duration::from_secs(2),
        );
    }
    // The scrubber runs regardless of cluster mode: silent bit-rot has no
    // membership event, so even a single node must scrub its fragments to catch
    // corruption before it accumulates past `m` (#220).
    let _scrub = verglas_write::spawn_scrub_loop(
        Arc::clone(&coordinator),
        std::time::Duration::from_secs(wb.scrub_interval_secs),
    );
    if single_node {
        eprintln!(
            "verglas-server {VERSION} write-back tier enabled (single-node, ack_deadline={}ms) — opt-in per prefix; PUTs fast-ack from local durability (degenerate k=1 m=0 w=1) and propagate to the origin in the background; a commit awaits its data files' propagation via the barrier",
            wb.ack_deadline_ms
        );
    } else {
        eprintln!(
            "verglas-server {VERSION} write-back tier enabled (k={} m={} w={}, ack_deadline={}ms) — opt-in per prefix, degrades to write-through below quorum (#180)",
            wb.k, wb.m, wb.w, wb.ack_deadline_ms
        );
    }
    Ok(WritebackTier::new(coordinator, serving, origin, policy))
}

/// Builds the opt-in policy from config: each configured prefix, or — when the
/// tier is enabled with no prefixes — an implicit rule matching every key.
fn build_writeback_policy(wb: &verglas_core::config::Writeback) -> WritebackPolicy {
    if !wb.enabled {
        return WritebackPolicy::disabled();
    }
    if wb.prefixes.is_empty() {
        return WritebackPolicy::new(vec![PrefixRule {
            prefix: String::new(),
            k: wb.k,
            m: wb.m,
            w: wb.w,
        }]);
    }
    let rules = wb
        .prefixes
        .iter()
        .map(|p| PrefixRule {
            prefix: p.prefix.clone(),
            k: p.k.unwrap_or(wb.k),
            m: p.m.unwrap_or(wb.m),
            w: p.w.unwrap_or(wb.w),
        })
        .collect();
    WritebackPolicy::new(rules)
}

/// Builds the fragment RPC client (#180): resolves peers through the agent's
/// live membership when clustered, else a disabled client (single-node places
/// only on itself, and then only after a write-through fallback never fires).
/// The request timeout is looser than a block fetch's because a fragment carries
/// a whole shard and its fsync is the durability the ack waits on.
fn build_fragment_client(
    config: &verglas_core::config::Config,
    agent: Option<&Arc<ClusterAgent>>,
) -> FragmentClient {
    match (config.cluster.as_ref(), agent) {
        (Some(cluster), Some(agent)) => FragmentClient::new(
            agent.resolver(),
            cluster.secret.clone(),
            std::time::Duration::from_millis(cluster.peer_connect_timeout_ms),
            std::time::Duration::from_secs(30),
        ),
        _ => FragmentClient::disabled(),
    }
}

/// Whether the operator has explicitly opted out of the backend startup probe
/// (#233) via `VERGLAS_DEV_ALLOW_MISSING_ORIGIN`. Truthy values are `1` and
/// `true` (case-insensitive). This is a dev/test escape hatch: a real deployment
/// must let the probe verify the origin before serving.
fn dev_allow_missing_origin() -> bool {
    std::env::var("VERGLAS_DEV_ALLOW_MISSING_ORIGIN")
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            value == "1" || value == "true"
        })
        .unwrap_or(false)
}

/// Builds the shared cache engine once, then runs the admin and S3 listeners
/// together. The engine is the read path *and* the admin purge handle (issue
/// #138) *and* the write invalidator, so it is constructed here and shared —
/// either listener failing tears the server down (a half-alive server is worse
/// than a dead one for the operator).
async fn serve(
    config: &verglas_core::config::Config,
    credentials: (String, String),
) -> Result<(), Box<dyn std::error::Error>> {
    // One backend store shared by the read and write passthroughs: it serves the
    // configured bucket set (`backend.bucket` / `backend.bucket_globs`, #235),
    // building each bucket's concurrency-limited client lazily on first request.
    // A request for a bucket outside the set returns NoSuchBucket. The credential
    // mode is resolved from the environment — logged so operators can eyeball it.
    let registry = verglas_backend::BackendStore::from_config(&config.backend);
    eprintln!(
        "verglas-server {VERSION} backend resolved: {} (serving bucket set `{}`)",
        registry.describe(),
        registry.bucket_set()
    );

    // Startup probe (#233): a configured backend bucket that cannot be reached or
    // authenticated must fail startup, not serve an empty store that answers
    // NoSuchKey for every read while looking healthy — "slow is acceptable, wrong
    // is never". The probe HEADs the configured single bucket through the same
    // refreshing provider chain the read path uses. Dev/test that runs without a
    // reachable origin opts out explicitly via VERGLAS_DEV_ALLOW_MISSING_ORIGIN.
    if dev_allow_missing_origin() {
        eprintln!(
            "verglas-server {VERSION} WARNING: VERGLAS_DEV_ALLOW_MISSING_ORIGIN set — skipping the backend startup probe; the origin is NOT verified (dev/test only)"
        );
    } else {
        registry.probe().await?;
        eprintln!("verglas-server {VERSION} backend startup probe passed");
    }

    let backend = PassthroughRead::new(registry.clone());

    // Serve-gating (#16): the admin listener comes up first, reporting
    // `starting` on /admin/healthz, so a load balancer gets a clear not-ready
    // signal (rather than connection-refused) while disk recovery runs. The
    // engine-dependent routes are wired through deferred slots filled — and the
    // health gate flipped — the instant recovery completes, so an NLB never
    // routes data-plane traffic to a node that would cold-miss everything.
    // Node-level metrics (#46): one registry + request instruments, shared
    // between the S3 front-end (which records per request) and the /metrics
    // render closure (which encodes it alongside the scrape-time snapshots).
    let node_metrics = Arc::new(verglas_core::metrics::NodeMetrics::new()?);

    // KV is part of every engine, not an optional service. Its non-evictable
    // log participates in shared disk accounting by actual bytes; the
    // heat-managed object cache receives no fixed carve or partition.
    let kv_capacity = config.cache.capacity_bytes.0;
    let kv_ram = (1024 * 1024)
        .min(kv_capacity)
        .min(config.cache.dram_bytes.0);
    let kv_store = verglas_kv::Store::open(
        &config.cache.dir.join("kv"),
        verglas_kv::StoreConfig {
            capacity_bytes: kv_capacity,
            ram_bytes: kv_ram,
        },
    )?;
    // Recovery-gated cache accounting takes over once the engine is open. Until
    // then, constrain KV against the bytes the shared directory already holds
    // so an early admin request cannot spend space occupied by a warm cache.
    let held_total = verglas_core::disk::allocated_bytes(&config.cache.dir);
    let kv_allocated = verglas_core::disk::allocated_bytes(&config.cache.dir.join("kv"));
    let non_kv_held = held_total.saturating_sub(kv_allocated);
    kv_store.set_capacity_ceiling(config.cache.capacity_bytes.0.saturating_sub(non_kv_held));
    let kv_admin_runtime = verglas_rest::kv::KvRuntime {
        store: kv_store.clone(),
        authorizer: verglas_rest::kv::KvAuthorizer::new(std::collections::HashMap::from([(
            credentials.1.clone(),
            verglas_rest::kv::KvGrant {
                tenant: credentials.0.clone(),
                namespace: "*".to_owned(),
                read: true,
                write: true,
            },
        )])),
    };
    eprintln!(
        "verglas-server {VERSION} KV recovery complete — always on under {}",
        config.cache.dir.join("kv").display(),
    );

    let health = admin::Health::starting();
    let purger_slot: admin::PurgerSlot = Arc::new(OnceLock::new());
    let stats_slot: admin::StatsSlot = Arc::new(OnceLock::new());

    // Server-instance tracking: report this node to the tenant's control plane
    // (register on boot, heartbeat every 5 min) so the control plane can count
    // active nodes. Spawned fully DETACHED
    // here — before any listener binds and outside the data-plane build — so it
    // never gates readiness and a dead control plane never affects serving. It
    // reads the live stats through `stats_slot` (filled after recovery), so the
    // heartbeat carries metrics once the engine is ready. PRIVACY INVARIANT: with
    // no `[control_plane]` config (and no stored login token), `from_config`
    // returns None — no task, no network — so a self-hosted server never phones
    // home.
    // Per-table metering source (#60), filled after recovery like the stats slot.
    // The reporter reads it each heartbeat and carries this window's deltas; an
    // unfilled slot means no metering (older-server-compatible), and no
    // [control_plane] still means no reporter at all (the privacy invariant).
    let metering_slot: node_report::MeteringSlot = Arc::new(OnceLock::new());
    if let Some(reporter) = node_report::NodeReporter::from_config(config, Some(stats_slot.clone()))
        .map(|r| r.with_metering(metering_slot.clone()))
    {
        eprintln!(
            "verglas-server {VERSION} control-plane node reporting enabled (register + 5m heartbeat)"
        );
        tokio::spawn(reporter.run());
    }
    let metrics_slot: admin::MetricsSlot = Arc::new(OnceLock::new());
    let table_metrics_slot: admin::TablesReportSlot = Arc::new(OnceLock::new());
    // The membership probe (#27) and the drain control (#31) exist only when
    // gossip is configured — a cluster of one has no peers to reroute to.
    let members_slot: Option<admin::MembersSlot> =
        config.cluster.is_some().then(|| Arc::new(OnceLock::new()));
    let drain_slot: Option<admin::DrainSlot> =
        config.cluster.is_some().then(|| Arc::new(OnceLock::new()));
    // Internal engine subsystems share one catalog handle. Public catalog
    // metadata calls bypass the server and use Iceberg REST directly.
    let tables_slot: Option<admin::TablesSlot> =
        config.catalog.is_some().then(|| Arc::new(OnceLock::new()));
    // The `graph` verb-family routes (`/v1/graphs/...`) drive the
    // graph-over-Iceberg engine through the same private catalog as the table
    // routes — a graph is just two plain tables plus the Puffin index in a
    // namespace — so the slot exists only when a catalog is configured and is
    // filled with the same handle after recovery.
    let graphs_slot: Option<admin::GraphsSlot> =
        config.catalog.is_some().then(|| Arc::new(OnceLock::new()));
    // The vector-index routes (`/v1/tables|graphs/{..}/indexes...`) drive the
    // streaming Vamana engine over the same private catalog. Each index is a
    // Puffin statistics attachment on the exact source snapshot it reflects.
    let vector_slot: Option<admin::VectorSlot> =
        config.catalog.is_some().then(|| Arc::new(OnceLock::new()));
    // The `verglas_sys` registry and watermark routes (#322) write and read
    // through a `SystemCatalog` over the same private catalog — the server is
    // the only local writer of `verglas_sys`. Filled after recovery.
    let sys_slot: Option<admin::SysSlot> =
        config.catalog.is_some().then(|| Arc::new(OnceLock::new()));
    // The platform queue routes (#328) need only a writable queue root, not the
    // cache engine, so the dir is created up front and the routes answer as soon
    // as the admin listener binds. Owned under the cache dir the server already
    // validated as writable at startup.
    let queue_dir: admin::QueueDir = Arc::new(config.cache.dir.join("platform/queues"));
    if let Err(e) = std::fs::create_dir_all(queue_dir.as_ref()) {
        eprintln!(
            "verglas-server {VERSION} could not create the platform queue dir {}: {e}",
            queue_dir.display()
        );
    }
    // Worker ingress exists only when an external scheduler is configured.
    // With no URL, Verglas boots normally and mounts no worker-trigger routes.
    let scheduler_url = std::env::var("VERGLAS_SCHEDULER_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let platform_slot: Option<admin::PlatformSlot> =
        (config.catalog.is_some() && scheduler_url.is_some()).then(|| Arc::new(OnceLock::new()));

    // Bind the admin and S3 listeners now, before serving, so their
    // kernel-assigned ports are known (issue #194): `verglas dev` passes
    // `VERGLAS_S3_ADDR`/`VERGLAS_ADMIN_ADDR = 127.0.0.1:0`, the server binds the
    // real ports here and reports them to the parent, and the resolved S3 port
    // feeds the local-access snapshot so the zero-config verbs reach the right
    // endpoint. Binding-and-holding is why there is no probe-then-bind race: the
    // port is owned the instant it is chosen, not re-acquired later.
    let admin_listener = tokio::net::TcpListener::bind(admin_listen_addr(Some(config))).await?;
    let resolved_admin_port = admin_listener.local_addr()?.port();
    report_port("admin", admin_listener.local_addr()?);

    // The S3 listener is pre-bound for the plain-HTTP path (local dev, where
    // ephemeral ports are used). The TLS path (production, fixed ports) binds
    // inside `serve_s3_tls`, so its port comes straight from config.
    let s3_bind = std::env::var("VERGLAS_S3_ADDR")
        .unwrap_or_else(|_| format!("0.0.0.0:{}", config.listen.s3_port));
    let (s3_listener, resolved_s3_port) = if config.listen.tls.is_none() {
        let listener = tokio::net::TcpListener::bind(&s3_bind).await?;
        let addr = listener.local_addr()?;
        report_port("s3", addr);
        (Some(listener), addr.port())
    } else {
        (None, config.listen.s3_port)
    };

    // Local-access snapshot (#287) for the zero-config CLI verbs: the S3
    // endpoint (the port just resolved), region/bucket, and the endpoint access
    // key id. The paired secret is never
    // put on this snapshot — the CLI reads it from the local 0600 creds file.
    let access = Some(build_local_access(
        config,
        &credentials.0,
        resolved_s3_port,
        resolved_admin_port,
    ));
    let catalog_gateway = config
        .catalog
        .as_ref()
        .map(verglas_catalog::CatalogGateway::from_config)
        .transpose()?;
    let catalog_source = catalog_gateway.as_ref().map(|gateway| gateway.source());

    // The standalone query worker dispatcher (opt-in, `[query_worker]`): a
    // config file + credentials file rendered once here, pointing the worker
    // at this server's own just-resolved loopback surface with the same
    // signing keypair the cache path uses. With no configured worker the
    // execution route returns 503; there is no embedded-engine fallback.
    let query_worker_dispatcher =
        build_query_worker_dispatcher(config, &credentials, resolved_s3_port, resolved_admin_port);
    let write_worker_dispatcher =
        build_write_worker_dispatcher(config, &credentials, resolved_s3_port, resolved_admin_port);
    let namespace_gateway = match (
        std::env::var("VERGLAS_CONTAINER_RUNTIME_URL")
            .ok()
            .filter(|value| !value.trim().is_empty()),
        std::env::var("VERGLAS_CONTAINER_RUNTIME_TOKEN")
            .ok()
            .filter(|value| !value.is_empty()),
    ) {
        (Some(endpoint), Some(token)) => Some(
            verglas_rest::namespace::NamespaceGateway::new(endpoint, token)
                .map_err(|error| format!("configure Integration namespace gateway: {error}"))?,
        ),
        (None, None) => None,
        _ => {
            return Err(
                "VERGLAS_CONTAINER_RUNTIME_URL and VERGLAS_CONTAINER_RUNTIME_TOKEN must be set together"
                    .into(),
            );
        }
    };
    let dashboard_runtime = match (&config.analytics, &tables_slot) {
        (Some(analytics), Some(tables)) => {
            Some(Arc::new(verglas_rest::dashboard::DashboardRuntime::new(
                tables.clone(),
                &analytics.rill,
                verglas_rest::dashboard::RillStorage {
                    endpoint: analytics.rill.s3_uri.clone(),
                    region: config
                        .backend
                        .region
                        .clone()
                        .unwrap_or_else(|| "us-east-1".to_owned()),
                    access_key_id: credentials.0.clone(),
                    secret_access_key: credentials.1.clone(),
                },
            )?))
        }
        (None, _) => None,
        (Some(_), None) => {
            return Err("analytics.rill requires a configured catalog".into());
        }
    };

    let admin_fut = serve_admin(
        admin_listener,
        health.clone(),
        admin::Slots {
            namespaces: namespace_gateway,
            kv: Some(kv_admin_runtime),
            purger: Some(purger_slot.clone()),
            stats: Some(stats_slot.clone()),
            metrics: Some(metrics_slot.clone()),
            table_metrics: Some(table_metrics_slot.clone()),
            members: members_slot.clone(),
            drain: drain_slot.clone(),
            catalog: catalog_gateway,
            access,
            tables: tables_slot.clone(),
            dashboards: dashboard_runtime,
            graphs: graphs_slot.clone(),
            vector: vector_slot.clone(),
            sys: sys_slot.clone(),
            queues: Some(queue_dir.clone()),
            platform: platform_slot.clone(),
            query_worker: query_worker_dispatcher.clone(),
            write_worker: write_worker_dispatcher.clone(),
        },
    );

    // The data plane: build the engine (recovery happens here), then serve S3.
    // Runs concurrently with the admin listener so /admin/healthz answers
    // `starting` throughout the recovery below.
    let data_plane = async {
        // Ownership ring: the live gossip ring when `[cluster]` is configured
        // (#27/#28), otherwise the degenerate one-member ring — same engine type
        // either way, so no read-path code branches on cluster mode. The gossip
        // agent, when present, keeps the ring in step with membership for the
        // life of the process (it is never shut down here; process exit tears it
        // down).
        let (ring, node_id, agent, advertise_addr) =
            build_ring(config, resolved_admin_port).await?;
        // The cache instance as a first-class ring member (transaction seam +
        // ring interface). Placement rides the same `LiveRing` the engine serves
        // from, so the instance never disagrees with the read path about who
        // owns a key; the commit log is the single-node no-quorum default here.
        // The fleet swaps in a clustered ring membership and a PG-quorum commit
        // log behind these same traits from the host-agent/PG-WAL side. Built
        // before the ring is moved into the engine (LiveRing is a cheap Arc
        // handle); it adds nothing to the read/write hot path.
        let cache_instance = build_cache_instance(ring.clone(), node_id.clone());
        eprintln!(
            "verglas-server {VERSION} cache instance `{}` ready — commit seam: {} (ring member; the fleet plugs a PG quorum + clustered ring in behind the CommitLog/RingMembership traits)",
            cache_instance.node_id().as_str(),
            if cache_instance.is_quorum() {
                "quorum"
            } else {
                "local no-quorum (single-node)"
            },
        );
        // Write-back tier state (#180), built once at construction so the tier
        // adds nothing to the read/write path when disabled. The fragment store
        // is shared between the peer server (serving fragments to peers) and the
        // coordinator (placing self-directed fragments), so both see one store.
        let writeback_on = config.cache.writeback.enabled;
        let node_id_for_writeback = node_id.clone();
        // The fragment store's byte ceiling is dynamic (#223): a shared atomic
        // the background disk poll updates each tick to what neither the block
        // cache nor KV is using — first come, first served, no carve.
        // It starts at 0 (nothing granted before the accounting has run); the
        // poll's first tick fires immediately after the engine is built, before
        // the S3 listener serves a byte.
        let fragment_ceiling = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let fragment_store = writeback_on.then(|| {
            LocalFragmentStore::with_dynamic_ceiling(
                &config.cache.dir,
                Arc::clone(&fragment_ceiling),
            )
        });
        let writeback_metrics = writeback_on.then(|| Arc::new(WritebackMetrics::default()));
        // Peer-fetch client (#29): resolves owners through live gossip
        // membership when clustered, else a disabled client (single-node owns
        // every key, so the peer rung is never reached). Same engine type.
        let peers = build_peer_client(config, agent.as_ref());
        // Disk recovery runs inside the engine build (both foyer stores rescan
        // their regions and rebuild the index). Time it for the operator log and
        // the #16 record.
        let recovery_start = std::time::Instant::now();
        let reader = HybridCacheEngine::new_with_background_fill_limit(
            backend,
            peers,
            ring,
            node_id,
            &config.cache,
            background_fill_limit(config.backend.max_concurrent_requests),
        )
        .await?;
        let recovery_elapsed = recovery_start.elapsed();
        eprintln!(
            "verglas-server {VERSION} cache recovery complete in {recovery_elapsed:?} — serving was gated on it (#16)"
        );
        // Runtime disk safety and budget sharing (#96/#223): a background poll
        // watches free NVMe and the engine's physical growth, and publishes two
        // decisions the hot paths read as plain atomics — pause block admission
        // when the filesystem nears full or the block cache would grow into
        // bytes held by KV or fragments (serve from origin, never crash), and
        // grant new fragments exactly the budget neither cache nor KV is using.
        // Owned by the data plane for the process lifetime.
        let growth_room = {
            let engine = reader.clone();
            move || engine.disk_growth_room_bytes()
        };
        let _disk_monitor = spawn_disk_monitor(
            config,
            reader.caching_paused_handle(),
            growth_room,
            Arc::clone(&fragment_ceiling),
            fragment_store.clone(),
            kv_store.clone(),
        );
        // Peer-fetch server (#29): serves this node's owned, cached blocks to
        // peers from the local tiers only. Held for the process lifetime;
        // dropping it on exit aborts the listener. Present only when `[cluster]`
        // advertises a peer address — otherwise there are no peers to serve.
        let _peer_server = start_peer_server(
            config,
            advertise_addr,
            reader.clone(),
            fragment_store.clone(),
        )
        .await?;

        // Warm-from-peers (#30): a node that just joined a pod owns ~1/N of the
        // keyspace cold, so for a configured window it pulls locally-missed
        // *owned* blocks from their previous holders over the peer network
        // before a cold backend fill. A single-node server has no pod, so it
        // never opens the window — its read path is byte-for-byte unchanged.
        if let Some(cluster) = &config.cluster
            && cluster.warm_from_peers_secs > 0
        {
            reader.begin_warming(std::time::Duration::from_secs(cluster.warm_from_peers_secs));
            eprintln!(
                "verglas-server {VERSION} warm-from-peers window open for {}s — owned cold misses pull from their previous holder before the backend (#30)",
                cluster.warm_from_peers_secs
            );
        }

        // Table lifecycle (#168 warming + #51 prefetch/retire): when a catalog is
        // watched, warm metadata into the pinned store, keep the logical-key map
        // current, feed the heat ledger from the request path, and repair the
        // cache after each compaction commit. Returns the warming progress for
        // /admin/stats and the serving-path heat/yield hooks (all None when no
        // catalog is configured or the features are off).
        let hooks = spawn_lifecycle(config, reader.clone(), catalog_source)?;
        let warming = hooks.warming;

        // Start accepting loopback S3 requests before opening any Iceberg
        // handles that use that endpoint. System-table bootstrap below reads
        // `verglas_sys.workers`; awaiting it before the S3 server runs creates
        // a circular startup wait on every catalog-backed server.
        let serving = verglas_tables::lifecycle::heat::HeatFeed::with_options(
            reader.clone(),
            hooks.heat_sender,
            hooks.yield_gate,
            hooks.telemetry.clone(),
            hooks.mapper.clone(),
        );
        let s3_config = config.clone();
        let s3_credentials = credentials.clone();
        let s3_reader = reader.clone();
        let s3_registry = registry.clone();
        let s3_agent = agent.clone();
        let s3_writeback_metrics = writeback_metrics.clone();
        let s3_node_metrics = node_metrics.clone();
        // The SigV4-gated data port always serves KV and can also dispatch the
        // configured execution roles. The authenticated access key becomes the
        // tenant before the KV router resolves a namespace or key.
        let s3_serving_api: Option<Arc<dyn verglas_s3::ServingApi>> =
            Some(Arc::new(V1ServingApi {
                router: admin::v1_serving_router(
                    query_worker_dispatcher.clone(),
                    write_worker_dispatcher.clone(),
                )
                .merge(verglas_rest::kv::router(verglas_rest::kv::KvRuntime {
                    store: kv_store.clone(),
                    authorizer: verglas_rest::kv::KvAuthorizer::default(),
                })),
            }));
        let s3_task = tokio::spawn(async move {
            serve_s3(
                &s3_config,
                s3_listener,
                s3_credentials,
                serving,
                s3_reader,
                s3_registry,
                node_id_for_writeback,
                s3_agent,
                fragment_store,
                s3_writeback_metrics,
                s3_node_metrics,
                s3_serving_api,
            )
            .await
            .map_err(|error| error.to_string())
        });

        // Recovery is done: wire the engine-dependent admin routes onto the
        // already-serving admin router, then flip the health gate so the LB may
        // start routing data-plane traffic here.
        let _ = purger_slot.set(Arc::new(reader.clone()) as Arc<dyn CachePurger>);
        let _ = stats_slot.set(stats_source(
            config,
            reader.clone(),
            warming,
            writeback_metrics.clone(),
        ));
        let _ = metrics_slot.set(metrics_source(
            config,
            node_metrics.clone(),
            (reader.clone(), kv_store.clone()),
            registry.clone(),
            writeback_metrics.clone(),
            hooks.telemetry.clone(),
            hooks.mapper.clone(),
        ));
        // Fill the per-table admin report and the metering source (both read the
        // same rollup, keeping the customer-auditable-billing property). Present
        // only when the mapper + telemetry exist (prefetch on).
        if let (Some(telemetry), Some(mapper)) = (&hooks.telemetry, &hooks.mapper) {
            {
                let telemetry = telemetry.clone();
                let mapper = mapper.clone();
                let _ = table_metrics_slot.set(Arc::new(move || {
                    telemetry.table_report(table_name_resolver(Some(&mapper)))
                }));
            }
            {
                let telemetry = telemetry.clone();
                let mapper = mapper.clone();
                let _ = metering_slot.set(Arc::new(move || {
                    telemetry.metering_snapshot(table_name_resolver(Some(&mapper)))
                }));
            }
        }
        if let (Some(slot), Some(agent)) = (&members_slot, &agent) {
            let _ = slot.set(members_source(agent.clone()));
        }
        if let (Some(slot), Some(agent)) = (&drain_slot, &agent) {
            let default_timeout = config
                .cluster
                .as_ref()
                .map(|c| c.drain_timeout_secs)
                .unwrap_or(0);
            let _ = slot.set(drain_source(agent.clone(), default_timeout));
        }
        // Internal engine services share a cache-pathed catalog handle.
        if let Some(slot) = &tables_slot {
            match build_tables_catalog(config, &credentials, resolved_s3_port, resolved_admin_port)
                .await
            {
                Ok(catalog) => {
                    // The registry/watermark routes (#322) share the handle:
                    // one private catalog client, one write authority.
                    let sys_catalog =
                        Arc::new(verglas_platform::SystemCatalog::new(catalog.clone()));
                    if let Some(sys) = &sys_slot {
                        let _ = sys.set(sys_catalog.clone());
                    }
                    // REST ingress resolves deployments through this registry
                    // and pushes complete worker events to the scheduler. Cron
                    // reconciliation and execution live only in that separate
                    // container.
                    if let (Some(platform), Some(scheduler_url)) = (&platform_slot, &scheduler_url)
                    {
                        let ingress = Arc::new(platform::SchedulerIngress::new(
                            sys_catalog.clone(),
                            scheduler_url.clone(),
                        ));
                        let _ = platform.set(ingress.clone());
                        if let Some(watcher) = hooks.watcher.clone() {
                            spawn_scheduler_catalog_events(watcher, ingress);
                        }
                        // Follow workers run continuously, not per-tick, so a
                        // dedicated manager keeps one runner alive per active
                        // follow worker — tailing a file or wrapping a command and
                        // streaming captured lines to the target table (the cloud
                        // lakehouse when this server is logged in).
                        let follow_manager =
                            Arc::new(follow::FollowManager::new(catalog.clone(), sys_catalog));
                        follow::spawn_follow_manager(follow_manager);
                    }
                    // The shared vector-index service owns only a disposable
                    // decoded-index cache. Iceberg metadata is the registry and
                    // customer object storage owns the Puffin files.
                    let vector_service = vector_slot
                        .as_ref()
                        .map(|_| Arc::new(verglas_vector::service::VectorService::new()));
                    // The graph routes drive the graph engine through the same
                    // private catalog — one write authority for the nodes and
                    // edges tables, exactly as for any other table.
                    if let Some(graphs) = &graphs_slot {
                        let _ = graphs.set(catalog.clone());
                    }
                    // The vector routes commit and discover snapshot-bound
                    // Puffin attachments through this private catalog handle.
                    if let (Some(vector), Some(service)) = (&vector_slot, vector_service) {
                        let runtime = Arc::new(admin::VectorRuntime {
                            catalog: catalog.clone(),
                            service,
                        });
                        let _ = vector.set(runtime);
                    }
                    let _ = slot.set(catalog);
                }
                Err(e) => eprintln!("verglas-server {VERSION} internal catalog unavailable: {e}"),
            }
        }
        health.mark_ready();
        match s3_task.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(std::io::Error::other(error).into()),
            Err(error) => Err(std::io::Error::other(error).into()),
        }
    };

    tokio::try_join!(admin_fut, data_plane).map(|_| ())
}

/// Spawns the background disk poll (#96/#223).
///
/// Once a second it reads the free NVMe under `cache.dir` and the engine's
/// remaining physical growth room (`growth_room` — logical capacity the sparse
/// foyer files have not consumed), and publishes two decisions the hot paths
/// consume as plain atomics. `caching_paused`: block admission stops when the
/// filesystem nears full (#96) or when the block cache would otherwise grow
/// into budget bytes KV or write-back fragments already hold (#223) — either way
/// the node degrades to origin fills, never crashes. `fragment_ceiling`: the
/// fragment store may hold what it holds plus exactly the budget the block
/// cache and durable KV log are not using — one `cache.capacity_bytes` budget,
/// shared first come, first served, no carve. The poll keeps every
/// `statfs`/`stat` off the serve and fill paths — they only read the atomics.
/// The first tick fires
/// immediately, so the granted ceiling is live before serving starts. The
/// returned task handle is held by the data plane for the process lifetime;
/// dropping it on exit stops the poll.
fn spawn_disk_monitor(
    config: &verglas_core::config::Config,
    caching_paused: Arc<std::sync::atomic::AtomicBool>,
    growth_room: impl Fn() -> u64 + Send + 'static,
    fragment_ceiling: Arc<std::sync::atomic::AtomicU64>,
    fragment_store: Option<LocalFragmentStore>,
    kv_store: verglas_kv::Store,
) -> tokio::task::JoinHandle<()> {
    use std::sync::atomic::Ordering;
    use verglas_core::disk::{DiskParams, free_bytes};

    let dir = config.cache.dir.clone();
    let capacity = config.cache.capacity_bytes.0;
    // Keep a headroom reserve so admission stops before the disk (or the shared
    // budget) is truly spent; the gap to `high_water` is the hysteresis band, so
    // hovering at the edge does not flap admission on and off. The reserve also
    // bounds how far foyer's in-flight flush pipeline can overshoot between
    // polls. Floored at 64 MiB so a tiny test budget still leaves a sane reserve.
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
            let frag_used = fragment_store.as_ref().map_or(0, |s| s.used_bytes());
            let kv_used = kv_store.used_bytes();
            let was_paused = caching_paused.load(Ordering::Relaxed);
            let free = free_bytes(&dir);
            let room = growth_room();
            let state = shared_disk_decision(free, room, frag_used, kv_used, was_paused, &params);
            let shared_available = state.fragment_max.saturating_sub(frag_used);
            kv_store.set_capacity_ceiling(kv_used.saturating_add(shared_available));
            caching_paused.store(state.caching_paused, Ordering::Relaxed);
            // Pause transitions must be loud (#300): a paused cache serves
            // correctly but admits nothing, and hit counters were the only way
            // to notice.
            if state.caching_paused != was_paused {
                if state.caching_paused {
                    tracing::warn!(
                        fs_free_bytes = free,
                        foyer_growth_room = room,
                        frag_used,
                        kv_used,
                        "cache admission paused: disk headroom below low water"
                    );
                } else {
                    tracing::info!(
                        fs_free_bytes = free,
                        foyer_growth_room = room,
                        frag_used,
                        kv_used,
                        "cache admission resumed"
                    );
                }
            }
            // The fragment ceiling is only meaningful when the tier is on; leave
            // it at its initial value otherwise (nothing reads it).
            if fragment_store.is_some() {
                fragment_ceiling.store(state.fragment_max, Ordering::Release);
            }
        }
    })
}

/// Accounts KV and write-back bytes as one non-evictable total while returning
/// a ceiling expressed only in fragment-store bytes.
fn shared_disk_decision(
    free: Option<u64>,
    foyer_growth_room: u64,
    frag_used: u64,
    kv_used: u64,
    was_paused: bool,
    params: &verglas_core::disk::DiskParams,
) -> verglas_core::disk::DiskState {
    let protected_used = frag_used.saturating_add(kv_used);
    let mut state = verglas_core::disk::disk_decision(
        free,
        foyer_growth_room,
        protected_used,
        was_paused,
        params,
    );
    state.fragment_max = state.fragment_max.saturating_sub(kv_used).max(frag_used);
    state
}

/// Builds the peer-fetch client (#29). When `[cluster]` is configured, it
/// resolves owner ids to advertised addresses through the agent's live
/// membership and carries the shared secret and tight timeout budget from
/// config. Without gossip there are no peers, so the client is disabled (an
/// empty resolver) — the single-member ring owns every key, so it is never
/// actually consulted.
fn build_peer_client(
    config: &verglas_core::config::Config,
    agent: Option<&Arc<ClusterAgent>>,
) -> PeerClient {
    match (config.cluster.as_ref(), agent) {
        (Some(cluster), Some(agent)) => PeerClient::new(
            agent.resolver(),
            cluster.secret.clone(),
            std::time::Duration::from_millis(cluster.peer_connect_timeout_ms),
            std::time::Duration::from_millis(cluster.peer_request_timeout_ms),
        ),
        _ => PeerClient::disabled(),
    }
}

/// Starts the peer-fetch server (#29) when `[cluster]` advertises a peer
/// address, binding it to that address and wiring it to the engine's cache-only
/// `local_block` endpoint. Returns `None` for a single-node server or a
/// clustered node that advertises no peer address (peers then backend-fill for
/// keys it owns). The listener runs until the returned handle drops.
async fn start_peer_server(
    config: &verglas_core::config::Config,
    advertise_addr: Option<std::net::SocketAddr>,
    engine: ServerEngine,
    fragment_store: Option<LocalFragmentStore>,
) -> Result<Option<PeerServer>, Box<dyn std::error::Error>> {
    let Some(cluster) = &config.cluster else {
        return Ok(None);
    };
    // The advertise address is the one gossip announced — already resolved from
    // an ephemeral `:0` request to a real port by `build_ring` (issue #194), so
    // the port the peer server binds matches what peers were told to reach.
    let Some(addr) = advertise_addr else {
        return Ok(None);
    };
    // When the write-back tier is on, the peer server also serves the fragment
    // put/get/delete endpoints backed by this node's fragment store (#180).
    let server = match fragment_store {
        Some(store) => {
            PeerServer::bind_with_fragments(
                addr,
                cluster.secret.clone(),
                peer_source(engine),
                fragment_handlers(store),
            )
            .await?
        }
        None => PeerServer::bind(addr, cluster.secret.clone(), peer_source(engine)).await?,
    };
    report_port("peer", server.local_addr());
    eprintln!(
        "verglas-server {VERSION} serving peer fetch on http://{} (cache-only; owners serve owned blocks)",
        server.local_addr()
    );
    Ok(Some(server))
}

/// Adapts the engine's cache-only `local_block` into the peer server's block
/// source: a clone of the engine per request, never a backend fill.
fn peer_source(engine: ServerEngine) -> LocalBlockFn {
    Arc::new(move |block| {
        let engine = engine.clone();
        Box::pin(async move { engine.local_block(&block).await })
    })
}

/// Wires the local fragment store behind the peer server's fragment handlers
/// (#180): store, load, delete, and headroom callbacks a peer coordinator
/// drives. The headroom callback lets a remote coordinator exclude this node
/// from placement when its fragment sub-budget is full.
fn fragment_handlers(store: LocalFragmentStore) -> FragmentHandlers {
    let store_put = store.clone();
    let store_stream = store.clone();
    let store_get = store.clone();
    let store_del = store.clone();
    let store_room = store.clone();
    let store_list = store;
    FragmentHandlers {
        store: Arc::new(move |record: FragmentRecord| {
            let store = store_put.clone();
            Box::pin(async move { store.store_fragment(&record) })
        }),
        store_stream: Arc::new(move |key: FragmentKey, shards| {
            let store = store_stream.clone();
            Box::pin(async move { stream_into_store(&store, &key, shards).await })
        }),
        load: Arc::new(move |key: FragmentKey| {
            let store = store_get.clone();
            Box::pin(async move { store.load_fragment(&key) })
        }),
        delete: Arc::new(move |key: FragmentKey| {
            let store = store_del.clone();
            Box::pin(async move { store.delete_fragment(&key) })
        }),
        headroom: Arc::new(move |bytes: u64| {
            let store = store_room.clone();
            Box::pin(async move { store.has_headroom(bytes) })
        }),
        list_prefix: Arc::new(move |prefix: String| {
            let store = store_list.clone();
            Box::pin(async move {
                store
                    .list_fragment_keys()
                    .into_iter()
                    .filter(|key| key.object_id.starts_with(&prefix))
                    .collect()
            })
        }),
    }
}

/// Streams a fragment's shards into `store`, appending each and committing once
/// the stream ends. A budget refusal or IO error aborts the write (the
/// uncommitted temp file is cleaned up on drop), so the peer answers 500 and the
/// coordinator counts it against quorum.
async fn stream_into_store(
    store: &LocalFragmentStore,
    key: &FragmentKey,
    mut shards: verglas_cluster::FragmentShardStream,
) -> Result<(), verglas_cluster::FragmentIoError> {
    use futures::StreamExt;
    let mut writer = store.open_fragment(key)?;
    while let Some(shard) = shards.next().await {
        writer.append(&shard)?;
    }
    writer.commit()
}

/// Builds the ownership ring and, when `[cluster]` is configured, the gossip
/// agent that drives it.
///
/// With `[cluster]`: starts chitchat gossip (#27), returns the live ring the
/// agent updates on membership change, this node's gossiped identity, and the
/// agent handle (shared via `Arc` so the admin surface can read membership).
/// Builds the cache instance (the ring member's coordination surface): its ring
/// membership over the server's live ring, and its transaction seam.
///
/// Placement is a [`verglas_instance::RingAdapter`] over the same `LiveRing` the
/// engine serves from, so the instance and the read path never disagree about
/// ownership. The commit log is the single-node no-quorum default
/// ([`verglas_instance::LocalCommitLog`]): a self-hosted node records commits
/// locally and immediately. The fleet replaces the commit log with its
/// PG-quorum implementation and, when it lands its remote peer transport, the
/// ring membership too — both behind the crate's traits, without a change here.
fn build_cache_instance(ring: LiveRing, node_id: NodeId) -> verglas_instance::CacheInstance {
    let membership = Arc::new(verglas_instance::RingAdapter::new(
        node_id,
        Arc::new(ring) as Arc<dyn verglas_core::ring::Ring + Send + Sync>,
    ));
    let commit_log = Arc::new(verglas_instance::LocalCommitLog::new());
    verglas_instance::CacheInstance::new(membership, commit_log)
}

/// Without it: a single-member `LiveRing` under the fixed `single` node id —
/// the turn-off path, ownership identical to a pre-cluster server.
async fn build_ring(
    config: &verglas_core::config::Config,
    resolved_admin_port: u16,
) -> Result<
    (
        LiveRing,
        NodeId,
        Option<Arc<ClusterAgent>>,
        Option<std::net::SocketAddr>,
    ),
    Box<dyn std::error::Error>,
> {
    match &config.cluster {
        Some(cluster) => {
            let mut agent_config =
                AgentConfig::from_config(cluster, &config.cache, resolved_admin_port)?;
            // Ephemeral ports (issue #194): when `verglas dev` asks for a
            // kernel-assigned gossip/peer port (`127.0.0.1:0`), resolve the real
            // port now so gossip advertises it correctly and the parent can seed
            // the next node at node 0's real gossip address.
            if agent_config.gossip_addr.port() == 0 {
                agent_config.gossip_addr = reserve_ephemeral_udp()?;
            }
            if agent_config.advertise_addr.is_some_and(|a| a.port() == 0) {
                agent_config.advertise_addr = Some(reserve_ephemeral_tcp()?);
            }
            let advertise_addr = agent_config.advertise_addr;
            report_port("gossip", agent_config.gossip_addr);
            eprintln!(
                "verglas-server {VERSION} joining pod `{}` as `{}` — gossip on {} (weight {} bytes, {} seed(s))",
                agent_config.pod_id,
                agent_config.node_id,
                agent_config.gossip_addr,
                agent_config.capacity_bytes,
                agent_config.seeds.len(),
            );
            let agent = ClusterAgent::spawn_udp(agent_config).await?;
            let ring = agent.ring();
            let node_id = agent.node_id().clone();
            Ok((ring, node_id, Some(Arc::new(agent)), advertise_addr))
        }
        None => {
            let node_id = NodeId::new(SINGLE_NODE_ID);
            let ring = LiveRing::single(node_id.clone(), config.cache.capacity_bytes.0.max(1));
            Ok((ring, node_id, None, None))
        }
    }
}

/// Builds the `/admin/members` source: a closure over the gossip agent that
/// snapshots the current live membership and this node's ring epoch into the
/// admin wire form (issue #27).
fn members_source(agent: Arc<ClusterAgent>) -> admin::MembersSource {
    use verglas_core::ring::Ring;
    Arc::new(move || {
        let epoch = agent.ring().epoch();
        let members = agent.members();
        let pod_id = members
            .first()
            .map(|m| m.pod_id.clone())
            .unwrap_or_default();
        MembersInfo {
            node_id: agent.node_id().as_str().to_owned(),
            pod_id,
            epoch,
            members: members.iter().map(member_info).collect(),
        }
    })
}

/// Builds the `/admin/drain` handler (issue #31): an async closure over the
/// gossip agent that marks this node `draining`, schedules its exit, and acks.
///
/// Entering `draining` gossips the state so peers shed this node's ownership to
/// its successors (which warm from it as a donor, #30's transfer reversed),
/// while it keeps serving reads. The node then exits after `timeout_secs` (the
/// request's, else the configured `default_timeout`) so the ring rebalances —
/// a timer, not a wait on "every shed key re-owned warm", which no node can
/// observe locally (early-exit on quiescent donor traffic is a noted follow-up).
/// A `0` timeout exits promptly. The exit is a clean `process::exit(0)`; gossip
/// teardown on the way out lets peers see the departure faster than by failure
/// detection.
fn drain_source(agent: Arc<ClusterAgent>, default_timeout: u64) -> admin::DrainHandler {
    use verglas_cluster::NodeState;
    Arc::new(move |request| {
        let agent = agent.clone();
        Box::pin(async move {
            let timeout_secs = request.timeout_secs.unwrap_or(default_timeout);
            let node_id = agent.node_id().as_str().to_owned();
            // Gossip the drain first, so peers begin rerouting before we start
            // counting down to exit.
            agent.set_state(NodeState::Draining).await;
            eprintln!(
                "verglas-server {VERSION} node `{node_id}` draining — serving as a donor for up to {timeout_secs}s, then exiting so the ring rebalances (#31)"
            );
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)).await;
                eprintln!(
                    "verglas-server {VERSION} drain window elapsed ({timeout_secs}s) — exiting cleanly; gossip announces the departure and the ring rebalances (#31)"
                );
                std::process::exit(0);
            });
            DrainAck {
                node_id,
                state: NodeState::Draining.as_str().to_owned(),
                timeout_secs,
            }
        })
    })
}

/// Maps a cluster agent's `NodeMeta` to the admin wire `MemberInfo` (addresses
/// rendered as strings; the admin surface carries no socket types).
fn member_info(meta: &verglas_cluster::NodeMeta) -> MemberInfo {
    MemberInfo {
        node_id: meta.node_id.as_str().to_owned(),
        generation: meta.generation,
        gossip_addr: meta.gossip_addr.to_string(),
        advertise_addr: meta.advertise_addr.map(|a| a.to_string()),
        admin_addr: meta.admin_addr.map(|a| a.to_string()),
        capacity_bytes: meta.capacity_bytes,
        state: meta.state.as_str().to_owned(),
    }
}

/// Waits for Ctrl-C before tearing down a listener.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Env var the `verglas dev` parent sets to its own pid, arming this server's
/// parent-death watch (issue #170). Must match the name the CLI writes.
const PARENT_PID_ENV: &str = "VERGLAS_PARENT_PID";

/// How often the parent-death watch samples `getppid()`. Bounds how long an
/// orphaned server can hold its port after the parent dies; small enough that a
/// leftover never squats a port meaningfully, large enough to be free.
#[cfg(unix)]
const PARENT_DEATH_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// Whether the recorded parent has died: the kernel reparents an orphan away
/// from its original parent (to pid 1 on both macOS and Linux), so a live
/// `current_ppid` that no longer equals `expected_parent_pid` means the parent
/// is gone. Comparing against the recorded pid — not a hardcoded `== 1` — keeps
/// the watch correct wherever pid 1 happens to sit. Pure so the decision is
/// unit-tested without spawning processes.
fn parent_is_gone(expected_parent_pid: i32, current_ppid: i32) -> bool {
    current_ppid != expected_parent_pid
}

/// Arms the parent-death watch when `verglas dev` launched this server (issue
/// #170). SIGKILL to the parent leaves no chance for a teardown handler to run,
/// so a belt-and-braces watch is needed: a background thread polls `getppid()`
/// and exits the server the moment it diverges from the recorded parent pid,
/// freeing the port instead of squatting it with stale keys.
///
/// PPID-polling is chosen over an inherited pipe held open by the parent: it
/// needs no file-descriptor plumbing through the child spawn and is portable
/// across macOS and Linux (both reparent orphans to pid 1). The pipe approach
/// would react without polling latency but at the cost of that fd plumbing;
/// 250 ms latency is irrelevant for freeing a dev port.
///
/// The watch arms ONLY when the env var is present, so a production server under
/// systemd/launchd — whose legitimate parent may itself be pid 1 — never arms it
/// and is never killed spuriously.
#[cfg(unix)]
fn spawn_parent_death_watch() {
    let Ok(raw) = std::env::var(PARENT_PID_ENV) else {
        return;
    };
    let Ok(parent_pid) = raw.parse::<i32>() else {
        eprintln!("verglas-server: ignoring unparseable {PARENT_PID_ENV}={raw:?}");
        return;
    };
    std::thread::Builder::new()
        .name("parent-death-watch".to_owned())
        .spawn(move || {
            loop {
                std::thread::sleep(PARENT_DEATH_POLL_INTERVAL);
                // SAFETY: getppid only reads the caller's parent pid and cannot
                // fail; the call touches no shared state.
                let ppid = unsafe { libc::getppid() };
                if parent_is_gone(parent_pid, ppid) {
                    eprintln!(
                        "verglas-server: parent {parent_pid} exited (reparented to {ppid}); shutting down so its port is not left squatted by an orphan (#170)"
                    );
                    std::process::exit(0);
                }
            }
        })
        .ok();
}

#[tokio::main]
async fn main() {
    if std::env::args().any(|arg| arg == "--version" || arg == "-V") {
        println!("verglas-server {VERSION}");
        return;
    }

    // Pin the process-level rustls CryptoProvider. Catalog HTTPS and other
    // rustls clients resolve TLS through the process default, and with more
    // than one provider feature in the dependency graph rustls panics at first
    // use unless one is installed explicitly.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Arm the parent-death watch before binding anything: if `verglas dev`
    // spawned us and later dies uncatchably, we must free our port (#170).
    #[cfg(unix)]
    spawn_parent_death_watch();

    // Install the ports reporter before any listener binds, so every resolved
    // ephemeral port reaches the `verglas dev` parent (issue #194).
    let _ = PORTS_REPORT.set(ports_file_from_args().map(|path| PortsReport { path }));

    let config = load_config_from_args();

    // Install the subscriber from the `[log]` config (or the built-in defaults
    // when config-less), before any serving work, so watcher/backend/fill logs
    // are visible from the first line (#61/#241). `RUST_LOG` still overrides the
    // level; `verglas dev` sets `VERGLAS_LOG_FORMAT=pretty` for human output.
    let (log_format, log_level) = match &config {
        Some(loaded) => (loaded.config.log.format, loaded.config.log.level.clone()),
        None => (verglas_core::config::LogFormat::Json, "info".to_owned()),
    };
    logging::install(log_format, &log_level);
    let result = match &config {
        Some(loaded) => {
            let config = &loaded.config;
            println!("verglas-server {VERSION} config ok: {}", config.summary());
            match loaded
                .endpoint_credentials
                .clone()
                .map_or_else(|| resolve_auth(config), Ok)
            {
                Ok(credentials) => serve(config, credentials).await,
                Err(e) => {
                    eprintln!("verglas-server: {e}");
                    std::process::exit(1);
                }
            }
        }
        // Without a config the S3 endpoint has no cache dir or auth to bind, so
        // only the admin surface comes up (used by `verglas` CLI smoke flows
        // and tests). No cache engine exists, so purge is unavailable and there
        // is nothing to recover — health is ready the instant it binds.
        None => match tokio::net::TcpListener::bind(admin_listen_addr(None)).await {
            Ok(listener) => {
                if let Ok(addr) = listener.local_addr() {
                    report_port("admin", addr);
                }
                serve_admin(listener, admin::Health::ready(), admin::Slots::default()).await
            }
            Err(e) => Err(e.into()),
        },
    };

    if let Err(error) = result {
        eprintln!("verglas-server failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use verglas_core::admin::{STATS_PATH, StatsInfo};

    /// Background fills consume only their configured quarter-share and are
    /// disabled when the origin budget is too small to reserve user capacity.
    #[test]
    fn background_fill_limit_reserves_foreground_capacity() {
        assert_eq!(background_fill_limit(1), 0);
        assert_eq!(background_fill_limit(3), 0);
        assert_eq!(background_fill_limit(4), 1);
        assert_eq!(background_fill_limit(64), 16);
    }

    /// KV consumes shared headroom by actual durable bytes and never receives a carve.
    #[test]
    fn kv_and_fragments_share_non_evictable_disk_accounting() {
        let params = verglas_core::disk::DiskParams {
            low_water: 100,
            high_water: 200,
        };
        let open = shared_disk_decision(None, 10_000, 500, 1_000, false, &params);
        assert!(!open.caching_paused);
        assert_eq!(open.fragment_max, 9_000);

        let full = shared_disk_decision(None, 10_000, 500, 9_500, false, &params);
        assert!(full.caching_paused);
        assert_eq!(
            full.fragment_max, 500,
            "acked fragments remain but cannot grow"
        );
    }

    /// Builds the engine in-process, mounts the admin router with its stats
    /// source, and drives `/admin/stats` over a real loopback socket. Proves the
    /// end-to-end wiring — `build_engine` -> `stats_source` -> `admin::router` —
    /// reports the configured budgets, the piece the subprocess integration
    /// tests exercise but never instrument.
    #[tokio::test]
    async fn admin_stats_reports_the_configured_budgets_in_process() {
        let dir = std::env::temp_dir().join(format!(
            "verglas-server-stats-unit-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).expect("create cache dir");
        let toml = format!(
            "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\ndram_bytes = \"80MB\"\n[backend]\nbucket = \"test-bucket\"\n",
            dir.display()
        );
        let config = verglas_core::config::Config::from_toml_str(&toml).expect("valid config");

        let registry = verglas_backend::BackendStore::from_config(&config.backend);
        let backend = PassthroughRead::new(registry);
        // Build the engine over the same ring path the server uses (a
        // single-member LiveRing when no `[cluster]` is configured).
        let (ring, node_id, agent, _advertise) = build_ring(&config, config.listen.admin_port)
            .await
            .expect("ring builds");
        let peers = build_peer_client(&config, agent.as_ref());
        let reader = HybridCacheEngine::new_with_background_fill_limit(
            backend,
            peers,
            ring,
            node_id,
            &config.cache,
            background_fill_limit(config.backend.max_concurrent_requests),
        )
        .await
        .expect("engine builds");
        let stats_slot: admin::StatsSlot = Arc::new(OnceLock::new());
        let _ = stats_slot.set(stats_source(&config, reader, None, None));
        let app = admin::router(
            VERSION,
            admin::Health::ready(),
            admin::Slots {
                stats: Some(stats_slot),
                ..admin::Slots::default()
            },
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let body: StatsInfo = reqwest::get(format!("http://{addr}{STATS_PATH}"))
            .await
            .expect("request")
            .json()
            .await
            .expect("stats json");
        assert_eq!(body.cache.dram_bytes, 80 * 1024 * 1024);
        assert_eq!(body.cache.capacity_bytes, 64 * 1024 * 1024);
        // A freshly built engine has served nothing, but the gauge is live.
        assert_eq!(body.counters.disk_hits, 0);

        server.abort();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The local-access snapshot (#287) is resolved from config and the endpoint
    /// access key id: the loopback S3 endpoint, backend region and bucket, and
    /// the access key id. A configured upstream catalog is exposed through the
    /// shallow local proxy and remains the authoritative service.
    /// The snapshot never carries the secret — the type has no field for it.
    #[test]
    fn build_local_access_reflects_the_config_and_access_key() {
        let toml = "\
[listen]\ns3_port = 9333\nadmin_port = 9334\n\
[cache]\ndir = \"/tmp/vg\"\ncapacity_bytes = \"64MB\"\n\
[backend]\nbucket = \"warehouse\"\nregion = \"eu-west-2\"\n\
[catalog]\nuri = \"http://127.0.0.1:8181\"\nwarehouse = \"s3://warehouse/tenant\"\n";
        let config = verglas_core::config::Config::from_toml_str(toml).expect("valid config");
        let access = build_local_access(
            &config,
            "VGKEY",
            config.listen.s3_port,
            config.listen.admin_port,
        );
        assert_eq!(access.s3_endpoint, "http://127.0.0.1:9333");
        assert_eq!(access.query_uri, "http://127.0.0.1:9334");
        assert_eq!(
            access.catalog_uri.as_deref(),
            Some("http://127.0.0.1:9334/catalog")
        );
        assert_eq!(access.warehouse.as_deref(), Some("s3://warehouse/tenant"));
        assert_eq!(access.region, "eu-west-2");
        assert_eq!(access.bucket.as_deref(), Some("warehouse"));
        assert_eq!(access.access_key_id.as_deref(), Some("VGKEY"));
        // The served snapshot must not carry the secret at all.
        let value = serde_json::to_value(&access).expect("serialize");
        assert!(
            value.get("secret_access_key").is_none(),
            "the local-access snapshot must not carry a secret_access_key field"
        );

        // With no catalog, S3 discovery is unchanged.
        let toml_no_catalog = "\
[listen]\ns3_port = 8333\nadmin_port = 8334\n\
[cache]\ndir = \"/tmp/vg\"\ncapacity_bytes = \"64MB\"\n\
[backend]\nbucket = \"b\"\n";
        let config = verglas_core::config::Config::from_toml_str(toml_no_catalog).expect("valid");
        let access = build_local_access(
            &config,
            "a",
            config.listen.s3_port,
            config.listen.admin_port,
        );
        assert!(access.catalog_uri.is_none());
        assert!(access.warehouse.is_none());
        // Region falls back to us-east-1 when the backend leaves it unset.
        assert_eq!(access.region, "us-east-1");
    }

    /// The parent-death watch (issue #170) fires exactly when the recorded
    /// parent pid no longer matches the live `getppid()` — the kernel reparents
    /// an orphan away from its original parent (to pid 1 on macOS and Linux), so
    /// a changed ppid means the parent died and this server must exit rather
    /// than squat its port.
    #[test]
    fn parent_is_gone_only_when_the_ppid_changes() {
        // Parent still alive: ppid unchanged.
        assert!(!parent_is_gone(4242, 4242));
        // Parent died: the orphan was reparented (to pid 1, or anything else).
        assert!(parent_is_gone(4242, 1));
        assert!(parent_is_gone(4242, 9999));
    }

    /// The config-less admin router omits the stats route entirely (404), so the
    /// server never fabricates zeros when there is no engine to report on.
    #[tokio::test]
    async fn admin_router_without_stats_omits_the_stats_route() {
        let app = admin::router(VERSION, admin::Health::ready(), admin::Slots::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let status = reqwest::get(format!("http://{addr}{STATS_PATH}"))
            .await
            .expect("request")
            .status();
        assert_eq!(status.as_u16(), 404);

        server.abort();
    }
}
