//! Server-instance reporting to the control plane (issue: node tracking).
//!
//! A logged-in server registers itself with the tenant's control plane and
//! heartbeats every few minutes, so the control plane can count active nodes per
//! tenant (licensing/billing) and the dashboard can show the fleet. The node is
//! identified by [`Config::resolve_cluster_id`] — the server's existing cluster
//! id (env > `[cluster].id` > hostname > `"localhost"`), never a second identity
//! and never an API key.
//!
//! PRIVACY INVARIANT (release-blocking): a server with NO `[control_plane]`
//! configuration makes NO network call to any control plane — a self-hosted
//! install never phones home. [`NodeReporter::from_config`] returns `None` unless
//! the machine looks logged in: a `[control_plane].url` is set AND the
//! control-plane token file (`~/.verglas/credentials/control-plane-token`,
//! written by `verglas login`) exists and is non-empty. No reporter ⇒ no task ⇒
//! no request. The reporter is also spawned fully detached from the data plane:
//! it never gates readiness and a dead control plane never affects serving.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;
use serde_json::{Value, json};

use verglas_core::admin::StatsInfo;
use verglas_core::config::Config;
use verglas_core::telemetry::TableCumulative;

use crate::VERSION;
use crate::admin::StatsSlot;

/// Heartbeat cadence. One constant, no config knob. The control plane's active
/// window is 2x this (10 minutes), so a single missed beat still reads active.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// How many metering windows to retain when the control plane is unreachable.
/// Small and bounded: with a 5-minute cadence this is an hour of buffered usage.
/// Beyond it, the oldest window is dropped (and the count logged) — never
/// unbounded memory on the serving box.
const MAX_BUFFERED_WINDOWS: usize = 12;

/// A source of the current cumulative per-table metering figures, filled after
/// recovery (like the stats slot). `None`/unfilled ⇒ heartbeats carry no
/// metering, which is exactly what an older server or a pre-recovery server
/// sends, and the control plane accepts.
pub type MeteringSource = std::sync::Arc<dyn Fn() -> Vec<TableCumulative> + Send + Sync>;

/// A deferred metering source, mirroring [`StatsSlot`].
pub type MeteringSlot = std::sync::Arc<OnceLock<MeteringSource>>;

/// The register endpoint, relative to the control-plane base URL.
const REGISTER_PATH: &str = "/v1/nodes/register";
/// The heartbeat endpoint, relative to the control-plane base URL.
const HEARTBEAT_PATH: &str = "/v1/nodes/heartbeat";

/// The token file `verglas login` writes, relative to `~/.verglas`.
const TOKEN_REL: &str = ".verglas/credentials/control-plane-token";

/// The identity a node registers under: its resolved cluster id, hostname, and
/// build/platform facts. Reuses the server's own cluster-id resolution.
#[derive(Clone, Debug)]
pub struct NodeIdentity {
    /// The server's cluster id — the stable per-tenant node key.
    pub node_id: String,
    /// The machine hostname, for display in the dashboard.
    pub name: String,
    /// The `verglas-server` crate version.
    pub version: String,
    /// The build OS (`std::env::consts::OS`), e.g. `linux`, `macos`.
    pub os: String,
    /// The build architecture (`std::env::consts::ARCH`), e.g. `x86_64`.
    pub arch: String,
}

impl NodeIdentity {
    /// Resolves this server's identity from its config. `node_id` is exactly
    /// [`Config::resolve_cluster_id`]; `name` is the machine hostname.
    pub fn from_config(config: &Config) -> NodeIdentity {
        NodeIdentity {
            node_id: config.resolve_cluster_id(),
            name: verglas_core::config::machine_hostname()
                .unwrap_or_else(|| "localhost".to_owned()),
            version: VERSION.to_owned(),
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
        }
    }
}

/// Failures talking to the control plane. Never surfaced to a user — the run
/// loop logs them at debug and retries on the next tick.
#[derive(Debug, thiserror::Error)]
pub enum ReporterError {
    /// The configured base URL could not be parsed.
    #[error("invalid control plane URL `{0}`")]
    InvalidUrl(String),
    /// The request could not be sent.
    #[error("control plane request failed: {0}")]
    Request(#[from] reqwest::Error),
    /// The control plane returned a non-success status.
    #[error("control plane returned HTTP {status} for {path}")]
    Status {
        /// The status returned.
        status: StatusCode,
        /// The path that returned it.
        path: &'static str,
    },
}

/// Reports this server to the control plane: one register on boot, then a
/// heartbeat every [`HEARTBEAT_INTERVAL`]. Holds a reqwest client, the base URL,
/// the bearer token, the node identity, and an optional stats source the
/// heartbeat snapshots into its `metrics` body.
pub struct NodeReporter {
    http: reqwest::Client,
    base: reqwest::Url,
    token: String,
    node: NodeIdentity,
    started: Instant,
    stats: Option<StatsSlot>,
    /// Per-table metering source (#60). `None` ⇒ heartbeats carry no metering.
    metering: Option<MeteringSlot>,
    /// The delta-windowing state: the last cumulative snapshot, the pending
    /// (buffered) windows, and the previous window boundary. Behind a mutex so
    /// the `&self` heartbeat can advance it each tick.
    metering_state: Mutex<MeteringState>,
}

impl NodeReporter {
    /// Builds a reporter ONLY for a logged-in machine (the privacy invariant):
    /// returns `None` unless `[control_plane].url` is set AND the control-plane
    /// token file exists and is non-empty. `stats` is the server's stats slot,
    /// used to fill each heartbeat's metrics once the engine is ready; heartbeats
    /// before it fills simply carry no metrics.
    pub fn from_config(config: &Config, stats: Option<StatsSlot>) -> Option<NodeReporter> {
        // Gate on the config FIRST: with no [control_plane], never even look at
        // the token file — a self-hosted server does nothing here.
        let url = config.control_plane.as_ref()?.url.clone();
        let token = read_token()?;
        Self::new(&url, &token, NodeIdentity::from_config(config), stats).ok()
    }

    /// Builds a reporter against an explicit base URL and token. Public so tests
    /// can point one at a mock; production goes through [`Self::from_config`]. A
    /// trailing slash is added so relative paths join onto the base.
    pub fn new(
        base_url: &str,
        token: &str,
        node: NodeIdentity,
        stats: Option<StatsSlot>,
    ) -> Result<NodeReporter, ReporterError> {
        let normalized = if base_url.ends_with('/') {
            base_url.to_owned()
        } else {
            format!("{base_url}/")
        };
        let base = reqwest::Url::parse(&normalized)
            .map_err(|_| ReporterError::InvalidUrl(base_url.to_owned()))?;
        Ok(NodeReporter {
            http: reqwest::Client::new(),
            base,
            token: token.to_owned(),
            node,
            started: Instant::now(),
            stats,
            metering: None,
            metering_state: Mutex::new(MeteringState::new(unix_secs())),
        })
    }

    /// Attaches the per-table metering source (#60). Builder-style so the #358
    /// register/heartbeat wiring and its tests are untouched. With no source
    /// attached, heartbeats carry no metering — an older-server-compatible body
    /// the control plane accepts.
    pub fn with_metering(mut self, metering: MeteringSlot) -> NodeReporter {
        self.metering = Some(metering);
        self
    }

    /// Registers this node (`POST /v1/nodes/register`). Idempotent on the control
    /// plane: first sight creates the row, a re-register refreshes it.
    pub async fn register(&self) -> Result<(), ReporterError> {
        let body = json!({
            "node_id": self.node.node_id,
            "name": self.node.name,
            "version": self.node.version,
            "os": self.node.os,
            "arch": self.node.arch,
        });
        self.post(REGISTER_PATH, &body).await
    }

    /// Heartbeats (`POST /v1/nodes/heartbeat`), carrying the current metrics
    /// snapshot when the stats slot is filled. The control plane advances
    /// last_seen and stores the latest metrics.
    pub async fn heartbeat(&self) -> Result<(), ReporterError> {
        let mut body = json!({
            "node_id": self.node.node_id,
            "version": self.node.version,
        });
        if let Some(metrics) = self.metrics_snapshot() {
            body["metrics"] = metrics;
        }
        // Per-table metering (#60): roll this window's DELTAS and carry every
        // still-unsent window. Billing needs additive, gap-free windows, so a
        // heartbeat that fails to reach the control plane keeps its windows
        // buffered (bounded) and the next heartbeat resends them.
        let pending = self.roll_metering_window();
        if !pending.is_empty() {
            body["metering"] = json!({
                "windows": pending.iter().map(MeteringWindow::to_json).collect::<Vec<_>>(),
            });
        }
        let result = self.post(HEARTBEAT_PATH, &body).await;
        if result.is_ok() {
            // The control plane accepted (and idempotently appended) the windows;
            // drop them from the buffer.
            self.clear_pending_metering();
        }
        result
    }

    /// Advances the metering window: reads the current cumulative snapshot,
    /// diffs it against the last one into a delta window, buffers that window
    /// (dropping the oldest beyond the cap, with a logged count), and returns the
    /// full set of still-unsent windows to carry on this heartbeat. A no-op
    /// (empty) when no metering source is wired or not yet filled.
    fn roll_metering_window(&self) -> Vec<MeteringWindow> {
        let Some(source) = self.metering.as_ref().and_then(|s| s.get()) else {
            return Vec::new();
        };
        let snapshot = source();
        let now = unix_secs();
        let mut state = self
            .metering_state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let window = compute_window(&state.last, &snapshot, state.window_end, now);
        state.window_end = now;
        state.last = snapshot.into_iter().map(|c| (c.table.clone(), c)).collect();
        // Only buffer a window that actually carries usage.
        if !window.per_table.is_empty() {
            let dropped = push_bounded(&mut state.pending, window, MAX_BUFFERED_WINDOWS);
            if dropped > 0 {
                state.dropped_windows += dropped;
                tracing::warn!(
                    dropped,
                    total_dropped = state.dropped_windows,
                    "control plane unreachable; dropped oldest metering window(s) past the buffer cap"
                );
            }
        }
        state.pending.iter().cloned().collect()
    }

    /// Clears the buffered metering windows after a successful heartbeat.
    fn clear_pending_metering(&self) {
        self.metering_state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pending
            .clear();
    }

    /// Snapshots the server's live stats into the flat metrics object the
    /// heartbeat carries, or `None` when the stats source is not yet available
    /// (the engine has not finished recovery). Reads only existing counters — no
    /// new instrumentation, nothing on any hot path.
    fn metrics_snapshot(&self) -> Option<Value> {
        let source = self.stats.as_ref()?.get()?;
        let stats = source();
        Some(metrics_from_stats(&stats, self.started.elapsed().as_secs()))
    }

    /// Sends a bearer-authenticated JSON POST to `path`.
    async fn post(&self, path: &'static str, body: &Value) -> Result<(), ReporterError> {
        let url = self
            .base
            .join(path.trim_start_matches('/'))
            .map_err(|_| ReporterError::InvalidUrl(path.to_owned()))?;
        let response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(ReporterError::Status { status, path });
        }
        Ok(())
    }

    /// The detached run loop: register once on boot, then heartbeat forever at
    /// the fixed interval. Every failure is logged at debug and retried on the
    /// next tick — a dead or unreachable control plane never affects serving.
    pub async fn run(self) {
        if let Err(e) = self.register().await {
            tracing::debug!(error = %e, "control-plane node register failed; will retry on heartbeat");
        }
        let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
        // The first tick fires immediately; consume it so heartbeats start one
        // full interval after boot (register already covered T=0).
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(e) = self.heartbeat().await {
                tracing::debug!(error = %e, "control-plane node heartbeat failed; will retry next tick");
            }
        }
    }
}

/// Maps a stats snapshot into the flat, numeric metrics object a heartbeat
/// carries. Computed entirely from counters the server already tracks (what
/// `/admin/stats` and `verglas status` read) — no new instrumentation. Kept
/// small: a dozen numbers the dashboard draws the fleet from.
pub fn metrics_from_stats(stats: &StatsInfo, uptime_secs: u64) -> Value {
    let c = &stats.counters;
    let hits = c.dram_hits + c.disk_hits + c.peer_hits + c.meta_hits;
    let misses = c.dram_misses + c.disk_misses + c.meta_misses;
    let lookups = hits + misses;
    let hit_rate = if lookups > 0 {
        hits as f64 / lookups as f64
    } else {
        0.0
    };
    let bytes_served = c.dram_bytes_served
        + c.disk_bytes_served
        + c.peer_bytes_served
        + c.backend_bytes_served
        + c.meta_bytes_served;
    json!({
        "uptime_secs": uptime_secs,
        "nvme_capacity_bytes": stats.cache.capacity_bytes,
        "dram_capacity_bytes": stats.cache.dram_bytes,
        "dram_used_bytes": stats.dram_usage_bytes,
        "cache_hits": hits,
        "cache_misses": misses,
        "hit_rate": hit_rate,
        "backend_fills": c.backend_fills,
        "bytes_served": bytes_served,
        "tables_warmed": stats.warming.as_ref().map(|w| w.tables_completed).unwrap_or(0),
    })
}

/// The delta-windowing state carried between heartbeats.
struct MeteringState {
    /// The last cumulative per-table snapshot, keyed by table name.
    last: HashMap<String, TableCumulative>,
    /// Unsent windows, oldest first, bounded by [`MAX_BUFFERED_WINDOWS`].
    pending: VecDeque<MeteringWindow>,
    /// The end of the previous window (unix seconds) = the start of the next.
    window_end: u64,
    /// Total windows dropped because the buffer was full (control plane down).
    dropped_windows: u64,
}

impl MeteringState {
    fn new(boot_wall: u64) -> MeteringState {
        MeteringState {
            last: HashMap::new(),
            pending: VecDeque::new(),
            // The first window starts at boot, so the first heartbeat's deltas
            // (counters are cumulative since boot) cover [boot, now] with no gap.
            window_end: boot_wall,
            dropped_windows: 0,
        }
    }
}

/// One table's usage delta within a window.
#[derive(Debug, Clone, PartialEq)]
struct TableDelta {
    table: String,
    hits: u64,
    misses: u64,
    bytes_served: u64,
    requests_avoided: u64,
}

/// A metering window: additive per-table deltas over `[window_start,
/// window_end)`. `window_start` doubles as the idempotency key on the control
/// plane, so a resent window never double-counts.
#[derive(Debug, Clone, PartialEq)]
struct MeteringWindow {
    window_start: u64,
    window_end: u64,
    per_table: Vec<TableDelta>,
}

impl MeteringWindow {
    fn to_json(&self) -> Value {
        json!({
            "window_start": self.window_start,
            "window_end": self.window_end,
            "per_table": self.per_table.iter().map(|d| json!({
                "table": d.table,
                "hits": d.hits,
                "misses": d.misses,
                "bytes_served": d.bytes_served,
                "requests_avoided": d.requests_avoided,
            })).collect::<Vec<_>>(),
        })
    }
}

/// Diffs a cumulative snapshot against the previous one into a window of
/// per-table deltas. Counters are monotonic since boot, so a saturating
/// subtraction yields this window's contribution; a table absent from `prev` is
/// new and contributes its full snapshot value. Tables whose every field is
/// unchanged are omitted, so an idle table adds nothing to the window.
fn compute_window(
    prev: &HashMap<String, TableCumulative>,
    snapshot: &[TableCumulative],
    window_start: u64,
    window_end: u64,
) -> MeteringWindow {
    let mut per_table = Vec::new();
    for cur in snapshot {
        let base = prev.get(&cur.table);
        let d = TableDelta {
            table: cur.table.clone(),
            hits: cur.hits.saturating_sub(base.map_or(0, |b| b.hits)),
            misses: cur.misses.saturating_sub(base.map_or(0, |b| b.misses)),
            bytes_served: cur
                .bytes_served
                .saturating_sub(base.map_or(0, |b| b.bytes_served)),
            requests_avoided: cur
                .requests_avoided
                .saturating_sub(base.map_or(0, |b| b.requests_avoided)),
        };
        if d.hits != 0 || d.misses != 0 || d.bytes_served != 0 || d.requests_avoided != 0 {
            per_table.push(d);
        }
    }
    per_table.sort_by(|a, b| a.table.cmp(&b.table));
    MeteringWindow {
        window_start,
        window_end,
        per_table,
    }
}

/// Pushes `window` onto the bounded buffer, dropping oldest windows past `cap`.
/// Returns how many were dropped.
fn push_bounded(buffer: &mut VecDeque<MeteringWindow>, window: MeteringWindow, cap: usize) -> u64 {
    buffer.push_back(window);
    let mut dropped = 0;
    while buffer.len() > cap {
        buffer.pop_front();
        dropped += 1;
    }
    dropped
}

/// Wall-clock unix seconds, saturating to 0 before the epoch (never in practice).
fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The control-plane token file path (`~/.verglas/credentials/control-plane-token`),
/// matching where `verglas login` writes it. `None` when HOME is unset.
fn token_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    Some(PathBuf::from(home).join(TOKEN_REL))
}

/// Reads the stored control-plane token, or `None` when the file is absent or
/// empty. The presence of this file (plus a `[control_plane]` URL) is what
/// distinguishes a logged-in machine from a self-hosted one.
fn read_token() -> Option<String> {
    let text = std::fs::read_to_string(token_path()?).ok()?;
    let trimmed = text.trim().to_owned();
    (!trimmed.is_empty()).then_some(trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use verglas_core::admin::{CacheConfigInfo, CountersInfo, WarmingInfo};

    fn zeroed_counters() -> CountersInfo {
        CountersInfo {
            dram_hits: 0,
            dram_misses: 0,
            disk_hits: 0,
            disk_misses: 0,
            peer_hits: 0,
            peer_misses: 0,
            peer_errors: 0,
            peer_served_blocks: 0,
            peer_served_bytes: 0,
            dram_bytes_served: 0,
            disk_bytes_served: 0,
            peer_bytes_served: 0,
            backend_bytes_served: 0,
            backend_fills: 0,
            backend_fill_bytes: 0,
            backend_heads: 0,
            non_cacheable_passthroughs: 0,
            meta_hits: 0,
            meta_misses: 0,
            meta_bytes_served: 0,
            retired_bytes_pending: 0,
            retired_bytes_reclaimed: 0,
            retired_files_reclaimed: 0,
        }
    }

    #[test]
    fn metrics_map_pools_hits_and_misses_across_tiers() {
        let mut counters = zeroed_counters();
        counters.dram_hits = 50;
        counters.disk_hits = 30;
        counters.meta_hits = 20; // 100 hits
        counters.dram_misses = 40;
        counters.disk_misses = 10; // 50 misses
        counters.backend_fills = 7;
        let stats = StatsInfo {
            cache: CacheConfigInfo {
                dir: "/c".to_owned(),
                capacity_bytes: 100,
                dram_bytes: 10,
            },
            counters,
            dram_usage_bytes: 5,
            dram_live_bytes: 5,
            dram_reclaimable_bytes: 0,
            warming: Some(WarmingInfo {
                tables_completed: 2,
                ..Default::default()
            }),
            writeback: None,
        };
        let m = metrics_from_stats(&stats, 123);
        assert_eq!(m["cache_hits"], json!(100));
        assert_eq!(m["cache_misses"], json!(50));
        assert_eq!(m["uptime_secs"], json!(123));
        assert_eq!(m["nvme_capacity_bytes"], json!(100));
        assert_eq!(m["backend_fills"], json!(7));
        assert_eq!(m["tables_warmed"], json!(2));
        let hit_rate = m["hit_rate"].as_f64().expect("hit_rate is a number");
        assert!((hit_rate - 100.0 / 150.0).abs() < 1e-9);
    }

    #[test]
    fn metrics_hit_rate_is_zero_before_any_lookup() {
        let stats = StatsInfo {
            cache: CacheConfigInfo {
                dir: "/c".to_owned(),
                capacity_bytes: 1,
                dram_bytes: 1,
            },
            counters: zeroed_counters(),
            dram_usage_bytes: 0,
            dram_live_bytes: 0,
            dram_reclaimable_bytes: 0,
            warming: None,
            writeback: None,
        };
        let m = metrics_from_stats(&stats, 0);
        assert_eq!(m["hit_rate"], json!(0.0));
        assert_eq!(m["tables_warmed"], json!(0));
    }

    fn cumulative(
        table: &str,
        hits: u64,
        misses: u64,
        bytes: u64,
        avoided: u64,
    ) -> TableCumulative {
        TableCumulative {
            table: table.to_owned(),
            hits,
            misses,
            bytes_served: bytes,
            requests_avoided: avoided,
        }
    }

    #[test]
    fn window_deltas_are_the_difference_since_the_last_snapshot() {
        let mut prev = HashMap::new();
        prev.insert(
            "db.orders".to_owned(),
            cumulative("db.orders", 100, 10, 5000, 100),
        );
        // Same table advanced; a brand-new table appears.
        let snapshot = vec![
            cumulative("db.orders", 150, 12, 8000, 150),
            cumulative("db.users", 7, 3, 900, 7),
        ];
        let window = compute_window(&prev, &snapshot, 1000, 1300);
        assert_eq!(window.window_start, 1000);
        assert_eq!(window.window_end, 1300);
        assert_eq!(window.per_table.len(), 2);
        let orders = window
            .per_table
            .iter()
            .find(|d| d.table == "db.orders")
            .expect("orders delta");
        // Deltas, not cumulative totals.
        assert_eq!(orders.hits, 50);
        assert_eq!(orders.misses, 2);
        assert_eq!(orders.bytes_served, 3000);
        assert_eq!(orders.requests_avoided, 50);
        let users = window
            .per_table
            .iter()
            .find(|d| d.table == "db.users")
            .expect("users delta");
        assert_eq!(users.hits, 7, "a new table contributes its full value");
    }

    #[test]
    fn a_table_with_no_change_is_omitted_from_the_window() {
        let mut prev = HashMap::new();
        prev.insert(
            "db.orders".to_owned(),
            cumulative("db.orders", 100, 10, 5000, 100),
        );
        let snapshot = vec![cumulative("db.orders", 100, 10, 5000, 100)];
        let window = compute_window(&prev, &snapshot, 0, 300);
        assert!(window.per_table.is_empty(), "idle table adds nothing");
    }

    #[test]
    fn buffer_drops_oldest_windows_past_the_cap() {
        let mut buffer = VecDeque::new();
        for start in 0..15u64 {
            let w = MeteringWindow {
                window_start: start,
                window_end: start + 1,
                per_table: vec![TableDelta {
                    table: "t".to_owned(),
                    hits: 1,
                    misses: 0,
                    bytes_served: 1,
                    requests_avoided: 1,
                }],
            };
            push_bounded(&mut buffer, w, MAX_BUFFERED_WINDOWS);
        }
        assert_eq!(buffer.len(), MAX_BUFFERED_WINDOWS);
        // The oldest (0,1,2) were dropped; the newest cap windows remain.
        assert_eq!(
            buffer.front().expect("front window").window_start,
            15 - MAX_BUFFERED_WINDOWS as u64
        );
        assert_eq!(buffer.back().expect("back window").window_start, 14);
    }
}
