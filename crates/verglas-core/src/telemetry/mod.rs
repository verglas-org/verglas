//! Per-table access telemetry (#60): the hot-path tape, the background rollup,
//! and the Prometheus families they project.
//!
//! Node-level metrics (#46) say the cache is healthy; these say *which of the
//! customer's tables* Verglas is accelerating and what it is worth — "your
//! `orders` table: 96% hit rate, 41M backend requests avoided this month." The
//! design keeps the request path almost free: it appends a 16-byte
//! [`tape::AccessEvent`] to a per-core tape (see [`tape`]) and a background task
//! folds those into labelled counters (see [`rollup`]).
//!
//! # Shape
//!
//! - [`Telemetry`] is the shared hub: N per-core [`tape::ShardTape`]s plus the
//!   [`rollup::Rollup`] behind a mutex. The serving path clones an `Arc` and
//!   calls [`Telemetry::record`] (hot). A background task calls
//!   [`Telemetry::roll_up`] ~1s (drains tapes → folds counters). Readers call
//!   [`Telemetry::table_report`] / [`Telemetry::render_families`] /
//!   [`Telemetry::metering_snapshot`] at scrape or heartbeat time.
//! - This crate provides no timer: the daemon (which owns a runtime) spawns the
//!   periodic `roll_up`, keeping `verglas-core` free of an async dependency.

pub mod rollup;
pub mod tape;

use std::fmt::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::read::ServedTier;

pub use rollup::{FleetTotals, Rollup, TableCumulative, TableMetricRow, TablesReport};
pub use tape::AccessEvent;

/// The interned id reserved for access that maps to no watched table.
pub const UNMAPPED: u32 = 0;
/// The interned id all table ids past the cardinality cap fold into.
pub const OVERFLOW: u32 = u32::MAX;

/// Encodes a mapper `TableId` (0-based) into a telemetry table id, reserving `0`
/// for `_unmapped`. The mapper hands out ids from 0, so telemetry shifts them up
/// by one: this is where the issue's "`0` = `_unmapped`" convention is enforced,
/// without renumbering the mapper's ids (which other subsystems key on).
pub fn encode_table_id(mapper_id: u32) -> u32 {
    mapper_id.saturating_add(1)
}

/// Decodes a telemetry table id back to the mapper's `TableId`, or `None` for
/// the reserved sentinels (`_unmapped` / `_overflow`).
pub fn decode_table_id(table: u32) -> Option<u32> {
    if table == UNMAPPED || table == OVERFLOW {
        None
    } else {
        Some(table - 1)
    }
}

/// The serving tier an access came from — the telemetry mirror of
/// [`ServedTier`], stored as a `u8` code inside the fixed-size event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Local DRAM tier (or the pinned metadata store).
    Dram,
    /// Local NVMe/disk tier.
    Nvme,
    /// A peer node's cache.
    Peer,
    /// A backend cache-fill (a miss that fetched and admitted).
    Backend,
    /// Direct-to-origin passthrough (no cache in the path).
    Passthrough,
}

impl Tier {
    /// The `u8` code stored in an [`AccessEvent`]. Matches [`ServedTier`]'s
    /// internal codes so the two stay aligned.
    pub fn code(self) -> u8 {
        match self {
            Tier::Dram => 0,
            Tier::Nvme => 1,
            Tier::Peer => 2,
            Tier::Backend => 3,
            Tier::Passthrough => 4,
        }
    }

    /// Decodes a tier code; `None` for an unrecognised byte.
    pub fn from_code(code: u8) -> Option<Tier> {
        match code {
            0 => Some(Tier::Dram),
            1 => Some(Tier::Nvme),
            2 => Some(Tier::Peer),
            3 => Some(Tier::Backend),
            4 => Some(Tier::Passthrough),
            _ => None,
        }
    }

    /// Maps a [`ServedTier`] to its telemetry tier.
    pub fn from_served(tier: ServedTier) -> Tier {
        match tier {
            ServedTier::Dram => Tier::Dram,
            ServedTier::Nvme => Tier::Nvme,
            ServedTier::Peer => Tier::Peer,
            ServedTier::Backend => Tier::Backend,
            ServedTier::Passthrough => Tier::Passthrough,
        }
    }

    /// Whether serving from this tier avoided a backend GET (dram/nvme/peer).
    pub fn is_hit(self) -> bool {
        matches!(self, Tier::Dram | Tier::Nvme | Tier::Peer)
    }
}

/// The S3 operation an access was — the telemetry mirror of `metrics::RequestOp`.
/// Only [`Op::Get`] currently flows through the tape (the serving decorator
/// wraps GET reads), but the dimension is carried for later ops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// GetObject.
    Get,
    /// HeadObject.
    Head,
    /// List.
    List,
    /// PutObject.
    Put,
}

impl Op {
    /// The `u8` code stored in an [`AccessEvent`].
    pub fn code(self) -> u8 {
        match self {
            Op::Get => 0,
            Op::Head => 1,
            Op::List => 2,
            Op::Put => 3,
        }
    }
}

/// Chooses the shard sizing: one tape per core, so the only atomics a record
/// touches are that core's own. Falls back to a single shard when parallelism is
/// unknown.
fn default_shard_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1)
}

std::thread_local! {
    /// This thread's shard index, assigned round-robin on first record. Global
    /// across [`Telemetry`] instances and taken modulo the shard count at use,
    /// so it stays in range even if a later instance has fewer shards.
    static SHARD_INDEX: std::cell::Cell<usize> = const { std::cell::Cell::new(usize::MAX) };
}

/// The shared telemetry hub: per-core tapes on the write side, one rollup on the
/// read side. Cheap to clone behind an `Arc`.
pub struct Telemetry {
    shards: Vec<Arc<tape::ShardTape>>,
    next_shard: AtomicUsize,
    rollup: Mutex<Rollup>,
}

impl Telemetry {
    /// A hub sized to the machine's parallelism with the default cardinality
    /// cap.
    pub fn new() -> Telemetry {
        Telemetry::with_shards(default_shard_count(), Rollup::new())
    }

    /// A hub with an explicit shard count and rollup (tests drive both).
    pub fn with_shards(shards: usize, rollup: Rollup) -> Telemetry {
        let shards = (0..shards.max(1))
            .map(|_| Arc::new(tape::ShardTape::new()))
            .collect();
        Telemetry {
            shards,
            next_shard: AtomicUsize::new(0),
            rollup: Mutex::new(rollup),
        }
    }

    /// Records one served-read event. **Hot path.** One thread-local shard
    /// lookup and one [`tape::ShardTape::record`] — see [`tape`] for the cost
    /// contract. Never allocates, locks, or formats a label.
    #[inline]
    pub fn record(&self, event: AccessEvent) {
        let n = self.shards.len();
        let idx = SHARD_INDEX.with(|cell| {
            let mut i = cell.get();
            if i == usize::MAX {
                i = self.next_shard.fetch_add(1, Ordering::Relaxed);
                cell.set(i);
            }
            i
        }) % n;
        self.shards[idx].record(event);
    }

    /// Drains every shard's inactive buffer and folds the events into the
    /// rollup, mirroring the shards' dropped-event total. Called ~1s by the
    /// daemon's background task; off any hot path.
    pub fn roll_up(&self) {
        let mut events = Vec::new();
        for shard in &self.shards {
            shard.drain_into(&mut events);
        }
        let dropped: u64 = self.shards.iter().map(|s| s.dropped()).sum();
        let mut rollup = self.rollup.lock().unwrap_or_else(|p| p.into_inner());
        rollup.fold(&events);
        rollup.set_dropped(dropped);
    }

    /// The per-table report for the admin API / CLI, ids resolved to names by
    /// `resolve`.
    pub fn table_report<F>(&self, resolve: F) -> TablesReport
    where
        F: Fn(u32) -> Option<String>,
    {
        self.rollup
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .report(resolve)
    }

    /// The cumulative per-table figures the metering exporter diffs.
    pub fn metering_snapshot<F>(&self, resolve: F) -> Vec<TableCumulative>
    where
        F: Fn(u32) -> Option<String>,
    {
        self.rollup
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .metering_snapshot(resolve)
    }

    /// Appends the per-table Prometheus families to `out`, extending the #46
    /// exposition. Raw counts only — no dollars (those are multiplied in at
    /// display time). `resolve` maps ids to `table` label values.
    pub fn render_families<F>(&self, resolve: F, out: &mut String)
    where
        F: Fn(u32) -> Option<String>,
    {
        let report = self.table_report(resolve);

        let _ = writeln!(
            out,
            "# HELP verglas_requests_avoided_total Backend GET requests avoided by serving a table's read from cache (dram/nvme/peer)."
        );
        let _ = writeln!(out, "# TYPE verglas_requests_avoided_total counter");
        for row in &report.tables {
            let _ = writeln!(
                out,
                "verglas_requests_avoided_total{{table=\"{}\"}} {}",
                escape(&row.table),
                row.requests_avoided
            );
        }

        let _ = writeln!(
            out,
            "# HELP verglas_latency_saved_seconds Estimated client latency saved by cache hits for a table (approximate — from per-table latency EWMAs)."
        );
        let _ = writeln!(out, "# TYPE verglas_latency_saved_seconds gauge");
        for row in &report.tables {
            let _ = writeln!(
                out,
                "verglas_latency_saved_seconds{{table=\"{}\"}} {}",
                escape(&row.table),
                row.latency_saved_seconds
            );
        }

        let _ = writeln!(
            out,
            "# HELP verglas_table_bytes_served_total Bytes served for a table across all tiers."
        );
        let _ = writeln!(out, "# TYPE verglas_table_bytes_served_total counter");
        for row in &report.tables {
            let _ = writeln!(
                out,
                "verglas_table_bytes_served_total{{table=\"{}\"}} {}",
                escape(&row.table),
                row.bytes_served
            );
        }

        // Sampling / cardinality health, so a scrape can see when telemetry is
        // shedding under load or has hit the table cap.
        let _ = writeln!(
            out,
            "# HELP verglas_telemetry_dropped_events_total Access events dropped at record time because a tape buffer was full."
        );
        let _ = writeln!(out, "# TYPE verglas_telemetry_dropped_events_total counter");
        let _ = writeln!(
            out,
            "verglas_telemetry_dropped_events_total {}",
            report.dropped_events
        );
    }
}

impl Default for Telemetry {
    fn default() -> Self {
        Telemetry::new()
    }
}

/// Escapes a Prometheus label value (`\` and `"` and newlines). Table names are
/// dotted identifiers so this rarely fires, but the exposition must stay
/// well-formed.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_event(table: u32, tier: Tier, bytes: u32, latency_us: u32) -> AccessEvent {
        AccessEvent {
            table,
            tier: tier.code(),
            op: Op::Get.code(),
            _pad: 0,
            bytes,
            latency_us,
        }
    }

    #[test]
    fn record_then_roll_up_reaches_the_report() {
        let tel = Telemetry::with_shards(4, Rollup::new());
        tel.record(get_event(1, Tier::Dram, 100, 30));
        tel.record(get_event(1, Tier::Backend, 500, 4000));
        tel.roll_up();
        let report = tel.table_report(|id| (id == 1).then(|| "db.orders".to_owned()));
        let row = report
            .tables
            .iter()
            .find(|t| t.table == "db.orders")
            .expect("orders present");
        assert_eq!(row.hits, 1);
        assert_eq!(row.misses, 1);
        assert_eq!(row.requests_avoided, 1);
    }

    #[test]
    fn render_families_emits_the_derived_series() {
        let tel = Telemetry::with_shards(2, Rollup::new());
        tel.record(get_event(1, Tier::Dram, 100, 30));
        tel.roll_up();
        let mut out = String::new();
        tel.render_families(|id| (id == 1).then(|| "db.orders".to_owned()), &mut out);
        assert!(out.contains("verglas_requests_avoided_total{table=\"db.orders\"} 1"));
        assert!(out.contains("# TYPE verglas_latency_saved_seconds gauge"));
        assert!(out.contains("verglas_telemetry_dropped_events_total 0"));
    }

    #[test]
    fn table_id_encoding_reserves_zero_for_unmapped() {
        // Mapper id 0 must not collide with the _unmapped sentinel.
        assert_eq!(encode_table_id(0), 1);
        assert_eq!(decode_table_id(1), Some(0));
        assert_eq!(decode_table_id(UNMAPPED), None);
        assert_eq!(decode_table_id(OVERFLOW), None);
        for mapper_id in [0u32, 1, 42, 999] {
            assert_eq!(decode_table_id(encode_table_id(mapper_id)), Some(mapper_id));
        }
    }

    #[test]
    fn tier_codes_round_trip_with_served_tier() {
        for tier in [
            ServedTier::Dram,
            ServedTier::Nvme,
            ServedTier::Peer,
            ServedTier::Backend,
            ServedTier::Passthrough,
        ] {
            let t = Tier::from_served(tier);
            assert_eq!(Tier::from_code(t.code()), Some(t));
        }
    }
}
