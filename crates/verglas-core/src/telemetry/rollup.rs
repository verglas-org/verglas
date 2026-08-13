//! Background aggregation of drained [`AccessEvent`]s into labelled per-table
//! counters (#60).
//!
//! The rollup owns the real state the request path never touches: a
//! `HashMap<(table, tier, op), CounterSet>` of raw counts and bytes, plus a
//! per-table latency EWMA pair used only to *estimate* latency saved. It is fed
//! by [`super::Telemetry::roll_up`] roughly once a second and read at scrape
//! time; nothing here is on a hot path, so it uses plain maps and floats.
//!
//! **Cardinality is bounded by construction.** Real tables come only from the
//! mapper's watched set (≤ ~1,000). To stay safe even if a misconfiguration or a
//! synthetic flood produces more distinct ids, the rollup caps the number of
//! distinct real table ids it will track; every id past the cap folds into the
//! single [`OVERFLOW`] series. Unmapped access (`table == 0`) folds into
//! [`UNMAPPED`]. Table *names* are resolved from ids only at scrape time, never
//! stored per event.
//!
//! **Dollars are never stored here.** The rollup keeps raw counts only; the
//! dollar value of avoided requests is multiplied in at read time (the CLI /
//! dashboard) from a published price, so a price change re-prices history
//! instead of freezing a wrong number into a counter.

use std::collections::HashMap;
use std::collections::HashSet;

use super::tape::AccessEvent;
use super::{OVERFLOW, Tier, UNMAPPED};

/// EWMA smoothing factor for the per-table latency estimates. A middling value:
/// responsive to a shift in backend latency without letting one slow fill swing
/// the estimate.
const EWMA_ALPHA: f64 = 0.2;

/// Default hard cap on distinct real table ids the rollup tracks. The mapper's
/// watched set is ≤ ~1,000; this leaves headroom while still guaranteeing the
/// series count can never explode. Ids past the cap fold into [`OVERFLOW`].
pub const DEFAULT_TABLE_CAP: usize = 4096;

/// Raw counts for one `(table, tier, op)` cell. Counts only — no derived or
/// priced values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CounterSet {
    /// Number of served reads recorded in this cell.
    pub count: u64,
    /// Total bytes served across those reads.
    pub bytes: u64,
}

/// Per-table latency EWMAs, in microseconds, used to estimate latency saved.
/// One EWMA over hit-served latency and one over backend-fill latency; the
/// saving per hit is their (clamped) difference.
#[derive(Debug, Clone, Copy, Default)]
struct LatencyEwma {
    served_us: f64,
    backend_us: f64,
    have_served: bool,
    have_backend: bool,
}

impl LatencyEwma {
    fn observe(&mut self, tier: Tier, latency_us: u32) {
        let x = latency_us as f64;
        if tier == Tier::Backend {
            self.backend_us = ewma(self.backend_us, x, self.have_backend);
            self.have_backend = true;
        } else if tier.is_hit() {
            self.served_us = ewma(self.served_us, x, self.have_served);
            self.have_served = true;
        }
    }

    /// Estimated seconds saved per hit: backend-fill latency minus hit-served
    /// latency, clamped at zero, in seconds. `None` until both sides have a
    /// sample (no estimate is better than a fabricated one).
    fn saved_per_hit_secs(&self) -> Option<f64> {
        if self.have_served && self.have_backend {
            Some(((self.backend_us - self.served_us).max(0.0)) / 1_000_000.0)
        } else {
            None
        }
    }
}

fn ewma(prev: f64, x: f64, initialised: bool) -> f64 {
    if initialised {
        EWMA_ALPHA * x + (1.0 - EWMA_ALPHA) * prev
    } else {
        x
    }
}

/// The aggregated telemetry state. Cumulative since server start; the metering
/// exporter takes deltas against its own last snapshot, so this never needs
/// resetting.
pub struct Rollup {
    /// `(table, tier code, op code)` → raw counts.
    counters: HashMap<(u32, u8, u8), CounterSet>,
    /// Per-table latency EWMAs for the latency-saved estimate.
    latency: HashMap<u32, LatencyEwma>,
    /// Distinct real table ids seen (excludes [`UNMAPPED`]/[`OVERFLOW`]), for
    /// the cardinality cap.
    real_tables: HashSet<u32>,
    /// Absolute count of events dropped on the tapes (buffers full at record
    /// time), mirrored from the shards on each roll.
    dropped_events: u64,
    /// Whether the cap has ever tripped (an id folded into [`OVERFLOW`]).
    overflowed: bool,
    /// The distinct-real-table cap.
    cap: usize,
}

impl Rollup {
    /// A fresh rollup with the default cardinality cap.
    pub fn new() -> Rollup {
        Rollup::with_cap(DEFAULT_TABLE_CAP)
    }

    /// A rollup with an explicit cap (tests drive the cap with a small value).
    pub fn with_cap(cap: usize) -> Rollup {
        Rollup {
            counters: HashMap::new(),
            latency: HashMap::new(),
            real_tables: HashSet::new(),
            dropped_events: 0,
            overflowed: false,
            cap: cap.max(1),
        }
    }

    /// Folds a batch of drained events into the counters. Off the hot path.
    pub fn fold(&mut self, events: &[AccessEvent]) {
        for e in events {
            let table = self.cap_table(e.table);
            let entry = self.counters.entry((table, e.tier, e.op)).or_default();
            entry.count += 1;
            entry.bytes += e.bytes as u64;
            if let Some(tier) = Tier::from_code(e.tier) {
                self.latency
                    .entry(table)
                    .or_default()
                    .observe(tier, e.latency_us);
            }
        }
    }

    /// Maps a raw table id through the cardinality cap: sentinels pass through;
    /// a new real id past the cap folds into [`OVERFLOW`].
    fn cap_table(&mut self, table: u32) -> u32 {
        if table == UNMAPPED || table == OVERFLOW {
            return table;
        }
        if self.real_tables.contains(&table) {
            return table;
        }
        if self.real_tables.len() >= self.cap {
            self.overflowed = true;
            return OVERFLOW;
        }
        self.real_tables.insert(table);
        table
    }

    /// Mirrors the shards' absolute dropped-event total.
    pub fn set_dropped(&mut self, dropped: u64) {
        self.dropped_events = dropped;
    }

    /// Total events dropped at record time (tape buffers full).
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events
    }

    /// Whether the cardinality cap has tripped.
    pub fn overflowed(&self) -> bool {
        self.overflowed
    }

    /// Number of distinct series in the counter map — the cardinality actually
    /// carried. Bounded by `(cap + 2) * tiers * ops`.
    pub fn series_len(&self) -> usize {
        self.counters.len()
    }

    /// Folds the per-`(tier, op)` cells into one cumulative row per table.
    fn per_table(&self) -> HashMap<u32, TableAccum> {
        let mut out: HashMap<u32, TableAccum> = HashMap::new();
        for (&(table, tier_code, _op), set) in &self.counters {
            let row = out.entry(table).or_default();
            row.bytes_served += set.bytes;
            match Tier::from_code(tier_code) {
                Some(t) if t.is_hit() => {
                    row.hits += set.count;
                    row.cache_bytes += set.bytes;
                }
                Some(Tier::Backend) => row.misses += set.count,
                _ => {}
            }
        }
        out
    }

    /// The per-table report the admin surface and CLI consume, with ids
    /// resolved to names by `resolve` (`0` → `_unmapped`, [`OVERFLOW`] →
    /// `_overflow`).
    pub fn report<F>(&self, resolve: F) -> TablesReport
    where
        F: Fn(u32) -> Option<String>,
    {
        let per = self.per_table();
        let mut tables: Vec<TableMetricRow> = per
            .iter()
            .map(|(&id, acc)| {
                let requests_avoided = acc.hits;
                let latency_saved_seconds = self
                    .latency
                    .get(&id)
                    .and_then(|e| e.saved_per_hit_secs())
                    .map(|s| s * acc.hits as f64)
                    .unwrap_or(0.0);
                TableMetricRow {
                    table: resolve_name(id, &resolve),
                    hits: acc.hits,
                    misses: acc.misses,
                    bytes_served: acc.bytes_served,
                    cache_bytes: acc.cache_bytes,
                    requests_avoided,
                    latency_saved_seconds,
                }
            })
            .collect();
        // Stable order: highest requests-avoided first, then name, so the CLI
        // and any golden test are deterministic.
        tables.sort_by(|a, b| {
            b.requests_avoided
                .cmp(&a.requests_avoided)
                .then_with(|| a.table.cmp(&b.table))
        });
        let totals = tables.iter().fold(FleetTotals::default(), |mut t, row| {
            t.hits += row.hits;
            t.misses += row.misses;
            t.bytes_served += row.bytes_served;
            t.requests_avoided += row.requests_avoided;
            t
        });
        TablesReport {
            tables,
            totals,
            dropped_events: self.dropped_events,
            overflowed: self.overflowed,
        }
    }

    /// The cumulative per-table figures the metering exporter diffs into window
    /// deltas. Ids resolved to names the same way as [`Self::report`].
    pub fn metering_snapshot<F>(&self, resolve: F) -> Vec<TableCumulative>
    where
        F: Fn(u32) -> Option<String>,
    {
        let mut out: Vec<TableCumulative> = self
            .per_table()
            .iter()
            .map(|(&id, acc)| TableCumulative {
                table: resolve_name(id, &resolve),
                hits: acc.hits,
                misses: acc.misses,
                bytes_served: acc.bytes_served,
                requests_avoided: acc.hits,
            })
            .collect();
        out.sort_by(|a, b| a.table.cmp(&b.table));
        out
    }
}

impl Default for Rollup {
    fn default() -> Self {
        Rollup::new()
    }
}

/// One table's folded cumulative counts.
#[derive(Debug, Clone, Copy, Default)]
struct TableAccum {
    hits: u64,
    misses: u64,
    bytes_served: u64,
    cache_bytes: u64,
}

fn resolve_name<F: Fn(u32) -> Option<String>>(id: u32, resolve: &F) -> String {
    match id {
        UNMAPPED => "_unmapped".to_owned(),
        OVERFLOW => "_overflow".to_owned(),
        other => resolve(other).unwrap_or_else(|| format!("table_{other}")),
    }
}

/// One row of the per-table report: everything `verglas tables` prints except
/// the dollar estimate, which is multiplied in at display time.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableMetricRow {
    /// Resolved table name (`_unmapped` / `_overflow` for the sentinels).
    pub table: String,
    /// Reads served from a cache tier (dram/nvme/peer).
    pub hits: u64,
    /// Reads that had to fill from the backend.
    pub misses: u64,
    /// Total bytes served for this table across all tiers.
    pub bytes_served: u64,
    /// Bytes served specifically from cache tiers (the "cached bytes" column).
    pub cache_bytes: u64,
    /// Would-be backend GETs avoided — every cache hit was one. Raw count; the
    /// dollar value is computed at display time.
    pub requests_avoided: u64,
    /// Estimated seconds of client latency saved (an approximation from the
    /// per-table latency EWMAs — label it as such wherever shown).
    pub latency_saved_seconds: f64,
}

impl TableMetricRow {
    /// Hit rate over cache-eligible reads (hits / (hits + misses)); `0.0` before
    /// any such read.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total > 0 {
            self.hits as f64 / total as f64
        } else {
            0.0
        }
    }
}

/// Fleet totals across all tables in a report.
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FleetTotals {
    /// Total cache hits.
    pub hits: u64,
    /// Total backend-fill misses.
    pub misses: u64,
    /// Total bytes served.
    pub bytes_served: u64,
    /// Total requests avoided.
    pub requests_avoided: u64,
}

/// The per-table report served by the admin API and rendered by `verglas
/// tables`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TablesReport {
    /// One row per table with recorded traffic, most-avoided first.
    pub tables: Vec<TableMetricRow>,
    /// Fleet totals across those rows.
    pub totals: FleetTotals,
    /// Events dropped at record time because a tape buffer was full (sampling
    /// under load).
    pub dropped_events: u64,
    /// Whether the cardinality cap has folded any table into `_overflow`.
    pub overflowed: bool,
}

/// Cumulative per-table figures for the metering exporter to diff.
#[derive(Debug, Clone, PartialEq)]
pub struct TableCumulative {
    /// Resolved table name.
    pub table: String,
    /// Cumulative cache hits.
    pub hits: u64,
    /// Cumulative backend-fill misses.
    pub misses: u64,
    /// Cumulative bytes served.
    pub bytes_served: u64,
    /// Cumulative requests avoided.
    pub requests_avoided: u64,
}

#[cfg(test)]
mod tests {
    use super::super::Op;
    use super::*;

    fn event(table: u32, tier: Tier, bytes: u32, latency_us: u32) -> AccessEvent {
        AccessEvent {
            table,
            tier: tier.code(),
            op: Op::Get.code(),
            _pad: 0,
            bytes,
            latency_us,
        }
    }

    fn no_names(_id: u32) -> Option<String> {
        None
    }

    #[test]
    fn folds_hits_and_misses_per_table() {
        let mut r = Rollup::new();
        r.fold(&[
            event(1, Tier::Dram, 100, 50),
            event(1, Tier::Nvme, 200, 80),
            event(1, Tier::Backend, 400, 5000),
            event(2, Tier::Peer, 50, 60),
        ]);
        let report = r.report(no_names);
        let t1 = report
            .tables
            .iter()
            .find(|t| t.table == "table_1")
            .expect("table 1 present");
        assert_eq!(t1.hits, 2);
        assert_eq!(t1.misses, 1);
        assert_eq!(t1.bytes_served, 700);
        assert_eq!(t1.cache_bytes, 300);
        assert_eq!(t1.requests_avoided, 2);
        assert!((t1.hit_rate() - 2.0 / 3.0).abs() < 1e-9);
        // Fleet totals reconcile.
        assert_eq!(report.totals.hits, 3);
        assert_eq!(report.totals.requests_avoided, 3);
    }

    #[test]
    fn unmapped_access_folds_into_one_series() {
        let mut r = Rollup::new();
        // 10k distinct unmapped *prefixes* would classify to table 0 — they must
        // all land on the single `_unmapped` row, never explode.
        for i in 0..10_000u32 {
            r.fold(&[event(UNMAPPED, Tier::Dram, i, 40)]);
        }
        let report = r.report(no_names);
        let unmapped: Vec<_> = report
            .tables
            .iter()
            .filter(|t| t.table == "_unmapped")
            .collect();
        assert_eq!(unmapped.len(), 1, "exactly one _unmapped series");
        assert_eq!(unmapped[0].hits, 10_000);
    }

    #[test]
    fn cardinality_cap_folds_excess_tables_into_overflow() {
        let mut r = Rollup::with_cap(1000);
        // 100k distinct real tables against a 1000 cap: the first 1000 keep
        // their identity, the rest fold into `_overflow`.
        for id in 1..=100_000u32 {
            r.fold(&[event(id, Tier::Dram, 1, 30)]);
        }
        assert!(r.overflowed());
        let report = r.report(no_names);
        let overflow: Vec<_> = report
            .tables
            .iter()
            .filter(|t| t.table == "_overflow")
            .collect();
        assert_eq!(overflow.len(), 1, "exactly one _overflow series");
        // 100k - 1000 folded in.
        assert_eq!(overflow[0].hits, 99_000);
        // Real tables tracked never exceed the cap.
        let real = report
            .tables
            .iter()
            .filter(|t| t.table != "_overflow")
            .count();
        assert_eq!(real, 1000);
    }

    #[test]
    fn latency_saved_uses_the_per_table_ewmas() {
        let mut r = Rollup::new();
        // One backend fill at 1_000_000us (1s) and two hits at 0us: saved ≈
        // 1s per hit × 2 hits = 2s.
        r.fold(&[
            event(7, Tier::Backend, 100, 1_000_000),
            event(7, Tier::Dram, 100, 0),
            event(7, Tier::Dram, 100, 0),
        ]);
        let report = r.report(no_names);
        let t = report
            .tables
            .iter()
            .find(|t| t.table == "table_7")
            .expect("table_7 present");
        assert!(
            (t.latency_saved_seconds - 2.0).abs() < 1e-6,
            "got {}",
            t.latency_saved_seconds
        );
    }

    #[test]
    fn name_resolution_maps_ids_to_dotted_names() {
        let mut r = Rollup::new();
        r.fold(&[event(3, Tier::Dram, 10, 20)]);
        let report = r.report(|id| (id == 3).then(|| "db.orders".to_owned()));
        assert_eq!(report.tables[0].table, "db.orders");
    }

    #[test]
    fn metering_snapshot_is_cumulative_per_table() {
        let mut r = Rollup::new();
        r.fold(&[
            event(1, Tier::Dram, 100, 10),
            event(1, Tier::Backend, 200, 900),
        ]);
        let snap = r.metering_snapshot(no_names);
        let t1 = snap
            .iter()
            .find(|t| t.table == "table_1")
            .expect("table_1 present");
        assert_eq!(t1.hits, 1);
        assert_eq!(t1.misses, 1);
        assert_eq!(t1.bytes_served, 300);
        assert_eq!(t1.requests_avoided, 1);
    }
}
