//! Node-level Prometheus metrics (#46): the live request instruments and the
//! text-exposition renderer for the server's `/metrics` endpoint.
//!
//! Two halves meet here. [`NodeMetrics`] owns a `prometheus::Registry` and the
//! metric families that must be emitted *live* from the serving path — the
//! request-duration histogram and the request counter — because they cannot be
//! reconstructed from a snapshot after the fact. Everything else Verglas already
//! counts (cache hits/misses, bytes-served-by-tier, admission, fill inflight,
//! backend health) is *read at scrape time* from the running engine and backend
//! and appended as plain exposition text by [`render`], keyed off a
//! [`MetricsSnapshot`] the server fills. Keeping the snapshot half text-only
//! means this crate needs no handle on the cache or backend types.
//!
//! The metric names are a stability contract: dashboards and (later) the billing
//! meter depend on them. This module is the reference; changes require review.
//! Node process/NIC/disk stats are deliberately absent — those come from
//! node-exporter, not this in-process registry.

use std::fmt::Write;

use prometheus::{CounterVec, HistogramVec, Registry, TextEncoder, proto::MetricFamily};

use crate::read::ServedTier;

/// The `Content-Type` a Prometheus text-exposition response must carry.
pub const EXPOSITION_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

/// The four request operations the request metrics label by. A read/write on the
/// S3 surface maps to exactly one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOp {
    /// GetObject (ranged or full).
    Get,
    /// HeadObject.
    Head,
    /// ListObjects / ListObjectsV2.
    List,
    /// PutObject (and the write surface generally).
    Put,
}

impl RequestOp {
    /// The lowercase `op` label. Stable — dashboards depend on these strings.
    pub fn label(self) -> &'static str {
        match self {
            RequestOp::Get => "get",
            RequestOp::Head => "head",
            RequestOp::List => "list",
            RequestOp::Put => "put",
        }
    }
}

/// The tenant label default until per-tenant auth is wired (#33). Node-level
/// metrics carry a single tenant dimension; every request is attributed here
/// until the auth layer supplies a real tenant.
pub const DEFAULT_TENANT: &str = "default";

/// Histogram buckets for `verglas_request_duration_seconds`, in seconds. Spans
/// a warm DRAM hit (tens of microseconds) through a slow backend fill (seconds),
/// which is the range a tiered cache's latency actually covers.
const DURATION_BUCKETS: &[f64] = &[
    0.000_1, 0.000_5, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// The live request instruments plus their registry. Cheap to share behind an
/// `Arc`: the S3 front-end clones one handle and records into it per request.
pub struct NodeMetrics {
    /// The registry the live families are registered in; [`render`] encodes it.
    registry: Registry,
    /// `verglas_request_duration_seconds{op,tier}` — end-to-end request latency,
    /// recorded once per request after the body drains.
    request_duration: HistogramVec,
    /// `verglas_requests_total{op,status,tenant}` — request count by outcome.
    requests_total: CounterVec,
}

impl NodeMetrics {
    /// Builds the live instruments and registers them. Fails only if a metric
    /// name collides in the registry, which is a programming error (fixed
    /// names), so the server treats a failure as fatal at startup.
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let request_duration = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "verglas_request_duration_seconds",
                "End-to-end request duration by operation and serving tier.",
            )
            .buckets(DURATION_BUCKETS.to_vec()),
            &["op", "tier"],
        )?;
        let requests_total = CounterVec::new(
            prometheus::Opts::new(
                "verglas_requests_total",
                "Total requests by operation, response status, and tenant.",
            ),
            &["op", "status", "tenant"],
        )?;
        registry.register(Box::new(request_duration.clone()))?;
        registry.register(Box::new(requests_total.clone()))?;
        Ok(NodeMetrics {
            registry,
            request_duration,
            requests_total,
        })
    }

    /// Records one completed request: its duration (seconds) under the op/tier
    /// histogram, and one increment of the op/status/tenant counter. Called once
    /// per request from the S3 front-end, off any cache lock — the histogram and
    /// counter are atomic internally.
    pub fn observe_request(
        &self,
        op: RequestOp,
        tier: ServedTier,
        status: u16,
        tenant: &str,
        seconds: f64,
    ) {
        let status = status.to_string();
        self.request_duration
            .with_label_values(&[op.label(), tier.label()])
            .observe(seconds);
        self.requests_total
            .with_label_values(&[op.label(), &status, tenant])
            .inc();
    }

    /// The live registry's metric families, gathered for [`render`].
    fn gather(&self) -> Vec<MetricFamily> {
        self.registry.gather()
    }
}

impl Default for NodeMetrics {
    /// Panics only on the fixed-name collision above — see [`NodeMetrics::new`].
    /// Prefer `new` where the error can be surfaced.
    fn default() -> Self {
        NodeMetrics::new().expect("node metric names are fixed and unique")
    }
}

/// One tier's cache occupancy and ceiling, in bytes (#46 gauges). `used` is the
/// live resident bytes; `capacity` is the configured hard ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierSize {
    /// Bytes currently resident in the tier.
    pub used: u64,
    /// The tier's configured capacity ceiling in bytes.
    pub capacity: u64,
}

/// The scrape-time snapshot the server fills from the running engine and backend
/// (#46). Everything here is a value Verglas already counts; [`render`] turns it
/// into the counter/gauge exposition families that sit alongside the live
/// request families. Plain numbers so this crate needs no cache/backend types.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Bytes served to clients, per tier: `(tier, bytes)`. Rendered as
    /// `verglas_bytes_served_total{tier}`.
    pub bytes_served: Vec<(ServedTier, u64)>,
    /// Block lookups that hit the DRAM or persistent Foyer tier.
    pub cache_hits: u64,
    /// Block lookups that missed every cache tier and had to fill.
    pub cache_misses: u64,
    /// Blocks the scan-resistant admission policy (#15) admitted.
    pub cache_admitted: u64,
    /// Blocks the admission policy rejected.
    pub cache_rejected: u64,
    /// Cache occupancy and ceiling per tier: `(tier, sizes)`. Rendered as
    /// `verglas_cache_size_bytes{tier}` and `verglas_cache_capacity_bytes{tier}`.
    pub cache_tiers: Vec<(ServedTier, TierSize)>,
    /// Singleflight fills in flight right now (#19) — the gauge.
    pub fill_inflight: u64,
    /// Backend requests by outcome: `(status, count)` where status is a stable
    /// string (`success`/`shed`). Rendered as `verglas_backend_requests_total`.
    pub backend_requests: Vec<(&'static str, u64)>,
    /// Total backend retry attempts issued (#20).
    pub backend_retries: u64,
    /// Times the circuit breaker (#20) opened.
    pub breaker_trips: u64,
    /// Requests the open/half-open breaker shed.
    pub breaker_rejections: u64,
    /// The breaker's current coarse state as a stable string
    /// (`closed`/`open`/`half_open`).
    pub breaker_state: &'static str,
}

/// A single exposition metric line: `name{labels} value`. Writes the label set
/// only when non-empty. Values are integers here (counters/gauges over `u64`),
/// so no float formatting quirks arise.
fn line(out: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    let _ = write!(out, "{name}");
    if !labels.is_empty() {
        let _ = out.write_char('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                let _ = out.write_char(',');
            }
            let _ = write!(out, "{k}=\"{v}\"");
        }
        let _ = out.write_char('}');
    }
    let _ = writeln!(out, " {value}");
}

/// Writes a `# HELP`/`# TYPE` header for one metric family.
fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// Renders the full node-level exposition: the live request families encoded
/// from `metrics`' registry, followed by the snapshot-derived counter and gauge
/// families. The result is a complete `text/plain; version=0.0.4` body.
///
/// Ordering is stable (live families first, then snapshot families in a fixed
/// order) so a scrape is deterministic and tests can assert on it.
pub fn render(metrics: &NodeMetrics, snapshot: &MetricsSnapshot) -> String {
    let mut out = String::new();

    // The live request families, encoded by prometheus' own text encoder.
    let encoder = TextEncoder::new();
    if let Ok(text) = encoder.encode_to_string(&metrics.gather()) {
        out.push_str(&text);
    }

    // verglas_bytes_served_total{tier}
    header(
        &mut out,
        "verglas_bytes_served_total",
        "counter",
        "Bytes served to clients by serving tier.",
    );
    for (tier, bytes) in &snapshot.bytes_served {
        line(
            &mut out,
            "verglas_bytes_served_total",
            &[("tier", tier.label())],
            *bytes,
        );
    }

    // Cache hit/miss/admission counters.
    header(
        &mut out,
        "verglas_cache_hits_total",
        "counter",
        "Cache block lookups served from a cache tier.",
    );
    line(
        &mut out,
        "verglas_cache_hits_total",
        &[],
        snapshot.cache_hits,
    );
    header(
        &mut out,
        "verglas_cache_misses_total",
        "counter",
        "Cache block lookups that missed every tier.",
    );
    line(
        &mut out,
        "verglas_cache_misses_total",
        &[],
        snapshot.cache_misses,
    );
    header(
        &mut out,
        "verglas_cache_admitted_total",
        "counter",
        "Blocks admitted by the scan-resistant admission policy (#15).",
    );
    line(
        &mut out,
        "verglas_cache_admitted_total",
        &[],
        snapshot.cache_admitted,
    );
    header(
        &mut out,
        "verglas_cache_rejected_total",
        "counter",
        "Blocks rejected by the admission policy (#15).",
    );
    line(
        &mut out,
        "verglas_cache_rejected_total",
        &[],
        snapshot.cache_rejected,
    );

    // Per-tier cache size and capacity gauges.
    header(
        &mut out,
        "verglas_cache_size_bytes",
        "gauge",
        "Bytes currently resident in each cache tier.",
    );
    for (tier, size) in &snapshot.cache_tiers {
        line(
            &mut out,
            "verglas_cache_size_bytes",
            &[("tier", tier.label())],
            size.used,
        );
    }
    header(
        &mut out,
        "verglas_cache_capacity_bytes",
        "gauge",
        "Configured capacity ceiling of each cache tier.",
    );
    for (tier, size) in &snapshot.cache_tiers {
        line(
            &mut out,
            "verglas_cache_capacity_bytes",
            &[("tier", tier.label())],
            size.capacity,
        );
    }

    // Fill manager gauge and backend health.
    header(
        &mut out,
        "verglas_fill_inflight",
        "gauge",
        "Singleflight backend fills currently in flight (#19).",
    );
    line(
        &mut out,
        "verglas_fill_inflight",
        &[],
        snapshot.fill_inflight,
    );

    header(
        &mut out,
        "verglas_backend_requests_total",
        "counter",
        "Backend fill requests by outcome status.",
    );
    for (status, count) in &snapshot.backend_requests {
        line(
            &mut out,
            "verglas_backend_requests_total",
            &[("status", status)],
            *count,
        );
    }
    header(
        &mut out,
        "verglas_backend_retries_total",
        "counter",
        "Backend retry attempts issued (#20).",
    );
    line(
        &mut out,
        "verglas_backend_retries_total",
        &[],
        snapshot.backend_retries,
    );
    header(
        &mut out,
        "verglas_circuit_breaker_trips_total",
        "counter",
        "Times the backend circuit breaker opened (#20).",
    );
    line(
        &mut out,
        "verglas_circuit_breaker_trips_total",
        &[],
        snapshot.breaker_trips,
    );
    header(
        &mut out,
        "verglas_circuit_breaker_rejections_total",
        "counter",
        "Requests the open/half-open circuit breaker shed (#20).",
    );
    line(
        &mut out,
        "verglas_circuit_breaker_rejections_total",
        &[],
        snapshot.breaker_rejections,
    );
    header(
        &mut out,
        "verglas_circuit_breaker_state",
        "gauge",
        "Circuit breaker state (1 for the active state, 0 otherwise).",
    );
    for state in ["closed", "open", "half_open"] {
        line(
            &mut out,
            "verglas_circuit_breaker_state",
            &[("state", state)],
            (state == snapshot.breaker_state) as u64,
        );
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A snapshot with distinct, recognisable numbers so a render assertion can
    /// tell each family apart.
    fn sample_snapshot() -> MetricsSnapshot {
        MetricsSnapshot {
            bytes_served: vec![
                (ServedTier::Dram, 1000),
                (ServedTier::Nvme, 200),
                (ServedTier::Backend, 30),
            ],
            cache_hits: 42,
            cache_misses: 7,
            cache_admitted: 5,
            cache_rejected: 2,
            cache_tiers: vec![
                (
                    ServedTier::Dram,
                    TierSize {
                        used: 512,
                        capacity: 4096,
                    },
                ),
                (
                    ServedTier::Nvme,
                    TierSize {
                        used: 8192,
                        capacity: 65536,
                    },
                ),
            ],
            fill_inflight: 3,
            backend_requests: vec![("success", 100), ("shed", 4)],
            backend_retries: 9,
            breaker_trips: 1,
            breaker_rejections: 4,
            breaker_state: "closed",
        }
    }

    /// The renderer emits every required metric family with the exact contract
    /// names and labels, and the live histogram carries the op/tier dimensions.
    #[test]
    fn render_emits_the_contract_families() {
        let metrics = NodeMetrics::new().expect("build metrics");
        metrics.observe_request(RequestOp::Get, ServedTier::Dram, 200, DEFAULT_TENANT, 0.002);
        let text = render(&metrics, &sample_snapshot());

        // Live families with labels.
        assert!(text.contains("verglas_request_duration_seconds_bucket"));
        assert!(text.contains("op=\"get\""));
        assert!(text.contains("tier=\"dram\""));
        assert!(text.contains("verglas_requests_total"));
        assert!(text.contains("status=\"200\""));
        assert!(text.contains("tenant=\"default\""));

        // Snapshot families.
        assert!(text.contains("verglas_bytes_served_total{tier=\"dram\"} 1000"));
        assert!(text.contains("verglas_bytes_served_total{tier=\"nvme\"} 200"));
        assert!(text.contains("verglas_cache_hits_total 42"));
        assert!(text.contains("verglas_cache_misses_total 7"));
        assert!(text.contains("verglas_cache_admitted_total 5"));
        assert!(text.contains("verglas_cache_rejected_total 2"));
        assert!(text.contains("verglas_cache_size_bytes{tier=\"dram\"} 512"));
        assert!(text.contains("verglas_cache_capacity_bytes{tier=\"nvme\"} 65536"));
        assert!(text.contains("verglas_fill_inflight 3"));
        assert!(text.contains("verglas_backend_requests_total{status=\"success\"} 100"));
        assert!(text.contains("verglas_backend_requests_total{status=\"shed\"} 4"));
        assert!(text.contains("verglas_backend_retries_total 9"));
        assert!(text.contains("verglas_circuit_breaker_trips_total 1"));
        assert!(text.contains("verglas_circuit_breaker_rejections_total 4"));
        assert!(text.contains("verglas_circuit_breaker_state{state=\"closed\"} 1"));
        assert!(text.contains("verglas_circuit_breaker_state{state=\"open\"} 0"));

        // Every counter family carries a TYPE header.
        assert!(text.contains("# TYPE verglas_bytes_served_total counter"));
        assert!(text.contains("# TYPE verglas_cache_size_bytes gauge"));
    }

    /// Counters move as requests are observed: two GETs put a count of 2 under
    /// the matching op/status/tenant series.
    #[test]
    fn observe_request_moves_the_counter() {
        let metrics = NodeMetrics::new().expect("build metrics");
        metrics.observe_request(RequestOp::Get, ServedTier::Dram, 200, DEFAULT_TENANT, 0.001);
        metrics.observe_request(RequestOp::Get, ServedTier::Nvme, 200, DEFAULT_TENANT, 0.003);
        let value = metrics
            .requests_total
            .with_label_values(&["get", "200", "default"])
            .get();
        assert_eq!(value, 2.0);
    }

    /// The breaker-state gauge is one-hot: exactly the reported state reads 1.
    #[test]
    fn breaker_state_is_one_hot() {
        let metrics = NodeMetrics::new().expect("build metrics");
        let mut snap = sample_snapshot();
        snap.breaker_state = "open";
        let text = render(&metrics, &snap);
        assert!(text.contains("verglas_circuit_breaker_state{state=\"open\"} 1"));
        assert!(text.contains("verglas_circuit_breaker_state{state=\"closed\"} 0"));
        assert!(text.contains("verglas_circuit_breaker_state{state=\"half_open\"} 0"));
    }
}
