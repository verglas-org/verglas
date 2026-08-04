//! `verglas table metrics` — per-table cache metrics.
//!
//! Reads the local server's per-table telemetry report and prints one line per
//! watched table: hit rate, cached bytes, backend requests avoided, and a
//! dollar-savings ESTIMATE. The server is the only source; with no server
//! listening the verb fails with a clear error naming the endpoint.
//!
//! # Dollars are presentation-only
//!
//! The server stores raw counts, never money — baking a price into a counter
//! freezes wrong history when prices change. This command multiplies avoided
//! requests by a fixed published rate at display time, so the estimate always
//! reflects the current rate and is always labelled an estimate.

use std::error::Error;

use verglas_core::admin::TABLE_METRICS_PATH;
use verglas_core::telemetry::TablesReport;

/// Published Amazon S3 GET request list price: $0.0004 per 1,000 GET requests
/// (S3 Standard, us-east-1, as published on the AWS S3 pricing page). A constant,
/// not a config knob: it is a display multiplier for an estimate, not a tuning
/// parameter. Update it here if AWS republishes the rate.
const S3_GET_PRICE_PER_1K: f64 = 0.0004;

/// Dispatches `verglas table metrics`: the local per-table cache metrics view.
pub async fn run(server_endpoint: &str, json: bool) -> Result<(), Box<dyn Error>> {
    let client = crate::backend::server(server_endpoint)?;
    let report: TablesReport = client.get(TABLE_METRICS_PATH).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&json_view(&report))?);
    } else {
        render(&report);
    }
    Ok(())
}

/// The dollar-savings estimate for `requests_avoided` at the published rate.
/// Pure display arithmetic — the property that makes pricing presentation-layer.
fn dollar_estimate(requests_avoided: u64, price_per_1k: f64) -> f64 {
    (requests_avoided as f64 / 1000.0) * price_per_1k
}

/// The JSON shape: the raw report plus a transparent pricing block and per-row
/// estimates, so a consumer sees both the raw counts and how the dollars were
/// derived (and could re-price with its own rate).
fn json_view(report: &TablesReport) -> serde_json::Value {
    let tables: Vec<serde_json::Value> = report
        .tables
        .iter()
        .map(|t| {
            serde_json::json!({
                "table": t.table,
                "hits": t.hits,
                "misses": t.misses,
                "hit_rate": t.hit_rate(),
                "cache_bytes": t.cache_bytes,
                "bytes_served": t.bytes_served,
                "requests_avoided": t.requests_avoided,
                "latency_saved_seconds": t.latency_saved_seconds,
                "savings_usd_estimate": dollar_estimate(t.requests_avoided, S3_GET_PRICE_PER_1K),
            })
        })
        .collect();
    serde_json::json!({
        "tables": tables,
        "totals": {
            "hits": report.totals.hits,
            "misses": report.totals.misses,
            "bytes_served": report.totals.bytes_served,
            "requests_avoided": report.totals.requests_avoided,
            "savings_usd_estimate": dollar_estimate(report.totals.requests_avoided, S3_GET_PRICE_PER_1K),
        },
        "pricing": {
            "price_per_1k_get_requests_usd": S3_GET_PRICE_PER_1K,
            "note": "estimate at published S3 GET list pricing",
        },
        "dropped_events": report.dropped_events,
        "overflowed": report.overflowed,
    })
}

/// Renders the human table.
fn render(report: &TablesReport) {
    if report.tables.is_empty() {
        println!("(no per-table traffic recorded yet)");
        return;
    }
    println!(
        "{:<28} {:>9} {:>12} {:>14} {:>16}",
        "TABLE", "HIT RATE", "CACHED", "AVOIDED", "SAVINGS (est)"
    );
    for t in &report.tables {
        println!(
            "{:<28} {:>8.1}% {:>12} {:>14} {:>16}",
            truncate(&t.table, 28),
            t.hit_rate() * 100.0,
            human_bytes(t.cache_bytes),
            t.requests_avoided,
            format!(
                "${:.2}",
                dollar_estimate(t.requests_avoided, S3_GET_PRICE_PER_1K)
            ),
        );
    }
    println!(
        "{:<28} {:>9} {:>12} {:>14} {:>16}",
        "TOTAL",
        "",
        human_bytes(report.totals.bytes_served),
        report.totals.requests_avoided,
        format!(
            "${:.2}",
            dollar_estimate(report.totals.requests_avoided, S3_GET_PRICE_PER_1K)
        ),
    );
    println!(
        "\nSavings are an ESTIMATE at published S3 GET list pricing (${:.4}/1k GET requests).",
        S3_GET_PRICE_PER_1K
    );
    if report.overflowed {
        println!("Note: more tables than the metrics cap — extra tables are grouped as _overflow.");
    }
    if report.dropped_events > 0 {
        println!(
            "Note: {} access events were sampled out under load (counts are a lower bound).",
            report.dropped_events
        );
    }
}

/// Human-readable byte size (binary units).
fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes == 0 {
        return "0 B".to_owned();
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Truncates a long table name for the fixed-width column.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_owned()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_is_avoided_over_a_thousand_times_the_rate() {
        // 41,000,000 avoided GETs at $0.0004/1k = $16.40 — the renewal-slide
        // number. Computed at display time, so a rate change re-prices it.
        let dollars = dollar_estimate(41_000_000, 0.0004);
        assert!((dollars - 16.40).abs() < 1e-9, "got {dollars}");
    }

    #[test]
    fn estimate_reprices_when_the_rate_changes() {
        // The presentation-layer property: same raw count, different displayed
        // dollars — nothing is frozen into a stored counter.
        let avoided = 10_000_000;
        assert!((dollar_estimate(avoided, 0.0004) - 4.0).abs() < 1e-9);
        assert!((dollar_estimate(avoided, 0.0005) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn human_bytes_scales_units() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(1024 * 1024 * 3), "3.0 MiB");
    }
}
