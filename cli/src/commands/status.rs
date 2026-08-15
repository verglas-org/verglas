//! `verglas status` — probe a running server over its admin HTTP API.
//!
//! There is no OS service manager. The server runs in Docker (or `verglas
//! dev`); this command only asks `/admin/version`, `/admin/healthz`, and
//! `/admin/stats` at the resolved server (`VERGLAS_ENDPOINT` or Verglas Cloud).

use std::error::Error;
use std::io::Write as _;

use verglas_core::admin::{HealthzInfo, StatsInfo};

/// Probes `endpoint` and prints server health, version, and cache warmth.
pub async fn run(endpoint: &str, token: Option<&str>, json: bool) -> Result<(), Box<dyn Error>> {
    let version = fetch_version(endpoint, token).await;
    let health = fetch_health(endpoint, token).await;
    let warmth = fetch_warmth(endpoint, token).await;
    let s3_hint = s3_endpoint_hint(endpoint);

    if json {
        let value = serde_json::json!({
            "version": version,
            "health": health,
            "warmth": warmth,
            "admin_endpoint": endpoint,
            "s3_endpoint": s3_hint,
        });
        writeln!(
            std::io::stdout(),
            "{}",
            serde_json::to_string_pretty(&value)?
        )?;
        return Ok(());
    }

    let version = version.as_deref().unwrap_or("unreachable");
    let health = health.as_deref().unwrap_or("unreachable");
    let warmth = warmth.as_deref().unwrap_or("unavailable");
    print!(
        "\n\
         \x20 Verglas server status\n\
         \x20 server:     {version}\n\
         \x20 health:     {health}\n\
         \x20 cache:      {warmth}\n\
         \x20 admin:      {endpoint}\n\
         \x20 s3:         {s3_hint}\n\n"
    );
    Ok(())
}

/// Best-effort S3 URL: admin port minus one when the URL is `http://host:admin`.
fn s3_endpoint_hint(admin: &str) -> String {
    let Ok(url) = reqwest::Url::parse(admin) else {
        return String::from("(unknown)");
    };
    let host = url.host_str().unwrap_or("127.0.0.1");
    let admin_port = url.port().unwrap_or(8334);
    let s3_port = admin_port.saturating_sub(1);
    format!("{}://{host}:{s3_port}", url.scheme())
}

/// Fetches `/admin/version` as `name version`, or `None` if unreachable.
async fn fetch_version(admin_endpoint: &str, token: Option<&str>) -> Option<String> {
    let client = crate::admin_client::AdminClient::new(admin_endpoint, token).ok()?;
    let info = client.version().await.ok()?;
    Some(format!("{} {}", info.name, info.version))
}

/// Fetches `/admin/healthz` status string, or `None` if unreachable.
async fn fetch_health(admin_endpoint: &str, token: Option<&str>) -> Option<String> {
    let url = format!("{admin_endpoint}{}", verglas_core::admin::HEALTHZ_PATH);
    let mut request = reqwest::Client::new().get(&url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let resp = request.send().await.ok()?;
    let info: HealthzInfo = resp.json().await.ok()?;
    Some(info.status)
}

/// Fetches `/admin/stats` warmth summary, or `None` if unreachable.
async fn fetch_warmth(admin_endpoint: &str, token: Option<&str>) -> Option<String> {
    let url = format!("{admin_endpoint}{}", verglas_core::admin::STATS_PATH);
    let mut request = reqwest::Client::new().get(&url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let resp = request.send().await.ok()?;
    let stats: StatsInfo = resp.json().await.ok()?;
    Some(render_warmth(&stats))
}

/// One-line cache-warmth summary from `/admin/stats`.
fn render_warmth(stats: &StatsInfo) -> String {
    let c = &stats.counters;
    let warm_hits = c.dram_hits + c.disk_hits;
    let total = warm_hits + c.dram_misses.max(c.disk_misses);
    let hit_pct = if total == 0 {
        0.0
    } else {
        (warm_hits as f64 / total as f64) * 100.0
    };
    format!(
        "dram {} live / {} ceiling, {} dram hits, {} disk hits ({hit_pct:.0}% warm)",
        human_bytes(stats.dram_live_bytes),
        human_bytes(stats.cache.dram_bytes),
        c.dram_hits,
        c.disk_hits,
    )
}

/// Formats a byte count as a short human string (`1.5GB`, `20MB`).
fn human_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;
    let (value, unit) = if bytes >= TB {
        (bytes as f64 / TB as f64, "TB")
    } else if bytes >= GB {
        (bytes as f64 / GB as f64, "GB")
    } else if bytes >= MB {
        (bytes as f64 / MB as f64, "MB")
    } else if bytes >= KB {
        (bytes as f64 / KB as f64, "KB")
    } else {
        return format!("{bytes}B");
    };
    let s = format!("{value:.1}");
    let s = s.strip_suffix(".0").unwrap_or(&s);
    format!("{s}{unit}")
}
