//! `verglas status` — report where this CLI points and whether it answers.
//!
//! On Verglas Cloud (the default) the CLI is a pure client of a hosted
//! tenant: status resolves the `[connection]` profile written by `verglas
//! login` and probes the tenant's own catalog and S3 endpoints. The admin
//! API (`/admin/version`, `/admin/healthz`, `/admin/stats`) exists only on
//! self-hosted servers, selected with `VERGLAS_ENDPOINT`.

use std::error::Error;
use std::io::Write as _;

use verglas_core::admin::{HealthzInfo, StatsInfo};

use crate::connection_profile::{self, ConnectionProfileError};

/// Reports connection status: the hosted tenant for Cloud, or the local
/// server's admin API for a self-hosted `VERGLAS_ENDPOINT`.
pub async fn run(endpoint: &str, token: Option<&str>, json: bool) -> Result<(), Box<dyn Error>> {
    if crate::backend::is_verglas_cloud(endpoint) {
        return run_cloud(endpoint, json).await;
    }
    run_local(endpoint, token, json).await
}

/// Cloud status: resolve the login profile and probe the tenant's endpoints.
async fn run_cloud(control_plane: &str, json: bool) -> Result<(), Box<dyn Error>> {
    let resolved =
        match connection_profile::resolve_from_environment(&connection_profile::environment()) {
            Ok(resolved) => resolved,
            Err(
                ConnectionProfileError::MissingProfile(_) | ConnectionProfileError::Incomplete(_),
            ) => {
                if json {
                    let value = serde_json::json!({
                        "signed_in": false,
                        "control_plane": control_plane,
                    });
                    writeln!(
                        std::io::stdout(),
                        "{}",
                        serde_json::to_string_pretty(&value)?
                    )?;
                } else {
                    print!(
                        "\n\
                     \x20 Verglas Cloud\n\
                     \x20 signed in:  no — run `verglas login`\n\
                     \x20 control plane: {control_plane}\n\n"
                    );
                }
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
    let control_plane =
        connection_profile::control_plane_url().unwrap_or_else(|| control_plane.to_owned());
    let catalog_uri = resolved.catalog_uri.clone().unwrap_or_default();
    let catalog_state = match health_url(&catalog_uri) {
        Some(url) => probe(&url, |status| status == 200).await,
        None => String::from("unknown"),
    };
    // Any HTTP answer from the S3 listener means it is up: an unsigned GET
    // legitimately yields 403.
    let s3_state = probe(&resolved.semantic_uri, |status| status < 500).await;

    if json {
        let value = serde_json::json!({
            "signed_in": true,
            "control_plane": control_plane,
            "catalog": { "url": catalog_uri, "state": catalog_state },
            "s3": { "url": resolved.semantic_uri, "state": s3_state },
            "query": resolved.query_uri,
            "region": resolved.region,
        });
        writeln!(
            std::io::stdout(),
            "{}",
            serde_json::to_string_pretty(&value)?
        )?;
        return Ok(());
    }
    print!(
        "\n\
         \x20 Verglas Cloud\n\
         \x20 signed in:  yes\n\
         \x20 control plane: {control_plane}\n\
         \x20 catalog:    {catalog_uri}  ({catalog_state})\n\
         \x20 s3:         {}  ({s3_state})\n\
         \x20 query:      {}\n\n",
        resolved.semantic_uri, resolved.query_uri,
    );
    Ok(())
}

/// The unauthenticated health probe for a catalog URL: the `/health` route at
/// the catalog's origin (the catalog API itself lives under `/catalog`).
fn health_url(catalog_uri: &str) -> Option<String> {
    let url = reqwest::Url::parse(catalog_uri).ok()?;
    let origin = url.origin().ascii_serialization();
    Some(format!("{origin}/health"))
}

/// Probes `url` and renders reachability: `ok(200)`/`up` per `healthy`, an
/// HTTP status otherwise, or `unreachable` on a connection failure.
async fn probe(url: &str, healthy: fn(u16) -> bool) -> String {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build();
    let Ok(client) = client else {
        return String::from("unreachable");
    };
    match client.get(url).send().await {
        Ok(response) if healthy(response.status().as_u16()) => String::from("healthy"),
        Ok(response) => format!("unhealthy (HTTP {})", response.status().as_u16()),
        Err(_) => String::from("unreachable"),
    }
}

/// Self-hosted status: probe the server's admin HTTP API directly.
async fn run_local(endpoint: &str, token: Option<&str>, json: bool) -> Result<(), Box<dyn Error>> {
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
