//! Backend resolution for the pure-client CLI — the one place that decides
//! which server a command targets.
//!
//! Cloud is the default (`https://api.verglas.dev`). Self-hosted OSS servers
//! are selected with `VERGLAS_ENDPOINT`, not a CLI flag.

use std::error::Error;

use verglas_core::admin::ENDPOINT_ENV;
use verglas_sdk::server::{ServerClient, ServerError};

/// Verglas Cloud control-plane URL.
pub const CLOUD_ENDPOINT: &str = "https://api.verglas.dev";

/// Resolves the admin/control-plane URL: `VERGLAS_ENDPOINT` if set, otherwise
/// Verglas Cloud. There is no `--server-endpoint` flag.
pub fn resolved_endpoint() -> String {
    std::env::var(ENDPOINT_ENV)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| CLOUD_ENDPOINT.to_owned())
}

/// Returns whether `endpoint` is Verglas Cloud (HTTPS on `*.verglas.dev`).
pub fn is_verglas_cloud(endpoint: &str) -> bool {
    let trimmed = endpoint.trim().trim_end_matches('/');
    let Some(rest) = trimmed.strip_prefix("https://") else {
        return false;
    };
    let host = rest
        .split('/')
        .next()
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("");
    host == "api.verglas.dev" || host.ends_with(".verglas.dev")
}

/// Dashboards are hosted only on Verglas Cloud. The OSS stack has no dashboard host.
pub fn require_cloud_dashboards(endpoint: &str) -> Result<(), Box<dyn Error>> {
    if is_verglas_cloud(endpoint) {
        return Ok(());
    }
    Err(
        "verglas dashboard runs on Verglas Cloud only; the OSS stack does not host dashboards"
            .into(),
    )
}

/// Resolves a [`ServerClient`] for `endpoint`. The client attempts no connection
/// until its first call and then fails fast with a bounded connect timeout.
pub fn server(endpoint: &str, token: Option<&str>) -> Result<ServerClient, ServerError> {
    ServerClient::new_with_token(endpoint, token)
}

#[cfg(test)]
mod tests {
    use super::{CLOUD_ENDPOINT, is_verglas_cloud, require_cloud_dashboards};

    #[test]
    fn cloud_hosts_are_accepted() {
        assert!(is_verglas_cloud(CLOUD_ENDPOINT));
        assert!(is_verglas_cloud("https://api.verglas.dev/"));
        assert!(is_verglas_cloud("https://preview.verglas.dev"));
        assert!(require_cloud_dashboards(CLOUD_ENDPOINT).is_ok());
    }

    #[test]
    fn oss_loopback_is_rejected() {
        assert!(!is_verglas_cloud("http://127.0.0.1:8334"));
        assert!(!is_verglas_cloud("https://127.0.0.1:8334"));
        assert!(!is_verglas_cloud("http://api.verglas.dev"));
        let dash_err = require_cloud_dashboards("http://127.0.0.1:8334")
            .expect_err("loopback must be rejected for dashboards");
        assert!(dash_err.to_string().contains("dashboard"));
    }
}
