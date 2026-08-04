//! The query role's configuration: an admin listen port, log settings, the
//! cache S3 endpoint this binary reads every table's data through, and the
//! Iceberg REST catalog to query against.
//!
//! Reuses `verglas_core::config::{Log, Catalog}` verbatim for `[log]` and
//! `[catalog]` — the same shapes operators already know from `verglas-server`, so a
//! fleet image renders one TOML dialect across every role. Everything else in
//! that server-shaped schema (cache sizing, origin backend, cluster gossip)
//! does not apply here and is deliberately not reused: this binary has none
//! of those, and a config knob for a feature that does not exist is worse
//! than no knob at all.
//!
//! `[cache]` here is a different (and much smaller) thing than `verglas-server`'s
//! `[cache]` section: it is not a cache tier, it is the single S3 endpoint
//! this binary is allowed to read table data through. There is no direct
//! object-store path anywhere in this binary (the platform rule: cache-or-503
//! applies to every role, not just verglas-server) — `[cache]` is that endpoint and
//! nothing else.

use std::path::Path;

use serde::{Deserialize, Serialize};
use verglas_core::config::Catalog;
pub use verglas_core::config::Log;

/// Root of the query role's configuration. Unknown fields are rejected at
/// parse time so a typo cannot silently become a default.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryConfig {
    /// The admin API port this binary serves `/v1/query`,
    /// `/v1/query/estimate`, and `/healthz` on.
    #[serde(default)]
    pub listen: Listen,
    /// Structured-logging output format and default verbosity — the same
    /// `[log]` shape `verglas-server` uses.
    #[serde(default)]
    pub log: Log,
    /// The cache S3 endpoint every table read goes through, and the keypair
    /// to sign those requests with.
    pub cache: CacheEndpoint,
    /// The Iceberg REST catalog to query against — the same `[catalog]` shape
    /// `verglas-server`'s catalog watcher uses (bearer or SigV4 auth, warehouse id).
    pub catalog: Catalog,
}

/// Ports this binary listens on.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Listen {
    /// Admin API port serving the query surface.
    pub admin_port: u16,
}

impl Default for Listen {
    /// One above `verglas-server`/`verglas-cache-node`'s default admin port (8334),
    /// so all three can run side by side on one dev host without a port
    /// clash.
    fn default() -> Self {
        Listen { admin_port: 8335 }
    }
}

/// The cache S3 endpoint this binary reads table data through, and the SigV4
/// keypair it signs those reads with.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct CacheEndpoint {
    /// Base URL of the cache node's S3 surface (a `verglas-server` or
    /// `verglas-cache-node` instance). Required: without it there is nowhere
    /// to read table data from, and this binary never falls back to a direct
    /// object-store path.
    pub s3_endpoint: String,
    /// Signing region. Unset defaults to `us-east-1`, matching the cache
    /// endpoint's own default.
    pub region: Option<String>,
    /// SigV4 keypair file (AWS-INI format, mode 0600) the cache endpoint
    /// accepts. Unset reads the endpoint anonymously (dev / no-auth cache
    /// nodes only).
    pub credentials_file: Option<String>,
    /// Profile in the credentials file. Unset uses `default`.
    pub credentials_profile: Option<String>,
}

impl QueryConfig {
    /// Reads, parses, and validates a config file — the one call `main` makes.
    pub fn load(path: &Path) -> Result<QueryConfig, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read config file {}: {e}", path.display()))?;
        let config = QueryConfig::from_toml_str(&text)?;
        config.validate()?;
        Ok(config)
    }

    /// Parses a TOML document without validating.
    pub fn from_toml_str(text: &str) -> Result<QueryConfig, String> {
        toml::from_str(text).map_err(|e| format!("invalid config: {e}"))
    }

    /// The one relevant line for a startup log: the endpoints this instance
    /// reads and queries against. Never includes credentials.
    pub fn summary(&self) -> String {
        format!(
            "admin_port={} cache={} catalog={}",
            self.listen.admin_port, self.cache.s3_endpoint, self.catalog.uri
        )
    }

    /// Semantic checks: the cache endpoint and catalog URI are http(s) URLs, and
    /// a named credentials file exists as a readable regular file. Iceberg's own
    /// catalog-open call would surface most of the same problems, but failing
    /// here first turns a typo'd path or scheme into an actionable startup
    /// error instead of an opaque connection failure.
    fn validate(&self) -> Result<(), String> {
        if self.cache.s3_endpoint.is_empty() {
            return Err("cache.s3_endpoint is required".to_owned());
        }
        if !self.cache.s3_endpoint.starts_with("http://")
            && !self.cache.s3_endpoint.starts_with("https://")
        {
            return Err(format!(
                "cache.s3_endpoint `{}` must start with http:// or https://",
                self.cache.s3_endpoint
            ));
        }
        if !self.catalog.uri.starts_with("http://") && !self.catalog.uri.starts_with("https://") {
            return Err(format!(
                "catalog.uri `{}` must start with http:// or https://",
                self.catalog.uri
            ));
        }
        if let Some(file) = &self.cache.credentials_file
            && !Path::new(file).is_file()
        {
            return Err(format!(
                "cache.credentials_file `{file}` does not exist or is not a readable file"
            ));
        }
        if let Some(file) = &self.catalog.credentials_file
            && !Path::new(file).is_file()
        {
            return Err(format!(
                "catalog.credentials_file `{file}` does not exist or is not a readable file"
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The minimal config a fleet image would render: an admin port, a cache
    /// endpoint, and a catalog URI — parses and validates.
    #[test]
    fn loads_a_minimal_config() {
        let toml = r#"
            [listen]
            admin_port = 9000

            [cache]
            s3_endpoint = "http://127.0.0.1:8333"

            [catalog]
            uri = "http://127.0.0.1:8334/catalog"
        "#;
        let config = QueryConfig::from_toml_str(toml).expect("parses");
        config.validate().expect("valid");
        assert_eq!(config.listen.admin_port, 9000);
        assert_eq!(config.cache.s3_endpoint, "http://127.0.0.1:8333");
        assert_eq!(config.catalog.uri, "http://127.0.0.1:8334/catalog");
    }

    /// With no `[listen]` at all, the port defaults to 8335 — one above the
    /// other two roles' default admin port, so all three can run side by side.
    #[test]
    fn defaults_the_admin_port() {
        let toml = r#"
            [cache]
            s3_endpoint = "http://127.0.0.1:8333"

            [catalog]
            uri = "http://127.0.0.1:8334/catalog"
        "#;
        let config = QueryConfig::from_toml_str(toml).expect("parses");
        assert_eq!(config.listen.admin_port, 8335);
    }

    /// A non-http(s) cache endpoint is rejected at validation, naming the
    /// field.
    #[test]
    fn rejects_a_non_http_cache_endpoint() {
        let toml = r#"
            [cache]
            s3_endpoint = "127.0.0.1:8333"

            [catalog]
            uri = "http://127.0.0.1:8334/catalog"
        "#;
        let config = QueryConfig::from_toml_str(toml).expect("parses");
        let err = config.validate().expect_err("rejected");
        assert!(err.contains("cache.s3_endpoint"), "{err}");
    }

    /// A credentials file that does not exist is rejected at validation, not
    /// deferred to a confusing runtime auth failure.
    #[test]
    fn rejects_a_missing_credentials_file() {
        let toml = r#"
            [cache]
            s3_endpoint = "http://127.0.0.1:8333"
            credentials_file = "/nonexistent/creds"

            [catalog]
            uri = "http://127.0.0.1:8334/catalog"
        "#;
        let config = QueryConfig::from_toml_str(toml).expect("parses");
        let err = config.validate().expect_err("rejected");
        assert!(err.contains("cache.credentials_file"), "{err}");
    }

    /// An unknown field anywhere is rejected — no typo silently ignored.
    #[test]
    fn rejects_unknown_fields() {
        let toml = r#"
            [cache]
            s3_endpoint = "http://127.0.0.1:8333"
            bogus = true

            [catalog]
            uri = "http://127.0.0.1:8334/catalog"
        "#;
        assert!(QueryConfig::from_toml_str(toml).is_err());
    }
}
