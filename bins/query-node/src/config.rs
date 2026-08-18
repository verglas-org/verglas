//! Environment-only configuration for the query node.
//!
//! A tenant query machine receives its coordinates as environment variables,
//! not a mounted config file (production parity with `images/cache/boot.sh`,
//! which renders its own env into a TOML file for `verglas-cache-node`; a
//! query worker has no comparable local file to render, so it reads the
//! variables directly). Every name is resolved through [`Env::get`] so tests
//! exercise the same parsing this binary's `main` does, without mutating the
//! real process environment.

use std::collections::HashMap;
use std::time::Duration;

/// The listen address a query node binds when `VERGLAS_QUERY_LISTEN` is unset.
/// The frozen local-lite harness overrides this to `18400`; production images
/// set it explicitly too, so this default only matters for a bare `cargo run`.
const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:8340";

/// The per-node DataFusion memory ceiling when `VERGLAS_QUERY_MEMORY_LIMIT_BYTES`
/// is unset: 1 GiB, the same figure the frozen acceptance protocol pins for its
/// own run.
const DEFAULT_MEMORY_LIMIT_BYTES: usize = 1024 * 1024 * 1024;

/// The per-query wall-clock budget when `VERGLAS_QUERY_TIMEOUT_SECS` is unset.
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 300;

/// A resolved, immutable configuration for one query node process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryNodeConfig {
    /// Address the HTTP listener binds. No authentication guards this
    /// surface in v1 (org-network-only, matching the cache node's admin
    /// port) — see the crate-level docs.
    pub listen_addr: String,
    /// Iceberg REST catalog base URI.
    pub catalog_uri: String,
    /// Catalog warehouse identifier, when the catalog requires one.
    pub warehouse: Option<String>,
    /// Catalog bearer token, when the catalog requires authenticated reads.
    pub catalog_token: Option<String>,
    /// S3 data endpoint data files and queries are routed through.
    pub s3_endpoint: Option<String>,
    /// SigV4 signing region for the S3 endpoint.
    pub region: String,
    /// S3 endpoint access key id.
    pub access_key_id: Option<String>,
    /// S3 endpoint secret access key.
    pub secret_access_key: Option<String>,
    /// The hard ceiling every DataFusion query session is opened with
    /// ([`verglas_iceberg::PreparedCatalog::open_with_memory_limit`]). A
    /// request naming a larger `memory_limit_bytes` is refused, never granted.
    pub memory_limit_bytes: usize,
    /// Per-query wall-clock timeout. A query still running past this is
    /// aborted and answered with a clean timeout error, never left to hang.
    pub query_timeout: Duration,
}

/// What went wrong resolving the environment into a [`QueryNodeConfig`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

impl QueryNodeConfig {
    /// Resolves configuration from the real process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_map(&std::env::vars().collect())
    }

    /// Resolves configuration from an explicit map — the seam the unit tests
    /// below drive, so a config-parsing bug is caught without mutating the
    /// real process environment (and without the cross-test races that come
    /// with `std::env::set_var` under a parallel test runner).
    pub fn from_map(env: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let get = |name: &str| {
            env.get(name)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        };
        let get_usize = |name: &str| -> Result<Option<usize>, ConfigError> {
            get(name)
                .map(|value| {
                    value.parse::<usize>().map_err(|_| {
                        ConfigError(format!("{name} is not a valid byte count: {value}"))
                    })
                })
                .transpose()
        };
        let get_u64 = |name: &str| -> Result<Option<u64>, ConfigError> {
            get(name)
                .map(|value| {
                    value
                        .parse::<u64>()
                        .map_err(|_| ConfigError(format!("{name} is not a valid integer: {value}")))
                })
                .transpose()
        };

        let listen_addr =
            get("VERGLAS_QUERY_LISTEN").unwrap_or_else(|| DEFAULT_LISTEN_ADDR.to_owned());
        let catalog_uri = get("VERGLAS_CATALOG_URI")
            .ok_or_else(|| ConfigError("VERGLAS_CATALOG_URI is required".to_owned()))?;
        let memory_limit_bytes =
            get_usize("VERGLAS_QUERY_MEMORY_LIMIT_BYTES")?.unwrap_or(DEFAULT_MEMORY_LIMIT_BYTES);
        let query_timeout_secs =
            get_u64("VERGLAS_QUERY_TIMEOUT_SECS")?.unwrap_or(DEFAULT_QUERY_TIMEOUT_SECS);

        Ok(Self {
            listen_addr,
            catalog_uri,
            warehouse: get("VERGLAS_WAREHOUSE"),
            catalog_token: get("VERGLAS_CATALOG_TOKEN"),
            // Named to match `images/cache/boot.sh`'s S3 endpoint/keypair env
            // vars, which carry the identical coordinates for the cache role.
            s3_endpoint: get("VERGLAS_BACKEND_S3_ENDPOINT"),
            region: get("VERGLAS_BACKEND_REGION").unwrap_or_else(|| "us-east-1".to_owned()),
            access_key_id: get("VERGLAS_BACKEND_ACCESS_KEY_ID"),
            secret_access_key: get("VERGLAS_BACKEND_SECRET_ACCESS_KEY"),
            memory_limit_bytes,
            query_timeout: Duration::from_secs(query_timeout_secs),
        })
    }

    /// Builds the [`verglas_iceberg::Connection`] this config resolves to —
    /// everything `open_catalog` needs to reach the REST catalog and route
    /// data-file IO through the configured S3 endpoint.
    pub fn connection(&self) -> verglas_iceberg::Connection {
        verglas_iceberg::Connection {
            catalog_uri: self.catalog_uri.clone(),
            token: self.catalog_token.clone(),
            warehouse: self.warehouse.clone(),
            s3_endpoint: self.s3_endpoint.clone(),
            region: self.region.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The only required variable is the catalog URI; everything else has a
    /// production-sane default.
    #[test]
    fn defaults_apply_when_only_the_catalog_uri_is_set() {
        let config = QueryNodeConfig::from_map(&map(&[(
            "VERGLAS_CATALOG_URI",
            "http://127.0.0.1:18181/catalog",
        )]))
        .expect("minimal config resolves");
        assert_eq!(config.listen_addr, DEFAULT_LISTEN_ADDR);
        assert_eq!(config.memory_limit_bytes, DEFAULT_MEMORY_LIMIT_BYTES);
        assert_eq!(
            config.query_timeout,
            Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS)
        );
        assert_eq!(config.region, "us-east-1");
        assert!(config.warehouse.is_none());
        assert!(config.catalog_token.is_none());
    }

    /// A missing catalog URI is a named, actionable error, not a panic.
    #[test]
    fn missing_catalog_uri_is_rejected() {
        let error = QueryNodeConfig::from_map(&map(&[])).expect_err("catalog uri is required");
        assert!(error.0.contains("VERGLAS_CATALOG_URI"));
    }

    /// Every documented override is honored and threads into the built
    /// `Connection` unchanged.
    #[test]
    fn full_environment_resolves_every_field_and_the_connection() {
        let config = QueryNodeConfig::from_map(&map(&[
            ("VERGLAS_QUERY_LISTEN", "0.0.0.0:18400"),
            ("VERGLAS_CATALOG_URI", "http://127.0.0.1:18181/catalog"),
            ("VERGLAS_WAREHOUSE", "default"),
            ("VERGLAS_CATALOG_TOKEN", "secret-token"),
            ("VERGLAS_BACKEND_S3_ENDPOINT", "http://127.0.0.1:19000"),
            ("VERGLAS_BACKEND_REGION", "us-west-2"),
            ("VERGLAS_BACKEND_ACCESS_KEY_ID", "access"),
            ("VERGLAS_BACKEND_SECRET_ACCESS_KEY", "secret"),
            ("VERGLAS_QUERY_MEMORY_LIMIT_BYTES", "134217728"),
            ("VERGLAS_QUERY_TIMEOUT_SECS", "30"),
        ]))
        .expect("full config resolves");
        assert_eq!(config.listen_addr, "0.0.0.0:18400");
        assert_eq!(config.memory_limit_bytes, 134_217_728);
        assert_eq!(config.query_timeout, Duration::from_secs(30));

        let connection = config.connection();
        assert_eq!(connection.catalog_uri, "http://127.0.0.1:18181/catalog");
        assert_eq!(connection.warehouse.as_deref(), Some("default"));
        assert_eq!(connection.token.as_deref(), Some("secret-token"));
        assert_eq!(
            connection.s3_endpoint.as_deref(),
            Some("http://127.0.0.1:19000")
        );
        assert_eq!(connection.region, "us-west-2");
        assert_eq!(connection.access_key_id.as_deref(), Some("access"));
        assert_eq!(connection.secret_access_key.as_deref(), Some("secret"));
    }

    /// A non-numeric byte count or timeout is a named, actionable error.
    #[test]
    fn invalid_numeric_overrides_are_rejected() {
        let error = QueryNodeConfig::from_map(&map(&[
            ("VERGLAS_CATALOG_URI", "http://127.0.0.1:18181/catalog"),
            ("VERGLAS_QUERY_MEMORY_LIMIT_BYTES", "not-a-number"),
        ]))
        .expect_err("bad byte count is rejected");
        assert!(error.0.contains("VERGLAS_QUERY_MEMORY_LIMIT_BYTES"));

        let error = QueryNodeConfig::from_map(&map(&[
            ("VERGLAS_CATALOG_URI", "http://127.0.0.1:18181/catalog"),
            ("VERGLAS_QUERY_TIMEOUT_SECS", "soon"),
        ]))
        .expect_err("bad timeout is rejected");
        assert!(error.0.contains("VERGLAS_QUERY_TIMEOUT_SECS"));
    }
}
