//! Builds the self-hosted server configuration from Docker Compose environment
//! variables. This is an explicit container startup mode; it does not read or
//! generate a server TOML file.

use std::collections::HashMap;

use verglas_core::config::{
    Analytics, Backend, ByteSize, Cache, Catalog, Config, Listen, Log, QueryWorker, Rill,
    WriteWorker,
};

/// A validated server configuration plus the S3 credentials accepted from
/// clients. Endpoint credentials stay outside [`Config`] so serialization of
/// the operator schema cannot expose their secret.
pub(crate) struct EnvironmentConfig {
    /// The ordinary immutable server configuration.
    pub(crate) config: Config,
    /// The keypair accepted by the Verglas S3 endpoint.
    pub(crate) endpoint_credentials: (String, String),
}

impl EnvironmentConfig {
    /// Reads the process environment used by the Docker image and validates the
    /// resulting server configuration before any listener binds.
    pub(crate) fn load() -> Result<Self, String> {
        Self::from_pairs(std::env::vars())
    }

    /// Builds a configuration from explicit key/value pairs. Keeping this
    /// constructor deterministic makes the container contract testable without
    /// mutating process-global environment variables.
    fn from_pairs<I, K, V>(pairs: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let values: HashMap<String, String> = pairs
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        let required = |name: &'static str| -> Result<String, String> {
            values
                .get(name)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| format!("{name} is required in --environment mode"))
        };

        let cache = Cache {
            dir: required("VERGLAS_CACHE_DIR")?.into(),
            capacity_bytes: parse_bytes(
                "VERGLAS_CACHE_CAPACITY",
                &required("VERGLAS_CACHE_CAPACITY")?,
            )?,
            dram_bytes: parse_bytes("VERGLAS_CACHE_DRAM", &required("VERGLAS_CACHE_DRAM")?)?,
            ..Cache::default()
        };

        let backend = Backend {
            bucket: Some(required("VERGLAS_BACKEND_BUCKET")?),
            endpoint: Some(required("VERGLAS_BACKEND_ENDPOINT")?),
            region: Some(required("VERGLAS_BACKEND_REGION")?),
            ..Backend::default()
        };

        let catalog = Catalog {
            uri: required("VERGLAS_CATALOG_URI")?,
            poll_interval_secs: 30,
            include: Vec::new(),
            exclude: Vec::new(),
            credentials_file: None,
            credentials_profile: None,
            bearer_token: Some(required("VERGLAS_CATALOG_BEARER_TOKEN")?),
            sigv4_region: None,
            sigv4_signing_name: None,
            warehouse: Some(required("VERGLAS_CATALOG_WAREHOUSE")?),
        };
        let query_worker = QueryWorker {
            binary: required("VERGLAS_QUERY_WORKER_BINARY")?,
        };
        let write_worker = WriteWorker {
            binary: required("VERGLAS_WRITE_WORKER_BINARY")?,
        };
        let endpoint_credentials = (
            required("VERGLAS_S3_ACCESS_KEY_ID")?,
            required("VERGLAS_S3_SECRET_ACCESS_KEY")?,
        );
        let analytics = Analytics {
            rill: Rill {
                uri: required("VERGLAS_RILL_URI")?,
                instance_id: required("VERGLAS_RILL_INSTANCE_ID")?,
                browser_uri: required("VERGLAS_RILL_BROWSER_URI")?,
                s3_uri: required("VERGLAS_RILL_S3_URI")?,
            },
        };
        let config = Config {
            listen: Listen::default(),
            log: Log::default(),
            cache,
            backend,
            auth: None,
            catalog: Some(catalog),
            query_worker: Some(query_worker),
            write_worker: Some(write_worker),
            analytics: Some(analytics),
            cluster: None,
            control_plane: None,
        };
        config
            .validate()
            .map_err(|error| format!("invalid environment configuration: {error}"))?;
        Ok(Self {
            config,
            endpoint_credentials,
        })
    }
}

/// Parses the same binary size suffixes accepted by the operator TOML schema.
fn parse_bytes(name: &'static str, value: &str) -> Result<ByteSize, String> {
    let value = value.trim();
    let digits_end = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let number: u64 = value[..digits_end]
        .parse()
        .map_err(|_| format!("{name} must be a size such as 20GB"))?;
    let multiplier = match value[digits_end..].trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "KB" => 1024,
        "MB" => 1024 * 1024,
        "GB" => 1024 * 1024 * 1024,
        "TB" => 1024_u64.pow(4),
        _ => return Err(format!("{name} must use B, KB, MB, GB, or TB")),
    };
    number
        .checked_mul(multiplier)
        .map(ByteSize)
        .ok_or_else(|| format!("{name} overflows a byte count"))
}

#[cfg(test)]
mod tests {
    use super::EnvironmentConfig;

    /// Returns a complete container environment rooted in a writable scratch
    /// cache so individual tests can vary one field.
    fn complete_environment() -> Vec<(String, String)> {
        let cache = tempfile::tempdir()
            .expect("cache directory")
            .keep()
            .display()
            .to_string();
        [
            ("VERGLAS_CACHE_DIR", cache.as_str()),
            ("VERGLAS_CACHE_CAPACITY", "64MB"),
            ("VERGLAS_CACHE_DRAM", "80MB"),
            ("VERGLAS_BACKEND_BUCKET", "lake"),
            (
                "VERGLAS_BACKEND_ENDPOINT",
                "https://account.r2.cloudflarestorage.com",
            ),
            ("VERGLAS_BACKEND_REGION", "auto"),
            ("VERGLAS_CATALOG_URI", "https://catalog.example.com"),
            ("VERGLAS_CATALOG_WAREHOUSE", "account_lake"),
            ("VERGLAS_CATALOG_BEARER_TOKEN", "catalog-token"),
            ("VERGLAS_S3_ACCESS_KEY_ID", "verglas-local"),
            ("VERGLAS_S3_SECRET_ACCESS_KEY", "endpoint-secret"),
            ("VERGLAS_QUERY_WORKER_BINARY", "/usr/bin/true"),
            ("VERGLAS_WRITE_WORKER_BINARY", "/usr/bin/true"),
            ("VERGLAS_RILL_URI", "http://rill:9009"),
            ("VERGLAS_RILL_INSTANCE_ID", "default"),
            ("VERGLAS_RILL_BROWSER_URI", "http://127.0.0.1:9009"),
            ("VERGLAS_RILL_S3_URI", "http://verglas-server:8333"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
    }

    #[test]
    fn complete_environment_builds_the_self_hosted_server() {
        let loaded = EnvironmentConfig::from_pairs(complete_environment())
            .expect("complete environment must validate");
        assert_eq!(loaded.config.backend.bucket.as_deref(), Some("lake"));
        assert_eq!(loaded.config.cache.capacity_bytes.0, 64 * 1024 * 1024);
        assert_eq!(loaded.endpoint_credentials.0, "verglas-local");
        let catalog = loaded.config.catalog.expect("catalog");
        assert_eq!(catalog.warehouse.as_deref(), Some("account_lake"));
        assert_eq!(catalog.bearer_token.as_deref(), Some("catalog-token"));
        let analytics = loaded.config.analytics.expect("analytics");
        assert_eq!(analytics.rill.uri, "http://rill:9009");
    }

    #[test]
    fn missing_required_value_names_the_compose_variable() {
        let environment = complete_environment()
            .into_iter()
            .filter(|(name, _)| name != "VERGLAS_CATALOG_URI");
        let error = EnvironmentConfig::from_pairs(environment)
            .err()
            .expect("missing catalog must fail");
        assert!(error.contains("VERGLAS_CATALOG_URI"), "{error}");
    }
}
