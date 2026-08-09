//! Builds the self-hosted server configuration from Docker Compose environment
//! variables. This is an explicit container startup mode; it does not read or
//! generate a server TOML file.

use std::collections::HashMap;

use verglas_core::config::{Backend, ByteSize, Cache, Config, Listen, Log};

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
        for name in [
            "VERGLAS_BACKEND_BUCKET",
            "VERGLAS_BACKEND_ENDPOINT",
            "VERGLAS_BACKEND_REGION",
            "VERGLAS_CATALOG_URI",
            "VERGLAS_CATALOG_WAREHOUSE",
            "VERGLAS_CATALOG_BEARER_TOKEN",
            "VERGLAS_S3_ACCESS_KEY_ID",
            "VERGLAS_S3_SECRET_ACCESS_KEY",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ] {
            if values
                .get(name)
                .is_some_and(|value| !value.trim().is_empty())
            {
                return Err(format!(
                    "{name} is not accepted in --environment mode; create provider and catalog resources through dynamic database bindings"
                ));
            }
        }

        let cache = Cache {
            dir: required("VERGLAS_CACHE_DIR")?.into(),
            capacity_bytes: parse_bytes(
                "VERGLAS_CACHE_CAPACITY",
                &required("VERGLAS_CACHE_CAPACITY")?,
            )?,
            dram_bytes: parse_bytes("VERGLAS_CACHE_DRAM", &required("VERGLAS_CACHE_DRAM")?)?,
            ..Cache::default()
        };

        let endpoint_credentials = (
            "verglas-local".to_owned(),
            required("VERGLAS_ACCESS_SERVICE_TOKEN")?,
        );
        let backend = Backend::default();
        let config = Config {
            listen: Listen::default(),
            log: Log::default(),
            cache,
            backend,
            auth: None,
            catalog: None,
            query_worker: None,
            write_worker: None,
            analytics: None,
            cluster: None,
        };
        config
            .validate_dynamic()
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
            ("VERGLAS_ACCESS_SERVICE_TOKEN", "local-access-token"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
    }

    #[test]
    fn complete_environment_builds_the_self_hosted_server() {
        let loaded = EnvironmentConfig::from_pairs(complete_environment())
            .expect("complete environment must validate");
        assert!(loaded.config.backend.bucket.is_none());
        assert!(loaded.config.backend.bucket_globs.is_empty());
        assert!(loaded.config.catalog.is_none());
        assert_eq!(loaded.config.cache.capacity_bytes.0, 64 * 1024 * 1024);
        assert_eq!(loaded.endpoint_credentials.0, "verglas-local");
        assert_eq!(loaded.endpoint_credentials.1, "local-access-token");
    }

    #[test]
    fn missing_required_value_names_the_compose_variable() {
        let environment = complete_environment()
            .into_iter()
            .filter(|(name, _)| name != "VERGLAS_ACCESS_SERVICE_TOKEN");
        let error = EnvironmentConfig::from_pairs(environment)
            .err()
            .expect("missing access token must fail");
        assert!(error.contains("VERGLAS_ACCESS_SERVICE_TOKEN"), "{error}");
    }

    #[test]
    fn static_provider_environment_is_rejected() {
        let mut environment = complete_environment();
        environment.push(("VERGLAS_BACKEND_BUCKET".to_owned(), "legacy".to_owned()));
        let error = EnvironmentConfig::from_pairs(environment)
            .err()
            .expect("static provider configuration must fail");
        assert!(error.contains("dynamic database bindings"), "{error}");
    }
}
