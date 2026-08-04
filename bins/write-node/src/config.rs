//! Configuration for the isolated logical write role.

use std::path::Path;

use serde::{Deserialize, Serialize};
use verglas_core::config::Catalog;

/// Root configuration accepted by `verglas-write`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WriteConfig {
    /// Port serving the private write-role API.
    #[serde(default)]
    pub listen: Listen,
    /// Cache S3 endpoint used for every object write.
    pub cache: CacheEndpoint,
    /// Real Iceberg REST catalog used for atomic commits.
    pub catalog: Catalog,
}

/// Write-role listener configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Listen {
    /// Private HTTP port. Zero requests an ephemeral port.
    pub admin_port: u16,
}

impl Default for Listen {
    /// Uses the next role port after the query worker.
    fn default() -> Self {
        Self { admin_port: 8336 }
    }
}

/// S3-compatible Verglas cache coordinates used by the writer.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct CacheEndpoint {
    /// Verglas S3 endpoint. There is no direct-origin fallback.
    pub s3_endpoint: String,
    /// SigV4 signing region.
    pub region: Option<String>,
    /// AWS-INI keypair accepted by the cache endpoint.
    pub credentials_file: Option<String>,
    /// Profile inside the credentials file.
    pub credentials_profile: Option<String>,
}

impl WriteConfig {
    /// Reads and validates a role configuration file.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("cannot read config file {}: {error}", path.display()))?;
        let config: Self =
            toml::from_str(&text).map_err(|error| format!("invalid config: {error}"))?;
        config.validate()?;
        Ok(config)
    }

    /// Validates endpoints and named credentials files before startup.
    fn validate(&self) -> Result<(), String> {
        validate_http_url("cache.s3_endpoint", &self.cache.s3_endpoint)?;
        validate_http_url("catalog.uri", &self.catalog.uri)?;
        for (field, path) in [
            (
                "cache.credentials_file",
                self.cache.credentials_file.as_deref(),
            ),
            (
                "catalog.credentials_file",
                self.catalog.credentials_file.as_deref(),
            ),
        ] {
            if let Some(path) = path
                && !Path::new(path).is_file()
            {
                return Err(format!(
                    "{field} `{path}` does not exist or is not readable"
                ));
            }
        }
        Ok(())
    }
}

/// Requires an HTTP(S) endpoint for a role connection.
fn validate_http_url(field: &str, value: &str) -> Result<(), String> {
    if value.starts_with("http://") || value.starts_with("https://") {
        Ok(())
    } else {
        Err(format!(
            "{field} `{value}` must start with http:// or https://"
        ))
    }
}
