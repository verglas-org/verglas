use std::{path::PathBuf, sync::LazyLock};

use lakekeeper::AuthZBackend;
use serde::{Deserialize, Serialize};
use url::Url;

/// Process-wide Verglas authorization configuration.
pub static CONFIG: LazyLock<DynAppConfig> = LazyLock::new(get_config);

/// Authorization fields loaded under the standard Lakekeeper environment prefixes.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DynAppConfig {
    /// Selected Lakekeeper authorization implementation.
    #[serde(default)]
    pub authz_backend: AuthZBackend,
    /// Verglas decision-service settings.
    pub verglas: Option<VerglasAuthzConfig>,
}

impl DynAppConfig {
    /// Returns whether this process selected the Verglas authorizer.
    #[must_use]
    pub fn is_verglas_enabled(&self) -> bool {
        self.authz_backend == AuthZBackend::External("verglas".to_owned())
    }
}

/// Verglas access-service connection for caller decisions and lifecycle sync.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VerglasAuthzConfig {
    /// Base URL of the tenant-local Verglas access service.
    pub endpoint: Url,
    /// File containing the short-lived `policy-engine` workload bearer.
    pub workload_credential_file: PathBuf,
    /// Verglas tenant containing the databases served by this catalog.
    pub tenant_id: String,
}

fn get_config() -> DynAppConfig {
    let defaults = figment::providers::Serialized::defaults(DynAppConfig::default());
    #[cfg(not(test))]
    let prefixes = &["ICEBERG_REST__", "LAKEKEEPER__"];
    #[cfg(test)]
    let prefixes = &["LAKEKEEPER_TEST__"];

    let mut config = figment::Figment::from(defaults);
    for prefix in prefixes {
        config = config.merge(figment::providers::Env::prefixed(prefix).split("__"));
    }
    config
        .extract()
        .unwrap_or_else(|error| panic!("Failed to extract Verglas authz config: {error}"))
}
