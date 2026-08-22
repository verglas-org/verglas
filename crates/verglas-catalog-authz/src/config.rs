//! Configuration for Catalog's local control-plane credential verifier.

use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use verglas_catalog_core::AuthZBackend;

/// Process-wide Catalog authorization configuration.
pub static CONFIG: LazyLock<DynAppConfig> = LazyLock::new(get_config);

/// Authorization fields loaded under the standard Catalog environment prefixes.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DynAppConfig {
    /// Selected Catalog authorization implementation.
    #[serde(default)]
    pub authz_backend: AuthZBackend,
    /// Control-plane credential-verification settings.
    pub credential: Option<CredentialAuthzConfig>,
}

impl DynAppConfig {
    /// Returns whether this process selected the Verglas authorizer.
    #[must_use]
    pub fn is_verglas_enabled(&self) -> bool {
        self.authz_backend == AuthZBackend::External("verglas".to_owned())
    }
}

/// control-plane-issued credential settings for this tenant's catalog.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CredentialAuthzConfig {
    /// Expected `iss` claim of credentials minted by the Worker.
    pub issuer: String,
    /// Worker-published control-plane JWKS JSON used for local signature checks.
    pub jwks: String,
    /// Tenant whose grants this Catalog process may serve.
    pub tenant_id: String,
}

/// Reads local configuration without any network dependency.
fn get_config() -> DynAppConfig {
    let defaults = figment::providers::Serialized::defaults(DynAppConfig::default());
    #[cfg(not(test))]
    let prefixes = &["ICEBERG_REST__", "VERGLAS_CATALOG__"];
    #[cfg(test)]
    let prefixes = &["VERGLAS_CATALOG_TEST__"];

    let mut config = figment::Figment::from(defaults);
    for prefix in prefixes {
        config = config.merge(figment::providers::Env::prefixed(prefix).split("__"));
    }
    config
        .extract()
        .unwrap_or_else(|error| panic!("Failed to extract control-plane authz config: {error}"))
}
