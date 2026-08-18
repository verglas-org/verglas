//! Configuration for Lakekeeper's local Cloudflare credential verifier.

use std::sync::LazyLock;

use lakekeeper::AuthZBackend;
use serde::{Deserialize, Serialize};

/// Process-wide Lakekeeper authorization configuration.
pub static CONFIG: LazyLock<DynAppConfig> = LazyLock::new(get_config);

/// Authorization fields loaded under the standard Lakekeeper environment prefixes.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DynAppConfig {
    /// Selected Lakekeeper authorization implementation.
    #[serde(default)]
    pub authz_backend: AuthZBackend,
    /// Cloudflare Worker credential-verification settings.
    pub cloudflare: Option<CloudflareAuthzConfig>,
}

impl DynAppConfig {
    /// Returns whether this process selected the Verglas authorizer.
    #[must_use]
    pub fn is_verglas_enabled(&self) -> bool {
        self.authz_backend == AuthZBackend::External("verglas".to_owned())
    }
}

/// Cloudflare-issued credential settings for this tenant's catalog.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CloudflareAuthzConfig {
    /// Expected `iss` claim of credentials minted by the Worker.
    pub issuer: String,
    /// Worker-published Cloudflare JWKS JSON used for local signature checks.
    pub jwks: String,
    /// Tenant whose grants this Lakekeeper process may serve.
    pub tenant_id: String,
}

/// Reads local configuration without any network dependency.
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
        .unwrap_or_else(|error| panic!("Failed to extract Cloudflare authz config: {error}"))
}
