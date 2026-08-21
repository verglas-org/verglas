#![warn(missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]
#![forbid(unsafe_code)]

//! control-plane-credential authorization for direct Catalog catalog access.

mod action;
mod authorizer;
mod client;
mod config;
mod resource;

pub use action::VerglasAction;
pub use authorizer::VerglasAuthorizer;
pub use client::{CallerDecision, DecisionClient, DecisionClientError};
pub use config::{CONFIG, CredentialAuthzConfig, DynAppConfig};
pub use resource::ResourceMapper;
use verglas_catalog_core::service::ServerId;

/// Builds an authorizer from the process-wide Catalog configuration.
pub async fn new_authorizer_from_default_config(
    server_id: ServerId,
) -> anyhow::Result<VerglasAuthorizer> {
    let config = CONFIG.credential.clone().ok_or_else(|| {
        anyhow::anyhow!("Verglas authz is enabled but `credential` is not configured")
    })?;
    VerglasAuthorizer::try_new(server_id, config)
}
