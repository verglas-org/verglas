#![warn(missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]
#![forbid(unsafe_code)]

//! Verglas-backed authorization for direct Lakekeeper catalog access.

mod action;
mod authorizer;
mod client;
mod config;
mod resource;

pub use action::VerglasAction;
pub use authorizer::VerglasAuthorizer;
pub use client::{
    CallerDecision, DecisionClient, DecisionClientError, ResourceSync, SyncResourceKind,
};
pub use config::{CONFIG, DynAppConfig, VerglasAuthzConfig};
use lakekeeper::service::ServerId;
pub use resource::ResourceMapper;

/// Builds an authorizer from the process-wide Lakekeeper configuration.
pub async fn new_authorizer_from_default_config(
    server_id: ServerId,
) -> anyhow::Result<VerglasAuthorizer> {
    let config = CONFIG.verglas.clone().ok_or_else(|| {
        anyhow::anyhow!("Verglas authz is enabled but `verglas` is not configured")
    })?;
    VerglasAuthorizer::try_new(server_id, config)
}
