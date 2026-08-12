//! Provisions the system Verglas Neon compute before any PostgreSQL-dependent service starts.

use std::path::PathBuf;

use async_trait::async_trait;
use clap::Parser;
use verglas_database::DatabaseServiceError;

/// Minimal shared trait boundary required by the managed Postgres implementation.
#[allow(dead_code)]
mod database_runtime {
    use super::*;

    /// Runtime lifecycle used by the ordinary Access database manager.
    #[async_trait]
    pub(crate) trait ManagedPostgresRuntime: Send + Sync {
        /// Reconciles all persistent components for one declared database.
        async fn ensure_database(&self, name: &str) -> Result<(), DatabaseServiceError>;

        /// Removes all database-owned runtime components.
        async fn delete_database(&self, name: &str) -> Result<(), DatabaseServiceError>;
    }
}
#[allow(dead_code)]
mod postgres_runtime;

/// One-shot system Neon bootstrap configuration.
#[derive(Debug, Parser)]
#[command(name = "verglas-neon-bootstrap", version)]
struct Args {
    /// Tenant whose system control database is being created.
    #[arg(long, env = "VERGLAS_TENANT_ID")]
    tenant_id: String,
    /// Authenticated desired-container API.
    #[arg(long, env = "VERGLAS_CONTAINER_RUNTIME_URL")]
    container_runtime_url: String,
    /// Bearer accepted by the desired-container API.
    #[arg(long, env = "VERGLAS_CONTAINER_RUNTIME_TOKEN", hide_env_values = true)]
    container_runtime_token: String,
    /// Cache-routed durable S3 endpoint.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_STORAGE_ENDPOINT")]
    storage_endpoint: String,
    /// Durable bucket used by Neon pageservers.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_STORAGE_BUCKET")]
    storage_bucket: String,
    /// S3 signing region.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_STORAGE_REGION")]
    storage_region: String,
    /// Cache endpoint access key.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_STORAGE_ACCESS_KEY_ID")]
    storage_access_key_id: String,
    /// Cache endpoint secret.
    #[arg(
        long,
        env = "VERGLAS_MANAGED_POSTGRES_STORAGE_SECRET_ACCESS_KEY",
        hide_env_values = true
    )]
    storage_secret_access_key: String,
    /// Verglas cache-ring safekeeper ingress.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_SAFEKEEPERS")]
    safekeepers: String,
    /// Hex-encoded key retained for ordinary managed database plans.
    #[arg(
        long,
        env = "VERGLAS_MANAGED_POSTGRES_CREDENTIAL_KEY",
        hide_env_values = true
    )]
    credential_key: String,
    /// Password shared only by default-stack system services.
    #[arg(long, env = "VERGLAS_SYSTEM_POSTGRES_PASSWORD", hide_env_values = true)]
    system_password: String,
    /// Shared directory visible to the desired-container runtime.
    #[arg(
        long,
        env = "VERGLAS_MANAGED_POSTGRES_CREDENTIAL_DIRECTORY",
        default_value = "/var/lib/verglas-container-runtime/postgres"
    )]
    credential_directory: PathBuf,
}

/// Reconciles system Neon and initializes every application-wide logical database.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let credential_key = hex::decode(&args.credential_key)
        .map_err(|_| "VERGLAS_MANAGED_POSTGRES_CREDENTIAL_KEY must be hexadecimal")?;
    let provisioner = postgres_runtime::ManagedPostgresProvisioner::new(
        postgres_runtime::ManagedPostgresConfig {
            runtime_endpoint: args.container_runtime_url,
            runtime_token: args.container_runtime_token,
            tenant_id: args.tenant_id,
            remote_endpoint: args.storage_endpoint,
            remote_bucket: args.storage_bucket,
            remote_region: args.storage_region,
            remote_access_key_id: args.storage_access_key_id,
            remote_secret_access_key: args.storage_secret_access_key,
            safekeepers: args.safekeepers,
            credential_key,
            access_endpoint: "http://verglas-access:8345".to_owned(),
            policy_engine_token_file: PathBuf::from("/var/run/verglas/neon/token"),
            tls_certificate_file: PathBuf::from("/var/run/verglas/neon/tls.crt"),
            tls_private_key_file: PathBuf::from("/var/run/verglas/neon/tls.key"),
            credential_directory: args.credential_directory,
        },
    )?;
    provisioner
        .ensure_system(
            "verglas_permissions",
            &args.system_password,
            &["verglas_scheduler", "verglas_catalog"],
        )
        .await?;
    Ok(())
}
