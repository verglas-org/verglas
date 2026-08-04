//! Production connection and serving loop for the isolated write role.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Bytes;
use iceberg::Catalog;
use verglas_api::table::CommitResponse;
use verglas_iceberg::Connection;

use crate::admin::{self, AppState, BatchCommitter};
use crate::config::WriteConfig;

/// Production committer over a real Iceberg REST catalog.
struct IcebergCommitter {
    catalog: Arc<dyn Catalog>,
}

#[async_trait]
impl BatchCommitter for IcebergCommitter {
    /// Writes Parquet through the cache endpoint and publishes the snapshot.
    async fn commit(
        &self,
        table: &str,
        batches: Vec<arrow_array::RecordBatch>,
        idempotency_key: Option<String>,
    ) -> Result<CommitResponse, String> {
        let ident = verglas_iceberg::parse_table_ident(table).map_err(|error| error.to_string())?;
        verglas_iceberg::tables_api::commit_batches(
            self.catalog.as_ref(),
            &ident,
            batches,
            idempotency_key,
        )
        .await
        .map_err(|error| error.to_string())
    }

    async fn ingest(
        &self,
        table: &str,
        mode: &str,
        format: &str,
        partition_by: Option<&str>,
        body: Bytes,
    ) -> Result<serde_json::Value, String> {
        let ident = verglas_iceberg::parse_table_ident(table).map_err(|error| error.to_string())?;
        let extension = match format {
            "csv" | "jsonl" | "parquet" => format,
            other => {
                return Err(format!(
                    "unknown format `{other}`: expected csv, jsonl, or parquet"
                ));
            }
        };
        let scratch =
            tempfile::tempdir().map_err(|error| format!("ingest scratch dir: {error}"))?;
        let path = scratch.path().join(format!("ingest.{extension}"));
        tokio::fs::write(&path, body)
            .await
            .map_err(|error| format!("ingest scratch write: {error}"))?;
        match mode {
            "create" => verglas_iceberg::write::create_table(
                self.catalog.as_ref(),
                &ident,
                &path,
                partition_by,
            )
            .await
            .map_err(|error| error.to_string())
            .and_then(|report| serde_json::to_value(report).map_err(|error| error.to_string())),
            "append" => verglas_iceberg::write::append(self.catalog.as_ref(), &ident, &path)
                .await
                .map_err(|error| error.to_string())
                .and_then(|report| serde_json::to_value(report).map_err(|error| error.to_string())),
            other => Err(format!("unknown mode `{other}`: expected create or append")),
        }
    }
}

/// Builds the Iceberg connection with all object I/O pinned to Verglas S3.
pub fn connection_for(config: &WriteConfig) -> Result<Connection, String> {
    let (access_key_id, secret_access_key) = resolve_keypair(
        config.cache.credentials_file.as_deref(),
        config.cache.credentials_profile.as_deref(),
    )?
    .map_or((None, None), |(id, secret)| (Some(id), Some(secret)));
    let token = config
        .catalog
        .resolve_bearer_token()
        .map_err(|error| error.to_string())?;
    Ok(Connection {
        catalog_uri: config.catalog.uri.clone(),
        token,
        warehouse: config.catalog.warehouse.clone(),
        s3_endpoint: Some(config.cache.s3_endpoint.clone()),
        region: config
            .cache
            .region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_owned()),
        access_key_id,
        secret_access_key,
    })
}

/// Opens the catalog and serves until terminated.
pub async fn run(
    config: &WriteConfig,
    ports_file: Option<std::path::PathBuf>,
) -> Result<(), String> {
    let catalog = verglas_iceberg::catalog::open_catalog(&connection_for(config)?)
        .await
        .map_err(|error| format!("cannot open catalog: {error}"))?;
    let state = AppState::new(Arc::new(IcebergCommitter { catalog }));
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.listen.admin_port))
        .await
        .map_err(|error| format!("cannot bind write role: {error}"))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("cannot read write role address: {error}"))?;
    if let Some(path) = ports_file {
        report_port(&path, address)?;
    }
    axum::serve(listener, admin::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|error| format!("write role server failed: {error}"))
}

/// Resolves the cache endpoint keypair from an AWS-INI file.
fn resolve_keypair(
    path: Option<&str>,
    profile: Option<&str>,
) -> Result<Option<(String, String)>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read credentials file `{path}`: {error}"))?;
    verglas_backend::read_aws_keypair(&text, profile.unwrap_or("default"))
        .ok_or_else(|| format!("credentials file `{path}` has no complete profile"))
        .map(Some)
}

/// Writes the ephemeral private port for the parent dispatcher.
fn report_port(path: &std::path::Path, address: std::net::SocketAddr) -> Result<(), String> {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("cannot open ports file {}: {error}", path.display()))?;
    writeln!(file, "admin {address}")
        .map_err(|error| format!("cannot write ports file {}: {error}", path.display()))
}

/// Resolves on SIGINT or SIGTERM.
async fn shutdown_signal() {
    let control_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = control_c => {},
        _ = terminate => {},
    }
}
