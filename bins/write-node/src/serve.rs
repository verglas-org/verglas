//! Production connection and serving loop for the isolated write role.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Bytes;
use iceberg::Catalog;
use verglas_api::table::CommitResponse;
use verglas_iceberg::Connection;

use crate::admin::{self, AppState, BatchCommitter, CommitPublisher};
use crate::config::WriteConfig;

/// Production committer over a real Iceberg REST catalog.
struct IcebergCommitter {
    catalog: Arc<dyn Catalog>,
}

/// Authenticated publisher for the database-scoped durable table event queue.
struct HttpCommitPublisher {
    endpoint: reqwest::Url,
    database: String,
    bearer: String,
    http: reqwest::Client,
}

#[async_trait]
impl CommitPublisher for HttpCommitPublisher {
    async fn publish(&self, table: &str, snapshot_id: &str) -> Result<(), String> {
        let mut endpoint = self.endpoint.clone();
        endpoint
            .path_segments_mut()
            .map_err(|_| "table event endpoint cannot carry path segments".to_owned())?
            .pop_if_empty()
            .push("v1")
            .push("databases")
            .push(&self.database)
            .push("commits")
            .push(table);
        let response = self
            .http
            .post(endpoint)
            .bearer_auth(&self.bearer)
            .json(&serde_json::json!({ "snapshotId": snapshot_id }))
            .send()
            .await
            .map_err(|error| format!("table event request failed: {error}"))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!(
                "table event endpoint returned HTTP {}",
                response.status()
            ))
        }
    }
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
        idempotency_key: Option<String>,
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
            "append" => match idempotency_key {
                Some(key) => {
                    let ingested =
                        verglas_iceberg::ingest::read(&path).map_err(|error| error.to_string())?;
                    verglas_iceberg::tables_api::commit_batches(
                        self.catalog.as_ref(),
                        &ident,
                        ingested.batches,
                        Some(key),
                    )
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(|report| {
                        serde_json::to_value(report).map_err(|error| error.to_string())
                    })
                }
                None => verglas_iceberg::write::append(self.catalog.as_ref(), &ident, &path)
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(|report| {
                        serde_json::to_value(report).map_err(|error| error.to_string())
                    }),
            },
            other => Err(format!("unknown mode `{other}`: expected create or append")),
        }
    }
}

/// Builds the Iceberg connection with all object I/O pinned to Verglas S3.
pub fn connection_for(config: &WriteConfig) -> Result<Connection, String> {
    let caller_bearer = std::env::var(verglas_core::RUN_BEARER_TOKEN_ENV)
        .ok()
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| "write role requires an inherited caller bearer".to_owned())?;
    connection_for_bearer(config, caller_bearer)
}

/// Resolves a catalog connection whose bearer was supplied by the ephemeral parent process.
fn connection_for_bearer(
    config: &WriteConfig,
    caller_bearer: String,
) -> Result<Connection, String> {
    let (access_key_id, secret_access_key) = resolve_keypair(
        config.cache.credentials_file.as_deref(),
        config.cache.credentials_profile.as_deref(),
    )?
    .map_or((None, None), |(id, secret)| (Some(id), Some(secret)));
    Ok(Connection {
        catalog_uri: config.catalog.uri.clone(),
        token: Some(caller_bearer),
        warehouse: config.catalog.warehouse.clone(),
        s3_endpoints: config.cache.s3_endpoints.clone(),
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
    let caller_bearer = std::env::var(verglas_core::RUN_BEARER_TOKEN_ENV)
        .ok()
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| "write role requires an inherited caller bearer".to_owned())?;
    let catalog = verglas_iceberg::catalog::open_catalog(&connection_for_bearer(
        config,
        caller_bearer.clone(),
    )?)
    .await
    .map_err(|error| format!("cannot open catalog: {error}"))?;
    let access_uri = std::env::var("VERGLAS_ACCESS_URI")
        .map_err(|_| "write role requires VERGLAS_ACCESS_URI".to_owned())?;
    let endpoint = reqwest::Url::parse(&access_uri)
        .map_err(|error| format!("invalid VERGLAS_ACCESS_URI: {error}"))?;
    let database = database_from_catalog_uri(&config.catalog.uri)?;
    let state = AppState::new(
        Arc::new(IcebergCommitter { catalog }),
        Arc::new(HttpCommitPublisher {
            endpoint,
            database,
            bearer: caller_bearer,
            http: reqwest::Client::new(),
        }),
    );
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

/// Extracts the database route from the writer's database-scoped catalog mount.
fn database_from_catalog_uri(uri: &str) -> Result<String, String> {
    let uri = reqwest::Url::parse(uri).map_err(|error| format!("invalid catalog URI: {error}"))?;
    let segments = uri
        .path_segments()
        .ok_or_else(|| "catalog URI has no path segments".to_owned())?
        .collect::<Vec<_>>();
    match segments.as_slice() {
        ["v1", "databases", database, "catalog"] if !database.is_empty() => {
            Ok((*database).to_owned())
        }
        _ => Err("catalog URI must end with /v1/databases/{database}/catalog".to_owned()),
    }
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use axum::extract::{OriginalUri, State};
    use axum::http::{HeaderMap, header};
    use axum::routing::get;
    use axum::{Json, Router};

    /// Captures catalog authentication while returning the two minimal REST responses this test needs.
    async fn catalog_response(
        State(authorization): State<Arc<Mutex<Option<String>>>>,
        OriginalUri(uri): OriginalUri,
        headers: HeaderMap,
    ) -> Json<serde_json::Value> {
        *authorization.lock().expect("authorization") = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = if uri.path() == "/v1/config" {
            serde_json::json!({"defaults": {}, "overrides": {}})
        } else {
            serde_json::json!({"namespaces": []})
        };
        Json(body)
    }

    /// Builds a write-role config with all catalog credentials intentionally absent.
    fn config(catalog_uri: String) -> WriteConfig {
        WriteConfig {
            listen: crate::config::Listen { admin_port: 0 },
            cache: crate::config::CacheEndpoint {
                s3_endpoints: vec!["http://127.0.0.1:8333".to_owned()],
                ..crate::config::CacheEndpoint::default()
            },
            catalog: verglas_core::config::Catalog {
                uri: catalog_uri,
                consistency: verglas_core::config::CatalogConsistency::Eventual,
                poll_interval_secs: 30,
                include: Vec::new(),
                exclude: Vec::new(),
                credentials_file: None,
                credentials_profile: None,
                bearer_token: None,
                sigv4_region: None,
                sigv4_signing_name: None,
                warehouse: None,
            },
        }
    }

    /// A catalog opened by the write role authenticates each internal request as its caller.
    #[tokio::test]
    async fn catalog_request_carries_inherited_caller_bearer() {
        let authorization = Arc::new(Mutex::new(None));
        let app = Router::new()
            .route("/v1/config", get(catalog_response))
            .route("/v1/namespaces", get(catalog_response))
            .with_state(authorization.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind catalog");
        let endpoint = format!("http://{}", listener.local_addr().expect("catalog address"));
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve catalog");
        });

        let connection = connection_for_bearer(&config(endpoint), "caller-token".to_owned())
            .expect("connection");
        let catalog = verglas_iceberg::catalog::open_catalog(&connection)
            .await
            .expect("open catalog");
        let _namespaces = catalog
            .list_namespaces(None)
            .await
            .expect("list namespaces");

        assert_eq!(
            authorization.lock().expect("authorization").as_deref(),
            Some("Bearer caller-token")
        );
    }
}
