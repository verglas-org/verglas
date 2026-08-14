//! SDK table routes hosted by the one self-hosted cache process.
//!
//! The routes open their Iceberg catalog lazily through this process's
//! loopback catalog gateway. That gateway is the sole provider-authentication
//! boundary, while table FileIO returns through the local S3 cache listener.

use std::io::Write as _;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use iceberg::Catalog;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::OnceCell;
use verglas_iceberg::tables_api::{self, CommitRequest};

/// Maximum bounded JSONL source upload the local Table API accepts.
const INGEST_BODY_LIMIT_BYTES: usize = 32 * 1024 * 1024;

/// Shared, lazy catalog connection for all Table requests.
#[derive(Clone)]
pub struct TableState {
    /// Fully-resolved connection to this process's local catalog and S3 gateways.
    connection: verglas_iceberg::Connection,
    /// One process-wide catalog handle; initialization happens only after the
    /// admin listener is serving the loopback `/catalog` mount.
    catalog: Arc<OnceCell<Arc<dyn Catalog>>>,
}

impl TableState {
    /// Creates state without opening the catalog before the loopback route exists.
    pub fn new(connection: verglas_iceberg::Connection) -> Self {
        Self {
            connection,
            catalog: Arc::new(OnceCell::new()),
        }
    }

    /// Opens the catalog once through the local gateway, retaining no provider credential.
    async fn catalog(&self) -> Result<&Arc<dyn Catalog>, String> {
        self.catalog
            .get_or_try_init(|| async {
                verglas_iceberg::catalog::open_catalog(&self.connection)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
    }
}

/// Mounts the stable SDK Table read, commit, and ingest routes on the local
/// admin listener. Explicit table creation is standard Iceberg REST and uses
/// the adjacent `/catalog` gateway advertised from `/admin/access`.
pub fn router(state: TableState) -> Router {
    Router::new()
        .route("/v1/tables/{name}/snapshot", get(snapshot))
        .route("/v1/tables/{name}/rows", get(rows))
        .route("/v1/tables/{name}/delta", get(delta))
        .route("/v1/tables/{name}/commit", post(commit))
        .route("/v1/ingest/{name}", post(ingest))
        .layer(axum::extract::DefaultBodyLimit::max(
            INGEST_BODY_LIMIT_BYTES,
        ))
        .with_state(state)
}

/// Pagination query for the current snapshot row route.
#[derive(Debug, Deserialize)]
struct RowsQuery {
    /// Maximum rows to return in this page.
    limit: Option<usize>,
    /// Opaque cursor returned by the preceding page.
    cursor: Option<String>,
}

/// Delta query for rows committed after a prior watermark.
#[derive(Debug, Deserialize)]
struct DeltaQuery {
    /// Opaque snapshot watermark to read after.
    since: Option<String>,
    /// Maximum rows to return.
    limit: Option<usize>,
}

/// Source-ingest query matching the current TypeScript and Rust SDK contract.
#[derive(Debug, Deserialize)]
struct IngestQuery {
    /// Create the table from the source or append to an existing table.
    mode: String,
    /// Source encoding: csv, jsonl, or parquet.
    format: String,
    /// Optional partition column for inferred create-table requests.
    partition_by: Option<String>,
}

/// Returns a local API error without leaking provider credentials.
fn table_error(error: impl std::fmt::Display) -> Response {
    (StatusCode::BAD_REQUEST, error.to_string()).into_response()
}

/// Parses one public dotted table name with the engine's canonical parser.
fn table_ident(name: &str) -> Result<iceberg::TableIdent, verglas_iceberg::AgentError> {
    verglas_iceberg::parse_table_ident(name)
}

/// Reads the current table snapshot without reading table rows.
async fn snapshot(State(state): State<TableState>, Path(name): Path<String>) -> Response {
    let ident = match table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    let catalog = match state.catalog().await {
        Ok(catalog) => catalog,
        Err(error) => return table_error(error),
    };
    match tables_api::snapshot(catalog.as_ref(), &ident).await {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => table_error(error),
    }
}

/// Reads one deterministic page of the current table snapshot.
async fn rows(
    State(state): State<TableState>,
    Path(name): Path<String>,
    Query(query): Query<RowsQuery>,
) -> Response {
    let ident = match table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    let catalog = match state.catalog().await {
        Ok(catalog) => catalog,
        Err(error) => return table_error(error),
    };
    match tables_api::rows(catalog.as_ref(), &ident, query.limit, query.cursor).await {
        Ok(rows) => Json(rows).into_response(),
        Err(error) => table_error(error),
    }
}

/// Reads rows committed after one watermark.
async fn delta(
    State(state): State<TableState>,
    Path(name): Path<String>,
    Query(query): Query<DeltaQuery>,
) -> Response {
    let ident = match table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    let catalog = match state.catalog().await {
        Ok(catalog) => catalog,
        Err(error) => return table_error(error),
    };
    match tables_api::delta(catalog.as_ref(), &ident, query.since, query.limit).await {
        Ok(delta) => Json(delta).into_response(),
        Err(error) => table_error(error),
    }
}

/// Commits JSON rows through the canonical Iceberg CAS append path.
async fn commit(
    State(state): State<TableState>,
    Path(name): Path<String>,
    Json(request): Json<CommitRequest>,
) -> Response {
    let ident = match table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    let catalog = match state.catalog().await {
        Ok(catalog) => catalog,
        Err(error) => return table_error(error),
    };
    match tables_api::commit(catalog.as_ref(), &ident, request).await {
        Ok(commit) => Json(commit).into_response(),
        Err(error) => table_error(error),
    }
}

/// Ingests a bounded source file, preserving idempotency for append requests.
async fn ingest(
    State(state): State<TableState>,
    Path(name): Path<String>,
    Query(query): Query<IngestQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let ident = match table_ident(&name) {
        Ok(ident) => ident,
        Err(error) => return table_error(error),
    };
    let catalog = match state.catalog().await {
        Ok(catalog) => catalog,
        Err(error) => return table_error(error),
    };
    let extension = match query.format.as_str() {
        "csv" | "jsonl" | "parquet" => query.format,
        other => {
            return table_error(format!(
                "unknown format `{other}`: expected csv, jsonl, or parquet"
            ));
        }
    };
    let mut source = match tempfile::Builder::new()
        .prefix("verglas-ingest-")
        .suffix(&format!(".{extension}"))
        .tempfile()
    {
        Ok(source) => source,
        Err(error) => return table_error(format!("create ingest scratch file: {error}")),
    };
    if let Err(error) = source.write_all(&body) {
        return table_error(format!("write ingest scratch file: {error}"));
    }
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let response: Result<Value, String> = match query.mode.as_str() {
        "create" => verglas_iceberg::write::create_table(
            catalog.as_ref(),
            &ident,
            source.path(),
            query.partition_by.as_deref(),
        )
        .await
        .map_err(|error| error.to_string())
        .and_then(|report| serde_json::to_value(report).map_err(|error| error.to_string())),
        "append" => match idempotency_key {
            Some(key) => {
                let ingested = match verglas_iceberg::ingest::read(source.path()) {
                    Ok(ingested) => ingested,
                    Err(error) => return table_error(error),
                };
                tables_api::commit_batches(catalog.as_ref(), &ident, ingested.batches, Some(key))
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(|report| {
                        serde_json::to_value(report).map_err(|error| error.to_string())
                    })
            }
            None => verglas_iceberg::write::append(catalog.as_ref(), &ident, source.path())
                .await
                .map_err(|error| error.to_string())
                .and_then(|report| serde_json::to_value(report).map_err(|error| error.to_string())),
        },
        other => Err(format!("unknown mode `{other}`: expected create or append")),
    };
    match response {
        Ok(response) => Json(response).into_response(),
        Err(error) => table_error(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    /// The TypeScript SDK's table read paths are mounted even though the catalog
    /// opens lazily only after the process begins serving its loopback gateway.
    #[tokio::test]
    async fn sdk_table_read_routes_are_not_missing() {
        let state = TableState::new(verglas_iceberg::Connection {
            catalog_uri: "http://127.0.0.1:9/catalog".to_owned(),
            token: None,
            warehouse: None,
            s3_endpoint: Some("http://127.0.0.1:8333".to_owned()),
            region: "us-east-1".to_owned(),
            access_key_id: Some("local".to_owned()),
            secret_access_key: Some("secret".to_owned()),
        });
        let app = router(state);
        for path in [
            "/v1/tables/sdk.events/snapshot",
            "/v1/tables/sdk.events/rows",
            "/v1/tables/sdk.events/delta",
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).expect("request"))
                .await
                .expect("response");
            assert_ne!(response.status(), StatusCode::NOT_FOUND, "missing {path}");
        }
    }
}
