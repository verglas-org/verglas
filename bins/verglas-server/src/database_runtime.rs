//! Synchronizes durable tenant database declarations into live catalog routes.
//!
//! The access service remains the durable owner. This data-plane module reads
//! only its non-secret public views and materializes one explicit Lakekeeper
//! gateway per managed lakehouse.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;
use verglas_catalog::{CatalogGateway, CatalogRuntimeRegistry, DatabaseId};
use verglas_core::config::Catalog;
use verglas_database::{CatalogRequest, DatabaseView};

const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// Failures that prevent a complete database routing snapshot from becoming live.
#[derive(Debug, Error)]
pub(crate) enum DatabaseRuntimeError {
    /// One configured service endpoint was not an absolute HTTP URL.
    #[error("invalid {name} endpoint: {detail}")]
    InvalidEndpoint {
        /// Configuration field containing the invalid endpoint.
        name: &'static str,
        /// URL parser's bounded diagnostic.
        detail: String,
    },
    /// The access service could not return the durable database inventory.
    #[error("database inventory request failed: {0}")]
    InventoryRequest(String),
    /// The access service returned a non-success response.
    #[error("database inventory returned HTTP {0}")]
    InventoryStatus(reqwest::StatusCode),
    /// The access service returned a malformed inventory document.
    #[error("database inventory response is invalid: {0}")]
    InventoryDecode(String),
    /// The scoped service-principal credential file is absent or empty.
    #[error("database inventory token file is invalid: {0}")]
    TokenFile(String),
    /// An external catalog cannot be activated without its scoped credential.
    #[error("database {0} uses an external catalog without a secret-safe data-plane binding")]
    ExternalCatalog(String),
    /// A database identifier or catalog gateway was invalid.
    #[error("database catalog runtime is invalid: {0}")]
    Catalog(String),
}

/// Collection envelope returned by the access service.
#[derive(Debug, Deserialize)]
struct DatabaseListResponse {
    /// Public non-secret definitions for this tenant.
    databases: Vec<DatabaseView>,
}

/// Pulls a complete inventory and atomically publishes its managed catalog gateways.
pub(crate) struct DatabaseCatalogSynchronizer {
    access_uri: reqwest::Url,
    access_token_file: PathBuf,
    managed_catalog_uri: reqwest::Url,
    catalogs: CatalogRuntimeRegistry,
    http: reqwest::Client,
}

impl DatabaseCatalogSynchronizer {
    /// Validates the access and managed Lakekeeper endpoints and creates a synchronizer.
    pub(crate) fn new(
        access_uri: impl AsRef<str>,
        access_token_file: impl Into<PathBuf>,
        managed_catalog_uri: impl AsRef<str>,
        catalogs: CatalogRuntimeRegistry,
    ) -> Result<Self, DatabaseRuntimeError> {
        let access_uri = parse_endpoint("VERGLAS_ACCESS_URI", access_uri.as_ref())?;
        let managed_catalog_uri =
            parse_endpoint("VERGLAS_MANAGED_CATALOG_URI", managed_catalog_uri.as_ref())?;
        Ok(Self {
            access_uri,
            access_token_file: access_token_file.into(),
            managed_catalog_uri,
            catalogs,
            http: reqwest::Client::new(),
        })
    }

    /// Builds the required Compose synchronizer when all three runtime values are present.
    pub(crate) fn from_environment(
        catalogs: CatalogRuntimeRegistry,
        required: bool,
    ) -> Result<Option<Self>, DatabaseRuntimeError> {
        let access_uri = nonempty_environment("VERGLAS_ACCESS_URI");
        let access_token_file = nonempty_environment("VERGLAS_ACCESS_TOKEN_FILE");
        let managed_catalog_uri = nonempty_environment("VERGLAS_MANAGED_CATALOG_URI");
        match (access_uri, access_token_file, managed_catalog_uri) {
            (None, None, None) if !required => Ok(None),
            (None, None, None) => Err(DatabaseRuntimeError::InvalidEndpoint {
                name: "database runtime",
                detail: "VERGLAS_ACCESS_URI, VERGLAS_ACCESS_TOKEN_FILE, and VERGLAS_MANAGED_CATALOG_URI are required in --environment mode".to_owned(),
            }),
            (Some(access_uri), Some(access_token_file), Some(managed_catalog_uri)) => Self::new(
                access_uri,
                access_token_file,
                managed_catalog_uri,
                catalogs,
            )
            .map(Some),
            _ => Err(DatabaseRuntimeError::InvalidEndpoint {
                name: "database runtime",
                detail: "VERGLAS_ACCESS_URI, VERGLAS_ACCESS_TOKEN_FILE, and VERGLAS_MANAGED_CATALOG_URI must be set together".to_owned(),
            }),
        }
    }

    /// Fetches one complete inventory and swaps its managed gateways into the registry.
    pub(crate) async fn refresh(&self) -> Result<(), DatabaseRuntimeError> {
        let token = tokio::fs::read_to_string(&self.access_token_file)
            .await
            .map_err(|error| DatabaseRuntimeError::TokenFile(error.to_string()))?;
        let token = token.trim();
        if token.is_empty() {
            return Err(DatabaseRuntimeError::TokenFile(
                "credential file is empty".to_owned(),
            ));
        }
        let inventory_uri = self.access_uri.join("v1/databases").map_err(|error| {
            DatabaseRuntimeError::InvalidEndpoint {
                name: "VERGLAS_ACCESS_URI",
                detail: error.to_string(),
            }
        })?;
        let response = self
            .http
            .get(inventory_uri)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|error| DatabaseRuntimeError::InventoryRequest(error.to_string()))?;
        if !response.status().is_success() {
            return Err(DatabaseRuntimeError::InventoryStatus(response.status()));
        }
        let inventory = response
            .json::<DatabaseListResponse>()
            .await
            .map_err(|error| DatabaseRuntimeError::InventoryDecode(error.to_string()))?;
        let mut gateways = Vec::new();
        for database in inventory.databases {
            match database {
                DatabaseView::Lakehouse {
                    name,
                    catalog: CatalogRequest::ManagedLakekeeper,
                    ..
                } => gateways.push((
                    DatabaseId::new(name.clone())
                        .map_err(|error| DatabaseRuntimeError::Catalog(error.to_string()))?,
                    self.managed_gateway(&name)?,
                )),
                DatabaseView::Lakehouse {
                    name,
                    catalog: CatalogRequest::External { .. },
                    ..
                } => return Err(DatabaseRuntimeError::ExternalCatalog(name)),
                DatabaseView::Postgres { .. } => {}
            }
        }
        self.catalogs
            .replace_all(gateways)
            .map_err(|error| DatabaseRuntimeError::Catalog(error.to_string()))
    }

    /// Continuously refreshes the live routing snapshot after a successful initial sync.
    pub(crate) fn spawn(self) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(REFRESH_INTERVAL);
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(error) = self.refresh().await {
                    eprintln!("verglas-server database catalog refresh failed: {error}");
                }
            }
        });
    }

    /// Builds one gateway to the shared Lakekeeper with a database-specific warehouse.
    fn managed_gateway(&self, database: &str) -> Result<CatalogGateway, DatabaseRuntimeError> {
        let uri = self.managed_catalog_uri.join("catalog").map_err(|error| {
            DatabaseRuntimeError::InvalidEndpoint {
                name: "VERGLAS_MANAGED_CATALOG_URI",
                detail: error.to_string(),
            }
        })?;
        let catalog = Catalog {
            consistency: verglas_core::config::CatalogConsistency::Eventual,
            uri: uri.to_string().trim_end_matches('/').to_owned(),
            poll_interval_secs: 30,
            include: Vec::new(),
            exclude: Vec::new(),
            credentials_file: None,
            credentials_profile: None,
            bearer_token: None,
            sigv4_region: None,
            sigv4_signing_name: None,
            warehouse: Some(database.to_owned()),
        };
        CatalogGateway::from_config(&catalog)
            .map_err(|error| DatabaseRuntimeError::Catalog(error.to_string()))
    }
}

/// Parses an absolute HTTP service endpoint and normalizes it for relative joins.
fn parse_endpoint(
    name: &'static str,
    endpoint: &str,
) -> Result<reqwest::Url, DatabaseRuntimeError> {
    let mut endpoint =
        reqwest::Url::parse(endpoint).map_err(|error| DatabaseRuntimeError::InvalidEndpoint {
            name,
            detail: error.to_string(),
        })?;
    if !matches!(endpoint.scheme(), "http" | "https") {
        return Err(DatabaseRuntimeError::InvalidEndpoint {
            name,
            detail: "endpoint must use http or https".to_owned(),
        });
    }
    if !endpoint.path().ends_with('/') {
        endpoint.set_path(&format!("{}/", endpoint.path()));
    }
    Ok(endpoint)
}

/// Reads one optional non-empty environment value.
fn nonempty_environment(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::body::{Body, to_bytes};
    use axum::extract::State;
    use axum::http::Request;
    use axum::routing::get;
    use axum::{Json, Router};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tower::ServiceExt;
    use verglas_catalog::CatalogRuntimeRegistry;

    use super::DatabaseCatalogSynchronizer;

    /// Serves one router on an ephemeral loopback listener.
    async fn serve(app: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        endpoint
    }

    /// Writes one scoped service-principal token without exposing it through process arguments.
    fn token_file(token: &str) -> std::path::PathBuf {
        let directory = tempfile::tempdir().expect("token tempdir").keep();
        let path = directory.join("access-token");
        std::fs::write(&path, token).expect("write token");
        path
    }

    #[tokio::test]
    async fn refresh_activates_managed_lakehouses_and_removes_deleted_routes() {
        let catalog_endpoint = serve(Router::new().route(
            "/catalog/v1/config",
            get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }),
        ))
        .await;
        let inventory = Arc::new(Mutex::new(json!({
            "databases": [
                {
                    "type": "lakehouse",
                    "name": "analytics",
                    "storage": {"mode": "managed"},
                    "catalog": {"mode": "managed-lakekeeper"}
                },
                {
                    "type": "lakehouse",
                    "name": "archive",
                    "storage": {"mode": "managed"},
                    "catalog": {"mode": "managed-lakekeeper"}
                },
                {
                    "type": "postgres",
                    "name": "operational",
                    "engine": {"mode": "managed-neon"}
                }
            ]
        })));
        let access_endpoint = serve(
            Router::new()
                .route(
                    "/v1/databases",
                    get(
                        |State(inventory): State<Arc<Mutex<serde_json::Value>>>,
                         headers: axum::http::HeaderMap| async move {
                            assert_eq!(
                                headers
                                    .get(axum::http::header::AUTHORIZATION)
                                    .and_then(|value| value.to_str().ok()),
                                Some("Bearer scoped-service-token")
                            );
                            Json(inventory.lock().expect("inventory").clone())
                        },
                    ),
                )
                .with_state(inventory.clone()),
        )
        .await;
        let catalogs = CatalogRuntimeRegistry::default();
        let synchronizer = DatabaseCatalogSynchronizer::new(
            access_endpoint,
            token_file("scoped-service-token"),
            catalog_endpoint,
            catalogs.clone(),
        )
        .expect("synchronizer");

        synchronizer.refresh().await.expect("refresh");
        let app = verglas_rest::compose_database_catalogs(Router::new(), catalogs.clone());
        let response = app
            .oneshot(
                Request::get("/v1/databases/analytics/catalog/v1/config")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert!(serde_json::from_slice::<serde_json::Value>(&body).is_ok());
        assert_eq!(catalogs.len().expect("catalog count"), 2);

        *inventory.lock().expect("inventory") = json!({
            "databases": [{
                "type": "lakehouse",
                "name": "analytics",
                "storage": {"mode": "managed"},
                "catalog": {"mode": "managed-lakekeeper"}
            }]
        });
        synchronizer.refresh().await.expect("replacement refresh");
        assert_eq!(catalogs.len().expect("catalog count"), 1);
        let removed = verglas_rest::compose_database_catalogs(Router::new(), catalogs.clone())
            .oneshot(
                Request::get("/v1/databases/archive/catalog/v1/config")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(removed.status(), axum::http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn refresh_rejects_external_catalogs_without_a_secret_safe_runtime_binding() {
        let access_endpoint = serve(Router::new().route(
            "/v1/databases",
            get(|| async {
                Json(json!({
                    "databases": [{
                        "type": "lakehouse",
                        "name": "customer",
                        "storage": {"mode": "scoped-secret", "data_path": "s3://customer/team"},
                        "catalog": {
                            "mode": "external",
                            "uri": "https://catalog.customer.example",
                            "warehouse": "customer"
                        }
                    }]
                }))
            }),
        ))
        .await;
        let synchronizer = DatabaseCatalogSynchronizer::new(
            access_endpoint,
            token_file("scoped-service-token"),
            "http://lakekeeper:8181",
            CatalogRuntimeRegistry::default(),
        )
        .expect("synchronizer");

        let error = synchronizer
            .refresh()
            .await
            .expect_err("external credential transport is not implemented");
        assert!(error.to_string().contains("customer"));
    }
}
