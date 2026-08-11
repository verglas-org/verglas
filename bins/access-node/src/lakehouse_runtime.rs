//! Managed Lakekeeper warehouse provisioning for tenant lakehouse databases.
//!
//! The access node is the only process that receives object-store credentials.
//! Lakekeeper receives them over its private management API; database discovery
//! and the Verglas data plane receive only the public database definition.

use std::path::{Path, PathBuf};

use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use verglas_database::DatabaseServiceError;

const NIL_PROJECT_ID: &str = "00000000-0000-0000-0000-000000000000";

/// Tenant-local configuration needed to provision managed Lakekeeper warehouses.
#[derive(Clone)]
pub(crate) struct LakekeeperProvisioner {
    endpoint: Url,
    bucket: String,
    key_prefix: String,
    storage_endpoint: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    caller_credential_file: PathBuf,
    http: reqwest::Client,
}

/// One warehouse returned by Lakekeeper's management API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Warehouse {
    /// Lakekeeper's immutable warehouse identifier.
    warehouse_id: String,
    /// Stable database-local warehouse name.
    name: String,
}

/// Lakekeeper warehouse collection response.
#[derive(Debug, Deserialize)]
struct WarehouseList {
    /// Every warehouse visible to the tenant Lakekeeper service.
    warehouses: Vec<Warehouse>,
}

/// Minimal server-info projection used for idempotent bootstrap.
#[derive(Debug, Deserialize)]
struct ServerInfo {
    /// Whether Lakekeeper has created its default project and server identity.
    bootstrapped: bool,
}

/// Create-warehouse request understood by Lakekeeper.
#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct CreateWarehouseRequest<'a> {
    /// Stable database-local warehouse name.
    warehouse_name: &'a str,
    /// Lakekeeper's built-in default project for this single-tenant service.
    project_id: &'static str,
    /// Database-specific managed S3 profile.
    storage_profile: S3StorageProfile,
    /// Private object-store credentials retained by Lakekeeper.
    storage_credential: S3StorageCredential<'a>,
}

/// S3-compatible storage profile embedded in a warehouse declaration.
#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct S3StorageProfile {
    /// Lakekeeper storage profile discriminator.
    r#type: &'static str,
    /// Shared managed object-store bucket.
    bucket: String,
    /// Unique database prefix inside the managed bucket.
    key_prefix: String,
    /// No cloud role is assumed in the local deployment.
    assume_role_arn: Option<String>,
    /// S3-compatible endpoint reached by Lakekeeper.
    endpoint: String,
    /// Provider region or `auto` for R2.
    region: String,
    /// Required for S3-compatible endpoints such as R2.
    path_style_access: bool,
    /// Lakekeeper's S3-compatible provider mode.
    flavor: &'static str,
    /// Local static credentials do not use STS.
    sts_enabled: bool,
}

/// Static S3 credential accepted by Lakekeeper's private management API.
#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct S3StorageCredential<'a> {
    /// Lakekeeper credential provider discriminator.
    r#type: &'static str,
    /// Lakekeeper credential shape discriminator.
    credential_type: &'static str,
    /// Managed object-store access key.
    access_key_id: &'a str,
    /// Managed object-store secret.
    secret_access_key: &'a str,
}

impl LakekeeperProvisioner {
    /// Validates configuration for one tenant's shared Lakekeeper deployment.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        endpoint: impl AsRef<str>,
        bucket: impl Into<String>,
        key_prefix: impl Into<String>,
        storage_endpoint: impl Into<String>,
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        caller_credential_file: impl Into<PathBuf>,
    ) -> Result<Self, DatabaseServiceError> {
        let mut endpoint = Url::parse(endpoint.as_ref()).map_err(provisioning_error)?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(DatabaseServiceError::Provisioning(
                "Lakekeeper endpoint must use http or https".to_owned(),
            ));
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        let provisioner = Self {
            endpoint,
            bucket: bucket.into(),
            key_prefix: key_prefix.into().trim_matches('/').to_owned(),
            storage_endpoint: storage_endpoint.into(),
            region: region.into(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            caller_credential_file: caller_credential_file.into(),
            http: reqwest::Client::new(),
        };
        if [
            provisioner.bucket.as_str(),
            provisioner.storage_endpoint.as_str(),
            provisioner.region.as_str(),
            provisioner.access_key_id.as_str(),
            provisioner.secret_access_key.as_str(),
        ]
        .into_iter()
        .any(str::is_empty)
        {
            return Err(DatabaseServiceError::Provisioning(
                "managed Lakekeeper storage configuration must not be empty".to_owned(),
            ));
        }
        Ok(provisioner)
    }

    /// Idempotently initializes the tenant Lakekeeper default project.
    pub(crate) async fn bootstrap(&self) -> Result<(), DatabaseServiceError> {
        let info_uri = self
            .endpoint
            .join("management/v1/info")
            .map_err(provisioning_error)?;
        let info = self
            .authenticated(self.http.get(info_uri))
            .await?
            .send()
            .await
            .map_err(provisioning_error)?;
        if !info.status().is_success() {
            return Err(DatabaseServiceError::Provisioning(format!(
                "Lakekeeper server info returned HTTP {}",
                info.status()
            )));
        }
        if info
            .json::<ServerInfo>()
            .await
            .map_err(provisioning_error)?
            .bootstrapped
        {
            return Ok(());
        }
        let bootstrap_uri = self
            .endpoint
            .join("management/v1/bootstrap")
            .map_err(provisioning_error)?;
        let response = self
            .authenticated(self.http.post(bootstrap_uri))
            .await?
            .json(&serde_json::json!({"accept-terms-of-use": true}))
            .send()
            .await
            .map_err(provisioning_error)?;
        if response.status().is_success() {
            return Ok(());
        }
        Err(DatabaseServiceError::Provisioning(format!(
            "Lakekeeper bootstrap returned HTTP {}",
            response.status()
        )))
    }

    /// Ensures exactly one managed warehouse exists for a database.
    pub(crate) async fn ensure_warehouse(
        &self,
        database: &str,
    ) -> Result<(), DatabaseServiceError> {
        if self.find_warehouse(database).await?.is_some() {
            return Ok(());
        }
        let request = CreateWarehouseRequest {
            warehouse_name: database,
            project_id: NIL_PROJECT_ID,
            storage_profile: S3StorageProfile {
                r#type: "s3",
                bucket: self.bucket.clone(),
                key_prefix: self.database_prefix(database),
                assume_role_arn: None,
                endpoint: self.storage_endpoint.clone(),
                region: self.region.clone(),
                path_style_access: true,
                flavor: "s3-compat",
                sts_enabled: false,
            },
            storage_credential: S3StorageCredential {
                r#type: "s3",
                credential_type: "access-key",
                access_key_id: &self.access_key_id,
                secret_access_key: &self.secret_access_key,
            },
        };
        let response = self
            .authenticated_for_database(self.http.post(self.management_uri()?), database)
            .await?
            .json(&request)
            .send()
            .await
            .map_err(provisioning_error)?;
        if response.status().is_success() {
            return Ok(());
        }
        if response.status() == StatusCode::CONFLICT
            && self.find_warehouse(database).await?.is_some()
        {
            return Ok(());
        }
        Err(DatabaseServiceError::Provisioning(format!(
            "Lakekeeper create warehouse {database} returned HTTP {}",
            response.status()
        )))
    }

    /// Deletes the database's managed warehouse before its durable declaration.
    pub(crate) async fn delete_warehouse(
        &self,
        database: &str,
    ) -> Result<(), DatabaseServiceError> {
        let Some(warehouse) = self.find_warehouse(database).await? else {
            return Ok(());
        };
        let uri = self
            .management_uri()?
            .join(&format!("warehouse/{}", warehouse.warehouse_id))
            .map_err(provisioning_error)?;
        let response = self
            .authenticated_for_database(self.http.delete(uri), database)
            .await?
            .send()
            .await
            .map_err(provisioning_error)?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        Err(DatabaseServiceError::Provisioning(format!(
            "Lakekeeper delete warehouse {database} returned HTTP {}",
            response.status()
        )))
    }

    /// Lists Lakekeeper warehouses and finds one exact database name.
    async fn find_warehouse(
        &self,
        database: &str,
    ) -> Result<Option<Warehouse>, DatabaseServiceError> {
        let response = self
            .authenticated_for_database(self.http.get(self.management_uri()?), database)
            .await?
            .send()
            .await
            .map_err(provisioning_error)?;
        if !response.status().is_success() {
            return Err(DatabaseServiceError::Provisioning(format!(
                "Lakekeeper list warehouses returned HTTP {}",
                response.status()
            )));
        }
        let warehouses = response
            .json::<WarehouseList>()
            .await
            .map_err(provisioning_error)?;
        Ok(warehouses
            .warehouses
            .into_iter()
            .find(|warehouse| warehouse.name == database))
    }

    /// Returns the warehouse collection management URI.
    fn management_uri(&self) -> Result<Url, DatabaseServiceError> {
        self.endpoint
            .join("management/v1/warehouse")
            .map_err(provisioning_error)
    }

    /// Derives a non-overlapping object prefix for one database warehouse.
    fn database_prefix(&self, database: &str) -> String {
        if self.key_prefix.is_empty() {
            format!("lakehouses/{database}")
        } else {
            format!("{}/lakehouses/{database}", self.key_prefix)
        }
    }

    /// Adds the rotating access-service caller credential required by Lakekeeper.
    async fn authenticated(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, DatabaseServiceError> {
        let token = read_credential(&self.caller_credential_file).await?;
        Ok(request.bearer_auth(token))
    }

    /// Adds caller authorization and the trusted database identity used for resource sync.
    async fn authenticated_for_database(
        &self,
        request: reqwest::RequestBuilder,
        database: &str,
    ) -> Result<reqwest::RequestBuilder, DatabaseServiceError> {
        Ok(self
            .authenticated(request)
            .await?
            .header("x-verglas-database-id", database))
    }
}

/// Reads a rotated bearer without retaining an obsolete credential in memory.
async fn read_credential(path: &Path) -> Result<String, DatabaseServiceError> {
    let token = tokio::fs::read_to_string(path)
        .await
        .map_err(provisioning_error)?;
    let token = token.trim();
    if token.is_empty() {
        return Err(DatabaseServiceError::Provisioning(
            "Lakekeeper caller credential is empty".to_owned(),
        ));
    }
    Ok(token.to_owned())
}

/// Removes transport internals and credential-bearing values from public failures.
fn provisioning_error(error: impl std::fmt::Display) -> DatabaseServiceError {
    DatabaseServiceError::Provisioning(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{delete, get, post};
    use axum::{Json, Router};
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::LakekeeperProvisioner;

    /// Shared request state for the Lakekeeper management stub.
    #[derive(Clone, Default)]
    struct StubState {
        requests: Arc<Mutex<Vec<(Value, String, String)>>>,
    }

    /// Serves a deterministic Lakekeeper management stub.
    async fn serve(state: StubState) -> String {
        let app = Router::new()
            .route(
                "/management/v1/warehouse",
                get(|| async { Json(json!({"warehouses": []})) }).post(capture_create),
            )
            .route(
                "/management/v1/warehouse/{id}",
                delete(|Path(_id): Path<String>| async { StatusCode::NO_CONTENT }),
            )
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        endpoint
    }

    /// Captures one create request without returning credential material.
    async fn capture_create(
        State(state): State<StubState>,
        headers: HeaderMap,
        Json(request): Json<Value>,
    ) -> StatusCode {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let database = headers
            .get("x-verglas-database-id")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        state
            .requests
            .lock()
            .expect("requests")
            .push((request, authorization, database));
        StatusCode::CREATED
    }

    #[tokio::test]
    async fn managed_warehouse_uses_database_specific_prefix_and_private_credentials() {
        let state = StubState::default();
        let endpoint = serve(state.clone()).await;
        let credential = tempfile::NamedTempFile::new().expect("credential");
        std::fs::write(credential.path(), "caller-token").expect("write credential");
        let provisioner = LakekeeperProvisioner::new(
            endpoint,
            "managed",
            "tenant-a",
            "http://object-store:8333",
            "auto",
            "access",
            "secret",
            credential.path(),
        )
        .expect("provisioner");

        provisioner
            .ensure_warehouse("analytics")
            .await
            .expect("warehouse");

        let requests = state.requests.lock().expect("requests");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0["warehouse-name"], "analytics");
        assert_eq!(
            requests[0].0["storage-profile"]["key-prefix"],
            "tenant-a/lakehouses/analytics"
        );
        assert_eq!(
            requests[0].0["storage-credential"]["secret-access-key"],
            "secret"
        );
        assert_eq!(requests[0].1, "Bearer caller-token");
        assert_eq!(requests[0].2, "analytics");
    }

    #[tokio::test]
    async fn bootstrap_reads_server_info_before_attempting_initialization() {
        let app = Router::new()
            .route(
                "/management/v1/info",
                get(|| async { Json(json!({"bootstrapped": true})) }),
            )
            .route(
                "/management/v1/bootstrap",
                post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        let credential = tempfile::NamedTempFile::new().expect("credential");
        std::fs::write(credential.path(), "caller-token").expect("write credential");
        let provisioner = LakekeeperProvisioner::new(
            endpoint,
            "managed",
            "tenant-a",
            "http://object-store:8333",
            "auto",
            "access",
            "secret",
            credential.path(),
        )
        .expect("provisioner");

        provisioner.bootstrap().await.expect("already bootstrapped");
    }
}
