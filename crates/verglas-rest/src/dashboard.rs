//! Rill project-resource management for on-prem Iceberg dashboards.
//!
//! Verglas resolves a table through its configured catalog and sends Rill
//! project files over Rill's runtime API. Rill owns its filesystem; no shared
//! volume or local-file fallback exists.

#[cfg(test)]
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
#[cfg(test)]
use std::sync::Arc;

use iceberg::spec::{PrimitiveType, Type};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use verglas_core::config::Rill;
use verglas_iceberg::parse_table_ident;

use crate::admin::TablesSlot;

/// Marker placed at the top of every Rill resource owned by Verglas.
const OWNED_MARKER: &str = "# managed-by: verglas";
/// Shared connector resource used by every generated model.
const CONNECTOR_PATH: &str = "connectors/verglas.yaml";

/// Request body for `POST /v1/dashboards`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateDashboardRequest {
    /// Dotted Iceberg table identifier.
    pub table: String,
    /// Optional stable dashboard name. The table-derived name is used when absent.
    #[serde(default)]
    pub name: Option<String>,
}

/// Dashboard information returned by the on-prem REST API.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DashboardInfo {
    /// Stable Rill resource name.
    pub name: String,
    /// Dotted Iceberg table identifier backing the dashboard.
    pub table: String,
    /// Browser-facing Rill Explore URL.
    pub url: String,
}

/// Response body for `GET /v1/dashboards`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DashboardList {
    /// Verglas-owned dashboards discovered in Rill.
    pub dashboards: Vec<DashboardInfo>,
}

/// Response body after deleting a dashboard.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DashboardDeleted {
    /// Name whose owned Rill resources were removed.
    pub deleted: String,
}

/// Errors from catalog resolution or Rill resource management.
#[derive(Debug, Error)]
pub enum DashboardError {
    /// The user supplied an invalid table or dashboard identifier.
    #[error("invalid dashboard request: {0}")]
    Invalid(String),
    /// The table could not be resolved through Iceberg.
    #[error("catalog table lookup failed: {0}")]
    Catalog(String),
    /// Rill rejected or failed a runtime API operation.
    #[error("Rill runtime request failed: {0}")]
    Rill(String),
    /// A resource exists at the generated path but Verglas does not own it.
    #[error("Rill resource `{0}` already exists and is not managed by Verglas")]
    Ownership(String),
    /// The named Verglas dashboard does not exist.
    #[error("dashboard `{0}` was not found")]
    NotFound(String),
}

/// Credentials and endpoint Rill uses to read Iceberg bytes through Verglas.
#[derive(Debug, Clone)]
pub struct RillStorage {
    /// Network-visible S3 endpoint inside the deployment.
    pub endpoint: String,
    /// SigV4 region.
    pub region: String,
    /// S3 access key accepted by Verglas.
    pub access_key_id: String,
    /// S3 secret accepted by Verglas.
    pub secret_access_key: String,
}

/// Runtime backing the optional dashboard REST routes.
#[derive(Clone)]
pub struct DashboardRuntime {
    tables: TablesSlot,
    rill: RillClient,
    storage: RillStorage,
    browser_uri: String,
    #[cfg(test)]
    recorder: Option<TestRecorder>,
}

impl DashboardRuntime {
    /// Builds a runtime from validated server configuration and resolved S3 credentials.
    pub fn new(
        tables: TablesSlot,
        config: &Rill,
        storage: RillStorage,
    ) -> Result<Self, DashboardError> {
        Ok(Self {
            tables,
            rill: RillClient::new(&config.uri, &config.instance_id)?,
            storage,
            browser_uri: config.browser_uri.trim_end_matches('/').to_owned(),
            #[cfg(test)]
            recorder: None,
        })
    }

    /// Creates or refreshes the Verglas-owned Rill resources for one table.
    pub async fn create(
        &self,
        request: CreateDashboardRequest,
    ) -> Result<DashboardInfo, DashboardError> {
        let ident = parse_table_ident(&request.table)
            .map_err(|error| DashboardError::Invalid(error.to_string()))?;
        let name = match request.name {
            Some(name) => validate_name(&name)?,
            None => resource_name(&request.table),
        };
        let catalog = self.tables.get().ok_or_else(|| {
            DashboardError::Catalog("cache engine is still recovering".to_owned())
        })?;
        let table = catalog
            .load_table(&ident)
            .await
            .map_err(|error| DashboardError::Catalog(error.to_string()))?;
        let metadata_location = table.metadata_location().ok_or_else(|| {
            DashboardError::Catalog(format!(
                "table `{}` has no catalog metadata location",
                request.table
            ))
        })?;

        self.ensure_project().await?;
        for directory in ["connectors", "models", "metrics", "dashboards"] {
            self.rill.create_directory(directory).await?;
        }

        self.put_owned(CONNECTOR_PATH, &connector_yaml(&self.storage), "connector")
            .await?;
        self.put_owned(
            &model_path(&name),
            &model_yaml(&request.table, metadata_location),
            &request.table,
        )
        .await?;
        self.put_owned(
            &metrics_path(&name),
            &metrics_yaml(&request.table, &name, table.metadata().current_schema()),
            &request.table,
        )
        .await?;
        self.put_owned(
            &dashboard_path(&name),
            &explore_yaml(&request.table, &name),
            &request.table,
        )
        .await?;
        Ok(self.info(name, request.table))
    }

    /// Bootstraps a new Rill project without modifying an existing project file.
    async fn ensure_project(&self) -> Result<(), DashboardError> {
        if self.rill.get_optional_file("rill.yaml").await?.is_none() {
            self.rill
                .put_file(
                    "rill.yaml",
                    "# managed-by: verglas\ncompiler: rillv1\ndisplay_name: Verglas analytics\nolap_connector: duckdb\n",
                    true,
                )
                .await?;
        }
        Ok(())
    }

    /// Lists Verglas-owned Explore resources from Rill.
    pub async fn list(&self) -> Result<DashboardList, DashboardError> {
        let mut dashboards = Vec::new();
        for path in self.rill.list_files("dashboards/*.yaml").await? {
            let Some(name) = path
                .strip_prefix("dashboards/")
                .and_then(|path| path.strip_suffix(".yaml"))
            else {
                continue;
            };
            let blob = self.rill.get_file(&path).await?;
            if let Some(table) = owned_table(&blob) {
                dashboards.push(self.info(name.to_owned(), table));
            }
        }
        dashboards.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(DashboardList { dashboards })
    }

    /// Reads one Verglas-owned dashboard from Rill.
    pub async fn show(&self, name: &str) -> Result<DashboardInfo, DashboardError> {
        let name = validate_name(name)?;
        let path = dashboard_path(&name);
        let blob = self
            .rill
            .get_optional_file(&path)
            .await?
            .ok_or_else(|| DashboardError::NotFound(name.clone()))?;
        let table = owned_table(&blob).ok_or(DashboardError::Ownership(path))?;
        Ok(self.info(name, table))
    }

    /// Deletes the three table-specific resources for a Verglas-owned dashboard.
    pub async fn delete(&self, name: &str) -> Result<DashboardDeleted, DashboardError> {
        let info = self.show(name).await?;
        let paths = [
            dashboard_path(&info.name),
            metrics_path(&info.name),
            model_path(&info.name),
        ];
        for path in &paths {
            let blob = self
                .rill
                .get_optional_file(path)
                .await?
                .ok_or_else(|| DashboardError::NotFound(info.name.clone()))?;
            if owned_table(&blob).as_deref() != Some(info.table.as_str()) {
                return Err(DashboardError::Ownership(path.clone()));
            }
        }
        for path in paths {
            self.rill.delete_file(&path).await?;
        }
        Ok(DashboardDeleted { deleted: info.name })
    }

    /// Creates a resource or refreshes it only after verifying Verglas ownership.
    async fn put_owned(&self, path: &str, blob: &str, owner: &str) -> Result<(), DashboardError> {
        match self.rill.get_optional_file(path).await? {
            Some(existing)
                if existing.starts_with(OWNED_MARKER)
                    && owned_table(&existing).as_deref() == Some(owner) =>
            {
                self.rill.put_file(path, blob, false).await
            }
            Some(_) => Err(DashboardError::Ownership(path.to_owned())),
            None => self.rill.put_file(path, blob, true).await,
        }
    }

    /// Builds the stable API response for a dashboard.
    fn info(&self, name: String, table: String) -> DashboardInfo {
        DashboardInfo {
            url: format!("{}/explore/{name}", self.browser_uri),
            name,
            table,
        }
    }

    /// Returns the test-only Rill file recorder.
    #[cfg(test)]
    pub(crate) fn test_recorder(&self) -> TestRecorder {
        self.recorder.clone().expect("test runtime has a recorder")
    }
}

/// Minimal client for Rill's local runtime project-file API.
#[derive(Clone)]
struct RillClient {
    base: Url,
    instance_id: String,
    http: reqwest::Client,
}

#[derive(Deserialize)]
struct RillFile {
    blob: String,
}

#[derive(Deserialize)]
struct RillFiles {
    files: Vec<RillEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RillEntry {
    path: String,
    is_dir: bool,
}

impl RillClient {
    /// Builds a client and validates the configured base URI.
    fn new(uri: &str, instance_id: &str) -> Result<Self, DashboardError> {
        let base = Url::parse(&format!("{}/", uri.trim_end_matches('/')))
            .map_err(|error| DashboardError::Invalid(format!("analytics.rill.uri: {error}")))?;
        Ok(Self {
            base,
            instance_id: instance_id.to_owned(),
            http: reqwest::Client::new(),
        })
    }

    /// Resolves a Rill runtime API path against the configured base URI.
    fn url(&self, suffix: &str) -> Result<Url, DashboardError> {
        self.base
            .join(&format!("v1/instances/{}/{suffix}", self.instance_id))
            .map_err(|error| DashboardError::Rill(error.to_string()))
    }

    /// Lists file paths matching a Rill repository glob.
    async fn list_files(&self, glob: &str) -> Result<Vec<String>, DashboardError> {
        let mut url = self.url("files")?;
        url.query_pairs_mut().append_pair("glob", glob);
        let response = self.http.get(url).send().await.map_err(rill_transport)?;
        let response = rill_success(response).await?;
        let files: RillFiles = response
            .json()
            .await
            .map_err(|error| DashboardError::Rill(error.to_string()))?;
        Ok(files
            .files
            .into_iter()
            .filter(|entry| !entry.is_dir)
            .map(|entry| entry.path.trim_start_matches('/').to_owned())
            .collect())
    }

    /// Ensures a Rill repository directory exists before writing resources.
    async fn create_directory(&self, path: &str) -> Result<(), DashboardError> {
        let response = self
            .http
            .post(self.url("files/dir")?)
            .json(&serde_json::json!({
                "instanceId": self.instance_id,
                "path": path,
            }))
            .send()
            .await
            .map_err(rill_transport)?;
        rill_success(response).await?;
        Ok(())
    }

    /// Reads one file, preserving not-found as `None`.
    async fn get_optional_file(&self, path: &str) -> Result<Option<String>, DashboardError> {
        if !self
            .list_files(path)
            .await?
            .iter()
            .any(|candidate| candidate == path)
        {
            return Ok(None);
        }
        let mut url = self.url("files/entry")?;
        url.query_pairs_mut().append_pair("path", path);
        let response = self.http.get(url).send().await.map_err(rill_transport)?;
        let response = rill_success(response).await?;
        let file: RillFile = response
            .json()
            .await
            .map_err(|error| DashboardError::Rill(error.to_string()))?;
        Ok(Some(file.blob))
    }

    /// Reads one required file.
    async fn get_file(&self, path: &str) -> Result<String, DashboardError> {
        self.get_optional_file(path)
            .await?
            .ok_or_else(|| DashboardError::NotFound(path.to_owned()))
    }

    /// Creates or updates one Rill project file.
    async fn put_file(
        &self,
        path: &str,
        blob: &str,
        create_only: bool,
    ) -> Result<(), DashboardError> {
        let response = self
            .http
            .post(self.url("files/entry")?)
            .json(&serde_json::json!({
                "instanceId": self.instance_id,
                "path": path,
                "blob": blob,
                "create": true,
                "createOnly": create_only,
            }))
            .send()
            .await
            .map_err(rill_transport)?;
        rill_success(response).await?;
        Ok(())
    }

    /// Deletes one Rill project file.
    async fn delete_file(&self, path: &str) -> Result<(), DashboardError> {
        let mut url = self.url("files/entry")?;
        url.query_pairs_mut()
            .append_pair("path", path)
            .append_pair("force", "true");
        let response = self.http.delete(url).send().await.map_err(rill_transport)?;
        rill_success(response).await?;
        Ok(())
    }
}

/// Maps a transport failure into the dashboard error contract.
fn rill_transport(error: reqwest::Error) -> DashboardError {
    DashboardError::Rill(error.to_string())
}

/// Preserves Rill's status and response text on failures.
async fn rill_success(response: reqwest::Response) -> Result<reqwest::Response, DashboardError> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    Err(DashboardError::Rill(format!("HTTP {status}: {body}")))
}

/// Converts a dotted table identifier to a Rill-safe resource name.
fn resource_name(table: &str) -> String {
    table
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// Validates an explicit Rill resource name.
fn validate_name(name: &str) -> Result<String, DashboardError> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(DashboardError::Invalid(
            "dashboard name must contain only ASCII letters, digits, or `_`".to_owned(),
        ));
    }
    Ok(name.to_ascii_lowercase())
}

/// Extracts the source table from a Verglas-owned Rill resource.
fn owned_table(blob: &str) -> Option<String> {
    if !blob.starts_with(OWNED_MARKER) {
        return None;
    }
    blob.lines()
        .find_map(|line| line.strip_prefix("# verglas-table: ").map(str::to_owned))
}

/// Quotes a string as a JSON scalar, which is valid YAML.
fn yaml_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned())
}

/// Renders the shared Rill S3 connector without exposing it through Verglas APIs.
fn connector_yaml(storage: &RillStorage) -> String {
    format!(
        "{OWNED_MARKER}\n# verglas-table: connector\ntype: connector\ndriver: s3\nendpoint: {}\nregion: {}\naws_access_key_id: {}\naws_secret_access_key: {}\n",
        yaml_string(&storage.endpoint),
        yaml_string(&storage.region),
        yaml_string(&storage.access_key_id),
        yaml_string(&storage.secret_access_key),
    )
}

/// Renders a direct Iceberg model whose object reads use the Verglas connector.
fn model_yaml(table: &str, metadata_location: &str) -> String {
    let sql_location = metadata_location.replace('\'', "''");
    format!(
        "{OWNED_MARKER}\n# verglas-table: {table}\ntype: model\nconnector: duckdb\ncreate_secrets_from_connectors: verglas\nmaterialize: true\nsql: |\n  SELECT *\n  FROM iceberg_scan('{sql_location}')\n"
    )
}

/// Renders a useful baseline metrics view from the Iceberg schema.
fn metrics_yaml(table: &str, name: &str, schema: &iceberg::spec::Schema) -> String {
    let mut dimensions = String::new();
    let mut measures = String::from(
        "measures:\n  - name: record_count\n    display_name: Record count\n    expression: COUNT(*)\n",
    );
    let mut timeseries = None;
    for field in schema.as_struct().fields() {
        let column = yaml_string(&field.name);
        let field_name = resource_name(&field.name);
        if let Type::Primitive(primitive) = field.field_type.as_ref() {
            dimensions.push_str(&format!(
                "  - name: {field_name}\n    display_name: {column}\n    column: {column}\n"
            ));
            if matches!(
                primitive,
                PrimitiveType::Date | PrimitiveType::Timestamp | PrimitiveType::Timestamptz
            ) && timeseries.is_none()
            {
                timeseries = Some(column.clone());
            }
            if matches!(
                primitive,
                PrimitiveType::Int
                    | PrimitiveType::Long
                    | PrimitiveType::Float
                    | PrimitiveType::Double
                    | PrimitiveType::Decimal { .. }
            ) {
                let sql_column = field.name.replace('"', "\"\"");
                measures.push_str(&format!(
                    "  - name: sum_{field_name}\n    display_name: {}\n    expression: SUM(\"{sql_column}\")\n",
                    yaml_string(&format!("Sum of {}", field.name))
                ));
            }
        }
    }
    let timeseries = timeseries
        .map(|column| format!("timeseries: {column}\n"))
        .unwrap_or_default();
    let dimensions = if dimensions.is_empty() {
        String::new()
    } else {
        format!("dimensions:\n{dimensions}")
    };
    format!(
        "{OWNED_MARKER}\n# verglas-table: {table}\nversion: 1\ntype: metrics_view\nmodel: {name}\n{timeseries}{dimensions}{measures}"
    )
}

/// Renders an Explore dashboard exposing every generated dimension and measure.
fn explore_yaml(table: &str, name: &str) -> String {
    format!(
        "{OWNED_MARKER}\n# verglas-table: {table}\ntype: explore\ntitle: {}\nmetrics_view: {name}\ndimensions: '*'\nmeasures: '*'\n",
        yaml_string(&format!("{table} dashboard"))
    )
}

/// Returns the Rill model resource path.
fn model_path(name: &str) -> String {
    format!("models/{name}.yaml")
}

/// Returns the Rill metrics resource path.
fn metrics_path(name: &str) -> String {
    format!("metrics/{name}.yaml")
}

/// Returns the Rill Explore resource path.
fn dashboard_path(name: &str) -> String {
    format!("dashboards/{name}.yaml")
}

/// In-memory view of files received by the test Rill runtime.
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct TestRecorder {
    files: Arc<std::sync::Mutex<BTreeMap<String, String>>>,
    directories: Arc<std::sync::Mutex<BTreeSet<String>>>,
}

#[cfg(test)]
impl TestRecorder {
    /// Returns a snapshot of recorded project files.
    pub(crate) fn files(&self) -> BTreeMap<String, String> {
        self.files.lock().expect("Rill recorder lock").clone()
    }

    /// Inserts a project file to arrange ownership-collision tests.
    pub(crate) fn insert(&self, path: &str, blob: &str) {
        self.files
            .lock()
            .expect("Rill recorder lock")
            .insert(path.to_owned(), blob.to_owned());
    }
}

/// Starts a test Rill file API and returns a dashboard runtime using it.
#[cfg(test)]
pub(crate) async fn test_runtime(tables: TablesSlot) -> DashboardRuntime {
    use axum::extract::{Query, State};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use axum::{Json, Router};

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Put {
        path: String,
        blob: String,
    }
    let recorder = TestRecorder::default();
    let app = Router::new()
        .route(
            "/v1/instances/default/files/entry",
            get(
                |State(recorder): State<TestRecorder>,
                 Query(query): Query<BTreeMap<String, String>>| async move {
                    let Some(path) = query.get("path") else {
                        return (axum::http::StatusCode::BAD_REQUEST, "missing path")
                            .into_response();
                    };
                    match recorder.files.lock().expect("lock").get(path).cloned() {
                        Some(blob) => Json(serde_json::json!({"blob": blob})).into_response(),
                        None => (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            "open file: no such file or directory",
                        )
                            .into_response(),
                    }
                },
            )
            .post(
                |State(recorder): State<TestRecorder>, Json(request): Json<Put>| async move {
                    let parent = request.path.split_once('/').map(|(parent, _)| parent);
                    if parent.is_some_and(|parent| {
                        !recorder.directories.lock().expect("lock").contains(parent)
                    }) {
                        return (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            "parent directory does not exist",
                        )
                            .into_response();
                    }
                    recorder
                        .files
                        .lock()
                        .expect("lock")
                        .insert(request.path, request.blob);
                    Json(serde_json::json!({})).into_response()
                },
            )
            .delete(
                |State(recorder): State<TestRecorder>,
                 Query(query): Query<BTreeMap<String, String>>| async move {
                    if let Some(path) = query.get("path") {
                        recorder.files.lock().expect("lock").remove(path);
                    }
                    Json(serde_json::json!({})).into_response()
                },
            ),
        )
        .route(
            "/v1/instances/default/files/dir",
            axum::routing::post(
                |State(recorder): State<TestRecorder>,
                 Json(request): Json<BTreeMap<String, String>>| async move {
                    if let Some(path) = request.get("path") {
                        recorder
                            .directories
                            .lock()
                            .expect("lock")
                            .insert(path.clone());
                    }
                    Json(serde_json::json!({})).into_response()
                },
            ),
        )
        .route(
            "/v1/instances/default/files",
            get(
                |State(recorder): State<TestRecorder>,
                 Query(query): Query<BTreeMap<String, String>>| async move {
                    let glob = query.get("glob").cloned().unwrap_or_else(|| "*".to_owned());
                    let files = recorder
                        .files
                        .lock()
                        .expect("lock")
                        .keys()
                        .filter(|path| {
                            glob == "*"
                                || glob.strip_suffix("*.yaml").is_some_and(|prefix| {
                                    path.starts_with(prefix) && path.ends_with(".yaml")
                                })
                                || path.as_str() == glob
                        })
                        .map(|path| serde_json::json!({"path": path, "isDir": false}))
                        .collect::<Vec<_>>();
                    Json(serde_json::json!({"files": files}))
                },
            ),
        )
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test Rill");
    let uri = format!("http://{}", listener.local_addr().expect("Rill address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve test Rill") });
    let config = Rill {
        uri,
        instance_id: "default".to_owned(),
        browser_uri: "http://127.0.0.1:9009".to_owned(),
        s3_uri: "http://verglas-server:8333".to_owned(),
    };
    let mut runtime = DashboardRuntime::new(
        tables,
        &config,
        RillStorage {
            endpoint: config.s3_uri.clone(),
            region: "us-east-1".to_owned(),
            access_key_id: "test".to_owned(),
            secret_access_key: "secret".to_owned(),
        },
    )
    .expect("test dashboard runtime");
    runtime.recorder = Some(recorder);
    runtime
}
