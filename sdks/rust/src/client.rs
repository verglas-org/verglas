//! Authenticated, streaming client for the Verglas daemon data plane.
//!
//! The client keeps transport, authentication, Arrow IPC, idempotency, and
//! table-contract validation in the SDK. Applications and the CLI therefore
//! call the same implementation instead of rebuilding HTTP behavior.

use std::collections::HashSet;
use std::pin::Pin;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_ipc::writer::StreamWriter;
use bytes::Bytes;
use futures::{SinkExt, Stream, StreamExt, TryStream, stream};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::OnceCell;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use verglas_api::table::CommitResponse;
use verglas_core::admin::{ACCESS_PATH, LocalAccess};

pub use verglas_api::{ColumnSpec, PartitionSpec, TableDefinition};

use crate::worker::ChangeEvent;

/// MIME type used by Verglas for Arrow IPC streaming requests and responses.
pub const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

/// A stream of Arrow record batches returned by a query.
pub type QueryStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, ClientError>> + Send>>;

/// A resumable stream of catalog commit notifications.
pub type FollowStream = Pin<Box<dyn Stream<Item = Result<ChangeEvent, ClientError>> + Send>>;

/// Configuration used to construct a [`Client`].
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    endpoint: String,
    token: Option<String>,
    catalog_token: Option<String>,
    catalog_uri: Option<String>,
    warehouse: Option<String>,
    s3_endpoint: Option<String>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl ConnectOptions {
    /// Creates options for a daemon endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: None,
            catalog_token: None,
            catalog_uri: None,
            warehouse: None,
            s3_endpoint: None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
        }
    }

    /// Resolves the standard SDK endpoint and bearer-token environment.
    ///
    /// `VERGLAS_ENDPOINT` defaults to the local daemon admin endpoint;
    /// `VERGLAS_TOKEN` is optional because a loopback daemon may not require
    /// bearer authentication.
    pub fn from_env() -> Self {
        let endpoint = std::env::var("VERGLAS_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:8334".to_owned());
        let mut options = Self::new(endpoint);
        options.token = nonempty_env("VERGLAS_TOKEN");
        options.catalog_token = nonempty_env("VERGLAS_CATALOG_TOKEN");
        options.catalog_uri = nonempty_env("VERGLAS_CATALOG_URI");
        options.warehouse = nonempty_env("VERGLAS_WAREHOUSE");
        options.s3_endpoint = nonempty_env("VERGLAS_S3_ENDPOINT");
        options
    }

    /// Adds the bearer token sent with every request.
    #[must_use]
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        let token = token.into();
        self.token = Some(token.clone());
        self.catalog_token = Some(token);
        self
    }

    /// Supplies the upstream Iceberg REST catalog explicitly.
    #[must_use]
    pub fn with_catalog_uri(mut self, catalog_uri: impl Into<String>) -> Self {
        self.catalog_uri = Some(catalog_uri.into());
        self
    }

    /// Supplies the Iceberg warehouse identifier explicitly.
    #[must_use]
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    /// Supplies the S3-compatible Verglas object-cache endpoint explicitly.
    #[must_use]
    pub fn with_s3_endpoint(mut self, s3_endpoint: impl Into<String>) -> Self {
        self.s3_endpoint = Some(s3_endpoint.into());
        self
    }

    /// Supplies the catalog bearer token without changing daemon authentication.
    #[must_use]
    pub fn with_catalog_token(mut self, token: impl Into<String>) -> Self {
        self.catalog_token = Some(token.into());
        self
    }

    /// Sets the TCP connection timeout.
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the timeout for receiving response headers.
    ///
    /// Streaming response bodies do not inherit this deadline.
    #[must_use]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

/// Result of ensuring that a table has an exact definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnsureTable {
    /// The table already existed with the requested definition.
    Existing,
    /// The table did not exist and was created.
    Created,
}

/// Aggregate result of appending a stream of bounded Arrow batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    /// Total rows acknowledged across all commits.
    pub rows_committed: u64,
    /// Number of commit requests made.
    pub commits: u64,
}

/// Errors returned by the Verglas data-plane client.
#[derive(Debug, Error)]
pub enum ClientError {
    /// The endpoint or HTTP client configuration is invalid.
    #[error("invalid client configuration: {0}")]
    Configuration(String),
    /// The request did not receive response headers before its deadline.
    #[error("request timed out waiting for response headers")]
    RequestTimeout,
    /// A Verglas service returned a non-success status.
    #[error("Verglas service returned HTTP {status}: {message}")]
    Http {
        /// HTTP status code.
        status: reqwest::StatusCode,
        /// Response body, retained for field-level diagnostics.
        message: String,
    },
    /// The HTTP transport failed.
    #[error("daemon transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    /// Arrow IPC encoding or decoding failed.
    #[error("Arrow IPC failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    /// The existing table differs from the requested contract.
    #[error("table {table} definition mismatch: expected {expected:?}, actual {actual:?}")]
    DefinitionMismatch {
        /// Dotted table name.
        table: String,
        /// Definition requested by the caller.
        expected: TableDefinition,
        /// Definition returned by the daemon.
        actual: TableDefinition,
    },
    /// The websocket change feed could not connect or exchanged invalid data.
    #[error("catalog change feed failed: {0}")]
    Feed(String),
    /// The requested replay cursor has aged out of feed retention.
    #[error("catalog change feed cursor expired: {reason}")]
    CursorExpired {
        /// Server-provided resynchronization reason.
        reason: String,
    },
}

/// Reusable authenticated client for a Verglas daemon.
#[derive(Debug, Clone)]
pub struct Client {
    endpoint: String,
    catalog_uri: String,
    warehouse: Option<String>,
    s3_endpoint: String,
    raw_catalog_token: Option<String>,
    daemon_token: Option<HeaderValue>,
    catalog_token: Option<HeaderValue>,
    catalog_prefix: std::sync::Arc<OnceCell<Option<String>>>,
    request_timeout: Duration,
    http: reqwest::Client,
}

impl Client {
    /// Constructs a reusable client and connection pool.
    pub async fn connect(options: ConnectOptions) -> Result<Self, ClientError> {
        let endpoint = options.endpoint.trim_end_matches('/').to_owned();
        reqwest::Url::parse(&endpoint)
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let daemon_token = options
            .token
            .clone()
            .map(|token| HeaderValue::from_str(&format!("Bearer {token}")))
            .transpose()
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let raw_catalog_token = options.catalog_token.or(options.token);
        let catalog_token = raw_catalog_token
            .clone()
            .map(|token| HeaderValue::from_str(&format!("Bearer {token}")))
            .transpose()
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let http = reqwest::Client::builder()
            .connect_timeout(options.connect_timeout)
            .build()?;
        let access = if options.catalog_uri.is_none() || options.s3_endpoint.is_none() {
            let response = tokio::time::timeout(
                options.request_timeout,
                http.get(format!("{endpoint}{ACCESS_PATH}")).send(),
            )
            .await
            .map_err(|_| ClientError::RequestTimeout)??;
            Some(response.error_for_status()?.json::<LocalAccess>().await?)
        } else {
            None
        };
        let catalog_uri = options
            .catalog_uri
            .or_else(|| access.as_ref().and_then(|value| value.catalog_uri.clone()))
            .ok_or_else(|| {
                ClientError::Configuration("daemon advertises no catalog URI".to_owned())
            })?
            .trim_end_matches('/')
            .to_owned();
        let s3_endpoint = options
            .s3_endpoint
            .or_else(|| access.as_ref().map(|value| value.s3_endpoint.clone()))
            .ok_or_else(|| {
                ClientError::Configuration("daemon advertises no S3 endpoint".to_owned())
            })?;
        Ok(Self {
            endpoint,
            catalog_uri,
            warehouse: options
                .warehouse
                .or_else(|| access.as_ref().and_then(|value| value.warehouse.clone())),
            s3_endpoint,
            raw_catalog_token,
            daemon_token,
            catalog_token,
            catalog_prefix: std::sync::Arc::new(OnceCell::new()),
            request_timeout: options.request_timeout,
            http,
        })
    }

    /// Returns the discovered upstream Iceberg REST catalog URI.
    pub fn catalog_uri(&self) -> &str {
        &self.catalog_uri
    }

    /// Returns the S3-compatible Verglas object-cache endpoint.
    pub fn s3_endpoint(&self) -> Option<&str> {
        Some(&self.s3_endpoint)
    }

    /// Creates a missing table or verifies the exact existing definition.
    pub async fn ensure_table(
        &self,
        table: &str,
        definition: &TableDefinition,
    ) -> Result<EnsureTable, ClientError> {
        let (namespace, name) = split_table_name(table)?;
        let namespace_path = namespace.join("\u{1f}");
        let table_url = self
            .catalog_url(&["namespaces", &namespace_path, "tables", name])
            .await?;
        let response = self
            .send(self.authorize_catalog(self.http.get(table_url.clone())))
            .await?;
        if response.status().is_success() {
            return self
                .verify_catalog_definition(table, definition, response)
                .await;
        }
        if response.status() != reqwest::StatusCode::NOT_FOUND {
            return Err(Self::http_error(response).await);
        }

        let namespaces_url = self.catalog_url(&["namespaces"]).await?;
        let namespace_response = self
            .send(
                self.authorize_catalog(self.http.post(namespaces_url))
                    .json(&json!({"namespace": namespace, "properties": {}})),
            )
            .await?;
        if !namespace_response.status().is_success()
            && namespace_response.status() != reqwest::StatusCode::CONFLICT
        {
            return Err(Self::http_error(namespace_response).await);
        }

        let create_url = self
            .catalog_url(&["namespaces", &namespace_path, "tables"])
            .await?;
        let response = self
            .send(
                self.authorize_catalog(self.http.post(create_url))
                    .json(&catalog_create_request(name, definition)?),
            )
            .await?;
        if response.status().is_success() {
            Ok(EnsureTable::Created)
        } else if response.status() == reqwest::StatusCode::CONFLICT {
            let response = self
                .send(self.authorize_catalog(self.http.get(table_url)))
                .await?;
            self.verify_catalog_definition(table, definition, response)
                .await
        } else {
            Err(Self::http_error(response).await)
        }
    }

    /// Appends each incoming Arrow batch as its own bounded idempotent commit.
    pub async fn append_stream<S>(
        &self,
        table: &str,
        batches: S,
        idempotency_key: &str,
    ) -> Result<AppendResult, ClientError>
    where
        S: TryStream<Ok = RecordBatch, Error = ClientError> + Send,
    {
        let mut batches = Box::pin(batches);
        let mut result = AppendResult {
            rows_committed: 0,
            commits: 0,
        };
        while let Some(batch) =
            futures::future::poll_fn(|context| batches.as_mut().try_poll_next(context))
                .await
                .transpose()?
        {
            let bytes = encode_batch(&batch)?;
            let commit_key = format!("{idempotency_key}:{}", result.commits);
            let url = self.url(&format!("/v1/write/{table}"));
            let request = self
                .authorize(self.http.post(url))
                .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
                .header("idempotency-key", commit_key)
                .body(bytes);
            let response = Self::require_success(self.send(request).await?).await?;
            let commit: CommitResponse = response.json().await?;
            result.rows_committed += commit.rows_committed;
            result.commits += 1;
        }
        Ok(result)
    }

    /// Executes SQL and incrementally decodes the Arrow IPC response.
    pub async fn query_stream(&self, sql: &str) -> Result<QueryStream, ClientError> {
        let request = self
            .authorize(self.http.post(self.url("/v1/query")))
            .header(ACCEPT, ARROW_STREAM_CONTENT_TYPE)
            .json(&QueryRequest { sql });
        let response = Self::require_success(self.send(request).await?).await?;
        let chunks = response.bytes_stream();
        let state = DecodeState {
            chunks: Box::pin(chunks),
            decoder: StreamDecoder::new(),
            buffer: Buffer::from(Vec::<u8>::new()),
            finished: false,
        };
        Ok(Box::pin(stream::try_unfold(state, decode_next)))
    }

    /// Follows commit notifications for the named tables, reconnecting from
    /// the last observed sequence after a socket drop.
    pub fn follow<I, T>(&self, tables: I, cursor: Option<u64>) -> Result<FollowStream, ClientError>
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let tables: HashSet<String> = tables.into_iter().map(Into::into).collect();
        if tables.is_empty() || tables.iter().any(String::is_empty) {
            return Err(ClientError::Configuration(
                "follow requires at least one non-empty table".to_owned(),
            ));
        }
        let state = FollowState {
            url: feed_url(&self.catalog_uri)?,
            token: self.raw_catalog_token.clone(),
            tables,
            cursor,
            socket: None,
            connected_once: false,
            reconnect_delay: None,
            backoff: Duration::from_millis(250),
        };
        Ok(Box::pin(stream::try_unfold(state, follow_next)))
    }

    /// Adds authentication to a request when configured.
    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.daemon_token {
            Some(token) => request.header(AUTHORIZATION, token.clone()),
            None => request,
        }
    }

    /// Adds catalog authentication to a direct REST request.
    fn authorize_catalog(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.catalog_token {
            Some(token) => request.header(AUTHORIZATION, token.clone()),
            None => request,
        }
    }

    /// Compares a REST load-table response with the caller's exact contract.
    async fn verify_catalog_definition(
        &self,
        table: &str,
        expected: &TableDefinition,
        response: reqwest::Response,
    ) -> Result<EnsureTable, ClientError> {
        let response = Self::require_success(response).await?;
        let value: Value = response.json().await?;
        let actual = definition_from_load_response(&value)?;
        if &actual == expected {
            Ok(EnsureTable::Existing)
        } else {
            Err(ClientError::DefinitionMismatch {
                table: table.to_owned(),
                expected: expected.clone(),
                actual,
            })
        }
    }

    /// Builds one standard Iceberg REST URL using the catalog-advertised prefix.
    async fn catalog_url(&self, suffix: &[&str]) -> Result<reqwest::Url, ClientError> {
        let prefix = self
            .catalog_prefix
            .get_or_try_init(|| self.discover_catalog_prefix())
            .await?;
        let mut url = reqwest::Url::parse(&self.catalog_uri)
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        {
            let mut segments = url.path_segments_mut().map_err(|_| {
                ClientError::Configuration("catalog URI cannot carry path segments".to_owned())
            })?;
            segments.pop_if_empty().push("v1");
            if let Some(prefix) = prefix {
                for segment in prefix.split('/').filter(|segment| !segment.is_empty()) {
                    segments.push(segment);
                }
            }
            for segment in suffix {
                segments.push(segment);
            }
        }
        Ok(url)
    }

    /// Reads the REST catalog configuration without pulling in an Iceberg engine.
    async fn discover_catalog_prefix(&self) -> Result<Option<String>, ClientError> {
        let mut url = reqwest::Url::parse(&self.catalog_uri)
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        url.path_segments_mut()
            .map_err(|_| ClientError::Configuration("invalid catalog URI".to_owned()))?
            .pop_if_empty()
            .push("v1")
            .push("config");
        if let Some(warehouse) = &self.warehouse {
            url.query_pairs_mut().append_pair("warehouse", warehouse);
        }
        let response = Self::require_success(
            self.send(self.authorize_catalog(self.http.get(url)))
                .await?,
        )
        .await?;
        let value: Value = response.json().await?;
        Ok(value
            .pointer("/overrides/prefix")
            .or_else(|| value.pointer("/defaults/prefix"))
            .and_then(Value::as_str)
            .map(str::to_owned))
    }

    /// Sends a request with a deadline only for response headers.
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, ClientError> {
        tokio::time::timeout(self.request_timeout, request.send())
            .await
            .map_err(|_| ClientError::RequestTimeout)?
            .map_err(ClientError::Transport)
    }

    /// Converts a non-success response into a diagnostic error.
    async fn require_success(
        response: reqwest::Response,
    ) -> Result<reqwest::Response, ClientError> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        Err(Self::http_error(response).await)
    }

    /// Retains a failed service response for actionable diagnostics.
    async fn http_error(response: reqwest::Response) -> ClientError {
        let status = response.status();
        let message = response.text().await.unwrap_or_default();
        ClientError::Http { status, message }
    }

    /// Joins an API path to the configured endpoint.
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.endpoint)
    }
}

#[derive(Debug, Serialize)]
struct QueryRequest<'a> {
    sql: &'a str,
}

/// Splits a dotted table identifier into namespace levels and table name.
fn split_table_name(table: &str) -> Result<(Vec<String>, &str), ClientError> {
    let (namespace, name) = table.rsplit_once('.').ok_or_else(|| {
        ClientError::Configuration(format!(
            "table `{table}` must include a namespace and table name"
        ))
    })?;
    let namespace = namespace.split('.').map(str::to_owned).collect::<Vec<_>>();
    if namespace.iter().any(String::is_empty) || name.is_empty() {
        return Err(ClientError::Configuration(format!(
            "table `{table}` contains an empty identifier"
        )));
    }
    Ok((namespace, name))
}

/// Renders the standard Iceberg REST create-table request.
fn catalog_create_request(name: &str, definition: &TableDefinition) -> Result<Value, ClientError> {
    let mut ids = std::collections::HashMap::new();
    let mut fields = Vec::with_capacity(definition.schema.len());
    for (index, column) in definition.schema.iter().enumerate() {
        let id = i32::try_from(index + 1)
            .map_err(|_| ClientError::Configuration("too many table columns".to_owned()))?;
        ids.insert(column.name.as_str(), id);
        fields.push(json!({
            "id": id,
            "name": column.name,
            "required": !column.nullable,
            "type": catalog_type(&column.type_name)?,
        }));
    }
    let mut partition_fields = Vec::with_capacity(definition.partitions.len());
    for (index, partition) in definition.partitions.iter().enumerate() {
        let source_id = ids.get(partition.source.as_str()).ok_or_else(|| {
            ClientError::Configuration(format!(
                "partition source `{}` is not a table column",
                partition.source
            ))
        })?;
        let field_id = 1000_i32
            .checked_add(
                i32::try_from(index).map_err(|_| {
                    ClientError::Configuration("too many partition fields".to_owned())
                })?,
            )
            .ok_or_else(|| ClientError::Configuration("too many partition fields".to_owned()))?;
        partition_fields.push(json!({
            "source-id": source_id,
            "field-id": field_id,
            "name": format!("{}-{}", partition.source, partition.transform),
            "transform": partition.transform,
        }));
    }
    Ok(json!({
        "name": name,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "identifier-field-ids": [],
            "fields": fields,
        },
        "partition-spec": {"spec-id": 0, "fields": partition_fields},
        "write-order": {"order-id": 0, "fields": []},
        "properties": {},
    }))
}

/// Converts the SDK's Arrow-oriented type spelling to Iceberg REST spelling.
fn catalog_type(type_name: &str) -> Result<String, ClientError> {
    let lowered = type_name.trim().to_ascii_lowercase();
    let value = match lowered.as_str() {
        "int64" | "long" => "long".to_owned(),
        "int32" | "int" => "int".to_owned(),
        "float64" | "double" => "double".to_owned(),
        "float32" | "float" => "float".to_owned(),
        "utf8" | "string" => "string".to_owned(),
        "boolean" | "bool" => "boolean".to_owned(),
        "date32" | "date" => "date".to_owned(),
        decimal if decimal.starts_with("decimal128(") && decimal.ends_with(')') => {
            format!("decimal({}", &decimal[11..])
        }
        other => {
            return Err(ClientError::Configuration(format!(
                "unsupported table column type `{other}`"
            )));
        }
    };
    Ok(value)
}

/// Extracts the exact SDK contract from a REST load-table response.
fn definition_from_load_response(value: &Value) -> Result<TableDefinition, ClientError> {
    let metadata = value.get("metadata").unwrap_or(value);
    let current_schema_id = metadata
        .get("current-schema-id")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            ClientError::Configuration("catalog metadata has no current schema".to_owned())
        })?;
    let schema = metadata
        .get("schemas")
        .and_then(Value::as_array)
        .and_then(|schemas| {
            schemas.iter().find(|schema| {
                schema.get("schema-id").and_then(Value::as_i64) == Some(current_schema_id)
            })
        })
        .ok_or_else(|| {
            ClientError::Configuration("catalog metadata omits current schema".to_owned())
        })?;
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| ClientError::Configuration("catalog schema has no fields".to_owned()))?;
    let mut names_by_id = std::collections::HashMap::new();
    let mut columns = Vec::with_capacity(fields.len());
    for field in fields {
        let id = field
            .get("id")
            .and_then(Value::as_i64)
            .ok_or_else(|| ClientError::Configuration("catalog field has no id".to_owned()))?;
        let name = field
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ClientError::Configuration("catalog field has no name".to_owned()))?;
        let type_name = sdk_type(field.get("type").ok_or_else(|| {
            ClientError::Configuration(format!("catalog field `{name}` has no type"))
        })?)?;
        names_by_id.insert(id, name.to_owned());
        columns.push(ColumnSpec {
            name: name.to_owned(),
            type_name,
            nullable: !field
                .get("required")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        });
    }
    let default_spec_id = metadata
        .get("default-spec-id")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let partition_fields = metadata
        .get("partition-specs")
        .and_then(Value::as_array)
        .and_then(|specs| {
            specs
                .iter()
                .find(|spec| spec.get("spec-id").and_then(Value::as_i64) == Some(default_spec_id))
        })
        .and_then(|spec| spec.get("fields"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut partitions = Vec::with_capacity(partition_fields.len());
    for field in partition_fields {
        let source_id = field
            .get("source-id")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                ClientError::Configuration("partition field has no source id".to_owned())
            })?;
        let source = names_by_id.get(&source_id).ok_or_else(|| {
            ClientError::Configuration(format!(
                "partition field references unknown source id {source_id}"
            ))
        })?;
        let transform = field
            .get("transform")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ClientError::Configuration("partition field has no transform".to_owned())
            })?;
        partitions.push(PartitionSpec {
            source: source.clone(),
            transform: transform.to_owned(),
        });
    }
    Ok(TableDefinition {
        schema: columns,
        partitions,
    })
}

/// Converts a primitive Iceberg REST type into the SDK's Arrow spelling.
fn sdk_type(value: &Value) -> Result<String, ClientError> {
    let value = value.as_str().ok_or_else(|| {
        ClientError::Configuration(
            "nested catalog types are not supported by TableDefinition".to_owned(),
        )
    })?;
    let lowered = value.to_ascii_lowercase();
    Ok(match lowered.as_str() {
        "long" => "int64".to_owned(),
        "int" => "int32".to_owned(),
        "double" => "float64".to_owned(),
        "float" => "float32".to_owned(),
        "string" => "utf8".to_owned(),
        "boolean" => "boolean".to_owned(),
        "date" => "date32".to_owned(),
        decimal if decimal.starts_with("decimal(") && decimal.ends_with(')') => {
            format!("decimal128({}", &decimal[8..])
        }
        other => {
            return Err(ClientError::Configuration(format!(
                "unsupported catalog column type `{other}`"
            )));
        }
    })
}

struct DecodeState {
    chunks: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    decoder: StreamDecoder,
    buffer: Buffer,
    finished: bool,
}

/// Connected websocket type used by the change feed.
type FeedSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

struct FollowState {
    url: String,
    token: Option<String>,
    tables: HashSet<String>,
    cursor: Option<u64>,
    socket: Option<FeedSocket>,
    connected_once: bool,
    reconnect_delay: Option<Duration>,
    backoff: Duration,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ServerFeedMessage {
    Hello {
        cursor: u64,
    },
    Change {
        seq: u64,
        table: String,
        snapshot_id: String,
        committed_at: String,
    },
    Resync {
        reason: String,
    },
}

/// Reads the next matching change, transparently reconnecting and resuming.
async fn follow_next(
    mut state: FollowState,
) -> Result<Option<(ChangeEvent, FollowState)>, ClientError> {
    loop {
        if state.socket.is_none() {
            if let Some(delay) = state.reconnect_delay.take() {
                tokio::time::sleep(delay).await;
            }
            match connect_feed(&state).await {
                Ok((socket, hello_cursor)) => {
                    if state.cursor.is_none() {
                        state.cursor = Some(hello_cursor);
                    }
                    state.socket = Some(socket);
                    state.connected_once = true;
                    state.backoff = Duration::from_millis(250);
                }
                Err(error) if !state.connected_once => return Err(error),
                Err(_) => {
                    state.reconnect_delay = Some(state.backoff);
                    state.backoff = (state.backoff * 2).min(Duration::from_secs(60));
                    continue;
                }
            }
        }

        let message = state
            .socket
            .as_mut()
            .expect("feed socket was connected")
            .next()
            .await;
        let text = match message {
            Some(Ok(Message::Text(text))) => text,
            Some(Ok(_)) => continue,
            Some(Err(_)) | None => {
                state.socket = None;
                state.reconnect_delay = Some(state.backoff);
                state.backoff = (state.backoff * 2).min(Duration::from_secs(60));
                continue;
            }
        };
        let message: ServerFeedMessage = match serde_json::from_str(&text) {
            Ok(message) => message,
            Err(_) => continue,
        };
        match message {
            ServerFeedMessage::Hello { .. } => continue,
            ServerFeedMessage::Resync { reason } => {
                return Err(ClientError::CursorExpired { reason });
            }
            ServerFeedMessage::Change {
                seq,
                table,
                snapshot_id,
                committed_at,
            } => {
                if state.cursor.is_some_and(|cursor| seq <= cursor) {
                    continue;
                }
                state.cursor = Some(seq);
                if !state.tables.contains(&table) {
                    continue;
                }
                return Ok(Some((
                    ChangeEvent {
                        seq,
                        table,
                        snapshot_id,
                        committed_at,
                    },
                    state,
                )));
            }
        }
    }
}

/// Opens one authenticated feed session, waits for hello, and subscribes from
/// the caller's cursor.
async fn connect_feed(state: &FollowState) -> Result<(FeedSocket, u64), ClientError> {
    let mut request = state
        .url
        .as_str()
        .into_client_request()
        .map_err(|error| ClientError::Feed(error.to_string()))?;
    if let Some(token) = &state.token {
        request.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
            tokio_tungstenite::tungstenite::http::HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|error| ClientError::Feed(error.to_string()))?,
        );
    }
    let (mut socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|error| ClientError::Feed(error.to_string()))?;
    loop {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => {
                let message: ServerFeedMessage = serde_json::from_str(&text)
                    .map_err(|error| ClientError::Feed(error.to_string()))?;
                if let ServerFeedMessage::Hello { cursor } = message {
                    let subscribe = serde_json::to_string(&serde_json::json!({
                        "type": "subscribe",
                        "cursor": state.cursor,
                    }))
                    .map_err(|error| ClientError::Feed(error.to_string()))?;
                    socket
                        .send(Message::Text(subscribe))
                        .await
                        .map_err(|error| ClientError::Feed(error.to_string()))?;
                    return Ok((socket, cursor));
                }
            }
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(ClientError::Feed(error.to_string())),
            None => return Err(ClientError::Feed("socket closed before hello".to_owned())),
        }
    }
}

/// Derives the websocket feed URL from an HTTP endpoint origin.
fn feed_url(endpoint: &str) -> Result<String, ClientError> {
    let mut url = reqwest::Url::parse(endpoint)
        .map_err(|error| ClientError::Configuration(error.to_string()))?;
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        other => {
            return Err(ClientError::Configuration(format!(
                "endpoint scheme `{other}` cannot carry a websocket feed"
            )));
        }
    };
    url.set_scheme(scheme)
        .map_err(|_| ClientError::Configuration("could not derive websocket scheme".to_owned()))?;
    url.set_path("/v1/catalog/feed");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string())
}

/// Decodes one batch while retaining decoder state across arbitrary HTTP chunks.
async fn decode_next(
    mut state: DecodeState,
) -> Result<Option<(RecordBatch, DecodeState)>, ClientError> {
    loop {
        if !state.buffer.is_empty()
            && let Some(batch) = state.decoder.decode(&mut state.buffer)?
        {
            return Ok(Some((batch, state)));
        }
        if state.finished {
            state.decoder.finish()?;
            return Ok(None);
        }
        match state.chunks.next().await {
            Some(chunk) => state.buffer = Buffer::from(chunk?.to_vec()),
            None => state.finished = true,
        }
    }
}

/// Encodes one bounded record batch as a complete Arrow IPC stream.
fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, ClientError> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(bytes)
}

/// Reads a non-empty environment value.
fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}
