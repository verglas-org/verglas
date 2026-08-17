//! Authenticated, streaming client for the Verglas server data plane.
//!
//! The client keeps transport, authentication, Arrow IPC, idempotency, and
//! table-contract validation in the SDK. Applications and the CLI therefore
//! call the same implementation instead of rebuilding HTTP behavior.
//!
//! [`DataClient`] is the separate /v0 client: the ONE append-ingest shape
//! (NDJSON to `/v0/events`) for table writes, vector writes, and log shipping
//! alike, plus SQL through `/v0/sql`. The retired tenant-local access-service
//! era (the old v1 access-token principal, resource, grant, and token CRUD
//! routes) has no client here — it was deleted with the surface it called.

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::OnceCell;
use verglas_core::admin::{ACCESS_PATH, LocalAccess};

pub use verglas_api::{ColumnSpec, PartitionSpec, TableDefinition};

use crate::worker::ChangeEvent;

/// One durable table commit and the queue receipt that fences its acknowledgement.
/// Opaque proof that a subscriber owns one delivery generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryReceipt {
    /// Stable delivery position.
    pub position: i64,
    /// Consumer process that owns the lease.
    pub owner: String,
    /// Monotonic generation fencing prior deliveries.
    pub generation: u64,
}

/// One NDJSON frame on a database subscription stream.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeliveryFrame {
    /// Stable delivery position.
    position: i64,
    /// Envelope payload carrying the table-change notification.
    payload: serde_json::Value,
    /// Receipt required for acknowledgement.
    receipt: DeliveryReceipt,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableChangeDelivery {
    /// Committed table snapshot notification.
    pub change: ChangeEvent,
    /// Receipt acknowledged after the subscriber closes its durable read frontier.
    pub receipt: DeliveryReceipt,
}

/// Connection lifecycle and durable deliveries from a database subscription.
#[derive(Debug, Clone, PartialEq)]
pub enum TableSubscriptionEvent {
    /// The authenticated push response is established.
    Connected,
    /// The transport dropped and the SDK is reconnecting from queue state.
    Disconnected,
    /// One fenced table commit delivery.
    Delivery(TableChangeDelivery),
}

/// A reconnecting push-only stream of acknowledged table commits.
pub type TableChangeStream =
    Pin<Box<dyn Stream<Item = Result<TableSubscriptionEvent, ClientError>> + Send>>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TableChangePayload {
    table: String,
    snapshot_id: String,
    committed_at: String,
}

/// A stream of reflected Integration method results decoded from NDJSON.
pub type NamespaceStream<T> = Pin<Box<dyn Stream<Item = Result<T, ClientError>> + Send>>;

/// Whether a reflected Integration method is bounded, mutating, or streaming.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NamespaceMethodMode {
    /// Bounded observation with no declared external mutation.
    Read,
    /// Bounded operation that may mutate the external system.
    Write,
    /// Long-lived sequence of output values.
    Stream,
}

/// One callable operation published by an Integration namespace.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct NamespaceMethodManifest {
    /// Human-readable operation purpose available to agents and management UIs.
    pub description: String,
    /// Execution and authorization behavior.
    pub mode: NamespaceMethodMode,
    /// JSON Schema for the single method argument.
    pub input: Value,
    /// JSON Schema for a bounded result or each streamed item.
    pub output: Value,
}

/// Reflection document through which one Integration composes into every SDK.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct NamespaceManifest {
    /// Stable SDK namespace owned by the Integration.
    pub namespace: String,
    /// User-facing Integration title.
    pub title: String,
    /// User-facing Integration purpose.
    pub description: String,
    /// Dot-separated method paths and their machine-readable contracts.
    pub methods: BTreeMap<String, NamespaceMethodManifest>,
}

/// Configuration used to construct a [`Client`].
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    endpoint: String,
    token: Option<String>,
    query_uri: Option<String>,
    access_uri: Option<String>,
    s3_endpoint: Option<String>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl ConnectOptions {
    /// Creates options for a server endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: None,
            query_uri: None,
            access_uri: None,
            s3_endpoint: None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(120),
        }
    }

    /// Resolves the standard SDK endpoint and bearer-token environment.
    ///
    /// `VERGLAS_ENDPOINT` defaults to the local server admin endpoint;
    /// `VERGLAS_TOKEN` is optional because a loopback server may not require
    /// bearer authentication.
    pub fn from_env() -> Self {
        let endpoint = std::env::var("VERGLAS_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:8334".to_owned());
        let mut options = Self::new(endpoint);
        options.token = nonempty_env("VERGLAS_TOKEN");
        options.query_uri = nonempty_env("VERGLAS_QUERY_URI");
        options.access_uri = nonempty_env("VERGLAS_ACCESS_URI");
        options.s3_endpoint = nonempty_env("VERGLAS_S3_ENDPOINT");
        options
    }

    /// Adds the bearer token sent with every request.
    #[must_use]
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Supplies the query and write API base URL explicitly.
    #[must_use]
    pub fn with_query_uri(mut self, query_uri: impl Into<String>) -> Self {
        self.query_uri = Some(query_uri.into());
        self
    }

    /// Supplies the standalone authorization API base URL explicitly.
    #[must_use]
    pub fn with_access_uri(mut self, access_uri: impl Into<String>) -> Self {
        self.access_uri = Some(access_uri.into());
        self
    }

    /// Supplies the S3-compatible Verglas object-cache endpoint explicitly.
    #[must_use]
    pub fn with_s3_endpoint(mut self, s3_endpoint: impl Into<String>) -> Self {
        self.s3_endpoint = Some(s3_endpoint.into());
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
    #[error("server transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    /// Arrow IPC encoding or decoding failed.
    #[error("Arrow IPC failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    /// A reflected namespace response was not valid JSON.
    #[error("namespace JSON failed: {0}")]
    NamespaceJson(#[from] serde_json::Error),
    /// A subscription delivery frame was not valid NDJSON.
    #[error("subscription delivery JSON failed: {0}")]
    DeliveryJson(serde_json::Error),
    /// The existing table differs from the requested contract.
    #[error("table {table} definition mismatch: expected {expected:?}, actual {actual:?}")]
    DefinitionMismatch {
        /// Dotted table name.
        table: String,
        /// Definition requested by the caller.
        expected: TableDefinition,
        /// Definition returned by the server.
        actual: TableDefinition,
    },
}

/// Reusable authenticated client for a Verglas server.
#[derive(Debug, Clone)]
pub struct Client {
    query_uri: String,
    access_uri: String,
    s3_endpoint: String,
    server_token: Option<HeaderValue>,
    request_timeout: Duration,
    http: reqwest::Client,
}

impl Client {
    /// Constructs a reusable client and connection pool.
    pub async fn connect(options: ConnectOptions) -> Result<Self, ClientError> {
        let endpoint = options.endpoint.trim_end_matches('/').to_owned();
        reqwest::Url::parse(&endpoint)
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let server_token = options
            .token
            .clone()
            .map(|token| HeaderValue::from_str(&format!("Bearer {token}")))
            .transpose()
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let http = reqwest::Client::builder()
            .connect_timeout(options.connect_timeout)
            .build()?;
        let access = if options.query_uri.is_none() || options.s3_endpoint.is_none() {
            let mut request = http.get(format!("{endpoint}{ACCESS_PATH}"));
            if let Some(token) = &server_token {
                request = request.header(AUTHORIZATION, token.clone());
            }
            let response = tokio::time::timeout(options.request_timeout, request.send())
                .await
                .map_err(|_| ClientError::RequestTimeout)??;
            Some(response.error_for_status()?.json::<LocalAccess>().await?)
        } else {
            None
        };
        let s3_endpoint = options
            .s3_endpoint
            .or_else(|| access.as_ref().map(|value| value.s3_endpoint.clone()))
            .ok_or_else(|| {
                ClientError::Configuration("server advertises no S3 endpoint".to_owned())
            })?;
        let query_uri = options
            .query_uri
            .or_else(|| access.as_ref().map(|value| value.query_uri.clone()))
            .ok_or_else(|| ClientError::Configuration("server advertises no query URI".to_owned()))?
            .trim_end_matches('/')
            .to_owned();
        let access_uri = options
            .access_uri
            .unwrap_or_else(|| query_uri.clone())
            .trim_end_matches('/')
            .to_owned();
        Ok(Self {
            query_uri,
            access_uri,
            s3_endpoint,
            server_token,
            request_timeout: options.request_timeout,
            http,
        })
    }

    /// Returns the query and write API base URL.
    pub fn query_uri(&self) -> &str {
        &self.query_uri
    }

    /// Returns the S3-compatible Verglas object-cache endpoint.
    pub fn s3_endpoint(&self) -> Option<&str> {
        Some(&self.s3_endpoint)
    }

    /// Selects one named database for every catalog and execution operation.
    pub fn database(&self, name: &str) -> Result<Database, ClientError> {
        if !is_database_name(name) {
            return Err(invalid_database_name());
        }
        Ok(Database {
            client: self.clone(),
            name: name.to_owned(),
            catalog_prefix: std::sync::Arc::new(OnceCell::new()),
        })
    }

    /// Lists every reflected Integration namespace visible to this principal.
    pub async fn namespaces(&self) -> Result<Vec<NamespaceManifest>, ClientError> {
        let response = Self::require_success(
            self.send(self.authorize(self.http.get(self.url("/v1/namespaces"))))
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Creates a lazy handle to one reflected Integration namespace.
    pub fn namespace(&self, namespace: &str) -> Result<Namespace, ClientError> {
        if namespace.is_empty() || namespace.contains('/') {
            return Err(ClientError::Configuration(
                "Integration namespace must be non-empty and contain no slash".to_owned(),
            ));
        }
        Ok(Namespace {
            client: self.clone(),
            name: namespace.to_owned(),
            manifest: std::sync::Arc::new(OnceCell::new()),
        })
    }

    /// Creates a missing table or verifies the exact existing definition.
    async fn ensure_table_for(
        &self,
        database: &str,
        catalog_prefix: &OnceCell<Option<String>>,
        table: &str,
        definition: &TableDefinition,
    ) -> Result<EnsureTable, ClientError> {
        let (namespace, name) = split_table_name(table)?;
        let namespace_path = namespace.join("\u{1f}");
        let table_url = self
            .catalog_url(
                database,
                catalog_prefix,
                &["namespaces", &namespace_path, "tables", name],
            )
            .await?;
        let response = self
            .send(self.authorize(self.http.get(table_url.clone())))
            .await?;
        if response.status().is_success() {
            return self
                .verify_catalog_definition(table, definition, response)
                .await;
        }
        if response.status() != reqwest::StatusCode::NOT_FOUND {
            return Err(Self::http_error(response).await);
        }

        let namespaces_url = self
            .catalog_url(database, catalog_prefix, &["namespaces"])
            .await?;
        let namespace_response = self
            .send(
                self.authorize(self.http.post(namespaces_url))
                    .json(&json!({"namespace": namespace, "properties": {}})),
            )
            .await?;
        if !namespace_response.status().is_success()
            && namespace_response.status() != reqwest::StatusCode::CONFLICT
        {
            return Err(Self::http_error(namespace_response).await);
        }

        let create_url = self
            .catalog_url(
                database,
                catalog_prefix,
                &["namespaces", &namespace_path, "tables"],
            )
            .await?;
        let response = self
            .send(
                self.authorize(self.http.post(create_url))
                    .json(&catalog_create_request(name, definition)?),
            )
            .await?;
        if response.status().is_success() {
            Ok(EnsureTable::Created)
        } else if response.status() == reqwest::StatusCode::CONFLICT {
            let response = self.send(self.authorize(self.http.get(table_url))).await?;
            self.verify_catalog_definition(table, definition, response)
                .await
        } else {
            Err(Self::http_error(response).await)
        }
    }

    /// Adds authentication to a request when configured.
    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.server_token {
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
    async fn catalog_url(
        &self,
        database: &str,
        catalog_prefix: &OnceCell<Option<String>>,
        suffix: &[&str],
    ) -> Result<reqwest::Url, ClientError> {
        let prefix = catalog_prefix
            .get_or_try_init(|| self.discover_catalog_prefix(database))
            .await?;
        let mut url = database_catalog_url(&self.query_uri, database)?;
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
    async fn discover_catalog_prefix(&self, database: &str) -> Result<Option<String>, ClientError> {
        let mut url = database_catalog_url(&self.query_uri, database)?;
        url.path_segments_mut()
            .map_err(|_| ClientError::Configuration("invalid catalog URI".to_owned()))?
            .pop_if_empty()
            .push("v1")
            .push("config");
        let response =
            Self::require_success(self.send(self.authorize(self.http.get(url))).await?).await?;
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
        format!("{}{path}", self.query_uri)
    }
}

/// One ingest acknowledgement, as returned by `POST /v0/events`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct IngestResult {
    /// Rows accepted into the datasource.
    pub successful_rows: u64,
    /// Rows rejected and quarantined instead of accepted.
    pub quarantined_rows: u64,
}

/// The /v0 data client: append-ingest (one shape, every kind of row) plus SQL.
///
/// Table writes, vector writes, and log shipping all resolve to the same
/// NDJSON append to `POST /v0/events?name=<datasource>`; SQL runs through
/// `POST /v0/sql`. This is a Tinybird-compatible surface on Verglas Cloud, a
/// deliberate break from the older per-kind write shapes.
#[derive(Debug, Clone)]
pub struct DataClient {
    base: String,
    token: String,
    http: reqwest::Client,
}

impl DataClient {
    /// Opens a /v0 data client bound to one endpoint and workspace bearer token.
    pub fn connect(base_url: &str, token: impl Into<String>) -> Result<Self, ClientError> {
        if base_url.trim().is_empty() {
            return Err(ClientError::Configuration(
                "data client: base_url is required".to_owned(),
            ));
        }
        let token = token.into();
        if token.trim().is_empty() {
            return Err(ClientError::Configuration(
                "data client: token is required".to_owned(),
            ));
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(5))
            .build()?;
        Ok(Self {
            base: base_url.trim_end_matches('/').to_owned(),
            token,
            http,
        })
    }

    /// Appends rows to a datasource as NDJSON via `POST /v0/events?name=<datasource>`.
    pub async fn append_events(
        &self,
        datasource: &str,
        rows: &[Value],
    ) -> Result<IngestResult, ClientError> {
        let response = self
            .http
            .post(format!("{}/v0/events", self.base))
            .query(&[("name", datasource)])
            .header(AUTHORIZATION, self.bearer()?)
            .header(CONTENT_TYPE, "application/x-ndjson")
            .body(ndjson(rows))
            .send()
            .await?;
        Self::decode(response).await
    }

    /// Same append shape as [`Self::append_events`] — a table write is just rows on a datasource.
    pub async fn table_write(
        &self,
        datasource: &str,
        rows: &[Value],
    ) -> Result<IngestResult, ClientError> {
        self.append_events(datasource, rows).await
    }

    /// Same append shape as [`Self::append_events`] — a vector write is just rows on a datasource.
    pub async fn vector_write(
        &self,
        datasource: &str,
        rows: &[Value],
    ) -> Result<IngestResult, ClientError> {
        self.append_events(datasource, rows).await
    }

    /// Runs a query through `POST /v0/sql`.
    pub async fn sql<T: DeserializeOwned>(&self, query: &str) -> Result<T, ClientError> {
        let response = self
            .http
            .post(format!("{}/v0/sql", self.base))
            .header(AUTHORIZATION, self.bearer()?)
            .json(&json!({ "q": query }))
            .send()
            .await?;
        Self::decode(response).await
    }

    /// Builds the bearer header value for the configured workspace token.
    fn bearer(&self) -> Result<HeaderValue, ClientError> {
        HeaderValue::from_str(&format!("Bearer {}", self.token))
            .map_err(|error| ClientError::Configuration(error.to_string()))
    }

    /// Decodes success JSON or converts a non-success response to a diagnostic error.
    async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, ClientError> {
        let response = Client::require_success(response).await?;
        response.json().await.map_err(ClientError::Transport)
    }
}

/// Encodes rows as NDJSON: one compact JSON object per line, trailing newline.
fn ndjson(rows: &[Value]) -> String {
    let mut out = String::new();
    for row in rows {
        out.push_str(&row.to_string());
        out.push('\n');
    }
    out
}

/// One named database selected for all catalog and execution operations.
#[derive(Debug, Clone)]
pub struct Database {
    client: Client,
    name: String,
    catalog_prefix: std::sync::Arc<OnceCell<Option<String>>>,
}

impl Database {
    /// Returns the database resource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Creates a missing table or verifies the exact existing definition.
    pub async fn ensure_table(
        &self,
        table: &str,
        definition: &TableDefinition,
    ) -> Result<EnsureTable, ClientError> {
        self.client
            .ensure_table_for(&self.name, &self.catalog_prefix, table, definition)
            .await
    }
    /// Subscribes to exact table commits through one durable push stream.
    ///
    /// `max` limits the number of fenced deliveries that can be in flight.
    pub fn subscribe<I, T>(
        &self,
        group: &str,
        owner: &str,
        tables: I,
        max: Option<usize>,
        lease_seconds: u64,
    ) -> Result<TableChangeStream, ClientError>
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let tables = tables.into_iter().map(Into::into).collect::<Vec<_>>();
        if group.is_empty()
            || owner.is_empty()
            || tables.is_empty()
            || tables.iter().any(|table| table.trim().is_empty())
        {
            return Err(ClientError::Configuration(
                "database subscribe requires non-empty group, owner, and tables".to_owned(),
            ));
        }
        let database = self.clone();
        let group = group.to_owned();
        let owner = owner.to_owned();
        Ok(Box::pin(async_stream::try_stream! {
            let mut backoff = Duration::from_millis(250);
            loop {
                let url = resource_url(
                    &database.client.access_uri,
                    "databases",
                    &database.name,
                    &["subscribe"],
                )?;
                let request = database.client.authorize(database.client.http.post(url)).json(&json!({
                    "group": group,
                    "owner": owner,
                    "tables": tables,
                    "max": max.unwrap_or(256),
                    "leaseSeconds": lease_seconds,
                }));
                let response = match database.client.send(request).await {
                    Ok(response) if response.status().is_success() => response,
                    Ok(response) if is_transient_subscription_status(response.status()) => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        continue;
                    }
                    Ok(response) => Err(Client::http_error(response).await)?,
                    Err(ClientError::Transport(_) | ClientError::RequestTimeout) => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        continue;
                    }
                    Err(error) => Err(error)?,
                };
                backoff = Duration::from_millis(250);
                yield TableSubscriptionEvent::Connected;
                let mut chunks = response.bytes_stream();
                let mut buffer = Vec::new();
                while let Some(chunk) = chunks.next().await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(_) => break,
                    };
                    buffer.extend_from_slice(&chunk);
                    while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
                        let frame = buffer.drain(..=newline).collect::<Vec<_>>();
                        if frame.len() == 1 {
                            continue;
                        }
                        let delivery: DeliveryFrame = serde_json::from_slice(&frame[..frame.len() - 1])
                            .map_err(ClientError::DeliveryJson)?;
                        let payload: TableChangePayload = serde_json::from_value(delivery.payload)
                            .map_err(ClientError::DeliveryJson)?;
                        yield TableSubscriptionEvent::Delivery(TableChangeDelivery {
                            change: ChangeEvent {
                                seq: u64::try_from(delivery.position).map_err(|_| {
                                    ClientError::Configuration("negative table event position".to_owned())
                                })?,
                                table: payload.table,
                                snapshot_id: payload.snapshot_id,
                                committed_at: payload.committed_at,
                            },
                            receipt: delivery.receipt,
                        });
                    }
                }
                yield TableSubscriptionEvent::Disconnected;
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }))
    }

    /// Acknowledges one table event after its durable rows have been published downstream.
    pub async fn ack(&self, group: &str, receipt: &DeliveryReceipt) -> Result<(), ClientError> {
        if group.is_empty() {
            return Err(ClientError::Configuration(
                "database subscription ack requires a non-empty group".to_owned(),
            ));
        }
        let url = resource_url(
            &self.client.access_uri,
            "databases",
            &self.name,
            &["subscriptions", "ack"],
        )?;
        Client::require_success(
            self.client
                .send(
                    self.client
                        .authorize(self.client.http.post(url))
                        .json(&json!({ "group": group, "receipt": receipt })),
                )
                .await?,
        )
        .await?;
        Ok(())
    }
}

/// A reflected Integration namespace bound to one authenticated client identity.
#[derive(Debug, Clone)]
pub struct Namespace {
    client: Client,
    name: String,
    manifest: std::sync::Arc<OnceCell<NamespaceManifest>>,
}

impl Namespace {
    /// Loads and caches the namespace's reflection manifest.
    pub async fn manifest(&self) -> Result<&NamespaceManifest, ClientError> {
        self.manifest
            .get_or_try_init(|| async {
                let response = Client::require_success(
                    self.client
                        .send(
                            self.client
                                .authorize(self.client.http.get(self.method_url(None)?)),
                        )
                        .await?,
                )
                .await?;
                response.json().await.map_err(ClientError::Transport)
            })
            .await
    }

    /// Invokes one reflected bounded read or write method.
    pub async fn invoke<Input, Output>(
        &self,
        method: &str,
        input: &Input,
    ) -> Result<Output, ClientError>
    where
        Input: Serialize + ?Sized,
        Output: DeserializeOwned,
    {
        let definition = self.method(method).await?;
        if definition.mode == NamespaceMethodMode::Stream {
            return Err(ClientError::Configuration(format!(
                "namespace method {}.{method} is a stream",
                self.name
            )));
        }
        let request = self
            .client
            .authorize(self.client.http.post(self.method_url(Some(method))?))
            .json(input);
        let response = Client::require_success(self.client.send(request).await?).await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Opens one reflected stream method and incrementally decodes its NDJSON items.
    pub async fn stream<Input, Output>(
        &self,
        method: &str,
        input: &Input,
    ) -> Result<NamespaceStream<Output>, ClientError>
    where
        Input: Serialize + ?Sized,
        Output: DeserializeOwned + Send + 'static,
    {
        let definition = self.method(method).await?;
        if definition.mode != NamespaceMethodMode::Stream {
            return Err(ClientError::Configuration(format!(
                "namespace method {}.{method} is not a stream",
                self.name
            )));
        }
        let request = self
            .client
            .authorize(self.client.http.post(self.method_url(Some(method))?))
            .header(ACCEPT, "application/x-ndjson")
            .json(input);
        let response = Client::require_success(self.client.send(request).await?).await?;
        let state = NamespaceDecodeState {
            chunks: Box::pin(response.bytes_stream()),
            buffered: Vec::new(),
            ready: VecDeque::new(),
            finished: false,
        };
        Ok(Box::pin(stream::try_unfold(state, namespace_decode_next)))
    }

    /// Resolves one declared method before allowing the invocation to leave the SDK.
    async fn method(&self, method: &str) -> Result<NamespaceMethodManifest, ClientError> {
        if method.is_empty() || method.contains('/') {
            return Err(ClientError::Configuration(
                "Integration method must be non-empty and contain no slash".to_owned(),
            ));
        }
        self.manifest()
            .await?
            .methods
            .get(method)
            .cloned()
            .ok_or_else(|| {
                ClientError::Configuration(format!(
                    "namespace {} does not declare method {method}",
                    self.name
                ))
            })
    }

    /// Builds the reflected manifest or invocation URL without interpolating path input.
    fn method_url(&self, method: Option<&str>) -> Result<reqwest::Url, ClientError> {
        let mut url = reqwest::Url::parse(&self.client.query_uri)
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let mut segments = url.path_segments_mut().map_err(|_| {
            ClientError::Configuration("query URI cannot carry path segments".to_owned())
        })?;
        segments
            .pop_if_empty()
            .push("v1")
            .push("namespaces")
            .push(&self.name);
        if let Some(method) = method {
            segments.push("invoke").push(method);
        }
        drop(segments);
        Ok(url)
    }
}

/// Incremental state for one NDJSON Integration response.
struct NamespaceDecodeState<T> {
    chunks: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    buffered: Vec<u8>,
    ready: VecDeque<T>,
    finished: bool,
}

/// Produces one decoded namespace stream item without buffering the complete response.
async fn namespace_decode_next<T>(
    mut state: NamespaceDecodeState<T>,
) -> Result<Option<(T, NamespaceDecodeState<T>)>, ClientError>
where
    T: DeserializeOwned,
{
    loop {
        if let Some(value) = state.ready.pop_front() {
            return Ok(Some((value, state)));
        }
        if state.finished {
            return Ok(None);
        }
        match state.chunks.next().await {
            Some(Ok(chunk)) => {
                state.buffered.extend_from_slice(&chunk);
                while let Some(newline) = state.buffered.iter().position(|byte| *byte == b'\n') {
                    let mut line: Vec<u8> = state.buffered.drain(..=newline).collect();
                    line.pop();
                    if line.iter().any(|byte| !byte.is_ascii_whitespace()) {
                        state.ready.push_back(serde_json::from_slice(&line)?);
                    }
                }
            }
            Some(Err(error)) => return Err(ClientError::Transport(error)),
            None => {
                state.finished = true;
                if state
                    .buffered
                    .iter()
                    .any(|byte| !byte.is_ascii_whitespace())
                {
                    state
                        .ready
                        .push_back(serde_json::from_slice(&state.buffered)?);
                    state.buffered.clear();
                }
            }
        }
    }
}

/// Builds a `/v1/{family}/{name}/...` URL with path-segment encoding.
fn resource_url(
    query_uri: &str,
    family: &str,
    name: &str,
    suffix: &[&str],
) -> Result<reqwest::Url, ClientError> {
    let mut url = reqwest::Url::parse(query_uri)
        .map_err(|error| ClientError::Configuration(error.to_string()))?;
    {
        let mut segments = url.path_segments_mut().map_err(|_| {
            ClientError::Configuration("query URI cannot carry path segments".to_owned())
        })?;
        segments.pop_if_empty().push("v1").push(family).push(name);
        for segment in suffix {
            segments.push(segment);
        }
    }
    Ok(url)
}

/// Builds the database-scoped Iceberg REST catalog mount.
fn database_catalog_url(query_uri: &str, database: &str) -> Result<reqwest::Url, ClientError> {
    let mut url = reqwest::Url::parse(query_uri)
        .map_err(|error| ClientError::Configuration(error.to_string()))?;
    url.path_segments_mut()
        .map_err(|_| ClientError::Configuration("query URI cannot carry path segments".to_owned()))?
        .pop_if_empty()
        .push("v1")
        .push("databases")
        .push(database)
        .push("catalog");
    Ok(url)
}

/// Checks the database resource-name grammar enforced by the database API.
fn is_database_name(name: &str) -> bool {
    let Some((first, remainder)) = name.as_bytes().split_first() else {
        return false;
    };
    (first.is_ascii_alphabetic() || *first == b'_')
        && remainder
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-'))
}

/// Returns the shared database-name configuration error.
fn invalid_database_name() -> ClientError {
    ClientError::Configuration(
        "database name must start with a letter or underscore and contain only ASCII letters, digits, underscores, or hyphens".to_owned(),
    )
}

/// Returns whether a subscription handshake should reconnect from durable state.
fn is_transient_subscription_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
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
            "name": format!("{}_{}", partition.source, partition.transform),
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

/// Reads a non-empty environment value.
fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}
