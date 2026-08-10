//! Authenticated, streaming client for the Verglas server data plane.
//!
//! The client keeps transport, authentication, Arrow IPC, idempotency, and
//! table-contract validation in the SDK. Applications and the CLI therefore
//! call the same implementation instead of rebuilding HTTP behavior.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::pin::Pin;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_ipc::writer::StreamWriter;
use bytes::Bytes;
use futures::{SinkExt, Stream, StreamExt, TryStream, stream};
use reqwest::header::{
    ACCEPT, AUTHORIZATION, CONTENT_TYPE, ETAG, HeaderName, HeaderValue, IF_MATCH, IF_NONE_MATCH,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::OnceCell;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use verglas_api::table::CommitResponse;
use verglas_authz::{AccessCheck, AccessDecision, Grant, Principal, Resource};
use verglas_core::admin::{ACCESS_PATH, LocalAccess};

pub use verglas_api::{ColumnSpec, PartitionSpec, TableDefinition};

use crate::graph::{
    BuildIndexRequest, EdgeInput, GraphCreateReport, GraphDirection, GraphFilter, GraphOp,
    GraphQueryRequest, GraphQueryResponse, GraphShowReport, IndexReport as GraphIndexReport,
    InsertEdgesRequest, InsertNodesRequest, InsertReport, NeighborView, NodeInput, PathView,
    ReachedView,
};
use crate::queue::{QueueAckResult, QueueEnqueueResult, QueuePollResult};
use crate::token::{
    AccessTokenCreateRequest, AccessTokenSummary, DatabaseConnectionToken,
    DatabaseConnectionTokenRequest, IssuedAccessToken,
};
use crate::vector::{
    DeclareIndexRequest, IndexInfo, IndexReport as VectorIndexReport, SearchRequest, SearchResponse,
};
use crate::worker::ChangeEvent;

/// MIME type used by Verglas for Arrow IPC streaming requests and responses.
pub const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

/// A stream of Arrow record batches returned by a query.
pub type QueryStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, ClientError>> + Send>>;

/// A resumable stream of catalog commit notifications.
pub type FollowStream = Pin<Box<dyn Stream<Item = Result<ChangeEvent, ClientError>> + Send>>;

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
    catalog_token: Option<String>,
    catalog_uri: Option<String>,
    query_uri: Option<String>,
    access_uri: Option<String>,
    warehouse: Option<String>,
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
            catalog_token: None,
            catalog_uri: None,
            query_uri: None,
            access_uri: None,
            warehouse: None,
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
        options.catalog_token = nonempty_env("VERGLAS_CATALOG_TOKEN");
        options.catalog_uri = nonempty_env("VERGLAS_CATALOG_URI");
        options.query_uri = nonempty_env("VERGLAS_QUERY_URI");
        options.access_uri = nonempty_env("VERGLAS_ACCESS_URI");
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

    /// Supplies the Iceberg REST endpoint clients should use explicitly.
    #[must_use]
    pub fn with_catalog_uri(mut self, catalog_uri: impl Into<String>) -> Self {
        self.catalog_uri = Some(catalog_uri.into());
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

    /// Supplies the catalog bearer token without changing server authentication.
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

/// Optional metadata and conditions for one durable KV put.
#[derive(Debug, Clone, Default)]
pub struct KvPutOptions {
    /// Relative logical lifetime in seconds.
    pub ttl_seconds: Option<u64>,
    /// Absolute logical expiration in Unix milliseconds.
    pub expires_at_ms: Option<u64>,
    /// MIME type returned with the raw value.
    pub content_type: Option<String>,
    /// Bounded application metadata.
    pub metadata: BTreeMap<String, String>,
    /// Required current version.
    pub if_match: Option<String>,
    /// Requires the key to have no live value.
    pub create_only: bool,
    /// Identity for one logical write. The SDK never retries it itself.
    pub idempotency_key: Option<String>,
}

/// Result of one committed KV put.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPutResult {
    /// Opaque committed version/ETag.
    pub version: String,
    /// Whether the server replayed an existing idempotent result.
    pub idempotent: bool,
}

/// The serving tier reported by the endpoint when available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvReadTier {
    /// The endpoint did not expose its local serving tier.
    Unspecified,
    /// The value came from process RAM.
    Ram,
    /// The value came from local NVMe.
    Nvme,
}

/// One raw KV value and its bounded metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvValue {
    /// Raw value bytes.
    pub bytes: Bytes,
    /// Opaque committed version/ETag.
    pub version: String,
    /// MIME type supplied on write.
    pub content_type: Option<String>,
    /// Commit time in Unix milliseconds.
    pub modified_at_ms: u64,
    /// Logical expiration time in Unix milliseconds.
    pub expires_at_ms: Option<u64>,
    /// Bounded application metadata.
    pub metadata: BTreeMap<String, String>,
    /// Local serving tier when the endpoint reports it.
    pub tier: KvReadTier,
}

/// Result of one idempotent KV delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct KvDeleteResult {
    /// Whether a live value was removed.
    pub removed: bool,
}

/// One metadata-only KV list entry.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct KvListEntry {
    /// Key in bytewise list order.
    pub key: String,
    /// Opaque committed version.
    pub version: String,
    /// Commit time in Unix milliseconds.
    pub modified_at_ms: u64,
    /// Logical expiration time.
    pub expires_at_ms: Option<u64>,
    /// MIME type supplied on write.
    pub content_type: Option<String>,
    /// Bounded application metadata.
    pub metadata: BTreeMap<String, String>,
}

/// One bounded metadata-only KV list page.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct KvListPage {
    /// Entries in deterministic bytewise order.
    pub entries: Vec<KvListEntry>,
    /// Opaque continuation cursor when more entries remain.
    pub next_cursor: Option<String>,
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

/// Reusable authenticated client for a Verglas server.
#[derive(Debug, Clone)]
pub struct Client {
    query_uri: String,
    access_uri: String,
    catalog_uri: String,
    warehouse: Option<String>,
    s3_endpoint: String,
    raw_catalog_token: Option<String>,
    server_token: Option<HeaderValue>,
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
        let server_token = options
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
        let access = if options.catalog_uri.is_none()
            || options.query_uri.is_none()
            || options.s3_endpoint.is_none()
        {
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
        let catalog_uri = options
            .catalog_uri
            .or_else(|| access.as_ref().and_then(|value| value.catalog_uri.clone()))
            .ok_or_else(|| {
                ClientError::Configuration("server advertises no catalog URI".to_owned())
            })?
            .trim_end_matches('/')
            .to_owned();
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
            catalog_uri,
            warehouse: options
                .warehouse
                .or_else(|| access.as_ref().and_then(|value| value.warehouse.clone())),
            s3_endpoint,
            raw_catalog_token,
            server_token,
            catalog_token,
            catalog_prefix: std::sync::Arc::new(OnceCell::new()),
            request_timeout: options.request_timeout,
            http,
        })
    }

    /// Returns the discovered Iceberg REST catalog URI.
    pub fn catalog_uri(&self) -> &str {
        &self.catalog_uri
    }

    /// Returns the query and write API base URL.
    pub fn query_uri(&self) -> &str {
        &self.query_uri
    }

    /// Returns the S3-compatible Verglas object-cache endpoint.
    pub fn s3_endpoint(&self) -> Option<&str> {
        Some(&self.s3_endpoint)
    }

    /// Returns a thin raw-byte handle to one tenant-authorized KV namespace.
    pub fn kv(&self, namespace: &str) -> Result<Kv, ClientError> {
        if namespace.is_empty() || namespace.contains('/') {
            return Err(ClientError::Configuration(
                "KV namespace must be non-empty and contain no slash".to_owned(),
            ));
        }
        Ok(Kv {
            client: self.clone(),
            namespace: namespace.to_owned(),
        })
    }

    /// Returns a handle to one durable queue.
    pub fn queue(&self, name: &str) -> Result<Queue, ClientError> {
        if name.is_empty() || name.contains('/') {
            return Err(ClientError::Configuration(
                "queue name must be non-empty and contain no slash".to_owned(),
            ));
        }
        Ok(Queue {
            client: self.clone(),
            name: name.to_owned(),
        })
    }

    /// Returns a handle to one property-graph namespace.
    pub fn graph(&self, namespace: &str) -> Result<Graph, ClientError> {
        if namespace.is_empty() || namespace.contains('/') {
            return Err(ClientError::Configuration(
                "graph namespace must be non-empty and contain no slash".to_owned(),
            ));
        }
        Ok(Graph {
            client: self.clone(),
            namespace: namespace.to_owned(),
        })
    }

    /// Returns a handle to one table for vector-index operations.
    pub fn table(&self, name: &str) -> Result<Table, ClientError> {
        if name.is_empty() || name.contains('/') {
            return Err(ClientError::Configuration(
                "table name must be non-empty and contain no slash".to_owned(),
            ));
        }
        Ok(Table {
            client: self.clone(),
            name: name.to_owned(),
        })
    }

    /// Creates one principal in the tenant authorization registry.
    pub async fn create_principal(&self, principal: &Principal) -> Result<Principal, ClientError> {
        self.access_json(
            self.http
                .post(self.access_url("/v1/access/principals"))
                .json(principal),
        )
        .await
    }

    /// Lists principals registered in one tenant.
    pub async fn list_principals(&self, tenant_id: &str) -> Result<Vec<Principal>, ClientError> {
        self.access_json(
            self.http
                .get(self.access_url("/v1/access/principals"))
                .query(&[("tenant_id", tenant_id)]),
        )
        .await
    }

    /// Creates one protected resource in the tenant hierarchy.
    pub async fn create_resource(&self, resource: &Resource) -> Result<Resource, ClientError> {
        self.access_json(
            self.http
                .post(self.access_url("/v1/access/resources"))
                .json(resource),
        )
        .await
    }

    /// Lists protected resources registered in one tenant.
    pub async fn list_resources(&self, tenant_id: &str) -> Result<Vec<Resource>, ClientError> {
        self.access_json(
            self.http
                .get(self.access_url("/v1/access/resources"))
                .query(&[("tenant_id", tenant_id)]),
        )
        .await
    }

    /// Creates one additive authorization grant.
    pub async fn create_access_grant(&self, grant: &Grant) -> Result<Grant, ClientError> {
        self.access_json(
            self.http
                .post(self.access_url("/v1/access/grants"))
                .json(grant),
        )
        .await
    }

    /// Lists authorization grants registered in one tenant.
    pub async fn list_access_grants(&self, tenant_id: &str) -> Result<Vec<Grant>, ClientError> {
        self.access_json(
            self.http
                .get(self.access_url("/v1/access/grants"))
                .query(&[("tenant_id", tenant_id)]),
        )
        .await
    }

    /// Evaluates one action with the client's scoped bearer credential.
    pub async fn check_access(&self, check: &AccessCheck) -> Result<AccessDecision, ClientError> {
        self.access_json(
            self.http
                .post(self.access_url("/v1/access/check"))
                .json(check),
        )
        .await
    }

    /// Mints a delegated token for the authenticated principal and returns its plaintext once.
    pub async fn create_access_token(
        &self,
        request: &AccessTokenCreateRequest,
    ) -> Result<IssuedAccessToken, ClientError> {
        self.access_json(
            self.http
                .post(self.access_url("/v1/access/tokens"))
                .json(request),
        )
        .await
    }

    /// Lists the authenticated principal's token inventory without exposing plaintext tokens.
    pub async fn list_access_tokens(&self) -> Result<Vec<AccessTokenSummary>, ClientError> {
        self.access_json(self.http.get(self.access_url("/v1/access/tokens")))
            .await
    }

    /// Revokes one token owned or manageable by the authenticated principal.
    pub async fn revoke_access_token(
        &self,
        token_id: &str,
    ) -> Result<AccessTokenSummary, ClientError> {
        self.access_json(
            self.http
                .delete(self.access_url(&format!("/v1/access/tokens/{token_id}"))),
        )
        .await
    }

    /// Exchanges the current authorized bearer for a short-lived Postgres connection token.
    pub async fn create_database_connection_token(
        &self,
        request: &DatabaseConnectionTokenRequest,
    ) -> Result<DatabaseConnectionToken, ClientError> {
        self.access_json(
            self.http
                .post(self.access_url("/v1/access/database-tokens"))
                .json(request),
        )
        .await
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

    /// Executes SQL through `POST /v1/databases/{database}/query` and incrementally decodes Arrow.
    pub async fn query_stream(
        &self,
        database: &str,
        sql: &str,
    ) -> Result<QueryStream, ClientError> {
        if !is_database_name(database) {
            return Err(ClientError::Configuration(
                "database name must start with a letter or underscore and contain only ASCII letters, digits, underscores, or hyphens".to_owned(),
            ));
        }
        let request = self
            .authorize(self.http.post(resource_url(
                &self.query_uri,
                "databases",
                database,
                &["query"],
            )?))
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
        match &self.server_token {
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

    /// Sends one authenticated authorization request and decodes its JSON result.
    async fn access_json<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, ClientError> {
        let response = Self::require_success(self.send(self.authorize(request)).await?).await?;
        response.json().await.map_err(ClientError::Transport)
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
        format!("{}{path}", self.query_uri)
    }

    /// Joins an authorization path to the configured access service.
    fn access_url(&self, path: &str) -> String {
        format!("{}{path}", self.access_uri)
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

/// A thin raw-byte client for one KV namespace.
#[derive(Debug, Clone)]
pub struct Kv {
    client: Client,
    namespace: String,
}

impl Kv {
    /// Durably sets raw bytes and returns the server-owned version.
    pub async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, ClientError> {
        if options.ttl_seconds.is_some() && options.expires_at_ms.is_some() {
            return Err(ClientError::Configuration(
                "KV put accepts ttl_seconds or expires_at_ms, not both".to_owned(),
            ));
        }
        let mut request = self
            .client
            .authorize(self.client.http.put(self.url(Some(key))?))
            .body(bytes);
        if let Some(ttl) = options.ttl_seconds {
            request = request.header("x-verglas-ttl-seconds", ttl);
        }
        if let Some(expires) = options.expires_at_ms {
            request = request.header("x-verglas-expires-at-ms", expires);
        }
        if let Some(content_type) = options.content_type {
            request = request.header(CONTENT_TYPE, content_type);
        }
        if let Some(expected) = options.if_match {
            request = request.header(IF_MATCH, expected);
        }
        if options.create_only {
            request = request.header(IF_NONE_MATCH, "*");
        }
        if let Some(idempotency_key) = options.idempotency_key {
            request = request.header("idempotency-key", idempotency_key);
        }
        for (name, value) in options.metadata {
            let name = HeaderName::from_bytes(format!("x-verglas-meta-{name}").as_bytes())
                .map_err(|error| ClientError::Configuration(error.to_string()))?;
            request = request.header(name, value);
        }
        let response = Client::require_success(self.client.send(request).await?).await?;
        let version = required_header(response.headers(), ETAG.as_str())?;
        let idempotent = response
            .headers()
            .get("x-verglas-idempotent")
            .and_then(|value| value.to_str().ok())
            == Some("true");
        Ok(KvPutResult {
            version,
            idempotent,
        })
    }

    /// Gets raw bytes, returning `None` for an absent or expired key.
    pub async fn get(&self, key: &str) -> Result<Option<KvValue>, ClientError> {
        let response = self
            .client
            .send(
                self.client
                    .authorize(self.client.http.get(self.url(Some(key))?)),
            )
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = Client::require_success(response).await?;
        let version = required_header(response.headers(), ETAG.as_str())?;
        let modified_at_ms = required_header(response.headers(), "x-verglas-modified-at-ms")?
            .parse::<u64>()
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let expires_at_ms = optional_u64_header(response.headers(), "x-verglas-expires-at-ms")?;
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let tier = match response
            .headers()
            .get("x-verglas-kv-tier")
            .and_then(|value| value.to_str().ok())
        {
            Some("ram") => KvReadTier::Ram,
            Some("nvme") => KvReadTier::Nvme,
            _ => KvReadTier::Unspecified,
        };
        let mut metadata = BTreeMap::new();
        for (name, value) in response.headers() {
            let Some(name) = name.as_str().strip_prefix("x-verglas-meta-") else {
                continue;
            };
            let value = value
                .to_str()
                .map_err(|error| ClientError::Configuration(error.to_string()))?;
            metadata.insert(name.to_owned(), value.to_owned());
        }
        let bytes = response.bytes().await?;
        Ok(Some(KvValue {
            bytes,
            version,
            content_type,
            modified_at_ms,
            expires_at_ms,
            metadata,
            tier,
        }))
    }

    /// Deletes one key idempotently with an optional expected version.
    pub async fn delete(
        &self,
        key: &str,
        if_match: Option<&str>,
    ) -> Result<KvDeleteResult, ClientError> {
        let mut request = self
            .client
            .authorize(self.client.http.delete(self.url(Some(key))?));
        if let Some(expected) = if_match {
            request = request.header(IF_MATCH, expected);
        }
        let response = Client::require_success(self.client.send(request).await?).await?;
        Ok(response.json().await?)
    }

    /// Lists one bounded metadata-only page without interpreting its cursor.
    pub async fn list(
        &self,
        prefix: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<KvListPage, ClientError> {
        let mut url = self.url(None)?;
        url.query_pairs_mut()
            .append_pair("prefix", prefix)
            .append_pair("limit", &limit.to_string());
        if let Some(cursor) = cursor {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }
        let response = Client::require_success(
            self.client
                .send(self.client.authorize(self.client.http.get(url)))
                .await?,
        )
        .await?;
        Ok(response.json().await?)
    }

    /// Builds one URL using path-segment encoding owned by the HTTP client.
    fn url(&self, key: Option<&str>) -> Result<reqwest::Url, ClientError> {
        let mut url = reqwest::Url::parse(&self.client.query_uri)
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        {
            let mut segments = url.path_segments_mut().map_err(|_| {
                ClientError::Configuration("KV endpoint cannot carry path segments".to_owned())
            })?;
            segments
                .pop_if_empty()
                .push("v1")
                .push("kv")
                .push(&self.namespace);
            if let Some(key) = key {
                if key.is_empty() {
                    return Err(ClientError::Configuration("KV key is required".to_owned()));
                }
                segments.push(key);
            }
        }
        Ok(url)
    }
}

/// Shared traversal filters for graph neighbor, k-hop, and path reads.
#[derive(Debug, Clone, Default)]
pub struct GraphReadOptions {
    /// Only follow edges with this predicate, when set.
    pub predicate: Option<String>,
    /// Only follow edges whose confidence is at least this, when set.
    pub min_confidence: Option<f64>,
    /// Direction to follow edges; defaults to [`GraphDirection::Out`].
    pub direction: GraphDirection,
    /// Read the graph as of this edge snapshot when set.
    pub as_of: Option<i64>,
}

/// A durable ordered queue bound to one authenticated client.
#[derive(Debug, Clone)]
pub struct Queue {
    client: Client,
    name: String,
}

impl Queue {
    /// Appends rows to the queue and returns the new end position.
    pub async fn enqueue(&self, rows: Vec<Value>) -> Result<QueueEnqueueResult, ClientError> {
        let response = Client::require_success(
            self.client
                .send(
                    self.client
                        .authorize(self.client.http.post(self.url(&["enqueue"])?))
                        .json(&json!({ "rows": rows })),
                )
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Polls up to `max` records for consumer group `group` from its watermark.
    pub async fn poll(
        &self,
        group: &str,
        max: Option<usize>,
    ) -> Result<QueuePollResult, ClientError> {
        if group.is_empty() {
            return Err(ClientError::Configuration(
                "queue poll requires a non-empty group".to_owned(),
            ));
        }
        let mut url = self.url(&["poll"])?;
        url.query_pairs_mut().append_pair("group", group);
        if let Some(max) = max {
            url.query_pairs_mut().append_pair("max", &max.to_string());
        }
        let response = Client::require_success(
            self.client
                .send(self.client.authorize(self.client.http.get(url)))
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Advances `group`'s watermark to `position` after the consumer commits work.
    pub async fn ack(&self, group: &str, position: u64) -> Result<QueueAckResult, ClientError> {
        if group.is_empty() {
            return Err(ClientError::Configuration(
                "queue ack requires a non-empty group".to_owned(),
            ));
        }
        let response = Client::require_success(
            self.client
                .send(
                    self.client
                        .authorize(self.client.http.post(self.url(&["ack"])?))
                        .json(&json!({ "group": group, "position": position })),
                )
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Builds a queue URL with path-segment encoding owned by the HTTP client.
    fn url(&self, suffix: &[&str]) -> Result<reqwest::Url, ClientError> {
        resource_url(&self.client.query_uri, "queues", &self.name, suffix)
    }
}

/// A property-graph namespace bound to one authenticated client.
#[derive(Debug, Clone)]
pub struct Graph {
    client: Client,
    namespace: String,
}

impl Graph {
    /// Creates the graph's nodes and edges tables. Idempotent.
    pub async fn create(&self) -> Result<GraphCreateReport, ClientError> {
        let response = Client::require_success(
            self.client
                .send(
                    self.client
                        .authorize(self.client.http.post(self.url(&[])?))
                        .json(&json!({})),
                )
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Shows backing tables, live counts, and whether an index is bound.
    pub async fn show(&self) -> Result<GraphShowReport, ClientError> {
        let response = Client::require_success(
            self.client
                .send(self.client.authorize(self.client.http.get(self.url(&[])?)))
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Appends nodes and returns the new nodes-table snapshot and count.
    pub async fn insert_nodes(&self, nodes: Vec<NodeInput>) -> Result<InsertReport, ClientError> {
        let response = Client::require_success(
            self.client
                .send(
                    self.client
                        .authorize(self.client.http.post(self.url(&["nodes"])?))
                        .json(&InsertNodesRequest { nodes }),
                )
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Appends edges and returns the new edges-table snapshot and count.
    pub async fn insert_edges(&self, edges: Vec<EdgeInput>) -> Result<InsertReport, ClientError> {
        let response = Client::require_success(
            self.client
                .send(
                    self.client
                        .authorize(self.client.http.post(self.url(&["edges"])?))
                        .json(&InsertEdgesRequest { edges }),
                )
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Builds or refreshes the adjacency index for the current edge snapshot.
    pub async fn build_index(&self) -> Result<GraphIndexReport, ClientError> {
        let response = Client::require_success(
            self.client
                .send(
                    self.client
                        .authorize(self.client.http.post(self.url(&["index"])?))
                        .json(&BuildIndexRequest::default()),
                )
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Returns the direct neighbors of `node`.
    pub async fn neighbors(
        &self,
        node: &str,
        opts: GraphReadOptions,
    ) -> Result<Vec<NeighborView>, ClientError> {
        let response = self
            .query(GraphQueryRequest {
                op: GraphOp::Neighbors,
                start: node.to_owned(),
                dst: None,
                direction: opts.direction,
                k: None,
                max_hops: None,
                filter: graph_filter(&opts),
                as_of: opts.as_of,
            })
            .await?;
        Ok(response.neighbors.unwrap_or_default())
    }

    /// Returns every node reached within `hops` of `node`.
    pub async fn k_hop(
        &self,
        node: &str,
        hops: u32,
        opts: GraphReadOptions,
    ) -> Result<Vec<ReachedView>, ClientError> {
        let response = self
            .query(GraphQueryRequest {
                op: GraphOp::KHop,
                start: node.to_owned(),
                dst: None,
                direction: opts.direction,
                k: Some(hops),
                max_hops: None,
                filter: graph_filter(&opts),
                as_of: opts.as_of,
            })
            .await?;
        Ok(response.reached.unwrap_or_default())
    }

    /// Returns shortest paths from `src` to `dst` within `max_hops`.
    pub async fn paths(
        &self,
        src: &str,
        dst: &str,
        max_hops: u32,
        opts: GraphReadOptions,
    ) -> Result<Vec<PathView>, ClientError> {
        let response = self
            .query(GraphQueryRequest {
                op: GraphOp::Paths,
                start: src.to_owned(),
                dst: Some(dst.to_owned()),
                direction: opts.direction,
                k: None,
                max_hops: Some(max_hops),
                filter: graph_filter(&opts),
                as_of: opts.as_of,
            })
            .await?;
        Ok(response.paths.unwrap_or_default())
    }

    /// Posts one traversal request to the graph query route.
    async fn query(&self, body: GraphQueryRequest) -> Result<GraphQueryResponse, ClientError> {
        let response = Client::require_success(
            self.client
                .send(
                    self.client
                        .authorize(self.client.http.post(self.url(&["query"])?))
                        .json(&body),
                )
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Builds a graph URL with path-segment encoding owned by the HTTP client.
    fn url(&self, suffix: &[&str]) -> Result<reqwest::Url, ClientError> {
        resource_url(&self.client.query_uri, "graphs", &self.namespace, suffix)
    }
}

/// A table handle for vector-index declaration and search.
#[derive(Debug, Clone)]
pub struct Table {
    client: Client,
    name: String,
}

impl Table {
    /// Declares a vector index on `field` and runs the initial build.
    pub async fn add_index(
        &self,
        field: &str,
        request: DeclareIndexRequest,
    ) -> Result<VectorIndexReport, ClientError> {
        if field.is_empty() {
            return Err(ClientError::Configuration(
                "index field must be non-empty".to_owned(),
            ));
        }
        let body = DeclareIndexRequest {
            field: field.to_owned(),
            metric: request.metric,
            id_field: request.id_field,
            params: request.params,
        };
        let response = Client::require_success(
            self.client
                .send(
                    self.client
                        .authorize(self.client.http.post(self.url(&["indexes"])?))
                        .json(&body),
                )
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Lists the vector indexes declared on this table.
    pub async fn list_indexes(&self) -> Result<Vec<IndexInfo>, ClientError> {
        let response = Client::require_success(
            self.client
                .send(
                    self.client
                        .authorize(self.client.http.get(self.url(&["indexes"])?)),
                )
                .await?,
        )
        .await?;
        let body: crate::vector::IndexListResponse =
            response.json().await.map_err(ClientError::Transport)?;
        Ok(body.indexes)
    }

    /// Searches an embedding field for the nearest neighbors of `request.vector`.
    pub async fn search_index(
        &self,
        field: &str,
        request: SearchRequest,
    ) -> Result<SearchResponse, ClientError> {
        if field.is_empty() {
            return Err(ClientError::Configuration(
                "index field must be non-empty".to_owned(),
            ));
        }
        let response = Client::require_success(
            self.client
                .send(
                    self.client
                        .authorize(
                            self.client
                                .http
                                .post(self.url(&["indexes", field, "search"])?),
                        )
                        .json(&request),
                )
                .await?,
        )
        .await?;
        response.json().await.map_err(ClientError::Transport)
    }

    /// Builds a table URL with path-segment encoding owned by the HTTP client.
    fn url(&self, suffix: &[&str]) -> Result<reqwest::Url, ClientError> {
        resource_url(&self.client.query_uri, "tables", &self.name, suffix)
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

/// Maps client read options onto the wire filter object.
fn graph_filter(opts: &GraphReadOptions) -> GraphFilter {
    GraphFilter {
        predicate: opts.predicate.clone(),
        min_confidence: opts.min_confidence,
    }
}

/// Reads one required text response header.
fn required_header(
    headers: &reqwest::header::HeaderMap,
    name: &str,
) -> Result<String, ClientError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| {
            ClientError::Configuration(format!("KV response is missing the `{name}` header"))
        })
}

/// Parses one optional decimal response header.
fn optional_u64_header(
    headers: &reqwest::header::HeaderMap,
    name: &str,
) -> Result<Option<u64>, ClientError> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map_err(|error| ClientError::Configuration(error.to_string()))?
                .parse::<u64>()
                .map_err(|error| ClientError::Configuration(error.to_string()))
        })
        .transpose()
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The default header deadline covers a cold isolated worker launch plus
    /// catalog planning against a remote warehouse.
    #[test]
    fn default_request_timeout_covers_cold_worker_startup() {
        assert_eq!(
            ConnectOptions::new("http://127.0.0.1:8334").request_timeout,
            Duration::from_secs(120)
        );
    }
}
