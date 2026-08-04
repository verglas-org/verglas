//! Authenticated, streaming client for the Verglas daemon data plane.
//!
//! The client keeps transport, authentication, Arrow IPC, idempotency, and
//! table-contract validation in the SDK. Applications and the CLI therefore
//! call the same implementation instead of rebuilding HTTP behavior.

use std::collections::HashSet;
use std::pin::Pin;
use std::time::Duration;

use arrow_array::RecordBatch;
use futures::{SinkExt, Stream, StreamExt, TryStream, TryStreamExt, stream};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::OnceCell;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use verglas_core::admin::{ACCESS_PATH, LocalAccess};
use verglas_iceberg::Connection;

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
    catalog_uri: Option<String>,
    warehouse: Option<String>,
    s3_endpoint: Option<String>,
    region: String,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl ConnectOptions {
    /// Creates options for a daemon endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: None,
            catalog_uri: None,
            warehouse: None,
            s3_endpoint: None,
            region: "us-east-1".to_owned(),
            access_key_id: None,
            secret_access_key: None,
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
        options.catalog_uri = nonempty_env("VERGLAS_CATALOG_URI");
        options.warehouse = nonempty_env("VERGLAS_WAREHOUSE");
        options.s3_endpoint = nonempty_env("VERGLAS_S3_ENDPOINT");
        options.region =
            nonempty_env("VERGLAS_S3_REGION").unwrap_or_else(|| "us-east-1".to_owned());
        options.access_key_id = nonempty_env("VERGLAS_S3_ACCESS_KEY_ID");
        options.secret_access_key = nonempty_env("VERGLAS_S3_SECRET_ACCESS_KEY");
        if let Some(token) =
            nonempty_env("VERGLAS_CATALOG_TOKEN").or_else(|| nonempty_env("VERGLAS_TOKEN"))
        {
            options.token = Some(token);
        }
        options
    }

    /// Adds the bearer token sent with every request.
    #[must_use]
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
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

    /// Supplies the S3 cache endpoint used for every data-file read and write.
    #[must_use]
    pub fn with_s3_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.s3_endpoint = Some(endpoint.into());
        self
    }

    /// Supplies the cache endpoint SigV4 region and keypair.
    #[must_use]
    pub fn with_s3_credentials(
        mut self,
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.region = region.into();
        self.access_key_id = Some(access_key_id.into());
        self.secret_access_key = Some(secret_access_key.into());
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
    /// The daemon returned a non-success status.
    #[error("daemon returned HTTP {status}: {message}")]
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
    /// The Iceberg catalog, table writer, or query engine failed.
    #[error("Verglas catalog operation failed: {0}")]
    Catalog(String),
    /// The requested replay cursor has aged out of feed retention.
    #[error("catalog change feed cursor expired: {reason}")]
    CursorExpired {
        /// Server-provided resynchronization reason.
        reason: String,
    },
}

/// Reusable authenticated client for a Verglas daemon.
#[derive(Clone)]
pub struct Client {
    raw_token: Option<String>,
    connection: Connection,
    catalog: std::sync::Arc<OnceCell<std::sync::Arc<dyn iceberg::Catalog>>>,
}

impl Client {
    /// Constructs a reusable client and connection pool.
    pub async fn connect(options: ConnectOptions) -> Result<Self, ClientError> {
        let endpoint = options.endpoint.trim_end_matches('/').to_owned();
        reqwest::Url::parse(&endpoint)
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let http = reqwest::Client::builder()
            .connect_timeout(options.connect_timeout)
            .build()?;
        let access = if options.catalog_uri.is_none() || options.s3_endpoint.is_none() {
            let url = format!("{endpoint}{ACCESS_PATH}");
            let response = tokio::time::timeout(options.request_timeout, http.get(url).send())
                .await
                .map_err(|_| ClientError::RequestTimeout)??;
            if !response.status().is_success() {
                return Err(ClientError::Configuration(format!(
                    "daemon access discovery returned HTTP {}",
                    response.status()
                )));
            }
            Some(response.json::<LocalAccess>().await?)
        } else {
            None
        };
        let catalog_uri = options
            .catalog_uri
            .or_else(|| access.as_ref().and_then(|access| access.catalog_uri.clone()))
            .ok_or_else(|| {
                ClientError::Configuration(
                    "no Iceberg catalog URI: set VERGLAS_CATALOG_URI or configure the daemon catalog"
                        .to_owned(),
                )
            })?;
        let s3_endpoint = options
            .s3_endpoint
            .or_else(|| access.as_ref().map(|access| access.s3_endpoint.clone()))
            .ok_or_else(|| {
                ClientError::Configuration(
                    "no Verglas S3 cache endpoint: set VERGLAS_S3_ENDPOINT or connect to a daemon"
                        .to_owned(),
                )
            })?;
        let connection = Connection {
            catalog_uri,
            token: options.token.clone(),
            warehouse: options
                .warehouse
                .or_else(|| access.as_ref().and_then(|access| access.warehouse.clone())),
            s3_endpoint: Some(s3_endpoint),
            region: access
                .as_ref()
                .map(|access| access.region.clone())
                .unwrap_or(options.region),
            access_key_id: options.access_key_id.or_else(|| {
                access
                    .as_ref()
                    .and_then(|access| access.access_key_id.clone())
            }),
            secret_access_key: options.secret_access_key,
        };
        Ok(Self {
            raw_token: options.token,
            connection,
            catalog: std::sync::Arc::new(OnceCell::new()),
        })
    }

    /// Returns the resolved upstream Iceberg REST catalog URI.
    pub fn catalog_uri(&self) -> &str {
        &self.connection.catalog_uri
    }

    /// Returns the resolved Verglas S3 cache endpoint.
    pub fn s3_endpoint(&self) -> Option<&str> {
        self.connection.s3_endpoint.as_deref()
    }

    /// Creates a missing table or verifies the exact existing definition.
    pub async fn ensure_table(
        &self,
        table: &str,
        definition: &TableDefinition,
    ) -> Result<EnsureTable, ClientError> {
        let catalog = self.catalog().await?;
        let ident = verglas_iceberg::parse_table_ident(table)
            .map_err(|error| ClientError::Catalog(error.to_string()))?;
        if catalog
            .table_exists(&ident)
            .await
            .map_err(|error| ClientError::Catalog(error.to_string()))?
        {
            verify_definition(catalog.as_ref(), &ident, table, definition).await
        } else {
            match verglas_iceberg::tables_api::create_table(
                catalog.as_ref(),
                &ident,
                definition.clone(),
            )
            .await
            {
                Ok(_) => Ok(EnsureTable::Created),
                Err(_create_error) if catalog.table_exists(&ident).await.unwrap_or(false) => {
                    verify_definition(catalog.as_ref(), &ident, table, definition).await
                }
                Err(error) => Err(ClientError::Catalog(error.to_string())),
            }
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
            let commit_key = format!("{idempotency_key}:{}", result.commits);
            let catalog = self.catalog().await?;
            let ident = verglas_iceberg::parse_table_ident(table)
                .map_err(|error| ClientError::Catalog(error.to_string()))?;
            let commit = verglas_iceberg::tables_api::commit_batches(
                catalog.as_ref(),
                &ident,
                vec![batch],
                Some(commit_key),
            )
            .await
            .map_err(|error| ClientError::Catalog(error.to_string()))?;
            result.rows_committed += commit.rows_committed;
            result.commits += 1;
        }
        Ok(result)
    }

    /// Executes SQL and incrementally decodes the Arrow IPC response.
    pub async fn query_stream(&self, sql: &str) -> Result<QueryStream, ClientError> {
        let catalog = self.catalog().await?;
        let execution = verglas_iceberg::query_stream(catalog, sql, None)
            .await
            .map_err(|error| ClientError::Catalog(error.to_string()))?;
        Ok(Box::pin(
            execution
                .batches
                .map_err(|error| ClientError::Catalog(error.to_string())),
        ))
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
            url: feed_url(&self.connection.catalog_uri)?,
            token: self.raw_token.clone(),
            tables,
            cursor,
            socket: None,
            connected_once: false,
            reconnect_delay: None,
            backoff: Duration::from_millis(250),
        };
        Ok(Box::pin(stream::try_unfold(state, follow_next)))
    }

    /// Opens the real Iceberg REST catalog once. Its FileIO remains pinned to
    /// the daemon's S3 endpoint, so the daemon is only the cache passthrough.
    async fn catalog(&self) -> Result<std::sync::Arc<dyn iceberg::Catalog>, ClientError> {
        self.catalog
            .get_or_try_init(|| async {
                verglas_iceberg::catalog::open_catalog(&self.connection)
                    .await
                    .map_err(|error| ClientError::Catalog(error.to_string()))
            })
            .await
            .cloned()
    }
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

fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

async fn verify_definition(
    catalog: &dyn iceberg::Catalog,
    ident: &iceberg::TableIdent,
    table: &str,
    expected: &TableDefinition,
) -> Result<EnsureTable, ClientError> {
    let actual = verglas_iceberg::tables_api::definition(catalog, ident)
        .await
        .map_err(|error| ClientError::Catalog(error.to_string()))?;
    if actual == *expected {
        Ok(EnsureTable::Existing)
    } else {
        Err(ClientError::DefinitionMismatch {
            table: table.to_owned(),
            expected: expected.clone(),
            actual,
        })
    }
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, routing::get};

    use super::*;

    #[tokio::test]
    async fn explicit_catalog_and_cache_skip_daemon_discovery() {
        let client = Client::connect(
            ConnectOptions::new("http://127.0.0.1:1")
                .with_catalog_uri("https://tenant.catalog.verglas.dev")
                .with_warehouse("s3://warehouse/tenant")
                .with_s3_endpoint("http://127.0.0.1:8333"),
        )
        .await
        .expect("fully explicit connection does not contact the daemon");

        assert_eq!(
            client.connection.catalog_uri,
            "https://tenant.catalog.verglas.dev"
        );
        assert_eq!(
            client.connection.s3_endpoint.as_deref(),
            Some("http://127.0.0.1:8333")
        );
    }

    #[tokio::test]
    async fn connect_discovers_catalog_coordinates_from_daemon() {
        let access = LocalAccess {
            s3_endpoint: "http://127.0.0.1:8333".to_owned(),
            catalog_uri: Some("https://tenant.catalog.verglas.dev".to_owned()),
            warehouse: Some("s3://warehouse/tenant".to_owned()),
            region: "auto".to_owned(),
            bucket: Some("warehouse".to_owned()),
            access_key_id: Some("VGKEY".to_owned()),
        };
        let app = Router::new().route(
            ACCESS_PATH,
            get({
                let access = access.clone();
                move || async move { Json(access) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });

        let client = Client::connect(ConnectOptions::new(format!("http://{address}")))
            .await
            .expect("discovered connection");
        assert_eq!(client.connection.catalog_uri, access.catalog_uri.unwrap());
        assert_eq!(client.connection.warehouse, access.warehouse);
        assert_eq!(
            client.connection.s3_endpoint.as_deref(),
            Some(access.s3_endpoint.as_str())
        );
        assert_eq!(client.connection.region, "auto");
        assert_eq!(client.connection.access_key_id.as_deref(), Some("VGKEY"));
    }

    #[test]
    fn follow_uses_catalog_origin() {
        assert_eq!(
            feed_url("https://tenant.catalog.verglas.dev").expect("feed URL"),
            "wss://tenant.catalog.verglas.dev/v1/catalog/feed"
        );
    }
}
