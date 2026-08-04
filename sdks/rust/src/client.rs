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
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use verglas_api::table::{CommitResponse, EnsureTableResponse};

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
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl ConnectOptions {
    /// Creates options for a daemon endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: None,
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
        if let Ok(token) = std::env::var("VERGLAS_TOKEN")
            && !token.is_empty()
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
    raw_token: Option<String>,
    token: Option<HeaderValue>,
    request_timeout: Duration,
    http: reqwest::Client,
}

impl Client {
    /// Constructs a reusable client and connection pool.
    pub fn connect(options: ConnectOptions) -> Result<Self, ClientError> {
        let endpoint = options.endpoint.trim_end_matches('/').to_owned();
        reqwest::Url::parse(&endpoint)
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let raw_token = options.token;
        let token = raw_token
            .clone()
            .map(|token| HeaderValue::from_str(&format!("Bearer {token}")))
            .transpose()
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let http = reqwest::Client::builder()
            .connect_timeout(options.connect_timeout)
            .build()?;
        Ok(Self {
            endpoint,
            raw_token,
            token,
            request_timeout: options.request_timeout,
            http,
        })
    }

    /// Creates a missing table or verifies the exact existing definition.
    pub async fn ensure_table(
        &self,
        table: &str,
        definition: &TableDefinition,
    ) -> Result<EnsureTable, ClientError> {
        let url = self.url(&format!("/v1/tables/{table}"));
        let response = self
            .send(self.authorize(self.http.post(url)).json(definition))
            .await?;
        if response.status() == reqwest::StatusCode::CONFLICT {
            let actual: EnsureTableResponse = response.json().await?;
            Err(ClientError::DefinitionMismatch {
                table: table.to_owned(),
                expected: definition.clone(),
                actual: actual.definition,
            })
        } else {
            let response = Self::require_success(response).await?;
            let result: EnsureTableResponse = response.json().await?;
            Ok(if result.created {
                EnsureTable::Created
            } else {
                EnsureTable::Existing
            })
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
            let url = self.url(&format!("/v1/tables/{table}/commit"));
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
            url: feed_url(&self.endpoint)?,
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

    /// Adds authentication to a request when configured.
    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(token) => request.header(AUTHORIZATION, token.clone()),
            None => request,
        }
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
        let message = response.text().await.unwrap_or_default();
        Err(ClientError::Http { status, message })
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
