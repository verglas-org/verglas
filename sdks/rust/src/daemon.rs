//! Generic JSON and file-ingest transport for daemon control-plane routes.
//!
//! Typed data-plane consumers should prefer [`crate::Client`]. The CLI also
//! drives registry, graph, lifecycle, and file-ingest routes; this reusable
//! transport keeps all of that HTTP behavior out of the CLI binary.

use std::path::Path;
use std::time::Duration;

use serde::de::DeserializeOwned;
use thiserror::Error;

/// A failure calling the daemon HTTP API.
#[derive(Debug, Error)]
pub enum DaemonError {
    /// No daemon accepted the connection.
    #[error(
        "no Verglas daemon reachable at {endpoint}: {detail}. The CLI is a pure client — point it at a running daemon with `--daemon-endpoint <url>` (or the VERGLAS_ENDPOINT env var), which may be a REMOTE daemon on another host or a cloud node, or start a local one with `verglas dev` (or `verglas start`). A local daemon is for edge/read latency, never required to use the platform."
    )]
    Unreachable {
        /// Endpoint that was tried.
        endpoint: String,
        /// Transport-level reason.
        detail: String,
    },
    /// The daemon returned a non-success status.
    #[error("daemon: {message} (HTTP {status})")]
    Api {
        /// HTTP status code.
        status: u16,
        /// Response body.
        message: String,
    },
    /// A successful response did not match the expected wire shape.
    #[error("failed to decode the daemon's response: {0}")]
    Decode(String),
    /// Local request input was invalid or unreadable.
    #[error("{0}")]
    Input(String),
}

/// Reusable pooled HTTP client for generic daemon routes.
#[derive(Debug, Clone)]
pub struct DaemonClient {
    base: String,
    http: reqwest::Client,
}

impl DaemonClient {
    /// Builds a client without making a request.
    pub fn new(endpoint: &str) -> Result<Self, DaemonError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|error| DaemonError::Input(format!("building the HTTP client: {error}")))?;
        Ok(Self {
            base: endpoint.trim_end_matches('/').to_owned(),
            http,
        })
    }

    /// Sends GET and decodes the JSON response.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, DaemonError> {
        let response = self
            .http
            .get(self.url(path))
            .send()
            .await
            .map_err(|error| self.unreachable(error))?;
        Self::decode(response).await
    }

    /// Sends a JSON POST and decodes the JSON response.
    pub async fn post_json<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<T, DaemonError> {
        let response = self
            .http
            .post(self.url(path))
            .json(body)
            .send()
            .await
            .map_err(|error| self.unreachable(error))?;
        Self::decode(response).await
    }

    /// Sends a JSON PUT and decodes the JSON response.
    pub async fn put_json<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<T, DaemonError> {
        let response = self
            .http
            .put(self.url(path))
            .json(body)
            .send()
            .await
            .map_err(|error| self.unreachable(error))?;
        Self::decode(response).await
    }

    /// Streams a CSV, JSONL, or Parquet file to the daemon ingest route.
    pub async fn ingest<T: DeserializeOwned>(
        &self,
        table: &str,
        source: &Path,
        mode: &str,
        partition_by: Option<&str>,
    ) -> Result<T, DaemonError> {
        let format = match source.extension().and_then(|extension| extension.to_str()) {
            Some("csv") => "csv",
            Some("jsonl") => "jsonl",
            Some("parquet") => "parquet",
            _ => {
                return Err(DaemonError::Input(format!(
                    "cannot infer a format for `{}`: expected a .csv, .parquet, or .jsonl file",
                    source.display()
                )));
            }
        };
        let bytes = tokio::fs::read(source).await.map_err(|error| {
            DaemonError::Input(format!("failed to read `{}`: {error}", source.display()))
        })?;
        let mut url = format!(
            "{}?mode={mode}&format={format}",
            self.url(&format!("/v1/tables/{table}/ingest"))
        );
        if let Some(column) = partition_by {
            url.push_str(&format!("&partition_by={column}"));
        }
        let response = self
            .http
            .post(url)
            .header("content-type", "application/octet-stream")
            .body(bytes)
            .send()
            .await
            .map_err(|error| self.unreachable(error))?;
        Self::decode(response).await
    }

    /// Joins an API path to the configured base endpoint.
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Maps a transport failure to the endpoint-aware error.
    fn unreachable(&self, error: reqwest::Error) -> DaemonError {
        DaemonError::Unreachable {
            endpoint: self.base.clone(),
            detail: error.to_string(),
        }
    }

    /// Decodes success JSON or retains the daemon's error body.
    async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, DaemonError> {
        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(DaemonError::Api {
                status: status.as_u16(),
                message,
            });
        }
        response
            .json::<T>()
            .await
            .map_err(|error| DaemonError::Decode(error.to_string()))
    }
}
