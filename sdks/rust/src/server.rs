//! Generic JSON and file-ingest transport for server control-plane routes.
//!
//! Typed data-plane consumers should prefer [`crate::Client`]. The CLI also
//! drives registry, graph, lifecycle, and file-ingest routes; this reusable
//! transport keeps all of that HTTP behavior out of the CLI binary.

use std::path::Path;
use std::time::Duration;

use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde::de::DeserializeOwned;
use thiserror::Error;

/// A failure calling the server HTTP API.
#[derive(Debug, Error)]
pub enum ServerError {
    /// No server accepted the connection.
    #[error(
        "no Verglas server reachable at {endpoint}: {detail}. The CLI is a pure client — point it at a running self-hosted server with `--server-endpoint <url>` (or the VERGLAS_ENDPOINT env var), for example a local Docker instance."
    )]
    Unreachable {
        /// Endpoint that was tried.
        endpoint: String,
        /// Transport-level reason.
        detail: String,
    },
    /// The server returned a non-success status.
    #[error("server: {message} (HTTP {status})")]
    Api {
        /// HTTP status code.
        status: u16,
        /// Response body.
        message: String,
    },
    /// A successful response did not match the expected wire shape.
    #[error("failed to decode the server's response: {0}")]
    Decode(String),
    /// Local request input was invalid or unreadable.
    #[error("{0}")]
    Input(String),
}

/// Reusable pooled HTTP client for generic server routes.
#[derive(Debug, Clone)]
pub struct ServerClient {
    base: String,
    http: reqwest::Client,
    bearer: Option<HeaderValue>,
}

impl ServerClient {
    /// Builds a client without making a request.
    pub fn new(endpoint: &str) -> Result<Self, ServerError> {
        Self::new_with_token(endpoint, None)
    }

    /// Builds a client that presents `token` on every server API request.
    pub fn new_with_token(endpoint: &str, token: Option<&str>) -> Result<Self, ServerError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|error| ServerError::Input(format!("building the HTTP client: {error}")))?;
        Ok(Self {
            base: endpoint.trim_end_matches('/').to_owned(),
            http,
            bearer: token
                .map(|token| HeaderValue::from_str(&format!("Bearer {token}")))
                .transpose()
                .map_err(|error| ServerError::Input(format!("invalid bearer token: {error}")))?,
        })
    }

    /// Sends GET and decodes the JSON response.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ServerError> {
        let response = self
            .authorize(self.http.get(self.url(path)))
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
    ) -> Result<T, ServerError> {
        let response = self
            .authorize(self.http.post(self.url(path)))
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
    ) -> Result<T, ServerError> {
        let response = self
            .authorize(self.http.put(self.url(path)))
            .json(body)
            .send()
            .await
            .map_err(|error| self.unreachable(error))?;
        Self::decode(response).await
    }

    /// Sends DELETE and decodes the JSON response.
    pub async fn delete<T: DeserializeOwned>(&self, path: &str) -> Result<T, ServerError> {
        let response = self
            .authorize(self.http.delete(self.url(path)))
            .send()
            .await
            .map_err(|error| self.unreachable(error))?;
        Self::decode(response).await
    }

    /// Streams a CSV, JSONL, or Parquet file to the server ingest route.
    pub async fn ingest<T: DeserializeOwned>(
        &self,
        table: &str,
        source: &Path,
        mode: &str,
        partition_by: Option<&str>,
    ) -> Result<T, ServerError> {
        let format = match source.extension().and_then(|extension| extension.to_str()) {
            Some("csv") => "csv",
            Some("jsonl") => "jsonl",
            Some("parquet") => "parquet",
            _ => {
                return Err(ServerError::Input(format!(
                    "cannot infer a format for `{}`: expected a .csv, .parquet, or .jsonl file",
                    source.display()
                )));
            }
        };
        let bytes = tokio::fs::read(source).await.map_err(|error| {
            ServerError::Input(format!("failed to read `{}`: {error}", source.display()))
        })?;
        let mut url = format!(
            "{}?mode={mode}&format={format}",
            self.url(&format!("/v1/ingest/{table}"))
        );
        if let Some(column) = partition_by {
            url.push_str(&format!("&partition_by={column}"));
        }
        let response = self
            .authorize(self.http.post(url))
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

    /// Adds the configured bearer credential to one outgoing server request.
    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.bearer {
            Some(token) => request.header(AUTHORIZATION, token.clone()),
            None => request,
        }
    }

    /// Maps a transport failure to the endpoint-aware error.
    fn unreachable(&self, error: reqwest::Error) -> ServerError {
        ServerError::Unreachable {
            endpoint: self.base.clone(),
            detail: error.to_string(),
        }
    }

    /// Decodes success JSON or retains the server's error body.
    async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, ServerError> {
        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(ServerError::Api {
                status: status.as_u16(),
                message,
            });
        }
        response
            .json::<T>()
            .await
            .map_err(|error| ServerError::Decode(error.to_string()))
    }
}
