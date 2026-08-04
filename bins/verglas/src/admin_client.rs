//! Admin API client for talking to the local running `verglasd`.
//!
//! Resolves the daemon base URL from CLI flags or `VERGLAS_ENDPOINT` and
//! performs typed HTTP calls against the private admin surface. The CLI
//! operates on the local node only, so no membership or remote-node calls
//! live here.

use reqwest::StatusCode;
use thiserror::Error;
use verglas_core::admin::{DRAIN_PATH, DrainAck, DrainRequest, VERSION_PATH, VersionInfo};

/// HTTP client for the daemon admin API.
#[derive(Debug, Clone)]
pub struct AdminClient {
    base_url: reqwest::Url,
    http: reqwest::Client,
}

/// Errors surfaced while calling the admin API.
#[derive(Debug, Error)]
pub enum AdminClientError {
    /// The configured endpoint URL could not be parsed.
    #[error("invalid admin endpoint URL: {0}")]
    InvalidEndpoint(String),
    /// The request could not be sent.
    #[error("admin request failed: {0}")]
    RequestFailed(reqwest::Error),
    /// The daemon returned a non-success HTTP status.
    #[error("admin API returned HTTP {status} for {path}")]
    UnexpectedStatus {
        /// HTTP status returned by the daemon.
        status: StatusCode,
        /// Request path that failed.
        path: &'static str,
    },
    /// The response body could not be decoded.
    #[error("failed to decode admin response from {path}: {source}")]
    DecodeFailed {
        /// Request path whose body failed to decode.
        path: &'static str,
        /// Underlying JSON error.
        source: reqwest::Error,
    },
}

impl AdminClient {
    /// Builds a client targeting `endpoint`.
    pub fn new(endpoint: &str) -> Result<Self, AdminClientError> {
        let base_url = reqwest::Url::parse(endpoint)
            .map_err(|error| AdminClientError::InvalidEndpoint(error.to_string()))?;
        let http = reqwest::Client::new();

        Ok(Self { base_url, http })
    }

    /// Fetches daemon version metadata from `GET /admin/version`.
    pub async fn version(&self) -> Result<VersionInfo, AdminClientError> {
        self.get(VERSION_PATH).await
    }

    /// Initiates a graceful drain of the local daemon via
    /// `POST /admin/drain` (#31), returning the daemon's ack.
    pub async fn drain(&self, request: &DrainRequest) -> Result<DrainAck, AdminClientError> {
        self.post(DRAIN_PATH, request).await
    }

    /// Performs a typed JSON `POST` against a path relative to the base URL.
    async fn post<Req, Resp>(
        &self,
        path: &'static str,
        body: &Req,
    ) -> Result<Resp, AdminClientError>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let url = self.base_url.join(path).map_err(|error| {
            AdminClientError::InvalidEndpoint(format!(
                "failed to resolve admin path {path} against {}: {error}",
                self.base_url
            ))
        })?;
        let response = self
            .http
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(AdminClientError::RequestFailed)?;

        let status = response.status();
        if !status.is_success() {
            return Err(AdminClientError::UnexpectedStatus { status, path });
        }

        response
            .json::<Resp>()
            .await
            .map_err(|source| AdminClientError::DecodeFailed { path, source })
    }

    /// Performs a typed JSON `GET` against a path relative to the base URL.
    async fn get<T>(&self, path: &'static str) -> Result<T, AdminClientError>
    where
        T: serde::de::DeserializeOwned,
    {
        let url = self.base_url.join(path).map_err(|error| {
            AdminClientError::InvalidEndpoint(format!(
                "failed to resolve admin path {path} against {}: {error}",
                self.base_url
            ))
        })?;
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(AdminClientError::RequestFailed)?;

        let status = response.status();
        if !status.is_success() {
            return Err(AdminClientError::UnexpectedStatus { status, path });
        }

        response
            .json::<T>()
            .await
            .map_err(|source| AdminClientError::DecodeFailed { path, source })
    }
}
