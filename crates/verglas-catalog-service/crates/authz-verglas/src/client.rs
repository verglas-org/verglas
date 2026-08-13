use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::VerglasAction;

/// Resource kinds accepted by the private Verglas policy synchronization API.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncResourceKind {
    Project,
    Warehouse,
    Namespace,
    Table,
    View,
    GenericTable,
    Role,
    Tag,
}

/// One exact resource projection owned by Lakekeeper.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResourceSync {
    pub id: String,
    pub kind: SyncResourceKind,
    pub parent_id: String,
}

impl ResourceSync {
    /// Constructs one resource projection with its stable authorization parent.
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        kind: SyncResourceKind,
        parent_id: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            kind,
            parent_id: parent_id.into(),
        }
    }
}

/// One caller-authenticated Verglas authorization question.
#[derive(Debug, Serialize)]
struct AccessAuthorize<'a> {
    audience: &'static str,
    resource_id: &'a str,
    action: VerglasAction,
}

/// Trusted caller identity returned by Verglas access authorization.
#[derive(Debug, Deserialize)]
struct AccessIdentity {
    tenant_id: String,
    principal_id: String,
    token_id: String,
    audience: String,
}

#[derive(Debug, Deserialize)]
struct AccessDecisionBody {
    allowed: bool,
}

#[derive(Debug, Deserialize)]
struct AccessAuthorizeResponse {
    identity: AccessIdentity,
    decision: AccessDecisionBody,
}

/// Trusted result of authorizing an opaque caller bearer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallerDecision {
    pub allowed: bool,
    pub tenant_id: String,
    pub principal_id: String,
    pub token_id: String,
}

/// Fail-closed errors from the Verglas decision boundary.
#[derive(Debug, thiserror::Error)]
pub enum DecisionClientError {
    #[error("Verglas authorization requires a forwarded caller bearer")]
    MissingCallerBearer,
    #[error("Verglas authorization returned tenant {actual}, expected {expected}")]
    UnexpectedTenant { expected: String, actual: String },
    #[error("invalid Verglas decision endpoint: {0}")]
    Endpoint(#[from] url::ParseError),
    #[error("failed to read Verglas workload credential file {path}: {source}")]
    Credential {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Verglas workload credential file {0} is empty")]
    EmptyCredential(PathBuf),
    #[error("Verglas decision request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("Verglas decision service returned HTTP {0}")]
    Status(reqwest::StatusCode),
    #[error("Verglas authorization response has audience {0}, expected data-plane")]
    Audience(String),
}

/// HTTP client for caller decisions and private policy lifecycle operations.
#[derive(Clone, Debug)]
pub struct DecisionClient {
    authorize_endpoint: Url,
    resource_sync_endpoint: Url,
    credential_file: PathBuf,
    http: reqwest::Client,
}

impl DecisionClient {
    /// Creates a client and fixes access paths relative to the configured base URL.
    pub fn new(
        endpoint: Url,
        credential_file: impl Into<PathBuf>,
    ) -> Result<Self, DecisionClientError> {
        Ok(Self {
            authorize_endpoint: endpoint.join("v1/access/authorize")?,
            resource_sync_endpoint: endpoint.join("v1/access/policy/resources")?,
            credential_file: credential_file.into(),
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }

    /// Evaluates one caller-bearer/resource/action tuple and fails closed.
    pub async fn authorize(
        &self,
        caller_bearer: &str,
        resource_id: &str,
        action: VerglasAction,
    ) -> Result<CallerDecision, DecisionClientError> {
        let response = self
            .http
            .post(self.authorize_endpoint.clone())
            .bearer_auth(caller_bearer)
            .json(&AccessAuthorize {
                audience: "data-plane",
                resource_id,
                action,
            })
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(DecisionClientError::Status(response.status()));
        }
        let response = response.json::<AccessAuthorizeResponse>().await?;
        if response.identity.audience != "data-plane" {
            return Err(DecisionClientError::Audience(response.identity.audience));
        }
        Ok(CallerDecision {
            allowed: response.decision.allowed,
            tenant_id: response.identity.tenant_id,
            principal_id: response.identity.principal_id,
            token_id: response.identity.token_id,
        })
    }

    /// Idempotently creates one exact Lakekeeper-owned resource projection.
    pub async fn upsert_resource(
        &self,
        resource: &ResourceSync,
    ) -> Result<(), DecisionClientError> {
        self.send_sync(
            self.http
                .post(self.resource_sync_endpoint.clone())
                .json(resource),
        )
        .await
    }

    /// Idempotently deletes one Lakekeeper-owned resource projection.
    pub async fn delete_resource(&self, id: &str) -> Result<(), DecisionClientError> {
        self.send_sync(
            self.http
                .delete(item_url(&self.resource_sync_endpoint, id)?),
        )
        .await
    }

    async fn send_sync(&self, request: reqwest::RequestBuilder) -> Result<(), DecisionClientError> {
        let token = read_credential(&self.credential_file).await?;
        let response = request.bearer_auth(token).send().await?;
        if !response.status().is_success() {
            return Err(DecisionClientError::Status(response.status()));
        }
        Ok(())
    }
}

fn item_url(collection: &Url, id: &str) -> Result<Url, DecisionClientError> {
    Ok(Url::parse(&format!(
        "{}/{}",
        collection.as_str().trim_end_matches('/'),
        urlencoding::encode(id)
    ))?)
}

async fn read_credential(path: &Path) -> Result<String, DecisionClientError> {
    let token = tokio::fs::read_to_string(path).await.map_err(|source| {
        DecisionClientError::Credential {
            path: path.to_owned(),
            source,
        }
    })?;
    let token = token.trim();
    if token.is_empty() {
        return Err(DecisionClientError::EmptyCredential(path.to_owned()));
    }
    Ok(token.to_owned())
}
