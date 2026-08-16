//! Access-token lifecycle wire contracts shared by the SDK and CLI.
//!
//! The access service derives the tenant and parent principal from the bearer
//! credential. Callers describe only the child token's name, audience, expiry,
//! and the grants the actor is allowed to delegate.

use serde::{Deserialize, Serialize};

/// One resource grant delegated to a newly minted access token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessTokenGrant {
    /// Protected resource identifier covered by this delegated grant.
    pub resource_id: String,
    /// Actions the token may perform on the resource.
    pub actions: Vec<String>,
}

impl AccessTokenGrant {
    /// Builds a grant from a resource identifier and a finite action set.
    pub fn new(
        resource_id: impl Into<String>,
        actions: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            resource_id: resource_id.into(),
            actions: actions.into_iter().map(Into::into).collect(),
        }
    }
}

/// Request to mint an independently revocable scoped token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessTokenCreateRequest {
    /// Human-readable label used in inventory and audit views.
    pub name: String,
    /// Runtime audience allowed to present the token.
    pub audience: String,
    /// Optional lifetime in seconds from issuance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_in_seconds: Option<u64>,
    /// Grants delegated from the authenticated actor to the token principal.
    pub grants: Vec<AccessTokenGrant>,
}

impl AccessTokenCreateRequest {
    /// Starts a token request with no expiry or delegated grants.
    pub fn new(name: impl Into<String>, audience: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            audience: audience.into(),
            expires_in_seconds: None,
            grants: Vec::new(),
        }
    }

    /// Sets a finite token lifetime in seconds.
    #[must_use]
    pub fn with_expiration_seconds(mut self, expires_in_seconds: u64) -> Self {
        self.expires_in_seconds = Some(expires_in_seconds);
        self
    }

    /// Adds one resource grant to the token request.
    #[must_use]
    pub fn with_grant(mut self, grant: AccessTokenGrant) -> Self {
        self.grants.push(grant);
        self
    }
}

/// Public metadata for an issued access token. It never contains its secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessTokenSummary {
    /// Stable token identifier used for revocation.
    pub id: String,
    /// Principal that delegated access to this token.
    pub parent_principal_id: String,
    /// Child principal represented when the token is presented.
    pub principal_id: String,
    /// Human-readable token label.
    pub name: String,
    /// Runtime audience allowed to present the token.
    pub audience: String,
    /// Unix timestamp for token issuance.
    pub created_at: i64,
    /// Optional Unix timestamp after which the token cannot authenticate.
    pub expires_at: Option<i64>,
    /// Most recent successful authenticated use, if any.
    pub last_used_at: Option<i64>,
    /// Revocation timestamp, if the token no longer authenticates.
    pub revoked_at: Option<i64>,
}

/// The one-time plaintext credential returned by token creation with its inventory metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuedAccessToken {
    /// Plaintext bearer credential. Persist it securely because it cannot be read again.
    pub token: String,
    #[serde(flatten)]
    /// Non-secret metadata describing the issued token.
    pub summary: AccessTokenSummary,
}
