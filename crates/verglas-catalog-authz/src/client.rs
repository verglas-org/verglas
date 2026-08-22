//! Local verification of control-plane-issued Catalog credentials.
//!
//! The control plane is the authorization authority. Catalog keeps a
//! configured JWKS snapshot and never makes a tenant-local policy request while
//! serving a catalog operation.

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use serde::Deserialize;

use crate::VerglasAction;

/// One resource/action permission embedded in a control-plane credential.
#[derive(Debug, Deserialize)]
struct CredentialScope {
    resource: String,
    action: String,
}

/// Claims minted by the Cloudflare control Worker for a data-plane service.
#[derive(Debug, Deserialize)]
struct CredentialClaims {
    tenant_id: String,
    sub: String,
    jti: String,
    scope: Vec<CredentialScope>,
}

/// Trusted result of authorizing a control-plane-issued caller credential.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallerDecision {
    pub allowed: bool,
    pub tenant_id: String,
    pub principal_id: String,
    pub token_id: String,
}

/// Fail-closed errors at the control-plane credential boundary.
#[derive(Debug, thiserror::Error)]
pub enum DecisionClientError {
    #[error("control-plane authorization requires a forwarded caller bearer")]
    MissingCallerBearer,
    #[error("control-plane JWKS is invalid: {0}")]
    Jwks(#[from] serde_json::Error),
    #[error("control-plane credential header is invalid: {0}")]
    Header(#[source] jsonwebtoken::errors::Error),
    #[error("control-plane credential does not identify a signing key")]
    MissingKeyId,
    #[error("control-plane JWKS has no key named {0}")]
    UnknownKeyId(String),
    #[error("control-plane JWKS key cannot verify this credential: {0}")]
    Key(#[source] jsonwebtoken::errors::Error),
    #[error("control-plane credential is invalid: {0}")]
    Credential(#[source] jsonwebtoken::errors::Error),
    #[error("control-plane credential has tenant {actual}, expected {expected}")]
    UnexpectedTenant { expected: String, actual: String },
}

/// Verifies short-lived scoped credentials against the configured control-plane JWKS.
#[derive(Clone, Debug)]
pub struct DecisionClient {
    issuer: String,
    tenant_id: String,
    jwks: JwkSet,
}

impl DecisionClient {
    /// Parses the pinned control-plane JWKS used by this Catalog deployment.
    pub fn new(
        issuer: impl Into<String>,
        jwks: &str,
        tenant_id: impl Into<String>,
    ) -> Result<Self, DecisionClientError> {
        Ok(Self {
            issuer: issuer.into(),
            tenant_id: tenant_id.into(),
            jwks: serde_json::from_str(jwks)?,
        })
    }

    /// Verifies a caller token and evaluates its exact or descendant scope locally.
    pub async fn authorize(
        &self,
        caller_bearer: &str,
        resource_id: &str,
        action: VerglasAction,
    ) -> Result<CallerDecision, DecisionClientError> {
        let header = decode_header(caller_bearer).map_err(DecisionClientError::Header)?;
        let key_id = header.kid.ok_or(DecisionClientError::MissingKeyId)?;
        let jwk = self
            .jwks
            .find(&key_id)
            .ok_or_else(|| DecisionClientError::UnknownKeyId(key_id.clone()))?;
        let key = DecodingKey::from_jwk(jwk).map_err(DecisionClientError::Key)?;
        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_audience(&["catalog"]);
        validation.set_issuer(&[&self.issuer]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        let claims = decode::<CredentialClaims>(caller_bearer, &key, &validation)
            .map_err(DecisionClientError::Credential)?
            .claims;

        if claims.tenant_id != self.tenant_id {
            return Err(DecisionClientError::UnexpectedTenant {
                expected: self.tenant_id.clone(),
                actual: claims.tenant_id,
            });
        }
        let action = action_name(action);
        let allowed = claims.scope.iter().any(|scope| {
            scope_matches(&scope.resource, resource_id)
                && (scope.action == action || scope.action == "admin")
        });

        Ok(CallerDecision {
            allowed,
            tenant_id: self.tenant_id.clone(),
            principal_id: claims.sub,
            token_id: claims.jti,
        })
    }
}

/// Returns the stable string used by Worker-issued credential scopes.
fn action_name(action: VerglasAction) -> &'static str {
    match action {
        VerglasAction::Discover => "discover",
        VerglasAction::Describe => "describe",
        VerglasAction::Query => "query",
        VerglasAction::Modify => "modify",
        VerglasAction::CreateChild => "create_child",
        VerglasAction::Execute => "execute",
        VerglasAction::ManageGrants => "manage_grants",
    }
}

/// Matches either one exact resource or a trailing `/*` descendant grant.
fn scope_matches(scope_resource: &str, resource_id: &str) -> bool {
    if scope_resource == resource_id {
        return true;
    }
    scope_resource
        .strip_suffix("/*")
        .is_some_and(|prefix| resource_id.starts_with(prefix))
}
