//! Signed access-token credentials and their durable active-token registry.
//!
//! A token carries a tenant-scoped child principal in a compact signed envelope.
//! The registry stores only public metadata and revocation state, never a bearer token.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::hmac;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{AuthzError, PrincipalId, ScopedTokenClaims, TenantId, validate_identifier};

/// Stable identifier for one revocable bearer credential.
pub type AccessTokenId = String;

/// A request to mint one signed credential for an already-created child principal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenMintRequest {
    /// Stable credential identity used for registry lookup and revocation.
    pub id: AccessTokenId,
    /// Tenant that owns the token and both principal identities.
    pub tenant_id: TenantId,
    /// Existing user or process that delegated access to the token principal.
    pub parent_principal_id: PrincipalId,
    /// Existing child process principal authenticated by this credential.
    pub principal_id: PrincipalId,
    /// Human-readable label shown in credential inventory.
    pub name: String,
    /// Receiving service that must accept the token.
    pub audience: String,
    /// Policy revision observed before creating the child principal's grants.
    pub policy_version: u64,
    /// Optional bounded execution identity.
    pub run_id: Option<String>,
    /// Unix timestamp at which the credential becomes valid.
    pub issued_at: u64,
    /// Unix timestamp after which the credential is rejected.
    pub expires_at: u64,
}

impl TokenMintRequest {
    /// Constructs a token request with no run binding.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: impl Into<AccessTokenId>,
        tenant_id: impl Into<TenantId>,
        parent_principal_id: impl Into<PrincipalId>,
        principal_id: impl Into<PrincipalId>,
        name: impl Into<String>,
        audience: impl Into<String>,
        policy_version: u64,
        issued_at: u64,
        expires_at: u64,
    ) -> Self {
        Self {
            id: id.into(),
            tenant_id: tenant_id.into(),
            parent_principal_id: parent_principal_id.into(),
            principal_id: principal_id.into(),
            name: name.into(),
            audience: audience.into(),
            policy_version,
            run_id: None,
            issued_at,
            expires_at,
        }
    }

    /// Binds the token to one Job run without changing its durable principal.
    #[must_use]
    pub fn with_run(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Rejects malformed, self-delegating, or already-expired requests.
    pub fn validate(&self) -> Result<(), AuthzError> {
        validate_identifier("token.id", &self.id)?;
        validate_identifier("token.tenant_id", &self.tenant_id)?;
        validate_identifier("token.parent_principal_id", &self.parent_principal_id)?;
        validate_identifier("token.principal_id", &self.principal_id)?;
        validate_identifier("token.audience", &self.audience)?;
        if self.name.trim().is_empty() || self.name.len() > 256 {
            return Err(AuthzError::Invalid(
                "token.name must contain 1 to 256 bytes".to_owned(),
            ));
        }
        if self.parent_principal_id == self.principal_id {
            return Err(AuthzError::Invalid(
                "a token principal must differ from its parent principal".to_owned(),
            ));
        }
        if let Some(run_id) = &self.run_id {
            validate_identifier("token.run_id", run_id)?;
        }
        if self.expires_at <= self.issued_at {
            return Err(AuthzError::Token(
                "expiration must be after issuance".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Public registry metadata that is safe to return from administration APIs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct AccessTokenMetadata {
    /// Stable credential identifier.
    pub id: AccessTokenId,
    /// Tenant that owns the credential.
    pub tenant_id: TenantId,
    /// Child process principal authenticated by the token.
    pub principal_id: PrincipalId,
    /// Principal that created or delegated to the child identity.
    pub parent_principal_id: PrincipalId,
    /// Human-readable label for credential inventory.
    pub name: String,
    /// Receiving service that can accept the credential.
    pub audience: String,
    /// Policy revision observed while issuing the credential.
    pub policy_version: u64,
    /// Optional run binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Unix timestamp at which the credential was created.
    pub created_at: u64,
    /// Unix timestamp after which the credential is rejected.
    pub expires_at: u64,
    /// Most recent successful authenticated use, when observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<u64>,
    /// Revocation timestamp. A present value always denies authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<u64>,
}

impl AccessTokenMetadata {
    /// Converts a validated mint request into registry-only metadata.
    fn from_request(request: &TokenMintRequest) -> Self {
        Self {
            id: request.id.clone(),
            tenant_id: request.tenant_id.clone(),
            principal_id: request.principal_id.clone(),
            parent_principal_id: request.parent_principal_id.clone(),
            name: request.name.clone(),
            audience: request.audience.clone(),
            policy_version: request.policy_version,
            run_id: request.run_id.clone(),
            created_at: request.issued_at,
            expires_at: request.expires_at,
            last_used_at: None,
            revoked_at: None,
        }
    }

    /// Rejects malformed metadata returned by a durable registry.
    pub fn validate(&self) -> Result<(), AuthzError> {
        TokenMintRequest {
            id: self.id.clone(),
            tenant_id: self.tenant_id.clone(),
            parent_principal_id: self.parent_principal_id.clone(),
            principal_id: self.principal_id.clone(),
            name: self.name.clone(),
            audience: self.audience.clone(),
            policy_version: self.policy_version,
            run_id: self.run_id.clone(),
            issued_at: self.created_at,
            expires_at: self.expires_at,
        }
        .validate()?;
        if let Some(last_used_at) = self.last_used_at
            && (last_used_at < self.created_at || last_used_at > self.expires_at)
        {
            return Err(AuthzError::Backend(
                "token registry has an invalid last-used timestamp".to_owned(),
            ));
        }
        if let Some(revoked_at) = self.revoked_at
            && revoked_at < self.created_at
        {
            return Err(AuthzError::Backend(
                "token registry has an invalid revocation timestamp".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Claims in the signed bearer envelope.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct SignedAccessTokenClaims {
    /// Stable credential identity required for durable active-state lookup.
    token_id: AccessTokenId,
    /// Tenant boundary enforced by every receiving service.
    tenant_id: TenantId,
    /// Child process principal authenticated by the token.
    principal_id: PrincipalId,
    /// Intended data plane or service.
    audience: String,
    /// Policy revision observed at issuance.
    policy_version: u64,
    /// Optional individual run identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    run_id: Option<String>,
    /// Unix issuance timestamp.
    issued_at: u64,
    /// Unix expiration timestamp.
    expires_at: u64,
}

/// A bearer token that deliberately omits serialization and debug disclosure.
pub struct SecretAccessToken(String);

impl SecretAccessToken {
    /// Returns the bearer value only for immediate delivery to its caller.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretAccessToken {
    /// Redacts bearer material from diagnostic output.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretAccessToken([REDACTED])")
    }
}

/// The one-time bearer material and its separately durable public metadata.
#[derive(Debug)]
pub struct MintedAccessToken {
    /// Bearer material that must be displayed once and never persisted by Verglas.
    pub token: SecretAccessToken,
    /// Public metadata persisted by the token registry.
    pub metadata: AccessTokenMetadata,
}

/// HMAC-SHA256 signer for compact self-contained access tokens.
#[derive(Clone)]
pub struct AccessTokenSigner {
    key: [u8; 32],
}

impl AccessTokenSigner {
    /// Constructs a signer from exactly 256 bits of operator-held secret material.
    #[must_use]
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Decodes exactly 32 bytes of standard base64 operator-held signing material.
    pub fn from_base64(encoded: &str) -> Result<Self, AuthzError> {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| AuthzError::Invalid("token signing key must be base64".to_owned()))?;
        let key: [u8; 32] = decoded.try_into().map_err(|_| {
            AuthzError::Invalid("token signing key must decode to exactly 32 bytes".to_owned())
        })?;
        Ok(Self::new(key))
    }

    /// Signs a validated request without persisting bearer material.
    pub fn mint(&self, request: &TokenMintRequest) -> Result<SecretAccessToken, AuthzError> {
        request.validate()?;
        let claims = SignedAccessTokenClaims {
            token_id: request.id.clone(),
            tenant_id: request.tenant_id.clone(),
            principal_id: request.principal_id.clone(),
            audience: request.audience.clone(),
            policy_version: request.policy_version,
            run_id: request.run_id.clone(),
            issued_at: request.issued_at,
            expires_at: request.expires_at,
        };
        let payload = serde_json::to_vec(&claims)
            .map_err(|error| AuthzError::Token(format!("could not encode claims: {error}")))?;
        let encoded_payload = URL_SAFE_NO_PAD.encode(payload);
        let signed = format!("vgt1.{encoded_payload}");
        let signature = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, &self.key),
            signed.as_bytes(),
        );
        Ok(SecretAccessToken(format!(
            "{signed}.{}",
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        )))
    }

    /// Verifies a token envelope and validates its service boundary and lifetime.
    pub fn verify(
        &self,
        raw: &str,
        expected_tenant: &str,
        expected_audience: &str,
        now: u64,
    ) -> Result<ScopedTokenClaims, AuthzError> {
        let (prefix, encoded_payload, encoded_signature) = split_token(raw)?;
        if prefix != "vgt1" {
            return Err(AuthzError::Token("unsupported token format".to_owned()));
        }
        let signature = URL_SAFE_NO_PAD
            .decode(encoded_signature)
            .map_err(|_| AuthzError::Token("token signature is malformed".to_owned()))?;
        let signed = format!("{prefix}.{encoded_payload}");
        hmac::verify(
            &hmac::Key::new(hmac::HMAC_SHA256, &self.key),
            signed.as_bytes(),
            &signature,
        )
        .map_err(|_| AuthzError::Token("token signature is invalid".to_owned()))?;
        let payload = URL_SAFE_NO_PAD
            .decode(encoded_payload)
            .map_err(|_| AuthzError::Token("token payload is malformed".to_owned()))?;
        let claims: SignedAccessTokenClaims = serde_json::from_slice(&payload)
            .map_err(|_| AuthzError::Token("token payload is invalid".to_owned()))?;
        let claims = ScopedTokenClaims {
            token_id: claims.token_id,
            tenant_id: claims.tenant_id,
            principal_id: claims.principal_id,
            audience: claims.audience,
            policy_version: claims.policy_version,
            run_id: claims.run_id,
            issued_at: claims.issued_at,
            expires_at: claims.expires_at,
        };
        claims.validate(expected_tenant, expected_audience, now)?;
        Ok(claims)
    }
}

impl fmt::Debug for AccessTokenSigner {
    /// Redacts signing material from diagnostic output.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AccessTokenSigner([REDACTED])")
    }
}

/// Creates a cryptographically random token identifier suitable for a child principal name.
#[must_use]
pub fn new_access_token_id() -> AccessTokenId {
    uuid::Uuid::new_v4().to_string()
}

/// Durable active-token state. Implementations never receive bearer material.
#[async_trait]
pub trait AccessTokenRegistry: Send + Sync {
    /// Persists public token metadata before the signed credential is returned.
    async fn create_token(
        &self,
        metadata: AccessTokenMetadata,
    ) -> Result<AccessTokenMetadata, AuthzError>;
    /// Returns metadata for one tenant-scoped token ID.
    async fn get_token(
        &self,
        tenant_id: &str,
        token_id: &str,
    ) -> Result<AccessTokenMetadata, AuthzError>;
    /// Lists the credential inventory delegated from one parent principal.
    async fn list_tokens(
        &self,
        tenant_id: &str,
        parent_principal_id: &str,
    ) -> Result<Vec<AccessTokenMetadata>, AuthzError>;
    /// Records a one-way token revocation and returns the updated metadata.
    async fn revoke_token(
        &self,
        tenant_id: &str,
        token_id: &str,
        revoked_at: u64,
    ) -> Result<AccessTokenMetadata, AuthzError>;
    /// Records the most recent successful authenticated use without changing validity.
    async fn record_token_use(
        &self,
        tenant_id: &str,
        token_id: &str,
        used_at: u64,
    ) -> Result<(), AuthzError>;
}

/// Signing and durable-active-state service used by REST and data-plane middleware.
#[derive(Clone)]
pub struct AccessTokenService {
    signer: AccessTokenSigner,
    registry: Arc<dyn AccessTokenRegistry>,
}

impl AccessTokenService {
    /// Creates a cloneable token service around one signing key and durable registry.
    pub fn new(signer: AccessTokenSigner, registry: Arc<dyn AccessTokenRegistry>) -> Self {
        Self { signer, registry }
    }

    /// Signs and durably registers metadata before bearer material leaves the service.
    pub async fn mint(&self, request: TokenMintRequest) -> Result<MintedAccessToken, AuthzError> {
        request.validate()?;
        let token = self.signer.mint(&request)?;
        let metadata = self
            .registry
            .create_token(AccessTokenMetadata::from_request(&request))
            .await?;
        Ok(MintedAccessToken { token, metadata })
    }

    /// Verifies a signed bearer token, rejects inactive registry state, and records use.
    pub async fn authenticate(
        &self,
        raw: &str,
        expected_tenant: &str,
        expected_audience: &str,
        now: u64,
    ) -> Result<ScopedTokenClaims, AuthzError> {
        let claims = self
            .signer
            .verify(raw, expected_tenant, expected_audience, now)?;
        let metadata = self
            .registry
            .get_token(&claims.tenant_id, &claims.token_id)
            .await
            .map_err(token_registry_error)?;
        metadata.validate().map_err(token_registry_error)?;
        if metadata.revoked_at.is_some()
            || metadata.tenant_id != claims.tenant_id
            || metadata.principal_id != claims.principal_id
            || metadata.audience != claims.audience
            || metadata.policy_version != claims.policy_version
            || metadata.run_id != claims.run_id
            || metadata.created_at != claims.issued_at
            || metadata.expires_at != claims.expires_at
        {
            return Err(AuthzError::Token(
                "token is inactive or does not match its registry metadata".to_owned(),
            ));
        }
        self.registry
            .record_token_use(&claims.tenant_id, &claims.token_id, now)
            .await
            .map_err(token_registry_error)?;
        Ok(claims)
    }

    /// Returns public metadata for credentials delegated from one principal.
    pub async fn list(
        &self,
        tenant_id: &str,
        parent_principal_id: &str,
    ) -> Result<Vec<AccessTokenMetadata>, AuthzError> {
        self.registry
            .list_tokens(tenant_id, parent_principal_id)
            .await
    }

    /// Returns public metadata for one token so callers can enforce owner-only lifecycle actions.
    pub async fn get(
        &self,
        tenant_id: &str,
        token_id: &str,
    ) -> Result<AccessTokenMetadata, AuthzError> {
        self.registry.get_token(tenant_id, token_id).await
    }

    /// Marks one credential inactive without deleting its audit metadata.
    pub async fn revoke(
        &self,
        tenant_id: &str,
        token_id: &str,
        revoked_at: u64,
    ) -> Result<AccessTokenMetadata, AuthzError> {
        self.registry
            .revoke_token(tenant_id, token_id, revoked_at)
            .await
    }
}

/// Maps registry absence and corruption into an authentication failure.
fn token_registry_error(error: AuthzError) -> AuthzError {
    match error {
        AuthzError::NotFound(_) | AuthzError::Invalid(_) | AuthzError::Conflict(_) => {
            AuthzError::Token("token is not active".to_owned())
        }
        other => other,
    }
}

/// Splits one bounded compact token without accepting additional segments.
fn split_token(raw: &str) -> Result<(&str, &str, &str), AuthzError> {
    if raw.len() > 16 * 1024 {
        return Err(AuthzError::Token("token exceeds the size limit".to_owned()));
    }
    let mut segments = raw.split('.');
    let prefix = segments
        .next()
        .ok_or_else(|| AuthzError::Token("token is malformed".to_owned()))?;
    let payload = segments
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AuthzError::Token("token payload is missing".to_owned()))?;
    let signature = segments
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AuthzError::Token("token signature is missing".to_owned()))?;
    if segments.next().is_some() {
        return Err(AuthzError::Token("token has too many segments".to_owned()));
    }
    Ok((prefix, payload, signature))
}

/// In-memory durable-registry substitute for contract tests and embedded callers.
#[derive(Debug, Clone, Default)]
pub struct MemoryAccessTokenRegistry {
    tokens: Arc<RwLock<BTreeMap<(TenantId, AccessTokenId), AccessTokenMetadata>>>,
}

impl MemoryAccessTokenRegistry {
    /// Creates an empty non-durable registry for tests.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl AccessTokenRegistry for MemoryAccessTokenRegistry {
    /// Stores metadata after validating that no plaintext token is present in the contract.
    async fn create_token(
        &self,
        metadata: AccessTokenMetadata,
    ) -> Result<AccessTokenMetadata, AuthzError> {
        metadata.validate()?;
        let key = (metadata.tenant_id.clone(), metadata.id.clone());
        let mut tokens = self.tokens.write().await;
        if tokens.contains_key(&key) {
            return Err(AuthzError::Conflict(format!(
                "token {} already exists",
                metadata.id
            )));
        }
        tokens.insert(key, metadata.clone());
        Ok(metadata)
    }

    /// Loads metadata by tenant and token identity.
    async fn get_token(
        &self,
        tenant_id: &str,
        token_id: &str,
    ) -> Result<AccessTokenMetadata, AuthzError> {
        self.tokens
            .read()
            .await
            .get(&(tenant_id.to_owned(), token_id.to_owned()))
            .cloned()
            .ok_or_else(|| AuthzError::NotFound(format!("token {token_id}")))
    }

    /// Lists one parent's tokens in deterministic identifier order.
    async fn list_tokens(
        &self,
        tenant_id: &str,
        parent_principal_id: &str,
    ) -> Result<Vec<AccessTokenMetadata>, AuthzError> {
        Ok(self
            .tokens
            .read()
            .await
            .iter()
            .filter(|((tenant, _), metadata)| {
                tenant == tenant_id && metadata.parent_principal_id == parent_principal_id
            })
            .map(|(_, metadata)| metadata.clone())
            .collect())
    }

    /// Sets revocation once and rejects attempts to overwrite audit timing.
    async fn revoke_token(
        &self,
        tenant_id: &str,
        token_id: &str,
        revoked_at: u64,
    ) -> Result<AccessTokenMetadata, AuthzError> {
        let mut tokens = self.tokens.write().await;
        let metadata = tokens
            .get_mut(&(tenant_id.to_owned(), token_id.to_owned()))
            .ok_or_else(|| AuthzError::NotFound(format!("token {token_id}")))?;
        if let Some(existing) = metadata.revoked_at {
            if existing != revoked_at {
                return Err(AuthzError::Conflict(format!(
                    "token {token_id} is already revoked"
                )));
            }
            return Ok(metadata.clone());
        }
        if revoked_at < metadata.created_at {
            return Err(AuthzError::Invalid(
                "token revocation cannot precede creation".to_owned(),
            ));
        }
        metadata.revoked_at = Some(revoked_at);
        Ok(metadata.clone())
    }

    /// Advances last-use time monotonically for a valid active token.
    async fn record_token_use(
        &self,
        tenant_id: &str,
        token_id: &str,
        used_at: u64,
    ) -> Result<(), AuthzError> {
        let mut tokens = self.tokens.write().await;
        let metadata = tokens
            .get_mut(&(tenant_id.to_owned(), token_id.to_owned()))
            .ok_or_else(|| AuthzError::NotFound(format!("token {token_id}")))?;
        if metadata.revoked_at.is_some()
            || used_at < metadata.created_at
            || used_at > metadata.expires_at
        {
            return Err(AuthzError::Token("token is not active".to_owned()));
        }
        metadata.last_used_at = Some(
            metadata
                .last_used_at
                .map_or(used_at, |previous| previous.max(used_at)),
        );
        Ok(())
    }
}
