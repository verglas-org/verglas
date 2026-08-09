//! Typed secret contracts, authorization-gated resolution, and encryption boundaries.
//!
//! Repositories receive only ciphertext. The service registers each secret as an
//! authorization resource and reveals plaintext only through an authorized resolution.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use url::Url;

use crate::{AccessCheck, Action, Authorizer, AuthzError, Grant, Resource, ResourceKind};

/// Provider contract associated with one secret value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecretKind {
    /// Credentials for an S3-compatible object-store scope.
    S3,
    /// Credentials for an Iceberg REST catalog scope.
    IcebergRest,
}

/// Public secret metadata that never contains secret material.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SecretMetadata {
    /// Tenant that owns the secret.
    pub tenant_id: String,
    /// Stable authorization resource identity.
    pub id: String,
    /// Provider contract associated with the value.
    pub kind: SecretKind,
    /// Canonical URI prefix on which the value may be used.
    pub scope: String,
    /// Latest committed value version.
    pub current_version: u64,
    /// Authorization category registered for this object.
    pub resource_kind: ResourceKind,
}

/// Input for creating one stable secret resource and its first value.
pub struct CreateSecret {
    /// Tenant that will own the secret.
    pub tenant_id: String,
    /// Principal that owns and can immediately use the new secret.
    pub principal_id: String,
    /// Stable resource identifier.
    pub id: String,
    /// Provider contract associated with the value.
    pub kind: SecretKind,
    /// URI prefix on which the value may be used.
    pub scope: String,
    /// Plaintext held only until encryption completes.
    pub value: Vec<u8>,
}

impl CreateSecret {
    /// Constructs a secret-creation request without putting the value in a serializable type.
    pub fn new(
        tenant_id: impl Into<String>,
        principal_id: impl Into<String>,
        id: impl Into<String>,
        kind: SecretKind,
        scope: impl Into<String>,
        value: impl AsRef<[u8]>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            principal_id: principal_id.into(),
            id: id.into(),
            kind,
            scope: scope.into(),
            value: value.as_ref().to_vec(),
        }
    }
}

/// Input for rotating one existing secret without changing its identity or scope.
pub struct ReplaceSecret {
    /// Tenant that owns the secret.
    pub tenant_id: String,
    /// Principal requesting rotation.
    pub principal_id: String,
    /// Stable secret resource identifier.
    pub id: String,
    /// New plaintext held only until encryption completes.
    pub value: Vec<u8>,
}

impl ReplaceSecret {
    /// Constructs an authorization-bound replacement request.
    pub fn new(
        tenant_id: impl Into<String>,
        principal_id: impl Into<String>,
        id: impl Into<String>,
        value: impl AsRef<[u8]>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            principal_id: principal_id.into(),
            id: id.into(),
            value: value.as_ref().to_vec(),
        }
    }
}

/// Input for resolving the most-specific authorized secret for one URI.
#[derive(Debug, Clone)]
pub struct ResolveSecret {
    /// Tenant in which resolution occurs.
    pub tenant_id: String,
    /// Principal that will use the secret.
    pub principal_id: String,
    /// Required provider contract.
    pub kind: SecretKind,
    /// Exact target URI the caller needs to access.
    pub uri: String,
}

impl ResolveSecret {
    /// Constructs one fail-closed scope-resolution request.
    pub fn new(
        tenant_id: impl Into<String>,
        principal_id: impl Into<String>,
        kind: SecretKind,
        uri: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            principal_id: principal_id.into(),
            kind,
            uri: uri.into(),
        }
    }
}

/// Authorized plaintext returned only to a trusted runtime resolution call.
pub struct ResolvedSecret {
    /// Stable secret resource used by the binding.
    pub resource_id: String,
    /// Exact value version returned.
    pub version: u64,
    /// Canonical scope that matched the requested URI.
    pub scope: String,
    value: Vec<u8>,
}

impl ResolvedSecret {
    /// Exposes plaintext to the authorized in-process consumer.
    pub fn expose(&self) -> &[u8] {
        &self.value
    }
}

/// Bounded failures from secret lifecycle and resolution operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SecretError {
    /// A request contains an invalid identifier, URI, type, or empty value.
    #[error("invalid secret request: {0}")]
    Invalid(String),
    /// No secret exists for the requested resource or scope.
    #[error("secret not found: {0}")]
    NotFound(String),
    /// Multiple equally specific authorized secrets make resolution unsafe.
    #[error("secret conflict: {0}")]
    Conflict(String),
    /// The principal cannot perform the requested secret operation.
    #[error("secret access forbidden: {0}")]
    Forbidden(String),
    /// Encryption, persistence, or authorization infrastructure failed.
    #[error("secret backend failed: {0}")]
    Backend(String),
}

impl From<AuthzError> for SecretError {
    /// Preserves stable authorization semantics without exposing backend state.
    fn from(error: AuthzError) -> Self {
        match error {
            AuthzError::Invalid(message) | AuthzError::Token(message) => Self::Invalid(message),
            AuthzError::NotFound(message) => Self::NotFound(message),
            AuthzError::Conflict(message) => Self::Conflict(message),
            AuthzError::Forbidden(message) => Self::Forbidden(message),
            AuthzError::Backend(message) => Self::Backend(message),
        }
    }
}

/// Encryption boundary used before values reach durable persistence.
pub trait SecretCipher: Send + Sync {
    /// Seals plaintext into an opaque authenticated envelope.
    fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, SecretError>;
    /// Opens one authenticated envelope for an authorized consumer.
    fn open(&self, ciphertext: &[u8]) -> Result<Vec<u8>, SecretError>;
}

/// AES-256-GCM cipher using a fresh random nonce for each value version.
pub struct AeadSecretCipher {
    key: LessSafeKey,
    random: SystemRandom,
}

impl AeadSecretCipher {
    /// Constructs a cipher from an exact 256-bit platform-owned key.
    pub fn new(key: &[u8]) -> Result<Self, SecretError> {
        let key = UnboundKey::new(&AES_256_GCM, key)
            .map_err(|_| SecretError::Invalid("encryption key must contain 32 bytes".to_owned()))?;
        Ok(Self {
            key: LessSafeKey::new(key),
            random: SystemRandom::new(),
        })
    }
}

impl SecretCipher for AeadSecretCipher {
    /// Prepends a random 96-bit nonce to an authenticated ciphertext.
    fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, SecretError> {
        if plaintext.is_empty() {
            return Err(SecretError::Invalid(
                "secret value must not be empty".to_owned(),
            ));
        }
        let mut nonce = [0_u8; 12];
        self.random
            .fill(&mut nonce)
            .map_err(|_| SecretError::Backend("secure random source unavailable".to_owned()))?;
        let mut envelope = nonce.to_vec();
        let mut body = plaintext.to_vec();
        self.key
            .seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce), Aad::empty(), &mut body)
            .map_err(|_| SecretError::Backend("secret encryption failed".to_owned()))?;
        envelope.extend_from_slice(&body);
        Ok(envelope)
    }

    /// Authenticates and decrypts one nonce-prefixed envelope.
    fn open(&self, ciphertext: &[u8]) -> Result<Vec<u8>, SecretError> {
        if ciphertext.len() < 12 {
            return Err(SecretError::Backend(
                "encrypted secret is truncated".to_owned(),
            ));
        }
        let nonce: [u8; 12] = ciphertext[..12]
            .try_into()
            .map_err(|_| SecretError::Backend("encrypted secret nonce is invalid".to_owned()))?;
        let mut body = ciphertext[12..].to_vec();
        let plaintext = self
            .key
            .open_in_place(Nonce::assume_unique_for_key(nonce), Aad::empty(), &mut body)
            .map_err(|_| {
                SecretError::Backend("encrypted secret authentication failed".to_owned())
            })?;
        Ok(plaintext.to_vec())
    }
}

/// One sealed current value returned by a repository lookup.
pub struct StoredSecret {
    /// Public metadata for the resource and current version.
    pub metadata: SecretMetadata,
    /// Opaque authenticated ciphertext.
    pub ciphertext: Vec<u8>,
}

/// Persistence boundary for encrypted, versioned secret values.
#[async_trait]
pub trait SecretRepository: Send + Sync {
    /// Creates metadata and its first encrypted value atomically.
    async fn create(
        &self,
        metadata: SecretMetadata,
        ciphertext: Vec<u8>,
    ) -> Result<SecretMetadata, SecretError>;
    /// Appends a value version and advances the current pointer atomically.
    async fn replace(
        &self,
        tenant_id: &str,
        id: &str,
        ciphertext: Vec<u8>,
    ) -> Result<SecretMetadata, SecretError>;
    /// Returns public metadata without value bytes.
    async fn get(&self, tenant_id: &str, id: &str) -> Result<SecretMetadata, SecretError>;
    /// Lists public metadata without value bytes.
    async fn list(&self, tenant_id: &str) -> Result<Vec<SecretMetadata>, SecretError>;
    /// Returns one stable resource's sealed current value.
    async fn current(&self, tenant_id: &str, id: &str) -> Result<StoredSecret, SecretError>;
    /// Returns sealed current values that match a provider type.
    async fn candidates(
        &self,
        tenant_id: &str,
        kind: SecretKind,
    ) -> Result<Vec<StoredSecret>, SecretError>;
    /// Removes a just-created secret during authorization-resource rollback.
    async fn delete(&self, tenant_id: &str, id: &str) -> Result<(), SecretError>;
}

/// Deterministic encrypted-value repository for unit tests and embedded callers.
#[derive(Default)]
pub struct MemorySecretRepository {
    state: RwLock<MemorySecretState>,
}

/// Tenant and resource identity used by the in-memory repository.
type MemorySecretKey = (String, String);

/// Public metadata paired with every retained encrypted version.
type MemorySecretRecord = (SecretMetadata, Vec<Vec<u8>>);

/// Complete deterministic in-memory secret state.
type MemorySecretState = BTreeMap<MemorySecretKey, MemorySecretRecord>;

impl MemorySecretRepository {
    /// Creates an empty repository.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SecretRepository for MemorySecretRepository {
    /// Creates one unique resource and its first sealed version.
    async fn create(
        &self,
        metadata: SecretMetadata,
        ciphertext: Vec<u8>,
    ) -> Result<SecretMetadata, SecretError> {
        let key = (metadata.tenant_id.clone(), metadata.id.clone());
        let mut state = self.state.write().await;
        if state.contains_key(&key) {
            return Err(SecretError::Conflict(metadata.id));
        }
        state.insert(key, (metadata.clone(), vec![ciphertext]));
        Ok(metadata)
    }

    /// Appends one sealed version while retaining historical ciphertext.
    async fn replace(
        &self,
        tenant_id: &str,
        id: &str,
        ciphertext: Vec<u8>,
    ) -> Result<SecretMetadata, SecretError> {
        let mut state = self.state.write().await;
        let (metadata, versions) = state
            .get_mut(&(tenant_id.to_owned(), id.to_owned()))
            .ok_or_else(|| SecretError::NotFound(id.to_owned()))?;
        versions.push(ciphertext);
        metadata.current_version = u64::try_from(versions.len())
            .map_err(|_| SecretError::Backend("secret version overflow".to_owned()))?;
        Ok(metadata.clone())
    }

    /// Returns public metadata only.
    async fn get(&self, tenant_id: &str, id: &str) -> Result<SecretMetadata, SecretError> {
        self.state
            .read()
            .await
            .get(&(tenant_id.to_owned(), id.to_owned()))
            .map(|(metadata, _)| metadata.clone())
            .ok_or_else(|| SecretError::NotFound(id.to_owned()))
    }

    /// Lists public metadata in stable resource order.
    async fn list(&self, tenant_id: &str) -> Result<Vec<SecretMetadata>, SecretError> {
        Ok(self
            .state
            .read()
            .await
            .iter()
            .filter(|((candidate_tenant, _), _)| candidate_tenant == tenant_id)
            .map(|(_, (metadata, _))| metadata.clone())
            .collect())
    }

    /// Returns the current sealed value for one stable resource identity.
    async fn current(&self, tenant_id: &str, id: &str) -> Result<StoredSecret, SecretError> {
        let state = self.state.read().await;
        let (metadata, versions) = state
            .get(&(tenant_id.to_owned(), id.to_owned()))
            .ok_or_else(|| SecretError::NotFound(id.to_owned()))?;
        let ciphertext = versions
            .last()
            .cloned()
            .ok_or_else(|| SecretError::Backend("secret has no value version".to_owned()))?;
        Ok(StoredSecret {
            metadata: metadata.clone(),
            ciphertext,
        })
    }

    /// Returns sealed current values for one provider contract.
    async fn candidates(
        &self,
        tenant_id: &str,
        kind: SecretKind,
    ) -> Result<Vec<StoredSecret>, SecretError> {
        self.state
            .read()
            .await
            .iter()
            .filter(|((candidate_tenant, _), (metadata, _))| {
                candidate_tenant == tenant_id && metadata.kind == kind
            })
            .map(|(_, (metadata, versions))| {
                versions
                    .last()
                    .cloned()
                    .map(|ciphertext| StoredSecret {
                        metadata: metadata.clone(),
                        ciphertext,
                    })
                    .ok_or_else(|| SecretError::Backend("secret has no value version".to_owned()))
            })
            .collect()
    }

    /// Removes one resource and all sealed versions.
    async fn delete(&self, tenant_id: &str, id: &str) -> Result<(), SecretError> {
        self.state
            .write()
            .await
            .remove(&(tenant_id.to_owned(), id.to_owned()))
            .map(|_| ())
            .ok_or_else(|| SecretError::NotFound(id.to_owned()))
    }
}

/// Authorization-gated lifecycle and longest-scope secret resolver.
pub struct SecretService {
    authorizer: Arc<dyn Authorizer>,
    repository: Arc<dyn SecretRepository>,
    cipher: Arc<dyn SecretCipher>,
}

impl SecretService {
    /// Composes authorization, encrypted persistence, and a testable cipher boundary.
    pub fn new(
        authorizer: Arc<dyn Authorizer>,
        repository: Arc<dyn SecretRepository>,
        cipher: Arc<dyn SecretCipher>,
    ) -> Self {
        Self {
            authorizer,
            repository,
            cipher,
        }
    }

    /// Creates a protected resource and first encrypted value without persisting plaintext.
    pub async fn create(&self, request: CreateSecret) -> Result<SecretMetadata, SecretError> {
        let scope = canonical_scope(request.kind, &request.scope)?;
        if request.value.is_empty() {
            return Err(SecretError::Invalid(
                "secret value must not be empty".to_owned(),
            ));
        }
        if request.id.len() > 243 {
            return Err(SecretError::Invalid(
                "secret id must contain at most 243 bytes".to_owned(),
            ));
        }
        let ciphertext = self.cipher.seal(&request.value)?;
        let resource = Resource::new(&request.tenant_id, &request.id, ResourceKind::Secret);
        resource.validate()?;
        self.authorizer.create_resource(resource).await?;
        let owner = Grant::new(
            format!("secret-owner-{}", request.id),
            &request.tenant_id,
            &request.principal_id,
            &request.id,
            BTreeSet::from([Action::Own]),
        );
        if let Err(error) = self.authorizer.create_grant(owner).await {
            let rollback = self
                .authorizer
                .delete_resource(&request.tenant_id, &request.id)
                .await;
            return match rollback {
                Ok(()) => Err(error.into()),
                Err(rollback) => Err(SecretError::Backend(format!(
                    "owner grant failed: {error}; resource rollback failed: {rollback}"
                ))),
            };
        }
        let metadata = SecretMetadata {
            tenant_id: request.tenant_id.clone(),
            id: request.id.clone(),
            kind: request.kind,
            scope,
            current_version: 1,
            resource_kind: ResourceKind::Secret,
        };
        match self.repository.create(metadata, ciphertext).await {
            Ok(created) => Ok(created),
            Err(error) => {
                let rollback = self
                    .authorizer
                    .delete_resource(&request.tenant_id, &request.id)
                    .await;
                match rollback {
                    Ok(()) => Err(error),
                    Err(rollback) => Err(SecretError::Backend(format!(
                        "secret persistence failed: {error}; resource rollback failed: {rollback}"
                    ))),
                }
            }
        }
    }

    /// Rotates one value only when the principal may modify the stable resource.
    pub async fn replace(&self, request: ReplaceSecret) -> Result<SecretMetadata, SecretError> {
        self.require(
            &request.tenant_id,
            &request.principal_id,
            &request.id,
            Action::Modify,
        )
        .await?;
        let ciphertext = self.cipher.seal(&request.value)?;
        self.repository
            .replace(&request.tenant_id, &request.id, ciphertext)
            .await
    }

    /// Returns public metadata without loading or decrypting a value.
    pub async fn get(&self, tenant_id: &str, id: &str) -> Result<SecretMetadata, SecretError> {
        self.repository.get(tenant_id, id).await
    }

    /// Lists public metadata without loading or decrypting values.
    pub async fn list(&self, tenant_id: &str) -> Result<Vec<SecretMetadata>, SecretError> {
        self.repository.list(tenant_id).await
    }

    /// Resolves a previously bound resource ID without repeating scope selection.
    pub async fn resolve_by_id(
        &self,
        tenant_id: &str,
        principal_id: &str,
        id: &str,
    ) -> Result<ResolvedSecret, SecretError> {
        self.require(tenant_id, principal_id, id, Action::UseSecret)
            .await?;
        let stored = self.repository.current(tenant_id, id).await?;
        self.open_stored(stored)
    }

    /// Resolves the longest authorized scope and rejects equal-specificity ambiguity.
    pub async fn resolve(&self, request: ResolveSecret) -> Result<ResolvedSecret, SecretError> {
        let target = canonical_target(request.kind, &request.uri)?;
        let mut candidates = self
            .repository
            .candidates(&request.tenant_id, request.kind)
            .await?
            .into_iter()
            .filter(|candidate| scope_matches(&candidate.metadata.scope, &target))
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(SecretError::NotFound(request.uri));
        }
        candidates.sort_by(|left, right| {
            right
                .metadata
                .scope
                .len()
                .cmp(&left.metadata.scope.len())
                .then_with(|| left.metadata.id.cmp(&right.metadata.id))
        });

        let mut offset = 0;
        while offset < candidates.len() {
            let length = candidates[offset].metadata.scope.len();
            let end = candidates[offset..]
                .iter()
                .position(|candidate| candidate.metadata.scope.len() != length)
                .map_or(candidates.len(), |relative| offset + relative);
            let mut authorized = Vec::new();
            for candidate in &candidates[offset..end] {
                let decision = self
                    .authorizer
                    .check(AccessCheck::new(
                        &request.tenant_id,
                        &request.principal_id,
                        &candidate.metadata.id,
                        Action::UseSecret,
                    ))
                    .await?;
                if decision.allowed {
                    authorized.push(candidate);
                }
            }
            match authorized.as_slice() {
                [] => offset = end,
                [selected] => {
                    return self.open_stored(StoredSecret {
                        metadata: selected.metadata.clone(),
                        ciphertext: selected.ciphertext.clone(),
                    });
                }
                _ => {
                    return Err(SecretError::Conflict(format!(
                        "multiple authorized {} secrets match {}",
                        length, request.uri
                    )));
                }
            }
        }
        Err(SecretError::Forbidden(format!(
            "principal {} cannot use a matching secret",
            request.principal_id
        )))
    }

    /// Opens one repository value after its authorization decision has succeeded.
    fn open_stored(&self, stored: StoredSecret) -> Result<ResolvedSecret, SecretError> {
        Ok(ResolvedSecret {
            resource_id: stored.metadata.id,
            version: stored.metadata.current_version,
            scope: stored.metadata.scope,
            value: self.cipher.open(&stored.ciphertext)?,
        })
    }

    /// Requires one authorization action on a stable secret resource.
    async fn require(
        &self,
        tenant_id: &str,
        principal_id: &str,
        resource_id: &str,
        action: Action,
    ) -> Result<(), SecretError> {
        let decision = self
            .authorizer
            .check(AccessCheck::new(
                tenant_id,
                principal_id,
                resource_id,
                action,
            ))
            .await?;
        if decision.allowed {
            Ok(())
        } else {
            Err(SecretError::Forbidden(format!(
                "principal {principal_id} cannot {} secret {resource_id}",
                action.as_str()
            )))
        }
    }
}

/// Validates and canonicalizes one provider-specific scope URI.
fn canonical_scope(kind: SecretKind, scope: &str) -> Result<String, SecretError> {
    let mut url = validate_uri(kind, scope)?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err(SecretError::Invalid(
            "secret scope must not contain a query or fragment".to_owned(),
        ));
    }
    let trimmed = url.path().trim_end_matches('/').to_owned();
    url.set_path(if trimmed.is_empty() { "/" } else { &trimmed });
    Ok(url.to_string().trim_end_matches('/').to_owned())
}

/// Validates and canonicalizes one target URI used for scope matching.
fn canonical_target(kind: SecretKind, target: &str) -> Result<String, SecretError> {
    Ok(validate_uri(kind, target)?.to_string())
}

/// Enforces provider-specific URI schemes and an explicit authority.
fn validate_uri(kind: SecretKind, value: &str) -> Result<Url, SecretError> {
    let url = Url::parse(value).map_err(|error| SecretError::Invalid(error.to_string()))?;
    let valid_scheme = match kind {
        SecretKind::S3 => url.scheme() == "s3",
        SecretKind::IcebergRest => matches!(url.scheme(), "http" | "https"),
    };
    if !valid_scheme || url.host_str().is_none() {
        return Err(SecretError::Invalid(format!(
            "URI scheme does not match secret type {kind:?}"
        )));
    }
    Ok(url)
}

/// Matches a canonical target on a path boundary beneath a canonical scope.
fn scope_matches(scope: &str, target: &str) -> bool {
    target == scope
        || target
            .strip_prefix(scope)
            .is_some_and(|suffix| scope.ends_with('/') || suffix.starts_with('/'))
}
