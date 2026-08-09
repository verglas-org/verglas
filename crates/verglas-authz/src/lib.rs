//! Universal authorization contracts shared by every Verglas data and runtime backend.
//!
//! The crate names principals, resources, actions, grants, workload-token claims,
//! and explainable decisions without importing a database or policy product.

mod memory;

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use memory::MemoryAuthorizer;

/// Stable tenant identifier carried by every authorization object.
pub type TenantId = String;

/// Stable principal identifier within one tenant.
pub type PrincipalId = String;

/// Stable resource identifier within one tenant.
pub type ResourceId = String;

/// Stable grant identifier within one tenant.
pub type GrantId = String;

/// A human or autonomous actor that can receive grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// An interactive human identity.
    User,
    /// An organization-owned non-human identity.
    ServiceAccount,
    /// One bounded agent session or turn.
    Agent,
    /// A durable Job definition.
    Job,
    /// One execution of a Job.
    JobRun,
    /// A composable Vessel definition or deployment.
    Vessel,
    /// An Application component inside a Vessel.
    Application,
    /// An Integration component inside a Vessel.
    Integration,
}

/// An actor registered in one tenant's authorization domain.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Principal {
    /// Tenant that owns this identity.
    pub tenant_id: TenantId,
    /// Stable identity within the tenant.
    pub id: PrincipalId,
    /// Actor lifecycle and delegation category.
    pub kind: PrincipalKind,
    /// Optional durable parent, such as a Job definition for a Job run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<PrincipalId>,
}

impl Principal {
    /// Constructs a principal without a parent identity.
    pub fn new(
        tenant_id: impl Into<TenantId>,
        id: impl Into<PrincipalId>,
        kind: PrincipalKind,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            id: id.into(),
            kind,
            parent_id: None,
        }
    }

    /// Attaches the durable principal from which this identity was derived.
    #[must_use]
    pub fn with_parent(mut self, parent_id: impl Into<PrincipalId>) -> Self {
        self.parent_id = Some(parent_id.into());
        self
    }

    /// Rejects identifiers that cannot be represented safely by every backend.
    pub fn validate(&self) -> Result<(), AuthzError> {
        validate_identifier("tenant_id", &self.tenant_id)?;
        validate_identifier("principal.id", &self.id)?;
        if let Some(parent_id) = &self.parent_id {
            validate_identifier("principal.parent_id", parent_id)?;
            if parent_id == &self.id {
                return Err(AuthzError::Invalid(
                    "a principal cannot be its own parent".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

/// A protected data, runtime, integration, or management object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// The root of one tenant's resource hierarchy.
    Tenant,
    /// A logical lakehouse or Postgres database.
    Database,
    /// A lakehouse warehouse beneath a database.
    Warehouse,
    /// A lakehouse namespace or Postgres schema.
    Namespace,
    /// A tabular data object.
    Table,
    /// A logical or materialized view.
    View,
    /// A snapshot-bound or independent vector index.
    VectorIndex,
    /// A graph namespace or graph index.
    Graph,
    /// A bounded object-store prefix.
    ObjectPrefix,
    /// A reflected external API connection.
    Integration,
    /// One callable operation on an Integration.
    IntegrationOperation,
    /// A credential that can be used without revealing its value.
    Secret,
    /// A durable Job definition.
    Job,
    /// A composable Vessel.
    Vessel,
    /// A callable or interactive Application.
    Application,
    /// A model or model-provider capability.
    Model,
    /// A durable event queue.
    Queue,
}

/// A protected object registered in one tenant's authorization domain.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Resource {
    /// Tenant that owns the resource.
    pub tenant_id: TenantId,
    /// Stable resource identity, independent of display name or backend path.
    pub id: ResourceId,
    /// Backend-neutral resource category.
    pub kind: ResourceKind,
    /// Optional parent from which grants may be inherited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<ResourceId>,
}

impl Resource {
    /// Constructs a root resource.
    pub fn new(
        tenant_id: impl Into<TenantId>,
        id: impl Into<ResourceId>,
        kind: ResourceKind,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            id: id.into(),
            kind,
            parent_id: None,
        }
    }

    /// Attaches the parent whose grants apply to this resource.
    #[must_use]
    pub fn with_parent(mut self, parent_id: impl Into<ResourceId>) -> Self {
        self.parent_id = Some(parent_id.into());
        self
    }

    /// Rejects malformed identifiers and a direct parent cycle.
    pub fn validate(&self) -> Result<(), AuthzError> {
        validate_identifier("tenant_id", &self.tenant_id)?;
        validate_identifier("resource.id", &self.id)?;
        if let Some(parent_id) = &self.parent_id {
            validate_identifier("resource.parent_id", parent_id)?;
            if parent_id == &self.id {
                return Err(AuthzError::Invalid(
                    "a resource cannot be its own parent".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

/// A backend-neutral operation that can be granted on a resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Reveal that a resource exists in bounded discovery results.
    Discover,
    /// Read resource metadata without reading its contents.
    Describe,
    /// Read or query resource contents.
    Query,
    /// Add new records without changing existing records.
    Append,
    /// Change or remove existing resource contents or metadata.
    Modify,
    /// Create a child beneath a container resource.
    CreateChild,
    /// Execute a Job, Integration operation, model, or other callable resource.
    Execute,
    /// Use a secret without reading its stored value.
    UseSecret,
    /// Deploy or update an executable resource.
    Deploy,
    /// Delegate privileges already held by the principal.
    PassGrants,
    /// Add or remove arbitrary grants on the resource.
    ManageGrants,
    /// Perform the complete resource-owner operation set.
    Own,
}

impl Action {
    /// Returns the stable relation name used by policy backends.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Discover => "discover",
            Self::Describe => "describe",
            Self::Query => "query",
            Self::Append => "append",
            Self::Modify => "modify",
            Self::CreateChild => "create_child",
            Self::Execute => "execute",
            Self::UseSecret => "use_secret",
            Self::Deploy => "deploy",
            Self::PassGrants => "pass_grants",
            Self::ManageGrants => "manage_grants",
            Self::Own => "own",
        }
    }

    /// Whether granting this action authorizes the requested operation.
    pub fn covers(self, requested: Self) -> bool {
        self == requested
            || self == Self::Own
            || matches!(
                (self, requested),
                (Self::ManageGrants, Self::PassGrants)
                    | (
                        Self::Modify,
                        Self::Append | Self::Query | Self::Describe | Self::Discover
                    )
                    | (Self::Append, Self::Describe | Self::Discover)
                    | (Self::Query, Self::Describe | Self::Discover)
                    | (Self::CreateChild, Self::Describe | Self::Discover)
                    | (Self::Describe, Self::Discover)
            )
    }
}

/// An additive set of actions assigned to a principal on one resource.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Grant {
    /// Stable grant identity used for revocation and explanation.
    pub id: GrantId,
    /// Tenant containing the principal and resource.
    pub tenant_id: TenantId,
    /// Principal receiving the actions.
    pub principal_id: PrincipalId,
    /// Resource on which the actions originate.
    pub resource_id: ResourceId,
    /// Non-empty action set.
    pub actions: BTreeSet<Action>,
}

impl Grant {
    /// Constructs one explicit grant.
    pub fn new(
        id: impl Into<GrantId>,
        tenant_id: impl Into<TenantId>,
        principal_id: impl Into<PrincipalId>,
        resource_id: impl Into<ResourceId>,
        actions: BTreeSet<Action>,
    ) -> Self {
        Self {
            id: id.into(),
            tenant_id: tenant_id.into(),
            principal_id: principal_id.into(),
            resource_id: resource_id.into(),
            actions,
        }
    }

    /// Rejects malformed identifiers and empty grants.
    pub fn validate(&self) -> Result<(), AuthzError> {
        validate_identifier("grant.id", &self.id)?;
        validate_identifier("tenant_id", &self.tenant_id)?;
        validate_identifier("grant.principal_id", &self.principal_id)?;
        validate_identifier("grant.resource_id", &self.resource_id)?;
        if self.actions.is_empty() {
            return Err(AuthzError::Invalid(
                "grant.actions must not be empty".to_owned(),
            ));
        }
        Ok(())
    }
}

/// One authorization question evaluated at a backend boundary.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct AccessCheck {
    /// Tenant in which the request is executing.
    pub tenant_id: TenantId,
    /// Acting human or process.
    pub principal_id: PrincipalId,
    /// Resource the principal wants to use.
    pub resource_id: ResourceId,
    /// Requested operation.
    pub action: Action,
}

impl AccessCheck {
    /// Constructs one access question.
    pub fn new(
        tenant_id: impl Into<TenantId>,
        principal_id: impl Into<PrincipalId>,
        resource_id: impl Into<ResourceId>,
        action: Action,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            principal_id: principal_id.into(),
            resource_id: resource_id.into(),
            action,
        }
    }

    /// Rejects identifiers that cannot be evaluated safely.
    pub fn validate(&self) -> Result<(), AuthzError> {
        validate_identifier("tenant_id", &self.tenant_id)?;
        validate_identifier("access.principal_id", &self.principal_id)?;
        validate_identifier("access.resource_id", &self.resource_id)
    }
}

/// Stable explanation category returned with every access decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionReason {
    /// An explicit grant exists on the requested resource.
    ExactGrant,
    /// A grant was inherited from an ancestor resource.
    InheritedGrant,
    /// No explicit or inherited grant authorizes the operation.
    NoMatchingGrant,
    /// The principal, resource, and request are not in one tenant.
    TenantMismatch,
}

impl DecisionReason {
    /// Returns the stable API representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExactGrant => "exact_grant",
            Self::InheritedGrant => "inherited_grant",
            Self::NoMatchingGrant => "no_matching_grant",
            Self::TenantMismatch => "tenant_mismatch",
        }
    }
}

/// Explainable allow or deny result.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct AccessDecision {
    /// Whether the operation may proceed.
    pub allowed: bool,
    /// Stable reason for the outcome.
    pub reason: DecisionReason,
    /// Grant responsible for an allow decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_id: Option<GrantId>,
    /// Exact or ancestor resource on which the grant was found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_resource_id: Option<ResourceId>,
    /// Tenant policy revision evaluated for this decision.
    pub policy_version: u64,
}

impl AccessDecision {
    /// Constructs a default-deny result.
    pub fn deny(reason: DecisionReason, policy_version: u64) -> Self {
        Self {
            allowed: false,
            reason,
            grant_id: None,
            matched_resource_id: None,
            policy_version,
        }
    }

    /// Constructs an allow result tied to its grant and resource.
    pub fn allow(
        reason: DecisionReason,
        grant_id: impl Into<GrantId>,
        matched_resource_id: impl Into<ResourceId>,
        policy_version: u64,
    ) -> Self {
        Self {
            allowed: true,
            reason,
            grant_id: Some(grant_id.into()),
            matched_resource_id: Some(matched_resource_id.into()),
            policy_version,
        }
    }
}

/// Claims carried by a short-lived workload credential.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ScopedTokenClaims {
    /// Tenant boundary enforced by every receiving service.
    pub tenant_id: TenantId,
    /// Human or process represented by the token.
    pub principal_id: PrincipalId,
    /// Intended data plane or service.
    pub audience: String,
    /// Policy revision the issuer used when minting the token.
    pub policy_version: u64,
    /// Optional individual run identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Unix timestamp at which the credential became valid.
    pub issued_at: u64,
    /// Unix timestamp after which the credential is rejected.
    pub expires_at: u64,
}

impl ScopedTokenClaims {
    /// Constructs claims for a principal and policy revision.
    pub fn new(
        tenant_id: impl Into<TenantId>,
        principal_id: impl Into<PrincipalId>,
        audience: impl Into<String>,
        policy_version: u64,
        issued_at: u64,
        expires_at: u64,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            principal_id: principal_id.into(),
            audience: audience.into(),
            policy_version,
            run_id: None,
            issued_at,
            expires_at,
        }
    }

    /// Binds the credential to one execution without changing its durable principal.
    #[must_use]
    pub fn with_run(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Verifies tenant, audience, identifier shape, and validity interval.
    pub fn validate(
        &self,
        expected_tenant: &str,
        expected_audience: &str,
        now: u64,
    ) -> Result<(), AuthzError> {
        validate_identifier("token.tenant_id", &self.tenant_id)?;
        validate_identifier("token.principal_id", &self.principal_id)?;
        validate_identifier("token.audience", &self.audience)?;
        if let Some(run_id) = &self.run_id {
            validate_identifier("token.run_id", run_id)?;
        }
        if self.tenant_id != expected_tenant {
            return Err(AuthzError::Token("tenant does not match".to_owned()));
        }
        if self.audience != expected_audience {
            return Err(AuthzError::Token("audience does not match".to_owned()));
        }
        if self.expires_at <= self.issued_at {
            return Err(AuthzError::Token(
                "expiration must be after issuance".to_owned(),
            ));
        }
        if now < self.issued_at || now > self.expires_at {
            return Err(AuthzError::Token(
                "credential is outside its validity interval".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Backend-neutral authorization administration and decision interface.
#[async_trait]
pub trait Authorizer: Send + Sync {
    /// Registers one principal and rejects duplicate identities.
    async fn create_principal(&self, principal: Principal) -> Result<Principal, AuthzError>;
    /// Returns one principal in the requested tenant.
    async fn get_principal(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> Result<Principal, AuthzError>;
    /// Lists principals without crossing a tenant boundary.
    async fn list_principals(&self, tenant_id: &str) -> Result<Vec<Principal>, AuthzError>;
    /// Removes one principal and all grants assigned to it.
    async fn delete_principal(&self, tenant_id: &str, principal_id: &str)
    -> Result<(), AuthzError>;
    /// Registers one resource after validating its parent.
    async fn create_resource(&self, resource: Resource) -> Result<Resource, AuthzError>;
    /// Returns one resource in the requested tenant.
    async fn get_resource(
        &self,
        tenant_id: &str,
        resource_id: &str,
    ) -> Result<Resource, AuthzError>;
    /// Lists resources without crossing a tenant boundary.
    async fn list_resources(&self, tenant_id: &str) -> Result<Vec<Resource>, AuthzError>;
    /// Removes one childless resource and grants originating on it.
    async fn delete_resource(&self, tenant_id: &str, resource_id: &str) -> Result<(), AuthzError>;
    /// Creates one additive grant after validating principal and resource ownership.
    async fn create_grant(&self, grant: Grant) -> Result<Grant, AuthzError>;
    /// Lists all grants owned by one tenant.
    async fn list_grants(&self, tenant_id: &str) -> Result<Vec<Grant>, AuthzError>;
    /// Revokes one grant by stable identity.
    async fn delete_grant(&self, tenant_id: &str, grant_id: &str) -> Result<(), AuthzError>;
    /// Returns the monotonically increasing tenant policy revision.
    async fn policy_version(&self, tenant_id: &str) -> Result<u64, AuthzError>;
    /// Evaluates an action with exact and ancestor-resource inheritance.
    async fn check(&self, check: AccessCheck) -> Result<AccessDecision, AuthzError>;
}

/// Durable registry operations required by a policy-backed authorization service.
#[async_trait]
pub trait AuthorizationRepository: Send + Sync {
    /// Persists one validated principal.
    async fn create_principal(&self, principal: Principal) -> Result<Principal, AuthzError>;
    /// Returns one tenant-scoped principal.
    async fn get_principal(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> Result<Principal, AuthzError>;
    /// Lists tenant-scoped principals.
    async fn list_principals(&self, tenant_id: &str) -> Result<Vec<Principal>, AuthzError>;
    /// Deletes one principal after its policy tuples have been removed.
    async fn delete_principal(&self, tenant_id: &str, principal_id: &str)
    -> Result<(), AuthzError>;
    /// Persists one validated resource.
    async fn create_resource(&self, resource: Resource) -> Result<Resource, AuthzError>;
    /// Returns one tenant-scoped resource.
    async fn get_resource(
        &self,
        tenant_id: &str,
        resource_id: &str,
    ) -> Result<Resource, AuthzError>;
    /// Lists tenant-scoped resources.
    async fn list_resources(&self, tenant_id: &str) -> Result<Vec<Resource>, AuthzError>;
    /// Reports an identifier collision outside the requested tenant without revealing its owner.
    async fn resource_exists_elsewhere(
        &self,
        tenant_id: &str,
        resource_id: &str,
    ) -> Result<bool, AuthzError>;
    /// Deletes one childless resource after its policy tuples have been removed.
    async fn delete_resource(&self, tenant_id: &str, resource_id: &str) -> Result<(), AuthzError>;
    /// Persists one validated grant.
    async fn create_grant(&self, grant: Grant) -> Result<Grant, AuthzError>;
    /// Returns one tenant-scoped grant.
    async fn get_grant(&self, tenant_id: &str, grant_id: &str) -> Result<Grant, AuthzError>;
    /// Lists tenant-scoped grants.
    async fn list_grants(&self, tenant_id: &str) -> Result<Vec<Grant>, AuthzError>;
    /// Deletes one grant after its policy tuples have been removed.
    async fn delete_grant(&self, tenant_id: &str, grant_id: &str) -> Result<(), AuthzError>;
    /// Finds the exact or inherited grant used to explain an allow decision.
    async fn matching_grant(
        &self,
        check: &AccessCheck,
    ) -> Result<Option<(Grant, ResourceId)>, AuthzError>;
    /// Returns the monotonically increasing tenant policy revision.
    async fn policy_version(&self, tenant_id: &str) -> Result<u64, AuthzError>;
}

/// Authorization service that keeps durable registry state and policy tuples synchronized.
pub struct AccessService {
    repository: Arc<dyn AuthorizationRepository>,
    policy: Arc<dyn PolicyEngine>,
}

impl AccessService {
    /// Constructs the mandatory service from its durable registry and policy evaluator.
    pub fn new(
        repository: Arc<dyn AuthorizationRepository>,
        policy: Arc<dyn PolicyEngine>,
    ) -> Self {
        Self { repository, policy }
    }

    /// Removes policy tuples for every grant matching one predicate.
    async fn remove_matching_grants(
        &self,
        tenant_id: &str,
        predicate: impl Fn(&Grant) -> bool,
    ) -> Result<(), AuthzError> {
        for grant in self
            .repository
            .list_grants(tenant_id)
            .await?
            .into_iter()
            .filter(predicate)
        {
            self.policy.delete_grant(&grant).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Authorizer for AccessService {
    /// Registers one principal in durable tenant state.
    async fn create_principal(&self, principal: Principal) -> Result<Principal, AuthzError> {
        self.repository.create_principal(principal).await
    }

    /// Returns one durable principal.
    async fn get_principal(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> Result<Principal, AuthzError> {
        self.repository.get_principal(tenant_id, principal_id).await
    }

    /// Lists durable principals in one tenant.
    async fn list_principals(&self, tenant_id: &str) -> Result<Vec<Principal>, AuthzError> {
        self.repository.list_principals(tenant_id).await
    }

    /// Removes one principal after revoking all policy tuples assigned to it.
    async fn delete_principal(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> Result<(), AuthzError> {
        self.repository
            .get_principal(tenant_id, principal_id)
            .await?;
        self.remove_matching_grants(tenant_id, |grant| grant.principal_id == principal_id)
            .await?;
        self.repository
            .delete_principal(tenant_id, principal_id)
            .await
    }

    /// Registers one resource and its inheritance edge.
    async fn create_resource(&self, resource: Resource) -> Result<Resource, AuthzError> {
        let created = self.repository.create_resource(resource).await?;
        if let Err(error) = self.policy.write_resource(&created).await {
            let rollback = self
                .repository
                .delete_resource(&created.tenant_id, &created.id)
                .await;
            return match rollback {
                Ok(()) => Err(error),
                Err(rollback) => Err(AuthzError::Backend(format!(
                    "policy write failed: {error}; registry rollback failed: {rollback}"
                ))),
            };
        }
        Ok(created)
    }

    /// Returns one durable resource.
    async fn get_resource(
        &self,
        tenant_id: &str,
        resource_id: &str,
    ) -> Result<Resource, AuthzError> {
        self.repository.get_resource(tenant_id, resource_id).await
    }

    /// Lists durable resources in one tenant.
    async fn list_resources(&self, tenant_id: &str) -> Result<Vec<Resource>, AuthzError> {
        self.repository.list_resources(tenant_id).await
    }

    /// Removes one childless resource after revoking its originating grants and parent edge.
    async fn delete_resource(&self, tenant_id: &str, resource_id: &str) -> Result<(), AuthzError> {
        let resource = self.repository.get_resource(tenant_id, resource_id).await?;
        if self
            .repository
            .list_resources(tenant_id)
            .await?
            .iter()
            .any(|candidate| candidate.parent_id.as_deref() == Some(resource_id))
        {
            return Err(AuthzError::Conflict(format!(
                "resource {resource_id} still has children"
            )));
        }
        self.remove_matching_grants(tenant_id, |grant| grant.resource_id == resource_id)
            .await?;
        self.policy.delete_resource(&resource).await?;
        self.repository
            .delete_resource(tenant_id, resource_id)
            .await
    }

    /// Creates one durable grant and then publishes its policy tuples.
    async fn create_grant(&self, grant: Grant) -> Result<Grant, AuthzError> {
        let created = self.repository.create_grant(grant).await?;
        if let Err(error) = self.policy.write_grant(&created).await {
            let rollback = self
                .repository
                .delete_grant(&created.tenant_id, &created.id)
                .await;
            return match rollback {
                Ok(()) => Err(error),
                Err(rollback) => Err(AuthzError::Backend(format!(
                    "policy write failed: {error}; registry rollback failed: {rollback}"
                ))),
            };
        }
        Ok(created)
    }

    /// Lists durable grants in one tenant.
    async fn list_grants(&self, tenant_id: &str) -> Result<Vec<Grant>, AuthzError> {
        self.repository.list_grants(tenant_id).await
    }

    /// Revokes policy tuples before deleting their durable grant record.
    async fn delete_grant(&self, tenant_id: &str, grant_id: &str) -> Result<(), AuthzError> {
        let grant = self.repository.get_grant(tenant_id, grant_id).await?;
        self.policy.delete_grant(&grant).await?;
        if let Err(error) = self.repository.delete_grant(tenant_id, grant_id).await {
            let restore = self.policy.write_grant(&grant).await;
            return match restore {
                Ok(()) => Err(error),
                Err(restore) => Err(AuthzError::Backend(format!(
                    "registry delete failed: {error}; policy restore failed: {restore}"
                ))),
            };
        }
        Ok(())
    }

    /// Returns the durable tenant policy revision.
    async fn policy_version(&self, tenant_id: &str) -> Result<u64, AuthzError> {
        self.repository.policy_version(tenant_id).await
    }

    /// Evaluates OpenFGA and verifies that the durable explanation agrees.
    async fn check(&self, check: AccessCheck) -> Result<AccessDecision, AuthzError> {
        check.validate()?;
        self.repository
            .get_principal(&check.tenant_id, &check.principal_id)
            .await?;
        match self
            .repository
            .get_resource(&check.tenant_id, &check.resource_id)
            .await
        {
            Ok(_) => {}
            Err(AuthzError::NotFound(_)) => {
                if self
                    .repository
                    .resource_exists_elsewhere(&check.tenant_id, &check.resource_id)
                    .await?
                {
                    let version = self.repository.policy_version(&check.tenant_id).await?;
                    return Ok(AccessDecision::deny(
                        DecisionReason::TenantMismatch,
                        version,
                    ));
                }
                return Err(AuthzError::NotFound(format!(
                    "resource {}",
                    check.resource_id
                )));
            }
            Err(error) => return Err(error),
        }
        let policy_allowed = self.policy.check(&check).await?;
        let matching = self.repository.matching_grant(&check).await?;
        if policy_allowed != matching.is_some() {
            return Err(AuthzError::Backend(
                "OpenFGA and the durable grant registry disagree".to_owned(),
            ));
        }
        let version = self.repository.policy_version(&check.tenant_id).await?;
        let Some((grant, matched_resource_id)) = matching else {
            return Ok(AccessDecision::deny(
                DecisionReason::NoMatchingGrant,
                version,
            ));
        };
        let reason = if matched_resource_id == check.resource_id {
            DecisionReason::ExactGrant
        } else {
            DecisionReason::InheritedGrant
        };
        Ok(AccessDecision::allow(
            reason,
            grant.id,
            matched_resource_id,
            version,
        ))
    }
}

/// Narrow policy-engine interface implemented by OpenFGA and other evaluators.
#[async_trait]
pub trait PolicyEngine: Send + Sync {
    /// Adds every action tuple represented by one grant.
    async fn write_grant(&self, grant: &Grant) -> Result<(), AuthzError>;
    /// Removes every action tuple represented by one grant.
    async fn delete_grant(&self, grant: &Grant) -> Result<(), AuthzError>;
    /// Adds a resource-parent relationship when the resource has a parent.
    async fn write_resource(&self, resource: &Resource) -> Result<(), AuthzError>;
    /// Removes a resource-parent relationship when the resource has a parent.
    async fn delete_resource(&self, resource: &Resource) -> Result<(), AuthzError>;
    /// Returns the policy backend's boolean answer for a validated question.
    async fn check(&self, check: &AccessCheck) -> Result<bool, AuthzError>;
}

/// Stable failures returned by authorization contracts and implementations.
#[derive(Debug, Error)]
pub enum AuthzError {
    /// A public contract failed validation.
    #[error("invalid authorization request: {0}")]
    Invalid(String),
    /// The requested principal, resource, or grant does not exist.
    #[error("authorization object not found: {0}")]
    NotFound(String),
    /// The requested mutation conflicts with existing authorization state.
    #[error("authorization conflict: {0}")]
    Conflict(String),
    /// A workload credential failed claim validation.
    #[error("invalid scoped token: {0}")]
    Token(String),
    /// The configured policy implementation failed.
    #[error("authorization backend failed: {0}")]
    Backend(String),
}

/// Enforces the common identifier subset used by URLs and policy object keys.
fn validate_identifier(field: &str, value: &str) -> Result<(), AuthzError> {
    if value.is_empty() || value.len() > 256 {
        return Err(AuthzError::Invalid(format!(
            "{field} must contain 1 to 256 bytes"
        )));
    }
    if value
        .bytes()
        .any(|byte| byte.is_ascii_control() || byte == b':')
    {
        return Err(AuthzError::Invalid(format!(
            "{field} must not contain control characters or ':'"
        )));
    }
    Ok(())
}
