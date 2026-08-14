//! Public authorization request and response contracts.
//!
//! These types describe the hosted access API without importing its private
//! policy engine or persistence implementation.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

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
    /// A durable job definition.
    Job,
    /// One execution of a job.
    JobRun,
    /// A named role assumed for one bounded operation.
    Role,
    /// A composable vessel definition or deployment.
    Vessel,
    /// An application component inside a vessel.
    Application,
    /// An integration component inside a vessel.
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
    /// Optional durable parent identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<PrincipalId>,
}

/// A protected data, runtime, integration, or management object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// The root of one tenant's resource hierarchy.
    Tenant,
    /// A project that groups databases and grants.
    Project,
    /// A logical lakehouse or Postgres database.
    Database,
    /// A lakehouse warehouse beneath a database.
    Warehouse,
    /// A lakehouse namespace or Postgres schema.
    Namespace,
    /// A tabular data object.
    Table,
    /// A table whose engine is selected by its database.
    GenericTable,
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
    /// One callable operation on an integration.
    IntegrationOperation,
    /// A credential usable without revealing its value.
    Secret,
    /// A classification tag attached to a resource.
    Tag,
    /// A role definition.
    Role,
    /// A durable job definition.
    Job,
    /// A composable vessel.
    Vessel,
    /// A callable or interactive application.
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
    /// Stable resource identity.
    pub id: ResourceId,
    /// Backend-neutral resource category.
    pub kind: ResourceKind,
    /// Optional parent from which grants may be inherited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<ResourceId>,
}

/// A backend-neutral operation that can be granted on a resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Reveal that a resource exists.
    Discover,
    /// Read resource metadata.
    Describe,
    /// Read or query resource contents.
    Query,
    /// Add records without changing existing records.
    Append,
    /// Change or remove contents or metadata.
    Modify,
    /// Create a child beneath a container resource.
    CreateChild,
    /// Execute a callable resource.
    Execute,
    /// Use a secret without reading its value.
    UseSecret,
    /// Deploy or update an executable resource.
    Deploy,
    /// Establish a database or service connection.
    Connect,
    /// Delegate privileges already held by the principal.
    PassGrants,
    /// Add or remove arbitrary grants.
    ManageGrants,
    /// Perform the complete resource-owner operation set.
    Own,
}

/// An additive set of actions assigned to a principal on one resource.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct AccessGrant {
    /// Stable grant identity.
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

/// One authorization question evaluated at a hosted boundary.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct AccessCheck {
    /// Tenant in which the request executes.
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
}

/// Stable explanation category returned with an access decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionReason {
    /// An explicit grant exists on the requested resource.
    ExactGrant,
    /// A grant was inherited from an ancestor resource.
    InheritedGrant,
    /// No grant authorizes the operation.
    NoMatchingGrant,
    /// The request crosses a tenant boundary.
    TenantMismatch,
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
    /// Resource on which the matching grant was found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_resource_id: Option<ResourceId>,
    /// Tenant policy revision evaluated for this decision.
    pub policy_version: u64,
}

/// Claims carried by a short-lived workload credential.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ScopedTokenClaims {
    /// Stable credential identity.
    pub token_id: String,
    /// Tenant boundary enforced by receiving services.
    pub tenant_id: TenantId,
    /// Human or process represented by the token.
    pub principal_id: PrincipalId,
    /// Intended data plane or service.
    pub audience: String,
    /// Policy revision used when minting the token.
    pub policy_version: u64,
    /// Optional individual run identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Unix timestamp at which the credential became valid.
    pub issued_at: u64,
    /// Unix timestamp after which the credential is rejected.
    pub expires_at: u64,
}
