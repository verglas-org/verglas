//! Deterministic in-memory authorization state used by tests and embedded callers.
//!
//! Production persistence belongs behind the same [`Authorizer`](crate::Authorizer)
//! contract; this implementation makes policy semantics executable without I/O.

use std::collections::BTreeMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::{
    AccessCheck, AccessDecision, Authorizer, AuthzError, DecisionReason, Grant, Principal, Resource,
};

/// One tenant-keyed snapshot of principals, resources, and grants.
#[derive(Debug, Default)]
struct State {
    principals: BTreeMap<(String, String), Principal>,
    resources: BTreeMap<(String, String), Resource>,
    grants: BTreeMap<(String, String), Grant>,
    policy_versions: BTreeMap<String, u64>,
}

/// Default-deny evaluator with exact and parent-resource inheritance.
#[derive(Debug, Default)]
pub struct MemoryAuthorizer {
    state: RwLock<State>,
}

impl MemoryAuthorizer {
    /// Creates an empty test authorization domain.
    ///
    /// This backend is intentionally non-durable and must never be selected by
    /// a Verglas runtime binary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Finds a resource under another tenant when an ID collision indicates a boundary violation.
    fn resource_exists_elsewhere(state: &State, tenant_id: &str, resource_id: &str) -> bool {
        state
            .resources
            .iter()
            .any(|((tenant, id), _)| tenant != tenant_id && id == resource_id)
    }

    /// Advances a tenant revision after one committed policy mutation.
    fn bump_policy_version(state: &mut State, tenant_id: &str) {
        let version = state
            .policy_versions
            .entry(tenant_id.to_owned())
            .or_default();
        *version = version.saturating_add(1);
    }
}

#[async_trait]
impl Authorizer for MemoryAuthorizer {
    /// Registers one principal and rejects duplicate identities.
    async fn create_principal(&self, principal: Principal) -> Result<Principal, AuthzError> {
        principal.validate()?;
        let key = (principal.tenant_id.clone(), principal.id.clone());
        let mut state = self.state.write().await;
        if state.principals.contains_key(&key) {
            return Err(AuthzError::Conflict(format!(
                "principal {} already exists",
                principal.id
            )));
        }
        if let Some(parent_id) = &principal.parent_id
            && !state
                .principals
                .contains_key(&(principal.tenant_id.clone(), parent_id.clone()))
        {
            return Err(AuthzError::NotFound(format!("principal {parent_id}")));
        }
        state.principals.insert(key, principal.clone());
        Self::bump_policy_version(&mut state, &principal.tenant_id);
        Ok(principal)
    }

    /// Returns one principal in the requested tenant.
    async fn get_principal(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> Result<Principal, AuthzError> {
        self.state
            .read()
            .await
            .principals
            .get(&(tenant_id.to_owned(), principal_id.to_owned()))
            .cloned()
            .ok_or_else(|| AuthzError::NotFound(format!("principal {principal_id}")))
    }

    /// Lists principals without crossing a tenant boundary.
    async fn list_principals(&self, tenant_id: &str) -> Result<Vec<Principal>, AuthzError> {
        Ok(self
            .state
            .read()
            .await
            .principals
            .iter()
            .filter(|((tenant, _), _)| tenant == tenant_id)
            .map(|(_, principal)| principal.clone())
            .collect())
    }

    /// Removes one principal and all grants assigned to it.
    async fn delete_principal(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> Result<(), AuthzError> {
        let mut state = self.state.write().await;
        let key = (tenant_id.to_owned(), principal_id.to_owned());
        if state.principals.remove(&key).is_none() {
            return Err(AuthzError::NotFound(format!("principal {principal_id}")));
        }
        state
            .grants
            .retain(|(tenant, _), grant| tenant != tenant_id || grant.principal_id != principal_id);
        Self::bump_policy_version(&mut state, tenant_id);
        Ok(())
    }

    /// Registers one resource after validating its parent.
    async fn create_resource(&self, resource: Resource) -> Result<Resource, AuthzError> {
        resource.validate()?;
        let key = (resource.tenant_id.clone(), resource.id.clone());
        let mut state = self.state.write().await;
        if state.resources.contains_key(&key) {
            return Err(AuthzError::Conflict(format!(
                "resource {} already exists",
                resource.id
            )));
        }
        if let Some(parent_id) = &resource.parent_id
            && !state
                .resources
                .contains_key(&(resource.tenant_id.clone(), parent_id.clone()))
        {
            return Err(AuthzError::NotFound(format!("resource {parent_id}")));
        }
        state.resources.insert(key, resource.clone());
        Self::bump_policy_version(&mut state, &resource.tenant_id);
        Ok(resource)
    }

    /// Returns one resource in the requested tenant.
    async fn get_resource(
        &self,
        tenant_id: &str,
        resource_id: &str,
    ) -> Result<Resource, AuthzError> {
        self.state
            .read()
            .await
            .resources
            .get(&(tenant_id.to_owned(), resource_id.to_owned()))
            .cloned()
            .ok_or_else(|| AuthzError::NotFound(format!("resource {resource_id}")))
    }

    /// Lists resources without crossing a tenant boundary.
    async fn list_resources(&self, tenant_id: &str) -> Result<Vec<Resource>, AuthzError> {
        Ok(self
            .state
            .read()
            .await
            .resources
            .iter()
            .filter(|((tenant, _), _)| tenant == tenant_id)
            .map(|(_, resource)| resource.clone())
            .collect())
    }

    /// Removes one childless resource and grants originating on it.
    async fn delete_resource(&self, tenant_id: &str, resource_id: &str) -> Result<(), AuthzError> {
        let mut state = self.state.write().await;
        if state.resources.iter().any(|((tenant, _), resource)| {
            tenant == tenant_id && resource.parent_id.as_deref() == Some(resource_id)
        }) {
            return Err(AuthzError::Conflict(format!(
                "resource {resource_id} still has children"
            )));
        }
        let key = (tenant_id.to_owned(), resource_id.to_owned());
        if state.resources.remove(&key).is_none() {
            return Err(AuthzError::NotFound(format!("resource {resource_id}")));
        }
        state
            .grants
            .retain(|(tenant, _), grant| tenant != tenant_id || grant.resource_id != resource_id);
        Self::bump_policy_version(&mut state, tenant_id);
        Ok(())
    }

    /// Creates one additive grant after validating principal and resource ownership.
    async fn create_grant(&self, grant: Grant) -> Result<Grant, AuthzError> {
        grant.validate()?;
        let mut state = self.state.write().await;
        let key = (grant.tenant_id.clone(), grant.id.clone());
        if state.grants.contains_key(&key) {
            return Err(AuthzError::Conflict(format!(
                "grant {} already exists",
                grant.id
            )));
        }
        if state.grants.values().any(|existing| {
            existing.tenant_id == grant.tenant_id
                && existing.principal_id == grant.principal_id
                && existing.resource_id == grant.resource_id
        }) {
            return Err(AuthzError::Conflict(format!(
                "principal {} already has a grant on resource {}",
                grant.principal_id, grant.resource_id
            )));
        }
        if !state
            .principals
            .contains_key(&(grant.tenant_id.clone(), grant.principal_id.clone()))
        {
            return Err(AuthzError::NotFound(format!(
                "principal {}",
                grant.principal_id
            )));
        }
        if !state
            .resources
            .contains_key(&(grant.tenant_id.clone(), grant.resource_id.clone()))
        {
            return Err(AuthzError::NotFound(format!(
                "resource {}",
                grant.resource_id
            )));
        }
        state.grants.insert(key, grant.clone());
        Self::bump_policy_version(&mut state, &grant.tenant_id);
        Ok(grant)
    }

    /// Lists all grants owned by one tenant.
    async fn list_grants(&self, tenant_id: &str) -> Result<Vec<Grant>, AuthzError> {
        Ok(self
            .state
            .read()
            .await
            .grants
            .iter()
            .filter(|((tenant, _), _)| tenant == tenant_id)
            .map(|(_, grant)| grant.clone())
            .collect())
    }

    /// Revokes one grant by stable identity.
    async fn delete_grant(&self, tenant_id: &str, grant_id: &str) -> Result<(), AuthzError> {
        let mut state = self.state.write().await;
        if state
            .grants
            .remove(&(tenant_id.to_owned(), grant_id.to_owned()))
            .is_none()
        {
            return Err(AuthzError::NotFound(format!("grant {grant_id}")));
        }
        Self::bump_policy_version(&mut state, tenant_id);
        Ok(())
    }

    /// Returns the test tenant's current policy revision.
    async fn policy_version(&self, tenant_id: &str) -> Result<u64, AuthzError> {
        Ok(self
            .state
            .read()
            .await
            .policy_versions
            .get(tenant_id)
            .copied()
            .unwrap_or_default())
    }

    /// Evaluates an action with exact and ancestor-resource inheritance.
    async fn check(&self, check: AccessCheck) -> Result<AccessDecision, AuthzError> {
        check.validate()?;
        let state = self.state.read().await;
        let policy_version = state
            .policy_versions
            .get(&check.tenant_id)
            .copied()
            .unwrap_or_default();
        let principal_key = (check.tenant_id.clone(), check.principal_id.clone());
        if !state.principals.contains_key(&principal_key) {
            return Err(AuthzError::NotFound(format!(
                "principal {}",
                check.principal_id
            )));
        }
        let resource_key = (check.tenant_id.clone(), check.resource_id.clone());
        let Some(mut resource) = state.resources.get(&resource_key) else {
            if Self::resource_exists_elsewhere(&state, &check.tenant_id, &check.resource_id) {
                return Ok(AccessDecision::deny(
                    DecisionReason::TenantMismatch,
                    policy_version,
                ));
            }
            return Err(AuthzError::NotFound(format!(
                "resource {}",
                check.resource_id
            )));
        };
        let exact_id = resource.id.clone();
        loop {
            if let Some(grant) = state.grants.values().find(|grant| {
                grant.tenant_id == check.tenant_id
                    && grant.principal_id == check.principal_id
                    && grant.resource_id == resource.id
                    && grant
                        .actions
                        .iter()
                        .any(|action| action.covers(check.action))
            }) {
                let reason = if resource.id == exact_id {
                    DecisionReason::ExactGrant
                } else {
                    DecisionReason::InheritedGrant
                };
                return Ok(AccessDecision::allow(
                    reason,
                    grant.id.clone(),
                    resource.id.clone(),
                    policy_version,
                ));
            }
            let Some(parent_id) = &resource.parent_id else {
                break;
            };
            resource = state
                .resources
                .get(&(check.tenant_id.clone(), parent_id.clone()))
                .ok_or_else(|| {
                    AuthzError::Backend(format!(
                        "resource {} references missing parent {parent_id}",
                        resource.id
                    ))
                })?;
        }
        Ok(AccessDecision::deny(
            DecisionReason::NoMatchingGrant,
            policy_version,
        ))
    }
}
