//! OpenFGA implementation of the Verglas policy-engine contract.
//!
//! The adapter stores relationship tuples. Canonical principals, resources,
//! grants, and policy revisions remain in `verglas_permissions`.

use std::collections::BTreeSet;

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use verglas_authz::{AccessCheck, Action, AuthzError, Grant, PolicyEngine, Resource};

/// Friendly DSL compiled into the authorization model during provisioning.
pub const AUTHORIZATION_MODEL: &str = include_str!("model.fga");

/// OpenFGA API representation of [`AUTHORIZATION_MODEL`].
pub const AUTHORIZATION_MODEL_JSON: &str = include_str!("model.json");

/// Creates or discovers the dedicated store and pins its current model.
pub async fn bootstrap(
    endpoint: &str,
    store_name: &str,
    bearer_token: &str,
) -> Result<OpenFgaConfig, AuthzError> {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.is_empty() || store_name.is_empty() || bearer_token.is_empty() {
        return Err(AuthzError::Invalid(
            "OpenFGA endpoint, store name, and bearer token are required".to_owned(),
        ));
    }
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|error| AuthzError::Backend(error.to_string()))?;
    let stores = request(bearer_token, client.get(format!("{endpoint}/stores")))
        .await?
        .json::<StoreList>()
        .await
        .map_err(|error| AuthzError::Backend(error.to_string()))?;
    let store_id = match stores
        .stores
        .into_iter()
        .find(|store| store.name == store_name)
    {
        Some(store) => store.id,
        None => {
            request(
                bearer_token,
                client
                    .post(format!("{endpoint}/stores"))
                    .json(&serde_json::json!({"name": store_name})),
            )
            .await?
            .json::<Store>()
            .await
            .map_err(|error| AuthzError::Backend(error.to_string()))?
            .id
        }
    };
    let models = request(
        bearer_token,
        client.get(format!("{endpoint}/stores/{store_id}/authorization-models")),
    )
    .await?
    .json::<ModelList>()
    .await
    .map_err(|error| AuthzError::Backend(error.to_string()))?;
    let authorization_model_id = match models.authorization_models.into_iter().next() {
        Some(model) => model.id,
        None => {
            let model: serde_json::Value = serde_json::from_str(AUTHORIZATION_MODEL_JSON)
                .map_err(|error| AuthzError::Backend(error.to_string()))?;
            request(
                bearer_token,
                client
                    .post(format!("{endpoint}/stores/{store_id}/authorization-models"))
                    .json(&model),
            )
            .await?
            .json::<ModelId>()
            .await
            .map_err(|error| AuthzError::Backend(error.to_string()))?
            .authorization_model_id
        }
    };
    OpenFgaConfig::new(endpoint, store_id, authorization_model_id, bearer_token)
}

/// Sends one bootstrap request and bounds backend diagnostics.
async fn request(
    bearer_token: &str,
    request: reqwest::RequestBuilder,
) -> Result<reqwest::Response, AuthzError> {
    let response = request
        .bearer_auth(bearer_token)
        .send()
        .await
        .map_err(|error| AuthzError::Backend(error.to_string()))?;
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let detail = response
        .text()
        .await
        .unwrap_or_else(|_| "response body unavailable".to_owned());
    Err(AuthzError::Backend(format!(
        "OpenFGA bootstrap returned {status}: {detail}"
    )))
}

#[derive(Debug, Deserialize)]
struct StoreList {
    #[serde(default)]
    stores: Vec<Store>,
}

#[derive(Debug, Deserialize)]
struct Store {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ModelList {
    #[serde(default)]
    authorization_models: Vec<Model>,
}

#[derive(Debug, Deserialize)]
struct Model {
    id: String,
}

#[derive(Debug, Deserialize)]
struct ModelId {
    authorization_model_id: String,
}

/// Required connection details for one tenant's OpenFGA store.
#[derive(Debug, Clone)]
pub struct OpenFgaConfig {
    /// Base HTTP endpoint of the tenant-local OpenFGA process.
    pub endpoint: String,
    /// Store containing this tenant deployment's relationship tuples.
    pub store_id: String,
    /// Immutable model revision used for checks.
    pub authorization_model_id: String,
    /// Pre-shared bearer credential accepted by OpenFGA.
    pub bearer_token: String,
}

impl OpenFgaConfig {
    /// Constructs a complete fail-closed OpenFGA configuration.
    pub fn new(
        endpoint: impl Into<String>,
        store_id: impl Into<String>,
        authorization_model_id: impl Into<String>,
        bearer_token: impl Into<String>,
    ) -> Result<Self, AuthzError> {
        let config = Self {
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            store_id: store_id.into(),
            authorization_model_id: authorization_model_id.into(),
            bearer_token: bearer_token.into(),
        };
        config.validate()?;
        Ok(config)
    }

    /// Rejects missing endpoints, identifiers, and service credentials.
    fn validate(&self) -> Result<(), AuthzError> {
        if self.endpoint.is_empty()
            || self.store_id.is_empty()
            || self.authorization_model_id.is_empty()
            || self.bearer_token.is_empty()
        {
            return Err(AuthzError::Invalid(
                "OpenFGA endpoint, store, model, and bearer token are required".to_owned(),
            ));
        }
        Ok(())
    }
}

/// HTTP policy adapter used by the standalone Verglas access service.
#[derive(Debug, Clone)]
pub struct OpenFgaPolicyEngine {
    client: Client,
    config: OpenFgaConfig,
}

impl OpenFgaPolicyEngine {
    /// Constructs an adapter with bounded request timeouts.
    pub fn new(config: OpenFgaConfig) -> Result<Self, AuthzError> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|error| AuthzError::Backend(error.to_string()))?;
        Ok(Self { client, config })
    }

    /// Sends one authenticated API request and requires success.
    async fn send<T: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<reqwest::Response, AuthzError> {
        let response = self
            .client
            .post(format!(
                "{}/stores/{}/{}",
                self.config.endpoint, self.config.store_id, path
            ))
            .bearer_auth(&self.config.bearer_token)
            .json(body)
            .send()
            .await
            .map_err(|error| AuthzError::Backend(error.to_string()))?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let detail = response
            .text()
            .await
            .unwrap_or_else(|_| "response body unavailable".to_owned());
        Err(AuthzError::Backend(format!(
            "OpenFGA returned {status}: {detail}"
        )))
    }

    /// Writes or deletes one set of relationship tuples.
    async fn mutate(&self, operation: &str, tuples: Vec<TupleKey>) -> Result<(), AuthzError> {
        if tuples.is_empty() {
            return Ok(());
        }
        let body = serde_json::json!({ operation: { "tuple_keys": tuples } });
        self.send("write", &body).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TupleKey {
    user: String,
    relation: String,
    object: String,
}

#[derive(Debug, Serialize)]
struct CheckRequest {
    tuple_key: TupleKey,
    authorization_model_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct CheckResponse {
    allowed: bool,
}

#[async_trait]
impl PolicyEngine for OpenFgaPolicyEngine {
    /// Writes exact and implied action tuples for one durable grant.
    async fn write_grant(&self, grant: &Grant) -> Result<(), AuthzError> {
        self.mutate("writes", grant_tuples(grant)).await
    }

    /// Removes every exact and implied action tuple for one durable grant.
    async fn delete_grant(&self, grant: &Grant) -> Result<(), AuthzError> {
        self.mutate("deletes", grant_tuples(grant)).await
    }

    /// Writes a resource inheritance edge when a parent exists.
    async fn write_resource(&self, resource: &Resource) -> Result<(), AuthzError> {
        let Some(parent_id) = &resource.parent_id else {
            return Ok(());
        };
        self.mutate("writes", vec![parent_tuple(resource, parent_id)])
            .await
    }

    /// Deletes a resource inheritance edge when a parent exists.
    async fn delete_resource(&self, resource: &Resource) -> Result<(), AuthzError> {
        let Some(parent_id) = &resource.parent_id else {
            return Ok(());
        };
        self.mutate("deletes", vec![parent_tuple(resource, parent_id)])
            .await
    }

    /// Evaluates one relation using the pinned authorization model revision.
    async fn check(&self, check: &AccessCheck) -> Result<bool, AuthzError> {
        let request = CheckRequest {
            tuple_key: TupleKey {
                user: principal_key(&check.tenant_id, &check.principal_id),
                relation: check.action.as_str().to_owned(),
                object: resource_key(&check.tenant_id, &check.resource_id),
            },
            authorization_model_id: self.config.authorization_model_id.clone(),
        };
        self.send("check", &request)
            .await?
            .json::<CheckResponse>()
            .await
            .map(|response| response.allowed)
            .map_err(|error| AuthzError::Backend(error.to_string()))
    }
}

/// Expands action implication into concrete OpenFGA relations.
fn grant_tuples(grant: &Grant) -> Vec<TupleKey> {
    all_actions()
        .into_iter()
        .filter(|requested| {
            grant
                .actions
                .iter()
                .any(|granted| granted.covers(*requested))
        })
        .map(Action::as_str)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|relation| TupleKey {
            user: principal_key(&grant.tenant_id, &grant.principal_id),
            relation: relation.to_owned(),
            object: resource_key(&grant.tenant_id, &grant.resource_id),
        })
        .collect()
}

/// Returns every public action in stable model order.
fn all_actions() -> [Action; 12] {
    [
        Action::Discover,
        Action::Describe,
        Action::Query,
        Action::Append,
        Action::Modify,
        Action::CreateChild,
        Action::Execute,
        Action::UseSecret,
        Action::Deploy,
        Action::PassGrants,
        Action::ManageGrants,
        Action::Own,
    ]
}

/// Constructs a tenant-qualified OpenFGA principal key.
fn principal_key(tenant_id: &str, principal_id: &str) -> String {
    format!("principal:{tenant_id}/{principal_id}")
}

/// Constructs a tenant-qualified OpenFGA resource key.
fn resource_key(tenant_id: &str, resource_id: &str) -> String {
    format!("resource:{tenant_id}/{resource_id}")
}

/// Constructs one parent tuple for a resource hierarchy edge.
fn parent_tuple(resource: &Resource, parent_id: &str) -> TupleKey {
    TupleKey {
        user: resource_key(&resource.tenant_id, parent_id),
        relation: "parent".to_owned(),
        object: resource_key(&resource.tenant_id, &resource.id),
    }
}
