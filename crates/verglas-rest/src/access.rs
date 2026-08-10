//! HTTP administration and decision surface for universal Verglas authorization.
//!
//! The router depends only on the backend-neutral [`Authorizer`] contract, so
//! the local composition and a standalone cloud microVM serve identical bytes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Extension, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use verglas_authz::{
    AccessCheck, AccessDecision, AccessTokenMetadata, AccessTokenService, Action, Authorizer,
    AuthzError, Grant, GrantDelegation, GrantRevocation, Principal, PrincipalKind, ReplaceSecret,
    ResolveSecret, Resource, ScopedTokenClaims, SecretError, SecretKind, SecretService,
    TargetJwtRequest, TargetJwtSigner, TokenMintRequest, new_access_token_id,
};
use verglas_catalog::DatabaseId;
use verglas_database::DatabaseKind;

use crate::data_plane::{
    AuthenticatedPrincipal, AuthorizationFailure, AuthorizationQuestion, DataPlaneAuthorizer,
};
use crate::database::{DatabaseAuthorization, DatabaseAuthorizationError};
use crate::queue::{QueueAuthorization, QueueAuthorizationError};

/// Shared authorization backend mounted by local and standalone servers.
pub type AccessRuntime = Arc<dyn Authorizer>;

/// Shared encrypted-secret service mounted only by the standalone access process.
pub type SecretRuntime = Arc<SecretService>;

/// Audience used only for interactive access administration sessions.
pub const ACCESS_AUDIENCE: &str = "access";
/// Audience for a user-controlled CLI or SDK credential that may cross both
/// access-management and data-plane boundaries.
pub const CLI_AUDIENCE: &str = "verglas-cli";

/// Audience accepted only by the trusted policy-engine check endpoint.
pub const POLICY_ENGINE_AUDIENCE: &str = "policy-engine";

/// Audience shared by local and remote Verglas data-plane boundaries.
pub const DATA_PLANE_AUDIENCE: &str = "data-plane";

/// Signed marker that distinguishes an OS identity session from delegated tokens.
const IDENTITY_SESSION_MARKER: &str = "identity-session";

/// Maximum lifetime accepted for an OS identity assertion.
const MAX_ASSERTION_LIFETIME_SECONDS: u64 = 60;

/// Lifetime of the access session returned for a valid identity assertion.
const SESSION_LIFETIME_SECONDS: u64 = 12 * 60 * 60;

/// Authenticated access-service state shared by every route.
#[derive(Clone)]
pub struct AccessHttpRuntime {
    authorizer: AccessRuntime,
    tokens: Arc<AccessTokenService>,
    tenant_id: Arc<str>,
    identity_assertion_key: Option<Arc<[u8]>>,
    used_assertions: Arc<Mutex<BTreeMap<String, u64>>>,
    secrets: Option<SecretRuntime>,
    target_jwt_signer: Option<Arc<TargetJwtSigner>>,
}

impl AccessHttpRuntime {
    /// Creates a mandatory bearer-token access runtime without identity exchange.
    pub fn new(
        authorizer: AccessRuntime,
        tokens: Arc<AccessTokenService>,
        tenant_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            authorizer,
            tokens,
            tenant_id: tenant_id.into(),
            identity_assertion_key: None,
            used_assertions: Arc::new(Mutex::new(BTreeMap::new())),
            secrets: None,
            target_jwt_signer: None,
        }
    }

    /// Enables compact HS256 identity assertion exchange with one shared issuer key.
    #[must_use]
    pub fn with_identity_assertion_key(mut self, key: impl Into<Arc<[u8]>>) -> Self {
        self.identity_assertion_key = Some(key.into());
        self
    }

    /// Adds the encrypted secret lifecycle to the authenticated router.
    #[must_use]
    pub fn with_secrets(mut self, secrets: SecretRuntime) -> Self {
        self.secrets = Some(secrets);
        self
    }

    /// Enables short-lived target database JWT exchange and public key discovery.
    #[must_use]
    pub fn with_target_jwt_signer(mut self, signer: TargetJwtSigner) -> Self {
        self.target_jwt_signer = Some(Arc::new(signer));
        self
    }
}

#[async_trait::async_trait]
impl DataPlaneAuthorizer for AccessHttpRuntime {
    /// Returns the exact audience used by the local data-plane boundary.
    fn audience(&self) -> &str {
        DATA_PLANE_AUDIENCE
    }

    /// Verifies the forwarded bearer locally and evaluates the current policy.
    async fn authorize(
        &self,
        authorization: &str,
        question: AuthorizationQuestion,
    ) -> Result<AuthenticatedPrincipal, AuthorizationFailure> {
        if question.audience.as_ref() != DATA_PLANE_AUDIENCE {
            return Err(AuthorizationFailure::Unauthenticated);
        }
        let token = authorization
            .strip_prefix("Bearer ")
            .filter(|value| !value.is_empty())
            .ok_or(AuthorizationFailure::Unauthenticated)?;
        let now = unix_time();
        let mut claims = None;
        let mut backend_failure = false;
        for audience in [DATA_PLANE_AUDIENCE, CLI_AUDIENCE] {
            match self
                .tokens
                .authenticate(token, &self.tenant_id, audience, now)
                .await
            {
                Ok(candidate) => {
                    claims = Some(candidate);
                    break;
                }
                Err(AuthzError::Backend(_)) => backend_failure = true,
                Err(_) => {}
            }
        }
        let claims = claims.ok_or(if backend_failure {
            AuthorizationFailure::Unavailable
        } else {
            AuthorizationFailure::Unauthenticated
        })?;
        let principal_id = if claims.run_id.as_deref() == Some(IDENTITY_SESSION_MARKER) {
            self.tokens
                .get(&claims.tenant_id, &claims.token_id)
                .await
                .map_err(|_| AuthorizationFailure::Unauthenticated)?
                .parent_principal_id
        } else {
            claims.principal_id.clone()
        };
        let decision = self
            .authorizer
            .check(AccessCheck::new(
                &claims.tenant_id,
                &principal_id,
                question.resource_id,
                question.action,
            ))
            .await
            .map_err(|_| AuthorizationFailure::Unavailable)?;
        if !decision.allowed {
            return Err(AuthorizationFailure::Forbidden);
        }
        Ok(AuthenticatedPrincipal {
            tenant_id: claims.tenant_id,
            principal_id,
            token_id: claims.token_id,
            audience: claims.audience,
        })
    }
}

#[async_trait::async_trait]
impl DatabaseAuthorization for AccessHttpRuntime {
    /// Registers one database resource and its authenticated creator ownership grant.
    async fn create_database_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        database: &str,
        kind: DatabaseKind,
    ) -> Result<(), DatabaseAuthorizationError> {
        if principal.tenant_id != self.tenant_id.as_ref() {
            return Err(DatabaseAuthorizationError::new("tenant mismatch"));
        }
        let resource_id = format!("database/{database}");
        let resource = Resource::new(
            &principal.tenant_id,
            &resource_id,
            verglas_authz::ResourceKind::Database,
        )
        .with_parent("tenant");
        let created = match self.authorizer.create_resource(resource.clone()).await {
            Ok(_) => true,
            Err(AuthzError::Conflict(_)) => {
                let existing = self
                    .authorizer
                    .get_resource(&principal.tenant_id, &resource_id)
                    .await
                    .map_err(database_authorization_error)?;
                if existing != resource {
                    return Err(DatabaseAuthorizationError::new(
                        "database resource conflicts with an existing definition",
                    ));
                }
                false
            }
            Err(error) => return Err(database_authorization_error(error)),
        };
        let (service_id, service_actions) = match kind {
            DatabaseKind::Lakehouse => (
                "service/verglas-lakekeeper",
                BTreeSet::from([Action::CreateChild, Action::Modify]),
            ),
            DatabaseKind::Postgres => ("service/verglas-neon", BTreeSet::from([Action::Connect])),
        };
        match self
            .authorizer
            .create_principal(Principal::new(
                &principal.tenant_id,
                service_id,
                PrincipalKind::ServiceAccount,
            ))
            .await
        {
            Ok(_) | Err(AuthzError::Conflict(_)) => {}
            Err(error) => {
                if created {
                    let _ = self
                        .authorizer
                        .delete_resource(&principal.tenant_id, &resource_id)
                        .await;
                }
                return Err(database_authorization_error(error));
            }
        }
        let grants = [
            Grant::new(
                format!("database-owner/{database}/{}", principal.principal_id),
                &principal.tenant_id,
                &principal.principal_id,
                &resource_id,
                BTreeSet::from([Action::Own]),
            ),
            Grant::new(
                format!("database-service/{database}/{service_id}"),
                &principal.tenant_id,
                service_id,
                &resource_id,
                service_actions,
            ),
        ];
        for grant in grants {
            match self.authorizer.create_grant(grant).await {
                Ok(_) | Err(AuthzError::Conflict(_)) => {}
                Err(error) => {
                    if created {
                        let _ = self
                            .authorizer
                            .delete_resource(&principal.tenant_id, &resource_id)
                            .await;
                    }
                    return Err(database_authorization_error(error));
                }
            }
        }
        Ok(())
    }

    /// Deletes one database authorization subtree after the route's modify check succeeds.
    async fn delete_database_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        database: &str,
    ) -> Result<(), DatabaseAuthorizationError> {
        if principal.tenant_id != self.tenant_id.as_ref() {
            return Err(DatabaseAuthorizationError::new("tenant mismatch"));
        }
        let root_id = format!("database/{database}");
        let resources = self
            .authorizer
            .list_resources(&principal.tenant_id)
            .await
            .map_err(database_authorization_error)?;
        let by_id: BTreeMap<_, _> = resources
            .iter()
            .map(|resource| (resource.id.as_str(), resource))
            .collect();
        let mut descendants: Vec<_> = resources
            .iter()
            .filter(|resource| is_descendant(resource, &root_id, &by_id))
            .collect();
        descendants.sort_by_key(|resource| std::cmp::Reverse(resource_depth(resource, &by_id)));
        for resource in descendants {
            match self
                .authorizer
                .delete_resource(&principal.tenant_id, &resource.id)
                .await
            {
                Ok(()) | Err(AuthzError::NotFound(_)) => {}
                Err(error) => return Err(database_authorization_error(error)),
            }
        }
        match self
            .authorizer
            .delete_resource(&principal.tenant_id, &root_id)
            .await
        {
            Ok(()) | Err(AuthzError::NotFound(_)) => Ok(()),
            Err(error) => Err(database_authorization_error(error)),
        }
    }
}

#[async_trait::async_trait]
impl QueueAuthorization for AccessHttpRuntime {
    /// Registers one queue resource and grants its authenticated creator ownership.
    async fn create_queue_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        queue: &str,
    ) -> Result<(), QueueAuthorizationError> {
        if principal.tenant_id != self.tenant_id.as_ref() {
            return Err(QueueAuthorizationError::new("tenant mismatch"));
        }
        let resource_id = format!("queue/{queue}");
        let resource = Resource::new(
            &principal.tenant_id,
            &resource_id,
            verglas_authz::ResourceKind::Queue,
        )
        .with_parent("tenant");
        let created = match self.authorizer.create_resource(resource.clone()).await {
            Ok(_) => true,
            Err(AuthzError::Conflict(_)) => {
                let existing = self
                    .authorizer
                    .get_resource(&principal.tenant_id, &resource_id)
                    .await
                    .map_err(queue_authorization_error)?;
                if existing != resource {
                    return Err(QueueAuthorizationError::new(
                        "queue resource conflicts with an existing definition",
                    ));
                }
                false
            }
            Err(error) => return Err(queue_authorization_error(error)),
        };
        let grant = Grant::new(
            format!("queue-owner/{queue}/{}", principal.principal_id),
            &principal.tenant_id,
            &principal.principal_id,
            &resource_id,
            BTreeSet::from([Action::Own]),
        );
        match self.authorizer.create_grant(grant).await {
            Ok(_) | Err(AuthzError::Conflict(_)) => Ok(()),
            Err(error) => {
                if created {
                    let _ = self
                        .authorizer
                        .delete_resource(&principal.tenant_id, &resource_id)
                        .await;
                }
                Err(queue_authorization_error(error))
            }
        }
    }

    /// Removes one queue authorization resource after its runtime is gone.
    async fn delete_queue_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        queue: &str,
    ) -> Result<(), QueueAuthorizationError> {
        if principal.tenant_id != self.tenant_id.as_ref() {
            return Err(QueueAuthorizationError::new("tenant mismatch"));
        }
        match self
            .authorizer
            .delete_resource(&principal.tenant_id, &format!("queue/{queue}"))
            .await
        {
            Ok(()) | Err(AuthzError::NotFound(_)) => Ok(()),
            Err(error) => Err(queue_authorization_error(error)),
        }
    }
}

/// Returns whether a resource has the database root in its parent chain.
fn is_descendant(
    resource: &Resource,
    root_id: &str,
    resources: &BTreeMap<&str, &Resource>,
) -> bool {
    let mut parent = resource.parent_id.as_deref();
    while let Some(parent_id) = parent {
        if parent_id == root_id {
            return true;
        }
        parent = resources
            .get(parent_id)
            .and_then(|candidate| candidate.parent_id.as_deref());
    }
    false
}

/// Counts parent edges for deterministic child-before-parent deletion.
fn resource_depth(resource: &Resource, resources: &BTreeMap<&str, &Resource>) -> usize {
    let mut depth = 0;
    let mut parent = resource.parent_id.as_deref();
    while let Some(parent_id) = parent {
        depth += 1;
        parent = resources
            .get(parent_id)
            .and_then(|candidate| candidate.parent_id.as_deref());
    }
    depth
}

/// Removes backend detail from a database authorization lifecycle failure.
fn database_authorization_error(error: AuthzError) -> DatabaseAuthorizationError {
    DatabaseAuthorizationError::new(error.to_string())
}

/// Removes backend detail from a queue authorization lifecycle failure.
fn queue_authorization_error(error: AuthzError) -> QueueAuthorizationError {
    QueueAuthorizationError::new(error.to_string())
}

/// Identity established only after signed-token and durable registry validation.
#[derive(Debug, Clone)]
struct AuthenticatedIdentity {
    claims: ScopedTokenClaims,
    actor_principal_id: String,
}

/// Tenant-local secret HTTP state owned by one access process.
#[derive(Clone)]
struct SecretHttpRuntime {
    access: AccessHttpRuntime,
    secrets: SecretRuntime,
}

/// Builds the complete access administration and decision API.
pub fn router(runtime: AccessHttpRuntime) -> Router {
    let protected = Router::new()
        .route(
            "/v1/access/principals",
            post(create_principal).get(list_principals),
        )
        .route(
            "/v1/access/principals/{id}",
            get(get_principal).delete(delete_principal),
        )
        .route(
            "/v1/access/resources",
            post(create_resource).get(list_resources),
        )
        .route(
            "/v1/access/resources/{id}",
            get(get_resource).delete(delete_resource),
        )
        .route("/v1/access/grants", post(create_grant).get(list_grants))
        .route("/v1/access/delegations", post(delegate_grant))
        .route("/v1/access/revocations", post(revoke_grant))
        .route("/v1/access/grants/{id}", delete(delete_grant))
        .route("/v1/access/tokens", post(create_token).get(list_tokens))
        .route("/v1/access/tokens/{id}", delete(revoke_token))
        .route("/v1/access/database-tokens", post(create_database_token))
        .route_layer(from_fn_with_state(runtime.clone(), require_access_identity));
    let mut routes = Router::new()
        .route("/v1/access/sessions", post(create_session))
        .route("/v1/access/authorize", post(authorize))
        .route("/.well-known/jwks.json", get(target_jwks))
        .route("/v1/access/check", post(check_access))
        .route("/v1/access/policy/resources", post(sync_resource))
        .route(
            "/v1/access/policy/resources/{*id}",
            delete(delete_synced_resource),
        )
        .route("/v1/access/policy/principals", post(sync_principal))
        .route(
            "/v1/access/policy/principals/{*id}",
            delete(delete_synced_principal),
        )
        .merge(protected)
        .with_state(runtime.clone());
    if let Some(secrets) = runtime.secrets.clone() {
        routes = routes.merge(secret_router(SecretHttpRuntime {
            access: runtime,
            secrets,
        }));
    }
    routes
}

/// Builds the secret-specific routes with an isolated state type.
fn secret_router(runtime: SecretHttpRuntime) -> Router {
    Router::new()
        .route("/v1/secrets", post(create_secret).get(list_secrets))
        .route("/v1/access/secrets/resolve", post(resolve_secret))
        .route("/v1/secrets/{id}", get(get_secret).put(replace_secret))
        .route_layer(from_fn_with_state(
            runtime.access.clone(),
            require_access_identity,
        ))
        .with_state(runtime)
}

/// Stable empty success response for idempotent-looking HTTP clients.
#[derive(Debug, Serialize)]
struct Deleted {
    deleted: bool,
}

/// Principal declaration whose tenant is always derived from the bearer.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreatePrincipalBody {
    id: String,
    kind: PrincipalKind,
    parent_id: Option<String>,
}

/// Resource declaration whose tenant is always derived from the bearer.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateResourceBody {
    id: String,
    kind: verglas_authz::ResourceKind,
    parent_id: Option<String>,
}

/// Grant declaration whose tenant and actor are always derived from the bearer.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateGrantBody {
    id: String,
    principal_id: String,
    resource_id: String,
    actions: BTreeSet<Action>,
}

impl CreateGrantBody {
    /// Binds an actor-free wire declaration to the authenticated tenant.
    fn into_grant(self, tenant_id: &str) -> Grant {
        Grant::new(
            self.id,
            tenant_id,
            self.principal_id,
            self.resource_id,
            self.actions,
        )
    }
}

/// JSON create body whose value is consumed immediately by the encryption boundary.
#[derive(Deserialize)]
struct CreateSecretBody {
    name: String,
    #[serde(rename = "type")]
    kind: SecretKind,
    scope: String,
    value: String,
}

/// JSON replacement body bound to an acting principal.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplaceSecretBody {
    value: String,
}

/// JSON resolution body for one provider URI.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveSecretBody {
    kind: SecretKind,
    uri: String,
}

/// Authorized material returned only by the explicit resolution endpoint.
#[derive(Debug, Serialize)]
struct ResolvedSecretBody {
    resource_id: String,
    version: u64,
    scope: String,
    value: String,
}

/// Creates one principal.
async fn create_principal(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Json(body): Json<CreatePrincipalBody>,
) -> Response {
    match body.parent_id.as_deref() {
        Some(parent_id) if parent_id == identity.actor_principal_id => {}
        _ => {
            if let Err(response) = require_management(&runtime, &identity).await {
                return response;
            }
        }
    }
    let mut principal = Principal::new(&identity.claims.tenant_id, body.id, body.kind);
    principal.parent_id = body.parent_id;
    result(
        StatusCode::CREATED,
        runtime.authorizer.create_principal(principal).await,
    )
}

/// Returns one tenant-scoped principal.
async fn get_principal(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = require_management(&runtime, &identity).await {
        return response;
    }
    result(
        StatusCode::OK,
        runtime
            .authorizer
            .get_principal(&identity.claims.tenant_id, &id)
            .await,
    )
}

/// Lists principals in one tenant.
async fn list_principals(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
) -> Response {
    if let Err(response) = require_management(&runtime, &identity).await {
        return response;
    }
    result(
        StatusCode::OK,
        runtime
            .authorizer
            .list_principals(&identity.claims.tenant_id)
            .await,
    )
}

/// Deletes one principal and its grants.
async fn delete_principal(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path(id): Path<String>,
) -> Response {
    let owned_child = runtime
        .authorizer
        .get_principal(&identity.claims.tenant_id, &id)
        .await
        .is_ok_and(|principal| {
            principal.parent_id.as_deref() == Some(&identity.actor_principal_id)
        });
    if !owned_child && let Err(response) = require_management(&runtime, &identity).await {
        return response;
    }
    deleted(
        runtime
            .authorizer
            .delete_principal(&identity.claims.tenant_id, &id)
            .await,
    )
}

/// Creates one resource.
async fn create_resource(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Json(body): Json<CreateResourceBody>,
) -> Response {
    match body.parent_id.as_deref() {
        Some(parent_id) => {
            if let Err(response) = require_action(
                &runtime,
                &identity.actor_principal_id,
                parent_id,
                Action::CreateChild,
            )
            .await
            {
                return response;
            }
        }
        None => {
            if let Err(response) = require_management(&runtime, &identity).await {
                return response;
            }
        }
    }
    let mut resource = Resource::new(&identity.claims.tenant_id, body.id, body.kind);
    resource.parent_id = body.parent_id;
    result(
        StatusCode::CREATED,
        runtime.authorizer.create_resource(resource).await,
    )
}

/// Returns one tenant-scoped resource.
async fn get_resource(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = require_action(
        &runtime,
        &identity.actor_principal_id,
        &id,
        Action::Discover,
    )
    .await
    {
        return response;
    }
    result(
        StatusCode::OK,
        runtime
            .authorizer
            .get_resource(&identity.claims.tenant_id, &id)
            .await,
    )
}

/// Lists resources in one tenant.
async fn list_resources(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
) -> Response {
    let resources = match runtime
        .authorizer
        .list_resources(&identity.claims.tenant_id)
        .await
    {
        Ok(resources) => resources,
        Err(error) => return error_response(error),
    };
    let mut visible = Vec::new();
    for resource in resources {
        match runtime
            .authorizer
            .check(AccessCheck::new(
                &identity.claims.tenant_id,
                &identity.actor_principal_id,
                &resource.id,
                Action::Discover,
            ))
            .await
        {
            Ok(decision) if decision.allowed => visible.push(resource),
            Ok(_) => {}
            Err(error) => return error_response(error),
        }
    }
    (StatusCode::OK, Json(visible)).into_response()
}

/// Deletes one childless resource and its originating grants.
async fn delete_resource(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) =
        require_action(&runtime, &identity.actor_principal_id, &id, Action::Modify).await
    {
        return response;
    }
    deleted(
        runtime
            .authorizer
            .delete_resource(&identity.claims.tenant_id, &id)
            .await,
    )
}

/// Creates one explicit grant.
async fn create_grant(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Json(body): Json<CreateGrantBody>,
) -> Response {
    let grant = body.into_grant(&identity.claims.tenant_id);
    result(
        StatusCode::CREATED,
        runtime
            .authorizer
            .delegate_grant(GrantDelegation::new(identity.actor_principal_id, grant))
            .await,
    )
}

/// Body for delegation; the authenticated bearer supplies the actor.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DelegateGrantBody {
    grant: CreateGrantBody,
}

/// Creates a grant under an existing principal's bounded delegation authority.
async fn delegate_grant(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Json(body): Json<DelegateGrantBody>,
) -> Response {
    let grant = body.grant.into_grant(&identity.claims.tenant_id);
    result(
        StatusCode::CREATED,
        runtime
            .authorizer
            .delegate_grant(GrantDelegation::new(identity.actor_principal_id, grant))
            .await,
    )
}

/// Body for revocation; the authenticated bearer supplies the actor and tenant.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeGrantBody {
    grant_id: String,
}

/// Revokes a grant under the actor's bounded grant-management authority.
async fn revoke_grant(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Json(body): Json<RevokeGrantBody>,
) -> Response {
    deleted(
        runtime
            .authorizer
            .revoke_grant(GrantRevocation {
                tenant_id: identity.claims.tenant_id,
                actor_principal_id: identity.actor_principal_id,
                grant_id: body.grant_id,
            })
            .await,
    )
}

/// Optional principal filter for bounded grant inventory.
#[derive(Debug, Deserialize)]
struct GrantListQuery {
    principal_id: Option<String>,
}

/// Lists grants for an owned child or, under management authority, the tenant.
async fn list_grants(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Query(query): Query<GrantListQuery>,
) -> Response {
    if let Some(principal_id) = &query.principal_id {
        let owned = principal_id == &identity.actor_principal_id
            || runtime
                .authorizer
                .get_principal(&identity.claims.tenant_id, principal_id)
                .await
                .is_ok_and(|principal| {
                    principal.parent_id.as_deref() == Some(&identity.actor_principal_id)
                });
        if !owned && let Err(response) = require_management(&runtime, &identity).await {
            return response;
        }
    } else if let Err(response) = require_management(&runtime, &identity).await {
        return response;
    }
    let grants = runtime
        .authorizer
        .list_grants(&identity.claims.tenant_id)
        .await
        .map(|grants| match query.principal_id {
            Some(principal_id) => grants
                .into_iter()
                .filter(|grant| grant.principal_id == principal_id)
                .collect(),
            None => grants,
        });
    result(StatusCode::OK, grants)
}

/// Deletes one grant.
async fn delete_grant(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path(id): Path<String>,
) -> Response {
    deleted(
        runtime
            .authorizer
            .revoke_grant(GrantRevocation {
                tenant_id: identity.claims.tenant_id,
                actor_principal_id: identity.actor_principal_id,
                grant_id: id,
            })
            .await,
    )
}

/// Request accepted by the public token-backed authorization boundary.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorizeBody {
    audience: String,
    resource_id: String,
    action: Action,
}

/// Verified identity returned beside an explainable policy decision.
#[derive(Debug, Serialize)]
struct AuthorizedIdentity {
    tenant_id: String,
    principal_id: String,
    token_id: String,
    audience: String,
}

/// Public authorization response consumed by remote data-plane boundaries.
#[derive(Debug, Serialize)]
struct AuthorizeResponse {
    identity: AuthorizedIdentity,
    decision: AccessDecision,
}

/// Verifies one bearer against its requested audience and evaluates current policy.
async fn authorize(
    State(runtime): State<AccessHttpRuntime>,
    headers: HeaderMap,
    Json(body): Json<AuthorizeBody>,
) -> Response {
    let identity = if matches!(
        body.audience.as_str(),
        ACCESS_AUDIENCE | DATA_PLANE_AUDIENCE
    ) {
        authenticate_any(&runtime, &headers, &[body.audience.as_str(), CLI_AUDIENCE]).await
    } else {
        authenticate(&runtime, &headers, &body.audience).await
    };
    let identity = match identity {
        Ok(identity) => identity,
        Err(response) => return response,
    };
    let decision = match runtime
        .authorizer
        .check(AccessCheck::new(
            &identity.claims.tenant_id,
            &identity.actor_principal_id,
            body.resource_id,
            body.action,
        ))
        .await
    {
        Ok(decision) => decision,
        Err(error) => return error_response(error),
    };
    (
        StatusCode::OK,
        Json(AuthorizeResponse {
            identity: AuthorizedIdentity {
                tenant_id: identity.claims.tenant_id,
                principal_id: identity.actor_principal_id,
                token_id: identity.claims.token_id,
                audience: identity.claims.audience,
            },
            decision,
        }),
    )
        .into_response()
}

/// Evaluates one full policy question only for trusted policy-engine services.
async fn check_access(
    State(runtime): State<AccessHttpRuntime>,
    headers: HeaderMap,
    Json(check): Json<AccessCheck>,
) -> Response {
    let identity = match authenticate_policy_engine(&runtime, &headers).await {
        Ok(identity) => identity,
        Err(response) => return response,
    };
    if check.tenant_id != identity.claims.tenant_id {
        return StatusCode::FORBIDDEN.into_response();
    }
    result(StatusCode::OK, runtime.authorizer.check(check).await)
}

/// Idempotently registers one policy-engine resource after a parent-scoped check.
async fn sync_resource(
    State(runtime): State<AccessHttpRuntime>,
    headers: HeaderMap,
    Json(body): Json<CreateResourceBody>,
) -> Response {
    let identity = match authenticate_policy_engine(&runtime, &headers).await {
        Ok(identity) => identity,
        Err(response) => return response,
    };
    if identity.actor_principal_id != "service/verglas-lakekeeper"
        || matches!(
            body.kind,
            verglas_authz::ResourceKind::Tenant | verglas_authz::ResourceKind::Database
        )
        || !is_lakekeeper_resource_id(&body.id)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(parent_id) = body.parent_id.clone() else {
        return error_response(AuthzError::Invalid(
            "policy-synced resources require a parent".to_owned(),
        ));
    };
    let Some(database_root) = lakekeeper_database_root(&body.id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    if parent_id != database_root
        && (!parent_id.starts_with(&format!("{database_root}/lakekeeper/")))
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    if let Err(response) = require_action(
        &runtime,
        &identity.actor_principal_id,
        &parent_id,
        Action::CreateChild,
    )
    .await
    {
        return response;
    }
    let mut resource = Resource::new(&identity.claims.tenant_id, body.id, body.kind);
    resource.parent_id = Some(parent_id);
    match runtime.authorizer.create_resource(resource.clone()).await {
        Ok(created) => (StatusCode::CREATED, Json(created)).into_response(),
        Err(AuthzError::Conflict(_)) => match runtime
            .authorizer
            .get_resource(&identity.claims.tenant_id, &resource.id)
            .await
        {
            Ok(existing) if existing == resource => {
                (StatusCode::OK, Json(existing)).into_response()
            }
            Ok(_) => error_response(AuthzError::Conflict(
                "resource exists with a different definition".to_owned(),
            )),
            Err(error) => error_response(error),
        },
        Err(error) => error_response(error),
    }
}

/// Idempotently removes one policy-engine resource under current modify authority.
async fn delete_synced_resource(
    State(runtime): State<AccessHttpRuntime>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let identity = match authenticate_policy_engine(&runtime, &headers).await {
        Ok(identity) => identity,
        Err(response) => return response,
    };
    let resource = match runtime
        .authorizer
        .get_resource(&identity.claims.tenant_id, &id)
        .await
    {
        Ok(resource) => resource,
        Err(AuthzError::NotFound(_)) => {
            return (StatusCode::OK, Json(Deleted { deleted: true })).into_response();
        }
        Err(error) => return error_response(error),
    };
    if identity.actor_principal_id != "service/verglas-lakekeeper"
        || resource.parent_id.is_none()
        || !is_lakekeeper_resource_id(&resource.id)
        || matches!(
            resource.kind,
            verglas_authz::ResourceKind::Tenant | verglas_authz::ResourceKind::Database
        )
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    if let Err(response) = require_action(
        &runtime,
        &identity.actor_principal_id,
        &resource.id,
        Action::Modify,
    )
    .await
    {
        return response;
    }
    deleted(
        runtime
            .authorizer
            .delete_resource(&identity.claims.tenant_id, &resource.id)
            .await,
    )
}

/// Idempotently registers one policy principal under tenant create-child authority.
async fn sync_principal(
    State(runtime): State<AccessHttpRuntime>,
    headers: HeaderMap,
    Json(body): Json<CreatePrincipalBody>,
) -> Response {
    let identity = match authenticate_policy_engine(&runtime, &headers).await {
        Ok(identity) => identity,
        Err(response) => return response,
    };
    if identity.actor_principal_id != "service/verglas-lakekeeper"
        || !is_lakekeeper_principal(&body.id, body.kind)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let mut principal = Principal::new(&identity.claims.tenant_id, body.id, body.kind);
    principal.parent_id = body.parent_id;
    match runtime.authorizer.create_principal(principal.clone()).await {
        Ok(created) => (StatusCode::CREATED, Json(created)).into_response(),
        Err(AuthzError::Conflict(_)) => match runtime
            .authorizer
            .get_principal(&identity.claims.tenant_id, &principal.id)
            .await
        {
            Ok(existing) if existing == principal => {
                (StatusCode::OK, Json(existing)).into_response()
            }
            Ok(_) => error_response(AuthzError::Conflict(
                "principal exists with a different definition".to_owned(),
            )),
            Err(error) => error_response(error),
        },
        Err(error) => error_response(error),
    }
}

/// Idempotently removes one policy principal under tenant grant-management authority.
async fn delete_synced_principal(
    State(runtime): State<AccessHttpRuntime>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let identity = match authenticate_policy_engine(&runtime, &headers).await {
        Ok(identity) => identity,
        Err(response) => return response,
    };
    if identity.actor_principal_id != "service/verglas-lakekeeper" {
        return StatusCode::FORBIDDEN.into_response();
    }
    if id.starts_with("user/") {
        return (StatusCode::OK, Json(Deleted { deleted: true })).into_response();
    }
    if !id.starts_with("lakekeeper-role/") {
        return StatusCode::FORBIDDEN.into_response();
    }
    match runtime
        .authorizer
        .delete_principal(&identity.claims.tenant_id, &id)
        .await
    {
        Ok(()) | Err(AuthzError::NotFound(_)) => {
            (StatusCode::OK, Json(Deleted { deleted: true })).into_response()
        }
        Err(error) => error_response(error),
    }
}

/// Returns whether an ID belongs beneath a canonical database Lakekeeper subtree.
fn is_lakekeeper_resource_id(id: &str) -> bool {
    lakekeeper_database_root(id).is_some()
}

/// Extracts the canonical database root from a Lakekeeper-owned resource ID.
fn lakekeeper_database_root(id: &str) -> Option<String> {
    let (database, _) = id.split_once("/lakekeeper/")?;
    if database.starts_with("database/") && database.len() > "database/".len() {
        Some(database.to_owned())
    } else {
        None
    }
}

/// Accepts only shared user identities or Lakekeeper-owned role principals.
fn is_lakekeeper_principal(id: &str, kind: PrincipalKind) -> bool {
    matches!(
        (kind, id),
        (PrincipalKind::User, value) if value.starts_with("user/")
    ) || matches!(
        (kind, id),
        (PrincipalKind::Role, value) if value.starts_with("lakekeeper-role/")
    )
}

/// Requested resource grant for a new child token.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenGrantBody {
    resource_id: String,
    actions: BTreeSet<Action>,
}

/// Bounded personal or process token request.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTokenBody {
    name: String,
    audience: String,
    expires_in_seconds: u64,
    grants: Vec<TokenGrantBody>,
}

/// One-time bearer response flattened beside safe registry metadata.
#[derive(Serialize)]
struct CreatedTokenResponse<'a> {
    token: &'a str,
    #[serde(flatten)]
    metadata: AccessTokenMetadata,
}

/// Optional owner-only token inventory filter.
#[derive(Debug, Deserialize)]
struct TokenListQuery {
    principal_id: Option<String>,
}

/// Compact identity assertion body issued by the authenticated OS backend.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionBody {
    assertion: String,
    audience: String,
}

/// Claims in the short-lived HS256 identity assertion.
#[derive(Debug, Deserialize)]
struct IdentityAssertionClaims {
    sub: String,
    tenant_id: String,
    aud: String,
    iat: u64,
    exp: u64,
    jti: String,
}

/// Minimal JOSE header accepted for identity assertions.
#[derive(Debug, Deserialize)]
struct IdentityAssertionHeader {
    alg: String,
    typ: String,
}

/// Access session response containing only the bearer and its expiration.
#[derive(Serialize)]
struct SessionResponse<'a> {
    token: &'a str,
    expires_at: u64,
}

/// Request for one short-lived database target credential.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabaseTokenBody {
    database_id: String,
    expires_in_seconds: Option<u64>,
}

/// One-time target bearer returned to an authorized database client.
#[derive(Serialize)]
struct DatabaseTokenResponse<'a> {
    token: &'a str,
    expires_at: u64,
}

/// Returns the public Ed25519 key set used by target database verifiers.
async fn target_jwks(State(runtime): State<AccessHttpRuntime>) -> Response {
    match runtime.target_jwt_signer {
        Some(signer) => (StatusCode::OK, Json(signer.jwks())).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// Exchanges current Connect authority for one database-bound EdDSA JWT.
async fn create_database_token(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Json(body): Json<DatabaseTokenBody>,
) -> Response {
    let Some(signer) = &runtime.target_jwt_signer else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let lifetime = body.expires_in_seconds.unwrap_or(15 * 60);
    if lifetime == 0 || lifetime > 15 * 60 {
        return error_response(AuthzError::Invalid(
            "database token lifetime must be between 1 and 900 seconds".to_owned(),
        ));
    }
    let database_id = match DatabaseId::new(&body.database_id) {
        Ok(database_id) => database_id,
        Err(_) => {
            return error_response(AuthzError::Invalid(
                "database_id must be a canonical database name".to_owned(),
            ));
        }
    };
    let database_id = database_id.as_str();
    let mut characters = database_id.chars();
    let Some(first) = characters.next() else {
        return error_response(AuthzError::Invalid(
            "database_id must be a canonical database name".to_owned(),
        ));
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !characters
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
    {
        return error_response(AuthzError::Invalid(
            "database_id must be a canonical database name".to_owned(),
        ));
    }
    let resource_id = format!("database/{database_id}");
    if let Err(response) = require_action(
        &runtime,
        &identity.actor_principal_id,
        &resource_id,
        Action::Connect,
    )
    .await
    {
        return response;
    }
    let now = unix_time();
    let expires_at = now.saturating_add(lifetime);
    let request = TargetJwtRequest::new(
        "verglas-neon",
        &identity.actor_principal_id,
        &identity.claims.token_id,
        &identity.claims.tenant_id,
        database_id,
        now,
        expires_at,
    );
    match signer.mint(request) {
        Ok(token) => (
            StatusCode::CREATED,
            Json(DatabaseTokenResponse {
                token: token.expose(),
                expires_at,
            }),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

/// Exchanges one single-use OS identity assertion for an access session token.
async fn create_session(
    State(runtime): State<AccessHttpRuntime>,
    Json(body): Json<SessionBody>,
) -> Response {
    let Some(key) = runtime.identity_assertion_key.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let now = unix_time();
    let claims = match verify_identity_assertion(&body.assertion, key, &runtime.tenant_id, now) {
        Ok(claims) => claims,
        Err(error) => return error_response(error),
    };
    if !matches!(
        body.audience.as_str(),
        ACCESS_AUDIENCE | DATA_PLANE_AUDIENCE
    ) {
        return error_response(AuthzError::Invalid(
            "session audience must be access or data-plane".to_owned(),
        ));
    }
    {
        let mut used = match runtime.used_assertions.lock() {
            Ok(used) => used,
            Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        };
        used.retain(|_, expires_at| *expires_at >= now);
        if used.insert(claims.jti.clone(), claims.exp).is_some() {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
    let principal = match runtime
        .authorizer
        .get_principal(&claims.tenant_id, &claims.sub)
        .await
    {
        Ok(principal) if principal.kind == PrincipalKind::User => principal,
        Ok(_) => return StatusCode::UNAUTHORIZED.into_response(),
        Err(AuthzError::NotFound(_)) => {
            match runtime
                .authorizer
                .create_principal(Principal::new(
                    &claims.tenant_id,
                    &claims.sub,
                    PrincipalKind::User,
                ))
                .await
            {
                Ok(principal) => principal,
                Err(error) => return error_response(error),
            }
        }
        Err(error) => return error_response(error),
    };
    let token_id = new_access_token_id();
    let session_principal_id = format!("session/{token_id}");
    if let Err(error) = runtime
        .authorizer
        .create_principal(
            Principal::new(
                &claims.tenant_id,
                &session_principal_id,
                PrincipalKind::Agent,
            )
            .with_parent(&principal.id),
        )
        .await
    {
        return error_response(error);
    }
    let policy_version = match runtime.authorizer.policy_version(&claims.tenant_id).await {
        Ok(version) => version,
        Err(error) => return error_response(error),
    };
    let expires_at = now.saturating_add(SESSION_LIFETIME_SECONDS);
    let request = TokenMintRequest::new(
        &token_id,
        &claims.tenant_id,
        &principal.id,
        &session_principal_id,
        "OS session",
        body.audience,
        policy_version,
        now,
        expires_at,
    )
    .with_run(IDENTITY_SESSION_MARKER);
    match runtime.tokens.mint(request).await {
        Ok(minted) => (
            StatusCode::CREATED,
            Json(SessionResponse {
                token: minted.token.expose(),
                expires_at,
            }),
        )
            .into_response(),
        Err(error) => {
            let _ = runtime
                .authorizer
                .delete_principal(&claims.tenant_id, &session_principal_id)
                .await;
            error_response(error)
        }
    }
}

/// Creates a child principal, delegates bounded grants, then mints one credential.
async fn create_token(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Json(body): Json<CreateTokenBody>,
) -> Response {
    let now = unix_time();
    let Some(expires_at) = now.checked_add(body.expires_in_seconds) else {
        return error_response(AuthzError::Invalid(
            "token lifetime is too large".to_owned(),
        ));
    };
    if body.expires_in_seconds == 0 || body.expires_in_seconds > 366 * 24 * 60 * 60 {
        return error_response(AuthzError::Invalid(
            "token lifetime must be between 1 second and 366 days".to_owned(),
        ));
    }
    if body.grants.iter().any(|grant| grant.actions.is_empty()) {
        return error_response(AuthzError::Invalid(
            "token grant action sets must not be empty".to_owned(),
        ));
    }
    if !matches!(body.audience.as_str(), CLI_AUDIENCE | DATA_PLANE_AUDIENCE) {
        return error_response(AuthzError::Invalid(
            "personal token audience must be verglas-cli or data-plane".to_owned(),
        ));
    }
    let token_id = new_access_token_id();
    let principal_id = format!("token/{token_id}");
    let principal = Principal::new(
        &identity.claims.tenant_id,
        &principal_id,
        PrincipalKind::ServiceAccount,
    )
    .with_parent(&identity.actor_principal_id);
    if let Err(error) = runtime.authorizer.create_principal(principal).await {
        return error_response(error);
    }
    for (index, requested) in body.grants.iter().enumerate() {
        let grant = Grant::new(
            format!("token-grant/{token_id}/{index}"),
            &identity.claims.tenant_id,
            &principal_id,
            &requested.resource_id,
            requested.actions.clone(),
        );
        if let Err(error) = runtime
            .authorizer
            .delegate_grant(GrantDelegation::new(&identity.actor_principal_id, grant))
            .await
        {
            let _ = runtime
                .authorizer
                .delete_principal(&identity.claims.tenant_id, &principal_id)
                .await;
            return error_response(error);
        }
    }
    let policy_version = match runtime
        .authorizer
        .policy_version(&identity.claims.tenant_id)
        .await
    {
        Ok(version) => version,
        Err(error) => return error_response(error),
    };
    let request = TokenMintRequest::new(
        &token_id,
        &identity.claims.tenant_id,
        &identity.actor_principal_id,
        &principal_id,
        body.name,
        body.audience,
        policy_version,
        now,
        expires_at,
    );
    match runtime.tokens.mint(request).await {
        Ok(minted) => (
            StatusCode::CREATED,
            Json(CreatedTokenResponse {
                token: minted.token.expose(),
                metadata: minted.metadata,
            }),
        )
            .into_response(),
        Err(error) => {
            let _ = runtime
                .authorizer
                .delete_principal(&identity.claims.tenant_id, &principal_id)
                .await;
            error_response(error)
        }
    }
}

/// Lists the caller's tokens, or another principal's inventory for an owner.
async fn list_tokens(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Query(query): Query<TokenListQuery>,
) -> Response {
    let principal_id = query
        .principal_id
        .unwrap_or_else(|| identity.actor_principal_id.clone());
    if principal_id != identity.actor_principal_id
        && let Err(response) = require_management(&runtime, &identity).await
    {
        return response;
    }
    result(
        StatusCode::OK,
        runtime
            .tokens
            .list(&identity.claims.tenant_id, &principal_id)
            .await,
    )
}

/// Revokes a self-owned token or any tenant token under grant-management authority.
async fn revoke_token(
    State(runtime): State<AccessHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path(id): Path<String>,
) -> Response {
    let metadata = match runtime.tokens.get(&identity.claims.tenant_id, &id).await {
        Ok(metadata) => metadata,
        Err(error) => return error_response(error),
    };
    if metadata.parent_principal_id != identity.actor_principal_id
        && metadata.id != identity.claims.token_id
        && let Err(response) = require_management(&runtime, &identity).await
    {
        return response;
    }
    result(
        StatusCode::OK,
        runtime
            .tokens
            .revoke(&identity.claims.tenant_id, &id, unix_time())
            .await,
    )
}

/// Creates one authorization resource and its encrypted first value.
async fn create_secret(
    State(runtime): State<SecretHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Json(body): Json<CreateSecretBody>,
) -> Response {
    secret_result(
        StatusCode::CREATED,
        runtime
            .secrets
            .create(verglas_authz::CreateSecret::new(
                runtime.access.tenant_id.as_ref(),
                identity.actor_principal_id,
                body.name,
                body.kind,
                body.scope,
                body.value.as_bytes(),
            ))
            .await,
    )
}

/// Returns one secret's public metadata without loading its value.
async fn get_secret(State(runtime): State<SecretHttpRuntime>, Path(id): Path<String>) -> Response {
    secret_result(
        StatusCode::OK,
        runtime
            .secrets
            .get(runtime.access.tenant_id.as_ref(), &id)
            .await,
    )
}

/// Lists public secret metadata without loading any values.
async fn list_secrets(State(runtime): State<SecretHttpRuntime>) -> Response {
    secret_result(
        StatusCode::OK,
        runtime
            .secrets
            .list(runtime.access.tenant_id.as_ref())
            .await,
    )
}

/// Replaces a secret's encrypted value while preserving its resource identity.
async fn replace_secret(
    State(runtime): State<SecretHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path(id): Path<String>,
    Json(body): Json<ReplaceSecretBody>,
) -> Response {
    secret_result(
        StatusCode::OK,
        runtime
            .secrets
            .replace(ReplaceSecret::new(
                runtime.access.tenant_id.as_ref(),
                identity.actor_principal_id,
                id,
                body.value.as_bytes(),
            ))
            .await,
    )
}

/// Resolves and decrypts the longest authorized provider scope for a trusted runtime.
async fn resolve_secret(
    State(runtime): State<SecretHttpRuntime>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Json(body): Json<ResolveSecretBody>,
) -> Response {
    let resolved = runtime
        .secrets
        .resolve(ResolveSecret::new(
            runtime.access.tenant_id.as_ref(),
            identity.actor_principal_id,
            body.kind,
            body.uri,
        ))
        .await
        .and_then(|resolved| {
            String::from_utf8(resolved.expose().to_vec())
                .map(|value| ResolvedSecretBody {
                    resource_id: resolved.resource_id,
                    version: resolved.version,
                    scope: resolved.scope,
                    value,
                })
                .map_err(|_| SecretError::Backend("secret value is not UTF-8".to_owned()))
        });
    secret_result(StatusCode::OK, resolved)
}

/// Authenticates access-management traffic and inserts a server-derived identity.
async fn require_access_identity(
    State(runtime): State<AccessHttpRuntime>,
    mut request: Request,
    next: Next,
) -> Response {
    match authenticate_any(
        &runtime,
        request.headers(),
        &[ACCESS_AUDIENCE, CLI_AUDIENCE],
    )
    .await
    {
        Ok(identity) => {
            request.extensions_mut().insert(identity);
            next.run(request).await
        }
        Err(response) => response,
    }
}

/// Validates a strict bearer and resolves an identity-session token to its human actor.
async fn authenticate(
    runtime: &AccessHttpRuntime,
    headers: &HeaderMap,
    audience: &str,
) -> Result<AuthenticatedIdentity, Response> {
    authenticate_any(runtime, headers, &[audience]).await
}

/// Validates a strict bearer against one of a bounded set of accepted audiences.
async fn authenticate_any(
    runtime: &AccessHttpRuntime,
    headers: &HeaderMap,
    audiences: &[&str],
) -> Result<AuthenticatedIdentity, Response> {
    let token = bearer(headers).ok_or_else(|| StatusCode::UNAUTHORIZED.into_response())?;
    let now = unix_time();
    let mut claims = None;
    for audience in audiences {
        if let Ok(candidate) = runtime
            .tokens
            .authenticate(token, &runtime.tenant_id, audience, now)
            .await
        {
            claims = Some(candidate);
            break;
        }
    }
    let claims = claims.ok_or_else(|| StatusCode::UNAUTHORIZED.into_response())?;
    let actor_principal_id = if claims.run_id.as_deref() == Some(IDENTITY_SESSION_MARKER) {
        runtime
            .tokens
            .get(&claims.tenant_id, &claims.token_id)
            .await
            .map_err(|_| StatusCode::UNAUTHORIZED.into_response())?
            .parent_principal_id
    } else {
        claims.principal_id.clone()
    };
    Ok(AuthenticatedIdentity {
        claims,
        actor_principal_id,
    })
}

/// Authenticates a policy-engine token whose delegating parent is a service account.
async fn authenticate_policy_engine(
    runtime: &AccessHttpRuntime,
    headers: &HeaderMap,
) -> Result<AuthenticatedIdentity, Response> {
    let identity = authenticate(runtime, headers, POLICY_ENGINE_AUDIENCE).await?;
    let parent = runtime
        .tokens
        .get(&identity.claims.tenant_id, &identity.claims.token_id)
        .await
        .map_err(error_response)?
        .parent_principal_id;
    let trusted = runtime
        .authorizer
        .get_principal(&identity.claims.tenant_id, &parent)
        .await
        .is_ok_and(|principal| principal.kind == PrincipalKind::ServiceAccount);
    if !trusted {
        return Err(StatusCode::FORBIDDEN.into_response());
    }
    Ok(AuthenticatedIdentity {
        claims: identity.claims,
        actor_principal_id: parent,
    })
}

/// Extracts one non-empty bearer value without accepting alternate schemes.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
}

/// Requires tenant grant-management authority for administrative inventory and mutation.
async fn require_management(
    runtime: &AccessHttpRuntime,
    identity: &AuthenticatedIdentity,
) -> Result<(), Response> {
    match runtime
        .authorizer
        .check(AccessCheck::new(
            &identity.claims.tenant_id,
            &identity.actor_principal_id,
            "tenant",
            Action::ManageGrants,
        ))
        .await
    {
        Ok(decision) if decision.allowed => Ok(()),
        Ok(_) => Err(StatusCode::FORBIDDEN.into_response()),
        Err(error) => Err(error_response(error)),
    }
}

/// Requires one current action for a principal on a stable resource.
async fn require_action(
    runtime: &AccessHttpRuntime,
    principal_id: &str,
    resource_id: &str,
    action: Action,
) -> Result<(), Response> {
    match runtime
        .authorizer
        .check(AccessCheck::new(
            runtime.tenant_id.as_ref(),
            principal_id,
            resource_id,
            action,
        ))
        .await
    {
        Ok(decision) if decision.allowed => Ok(()),
        Ok(_) => Err(StatusCode::FORBIDDEN.into_response()),
        Err(error) => Err(error_response(error)),
    }
}

/// Verifies the compact HS256 assertion header, signature, claims, and lifetime.
fn verify_identity_assertion(
    assertion: &str,
    key: &[u8],
    tenant_id: &str,
    now: u64,
) -> Result<IdentityAssertionClaims, AuthzError> {
    if assertion.len() > 16 * 1024 {
        return Err(AuthzError::Token(
            "identity assertion exceeds the size limit".to_owned(),
        ));
    }
    let mut segments = assertion.split('.');
    let encoded_header = segments
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AuthzError::Token("identity assertion header is missing".to_owned()))?;
    let encoded_claims = segments
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AuthzError::Token("identity assertion claims are missing".to_owned()))?;
    let signature = segments
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AuthzError::Token("identity assertion signature is missing".to_owned()))?;
    if segments.next().is_some() {
        return Err(AuthzError::Token(
            "identity assertion has too many segments".to_owned(),
        ));
    }
    let signature = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| AuthzError::Token("identity assertion signature is malformed".to_owned()))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|_| AuthzError::Token("identity assertion key is invalid".to_owned()))?;
    mac.update(format!("{encoded_header}.{encoded_claims}").as_bytes());
    mac.verify_slice(&signature)
        .map_err(|_| AuthzError::Token("identity assertion signature is invalid".to_owned()))?;
    let header: IdentityAssertionHeader = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(encoded_header)
            .map_err(|_| AuthzError::Token("identity assertion header is malformed".to_owned()))?,
    )
    .map_err(|_| AuthzError::Token("identity assertion header is invalid".to_owned()))?;
    if header.alg != "HS256" || header.typ != "JWT" {
        return Err(AuthzError::Token(
            "identity assertion must use an HS256 JWT header".to_owned(),
        ));
    }
    let claims: IdentityAssertionClaims =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded_claims).map_err(|_| {
            AuthzError::Token("identity assertion claims are malformed".to_owned())
        })?)
        .map_err(|_| AuthzError::Token("identity assertion claims are invalid".to_owned()))?;
    if claims.tenant_id != tenant_id
        || claims.aud != "verglas-access"
        || !claims.sub.starts_with("user/")
        || claims.jti.is_empty()
        || claims.exp <= claims.iat
        || claims.exp.saturating_sub(claims.iat) > MAX_ASSERTION_LIFETIME_SECONDS
        || now < claims.iat
        || now > claims.exp
    {
        return Err(AuthzError::Token(
            "identity assertion claims do not match this access service".to_owned(),
        ));
    }
    Ok(claims)
}

/// Returns the current Unix timestamp, saturating only before the epoch.
fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

/// Serializes one successful typed result or maps its stable authorization error.
fn result<T: Serialize>(status: StatusCode, value: Result<T, AuthzError>) -> Response {
    match value {
        Ok(value) => (status, Json(value)).into_response(),
        Err(error) => error_response(error),
    }
}

/// Serializes one secret result or maps its bounded failure category.
fn secret_result<T: Serialize>(status: StatusCode, value: Result<T, SecretError>) -> Response {
    match value {
        Ok(value) => (status, Json(value)).into_response(),
        Err(error) => secret_error_response(error),
    }
}

/// Serializes a successful delete without inventing a nullable resource body.
fn deleted(value: Result<(), AuthzError>) -> Response {
    result(StatusCode::OK, value.map(|()| Deleted { deleted: true }))
}

/// Maps backend-neutral failures to stable HTTP statuses and bounded JSON.
fn error_response(error: AuthzError) -> Response {
    let status = match &error {
        AuthzError::Invalid(_) | AuthzError::Token(_) => StatusCode::BAD_REQUEST,
        AuthzError::NotFound(_) => StatusCode::NOT_FOUND,
        AuthzError::Conflict(_) => StatusCode::CONFLICT,
        AuthzError::Forbidden(_) => StatusCode::FORBIDDEN,
        AuthzError::Backend(_) => StatusCode::BAD_GATEWAY,
    };
    (
        status,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}

/// Maps secret failures without including values or ciphertext in error bodies.
fn secret_error_response(error: SecretError) -> Response {
    let status = match &error {
        SecretError::Invalid(_) => StatusCode::BAD_REQUEST,
        SecretError::NotFound(_) => StatusCode::NOT_FOUND,
        SecretError::Conflict(_) => StatusCode::CONFLICT,
        SecretError::Forbidden(_) => StatusCode::FORBIDDEN,
        SecretError::Backend(_) => StatusCode::BAD_GATEWAY,
    };
    (
        status,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}
