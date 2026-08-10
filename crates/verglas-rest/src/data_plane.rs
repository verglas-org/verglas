//! Fail-closed bearer authorization for Verglas data-plane HTTP routes.
//!
//! The boundary forwards the opaque credential and one resource-action question
//! to the tenant access service. It trusts only the identity returned after the
//! access service validates token state and evaluates current policy.

use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use verglas_authz::{AccessDecision, Action};
use verglas_catalog::{CatalogRuntimeRegistry, DatabaseId};
use verglas_database::DatabaseManager;

use crate::access::CLI_AUDIENCE;

/// Verified identity inserted into a request after a successful access check.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AuthenticatedPrincipal {
    /// Tenant containing both the principal and protected resource.
    pub tenant_id: String,
    /// Token child principal evaluated by current policy.
    pub principal_id: String,
    /// Stable credential identifier used for revocation and audit.
    pub token_id: String,
    /// Audience verified by the access service.
    pub audience: String,
}

/// Exact bearer header whose token was verified by the access service.
///
/// The value stays private so debug output and downstream application code
/// cannot accidentally reveal it. Trusted transports may forward it unchanged.
#[derive(Clone)]
pub struct AuthenticatedBearer(axum::http::HeaderValue);

impl AuthenticatedBearer {
    /// Returns the verified header for an authenticated downstream hop.
    pub fn header_value(&self) -> &axum::http::HeaderValue {
        &self.0
    }
}

/// Immutable database identity resolved from a tenant-local route name after authorization.
#[derive(Clone, Debug)]
pub struct AuthenticatedDatabaseId(String);

impl AuthenticatedDatabaseId {
    /// Returns the policy and catalog identity, never the user-facing route name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One stable resource and least-privilege operation derived from an HTTP route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuthorizationQuestion {
    /// Receiving service selected by the data-plane boundary.
    pub audience: Arc<str>,
    /// Stable authorization resource registered in the tenant.
    pub resource_id: String,
    /// Operation required by the route.
    pub action: Action,
}

/// Successful access-service response containing verified identity and policy decision.
#[derive(Debug, Deserialize)]
struct AuthorizationAnswer {
    /// Identity derived from the bearer token, never caller-supplied fields.
    identity: AuthenticatedPrincipal,
    /// Decision evaluated against current grants.
    decision: AccessDecision,
}

/// Access-service client shared by all data-plane route checks.
#[derive(Clone)]
pub struct DataPlaneAccess {
    endpoint: Option<reqwest::Url>,
    audience: Arc<str>,
    http: reqwest::Client,
}

impl DataPlaneAccess {
    /// Creates a client for one access authority and expected audience.
    pub fn new(endpoint: impl AsRef<str>, audience: impl Into<Arc<str>>) -> Result<Self, String> {
        let mut endpoint = reqwest::Url::parse(endpoint.as_ref())
            .map_err(|error| format!("invalid access endpoint: {error}"))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err("access endpoint must use http or https".to_owned());
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        Ok(Self {
            endpoint: Some(endpoint),
            audience: audience.into(),
            http: reqwest::Client::new(),
        })
    }

    /// Creates a boundary that rejects protected traffic because no authority is configured.
    pub fn unavailable(audience: impl Into<Arc<str>>) -> Self {
        Self {
            endpoint: None,
            audience: audience.into(),
            http: reqwest::Client::new(),
        }
    }
}

/// Backend-neutral authorization boundary used by remote and colocated access services.
#[async_trait]
pub trait DataPlaneAuthorizer: Send + Sync {
    /// Returns the exact audience accepted by this authorization boundary.
    fn audience(&self) -> &str;

    /// Authenticates an opaque bearer and evaluates one current-policy question.
    async fn authorize(
        &self,
        authorization: &str,
        question: AuthorizationQuestion,
    ) -> Result<AuthenticatedPrincipal, AuthorizationFailure>;
}

/// Resolves a tenant-local database route name to its immutable authorization ID.
#[async_trait]
pub trait DatabaseResourceResolver: Send + Sync {
    /// Returns the immutable ID registered as `database/{id}` in policy.
    async fn resolve_database_id(&self, name: &str) -> Result<String, AuthorizationFailure>;
}

struct ManagedDatabaseResolver {
    service: Arc<dyn DatabaseManager>,
    tenant_id: Arc<str>,
}

#[async_trait]
impl DatabaseResourceResolver for ManagedDatabaseResolver {
    async fn resolve_database_id(&self, name: &str) -> Result<String, AuthorizationFailure> {
        self.service
            .get_database(&self.tenant_id, name)
            .await
            .map(|database| database.id().to_owned())
            .map_err(|_| AuthorizationFailure::Unavailable)
    }
}

struct CatalogDatabaseResolver(CatalogRuntimeRegistry);

#[async_trait]
impl DatabaseResourceResolver for CatalogDatabaseResolver {
    async fn resolve_database_id(&self, name: &str) -> Result<String, AuthorizationFailure> {
        let route = DatabaseId::new(name).map_err(|_| AuthorizationFailure::Unavailable)?;
        self.0
            .authorization_id(&route)
            .map(|database| database.as_str().to_owned())
            .ok_or(AuthorizationFailure::Unavailable)
    }
}

#[async_trait]
impl DataPlaneAuthorizer for DataPlaneAccess {
    /// Returns the audience configured for the remote access service.
    fn audience(&self) -> &str {
        &self.audience
    }

    /// Forwards an opaque bearer and returns only an allowed, audience-bound identity.
    async fn authorize(
        &self,
        authorization: &str,
        question: AuthorizationQuestion,
    ) -> Result<AuthenticatedPrincipal, AuthorizationFailure> {
        let Some(endpoint) = &self.endpoint else {
            return Err(AuthorizationFailure::Unavailable);
        };
        let uri = endpoint
            .join("v1/access/authorize")
            .map_err(|_| AuthorizationFailure::Unavailable)?;
        let response = self
            .http
            .post(uri)
            .header(header::AUTHORIZATION.as_str(), authorization)
            .json(&question)
            .send()
            .await
            .map_err(|_| AuthorizationFailure::Unavailable)?;
        match response.status() {
            StatusCode::UNAUTHORIZED => return Err(AuthorizationFailure::Unauthenticated),
            StatusCode::FORBIDDEN => return Err(AuthorizationFailure::Forbidden),
            status if !status.is_success() => return Err(AuthorizationFailure::Unavailable),
            _ => {}
        }
        let answer = response
            .json::<AuthorizationAnswer>()
            .await
            .map_err(|_| AuthorizationFailure::Unavailable)?;
        if !matches!(
            answer.identity.audience.as_str(),
            audience if audience == self.audience.as_ref() || audience == CLI_AUDIENCE
        ) || answer.identity.tenant_id.is_empty()
            || answer.identity.principal_id.is_empty()
            || answer.identity.token_id.is_empty()
        {
            return Err(AuthorizationFailure::Unauthenticated);
        }
        if !answer.decision.allowed {
            return Err(AuthorizationFailure::Forbidden);
        }
        Ok(answer.identity)
    }
}

/// Mounts mandatory authorization around every recognized data-plane route.
pub fn protect<A>(router: Router, access: A) -> Router
where
    A: DataPlaneAuthorizer + 'static,
{
    protect_with_resolver(router, access, None)
}

/// Protects database routes after resolving names through the durable access inventory.
pub fn protect_managed_databases<A>(
    router: Router,
    access: A,
    service: Arc<dyn DatabaseManager>,
    tenant_id: String,
) -> Router
where
    A: DataPlaneAuthorizer + 'static,
{
    protect_with_resolver(
        router,
        access,
        Some(Arc::new(ManagedDatabaseResolver {
            service,
            tenant_id: Arc::from(tenant_id),
        })),
    )
}

/// Protects cache database routes using the exact immutable IDs bound by its registry.
pub fn protect_catalog_databases<A>(
    router: Router,
    access: A,
    catalogs: CatalogRuntimeRegistry,
) -> Router
where
    A: DataPlaneAuthorizer + 'static,
{
    protect_with_resolver(
        router,
        access,
        Some(Arc::new(CatalogDatabaseResolver(catalogs))),
    )
}

#[derive(Clone)]
struct AuthorizationRuntime {
    access: Arc<dyn DataPlaneAuthorizer>,
    databases: Option<Arc<dyn DatabaseResourceResolver>>,
}

fn protect_with_resolver<A>(
    router: Router,
    access: A,
    databases: Option<Arc<dyn DatabaseResourceResolver>>,
) -> Router
where
    A: DataPlaneAuthorizer + 'static,
{
    router.layer(middleware::from_fn_with_state(
        AuthorizationRuntime {
            access: Arc::new(access),
            databases,
        },
        authorize_request,
    ))
}

/// Authenticates and authorizes one protected request before running its handler.
async fn authorize_request(
    State(runtime): State<AuthorizationRuntime>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(mut target) = route_target(request.method(), request.uri().path()) else {
        return next.run(request).await;
    };
    let mut resolved_database_id = None;
    if let Some(databases) = &runtime.databases
        && let Some(name) = database_route_name(request.uri().path())
        && target.resource_id == format!("database/{name}")
    {
        let database_id = match databases.resolve_database_id(name).await {
            Ok(database_id) => database_id,
            Err(failure) => return failure.into_response(),
        };
        target.resource_id = format!("database/{database_id}");
        resolved_database_id = Some(database_id);
    }
    let authorization = match bearer_header(request.headers()) {
        Ok(value) => value,
        Err(failure) => return failure.into_response(),
    };
    match runtime
        .access
        .authorize(
            authorization,
            AuthorizationQuestion {
                audience: Arc::from(runtime.access.audience()),
                resource_id: target.resource_id,
                action: target.action,
            },
        )
        .await
    {
        Ok(principal) => {
            let Some(authorization) = request.headers().get(header::AUTHORIZATION).cloned() else {
                return AuthorizationFailure::Unauthenticated.into_response();
            };
            request.extensions_mut().insert(principal);
            request
                .extensions_mut()
                .insert(AuthenticatedBearer(authorization));
            if let Some(database_id) = resolved_database_id {
                request
                    .extensions_mut()
                    .insert(AuthenticatedDatabaseId(database_id));
            }
            next.run(request).await
        }
        Err(failure) => failure.into_response(),
    }
}

fn database_route_name(path: &str) -> Option<&str> {
    let mut segments = path.trim_matches('/').split('/');
    match (segments.next(), segments.next(), segments.next()) {
        (Some("v1"), Some("databases"), Some(name)) if !name.is_empty() => Some(name),
        _ => None,
    }
}

/// Returns a strict bearer header without parsing or retaining the token.
fn bearer_header(headers: &HeaderMap) -> Result<&str, AuthorizationFailure> {
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(AuthorizationFailure::Unauthenticated)?;
    value
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty())
        .map(|_| value)
        .ok_or(AuthorizationFailure::Unauthenticated)
}

/// Internal authorization target derived only from the matched public route.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteTarget {
    resource_id: String,
    action: Action,
}

impl RouteTarget {
    /// Creates a stable resource/action pair.
    fn new(resource_id: impl Into<String>, action: Action) -> Self {
        Self {
            resource_id: resource_id.into(),
            action,
        }
    }
}

/// Maps every data-plane family to its registered resource and least-privilege action.
fn route_target(method: &Method, path: &str) -> Option<RouteTarget> {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    match segments.as_slice() {
        ["v1", "databases"] => Some(RouteTarget::new(
            "tenant",
            if method == Method::GET {
                Action::Discover
            } else {
                Action::CreateChild
            },
        )),
        ["v1", "databases", database] => Some(RouteTarget::new(
            format!("database/{database}"),
            if method == Method::GET {
                Action::Describe
            } else {
                Action::Modify
            },
        )),
        ["v1", "queues"] => Some(RouteTarget::new(
            "tenant",
            if method == Method::GET {
                Action::Discover
            } else {
                Action::CreateChild
            },
        )),
        ["v1", "queues", name] => Some(RouteTarget::new(
            format!("queue/{name}"),
            if method == Method::GET {
                Action::Describe
            } else {
                Action::Modify
            },
        )),
        ["v1", "databases", database, "query"] => Some(RouteTarget::new(
            format!("database/{database}"),
            Action::Query,
        )),
        ["v1", "databases", database, "catalog", rest @ ..] if !rest.is_empty() => Some(
            RouteTarget::new(format!("database/{database}"), catalog_action(method, rest)),
        ),
        ["v1", "databases", database, "tables"] => Some(RouteTarget::new(
            format!("database/{database}"),
            if method == Method::GET {
                Action::Discover
            } else {
                Action::CreateChild
            },
        )),
        ["v1", "databases", database, "tables", name] => Some(RouteTarget::new(
            format!("table/{database}/{name}"),
            if method == Method::GET {
                Action::Describe
            } else {
                Action::Modify
            },
        )),
        ["v1", "databases", database, "tables", name, "indexes"] => Some(RouteTarget::new(
            format!("table/{database}/{name}"),
            if method == Method::GET {
                Action::Describe
            } else {
                Action::Modify
            },
        )),
        [
            "v1",
            "databases",
            database,
            "tables",
            name,
            "indexes",
            field,
            operation,
        ] => Some(RouteTarget::new(
            format!("vector/{database}/{name}/{field}"),
            if *operation == "search" {
                Action::Query
            } else {
                Action::Modify
            },
        )),
        ["v1", "databases", database, "tables", name, rest @ ..] if !rest.is_empty() => {
            Some(RouteTarget::new(
                format!("table/{database}/{name}"),
                table_action(method, rest),
            ))
        }
        ["v1", "databases", database, "write", name]
        | ["v1", "databases", database, "ingest", name] => Some(RouteTarget::new(
            format!("table/{database}/{name}"),
            Action::Append,
        )),
        ["v1", "databases", database, "graphs", namespace] => Some(RouteTarget::new(
            format!("graph/{database}/{namespace}"),
            if method == Method::GET {
                Action::Describe
            } else {
                Action::Modify
            },
        )),
        ["v1", "databases", database, "graphs", namespace, "indexes"] => Some(RouteTarget::new(
            format!("graph/{database}/{namespace}"),
            if method == Method::GET {
                Action::Describe
            } else {
                Action::Modify
            },
        )),
        [
            "v1",
            "databases",
            database,
            "graphs",
            namespace,
            "indexes",
            field,
            "search",
        ] => Some(RouteTarget::new(
            format!("vector/{database}/{namespace}/{field}"),
            Action::Query,
        )),
        ["v1", "databases", database, "graphs", namespace, operation] => Some(RouteTarget::new(
            format!("graph/{database}/{namespace}"),
            match *operation {
                "query" => Action::Query,
                "nodes" | "edges" => Action::Append,
                "index" => Action::Modify,
                _ if method == Method::GET => Action::Describe,
                _ => Action::Modify,
            },
        )),
        ["v1", "kv", namespace] | ["v1", "kv", namespace, ..] => Some(RouteTarget::new(
            format!("kv/{namespace}"),
            if method == Method::GET {
                Action::Query
            } else {
                Action::Modify
            },
        )),
        ["v1", "queues", name, operation] => Some(RouteTarget::new(
            format!("queue/{name}"),
            match *operation {
                "poll" => Action::Query,
                "enqueue" => Action::Append,
                _ => Action::Modify,
            },
        )),
        _ => None,
    }
}

/// Maps an Iceberg REST catalog operation to a database-level action.
fn catalog_action(method: &Method, path: &[&str]) -> Action {
    if method == Method::GET {
        return Action::Describe;
    }
    if path
        .last()
        .is_some_and(|segment| matches!(*segment, "namespaces" | "tables"))
    {
        Action::CreateChild
    } else {
        Action::Modify
    }
}

/// Maps an SDK table operation to its narrowest table action.
fn table_action(method: &Method, path: &[&str]) -> Action {
    if method == Method::GET {
        match path.first().copied() {
            Some("rows" | "delta" | "snapshot") => Action::Query,
            _ => Action::Describe,
        }
    } else {
        match path.first().copied() {
            Some("commit" | "ingest") => Action::Append,
            _ => Action::Modify,
        }
    }
}

/// Bounded failure classes exposed by the data-plane boundary.
pub enum AuthorizationFailure {
    /// The credential was absent, malformed, expired, revoked, or otherwise invalid.
    Unauthenticated,
    /// The authenticated principal lacks the requested current-policy grant.
    Forbidden,
    /// The authorization authority could not provide a trustworthy answer.
    Unavailable,
}

impl IntoResponse for AuthorizationFailure {
    /// Renders stable status codes without returning token or policy detail.
    fn into_response(self) -> Response<Body> {
        match self {
            Self::Unauthenticated => (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                "data-plane authentication required",
            )
                .into_response(),
            Self::Forbidden => (StatusCode::FORBIDDEN, "data-plane access denied").into_response(),
            Self::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "authorization service unavailable",
            )
                .into_response(),
        }
    }
}
