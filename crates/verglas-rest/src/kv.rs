//! Authenticated HTTP surface for the tenant-scoped persistent KV engine. The
//! router authorizes a tenant and exact namespace before constructing a storage
//! scope, and it never logs keys, values, tokens, or application metadata.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use verglas_kv::{DeleteOptions, Error, PutOptions, Scope};

const DEFAULT_LIST_LIMIT: usize = 100;
const MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;
const METADATA_PREFIX: &str = "x-verglas-meta-";
const EXPIRES_AT_HEADER: &str = "x-verglas-expires-at-ms";
const TTL_HEADER: &str = "x-verglas-ttl-seconds";
const MODIFIED_AT_HEADER: &str = "x-verglas-modified-at-ms";
const IDEMPOTENCY_HEADER: &str = "idempotency-key";

type ApiResult<T> = Result<T, Box<Response>>;

/// A verb granted on one KV namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvVerb {
    /// Get and list.
    Read,
    /// Put and delete.
    Write,
}

/// Identity already authenticated by the outer data-plane boundary.
#[derive(Debug, Clone)]
pub struct AuthenticatedKvPrincipal {
    /// Tenant that owns every resolved key.
    pub tenant: String,
}

/// One bearer token's tenant, namespace, and verb grant.
#[derive(Debug, Clone)]
pub struct KvGrant {
    /// Tenant that owns the namespace.
    pub tenant: String,
    /// Exact namespace, or `*` for the single-tenant local boundary.
    pub namespace: String,
    /// Whether get and list are allowed.
    pub read: bool,
    /// Whether put and delete are allowed.
    pub write: bool,
}

/// Immutable bearer-token grant map used by the open-source composition.
#[derive(Clone, Default)]
pub struct KvAuthorizer {
    grants: Arc<HashMap<String, KvGrant>>,
}

impl KvAuthorizer {
    /// Builds an authorizer from tokens and their exact scoped grants.
    pub fn new(grants: HashMap<String, KvGrant>) -> Self {
        Self {
            grants: Arc::new(grants),
        }
    }

    /// Resolves one bearer token and checks its exact namespace verb grant.
    fn authorize(&self, headers: &HeaderMap, namespace: &str, verb: KvVerb) -> AuthResult {
        let Some(value) = headers.get(header::AUTHORIZATION) else {
            return AuthResult::Unauthenticated;
        };
        let Ok(value) = value.to_str() else {
            return AuthResult::Unauthenticated;
        };
        let Some(token) = value
            .strip_prefix("Bearer ")
            .filter(|token| !token.is_empty())
        else {
            return AuthResult::Unauthenticated;
        };
        let Some(grant) = self.grants.get(token) else {
            return AuthResult::Unauthenticated;
        };
        let namespace_allowed = grant.namespace == "*" || grant.namespace == namespace;
        let verb_allowed = match verb {
            KvVerb::Read => grant.read,
            KvVerb::Write => grant.write,
        };
        if namespace_allowed && verb_allowed {
            AuthResult::Authorized(grant.tenant.clone())
        } else {
            AuthResult::Forbidden
        }
    }
}

enum AuthResult {
    Authorized(String),
    Unauthenticated,
    Forbidden,
}

/// KV engine plus authorization policy shared by all KV routes.
#[derive(Clone)]
pub struct KvRuntime {
    /// Durable engine handle.
    pub store: verglas_kv::Store,
    /// Bearer-token authorizer used when no outer principal is present.
    pub authorizer: KvAuthorizer,
}

/// Builds the authenticated get, put, delete, and prefix-list router.
pub fn router(runtime: KvRuntime) -> Router {
    Router::new()
        .route("/v1/kv/{namespace}", get(list))
        .route(
            "/v1/kv/{namespace}/{*key}",
            get(get_value).put(put_value).delete(delete_value),
        )
        .layer(axum::extract::DefaultBodyLimit::max(MAX_VALUE_BYTES))
        .with_state(runtime)
}

/// Authorizes before constructing the tenant/namespace storage scope.
fn authorize_scope(
    runtime: &KvRuntime,
    headers: &HeaderMap,
    principal: Option<Extension<AuthenticatedKvPrincipal>>,
    namespace: &str,
    verb: KvVerb,
) -> ApiResult<Scope> {
    let result = match principal {
        Some(Extension(principal)) => AuthResult::Authorized(principal.tenant),
        None => runtime.authorizer.authorize(headers, namespace, verb),
    };
    match result {
        AuthResult::Authorized(tenant) => {
            Scope::new(tenant, namespace).map_err(|error| Box::new(kv_error(error)))
        }
        AuthResult::Unauthenticated => Err(Box::new(
            (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                "KV authentication required",
            )
                .into_response(),
        )),
        AuthResult::Forbidden => Err(Box::new(
            (StatusCode::FORBIDDEN, "KV namespace access denied").into_response(),
        )),
    }
}

/// Returns one raw value with bounded metadata headers.
async fn get_value(
    State(runtime): State<KvRuntime>,
    principal: Option<Extension<AuthenticatedKvPrincipal>>,
    Path((namespace, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let scope = match authorize_scope(&runtime, &headers, principal, &namespace, KvVerb::Read) {
        Ok(scope) => scope,
        Err(response) => return *response,
    };
    match runtime.store.get(&scope, &key) {
        Ok(Some(value)) => value_response(value),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => kv_error(error),
    }
}

/// Durably stores one raw value before returning its committed version.
async fn put_value(
    State(runtime): State<KvRuntime>,
    principal: Option<Extension<AuthenticatedKvPrincipal>>,
    Path((namespace, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let scope = match authorize_scope(&runtime, &headers, principal, &namespace, KvVerb::Write) {
        Ok(scope) => scope,
        Err(response) => return *response,
    };
    let options = match put_options(&headers) {
        Ok(options) => options,
        Err(response) => return *response,
    };
    match runtime.store.put(&scope, &key, body, options) {
        Ok(result) => {
            let mut response = StatusCode::CREATED.into_response();
            if let Ok(value) = HeaderValue::from_str(&result.version) {
                response.headers_mut().insert(header::ETAG, value);
            }
            response.headers_mut().insert(
                HeaderName::from_static("x-verglas-idempotent"),
                HeaderValue::from_static(if result.idempotent { "true" } else { "false" }),
            );
            response
        }
        Err(error) => kv_error(error),
    }
}

/// Durably records an idempotent deletion and returns whether a live value existed.
async fn delete_value(
    State(runtime): State<KvRuntime>,
    principal: Option<Extension<AuthenticatedKvPrincipal>>,
    Path((namespace, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let scope = match authorize_scope(&runtime, &headers, principal, &namespace, KvVerb::Write) {
        Ok(scope) => scope,
        Err(response) => return *response,
    };
    let options = DeleteOptions {
        if_match: header_text(&headers, header::IF_MATCH.as_str()),
    };
    match runtime.store.delete(&scope, &key, options) {
        Ok(result) => Json(json!({"removed": result.removed})).into_response(),
        Err(error) => kv_error(error),
    }
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default)]
    prefix: String,
    limit: Option<usize>,
    cursor: Option<String>,
}

/// Returns one bounded metadata-only page in deterministic bytewise order.
async fn list(
    State(runtime): State<KvRuntime>,
    principal: Option<Extension<AuthenticatedKvPrincipal>>,
    Path(namespace): Path<String>,
    Query(query): Query<ListQuery>,
    headers: HeaderMap,
) -> Response {
    let scope = match authorize_scope(&runtime, &headers, principal, &namespace, KvVerb::Read) {
        Ok(scope) => scope,
        Err(response) => return *response,
    };
    match runtime.store.list(
        &scope,
        &query.prefix,
        query.limit.unwrap_or(DEFAULT_LIST_LIMIT),
        query.cursor.as_deref(),
    ) {
        Ok(page) => Json(page).into_response(),
        Err(error) => kv_error(error),
    }
}

/// Parses conditional, TTL, content, idempotency, and application metadata headers.
fn put_options(headers: &HeaderMap) -> ApiResult<PutOptions> {
    let ttl = parse_u64_header(headers, TTL_HEADER)?;
    let absolute = parse_u64_header(headers, EXPIRES_AT_HEADER)?;
    if ttl.is_some() && absolute.is_some() {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                "set either x-verglas-ttl-seconds or x-verglas-expires-at-ms",
            )
                .into_response(),
        ));
    }
    let expires_at_ms = match ttl {
        Some(seconds) => Some(
            unix_ms()?
                .checked_add(seconds.saturating_mul(1000))
                .ok_or_else(|| {
                    Box::new((StatusCode::BAD_REQUEST, "TTL overflows").into_response())
                })?,
        ),
        None => absolute,
    };
    let create_only = match header_text(headers, header::IF_NONE_MATCH.as_str()) {
        Some(value) if value == "*" => true,
        Some(_) => {
            return Err(Box::new(
                (StatusCode::BAD_REQUEST, "If-None-Match supports only `*`").into_response(),
            ));
        }
        None => false,
    };
    let mut metadata = BTreeMap::new();
    for (name, value) in headers {
        let Some(key) = name.as_str().strip_prefix(METADATA_PREFIX) else {
            continue;
        };
        let value = value.to_str().map_err(|_| {
            Box::new((StatusCode::BAD_REQUEST, "metadata header is not text").into_response())
        })?;
        metadata.insert(key.to_owned(), value.to_owned());
    }
    Ok(PutOptions {
        content_type: header_text(headers, header::CONTENT_TYPE.as_str()),
        expires_at_ms,
        metadata,
        if_match: header_text(headers, header::IF_MATCH.as_str()),
        create_only,
        idempotency_key: header_text(headers, IDEMPOTENCY_HEADER),
    })
}

/// Parses one optional unsigned-integer header.
fn parse_u64_header(headers: &HeaderMap, name: &str) -> ApiResult<Option<u64>> {
    header_text(headers, name)
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                Box::new((StatusCode::BAD_REQUEST, "KV time header is invalid").into_response())
            })
        })
        .transpose()
}

/// Reads one UTF-8 header value, treating invalid bytes as absent.
fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// Returns current Unix milliseconds for relative TTL resolution.
fn unix_ms() -> ApiResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            Box::new((StatusCode::INTERNAL_SERVER_ERROR, "system clock is invalid").into_response())
        })?
        .as_millis()
        .try_into()
        .map_err(|_| {
            Box::new((StatusCode::INTERNAL_SERVER_ERROR, "system clock overflowed").into_response())
        })
}

/// Renders a raw value and its bounded metadata headers.
fn value_response(value: verglas_kv::Value) -> Response {
    let tier = match value.tier {
        verglas_kv::ReadTier::Ram => "ram",
        verglas_kv::ReadTier::Nvme => "nvme",
    };
    let mut response = Response::new(Body::from(value.bytes));
    response.headers_mut().insert(
        HeaderName::from_static("x-verglas-kv-tier"),
        HeaderValue::from_static(tier),
    );
    if let Ok(etag) = HeaderValue::from_str(&value.version) {
        response.headers_mut().insert(header::ETAG, etag);
    }
    if let Some(content_type) = value.content_type
        && let Ok(content_type) = HeaderValue::from_str(&content_type)
    {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    insert_u64_header(
        response.headers_mut(),
        MODIFIED_AT_HEADER,
        value.modified_at_ms,
    );
    if let Some(expires_at_ms) = value.expires_at_ms {
        insert_u64_header(response.headers_mut(), EXPIRES_AT_HEADER, expires_at_ms);
    }
    for (key, value) in value.metadata {
        let name = format!("{METADATA_PREFIX}{key}");
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    response
}

/// Inserts one decimal metadata header.
fn insert_u64_header(headers: &mut HeaderMap, name: &'static str, value: u64) {
    if let Ok(value) = HeaderValue::from_str(&value.to_string()) {
        headers.insert(HeaderName::from_static(name), value);
    }
}

/// Maps engine errors onto stable, honest HTTP statuses.
fn kv_error(error: Error) -> Response {
    let status = match error {
        Error::Invalid(_) => StatusCode::BAD_REQUEST,
        Error::Precondition => StatusCode::PRECONDITION_FAILED,
        Error::Capacity => StatusCode::INSUFFICIENT_STORAGE,
        Error::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string()).into_response()
}
