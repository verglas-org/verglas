//! Semantic REST-JSON dispatch on the cache node's S3 listener.
//!
//! This module owns only wire routing and error rendering. A [`SemanticApi`]
//! implementation owns all meaning and must use Iceberg tables as its source of
//! truth; Puffin is an optional, snapshot-bound acceleration artifact.

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, Method, Request, StatusCode, Uri},
    middleware::Next,
    response::IntoResponse,
    routing::{get, post},
};
use iceberg::{Catalog, NamespaceIdent};
use serde_json::{Map, Value, json};
use verglas_graph::{
    Direction, Edge, Graph, Neighbor, Node, Path, Reached, Subgraph, TraversalFilter,
    TripletReceipt,
};
use verglas_iceberg::tables_api;
use verglas_vector::{MaintenanceConfig, Metric, StringKeyIndex, VamanaParams};

/// The operations in the checked-in AWS S3 Vectors and Verglas Graph contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticOperation {
    /// An AWS S3 Vectors REST-JSON operation.
    S3Vectors(&'static str),
    /// A Verglas Graph REST-JSON operation.
    Graph(&'static str),
}

/// Error returned by a semantic engine while serving a protocol operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticError {
    /// HTTP status returned to the client.
    pub status: StatusCode,
    /// Stable REST-JSON error code.
    pub code: &'static str,
    /// Safe operator/client-facing explanation.
    pub message: String,
}

/// Static credential material used by the semantic SigV4 verifier.
#[derive(Clone)]
pub struct SemanticCredentials {
    access_key_id: Arc<str>,
    secret_access_key: Arc<str>,
}

impl SemanticCredentials {
    /// Constructs verifier credentials from the cache-node S3 keypair.
    pub fn new(access_key_id: String, secret_access_key: String) -> Self {
        Self {
            access_key_id: Arc::from(access_key_id),
            secret_access_key: Arc::from(secret_access_key),
        }
    }
}

/// Maximum accepted client/server timestamp difference for semantic SigV4.
const MAX_SIGV4_CLOCK_SKEW: Duration = Duration::from_secs(15 * 60);

impl SemanticError {
    /// Builds a validation error without exposing internal state.
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "ValidationException",
            message: message.into(),
        }
    }

    /// Builds an unavailable error for a node without a configured Iceberg catalog.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "ServiceUnavailableException",
            message: message.into(),
        }
    }

    /// Builds a missing-resource response without inventing local state.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NotFoundException",
            message: message.into(),
        }
    }
}

/// Durable semantic implementation selected by the cache-node runtime.
///
/// Implementations must not keep bucket, index, vector, or graph membership in
/// process memory. A restart must rehydrate solely from the customer-owned
/// Iceberg snapshot and its Puffin attachments.
#[async_trait]
pub trait SemanticApi: Send + Sync + 'static {
    /// Executes one exact REST-JSON operation and returns its response object.
    async fn call(
        &self,
        operation: SemanticOperation,
        input: Value,
    ) -> Result<Value, SemanticError>;
}

/// Router state shared by all semantic endpoints.
#[derive(Clone)]
struct SemanticState {
    /// The explicitly wired durable semantic implementation.
    api: Arc<dyn SemanticApi>,
}

/// Builds the semantic routes that take precedence over the ordinary S3 fallback.
///
/// Every route is intentionally declared from the two checked-in contracts.
/// This is an extension seam for future wire-format evolution; no negotiation
/// or compatibility path exists in this prototype.
pub fn router(api: Arc<dyn SemanticApi>) -> Router {
    semantic_router(api).merge(documentation_router())
}

/// Builds only semantic operations so authentication can exclude public docs.
fn semantic_router(api: Arc<dyn SemanticApi>) -> Router {
    let state = SemanticState { api };
    Router::new()
        .route("/CreateIndex", post(dispatch_s3))
        .route("/CreateVectorBucket", post(dispatch_s3))
        .route("/DeleteIndex", post(dispatch_s3))
        .route("/DeleteVectorBucket", post(dispatch_s3))
        .route("/DeleteVectorBucketPolicy", post(dispatch_s3))
        .route("/DeleteVectors", post(dispatch_s3))
        .route("/GetIndex", post(dispatch_s3))
        .route("/GetVectorBucket", post(dispatch_s3))
        .route("/GetVectorBucketPolicy", post(dispatch_s3))
        .route("/GetVectors", post(dispatch_s3))
        .route("/ListIndexes", post(dispatch_s3))
        .route("/ListVectorBuckets", post(dispatch_s3))
        .route("/ListVectors", post(dispatch_s3))
        .route("/PutVectorBucketPolicy", post(dispatch_s3))
        .route("/PutVectors", post(dispatch_s3))
        .route("/QueryVectors", post(dispatch_s3))
        .route(
            "/tags/{resourceArn}",
            get(dispatch_s3).post(dispatch_s3).delete(dispatch_s3),
        )
        .route("/CreateGraph", post(dispatch_graph))
        .route("/DeleteGraph", post(dispatch_graph))
        .route("/GetGraph", post(dispatch_graph))
        .route("/ListGraphs", post(dispatch_graph))
        .route("/PutNodes", post(dispatch_graph))
        .route("/PutEdges", post(dispatch_graph))
        .route("/GetNeighbors", post(dispatch_graph))
        .route("/QueryKHop", post(dispatch_graph))
        .route("/QueryNeighborhood", post(dispatch_graph))
        .route("/QueryPaths", post(dispatch_graph))
        .route("/QueryPrecedents", post(dispatch_graph))
        .route("/BuildGraphIndex", post(dispatch_graph))
        .with_state(state)
}

/// Serves the checked-in S3 listener contract without exposing a second API family.
async fn s3_openapi() -> impl IntoResponse {
    (
        [("content-type", "application/json")],
        include_str!("../models/s3-openapi.json"),
    )
}

/// Directs browsers to the listener OpenAPI document for interactive inspection.
async fn s3_swagger() -> impl IntoResponse {
    (
        [("content-type", "text/html; charset=utf-8")],
        "<!doctype html><title>Verglas S3 API</title><p>Open <a href=\"/api-docs/s3/openapi.json\">the S3 listener OpenAPI document</a>.</p>",
    )
}

/// Builds semantic routes protected by header-signed AWS SigV4 requests.
pub fn router_with_sigv4(api: Arc<dyn SemanticApi>, credentials: SemanticCredentials) -> Router {
    documentation_router().merge(
        semantic_router(api).layer(axum::middleware::from_fn_with_state(
            credentials,
            semantic_sigv4,
        )),
    )
}

/// Builds unauthenticated API-documentation routes for the public listener.
fn documentation_router() -> Router {
    Router::new()
        .route("/api-docs/s3/openapi.json", get(s3_openapi))
        .route("/swagger-ui", get(s3_swagger))
}

/// Buffers the bounded JSON request once, verifies it, then restores it for dispatch.
async fn semantic_sigv4(
    State(credentials): State<SemanticCredentials>,
    request: Request<Body>,
    next: Next,
) -> axum::response::Response {
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, 4 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(error) => return semantic_error(SemanticError::validation(error.to_string())),
    };
    if let Err(error) = verify_sigv4(&parts, &bytes, &credentials) {
        return semantic_error(error);
    }
    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

/// Resolves and runs an AWS operation from its exact request URI.
async fn dispatch_s3(
    State(state): State<SemanticState>,
    method: Method,
    uri: Uri,
    body: Option<Json<Value>>,
) -> impl IntoResponse {
    let Some(operation) = s3_operation(uri.path(), &method) else {
        return not_found();
    };
    let mut input = body.map_or(Value::Object(Default::default()), |body| body.0);
    if let Some(resource) = uri.path().strip_prefix("/tags/") {
        let Ok(resource) = percent_encoding::percent_decode_str(resource).decode_utf8() else {
            return not_found();
        };
        input["resourceArn"] = Value::String(resource.into_owned());
    }
    if matches!(operation, SemanticOperation::S3Vectors("UntagResource")) {
        let keys = uri
            .query()
            .unwrap_or_default()
            .split('&')
            .filter_map(|item| {
                item.split_once('=')
                    .filter(|(name, _)| *name == "tagKeys")
                    .map(|(_, value)| {
                        percent_encoding::percent_decode_str(value)
                            .decode_utf8()
                            .ok()
                            .map(|value| Value::String(value.into_owned()))
                    })
            })
            .collect::<Option<Vec<_>>>();
        let Some(keys) = keys else {
            return semantic_error(SemanticError::validation("tagKeys query is invalid"));
        };
        input["tagKeys"] = Value::Array(keys);
    }
    dispatch(state, operation, input).await
}

/// Resolves and runs a graph operation from its exact request URI.
async fn dispatch_graph(
    State(state): State<SemanticState>,
    uri: Uri,
    body: Option<Json<Value>>,
) -> impl IntoResponse {
    let Some(operation) = graph_operation(uri.path()) else {
        return not_found();
    };
    dispatch(
        state,
        operation,
        body.map_or(Value::Object(Default::default()), |body| body.0),
    )
    .await
}

/// Calls the durable adapter and maps its result to REST-JSON.
async fn dispatch(
    state: SemanticState,
    operation: SemanticOperation,
    body: Value,
) -> axum::response::Response {
    match state.api.call(operation, body).await {
        Ok(output) => (StatusCode::OK, Json(output)).into_response(),
        Err(error) => semantic_error(error),
    }
}

/// Returns the protocol's standard unknown-operation response.
fn not_found() -> axum::response::Response {
    semantic_error(SemanticError::not_found("unknown semantic operation"))
}

/// Renders protocol errors with the REST-JSON code field expected by AWS clients.
fn semantic_error(error: SemanticError) -> axum::response::Response {
    let mut response = (error.status, Json(json!({"message": error.message}))).into_response();
    if let Ok(value) = axum::http::HeaderValue::from_str(error.code) {
        response.headers_mut().insert("x-amzn-errortype", value);
    }
    response
}

/// Maps each exact S3 Vectors URI to its operation name.
fn s3_operation(path: &str, method: &Method) -> Option<SemanticOperation> {
    if path.starts_with("/tags/") {
        return match *method {
            Method::GET => Some(SemanticOperation::S3Vectors("ListTagsForResource")),
            Method::POST => Some(SemanticOperation::S3Vectors("TagResource")),
            Method::DELETE => Some(SemanticOperation::S3Vectors("UntagResource")),
            _ => None,
        };
    }
    const OPERATIONS: [&str; 16] = [
        "CreateIndex",
        "CreateVectorBucket",
        "DeleteIndex",
        "DeleteVectorBucket",
        "DeleteVectorBucketPolicy",
        "DeleteVectors",
        "GetIndex",
        "GetVectorBucket",
        "GetVectorBucketPolicy",
        "GetVectors",
        "ListIndexes",
        "ListVectorBuckets",
        "ListVectors",
        "PutVectorBucketPolicy",
        "PutVectors",
        "QueryVectors",
    ];
    let operation = path.strip_prefix('/')?;
    OPERATIONS
        .into_iter()
        .find(|candidate| *candidate == operation)
        .map(SemanticOperation::S3Vectors)
}

/// Maps each exact Graph URI to its operation name.
fn graph_operation(path: &str) -> Option<SemanticOperation> {
    const OPERATIONS: [&str; 12] = [
        "CreateGraph",
        "DeleteGraph",
        "GetGraph",
        "ListGraphs",
        "PutNodes",
        "PutEdges",
        "GetNeighbors",
        "QueryKHop",
        "QueryNeighborhood",
        "QueryPaths",
        "QueryPrecedents",
        "BuildGraphIndex",
    ];
    let operation = path.strip_prefix('/')?;
    OPERATIONS
        .into_iter()
        .find(|candidate| *candidate == operation)
        .map(SemanticOperation::Graph)
}

/// Verifies the complete header-signed AWS4 request for this semantic operation.
fn verify_sigv4(
    parts: &axum::http::request::Parts,
    body: &[u8],
    credentials: &SemanticCredentials,
) -> Result<(), SemanticError> {
    let fields = parse_authorization(header(&parts.headers, "authorization")?)?;
    let (access_key, date, region, service) = parse_credential(fields.credential)?;
    if access_key != credentials.access_key_id.as_ref() {
        return Err(auth_error("InvalidAccessKeyId", "unknown access key"));
    }
    let expected_service = if is_graph_operation(parts.uri.path()) {
        "verglasgraphs"
    } else {
        "s3vectors"
    };
    if service != expected_service {
        return Err(auth_error(
            "SignatureDoesNotMatch",
            "unexpected SigV4 signing service",
        ));
    }
    let amz_date = header(&parts.headers, "x-amz-date")?;
    if !valid_amz_date(amz_date, date) {
        return Err(auth_error(
            "SignatureDoesNotMatch",
            "credential date does not match x-amz-date",
        ));
    }
    if !within_clock_skew(amz_date) {
        return Err(auth_error(
            "RequestTimeTooSkewed",
            "x-amz-date is outside the accepted clock skew",
        ));
    }
    let supplied_hash = header(&parts.headers, "x-amz-content-sha256")?;
    if supplied_hash != sha256_hex(body) {
        return Err(auth_error(
            "SignatureDoesNotMatch",
            "payload hash does not match request body",
        ));
    }
    let headers = canonical_headers(&parts.headers, fields.signed_headers)?;
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        parts.method,
        canonical_uri(parts.uri.path()),
        canonical_query(parts.uri.query().unwrap_or_default()),
        headers,
        fields.signed_headers,
        supplied_hash
    );
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let signed = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let expected = signature(
        credentials.secret_access_key.as_ref(),
        date,
        region,
        service,
        &signed,
    );
    if !constant_time_eq(expected.as_bytes(), fields.signature.as_bytes()) {
        return Err(auth_error("SignatureDoesNotMatch", "signature mismatch"));
    }
    Ok(())
}

/// Parsed fields from one strict AWS4 authorization header.
struct AuthorizationFields<'a> {
    credential: &'a str,
    signed_headers: &'a str,
    signature: &'a str,
}

/// Parses the strict AWS4-HMAC-SHA256 Authorization grammar.
fn parse_authorization(value: &str) -> Result<AuthorizationFields<'_>, SemanticError> {
    let payload = value.strip_prefix("AWS4-HMAC-SHA256 ").ok_or_else(|| {
        auth_error(
            "SignatureDoesNotMatch",
            "unsupported authorization algorithm",
        )
    })?;
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for field in payload.split(',').map(str::trim) {
        let (name, value) = field
            .split_once('=')
            .ok_or_else(|| auth_error("SignatureDoesNotMatch", "malformed authorization header"))?;
        match name {
            "Credential" => credential = Some(value),
            "SignedHeaders" => signed_headers = Some(value),
            "Signature" => signature = Some(value),
            _ => {
                return Err(auth_error(
                    "SignatureDoesNotMatch",
                    "unknown authorization field",
                ));
            }
        }
    }
    Ok(AuthorizationFields {
        credential: credential
            .ok_or_else(|| auth_error("SignatureDoesNotMatch", "missing Credential"))?,
        signed_headers: signed_headers
            .ok_or_else(|| auth_error("SignatureDoesNotMatch", "missing SignedHeaders"))?,
        signature: signature
            .ok_or_else(|| auth_error("SignatureDoesNotMatch", "missing Signature"))?,
    })
}

/// Splits an AWS4 credential scope and rejects extensions.
fn parse_credential(value: &str) -> Result<(&str, &str, &str, &str), SemanticError> {
    let mut parts = value.split('/');
    let access = parts
        .next()
        .ok_or_else(|| auth_error("SignatureDoesNotMatch", "missing access key"))?;
    let date = parts
        .next()
        .ok_or_else(|| auth_error("SignatureDoesNotMatch", "missing signing date"))?;
    let region = parts
        .next()
        .ok_or_else(|| auth_error("SignatureDoesNotMatch", "missing signing region"))?;
    let service = parts
        .next()
        .ok_or_else(|| auth_error("SignatureDoesNotMatch", "missing signing service"))?;
    if parts.next() != Some("aws4_request") || parts.next().is_some() {
        return Err(auth_error(
            "SignatureDoesNotMatch",
            "invalid credential scope",
        ));
    }
    Ok((access, date, region, service))
}

/// Builds canonical header lines in SignedHeaders order and requires core headers.
fn canonical_headers(headers: &HeaderMap, signed: &str) -> Result<String, SemanticError> {
    let mut names = std::collections::BTreeSet::new();
    let mut output = String::new();
    for name in signed.split(';') {
        if name.is_empty() || name != name.to_ascii_lowercase() || !names.insert(name) {
            return Err(auth_error("SignatureDoesNotMatch", "invalid SignedHeaders"));
        }
        let values = headers.get_all(name);
        if values.iter().next().is_none() {
            return Err(auth_error(
                "SignatureDoesNotMatch",
                "signed header missing from request",
            ));
        }
        let text = values
            .iter()
            .map(|value| {
                value
                    .to_str()
                    .map(normalize_header)
                    .map_err(|_| auth_error("SignatureDoesNotMatch", "non-text signed header"))
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(",");
        output.push_str(name);
        output.push(':');
        output.push_str(&text);
        output.push('\n');
    }
    for required in ["host", "x-amz-date", "x-amz-content-sha256"] {
        if !names.contains(required) {
            return Err(auth_error(
                "SignatureDoesNotMatch",
                "required header is not signed",
            ));
        }
    }
    Ok(output)
}

/// Collapses linear whitespace in a canonical header value.
fn normalize_header(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Reads a required textual SigV4 header.
fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, SemanticError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| auth_error("SignatureDoesNotMatch", "required SigV4 header missing"))
}

/// Checks that a timestamp matches its scope date and AWS basic format.
fn valid_amz_date(value: &str, date: &str) -> bool {
    value.len() == 16
        && value.ends_with('Z')
        && value.get(..8) == Some(date)
        && value
            .as_bytes()
            .iter()
            .enumerate()
            .all(|(index, byte)| match index {
                8 => *byte == b'T',
                15 => *byte == b'Z',
                _ => byte.is_ascii_digit(),
            })
}

/// Rejects correctly shaped dates outside the hard clock skew window.
fn within_clock_skew(value: &str) -> bool {
    chrono::NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ").is_ok_and(|time| {
        chrono::Utc::now()
            .signed_duration_since(time.and_utc())
            .num_seconds()
            .unsigned_abs()
            <= MAX_SIGV4_CLOCK_SKEW.as_secs()
    })
}

/// Canonicalizes a URI path by decoding transport escapes then AWS-encoding components.
fn canonical_uri(path: &str) -> String {
    (if path.is_empty() { "/" } else { path })
        .split('/')
        .map(canonical_component)
        .collect::<Vec<_>>()
        .join("/")
}

/// Identifies Graph paths to bind their distinct signing name.
fn is_graph_operation(path: &str) -> bool {
    graph_operation(path).is_some()
}

/// Canonicalizes sorted encoded query key/value pairs for SigV4.
fn canonical_query(query: &str) -> String {
    let mut pairs = query
        .split('&')
        .filter(|piece| !piece.is_empty())
        .map(|piece| match piece.split_once('=') {
            Some((key, value)) => (canonical_component(key), canonical_component(value)),
            None => (canonical_component(piece), String::new()),
        })
        .collect::<Vec<_>>();
    pairs.sort_unstable();
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// AWS-encodes a URI/query component after decoding original escaped octets.
fn canonical_component(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .collect::<Vec<_>>()
        .into_iter()
        .fold(String::new(), |mut output, byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                output.push(char::from(byte));
            } else {
                output.push_str(&format!("%{byte:02X}"));
            }
            output
        })
}

/// Hashes bytes as a lower-case SHA-256 hexadecimal digest.
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(bytes))
}

/// Derives the AWS4 signing key and signs one canonical string.
fn signature(secret: &str, date: &str, region: &str, service: &str, string: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    fn sign(key: &[u8], bytes: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("SHA256 supports HMAC");
        mac.update(bytes);
        mac.finalize().into_bytes().to_vec()
    }
    let date_key = sign(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let region_key = sign(&date_key, region.as_bytes());
    let service_key = sign(&region_key, service.as_bytes());
    let key = sign(&service_key, b"aws4_request");
    hex::encode(sign(&key, string.as_bytes()))
}

/// Compares signature bytes without early exit.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0_u8, |difference, (a, b)| difference | (a ^ b))
            == 0
}

/// Constructs a SigV4 authentication error response.
fn auth_error(code: &'static str, message: impl Into<String>) -> SemanticError {
    SemanticError {
        status: StatusCode::FORBIDDEN,
        code,
        message: message.into(),
    }
}

/// A deliberately fail-closed implementation used until the cache node has a
/// native Iceberg catalog runtime. It stores nothing and cannot fabricate data.
pub struct UnavailableSemanticApi;

#[async_trait]
impl SemanticApi for UnavailableSemanticApi {
    /// Rejects requests rather than constructing an authoritative local registry.
    async fn call(
        &self,
        _operation: SemanticOperation,
        _input: Value,
    ) -> Result<Value, SemanticError> {
        Err(SemanticError::unavailable(
            "semantic APIs require a configured native Iceberg catalog",
        ))
    }
}

/// Iceberg-backed Graph adapter used by the cache-node runtime.
///
/// It has no registry: a graph name is an Iceberg namespace and [`Graph`] is
/// reopened for every operation. Consequently the graph's node/edge tables and
/// snapshot-bound Puffin attachment remain the only durable state.
pub struct IcebergCatalogSemanticStore {
    /// The catalog that owns all graph tables and Puffin statistics files.
    catalog: Arc<dyn Catalog>,
}

const GRAPH_NAMESPACE_PREFIX: &str = "verglasgraph_";
const VECTOR_NAMESPACE_PREFIX: &str = "verglasvector_";
const GRAPH_NAME_PROPERTY: &str = "verglas.graph.name";
const VECTOR_BUCKET_NAME_PROPERTY: &str = "verglas.s3vectors.name";

fn semantic_namespace(prefix: &str, name: &str) -> String {
    format!("{prefix}{}", sha256_hex(name.as_bytes()))
}

impl IcebergCatalogSemanticStore {
    /// Creates an adapter over one already-open Iceberg catalog.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    /// Opens the named graph directly from its Iceberg namespace.
    fn graph(&self, input: &Value) -> Result<Graph, SemanticError> {
        let graph = required_bounded_string(input, "graphName", 1, 255)?;
        Graph::open(
            self.catalog.clone(),
            &semantic_namespace(GRAPH_NAMESPACE_PREFIX, graph),
        )
        .map_err(graph_error)
    }

    /// Ranks `Decision` nodes by BM25 lexical match to `query`, adding a
    /// structural boost when `entities` overlaps a decision's direct
    /// neighbors. Fully deterministic: given the same graph snapshot and
    /// request, the score computation and the sort (score desc, decisionId
    /// asc) always produce the same order and the same floating-point values.
    async fn query_precedents(&self, graph: &Graph, input: &Value) -> Result<Value, SemanticError> {
        let query_text = required_bounded_string(input, "query", 1, 4096)?;
        let limit = optional_precedents_limit(input)?;
        let entities = optional_string_list(input, "entities", 1, 1024)?;

        let decisions: Vec<(String, String)> = graph
            .load_nodes(None)
            .await
            .map_err(graph_error)?
            .into_iter()
            .filter(|node| node.labels.iter().any(|label| label == "Decision"))
            .filter_map(|node| {
                node.properties
                    .get("objective")
                    .and_then(Value::as_str)
                    .map(|objective| (node.id.clone(), objective.to_owned()))
            })
            .collect();

        let documents: Vec<Vec<String>> = decisions
            .iter()
            .map(|(_, objective)| verglas_graph::precedent::tokenize(objective))
            .collect();
        let query_tokens = verglas_graph::precedent::tokenize(query_text);
        let lexical_scores = verglas_graph::precedent::bm25_scores(&documents, &query_tokens);

        let entity_set: std::collections::BTreeSet<&str> =
            entities.iter().map(String::as_str).collect();
        let reader = if entity_set.is_empty() {
            None
        } else {
            Some(graph.reader(None).await.map_err(graph_error)?)
        };

        let mut precedents = decisions
            .into_iter()
            .zip(lexical_scores)
            .map(|((decision_id, objective), lexical_score)| {
                let shared_entities: Vec<String> = reader
                    .as_ref()
                    .map(|reader| {
                        reader
                            .get_neighbors(
                                &decision_id,
                                Direction::Both,
                                &TraversalFilter::default(),
                            )
                            .into_iter()
                            .map(|neighbor| neighbor.node_id)
                            .filter(|node_id| entity_set.contains(node_id.as_str()))
                            .collect::<std::collections::BTreeSet<_>>()
                            .into_iter()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let structural_score = shared_entities.len() as f64;
                let score = lexical_score + structural_score;
                PrecedentRow {
                    decision_id,
                    score,
                    lexical_score,
                    structural_score,
                    shared_entities,
                    objective,
                }
            })
            .collect::<Vec<_>>();

        precedents.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.decision_id.cmp(&b.decision_id))
        });
        if let Some(limit) = limit {
            precedents.truncate(limit);
        }

        Ok(json!({
            "precedents": precedents.into_iter().map(precedent_to_json).collect::<Vec<_>>()
        }))
    }
}

/// One scored `Decision` node, before rendering to the wire response shape.
struct PrecedentRow {
    /// The decision node's id.
    decision_id: String,
    /// The final ranking score: `lexical_score + structural_score`.
    score: f64,
    /// The BM25 score of `query` against the decision's `objective`.
    lexical_score: f64,
    /// The count of `entities` that are direct neighbors of the decision.
    structural_score: f64,
    /// The supplied entity ids that are direct neighbors of the decision,
    /// sorted ascending for a deterministic response.
    shared_entities: Vec<String>,
    /// The decision node's `objective` property.
    objective: String,
}

/// Renders one precedent in the public camel-case response shape.
fn precedent_to_json(row: PrecedentRow) -> Value {
    json!({
        "decisionId": row.decision_id,
        "score": row.score,
        "lexicalScore": row.lexical_score,
        "structuralScore": row.structural_score,
        "sharedEntities": row.shared_entities,
        "objective": row.objective,
    })
}

/// Parses the optional QueryPrecedents result cap (1..=1000; unset means no cap).
fn optional_precedents_limit(input: &Value) -> Result<Option<usize>, SemanticError> {
    let Some(value) = input.get("limit") else {
        return Ok(None);
    };
    value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| (1..=1000).contains(value))
        .map(Some)
        .ok_or_else(|| SemanticError::validation("limit must be an integer from 1 to 1000"))
}

#[async_trait]
impl SemanticApi for IcebergCatalogSemanticStore {
    /// Runs semantic operations against customer-owned Iceberg tables.
    async fn call(
        &self,
        operation: SemanticOperation,
        input: Value,
    ) -> Result<Value, SemanticError> {
        if let SemanticOperation::S3Vectors(operation) = operation {
            return self.vector_call(operation, input).await;
        }
        let SemanticOperation::Graph(operation) = operation else {
            unreachable!("semantic operation is vector or graph")
        };
        if operation == "ListGraphs" {
            let max_results = list_max_results(&input)?;
            let next_token = optional_bounded_string(&input, "nextToken", 1, 2048)?;
            let namespaces = self
                .catalog
                .list_namespaces(None)
                .await
                .map_err(iceberg_error)?;
            let mut names = Vec::new();
            for namespace in namespaces {
                let Some(encoded) = (namespace.len() == 1)
                    .then(|| namespace.first().cloned())
                    .flatten()
                    .filter(|name| name.starts_with(GRAPH_NAMESPACE_PREFIX))
                else {
                    continue;
                };
                let graph = Graph::open(self.catalog.clone(), &encoded).map_err(graph_error)?;
                let table = self
                    .catalog
                    .load_table(graph.nodes_ident())
                    .await
                    .map_err(iceberg_error)?;
                if let Some(name) = table.metadata().properties().get(GRAPH_NAME_PROPERTY) {
                    names.push(name.clone());
                }
            }
            names.sort();
            names.dedup();
            let names = names
                .into_iter()
                .filter(|name| {
                    next_token
                        .as_ref()
                        .is_none_or(|token| name.as_str() > *token)
                })
                .collect::<Vec<_>>();
            let more = names.len() > max_results;
            let graphs = names
                .into_iter()
                .take(max_results)
                .map(|graph_name| json!({"graphName":graph_name}))
                .collect::<Vec<_>>();
            let mut output = Map::from_iter([("graphs".to_owned(), Value::Array(graphs))]);
            if more
                && let Some(last) = output
                    .get("graphs")
                    .and_then(Value::as_array)
                    .and_then(|values| values.last())
                    .and_then(|value| value.get("graphName"))
                    .cloned()
            {
                output.insert("nextToken".to_owned(), last);
            }
            return Ok(Value::Object(output));
        }
        let graph = self.graph(&input)?;
        match operation {
            "CreateGraph" => {
                graph
                    .ensure_tables_with_properties(HashMap::from([(
                        GRAPH_NAME_PROPERTY.to_owned(),
                        required_bounded_string(&input, "graphName", 1, 255)?.to_owned(),
                    )]))
                    .await
                    .map_err(graph_error)?;
                Ok(json!({}))
            }
            "DeleteGraph" => {
                self.catalog
                    .drop_table(graph.nodes_ident())
                    .await
                    .map_err(iceberg_error)?;
                self.catalog
                    .drop_table(graph.edges_ident())
                    .await
                    .map_err(iceberg_error)?;
                self.catalog
                    .drop_namespace(&NamespaceIdent::new(semantic_namespace(
                        GRAPH_NAMESPACE_PREFIX,
                        required_bounded_string(&input, "graphName", 1, 255)?,
                    )))
                    .await
                    .map_err(iceberg_error)?;
                Ok(json!({}))
            }
            "PutNodes" => {
                let rows = input
                    .get("nodes")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        SemanticError::validation("nodes is required and must be an array")
                    })?;
                let nodes = rows
                    .iter()
                    .map(node_from_json)
                    .collect::<Result<Vec<_>, _>>()?;
                let snapshot = graph.insert_nodes(&nodes).await.map_err(graph_error)?;
                Ok(snapshot_output(snapshot))
            }
            "PutEdges" => {
                let rows = input
                    .get("edges")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        SemanticError::validation("edges is required and must be an array")
                    })?;
                let edges = rows
                    .iter()
                    .map(edge_from_json)
                    .collect::<Result<Vec<_>, _>>()?;
                let snapshot = graph.insert_edges(&edges).await.map_err(graph_error)?;
                Ok(snapshot_output(snapshot))
            }
            "BuildGraphIndex" => {
                Ok(json!({"index": graph.build_index(None).await.map_err(graph_error)?.is_some()}))
            }
            "GetNeighbors" => {
                let reader = graph.reader(None).await.map_err(graph_error)?;
                let node = required_bounded_string(&input, "nodeId", 1, 1024)?;
                Ok(
                    json!({"neighbors": reader.get_neighbors(node, direction(&input)?, &filter(&input)?).into_iter().map(neighbor_to_json).collect::<Vec<_>>() }),
                )
            }
            "QueryKHop" => {
                let reader = graph.reader(None).await.map_err(graph_error)?;
                let node = required_bounded_string(&input, "nodeId", 1, 1024)?;
                let hops = required_u32(&input, "k")?;
                Ok(
                    json!({"nodes": reader.k_hop(node, hops, direction(&input)?, &filter(&input)?).into_iter().map(reached_to_json).collect::<Vec<_>>() }),
                )
            }
            "QueryNeighborhood" => {
                let reader = graph.reader(None).await.map_err(graph_error)?;
                let node = required_bounded_string(&input, "nodeId", 1, 1024)?;
                let hops = required_u32(&input, "k")?;
                Ok(
                    json!({"neighborhood": subgraph_to_json(reader.neighborhood(node, hops, direction(&input)?, &filter(&input)?))}),
                )
            }
            "QueryPaths" => {
                let reader = graph.reader(None).await.map_err(graph_error)?;
                Ok(
                    json!({"paths": reader.paths(required_bounded_string(&input, "sourceId", 1, 1024)?, required_bounded_string(&input, "targetId", 1, 1024)?, required_u32(&input, "maxHops")?, direction(&input)?, &filter(&input)?).into_iter().map(path_to_json).collect::<Vec<_>>() }),
                )
            }
            "GetGraph" => Ok(graph_output(
                required_bounded_string(&input, "graphName", 1, 255)?,
                graph.current_edges_snapshot().await.map_err(graph_error)?,
            )),
            "QueryPrecedents" => self.query_precedents(&graph, &input).await,
            _ => Err(SemanticError::validation("unknown graph operation")),
        }
    }
}

impl IcebergCatalogSemanticStore {
    /// Executes the vector operations whose durable state is an Iceberg table.
    async fn vector_call(&self, operation: &str, input: Value) -> Result<Value, SemanticError> {
        validate_vector_input(operation, &input)?;
        let mut input = normalize_vector_arn(input)?;
        input["operation"] = Value::String(operation.to_owned());
        if operation == "CreateVectorBucket" {
            validate_resource_name(
                required_string(&input, "vectorBucketName")?,
                "vectorBucketName",
            )?;
            let encryption = encryption_configuration(input.get("encryptionConfiguration"))?;
            let tags = tags_map(input.get("tags"))?;
            let control = bucket_control_ident(&input)?;
            tables_api::create_table_with_properties(
                self.catalog.as_ref(),
                &control,
                bucket_control_definition(),
                HashMap::from([
                    ("verglas.s3vectors.kind".to_owned(), "bucket".to_owned()),
                    (
                        VECTOR_BUCKET_NAME_PROPERTY.to_owned(),
                        required_string(&input, "vectorBucketName")?.to_owned(),
                    ),
                    (
                        "verglas.s3vectors.created".to_owned(),
                        now_seconds().to_string(),
                    ),
                    (
                        "verglas.s3vectors.encryption".to_owned(),
                        encryption.to_string(),
                    ),
                ]),
            )
            .await
            .map_err(iceberg_error)?;
            self.append_tag_events(&control, &tags, &[]).await?;
            return Ok(json!({"vectorBucketArn": bucket_arn(&input)?}));
        }
        if operation == "DeleteVectorBucket" {
            let namespace = bucket_namespace(&input)?;
            let tables = self
                .catalog
                .list_tables(&namespace)
                .await
                .map_err(iceberg_error)?;
            if tables
                .iter()
                .any(|table| table.name() != BUCKET_CONTROL_TABLE)
            {
                return Err(SemanticError::validation(
                    "vector bucket still contains indexes",
                ));
            }
            self.catalog
                .drop_table(&bucket_control_ident(&input)?)
                .await
                .map_err(iceberg_error)?;
            self.catalog
                .drop_namespace(&namespace)
                .await
                .map_err(iceberg_error)?;
            return Ok(json!({}));
        }
        if operation == "GetVectorBucket" {
            let control = self.bucket_control(&input).await?;
            let creation_time =
                stored_number(control.metadata().properties(), "verglas.s3vectors.created")?;
            let encryption = stored_json(
                control.metadata().properties(),
                "verglas.s3vectors.encryption",
            )?;
            return Ok(
                json!({"vectorBucket": {"vectorBucketName": required_string(&input, "vectorBucketName")?, "vectorBucketArn": bucket_arn(&input)?, "creationTime": creation_time, "encryptionConfiguration": encryption}}),
            );
        }
        if operation == "ListVectorBuckets" {
            let mut buckets = Vec::new();
            for namespace in self
                .catalog
                .list_namespaces(None)
                .await
                .map_err(iceberg_error)?
            {
                if namespace.len() != 1
                    || !namespace
                        .first()
                        .is_some_and(|name| name.starts_with(VECTOR_NAMESPACE_PREFIX))
                {
                    continue;
                }
                let control = match self
                    .catalog
                    .load_table(&bucket_control_ident_from_namespace(&namespace)?)
                    .await
                {
                    Ok(table) => table,
                    Err(_) => continue,
                };
                if control
                    .metadata()
                    .properties()
                    .get("verglas.s3vectors.kind")
                    .map(String::as_str)
                    != Some("bucket")
                {
                    continue;
                }
                let name = control
                    .metadata()
                    .properties()
                    .get(VECTOR_BUCKET_NAME_PROPERTY)
                    .cloned()
                    .ok_or_else(|| {
                        SemanticError::validation("managed bucket name metadata is missing")
                    })?;
                buckets.push(json!({"vectorBucketName": name, "vectorBucketArn": format!("arn:aws:s3vectors:us-east-1:000000000000:bucket/{name}"), "creationTime": stored_number(control.metadata().properties(), "verglas.s3vectors.created")?}));
            }
            if let Some(prefix) = input.get("prefix").and_then(Value::as_str) {
                buckets.retain(|value| {
                    value["vectorBucketName"]
                        .as_str()
                        .is_some_and(|name| name.starts_with(prefix))
                });
            }
            buckets.sort_by_key(|value| {
                value["vectorBucketName"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned()
            });
            let (vector_buckets, next_token) = page(&input, buckets)?;
            let mut output =
                Map::from_iter([("vectorBuckets".to_owned(), Value::Array(vector_buckets))]);
            if let Some(next_token) = next_token {
                output.insert("nextToken".to_owned(), json!(next_token));
            }
            return Ok(Value::Object(output));
        }
        if matches!(
            operation,
            "PutVectorBucketPolicy" | "DeleteVectorBucketPolicy" | "GetVectorBucketPolicy"
        ) {
            if operation == "GetVectorBucketPolicy" {
                return Ok(
                    json!({"policy": self.control_state(&bucket_control_ident(&input)?, "policy").await?.ok_or_else(|| SemanticError::not_found("vector bucket policy does not exist"))?}),
                );
            }
            if operation == "PutVectorBucketPolicy" {
                self.append_control_event(
                    &bucket_control_ident(&input)?,
                    "policy",
                    Value::String(
                        input
                            .get("policy")
                            .and_then(Value::as_str)
                            .ok_or_else(|| SemanticError::validation("policy is required"))?
                            .to_owned(),
                    ),
                    false,
                )
                .await?;
            } else {
                self.append_control_event(
                    &bucket_control_ident(&input)?,
                    "policy",
                    Value::Null,
                    true,
                )
                .await?;
            }
            return Ok(json!({}));
        }
        if operation == "ListIndexes" {
            let namespace = bucket_namespace(&input)?;
            let bucket = required_string(&input, "vectorBucketName")?;
            let mut indexes = Vec::new();
            for ident in self
                .catalog
                .list_tables(&namespace)
                .await
                .map_err(iceberg_error)?
            {
                let name = ident.name().to_owned();
                if name.starts_with("_verglas_vector_") {
                    continue;
                }
                let table = self
                    .catalog
                    .load_table(&ident)
                    .await
                    .map_err(iceberg_error)?;
                let Some(text) = table.metadata().properties().get("verglas.s3vectors.index")
                else {
                    continue;
                };
                let metadata: Value = serde_json::from_str(text)
                    .map_err(|_| SemanticError::validation("index metadata is corrupt"))?;
                indexes.push(json!({"vectorBucketName": bucket, "indexName": name, "indexArn": format!("arn:aws:s3vectors:us-east-1:000000000000:bucket/{bucket}/index/{name}"), "creationTime": metadata["creationTime"]}));
            }
            if let Some(prefix) = input.get("prefix").and_then(Value::as_str) {
                indexes.retain(|value| {
                    value["indexName"]
                        .as_str()
                        .is_some_and(|name| name.starts_with(prefix))
                });
            }
            indexes.sort_by_key(|value| value["indexName"].as_str().unwrap_or_default().to_owned());
            let (indexes, next_token) = page(&input, indexes)?;
            let mut output = Map::from_iter([("indexes".to_owned(), Value::Array(indexes))]);
            if let Some(next_token) = next_token {
                output.insert("nextToken".to_owned(), json!(next_token));
            }
            return Ok(Value::Object(output));
        }
        if matches!(
            operation,
            "TagResource" | "UntagResource" | "ListTagsForResource"
        ) {
            let ident = input
                .get("indexName")
                .is_some()
                .then(|| vector_ident(&input))
                .transpose()?;
            return self.tags_call(operation, &input, ident.as_ref()).await;
        }
        let ident = vector_ident(&input)?;
        match operation {
            "CreateIndex" => {
                validate_resource_name(required_string(&input, "indexName")?, "indexName")?;
                validate_index_definition(&input)?;
                let encryption = match input.get("encryptionConfiguration") {
                    Some(value) => encryption_configuration(Some(value))?,
                    None => self.bucket_encryption(&input).await?,
                };
                let tags = tags_map(input.get("tags"))?;
                let definition = tables_api::CreateTableRequest {
                    schema: vec![
                        tables_api::ColumnSpec::required("key", "string"),
                        tables_api::ColumnSpec::nullable("data", "list<float32>"),
                        tables_api::ColumnSpec::nullable("metadata", "string"),
                        tables_api::ColumnSpec::required("deleted", "boolean"),
                    ],
                    partitions: Vec::new(),
                };
                let metadata = json!({"creationTime": now_seconds(), "dataType": required_string(&input, "dataType")?, "dimension": input.get("dimension").cloned().ok_or_else(|| SemanticError::validation("dimension is required"))?, "distanceMetric": required_string(&input, "distanceMetric")?, "metadataConfiguration": input.get("metadataConfiguration").cloned().unwrap_or(Value::Null), "encryptionConfiguration": encryption}).to_string();
                tables_api::create_table_with_properties(
                    self.catalog.as_ref(),
                    &ident,
                    definition,
                    HashMap::from([("verglas.s3vectors.index".to_owned(), metadata)]),
                )
                .await
                .map_err(iceberg_error)?;
                let tag_control = index_control_ident(&input)?;
                tables_api::create_table_with_properties(
                    self.catalog.as_ref(),
                    &tag_control,
                    bucket_control_definition(),
                    HashMap::from([(
                        "verglas.s3vectors.kind".to_owned(),
                        "index-control".to_owned(),
                    )]),
                )
                .await
                .map_err(iceberg_error)?;
                self.append_tag_events(&tag_control, &tags, &[]).await?;
                Ok(json!({"indexArn": vector_arn(&input)?}))
            }
            "DeleteIndex" => {
                self.catalog
                    .drop_table(&index_control_ident(&input)?)
                    .await
                    .map_err(iceberg_error)?;
                self.catalog
                    .drop_table(&ident)
                    .await
                    .map_err(iceberg_error)?;
                Ok(json!({}))
            }
            "GetIndex" => {
                let table = self
                    .catalog
                    .load_table(&ident)
                    .await
                    .map_err(iceberg_error)?;
                let value = table
                    .metadata()
                    .properties()
                    .get("verglas.s3vectors.index")
                    .ok_or_else(|| SemanticError::validation("index metadata is absent"))?;
                let metadata: Value = serde_json::from_str(value)
                    .map_err(|_| SemanticError::validation("index metadata is corrupt"))?;
                let mut index = Map::from_iter([
                    (
                        "vectorBucketName".to_owned(),
                        json!(required_string(&input, "vectorBucketName")?),
                    ),
                    (
                        "indexName".to_owned(),
                        json!(required_string(&input, "indexName")?),
                    ),
                    ("indexArn".to_owned(), json!(vector_arn(&input)?)),
                    ("creationTime".to_owned(), metadata["creationTime"].clone()),
                    ("dataType".to_owned(), metadata["dataType"].clone()),
                    ("dimension".to_owned(), metadata["dimension"].clone()),
                    (
                        "distanceMetric".to_owned(),
                        metadata["distanceMetric"].clone(),
                    ),
                ]);
                for field in ["metadataConfiguration", "encryptionConfiguration"] {
                    if let Some(value) = metadata.get(field).filter(|value| !value.is_null()) {
                        index.insert(field.to_owned(), value.clone());
                    }
                }
                Ok(json!({"index":index}))
            }
            "PutVectors" | "DeleteVectors" => {
                let values = if operation == "PutVectors" {
                    input.get("vectors")
                } else {
                    input.get("keys")
                }
                .and_then(Value::as_array)
                .ok_or_else(|| SemanticError::validation("vectors or keys is required"))?;
                if operation == "PutVectors" {
                    self.validate_vectors(values, &ident).await?;
                }
                let rows = values
                    .iter()
                    .map(|value| vector_row(value, operation == "DeleteVectors"))
                    .collect::<Result<Vec<_>, _>>()?;
                tables_api::commit(
                    self.catalog.as_ref(),
                    &ident,
                    tables_api::CommitRequest {
                        rows,
                        idempotency_key: None,
                    },
                )
                .await
                .map_err(iceberg_error)?;
                if let Err(error) = self.attach_vector_bridge(&ident, &input).await {
                    tracing::warn!(%error.message, "vector Puffin attachment failed after authoritative Iceberg commit; query will scan");
                }
                Ok(json!({}))
            }
            "GetVectors" => {
                let wanted = input
                    .get("keys")
                    .and_then(Value::as_array)
                    .ok_or_else(|| SemanticError::validation("keys is required"))?
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<std::collections::HashSet<_>>();
                let vectors = live_vectors(self.catalog.as_ref(), &ident)
                    .await?
                    .into_iter()
                    .filter(|vector| wanted.contains(vector.key.as_str()))
                    .map(|vector| vector.output(&input))
                    .collect::<Vec<_>>();
                Ok(json!({"vectors": vectors}))
            }
            "ListVectors" => {
                let mut vectors = live_vectors(self.catalog.as_ref(), &ident)
                    .await?
                    .into_iter()
                    .map(|vector| vector.output(&input))
                    .collect::<Vec<_>>();
                if let (Some(count), Some(index)) = (
                    input.get("segmentCount").and_then(Value::as_u64),
                    input.get("segmentIndex").and_then(Value::as_u64),
                ) {
                    if count == 0 || count > 16 || index >= count || index > 15 {
                        return Err(SemanticError::validation(
                            "segmentCount must be 1..16 and segmentIndex less than it",
                        ));
                    }
                    vectors.retain(|value| {
                        stable_segment(value["key"].as_str().unwrap_or_default(), count) == index
                    });
                } else if input.get("segmentCount").is_some() || input.get("segmentIndex").is_some()
                {
                    return Err(SemanticError::validation(
                        "segmentCount and segmentIndex must be provided together",
                    ));
                }
                let (vectors, next_token) = page(&input, vectors)?;
                let mut output = Map::from_iter([("vectors".to_owned(), Value::Array(vectors))]);
                if let Some(next_token) = next_token {
                    output.insert("nextToken".to_owned(), json!(next_token));
                }
                Ok(Value::Object(output))
            }
            "QueryVectors" => {
                let query = input
                    .get("queryVector")
                    .ok_or_else(|| SemanticError::validation("queryVector is required"))
                    .and_then(vector_data)?;
                self.validate_query(&query, &ident, &input).await?;
                let table = self
                    .catalog
                    .load_table(&ident)
                    .await
                    .map_err(iceberg_error)?;
                let snapshot = table.metadata().current_snapshot_id();
                let definition = self.index_definition(&ident).await?;
                if let Some(filter) = input.get("filter") {
                    validate_filter(filter)?;
                    validate_filterable(filter, &non_filterable_keys(&definition)?)?;
                }
                let metric = match definition["distanceMetric"].as_str() {
                    Some("cosine") => Metric::Cosine,
                    Some("euclidean") => Metric::L2,
                    _ => return Err(SemanticError::validation("index distanceMetric is invalid")),
                };
                if input.get("filter").is_none()
                    && let Some(snapshot) = snapshot
                    && let Some(attached) =
                        verglas_vector::attachment::load_string_key_index_for_snapshot(
                            &table, snapshot, "data",
                        )
                        .await
                        .map_err(|error| SemanticError::unavailable(error.to_string()))?
                {
                    let metadata = if input
                        .get("returnMetadata")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                    {
                        live_vectors(self.catalog.as_ref(), &ident)
                            .await?
                            .into_iter()
                            .map(|vector| (vector.key, vector.metadata))
                            .collect::<std::collections::HashMap<_, _>>()
                    } else {
                        std::collections::HashMap::new()
                    };
                    let ranked = attached
                        .bridge
                        .query(&query, required_u32(&input, "topK")? as usize, 64)
                        .map_err(|error| SemanticError::unavailable(error.to_string()))?
                        .into_iter()
                        .map(|neighbor| {
                            let metadata = metadata.get(&neighbor.key).cloned().flatten();
                            (neighbor.distance, neighbor.key, metadata)
                        })
                        .collect::<Vec<_>>();
                    return query_response(&input, &definition, ranked);
                }
                let mut vectors = live_vectors(self.catalog.as_ref(), &ident).await?;
                if let Some(filter) = input.get("filter") {
                    let mut filtered = Vec::new();
                    for vector in vectors {
                        if matches_filter(vector.metadata.as_ref(), filter)? {
                            filtered.push(vector);
                        }
                    }
                    vectors = filtered;
                }
                vectors.retain(|vector| vector.data.len() == query.len());
                vectors.sort_by(|left, right| {
                    metric
                        .report_distance(&metric.prepare(&left.data), &metric.prepare(&query))
                        .total_cmp(
                            &metric.report_distance(
                                &metric.prepare(&right.data),
                                &metric.prepare(&query),
                            ),
                        )
                });
                let ranked = vectors
                    .into_iter()
                    .take(required_u32(&input, "topK")? as usize)
                    .map(|vector| {
                        let distance = metric.report_distance(
                            &metric.prepare(&vector.data),
                            &metric.prepare(&query),
                        );
                        (distance, vector.key, vector.metadata)
                    })
                    .collect::<Vec<_>>();
                query_response(&input, &definition, ranked)
            }
            _ => Err(SemanticError::validation("unknown S3 Vectors operation")),
        }
    }

    /// Enforces index dimensionality and metric domain before durable append.
    async fn validate_vectors(
        &self,
        values: &[Value],
        ident: &iceberg::TableIdent,
    ) -> Result<(), SemanticError> {
        let metadata = self.index_definition(ident).await?;
        let dim = dimension(&metadata)?;
        let cosine = metadata["distanceMetric"].as_str() == Some("cosine");
        for value in values {
            let vector = vector_data(
                value
                    .get("data")
                    .ok_or_else(|| SemanticError::validation("vector data is required"))?,
            )?;
            if vector.len() != dim || cosine && !vector.iter().any(|value| *value != 0.0) {
                return Err(SemanticError::validation(
                    "vector does not satisfy index dimension or cosine domain",
                ));
            }
        }
        Ok(())
    }

    /// Enforces query dimensionality and top-K bounds from the persisted definition.
    async fn validate_query(
        &self,
        query: &[f32],
        ident: &iceberg::TableIdent,
        input: &Value,
    ) -> Result<(), SemanticError> {
        let metadata = self.index_definition(ident).await?;
        if query.len() != dimension(&metadata)? {
            return Err(SemanticError::validation(
                "queryVector dimension does not match index",
            ));
        }
        if metadata["distanceMetric"].as_str() == Some("cosine")
            && !query.iter().any(|value| *value != 0.0)
        {
            return Err(SemanticError::validation(
                "cosine queryVector must not be zero",
            ));
        }
        if required_u32(input, "topK")? == 0 {
            return Err(SemanticError::validation("topK must be positive"));
        }
        Ok(())
    }

    /// Loads the authoritative per-index definition from the table metadata.
    async fn index_definition(&self, ident: &iceberg::TableIdent) -> Result<Value, SemanticError> {
        let table = self
            .catalog
            .load_table(ident)
            .await
            .map_err(iceberg_error)?;
        table
            .metadata()
            .properties()
            .get("verglas.s3vectors.index")
            .ok_or_else(|| SemanticError::validation("index metadata is absent"))
            .and_then(|text| {
                serde_json::from_str(text)
                    .map_err(|_| SemanticError::validation("index metadata is corrupt"))
            })
    }

    /// Loads the effective bucket encryption configuration from its namespace.
    async fn bucket_encryption(&self, input: &Value) -> Result<Value, SemanticError> {
        let control = self.bucket_control(input).await?;
        stored_json(
            control.metadata().properties(),
            "verglas.s3vectors.encryption",
        )
    }

    /// Rebuilds and attaches a lossless Vamana bridge to the exact committed snapshot.
    async fn attach_vector_bridge(
        &self,
        ident: &iceberg::TableIdent,
        input: &Value,
    ) -> Result<(), SemanticError> {
        let table = self
            .catalog
            .load_table(ident)
            .await
            .map_err(iceberg_error)?;
        let Some(snapshot) = table.metadata().current_snapshot_id() else {
            return Ok(());
        };
        let metadata = table
            .metadata()
            .properties()
            .get("verglas.s3vectors.index")
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
            .ok_or_else(|| SemanticError::validation("index metadata is absent"))?;
        let dim = metadata
            .get("dimension")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| SemanticError::validation("index dimension is invalid"))?;
        let metric = match metadata.get("distanceMetric").and_then(Value::as_str) {
            Some("cosine") => Metric::Cosine,
            Some("euclidean") => Metric::L2,
            _ => return Err(SemanticError::validation("index distanceMetric is invalid")),
        };
        let mut bridge = StringKeyIndex::new(metric, dim, VamanaParams::default(), 0)
            .map_err(|error| SemanticError::unavailable(error.to_string()))?;
        for vector in live_vectors(self.catalog.as_ref(), ident).await? {
            if vector.data.len() == dim {
                bridge
                    .put(&vector.key, &vector.data)
                    .map_err(|error| SemanticError::unavailable(error.to_string()))?;
            }
        }
        bridge.index_mut().set_reflected_snapshot(snapshot);
        let config = MaintenanceConfig::new("key", "data", metric);
        verglas_vector::attachment::attach_string_key_index(
            self.catalog.as_ref(),
            &table,
            snapshot,
            &config,
            bridge.index(),
            bridge.keys(),
        )
        .await
        .map_err(|error| SemanticError::unavailable(error.to_string()))?;
        let _ = input;
        Ok(())
    }

    /// Persists tags on the bucket namespace or the index table selected by ARN.
    async fn tags_call(
        &self,
        operation: &str,
        input: &Value,
        ident: Option<&iceberg::TableIdent>,
    ) -> Result<Value, SemanticError> {
        let _resource = required_string(input, "resourceArn")?;
        if ident.is_some() {
            let control = index_control_ident(input)?;
            let mut tags = self.control_tags(&control).await?;
            let before = tags.clone();
            mutate_tags(operation, input, &mut tags)?;
            if operation != "ListTagsForResource" {
                let removed = before
                    .keys()
                    .filter(|key| !tags.contains_key(*key))
                    .cloned()
                    .collect::<Vec<_>>();
                self.append_tag_events(&control, &tags, &removed).await?;
            }
            return Ok(json!({"tags": tags}));
        }
        let control = bucket_control_ident(input)?;
        let mut tags = self.control_tags(&control).await?;
        let before = tags.clone();
        mutate_tags(operation, input, &mut tags)?;
        if operation != "ListTagsForResource" {
            let removed = before
                .keys()
                .filter(|key| !tags.contains_key(*key))
                .cloned()
                .collect::<Vec<_>>();
            self.append_tag_events(&control, &tags, &removed).await?;
        }
        Ok(json!({"tags": tags}))
    }

    /// Loads the bucket control table that is authoritative for bucket metadata.
    async fn bucket_control(&self, input: &Value) -> Result<iceberg::table::Table, SemanticError> {
        self.catalog
            .load_table(&bucket_control_ident(input)?)
            .await
            .map_err(iceberg_error)
    }

    /// Appends one control event, preserving every concurrent mutation in Iceberg order.
    async fn append_control_event(
        &self,
        ident: &iceberg::TableIdent,
        key: &str,
        value: Value,
        deleted: bool,
    ) -> Result<(), SemanticError> {
        tables_api::commit(
            self.catalog.as_ref(),
            ident,
            tables_api::CommitRequest {
                rows: vec![json!({"key":key,"value":value.to_string(),"deleted":deleted})],
                idempotency_key: None,
            },
        )
        .await
        .map_err(iceberg_error)?;
        Ok(())
    }

    /// Resolves one control key from its append-only snapshot lineage.
    async fn control_state(
        &self,
        ident: &iceberg::TableIdent,
        wanted: &str,
    ) -> Result<Option<Value>, SemanticError> {
        let rows = tables_api::rows_in_commit_order(self.catalog.as_ref(), ident)
            .await
            .map_err(iceberg_error)?;
        let mut result = None;
        for row in rows {
            if row.get("key").and_then(Value::as_str) == Some(wanted) {
                result = if row.get("deleted").and_then(Value::as_bool).unwrap_or(false) {
                    None
                } else {
                    row.get("value")
                        .and_then(Value::as_str)
                        .and_then(|text| serde_json::from_str(text).ok())
                };
            }
        }
        Ok(result)
    }

    /// Resolves every independently appended tag mutation from a control table.
    async fn control_tags(
        &self,
        ident: &iceberg::TableIdent,
    ) -> Result<serde_json::Map<String, Value>, SemanticError> {
        let rows = tables_api::rows_in_commit_order(self.catalog.as_ref(), ident)
            .await
            .map_err(iceberg_error)?;
        let mut tags = serde_json::Map::new();
        for row in rows {
            let Some(key) = row
                .get("key")
                .and_then(Value::as_str)
                .and_then(|key| key.strip_prefix("tag:"))
            else {
                continue;
            };
            if row.get("deleted").and_then(Value::as_bool).unwrap_or(false) {
                tags.remove(key);
            } else if let Some(value) = row
                .get("value")
                .and_then(Value::as_str)
                .and_then(|value| serde_json::from_str(value).ok())
            {
                tags.insert(key.to_owned(), value);
            }
        }
        Ok(tags)
    }

    /// Appends independent key-level tag events so concurrent writers compose.
    async fn append_tag_events(
        &self,
        ident: &iceberg::TableIdent,
        tags: &serde_json::Map<String, Value>,
        removed: &[String],
    ) -> Result<(), SemanticError> {
        let mut rows = tags.iter().map(|(key, value)| json!({"key":format!("tag:{key}"),"value":value.to_string(),"deleted":false})).collect::<Vec<_>>();
        rows.extend(
            removed
                .iter()
                .map(|key| json!({"key":format!("tag:{key}"),"value":"null","deleted":true})),
        );
        if rows.is_empty() {
            return Ok(());
        }
        tables_api::commit(
            self.catalog.as_ref(),
            ident,
            tables_api::CommitRequest {
                rows,
                idempotency_key: None,
            },
        )
        .await
        .map_err(iceberg_error)?;
        Ok(())
    }
}

/// Normalizes modeled ARN selectors to the name fields the Iceberg adapter owns.
fn normalize_vector_arn(mut input: Value) -> Result<Value, SemanticError> {
    for field in ["vectorBucketArn", "indexArn", "resourceArn"] {
        let Some(arn) = input.get(field).and_then(Value::as_str).map(str::to_owned) else {
            continue;
        };
        if let Some((bucket, index)) = arn_parts(&arn)? {
            input["vectorBucketName"] = Value::String(bucket);
            if let Some(index) = index {
                input["indexName"] = Value::String(index);
            }
        }
    }
    Ok(input)
}

/// Enforces the model's name-or-ARN selector and scalar-member invariants.
fn validate_vector_input(operation: &str, input: &Value) -> Result<(), SemanticError> {
    let object = input
        .as_object()
        .ok_or_else(|| SemanticError::validation("request must be a JSON object"))?;
    let allowed: &[&str] = match operation {
        "CreateVectorBucket" => &["vectorBucketName", "encryptionConfiguration", "tags"],
        "ListVectorBuckets" => &["maxResults", "nextToken", "prefix"],
        "CreateIndex" => &[
            "vectorBucketName",
            "vectorBucketArn",
            "indexName",
            "dataType",
            "dimension",
            "distanceMetric",
            "metadataConfiguration",
            "encryptionConfiguration",
            "tags",
        ],
        "ListIndexes" => &[
            "vectorBucketName",
            "vectorBucketArn",
            "maxResults",
            "nextToken",
            "prefix",
        ],
        "TagResource" => &["resourceArn", "tags"],
        "UntagResource" => &["resourceArn", "tagKeys"],
        "ListTagsForResource" => &["resourceArn"],
        "DeleteVectorBucket"
        | "GetVectorBucket"
        | "PutVectorBucketPolicy"
        | "GetVectorBucketPolicy"
        | "DeleteVectorBucketPolicy" => &["vectorBucketName", "vectorBucketArn", "policy"],
        "DeleteIndex" | "GetIndex" | "DeleteVectors" | "GetVectors" | "PutVectors"
        | "ListVectors" | "QueryVectors" => &[
            "vectorBucketName",
            "indexName",
            "indexArn",
            "keys",
            "vectors",
            "returnData",
            "returnMetadata",
            "maxResults",
            "nextToken",
            "segmentCount",
            "segmentIndex",
            "topK",
            "queryVector",
            "filter",
            "returnDistance",
        ],
        _ => return Err(SemanticError::validation("unknown S3 Vectors operation")),
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(SemanticError::validation(
            "request contains an unknown member",
        ));
    }
    match operation {
        "CreateVectorBucket" => required_name(input, "vectorBucketName")?,
        "ListVectorBuckets" => {}
        "CreateIndex" | "ListIndexes" => bucket_selector(input)?,
        "TagResource" | "UntagResource" | "ListTagsForResource" => {
            required_string(input, "resourceArn")?;
        }
        "DeleteVectorBucket"
        | "GetVectorBucket"
        | "PutVectorBucketPolicy"
        | "GetVectorBucketPolicy"
        | "DeleteVectorBucketPolicy" => bucket_selector(input)?,
        _ => index_selector(input)?,
    }
    for field in ["returnData", "returnMetadata", "returnDistance"] {
        if let Some(value) = input.get(field)
            && !value.is_boolean()
        {
            return Err(SemanticError::validation(format!(
                "{field} must be boolean"
            )));
        }
    }
    if let Some(prefix) = input.get("prefix") {
        bounded_string(prefix, "prefix", 0, 63)?;
    }
    if let Some(token) = input.get("nextToken") {
        bounded_string(token, "nextToken", 1, 4096)?;
    }
    if let Some(policy) = input.get("policy")
        && !policy.is_string()
    {
        return Err(SemanticError::validation("policy must be a string"));
    }
    Ok(())
}

/// Requires exactly one bucket selector, not a conflicting name and ARN pair.
fn bucket_selector(input: &Value) -> Result<(), SemanticError> {
    match (input.get("vectorBucketName"), input.get("vectorBucketArn")) {
        (Some(name), None) => bounded_string(name, "vectorBucketName", 3, 63),
        (None, Some(arn)) => {
            required_arn(arn, "vectorBucketArn")?;
            Ok(())
        }
        _ => Err(SemanticError::validation(
            "provide exactly one of vectorBucketName or vectorBucketArn",
        )),
    }
}

/// Requires exactly one index selector: an ARN or the complete name pair.
fn index_selector(input: &Value) -> Result<(), SemanticError> {
    match (
        input.get("indexArn"),
        input.get("vectorBucketName"),
        input.get("indexName"),
    ) {
        (Some(arn), None, None) => {
            required_arn(arn, "indexArn")?;
            Ok(())
        }
        (None, Some(bucket), Some(index)) => {
            bounded_string(bucket, "vectorBucketName", 3, 63)?;
            bounded_string(index, "indexName", 3, 63)
        }
        _ => Err(SemanticError::validation(
            "provide indexArn or both vectorBucketName and indexName",
        )),
    }
}

/// Validates a required resource name using its modeled string bounds.
fn required_name(input: &Value, field: &str) -> Result<(), SemanticError> {
    bounded_string(
        input
            .get(field)
            .ok_or_else(|| SemanticError::validation(format!("{field} is required")))?,
        field,
        3,
        63,
    )
}

/// Validates a string member against inclusive model bounds.
fn bounded_string(value: &Value, field: &str, min: usize, max: usize) -> Result<(), SemanticError> {
    let value = value
        .as_str()
        .ok_or_else(|| SemanticError::validation(format!("{field} must be a string")))?;
    if !(min..=max).contains(&value.len()) {
        return Err(SemanticError::validation(format!(
            "{field} length must be {min}..{max}"
        )));
    }
    Ok(())
}

/// Validates an ARN-shaped selector before the ARN parser maps it to table names.
fn required_arn(value: &Value, field: &str) -> Result<(), SemanticError> {
    let arn = value
        .as_str()
        .ok_or_else(|| SemanticError::validation(format!("{field} must be a string")))?;
    if arn.is_empty() || arn.len() > 2048 {
        return Err(SemanticError::validation(format!(
            "{field} length is invalid"
        )));
    }
    Ok(())
}

/// Parses local S3 Vector bucket and index ARNs without guessing other ARN forms.
fn arn_parts(arn: &str) -> Result<Option<(String, Option<String>)>, SemanticError> {
    let mut parts = arn.splitn(6, ':');
    if parts.next() != Some("arn")
        || parts.next() != Some("aws")
        || parts.next() != Some("s3vectors")
    {
        return Ok(None);
    };
    let _region = parts
        .next()
        .ok_or_else(|| SemanticError::validation("invalid S3 Vectors ARN"))?;
    let account = parts
        .next()
        .ok_or_else(|| SemanticError::validation("invalid S3 Vectors ARN"))?;
    let resource = parts
        .next()
        .ok_or_else(|| SemanticError::validation("invalid S3 Vectors ARN"))?;
    if account.len() != 12 || !account.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SemanticError::validation("invalid S3 Vectors ARN account"));
    }
    let Some(resource) = resource.strip_prefix("bucket/") else {
        return Err(SemanticError::validation("invalid S3 Vectors ARN resource"));
    };
    let (bucket, index) = match resource.split_once("/index/") {
        Some((bucket, index)) => (bucket, Some(index)),
        None => (resource, None),
    };
    if bucket.is_empty() || index.is_some_and(str::is_empty) {
        return Err(SemanticError::validation("invalid S3 Vectors ARN"));
    }
    Ok(Some((bucket.to_owned(), index.map(str::to_owned))))
}

/// Applies one tag mutation to an authoritative JSON property map.
fn mutate_tags(
    operation: &str,
    input: &Value,
    tags: &mut serde_json::Map<String, Value>,
) -> Result<(), SemanticError> {
    if operation == "TagResource" {
        let requested = tags_map(input.get("tags"))?;
        for (name, value) in requested {
            tags.insert(name.clone(), value.clone());
        }
    }
    if operation == "UntagResource" {
        for name in input
            .get("tagKeys")
            .and_then(Value::as_array)
            .ok_or_else(|| SemanticError::validation("tagKeys is required"))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| SemanticError::validation("tagKeys must contain strings"))
            })
            .collect::<Result<Vec<_>, _>>()?
        {
            tags.remove(name);
        }
    }
    Ok(())
}

/// Resolves a bucket name to the customer-owned Iceberg namespace.
fn bucket_namespace(input: &Value) -> Result<NamespaceIdent, SemanticError> {
    Ok(NamespaceIdent::new(semantic_namespace(
        VECTOR_NAMESPACE_PREFIX,
        required_string(input, "vectorBucketName")?,
    )))
}

/// Reserved table name used only for durable vector bucket control-plane state.
const BUCKET_CONTROL_TABLE: &str = "verglas_vector_bucket";

/// Prefix reserved for auxiliary append-only resource control tables.
const INDEX_CONTROL_PREFIX: &str = "verglas_vector_index_";

/// Resolves the Iceberg table that stores bucket metadata, tags, and policy.
fn bucket_control_ident(input: &Value) -> Result<iceberg::TableIdent, SemanticError> {
    Ok(iceberg::TableIdent::new(
        bucket_namespace(input)?,
        BUCKET_CONTROL_TABLE.to_owned(),
    ))
}

/// Resolves the sibling control table that carries mutable index tags.
fn index_control_ident(input: &Value) -> Result<iceberg::TableIdent, SemanticError> {
    Ok(iceberg::TableIdent::new(
        bucket_namespace(input)?,
        format!(
            "{INDEX_CONTROL_PREFIX}{}",
            required_string(input, "indexName")?
        ),
    ))
}

/// Resolves a control table from a catalog namespace returned by list_namespaces.
fn bucket_control_ident_from_namespace(
    namespace: &NamespaceIdent,
) -> Result<iceberg::TableIdent, SemanticError> {
    if namespace.is_empty() {
        return Err(SemanticError::validation(
            "managed bucket namespace is empty",
        ));
    }
    Ok(iceberg::TableIdent::new(
        namespace.clone(),
        BUCKET_CONTROL_TABLE.to_owned(),
    ))
}

/// Defines the empty durable control table schema kept apart from customer vectors.
fn bucket_control_definition() -> tables_api::CreateTableRequest {
    tables_api::CreateTableRequest {
        schema: vec![
            tables_api::ColumnSpec::required("key", "string"),
            tables_api::ColumnSpec::required("value", "string"),
            tables_api::ColumnSpec::required("deleted", "boolean"),
        ],
        partitions: Vec::new(),
    }
}

/// Builds the stable ARN for a bucket.
fn bucket_arn(input: &Value) -> Result<String, SemanticError> {
    Ok(format!(
        "arn:aws:s3vectors:us-east-1:000000000000:bucket/{}",
        required_string(input, "vectorBucketName")?
    ))
}

/// Returns a durable creation timestamp recorded with each catalog resource.
fn now_seconds() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

/// Reads a JSON property that is required for a durable resource response.
fn stored_json(properties: &HashMap<String, String>, key: &str) -> Result<Value, SemanticError> {
    properties
        .get(key)
        .ok_or_else(|| SemanticError::validation(format!("resource metadata {key} is absent")))
        .and_then(|text| {
            serde_json::from_str(text).map_err(|_| {
                SemanticError::validation(format!("resource metadata {key} is corrupt"))
            })
        })
}

/// Reads a numeric timestamp property without fabricating a missing value.
fn stored_number(properties: &HashMap<String, String>, key: &str) -> Result<i64, SemanticError> {
    properties
        .get(key)
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| SemanticError::validation(format!("resource metadata {key} is absent")))
}

/// Validates an AWS bucket or index identifier before it becomes a catalog name.
fn validate_resource_name(value: &str, field: &str) -> Result<(), SemanticError> {
    if !(3..=63).contains(&value.len()) {
        return Err(SemanticError::validation(format!(
            "{field} must be 3..63 characters"
        )));
    }
    Ok(())
}

/// Validates the modeled index definition before the durable table is created.
fn validate_index_definition(input: &Value) -> Result<(), SemanticError> {
    if required_string(input, "dataType")? != "float32" {
        return Err(SemanticError::validation("dataType must be float32"));
    }
    let dimension = required_u32(input, "dimension")?;
    if dimension == 0 || dimension > 4096 {
        return Err(SemanticError::validation("dimension must be 1..4096"));
    }
    match required_string(input, "distanceMetric")? {
        "euclidean" | "cosine" => {}
        _ => {
            return Err(SemanticError::validation(
                "distanceMetric must be euclidean or cosine",
            ));
        }
    }
    if let Some(configuration) = input.get("metadataConfiguration") {
        let keys = configuration
            .get("nonFilterableMetadataKeys")
            .and_then(Value::as_array)
            .ok_or_else(|| SemanticError::validation("nonFilterableMetadataKeys is required"))?;
        if keys.is_empty() || keys.len() > 10 {
            return Err(SemanticError::validation(
                "nonFilterableMetadataKeys must contain 1..10 keys",
            ));
        }
        for key in keys {
            let key = key.as_str().ok_or_else(|| {
                SemanticError::validation("nonFilterableMetadataKeys must be strings")
            })?;
            if key.is_empty() || key.len() > 63 {
                return Err(SemanticError::validation(
                    "metadata key must be 1..63 characters",
                ));
            }
        }
    }
    Ok(())
}

/// Validates and materializes effective resource encryption configuration.
fn encryption_configuration(value: Option<&Value>) -> Result<Value, SemanticError> {
    let value = value
        .cloned()
        .unwrap_or_else(|| json!({"sseType":"AES256"}));
    let object = value
        .as_object()
        .ok_or_else(|| SemanticError::validation("encryptionConfiguration must be an object"))?;
    let sse = object
        .get("sseType")
        .and_then(Value::as_str)
        .ok_or_else(|| SemanticError::validation("encryptionConfiguration.sseType is required"))?;
    let has_kms = object.get("kmsKeyArn").is_some();
    match (sse, has_kms) {
        ("AES256", false) | ("aws:kms", true) => Ok(value),
        ("AES256", true) => Err(SemanticError::validation("kmsKeyArn requires aws:kms")),
        ("aws:kms", false) => Err(SemanticError::validation("aws:kms requires kmsKeyArn")),
        _ => Err(SemanticError::validation(
            "encryptionConfiguration.sseType is invalid",
        )),
    }
}

/// Validates a modeled tag map and returns its durable object representation.
fn tags_map(value: Option<&Value>) -> Result<serde_json::Map<String, Value>, SemanticError> {
    let tags = value
        .cloned()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let object = tags
        .as_object()
        .ok_or_else(|| SemanticError::validation("tags must be an object"))?;
    for (key, value) in object {
        if key.is_empty() || key.len() > 128 || value.as_str().is_none_or(|tag| tag.len() > 256) {
            return Err(SemanticError::validation(
                "tags contain an invalid key or value",
            ));
        }
    }
    Ok(object.clone())
}

/// Applies the AWS cursor/max-results contract to an already stably sorted list.
fn page(input: &Value, values: Vec<Value>) -> Result<(Vec<Value>, Option<String>), SemanticError> {
    let binding = format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        input
            .get("operation")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        input
            .get("vectorBucketName")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        input
            .get("indexName")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        input
            .get("prefix")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        input
            .get("segmentCount")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        input
            .get("segmentIndex")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        input
            .get("returnData")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        input
            .get("returnMetadata")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    );
    let last = input
        .get("nextToken")
        .and_then(Value::as_str)
        .map(|token| decode_cursor(token, &binding))
        .transpose()?;
    let offset = last
        .as_deref()
        .map(|last| {
            values
                .iter()
                .position(|value| listing_key(input, value) > last)
                .unwrap_or(values.len())
        })
        .unwrap_or(0);
    let limit = input
        .get("maxResults")
        .and_then(Value::as_u64)
        .map(|value| {
            usize::try_from(value).map_err(|_| SemanticError::validation("maxResults is too large"))
        })
        .transpose()?
        .unwrap_or(500);
    if limit == 0 {
        return Err(SemanticError::validation("maxResults must be positive"));
    }
    let maximum = if input.get("operation").and_then(Value::as_str) == Some("ListVectors") {
        1000
    } else {
        500
    };
    if limit > maximum {
        return Err(SemanticError::validation(format!(
            "maxResults must not exceed {maximum}"
        )));
    }
    let end = offset.saturating_add(limit).min(values.len());
    let next =
        (end < values.len()).then(|| encode_cursor(&binding, listing_key(input, &values[end - 1])));
    Ok((values[offset..end].to_vec(), next))
}

/// Returns a stable key for an already sorted list response.
fn listing_key<'a>(input: &Value, value: &'a Value) -> &'a str {
    let field = match input.get("operation").and_then(Value::as_str) {
        Some("ListVectorBuckets") => "vectorBucketName",
        Some("ListIndexes") => "indexName",
        _ => "key",
    };
    value.get(field).and_then(Value::as_str).unwrap_or_default()
}

/// Encodes a resource- and filter-bound last-key cursor.
fn encode_cursor(binding: &str, last: &str) -> String {
    base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        format!("{binding}\n{last}"),
    )
}

/// Rejects cursors reused with a different resource or filter.
fn decode_cursor(token: &str, binding: &str) -> Result<String, SemanticError> {
    let text = String::from_utf8(
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, token)
            .map_err(|_| SemanticError::validation("nextToken is invalid"))?,
    )
    .map_err(|_| SemanticError::validation("nextToken is invalid"))?;
    let mut parts = text.splitn(2, '\n');
    if parts.next() != Some(binding) {
        return Err(SemanticError::validation(
            "nextToken does not match this listing",
        ));
    }
    parts
        .next()
        .map(str::to_owned)
        .ok_or_else(|| SemanticError::validation("nextToken is invalid"))
}

/// Assigns a key to a deterministic list segment without process-random hashing.
fn stable_segment(key: &str, count: u64) -> u64 {
    key.bytes().fold(0_u64, |hash, byte| {
        hash.wrapping_mul(1099511628211)
            .wrapping_add(u64::from(byte))
    }) % count
}

/// Resolves a vector bucket/index name to its customer-owned Iceberg table.
fn vector_ident(input: &Value) -> Result<iceberg::TableIdent, SemanticError> {
    Ok(iceberg::TableIdent::new(
        bucket_namespace(input)?,
        required_string(input, "indexName")?.to_owned(),
    ))
}

/// Builds the stable local ARN returned for a newly created vector index.
fn vector_arn(input: &Value) -> Result<String, SemanticError> {
    Ok(format!(
        "arn:aws:s3vectors:us-east-1:000000000000:bucket/{}/index/{}",
        required_string(input, "vectorBucketName")?,
        required_string(input, "indexName")?
    ))
}

/// Converts an S3 Vector write/delete item into an append-only Iceberg row.
fn vector_row(value: &Value, deleted: bool) -> Result<Value, SemanticError> {
    let key = if deleted {
        value
            .as_str()
            .ok_or_else(|| SemanticError::validation("key must be a string"))?
    } else {
        required_string(value, "key")?
    };
    let data = if deleted {
        Value::Null
    } else {
        value
            .get("data")
            .ok_or_else(|| SemanticError::validation("vector data is required"))
            .and_then(vector_data)
            .map(|values| json!(values))?
    };
    Ok(
        json!({"key": key, "data": data, "metadata": if deleted { None } else { value.get("metadata").map(Value::to_string) }, "deleted": deleted}),
    )
}

/// Parses the exact AWS VectorData union; only float32 is supported by this index.
fn vector_data(value: &Value) -> Result<Vec<f32>, SemanticError> {
    if value.as_object().is_none_or(|object| object.len() != 1) {
        return Err(SemanticError::validation(
            "VectorData must contain exactly one float32 variant",
        ));
    }
    let values = value
        .get("float32")
        .and_then(Value::as_array)
        .ok_or_else(|| SemanticError::validation("VectorData must be an object with float32"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_f64()
                .and_then(|number| {
                    let cast = number as f32;
                    cast.is_finite().then_some(cast)
                })
                .ok_or_else(|| {
                    SemanticError::validation("VectorData.float32 must contain finite numbers")
                })
        })
        .collect()
}

/// Extracts the required fixed vector dimension from an index definition.
fn dimension(definition: &Value) -> Result<usize, SemanticError> {
    definition["dimension"]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| SemanticError::validation("index dimension is invalid"))
}

/// Renders one QueryVectors result while honoring optional result fields.
fn query_output(key: String, distance: f32, metadata: Option<Value>, input: &Value) -> Value {
    let mut value = json!({"key": key});
    if input
        .get("returnDistance")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        value["distance"] = json!(distance);
    }
    if input
        .get("returnMetadata")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        value["metadata"] = metadata.unwrap_or(Value::Null);
    }
    value
}

/// Pages a deterministically ranked query without exposing the ranking cursor.
fn query_response(
    input: &Value,
    definition: &Value,
    mut ranked: Vec<(f32, String, Option<Value>)>,
) -> Result<Value, SemanticError> {
    ranked.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let binding = format!(
        "QueryVectors|{}|{}|{}|{}|{}|{}|{}",
        input
            .get("vectorBucketName")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        input
            .get("indexName")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        input.get("queryVector").cloned().unwrap_or(Value::Null),
        input.get("filter").cloned().unwrap_or(Value::Null),
        required_u32(input, "topK")?,
        input
            .get("returnDistance")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        input
            .get("returnMetadata")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    );
    let last = input
        .get("nextToken")
        .and_then(Value::as_str)
        .map(|token| decode_cursor(token, &binding))
        .transpose()?;
    if let Some(last) = last {
        let (distance, key) = decode_rank(&last)?;
        ranked.retain(|candidate| {
            candidate.0.total_cmp(&distance).is_gt()
                || (candidate.0.total_cmp(&distance).is_eq() && candidate.1.as_str() > key)
        });
    }
    let page_size = 100_usize;
    let has_more = ranked.len() > page_size;
    let page = ranked.into_iter().take(page_size).collect::<Vec<_>>();
    let next_token = if has_more {
        let (distance, key, _) = page
            .last()
            .ok_or_else(|| SemanticError::validation("query page is empty"))?;
        Some(encode_cursor(&binding, &encode_rank(*distance, key)))
    } else {
        None
    };
    let mut output = Map::from_iter([
        (
            "distanceMetric".to_owned(),
            definition["distanceMetric"].clone(),
        ),
        (
            "vectors".to_owned(),
            Value::Array(
                page.into_iter()
                    .map(|(distance, key, metadata)| query_output(key, distance, metadata, input))
                    .collect::<Vec<_>>(),
            ),
        ),
    ]);
    if let Some(next_token) = next_token {
        output.insert("nextToken".to_owned(), json!(next_token));
    }
    Ok(Value::Object(output))
}

/// Encodes the unrendered rank and key that identify a query continuation.
fn encode_rank(distance: f32, key: &str) -> String {
    format!("{:08x}\n{key}", distance.to_bits())
}

/// Decodes a query rank continuation without accepting malformed cursors.
fn decode_rank(value: &str) -> Result<(f32, &str), SemanticError> {
    let (bits, key) = value
        .split_once('\n')
        .ok_or_else(|| SemanticError::validation("nextToken is invalid"))?;
    let bits = u32::from_str_radix(bits, 16)
        .map_err(|_| SemanticError::validation("nextToken is invalid"))?;
    Ok((f32::from_bits(bits), key))
}

/// Extracts the configured metadata keys that cannot appear in a filter.
fn non_filterable_keys(definition: &Value) -> Result<Vec<String>, SemanticError> {
    let Some(configuration) = definition.get("metadataConfiguration") else {
        return Ok(Vec::new());
    };
    if configuration.is_null() {
        return Ok(Vec::new());
    }
    configuration
        .get("nonFilterableMetadataKeys")
        .and_then(Value::as_array)
        .ok_or_else(|| SemanticError::validation("index metadataConfiguration is corrupt"))?
        .iter()
        .map(|key| {
            key.as_str()
                .map(str::to_owned)
                .ok_or_else(|| SemanticError::validation("index metadataConfiguration is corrupt"))
        })
        .collect()
}

/// Rejects a query that references a key the index explicitly keeps non-filterable.
fn validate_filterable(filter: &Value, prohibited: &[String]) -> Result<(), SemanticError> {
    let object = filter
        .as_object()
        .ok_or_else(|| SemanticError::validation("filter must be an object"))?;
    for (field, predicate) in object {
        if field == "$and" || field == "$or" {
            for nested in predicate
                .as_array()
                .ok_or_else(|| SemanticError::validation("logical filter must be an array"))?
            {
                validate_filterable(nested, prohibited)?;
            }
        } else if !field.starts_with('$') && prohibited.iter().any(|key| key == field) {
            return Err(SemanticError::validation(format!(
                "metadata key {field} is not filterable"
            )));
        }
    }
    Ok(())
}

/// Validates the complete documented metadata filter grammar before scanning rows.
fn validate_filter(filter: &Value) -> Result<(), SemanticError> {
    let object = filter
        .as_object()
        .ok_or_else(|| SemanticError::validation("filter must be an object"))?;
    for (field, predicate) in object {
        if field == "$and" || field == "$or" {
            let values = predicate
                .as_array()
                .filter(|values| !values.is_empty())
                .ok_or_else(|| {
                    SemanticError::validation("logical filter must be a nonempty array")
                })?;
            for nested in values {
                validate_filter(nested)?;
            }
            continue;
        }
        if field.starts_with('$') {
            return Err(SemanticError::validation(
                "unknown logical metadata filter operator",
            ));
        }
        validate_field_predicate(predicate)?;
    }
    Ok(())
}

/// Validates one field predicate without relying on the presence of a matching row.
fn validate_field_predicate(predicate: &Value) -> Result<(), SemanticError> {
    let Some(operators) = predicate
        .as_object()
        .filter(|map| map.keys().any(|key| key.starts_with('$')))
    else {
        return primitive(predicate, "metadata equality value");
    };
    if operators.keys().any(|key| !key.starts_with('$')) {
        return Err(SemanticError::validation(
            "metadata predicate mixes operators and fields",
        ));
    }
    for (operator, expected) in operators {
        match operator.as_str() {
            "$eq" | "$ne" => primitive(expected, "metadata comparison value")?,
            "$gt" | "$gte" | "$lt" | "$lte" if expected.is_number() => {}
            "$gt" | "$gte" | "$lt" | "$lte" => {
                return Err(SemanticError::validation(
                    "comparison value must be numeric",
                ));
            }
            "$exists" if expected.is_boolean() => {}
            "$exists" => return Err(SemanticError::validation("$exists must be boolean")),
            "$in" | "$nin" => {
                let values = expected
                    .as_array()
                    .filter(|values| !values.is_empty())
                    .ok_or_else(|| SemanticError::validation("$in/$nin must be nonempty arrays"))?;
                for value in values {
                    primitive(value, "$in/$nin values")?;
                }
            }
            _ => {
                return Err(SemanticError::validation(
                    "unknown metadata filter operator",
                ));
            }
        }
    }
    Ok(())
}

/// Ensures a filter comparison value is one of the metadata primitive values.
fn primitive(value: &Value, message: &str) -> Result<(), SemanticError> {
    if value.is_string() || value.is_number() || value.is_boolean() || value.is_null() {
        Ok(())
    } else {
        Err(SemanticError::validation(format!(
            "{message} must be primitive"
        )))
    }
}

/// Evaluates the documented S3 Vectors metadata predicate subset against one row.
fn matches_filter(metadata: Option<&Value>, filter: &Value) -> Result<bool, SemanticError> {
    let metadata = metadata
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let object = filter
        .as_object()
        .ok_or_else(|| SemanticError::validation("filter must be an object"))?;
    for (field, predicate) in object {
        if field == "$and" || field == "$or" {
            let list = predicate
                .as_array()
                .filter(|list| !list.is_empty())
                .ok_or_else(|| {
                    SemanticError::validation("logical filter must be a nonempty array")
                })?;
            let matches = list
                .iter()
                .map(|item| matches_filter(Some(&Value::Object(metadata.clone())), item))
                .collect::<Result<Vec<_>, _>>()?;
            if (field == "$and" && !matches.iter().all(|value| *value))
                || (field == "$or" && !matches.iter().any(|value| *value))
            {
                return Ok(false);
            }
            continue;
        }
        if field.starts_with('$') {
            return Err(SemanticError::validation(
                "unknown logical metadata filter operator",
            ));
        }
        let value = metadata.get(field);
        if !matches_field(value, predicate)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Evaluates all operators on one metadata field as an AND expression.
fn matches_field(value: Option<&Value>, predicate: &Value) -> Result<bool, SemanticError> {
    let operators = predicate
        .as_object()
        .filter(|object| object.keys().any(|key| key.starts_with('$')));
    if let Some(operators) = operators {
        for (operator, expected) in operators {
            let result = match operator.as_str() {
                "$eq" => value.is_some_and(|actual| equal_metadata(actual, expected)),
                "$ne" => !value.is_some_and(|actual| equal_metadata(actual, expected)),
                "$exists" => {
                    value.is_some()
                        == expected
                            .as_bool()
                            .ok_or_else(|| SemanticError::validation("$exists must be boolean"))?
                }
                "$gt" | "$gte" | "$lt" | "$lte" => {
                    let Some(actual) = value.and_then(Value::as_f64) else {
                        return Ok(false);
                    };
                    let expected = expected.as_f64().ok_or_else(|| {
                        SemanticError::validation("comparison value must be numeric")
                    })?;
                    match operator.as_str() {
                        "$gt" => actual > expected,
                        "$gte" => actual >= expected,
                        "$lt" => actual < expected,
                        _ => actual <= expected,
                    }
                }
                "$in" | "$nin" => {
                    let choices = expected
                        .as_array()
                        .filter(|choices| !choices.is_empty())
                        .ok_or_else(|| {
                            SemanticError::validation("$in/$nin must be nonempty arrays")
                        })?;
                    let found = value.is_some_and(|actual| {
                        choices.iter().any(|choice| equal_metadata(actual, choice))
                    });
                    if operator == "$in" { found } else { !found }
                }
                _ => {
                    return Err(SemanticError::validation(
                        "unknown metadata filter operator",
                    ));
                }
            };
            if !result {
                return Ok(false);
            }
        }
        Ok(true)
    } else {
        Ok(value.is_some_and(|actual| equal_metadata(actual, predicate)))
    }
}

/// Compares metadata scalars and treats arrays as membership sets for equality.
fn equal_metadata(actual: &Value, expected: &Value) -> bool {
    actual == expected
        || actual
            .as_array()
            .is_some_and(|values| values.contains(expected))
}

/// A source-table vector with its exact customer-provided key preserved.
struct LiveVector {
    key: String,
    data: Vec<f32>,
    metadata: Option<Value>,
}

impl LiveVector {
    /// Renders Get/List output without exposing the implementation's scan state.
    fn output(&self, input: &Value) -> Value {
        let mut output = json!({"key": self.key});
        if input
            .get("returnData")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            output["data"] = json!({"float32": self.data});
        }
        if input
            .get("returnMetadata")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            output["metadata"] = self.metadata.clone().unwrap_or(Value::Null);
        }
        output
    }
}

/// Scans the current source snapshot; this is the correct, slower turn-off path.
async fn live_vectors(
    catalog: &dyn Catalog,
    ident: &iceberg::TableIdent,
) -> Result<Vec<LiveVector>, SemanticError> {
    let rows = tables_api::rows_in_commit_order(catalog, ident)
        .await
        .map_err(iceberg_error)?;
    let mut current = std::collections::BTreeMap::new();
    for row in rows {
        let Some(key) = row.get("key").and_then(Value::as_str) else {
            continue;
        };
        if row.get("deleted").and_then(Value::as_bool).unwrap_or(false) {
            current.remove(key);
            continue;
        }
        let data = row
            .get("data")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_f64)
                    .map(|number| number as f32)
                    .collect()
            })
            .unwrap_or_default();
        let metadata = row
            .get("metadata")
            .and_then(Value::as_str)
            .and_then(|text| serde_json::from_str(text).ok());
        current.insert(
            key.to_owned(),
            LiveVector {
                key: key.to_owned(),
                data,
                metadata,
            },
        );
    }
    Ok(current.into_values().collect())
}

/// Converts an Iceberg error to a safe service failure.
fn iceberg_error(error: impl std::fmt::Display) -> SemanticError {
    let message = error.to_string();
    let (status, code) =
        if message.starts_with("TableNotFound") || message.starts_with("NamespaceNotFound") {
            (StatusCode::NOT_FOUND, "NotFoundException")
        } else if message.starts_with("TableAlreadyExists")
            || message.starts_with("NamespaceAlreadyExists")
            || message.starts_with("CatalogCommitConflicts")
            || message.starts_with("PreconditionFailed")
        {
            (StatusCode::CONFLICT, "ConflictException")
        } else if message.starts_with("DataInvalid") {
            (StatusCode::BAD_REQUEST, "ValidationException")
        } else if message.starts_with("Unexpected") {
            (StatusCode::INTERNAL_SERVER_ERROR, "InternalServerException")
        } else {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailableException",
            )
        };
    SemanticError {
        status,
        code,
        message,
    }
}

/// Converts a REST-JSON node object to the graph engine's durable row model.
fn node_from_json(value: &Value) -> Result<Node, SemanticError> {
    let mut node = Node::new(required_bounded_string(value, "id", 1, 1024)?);
    node.labels = optional_string_list(value, "labels", 1, 255)?;
    node.properties = optional_properties(value)?;
    Ok(node)
}

/// Converts a REST-JSON edge object to the graph engine's append-only model.
fn edge_from_json(value: &Value) -> Result<Edge, SemanticError> {
    let mut edge = Edge::new(
        required_bounded_string(value, "sourceId", 1, 1024)?,
        required_bounded_string(value, "predicate", 1, 1024)?,
        required_bounded_string(value, "targetId", 1, 1024)?,
        required_bounded_string(value, "provenance", 1, 1024)?,
    );
    if let Some(id) = optional_bounded_string(value, "edgeId", 1, 255)? {
        edge.edge_id = id.to_owned();
    }
    if let Some(confidence) = optional_confidence(value, "confidence")? {
        edge.confidence = confidence;
    }
    edge.properties = optional_properties(value)?;
    Ok(edge)
}

/// Builds a mutation output without serializing an absent snapshot as null.
fn snapshot_output(snapshot: Option<i64>) -> Value {
    let mut output = Map::new();
    if let Some(snapshot) = snapshot {
        output.insert("snapshotId".to_owned(), json!(snapshot));
    }
    Value::Object(output)
}

/// Builds a graph response without serializing an absent edge snapshot as null.
fn graph_output(graph_name: &str, snapshot: Option<i64>) -> Value {
    let mut output = Map::from_iter([("graphName".to_owned(), json!(graph_name))]);
    if let Some(snapshot) = snapshot {
        output.insert("edgesSnapshotId".to_owned(), json!(snapshot));
    }
    Value::Object(output)
}

/// Renders engine direction in the Graph REST-JSON enum spelling.
fn direction_to_json(direction: Direction) -> &'static str {
    match direction {
        Direction::Out => "out",
        Direction::In => "in",
        Direction::Both => "both",
    }
}

/// Renders a graph neighbor in the public camel-case response shape.
fn neighbor_to_json(neighbor: Neighbor) -> Value {
    json!({"nodeId":neighbor.node_id,"predicate":neighbor.predicate,"confidence":neighbor.confidence,"edgeId":neighbor.edge_id,"provenance":neighbor.provenance,"direction":direction_to_json(neighbor.direction)})
}

/// Renders a reached node in the public camel-case response shape.
fn reached_to_json(reached: Reached) -> Value {
    json!({"nodeId":reached.node_id,"hops":reached.hops,"pathConfidence":reached.path_confidence})
}

/// Renders one traversed edge receipt in the public camel-case response shape.
fn triplet_receipt_to_json(receipt: TripletReceipt) -> Value {
    json!({"sourceId":receipt.src_id,"predicate":receipt.predicate,"targetId":receipt.dst_id,"confidence":receipt.confidence,"edgeId":receipt.edge_id,"provenance":receipt.provenance})
}

/// Renders a traversal subgraph in the public REST-JSON response shape.
fn subgraph_to_json(subgraph: Subgraph) -> Value {
    json!({"nodes":subgraph.nodes.into_iter().map(reached_to_json).collect::<Vec<_>>(),"edges":subgraph.edges.into_iter().map(triplet_receipt_to_json).collect::<Vec<_>>()})
}

/// Renders a graph path in the public REST-JSON response shape.
fn path_to_json(path: Path) -> Value {
    json!({"nodes":path.nodes,"edges":path.edges.into_iter().map(triplet_receipt_to_json).collect::<Vec<_>>(),"confidence":path.confidence})
}

/// Validates a required bounded string supplied by the Graph protocol.
fn required_bounded_string<'a>(
    value: &'a Value,
    field: &str,
    minimum: usize,
    maximum: usize,
) -> Result<&'a str, SemanticError> {
    let string = value.get(field).and_then(Value::as_str).ok_or_else(|| {
        SemanticError::validation(format!("{field} is required and must be a string"))
    })?;
    validate_string(string, field, minimum, maximum)?;
    Ok(string)
}

/// Validates an optional bounded string supplied by the Graph protocol.
fn optional_bounded_string<'a>(
    value: &'a Value,
    field: &str,
    minimum: usize,
    maximum: usize,
) -> Result<Option<&'a str>, SemanticError> {
    let Some(member) = value.get(field) else {
        return Ok(None);
    };
    let string = member
        .as_str()
        .ok_or_else(|| SemanticError::validation(format!("{field} must be a string")))?;
    validate_string(string, field, minimum, maximum)?;
    Ok(Some(string))
}

/// Checks one Graph protocol string's inclusive bounds.
fn validate_string(
    string: &str,
    field: &str,
    minimum: usize,
    maximum: usize,
) -> Result<(), SemanticError> {
    if !(minimum..=maximum).contains(&string.len()) {
        return Err(SemanticError::validation(format!(
            "{field} length must be between {minimum} and {maximum}"
        )));
    }
    Ok(())
}

/// Parses an optional bounded label list from a Graph node.
fn optional_string_list(
    value: &Value,
    field: &str,
    minimum: usize,
    maximum: usize,
) -> Result<Vec<String>, SemanticError> {
    let Some(member) = value.get(field) else {
        return Ok(Vec::new());
    };
    member
        .as_array()
        .ok_or_else(|| SemanticError::validation(format!("{field} must be an array")))?
        .iter()
        .map(|item| {
            let label = item.as_str().ok_or_else(|| {
                SemanticError::validation(format!("{field} members must be strings"))
            })?;
            validate_string(label, field, minimum, maximum)?;
            Ok(label.to_owned())
        })
        .collect()
}

/// Parses optional Graph properties without silently discarding an invalid object.
fn optional_properties(value: &Value) -> Result<Map<String, Value>, SemanticError> {
    let Some(member) = value.get("properties") else {
        return Ok(Map::new());
    };
    let properties = member
        .as_object()
        .ok_or_else(|| SemanticError::validation("properties must be an object"))?;
    for key in properties.keys() {
        validate_string(key, "properties key", 1, 255)?;
    }
    Ok(properties.clone())
}

/// Parses an optional closed-unit-interval Graph confidence.
fn optional_confidence(value: &Value, field: &str) -> Result<Option<f64>, SemanticError> {
    let Some(value) = value.get(field) else {
        return Ok(None);
    };
    let confidence = value
        .as_f64()
        .filter(|number| number.is_finite() && (0.0..=1.0).contains(number))
        .ok_or_else(|| SemanticError::validation(format!("{field} must be between 0 and 1")))?;
    Ok(Some(confidence))
}

/// Reads one required string field from an operation input.
fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, SemanticError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| SemanticError::validation(format!("{field} is required")))
}

/// Reads one required non-negative hop count from an operation input.
fn required_u32(value: &Value, field: &str) -> Result<u32, SemanticError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|number| u32::try_from(number).ok())
        .ok_or_else(|| SemanticError::validation(format!("{field} is required")))
}

/// Parses the optional traversal direction, defaulting to outgoing edges.
fn direction(input: &Value) -> Result<Direction, SemanticError> {
    match input
        .get("direction")
        .and_then(Value::as_str)
        .unwrap_or("out")
    {
        "out" => Ok(Direction::Out),
        "in" => Ok(Direction::In),
        "both" => Ok(Direction::Both),
        _ => Err(SemanticError::validation(
            "direction must be out, in, or both",
        )),
    }
}

/// Parses Graph traversal filters and rejects out-of-contract predicate values.
fn filter(input: &Value) -> Result<TraversalFilter, SemanticError> {
    let Some(value) = input.get("filter") else {
        return Ok(TraversalFilter::default());
    };
    let object = value
        .as_object()
        .ok_or_else(|| SemanticError::validation("filter must be an object"))?;
    let predicate = match object.get("predicate") {
        Some(value) => {
            Some(required_bounded_string(&json!({"value":value}), "value", 1, 1024)?.to_owned())
        }
        None => None,
    };
    let min_confidence = match object.get("minConfidence") {
        Some(value) => {
            let number = value
                .as_f64()
                .filter(|number| number.is_finite() && (0.0..=1.0).contains(number))
                .ok_or_else(|| SemanticError::validation("filter.minConfidence must be 0..1"))?;
            Some(number)
        }
        None => None,
    };
    Ok(TraversalFilter {
        predicate,
        min_confidence,
    })
}

/// Parses the bounded ListGraphs page size.
fn list_max_results(input: &Value) -> Result<usize, SemanticError> {
    let Some(value) = input.get("maxResults") else {
        return Ok(1000);
    };
    value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| (1..=1000).contains(value))
        .ok_or_else(|| SemanticError::validation("maxResults must be an integer from 1 to 1000"))
}

/// Converts graph-engine errors to a safe REST-JSON service failure.
fn graph_error(error: verglas_graph::GraphError) -> SemanticError {
    SemanticError::unavailable(error.to_string())
}

// ---------------------------------------------------------------------------
// Semantic mutation events (R7-A)
// ---------------------------------------------------------------------------
//
// Trust model: publishing is a best-effort side channel to the control-plane
// queue ingress, not part of the durability contract. The bearer token and
// tenant id come from the cache node's own configuration (never from the
// caller's request), so every event this node publishes is attributed to the
// tenant this node is configured for. By the time an event is queued the
// mutation is already durably committed to Iceberg; a dropped or delayed
// event only delays a downstream subscriber's notice of that commit, it
// never puts data at risk. Publish failures (unreachable queue, non-2xx
// response, timeout) are logged at `warn` and otherwise ignored: they must
// never delay or fail the client's operation.

/// Configuration for the fire-and-forget mutation event publisher.
#[derive(Clone, Debug)]
pub struct EventPublishConfig {
    /// Control-plane queue ingress URL that accepts one JSON event per POST.
    pub queue_url: String,
    /// Bearer token presented on the `authorization` header.
    pub token: String,
    /// Tenant identifier attached to every published event.
    pub tenant_id: String,
}

impl EventPublishConfig {
    /// Reads the full trio from the environment. Any variable that is absent
    /// or empty disables publishing, so a partially configured node fails
    /// closed instead of publishing with a blank credential or tenant.
    pub fn from_env() -> Option<Self> {
        Some(Self {
            queue_url: non_empty_env("VERGLAS_EVENT_QUEUE_URL")?,
            token: non_empty_env("VERGLAS_CACHE_EVENT_TOKEN")?,
            tenant_id: non_empty_env("VERGLAS_TENANT_ID")?,
        })
    }
}

/// Reads an environment variable, treating unset or empty as absent.
fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// Bound on one publish attempt so a hung or slow queue endpoint cannot let
/// spawned publish tasks pile up without limit; a caller's own request is
/// never blocked on this regardless of the bound.
const EVENT_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

/// The semantic mutation families that publish a commit event. This is the
/// single source of truth for "which operations matter to subscribers": the
/// match in [`mutation_kind`] is exhaustive over the two known families and
/// falls through to "not a mutation" for everything else, so adding a new
/// mutating operation to the checked-in contracts without deciding whether
/// it belongs here is a deliberate, visible choice rather than a silent gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationKind {
    VectorCommit,
    GraphCommit,
}

/// Classifies an operation as a publishable mutation, if it is one.
fn mutation_kind(operation: SemanticOperation) -> Option<MutationKind> {
    match operation {
        SemanticOperation::S3Vectors("PutVectors" | "DeleteVectors") => {
            Some(MutationKind::VectorCommit)
        }
        SemanticOperation::Graph("PutNodes" | "PutEdges") => Some(MutationKind::GraphCommit),
        _ => None,
    }
}

/// Derives the `(eventType, subject)` pair for a successful mutation from its
/// request input, or `None` if this operation does not publish. Field
/// extraction tolerates missing or oddly typed fields by skipping the
/// publish (with a warning) rather than panicking or inventing a value.
fn mutation_event(operation: SemanticOperation, input: &Value) -> Option<(&'static str, String)> {
    match mutation_kind(operation)? {
        MutationKind::VectorCommit => {
            let bucket = string_field(input, "vectorBucketName");
            let index = string_field(input, "indexName");
            match (bucket, index) {
                (Some(bucket), Some(index)) => {
                    Some(("org.verglas.vector.commit", format!("{bucket}.{index}")))
                }
                _ => {
                    tracing::warn!(
                        ?operation,
                        "mutation event input is missing vectorBucketName/indexName; skipping publish"
                    );
                    None
                }
            }
        }
        MutationKind::GraphCommit => match string_field(input, "graphName") {
            Some(graph) => Some(("org.verglas.graph.commit", graph)),
            None => {
                tracing::warn!(
                    ?operation,
                    "mutation event input is missing graphName; skipping publish"
                );
                None
            }
        },
    }
}

/// Reads a non-empty string field, tolerating a missing key, a null, or a
/// non-string value by returning `None`.
fn string_field(input: &Value, field: &str) -> Option<String> {
    input
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Decorates a [`SemanticApi`] so every successful mutating operation
/// publishes one fire-and-forget JSON event to the control-plane queue
/// ingress. See the trust-model note above this section.
pub fn wrap_with_event_publisher(
    inner: Arc<dyn SemanticApi>,
    config: EventPublishConfig,
) -> Arc<dyn SemanticApi> {
    let client = reqwest::Client::builder()
        .timeout(EVENT_PUBLISH_TIMEOUT)
        .build()
        .expect("a reqwest client with only a bounded timeout is always constructible");
    Arc::new(EventPublishingSemanticApi {
        inner,
        config,
        client,
    })
}

/// [`SemanticApi`] decorator that publishes mutation events after successful
/// calls. See the trust-model note above this section.
struct EventPublishingSemanticApi {
    inner: Arc<dyn SemanticApi>,
    config: EventPublishConfig,
    client: reqwest::Client,
}

#[async_trait]
impl SemanticApi for EventPublishingSemanticApi {
    async fn call(
        &self,
        operation: SemanticOperation,
        input: Value,
    ) -> Result<Value, SemanticError> {
        // Computed before the input is moved into the inner call, and only
        // ever published once that call has returned successfully.
        let event = mutation_event(operation, &input);
        let output = self.inner.call(operation, input).await?;
        if let Some((event_type, subject)) = event {
            self.publish(event_type, subject);
        }
        Ok(output)
    }
}

impl EventPublishingSemanticApi {
    /// Spawns the publish so it can never delay or fail the caller's result.
    fn publish(&self, event_type: &'static str, subject: String) {
        let client = self.client.clone();
        let queue_url = self.config.queue_url.clone();
        let token = self.config.token.clone();
        let tenant_id = self.config.tenant_id.clone();
        tokio::spawn(async move {
            let body = json!({
                "eventType": event_type,
                "tenant_id": tenant_id,
                "subject": subject,
            });
            match client
                .post(&queue_url)
                .bearer_auth(&token)
                .json(&body)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {}
                Ok(response) => tracing::warn!(
                    status = %response.status(),
                    event_type,
                    subject,
                    "semantic mutation event publish was rejected"
                ),
                Err(error) => tracing::warn!(
                    %error,
                    event_type,
                    subject,
                    "semantic mutation event publish failed"
                ),
            }
        });
    }
}

#[cfg(test)]
mod event_publish_mapping_tests {
    use super::*;

    /// PutVectors and DeleteVectors both map to a vector commit event keyed
    /// by "<bucket>.<index>", exactly the pair the frozen acceptance test
    /// asserts on.
    #[test]
    fn vector_mutations_map_to_a_vector_commit_event() {
        let input = json!({"vectorBucketName": "embeddings", "indexName": "docs"});
        for operation in [
            SemanticOperation::S3Vectors("PutVectors"),
            SemanticOperation::S3Vectors("DeleteVectors"),
        ] {
            let (event_type, subject) = mutation_event(operation, &input).expect("mutation");
            assert_eq!(event_type, "org.verglas.vector.commit");
            assert_eq!(subject, "embeddings.docs");
        }
    }

    /// PutNodes and PutEdges both map to a graph commit event keyed by the
    /// graph name alone.
    #[test]
    fn graph_mutations_map_to_a_graph_commit_event() {
        let input = json!({"graphName": "social"});
        for operation in [
            SemanticOperation::Graph("PutNodes"),
            SemanticOperation::Graph("PutEdges"),
        ] {
            let (event_type, subject) = mutation_event(operation, &input).expect("mutation");
            assert_eq!(event_type, "org.verglas.graph.commit");
            assert_eq!(subject, "social");
        }
    }

    /// Reads and unrelated operations never publish, whatever their input.
    #[test]
    fn non_mutating_operations_never_publish() {
        assert!(
            mutation_event(
                SemanticOperation::S3Vectors("QueryVectors"),
                &json!({"vectorBucketName": "embeddings", "indexName": "docs"}),
            )
            .is_none()
        );
        assert!(
            mutation_event(
                SemanticOperation::Graph("GetGraph"),
                &json!({"graphName": "social"}),
            )
            .is_none()
        );
        assert!(
            mutation_event(
                SemanticOperation::S3Vectors("CreateVectorBucket"),
                &json!({"vectorBucketName": "embeddings"}),
            )
            .is_none()
        );
    }

    /// A missing indexName on a vector mutation skips the publish rather
    /// than fabricating a subject like "embeddings." or panicking.
    #[test]
    fn vector_mutation_without_index_name_does_not_publish() {
        assert!(
            mutation_event(
                SemanticOperation::S3Vectors("PutVectors"),
                &json!({"vectorBucketName": "embeddings"}),
            )
            .is_none()
        );
    }

    /// A missing vectorBucketName is symmetric with the missing-index case.
    #[test]
    fn vector_mutation_without_bucket_name_does_not_publish() {
        assert!(
            mutation_event(
                SemanticOperation::S3Vectors("DeleteVectors"),
                &json!({"indexName": "docs"}),
            )
            .is_none()
        );
    }

    /// A non-string graphName (e.g. sent as a number) is tolerated as an
    /// absent field, not coerced to a string.
    #[test]
    fn graph_mutation_with_non_string_graph_name_does_not_publish() {
        assert!(
            mutation_event(
                SemanticOperation::Graph("PutNodes"),
                &json!({"graphName": 42}),
            )
            .is_none()
        );
    }

    /// An empty-string field is treated the same as an absent one.
    #[test]
    fn empty_string_fields_do_not_publish() {
        assert!(
            mutation_event(
                SemanticOperation::Graph("PutEdges"),
                &json!({"graphName": ""}),
            )
            .is_none()
        );
        assert!(
            mutation_event(
                SemanticOperation::S3Vectors("PutVectors"),
                &json!({"vectorBucketName": "embeddings", "indexName": ""}),
            )
            .is_none()
        );
    }

    /// A completely missing input object (no fields at all) does not panic.
    #[test]
    fn empty_object_input_does_not_publish() {
        assert!(mutation_event(SemanticOperation::Graph("PutNodes"), &json!({})).is_none());
        assert!(mutation_event(SemanticOperation::S3Vectors("PutVectors"), &json!({})).is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_tables_use_the_provider_safe_bucket_namespace() {
        let input = json!({"vectorBucketName":"bucket-with-hyphens","indexName":"index-one"});
        let control = bucket_control_ident(&input).expect("control ident");
        let vectors = vector_ident(&input).expect("vector ident");
        let namespace = control.namespace().first().expect("namespace");
        assert!(namespace.starts_with(VECTOR_NAMESPACE_PREFIX));
        assert!(!namespace.contains('-'));
        assert_eq!(vectors.namespace(), control.namespace());
    }

    /// A cursor advances by key, so inserting an earlier key cannot duplicate a page.
    #[test]
    fn cursor_is_bound_and_advances_by_last_key() {
        let input = json!({"operation":"ListVectors", "vectorBucketName":"b", "indexName":"i", "maxResults":1});
        let first = vec![json!({"key":"b"}), json!({"key":"d"})];
        let (page_one, token) = page(&input, first).expect("page");
        assert_eq!(page_one[0]["key"], "b");
        let mut second_input = input.clone();
        second_input["nextToken"] = json!(token.expect("token"));
        let second = vec![json!({"key":"a"}), json!({"key":"b"}), json!({"key":"d"})];
        let (page_two, _) = page(&second_input, second).expect("page");
        assert_eq!(page_two[0]["key"], "d");
    }

    /// A cursor cannot be replayed with another list resource or filter.
    #[test]
    fn cursor_rejects_a_different_binding_and_zero_limit() {
        let input = json!({"operation":"ListIndexes", "vectorBucketName":"b", "maxResults":1});
        let (_, token) = page(
            &input,
            vec![json!({"indexName":"a"}), json!({"indexName":"b"})],
        )
        .expect("page");
        let error = page(&json!({"operation":"ListIndexes", "vectorBucketName":"other", "maxResults":1, "nextToken":token}), vec![]).expect_err("binding mismatch");
        assert_eq!(error.code, "ValidationException");
        assert!(page(&json!({"operation":"ListVectors", "maxResults":0}), vec![]).is_err());
    }

    /// Only the AWS-modelled float32 union variant enters the vector engine.
    #[test]
    fn vector_data_requires_the_float32_union() {
        assert_eq!(
            vector_data(&json!({"float32":[1.0, 2.0]})).expect("union"),
            vec![1.0, 2.0]
        );
        assert!(vector_data(&json!([1.0, 2.0])).is_err());
        assert!(vector_data(&json!({"float32":["bad"]})).is_err());
    }

    /// Metadata predicates compose comparison, membership, and logical operators.
    #[test]
    fn metadata_filters_follow_the_documented_operator_rules() {
        let metadata = json!({"score": 9, "labels": ["hot", "new"], "name":"ice"});
        assert!(
            matches_filter(
                Some(&metadata),
                &json!({"score":{"$gte":8}, "labels":"hot"})
            )
            .expect("filter")
        );
        assert!(
            matches_filter(
                Some(&metadata),
                &json!({"$or":[{"name":"no"},{"score":{"$gt":8}}]})
            )
            .expect("filter")
        );
        assert!(!matches_filter(Some(&metadata), &json!({"score":{"$lt":8}})).expect("filter"));
        assert!(
            matches_filter(Some(&metadata), &json!({"labels":{"$nin":["old"]}})).expect("filter")
        );
        assert!(matches_filter(Some(&metadata), &json!({"score":{"$wat":1}})).is_err());
    }

    /// Query cursors bind the ranked request and continue after an exact rank/key pair.
    #[test]
    fn query_cursor_paginates_ranked_results_without_distance_output() {
        let input = json!({"vectorBucketName":"bucket-one","indexName":"index-one","queryVector":{"float32":[1.0]},"topK":101});
        let ranked = (0..101)
            .map(|value| (value as f32, format!("key-{value:03}"), None))
            .collect();
        let first = query_response(&input, &json!({"distanceMetric":"euclidean"}), ranked)
            .expect("query page");
        assert_eq!(first["vectors"].as_array().map(Vec::len), Some(100));
        assert!(first["vectors"][0].get("distance").is_none());
        let second = query_response(
            &json!({"vectorBucketName":"bucket-one","indexName":"index-one","queryVector":{"float32":[1.0]},"topK":101,"nextToken":first["nextToken"]}),
            &json!({"distanceMetric":"euclidean"}),
            (0..101).map(|value| (value as f32, format!("key-{value:03}"), None)).collect(),
        )
        .expect("continuation");
        assert_eq!(second["vectors"][0]["key"], "key-100");
    }

    /// A prohibited index key and malformed `$in` filter fail before any scan.
    #[test]
    fn filters_reject_non_filterable_and_nonprimitive_values() {
        let definition = json!({"metadataConfiguration":{"nonFilterableMetadataKeys":["private"]}});
        assert!(
            validate_filterable(
                &json!({"private":"x"}),
                &non_filterable_keys(&definition).expect("keys")
            )
            .is_err()
        );
        assert!(validate_filter(&json!({"label":{"$in":[{"nested":true}]}})).is_err());
    }

    /// AWS selectors reject ambiguity rather than letting a name silently override an ARN.
    #[test]
    fn vector_selectors_require_exactly_one_modelled_form() {
        assert!(validate_vector_input("GetIndex", &json!({"indexArn":"arn:aws:s3vectors:us-east-1:000000000000:bucket/bucket-one/index/index-one","indexName":"index-one","vectorBucketName":"bucket-one"})).is_err());
        assert!(validate_vector_input("GetVectorBucket", &json!({"vectorBucketName":"bucket-one","vectorBucketArn":"arn:aws:s3vectors:us-east-1:000000000000:bucket/bucket-one"})).is_err());
        assert!(
            validate_vector_input(
                "GetIndex",
                &json!({"vectorBucketName":"bucket-one","indexName":"index-one","unknown":true})
            )
            .is_err()
        );
    }
}
