//! Iceberg REST catalog transport shared by the watcher and the loopback
//! read-through gateway. Watcher refreshes retain complete successful GET
//! responses while parsing only pointer fields; local clients reuse those
//! responses, and catalog mutations remain authenticated write-through calls.
//!
//! # Why not iceberg-rust's catalog client
//!
//! `iceberg` + `iceberg-catalog-rest` pull the Arrow/Parquet/Avro stacks into
//! a crate that, for polling, only compares two scalar fields — and their
//! `load_table` deserializes the *entire* table metadata into a typed model,
//! which both costs more and fails hard on metadata constructs the model
//! doesn't know. A watcher must be robust to anything the catalog stores
//! beyond the two fields it reads, so this module speaks the REST spec
//! directly and deserializes leniently (unknown fields ignored). The mapper
//! (#49) is where full metadata parsing (and possibly iceberg-rust) belongs.
//!
//! # Auth
//!
//! Two modes (#236): a static bearer token, or AWS SigV4 for AWS-backed
//! catalogs (S3 Tables, Glue). Because this module owns its `reqwest` client, it
//! signs requests directly — no signing proxy and no `iceberg-catalog-rest`
//! (which has no signing hook). SigV4 signs the `/v1/config` call too, so the
//! warehouse-prefix bootstrap works signed end-to-end.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign as sigv4_sign};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use bytes::Bytes;
use flate2::read::MultiGzDecoder;
use reqwest::header::{
    ACCEPT_ENCODING, AUTHORIZATION, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, HOST,
    TRANSFER_ENCODING,
};
use reqwest::{Method, header::HeaderMap, header::HeaderValue};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::sync::RwLock;
use verglas_core::config;

use super::{CatalogError, CatalogMutation, CatalogSource, TableIdent, TableState};

/// SigV4 signing settings resolved from `[catalog]`: the region, the signing
/// name (`s3tables` / `glue`), and a credentials provider (a named AWS-INI file
/// or the ambient chain). The provider is shared so credentials refresh over the
/// watcher's lifetime without rebuilding the source.
#[derive(Clone, Debug)]
struct SigV4 {
    /// SigV4 signing region, e.g. `us-west-2`.
    region: String,
    /// SigV4 signing name (service), e.g. `s3tables`.
    signing_name: String,
    /// AWS credentials provider used to sign each request.
    credentials: SharedCredentialsProvider,
}

/// Namespaces can nest; the spec lists one level per call, so enumeration
/// recurses. This cap bounds the walk on pathological catalogs.
const MAX_NAMESPACE_DEPTH: usize = 8;

/// Per-request ceiling so a hung catalog cannot stall a poll cycle.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Hard memory ceiling for buffered REST catalog responses. This cache holds
/// control-plane JSON only; immutable Iceberg objects use the metadata store.
const CATALOG_RESPONSE_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// Byte-bounded response map shared by watcher refreshes and local clients.
#[derive(Debug)]
struct CachedResponses {
    /// Maximum sum of cached response body bytes.
    budget: usize,
    /// Current sum of cached response body bytes.
    bytes: usize,
    /// Path-and-query keyed successful GET responses.
    entries: HashMap<String, CatalogResponse>,
}

impl CachedResponses {
    /// Creates an empty response cache with a hard byte ceiling.
    fn new(budget: usize) -> Self {
        Self {
            budget,
            bytes: 0,
            entries: HashMap::new(),
        }
    }

    /// Returns a cloned cached response for `key`.
    fn get(&self, key: &str) -> Option<CatalogResponse> {
        self.entries.get(key).cloned()
    }

    /// Inserts one response, evicting existing entries until the hard byte
    /// ceiling can accommodate it. Responses larger than the whole budget are
    /// served but not retained.
    fn insert(&mut self, key: String, response: CatalogResponse) -> bool {
        // First population establishes generation zero; it is not a catalog
        // change. Only replacing a prepared response with different content
        // invalidates an already-built query session. Successful mutations bump
        // the generation separately before their cleared entries repopulate.
        let changed = self.entries.get(&key).is_some_and(|previous| {
            previous.status != response.status || previous.body != response.body
        });
        if let Some(previous) = self.entries.remove(&key) {
            self.bytes = self.bytes.saturating_sub(previous.body.len());
        }
        let size = response.body.len();
        if size > self.budget {
            return changed;
        }
        while self.bytes.saturating_add(size) > self.budget {
            let Some(victim) = self.entries.keys().next().cloned() else {
                break;
            };
            if let Some(removed) = self.entries.remove(&victim) {
                self.bytes = self.bytes.saturating_sub(removed.body.len());
            }
        }
        self.bytes = self.bytes.saturating_add(size);
        self.entries.insert(key, response);
        changed
    }

    /// Drops every cached GET after a successful catalog mutation.
    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }
}

/// An Iceberg REST catalog as a [`CatalogSource`].
#[derive(Clone)]
pub struct RestCatalogSource {
    /// Base URI (before `/v1/...`), no trailing slash.
    base: String,
    /// Optional static bearer token added to every request (bearer mode).
    bearer_token: Option<String>,
    /// Optional SigV4 signing (AWS-backed catalogs). Mutually exclusive with
    /// `bearer_token` by config validation.
    sigv4: Option<SigV4>,
    /// Optional warehouse passed to `/v1/config`.
    warehouse: Option<String>,
    /// Whether this catalog supports nested namespaces and `parent` queries.
    /// AWS S3 Tables is flat; other REST catalogs retain recursive discovery.
    nested_namespaces: bool,
    /// Shared HTTP client (connection pooling, timeouts).
    http: reqwest::Client,
    /// Route prefix advertised by `/v1/config`, fetched lazily once.
    prefix: Arc<tokio::sync::OnceCell<String>>,
    /// Successful GET responses shared with the loopback catalog gateway.
    responses: Arc<RwLock<CachedResponses>>,
    /// Monotonic catalog generation advanced only when a prepared REST response
    /// changes or a successful mutation invalidates the prepared response set.
    generation: Arc<AtomicU64>,
    /// Highest quorum mutation fully reflected by the response cache.
    applied_sequence: Arc<AtomicU64>,
}

impl std::fmt::Debug for RestCatalogSource {
    /// Describes transport state without exposing bearer credentials.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RestCatalogSource")
            .field("base", &self.base)
            .field(
                "authenticated",
                &(self.bearer_token.is_some() || self.sigv4.is_some()),
            )
            .field("warehouse", &self.warehouse)
            .field("nested_namespaces", &self.nested_namespaces)
            .field("generation", &self.generation.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

/// One buffered Iceberg REST response returned through the loopback gateway.
/// Bodies are small catalog JSON documents or empty commit acknowledgements;
/// object data never passes through this type.
#[derive(Clone, Debug)]
pub struct CatalogResponse {
    /// Upstream HTTP status code.
    pub status: u16,
    /// End-to-end response headers with transfer framing removed.
    pub headers: HeaderMap,
    /// Complete response body.
    pub body: Bytes,
}

/// A loopback Iceberg REST gateway sharing successful GETs with the daemon's
/// catalog watcher. Reads are served from the watcher's last-known response;
/// mutations remain write-through and invalidate cached reads on success.
#[derive(Clone, Debug)]
pub struct CatalogGateway {
    /// Shared authenticated upstream client and response cache.
    source: RestCatalogSource,
    /// Stable database identity injected only by the trusted runtime registry.
    database_id: Option<String>,
}

impl CatalogGateway {
    /// Builds a gateway from the daemon's catalog configuration.
    pub fn from_config(config: &config::Catalog) -> Result<Self, config::ConfigError> {
        Ok(Self {
            source: RestCatalogSource::from_config(config)?,
            database_id: None,
        })
    }

    /// Binds this gateway to the stable database selected by the runtime registry.
    pub(crate) fn bind_database(mut self, database_id: &str) -> Self {
        self.database_id = Some(database_id.to_owned());
        self
    }

    /// Returns a watcher source backed by the same client and response cache.
    pub fn source(&self) -> RestCatalogSource {
        self.source.clone()
    }

    /// Current prepared-catalog generation for query-session invalidation.
    pub fn generation(&self) -> u64 {
        self.source.generation.load(Ordering::Acquire)
    }

    /// Highest strong catalog sequence visible to local query clients.
    pub fn applied_sequence(&self) -> u64 {
        self.source.applied_sequence.load(Ordering::Acquire)
    }

    /// Applies one durable pointer and invalidates prepared catalog responses.
    ///
    /// Replay deliberately performs no upstream table load: a REST catalog
    /// exposes only the current pointer and cannot reproduce every historical
    /// outbox position after a restart. The next fenced read repopulates the
    /// response cache from the current catalog state.
    pub async fn apply_mutation(
        &self,
        mutation: &CatalogMutation,
    ) -> Result<Option<TableState>, CatalogError> {
        if mutation.sequence <= self.applied_sequence() {
            if mutation.operation == "dropTable" {
                return Ok(None);
            }
            return Ok(mutation
                .metadata_location
                .as_ref()
                .map(|location| TableState {
                    metadata_location: location.clone(),
                    current_snapshot_id: mutation.snapshot_id,
                }));
        }
        self.source.responses.write().await.clear();
        let dropped = mutation.operation == "dropTable";
        let state = if dropped {
            None
        } else {
            mutation
                .metadata_location
                .as_ref()
                .map(|location| TableState {
                    metadata_location: location.clone(),
                    current_snapshot_id: mutation.snapshot_id,
                })
        };
        self.source.generation.fetch_add(1, Ordering::AcqRel);
        self.source
            .applied_sequence
            .store(mutation.sequence, Ordering::Release);
        Ok(state)
    }

    /// Serves one local catalog request. Successful watcher or gateway GETs are
    /// reusable without upstream I/O; successful mutations clear cached GETs so
    /// the next load observes the commit before being cached again.
    pub async fn request(
        &self,
        method: Method,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<CatalogResponse, CatalogError> {
        self.source
            .gateway_request(method, path_and_query, headers, body, None, None)
            .await
    }

    /// Serves one request using the exact bearer verified at the public data-plane boundary.
    /// Authenticated reads always go upstream and are never inserted into or served from the
    /// watcher cache, preventing responses from crossing principal boundaries.
    pub async fn authenticated_request(
        &self,
        method: Method,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
        authorization: HeaderValue,
    ) -> Result<CatalogResponse, CatalogError> {
        let valid = authorization
            .to_str()
            .ok()
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|token| !token.is_empty());
        if !valid {
            return Err(CatalogError::Auth {
                detail: "verified catalog credential is not a bearer header".to_owned(),
            });
        }
        let database_id = self
            .database_id
            .as_deref()
            .ok_or_else(|| CatalogError::Auth {
                detail: "authenticated catalog gateway has no database binding".to_owned(),
            })?;
        let database_id = HeaderValue::from_str(database_id).map_err(|_| CatalogError::Auth {
            detail: "catalog database binding is not a valid header value".to_owned(),
        })?;
        self.source
            .gateway_request(
                method,
                path_and_query,
                headers,
                body,
                Some(authorization),
                Some(database_id),
            )
            .await
    }
}

/// `GET /v1/config` response: we only care about a `prefix` override.
#[derive(Deserialize, Default)]
struct ConfigResponse {
    #[serde(default)]
    overrides: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    defaults: serde_json::Map<String, serde_json::Value>,
}

/// `GET .../namespaces` response.
#[derive(Deserialize, Default)]
struct ListNamespacesResponse {
    #[serde(default)]
    namespaces: Vec<Vec<String>>,
}

/// `GET .../namespaces/{ns}/tables` response.
#[derive(Deserialize, Default)]
struct ListTablesResponse {
    #[serde(default)]
    identifiers: Vec<RestTableIdent>,
}

/// A table identifier as the REST spec serializes it.
#[derive(Deserialize)]
struct RestTableIdent {
    namespace: Vec<String>,
    name: String,
}

/// `GET .../tables/{table}` (load table) response — pointer fields only;
/// everything else in the (large) metadata document is ignored.
#[derive(Deserialize)]
struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: Option<String>,
    #[serde(default)]
    metadata: LoadTableMetadata,
}

/// The two-field view of `metadata` a cheap poll reads.
#[derive(Deserialize, Default)]
struct LoadTableMetadata {
    #[serde(rename = "current-snapshot-id", default)]
    current_snapshot_id: Option<i64>,
}

/// Percent-encodes one URL path segment or query value (RFC 3986 unreserved
/// characters pass through). Needed because multipart namespaces are joined
/// with the `0x1F` unit separator, which must travel encoded.
fn encode(component: &str) -> String {
    let mut out = String::with_capacity(component.len());
    for byte in component.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Joins namespace levels with the unit separator and encodes the result,
/// as the REST spec requires for namespace path/query components.
fn encode_namespace(namespace: &[String]) -> String {
    encode(&namespace.join("\u{1f}"))
}

/// The host (`host[:port]`) of a URL, for the SigV4 `host` header the signature
/// covers. Falls back to the whole string if the URL has no `//host` — the
/// signer then signs whatever the request actually sends.
fn url_host(url: &str) -> String {
    url.split_once("://")
        .map(|(_, rest)| rest.split(['/', '?']).next().unwrap_or(rest))
        .unwrap_or(url)
        .to_owned()
}

/// Decompresses a gzip-encoded catalog response body. A fresh decoder per call
/// keeps the decode path free of shared state, so concurrent proxied requests
/// cannot interleave and garble each other's bytes. `MultiGzDecoder` accepts a
/// single member or several concatenated members.
fn gunzip(body: &[u8], url: &str) -> Result<Bytes, CatalogError> {
    let mut decoder = MultiGzDecoder::new(body);
    let mut out = Vec::with_capacity(body.len().saturating_mul(4));
    decoder
        .read_to_end(&mut out)
        .map_err(|error| CatalogError::Malformed {
            url: url.to_owned(),
            detail: format!("gzip decode failed: {error}"),
        })?;
    Ok(Bytes::from(out))
}

/// Builds the SigV4 signing config from `[catalog]` when SigV4 is enabled. The
/// credentials provider is a named AWS-INI file (with an optional profile,
/// mirroring `[backend]`) or the ambient AWS chain when no file is named. The
/// provider resolves lazily, so no I/O happens here. Returns `None` when SigV4
/// is off.
fn build_sigv4(config: &config::Catalog) -> Option<SigV4> {
    if !config.sigv4_enabled() {
        return None;
    }
    // Both are `Some` when `sigv4_enabled()`.
    let region = config.sigv4_region.clone()?;
    let signing_name = config.sigv4_signing_name.clone()?;
    let credentials = match &config.credentials_file {
        Some(file) => {
            // A named AWS-INI credentials file: point the profile provider at it.
            // Resolution is lazy (first `provide_credentials`), so a missing file
            // surfaces as a signing error at poll time, not here.
            let profile = config.credentials_profile.as_deref().unwrap_or("default");
            let profile_files = aws_runtime::env_config::file::EnvConfigFiles::builder()
                .with_file(
                    aws_runtime::env_config::file::EnvConfigFileKind::Credentials,
                    file,
                )
                .build();
            let provider = aws_config::profile::ProfileFileCredentialsProvider::builder()
                .profile_name(profile)
                .profile_files(profile_files)
                .build();
            SharedCredentialsProvider::new(provider)
        }
        None => {
            // No file named: the ambient AWS chain (env, SSO cache, instance
            // role). Built lazily on first use so the sync constructor does no I/O.
            SharedCredentialsProvider::new(LazyDefaultChain::default())
        }
    };
    Some(SigV4 {
        region,
        signing_name,
        credentials,
    })
}

/// A [`ProvideCredentials`] over the ambient AWS default chain, built on first
/// use. The default chain's builder is async, so this defers it out of the
/// synchronous [`RestCatalogSource::from_config`] and caches the built chain.
#[derive(Default)]
struct LazyDefaultChain {
    /// The built chain, resolved once on first credential request.
    chain:
        tokio::sync::OnceCell<aws_config::default_provider::credentials::DefaultCredentialsChain>,
}

impl std::fmt::Debug for LazyDefaultChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyDefaultChain").finish_non_exhaustive()
    }
}

impl ProvideCredentials for LazyDefaultChain {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(async move {
            let chain = self
                .chain
                .get_or_init(|| {
                    aws_config::default_provider::credentials::DefaultCredentialsChain::builder()
                        .build()
                })
                .await;
            chain.provide_credentials().await
        })
    }
}

impl RestCatalogSource {
    /// Creates a source for a catalog at `uri` with no auth or warehouse.
    pub fn new(uri: impl Into<String>) -> RestCatalogSource {
        RestCatalogSource::build(uri.into(), None, None, None)
    }

    /// Creates a source from the daemon's `[catalog]` config section. In SigV4
    /// mode, resolves the AWS credentials provider (a named AWS-INI file with an
    /// optional profile, or the ambient chain) and no bearer token. Otherwise
    /// resolves the bearer token from `credentials_file` (or the inline token),
    /// so a missing token file or a both-set conflict surfaces here rather than
    /// as a silent no-auth poll.
    pub fn from_config(config: &config::Catalog) -> Result<RestCatalogSource, config::ConfigError> {
        let sigv4 = build_sigv4(config);
        let bearer_token = config.resolve_bearer_token()?;
        Ok(RestCatalogSource::build(
            config.uri.clone(),
            bearer_token,
            sigv4,
            config.warehouse.clone(),
        ))
    }

    /// Shared constructor: normalizes the base URI and builds the client.
    fn build(
        uri: String,
        bearer_token: Option<String>,
        sigv4: Option<SigV4>,
        warehouse: Option<String>,
    ) -> RestCatalogSource {
        // The signing service is an explicit provider capability, not a URI
        // heuristic. AWS documents S3 Tables namespaces as single-level and
        // rejects the REST catalog's optional `parent` query with HTTP 400.
        let nested_namespaces = sigv4
            .as_ref()
            .map(|settings| settings.signing_name != "s3tables")
            .unwrap_or(true);
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            // Builder failure means TLS init is broken; the plain client
            // shares that fate, so this fallback only normalizes the panic
            // site — it cannot mask a real error.
            .unwrap_or_else(|_| reqwest::Client::new());
        RestCatalogSource {
            base: uri.trim_end_matches('/').to_owned(),
            bearer_token,
            sigv4,
            warehouse,
            nested_namespaces,
            http,
            prefix: Arc::new(tokio::sync::OnceCell::new()),
            responses: Arc::new(RwLock::new(CachedResponses::new(
                CATALOG_RESPONSE_CACHE_BYTES,
            ))),
            generation: Arc::new(AtomicU64::new(0)),
            applied_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Converts a full upstream URL into the stable path-and-query cache key
    /// local clients send to the gateway.
    fn cache_key(&self, url: &str) -> String {
        url.strip_prefix(&self.base).unwrap_or(url).to_owned()
    }

    /// Resolves a local gateway path into the upstream path owned by this cache.
    /// Query workers deliberately omit warehouse configuration; the cache adds
    /// it to config discovery so the request shares the watcher's prepared entry.
    fn gateway_path(&self, path_and_query: &str) -> String {
        if path_and_query == "/v1/config"
            && let Some(warehouse) = &self.warehouse
        {
            return format!("/v1/config?warehouse={}", encode(warehouse));
        }
        path_and_query.to_owned()
    }

    /// Sends one request to the configured upstream catalog, applying bearer or
    /// SigV4 authentication and buffering the small REST response body.
    async fn send_upstream(
        &self,
        method: Method,
        url: &str,
        headers: HeaderMap,
        body: Bytes,
        forwarded_authorization: Option<&HeaderValue>,
        trusted_database_id: Option<&HeaderValue>,
    ) -> Result<CatalogResponse, CatalogError> {
        let mut request = self.http.request(method.clone(), url);
        for (name, value) in &headers {
            if name == HOST
                || name == CONTENT_LENGTH
                || name == CONNECTION
                || name == TRANSFER_ENCODING
                || name == AUTHORIZATION
                || name == ACCEPT_ENCODING
                || name.as_str().eq_ignore_ascii_case("x-verglas-database-id")
            {
                continue;
            }
            request = request.header(name, value);
        }
        if let Some(authorization) = forwarded_authorization {
            request = request.header(AUTHORIZATION, authorization);
        } else {
            if let Some(token) = &self.bearer_token {
                request = request.bearer_auth(token);
            }
            if let Some(sigv4) = &self.sigv4 {
                request = self
                    .sign(sigv4, &method, url, &headers, &body, request)
                    .await?;
            }
        }
        if let Some(database_id) = trusted_database_id {
            request = request.header("x-verglas-database-id", database_id);
        }
        // The daemon negotiates compression with the upstream itself and always
        // hands clients decoded JSON, so the client's own Accept-Encoding never
        // reaches the origin (stripped above) and cannot decide the wire framing.
        request = request.header(ACCEPT_ENCODING, "gzip");
        let response = request.body(body).send().await?;
        let status = response.status().as_u16();
        let mut response_headers = response.headers().clone();
        let content_encoding = response_headers
            .get(CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok())
            .map(str::to_ascii_lowercase);
        response_headers.remove(TRANSFER_ENCODING);
        response_headers.remove(CONTENT_LENGTH);
        let raw = response.bytes().await?;
        // Decode compressed upstream bodies here so every local client — the
        // CLI, the sidecar's iceberg-rust client, curl — receives plain JSON
        // regardless of what it can decompress. Identity bodies pass through
        // untouched. The decoder is built per call, so concurrent proxied
        // requests never share decode state.
        let body = match content_encoding.as_deref() {
            Some("gzip") | Some("x-gzip") if !raw.is_empty() => {
                response_headers.remove(CONTENT_ENCODING);
                gunzip(&raw, url)?
            }
            Some("gzip") | Some("x-gzip") => {
                response_headers.remove(CONTENT_ENCODING);
                raw
            }
            _ => raw,
        };
        Ok(CatalogResponse {
            status,
            headers: response_headers,
            body,
        })
    }

    /// Executes a loopback gateway request against the shared response cache or
    /// the authenticated upstream catalog on a miss.
    async fn gateway_request(
        &self,
        method: Method,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
        forwarded_authorization: Option<HeaderValue>,
        trusted_database_id: Option<HeaderValue>,
    ) -> Result<CatalogResponse, CatalogError> {
        if !path_and_query.starts_with('/') {
            return Err(CatalogError::Malformed {
                url: path_and_query.to_owned(),
                detail: "catalog gateway path must start with '/'".to_owned(),
            });
        }
        let resolved_path = self.gateway_path(path_and_query);
        if forwarded_authorization.is_none()
            && method == Method::GET
            && let Some(response) = self.responses.read().await.get(&resolved_path)
        {
            return Ok(response);
        }

        let url = format!("{}{}", self.base, resolved_path);
        let response = self
            .send_upstream(
                method.clone(),
                &url,
                headers,
                body,
                forwarded_authorization.as_ref(),
                trusted_database_id.as_ref(),
            )
            .await?;
        if (200..300).contains(&response.status) {
            if method == Method::GET && forwarded_authorization.is_none() {
                let changed = self
                    .responses
                    .write()
                    .await
                    .insert(resolved_path, response.clone());
                if changed {
                    self.generation.fetch_add(1, Ordering::AcqRel);
                }
            } else {
                self.responses.write().await.clear();
                self.generation.fetch_add(1, Ordering::AcqRel);
            }
        }
        Ok(response)
    }

    /// One authenticated GET decoded as JSON; non-2xx becomes
    /// [`CatalogError::Status`], a bad body [`CatalogError::Malformed`]. The
    /// request is bearer-authenticated or SigV4-signed per the configured mode
    /// (a GET carries an empty body, whose payload hash SigV4 signs).
    async fn get_json<T: DeserializeOwned>(&self, url: &str) -> Result<T, CatalogError> {
        let response = self
            .send_upstream(Method::GET, url, HeaderMap::new(), Bytes::new(), None, None)
            .await?;
        if !(200..300).contains(&response.status) {
            return Err(CatalogError::Status {
                status: response.status,
                url: url.to_owned(),
            });
        }
        let key = self.cache_key(url);
        let changed = self.responses.write().await.insert(key, response.clone());
        if changed {
            self.generation.fetch_add(1, Ordering::AcqRel);
        }
        serde_json::from_slice::<T>(&response.body).map_err(|e| CatalogError::Malformed {
            url: url.to_owned(),
            detail: e.to_string(),
        })
    }

    /// Signs a GET request with SigV4 (empty body) and stamps the emitted
    /// `Authorization` (and `x-amz-security-token` for session credentials)
    /// headers onto it. Resolves credentials per request so a rotated identity is
    /// picked up; a resolution or signing failure becomes [`CatalogError::Auth`].
    async fn sign(
        &self,
        sigv4: &SigV4,
        method: &Method,
        url: &str,
        request_headers: &HeaderMap,
        body: &[u8],
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, CatalogError> {
        let credentials =
            sigv4
                .credentials
                .provide_credentials()
                .await
                .map_err(|e| CatalogError::Auth {
                    detail: format!("cannot resolve AWS credentials for SigV4: {e}"),
                })?;
        let identity: Identity = credentials.into();
        let signing_settings = SigningSettings::default();
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&sigv4.region)
            .name(&sigv4.signing_name)
            .time(std::time::SystemTime::now())
            .settings(signing_settings)
            .build()
            .map_err(|e| CatalogError::Auth {
                detail: format!("cannot build SigV4 params: {e}"),
            })?;
        // Sign over the request as the origin will see it: the full URL, true
        // host, forwarded end-to-end headers, and the complete buffered body.
        let host = url_host(url);
        let mut headers = vec![("host".to_owned(), host)];
        for (name, value) in request_headers {
            if name == HOST
                || name == CONTENT_LENGTH
                || name == CONNECTION
                || name == TRANSFER_ENCODING
                || name == AUTHORIZATION
                // The daemon sets its own Accept-Encoding after signing, so it
                // must stay out of the signed header set to match the wire.
                || name == ACCEPT_ENCODING
            {
                continue;
            }
            if let Ok(value) = value.to_str() {
                headers.push((name.as_str().to_owned(), value.to_owned()));
            }
        }
        let signable = SignableRequest::new(
            method.as_str(),
            url,
            headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
            SignableBody::Bytes(body),
        )
        .map_err(|e| CatalogError::Auth {
            detail: format!("cannot build signable request: {e}"),
        })?;
        let (instructions, _sig) = sigv4_sign(signable, &signing_params.into())
            .map_err(|e| CatalogError::Auth {
                detail: format!("SigV4 signing failed: {e}"),
            })?
            .into_parts();
        // Apply the signer's headers (Authorization, x-amz-*) onto the reqwest
        // builder. `headers` are (name, value) pairs; params carry no query.
        let mut signed = request;
        for header in instructions.headers() {
            signed = signed.header(header.0, header.1);
        }
        Ok(signed)
    }

    /// Route root including the catalog's prefix (`{base}/v1` or
    /// `{base}/v1/{prefix}`), resolving `/v1/config` on first use. A failed
    /// fetch is retried on the next call — the cell only caches success.
    async fn route_root(&self) -> Result<String, CatalogError> {
        let prefix = self
            .prefix
            .get_or_try_init(|| async {
                let mut url = format!("{}/v1/config", self.base);
                if let Some(warehouse) = &self.warehouse {
                    url.push_str("?warehouse=");
                    url.push_str(&encode(warehouse));
                }
                let response: ConfigResponse = self.get_json(&url).await?;
                let prefix = response
                    .overrides
                    .get("prefix")
                    .or_else(|| response.defaults.get("prefix"))
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                Ok::<_, CatalogError>(prefix.to_owned())
            })
            .await?;
        if prefix.is_empty() {
            Ok(format!("{}/v1", self.base))
        } else {
            Ok(format!("{}/v1/{prefix}", self.base))
        }
    }

    /// Enumerates all namespaces, walking nested levels via the `parent`
    /// query parameter, bounded by [`MAX_NAMESPACE_DEPTH`].
    async fn list_namespaces(&self, root: &str) -> Result<Vec<Vec<String>>, CatalogError> {
        let mut seen: HashSet<Vec<String>> = HashSet::new();
        let url = format!("{root}/namespaces");
        let response: ListNamespacesResponse = self.get_json(&url).await?;
        let mut queue = Vec::new();
        for namespace in response.namespaces {
            if seen.insert(namespace.clone())
                && self.nested_namespaces
                && namespace.len() < MAX_NAMESPACE_DEPTH
            {
                queue.push(namespace);
            }
        }
        while let Some(parent) = queue.pop() {
            let url = format!("{root}/namespaces?parent={}", encode_namespace(&parent));
            let response: ListNamespacesResponse = self.get_json(&url).await?;
            for namespace in response.namespaces {
                // `seen` both dedups and guarantees the walk terminates even
                // if a catalog echoes ancestors back.
                if seen.insert(namespace.clone()) && namespace.len() < MAX_NAMESPACE_DEPTH {
                    queue.push(namespace);
                }
            }
        }
        Ok(seen.into_iter().collect())
    }
}

#[async_trait::async_trait]
impl CatalogSource for RestCatalogSource {
    /// Walks namespaces and lists their tables. Cost per poll: one request
    /// per namespace level plus one per namespace — no table metadata reads.
    async fn list_tables(&self) -> Result<Vec<TableIdent>, CatalogError> {
        let root = self.route_root().await?;
        let mut tables = Vec::new();
        for namespace in self.list_namespaces(&root).await? {
            let url = format!("{root}/namespaces/{}/tables", encode_namespace(&namespace));
            let response: ListTablesResponse = self.get_json(&url).await?;
            tables.extend(response.identifiers.into_iter().map(|id| TableIdent {
                namespace: id.namespace,
                name: id.name,
            }));
        }
        Ok(tables)
    }

    /// Loads one table and extracts only the pointer fields.
    async fn table_pointer(&self, table: &TableIdent) -> Result<TableState, CatalogError> {
        let root = self.route_root().await?;
        let url = format!(
            "{root}/namespaces/{}/tables/{}",
            encode_namespace(&table.namespace),
            encode(&table.name)
        );
        let response: LoadTableResponse = self.get_json(&url).await?;
        let metadata_location =
            response
                .metadata_location
                .ok_or_else(|| CatalogError::Malformed {
                    url: url.clone(),
                    detail: "load-table response carries no metadata-location".to_owned(),
                })?;
        Ok(TableState {
            metadata_location,
            // Spec quirk: v1 metadata uses -1 for "no current snapshot".
            current_snapshot_id: response.metadata.current_snapshot_id.filter(|id| *id >= 0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Catalog response caching obeys its hard byte ceiling by evicting older
    /// responses before admitting a new one.
    #[test]
    fn cached_responses_never_exceed_their_byte_budget() {
        let mut cache = CachedResponses::new(10);
        let response = |size| CatalogResponse {
            status: 200,
            headers: HeaderMap::new(),
            body: Bytes::from(vec![0; size]),
        };
        cache.insert("/one".to_owned(), response(6));
        cache.insert("/two".to_owned(), response(6));
        assert!(cache.bytes <= 10);
        assert!(cache.entries.contains_key("/two"));
        assert!(!cache.entries.contains_key("/one"));
    }

    /// A local query worker does not know the upstream warehouse. The cache
    /// gateway adds its owned warehouse to `/v1/config`, matching the response
    /// the watcher already prepared.
    #[test]
    fn gateway_config_uses_the_cache_owned_warehouse() {
        let source = RestCatalogSource::build(
            "http://catalog".to_owned(),
            None,
            None,
            Some("tenant-001".to_owned()),
        );
        assert_eq!(
            source.gateway_path("/v1/config"),
            "/v1/config?warehouse=tenant-001"
        );
    }
}
