//! Peer-fetch RPC (#29): the block-transfer server and client that turn N
//! nodes into one logical cache.
//!
//! A pod node that receives a client request for a key it does not own asks
//! the *owner* for the block before falling back to a backend fill — the
//! "home/peer cell over the pod network" rung of the miss ladder (WHITEPAPER
//! §3.3). This module is both halves of that rung:
//!
//! - [`PeerServer`] listens on this node's advertised peer address and serves
//!   blocks **from the local cache tiers only** (a callback the server wires to
//!   the engine). A miss is a clean miss — it never fills from the backend, so
//!   a peer request can never trigger a fill (no recursion, no fill
//!   amplification), and it matches the full [`BlockKey`] (ETag included) so it
//!   can never serve a different version's bytes.
//! - [`PeerClient`] implements [`PeerFetch`]: it resolves the owner's
//!   advertised address from the live membership, fetches the block with a
//!   tight timeout, and degrades any slow/failed/absent peer to a clean miss or
//!   a transport error the caller falls back from — the ladder continues to the
//!   backend, it never errors to the S3 client (slow is acceptable; wrong is
//!   never).
//!
//! ## Transport choice: HTTP/2 over the existing hyper stack
//!
//! We serve peer blocks over HTTP (axum/hyper server, reqwest client) rather
//! than adding gRPC/tonic. The requirements — streaming bodies, connection
//! pooling, multiplexing — are all met by HTTP/2: reqwest keeps a pooled,
//! multiplexed h2 connection per peer (prior-knowledge cleartext h2, since the
//! pod network is private, #37), block bodies stream as `application/octet-
//! stream`, and we reuse the exact hyper stack the S3 front-end and admin API
//! already build on — no protobuf toolchain, no second server flavor.
//!
//! ## Wire format is an internal detail, not a contract
//!
//! Per the prototype rules (AGENTS.md) there is deliberately **no protocol
//! version negotiation**: the request is a small JSON envelope, the response is
//! raw bytes or `204 No Content`, and the `/v0/` path segment is a marker for
//! the day rolling upgrades need wire-compat (#79) — it is not checked. The
//! request carries the full [`BlockKey`] and no ring epoch: exact-`BlockKey`
//! matching already makes a stale-epoch request safe (it either hits the right
//! bytes or misses cleanly), so a `NotOwner` round-trip would buy nothing here.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::Router;
use axum::body::Bytes as AxumBytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use verglas_core::BlockKey;
use verglas_core::node::NodeId;
use verglas_core::peer::{PeerFetch, PeerFetchError};

use crate::fragments::{FragmentIoError, FragmentKey, FragmentRecord, LoadedFragment};
use crate::member::NodeMeta;

/// HTTP header carrying the shared cluster secret (peer auth, #29). Absent or
/// wrong when the server requires one is a `403`, not a serve.
const SECRET_HEADER: &str = "x-verglas-cluster-secret";

/// Path the peer block endpoint is served under. The `v0` segment is a
/// wire-compat extension point (#79) — it is not negotiated (prototype rules).
const BLOCK_PATH: &str = "/peer/v0/block";

/// Write-back fragment store endpoint (#180): POST stores the fragment carried
/// in the body, identified by the object-id and index headers below.
const FRAGMENT_PUT_PATH: &str = "/peer/v0/fragment";

/// Write-back fragment read endpoint (#180): POST a [`FragmentRef`] JSON body,
/// get the fragment bytes (200) or a clean miss (204).
const FRAGMENT_GET_PATH: &str = "/peer/v0/fragment/get";

/// Write-back fragment delete endpoint (#180): POST a [`FragmentRef`] JSON body,
/// get 204 whether or not the fragment was present (idempotent).
const FRAGMENT_DELETE_PATH: &str = "/peer/v0/fragment/delete";

/// Write-back fragment headroom endpoint (#180): the byte count wanted travels
/// in a header; the response is `200` with body `1` (room) or `0` (full). Used
/// by placement to exclude a peer whose fragment store is at its budget.
const FRAGMENT_HEADROOM_PATH: &str = "/peer/v0/fragment/headroom";

/// Header carrying the byte count a headroom check asks about.
const FRAGMENT_BYTES_HEADER: &str = "x-verglas-fragment-bytes";

/// Header carrying the fragment's object id on a fragment PUT.
const FRAGMENT_OBJECT_HEADER: &str = "x-verglas-fragment-object";

/// Header carrying the fragment's index on a fragment PUT.
const FRAGMENT_INDEX_HEADER: &str = "x-verglas-fragment-index";

/// Header carrying a fragment's CRC32C on a fragment GET response (#220), so the
/// requesting node can verify the fragment end-to-end before using it in
/// reassembly. A missing or unparseable header is treated as a checksum of 0,
/// which the codec verification then rejects — corrupt-safe.
const FRAGMENT_CHECKSUM_HEADER: &str = "x-verglas-fragment-crc32c";

/// Resolves a pod node id to the address it advertises for peer fetch.
///
/// The client asks the ring who owns a key, then asks this to turn that owner
/// id into a socket address. A `None` (unknown node, or a node advertising no
/// peer address) is a clean miss: the caller falls through to a backend fill.
pub trait PeerResolver: Send + Sync {
    /// Returns `node`'s advertised peer-fetch address, or `None` if the node is
    /// unknown or advertises none.
    fn resolve(&self, node: &NodeId) -> Option<SocketAddr>;
}

/// A fixed node→address map, for wiring a single-node server (empty, so it
/// resolves nothing) and for tests that point a client at a known server.
#[derive(Debug, Default, Clone)]
pub struct StaticResolver {
    /// The known node→peer-address entries.
    map: HashMap<NodeId, SocketAddr>,
}

impl StaticResolver {
    /// An empty resolver — resolves nothing, so every fetch is a clean miss.
    /// This is the single-node (`no [cluster]`) wiring: the ring owns every key
    /// locally, so the peer rung is never reached anyway.
    pub fn new() -> Self {
        Self::default()
    }

    /// A resolver with one `node → addr` entry.
    pub fn with(node: NodeId, addr: SocketAddr) -> Self {
        Self {
            map: HashMap::from([(node, addr)]),
        }
    }
}

impl PeerResolver for StaticResolver {
    /// Looks up the node's advertised address in the fixed map.
    fn resolve(&self, node: &NodeId) -> Option<SocketAddr> {
        self.map.get(node).copied()
    }
}

/// A resolver over the cluster agent's live membership snapshot: it reads the
/// same lock-free `ArcSwap` the agent updates on every gossip change, so the
/// address it returns tracks membership without any wiring of its own.
pub(crate) struct GossipResolver {
    /// The membership snapshot the cluster agent swaps atomically.
    pub(crate) members: Arc<ArcSwap<Vec<NodeMeta>>>,
}

impl PeerResolver for GossipResolver {
    /// Finds the member with this id and returns its advertised peer address.
    fn resolve(&self, node: &NodeId) -> Option<SocketAddr> {
        self.members
            .load()
            .iter()
            .find(|m| &m.node_id == node)
            .and_then(|m| m.advertise_addr)
    }
}

/// A cache-only lookup of one block by its exact [`BlockKey`], returning the
/// bytes if this node has them resident and `None` otherwise.
///
/// Invariant the server must uphold when it wires this: **never fill from the
/// backend here.** A peer miss must return a miss so the requester fills once
/// at the owner — filling on a peer request would recurse and amplify fills.
pub type LocalBlockFn =
    Arc<dyn Fn(BlockKey) -> Pin<Box<dyn Future<Output = Option<Bytes>> + Send>> + Send + Sync>;

/// The JSON request envelope: the full block identity, flattened. No ring epoch
/// and no version tag travel with it (see the module docs — exact-`BlockKey`
/// matching subsumes both).
#[derive(Debug, Serialize, Deserialize)]
struct BlockRequest {
    /// Immutable storage binding selecting the origin.
    storage_binding_id: String,
    /// Origin bucket of the object.
    bucket: String,
    /// Object key within the bucket.
    key: String,
    /// ETag of the exact object version wanted.
    etag: String,
    /// Block geometry of the exact byte range wanted.
    block_bytes: u64,
    /// Zero-based block index within the object.
    block_index: u64,
}

impl BlockRequest {
    /// Flattens a `BlockKey` into the wire envelope.
    fn from_key(block: &BlockKey) -> Self {
        Self {
            storage_binding_id: block.object.storage_binding_id.clone(),
            bucket: block.object.bucket.clone(),
            key: block.object.key.clone(),
            etag: block.etag.clone(),
            block_bytes: block.block_bytes,
            block_index: block.block_index,
        }
    }

    /// Rebuilds the `BlockKey` the envelope names.
    fn into_key(self) -> BlockKey {
        BlockKey {
            object: verglas_core::CacheKey {
                storage_binding_id: self.storage_binding_id,
                bucket: self.bucket,
                key: self.key,
            },
            etag: self.etag,
            block_bytes: self.block_bytes,
            block_index: self.block_index,
        }
    }
}

/// Stores one write-back fragment durably on this node, returning `Ok` once it
/// is on NVMe (#180). Wired by the server to the local fragment store.
pub type FragmentStoreFn = Arc<
    dyn Fn(FragmentRecord) -> Pin<Box<dyn Future<Output = Result<(), FragmentIoError>> + Send>>
        + Send
        + Sync,
>;

/// Loads one fragment by key, `None` when this node lacks it (#180). The loaded
/// fragment carries its checksum so the reader verifies end-to-end (#220).
pub type FragmentLoadFn = Arc<
    dyn Fn(
            FragmentKey,
        )
            -> Pin<Box<dyn Future<Output = Result<Option<LoadedFragment>, FragmentIoError>> + Send>>
        + Send
        + Sync,
>;

/// Deletes one fragment by key; missing is success (#180).
pub type FragmentDeleteFn = Arc<
    dyn Fn(FragmentKey) -> Pin<Box<dyn Future<Output = Result<(), FragmentIoError>> + Send>>
        + Send
        + Sync,
>;

/// Answers whether the local fragment store can still hold `bytes` under its
/// budget (#180). True means placement may target this node.
pub type FragmentHeadroomFn =
    Arc<dyn Fn(u64) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

/// A stream of a fragment's shard chunks in stripe order (#180).
pub type FragmentShardStream = Pin<Box<dyn futures::Stream<Item = Bytes> + Send>>;

/// Stores one fragment by streaming its shards into the local store, so the
/// receiving node holds one stripe at a time rather than the whole fragment
/// (#180). `Ok` means the whole fragment is durable.
pub type FragmentStoreStreamFn = Arc<
    dyn Fn(
            FragmentKey,
            FragmentShardStream,
        ) -> Pin<Box<dyn Future<Output = Result<(), FragmentIoError>> + Send>>
        + Send
        + Sync,
>;

/// The fragment callbacks the server wires to the local fragment store when the
/// write-back tier is enabled.
#[derive(Clone)]
pub struct FragmentHandlers {
    /// Store a fragment durably.
    pub store: FragmentStoreFn,
    /// Store a fragment by streaming its shards (bounded peer memory).
    pub store_stream: FragmentStoreStreamFn,
    /// Load a fragment (clean miss when absent).
    pub load: FragmentLoadFn,
    /// Delete a fragment (idempotent).
    pub delete: FragmentDeleteFn,
    /// Report whether this node has budget headroom for a fragment.
    pub headroom: FragmentHeadroomFn,
}

/// Shared state the block handler reads: the local-cache lookup, the optional
/// required secret, and the optional write-back fragment handlers.
#[derive(Clone)]
struct ServerState {
    /// Cache-only block lookup (never fills — see [`LocalBlockFn`]).
    source: LocalBlockFn,
    /// The secret a request must present, or `None` to accept unauthenticated
    /// requests (dev / trusted-network setups).
    secret: Option<String>,
    /// Fragment store handlers, present only when write-back is enabled.
    fragments: Option<FragmentHandlers>,
}

/// The JSON envelope naming one fragment for a read or delete.
#[derive(Debug, Serialize, Deserialize)]
struct FragmentRef {
    /// The object the fragment belongs to.
    object_id: String,
    /// The fragment index within the `k+m` set.
    index: usize,
}

/// The peer-fetch server: listens on this node's advertised address and serves
/// blocks from the local cache tiers only.
pub struct PeerServer {
    /// The serving task; aborted on [`PeerServer::shutdown`].
    task: JoinHandle<()>,
    /// The address actually bound (a `:0` request resolves to a real port).
    local_addr: SocketAddr,
}

impl PeerServer {
    /// Binds `addr` and starts serving peer block requests backed by `source`,
    /// requiring `secret` when present. Returns once the socket is bound, so a
    /// client can connect immediately.
    pub async fn bind(
        addr: SocketAddr,
        secret: Option<String>,
        source: LocalBlockFn,
    ) -> std::io::Result<Self> {
        Self::bind_inner(addr, secret, source, None).await
    }

    /// Binds like [`PeerServer::bind`] and additionally serves the write-back
    /// fragment endpoints (#180) backed by `fragments`.
    pub async fn bind_with_fragments(
        addr: SocketAddr,
        secret: Option<String>,
        source: LocalBlockFn,
        fragments: FragmentHandlers,
    ) -> std::io::Result<Self> {
        Self::bind_inner(addr, secret, source, Some(fragments)).await
    }

    /// Shared bind path: registers the block route always and the fragment
    /// routes only when write-back handlers are wired.
    async fn bind_inner(
        addr: SocketAddr,
        secret: Option<String>,
        source: LocalBlockFn,
        fragments: Option<FragmentHandlers>,
    ) -> std::io::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let mut app = Router::new().route(BLOCK_PATH, post(serve_block));
        if fragments.is_some() {
            app = app
                .route(FRAGMENT_PUT_PATH, post(store_fragment))
                .route(FRAGMENT_GET_PATH, post(load_fragment))
                .route(FRAGMENT_DELETE_PATH, post(delete_fragment))
                .route(FRAGMENT_HEADROOM_PATH, post(fragment_headroom));
        }
        let app = app.with_state(ServerState {
            source,
            secret,
            fragments,
        });
        let task = tokio::spawn(async move {
            // A serving error tears down only the peer surface; the ladder
            // degrades to backend fills, so the server keeps serving.
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { task, local_addr })
    }

    /// The address the server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops serving. In-flight requests are dropped; a peer that was mid-fetch
    /// sees a transport error and degrades to a backend fill.
    pub async fn shutdown(self) {
        self.task.abort();
    }
}

/// Serves one block request: authenticate, then return the exact cached block
/// or a clean miss. Never fills from the backend — the `source` is cache-only.
///
/// Adopts the originating request id from the peer header (#61) for the whole
/// handler, so this owner's cache-lookup logs carry the id the client on the
/// requesting node saw — the cross-node correlation key.
async fn serve_block(
    State(state): State<ServerState>,
    headers: HeaderMap,
    body: AxumBytes,
) -> Response {
    let request_id = headers
        .get(verglas_core::trace::REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(verglas_core::trace::RequestId::from_wire);
    let work = serve_block_inner(state, headers, body);
    match request_id {
        Some(id) => verglas_core::trace::scope(id, work).await,
        None => work.await,
    }
}

/// The block-serve work, run under the originating request id when the caller
/// propagated one. Split from [`serve_block`] only so the trace scope wraps the
/// whole handler future.
async fn serve_block_inner(state: ServerState, headers: HeaderMap, body: AxumBytes) -> Response {
    // Auth first: a request without the configured secret gets bytes from no
    // one, security-group restrictions (#37) notwithstanding — defence in depth.
    if let Some(required) = &state.secret {
        let presented = headers.get(SECRET_HEADER).and_then(|v| v.to_str().ok());
        if presented != Some(required.as_str()) {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let Ok(request) = serde_json::from_slice::<BlockRequest>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let key = request.into_key();
    match (state.source)(key).await {
        // A hit: the exact block bytes, streamed as octets.
        Some(bytes) => {
            tracing::debug!(
                request_id = verglas_core::trace::current().as_ref().map(|r| r.as_str()),
                served = bytes.len(),
                "peer block served from cache"
            );
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                bytes,
            )
                .into_response()
        }
        // A clean miss: 204, no body. The caller falls through to a backend
        // fill; it is never an error.
        None => {
            tracing::debug!(
                request_id = verglas_core::trace::current().as_ref().map(|r| r.as_str()),
                "peer block miss (caller will backend-fill)"
            );
            StatusCode::NO_CONTENT.into_response()
        }
    }
}

/// Returns true when the request presents the secret the server requires (or
/// the server requires none).
fn authorized(state: &ServerState, headers: &HeaderMap) -> bool {
    let Some(required) = &state.secret else {
        return true;
    };
    headers
        .get(SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|presented| presented == required)
}

/// Stores one write-back fragment (#180): the body is the fragment bytes, the
/// object id and index travel in headers. The body streams into the local
/// store shard by shard, so the receiving node holds one stripe at a time, not
/// the whole fragment. `Ok` means the fragment is durable.
async fn store_fragment(
    State(state): State<ServerState>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(handlers) = &state.fragments else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let object_id = headers
        .get(FRAGMENT_OBJECT_HEADER)
        .and_then(|v| v.to_str().ok());
    let index = headers
        .get(FRAGMENT_INDEX_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());
    let (Some(object_id), Some(index)) = (object_id, index) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let key = FragmentKey {
        object_id: object_id.to_owned(),
        index,
    };
    // Turn the request body into a stream of `Bytes` chunks. A transport error
    // mid-stream ends the stream; the store then sees a short fragment and the
    // commit still fsyncs what arrived — but the coordinator only counts a `200`
    // as durable, so a truncated upload that errors the connection never acks.
    use futures::StreamExt;
    let shards = body
        .into_data_stream()
        .filter_map(|chunk| async move { chunk.ok() })
        .boxed();
    match (handlers.store_stream)(key, shards).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

/// Loads one write-back fragment (#180): 200 with bytes on a hit, 204 on a
/// clean miss.
async fn load_fragment(
    State(state): State<ServerState>,
    headers: HeaderMap,
    body: AxumBytes,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(handlers) = &state.fragments else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(request) = serde_json::from_slice::<FragmentRef>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let key = FragmentKey {
        object_id: request.object_id,
        index: request.index,
    };
    match (handlers.load)(key).await {
        Ok(Some(loaded)) => (
            StatusCode::OK,
            [
                (
                    axum::http::header::CONTENT_TYPE,
                    "application/octet-stream".to_owned(),
                ),
                (
                    axum::http::HeaderName::from_static(FRAGMENT_CHECKSUM_HEADER),
                    loaded.checksum.to_string(),
                ),
            ],
            loaded.bytes,
        )
            .into_response(),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

/// Deletes one write-back fragment (#180): 204 whether or not it was present.
async fn delete_fragment(
    State(state): State<ServerState>,
    headers: HeaderMap,
    body: AxumBytes,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(handlers) = &state.fragments else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(request) = serde_json::from_slice::<FragmentRef>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let key = FragmentKey {
        object_id: request.object_id,
        index: request.index,
    };
    match (handlers.delete)(key).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

/// Answers a fragment headroom check (#180): `200` with body `1` when this
/// node's fragment store can hold the requested bytes under its budget, `0`
/// when it is full. A bad request or a node without the tier is treated by the
/// caller as no headroom.
async fn fragment_headroom(
    State(state): State<ServerState>,
    headers: HeaderMap,
    _body: AxumBytes,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(handlers) = &state.fragments else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let bytes = headers
        .get(FRAGMENT_BYTES_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let Some(bytes) = bytes else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let answer = if (handlers.headroom)(bytes).await {
        "1"
    } else {
        "0"
    };
    (StatusCode::OK, answer).into_response()
}

/// The peer-fetch client: implements [`PeerFetch`] over HTTP/2 to a peer's
/// advertised address, with a tight connect/request timeout budget so a slow or
/// dead peer degrades to a backend fill fast.
pub struct PeerClient {
    /// Pooled, multiplexed HTTP/2 client shared across all peers.
    http: reqwest::Client,
    /// Resolves an owner node id to its advertised peer address.
    resolver: Arc<dyn PeerResolver>,
    /// The shared secret presented to peers, when the pod requires one.
    secret: Option<String>,
}

impl PeerClient {
    /// Builds a client that resolves owners through `resolver`, presents
    /// `secret` (when set), and holds `connect_timeout`/`request_timeout` as the
    /// hard budget for reaching a peer. Both timeouts are tight by design: the
    /// peer hop should be ~100–500µs, so a peer that has not answered in a few
    /// tens of milliseconds is treated as dead and the ladder falls to the
    /// backend (slow is acceptable; stuck is not).
    pub fn new(
        resolver: Arc<dyn PeerResolver>,
        secret: Option<String>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Self {
        let http = reqwest::Client::builder()
            // Prior-knowledge cleartext h2: the pod network is private, so we
            // skip the HTTP/1→2 upgrade dance and get multiplexing directly.
            .http2_prior_knowledge()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            // A misbuilt client would be a startup bug; fall back to a default
            // client rather than making construction fallible.
            .unwrap_or_default();
        Self {
            http,
            resolver,
            secret,
        }
    }

    /// A disabled client: an empty resolver, so every fetch is a clean miss.
    /// The single-node (`no [cluster]`) wiring — the ring owns every key
    /// locally, so this is never actually called.
    pub fn disabled() -> Self {
        Self::new(
            Arc::new(StaticResolver::new()),
            None,
            Duration::from_millis(5),
            Duration::from_millis(50),
        )
    }
}

impl PeerFetch for PeerClient {
    /// Fetches the exact block from `node`'s advertised address. An unknown or
    /// unadvertised owner is a clean miss (`Ok(None)`); a reachable owner that
    /// lacks the block answers `204` (also `Ok(None)`); a hit returns the bytes;
    /// anything else (timeout, refused connection, auth reject) is an `Err` the
    /// caller degrades from.
    fn fetch(
        &self,
        node: NodeId,
        block: &BlockKey,
    ) -> impl Future<Output = Result<Option<Bytes>, PeerFetchError>> + Send {
        let addr = self.resolver.resolve(&node);
        let request = BlockRequest::from_key(block);
        let http = self.http.clone();
        let secret = self.secret.clone();
        async move {
            // No address for the owner ⇒ nowhere to ask ⇒ clean miss.
            let Some(addr) = addr else {
                return Ok(None);
            };
            let url = format!("http://{addr}{BLOCK_PATH}");
            let mut builder = http.post(&url).json(&request);
            if let Some(secret) = &secret {
                builder = builder.header(SECRET_HEADER, secret);
            }
            // Carry the originating request id to the owner (#61), so a block
            // this node cold-fills on a peer's behalf logs under the id the
            // client saw — the cross-node correlation key.
            if let Some(request_id) = verglas_core::trace::current() {
                builder =
                    builder.header(verglas_core::trace::REQUEST_ID_HEADER, request_id.as_str());
            }
            let response = builder
                .send()
                .await
                .map_err(|e| PeerFetchError::Unavailable {
                    node: node.clone(),
                    reason: e.to_string(),
                })?;
            match response.status().as_u16() {
                200 => {
                    let bytes =
                        response
                            .bytes()
                            .await
                            .map_err(|e| PeerFetchError::Unavailable {
                                node: node.clone(),
                                reason: format!("reading peer body: {e}"),
                            })?;
                    Ok(Some(bytes))
                }
                // The owner does not have this exact block cached.
                204 => Ok(None),
                // A refused auth or any other status is a failure to answer, not
                // a miss: surface it so the caller falls back to the backend.
                other => Err(PeerFetchError::Unavailable {
                    node,
                    reason: format!("peer returned HTTP {other}"),
                }),
            }
        }
    }
}

/// A write-back fragment RPC failure (#180). Unlike a peer block fetch, a
/// fragment placement failure is not silently degraded: the write coordinator
/// counts it against quorum and falls back to write-through if quorum is lost.
#[derive(Debug, thiserror::Error)]
pub enum FragmentRpcError {
    /// The node advertises no peer address, so it cannot be reached.
    #[error("no peer endpoint for node {node}")]
    NoEndpoint {
        /// The unreachable node.
        node: NodeId,
    },
    /// The peer could not be reached, or returned a non-success status.
    #[error("fragment rpc to {node} failed: {reason}")]
    Unavailable {
        /// The node that failed to answer.
        node: NodeId,
        /// A human-readable reason.
        reason: String,
    },
}

/// The write-back fragment RPC client (#180): places, reads, and deletes
/// fragments on peer nodes over the same HTTP/2 stack and resolver as
/// [`PeerClient`]. It shares the resolver so it tracks live membership.
#[derive(Clone)]
pub struct FragmentClient {
    /// Pooled, multiplexed HTTP/2 client.
    http: reqwest::Client,
    /// Resolves a node id to its advertised peer address.
    resolver: Arc<dyn PeerResolver>,
    /// The shared secret presented to peers, when the pod requires one.
    secret: Option<String>,
}

impl FragmentClient {
    /// Builds a fragment client over `resolver`, presenting `secret` when set.
    /// The request timeout is looser than a block fetch's because a fragment
    /// carries a whole shard, not a cache block, and durability (fsync on the
    /// peer) is the ack — a fragment placement is worth waiting a little for.
    pub fn new(
        resolver: Arc<dyn PeerResolver>,
        secret: Option<String>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Self {
        let http = reqwest::Client::builder()
            .http2_prior_knowledge()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .unwrap_or_default();
        Self {
            http,
            resolver,
            secret,
        }
    }

    /// A disabled client (empty resolver) for the single-node wiring.
    pub fn disabled() -> Self {
        Self::new(
            Arc::new(StaticResolver::new()),
            None,
            Duration::from_millis(5),
            Duration::from_secs(30),
        )
    }

    /// Applies the shared secret header when the pod requires one.
    fn authenticated(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.secret {
            Some(secret) => builder.header(SECRET_HEADER, secret),
            None => builder,
        }
    }

    /// Resolves a node to its peer URL for `path`, or a [`FragmentRpcError`].
    fn url_for(&self, node: &NodeId, path: &str) -> Result<String, FragmentRpcError> {
        self.resolver
            .resolve(node)
            .map(|addr| format!("http://{addr}{path}"))
            .ok_or_else(|| FragmentRpcError::NoEndpoint { node: node.clone() })
    }

    /// Places one fragment on `node`, returning `Ok` only when the peer reports
    /// the fragment durable (HTTP 200).
    pub async fn put_fragment(
        &self,
        node: &NodeId,
        record: FragmentRecord,
    ) -> Result<(), FragmentRpcError> {
        let url = self.url_for(node, FRAGMENT_PUT_PATH)?;
        let builder = self
            .http
            .post(&url)
            .header(FRAGMENT_OBJECT_HEADER, &record.key.object_id)
            .header(FRAGMENT_INDEX_HEADER, record.key.index.to_string())
            .body(record.bytes);
        let response = self.authenticated(builder).send().await.map_err(|e| {
            FragmentRpcError::Unavailable {
                node: node.clone(),
                reason: e.to_string(),
            }
        })?;
        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let detail = response
                .text()
                .await
                .unwrap_or_else(|error| format!("unreadable response body: {error}"));
            let detail: String = detail.chars().take(512).collect();
            Err(FragmentRpcError::Unavailable {
                node: node.clone(),
                reason: format!("peer returned HTTP {}: {detail}", status.as_u16()),
            })
        }
    }

    /// Places one fragment on `node` by streaming its shards as the request
    /// body, so neither the sender nor the receiver buffers the whole fragment.
    /// `Ok` only when the peer reports the fragment durable (HTTP 200).
    pub async fn put_fragment_stream(
        &self,
        node: &NodeId,
        key: FragmentKey,
        shards: Pin<Box<dyn futures::Stream<Item = Bytes> + Send>>,
    ) -> Result<(), FragmentRpcError> {
        use futures::StreamExt;
        let url = self.url_for(node, FRAGMENT_PUT_PATH)?;
        let body = reqwest::Body::wrap_stream(shards.map(Ok::<Bytes, std::io::Error>));
        let builder = self
            .http
            .post(&url)
            .header(FRAGMENT_OBJECT_HEADER, &key.object_id)
            .header(FRAGMENT_INDEX_HEADER, key.index.to_string())
            .body(body);
        let response = self.authenticated(builder).send().await.map_err(|e| {
            FragmentRpcError::Unavailable {
                node: node.clone(),
                reason: e.to_string(),
            }
        })?;
        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let detail = response
                .text()
                .await
                .unwrap_or_else(|error| format!("unreadable response body: {error}"));
            let detail: String = detail.chars().take(512).collect();
            Err(FragmentRpcError::Unavailable {
                node: node.clone(),
                reason: format!("peer returned HTTP {}: {detail}", status.as_u16()),
            })
        }
    }

    /// Reads one fragment from `node`: `Some` on a hit, `None` on a clean miss
    /// (204), an error on any transport or non-success status. The returned
    /// [`LoadedFragment`] carries the checksum from the response header so the
    /// caller verifies the fragment end-to-end before reassembly (#220).
    pub async fn get_fragment(
        &self,
        node: &NodeId,
        key: &FragmentKey,
    ) -> Result<Option<LoadedFragment>, FragmentRpcError> {
        let url = self.url_for(node, FRAGMENT_GET_PATH)?;
        let request = FragmentRef {
            object_id: key.object_id.clone(),
            index: key.index,
        };
        let builder = self.http.post(&url).json(&request);
        let response = self.authenticated(builder).send().await.map_err(|e| {
            FragmentRpcError::Unavailable {
                node: node.clone(),
                reason: e.to_string(),
            }
        })?;
        match response.status().as_u16() {
            200 => {
                // A missing or unparseable checksum header parses as 0, which
                // fails verification at reassembly — corrupt-safe, never a
                // silent accept.
                let checksum = response
                    .headers()
                    .get(FRAGMENT_CHECKSUM_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u32>().ok())
                    .unwrap_or(0);
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|e| FragmentRpcError::Unavailable {
                        node: node.clone(),
                        reason: format!("reading fragment body: {e}"),
                    })?;
                Ok(Some(LoadedFragment { bytes, checksum }))
            }
            204 => Ok(None),
            other => Err(FragmentRpcError::Unavailable {
                node: node.clone(),
                reason: format!("peer returned HTTP {other}"),
            }),
        }
    }

    /// Asks `node` whether its fragment store can hold `bytes` under its budget.
    /// `Ok(true)` means placement may target it. Any transport failure or
    /// non-success status is an error the caller treats as no headroom.
    pub async fn headroom(&self, node: &NodeId, bytes: u64) -> Result<bool, FragmentRpcError> {
        let url = self.url_for(node, FRAGMENT_HEADROOM_PATH)?;
        let builder = self
            .http
            .post(&url)
            .header(FRAGMENT_BYTES_HEADER, bytes.to_string());
        let response = self.authenticated(builder).send().await.map_err(|e| {
            FragmentRpcError::Unavailable {
                node: node.clone(),
                reason: e.to_string(),
            }
        })?;
        if !response.status().is_success() {
            return Err(FragmentRpcError::Unavailable {
                node: node.clone(),
                reason: format!("peer returned HTTP {}", response.status().as_u16()),
            });
        }
        let body = response
            .bytes()
            .await
            .map_err(|e| FragmentRpcError::Unavailable {
                node: node.clone(),
                reason: format!("reading headroom body: {e}"),
            })?;
        Ok(body.first() == Some(&b'1'))
    }

    /// Deletes one fragment on `node` (idempotent on the peer side).
    pub async fn delete_fragment(
        &self,
        node: &NodeId,
        key: &FragmentKey,
    ) -> Result<(), FragmentRpcError> {
        let url = self.url_for(node, FRAGMENT_DELETE_PATH)?;
        let request = FragmentRef {
            object_id: key.object_id.clone(),
            index: key.index,
        };
        let builder = self.http.post(&url).json(&request);
        let response = self.authenticated(builder).send().await.map_err(|e| {
            FragmentRpcError::Unavailable {
                node: node.clone(),
                reason: e.to_string(),
            }
        })?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(FragmentRpcError::Unavailable {
                node: node.clone(),
                reason: format!("peer returned HTTP {}", response.status().as_u16()),
            })
        }
    }
}
