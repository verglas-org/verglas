//! Authenticated HTTP/2 transport for OpenRaft peer RPCs.
//!
//! It dispatches vote, append, and snapshot traffic only to registered local groups.

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use openraft::{
    BasicNode,
    error::{InstallSnapshotError, NetworkError, RPCError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, RwLock as StdRwLock},
};
use tokio::sync::RwLock;
use verglas_consensus::VerglasRaftConfig;

/// Authenticates internal consensus traffic.
const SECRET_HEADER: &str = "x-verglas-cluster-secret";
type Raft = openraft::Raft<VerglasRaftConfig>;
type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<u64, E>;
type Net<T, E = openraft::error::Infallible> = Result<T, RPCError<u64, BasicNode, RaftError<E>>>;
/// Installs one group locally before its first OpenRaft RPC arrives.
pub type OpenGroupFn =
    Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;
/// Initializes one already-open group from its deterministic bootstrap voter.
pub type BootstrapGroupFn = OpenGroupFn;
/// Routes one opaque universal group command to the local consensus runtime.
pub type GroupCommandFn = Arc<
    dyn Fn(String, Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>>
        + Send
        + Sync,
>;
/// Typed peer lifecycle request for one Raft voter replacement.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MembershipReplace {
    pub group: String,
    pub remove: u64,
    pub add: u64,
    pub node_id: String,
    pub address: SocketAddr,
}
/// Applies a leader-owned typed membership lifecycle request.
pub type MembershipReplaceFn = Arc<
    dyn Fn(MembershipReplace) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// Local Raft RPC registry and authenticated Axum endpoint factory.
#[derive(Clone)]
pub struct RaftRpcRegistry {
    groups: Arc<RwLock<BTreeMap<(String, u64), Raft>>>,
    targets: Arc<RwLock<BTreeSet<u64>>>,
    secret: Arc<str>,
    opener: Arc<RwLock<Option<OpenGroupFn>>>,
    bootstrapper: Arc<RwLock<Option<BootstrapGroupFn>>>,
    commander: Arc<RwLock<Option<GroupCommandFn>>>,
    membership_replacer: Arc<RwLock<Option<MembershipReplaceFn>>>,
}
impl RaftRpcRegistry {
    /// Creates an empty registry guarded by `secret`.
    #[must_use]
    pub fn new(secret: impl Into<String>) -> Self {
        Self {
            groups: Arc::new(RwLock::new(BTreeMap::new())),
            targets: Arc::new(RwLock::new(BTreeSet::new())),
            secret: Arc::from(secret.into()),
            opener: Arc::new(RwLock::new(None)),
            bootstrapper: Arc::new(RwLock::new(None)),
            commander: Arc::new(RwLock::new(None)),
            membership_replacer: Arc::new(RwLock::new(None)),
        }
    }
    /// Adds a locally served Raft voter.
    pub async fn register(&self, group: impl Into<String>, node_id: u64, raft: Raft) {
        self.register_target(node_id).await;
        self.groups
            .write()
            .await
            .insert((group.into(), node_id), raft);
    }
    /// Registers a local Raft voter identity before its durable groups reopen.
    ///
    /// Peer RPCs for an unregistered identity return `404` without invoking the
    /// group opener, so an authenticated peer cannot create state for another
    /// voter by choosing its numeric target.
    pub async fn register_target(&self, node_id: u64) {
        self.targets.write().await.insert(node_id);
    }
    /// Installs the process callback that creates persistent local group state.
    pub async fn set_opener(&self, opener: OpenGroupFn) {
        *self.opener.write().await = Some(opener);
    }
    /// Installs the callback allowed to initialize a fully provisioned group.
    pub async fn set_bootstrapper(&self, bootstrapper: BootstrapGroupFn) {
        *self.bootstrapper.write().await = Some(bootstrapper);
    }
    /// Installs the universal command callback shared by WAL and catalog groups.
    pub async fn set_commander(&self, commander: GroupCommandFn) {
        *self.commander.write().await = Some(commander);
    }
    /// Installs the leader-only membership lifecycle callback.
    pub async fn set_membership_replacer(&self, replacer: MembershipReplaceFn) {
        *self.membership_replacer.write().await = Some(replacer);
    }
    /// Returns the vote, append, and snapshot peer router.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/consensus/v1/{group}/{target}/append", post(append))
            .route("/consensus/v1/{group}/{target}/snapshot", post(snapshot))
            .route("/consensus/v1/{group}/{target}/vote", post(vote))
            .route("/consensus/v1/{group}/open", post(open_group))
            .route("/consensus/v1/{group}/bootstrap", post(bootstrap_group))
            .route("/consensus/v1/{group}/command", post(group_command))
            .route("/consensus/v1/membership/replace", post(membership_replace))
            .with_state(self.clone())
    }
    /// Authenticates, reopens, then resolves exactly one configured local voter.
    ///
    /// Reopening is idempotent and happens before the in-memory lookup so a
    /// node that retained durable Raft state across restart can receive normal
    /// replication traffic without a separate recovery control request.
    async fn target(
        &self,
        headers: &HeaderMap,
        group: &str,
        target: u64,
    ) -> Result<Raft, StatusCode> {
        if headers.get(SECRET_HEADER).and_then(|v| v.to_str().ok()) != Some(&self.secret) {
            return Err(StatusCode::FORBIDDEN);
        }
        if !self.targets.read().await.contains(&target) {
            return Err(StatusCode::NOT_FOUND);
        }
        let opener = self
            .opener
            .read()
            .await
            .clone()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        opener(group.to_owned())
            .await
            .map_err(|_| StatusCode::CONFLICT)?;
        self.groups
            .read()
            .await
            .get(&(group.to_owned(), target))
            .cloned()
            .ok_or(StatusCode::NOT_FOUND)
    }

    /// Checks only shared peer authentication for non-Raft control calls.
    fn authorize(&self, headers: &HeaderMap) -> Result<(), StatusCode> {
        if headers.get(SECRET_HEADER).and_then(|v| v.to_str().ok()) == Some(&self.secret) {
            Ok(())
        } else {
            Err(StatusCode::FORBIDDEN)
        }
    }
}

/// Routes a typed membership replacement only to the process lifecycle owner.
async fn membership_replace(
    State(registry): State<RaftRpcRegistry>,
    headers: HeaderMap,
    Json(request): Json<MembershipReplace>,
) -> Result<StatusCode, (StatusCode, String)> {
    registry
        .authorize(&headers)
        .map_err(|status| (status, "peer authentication failed".to_owned()))?;
    let callback = registry.membership_replacer.read().await.clone().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "membership runtime unavailable".to_owned(),
    ))?;
    callback(request)
        .await
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(|error| (StatusCode::CONFLICT, error))
}

/// Routes one authenticated application command without interpreting its body.
async fn group_command(
    State(registry): State<RaftRpcRegistry>,
    Path(group): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Vec<u8>, (StatusCode, String)> {
    registry
        .authorize(&headers)
        .map_err(|status| (status, "peer authentication failed".to_owned()))?;
    let commander = registry.commander.read().await.clone().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "group runtime unavailable".to_owned(),
    ))?;
    commander(group, body.to_vec())
        .await
        .map_err(|error| (StatusCode::CONFLICT, error))
}

/// Initializes one fully provisioned group on its designated first voter.
async fn bootstrap_group(
    State(registry): State<RaftRpcRegistry>,
    Path(group): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, String)> {
    registry
        .authorize(&headers)
        .map_err(|status| (status, "peer authentication failed".to_owned()))?;
    let bootstrapper = registry.bootstrapper.read().await.clone().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "group runtime unavailable".to_owned(),
    ))?;
    bootstrapper(group)
        .await
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(|error| (StatusCode::CONFLICT, error))
}

/// Creates a group's local persistent replica before election traffic begins.
async fn open_group(
    State(registry): State<RaftRpcRegistry>,
    Path(group): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, String)> {
    registry
        .authorize(&headers)
        .map_err(|status| (status, "peer authentication failed".to_owned()))?;
    let opener = registry.opener.read().await.clone().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "group runtime unavailable".to_owned(),
    ))?;
    opener(group)
        .await
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(|error| (StatusCode::CONFLICT, error))
}
/// Dispatches AppendEntries after header authentication.
async fn append(
    State(registry): State<RaftRpcRegistry>,
    Path((group, target)): Path<(String, u64)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<AppendEntriesResponse<u64>>, StatusCode> {
    let raft = registry.target(&headers, &group, target).await?;
    let request = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    raft.append_entries(request)
        .await
        .map(Json)
        .map_err(|_| StatusCode::CONFLICT)
}
/// Dispatches InstallSnapshot after header authentication.
async fn snapshot(
    State(registry): State<RaftRpcRegistry>,
    Path((group, target)): Path<(String, u64)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<InstallSnapshotResponse<u64>>, StatusCode> {
    let raft = registry.target(&headers, &group, target).await?;
    let request = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    raft.install_snapshot(request)
        .await
        .map(Json)
        .map_err(|_| StatusCode::CONFLICT)
}
/// Dispatches Vote after header authentication.
async fn vote(
    State(registry): State<RaftRpcRegistry>,
    Path((group, target)): Path<(String, u64)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<VoteResponse<u64>>, StatusCode> {
    let raft = registry.target(&headers, &group, target).await?;
    let request = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    raft.vote(request)
        .await
        .map(Json)
        .map_err(|_| StatusCode::CONFLICT)
}

/// Resolves numeric voter IDs to explicit consensus peer addresses.
pub trait RaftAddressResolver: Send + Sync + Clone + 'static {
    /// Finds a target address.
    fn resolve(&self, target: u64) -> Option<SocketAddr>;
}
/// Fixed address resolver for explicit configuration and tests.
#[derive(Clone, Default)]
pub struct StaticRaftAddressResolver {
    addresses: Arc<StdRwLock<BTreeMap<u64, SocketAddr>>>,
}
impl StaticRaftAddressResolver {
    /// Creates a resolver from an exact voter map.
    #[must_use]
    pub fn new(addresses: BTreeMap<u64, SocketAddr>) -> Self {
        Self {
            addresses: Arc::new(StdRwLock::new(addresses)),
        }
    }
    /// Registers the exact peer address announced for one prospective voter.
    ///
    /// This is called before learner replication begins; unknown peer identities
    /// remain a transport error rather than resolving through ordinary ring gossip.
    pub fn register(&self, target: u64, address: SocketAddr) -> Result<(), &'static str> {
        if target == 0 {
            return Err("raft voter id must not be zero");
        }
        let mut addresses = self
            .addresses
            .write()
            .map_err(|_| "raft address registry is poisoned")?;
        if let Some(existing) = addresses.get(&target) {
            if *existing != address {
                return Err("raft voter id is already bound to another address");
            }
            return Ok(());
        }
        addresses.insert(target, address);
        Ok(())
    }
}
impl RaftAddressResolver for StaticRaftAddressResolver {
    /// Looks up a configured voter.
    fn resolve(&self, target: u64) -> Option<SocketAddr> {
        self.addresses.read().ok()?.get(&target).copied()
    }
}

/// OpenRaft HTTP/2 network factory.
#[derive(Clone)]
pub struct RaftHttpTransport<R> {
    group: Arc<str>,
    resolver: R,
    client: Client,
    secret: Arc<str>,
}
impl<R: RaftAddressResolver> RaftHttpTransport<R> {
    /// Forwards one typed membership lifecycle request to the observed leader.
    pub async fn membership_replace(
        &self,
        target: u64,
        request: MembershipReplace,
    ) -> Result<(), NetworkError> {
        let address = self.resolver.resolve(target).ok_or_else(|| {
            NetworkError::new(&io::Error::new(
                io::ErrorKind::NotFound,
                "unknown raft voter",
            ))
        })?;
        let response = self
            .client
            .post(format!("http://{address}/consensus/v1/membership/replace"))
            .header(SECRET_HEADER, self.secret.as_ref())
            .json(&request)
            .send()
            .await
            .map_err(|error| NetworkError::new(&error))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(NetworkError::new(&io::Error::other(
                "raft leader refused membership lifecycle operation",
            )))
        }
    }
    /// Builds pooled prior-knowledge HTTP/2 clients for internal peer traffic.
    pub fn new(
        group: impl Into<String>,
        resolver: R,
        secret: impl Into<String>,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            group: Arc::from(group.into()),
            resolver,
            client: Client::builder().http2_prior_knowledge().build()?,
            secret: Arc::from(secret.into()),
        })
    }

    /// Asks one configured voter to create this group before Raft initialization.
    pub async fn open_group(&self, target: u64) -> Result<(), NetworkError> {
        self.control_call(target, "open").await
    }

    /// Asks the deterministic first voter to initialize a provisioned group.
    pub async fn bootstrap_group(&self, target: u64) -> Result<(), NetworkError> {
        self.control_call(target, "bootstrap").await
    }

    /// Routes one serialized universal command to a specific group leader.
    pub async fn command(&self, target: u64, body: Vec<u8>) -> Result<Vec<u8>, NetworkError> {
        let address = self.resolver.resolve(target).ok_or_else(|| {
            NetworkError::new(&io::Error::new(
                io::ErrorKind::NotFound,
                "unknown raft voter",
            ))
        })?;
        let response = self
            .client
            .post(format!(
                "http://{address}/consensus/v1/{}/command",
                percent_encoding::utf8_percent_encode(
                    &self.group,
                    percent_encoding::NON_ALPHANUMERIC
                )
            ))
            .header(SECRET_HEADER, self.secret.as_ref())
            .body(body)
            .send()
            .await
            .map_err(|error| NetworkError::new(&error))?;
        if !response.status().is_success() {
            return Err(NetworkError::new(&io::Error::other(
                "group leader refused application command",
            )));
        }
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| NetworkError::new(&error))
    }

    /// Sends one authenticated group lifecycle operation.
    async fn control_call(&self, target: u64, operation: &str) -> Result<(), NetworkError> {
        let address = self.resolver.resolve(target).ok_or_else(|| {
            NetworkError::new(&io::Error::new(
                io::ErrorKind::NotFound,
                "unknown raft voter",
            ))
        })?;
        let response = self
            .client
            .post(format!(
                "http://{address}/consensus/v1/{}/{operation}",
                percent_encoding::utf8_percent_encode(
                    &self.group,
                    percent_encoding::NON_ALPHANUMERIC
                )
            ))
            .header(SECRET_HEADER, self.secret.as_ref())
            .send()
            .await
            .map_err(|error| NetworkError::new(&error))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(NetworkError::new(&io::Error::other(
                "raft peer refused group lifecycle operation",
            )))
        }
    }
}
impl<R: RaftAddressResolver> RaftNetworkFactory<VerglasRaftConfig> for RaftHttpTransport<R> {
    type Network = RaftHttpNetwork<R>;
    /// Opens a logical connection to a numeric voter.
    async fn new_client(&mut self, target: u64, _: &BasicNode) -> Self::Network {
        RaftHttpNetwork {
            target,
            group: self.group.clone(),
            resolver: self.resolver.clone(),
            client: self.client.clone(),
            secret: self.secret.clone(),
        }
    }
}
/// A logical HTTP connection to one OpenRaft voter.
pub struct RaftHttpNetwork<R> {
    target: u64,
    group: Arc<str>,
    resolver: R,
    client: Client,
    secret: Arc<str>,
}
impl<R: RaftAddressResolver> RaftHttpNetwork<R> {
    /// Sends one JSON RPC and fail-closes transport and HTTP failures as network errors.
    async fn call<Req: serde::Serialize + ?Sized, Resp: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        request: &Req,
    ) -> Result<Resp, NetworkError> {
        let address = self.resolver.resolve(self.target).ok_or_else(|| {
            NetworkError::new(&io::Error::new(
                io::ErrorKind::NotFound,
                "unknown raft voter",
            ))
        })?;
        let response = self
            .client
            .post(format!(
                "http://{address}/consensus/v1/{}/{}/{method}",
                percent_encoding::utf8_percent_encode(
                    &self.group,
                    percent_encoding::NON_ALPHANUMERIC
                ),
                self.target
            ))
            .header(SECRET_HEADER, self.secret.as_ref())
            .json(request)
            .send()
            .await
            .map_err(|e| NetworkError::new(&e))?;
        if !response.status().is_success() {
            return Err(NetworkError::new(&io::Error::other(
                "raft peer rejected RPC",
            )));
        }
        response.json().await.map_err(|e| NetworkError::new(&e))
    }
}
impl<R: RaftAddressResolver> RaftNetwork<VerglasRaftConfig> for RaftHttpNetwork<R> {
    /// Sends AppendEntries.
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<VerglasRaftConfig>,
        _: RPCOption,
    ) -> Net<AppendEntriesResponse<u64>> {
        self.call("append", &request)
            .await
            .map_err(RPCError::Network)
    }
    /// Sends InstallSnapshot.
    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<VerglasRaftConfig>,
        _: RPCOption,
    ) -> Net<InstallSnapshotResponse<u64>, InstallSnapshotError> {
        self.call("snapshot", &request)
            .await
            .map_err(RPCError::Network)
    }
    /// Sends Vote.
    async fn vote(&mut self, request: VoteRequest<u64>, _: RPCOption) -> Net<VoteResponse<u64>> {
        self.call("vote", &request).await.map_err(RPCError::Network)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn authenticated_membership_transport_reaches_registered_callback() {
        let registry = RaftRpcRegistry::new("secret");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        registry
            .set_membership_replacer(Arc::new(move |request| {
                let observed = Arc::clone(&observed);
                Box::pin(async move {
                    if request.group != "timeline/a" || request.remove != 1 || request.add != 2 {
                        return Err("wrong lifecycle request".to_owned());
                    }
                    observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(())
                })
            }))
            .await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            axum::serve(listener, registry.router())
                .await
                .expect("server");
        });
        let transport = RaftHttpTransport::new(
            "timeline/a",
            StaticRaftAddressResolver::new(BTreeMap::from([(7, address)])),
            "secret",
        )
        .expect("transport");
        transport
            .membership_replace(
                7,
                MembershipReplace {
                    group: "timeline/a".to_owned(),
                    remove: 1,
                    add: 2,
                    node_id: "node-b".to_owned(),
                    address,
                },
            )
            .await
            .expect("forwarded lifecycle request");
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        server.abort();
    }
}
