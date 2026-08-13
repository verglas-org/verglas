//! Dedicated Neon wire keeper. Cache nodes retain only private fragment RPC and
//! read service; this workload owns the long-lived PostgreSQL WAL connections.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use verglas_backend::{BackendStore, BackendStores};
use verglas_cluster::{
    FragmentClient, FragmentKey, FragmentRecord, LoadedFragment, peer::PeerResolver,
};
use verglas_core::config::Config;
use verglas_core::node::NodeId;
use verglas_s3::PassthroughOrigin;
use verglas_safekeeper::{
    AppendGeometry, FragmentTransport, LiveMembership, SafekeeperServer, TransportError,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const BINDING: &str = "managed-lakehouse";

struct Resolver(HashMap<NodeId, SocketAddr>);
impl PeerResolver for Resolver {
    fn resolve(&self, node: &NodeId) -> Option<SocketAddr> {
        self.0.get(node).copied()
    }
}

struct Membership {
    nodes: Vec<NodeId>,
}
impl LiveMembership for Membership {
    fn self_id(&self) -> NodeId {
        NodeId::new("ec-keeper")
    }
    fn live_nodes(&self) -> Vec<NodeId> {
        self.nodes.clone()
    }
    fn epoch(&self) -> u64 {
        1
    }
}

/// Keeper-only peer client. It deliberately has no local fragment store: the
/// membership contains only cache nodes and every durable shard RPC is remote.
struct RemoteFragmentTransport {
    client: FragmentClient,
}

#[async_trait::async_trait]
impl FragmentTransport for RemoteFragmentTransport {
    async fn has_headroom(&self, node: &NodeId, bytes: u64) -> bool {
        self.client.headroom(node, bytes).await.unwrap_or(false)
    }
    async fn place(&self, node: &NodeId, record: FragmentRecord) -> Result<(), TransportError> {
        self.client
            .put_fragment(node, record)
            .await
            .map_err(Into::into)
    }
    async fn place_stream(
        &self,
        node: &NodeId,
        key: FragmentKey,
        shards: verglas_write::transport::ShardStream,
    ) -> Result<(), TransportError> {
        self.client
            .put_fragment_stream(node, key, shards)
            .await
            .map_err(Into::into)
    }
    async fn load(
        &self,
        node: &NodeId,
        key: &FragmentKey,
    ) -> Result<Option<LoadedFragment>, TransportError> {
        self.client
            .get_fragment(node, key)
            .await
            .map_err(Into::into)
    }
    async fn delete(&self, node: &NodeId, key: &FragmentKey) -> Result<(), TransportError> {
        self.client
            .delete_fragment(node, key)
            .await
            .map_err(Into::into)
    }
    async fn list_prefix(
        &self,
        node: &NodeId,
        prefix: &str,
    ) -> Result<Vec<FragmentKey>, TransportError> {
        self.client
            .list_fragments(node, prefix)
            .await
            .map_err(Into::into)
    }
}

async fn peers() -> Result<Vec<(NodeId, SocketAddr)>, String> {
    let raw = std::env::var("VERGLAS_RING_PEERS")
        .map_err(|_| "VERGLAS_RING_PEERS is required".to_owned())?;
    let mut parsed = Vec::new();
    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (id, address) = entry
            .split_once('=')
            .ok_or_else(|| format!("invalid ring peer `{entry}` (want id=host:port)"))?;
        let address = match address.parse() {
            Ok(address) => address,
            Err(_) => tokio::net::lookup_host(address)
                .await
                .map_err(|error| format!("resolve ring peer `{entry}`: {error}"))?
                .next()
                .ok_or_else(|| format!("ring peer `{entry}` resolved to no addresses"))?,
        };
        parsed.push((NodeId::new(id.trim()), address));
    }
    if parsed.is_empty() {
        return Err("VERGLAS_RING_PEERS is empty".to_owned());
    }
    Ok(parsed)
}

fn geometry(member_count: usize) -> Result<AppendGeometry, String> {
    let field = |name| {
        std::env::var(name)
            .map_err(|_| format!("{name} is required"))?
            .parse::<usize>()
            .map_err(|_| format!("{name} must be an unsigned integer"))
    };
    let geometry = AppendGeometry::new(
        field("VERGLAS_EC_KEEPER_EC_K")?,
        field("VERGLAS_EC_KEEPER_EC_M")?,
        field("VERGLAS_EC_KEEPER_EC_W")?,
    )
    .map_err(|error| error.to_string())?;
    if geometry.total() > member_count || geometry.w > member_count {
        return Err(format!(
            "EC geometry needs {} fragment holders (ack {}) but VERGLAS_RING_PEERS has {member_count}",
            geometry.total(),
            geometry.w
        ));
    }
    Ok(geometry)
}

#[tokio::main]
async fn main() {
    if std::env::args().any(|arg| arg == "--version" || arg == "-V") {
        println!("verglas-ec-keeper {VERSION}");
        return;
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let config_path = match args.as_slice() {
        [flag, path] if flag == "--config" => path,
        _ => {
            eprintln!("verglas-ec-keeper: --config <path> is required");
            std::process::exit(1);
        }
    };
    let config = match Config::load(std::path::Path::new(config_path)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("verglas-ec-keeper: {error}");
            std::process::exit(1);
        }
    };
    let Some(bucket) = config.backend.bucket.clone() else {
        eprintln!("verglas-ec-keeper: backend.bucket is required");
        std::process::exit(1);
    };
    let peers = match peers().await {
        Ok(peers) => peers,
        Err(error) => {
            eprintln!("verglas-ec-keeper: {error}");
            std::process::exit(1);
        }
    };
    let secret = std::env::var("VERGLAS_CLUSTER_SECRET")
        .ok()
        .filter(|value| !value.is_empty());
    let resolver: Arc<dyn PeerResolver> = Arc::new(Resolver(peers.iter().cloned().collect()));
    let client = FragmentClient::new(
        resolver,
        secret,
        Duration::from_millis(500),
        Duration::from_secs(30),
    );
    let state = std::env::var("VERGLAS_EC_KEEPER_STATE")
        .unwrap_or_else(|_| "/var/lib/verglas-ec-keeper".to_owned());
    let transport = Arc::new(RemoteFragmentTransport { client });
    let membership: Arc<dyn LiveMembership> = Arc::new(Membership {
        nodes: peers.iter().map(|(id, _)| id.clone()).collect(),
    });
    let stores = BackendStore::from_config(BINDING, &config.backend);
    let listen: SocketAddr = std::env::var("VERGLAS_EC_KEEPER_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:5454".to_owned())
        .parse()
        .expect("valid VERGLAS_EC_KEEPER_ADDR");
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .expect("bind EC keeper listener");
    let layout = match geometry(peers.len()) {
        Ok(layout) => layout,
        Err(error) => {
            eprintln!("verglas-ec-keeper: {error}");
            std::process::exit(1);
        }
    };
    let origin: Arc<dyn BackendStores> = stores;
    let mut server = SafekeeperServer::new(
        0xec_0001,
        Arc::new(PassthroughOrigin::new(origin)),
        BINDING,
        bucket,
        "neon-wal",
        transport,
        membership,
        std::path::Path::new(&state).join("timelines"),
        layout,
    );
    if let Ok(endpoint) = std::env::var("VERGLAS_EC_KEEPER_BROKER_ENDPOINT") {
        let advertise = std::env::var("VERGLAS_EC_KEEPER_ADVERTISE_ADDR")
            .expect("VERGLAS_EC_KEEPER_ADVERTISE_ADDR is required with broker endpoint");
        server = server.with_broker(endpoint, advertise);
    }
    eprintln!(
        "verglas-ec-keeper {VERSION} listening on {} (EC k={}, m={}, ack quorum={})",
        listener.local_addr().expect("listener addr"),
        layout.k,
        layout.m,
        layout.w
    );
    if let Err(error) = server.serve(listener).await {
        eprintln!("verglas-ec-keeper failed: {error}");
        std::process::exit(1);
    }
}
