//! Integration tests for the peer-fetch RPC pair (#29): a [`PeerServer`]
//! serving locally-cached blocks and a [`PeerClient`] fetching them, mapped to
//! the issue's acceptance criteria.
//!
//! - a block a server has cached is served byte-identically to a client;
//! - a block the server does not have is a clean miss (`Ok(None)`), never an
//!   error — so the caller's miss ladder falls through to a backend fill;
//! - an ETag the server does not have (the object was overwritten) is a miss,
//!   never wrong bytes — the server matches the full `BlockKey`, ETag included;
//! - a wrong/absent cluster secret is rejected (auth), surfacing as an error
//!   the caller degrades from, not a silent wrong-bytes serve;
//! - a dead peer degrades to an error inside the tight timeout budget, so the
//!   ladder continues to the backend fast (slow is fine, stuck is not);
//! - an unknown node (no advertised address) is a clean miss.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use verglas_cluster::peer::{LocalBlockFn, PeerClient, PeerServer, StaticResolver};
use verglas_core::node::NodeId;
use verglas_core::peer::PeerFetch;
use verglas_core::{BlockKey, CacheKey};

/// Builds a `BlockKey` for `(bucket, key, etag, index)`.
fn block_key(bucket: &str, key: &str, etag: &str, index: u64) -> BlockKey {
    BlockKey {
        object: CacheKey {
            storage_binding_id: "default".to_owned(),
            bucket: bucket.to_owned(),
            key: key.to_owned(),
        },
        etag: etag.to_owned(),
        block_bytes: 2 * 1024 * 1024,
        block_index: index,
    }
}

/// A `LocalBlockFn` over a fixed in-memory map of `BlockKey -> bytes`,
/// standing in for a node's local cache tiers (cache-only, never a fill).
fn map_source(entries: Vec<(BlockKey, Bytes)>) -> LocalBlockFn {
    let map: Arc<HashMap<BlockKey, Bytes>> = Arc::new(entries.into_iter().collect());
    Arc::new(move |bk: BlockKey| {
        let map = Arc::clone(&map);
        Box::pin(async move { map.get(&bk).cloned() })
    })
}

/// Spins up a server over `entries` with `secret`, plus a client whose
/// resolver points `owner` at that server with the same secret.
async fn server_and_client(
    owner: &NodeId,
    secret: Option<String>,
    entries: Vec<(BlockKey, Bytes)>,
) -> (PeerServer, PeerClient) {
    let server = PeerServer::bind(
        "127.0.0.1:0".parse().expect("addr"),
        secret.clone(),
        map_source(entries),
    )
    .await
    .expect("bind peer server");
    let resolver = StaticResolver::with(owner.clone(), server.local_addr());
    let client = PeerClient::new(
        Arc::new(resolver),
        secret,
        Duration::from_millis(50),
        Duration::from_millis(500),
    );
    (server, client)
}

#[tokio::test]
async fn cached_block_is_served_byte_identically() {
    let owner = NodeId::new("owner");
    let bk = block_key("bkt", "obj", "etag-v1", 0);
    let payload = Bytes::from((0..4096u32).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
    let (server, client) = server_and_client(
        &owner,
        Some("s3cr3t".to_owned()),
        vec![(bk.clone(), payload.clone())],
    )
    .await;

    let got = client
        .fetch(owner.clone(), &bk)
        .await
        .expect("no transport error");
    assert_eq!(got, Some(payload), "peer must serve the exact cached bytes");

    server.shutdown().await;
}

#[tokio::test]
async fn missing_block_is_a_clean_miss_not_an_error() {
    let owner = NodeId::new("owner");
    let present = block_key("bkt", "obj", "etag-v1", 0);
    let absent = block_key("bkt", "obj", "etag-v1", 9);
    let (server, client) =
        server_and_client(&owner, None, vec![(present, Bytes::from_static(b"hi"))]).await;

    let got = client
        .fetch(owner.clone(), &absent)
        .await
        .expect("miss is not an error");
    assert_eq!(got, None, "a block the server lacks is a clean miss");

    server.shutdown().await;
}

#[tokio::test]
async fn stale_etag_never_serves_wrong_bytes() {
    // The server only holds v2 of the block; a request for the old v1 BlockKey
    // (a caller whose mapping is stale) must miss, never serve the v2 bytes.
    let owner = NodeId::new("owner");
    let v1 = block_key("bkt", "obj", "etag-v1", 0);
    let v2 = block_key("bkt", "obj", "etag-v2", 0);
    let (server, client) = server_and_client(
        &owner,
        None,
        vec![(v2.clone(), Bytes::from_static(b"the-new-version"))],
    )
    .await;

    let got = client
        .fetch(owner.clone(), &v1)
        .await
        .expect("miss is not an error");
    assert_eq!(
        got, None,
        "a stale ETag must miss, never serve a different version"
    );

    server.shutdown().await;
}

/// A peer configured with a different data-block geometry cannot satisfy a
/// request by index alone: it must miss rather than return bytes from another
/// offset in the same object.
#[tokio::test]
async fn divergent_block_geometry_is_a_clean_miss() {
    let owner = NodeId::new("owner");
    let cached = block_key("bkt", "obj", "etag-v1", 1);
    let (server, client) = server_and_client(
        &owner,
        Some("s3cr3t".to_owned()),
        vec![(cached.clone(), Bytes::from_static(b"cached-two-mib-block"))],
    )
    .await;
    let mut request = cached;
    request.block_bytes = 4 * 1024 * 1024;

    assert_eq!(
        client.fetch(owner, &request).await.expect("peer response"),
        None,
        "a geometry mismatch must degrade to the backend fill path"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn wrong_secret_is_rejected() {
    let owner = NodeId::new("owner");
    let bk = block_key("bkt", "obj", "etag-v1", 0);
    // Server requires a secret; client presents the wrong one.
    let server = PeerServer::bind(
        "127.0.0.1:0".parse().expect("addr"),
        Some("correct-secret".to_owned()),
        map_source(vec![(bk.clone(), Bytes::from_static(b"secret-bytes"))]),
    )
    .await
    .expect("bind");
    let client = PeerClient::new(
        Arc::new(StaticResolver::with(owner.clone(), server.local_addr())),
        Some("wrong-secret".to_owned()),
        Duration::from_millis(50),
        Duration::from_millis(500),
    );

    let result = client.fetch(owner.clone(), &bk).await;
    assert!(
        result.is_err(),
        "an unauthenticated peer must not serve bytes"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn dead_peer_degrades_within_the_timeout_budget() {
    let owner = NodeId::new("owner");
    let bk = block_key("bkt", "obj", "etag-v1", 0);
    // Resolve the owner to an address where nothing listens.
    let dead: SocketAddr = "127.0.0.1:1".parse().expect("addr");
    let client = PeerClient::new(
        Arc::new(StaticResolver::with(owner.clone(), dead)),
        None,
        Duration::from_millis(50),
        Duration::from_millis(200),
    );

    let start = Instant::now();
    let result = client.fetch(owner.clone(), &bk).await;
    let elapsed = start.elapsed();
    assert!(
        result.is_err(),
        "a dead peer is a transport error, not a hang"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "a dead peer must fail fast (was {elapsed:?})"
    );
}

#[tokio::test]
async fn unknown_node_is_a_clean_miss() {
    // The resolver knows nothing about this node, so there is nowhere to ask —
    // a clean miss, so the ladder falls through to a backend fill.
    let client = PeerClient::new(
        Arc::new(StaticResolver::new()),
        None,
        Duration::from_millis(50),
        Duration::from_millis(200),
    );
    let bk = block_key("bkt", "obj", "etag-v1", 0);
    let got = client
        .fetch(NodeId::new("ghost"), &bk)
        .await
        .expect("unknown node is not an error");
    assert_eq!(got, None);
}
