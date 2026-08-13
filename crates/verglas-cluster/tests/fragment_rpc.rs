//! Integration tests for the write-back fragment RPC (#180): a [`PeerServer`]
//! bound with fragment handlers over a [`LocalFragmentStore`], and a
//! [`FragmentClient`] placing, reading, and deleting fragments.
//!
//! - a fragment placed on a peer reads back byte-identically;
//! - a fragment the peer lacks is a clean miss (`Ok(None)`);
//! - delete is idempotent and removes the fragment;
//! - a wrong cluster secret is rejected;
//! - an unreachable node is a placement error the coordinator counts against
//!   quorum (never a silent success).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use bytes::Bytes;
use verglas_cluster::fragments::{FragmentKey, FragmentRecord, LocalFragmentStore};
use verglas_cluster::peer::{
    FragmentClient, FragmentHandlers, LocalBlockFn, LocalBlockStoreFn, PeerServer,
};
use verglas_cluster::{OpenGroupFn, RaftRpcRegistry, StaticResolver};
use verglas_core::node::NodeId;

/// A block source that has nothing (fragment tests do not exercise blocks).
fn empty_blocks() -> LocalBlockFn {
    Arc::new(|_bk| Box::pin(async move { None }))
}

/// A block-placement callback that refuses every request for tests that only
/// exercise another internal peer route.
fn reject_block_placement() -> LocalBlockStoreFn {
    Arc::new(|_key, _bytes| Box::pin(async move { Ok(false) }))
}

/// Wires a `LocalFragmentStore` behind the fragment handler callbacks.
fn handlers_for(store: LocalFragmentStore) -> FragmentHandlers {
    let s1 = store.clone();
    let s1b = store.clone();
    let s2 = store.clone();
    let s3 = store.clone();
    let s4 = store.clone();
    let s5 = store;
    FragmentHandlers {
        store: Arc::new(move |record| {
            let s = s1.clone();
            Box::pin(async move { s.store_fragment(&record) })
        }),
        store_stream: Arc::new(move |key, mut shards| {
            let s = s1b.clone();
            Box::pin(async move {
                use futures::StreamExt;
                let mut writer = s.open_fragment(&key)?;
                while let Some(shard) = shards.next().await {
                    writer.append(&shard)?;
                }
                writer.commit()
            })
        }),
        load: Arc::new(move |key| {
            let s = s2.clone();
            Box::pin(async move { s.load_fragment(&key) })
        }),
        delete: Arc::new(move |key| {
            let s = s3.clone();
            Box::pin(async move { s.delete_fragment(&key) })
        }),
        headroom: Arc::new(move |bytes| {
            let s = s4.clone();
            Box::pin(async move { s.has_headroom(bytes) })
        }),
        list_prefix: Arc::new(move |prefix| {
            let s = s5.clone();
            Box::pin(async move {
                s.list_fragment_keys()
                    .into_iter()
                    .filter(|key| key.object_id.starts_with(&prefix))
                    .collect()
            })
        }),
    }
}

/// Wires fragment handlers whose read never answers, simulating a peer that
/// accepts a connection but has stopped making recovery progress.
fn handlers_with_hanging_load(store: LocalFragmentStore) -> FragmentHandlers {
    let mut handlers = handlers_for(store);
    handlers.load = Arc::new(|_key| Box::pin(std::future::pending()));
    handlers
}

/// Binds a fragment server over a fresh store, returning the server, a client
/// pointed at it as `node`, and the backing store.
async fn server_and_client(
    node: &NodeId,
    secret: Option<String>,
) -> (PeerServer, FragmentClient, LocalFragmentStore) {
    let dir = std::env::temp_dir().join(format!(
        "verglas-fragrpc-{}-{}",
        std::process::id(),
        node.as_str()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let store = LocalFragmentStore::new(&dir);
    let server = PeerServer::bind_with_fragments(
        "127.0.0.1:0".parse().expect("addr"),
        secret.clone(),
        empty_blocks(),
        handlers_for(store.clone()),
    )
    .await
    .expect("bind fragment server");
    let resolver = StaticResolver::with(node.clone(), server.local_addr());
    let client = FragmentClient::new(
        Arc::new(resolver),
        secret,
        Duration::from_millis(50),
        Duration::from_secs(2),
    );
    (server, client, store)
}

#[tokio::test]
async fn placed_fragment_reads_back_byte_identical() {
    let node = NodeId::new("peer-0");
    let (server, client, _store) = server_and_client(&node, Some("s3cr3t".to_owned())).await;
    let key = FragmentKey {
        object_id: "obj-1".to_owned(),
        index: 2,
    };
    let payload = Bytes::from((0..8192u32).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
    let record = FragmentRecord::new(key.clone(), payload.clone());
    client
        .put_fragment(&node, record.clone())
        .await
        .expect("place fragment");

    let got = client
        .get_fragment(&node, &key)
        .await
        .expect("read")
        .expect("present");
    assert_eq!(
        got.bytes, payload,
        "peer must serve the exact fragment bytes"
    );
    // The checksum survives the RPC round-trip, so the reader verifies
    // end-to-end (#220).
    assert_eq!(got.checksum, record.checksum);
    assert!(got.is_healthy(), "round-tripped fragment verifies");
    server.shutdown().await;
}

#[tokio::test]
async fn missing_fragment_is_a_clean_miss() {
    let node = NodeId::new("peer-1");
    let (server, client, _store) = server_and_client(&node, None).await;
    let got = client
        .get_fragment(
            &node,
            &FragmentKey {
                object_id: "absent".to_owned(),
                index: 0,
            },
        )
        .await
        .expect("miss is not an error");
    assert_eq!(got, None);
    server.shutdown().await;
}

#[tokio::test]
async fn delete_removes_the_fragment() {
    let node = NodeId::new("peer-2");
    let (server, client, _store) = server_and_client(&node, None).await;
    let key = FragmentKey {
        object_id: "obj".to_owned(),
        index: 1,
    };
    client
        .put_fragment(
            &node,
            FragmentRecord::new(key.clone(), Bytes::from_static(b"bytes")),
        )
        .await
        .expect("place");
    client.delete_fragment(&node, &key).await.expect("delete");
    assert_eq!(
        client.get_fragment(&node, &key).await.expect("read"),
        None,
        "fragment must be gone after delete"
    );
    // A second delete is still success (idempotent).
    client
        .delete_fragment(&node, &key)
        .await
        .expect("delete again");
    server.shutdown().await;
}

#[tokio::test]
async fn wrong_secret_is_rejected() {
    let node = NodeId::new("peer-3");
    let (server, _good, _store) = server_and_client(&node, Some("correct".to_owned())).await;
    let bad = FragmentClient::new(
        Arc::new(StaticResolver::with(node.clone(), server.local_addr())),
        Some("wrong".to_owned()),
        Duration::from_millis(50),
        Duration::from_secs(2),
    );
    let result = bad
        .put_fragment(
            &node,
            FragmentRecord::new(
                FragmentKey {
                    object_id: "obj".to_owned(),
                    index: 0,
                },
                Bytes::from_static(b"x"),
            ),
        )
        .await;
    assert!(result.is_err(), "a wrong secret must be rejected");
    server.shutdown().await;
}

#[tokio::test]
async fn unreachable_node_is_a_placement_error() {
    // No resolver entry: nowhere to place, so the coordinator gets an error it
    // counts against quorum, never a silent success.
    let client = FragmentClient::new(
        Arc::new(StaticResolver::new()),
        None,
        Duration::from_millis(50),
        Duration::from_millis(500),
    );
    let result = client
        .put_fragment(
            &NodeId::new("ghost"),
            FragmentRecord::new(
                FragmentKey {
                    object_id: "obj".to_owned(),
                    index: 0,
                },
                Bytes::from_static(b"x"),
            ),
        )
        .await;
    assert!(result.is_err(), "an unreachable node is a placement error");
}

/// Recovery reads must not inherit the long durability-placement deadline.
#[tokio::test]
async fn fragment_read_times_out_before_the_durability_placement_budget() {
    let node = NodeId::new("blackholed-peer");
    let directory =
        std::env::temp_dir().join(format!("verglas-fragrpc-blackhole-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    let server = PeerServer::bind_with_fragments(
        "127.0.0.1:0".parse().expect("addr"),
        None,
        empty_blocks(),
        handlers_with_hanging_load(LocalFragmentStore::new(&directory)),
    )
    .await
    .expect("bind blackholed fragment server");
    let client = FragmentClient::new(
        Arc::new(StaticResolver::with(node.clone(), server.local_addr())),
        None,
        Duration::from_millis(500),
        Duration::from_secs(30),
    );

    let result = tokio::time::timeout(
        Duration::from_millis(300),
        client.get_fragment(
            &node,
            &FragmentKey {
                object_id: "retained-catalog-header".to_owned(),
                index: 0,
            },
        ),
    )
    .await;
    assert!(
        matches!(result, Ok(Err(_))),
        "a stalled recovery read must fail within its short availability budget: {result:?}"
    );
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(directory);
}

/// Raft vote RPCs use the peer runtime, not the public S3/WAL/NBD runtime.
///
/// The two public runtime workers block at the same time. A real authenticated
/// vote route still reaches its group opener and returns its normal `404` for
/// the deliberately unregistered group before either public worker wakes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_vote_remains_responsive_when_public_runtime_is_saturated() {
    let registry = RaftRpcRegistry::new("peer-secret");
    registry.register_target(42).await;
    let opened = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&opened);
    let opener: OpenGroupFn = Arc::new(move |_group| {
        let observed = Arc::clone(&observed);
        Box::pin(async move {
            observed.store(true, Ordering::Release);
            Ok(())
        })
    });
    registry.set_opener(opener).await;

    let directory = std::env::temp_dir().join(format!(
        "verglas-peer-runtime-isolation-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    let server = PeerServer::bind_with_store_fragments_and_router(
        "127.0.0.1:0".parse().expect("addr"),
        Some("peer-secret".to_owned()),
        empty_blocks(),
        reject_block_placement(),
        handlers_for(LocalFragmentStore::new(&directory)),
        registry.router(),
    )
    .await
    .expect("bind peer runtime");

    let release = Arc::new(Barrier::new(3));
    let blockers = (0..2)
        .map(|_| {
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                release.wait();
                std::thread::sleep(Duration::from_millis(300));
            })
        })
        .collect::<Vec<_>>();
    let address = server.local_addr();
    let client_release = Arc::clone(&release);
    let client = std::thread::spawn(move || -> std::io::Result<(Duration, String)> {
        client_release.wait();
        let started = Instant::now();
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(1))?;
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        stream.write_all(
            format!(
                "POST /consensus/v1/runtime-isolation/42/vote HTTP/1.1\r\nHost: {address}\r\nx-verglas-cluster-secret: peer-secret\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok((started.elapsed(), response))
    });
    let response = tokio::task::spawn_blocking(move || client.join())
        .await
        .expect("join request thread")
        .expect("request thread did not panic")
        .expect("peer vote request");

    assert!(
        response.0 < Duration::from_millis(200),
        "vote response waited for the public runtime: {:?}",
        response.0
    );
    assert!(
        response.1.starts_with("HTTP/1.1 404"),
        "registered target with no opened Raft group returns 404: {}",
        response.1
    );
    assert!(
        opened.load(Ordering::Acquire),
        "the vote route must run its registered group opener"
    );
    for blocker in blockers {
        blocker.await.expect("public runtime blocker");
    }
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(directory);
}
