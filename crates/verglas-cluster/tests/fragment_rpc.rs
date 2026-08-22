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
use std::net::{Shutdown, TcpStream};
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
                    let shard = shard?;
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

/// Wires fragment handlers whose read is slower than a cache-block lookup but
/// still comfortably within the peer request budget used for durable shards.
fn handlers_with_delayed_load(store: LocalFragmentStore) -> FragmentHandlers {
    let mut handlers = handlers_for(store.clone());
    handlers.load = Arc::new(move |key| {
        let store = store.clone();
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            store.load_fragment(&key)
        })
    });
    handlers
}

/// Wires fragment handlers whose store path blocks the OS thread it runs on
/// for a fixed duration before committing, standing in for the aggregate
/// synchronous cost (checksum, framing, small syscalls) a real fragment write
/// pays on the peer runtime. Unlike [`handlers_with_delayed_load`]'s async
/// sleep, this genuinely occupies a worker thread — exactly like the real
/// work it represents — so it exercises the peer server's worker-thread
/// ceiling instead of just adding latency that every worker shares for free.
fn handlers_with_blocking_store(store: LocalFragmentStore) -> FragmentHandlers {
    let mut handlers = handlers_for(store.clone());
    handlers.store_stream = Arc::new(move |key, mut shards| {
        let store = store.clone();
        Box::pin(async move {
            use futures::StreamExt;
            let mut writer = store.open_fragment(&key)?;
            while let Some(shard) = shards.next().await {
                let shard = shard?;
                writer.append(&shard)?;
            }
            std::thread::sleep(Duration::from_millis(60));
            writer.commit()
        })
    });
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
        Duration::from_secs(6),
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
        "a stalled recovery read must fail before the durability placement budget: {result:?}"
    );
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(directory);
}

/// Durable fragments are much larger than cache blocks and can take hundreds
/// of milliseconds to stream from a CPU-throttled peer. Such a healthy peer
/// must not be mistaken for a missing fragment during reconstruction.
#[tokio::test]
async fn fragment_read_allows_a_healthy_slow_peer() {
    let node = NodeId::new("slow-healthy-peer");
    let directory =
        std::env::temp_dir().join(format!("verglas-fragrpc-slow-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    let store = LocalFragmentStore::new(&directory);
    let key = FragmentKey {
        object_id: "large-durable-shard".to_owned(),
        index: 1,
    };
    let payload = Bytes::from_static(b"durable-fragment");
    store
        .store_fragment(&FragmentRecord::new(key.clone(), payload.clone()))
        .expect("seed fragment");
    let server = PeerServer::bind_with_fragments(
        "127.0.0.1:0".parse().expect("addr"),
        None,
        empty_blocks(),
        handlers_with_delayed_load(store),
    )
    .await
    .expect("bind slow peer");
    let client = FragmentClient::new(
        Arc::new(StaticResolver::with(node.clone(), server.local_addr())),
        None,
        Duration::from_millis(500),
        Duration::from_secs(30),
    );

    let loaded = client
        .get_fragment(&node, &key)
        .await
        .expect("healthy slow peer must remain readable")
        .expect("fragment exists");
    assert_eq!(loaded.bytes, payload);
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(directory);
}

/// A disconnected fragment sender is an upload failure, never a clean end of
/// stream. The peer runtime must discard the partial file and keep a live vote
/// route responsive after the cancelled upload is reaped.
#[tokio::test]
async fn cancelled_fragment_upload_never_commits_and_does_not_block_a_vote() {
    let registry = RaftRpcRegistry::new("peer-secret");
    registry.register_target(42).await;
    registry
        .set_opener(Arc::new(|_group| Box::pin(async move { Ok(()) })))
        .await;
    let directory = std::env::temp_dir().join(format!(
        "verglas-fragrpc-cancelled-upload-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    let store = LocalFragmentStore::new(&directory);
    let server = PeerServer::bind_with_store_fragments_and_router(
        "127.0.0.1:0".parse().expect("addr"),
        Some("peer-secret".to_owned()),
        empty_blocks(),
        reject_block_placement(),
        handlers_for(store.clone()),
        registry.router(),
    )
    .await
    .expect("bind peer runtime");
    let address = server.local_addr();
    let upload_response = tokio::task::spawn_blocking(move || -> std::io::Result<String> {
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(1))?;
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        stream.write_all(
            format!(
                "POST /peer/v0/fragment HTTP/1.1\r\nHost: {address}\r\nx-verglas-cluster-secret: peer-secret\r\nx-verglas-fragment-object: cancelled-upload\r\nx-verglas-fragment-index: 0\r\nContent-Length: 8\r\nConnection: close\r\n\r\ncut"
            )
            .as_bytes(),
        )?;
        stream.shutdown(Shutdown::Write)?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    })
    .await
    .expect("join upload task")
    .expect("send partial upload");
    assert!(
        upload_response.starts_with("HTTP/1.1 500"),
        "cancelled upload must fail instead of acknowledging a truncated fragment: {upload_response}"
    );
    let key = FragmentKey {
        object_id: "cancelled-upload".to_owned(),
        index: 0,
    };
    assert!(
        store
            .load_fragment(&key)
            .expect("inspect fragment")
            .is_none(),
        "a cancelled upload must leave no live fragment"
    );

    let vote = tokio::task::spawn_blocking(move || -> std::io::Result<(Duration, String)> {
        let started = Instant::now();
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(1))?;
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        stream.write_all(
            format!(
                "POST /consensus/v1/cancelled-upload/42/vote HTTP/1.1\r\nHost: {address}\r\nx-verglas-cluster-secret: peer-secret\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok((started.elapsed(), response))
    })
    .await
    .expect("join vote task")
    .expect("vote request");
    assert!(
        vote.0 < Duration::from_millis(200),
        "vote was delayed: {vote:?}"
    );
    assert!(
        vote.1.starts_with("HTTP/1.1 404"),
        "unexpected vote route: {vote:?}"
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

/// Reproduces the write-back quorum-shortfall failure under concurrent load
/// (PERF-OBJECTIVE.md M1 at concurrency 16, hard gate P1: zero write errors).
///
/// One node's peer server serves every fragment placement addressed to it,
/// from every concurrent object write in the whole cluster. Sixteen
/// concurrent PUTs at `k=2 m=2` each place up to 4 fragments, so a busy node
/// can see well over a dozen placements land inside the same short window.
/// If the peer server's dedicated runtime cannot run enough of them at once,
/// requests queue behind each other on the same fixed-size worker pool until
/// the client's request budget expires — surfacing as a placement error the
/// write-back coordinator counts against quorum on a perfectly healthy node
/// (`write-back quorum requires 3 durable fragments; only 1 available`).
///
/// `handlers_with_blocking_store` stands in for the real per-request cost
/// every fragment write pays; the request timeout below is set well above one
/// placement's own cost but well below sixteen of them queued behind a
/// two-thread server, so a too-small worker pool fails outright here exactly
/// as it does in the real cluster.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_fragment_placements_do_not_time_out_under_load() {
    let node = NodeId::new("loaded-peer");
    let directory =
        std::env::temp_dir().join(format!("verglas-fragrpc-loaded-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    let store = LocalFragmentStore::new(&directory);
    let server = PeerServer::bind_with_fragments(
        "127.0.0.1:0".parse().expect("addr"),
        None,
        empty_blocks(),
        handlers_with_blocking_store(store),
    )
    .await
    .expect("bind loaded fragment server");
    let client = FragmentClient::new(
        Arc::new(StaticResolver::with(node.clone(), server.local_addr())),
        None,
        Duration::from_millis(500),
        // Comfortably above one placement's own ~60ms cost, comfortably below
        // sixteen of them serialized two at a time (~480ms tail).
        Duration::from_millis(300),
    );

    let placements = (0..16).map(|i| {
        let client = client.clone();
        let node = node.clone();
        tokio::spawn(async move {
            client
                .put_fragment(
                    &node,
                    FragmentRecord::new(
                        FragmentKey {
                            object_id: format!("loaded-object-{i}"),
                            index: 0,
                        },
                        Bytes::from_static(b"fragment-under-load"),
                    ),
                )
                .await
        })
    });
    let results = futures::future::join_all(placements).await;
    let failures: Vec<String> = results
        .into_iter()
        .enumerate()
        .filter_map(
            |(i, joined)| match joined.expect("placement task panicked") {
                Ok(()) => None,
                Err(error) => Some(format!("placement {i}: {error}")),
            },
        )
        .collect();
    assert!(
        failures.is_empty(),
        "a healthy node must not fail fragment placements under concurrency 16: {failures:?}"
    );

    server.shutdown().await;
    let _ = std::fs::remove_dir_all(directory);
}
