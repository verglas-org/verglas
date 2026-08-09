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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use verglas_cluster::StaticResolver;
use verglas_cluster::fragments::{FragmentKey, FragmentRecord, LocalFragmentStore};
use verglas_cluster::peer::{FragmentClient, FragmentHandlers, LocalBlockFn, PeerServer};
use verglas_core::node::NodeId;

/// A block source that has nothing (fragment tests do not exercise blocks).
fn empty_blocks() -> LocalBlockFn {
    Arc::new(|_bk| Box::pin(async move { None }))
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
async fn list_fragments_filters_by_object_namespace() {
    let node = NodeId::new("peer-list");
    let (server, client, _store) = server_and_client(&node, None).await;
    for (object_id, index) in [("journal-key-a", 0), ("journal-key-b", 1), ("data", 2)] {
        client
            .put_fragment(
                &node,
                FragmentRecord::new(
                    FragmentKey {
                        object_id: object_id.to_owned(),
                        index,
                    },
                    Bytes::from_static(b"bytes"),
                ),
            )
            .await
            .expect("place");
    }
    let mut listed = client
        .list_fragments(&node, "journal-key-")
        .await
        .expect("list");
    listed.sort_by(|a, b| a.object_id.cmp(&b.object_id));
    assert_eq!(
        listed
            .into_iter()
            .map(|key| (key.object_id, key.index))
            .collect::<Vec<_>>(),
        vec![
            ("journal-key-a".to_owned(), 0),
            ("journal-key-b".to_owned(), 1)
        ]
    );
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
