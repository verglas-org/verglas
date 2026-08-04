//! Integration tests for the peer-fetch trait's M1 (cluster-of-one)
//! implementation: the no-op fetch is always a clean miss, so the miss
//! ladder falls through to the backend fill.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use verglas_core::node::NodeId;
use verglas_core::peer::{NoopPeerFetch, PeerFetch};
use verglas_core::{BlockKey, CacheKey};

/// Drives a future that must complete on its first poll. The no-op fetch
/// never awaits anything, so this avoids pulling an async runtime into
/// `verglas-core`'s dev-dependencies just to unwrap a ready value.
fn poll_ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("no-op peer fetch must complete on first poll"),
    }
}

#[test]
fn noop_peer_fetch_is_a_clean_miss() {
    let peer = NoopPeerFetch;
    let block = BlockKey {
        object: CacheKey {
            bucket: "lake".to_owned(),
            key: "warehouse/db/table/data/part-000001.parquet".to_owned(),
        },
        etag: "etag-abc".to_owned(),
        block_bytes: 2 * 1024 * 1024,
        block_index: 0,
    };
    let result = poll_ready(peer.fetch(NodeId::new("node-solo"), &block));
    assert!(matches!(result, Ok(None)));
}
