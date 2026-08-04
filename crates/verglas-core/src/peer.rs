//! Peer fetch: asking another node in the pod for the bytes it owns.
//!
//! Sits between the local cache tiers and the backend fill in the miss
//! ladder: local DRAM → local NVMe → peer fetch → backend. M1 is a
//! cluster of one, so its implementation ([`NoopPeerFetch`]) always misses
//! and the ladder falls through to the backend — but the call site exists
//! now, so M2's peer RPC (#29) plugs in without touching the read path.

use std::error::Error;
use std::fmt;
use std::future::Future;

use bytes::Bytes;

use crate::BlockKey;
use crate::node::NodeId;

/// Error contacting a peer for a key it owns.
///
/// Errors mean "the peer could not answer", not "the peer does not have the
/// bytes" — a clean miss is `Ok(None)`. Callers degrade to a backend fill
/// (slow is acceptable; wrong is never), they do not fail the read.
#[derive(Debug)]
pub enum PeerFetchError {
    /// The peer was unreachable or refused the request.
    Unavailable {
        /// The peer that could not be reached.
        node: NodeId,
        /// Transport-level detail for logs.
        reason: String,
    },
}

impl fmt::Display for PeerFetchError {
    /// Renders the failing peer and transport detail for logs.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { node, reason } => {
                write!(f, "peer {node} unavailable: {reason}")
            }
        }
    }
}

impl Error for PeerFetchError {}

/// Fetches one cached block from the pod node that owns it.
///
/// The unit of transfer is a whole [`BlockKey`] — object + ETag + block index
/// — not a `(key, range)` pair, because the ETag is what makes a peer serve
/// *exactly* the version the caller resolved and nothing else. A peer holding
/// a newer version of the object must miss (the caller re-resolves and reads
/// coherently), never serve a different version's bytes: wrong is never. The
/// block index alone addresses the block's byte range (the block size is a
/// cache-engine constant), so no range travels on the wire.
///
/// Async-style decision: native RPITIT (`-> impl Future + Send`) rather than
/// the `async-trait` crate — no boxed futures, no extra dependency, and the
/// `Send` bound is explicit so multi-threaded executors can drive it. The
/// trade-off is that the trait is not dyn-compatible; call sites are generic
/// over `P: PeerFetch`. If dynamic dispatch is ever needed, switching to boxed
/// futures is a mechanical change confined to this trait (extension point).
pub trait PeerFetch: Send + Sync {
    /// Requests the block identified by `block` from `node` (the key's owner
    /// per the ring).
    ///
    /// Returns `Ok(Some(bytes))` on a hit for the exact `BlockKey`, `Ok(None)`
    /// when the owner does not have that block cached (including when it holds
    /// only a different version), and `Err` only for transport-level failures.
    fn fetch(
        &self,
        node: NodeId,
        block: &BlockKey,
    ) -> impl Future<Output = Result<Option<Bytes>, PeerFetchError>> + Send;
}

/// The M1 cluster-of-one implementation: there are no peers, so every fetch
/// is a clean miss and the miss ladder falls through to the backend fill.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopPeerFetch;

impl PeerFetch for NoopPeerFetch {
    /// Always resolves to `Ok(None)` immediately; never errors, never awaits.
    fn fetch(
        &self,
        _node: NodeId,
        _block: &BlockKey,
    ) -> impl Future<Output = Result<Option<Bytes>, PeerFetchError>> + Send {
        std::future::ready(Ok(None))
    }
}
