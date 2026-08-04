//! Node identity within a Verglas pod.
//!
//! Every cache key has a home node chosen by the ring (see [`crate::ring`]);
//! `NodeId` is how that node is named everywhere above the transport layer.

use std::fmt;
use std::sync::Arc;

/// Identity of a single node in a Verglas pod.
///
/// Newtype over a shared string so that identities coming from cluster
/// membership (gossip, #27) can be carried around verbatim. Cloning is a
/// refcount bump, not an allocation — ownership lookups happen on the read
/// hot path, which must not allocate (see AGENTS.md standing invariants).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(Arc<str>);

impl NodeId {
    /// Wraps a node name as a `NodeId`.
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }

    /// Returns the node name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    /// Formats the node id as its bare name, for logs and error messages.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
