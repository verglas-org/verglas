//! Gossip membership and the live weighted rendezvous ring for a Verglas pod.
//!
//! This crate is the pod's **cluster agent** (WHITEPAPER §3.2): it runs
//! `chitchat` gossip so nodes learn who is in the pod, who is alive, and each
//! node's addresses and capacity (#27), and it turns that live membership into
//! a weighted rendezvous ring answering `owner(key)` (#28). It is deliberately
//! a separate crate from `verglas-core` so `chitchat` and its transitive
//! transport dependencies stay out of the graph every other crate builds on —
//! the same acyclic-dependency reasoning that keeps the core types crate lean.
//!
//! No consensus lives here: membership is eventually-consistent gossip, and a
//! brief disagreement costs at worst a redundant backend fill, never a wrong
//! answer.

pub mod agent;
pub mod fragments;
pub mod member;
pub mod peer;
pub mod ring;

/// Gossip transports, re-exported so consumers construct them without a direct
/// `chitchat` dependency: [`UdpTransport`](transport::UdpTransport) in the
/// daemon, [`ChannelTransport`](transport::ChannelTransport) (in-process) in
/// tests. This is the only chitchat surface that leaks past this crate.
pub mod transport {
    pub use chitchat::transport::{ChannelTransport, Transport, UdpTransport};
}

pub use agent::{AgentConfig, ClusterAgent, ClusterError};
pub use fragments::{
    FragmentHealth, FragmentIoError, FragmentKey, FragmentRecord, FragmentWriter, LoadedFragment,
    LocalFragmentStore, fragment_checksum,
};
pub use member::{NodeMeta, NodeState};
pub use peer::{
    FragmentClient, FragmentDeleteFn, FragmentHandlers, FragmentHeadroomFn, FragmentLoadFn,
    FragmentRpcError, FragmentShardStream, FragmentStoreFn, FragmentStoreStreamFn, LocalBlockFn,
    PeerClient, PeerResolver, PeerServer, StaticResolver,
};
pub use ring::{LiveRing, RingMember, WeightedMember};
