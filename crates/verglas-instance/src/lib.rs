//! The verglas **cache instance**: a first-class, independently-deployable ring
//! member, and the two seams the fleet plugs into it.
//!
//! A cache instance is one verglas node in a ring of three per tenant. It is
//! the local S3 surface a pipeline writes through, and its committed writes
//! funnel through a shared transaction layer. This crate gives that instance a
//! clean crate home, separate from the cache mechanics ([`verglas_cache`]), the
//! gossip/ring transport ([`verglas_cluster`]), the metadata/warming tier
//! ([`verglas_tables`]), and the server binary — so both the local server and
//! the fleet's host-agent instantiate the *same* library type.
//!
//! It exposes two interfaces as **traits with a working local default**, so a
//! single self-hosted node runs a full lakehouse for free and the fleet's
//! at-scale implementations plug in behind the same traits without a source
//! change here:
//!
//! - [`CommitLog`] — the **transaction seam**: where a cache instance records a
//!   committed write. Default [`LocalCommitLog`] (no quorum, single-node
//!   read-your-writes); the fleet supplies a PostgreSQL-quorum implementation
//!   behind the same trait.
//! - [`RingMembership`] — the **ring interface**: placement (who owns which
//!   shard) and read-from-peer. Default [`LocalRing`] (one in-process member,
//!   no peers); the fleet supplies gossip membership + peer RPC (and later
//!   RDMA) behind the same trait, transport-agnostic like the catalog
//!   change-feed pattern. [`RingAdapter`] bridges any existing key-ownership
//!   [`verglas_core::ring::Ring`] (e.g. the server's live gossip ring) into the
//!   interface for placement.
//!
//! [`CacheInstance`] composes the two into the deployable unit. The cache engine
//! and S3 surface are wired *around* the instance, never owned by it —
//! cache-pathed I/O stays a wiring property (where the engine's storage factory
//! points), so this crate never depends on the engine backward.

pub mod commit_log;
pub mod instance;
pub mod ring;

pub use commit_log::{
    CommitAck, CommitLog, CommitLogError, CommitRecord, LocalCommitLog, QuorumKind,
};
pub use instance::CacheInstance;
pub use ring::{LocalRing, RingAdapter, RingError, RingMembership};
