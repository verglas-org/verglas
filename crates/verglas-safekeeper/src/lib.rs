//! Thin Neon WAL protocol over Verglas coded Multi-Raft timeline groups.
//!
//! Neon computes do not vote, elect a donor, count safekeeper acknowledgements,
//! or advance commit positions. They acquire a writer epoch and submit exact WAL
//! ranges through any Verglas ingress; the shared consensus engine owns safety.

mod consensus_protocol;
mod segment_archiver;

pub use consensus_protocol::{
    AppendGeometry, ConsensusTimeline, WalProtocolError, WalRequest, WalResponse, WalWireError,
};
pub use segment_archiver::{
    ArchiveError, ArchiveObject, ArchiveTimeline, ConsensusArchiveTimeline, ImmutableSegmentStore,
    SegmentArchiver, SegmentRelease,
};
