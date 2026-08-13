//! Snapshot-driven prefetch and orphan retirement (#51).
//!
//! When an Iceberg `OPTIMIZE` / `rewrite_data_files` job commits, every cached
//! byte of the rewritten partitions is orphaned and the "same" data now lives in
//! objects the cache has never seen. A format-blind cache craters and re-earns
//! its hit rate from the origin. This module watches commits, classifies them
//! ([`diff`]), tracks per-partition/column access heat that survives file
//! renames by logical identity ([`heat`]), prefetches the replacement files'
//! hot chunks before queries ask ([`prefetch`]), and demotes the orphaned
//! entries without breaking time travel ([`retire`]).
//!
//! # Layering
//!
//! Nothing here runs on the request hot path. The heat ledger is *fed* from the
//! request path through a drop-on-full channel (a bounded queue whose overflow
//! is discarded — losing samples costs warmth, not correctness), and every
//! heavy step — `classify()`, EWMA aggregation, planning, fills — happens on
//! background tasks. The prefetch executor shares one byte budget
//! ([`crate::warming::budget::TokenBucket`]) and one politeness story with the
//! eager warming pipeline (#168).

mod coordinator;
pub mod diff;
pub mod heat;
pub mod prefetch;
pub mod retire;

pub use coordinator::PrefetchCoordinator;
