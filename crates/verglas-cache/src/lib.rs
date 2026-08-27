//! Single-node Foyer DRAM and persistent origin cache.
//!
//! The crate exposes fixed-size range keys, read counters, and the cache
//! engine behind the core `ObjectRead` and `Invalidation` traits. Origin bytes
//! are immutable under ETag-keyed entries and every miss has one origin fill.

mod admission;
pub mod block;
pub mod counters;
pub mod engine;
mod entry;
mod foyer_metrics;

pub use block::BLOCK_SIZE_BYTES;
pub use counters::{CacheCounters, CountersSnapshot};
pub use engine::{EngineError, HybridCacheEngine, validate_cache_budgets};
