//! The hybrid DRAM+NVMe cache engine (issue #12): foyer wired in behind the
//! `ObjectRead` interface, serving byte ranges out of fixed-size blocks keyed by
//! `(bucket, key, etag, block_bytes, block_index)` and filling misses from the backend
//! through the ring-disciplined miss ladder. We deliberately adopt foyer for
//! the cache mechanics — Verglas's differentiation is *what* it caches, not
//! how eviction works.
//!
//! Layout: [`block`] owns the range math, [`entry`]
//! the cache entry model and its on-disk codec, [`counters`] the read-path
//! metrics, `admission` the scan-resistant block-admission policy (#15),
//! [`classify`] the metadata/data store routing (#50), `meta_store` the
//! dedicated pinned-metadata instance (#50), `demotion` the evict-first
//! retirement set (#198), and [`engine`] the miss ladder and foyer integration
//! itself.

mod admission;
pub mod block;
pub mod classify;
pub mod counters;
mod demotion;
pub mod engine;
pub mod entry;
mod foyer_metrics;
mod materialized;
mod meta_store;
pub mod writeback_codec;

pub use block::BLOCK_SIZE_BYTES;
pub use classify::{MetaClass, MetaClassifier, MetaRouter};
pub use counters::{CacheCounters, CountersSnapshot};
pub use engine::{
    BlockDemoter, CachePurger, DemoteReceipt, DemoteRequest, EngineError, HardEviction,
    HybridCacheEngine,
};
pub use materialized::{MaterializedPageError, MaterializedPageKey, POSTGRES_PAGE_BYTES};
pub use writeback_codec::{
    CodecError, Encoded, Fragment, Geometry, MAX_STRIPE_CHUNK, StripeEncoder, encode, reassemble,
};
