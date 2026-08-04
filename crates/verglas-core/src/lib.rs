//! Core types shared across the Verglas workspace.
//!
//! This crate holds the configuration types, error types, logical cache key
//! definitions, the data-plane interfaces ([`read`]'s `ObjectRead`, [`write`]'s
//! `ObjectWrite`/`Invalidation`, and [`list`]'s `ObjectList`), and admin API
//! wire types that every other
//! Verglas crate builds on. Keeping them here keeps the dependency graph
//! acyclic: `verglas-cache`, `verglas-s3`, and `verglas-tables` all depend on
//! `verglas-core`, never on each other's internals.

pub mod admin;
pub mod config;
pub mod config_template;
pub mod disk;
pub mod glob;
pub mod list;
pub mod medium;
pub mod metrics;

pub mod node;
pub mod peer;
pub mod read;
pub mod ring;
pub mod telemetry;
pub mod trace;
pub mod write;

/// Logical identity of a cached object: which object a request names,
/// independent of which version of it currently exists.
///
/// This is the granularity of ring ownership (see [`ring`]) and of
/// invalidation: every request path resolves `owner(&CacheKey)` before
/// touching any cache state. Versioning lives one level down, in
/// [`BlockKey`], so that ownership is stable across object overwrites.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// Origin bucket the object lives in.
    pub bucket: String,
    /// Object key within the bucket.
    pub key: String,
}

/// Identity of one fixed-size block of one *version* of an object — the key
/// cache entries are stored under (issue #12; M1 granularity is
/// `(bucket, key, etag, block_bytes, block_index)`, column-chunk granularity
/// arrives in M3).
///
/// The ETag is part of the key (the foundation of #14's immutability fast
/// path): a cached block can never be stale, because overwriting the object
/// changes its ETag and therefore addresses different cache entries. The only
/// state that can go stale is the `CacheKey` → ETag *mapping*, whose
/// freshness policy is #14's scope. Invariant: a `BlockKey` is only ever
/// constructed with an ETag the origin actually reported for these bytes —
/// objects whose origin responses carry no ETag are non-cacheable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockKey {
    /// The object this block belongs to.
    pub object: CacheKey,
    /// Entity tag of the object version these bytes came from, exactly as
    /// the origin reported it (no quote normalization; comparisons are
    /// byte-exact against the same origin's later responses).
    pub etag: String,
    /// Size of each block in this cache geometry. It travels with peer reads so
    /// nodes with divergent configuration can only miss, never serve bytes from
    /// the wrong offset.
    pub block_bytes: u64,
    /// Zero-based index of the block within the object. Together with
    /// [`BlockKey::block_bytes`] it addresses
    /// `[index * block_bytes, (index + 1) * block_bytes)` of the object.
    pub block_index: u64,
}
