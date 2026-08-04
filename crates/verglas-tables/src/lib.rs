//! Table-format awareness: Iceberg and Parquet metadata.
//!
//! This crate understands table formats: parsing Iceberg snapshots, manifest
//! lists, and manifests ([`iceberg`]), and Parquet footers ([`parquet_meta`]),
//! behind a single metadata-IO interface ([`fetch`]), so the cache can make
//! format-aware decisions. The keystone is the logical key [`mapper`]: it
//! translates an object byte-range into table / file / column-chunk identity on
//! a lock-free hot path.

pub mod catalog;
pub mod fetch;
pub mod iceberg;
pub mod lifecycle;
pub mod mapper;
pub mod parquet_meta;
pub mod warming;

/// Placeholder summary of a parsed Iceberg manifest.
pub struct ManifestSummary {
    /// Data files referenced by the manifest, as logical cache keys.
    pub data_files: Vec<verglas_core::CacheKey>,
}
