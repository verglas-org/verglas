//! Dependency-leaf wire contracts shared by Verglas clients and engines.
//!
//! This crate contains only serializable request and response shapes. It has no
//! HTTP client, storage engine, Iceberg, DataFusion, or server behavior, so both
//! the public client packages and `verglas-iceberg` can consume the same
//! contract without either architectural layer depending on the other.

pub mod report;
pub mod table;

pub use report::{CompactReport, CompactionReport};
pub use table::{
    ColumnSpec, EnsureTableResponse, IngestAckResponse, PartitionSpec, TableDefinition,
};
