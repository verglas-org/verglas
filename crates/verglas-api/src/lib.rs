//! Dependency-leaf wire contracts shared by Verglas clients and engines.
//!
//! This crate contains only serializable request and response shapes. It has no
//! HTTP client, storage engine, Iceberg, DataFusion, or server behavior, so both
//! `verglas-sdk` and `verglas-iceberg` can depend on the same contract without
//! either architectural layer depending on the other.

pub mod query;
pub mod report;
pub mod table;

pub use report::{CompactReport, CompactionReport};
pub use table::{ColumnSpec, EnsureTableResponse, PartitionSpec, TableDefinition};
