//! The pure-client Iceberg engine (issue #320).
//!
//! This crate is the engine behind every Verglas table operation: catalog open,
//! the CAS write path, the DataFusion query path, table inspection, compaction,
//! and the commit/snapshot/rows/delta table API the server serves to
//! `@verglas/sdk`. It is purely a client — it speaks to any Iceberg REST catalog
//! (a real one or the server's loopback gateway) and reads and writes data files
//! through an S3 endpoint. It links no server internals and no cache: it reads
//! and writes wherever its endpoint points (WHITEPAPER §7.4). The server points
//! it at its own S3 surface for cache residency; the cloud committer points it
//! straight at object storage.
//!
//! The moving parts:
//!
//! - [`conn`]: the resolved [`Connection`] the engine opens a catalog from (the
//!   CLI resolves flags/environment/server-probe into one).
//! - [`catalog`]: build a REST catalog wired to the endpoint.
//! - [`storage`]: the fixed-part S3 storage factory (fixed-part multipart uploads for strict S3-compatible stores).
//! - [`ingest`]: read a CSV / JSONL / Parquet source into Arrow batches with an
//!   inferred schema.
//! - [`write`]: create tables and append data (schema-checked, CAS commit).
//! - [`inspect`]: list, show, and history.
//! - [`query`]: embedded SQL with optional time travel.
//! - [`estimate`]: plan-based memory estimation for a query, without
//!   executing it — sizes a query worker's initial memory grant.
//! - [`retention`]: snapshot-history expiry any table's maintenance can call
//!   (used after compaction; catalog-side queues own telemetry retention).
//! - [`compaction`]: small-file compaction (bin-pack / sort) via REPLACE commits,
//!   snapshot expiry after each pass, and conservative orphan-file cleanup.
//! - [`tables_api`]: the commit/snapshot/rows/delta contract the server serves.
//! - [`ident`]: parse `namespace.name` table identifiers.
//! - [`error`]: the one error enum shared across every operation.
//! - [`report`]: the stable `--output json` result data shapes (the CLI renders
//!   the human-readable forms).

pub mod catalog;
pub mod compaction;
pub mod conn;
pub mod error;
pub mod estimate;
pub mod ident;
pub mod ingest;
pub mod inspect;
pub mod query;
pub mod report;
pub mod retention;
pub mod storage;
pub mod tables_api;
pub mod write;

pub use compaction::{
    CompactReport, CompactionOptions, CompactionReport, OrphanReport, cleanup_orphans,
    compact_table, compact_table_once, run_compaction, undersized_file_count,
};
pub use conn::Connection;
pub use error::{AgentError, Result};
pub use estimate::estimate;
pub use ident::parse_table_ident;
pub use query::{
    PreparedCatalog, QueryExecution, TimeTravel, batch_to_json_rows_fragment, query_stream,
};
pub use report::{
    AppendReport, CreateReport, FieldInfo, HistoryReport, ListReport, QueryMemoryEstimate,
    QueryReport, ShowReport, SnapshotInfo, TableRef,
};
pub use retention::{DEFAULT_KEEP_LAST_SNAPSHOTS, ExpireReport, expire_snapshots};
