//! Client-side Iceberg table ingest (#145).
//!
//! The SDK is the whole write path for `verglas table create`/`append`: it
//! infers an Arrow schema from a CSV/JSONL/Parquet source, creates the table
//! (and its namespace, when missing) through the tenant's Iceberg REST
//! catalog, writes Parquet data files straight to object storage through
//! `FileIO` opened with catalog-vended credentials, and commits an append
//! snapshot. No server sits in this path — the turn-off invariant holds: a
//! cache node being down does not stop ingest.
//!
//! [`create_table`] and [`append`] are the two entry points a caller (the CLI)
//! uses: both take plain strings and paths, resolve their own catalog
//! connection, and return the same [`verglas_api::report`] shapes every other
//! table verb renders. The lower-level [`catalog::open_catalog`] and the
//! `&dyn iceberg::Catalog`-generic functions in [`write`] are exposed for
//! tests that swap in a different catalog/storage (a mock REST catalog, or an
//! in-process `MemoryCatalog`) without going through a real object store.

pub mod catalog;
mod error;
mod ident;
mod read;
mod storage;
mod write;

pub use catalog::{CatalogConfig, open_catalog};
pub use error::{IngestError, Result};
pub use ident::parse_table_ident;
pub use read::{Format, Ingested};
pub use write::{append as append_with_catalog, create_table as create_table_with_catalog};

use std::path::Path;

use verglas_api::report::{AppendReport, CreateReport};

/// Creates `table` (`namespace.name`) from `source`, inferring its schema and
/// writing the rows as the first append. `partition_by`, when set, adds an
/// identity partition on that column. Opens its own catalog connection from
/// `config` — see [`CatalogConfig`].
pub async fn create_table(
    config: &CatalogConfig,
    table: &str,
    source: &Path,
    partition_by: Option<&str>,
) -> Result<CreateReport> {
    let ident = ident::parse_table_ident(table)?;
    let catalog = catalog::open_catalog(config).await?;
    write::create_table(catalog.as_ref(), &ident, source, partition_by).await
}

/// Appends `source` to the existing table `table` (`namespace.name`) after a
/// schema-compatibility check. On mismatch the table is left unchanged and the
/// offending column is named. Opens its own catalog connection from `config`.
pub async fn append(config: &CatalogConfig, table: &str, source: &Path) -> Result<AppendReport> {
    let ident = ident::parse_table_ident(table)?;
    let catalog = catalog::open_catalog(config).await?;
    write::append(catalog.as_ref(), &ident, source).await
}
