//! A thin wrapper over `verglas-iceberg`: open the catalog, ensure the CDC
//! namespace and one table per PG table, append change rows, and evolve a table
//! by adding a nullable column.
//!
//! All Iceberg data-plane IO routes through the connection's `s3_endpoint` — the
//! injected cache endpoint, never a direct R2 URL (the platform's cache-or-503
//! rule). This module owns none of that policy; it just carries the endpoint the
//! runner resolved into [`verglas_iceberg::Connection`].
//!
//! # Data-file format seam
//!
//! [`verglas_iceberg::write::append_batches`] writes **Parquet** data files:
//! iceberg-rust 0.9.1 has no Avro data-file writer (confirmed). Streaming CDC
//! wants small Avro data files (row-oriented, cheap to append) compacted to
//! Parquet later. When an Avro writer is added to the verglas-org/iceberg-rust
//! fork — mirroring the `[patch.crates-io]` git-pin discipline that already
//! carries `TableCommit::from_parts` — the append below should take a
//! `DataFileFormat` and default to Avro for the streaming tier. See the TODO at
//! the append call. Until then every append is Parquet, which keeps the
//! workspace green.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use iceberg::spec::{NestedField, Schema as IcebergSchema, Type};
use iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};
use verglas_iceberg::Connection;

use crate::schema::{RESERVED_COLUMN_COUNT, data_columns_of};
use crate::{CdcError, Result};

/// The Iceberg namespace every CDC table lands in.
pub const CDC_NAMESPACE: &str = "pg_analytics";

/// The snapshot-summary property carrying the CDC end LSN of an append.
pub const PROP_END_LSN: &str = "verglas.cdc.end_lsn";
/// The snapshot-summary property carrying the replication slot name.
pub const PROP_SLOT: &str = "verglas.cdc.slot";

/// The connection settings the runner resolves from its injected environment.
/// Every field maps to a [`verglas_iceberg::Connection`] field; `s3_endpoint` is
/// the cache endpoint, and is never a direct-R2 URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkConfig {
    /// Iceberg REST catalog base URI.
    pub catalog_uri: String,
    /// Optional catalog bearer token.
    pub token: Option<String>,
    /// Optional catalog warehouse identifier.
    pub warehouse: Option<String>,
    /// The cache S3 endpoint all data-file IO routes through.
    pub s3_endpoint: Option<String>,
    /// SigV4 signing region.
    pub region: String,
    /// Endpoint access key id.
    pub access_key_id: Option<String>,
    /// Endpoint secret access key.
    pub secret_access_key: Option<String>,
}

impl SinkConfig {
    /// Builds the engine [`Connection`] this config resolves to.
    pub fn connection(&self) -> Connection {
        Connection {
            catalog_uri: self.catalog_uri.clone(),
            token: self.token.clone(),
            warehouse: self.warehouse.clone(),
            s3_endpoint: self.s3_endpoint.clone(),
            region: self.region.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
        }
    }
}

/// The Iceberg identifier a PG table maps to: `pg_analytics.<pgschema>_<pgtable>`.
pub fn table_ident(pg_schema: &str, pg_table: &str) -> Result<TableIdent> {
    let dotted = format!("{CDC_NAMESPACE}.{pg_schema}_{pg_table}");
    Ok(verglas_iceberg::parse_table_ident(&dotted)?)
}

/// Opens the REST catalog for `config`, routing FileIO through its cache
/// endpoint.
pub async fn open_catalog(config: &SinkConfig) -> Result<Arc<dyn Catalog>> {
    Ok(verglas_iceberg::catalog::open_catalog(&config.connection()).await?)
}

/// The state of a table after [`ensure_table`]: whether it already existed, and
/// its current data columns (name, Arrow type), excluding the reserved metadata
/// columns. The runner diffs a fresh relation against `data_columns` to decide
/// evolution.
#[derive(Debug, Clone)]
pub struct TableState {
    /// Whether the table already existed (vs just created).
    pub existed: bool,
    /// The table's current data columns, excluding the reserved `_vg_*` block.
    pub data_columns: Vec<(String, DataType)>,
}

/// Ensures `ident` exists with `schema` (the change-row schema). If the table is
/// absent it is created; if present it is loaded. Either way the table's current
/// data columns are returned for the evolution diff. Idempotent.
pub async fn ensure_table(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    schema: &SchemaRef,
) -> Result<TableState> {
    match catalog.load_table(ident).await {
        Ok(table) => {
            let arrow = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(CdcError::from)?;
            Ok(TableState {
                existed: true,
                data_columns: data_columns_of(&arrow),
            })
        }
        Err(_) => {
            // Not present (or unreadable): create it from the change-row schema.
            verglas_iceberg::write::create_table_from_schema(catalog, ident, schema, None).await?;
            Ok(TableState {
                existed: false,
                data_columns: data_columns_of(schema),
            })
        }
    }
}

/// Appends change-row `batches` to `ident`, stamping the CDC end LSN and slot on
/// the new snapshot's summary. Returns the rows appended.
///
/// TODO(avro-data-files): `append_batches` writes Parquet data files today —
/// iceberg-rust 0.9.1 has no Avro data-file writer. Once one lands on the
/// verglas-org/iceberg-rust fork (mirroring the `from_parts` patch discipline),
/// the streaming CDC tier should append Avro (`DataFileFormat::Avro`) and
/// compact to Parquet later. Do not attempt Avro here until that writer exists.
pub async fn append(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    batches: Vec<RecordBatch>,
    end_lsn: i64,
    slot: &str,
) -> Result<u64> {
    let mut props = HashMap::new();
    props.insert(PROP_END_LSN.to_owned(), end_lsn.to_string());
    props.insert(PROP_SLOT.to_owned(), slot.to_owned());
    let report = verglas_iceberg::write::append_batches(catalog, ident, batches, props).await?;
    Ok(report.records_added)
}

/// Adds one nullable column `name` of Arrow type `data_type` to `ident`.
///
/// iceberg-rust 0.9.1's public `Transaction` API supports only fast-append and
/// property updates — it has no schema-update action — so the add-column commit
/// is hand-rolled, exactly as retention hand-rolls its overwrite commit
/// (`crates/verglas-iceberg/src/retention.rs::commit_overwrite`): a new
/// [`IcebergSchema`] carrying the table's existing fields plus the new field is
/// committed through [`Catalog::update_table`] with a
/// [`TableCommit::from_parts`] built from an `AddSchema` + `SetCurrentSchema`
/// update pair, guarded by `UuidMatch` and `CurrentSchemaIdMatch` so a
/// concurrent evolution forces a conflict rather than a clobber.
pub async fn evolve_add_column(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    name: &str,
    data_type: &DataType,
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let metadata = table.metadata();
    let current = metadata.current_schema();

    // The new field gets the next unused field id; the new schema the next id.
    let new_field_id = metadata.last_column_id() + 1;
    let new_schema_id = metadata.current_schema_id() + 1;
    let field_type: Type = iceberg::arrow::arrow_type_to_type(data_type).map_err(CdcError::from)?;

    // Existing fields, unchanged, plus the new nullable column.
    let mut fields = current.as_struct().fields().to_vec();
    fields.push(Arc::new(NestedField::optional(
        new_field_id,
        name,
        field_type,
    )));
    let new_schema = IcebergSchema::builder()
        .with_schema_id(new_schema_id)
        .with_fields(fields)
        .build()
        .map_err(CdcError::from)?;

    let updates = vec![
        TableUpdate::AddSchema { schema: new_schema },
        // -1 selects the just-added schema as current.
        TableUpdate::SetCurrentSchema { schema_id: -1 },
    ];
    let requirements = vec![
        TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        },
        TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id(),
        },
    ];
    let commit = TableCommit::from_parts(ident.clone(), requirements, updates);
    catalog.update_table(commit).await?;
    Ok(())
}

/// Whether a change-row schema has the reserved metadata block plus at least one
/// data column — a cheap sanity check callers can assert before an append.
pub fn has_data_columns(schema: &SchemaRef) -> bool {
    schema.fields().len() > RESERVED_COLUMN_COUNT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_pg_table_to_pg_analytics_ident() {
        let ident = table_ident("public", "orders").expect("ident");
        assert_eq!(ident.namespace().as_ref(), &vec!["pg_analytics".to_owned()]);
        assert_eq!(ident.name(), "public_orders");
    }

    #[test]
    fn connection_carries_the_cache_endpoint() {
        let cfg = SinkConfig {
            catalog_uri: "https://catalog.example".to_owned(),
            token: Some("t".to_owned()),
            warehouse: Some("wh".to_owned()),
            s3_endpoint: Some("https://cache.internal".to_owned()),
            region: "us-east-1".to_owned(),
            access_key_id: Some("id".to_owned()),
            secret_access_key: Some("secret".to_owned()),
        };
        let conn = cfg.connection();
        assert_eq!(conn.s3_endpoint.as_deref(), Some("https://cache.internal"));
        assert_eq!(conn.catalog_uri, "https://catalog.example");
    }
}
