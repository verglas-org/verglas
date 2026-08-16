//! Creating tables and appending data through an Iceberg catalog (#145).
//!
//! `create_table` infers a schema from the source file, creates the table
//! (with an optional identity partition column), and writes the rows as one
//! initial append. `append` loads the table, checks the source is
//! schema-compatible (naming the offending column otherwise), and commits a
//! fast append. Data files are written through the table's `FileIO` — opened
//! with catalog-vended credentials, see [`super::catalog::open_catalog`] — so
//! this never depends on a running cache node or server (the turn-off
//! invariant: Verglas off, ingest still works).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema as ArrowSchema};
use iceberg::arrow::{
    RecordBatchPartitionSplitter, arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema,
};
use iceberg::spec::{
    DataFile, DataFileFormat, NestedFieldRef, Schema as IcebergSchema, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;

use super::error::{IngestError, Result};
use super::ident::ident_to_dotted;
use super::read;
use verglas_api::report::{AppendReport, CreateReport, FieldInfo};

/// Converts an Iceberg schema field into the shared API report shape.
fn field_info(field: &NestedFieldRef) -> FieldInfo {
    FieldInfo {
        id: field.id,
        name: field.name.clone(),
        r#type: type_label(&field.field_type),
        required: field.required,
    }
}

/// Produces the stable API label for an Iceberg type.
fn type_label(ty: &Type) -> String {
    match serde_json::to_value(ty) {
        Ok(serde_json::Value::String(value)) => value,
        Ok(other) => other.to_string(),
        Err(_) => "unknown".to_owned(),
    }
}

/// Creates a table from a source file, then writes the rows as the first
/// append. `partition_by`, when set, adds an identity partition on that
/// column. The source format is inferred from the file extension (CSV /
/// JSONL / Parquet).
pub async fn create_table(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    source: &Path,
    partition_by: Option<&str>,
) -> Result<CreateReport> {
    let ingested = read::read(source)?;
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&ingested.schema)?;

    // An optional identity partition on a named column. The source id is
    // looked up in the freshly assigned schema; an unknown column is a clear
    // error.
    let partition_spec = match partition_by {
        Some(column) => Some(build_identity_partition_spec(&iceberg_schema, column)?),
        None => None,
    };

    ensure_namespace(catalog, ident).await?;

    let creation = TableCreation::builder()
        .name(ident.name().to_owned())
        .schema(iceberg_schema.clone())
        .partition_spec_opt(partition_spec)
        .build();
    let table = catalog.create_table(ident.namespace(), creation).await?;

    // Write the rows as the initial append (a create with no data leaves an
    // empty table, which is still valid).
    let (records, files, snapshot_id) = write_append(catalog, &table, &ingested.batches).await?;

    let schema = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(field_info)
        .collect();
    Ok(CreateReport {
        table: ident_to_dotted(ident),
        operation: "create".to_owned(),
        schema,
        partition_by: partition_by.into_iter().map(str::to_owned).collect(),
        records_added: records,
        data_files_added: files,
        snapshot_id,
    })
}

/// Appends a source file to an existing table after a schema-compatibility
/// check. On mismatch the table is left unchanged and the offending column is
/// named.
pub async fn append(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    source: &Path,
) -> Result<AppendReport> {
    let ingested = read::read(source)?;
    let table = catalog.load_table(ident).await?;
    let (records, files, snapshot_id) = write_append(catalog, &table, &ingested.batches).await?;
    Ok(AppendReport {
        table: ident_to_dotted(ident),
        operation: "append".to_owned(),
        records_added: records,
        data_files_added: files,
        snapshot_id,
    })
}

/// How many commit attempts a fast append gets before a conflict surfaces.
/// Bounded so a persistently contended table fails loudly instead of spinning.
const COMMIT_ATTEMPTS: u32 = 8;

/// Coerces the batches to the table schema, writes them as Parquet data files
/// through the table's `FileIO`, and commits a fast append. Returns the row
/// count, data-file count, and the new snapshot id.
///
/// With no rows the table is left untouched (zero, zero, current snapshot) —
/// `create` with an empty source still produces a valid, empty table.
async fn write_append(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    batches: &[RecordBatch],
) -> Result<(u64, u64, Option<i64>)> {
    let table_name = ident_to_dotted(table.identifier());
    let iceberg_schema = table.metadata().current_schema();
    let target_arrow = Arc::new(schema_to_arrow_schema(iceberg_schema)?);
    let batches = coerce_batches(batches, &target_arrow, &table_name)?;
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    if row_count == 0 {
        return Ok((0, 0, table.metadata().current_snapshot_id()));
    }

    let data_files = write_data_files(table, iceberg_schema, batches).await?;
    let file_count = data_files.len() as u64;
    let committed = commit_data_files(catalog, table, data_files).await?;

    Ok((
        row_count,
        file_count,
        committed.metadata().current_snapshot_id(),
    ))
}

/// Commits already-written data files as a fast append, retrying past
/// concurrent writers.
///
/// Duplicate-file validation is skipped: our data files were just written
/// under fresh UUID names, so re-adding an existing file is impossible. On
/// `CatalogCommitConflicts` the table is reloaded and the same files
/// re-attached (they are already durable; only the metadata swing is redone),
/// with a short linear backoff, up to [`COMMIT_ATTEMPTS`] tries.
async fn commit_data_files(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    data_files: Vec<DataFile>,
) -> Result<iceberg::table::Table> {
    let mut current = table.clone();
    let mut attempt = 1u32;
    loop {
        let tx = Transaction::new(&current);
        let action = tx
            .fast_append()
            .with_check_duplicate(false)
            .add_data_files(data_files.clone());
        let tx = action.apply(tx)?;
        match tx.commit(catalog).await {
            Ok(committed) => return Ok(committed),
            Err(e)
                if e.kind() == iceberg::ErrorKind::CatalogCommitConflicts
                    && attempt < COMMIT_ATTEMPTS =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(250 * u64::from(attempt)))
                    .await;
                current = catalog.load_table(table.identifier()).await?;
                attempt += 1;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Writes `batches` as one or more Parquet data files under the table's data
/// location, through the table's configured `FileIO`. An unpartitioned table
/// takes the plain data-file writer; a partitioned one takes a fanout writer,
/// splitting each batch by its partition value so every file lands in the
/// right partition.
async fn write_data_files(
    table: &iceberg::table::Table,
    iceberg_schema: &Arc<IcebergSchema>,
    batches: Vec<RecordBatch>,
) -> Result<Vec<DataFile>> {
    let location_gen = DefaultLocationGenerator::new(table.metadata())?;
    let file_name_gen = DefaultFileNameGenerator::new(
        format!("verglas-{}", uuid::Uuid::new_v4()),
        None,
        DataFileFormat::Parquet,
    );
    let parquet_writer =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_schema.clone());
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );
    let data_file_builder = DataFileWriterBuilder::new(rolling);

    let partition_spec = table.metadata().default_partition_spec();
    if partition_spec.fields().is_empty() {
        let mut writer = data_file_builder.build(None).await?;
        for batch in batches {
            writer.write(batch).await?;
        }
        return Ok(writer.close().await?);
    }

    // Partitioned: split each batch by partition value and fan out to a data
    // file per partition. The splitter computes partition values from the rows.
    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        iceberg_schema.clone(),
        partition_spec.clone(),
    )?;
    let mut writer = FanoutWriter::new(data_file_builder);
    for batch in batches {
        for (partition_key, partition_batch) in splitter.split(&batch)? {
            writer.write(partition_key, partition_batch).await?;
        }
    }
    Ok(writer.close().await?)
}

/// Reorders and casts the ingested batches to match the table's Arrow schema
/// (which carries the Iceberg field ids the writer needs). A source column the
/// table does not have, a table column the source lacks, or a type that
/// cannot be cast is an [`IngestError::SchemaMismatch`] naming the column —
/// the append is rejected before any data file is written.
fn coerce_batches(
    batches: &[RecordBatch],
    target: &Arc<ArrowSchema>,
    table_name: &str,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let source_schema = batches[0].schema();

    // A source column that is not part of the table is rejected by name.
    for source_field in source_schema.fields() {
        if target.field_with_name(source_field.name()).is_err() {
            return Err(IngestError::SchemaMismatch {
                table: table_name.to_owned(),
                column: source_field.name().clone(),
                detail: "is not a column of the table".to_owned(),
            });
        }
    }

    // For each table column, find its source index and target type once.
    let mut plan: Vec<(usize, DataType)> = Vec::with_capacity(target.fields().len());
    for target_field in target.fields() {
        let source_index = source_schema.index_of(target_field.name()).map_err(|_| {
            IngestError::SchemaMismatch {
                table: table_name.to_owned(),
                column: target_field.name().clone(),
                detail: "is required by the table but missing from the source".to_owned(),
            }
        })?;
        plan.push((source_index, target_field.data_type().clone()));
    }

    let mut coerced = Vec::with_capacity(batches.len());
    for batch in batches {
        let mut columns = Vec::with_capacity(plan.len());
        for (target_field, (source_index, target_type)) in target.fields().iter().zip(&plan) {
            let column = batch.column(*source_index);
            let column = if column.data_type() == target_type {
                column.clone()
            } else {
                arrow_cast::cast(column, target_type).map_err(|e| IngestError::SchemaMismatch {
                    table: table_name.to_owned(),
                    column: target_field.name().clone(),
                    detail: format!(
                        "source type {} cannot be written as table type {}: {e}",
                        column.data_type(),
                        target_type
                    ),
                })?
            };
            columns.push(column);
        }
        coerced.push(RecordBatch::try_new(target.clone(), columns).map_err(|e| {
            IngestError::SchemaMismatch {
                table: table_name.to_owned(),
                column: "<row>".to_owned(),
                detail: e.to_string(),
            }
        })?);
    }
    Ok(coerced)
}

/// Builds an unbound identity partition spec on `column`, resolving its field
/// id in `schema`. An unknown source column is a clear error.
fn build_identity_partition_spec(
    schema: &IcebergSchema,
    column: &str,
) -> Result<UnboundPartitionSpec> {
    let field = schema
        .field_by_name(column)
        .ok_or_else(|| IngestError::UnknownPartitionColumn(column.to_owned()))?;
    let mut builder = UnboundPartitionSpec::builder();
    // Explicit partition field id from 1000 (the Iceberg convention) so the
    // create-table request carries it; strict REST catalogs reject an unbound
    // spec whose partition field ids are null.
    builder = builder.add_partition_fields([iceberg::spec::UnboundPartitionField::builder()
        .source_id(field.id)
        .field_id(1000)
        .name(column.to_owned())
        .transform(Transform::Identity)
        .build()])?;
    Ok(builder.build())
}

/// Ensures the table's namespace exists, creating each level (parents first)
/// so a first `create` in a fresh namespace just works.
///
/// It does not probe with `namespace_exists`: that is a HEAD request, and some
/// REST catalogs answer HEAD with 400 rather than 404. Instead each level is
/// created and an already-exists result is treated as success — idempotent
/// and portable.
async fn ensure_namespace(catalog: &dyn Catalog, ident: &TableIdent) -> Result<()> {
    let levels = ident.namespace().as_ref();
    for depth in 1..=levels.len() {
        let partial = iceberg::NamespaceIdent::from_vec(levels[..depth].to_vec())?;
        if catalog.get_namespace(&partial).await.is_ok() {
            continue;
        }
        match catalog.create_namespace(&partial, HashMap::new()).await {
            Ok(_) => {}
            Err(e) if is_already_exists(&e) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Whether an Iceberg error means the namespace was already present.
fn is_already_exists(error: &iceberg::Error) -> bool {
    error.kind() == iceberg::ErrorKind::NamespaceAlreadyExists
        || error.to_string().contains("already exists")
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use iceberg::CatalogBuilder;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};

    use super::*;
    use crate::ingest::ident::parse_table_ident;

    /// Builds an in-process memory catalog over a fresh temp warehouse path —
    /// no network, no REST catalog. `create_table`/`append` take `&dyn
    /// Catalog`, so this exercises the exact same write path a real REST
    /// catalog does.
    async fn memory_catalog() -> Arc<dyn Catalog> {
        let warehouse = tempfile::tempdir().expect("warehouse tempdir");
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.path().to_str().expect("utf8 path").to_string(),
                )]),
            )
            .await
            .expect("memory catalog");
        std::mem::forget(warehouse);
        Arc::new(catalog)
    }

    /// Writes `contents` to a temp file named `name`, leaking the tempdir so
    /// the path stays valid for the rest of the short-lived test process.
    fn source_file(name: &str, contents: &str) -> std::path::PathBuf {
        let dir = tempfile::tempdir().expect("source tempdir");
        let path = dir.path().join(name);
        let mut file = std::fs::File::create(&path).expect("create source");
        file.write_all(contents.as_bytes()).expect("write source");
        std::mem::forget(dir);
        path
    }

    /// Acceptance (#145): appending a source whose column type cannot be cast
    /// to the table's column type is rejected, naming the offending column,
    /// and leaves the table's row count unchanged.
    #[tokio::test]
    async fn append_with_incompatible_column_names_the_column_and_leaves_table_unchanged() {
        let catalog = memory_catalog().await;
        let ident = parse_table_ident("sales.orders").expect("ident");

        let first = source_file("orders.csv", "id,amount\n1,10.5\n2,20.0\n");
        let create = create_table(catalog.as_ref(), &ident, &first, None)
            .await
            .expect("create");
        assert_eq!(create.records_added, 2);

        // `amount` is a struct in the second source, not the double the table
        // expects — a type arrow_cast cannot coerce.
        let bad = source_file("bad.jsonl", "{\"id\":3,\"amount\":{\"cents\":500}}\n");
        let err = append(catalog.as_ref(), &ident, &bad)
            .await
            .expect_err("incompatible column must be rejected");
        match err {
            IngestError::SchemaMismatch { table, column, .. } => {
                assert_eq!(table, "sales.orders");
                assert_eq!(column, "amount", "the offending column must be named");
            }
            other => panic!("expected SchemaMismatch, got {other:?}"),
        }

        // The table is unchanged: still two rows, one snapshot.
        let table = catalog.load_table(&ident).await.expect("load table");
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .expect("snapshot")
                .summary()
                .additional_properties
                .get("total-records")
                .map(String::as_str),
            Some("2")
        );
    }

    /// Acceptance (#145): a source column the table does not have is rejected
    /// by name, not silently dropped.
    #[tokio::test]
    async fn append_with_unknown_column_names_the_column() {
        let catalog = memory_catalog().await;
        let ident = parse_table_ident("sales.orders").expect("ident");

        let first = source_file("orders.csv", "id,amount\n1,10.5\n");
        create_table(catalog.as_ref(), &ident, &first, None)
            .await
            .expect("create");

        let extra = source_file("extra.csv", "id,amount,discount\n2,5.0,0.1\n");
        let err = append(catalog.as_ref(), &ident, &extra)
            .await
            .expect_err("unknown column must be rejected");
        match err {
            IngestError::SchemaMismatch { column, .. } => assert_eq!(column, "discount"),
            other => panic!("expected SchemaMismatch, got {other:?}"),
        }
    }

    /// Acceptance (#145): create infers types per format and the first
    /// append's row/file/snapshot counters are reported; a second append adds
    /// a second snapshot.
    #[tokio::test]
    async fn csv_create_then_append_reports_counts_and_snapshots() {
        let catalog = memory_catalog().await;
        let ident = parse_table_ident("agent_data.scores").expect("ident");

        let first = source_file(
            "rows.csv",
            "id,name,score\n1,alpha,0.9\n2,beta,0.7\n3,gamma,0.4\n",
        );
        let create = create_table(catalog.as_ref(), &ident, &first, None)
            .await
            .expect("create");
        assert_eq!(create.records_added, 3);
        assert!(create.data_files_added >= 1);
        assert!(create.snapshot_id.is_some());

        let second = source_file("more.csv", "id,name,score\n4,delta,0.95\n");
        let append_report = append(catalog.as_ref(), &ident, &second)
            .await
            .expect("append");
        assert_eq!(append_report.records_added, 1);
        assert_ne!(append_report.snapshot_id, create.snapshot_id);
    }

    /// Acceptance (#145): `--partition-by` on an unknown column is a clear
    /// error naming the column, not a panic or a silently ignored partition.
    #[tokio::test]
    async fn create_with_unknown_partition_column_is_rejected() {
        let catalog = memory_catalog().await;
        let ident = parse_table_ident("sales.orders").expect("ident");
        let source = source_file("orders.csv", "id,amount\n1,10.5\n");

        let err = create_table(catalog.as_ref(), &ident, &source, Some("nope"))
            .await
            .expect_err("unknown partition column must be rejected");
        assert!(matches!(err, IngestError::UnknownPartitionColumn(column) if column == "nope"));
    }

    /// Acceptance (#145): `--partition-by` on a real column actually
    /// partitions the table (not silently dropped) and the reported
    /// partition_by names it.
    #[tokio::test]
    async fn create_with_partition_by_reports_and_applies_the_partition() {
        let catalog = memory_catalog().await;
        let ident = parse_table_ident("sales.orders").expect("ident");
        let source = source_file("orders.csv", "id,region\n1,east\n2,west\n3,east\n");

        let create = create_table(catalog.as_ref(), &ident, &source, Some("region"))
            .await
            .expect("create with partition");
        assert_eq!(create.partition_by, vec!["region".to_owned()]);
        assert_eq!(create.records_added, 3);

        let table = catalog.load_table(&ident).await.expect("load table");
        let spec = table.metadata().default_partition_spec();
        assert_eq!(spec.fields().len(), 1);
        assert_eq!(spec.fields()[0].name, "region");
    }
}
