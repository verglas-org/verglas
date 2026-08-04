//! Creating tables and appending data through an Iceberg catalog.
//!
//! `create` infers a schema from the source file, creates the table (with an
//! optional identity partition column), and writes the rows as one initial
//! append. `append` loads the table, checks the source is schema-compatible
//! (naming the offending column otherwise), and commits a fast append.
//! `append_batches` commits already-computed Arrow batches (a materialized
//! view's result) through the same coerce/validate/commit path, and can stamp
//! snapshot-summary properties (the MV's consumed watermark). Data files are
//! written through the table's FileIO — the S3 endpoint the catalog was opened
//! with — so a daemon in the path gives cache residency and write-back.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema as ArrowSchema, SchemaRef};
use iceberg::arrow::{
    RecordBatchPartitionSplitter, arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema,
};
use iceberg::spec::{
    DataFile, DataFileFormat, Schema as IcebergSchema, Transform, UnboundPartitionSpec,
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

use crate::error::{AgentError, Result};
use crate::ident::ident_to_dotted;
use crate::ingest;
use crate::report::{AppendReport, CreateReport};

/// Creates a table from a source file, then writes the rows as the first
/// append. `partition_by`, when set, adds an identity partition on that column.
/// The source format is inferred from the file extension (CSV / JSONL /
/// Parquet).
pub async fn create_table(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    source: &Path,
    partition_by: Option<&str>,
) -> Result<CreateReport> {
    let ingested = ingest::read(source)?;
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&ingested.schema)?;

    // An optional identity partition on a named column. The source id is looked
    // up in the freshly assigned schema; an unknown column is a clear error.
    let partition_spec = match partition_by {
        Some(column) => Some(build_partition_spec(
            &iceberg_schema,
            &[PartitionField::identity(column)],
        )?),
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
    let (records, files, snapshot_id) =
        write_append(catalog, &table, &ingested.batches, &HashMap::new()).await?;

    let schema = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(crate::report::field_info)
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
    let ingested = ingest::read(source)?;
    let table = catalog.load_table(ident).await?;
    let (records, files, snapshot_id) =
        write_append(catalog, &table, &ingested.batches, &HashMap::new()).await?;
    Ok(AppendReport {
        table: ident_to_dotted(ident),
        operation: "append".to_owned(),
        records_added: records,
        data_files_added: files,
        snapshot_id,
    })
}

/// Appends already-in-memory Arrow batches to an existing table through the same
/// CAS append path as [`append`]: coerce to the table schema (naming any
/// mismatched column), write Parquet data files through the table's FileIO, and
/// commit a fast append with commit-conflict retry. `snapshot_properties` ride
/// on the new snapshot's summary — the SQL MV executor stamps its consumed
/// watermark there so the output table itself is the record of what was
/// processed. When the batches hold no rows but properties are set, a
/// metadata-only snapshot is still committed so the watermark advances. This is
/// also the batch entry point for the config-driven source runtime, which passes
/// empty properties; the batches carry their own Arrow schema either way.
pub async fn append_batches(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    batches: Vec<RecordBatch>,
    snapshot_properties: HashMap<String, String>,
) -> Result<AppendReport> {
    let table = catalog.load_table(ident).await?;
    let (records, files, snapshot_id) =
        write_append(catalog, &table, &batches, &snapshot_properties).await?;
    Ok(AppendReport {
        table: ident_to_dotted(ident),
        operation: "append".to_owned(),
        records_added: records,
        data_files_added: files,
        snapshot_id,
    })
}

/// The Iceberg transform a partition field applies to its source column. The two
/// the write path builds partition specs from: `Identity` (partition by the
/// column value) and `Month` (partition by the month of a date/timestamp column).
/// This is a domain-neutral capability — the caller names the columns and picks
/// the transform; the write path knows nothing about what the table means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionTransform {
    /// Partition by the source column's value unchanged.
    Identity,
    /// Partition by the month of the source date/timestamp column.
    Month,
}

/// One partition column of an explicit partition spec: the source column name and
/// the transform to apply. A spec is an ordered list of these.
#[derive(Debug, Clone)]
pub struct PartitionField {
    /// The name of the source column this partition is derived from.
    pub source: String,
    /// The transform applied to the source column.
    pub transform: PartitionTransform,
}

impl PartitionField {
    /// A partition on the source column's value unchanged.
    pub fn identity(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            transform: PartitionTransform::Identity,
        }
    }

    /// A partition on the month of the source date/timestamp column.
    pub fn month(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            transform: PartitionTransform::Month,
        }
    }
}

/// Creates an empty table from an Arrow schema (no source file). The
/// config-driven source runtime uses this to materialize a source's target
/// table on first run; the connector's declared schema is the table's schema.
/// An optional identity partition is added on `partition_by`. This is the
/// single-identity convenience over [`create_table_with_partitions`].
pub async fn create_table_from_schema(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    schema: &SchemaRef,
    partition_by: Option<&str>,
) -> Result<CreateReport> {
    let partitions: Vec<PartitionField> = partition_by
        .map(|column| vec![PartitionField::identity(column)])
        .unwrap_or_default();
    create_table_with_partitions(catalog, ident, schema, &partitions).await
}

/// Creates an empty table from an explicit Arrow schema and an ordered partition
/// spec. The schema carries the exact column types (any Arrow type, including
/// `Decimal128` and `Date32`) and per-field nullability the caller declares; the
/// partition spec is a list of [`PartitionField`]s, each an identity or month
/// transform on a named source column, so a table can partition on several
/// columns with mixed transforms. This is the general form; the caller owns the
/// schema and spec, and the write path is agnostic to what the table means.
pub async fn create_table_with_partitions(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    schema: &SchemaRef,
    partitions: &[PartitionField],
) -> Result<CreateReport> {
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(schema)?;
    let partition_spec = if partitions.is_empty() {
        None
    } else {
        Some(build_partition_spec(&iceberg_schema, partitions)?)
    };
    ensure_namespace(catalog, ident).await?;
    let creation = TableCreation::builder()
        .name(ident.name().to_owned())
        .schema(iceberg_schema)
        .partition_spec_opt(partition_spec)
        .build();
    let table = catalog.create_table(ident.namespace(), creation).await?;
    let schema = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(crate::report::field_info)
        .collect();
    Ok(CreateReport {
        table: ident_to_dotted(ident),
        operation: "create".to_owned(),
        schema,
        partition_by: partitions.iter().map(|p| p.source.clone()).collect(),
        records_added: 0,
        data_files_added: 0,
        snapshot_id: table.metadata().current_snapshot_id(),
    })
}

/// Ensures the table's namespace exists, creating each level (parents first) so
/// a first `create` in a fresh namespace just works.
///
/// It does not probe with `namespace_exists`: that is a HEAD request, and some
/// REST catalogs (e.g. tabulario/iceberg-rest) answer HEAD with 400 rather than
/// 404. Instead each level is created and an already-exists result is treated as
/// success — idempotent and portable.
async fn ensure_namespace(catalog: &dyn Catalog, ident: &TableIdent) -> Result<()> {
    let levels = ident.namespace().as_ref();
    for depth in 1..=levels.len() {
        let partial = iceberg::NamespaceIdent::from_vec(levels[..depth].to_vec())?;
        // A GET-based existence check (works everywhere, unlike HEAD).
        if catalog.get_namespace(&partial).await.is_ok() {
            continue;
        }
        match catalog.create_namespace(&partial, HashMap::new()).await {
            Ok(_) => {}
            // Tolerate a concurrent create. Some REST catalogs report the 409 as
            // a specific kind, others as `Unexpected` with an "already exists"
            // message; accept both.
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

/// How many commit attempts a fast append gets before a conflict surfaces.
/// Bounded so a persistently contended table fails loudly instead of spinning.
const COMMIT_ATTEMPTS: u32 = 8;

/// Coerces the batches to the table schema, writes them as Parquet data files
/// through the table's FileIO, and commits a fast append carrying
/// `snapshot_properties` on the new snapshot. Returns the row count, data-file
/// count, and the new snapshot id.
///
/// With no rows and no properties the table is left untouched (zero, zero,
/// current snapshot). With no rows but properties set, a metadata-only snapshot
/// is committed so the properties (a watermark) are recorded — Iceberg allows a
/// property-only append.
async fn write_append(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    batches: &[RecordBatch],
    snapshot_properties: &HashMap<String, String>,
) -> Result<(u64, u64, Option<i64>)> {
    let table_name = ident_to_dotted(table.identifier());
    let iceberg_schema = table.metadata().current_schema();
    let target_arrow = Arc::new(schema_to_arrow_schema(iceberg_schema)?);
    let batches = coerce_batches(batches, &target_arrow, &table_name)?;
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    if row_count == 0 && snapshot_properties.is_empty() {
        return Ok((0, 0, table.metadata().current_snapshot_id()));
    }

    let data_files = if row_count == 0 {
        Vec::new()
    } else {
        write_data_files(table, iceberg_schema, batches).await?
    };
    let file_count = data_files.len() as u64;
    let committed = commit_data_files(catalog, table, data_files, snapshot_properties).await?;

    Ok((
        row_count,
        file_count,
        committed.metadata().current_snapshot_id(),
    ))
}

/// Commits already-written data files as a fast append, retrying past
/// concurrent writers (#306).
///
/// Duplicate-file validation is skipped: it loads every manifest of the table,
/// which on a long table holds the commit window open long enough that a busy
/// concurrent writer always lands first — and our data files were just written
/// under fresh UUID names, so re-adding an existing file is impossible. On
/// `CatalogCommitConflicts` the table is reloaded and the same files re-attached
/// (they are already durable; only the metadata swing is redone), with a short
/// linear backoff, up to [`COMMIT_ATTEMPTS`] tries.
async fn commit_data_files(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    data_files: Vec<DataFile>,
    snapshot_properties: &HashMap<String, String>,
) -> Result<iceberg::table::Table> {
    let mut current = table.clone();
    let mut attempt = 1u32;
    loop {
        let tx = Transaction::new(&current);
        let action = tx
            .fast_append()
            .with_check_duplicate(false)
            .set_snapshot_properties(snapshot_properties.clone())
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
/// location, through the table's configured FileIO. An unpartitioned table
/// takes the plain data-file writer; a partitioned one takes a fanout writer,
/// splitting each batch by its partition value so every file lands in the right
/// partition.
async fn write_data_files(
    table: &iceberg::table::Table,
    iceberg_schema: &Arc<IcebergSchema>,
    batches: Vec<RecordBatch>,
) -> Result<Vec<DataFile>> {
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
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
/// table does not have, a table column the source lacks, or a type that cannot
/// be cast is a [`AgentError::SchemaMismatch`] naming the column — the append is
/// rejected before any data file is written.
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
            return Err(AgentError::SchemaMismatch {
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
            AgentError::SchemaMismatch {
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
                arrow_cast::cast(column, target_type).map_err(|e| AgentError::SchemaMismatch {
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
            AgentError::SchemaMismatch {
                table: table_name.to_owned(),
                column: "<row>".to_owned(),
                detail: e.to_string(),
            }
        })?);
    }
    Ok(coerced)
}

/// Builds an unbound partition spec from an ordered list of [`PartitionField`]s,
/// resolving each source column's field id in `schema`. An identity field is
/// named for its source column; a month field is named `<source>_month` (the
/// Iceberg convention), so the two never collide and the derived column reads
/// clearly. An unknown source column is a clear error.
fn build_partition_spec(
    schema: &IcebergSchema,
    partitions: &[PartitionField],
) -> Result<UnboundPartitionSpec> {
    let mut builder = UnboundPartitionSpec::builder();
    // Assign explicit partition field ids from 1000 (the Iceberg convention) so
    // the create-table request carries them; strict REST catalogs reject an
    // unbound spec whose partition field ids are null.
    for (index, partition) in partitions.iter().enumerate() {
        let field = schema
            .field_by_name(&partition.source)
            .ok_or_else(|| AgentError::Ingest {
                path: partition.source.clone(),
                detail: format!(
                    "partition column `{}` is not in the schema",
                    partition.source
                ),
            })?;
        let (name, transform) = match partition.transform {
            PartitionTransform::Identity => (partition.source.clone(), Transform::Identity),
            PartitionTransform::Month => (format!("{}_month", partition.source), Transform::Month),
        };
        builder =
            builder.add_partition_fields([iceberg::spec::UnboundPartitionField::builder()
                .source_id(field.id)
                .field_id(1000 + index as i32)
                .name(name)
                .transform(transform)
                .build()])?;
    }
    Ok(builder.build())
}
