//! Deterministic Parquet writing and table creation for Iceberg Sink batches.
//!
//! This module owns only the bounded writer used by `commit_sink_batch`, plus a
//! small table metadata cache and explicit-schema table creation helper.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::spec::{DataFile, Schema as IcebergSchema};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation, TableIdent};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::error::{AgentError, Result};
use crate::ident::ident_to_dotted;

/// Writer controls used by a deterministic Sink batch.
#[derive(Clone, Debug, Default)]
pub(crate) struct AppendWriteOptions {
    /// The exact relative Parquet path for the batch.
    pub(crate) file_name: Option<String>,
    /// The Parquet codec selected by the Sink.
    pub(crate) compression: Option<Compression>,
    /// Key/value metadata proving ownership of an orphan file.
    pub(crate) file_metadata: Vec<(String, String)>,
}

/// Cache of recently loaded Iceberg table metadata for one runtime process.
#[derive(Default)]
pub struct TableCache {
    tables: Mutex<HashMap<TableIdent, iceberg::table::Table>>,
}

impl TableCache {
    /// Creates an empty table metadata cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads a table once and returns the cached metadata on later calls.
    pub async fn get_or_load(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
    ) -> Result<iceberg::table::Table> {
        if let Some(table) = self.tables.lock().await.get(ident) {
            return Ok(table.clone());
        }
        let table = catalog.load_table(ident).await?;
        self.tables
            .lock()
            .await
            .insert(ident.clone(), table.clone());
        Ok(table)
    }

    /// Reloads the authoritative table after creation or commit.
    pub(crate) async fn reload(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
    ) -> Result<iceberg::table::Table> {
        let table = catalog.load_table(ident).await?;
        self.tables
            .lock()
            .await
            .insert(ident.clone(), table.clone());
        Ok(table)
    }

    /// Refreshes the cache with a committed table.
    async fn put(&self, ident: TableIdent, table: iceberg::table::Table) {
        self.tables.lock().await.insert(ident, table);
    }
}

/// Creates an empty unpartitioned table from an explicit Arrow schema.
pub async fn create_table_from_schema(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    schema: &SchemaRef,
) -> Result<iceberg::table::Table> {
    create_table_from_schema_with_properties(catalog, ident, schema, HashMap::new()).await
}

/// Creates an empty unpartitioned table with immutable table properties.
pub async fn create_table_from_schema_with_properties(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    schema: &SchemaRef,
    properties: HashMap<String, String>,
) -> Result<iceberg::table::Table> {
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(schema)?;
    ensure_namespace(catalog, ident).await?;
    let creation = TableCreation::builder()
        .name(ident.name().to_owned())
        .schema(iceberg_schema)
        .properties(properties)
        .build();
    Ok(catalog.create_table(ident.namespace(), creation).await?)
}

/// Ensures every namespace level exists, tolerating concurrent creation.
async fn ensure_namespace(catalog: &dyn Catalog, ident: &TableIdent) -> Result<()> {
    let levels = ident.namespace().as_ref();
    for depth in 1..=levels.len() {
        let partial = iceberg::NamespaceIdent::from_vec(levels[..depth].to_vec())?;
        if catalog.get_namespace(&partial).await.is_ok() {
            continue;
        }
        match catalog.create_namespace(&partial, HashMap::new()).await {
            Ok(_) => {}
            Err(error) if is_already_exists(&error) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Detects an idempotent namespace-create response from a REST catalog.
fn is_already_exists(error: &iceberg::Error) -> bool {
    error.kind() == iceberg::ErrorKind::NamespaceAlreadyExists
        || error.to_string().contains("already exists")
}

/// Appends batches using deterministic Sink writer controls and refreshes cache.
pub(crate) async fn append_batches_from_table_with_options(
    catalog: &dyn Catalog,
    cache: &TableCache,
    table: &iceberg::table::Table,
    batches: Vec<RecordBatch>,
    snapshot_properties: HashMap<String, String>,
    options: AppendWriteOptions,
) -> Result<WriteReport> {
    let ident = table.identifier().clone();
    let (records, snapshot_id, committed) =
        write_append(catalog, table, &batches, &snapshot_properties, &options).await?;
    cache.put(ident, committed).await;
    Ok(WriteReport {
        records_added: records,
        snapshot_id,
    })
}

/// The bounded result needed to construct a Sink receipt.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WriteReport {
    /// Rows written to the committed snapshot.
    pub(crate) records_added: u64,
    /// New snapshot identity, if a commit was made.
    pub(crate) snapshot_id: Option<i64>,
}

const COMMIT_ATTEMPTS: u32 = 8;

/// Coerces batches, writes one deterministic Parquet file, and commits an append.
async fn write_append(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    batches: &[RecordBatch],
    snapshot_properties: &HashMap<String, String>,
    options: &AppendWriteOptions,
) -> Result<(u64, Option<i64>, iceberg::table::Table)> {
    let table_name = ident_to_dotted(table.identifier());
    let iceberg_schema = table.metadata().current_schema();
    let target_arrow = Arc::new(schema_to_arrow_schema(iceberg_schema)?);
    let batches = coerce_batches(batches, &target_arrow, &table_name)?;
    let row_count: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    if row_count == 0 && snapshot_properties.is_empty() {
        return Ok((0, table.metadata().current_snapshot_id(), table.clone()));
    }
    if !table
        .metadata()
        .default_partition_spec()
        .fields()
        .is_empty()
    {
        return Err(AgentError::InvalidRequest(
            "Sink tables must be unpartitioned".to_owned(),
        ));
    }
    let data_files = if row_count == 0 {
        Vec::new()
    } else {
        write_data_file(table, iceberg_schema, batches, options).await?
    };
    let committed = commit_data_files(catalog, table, data_files, snapshot_properties).await?;
    Ok((
        row_count,
        committed.metadata().current_snapshot_id(),
        committed,
    ))
}

/// Commits already-written files with bounded optimistic conflict retries.
async fn commit_data_files(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    data_files: Vec<DataFile>,
    snapshot_properties: &HashMap<String, String>,
) -> Result<iceberg::table::Table> {
    let mut current = table.clone();
    for attempt in 1..=COMMIT_ATTEMPTS {
        let tx = Transaction::new(&current);
        let action = tx
            .fast_append()
            .with_check_duplicate(false)
            .set_snapshot_properties(snapshot_properties.clone())
            .add_data_files(data_files.clone());
        let tx = action.apply(tx)?;
        match tx.commit(catalog).await {
            Ok(committed) => return Ok(committed),
            Err(error)
                if error.kind() == iceberg::ErrorKind::CatalogCommitConflicts
                    && attempt < COMMIT_ATTEMPTS =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(250 * u64::from(attempt)))
                    .await;
                current = catalog.load_table(table.identifier()).await?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(AgentError::InvalidRequest(
        "Iceberg commit retry budget exhausted".to_owned(),
    ))
}

/// Generates the exact first file name and deterministic suffixes if needed.
#[derive(Clone, Debug)]
struct FixedFileNameGenerator {
    file_name: String,
    next_suffix: Arc<std::sync::atomic::AtomicU64>,
}

impl FixedFileNameGenerator {
    /// Creates a generator whose first output is `file_name`.
    fn new(file_name: String) -> Self {
        Self {
            file_name,
            next_suffix: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl FileNameGenerator for FixedFileNameGenerator {
    /// Returns the deterministic base path and stable suffixes thereafter.
    fn generate_file_name(&self) -> String {
        let suffix = self
            .next_suffix
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if suffix == 0 {
            return self.file_name.clone();
        }
        let stem = self
            .file_name
            .strip_suffix(".parquet")
            .unwrap_or(&self.file_name);
        format!("{stem}-{suffix:05}.parquet")
    }
}

/// Rejects an existing deterministic file unless its metadata proves this batch.
async fn ensure_existing_file_matches(
    table: &iceberg::table::Table,
    location_gen: &DefaultLocationGenerator,
    file_name: &str,
    expected_metadata: &[(String, String)],
) -> Result<()> {
    let path = location_gen.generate_location(None, file_name);
    let input = table.file_io().new_input(&path)?;
    if !input.exists().await? {
        return Ok(());
    }
    let bytes = input.read().await?;
    let metadata =
        ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::default()).map_err(|error| {
            AgentError::InvalidRequest(format!(
                "deterministic file `{path}` is not Parquet: {error}"
            ))
        })?;
    let actual = metadata
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default();
    for (key, expected) in expected_metadata {
        let found = actual
            .iter()
            .find(|entry| entry.key == key.as_str())
            .and_then(|entry| entry.value.as_deref());
        if found != Some(expected.as_str()) {
            return Err(AgentError::InvalidRequest(format!(
                "deterministic file `{path}` has a different `{key}`"
            )));
        }
    }
    Ok(())
}

/// Writes all rows as one unpartitioned Parquet data file.
async fn write_data_file(
    table: &iceberg::table::Table,
    iceberg_schema: &Arc<IcebergSchema>,
    batches: Vec<RecordBatch>,
    options: &AppendWriteOptions,
) -> Result<Vec<DataFile>> {
    let file_name = options.file_name.as_deref().ok_or_else(|| {
        AgentError::InvalidRequest("Sink writer requires a deterministic file name".to_owned())
    })?;
    let location_gen = DefaultLocationGenerator::new(table.metadata())?;
    ensure_existing_file_matches(table, &location_gen, file_name, &options.file_metadata).await?;
    let mut properties = WriterProperties::builder();
    if let Some(compression) = options.compression {
        properties = properties.set_compression(compression);
    }
    if !options.file_metadata.is_empty() {
        let metadata = options
            .file_metadata
            .iter()
            .map(|(key, value)| parquet::file::metadata::KeyValue::new(key.clone(), value.clone()))
            .collect();
        properties = properties.set_key_value_metadata(Some(metadata));
    }
    let parquet_writer = ParquetWriterBuilder::new(properties.build(), iceberg_schema.clone());
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer,
        table.file_io().clone(),
        location_gen,
        FixedFileNameGenerator::new(file_name.to_owned()),
    );
    let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
    for batch in batches {
        writer.write(batch).await?;
    }
    Ok(writer.close().await?)
}

/// Reorders batches to the table schema and rejects missing or unknown columns.
fn coerce_batches(
    batches: &[RecordBatch],
    target: &Arc<arrow_schema::Schema>,
    table_name: &str,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let source_schema = batches[0].schema();
    for source_field in source_schema.fields() {
        if target.field_with_name(source_field.name()).is_err() {
            return Err(AgentError::SchemaMismatch {
                table: table_name.to_owned(),
                column: source_field.name().clone(),
                detail: "is not a column of the table".to_owned(),
            });
        }
    }
    let mut indices = Vec::with_capacity(target.fields().len());
    for target_field in target.fields() {
        let source_index = source_schema.index_of(target_field.name()).map_err(|_| {
            AgentError::SchemaMismatch {
                table: table_name.to_owned(),
                column: target_field.name().clone(),
                detail: "is required by the table but missing from the source".to_owned(),
            }
        })?;
        indices.push(source_index);
    }
    let mut output = Vec::with_capacity(batches.len());
    for batch in batches {
        let columns = indices
            .iter()
            .map(|index| batch.column(*index).clone())
            .collect();
        output.push(
            RecordBatch::try_new(target.clone(), columns).map_err(|error| {
                AgentError::SchemaMismatch {
                    table: table_name.to_owned(),
                    column: "<row>".to_owned(),
                    detail: error.to_string(),
                }
            })?,
        );
    }
    Ok(output)
}
