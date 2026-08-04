//! The SDK-facing table API the daemon serves to `@verglas/sdk`.
//!
//! `@verglas/sdk` reads and writes Iceberg tables through a small HTTP contract
//! — commit rows, read the current snapshot, page the current rows, and pull the
//! delta since a watermark. This module is the domain-neutral engine behind that
//! contract, written against a `&dyn Catalog` so it is exercised hermetically and
//! wrapped by thin admin routes in the daemon.
//!
//! Every write goes through [`crate::write::append_batches`], the one CAS append
//! path — JSON rows are converted to Arrow against the target table's schema and
//! committed as their own snapshot. The new snapshot id is the opaque
//! `watermark` the SDK carries as a cursor: a `delta` request returns the rows in
//! snapshots newer than the watermark it is given. Because the write path only
//! ever appends, the delta is exactly the data files present at the tip but not
//! at the `since` snapshot.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::{ArrowReaderBuilder, schema_to_arrow_schema};
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use serde_json::Value;
pub use verglas_api::table::{
    ColumnSpec, CommitRequest, CommitResponse, CreateTableResponse, DeltaResponse,
    EnsureTableResponse, PartitionSpec, RowsResponse, SnapshotResponse,
    TableDefinition as CreateTableRequest,
};

use crate::error::{AgentError, Result};
use crate::ident::ident_to_dotted;
use crate::write::{self, PartitionField, PartitionTransform, append_batches};

/// The snapshot-summary property that records a commit's idempotency key, so a
/// replay of the same key is recognised and not written twice.
const IDEMPOTENCY_KEY_PROP: &str = "verglas.commit.idempotency-key";

/// The snapshot-summary property that records how many rows a keyed commit wrote,
/// so a replay can return the original row count without re-reading the data.
const IDEMPOTENCY_ROWS_PROP: &str = "verglas.commit.rows";

/// Appends the request's rows to the table and returns the new snapshot as the
/// watermark. When `idempotency_key` matches a snapshot the table already
/// carries, the commit is a replay: the original snapshot and row count are
/// returned and nothing is written. The rows are converted to Arrow against the
/// table's schema before the append; a row carrying a column the table does not
/// have — or omitting a required one — is rejected by name, unchanged.
pub async fn commit(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    request: CommitRequest,
) -> Result<CommitResponse> {
    let table = catalog.load_table(ident).await?;
    let table_name = ident_to_dotted(ident);

    // A replay: the key already rode on a committed snapshot. Return the
    // original result without writing again.
    if let Some(key) = request.idempotency_key.as_deref()
        && let Some(existing) = find_idempotent_commit(&table, key)
    {
        return Ok(existing);
    }

    let target = schema_to_arrow_schema(table.metadata().current_schema())?;
    let batches = rows_to_batches(&request.rows, &target, &table_name)?;

    commit_batches(catalog, ident, batches, request.idempotency_key).await
}

/// Appends already-decoded Arrow batches through the same idempotent CAS path
/// as JSON commits.
pub async fn commit_batches(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    batches: Vec<RecordBatch>,
    idempotency_key: Option<String>,
) -> Result<CommitResponse> {
    let table = catalog.load_table(ident).await?;
    if let Some(key) = idempotency_key.as_deref()
        && let Some(existing) = find_idempotent_commit(&table, key)
    {
        return Ok(existing);
    }

    let mut properties = HashMap::new();
    if let Some(key) = idempotency_key.as_deref() {
        let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
        properties.insert(IDEMPOTENCY_KEY_PROP.to_owned(), key.to_owned());
        properties.insert(IDEMPOTENCY_ROWS_PROP.to_owned(), row_count.to_string());
    }

    let report = append_batches(catalog, ident, batches, properties).await?;
    let snapshot_id = report
        .snapshot_id
        .map(|id| id.to_string())
        .unwrap_or_default();
    Ok(CommitResponse {
        rows_committed: report.records_added,
        watermark: snapshot_id.clone(),
        snapshot_id,
        idempotent: false,
    })
}

/// Returns the exact SDK-facing schema and partition definition of a table.
pub async fn definition(catalog: &dyn Catalog, ident: &TableIdent) -> Result<CreateTableRequest> {
    let table = catalog.load_table(ident).await?;
    let metadata = table.metadata();
    let arrow_schema = schema_to_arrow_schema(metadata.current_schema())?;
    let schema = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            Ok(ColumnSpec {
                name: field.name().clone(),
                type_name: data_type_name(field.data_type())?,
                nullable: field.is_nullable(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let partitions = metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| {
            let source = metadata
                .current_schema()
                .field_by_id(field.source_id)
                .ok_or_else(|| {
                    AgentError::TableApi(format!(
                        "partition field `{}` references missing source id {}",
                        field.name, field.source_id
                    ))
                })?;
            Ok(PartitionSpec {
                source: source.name.clone(),
                transform: field.transform.to_string(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(CreateTableRequest { schema, partitions })
}

/// Renders a supported Arrow data type in the create-table wire spelling.
fn data_type_name(data_type: &DataType) -> Result<String> {
    match data_type {
        DataType::Int64 => Ok("int64".to_owned()),
        DataType::Int32 => Ok("int32".to_owned()),
        DataType::Float64 => Ok("float64".to_owned()),
        DataType::Float32 => Ok("float32".to_owned()),
        DataType::Utf8 => Ok("utf8".to_owned()),
        DataType::Boolean => Ok("boolean".to_owned()),
        DataType::Date32 => Ok("date32".to_owned()),
        DataType::Decimal128(precision, scale) => Ok(format!("decimal128({precision},{scale})")),
        other => Err(AgentError::TableApi(format!(
            "table definition cannot represent Arrow type `{other}`"
        ))),
    }
}

/// Creates a table from an explicit schema and partition spec through the generic
/// write path ([`write::create_table_with_partitions`]). The column types are
/// parsed from their string names and the partition transforms from theirs; an
/// unknown type or transform is rejected by name so the caller can fix the
/// request. The engine does not interpret the columns — it builds exactly the
/// schema and spec asked for.
pub async fn create_table(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    request: CreateTableRequest,
) -> Result<CreateTableResponse> {
    let table_name = ident_to_dotted(ident);
    let mut fields = Vec::with_capacity(request.schema.len());
    for column in &request.schema {
        let data_type = parse_data_type(&column.type_name, &table_name, &column.name)?;
        fields.push(Field::new(&column.name, data_type, column.nullable));
    }
    let schema = Arc::new(ArrowSchema::new(fields));

    let mut partitions = Vec::with_capacity(request.partitions.len());
    for partition in &request.partitions {
        let transform = parse_transform(&partition.transform, &table_name, &partition.source)?;
        partitions.push(PartitionField {
            source: partition.source.clone(),
            transform,
        });
    }

    let report = write::create_table_with_partitions(catalog, ident, &schema, &partitions).await?;
    Ok(CreateTableResponse {
        table: report.table,
        columns: report.schema.into_iter().map(|f| f.name).collect(),
    })
}

/// Parses an Arrow `DataType` from its string name. Supports the primitives the
/// SDK table create exposes, plus `decimal128(precision,scale)`. An unrecognised
/// name is a schema mismatch naming the offending column.
fn parse_data_type(type_name: &str, table: &str, column: &str) -> Result<DataType> {
    let lowered = type_name.trim().to_ascii_lowercase();
    let mismatch = |detail: String| AgentError::SchemaMismatch {
        table: table.to_owned(),
        column: column.to_owned(),
        detail,
    };
    if let Some(args) = lowered
        .strip_prefix("decimal128(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        let (precision, scale) = args.split_once(',').ok_or_else(|| {
            mismatch(format!("`{type_name}` must be decimal128(precision,scale)"))
        })?;
        let precision: u8 = precision
            .trim()
            .parse()
            .map_err(|_| mismatch(format!("`{type_name}` has an invalid decimal precision")))?;
        let scale: i8 = scale
            .trim()
            .parse()
            .map_err(|_| mismatch(format!("`{type_name}` has an invalid decimal scale")))?;
        return Ok(DataType::Decimal128(precision, scale));
    }
    match lowered.as_str() {
        "int64" | "long" => Ok(DataType::Int64),
        "int32" | "int" => Ok(DataType::Int32),
        "float64" | "double" => Ok(DataType::Float64),
        "float32" | "float" => Ok(DataType::Float32),
        "utf8" | "string" => Ok(DataType::Utf8),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "date32" | "date" => Ok(DataType::Date32),
        other => Err(mismatch(format!(
            "`{other}` is not a supported column type"
        ))),
    }
}

/// Parses a [`PartitionTransform`] from its string name. An unrecognised name is
/// a schema mismatch naming the offending partition source column.
fn parse_transform(name: &str, table: &str, source: &str) -> Result<PartitionTransform> {
    match name.trim().to_ascii_lowercase().as_str() {
        "identity" => Ok(PartitionTransform::Identity),
        "month" => Ok(PartitionTransform::Month),
        other => Err(AgentError::SchemaMismatch {
            table: table.to_owned(),
            column: source.to_owned(),
            detail: format!("`{other}` is not a supported partition transform"),
        }),
    }
}

/// Returns the replay result if `key` was already committed, scanning the
/// table's snapshot summaries for the recorded key. The stored row count rides
/// back so a replay reports the same `rowsCommitted` as the original.
fn find_idempotent_commit(table: &Table, key: &str) -> Option<CommitResponse> {
    for snapshot in table.metadata().snapshots() {
        let props = &snapshot.summary().additional_properties;
        if props.get(IDEMPOTENCY_KEY_PROP).map(String::as_str) == Some(key) {
            let rows_committed = props
                .get(IDEMPOTENCY_ROWS_PROP)
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            let id = snapshot.snapshot_id().to_string();
            return Some(CommitResponse {
                watermark: id.clone(),
                snapshot_id: id,
                rows_committed,
                idempotent: true,
            });
        }
    }
    None
}

/// Reports the current snapshot id, the watermark (the same id), and the live
/// row count. A table with no data yet reports an empty id and a zero count.
pub async fn snapshot(catalog: &dyn Catalog, ident: &TableIdent) -> Result<SnapshotResponse> {
    let table = catalog.load_table(ident).await?;
    let id = table
        .metadata()
        .current_snapshot_id()
        .map(|id| id.to_string())
        .unwrap_or_default();
    let record_count = if table.metadata().current_snapshot().is_some() {
        live_row_count(&table).await?
    } else {
        0
    };
    Ok(SnapshotResponse {
        watermark: id.clone(),
        snapshot_id: id,
        record_count,
    })
}

/// Reads a page of the current snapshot's rows. `cursor` is an opaque offset (the
/// empty/`None` cursor starts at the beginning); `limit` caps the page. The
/// `next_cursor` is present only when more rows remain. Rows are read in a stable
/// order (by data-file path) so paging is deterministic within a snapshot.
pub async fn rows(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    limit: Option<usize>,
    cursor: Option<String>,
) -> Result<RowsResponse> {
    let table = catalog.load_table(ident).await?;
    let offset = parse_cursor(cursor.as_deref())?;
    if table.metadata().current_snapshot().is_none() {
        return Ok(RowsResponse {
            rows: Vec::new(),
            next_cursor: None,
        });
    }

    let tasks = planned_files(&table, None).await?;
    let all = read_tasks_to_json(&table, tasks).await?;
    let (page, next_cursor) = paginate(all, offset, limit);
    Ok(RowsResponse {
        rows: page,
        next_cursor,
    })
}

/// Returns the rows committed after the `since` watermark, plus the current tip.
///
/// The write path only appends, so the delta is exactly the data files present
/// at the tip but not at the `since` snapshot. A missing/empty `since` reads from
/// the beginning; a `since` that names no snapshot of the table is a clear error
/// (a stale cursor the caller must resync). `limit` caps the number of rows
/// returned.
pub async fn delta(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    since: Option<String>,
    limit: Option<usize>,
) -> Result<DeltaResponse> {
    let table = catalog.load_table(ident).await?;
    let tip = table
        .metadata()
        .current_snapshot_id()
        .map(|id| id.to_string())
        .unwrap_or_default();

    if table.metadata().current_snapshot().is_none() {
        return Ok(DeltaResponse {
            rows: Vec::new(),
            watermark: tip,
        });
    }

    // The set of data files already present at `since`; empty when reading from
    // the beginning.
    let seen: HashSet<String> = match since.as_deref() {
        Some(watermark) if !watermark.is_empty() => {
            let since_id: i64 = watermark.parse().map_err(|_| {
                AgentError::TableApi(format!("`{watermark}` is not a valid watermark"))
            })?;
            if table.metadata().snapshot_by_id(since_id).is_none() {
                return Err(AgentError::TableApi(format!(
                    "watermark `{watermark}` names no snapshot of `{}`",
                    ident_to_dotted(ident)
                )));
            }
            planned_files(&table, Some(since_id))
                .await?
                .into_iter()
                .map(|task| task.data_file_path)
                .collect()
        }
        _ => HashSet::new(),
    };

    let new_tasks: Vec<FileScanTask> = planned_files(&table, None)
        .await?
        .into_iter()
        .filter(|task| !seen.contains(&task.data_file_path))
        .collect();
    let mut rows = read_tasks_to_json(&table, new_tasks).await?;
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(DeltaResponse {
        rows,
        watermark: tip,
    })
}

/// Converts JSON rows to a single Arrow batch matching the table's schema.
///
/// Each row must be a JSON object whose keys are all columns of the table; an
/// unknown key, or a missing required (non-nullable) column, is rejected by name
/// so the caller can fix the row before anything is written. Omitted nullable
/// columns decode to null. The batch is then handed to the CAS append path,
/// which coerces column types to the table's.
fn rows_to_batches(
    rows: &[Value],
    target: &arrow_schema::Schema,
    table_name: &str,
) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    for row in rows {
        let object = row.as_object().ok_or_else(|| AgentError::SchemaMismatch {
            table: table_name.to_owned(),
            column: "<row>".to_owned(),
            detail: "a commit row must be a JSON object".to_owned(),
        })?;
        for key in object.keys() {
            if target.field_with_name(key).is_err() {
                return Err(AgentError::SchemaMismatch {
                    table: table_name.to_owned(),
                    column: key.clone(),
                    detail: "is not a column of the table".to_owned(),
                });
            }
        }
        for field in target.fields() {
            if !field.is_nullable() && !object.contains_key(field.name()) {
                return Err(AgentError::SchemaMismatch {
                    table: table_name.to_owned(),
                    column: field.name().clone(),
                    detail: "is required by the table but missing from a commit row".to_owned(),
                });
            }
        }
    }

    let schema = std::sync::Arc::new(target.clone());
    let mut decoder = arrow_json::ReaderBuilder::new(schema)
        .build_decoder()
        .map_err(|e| AgentError::SchemaMismatch {
            table: table_name.to_owned(),
            column: "<row>".to_owned(),
            detail: e.to_string(),
        })?;
    decoder
        .serialize(rows)
        .map_err(|e| AgentError::SchemaMismatch {
            table: table_name.to_owned(),
            column: "<row>".to_owned(),
            detail: e.to_string(),
        })?;
    match decoder.flush().map_err(|e| AgentError::SchemaMismatch {
        table: table_name.to_owned(),
        column: "<row>".to_owned(),
        detail: e.to_string(),
    })? {
        Some(batch) => Ok(vec![batch]),
        None => Ok(Vec::new()),
    }
}

/// Plans the live data files of `table` at `snapshot_id` (the current snapshot
/// when `None`), sorted by data-file path so reads are deterministic.
async fn planned_files(table: &Table, snapshot_id: Option<i64>) -> Result<Vec<FileScanTask>> {
    let mut builder = table.scan();
    if let Some(id) = snapshot_id {
        builder = builder.snapshot_id(id);
    }
    let scan = builder.build()?;
    let mut tasks: Vec<FileScanTask> = scan.plan_files().await?.try_collect().await?;
    tasks.sort_by(|a, b| {
        (a.data_file_path.as_str(), a.start).cmp(&(b.data_file_path.as_str(), b.start))
    });
    Ok(tasks)
}

/// Sums the live row count across the current snapshot's data files, counting a
/// file split across tasks once.
async fn live_row_count(table: &Table) -> Result<u64> {
    let tasks = planned_files(table, None).await?;
    let mut seen: HashSet<&str> = HashSet::new();
    let mut rows = 0u64;
    for task in &tasks {
        if seen.insert(task.data_file_path.as_str()) {
            rows += task.record_count.unwrap_or(0);
        }
    }
    Ok(rows)
}

/// Reads `tasks` through the table's FileIO and returns every row as a JSON
/// object keyed by column name. Null-valued columns are omitted from a row's
/// object (the Arrow JSON encoder's behaviour), matching how the SDK reads them.
async fn read_tasks_to_json(table: &Table, tasks: Vec<FileScanTask>) -> Result<Vec<Value>> {
    if tasks.is_empty() {
        return Ok(Vec::new());
    }
    let reader = ArrowReaderBuilder::new(table.file_io().clone()).build();
    let stream = futures::stream::iter(tasks.into_iter().map(Ok)).boxed();
    let batches: Vec<RecordBatch> = reader.read(stream)?.try_collect().await?;
    batches_to_json(&batches)
}

/// Encodes Arrow batches as a flat list of JSON row objects.
fn batches_to_json(batches: &[RecordBatch]) -> Result<Vec<Value>> {
    let non_empty: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
    if non_empty.is_empty() {
        return Ok(Vec::new());
    }
    let mut buffer = Vec::new();
    {
        let mut writer = arrow_json::ArrayWriter::new(&mut buffer);
        writer
            .write_batches(&non_empty)
            .map_err(|e| AgentError::Query(format!("encoding rows to JSON failed: {e}")))?;
        writer
            .finish()
            .map_err(|e| AgentError::Query(format!("encoding rows to JSON failed: {e}")))?;
    }
    match serde_json::from_slice(&buffer)
        .map_err(|e| AgentError::Query(format!("decoding encoded rows failed: {e}")))?
    {
        Value::Array(rows) => Ok(rows),
        _ => Ok(Vec::new()),
    }
}

/// Parses an opaque paging cursor into a row offset. The absent/empty cursor is
/// offset zero; anything else must be a non-negative integer.
fn parse_cursor(cursor: Option<&str>) -> Result<usize> {
    match cursor {
        None | Some("") => Ok(0),
        Some(value) => value
            .parse()
            .map_err(|_| AgentError::TableApi(format!("`{value}` is not a valid cursor"))),
    }
}

/// Slices `rows` to the page starting at `offset` with at most `limit` rows,
/// returning the page and the cursor for the next page when more rows remain.
fn paginate(rows: Vec<Value>, offset: usize, limit: Option<usize>) -> (Vec<Value>, Option<String>) {
    let total = rows.len();
    let start = offset.min(total);
    let end = match limit {
        Some(limit) => start.saturating_add(limit).min(total),
        None => total,
    };
    let next_cursor = (end < total).then(|| end.to_string());
    let page = rows[start..end].to_vec();
    (page, next_cursor)
}
