//! The row types, Iceberg schemas, and Arrow codecs for the `verglas_sys`
//! system tables (workers).
//!
//! Every table is flat: strings, timestamps, longs. Config and index params
//! travel as opaque JSON strings — the platform stores them verbatim and never
//! inspects a secret. Each row carries a `revision`, and state changes append a
//! new revision rather than mutating a row.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use chrono::{DateTime, Utc};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use serde::{Deserialize, Serialize};

use super::{PlatformError, SystemState};

/// The v0 placement for every declaration: the local node. Distributed
/// placement is an extension point, noted but not built (prototype rules).
pub const PLACEMENT_LOCAL: &str = "local";

/// An empty JSON object, the default `config` for a declaration that carries no
/// configuration (the MV `config` column's default).
fn default_config() -> String {
    "{}".to_owned()
}

/// Builds a required string field.
fn req_str(id: i32, name: &str) -> NestedField {
    NestedField::required(id, name, Type::Primitive(PrimitiveType::String))
}

/// Builds an optional string field.
fn opt_str(id: i32, name: &str) -> NestedField {
    NestedField::optional(id, name, Type::Primitive(PrimitiveType::String))
}

/// Reads a required string cell.
fn str_at(batch: &RecordBatch, col: usize, row: usize) -> Result<String, PlatformError> {
    let arr = batch
        .column(col)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PlatformError::Decode(format!("column {col} is not a string")))?;
    Ok(arr.value(row).to_owned())
}

/// Reads an optional string cell (null becomes `None`).
fn opt_str_at(
    batch: &RecordBatch,
    col: usize,
    row: usize,
) -> Result<Option<String>, PlatformError> {
    let arr = batch
        .column(col)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PlatformError::Decode(format!("column {col} is not a string")))?;
    Ok(if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row).to_owned())
    })
}

/// Reads a required timestamptz cell as a UTC instant.
fn ts_at(batch: &RecordBatch, col: usize, row: usize) -> Result<DateTime<Utc>, PlatformError> {
    let arr = batch
        .column(col)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| PlatformError::Decode(format!("column {col} is not a timestamp")))?;
    DateTime::from_timestamp_micros(arr.value(row))
        .ok_or_else(|| PlatformError::Decode("timestamp out of range".to_owned()))
}

/// Reads a required long cell.
fn i64_at(batch: &RecordBatch, col: usize, row: usize) -> Result<i64, PlatformError> {
    let arr = batch
        .column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| PlatformError::Decode(format!("column {col} is not a long")))?;
    Ok(arr.value(row))
}

/// Encodes a UTC-timestamp column.
fn ts_col(values: Vec<i64>) -> Arc<TimestampMicrosecondArray> {
    Arc::new(TimestampMicrosecondArray::from(values).with_timezone("+00:00"))
}

// --- workers -------------------------------------------------------------

/// The `verglas_sys.workers` table name — the single deployment registry that
/// holds every local deployment.
pub const WORKERS_TABLE: &str = "workers";

/// The default triggers JSON for a worker that names none: an empty array (an
/// on-demand worker, run only when a request routes to it).
pub const EMPTY_TRIGGERS: &str = "[]";

/// The triggers default used by the worker `Spec` serde deserializer.
fn default_triggers() -> String {
    EMPTY_TRIGGERS.to_owned()
}

/// The inputs to declare a worker. State, placement, revision, and created_at
/// are set by the catalog, not the caller.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WorkerSpec {
    /// The worker name (the primary key).
    pub name: String,
    /// The launch spec as a JSON string — `{"command","args","cwd"}` for a
    /// subprocess worker; empty for a built-in the server runs directly.
    #[serde(default)]
    pub code: String,
    /// The triggers as a JSON array of `TriggerSpec` (cron/webhook/event).
    /// Defaults to `[]` (on-demand).
    #[serde(default = "default_triggers")]
    pub triggers: String,
    /// The output table the worker writes, or `None` for an ephemeral worker
    /// that writes no table of its own.
    pub output: Option<String>,
    /// Worker config as a JSON string. Secrets are referenced by NAME only.
    #[serde(default = "default_config")]
    pub config: String,
    /// Who declared the worker.
    pub created_by: String,
}

/// One row of `verglas_sys.workers`: a declared worker at a given revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkerRow {
    /// The worker name (primary key).
    pub name: String,
    /// The launch spec JSON (`{"command","args","cwd"}`), or empty for a
    /// built-in.
    pub code: String,
    /// The triggers as a JSON array of `TriggerSpec`.
    pub triggers: String,
    /// The output table, or `None` for an ephemeral worker.
    pub output: Option<String>,
    /// Worker config JSON (secrets by name only).
    pub config: String,
    /// Lifecycle state.
    pub state: SystemState,
    /// Placement; `local` for v0.
    pub placement: String,
    /// Who declared the worker.
    pub created_by: String,
    /// When the worker was first declared (stable across revisions).
    pub created_at: DateTime<Utc>,
    /// Monotone revision; pause/resume/redeclare each append the next one.
    pub revision: i64,
}

/// The Iceberg schema for `verglas_sys.workers`.
pub fn worker_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_identifier_field_ids(vec![1])
        .with_fields(vec![
            req_str(1, "name").into(),
            req_str(2, "code").into(),
            req_str(3, "triggers").into(),
            opt_str(4, "output").into(),
            req_str(5, "config").into(),
            req_str(6, "state").into(),
            req_str(7, "placement").into(),
            req_str(8, "created_by").into(),
            NestedField::required(9, "created_at", Type::Primitive(PrimitiveType::Timestamptz))
                .into(),
            NestedField::required(10, "revision", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("worker schema is well-formed")
}

/// Encodes worker rows into a record batch bound to `live_schema`.
pub fn encode_workers(rows: &[WorkerRow], live_schema: &Schema) -> RecordBatch {
    let schema = Arc::new(schema_to_arrow_schema(live_schema).expect("workers schema to arrow"));
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(
            rows.iter()
                .map(|r| Some(r.name.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.code.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.triggers.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| r.output.clone())
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.config.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.state.as_str()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.placement.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.created_by.clone()))
                .collect::<StringArray>(),
        ),
        ts_col(
            rows.iter()
                .map(|r| r.created_at.timestamp_micros())
                .collect(),
        ),
        Arc::new(rows.iter().map(|r| r.revision).collect::<Int64Array>()),
    ];
    RecordBatch::try_new(schema, columns).expect("worker columns match the schema")
}

/// Decodes a record batch back into worker rows, the inverse of
/// [`encode_workers`].
pub fn decode_workers(batch: &RecordBatch) -> Result<Vec<WorkerRow>, PlatformError> {
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        out.push(WorkerRow {
            name: str_at(batch, 0, i)?,
            code: str_at(batch, 1, i)?,
            triggers: str_at(batch, 2, i)?,
            output: opt_str_at(batch, 3, i)?,
            config: str_at(batch, 4, i)?,
            state: SystemState::parse(&str_at(batch, 5, i)?)?,
            placement: str_at(batch, 6, i)?,
            created_by: str_at(batch, 7, i)?,
            created_at: ts_at(batch, 8, i)?,
            revision: i64_at(batch, 9, i)?,
        });
    }
    Ok(out)
}
