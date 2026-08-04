//! The row types, Iceberg schemas, and Arrow codecs for the `verglas_sys`
//! system tables (workers, deployment watermarks, and vector indexes).
//!
//! Every table is flat: strings, timestamps, longs. Config, watermark, and
//! index params travel as opaque JSON strings — the platform stores them
//! verbatim and never inspects a secret. Each row carries a `revision`, and
//! state changes append a new revision rather than mutating a row.

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

/// Reads an optional long cell (null becomes `None`).
fn opt_i64_at(batch: &RecordBatch, col: usize, row: usize) -> Result<Option<i64>, PlatformError> {
    let arr = batch
        .column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| PlatformError::Decode(format!("column {col} is not a long")))?;
    Ok(if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row))
    })
}

/// Builds an optional long field.
fn opt_long(id: i32, name: &str) -> NestedField {
    NestedField::optional(id, name, Type::Primitive(PrimitiveType::Long))
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
    /// subprocess worker; empty for a built-in the daemon runs directly.
    #[serde(default)]
    pub code: String,
    /// The triggers as a JSON array of `TriggerSpec` (cron/webhook/websocket/
    /// data_change). Defaults to `[]` (on-demand).
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

// --- deployment watermarks -------------------------------------------------

/// The `verglas_sys.watermarks` table name.
pub const WATERMARKS_TABLE: &str = "watermarks";

/// One row of `verglas_sys.watermarks`: a deployment's durable cross-run
/// watermark at a given revision. The daemon's `/v1/watermark` routes (#322)
/// serve the highest-revision row per deployment; a set appends the next
/// revision, never mutating, so the snapshot log records every advance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WatermarkRow {
    /// The deployment the watermark belongs to (primary key).
    pub deployment: String,
    /// The opaque watermark value. The store never parses it.
    pub watermark: String,
    /// When this revision was written.
    pub updated_at: DateTime<Utc>,
    /// Monotone revision; each set appends the next one.
    pub revision: i64,
}

/// The Iceberg schema for `verglas_sys.watermarks`.
pub fn watermark_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_identifier_field_ids(vec![1])
        .with_fields(vec![
            req_str(1, "deployment").into(),
            req_str(2, "watermark").into(),
            NestedField::required(3, "updated_at", Type::Primitive(PrimitiveType::Timestamptz))
                .into(),
            NestedField::required(4, "revision", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("watermark schema is well-formed")
}

/// Encodes watermark rows into a record batch bound to `live_schema`.
pub fn encode_watermarks(rows: &[WatermarkRow], live_schema: &Schema) -> RecordBatch {
    let schema = Arc::new(schema_to_arrow_schema(live_schema).expect("watermarks schema to arrow"));
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(
            rows.iter()
                .map(|r| Some(r.deployment.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.watermark.clone()))
                .collect::<StringArray>(),
        ),
        ts_col(
            rows.iter()
                .map(|r| r.updated_at.timestamp_micros())
                .collect(),
        ),
        Arc::new(rows.iter().map(|r| r.revision).collect::<Int64Array>()),
    ];
    RecordBatch::try_new(schema, columns).expect("watermark columns match the schema")
}

/// Decodes a record batch back into watermark rows, the inverse of
/// [`encode_watermarks`].
pub fn decode_watermarks(batch: &RecordBatch) -> Result<Vec<WatermarkRow>, PlatformError> {
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        out.push(WatermarkRow {
            deployment: str_at(batch, 0, i)?,
            watermark: str_at(batch, 1, i)?,
            updated_at: ts_at(batch, 2, i)?,
            revision: i64_at(batch, 3, i)?,
        });
    }
    Ok(out)
}

// --- vector indexes --------------------------------------------------------

/// The `verglas_sys.indexes` table name.
pub const INDEXES_TABLE: &str = "indexes";

/// The `target_kind` value for an index over a plain table's field.
pub const TARGET_KIND_TABLE: &str = "table";

/// The `target_kind` value for an index over a graph node-table field.
pub const TARGET_KIND_GRAPH: &str = "graph";

/// The inputs to declare a vector index. State, revision, and timestamps are set
/// by the catalog, not the caller.
///
/// The blob itself never travels through here: it stays cluster-local in the
/// shadow store (the `verglas-vector` divergence from #91). `blob_ref` records
/// only *where* the latest blob lives, so a reboot knows whether to rehydrate a
/// present blob or schedule a rebuild.
#[derive(Debug, Clone, Default)]
pub struct IndexSpec {
    /// The registry key: `<cluster_id>/<target>/<field>` (composed by the caller
    /// so the same target+field on two clusters are two independent rows).
    pub name: String,
    /// `table` or `graph` (see [`TARGET_KIND_TABLE`]/[`TARGET_KIND_GRAPH`]).
    pub target_kind: String,
    /// The logical target the index is built over (`tbl:ns.table` or `graph:ns`).
    pub target: String,
    /// The embedding column the index is built over.
    pub field: String,
    /// The distance metric (`l2` or `cosine`).
    pub metric: String,
    /// The Vamana build parameters plus the id column, as a JSON string
    /// (`{"r":64,"l":100,"alpha":1.2,"idField":"id"}`). Opaque to the platform;
    /// the daemon parses it back into a maintenance config on rehydration.
    pub params: String,
    /// The cluster this index is served from. A daemon rehydrates only the rows
    /// carrying its own cluster id.
    pub cluster_id: String,
    /// The source snapshot the current blob reflects, or `None` before the first
    /// build lands a blob.
    pub reflected_snapshot: Option<i64>,
    /// Where the latest blob lives in the shadow store, or `None` before the
    /// first build.
    pub blob_ref: Option<String>,
    /// Who declared the index.
    pub created_by: String,
}

/// One row of `verglas_sys.indexes`: a declared vector index at a given revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexRow {
    /// The registry key (`<cluster_id>/<target>/<field>`, the primary key).
    pub name: String,
    /// `table` or `graph`.
    pub target_kind: String,
    /// The logical target (`tbl:ns.table` or `graph:ns`).
    pub target: String,
    /// The embedding column.
    pub field: String,
    /// The distance metric.
    pub metric: String,
    /// The Vamana params plus id column, as a JSON string.
    pub params: String,
    /// The cluster this index is served from.
    pub cluster_id: String,
    /// The reflected source snapshot, or `None` before the first build.
    pub reflected_snapshot: Option<i64>,
    /// The shadow-store location of the latest blob, or `None` before the first
    /// build.
    pub blob_ref: Option<String>,
    /// Lifecycle state.
    pub state: SystemState,
    /// Who declared the index.
    pub created_by: String,
    /// When the index was first declared (stable across revisions).
    pub created_at: DateTime<Utc>,
    /// When this revision was written.
    pub updated_at: DateTime<Utc>,
    /// Monotone revision; declare, state changes, and build updates each append
    /// the next one.
    pub revision: i64,
}

/// The Iceberg schema for `verglas_sys.indexes`.
pub fn index_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_identifier_field_ids(vec![1])
        .with_fields(vec![
            req_str(1, "name").into(),
            req_str(2, "target_kind").into(),
            req_str(3, "target").into(),
            req_str(4, "field").into(),
            req_str(5, "metric").into(),
            req_str(6, "params").into(),
            req_str(7, "cluster_id").into(),
            opt_long(8, "reflected_snapshot").into(),
            opt_str(9, "blob_ref").into(),
            req_str(10, "state").into(),
            req_str(11, "created_by").into(),
            NestedField::required(
                12,
                "created_at",
                Type::Primitive(PrimitiveType::Timestamptz),
            )
            .into(),
            NestedField::required(
                13,
                "updated_at",
                Type::Primitive(PrimitiveType::Timestamptz),
            )
            .into(),
            NestedField::required(14, "revision", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("index schema is well-formed")
}

/// Encodes index rows into a record batch bound to `live_schema`.
pub fn encode_indexes(rows: &[IndexRow], live_schema: &Schema) -> RecordBatch {
    let schema = Arc::new(schema_to_arrow_schema(live_schema).expect("indexes schema to arrow"));
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(
            rows.iter()
                .map(|r| Some(r.name.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.target_kind.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.target.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.field.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.metric.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.params.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.cluster_id.clone()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| r.reflected_snapshot)
                .collect::<Int64Array>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| r.blob_ref.clone())
                .collect::<StringArray>(),
        ),
        Arc::new(
            rows.iter()
                .map(|r| Some(r.state.as_str()))
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
        ts_col(
            rows.iter()
                .map(|r| r.updated_at.timestamp_micros())
                .collect(),
        ),
        Arc::new(rows.iter().map(|r| r.revision).collect::<Int64Array>()),
    ];
    RecordBatch::try_new(schema, columns).expect("index columns match the schema")
}

/// Decodes a record batch back into index rows, the inverse of
/// [`encode_indexes`].
pub fn decode_indexes(batch: &RecordBatch) -> Result<Vec<IndexRow>, PlatformError> {
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        out.push(IndexRow {
            name: str_at(batch, 0, i)?,
            target_kind: str_at(batch, 1, i)?,
            target: str_at(batch, 2, i)?,
            field: str_at(batch, 3, i)?,
            metric: str_at(batch, 4, i)?,
            params: str_at(batch, 5, i)?,
            cluster_id: str_at(batch, 6, i)?,
            reflected_snapshot: opt_i64_at(batch, 7, i)?,
            blob_ref: opt_str_at(batch, 8, i)?,
            state: SystemState::parse(&str_at(batch, 9, i)?)?,
            created_by: str_at(batch, 10, i)?,
            created_at: ts_at(batch, 11, i)?,
            updated_at: ts_at(batch, 12, i)?,
            revision: i64_at(batch, 13, i)?,
        });
    }
    Ok(out)
}
