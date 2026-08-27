//! Sink-owned Iceberg request validation, schema inference, and replay receipts.
//!
//! A Sink batch has one deterministic file identity and one snapshot-summary
//! identity. The commit path never claims an unowned table and never overwrites
//! an orphaned file with different batch metadata.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::table::Table;
use iceberg::{Catalog, ErrorKind, TableIdent};
use parquet::basic::Compression;
use serde_json::Value;

use crate::error::{AgentError, Result};
use crate::ident::ident_to_dotted;
use crate::write::{self, AppendWriteOptions, TableCache};

/// Snapshot property carrying the deterministic Pipeline batch identity.
pub const SINK_BATCH_ID_PROPERTY: &str = "verglas.sink.batch-id";
/// Snapshot property carrying the complete batch payload digest.
pub const SINK_PAYLOAD_DIGEST_PROPERTY: &str = "verglas.sink.payload-digest";
/// Snapshot property carrying the deterministic Parquet file identity.
pub const SINK_FILE_ID_PROPERTY: &str = "verglas.sink.file-id";
/// Snapshot property carrying the accepted row count.
pub const SINK_ROW_COUNT_PROPERTY: &str = "verglas.sink.row-count";
/// Table and snapshot property carrying the Sink owner.
pub const SINK_OWNER_PROPERTY: &str = "verglas.sink.owner";
/// Table and snapshot property carrying the immutable Parquet codec.
pub const SINK_COMPRESSION_PROPERTY: &str = "verglas.sink.compression";

const MAX_SINK_ROWS: usize = 10_000;
const MAX_SINK_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_SINK_COLUMNS: usize = 128;
const MAX_SINK_NAME_BYTES: usize = 128;

/// The codecs accepted by the Iceberg Sink contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkCompression {
    /// Zstandard compression.
    Zstd,
    /// Snappy compression.
    Snappy,
    /// Gzip compression.
    Gzip,
    /// LZ4 raw-block compression.
    Lz4,
    /// No Parquet compression.
    Uncompressed,
}

impl SinkCompression {
    /// Returns the protocol spelling of this codec.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Zstd => "zstd",
            Self::Snappy => "snappy",
            Self::Gzip => "gzip",
            Self::Lz4 => "lz4",
            Self::Uncompressed => "uncompressed",
        }
    }

    /// Converts this codec to the Parquet writer setting.
    pub(crate) fn parquet(self) -> Compression {
        match self {
            Self::Zstd => Compression::ZSTD(Default::default()),
            Self::Snappy => Compression::SNAPPY,
            Self::Gzip => Compression::GZIP(Default::default()),
            Self::Lz4 => Compression::LZ4_RAW,
            Self::Uncompressed => Compression::UNCOMPRESSED,
        }
    }
}

impl fmt::Display for SinkCompression {
    /// Renders the lowercase wire spelling.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for SinkCompression {
    type Err = String;

    /// Parses one supported codec spelling.
    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "zstd" => Ok(Self::Zstd),
            "snappy" => Ok(Self::Snappy),
            "gzip" => Ok(Self::Gzip),
            "lz4" => Ok(Self::Lz4),
            "uncompressed" => Ok(Self::Uncompressed),
            other => Err(format!(
                "unsupported Sink compression `{other}`; expected zstd, snappy, gzip, lz4, or uncompressed"
            )),
        }
    }
}

/// Immutable identity and codec configuration for one Sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkBatchConfig {
    /// The Sink resource identity and table owner.
    pub sink_id: String,
    /// The codec used for every file this Sink writes.
    pub compression: SinkCompression,
}

impl SinkBatchConfig {
    /// Creates a Sink configuration from its owner and codec.
    pub fn new(sink_id: impl Into<String>, compression: SinkCompression) -> Self {
        Self {
            sink_id: sink_id.into(),
            compression,
        }
    }
}

/// One deterministic Pipeline batch handed to the Sink commit capability.
#[derive(Debug, Clone, PartialEq)]
pub struct SinkBatchRequest {
    /// The deterministic Pipeline batch identity.
    pub batch_id: String,
    /// The digest of the complete batch payload.
    pub payload_digest: String,
    /// The deterministic Parquet file identity.
    pub file_id: String,
    /// JSON object rows selected by the Pipeline.
    pub records: Vec<Value>,
}

impl SinkBatchRequest {
    /// Creates a request with its deterministic file identity.
    pub fn new(
        batch_id: impl Into<String>,
        payload_digest: impl Into<String>,
        sink_id: &str,
        records: Vec<Value>,
    ) -> Self {
        let batch_id = batch_id.into();
        Self {
            file_id: deterministic_sink_file_id(sink_id, &batch_id),
            batch_id,
            payload_digest: payload_digest.into(),
            records,
        }
    }
}

/// Receipt returned after a Sink batch is committed or replayed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkCommitReceipt {
    /// The deterministic Pipeline batch identity.
    pub batch_id: String,
    /// The deterministic Parquet file identity.
    pub file_id: String,
    /// The committed Iceberg snapshot identity.
    pub snapshot_id: String,
    /// Number of rows accepted into the snapshot.
    pub rows_committed: u64,
    /// System spelling for the accepted row count.
    pub accepted: u64,
}

/// Returns the deterministic Sink file path for one batch.
pub fn deterministic_sink_file_id(sink_id: &str, batch_id: &str) -> String {
    format!(
        "verglas/{sink_id}/batch-{}.parquet",
        sha256_hex(batch_id.as_bytes())
    )
}

/// Commits one deterministic Pipeline batch through an Iceberg catalog.
pub async fn commit_sink_batch(
    catalog: &dyn Catalog,
    cache: &TableCache,
    ident: &TableIdent,
    config: &SinkBatchConfig,
    request: SinkBatchRequest,
) -> Result<SinkCommitReceipt> {
    validate_sink_request(config, &request)?;
    let table = ensure_sink_table(catalog, cache, ident, config, &request.records).await?;
    if let Some(snapshot) = sink_snapshot_for_batch(&table, &request.batch_id) {
        return sink_replay_receipt(snapshot, config, &request);
    }

    let table_name = ident_to_dotted(ident);
    let target = schema_to_arrow_schema(table.metadata().current_schema())?;
    let batches = rows_to_batches(&request.records, &target, &table_name)?;
    let row_count: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    if row_count == 0 {
        return Err(AgentError::InvalidRequest(
            "a Sink batch must contain at least one row".to_owned(),
        ));
    }

    let properties = HashMap::from([
        (SINK_BATCH_ID_PROPERTY.to_owned(), request.batch_id.clone()),
        (
            SINK_PAYLOAD_DIGEST_PROPERTY.to_owned(),
            request.payload_digest.clone(),
        ),
        (SINK_FILE_ID_PROPERTY.to_owned(), request.file_id.clone()),
        (SINK_ROW_COUNT_PROPERTY.to_owned(), row_count.to_string()),
        (SINK_OWNER_PROPERTY.to_owned(), config.sink_id.clone()),
        (
            SINK_COMPRESSION_PROPERTY.to_owned(),
            config.compression.to_string(),
        ),
    ]);
    let file_metadata = properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let report = write::append_batches_from_table_with_options(
        catalog,
        cache,
        &table,
        batches,
        properties,
        AppendWriteOptions {
            file_name: Some(request.file_id.clone()),
            compression: Some(config.compression.parquet()),
            file_metadata,
        },
    )
    .await?;
    let snapshot_id = report
        .snapshot_id
        .map(|id| id.to_string())
        .ok_or_else(|| AgentError::InvalidRequest("Sink append produced no snapshot".to_owned()))?;
    Ok(SinkCommitReceipt {
        batch_id: request.batch_id,
        file_id: request.file_id,
        snapshot_id,
        rows_committed: report.records_added,
        accepted: report.records_added,
    })
}

/// Validates bounded identities and row envelopes before catalog access.
fn validate_sink_request(config: &SinkBatchConfig, request: &SinkBatchRequest) -> Result<()> {
    let sink_id = config.sink_id.trim();
    if sink_id.is_empty() || sink_id.len() > MAX_SINK_NAME_BYTES || sink_id != config.sink_id {
        return Err(AgentError::InvalidRequest(
            "Sink owner must be a non-empty untrimmed name of at most 128 bytes".to_owned(),
        ));
    }
    if sink_id.contains('/') || sink_id.contains('\\') || sink_id == "." || sink_id == ".." {
        return Err(AgentError::InvalidRequest(
            "Sink owner must be one path-safe resource name".to_owned(),
        ));
    }
    if request.batch_id.trim().is_empty() || request.payload_digest.trim().is_empty() {
        return Err(AgentError::InvalidRequest(
            "Sink batch_id and payload_digest must be non-empty".to_owned(),
        ));
    }
    if request.records.is_empty() || request.records.len() > MAX_SINK_ROWS {
        return Err(AgentError::InvalidRequest(format!(
            "Sink records must contain between 1 and {MAX_SINK_ROWS} rows"
        )));
    }
    if request.records.iter().any(|record| !record.is_object()) {
        return Err(AgentError::InvalidRequest(
            "Sink records must contain JSON objects".to_owned(),
        ));
    }
    let encoded = serde_json::to_vec(&request.records).map_err(|error| {
        AgentError::InvalidRequest(format!("Sink records are not JSON: {error}"))
    })?;
    if encoded.len() > MAX_SINK_BATCH_BYTES {
        return Err(AgentError::InvalidRequest(format!(
            "Sink records exceed the {MAX_SINK_BATCH_BYTES}-byte bound"
        )));
    }
    let expected = deterministic_sink_file_id(sink_id, &request.batch_id);
    if request.file_id != expected {
        return Err(AgentError::InvalidRequest(format!(
            "file_id `{}` is not deterministic; expected `{expected}`",
            request.file_id
        )));
    }
    Ok(())
}

/// Creates and validates the unpartitioned Sink-owned table.
async fn ensure_sink_table(
    catalog: &dyn Catalog,
    cache: &TableCache,
    ident: &TableIdent,
    config: &SinkBatchConfig,
    records: &[Value],
) -> Result<Table> {
    let missing = match catalog.load_table(ident).await {
        Ok(_) => false,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
            ) =>
        {
            true
        }
        Err(error) => return Err(error.into()),
    };
    if missing {
        let schema = infer_sink_schema(records, &ident_to_dotted(ident))?;
        let properties = HashMap::from([
            (SINK_OWNER_PROPERTY.to_owned(), config.sink_id.clone()),
            (
                SINK_COMPRESSION_PROPERTY.to_owned(),
                config.compression.to_string(),
            ),
        ]);
        match write::create_table_from_schema_with_properties(catalog, ident, &schema, properties)
            .await
        {
            Ok(_) => {}
            Err(AgentError::Iceberg(error)) if error.kind() == ErrorKind::TableAlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    let table = cache.reload(catalog, ident).await?;
    let properties = table.metadata().properties();
    if properties.get(SINK_OWNER_PROPERTY) != Some(&config.sink_id) {
        return Err(AgentError::InvalidRequest(format!(
            "table `{}` is not owned by Sink `{}`",
            ident_to_dotted(ident),
            config.sink_id
        )));
    }
    if properties.get(SINK_COMPRESSION_PROPERTY) != Some(&config.compression.to_string()) {
        return Err(AgentError::InvalidRequest(format!(
            "table `{}` has a different Sink compression configuration",
            ident_to_dotted(ident)
        )));
    }
    if !table
        .metadata()
        .default_partition_spec()
        .fields()
        .is_empty()
    {
        return Err(AgentError::InvalidRequest(format!(
            "Sink table `{}` must be unpartitioned",
            ident_to_dotted(ident)
        )));
    }
    Ok(table)
}

/// Tracks one inferred top-level JSON field.
struct InferredColumn {
    /// Primitive type observed so far.
    data_type: Option<DataType>,
    /// Whether null or omission was observed.
    nullable: bool,
    /// Rows explicitly carrying this field.
    present_rows: usize,
}

/// Infers a bounded flat primitive Arrow schema from JSON objects.
fn infer_sink_schema(rows: &[Value], table_name: &str) -> Result<Arc<ArrowSchema>> {
    let mut columns: BTreeMap<String, InferredColumn> = BTreeMap::new();
    for row in rows {
        let object = row.as_object().ok_or_else(|| AgentError::SchemaMismatch {
            table: table_name.to_owned(),
            column: "<row>".to_owned(),
            detail: "a Sink row must be a JSON object".to_owned(),
        })?;
        if object.is_empty() {
            return Err(AgentError::SchemaMismatch {
                table: table_name.to_owned(),
                column: "<row>".to_owned(),
                detail: "a Sink row must contain at least one field".to_owned(),
            });
        }
        for (name, value) in object {
            if name.is_empty() || name.len() > MAX_SINK_NAME_BYTES {
                return Err(AgentError::SchemaMismatch {
                    table: table_name.to_owned(),
                    column: name.clone(),
                    detail: "column names must be between 1 and 128 bytes".to_owned(),
                });
            }
            if columns.len() == MAX_SINK_COLUMNS && !columns.contains_key(name) {
                return Err(AgentError::SchemaMismatch {
                    table: table_name.to_owned(),
                    column: name.clone(),
                    detail: format!("Sink schema exceeds the {MAX_SINK_COLUMNS}-column bound"),
                });
            }
            let value_type = sink_value_type(value, table_name, name)?;
            let column = columns.entry(name.clone()).or_insert(InferredColumn {
                data_type: None,
                nullable: false,
                present_rows: 0,
            });
            column.present_rows += 1;
            if value.is_null() {
                column.nullable = true;
            }
            if let Some(value_type) = value_type {
                column.data_type = Some(merge_sink_types(
                    column.data_type.take(),
                    value_type,
                    table_name,
                    name,
                )?);
            }
        }
    }
    let fields: Vec<Field> = columns
        .into_iter()
        .map(|(name, column)| {
            Field::new(
                name,
                column.data_type.unwrap_or(DataType::Utf8),
                column.nullable || column.present_rows != rows.len(),
            )
        })
        .collect();
    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// Returns one supported primitive type for a JSON value.
fn sink_value_type(value: &Value, table_name: &str, column: &str) -> Result<Option<DataType>> {
    match value {
        Value::Null => Ok(None),
        Value::Bool(_) => Ok(Some(DataType::Boolean)),
        Value::Number(number) if number.is_i64() || number.is_u64() => {
            if number.is_u64() && number.as_u64().is_some_and(|value| value > i64::MAX as u64) {
                Ok(Some(DataType::Float64))
            } else {
                Ok(Some(DataType::Int64))
            }
        }
        Value::Number(_) => Ok(Some(DataType::Float64)),
        Value::String(_) => Ok(Some(DataType::Utf8)),
        Value::Array(_) | Value::Object(_) => Err(AgentError::SchemaMismatch {
            table: table_name.to_owned(),
            column: column.to_owned(),
            detail: "nested arrays and objects are not supported".to_owned(),
        }),
    }
}

/// Merges two primitive inferences without silently changing their meaning.
fn merge_sink_types(
    existing: Option<DataType>,
    incoming: DataType,
    table_name: &str,
    column: &str,
) -> Result<DataType> {
    let Some(existing) = existing else {
        return Ok(incoming);
    };
    if existing == incoming {
        return Ok(existing);
    }
    if matches!(
        (&existing, &incoming),
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64)
    ) {
        return Ok(DataType::Float64);
    }
    Err(AgentError::SchemaMismatch {
        table: table_name.to_owned(),
        column: column.to_owned(),
        detail: format!("cannot infer one primitive type from {existing} and {incoming}"),
    })
}

/// Converts JSON rows to Arrow arrays without a JSON reader dependency.
fn rows_to_batches(
    rows: &[Value],
    target: &arrow_schema::Schema,
    table_name: &str,
) -> Result<Vec<RecordBatch>> {
    let objects = rows
        .iter()
        .map(|row| {
            row.as_object().ok_or_else(|| AgentError::SchemaMismatch {
                table: table_name.to_owned(),
                column: "<row>".to_owned(),
                detail: "a Sink row must be a JSON object".to_owned(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let values = objects
            .iter()
            .map(|object| object.get(field.name()))
            .collect::<Vec<_>>();
        columns.push(json_column(field, &values, table_name)?);
    }
    Ok(vec![
        RecordBatch::try_new(Arc::new(target.clone()), columns).map_err(|error| {
            AgentError::SchemaMismatch {
                table: table_name.to_owned(),
                column: "<row>".to_owned(),
                detail: error.to_string(),
            }
        })?,
    ])
}

/// Builds one Arrow primitive array from optional JSON values.
fn json_column(field: &Field, values: &[Option<&Value>], table_name: &str) -> Result<ArrayRef> {
    let mismatch = |detail: String| AgentError::SchemaMismatch {
        table: table_name.to_owned(),
        column: field.name().clone(),
        detail,
    };
    match field.data_type() {
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| value.and_then(Value::as_bool))
                .collect::<Vec<_>>(),
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| value.and_then(Value::as_i64))
                .collect::<Vec<_>>(),
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| value.and_then(Value::as_f64))
                .collect::<Vec<_>>(),
        ))),
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| value.and_then(Value::as_str))
                .collect::<Vec<_>>(),
        ))),
        other => Err(mismatch(format!("unsupported Sink column type `{other}`"))),
    }
}

/// Finds a snapshot carrying a committed batch identity.
fn sink_snapshot_for_batch(
    table: &Table,
    batch_id: &str,
) -> Option<(i64, HashMap<String, String>)> {
    table.metadata().snapshots().find_map(|snapshot| {
        let properties = &snapshot.summary().additional_properties;
        (properties.get(SINK_BATCH_ID_PROPERTY).map(String::as_str) == Some(batch_id))
            .then(|| (snapshot.snapshot_id(), properties.clone()))
    })
}

/// Validates replay properties and returns the original receipt.
fn sink_replay_receipt(
    (snapshot_id, properties): (i64, HashMap<String, String>),
    config: &SinkBatchConfig,
    request: &SinkBatchRequest,
) -> Result<SinkCommitReceipt> {
    for (key, expected) in [
        (
            SINK_PAYLOAD_DIGEST_PROPERTY,
            request.payload_digest.as_str(),
        ),
        (SINK_FILE_ID_PROPERTY, request.file_id.as_str()),
        (SINK_OWNER_PROPERTY, config.sink_id.as_str()),
        (SINK_COMPRESSION_PROPERTY, config.compression.as_str()),
    ] {
        if properties.get(key).map(String::as_str) != Some(expected) {
            return Err(AgentError::InvalidRequest(format!(
                "Sink batch `{}` was reused with a different {key}",
                request.batch_id
            )));
        }
    }
    let rows = properties
        .get(SINK_ROW_COUNT_PROPERTY)
        .ok_or_else(|| AgentError::InvalidRequest("Sink snapshot has no row count".to_owned()))?
        .parse::<u64>()
        .map_err(|_| {
            AgentError::InvalidRequest("Sink snapshot has invalid row count".to_owned())
        })?;
    Ok(SinkCommitReceipt {
        batch_id: request.batch_id.clone(),
        file_id: request.file_id.clone(),
        snapshot_id: snapshot_id.to_string(),
        rows_committed: rows,
        accepted: rows,
    })
}

/// Computes the deterministic SHA-256 hexadecimal identity used in file paths.
fn sha256_hex(input: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a_2f98,
        0x7137_4491,
        0xb5c0_fbcf,
        0xe9b5_dba5,
        0x3956_c25b,
        0x59f1_11f1,
        0x923f_82a4,
        0xab1c_5ed5,
        0xd807_aa98,
        0x1283_5b01,
        0x2431_85be,
        0x550c_7dc3,
        0x72be_5d74,
        0x80de_b1fe,
        0x9bdc_06a7,
        0xc19b_f174,
        0xe49b_69c1,
        0xefbe_4786,
        0x0fc1_9dc6,
        0x240c_a1cc,
        0x2de9_2c6f,
        0x4a74_84aa,
        0x5cb0_a9dc,
        0x76f9_88da,
        0x983e_5152,
        0xa831_c66d,
        0xb003_27c8,
        0xbf59_7fc7,
        0xc6e0_0bf3,
        0xd5a7_9147,
        0x06ca_6351,
        0x1429_2967,
        0x27b7_0a85,
        0x2e1b_2138,
        0x4d2c_6dfc,
        0x5338_0d13,
        0x650a_7354,
        0x766a_0abb,
        0x81c2_c92e,
        0x9272_2c85,
        0xa2bf_e8a1,
        0xa81a_664b,
        0xc24b_8b70,
        0xc76c_51a3,
        0xd192_e819,
        0xd699_0624,
        0xf40e_3585,
        0x106a_a070,
        0x19a4_c116,
        0x1e37_6c08,
        0x2748_774c,
        0x34b0_bcb5,
        0x391c_0cb3,
        0x4ed8_aa4a,
        0x5b9c_ca4f,
        0x682e_6ff3,
        0x748f_82ee,
        0x78a5_636f,
        0x84c8_7814,
        0x8cc7_0208,
        0x90be_fffa,
        0xa450_6ceb,
        0xbef9_a3f7,
        0xc671_78f2,
    ];
    let mut message = input.to_vec();
    let bit_length = (message.len() as u64) * 8;
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_length.to_be_bytes());
    let mut hash: [u32; 8] = [
        0x6a09_e667,
        0xbb67_ae85,
        0x3c6e_f372,
        0xa54f_f53a,
        0x510e_527f,
        0x9b05_688c,
        0x1f83_d9ab,
        0x5be0_cd19,
    ];
    for chunk in message.chunks_exact(64) {
        let mut schedule = [0u32; 64];
        for (index, word) in schedule[..16].iter_mut().enumerate() {
            let start = index * 4;
            *word = u32::from_be_bytes([
                chunk[start],
                chunk[start + 1],
                chunk[start + 2],
                chunk[start + 3],
            ]);
        }
        for index in 16..64 {
            let s0 = schedule[index - 15].rotate_right(7)
                ^ schedule[index - 15].rotate_right(18)
                ^ (schedule[index - 15] >> 3);
            let s1 = schedule[index - 2].rotate_right(17)
                ^ schedule[index - 2].rotate_right(19)
                ^ (schedule[index - 2] >> 10);
            schedule[index] = schedule[index - 16]
                .wrapping_add(s0)
                .wrapping_add(schedule[index - 7])
                .wrapping_add(s1);
        }
        let mut working = hash;
        for index in 0..64 {
            let sum1 = working[4].rotate_right(6)
                ^ working[4].rotate_right(11)
                ^ working[4].rotate_right(25);
            let choice = (working[4] & working[5]) ^ ((!working[4]) & working[6]);
            let temp1 = working[7]
                .wrapping_add(sum1)
                .wrapping_add(choice)
                .wrapping_add(K[index])
                .wrapping_add(schedule[index]);
            let sum0 = working[0].rotate_right(2)
                ^ working[0].rotate_right(13)
                ^ working[0].rotate_right(22);
            let majority =
                (working[0] & working[1]) ^ (working[0] & working[2]) ^ (working[1] & working[2]);
            let temp2 = sum0.wrapping_add(majority);
            let previous = working;
            working = [
                temp1.wrapping_add(temp2),
                previous[0],
                previous[1],
                previous[2],
                previous[3].wrapping_add(temp1),
                previous[4],
                previous[5],
                previous[6],
            ];
        }
        for (state, value) in hash.iter_mut().zip(working) {
            *state = (*state).wrapping_add(value);
        }
    }
    let mut output = String::with_capacity(64);
    for word in hash {
        use std::fmt::Write as _;
        write!(output, "{word:08x}").expect("writing String cannot fail");
    }
    output
}
