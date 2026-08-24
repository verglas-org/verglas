//! The SDK-facing table API the server serves to `@verglas/sdk`.
//!
//! `@verglas/sdk` reads and writes Iceberg tables through a small HTTP contract
//! — commit rows, read the current snapshot, page the current rows, and pull the
//! delta since a watermark. This module is the domain-neutral engine behind that
//! contract, written against a `&dyn Catalog` so it is exercised hermetically and
//! wrapped by thin admin routes in the server.
//!
//! Every write goes through [`crate::write::append_batches`], the one CAS append
//! path — JSON rows are converted to Arrow against the target table's schema and
//! committed as their own snapshot. The new snapshot id is the opaque
//! `watermark` the SDK carries as a cursor: a `delta` request returns the rows in
//! snapshots newer than the watermark it is given. Because the write path only
//! ever appends, the delta is exactly the data files present at the tip but not
//! at the `since` snapshot.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::{ArrowReaderBuilder, schema_to_arrow_schema};
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use parquet::basic::Compression;
use serde_json::Value;
pub use verglas_api::table::{
    ColumnSpec, CommitRequest, CommitResponse, CreateTableResponse, DeltaResponse,
    EnsureTableResponse, PartitionSpec, RowsResponse, SnapshotResponse,
    TableDefinition as CreateTableRequest,
};

use crate::error::{AgentError, Result};
use crate::ident::ident_to_dotted;
pub use crate::write::TableCache;
use crate::write::{self, AppendWriteOptions, PartitionField, PartitionTransform, append_batches};

/// The snapshot-summary property that records a commit's idempotency key, so a
/// replay of the same key is recognised and not written twice.
const IDEMPOTENCY_KEY_PROP: &str = "verglas.commit.idempotency-key";

/// The snapshot-summary property that records how many rows a keyed commit wrote,
/// so a replay can return the original row count without re-reading the data.
const IDEMPOTENCY_ROWS_PROP: &str = "verglas.commit.rows";

/// The property identifying the Sink that created and owns the table and snapshot.
pub const SINK_OWNER_PROPERTY: &str = "verglas.sink.owner";
/// The table property fixing the Sink's Parquet compression configuration.
pub const SINK_COMPRESSION_PROPERTY: &str = "verglas.sink.compression";
/// The snapshot property carrying the deterministic Pipeline batch identity.
pub const SINK_BATCH_ID_PROPERTY: &str = "verglas.sink.batch-id";
/// The snapshot property carrying the payload digest for a batch.
pub const SINK_PAYLOAD_DIGEST_PROPERTY: &str = "verglas.sink.payload-digest";
/// The snapshot property carrying the deterministic data-file identity.
pub const SINK_FILE_ID_PROPERTY: &str = "verglas.sink.file-id";
/// The snapshot property carrying the committed row count.
pub const SINK_ROW_COUNT_PROPERTY: &str = "verglas.sink.row-count";
/// The codecs accepted by the Iceberg Sink contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkCompression {
    /// Zstandard compression with the Parquet default level.
    Zstd,
    /// Snappy compression.
    Snappy,
    /// Gzip compression with the Parquet default level.
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

    /// Converts the protocol codec into parquet-rs's writer setting.
    fn parquet(self) -> Compression {
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
    /// Renders the lowercase protocol spelling.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for SinkCompression {
    type Err = String;

    /// Parses one of the five protocol codec spellings.
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

/// Immutable identity and codec configuration for one Iceberg Sink owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkBatchConfig {
    /// The Sink resource identity and table owner.
    pub sink_id: String,
    /// The codec used for every data file this Sink writes.
    pub compression: SinkCompression,
}

impl SinkBatchConfig {
    /// Creates a Sink configuration from an owner and validated codec.
    pub fn new(sink_id: impl Into<String>, compression: SinkCompression) -> Self {
        Self {
            sink_id: sink_id.into(),
            compression,
        }
    }
}

/// One deterministic Pipeline batch handed to the Sink-owned commit engine.
#[derive(Debug, Clone, PartialEq)]
pub struct SinkBatchRequest {
    /// The deterministic Pipeline batch identity.
    pub batch_id: String,
    /// The digest of the complete batch payload.
    pub payload_digest: String,
    /// The deterministic data-file identity derived from the batch and Sink.
    pub file_id: String,
    /// JSON object rows selected by the Pipeline.
    pub records: Vec<Value>,
}

impl SinkBatchRequest {
    /// Creates a request after deriving its deterministic file identity.
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

/// The receipt returned after a Sink batch is committed or replayed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkCommitReceipt {
    /// The deterministic Pipeline batch identity.
    pub batch_id: String,
    /// The deterministic data-file identity.
    pub file_id: String,
    /// The committed Iceberg snapshot id.
    pub snapshot_id: String,
    /// Number of rows accepted into the snapshot.
    pub rows_committed: u64,
    /// The system Sink receipt spelling for the same accepted row count.
    pub accepted: u64,
}

const MAX_SINK_ROWS: usize = 10_000;
const MAX_SINK_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_SINK_COLUMNS: usize = 128;
const MAX_SINK_NAME_BYTES: usize = 128;

/// Returns the deterministic protocol file id for `sink_id` and `batch_id`.
pub fn deterministic_sink_file_id(sink_id: &str, batch_id: &str) -> String {
    format!(
        "verglas/{sink_id}/batch-{}.parquet",
        sha256_hex(batch_id.as_bytes())
    )
}

/// Computes SHA-256 without widening the crate's dependency surface for one
/// deterministic object name. The implementation follows FIPS 180-4's
/// 512-bit block schedule and big-endian digest encoding.
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
        let _ = write!(output, "{word:08x}");
    }
    output
}

/// Commits one Pipeline batch through the Sink-owned Iceberg path.
///
/// A missing table is created from a bounded top-level JSON-object inference and
/// stamped with the Sink owner and codec. Existing tables must carry both
/// properties for this Sink. Batch identity is resolved from snapshot summaries
/// before any write; a matching identity returns its original receipt, while a
/// changed digest, file, or configuration is rejected. New rows use the normal
/// schema-aware JSON-to-Arrow conversion and the existing CAS append path with
/// only the deterministic file and codec controls supplied by the Sink.
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
        return Err(AgentError::TableApi(
            "a Sink batch must contain at least one row".to_owned(),
        ));
    }

    let mut snapshot_properties = HashMap::new();
    snapshot_properties.insert(SINK_BATCH_ID_PROPERTY.to_owned(), request.batch_id.clone());
    snapshot_properties.insert(
        SINK_PAYLOAD_DIGEST_PROPERTY.to_owned(),
        request.payload_digest.clone(),
    );
    snapshot_properties.insert(SINK_FILE_ID_PROPERTY.to_owned(), request.file_id.clone());
    snapshot_properties.insert(SINK_ROW_COUNT_PROPERTY.to_owned(), row_count.to_string());
    snapshot_properties.insert(SINK_OWNER_PROPERTY.to_owned(), config.sink_id.clone());
    snapshot_properties.insert(
        SINK_COMPRESSION_PROPERTY.to_owned(),
        config.compression.to_string(),
    );

    let file_metadata = snapshot_properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let options = AppendWriteOptions {
        file_name: Some(request.file_id.clone()),
        compression: Some(config.compression.parquet()),
        file_metadata,
    };
    let report = write::append_batches_from_table_with_options(
        catalog,
        cache,
        &table,
        batches,
        snapshot_properties,
        options,
    )
    .await?;
    let snapshot_id = report
        .snapshot_id
        .map(|id| id.to_string())
        .ok_or_else(|| AgentError::TableApi("Sink append did not produce a snapshot".to_owned()))?;
    Ok(SinkCommitReceipt {
        batch_id: request.batch_id,
        file_id: request.file_id,
        snapshot_id,
        rows_committed: report.records_added,
        accepted: report.records_added,
    })
}

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

/// Same contract as [`commit`], but resolves the table through `cache` (see
/// [`TableCache`]) instead of an unconditional `catalog.load_table` — the
/// warm-append path the server's commit route takes.
pub async fn commit_cached(
    catalog: &dyn Catalog,
    cache: &TableCache,
    ident: &TableIdent,
    request: CommitRequest,
) -> Result<CommitResponse> {
    let table = cache.get_or_load(catalog, ident).await?;
    let table_name = ident_to_dotted(ident);

    if let Some(key) = request.idempotency_key.as_deref()
        && let Some(existing) = find_idempotent_commit(&table, key)
    {
        return Ok(existing);
    }

    let target = schema_to_arrow_schema(table.metadata().current_schema())?;
    let batches = rows_to_batches(&request.rows, &target, &table_name)?;

    let mut properties = HashMap::new();
    if let Some(key) = request.idempotency_key.as_deref() {
        let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
        properties.insert(IDEMPOTENCY_KEY_PROP.to_owned(), key.to_owned());
        properties.insert(IDEMPOTENCY_ROWS_PROP.to_owned(), row_count.to_string());
    }

    let report =
        write::append_batches_from_table(catalog, cache, &table, batches, properties).await?;
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

/// Same contract as [`commit_batches`], but resolves the table through
/// `cache` (see [`TableCache`]) instead of an unconditional
/// `catalog.load_table` — the warm-append path the server's ingest route
/// takes for a keyed commit.
pub async fn commit_batches_cached(
    catalog: &dyn Catalog,
    cache: &TableCache,
    ident: &TableIdent,
    batches: Vec<RecordBatch>,
    idempotency_key: Option<String>,
) -> Result<CommitResponse> {
    let table = cache.get_or_load(catalog, ident).await?;
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

    let report =
        write::append_batches_from_table(catalog, cache, &table, batches, properties).await?;
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
    create_table_with_properties(catalog, ident, request, HashMap::new()).await
}

/// Creates a table with immutable-at-create metadata properties for resource owners.
pub async fn create_table_with_properties(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    request: CreateTableRequest,
    properties: HashMap<String, String>,
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

    let report = write::create_table_with_partitions_and_properties(
        catalog,
        ident,
        &schema,
        &partitions,
        properties,
    )
    .await?;
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
        "list<float32>" | "list<float>" => Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Float32,
            true,
        )))),
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

/// Validates the bounded identity and row envelope before touching the catalog.
fn validate_sink_request(config: &SinkBatchConfig, request: &SinkBatchRequest) -> Result<()> {
    let sink_id = config.sink_id.trim();
    if sink_id.is_empty() || sink_id.len() > MAX_SINK_NAME_BYTES || sink_id != config.sink_id {
        return Err(AgentError::TableApi(
            "Sink owner must be a non-empty untrimmed name of at most 128 bytes".to_owned(),
        ));
    }
    if sink_id.contains('/') || sink_id.contains('\\') || sink_id == "." || sink_id == ".." {
        return Err(AgentError::TableApi(
            "Sink owner must be one path-safe resource name".to_owned(),
        ));
    }
    if request.batch_id.trim().is_empty() {
        return Err(AgentError::TableApi(
            "Sink batch_id must be non-empty".to_owned(),
        ));
    }
    if request.payload_digest.trim().is_empty() {
        return Err(AgentError::TableApi(
            "Sink payload_digest must be non-empty".to_owned(),
        ));
    }
    if request.records.is_empty() || request.records.len() > MAX_SINK_ROWS {
        return Err(AgentError::TableApi(format!(
            "Sink records must contain between 1 and {MAX_SINK_ROWS} rows"
        )));
    }
    if request.records.iter().any(|record| !record.is_object()) {
        return Err(AgentError::TableApi(
            "Sink records must contain JSON objects".to_owned(),
        ));
    }
    let encoded = serde_json::to_vec(&request.records)
        .map_err(|error| AgentError::TableApi(format!("Sink records are not JSON: {error}")))?;
    if encoded.len() > MAX_SINK_BATCH_BYTES {
        return Err(AgentError::TableApi(format!(
            "Sink records exceed the {MAX_SINK_BATCH_BYTES}-byte bound"
        )));
    }
    let expected_file = deterministic_sink_file_id(sink_id, &request.batch_id);
    if request.file_id != expected_file {
        return Err(AgentError::TableApi(format!(
            "file_id `{}` is not the deterministic file for this batch; expected `{expected_file}`",
            request.file_id
        )));
    }
    Ok(())
}

/// Ensures a Sink-owned table exists, then validates its immutable properties.
async fn ensure_sink_table(
    catalog: &dyn Catalog,
    cache: &TableCache,
    ident: &TableIdent,
    config: &SinkBatchConfig,
    records: &[Value],
) -> Result<Table> {
    let table_missing = match catalog.load_table(ident).await {
        Ok(_) => false,
        Err(error)
            if matches!(
                error.kind(),
                iceberg::ErrorKind::TableNotFound | iceberg::ErrorKind::NamespaceNotFound
            ) =>
        {
            true
        }
        Err(error) => return Err(error.into()),
    };
    if table_missing {
        let schema = infer_sink_schema(records, &ident_to_dotted(ident))?;
        let properties = HashMap::from([
            (SINK_OWNER_PROPERTY.to_owned(), config.sink_id.clone()),
            (
                SINK_COMPRESSION_PROPERTY.to_owned(),
                config.compression.to_string(),
            ),
        ]);
        match write::create_table_with_partitions_and_properties(
            catalog,
            ident,
            &schema,
            &[],
            properties,
        )
        .await
        {
            Ok(_) => {}
            Err(AgentError::Iceberg(error))
                if error.kind() == iceberg::ErrorKind::TableAlreadyExists => {}
            Err(error) => return Err(error),
        }
    }

    let table = cache.reload(catalog, ident).await?;
    let properties = table.metadata().properties();
    match properties.get(SINK_OWNER_PROPERTY) {
        Some(owner) if owner == &config.sink_id => {}
        Some(owner) => {
            return Err(AgentError::TableApi(format!(
                "table `{}` is owned by Sink `{owner}`, not `{}`",
                ident_to_dotted(ident),
                config.sink_id
            )));
        }
        None => {
            return Err(AgentError::TableApi(format!(
                "table `{}` is not owned by a Sink",
                ident_to_dotted(ident)
            )));
        }
    }
    if properties.get(SINK_COMPRESSION_PROPERTY) != Some(&config.compression.to_string()) {
        return Err(AgentError::TableApi(format!(
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
        return Err(AgentError::TableApi(format!(
            "Sink table `{}` must be unpartitioned",
            ident_to_dotted(ident)
        )));
    }
    Ok(table)
}

/// The in-memory state accumulated while inferring one top-level JSON column.
struct InferredColumn {
    /// The primitive type observed so far, if any non-null value was present.
    data_type: Option<DataType>,
    /// Whether a row omitted or explicitly nulled this column.
    nullable: bool,
    /// Number of rows that explicitly carried this column.
    present_rows: usize,
}

/// Infers a bounded flat Arrow schema from JSON object rows.
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

    let fields = columns
        .into_iter()
        .map(|(name, column)| {
            let data_type = column.data_type.unwrap_or(DataType::Utf8);
            let nullable = column.nullable || column.present_rows != rows.len();
            Field::new(name, data_type, nullable)
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// Infers one supported primitive type from a JSON value.
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
            detail: "nested arrays and objects are not supported by bounded Sink inference"
                .to_owned(),
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

/// Returns a previously committed Sink snapshot for `batch_id` and its summary.
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

/// Validates a replay summary and returns its original receipt.
fn sink_replay_receipt(
    (snapshot_id, properties): (i64, HashMap<String, String>),
    config: &SinkBatchConfig,
    request: &SinkBatchRequest,
) -> Result<SinkCommitReceipt> {
    let expected = [
        (
            SINK_PAYLOAD_DIGEST_PROPERTY,
            request.payload_digest.as_str(),
        ),
        (SINK_FILE_ID_PROPERTY, request.file_id.as_str()),
        (SINK_OWNER_PROPERTY, config.sink_id.as_str()),
        (SINK_COMPRESSION_PROPERTY, config.compression.as_str()),
    ];
    for (key, value) in expected {
        if properties.get(key).map(String::as_str) != Some(value) {
            return Err(AgentError::TableApi(format!(
                "Sink batch `{}` was reused with a different {key}",
                request.batch_id
            )));
        }
    }
    let rows_committed = properties
        .get(SINK_ROW_COUNT_PROPERTY)
        .ok_or_else(|| AgentError::TableApi("Sink snapshot is missing its row count".to_owned()))?
        .parse::<u64>()
        .map_err(|_| AgentError::TableApi("Sink snapshot has an invalid row count".to_owned()))?;
    Ok(SinkCommitReceipt {
        batch_id: request.batch_id.clone(),
        file_id: request.file_id.clone(),
        snapshot_id: snapshot_id.to_string(),
        rows_committed,
        accepted: rows_committed,
    })
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

/// Returns every current-table row in committed snapshot order, oldest first.
///
/// Append-only semantic tables use this to resolve last-write-wins updates and
/// tombstones without relying on file-path order. Each snapshot contributes
/// only files introduced at that commit, so concurrent commits remain ordered
/// by the catalog's committed parent lineage.
pub async fn rows_in_commit_order(catalog: &dyn Catalog, ident: &TableIdent) -> Result<Vec<Value>> {
    let table = catalog.load_table(ident).await?;
    let mut lineage = Vec::new();
    let mut cursor = table.metadata().current_snapshot_id();
    while let Some(id) = cursor {
        let snapshot = table
            .metadata()
            .snapshot_by_id(id)
            .ok_or_else(|| AgentError::TableApi(format!("snapshot {id} is absent")))?;
        lineage.push((id, snapshot.parent_snapshot_id()));
        cursor = snapshot.parent_snapshot_id();
    }
    lineage.reverse();
    let mut rows = Vec::new();
    for (id, parent) in lineage {
        let tasks = planned_files(&table, Some(id)).await?;
        let parent_files = match parent {
            Some(parent) => planned_files(&table, Some(parent))
                .await?
                .into_iter()
                .map(|task| task.data_file_path)
                .collect::<HashSet<_>>(),
            None => HashSet::new(),
        };
        let added = tasks
            .into_iter()
            .filter(|task| !parent_files.contains(&task.data_file_path))
            .collect();
        rows.extend(read_tasks_to_json(&table, added).await?);
    }
    Ok(rows)
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
    let runtime = iceberg::Runtime::try_current()?;
    let reader = ArrowReaderBuilder::new(table.file_io().clone(), runtime).build();
    let stream = futures::stream::iter(tasks.into_iter().map(Ok)).boxed();
    let batches: Vec<RecordBatch> = reader.read(stream)?.stream().try_collect().await?;
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
