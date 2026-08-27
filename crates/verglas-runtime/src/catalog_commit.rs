//! Stateless Iceberg proposal writing for the Catalog product.
//!
//! The guest sends one bounded operation envelope. This host capability uses a
//! host-owned storage factory to write Parquet, manifest, and metadata objects,
//! then returns the complete immutable proposal for SQLite to publish. It does
//! not retain or advance a Catalog head.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};
use iceberg::spec::{
    DataFile, FormatVersion, ManifestFile, ManifestList, ManifestListWriter, Snapshot, Summary,
    TableMetadata, TableMetadataBuilder,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    MetadataLocation, Runtime, TableCommit, TableCreation, TableIdent, TableRequirement,
    TableUpdate,
};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use verglas_do_wasm::{HostError, Request, Response};
use verglas_iceberg::{
    SINK_BATCH_ID_PROPERTY, SINK_COMPRESSION_PROPERTY, SINK_FILE_ID_PROPERTY, SINK_OWNER_PROPERTY,
    SINK_PAYLOAD_DIGEST_PROPERTY, SINK_ROW_COUNT_PROPERTY, SinkCompression,
    deterministic_sink_file_id,
};

/// Hard ceiling shared with the Catalog product's frozen commit envelope.
pub const MAX_CATALOG_COMMIT_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Maximum encoded body size for a client-facing capability error.
const MAX_CATALOG_ERROR_BODY_BYTES: usize = 16 * 1024;

/// A capability failure that can be returned directly to the Catalog or caller.
enum CommitFailure {
    /// A malformed request or rejected request value.
    BadRequest(String),
    /// A valid request whose Iceberg compare-and-swap requirement failed.
    Conflict(String),
    /// A real host backend failure that must cross the capability boundary.
    Host(HostError),
}

/// Result type used while constructing one Catalog proposal.
type CommitResult<T> = Result<T, CommitFailure>;

/// Creates a bounded client request failure.
fn bad_request(message: impl Into<String>) -> CommitFailure {
    CommitFailure::BadRequest(message.into())
}

/// Creates a bounded Iceberg conflict failure.
fn conflict(message: impl Into<String>) -> CommitFailure {
    CommitFailure::Conflict(message.into())
}

/// Converts a host failure without turning it into a client response.
fn host_failure(error: HostError) -> CommitFailure {
    CommitFailure::Host(error)
}

/// Converts one Iceberg error to a client status or real host failure.
fn iceberg_failure(error: iceberg::Error) -> CommitFailure {
    match error.kind() {
        iceberg::ErrorKind::CatalogCommitConflicts => conflict(error.to_string()),
        iceberg::ErrorKind::Unexpected => host_failure(backend(error)),
        _ => bad_request(error.to_string()),
    }
}

/// Builds a bounded JSON HTTP error response for a rejected request.
fn error_response(status: u16, message: impl Into<String>) -> Response {
    let message: String = message.into().chars().take(4096).collect();
    let body = serde_json::to_vec(&json!({
        "error": {"message": message},
    }))
    .ok()
    .filter(|body| body.len() <= MAX_CATALOG_ERROR_BODY_BYTES)
    .unwrap_or_else(|| br#"{"error":{"message":"Catalog commit request failed"}}"#.to_vec());
    Response {
        status,
        headers: vec![("content-type".to_owned(), "application/json".to_owned())],
        body,
        accept_ws: None,
    }
}

/// Object-safe service intercepted before ordinary Durable Object routing.
#[async_trait]
pub trait CatalogCommitService: Send + Sync {
    /// Validates and writes one exact Catalog proposal.
    async fn commit(&self, request: Request) -> Result<Response, HostError>;
}

/// Immutable host-owned destination and Sink identity fence.
#[derive(Clone, Debug)]
pub struct CatalogCommitServiceConfig {
    sink_id: String,
    bucket: String,
    namespace: String,
    table: String,
    compression: SinkCompression,
    warehouse: Option<String>,
}

impl CatalogCommitServiceConfig {
    /// Creates the destination and owner fence used by the capability.
    pub fn new(
        sink_id: impl Into<String>,
        bucket: impl Into<String>,
        namespace: impl Into<String>,
        table: impl Into<String>,
        compression: SinkCompression,
    ) -> Self {
        Self {
            sink_id: sink_id.into(),
            bucket: bucket.into(),
            namespace: namespace.into(),
            table: table.into(),
            compression,
            warehouse: None,
        }
    }

    /// Pins table creation and initial Sink proposals to one host warehouse.
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }
}

/// Stateless proposal writer over a host-owned [`StorageFactory`].
pub struct IcebergCatalogCommitService {
    storage_factory: Arc<dyn StorageFactory>,
    config: CatalogCommitServiceConfig,
}

impl IcebergCatalogCommitService {
    /// Creates a proposal writer without a Catalog or mutable table head.
    pub fn new(
        storage_factory: Arc<dyn StorageFactory>,
        config: CatalogCommitServiceConfig,
    ) -> Self {
        Self {
            storage_factory,
            config,
        }
    }

    /// Builds one request-scoped FileIO from the immutable host factory.
    fn file_io(&self) -> FileIO {
        FileIOBuilder::new(Arc::clone(&self.storage_factory)).build()
    }
}

/// The operations admitted by the private Catalog capability.
#[derive(Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
enum CommitOperation {
    /// Writes initial table metadata and returns its metadata proposal.
    #[serde(rename = "create-table")]
    CreateTable {
        warehouse: String,
        namespace: Vec<String>,
        request: Box<CreateTableRequest>,
    },
    /// Writes a Sink data file and returns the next metadata proposal.
    #[serde(rename = "commit-sink-batch")]
    CommitSinkBatch {
        #[serde(default)]
        current_metadata_location: Option<String>,
        request: Box<SinkRequest>,
    },
    /// Applies standard Iceberg requirements and updates to current metadata.
    #[serde(rename = "commit-table")]
    CommitTable {
        current_metadata_location: String,
        request_json: String,
    },
    /// Reads and validates an existing metadata file for table registration.
    #[serde(rename = "register-table")]
    RegisterTable { metadata_location: String },
}

/// Standard Iceberg REST table commit fields preserved by the Catalog product.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableCommitRequest {
    identifier: TableIdent,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

/// Iceberg REST table creation fields consumed by the proposal writer.
#[derive(Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct CreateTableRequest {
    name: String,
    schema: iceberg::spec::Schema,
    #[serde(default)]
    location: Option<String>,
    #[serde(default)]
    partition_spec: Option<iceberg::spec::UnboundPartitionSpec>,
    #[serde(default, rename = "write-order")]
    write_order: Option<iceberg::spec::SortOrder>,
    #[serde(default)]
    properties: HashMap<String, String>,
    #[serde(default)]
    format_version: Option<FormatVersion>,
    #[serde(default)]
    stage_create: bool,
}

/// Frozen Sink fields carried inside a proposal operation.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SinkRequest {
    batch_id: String,
    file_id: String,
    sink_id: String,
    pipeline_id: String,
    sql_digest: String,
    source: String,
    first_sequence: u64,
    last_sequence: u64,
    bucket: String,
    namespace: String,
    table: String,
    format: String,
    compression: String,
    roll_interval_seconds: u64,
    roll_size_bytes: u64,
    records: Vec<Value>,
}

impl SinkRequest {
    /// Applies host destination, identity, and bounded-envelope checks.
    fn validate(&self, config: &CatalogCommitServiceConfig) -> CommitResult<()> {
        if self.sink_id != config.sink_id
            || self.bucket != config.bucket
            || self.namespace != config.namespace
            || self.table != config.table
        {
            return Err(bad_request(
                "Catalog commit destination does not match host configuration",
            ));
        }
        if self.format != "parquet" || self.compression != config.compression.as_str() {
            return Err(bad_request(
                "Catalog commit format or compression does not match host configuration",
            ));
        }
        if self.file_id != deterministic_sink_file_id(&self.sink_id, &self.batch_id) {
            return Err(bad_request(
                "Catalog commit file identity is not deterministic",
            ));
        }
        if self.pipeline_id.trim().is_empty()
            || self.source.trim().is_empty()
            || self.sql_digest.len() != 64
            || self.sql_digest != self.sql_digest.to_ascii_lowercase()
            || !self.sql_digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            || self.first_sequence == 0
            || self.last_sequence == 0
            || self.first_sequence > self.last_sequence
            || !(60..=24 * 60 * 60).contains(&self.roll_interval_seconds)
            || !(1..=512 * 1024 * 1024).contains(&self.roll_size_bytes)
            || self.records.is_empty()
            || self.records.len() > MAX_SINK_ROWS
        {
            return Err(bad_request("Catalog commit envelope is invalid"));
        }
        Ok(())
    }
}

/// The Sink row ceiling shared by the product and host capability.
const MAX_SINK_ROWS: usize = 10_000;

/// A generated Parquet data file and its accepted row count.
struct DataFileProposal {
    /// The Iceberg data-file descriptor.
    data_file: DataFile,
    /// Number of rows encoded into the file.
    row_count: u64,
}

/// Returns one case-insensitive request header value.
fn header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request
        .headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// Returns the path portion of a relative or absolute URI.
fn request_path(uri: &str) -> &str {
    let path = if let Some((_, authority_and_path)) = uri.split_once("://") {
        authority_and_path
            .find('/')
            .map_or("/", |index| &authority_and_path[index..])
    } else {
        uri
    };
    path.split('?').next().map_or(path, |value| value)
}

/// Verifies the identity headers that authorize one Sink proposal.
fn validate_sink_headers(request: &Request, sink: &SinkRequest) -> CommitResult<()> {
    for (name, expected) in [
        ("x-verglas-sink-id", sink.sink_id.as_str()),
        ("x-verglas-batch-id", sink.batch_id.as_str()),
        ("x-verglas-file-id", sink.file_id.as_str()),
        ("x-verglas-pipeline-id", sink.pipeline_id.as_str()),
        ("x-verglas-sql-digest", sink.sql_digest.as_str()),
    ] {
        if header(request, name) != Some(expected) {
            return Err(bad_request(format!(
                "Catalog commit header {name} does not match its envelope"
            )));
        }
    }
    Ok(())
}

/// Converts an Iceberg error into the host capability error shape.
fn backend<E: std::fmt::Display>(error: E) -> HostError {
    HostError::backend(error.to_string())
}

/// Validates one namespace or table path component without imposing the Sink fence.
fn validate_path_component(component: &str, label: &str) -> CommitResult<()> {
    if component.is_empty()
        || component.len() > 128
        || component == "."
        || component == ".."
        || component.contains(['/', '\\'])
        || component
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || b"._-".contains(&byte)))
    {
        return Err(bad_request(format!(
            "Catalog {label} path component is invalid"
        )));
    }
    Ok(())
}

/// Validates a public REST namespace without applying the configured Sink identity.
fn validate_namespace(namespace: &[String]) -> CommitResult<()> {
    if namespace.is_empty() {
        return Err(bad_request("Catalog namespace must not be empty"));
    }
    for segment in namespace {
        validate_path_component(segment, "namespace")?;
    }
    Ok(())
}

/// Joins a warehouse URI or filesystem root with an Iceberg table path.
fn table_location(warehouse: &str, namespace: &[String], table: &str) -> String {
    format!(
        "{}/{}/{}",
        warehouse.trim_end_matches('/'),
        namespace.join("/"),
        table
    )
}

/// Splits the configured dotted namespace into its storage path segments.
fn configured_namespace_segments(namespace: &str) -> Vec<String> {
    namespace.split('.').map(ToOwned::to_owned).collect()
}

/// Returns the host-pinned warehouse required for metadata proposals.
fn host_warehouse(config: &CatalogCommitServiceConfig) -> CommitResult<&str> {
    let warehouse = config
        .warehouse
        .as_deref()
        .ok_or_else(|| bad_request("Catalog proposals require a host warehouse"))?;
    if warehouse.trim().is_empty() || warehouse.len() > 1024 {
        return Err(bad_request("Catalog warehouse is invalid"));
    }
    Ok(warehouse)
}

/// Returns the only table-location root accepted for Sink proposals.
fn sink_table_root(config: &CatalogCommitServiceConfig) -> CommitResult<String> {
    let warehouse = host_warehouse(config)?;
    let namespace = configured_namespace_segments(&config.namespace);
    validate_namespace(&namespace)?;
    validate_path_component(&config.table, "table")?;
    Ok(table_location(warehouse, &namespace, &config.table))
}

/// Checks that a location stays below a warehouse without dot-segment escapes.
fn location_under_warehouse(location: &str, warehouse: &str) -> bool {
    let prefix = format!("{}/", warehouse.trim_end_matches('/'));
    location.strip_prefix(&prefix).is_some_and(|relative| {
        !relative.is_empty()
            && !relative.contains(['\\', '?', '#'])
            && !relative.chars().any(char::is_control)
            && relative
                .split('/')
                .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
    })
}

/// Validates and bounds one metadata location before any host storage access.
fn validate_metadata_location(location: &str, warehouse: &str, message: &str) -> CommitResult<()> {
    if location.is_empty()
        || location.len() > 2048
        || !location_under_warehouse(location, warehouse)
    {
        return Err(bad_request(message));
    }
    Ok(())
}

/// Validates the operation warehouse against the optional host pin.
fn validate_warehouse(warehouse: &str, configured_warehouse: Option<&String>) -> CommitResult<()> {
    if warehouse.trim().is_empty() || warehouse.len() > 1024 {
        return Err(bad_request("Catalog warehouse is invalid"));
    }
    if configured_warehouse.is_some_and(|expected| expected != warehouse) {
        return Err(bad_request(
            "Catalog warehouse does not match host configuration",
        ));
    }
    Ok(())
}

/// Returns a location under the supplied warehouse, rejecting an escape.
fn requested_table_location(
    warehouse: &str,
    namespace: &[String],
    request: &CreateTableRequest,
) -> CommitResult<String> {
    let expected = table_location(warehouse, namespace, &request.name);
    let location = request.location.as_deref().unwrap_or(&expected);
    validate_metadata_location(
        location,
        warehouse,
        "Catalog table location must remain under its warehouse",
    )?;
    Ok(location.to_owned())
}

/// Returns the metadata JSON and its complete metadata-file location.
fn metadata_response(metadata_location: &str, metadata: &TableMetadata) -> CommitResult<Value> {
    Ok(json!({
        "metadata-location": metadata_location,
        "metadata": serde_json::to_value(metadata)
            .map_err(|error| bad_request(error.to_string()))?,
    }))
}

/// Converts a request into one immutable table metadata file.
async fn create_table_proposal(
    file_io: &FileIO,
    config: &CatalogCommitServiceConfig,
    warehouse: String,
    namespace: Vec<String>,
    request: CreateTableRequest,
) -> CommitResult<Value> {
    validate_warehouse(&warehouse, config.warehouse.as_ref())?;
    validate_namespace(&namespace)?;
    validate_path_component(&request.name, "table")?;
    if request.stage_create {
        return Err(bad_request(
            "Catalog stage-create requests are not supported",
        ));
    }
    let location = requested_table_location(&warehouse, &namespace, &request)?;
    let creation = TableCreation::builder()
        .name(request.name)
        .location(location)
        .schema(request.schema)
        .partition_spec(request.partition_spec.unwrap_or_default())
        .sort_order(
            request
                .write_order
                .unwrap_or_else(iceberg::spec::SortOrder::unsorted_order),
        )
        .properties(request.properties)
        .format_version(request.format_version.unwrap_or(FormatVersion::V2))
        .build();
    let metadata = TableMetadataBuilder::from_table_creation(creation)
        .map_err(iceberg_failure)?
        .build()
        .map_err(iceberg_failure)?
        .metadata;
    let metadata_location = MetadataLocation::new_with_metadata(metadata.location(), &metadata);
    let metadata_location_text = metadata_location.to_string();
    metadata
        .write_to(file_io, &metadata_location)
        .await
        .map_err(iceberg_failure)?;
    metadata_response(&metadata_location_text, &metadata)
}

/// Returns the compression requested by the Sink as a Parquet codec.
fn parquet_compression(compression: SinkCompression) -> Compression {
    match compression {
        SinkCompression::Zstd => Compression::ZSTD(Default::default()),
        SinkCompression::Snappy => Compression::SNAPPY,
        SinkCompression::Gzip => Compression::GZIP(Default::default()),
        SinkCompression::Lz4 => Compression::LZ4_RAW,
        SinkCompression::Uncompressed => Compression::UNCOMPRESSED,
    }
}

/// Infers one primitive Arrow schema from JSON Sink records.
fn infer_arrow_schema(records: &[Value], table_name: &str) -> CommitResult<Arc<ArrowSchema>> {
    if records.is_empty() {
        return Err(bad_request("Catalog Sink batch must contain rows"));
    }
    let mut columns: BTreeMap<String, InferredColumn> = BTreeMap::new();
    for record in records {
        let object = record
            .as_object()
            .ok_or_else(|| bad_request("Catalog Sink rows must be JSON objects"))?;
        for (name, value) in object {
            if name.is_empty() || name.len() > 128 {
                return Err(bad_request("Catalog Sink column name is invalid"));
            }
            let entry = columns.entry(name.clone()).or_default();
            entry.present_rows += 1;
            if value.is_null() {
                entry.nullable = true;
            } else {
                entry.data_type = Some(merge_data_type(
                    entry.data_type.take(),
                    json_data_type(value, table_name, name)?,
                    table_name,
                    name,
                )?);
            }
        }
    }
    if columns.is_empty() {
        return Err(bad_request("Catalog Sink rows contain no columns"));
    }
    let row_count = records.len();
    let fields: Vec<Field> = columns
        .into_iter()
        .map(|(name, column)| {
            Field::new(
                name,
                column.data_type.unwrap_or(DataType::Utf8),
                column.nullable || column.present_rows != row_count,
            )
        })
        .collect();
    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// Tracks one inferred primitive Sink column.
#[derive(Default)]
struct InferredColumn {
    /// The merged primitive type, if any non-null value was seen.
    data_type: Option<DataType>,
    /// Whether any source row explicitly contained null.
    nullable: bool,
    /// Number of rows that contained this column.
    present_rows: usize,
}

/// Returns the Arrow type represented by one JSON value.
fn json_data_type(value: &Value, _table_name: &str, _column: &str) -> CommitResult<DataType> {
    match value {
        Value::Bool(_) => Ok(DataType::Boolean),
        Value::Number(number) if number.is_i64() || number.is_u64() => Ok(DataType::Int64),
        Value::Number(_) => Ok(DataType::Float64),
        Value::String(_) => Ok(DataType::Utf8),
        Value::Null => Err(bad_request("null has no primitive Sink type")),
        Value::Array(_) | Value::Object(_) => Err(bad_request(
            "Catalog Sink rows support only primitive JSON values",
        )),
    }
}

/// Merges numeric widening while rejecting semantic type changes.
fn merge_data_type(
    current: Option<DataType>,
    incoming: DataType,
    _table_name: &str,
    _column: &str,
) -> CommitResult<DataType> {
    let Some(current) = current else {
        return Ok(incoming);
    };
    if current == incoming {
        return Ok(current);
    }
    if matches!(
        (&current, &incoming),
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64)
    ) {
        return Ok(DataType::Float64);
    }
    Err(bad_request(format!(
        "Catalog Sink column type changed from {current} to {incoming}"
    )))
}

/// Converts JSON records to Arrow batches using the target table schema.
fn rows_to_batches(records: &[Value], target: &Arc<ArrowSchema>) -> CommitResult<Vec<RecordBatch>> {
    let objects = records
        .iter()
        .map(|record| {
            record
                .as_object()
                .ok_or_else(|| bad_request("Catalog Sink rows must be JSON objects"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for object in &objects {
        for name in object.keys() {
            if target.field_with_name(name).is_err() {
                return Err(bad_request(format!(
                    "Catalog Sink row contains unknown column `{name}`"
                )));
            }
        }
    }
    for field in target.fields() {
        if !field.is_nullable()
            && objects
                .iter()
                .any(|object| object.get(field.name()).is_none_or(Value::is_null))
        {
            return Err(bad_request(format!(
                "Catalog Sink row is missing required column `{}`",
                field.name()
            )));
        }
    }
    let columns = target
        .fields()
        .iter()
        .map(|field| {
            let values = objects
                .iter()
                .map(|object| object.get(field.name()))
                .collect::<Vec<_>>();
            json_column(field, &values)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(vec![
        RecordBatch::try_new(Arc::clone(target), columns)
            .map_err(|error| bad_request(error.to_string()))?,
    ])
}

/// Converts one target column's JSON values into an Arrow array.
fn json_column(field: &Field, values: &[Option<&Value>]) -> CommitResult<ArrayRef> {
    match field.data_type() {
        DataType::Boolean => values
            .iter()
            .map(|value| {
                value.map_or(Ok(None), |value| {
                    if value.is_null() {
                        Ok(None)
                    } else {
                        value
                            .as_bool()
                            .ok_or_else(|| bad_request("Sink boolean value is invalid"))
                            .map(Some)
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|values| Arc::new(BooleanArray::from(values)) as ArrayRef),
        DataType::Int64 => values
            .iter()
            .map(|value| {
                value.map_or(Ok(None), |value| {
                    if value.is_null() {
                        Ok(None)
                    } else {
                        value
                            .as_i64()
                            .ok_or_else(|| bad_request("Sink integer value is invalid"))
                            .map(Some)
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|values| Arc::new(Int64Array::from(values)) as ArrayRef),
        DataType::Float64 => values
            .iter()
            .map(|value| {
                value.map_or(Ok(None), |value| {
                    if value.is_null() {
                        Ok(None)
                    } else {
                        value
                            .as_f64()
                            .or_else(|| value.as_i64().map(|integer| integer as f64))
                            .ok_or_else(|| bad_request("Sink float value is invalid"))
                            .map(Some)
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|values| Arc::new(Float64Array::from(values)) as ArrayRef),
        DataType::Utf8 => values
            .iter()
            .map(|value| {
                value.map_or(Ok(None), |value| {
                    if value.is_null() {
                        Ok(None)
                    } else {
                        value
                            .as_str()
                            .ok_or_else(|| bad_request("Sink string value is invalid"))
                            .map(ToOwned::to_owned)
                            .map(Some)
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|values| Arc::new(StringArray::from(values)) as ArrayRef),
        unsupported => Err(bad_request(format!(
            "Catalog Sink schema contains unsupported type {unsupported}"
        ))),
    }
}

/// Generates the exact first file name and stable suffixes thereafter.
#[derive(Clone, Debug)]
struct FixedFileNameGenerator {
    /// The deterministic first file name.
    file_name: String,
    /// Suffix counter for rolling files.
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

/// Writes the deterministic Parquet data file through host FileIO.
async fn write_data_file(
    file_io: &FileIO,
    metadata: &TableMetadata,
    records: &[Value],
    request: &SinkRequest,
    payload_digest: &str,
    compression: SinkCompression,
) -> CommitResult<DataFileProposal> {
    let iceberg_schema = metadata.current_schema();
    let arrow_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
            .map_err(|error| bad_request(error.to_string()))?,
    );
    let batches = rows_to_batches(records, &arrow_schema)?;
    let row_count: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    let file_name = deterministic_sink_file_id(&request.sink_id, &request.batch_id);
    let location_gen = DefaultLocationGenerator::new(metadata).map_err(iceberg_failure)?;
    let path = location_gen.generate_location(None, &file_name);
    let file_metadata = vec![
        (SINK_OWNER_PROPERTY.to_owned(), request.sink_id.clone()),
        (SINK_BATCH_ID_PROPERTY.to_owned(), request.batch_id.clone()),
        (SINK_FILE_ID_PROPERTY.to_owned(), file_name.clone()),
        (
            SINK_PAYLOAD_DIGEST_PROPERTY.to_owned(),
            payload_digest.to_owned(),
        ),
        (SINK_ROW_COUNT_PROPERTY.to_owned(), row_count.to_string()),
        (
            SINK_COMPRESSION_PROPERTY.to_owned(),
            compression.as_str().to_owned(),
        ),
    ];
    let input = file_io.new_input(&path).map_err(iceberg_failure)?;
    if input.exists().await.map_err(iceberg_failure)? {
        let bytes = input.read().await.map_err(iceberg_failure)?;
        let parquet_metadata = ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::default())
            .map_err(|error| bad_request(error.to_string()))?;
        let actual = parquet_metadata
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        for (key, expected) in &file_metadata {
            let found = actual
                .iter()
                .find(|entry| entry.key == *key)
                .and_then(|entry| entry.value.as_deref());
            if found != Some(expected.as_str()) {
                return Err(bad_request(format!(
                    "deterministic file `{path}` has a different `{key}`"
                )));
            }
        }
    }
    let mut properties = WriterProperties::builder();
    properties = properties.set_compression(parquet_compression(compression));
    let key_values = file_metadata
        .iter()
        .map(|(key, value)| parquet::file::metadata::KeyValue::new(key.clone(), value.clone()))
        .collect();
    properties = properties.set_key_value_metadata(Some(key_values));
    let parquet_writer = ParquetWriterBuilder::new(properties.build(), iceberg_schema.clone());
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer,
        file_io.clone(),
        location_gen,
        FixedFileNameGenerator::new(file_name),
    );
    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .map_err(iceberg_failure)?;
    for batch in batches {
        writer.write(batch).await.map_err(iceberg_failure)?;
    }
    let data_files = writer.close().await.map_err(iceberg_failure)?;
    let data_file = data_files
        .into_iter()
        .next()
        .ok_or_else(|| bad_request("Catalog Sink writer produced no data file"))?;
    Ok(DataFileProposal {
        data_file,
        row_count,
    })
}

/// Returns a positive deterministic snapshot id for one input metadata state.
fn snapshot_id(batch_id: &str, current_metadata_location: Option<&str>) -> i64 {
    let mut digest = Sha256::new();
    digest.update(batch_id.as_bytes());
    digest.update([0]);
    if let Some(location) = current_metadata_location {
        digest.update(location.as_bytes());
    }
    let bytes = digest.finalize();
    let mut id =
        i64::from_be_bytes(bytes[..8].try_into().expect("digest has eight bytes")) & i64::MAX;
    if id == 0 {
        id = 1;
    }
    id
}

/// Reads all manifests from the current snapshot, if one exists.
async fn existing_manifests(
    file_io: &FileIO,
    metadata: &TableMetadata,
) -> CommitResult<Vec<ManifestFile>> {
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(Vec::new());
    };
    let input = file_io
        .new_input(snapshot.manifest_list())
        .map_err(iceberg_failure)?;
    let bytes = input.read().await.map_err(iceberg_failure)?;
    Ok(
        ManifestList::parse_with_version(&bytes, metadata.format_version())
            .map_err(iceberg_failure)?
            .consume_entries()
            .into_iter()
            .collect(),
    )
}

/// Writes one manifest and manifest-list proposal for the new Sink snapshot.
async fn write_snapshot_files(
    file_io: &FileIO,
    metadata: &TableMetadata,
    data_file: DataFile,
    batch_id: &str,
    snapshot_id: i64,
    sequence_number: i64,
) -> CommitResult<String> {
    let schema = metadata.current_schema().clone();
    let partition_spec = (**metadata.default_partition_spec()).clone();
    let stem = format!(
        "{}-{snapshot_id}",
        deterministic_sink_file_id("manifest", batch_id)
    );
    let manifest_path = format!("{}/metadata/{stem}.avro", metadata.location());
    let output = file_io
        .new_output(&manifest_path)
        .map_err(iceberg_failure)?;
    let mut writer = match metadata.format_version() {
        FormatVersion::V1 => iceberg::spec::ManifestWriterBuilder::new(
            output,
            Some(snapshot_id),
            schema,
            partition_spec,
        )
        .build_v1(),
        FormatVersion::V2 => iceberg::spec::ManifestWriterBuilder::new(
            output,
            Some(snapshot_id),
            schema,
            partition_spec,
        )
        .build_v2_data(),
        FormatVersion::V3 => iceberg::spec::ManifestWriterBuilder::new(
            output,
            Some(snapshot_id),
            schema,
            partition_spec,
        )
        .build_v3_data(),
    };
    writer
        .add_file(data_file, sequence_number)
        .map_err(iceberg_failure)?;
    let new_manifest = writer
        .write_manifest_file()
        .await
        .map_err(iceberg_failure)?;
    let manifest_list_path = format!(
        "{}/metadata/snap-{snapshot_id}-{}.avro",
        metadata.location(),
        deterministic_sink_file_id("manifest-list", batch_id)
    );
    let output = file_io
        .new_output(&manifest_list_path)
        .map_err(iceberg_failure)?;
    let mut manifest_list_writer = match metadata.format_version() {
        FormatVersion::V1 => ManifestListWriter::v1(
            output.writer().await.map_err(iceberg_failure)?,
            snapshot_id,
            metadata.current_snapshot_id(),
        ),
        FormatVersion::V2 => ManifestListWriter::v2(
            output.writer().await.map_err(iceberg_failure)?,
            snapshot_id,
            metadata.current_snapshot_id(),
            sequence_number,
        ),
        FormatVersion::V3 => ManifestListWriter::v3(
            output.writer().await.map_err(iceberg_failure)?,
            snapshot_id,
            metadata.current_snapshot_id(),
            sequence_number,
            Some(metadata.next_row_id()),
        ),
    };
    let mut manifests = existing_manifests(file_io, metadata).await?;
    manifests.push(new_manifest);
    manifest_list_writer
        .add_manifests(manifests.into_iter())
        .map_err(iceberg_failure)?;
    manifest_list_writer
        .close()
        .await
        .map_err(iceberg_failure)?;
    Ok(manifest_list_path)
}

/// Reads and validates one existing metadata file for table registration.
async fn register_table_proposal(
    file_io: &FileIO,
    config: &CatalogCommitServiceConfig,
    metadata_location: String,
) -> CommitResult<Value> {
    let warehouse = host_warehouse(config)?;
    validate_metadata_location(
        &metadata_location,
        warehouse,
        "Catalog register-table metadata location does not match host warehouse",
    )?;
    let parsed_location =
        MetadataLocation::from_str(&metadata_location).map_err(iceberg_failure)?;
    let metadata = TableMetadata::read_from(file_io, &metadata_location)
        .await
        .map_err(iceberg_failure)?;
    validate_metadata_location(
        metadata.location(),
        warehouse,
        "Catalog register-table metadata table location does not match host warehouse",
    )?;
    metadata_response(&parsed_location.to_string(), &metadata)
}

/// Applies one standard Iceberg table commit and writes its next metadata file.
async fn table_commit_proposal(
    file_io: &FileIO,
    config: &CatalogCommitServiceConfig,
    current_metadata_location: String,
    request_json: String,
) -> CommitResult<Value> {
    let request: TableCommitRequest = serde_json::from_str(&request_json).map_err(|error| {
        bad_request(format!(
            "Catalog table commit request is invalid JSON: {error}"
        ))
    })?;
    validate_namespace(request.identifier.namespace().as_ref())?;
    validate_path_component(request.identifier.name(), "table")?;
    let warehouse = host_warehouse(config)?;
    validate_metadata_location(
        &current_metadata_location,
        warehouse,
        "Catalog table commit metadata location does not match host warehouse",
    )?;
    MetadataLocation::from_str(&current_metadata_location).map_err(iceberg_failure)?;
    let base_metadata = TableMetadata::read_from(file_io, &current_metadata_location)
        .await
        .map_err(iceberg_failure)?;
    validate_metadata_location(
        base_metadata.location(),
        warehouse,
        "Catalog table commit metadata location does not match host warehouse",
    )?;
    let table = Table::builder()
        .file_io(file_io.clone())
        .metadata(Arc::new(base_metadata))
        .metadata_location(current_metadata_location)
        .identifier(request.identifier.clone())
        .runtime(Runtime::try_current().map_err(iceberg_failure)?)
        .build()
        .map_err(iceberg_failure)?;
    let committed =
        TableCommit::from_parts(request.identifier, request.requirements, request.updates)
            .apply(table)
            .map_err(iceberg_failure)?;
    if !location_under_warehouse(committed.metadata().location(), warehouse) {
        return Err(bad_request(
            "Catalog table commit cannot move the table outside its warehouse",
        ));
    }
    let metadata = committed.metadata();
    let committed_location = committed
        .metadata_location_result()
        .map_err(iceberg_failure)?;
    let next_metadata_location =
        MetadataLocation::from_str(committed_location).map_err(iceberg_failure)?;
    let next_metadata_location_text = next_metadata_location.to_string();
    validate_metadata_location(
        &next_metadata_location_text,
        warehouse,
        "Catalog table commit metadata location does not match host warehouse",
    )?;
    metadata
        .write_to(file_io, &next_metadata_location)
        .await
        .map_err(iceberg_failure)?;
    metadata_response(&next_metadata_location_text, metadata)
}

/// Writes one Sink metadata proposal from the supplied current metadata.
async fn sink_batch_proposal(
    file_io: &FileIO,
    config: &CatalogCommitServiceConfig,
    current_metadata_location: Option<String>,
    request: SinkRequest,
    request_bytes: &[u8],
) -> CommitResult<Value> {
    request.validate(config)?;
    let expected_root = sink_table_root(config)?;
    let warehouse = host_warehouse(config)?;
    let payload_digest = hex::encode(Sha256::digest(request_bytes));
    let base_metadata = if let Some(location) = current_metadata_location.as_deref() {
        validate_metadata_location(
            location,
            warehouse,
            "Catalog Sink metadata location does not match host warehouse",
        )?;
        MetadataLocation::from_str(location).map_err(iceberg_failure)?;
        TableMetadata::read_from(file_io, location)
            .await
            .map_err(iceberg_failure)?
    } else {
        let arrow_schema = infer_arrow_schema(&request.records, &request.table)?;
        let schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&arrow_schema)
            .map_err(|error| bad_request(error.to_string()))?;
        let location = expected_root.clone();
        let properties = HashMap::from([
            (SINK_OWNER_PROPERTY.to_owned(), config.sink_id.clone()),
            (
                SINK_COMPRESSION_PROPERTY.to_owned(),
                config.compression.as_str().to_owned(),
            ),
        ]);
        TableMetadataBuilder::from_table_creation(
            TableCreation::builder()
                .name(config.table.clone())
                .location(location)
                .schema(schema)
                .properties(properties)
                .format_version(FormatVersion::V2)
                .build(),
        )
        .map_err(iceberg_failure)?
        .build()
        .map_err(iceberg_failure)?
        .metadata
    };
    let root_prefix = format!("{}/", expected_root.trim_end_matches('/'));
    validate_metadata_location(
        base_metadata.location(),
        warehouse,
        "Catalog Sink metadata location does not match host configuration",
    )?;
    if base_metadata.location() != expected_root
        && !base_metadata.location().starts_with(&root_prefix)
    {
        return Err(bad_request(
            "Catalog Sink metadata location does not match host configuration",
        ));
    }
    if !base_metadata.default_partition_spec().fields().is_empty() {
        return Err(bad_request("Catalog Sink tables must be unpartitioned"));
    }
    if let Some(owner) = base_metadata.properties().get(SINK_OWNER_PROPERTY)
        && owner != &config.sink_id
    {
        return Err(bad_request("Catalog Sink owner does not match host"));
    }
    if let Some(codec) = base_metadata.properties().get(SINK_COMPRESSION_PROPERTY)
        && codec != config.compression.as_str()
    {
        return Err(bad_request(
            "Catalog Sink compression does not match table metadata",
        ));
    }
    let data_file = write_data_file(
        file_io,
        &base_metadata,
        &request.records,
        &request,
        &payload_digest,
        config.compression,
    )
    .await?;
    let id = snapshot_id(&request.batch_id, current_metadata_location.as_deref());
    let sequence_number = if base_metadata.format_version() == FormatVersion::V1 {
        0
    } else {
        base_metadata.last_sequence_number().saturating_add(1)
    };
    let manifest_list = write_snapshot_files(
        file_io,
        &base_metadata,
        data_file.data_file,
        &request.batch_id,
        id,
        sequence_number,
    )
    .await?;
    let summary = Summary {
        operation: iceberg::spec::Operation::Append,
        additional_properties: HashMap::from([
            (SINK_OWNER_PROPERTY.to_owned(), config.sink_id.clone()),
            (SINK_BATCH_ID_PROPERTY.to_owned(), request.batch_id.clone()),
            (SINK_FILE_ID_PROPERTY.to_owned(), request.file_id.clone()),
            (SINK_PAYLOAD_DIGEST_PROPERTY.to_owned(), payload_digest),
            (
                SINK_ROW_COUNT_PROPERTY.to_owned(),
                data_file.row_count.to_string(),
            ),
            ("added-data-files".to_owned(), "1".to_owned()),
            ("added-records".to_owned(), data_file.row_count.to_string()),
        ]),
    };
    let snapshot = Snapshot::builder()
        .with_snapshot_id(id)
        .with_parent_snapshot_id(base_metadata.current_snapshot_id())
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| bad_request(error.to_string()))?
                .as_millis() as i64,
        )
        .with_manifest_list(manifest_list)
        .with_summary(summary)
        .with_schema_id(base_metadata.current_schema_id())
        .build();
    let metadata =
        TableMetadataBuilder::new_from_metadata(base_metadata, current_metadata_location.clone())
            .set_branch_snapshot(snapshot, "main")
            .map_err(iceberg_failure)?
            .build()
            .map_err(iceberg_failure)?
            .metadata;
    let metadata_location = if let Some(current) = current_metadata_location {
        MetadataLocation::from_str(&current)
            .map_err(iceberg_failure)?
            .with_new_metadata(&metadata)
            .with_next_version()
    } else {
        MetadataLocation::new_with_metadata(metadata.location(), &metadata)
    };
    let metadata_location_text = metadata_location.to_string();
    validate_metadata_location(
        &metadata_location_text,
        warehouse,
        "Catalog Sink metadata location does not match host warehouse",
    )?;
    metadata
        .write_to(file_io, &metadata_location)
        .await
        .map_err(iceberg_failure)?;
    Ok(json!({
        "committed": true,
        "batch_id": request.batch_id,
        "file_id": request.file_id,
        "snapshot_id": id.to_string(),
        "metadata_location": metadata_location_text,
        "metadata": serde_json::to_value(&metadata)
            .map_err(|error| bad_request(error.to_string()))?,
        "rows_committed": data_file.row_count,
    }))
}

#[async_trait]
impl CatalogCommitService for IcebergCatalogCommitService {
    /// Validates one private operation and returns its immutable publication proposal.
    async fn commit(&self, request: Request) -> Result<Response, HostError> {
        if request.method != "POST" || request_path(&request.uri) != "/catalog/commit" {
            return Ok(error_response(
                400,
                "Catalog commit capability accepts only POST /catalog/commit",
            ));
        }
        if !header(&request, "content-type").is_some_and(|value| {
            value.eq_ignore_ascii_case("application/json")
                || value.to_ascii_lowercase().starts_with("application/json;")
        }) {
            return Ok(error_response(
                400,
                "Catalog commit content-type must be application/json",
            ));
        }
        if request.body.len() > MAX_CATALOG_COMMIT_BODY_BYTES {
            return Ok(error_response(
                400,
                format!(
                    "Catalog commit body exceeds the {MAX_CATALOG_COMMIT_BODY_BYTES}-byte ceiling"
                ),
            ));
        }
        let operation: CommitOperation = match serde_json::from_slice(&request.body) {
            Ok(operation) => operation,
            Err(error) => {
                return Ok(error_response(
                    400,
                    format!("Catalog commit body is not valid JSON: {error}"),
                ));
            }
        };
        let file_io = self.file_io();
        let proposal = match operation {
            CommitOperation::CreateTable {
                warehouse,
                namespace,
                request,
            } => {
                create_table_proposal(&file_io, &self.config, warehouse, namespace, *request).await
            }
            CommitOperation::CommitSinkBatch {
                current_metadata_location,
                request: sink,
            } => {
                let sink = *sink;
                let result = async {
                    validate_sink_headers(&request, &sink)?;
                    let request_bytes = request_bytes_for_digest(&sink)?;
                    sink_batch_proposal(
                        &file_io,
                        &self.config,
                        current_metadata_location,
                        sink,
                        &request_bytes,
                    )
                    .await
                };
                result.await
            }
            CommitOperation::CommitTable {
                current_metadata_location,
                request_json,
            } => {
                table_commit_proposal(
                    &file_io,
                    &self.config,
                    current_metadata_location,
                    request_json,
                )
                .await
            }
            CommitOperation::RegisterTable { metadata_location } => {
                register_table_proposal(&file_io, &self.config, metadata_location).await
            }
        };
        let value = match proposal {
            Ok(value) => value,
            Err(CommitFailure::BadRequest(message)) => return Ok(error_response(400, message)),
            Err(CommitFailure::Conflict(message)) => return Ok(error_response(409, message)),
            Err(CommitFailure::Host(error)) => return Err(error),
        };
        let body = match serde_json::to_vec(&value) {
            Ok(body) => body,
            Err(error) => return Ok(error_response(400, error.to_string())),
        };
        Ok(Response {
            status: 200,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body,
            accept_ws: None,
        })
    }
}

/// Serializes Sink fields for a stable payload digest without retaining state.
fn request_bytes_for_digest(request: &SinkRequest) -> CommitResult<Vec<u8>> {
    serde_json::to_vec(&json!({
        "batch_id": &request.batch_id,
        "file_id": &request.file_id,
        "sink_id": &request.sink_id,
        "pipeline_id": &request.pipeline_id,
        "sql_digest": &request.sql_digest,
        "source": &request.source,
        "first_sequence": request.first_sequence,
        "last_sequence": request.last_sequence,
        "bucket": &request.bucket,
        "namespace": &request.namespace,
        "table": &request.table,
        "format": &request.format,
        "compression": &request.compression,
        "roll_interval_seconds": request.roll_interval_seconds,
        "roll_size_bytes": request.roll_size_bytes,
        "records": &request.records,
    }))
    .map_err(|error| bad_request(error.to_string()))
}
