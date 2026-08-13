//! Shared fixture builder for the mapper tests and benchmarks.
//!
//! Builds real Iceberg tables in an in-memory object store: real Parquet data
//! files (footer + multiple row groups, optionally with a nested struct column
//! so a file has several leaf column chunks), real Avro object-container data
//! files, and spec-shaped Avro manifests / manifest lists plus a JSON
//! `metadata.json`. The manifests carry exactly the fields the mapper reads
//! (`file_path`, `file_format`, `file_size_in_bytes`, `status`, `partition`);
//! full Iceberg spec completeness is out of scope — only what this crate's
//! parser needs. Hand-built manifests (rather than pyiceberg) keep the fixtures
//! hermetic and let a single test build partitioned, multi-snapshot,
//! schema-evolved, nested, Avro, and mixed-format tables.

#![allow(dead_code)]

use std::sync::Arc;

use apache_avro::types::Value as AvroValue;
use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema};
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path as StorePath,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use verglas_tables::mapper::FileFormat;

/// Bucket every fixture object lives under.
pub const BUCKET: &str = "lake";

/// One data file to place in a snapshot.
#[derive(Clone)]
pub struct FileSpec {
    /// Path relative to the table root (e.g. `data/part=a/f0.parquet`).
    pub rel_path: String,
    /// Physical format.
    pub format: FileFormat,
    /// Encoded file bytes.
    pub bytes: Vec<u8>,
    /// Partition tuple as `(field, value)` pairs.
    pub partition: Vec<(String, String)>,
}

/// One snapshot: an id and the live files it references.
#[derive(Clone)]
pub struct SnapshotSpec {
    /// Snapshot id.
    pub snapshot_id: i64,
    /// Live data files in the snapshot.
    pub files: Vec<FileSpec>,
}

/// A built fixture table: what a test needs to drive and assert the mapper.
pub struct FixtureTable {
    /// Table root as an object key prefix (no trailing slash).
    pub root: String,
    /// The current `metadata.json` as an `s3://` URI (what a catalog points at).
    pub metadata_uri: String,
    /// The current `metadata.json` object key.
    pub metadata_key: String,
    /// Current snapshot id.
    pub current_snapshot_id: i64,
    /// All snapshot ids present, ascending.
    pub snapshot_ids: Vec<i64>,
    /// Object key of every data file written, with its format and etag.
    pub files: Vec<BuiltFile>,
}

/// A written data file's identity plus its ETag (for footer memoization tests).
pub struct BuiltFile {
    /// Object key.
    pub key: String,
    /// Format.
    pub format: FileFormat,
    /// Size in bytes.
    pub size: u64,
    /// ETag the store reported.
    pub etag: String,
    /// Which snapshot(s) reference it.
    pub snapshot_ids: Vec<i64>,
}

impl FixtureTable {
    /// The object key of the first data file of `format` (test convenience).
    pub fn first_of(&self, format: FileFormat) -> &BuiltFile {
        self.files
            .iter()
            .find(|f| f.format == format)
            .expect("a file of the requested format exists")
    }
}

/// A fresh in-memory object store to build fixtures in.
pub fn new_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

/// PUTs bytes and returns the store-assigned ETag.
async fn put(store: &Arc<dyn ObjectStore>, key: &str, bytes: Vec<u8>) -> String {
    store
        .put(&StorePath::from(key), PutPayload::from(bytes))
        .await
        .expect("put succeeds");
    store
        .head(&StorePath::from(key))
        .await
        .expect("head succeeds")
        .e_tag
        .unwrap_or_default()
}

/// Builds a table from its snapshots and writes every object into `store`.
/// Data files are written once (deduped by key across snapshots); each snapshot
/// gets a manifest + manifest list; a single `metadata.json` names them all.
pub async fn build_table(
    store: &Arc<dyn ObjectStore>,
    root: &str,
    snapshots: &[SnapshotSpec],
    current_snapshot_id: i64,
) -> FixtureTable {
    let mut built: Vec<BuiltFile> = Vec::new();

    // Write data files (dedup by key), tracking which snapshots reference each.
    for snap in snapshots {
        for file in &snap.files {
            let key = format!("{root}/{}", file.rel_path);
            if let Some(existing) = built.iter_mut().find(|b| b.key == key) {
                existing.snapshot_ids.push(snap.snapshot_id);
                continue;
            }
            let etag = put(store, &key, file.bytes.clone()).await;
            built.push(BuiltFile {
                key,
                format: file.format,
                size: file.bytes.len() as u64,
                etag,
                snapshot_ids: vec![snap.snapshot_id],
            });
        }
    }

    // Per-snapshot manifest + manifest list.
    let mut snapshot_refs: Vec<(i64, String)> = Vec::new();
    for snap in snapshots {
        let manifest_key = format!("{root}/metadata/{}-m0.avro", snap.snapshot_id);
        let manifest_bytes = build_manifest(root, snap);
        put(store, &manifest_key, manifest_bytes.clone()).await;

        let list_key = format!(
            "{root}/metadata/snap-{}-manifest-list.avro",
            snap.snapshot_id
        );
        let list_bytes =
            build_manifest_list(&manifest_key, manifest_bytes.len() as u64, snap.snapshot_id);
        put(store, &list_key, list_bytes).await;
        snapshot_refs.push((snap.snapshot_id, list_key));
    }

    let metadata_key = format!("{root}/metadata/v1.metadata.json");
    let metadata_json = build_metadata_json(root, current_snapshot_id, &snapshot_refs);
    put(store, &metadata_key, metadata_json.into_bytes()).await;

    let mut snapshot_ids: Vec<i64> = snapshots.iter().map(|s| s.snapshot_id).collect();
    snapshot_ids.sort_unstable();

    FixtureTable {
        root: root.to_owned(),
        metadata_uri: format!("s3://{BUCKET}/{metadata_key}"),
        metadata_key,
        current_snapshot_id,
        snapshot_ids,
        files: built,
    }
}

/// The Avro manifest schema: `manifest_entry` with a nested `data_file` and a
/// `partition` map. Only the mapper-read fields are present.
const MANIFEST_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": "long"},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "data_file",
      "fields": [
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "file_size_in_bytes", "type": "long"},
        {"name": "record_count", "type": "long"},
        {"name": "partition", "type": {"type": "map", "values": "string"}}
      ]
    }}
  ]
}"#;

/// Renders an Iceberg `file_format` string for a format.
fn format_str(format: FileFormat) -> &'static str {
    match format {
        FileFormat::Parquet => "PARQUET",
        FileFormat::Avro => "AVRO",
        FileFormat::Orc => "ORC",
    }
}

/// Builds a manifest Avro: one live entry per data file in the snapshot.
fn build_manifest(root: &str, snap: &SnapshotSpec) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(MANIFEST_SCHEMA).expect("manifest schema");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for file in &snap.files {
        let partition: std::collections::HashMap<String, AvroValue> = file
            .partition
            .iter()
            .map(|(k, v)| (k.clone(), AvroValue::String(v.clone())))
            .collect();
        let data_file = AvroValue::Record(vec![
            (
                "file_path".to_owned(),
                AvroValue::String(format!("s3://{BUCKET}/{root}/{}", file.rel_path)),
            ),
            (
                "file_format".to_owned(),
                AvroValue::String(format_str(file.format).to_owned()),
            ),
            (
                "file_size_in_bytes".to_owned(),
                AvroValue::Long(file.bytes.len() as i64),
            ),
            ("record_count".to_owned(), AvroValue::Long(0)),
            ("partition".to_owned(), AvroValue::Map(partition)),
        ]);
        let entry = AvroValue::Record(vec![
            ("status".to_owned(), AvroValue::Int(1)),
            ("snapshot_id".to_owned(), AvroValue::Long(snap.snapshot_id)),
            ("data_file".to_owned(), data_file),
        ]);
        writer.append(entry).expect("manifest append");
    }
    writer.into_inner().expect("manifest finalize")
}

/// The Avro manifest-list schema (`manifest_file`).
const MANIFEST_LIST_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "added_snapshot_id", "type": "long"}
  ]
}"#;

/// Builds a manifest-list Avro pointing at one manifest.
fn build_manifest_list(manifest_key: &str, manifest_length: u64, snapshot_id: i64) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(MANIFEST_LIST_SCHEMA).expect("list schema");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    let record = AvroValue::Record(vec![
        (
            "manifest_path".to_owned(),
            AvroValue::String(format!("s3://{BUCKET}/{manifest_key}")),
        ),
        (
            "manifest_length".to_owned(),
            AvroValue::Long(manifest_length as i64),
        ),
        ("partition_spec_id".to_owned(), AvroValue::Int(0)),
        ("added_snapshot_id".to_owned(), AvroValue::Long(snapshot_id)),
    ]);
    writer.append(record).expect("list append");
    writer.into_inner().expect("list finalize")
}

/// Builds a minimal `metadata.json` naming all snapshots and the current one.
fn build_metadata_json(root: &str, current: i64, snapshots: &[(i64, String)]) -> String {
    let snaps: Vec<serde_json::Value> = snapshots
        .iter()
        .map(|(id, list_key)| {
            serde_json::json!({
                "snapshot-id": id,
                "sequence-number": 1,
                "timestamp-ms": 0,
                "manifest-list": format!("s3://{BUCKET}/{list_key}"),
                "summary": {"operation": "append"},
                "schema-id": 0
            })
        })
        .collect();
    let doc = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000000",
        "location": format!("s3://{BUCKET}/{root}"),
        "current-snapshot-id": current,
        "snapshots": snaps
    });
    serde_json::to_string(&doc).expect("metadata json")
}

/// Rows per generated Parquet file — enough for several row groups.
const PARQUET_ROWS: usize = 4_000;
/// Parquet row-group size, so each file has multiple row groups (hence multiple
/// column chunks to range-resolve).
const PARQUET_ROW_GROUP_ROWS: usize = 1_000;

/// Generates a flat Parquet file (`id`, `name`, `value`) with several row
/// groups; `salt` varies content. Returns encoded bytes.
pub fn parquet_flat(salt: i64) -> Vec<u8> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let ids: Int64Array = (0..PARQUET_ROWS as i64).map(|i| i + salt).collect();
    let names: StringArray = (0..PARQUET_ROWS)
        .map(|i| Some(format!("r{salt}-{i}")))
        .collect();
    let values: Float64Array = (0..PARQUET_ROWS).map(|i| i as f64 * 1.5).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(names), Arc::new(values)],
    )
    .expect("batch");
    write_parquet(schema, batch)
}

/// Generates a Parquet file with a nested struct column (`nested {a, b}`) so it
/// has several leaf column chunks per row group. Returns encoded bytes.
pub fn parquet_nested(salt: i64) -> Vec<u8> {
    let nested_fields: Fields = vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Float64, false),
    ]
    .into();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("nested", DataType::Struct(nested_fields.clone()), false),
    ]));
    let ids: Int64Array = (0..PARQUET_ROWS as i64).map(|i| i + salt).collect();
    let a: Int64Array = (0..PARQUET_ROWS as i64).map(|i| i * 2).collect();
    let b: Float64Array = (0..PARQUET_ROWS).map(|i| i as f64).collect();
    let nested = StructArray::new(
        nested_fields,
        vec![Arc::new(a) as _, Arc::new(b) as _],
        None,
    );
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(nested)])
        .expect("nested batch");
    write_parquet(schema, batch)
}

/// Generates a schema-evolved Parquet file (adds an `extra` column). Returns
/// encoded bytes — a later-snapshot file with a wider schema than the first.
pub fn parquet_evolved(salt: i64) -> Vec<u8> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("extra", DataType::Int64, true),
    ]));
    let ids: Int64Array = (0..PARQUET_ROWS as i64).map(|i| i + salt).collect();
    let names: StringArray = (0..PARQUET_ROWS)
        .map(|i| Some(format!("r{salt}-{i}")))
        .collect();
    let values: Float64Array = (0..PARQUET_ROWS).map(|i| i as f64 * 1.5).collect();
    let extra: Int64Array = (0..PARQUET_ROWS as i64).map(Some).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids),
            Arc::new(names),
            Arc::new(values),
            Arc::new(extra),
        ],
    )
    .expect("evolved batch");
    write_parquet(schema, batch)
}

/// Writes a record batch to Parquet bytes with a small row-group size.
fn write_parquet(schema: Arc<ArrowSchema>, batch: RecordBatch) -> Vec<u8> {
    let props = WriterProperties::builder()
        .set_max_row_group_size(PARQUET_ROW_GROUP_ROWS)
        .build();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
    buf
}

/// The Avro data-file record schema.
const AVRO_DATA_SCHEMA: &str = r#"{
  "type": "record",
  "name": "row",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "value", "type": "double"}
  ]
}"#;

/// Generates an Avro object-container data file with several sync-marked blocks.
pub fn avro_file(salt: i64) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(AVRO_DATA_SCHEMA).expect("avro schema");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for i in 0..2_000i64 {
        let record = AvroValue::Record(vec![
            ("id".to_owned(), AvroValue::Long(i + salt)),
            ("value".to_owned(), AvroValue::Double(i as f64)),
        ]);
        writer.append(record).expect("avro append");
        if (i + 1) % 500 == 0 {
            writer.flush().expect("avro flush");
        }
    }
    writer.into_inner().expect("avro finalize")
}
