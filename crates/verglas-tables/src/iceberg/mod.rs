//! Iceberg metadata parsing: `metadata.json → manifest list → manifests →
//! data files`.
//!
//! Reads only what the mapper needs to build its lookup structures — each data
//! file's key, format, size, partition tuple, and status — via the
//! [`MetadataFetch`](crate::fetch::MetadataFetch) interface, so it works the same in
//! the server (through the cache) and offline (through `object_store`).
//!
//! # iceberg-rust vs. apache-avro
//!
//! Manifest lists and manifests are Avro object-container files; the mapper
//! reads a handful of scalar fields per entry (`file_path`, `file_format`,
//! `file_size_in_bytes`, `status`, `partition`). We parse them with
//! `apache-avro` directly rather than pulling in `iceberg-rust`: the workspace
//! already depends on `apache-avro` (the bench seed writes spec-shaped
//! manifests with it), the field set we read is tiny and stable, and
//! `iceberg-rust` would add a large dependency surface (its own catalog, IO,
//! and table abstractions) for a strictly narrower need than the polling
//! watcher (#47) already deliberately avoided it for. If a future issue needs
//! full manifest semantics (equality/position deletes, split planning), that
//! is the moment to revisit.

use crate::fetch::{FetchError, MetadataFetch, ObjectPath};
use crate::mapper::{FileFormat, MapError};

/// A live data file discovered in a snapshot's manifests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFileEntry {
    /// Object key within the bucket (scheme + bucket stripped).
    pub path: String,
    /// The file's physical format, read straight from the manifest entry's
    /// `file_format` — never guessed by trying to parse the file.
    pub format: FileFormat,
    /// Size in bytes as recorded in the manifest.
    pub size: u64,
    /// Partition tuple as `(field name, stringified value)` pairs; empty for
    /// an unpartitioned table. Carried for per-partition metrics (#60); not on
    /// the classify hot path.
    pub partition: Vec<(String, Option<String>)>,
}

/// A manifest-entry status (Iceberg spec §Manifests). A commit records which
/// files it *added* and *deleted* in the manifests it rewrote; #51's diff reads
/// exactly these, in the changed manifests only, rather than diffing whole file
/// listings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryStatus {
    /// The file existed before this commit and still does (`0`).
    Existing,
    /// The file was added by this commit (`1`).
    Added,
    /// The file was removed by this commit (`2`).
    Deleted,
}

impl EntryStatus {
    /// Maps an Iceberg status int (`0`/`1`/`2`) to a status; anything else is
    /// treated as `Existing` (the spec's default for an unset status field).
    fn from_int(status: i64) -> EntryStatus {
        match status {
            1 => EntryStatus::Added,
            2 => EntryStatus::Deleted,
            _ => EntryStatus::Existing,
        }
    }
}

/// One manifest entry with its status — the unit #51's diff reads from a changed
/// manifest to learn what a commit added and removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    /// Whether the commit added, kept, or removed this file.
    pub status: EntryStatus,
    /// The data file the entry names.
    pub file: DataFileEntry,
}

/// One manifest-list entry: the manifest key plus which snapshot added it.
/// #51 uses `added_snapshot_id` to read only the manifests a commit rewrote
/// (added_snapshot_id == the new snapshot id) instead of every manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestFileRef {
    /// The manifest (Avro) object key.
    pub manifest_key: String,
    /// The snapshot that added this manifest to the table, if recorded.
    pub added_snapshot_id: Option<i64>,
}

/// One snapshot's fully resolved file set plus the metadata objects that name
/// it — everything the mapper needs to build (or rebuild) a `TableIndex` for
/// that snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotPlan {
    /// The snapshot id this plan resolves.
    pub snapshot_id: i64,
    /// The `metadata.json` key the walk started from.
    pub metadata_key: String,
    /// The manifest-list (Avro) key the snapshot names.
    pub manifest_list_key: String,
    /// The manifest (Avro) keys the manifest list points to.
    pub manifest_keys: Vec<String>,
    /// Live (non-deleted) data files in the snapshot.
    pub data_files: Vec<DataFileEntry>,
}

/// The current-snapshot pointer and available snapshots parsed from a
/// `metadata.json`. Retaining all snapshots (not just the current one) is what
/// lets the mapper resolve time-travel reads of recent snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataDoc {
    /// The table's object-store root location (key prefix, bucket stripped),
    /// e.g. `warehouse/db/t`. The mapper's longest-prefix match keys on this.
    pub table_location: String,
    /// The current snapshot id, if the table has any snapshots.
    pub current_snapshot_id: Option<i64>,
    /// Every snapshot the metadata names: id → manifest-list key.
    pub snapshots: Vec<SnapshotRef>,
}

/// One snapshot named by `metadata.json`: its id, manifest-list key, and the
/// commit shape the diff/prefetch layer (#51) classifies on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRef {
    /// The snapshot id.
    pub snapshot_id: i64,
    /// The manifest-list (Avro) key it names.
    pub manifest_list_key: String,
    /// The snapshot's `summary.operation` (`append` / `replace` / `overwrite` /
    /// `delete`), lower-cased, if the metadata recorded one. `replace` is the
    /// compaction case #51 carries heat across (Iceberg spec §Snapshots).
    pub operation: Option<String>,
    /// The parent snapshot id, if any — the predecessor a commit is diffed
    /// against. `None` for the very first snapshot.
    pub parent_snapshot_id: Option<i64>,
}

impl MetadataDoc {
    /// The manifest-list key for `snapshot_id`, or `None` if unknown.
    fn manifest_list_for(&self, snapshot_id: i64) -> Option<&str> {
        self.snapshots
            .iter()
            .find(|s| s.snapshot_id == snapshot_id)
            .map(|s| s.manifest_list_key.as_str())
    }
}

/// Strips an `s3://bucket/` / `s3a://bucket/` prefix from an Iceberg URI,
/// yielding the object key; a bare key is returned unchanged.
fn uri_to_key(uri: &str, bucket: &str) -> String {
    for scheme in ["s3://", "s3a://", "gs://"] {
        let prefix = format!("{scheme}{bucket}/");
        if let Some(key) = uri.strip_prefix(&prefix) {
            return key.to_owned();
        }
    }
    uri.trim_start_matches('/').to_owned()
}

/// Splits an absolute object URI into `(bucket, key)`. Handles `s3://`,
/// `s3a://`, and `gs://`; returns `None` for a URI without a recognized scheme
/// (the mapper needs an explicit bucket, so a bare key cannot be resolved
/// here). The updater uses this to turn a catalog `metadata_location` into a
/// bucket + key.
pub fn parse_object_uri(uri: &str) -> Option<(String, String)> {
    for scheme in ["s3://", "s3a://", "gs://"] {
        if let Some(rest) = uri.strip_prefix(scheme) {
            let (bucket, key) = rest.split_once('/')?;
            if bucket.is_empty() || key.is_empty() {
                return None;
            }
            return Some((bucket.to_owned(), key.to_owned()));
        }
    }
    None
}

/// Reads a whole (small) metadata object through the fetch interface.
async fn fetch_whole(
    fetch: &dyn MetadataFetch,
    storage_binding_id: &str,
    bucket: &str,
    key: &str,
) -> Result<bytes::Bytes, FetchError> {
    let path = ObjectPath {
        storage_binding_id: storage_binding_id.to_owned(),
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    };
    fetch_all(fetch, &path).await
}

/// Reads a whole object through the range-only [`MetadataFetch`] interface by
/// growing a suffix window until it stops filling. Used only for small
/// metadata objects (JSON docs, Avro manifests) where no HEAD is available to
/// learn the size first.
async fn fetch_all(
    fetch: &dyn MetadataFetch,
    path: &ObjectPath,
) -> Result<bytes::Bytes, FetchError> {
    /// Where the growth loop starts; one read covers most metadata.json bodies.
    const START: u64 = 64 * 1024;
    /// Hard ceiling so a pathological object cannot drive unbounded reads.
    const CAP: u64 = 256 * 1024 * 1024;
    let mut window = START;
    loop {
        let bytes = fetch.fetch_suffix(path, window).await?;
        // A short read means the window covered the whole object.
        if (bytes.len() as u64) < window || window >= CAP {
            return Ok(bytes);
        }
        window = window.saturating_mul(4).min(CAP);
    }
}

/// The gzip member magic (RFC 1952 §2.3.1): the first two bytes of every gzip
/// stream. Authoritative signal that a metadata object is compressed.
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// The Iceberg spec filename convention for a gzip-compressed metadata object
/// written under `write.metadata.compression-codec=gzip`
/// (`NNNNN-<uuid>.gz.metadata.json`).
const GZIP_METADATA_SUFFIX: &str = ".gz.metadata.json";

/// Returns the plain JSON bytes for a metadata object, gunzipping first when
/// the object is gzip-compressed (#336).
///
/// An object is treated as gzip when its bytes begin with [`GZIP_MAGIC`]
/// (authoritative) or its `key` carries the `.gz.metadata.json` suffix; the
/// magic wins, so a compressed object under a plainly-named key is still
/// decompressed and a plain object under a `.gz`-named key is not corrupted.
/// Constraint: a decompression failure surfaces as [`MapError::Malformed`] on
/// `key` — the same warn-and-retry class as a parse failure, never a panic — so
/// the warming and mapper callers behave identically to a malformed doc. Plain
/// bytes borrow through untouched.
fn gunzip_metadata_if_needed<'a>(
    bytes: &'a [u8],
    key: &str,
) -> Result<std::borrow::Cow<'a, [u8]>, MapError> {
    let is_gzip = bytes.starts_with(&GZIP_MAGIC) || key.ends_with(GZIP_METADATA_SUFFIX);
    if !is_gzip {
        return Ok(std::borrow::Cow::Borrowed(bytes));
    }
    let mut decoder = flate2::read::GzDecoder::new(bytes);
    let mut plain = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut plain).map_err(|e| MapError::Malformed {
        object: key.to_owned(),
        detail: format!("gzip metadata decompress: {e}"),
    })?;
    Ok(std::borrow::Cow::Owned(plain))
}

/// Parses a `metadata.json` into the current pointer plus every snapshot it
/// names. Lenient: reads only the fields the mapper needs. Gunzips a
/// gzip-compressed object (`*.gz.metadata.json`, #336) before parsing.
pub fn parse_metadata_json(bytes: &[u8], bucket: &str, key: &str) -> Result<MetadataDoc, MapError> {
    let malformed = |detail: String| MapError::Malformed {
        object: key.to_owned(),
        detail,
    };
    let bytes = gunzip_metadata_if_needed(bytes, key)?;
    let doc: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| malformed(format!("metadata.json: {e}")))?;

    let location = doc
        .get("location")
        .and_then(serde_json::Value::as_str)
        .map(|loc| uri_to_key(loc, bucket))
        .ok_or_else(|| malformed("metadata.json has no location".to_owned()))?;

    let current_snapshot_id = doc
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        // Iceberg writes -1 for "no current snapshot"; normalize to None.
        .filter(|id| *id >= 0);

    let mut snapshots = Vec::new();
    if let Some(array) = doc.get("snapshots").and_then(serde_json::Value::as_array) {
        for snap in array {
            let id = snap.get("snapshot-id").and_then(serde_json::Value::as_i64);
            let list = snap
                .get("manifest-list")
                .and_then(serde_json::Value::as_str);
            if let (Some(id), Some(list)) = (id, list) {
                // `summary.operation` is the commit shape #51 classifies on;
                // normalize to lower-case so a writer's casing never matters.
                let operation = snap
                    .get("summary")
                    .and_then(|s| s.get("operation"))
                    .and_then(serde_json::Value::as_str)
                    .map(|op| op.to_ascii_lowercase());
                let parent_snapshot_id = snap
                    .get("parent-snapshot-id")
                    .and_then(serde_json::Value::as_i64)
                    .filter(|id| *id >= 0);
                snapshots.push(SnapshotRef {
                    snapshot_id: id,
                    manifest_list_key: uri_to_key(list, bucket),
                    operation,
                    parent_snapshot_id,
                });
            }
        }
    }

    Ok(MetadataDoc {
        table_location: location.trim_end_matches('/').to_owned(),
        current_snapshot_id,
        snapshots,
    })
}

/// Walks a specific snapshot from a `metadata.json`: parse the doc, follow the
/// snapshot's manifest list, read each manifest, and collect live data files.
///
/// `snapshot_id` selects which snapshot to resolve — the current one for a
/// fresh build, an older one for a time-travel rebuild. Errors if the metadata
/// names no such snapshot.
pub async fn walk_snapshot(
    fetch: &dyn MetadataFetch,
    storage_binding_id: &str,
    bucket: &str,
    metadata_key: &str,
    snapshot_id: i64,
) -> Result<SnapshotPlan, MapError> {
    let doc_bytes = fetch_whole(fetch, storage_binding_id, bucket, metadata_key)
        .await
        .map_err(MapError::Fetch)?;
    let doc = parse_metadata_json(&doc_bytes, bucket, metadata_key)?;
    let manifest_list_key = doc
        .manifest_list_for(snapshot_id)
        .ok_or_else(|| MapError::Malformed {
            object: metadata_key.to_owned(),
            detail: format!("metadata.json names no snapshot {snapshot_id}"),
        })?
        .to_owned();

    let (manifest_keys, data_files) =
        walk_manifest_list(fetch, storage_binding_id, bucket, &manifest_list_key).await?;

    Ok(SnapshotPlan {
        snapshot_id,
        metadata_key: metadata_key.to_owned(),
        manifest_list_key,
        manifest_keys,
        data_files,
    })
}

/// Reads a manifest list and every manifest it names, returning the manifest
/// keys and the live data files across them. The reusable core the builder
/// calls once per retained snapshot after parsing `metadata.json` a single
/// time.
pub async fn walk_manifest_list(
    fetch: &dyn MetadataFetch,
    storage_binding_id: &str,
    bucket: &str,
    manifest_list_key: &str,
) -> Result<(Vec<String>, Vec<DataFileEntry>), MapError> {
    let list_bytes = fetch_whole(fetch, storage_binding_id, bucket, manifest_list_key)
        .await
        .map_err(MapError::Fetch)?;
    let manifest_keys = parse_manifest_list(&list_bytes, bucket, manifest_list_key)?;

    let mut data_files = Vec::new();
    for manifest_key in &manifest_keys {
        let manifest_bytes = fetch_whole(fetch, storage_binding_id, bucket, manifest_key)
            .await
            .map_err(MapError::Fetch)?;
        parse_manifest(&manifest_bytes, bucket, manifest_key, &mut data_files)?;
    }
    Ok((manifest_keys, data_files))
}

/// Looks up a named field in an Avro record value.
fn avro_field<'a>(
    record: &'a apache_avro::types::Value,
    name: &str,
) -> Option<&'a apache_avro::types::Value> {
    match record {
        apache_avro::types::Value::Record(fields) => {
            fields.iter().find(|(k, _)| k == name).map(|(_, v)| v)
        }
        _ => None,
    }
}

/// Reads an Avro value as a string, unwrapping a union if present.
fn avro_string(value: &apache_avro::types::Value) -> Option<String> {
    match value {
        apache_avro::types::Value::String(s) => Some(s.clone()),
        apache_avro::types::Value::Enum(_, s) => Some(s.clone()),
        apache_avro::types::Value::Union(_, inner) => avro_string(inner),
        _ => None,
    }
}

/// Reads an Avro value as an i64, unwrapping a union if present.
fn avro_long(value: &apache_avro::types::Value) -> Option<i64> {
    match value {
        apache_avro::types::Value::Long(n) => Some(*n),
        apache_avro::types::Value::Int(n) => Some(i64::from(*n)),
        apache_avro::types::Value::Union(_, inner) => avro_long(inner),
        _ => None,
    }
}

/// Stringifies a scalar Avro partition value for the partition tuple; `None`
/// for a null. Non-scalar values are rendered with `Debug` as a last resort.
fn avro_scalar_string(value: &apache_avro::types::Value) -> Option<String> {
    use apache_avro::types::Value;
    match value {
        Value::Null => None,
        Value::Boolean(b) => Some(b.to_string()),
        Value::Int(n) => Some(n.to_string()),
        Value::Long(n) => Some(n.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Double(f) => Some(f.to_string()),
        Value::String(s) => Some(s.clone()),
        Value::Enum(_, s) => Some(s.clone()),
        Value::Union(_, inner) => avro_scalar_string(inner),
        other => Some(format!("{other:?}")),
    }
}

/// Parses a manifest-list Avro object container, returning each entry's
/// `manifest_path` as an object key. Public so the warming job (#168) can parse
/// a manifest list it read through the cache as a whole-object `Full` read (the
/// shape a planning engine uses) rather than re-reading it.
pub fn parse_manifest_list(bytes: &[u8], bucket: &str, key: &str) -> Result<Vec<String>, MapError> {
    Ok(parse_manifest_list_refs(bytes, bucket, key)?
        .into_iter()
        .map(|r| r.manifest_key)
        .collect())
}

/// Parses a manifest-list Avro object container into [`ManifestFileRef`]s,
/// carrying each entry's `added_snapshot_id` alongside its key. #51's diff uses
/// this to select the manifests a specific commit rewrote (those whose
/// `added_snapshot_id` equals the new snapshot id) so it never reads unchanged
/// manifests.
pub fn parse_manifest_list_refs(
    bytes: &[u8],
    bucket: &str,
    key: &str,
) -> Result<Vec<ManifestFileRef>, MapError> {
    let reader = apache_avro::Reader::new(bytes).map_err(|e| MapError::Avro {
        object: key.to_owned(),
        detail: e.to_string(),
    })?;
    let mut refs = Vec::new();
    for value in reader {
        let value = value.map_err(|e| MapError::Avro {
            object: key.to_owned(),
            detail: e.to_string(),
        })?;
        let path = avro_field(&value, "manifest_path")
            .and_then(avro_string)
            .ok_or_else(|| MapError::Malformed {
                object: key.to_owned(),
                detail: "manifest_file has no manifest_path".to_owned(),
            })?;
        let added_snapshot_id = avro_field(&value, "added_snapshot_id").and_then(avro_long);
        refs.push(ManifestFileRef {
            manifest_key: uri_to_key(&path, bucket),
            added_snapshot_id,
        });
    }
    Ok(refs)
}

/// Parses a manifest Avro object container, appending its live (non-deleted)
/// data files to `out`. Reads `file_format` from the entry (the addendum's
/// first-class-Avro requirement — format is never inferred from the bytes).
/// Public so the warming job (#168) can enumerate a snapshot's data files from
/// manifests it read through the cache as `Full` reads.
pub fn parse_manifest(
    bytes: &[u8],
    bucket: &str,
    key: &str,
    out: &mut Vec<DataFileEntry>,
) -> Result<(), MapError> {
    let mut entries = Vec::new();
    parse_manifest_entries(bytes, bucket, key, &mut entries)?;
    for entry in entries {
        // The live file set excludes files this commit removed; ADDED and
        // EXISTING files are live.
        if entry.status != EntryStatus::Deleted {
            out.push(entry.file);
        }
    }
    Ok(())
}

/// Parses a manifest Avro object container into [`ManifestEntry`]s, preserving
/// each entry's status (ADDED / EXISTING / DELETED). #51's diff reads this from
/// a commit's changed manifests to learn what it added and removed; the
/// live-set walk ([`parse_manifest`]) filters DELETED entries out afterward.
pub fn parse_manifest_entries(
    bytes: &[u8],
    bucket: &str,
    key: &str,
    out: &mut Vec<ManifestEntry>,
) -> Result<(), MapError> {
    let reader = apache_avro::Reader::new(bytes).map_err(|e| MapError::Avro {
        object: key.to_owned(),
        detail: e.to_string(),
    })?;
    for value in reader {
        let entry = value.map_err(|e| MapError::Avro {
            object: key.to_owned(),
            detail: e.to_string(),
        })?;
        // An unset status field defaults to EXISTING per the spec.
        let status = EntryStatus::from_int(
            avro_field(&entry, "status")
                .and_then(avro_long)
                .unwrap_or(0),
        );
        let data_file = avro_field(&entry, "data_file").ok_or_else(|| MapError::Malformed {
            object: key.to_owned(),
            detail: "manifest entry has no data_file".to_owned(),
        })?;
        let path = avro_field(data_file, "file_path")
            .and_then(avro_string)
            .ok_or_else(|| MapError::Malformed {
                object: key.to_owned(),
                detail: "data_file has no file_path".to_owned(),
            })?;
        let format_str = avro_field(data_file, "file_format")
            .and_then(avro_string)
            .ok_or_else(|| MapError::Malformed {
                object: key.to_owned(),
                detail: "data_file has no file_format".to_owned(),
            })?;
        let format = FileFormat::parse(&format_str).ok_or_else(|| MapError::Malformed {
            object: key.to_owned(),
            detail: format!("unknown file_format `{format_str}`"),
        })?;
        let size = avro_field(data_file, "file_size_in_bytes")
            .and_then(avro_long)
            .unwrap_or(0)
            .max(0) as u64;
        let partition = avro_field(data_file, "partition")
            .map(parse_partition)
            .unwrap_or_default();
        out.push(ManifestEntry {
            status,
            file: DataFileEntry {
                path: uri_to_key(&path, bucket),
                format,
                size,
                partition,
            },
        });
    }
    Ok(())
}

/// Turns an Avro partition value into `(field, stringified value)` pairs.
/// Iceberg encodes the partition tuple as a struct (an Avro record); we also
/// accept a map so fixtures with heterogeneous partition columns need only one
/// manifest schema. Map order is nondeterministic, so map pairs are sorted by
/// field name for a stable tuple.
fn parse_partition(value: &apache_avro::types::Value) -> Vec<(String, Option<String>)> {
    use apache_avro::types::Value;
    match value {
        Value::Record(fields) => fields
            .iter()
            .map(|(name, v)| (name.clone(), avro_scalar_string(v)))
            .collect(),
        Value::Map(entries) => {
            let mut pairs: Vec<(String, Option<String>)> = entries
                .iter()
                .map(|(name, v)| (name.clone(), avro_scalar_string(v)))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            pairs
        }
        Value::Union(_, inner) => parse_partition(inner),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_to_key_strips_scheme_and_bucket() {
        assert_eq!(
            uri_to_key("s3://lake/warehouse/db/t/metadata/x.avro", "lake"),
            "warehouse/db/t/metadata/x.avro"
        );
        assert_eq!(uri_to_key("data/f.parquet", "lake"), "data/f.parquet");
    }

    #[test]
    fn metadata_json_parses_location_and_snapshots() {
        let json = serde_json::json!({
            "location": "s3://lake/warehouse/db/t",
            "current-snapshot-id": 42,
            "snapshots": [
                {"snapshot-id": 41, "manifest-list": "s3://lake/warehouse/db/t/metadata/snap-41.avro"},
                {"snapshot-id": 42, "manifest-list": "s3://lake/warehouse/db/t/metadata/snap-42.avro"}
            ]
        })
        .to_string();
        let doc = parse_metadata_json(json.as_bytes(), "lake", "m.json").expect("parses");
        assert_eq!(doc.table_location, "warehouse/db/t");
        assert_eq!(doc.current_snapshot_id, Some(42));
        assert_eq!(doc.snapshots.len(), 2);
        assert_eq!(
            doc.manifest_list_for(41),
            Some("warehouse/db/t/metadata/snap-41.avro")
        );
    }

    #[test]
    fn negative_current_snapshot_is_none() {
        let json = serde_json::json!({
            "location": "s3://lake/t",
            "current-snapshot-id": -1,
            "snapshots": []
        })
        .to_string();
        let doc = parse_metadata_json(json.as_bytes(), "lake", "m.json").expect("parses");
        assert_eq!(doc.current_snapshot_id, None);
    }

    #[test]
    fn malformed_metadata_json_errors() {
        assert!(parse_metadata_json(b"not json", "lake", "m.json").is_err());
        // Missing `location`.
        let json = serde_json::json!({"current-snapshot-id": 1}).to_string();
        assert!(parse_metadata_json(json.as_bytes(), "lake", "m.json").is_err());
    }

    /// gzip-compresses bytes into one member (test fixture).
    fn gzip(bytes: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).expect("gzip write");
        encoder.finish().expect("gzip finish")
    }

    #[test]
    fn gzip_metadata_object_parses_after_gunzip() {
        let json = serde_json::json!({
            "location": "s3://lake/warehouse/db/t",
            "current-snapshot-id": 7,
            "snapshots": []
        })
        .to_string();
        let compressed = gzip(json.as_bytes());
        // Named per the Iceberg convention; magic present too.
        let doc = parse_metadata_json(&compressed, "lake", "00007-uuid.gz.metadata.json")
            .expect("gzip metadata parses");
        assert_eq!(doc.table_location, "warehouse/db/t");
        assert_eq!(doc.current_snapshot_id, Some(7));
    }

    #[test]
    fn gzip_magic_wins_over_plain_name() {
        // Compressed bytes under a plainly-named key must still be gunzipped:
        // the magic bytes are authoritative.
        let json = serde_json::json!({"location": "s3://lake/t", "snapshots": []}).to_string();
        let compressed = gzip(json.as_bytes());
        let doc = parse_metadata_json(&compressed, "lake", "00003-uuid.metadata.json")
            .expect("magic-detected gzip parses under a plain name");
        assert_eq!(doc.table_location, "t");
    }

    #[test]
    fn gz_name_alone_triggers_decompression() {
        // The `.gz.metadata.json` suffix alone triggers decompression (the
        // spec's naming convention is a signal in its own right). A file so
        // named that is not actually gzip is anomalous; it fails as `Malformed`
        // (warn-and-retry), never silently mis-parses.
        let json = serde_json::json!({"location": "s3://lake/t", "snapshots": []}).to_string();
        let err = parse_metadata_json(json.as_bytes(), "lake", "00003-uuid.gz.metadata.json")
            .expect_err("a plain object under a .gz name is treated as malformed");
        assert!(matches!(err, MapError::Malformed { .. }));
    }

    #[test]
    fn plain_named_plain_bytes_parse_unchanged() {
        // The common path: a plainly-named, uncompressed object parses directly
        // with no decompression attempt (no regression).
        let json = serde_json::json!({"location": "s3://lake/t", "snapshots": []}).to_string();
        let doc = parse_metadata_json(json.as_bytes(), "lake", "00003-uuid.metadata.json")
            .expect("plain JSON parses under a plain name");
        assert_eq!(doc.table_location, "t");
    }

    #[test]
    fn corrupt_gzip_metadata_is_malformed_not_panic() {
        // Gzip magic but a truncated member: decompression fails and must map
        // to the warn-and-retry `Malformed` class, never a panic.
        let mut corrupt = GZIP_MAGIC.to_vec();
        corrupt.extend_from_slice(b"\x08garbage-not-a-valid-deflate-stream");
        let err = parse_metadata_json(&corrupt, "lake", "00009-uuid.gz.metadata.json")
            .expect_err("corrupt gzip errors");
        assert!(matches!(err, MapError::Malformed { .. }));
    }

    #[test]
    fn avro_scalars_stringify_across_types() {
        use apache_avro::types::Value;
        assert_eq!(avro_scalar_string(&Value::Null), None);
        assert_eq!(
            avro_scalar_string(&Value::Boolean(true)),
            Some("true".into())
        );
        assert_eq!(avro_scalar_string(&Value::Int(7)), Some("7".into()));
        assert_eq!(avro_scalar_string(&Value::Long(9)), Some("9".into()));
        assert_eq!(avro_scalar_string(&Value::Double(1.5)), Some("1.5".into()));
        assert_eq!(
            avro_scalar_string(&Value::Enum(0, "A".into())),
            Some("A".into())
        );
        // A union unwraps to its inner value.
        assert_eq!(
            avro_scalar_string(&Value::Union(1, Box::new(Value::Int(3)))),
            Some("3".into())
        );
    }

    #[test]
    fn avro_long_and_string_unwrap_unions_and_variants() {
        use apache_avro::types::Value;
        assert_eq!(avro_long(&Value::Int(4)), Some(4));
        assert_eq!(
            avro_long(&Value::Union(0, Box::new(Value::Long(5)))),
            Some(5)
        );
        assert_eq!(avro_long(&Value::String("x".into())), None);
        assert_eq!(
            avro_string(&Value::Enum(0, "PARQUET".into())),
            Some("PARQUET".into())
        );
        assert_eq!(
            avro_string(&Value::Union(0, Box::new(Value::String("k".into())))),
            Some("k".into())
        );
    }

    #[test]
    fn partition_parses_record_and_map_forms() {
        use apache_avro::types::Value;
        let record = Value::Record(vec![
            ("day".to_owned(), Value::Int(3)),
            ("region".to_owned(), Value::String("eu".to_owned())),
        ]);
        assert_eq!(
            parse_partition(&record),
            vec![
                ("day".to_owned(), Some("3".to_owned())),
                ("region".to_owned(), Some("eu".to_owned())),
            ]
        );
        // Map form is sorted by field name for determinism.
        let mut map = std::collections::HashMap::new();
        map.insert("b".to_owned(), Value::String("2".to_owned()));
        map.insert("a".to_owned(), Value::String("1".to_owned()));
        assert_eq!(
            parse_partition(&Value::Map(map)),
            vec![
                ("a".to_owned(), Some("1".to_owned())),
                ("b".to_owned(), Some("2".to_owned())),
            ]
        );
        // Anything else is an empty tuple.
        assert!(parse_partition(&Value::Null).is_empty());
    }
}
