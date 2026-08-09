//! Validation against REAL pyiceberg-written Iceberg v2 metadata (checked-in
//! fixture, `tests/fixtures/pyiceberg-v2/` — see its README/regenerate.py).
//!
//! The hand-built fixtures elsewhere in this suite carry only the fields the
//! parser reads, which would make the validation circular. This suite walks a
//! table written by pyiceberg 0.8.1 — full embedded Avro schemas, v2
//! field-id-labelled partition records, int status enums, sequence-number
//! fields, nullable unions everywhere — and proves the lenient by-name reader
//! and the mapper handle it: real data-file paths classify with correct
//! identity, and a range inside a real Parquet column chunk resolves.
//!
//! Red-first is not meaningful for a fixture test, so the negative tests
//! corrupt a copy of the fixture (drop `file_format` from a manifest; drop
//! `location` from metadata.json) and assert the walk FAILS — proving the
//! assertions are live.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path as StorePath,
};
use verglas_tables::catalog::TableIdent;
use verglas_tables::fetch::ObjectStoreFetch;
use verglas_tables::iceberg;
use verglas_tables::mapper::{BuildInputs, Classify, FileFormat, Mapper, MetaKind};

/// The sidecar `regenerate.py` writes next to the objects.
#[derive(serde::Deserialize)]
struct Sidecar {
    bucket: String,
    metadata_key: String,
    current_snapshot_id: i64,
    snapshot_ids: Vec<i64>,
    data_file_keys: Vec<String>,
}

/// Absolute path of the checked-in fixture directory.
fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pyiceberg-v2")
}

/// Loads the fixture: every object under `objects/` into an in-memory store
/// (key = path relative to `objects/`), plus the parsed sidecar.
async fn load_fixture() -> (Arc<dyn ObjectStore>, Sidecar) {
    let dir = fixture_dir();
    let sidecar: Sidecar = serde_json::from_slice(
        &std::fs::read(dir.join("fixture.json")).expect("fixture.json present"),
    )
    .expect("sidecar parses");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let objects_root = dir.join("objects");
    let mut stack = vec![objects_root.clone()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).expect("fixture dir readable") {
            let entry = entry.expect("entry");
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let key = path
                .strip_prefix(&objects_root)
                .expect("under objects/")
                .to_string_lossy()
                .into_owned();
            let bytes = std::fs::read(&path).expect("object readable");
            store
                .put(&StorePath::from(key.as_str()), PutPayload::from(bytes))
                .await
                .expect("put");
        }
    }
    (store, sidecar)
}

/// Builds a mapper over the fixture through the offline fetch impl.
async fn build_mapper(
    store: &Arc<dyn ObjectStore>,
    sidecar: &Sidecar,
) -> (Arc<Mapper>, TableIdent) {
    let ident = TableIdent::new(&["db"], "events");
    let mapper = Arc::new(Mapper::new());
    let fetch = ObjectStoreFetch::new(Arc::clone(store));
    mapper
        .apply_table(
            &fetch,
            &BuildInputs {
                storage_binding_id: "managed-lakehouse".to_owned(),
                ident: ident.clone(),
                bucket: sidecar.bucket.clone(),
                metadata_key: sidecar.metadata_key.clone(),
                snapshot_ids: sidecar.snapshot_ids.clone(),
                current_snapshot_id: Some(sidecar.current_snapshot_id),
            },
        )
        .await
        .expect("real pyiceberg metadata builds");
    (mapper, ident)
}

#[tokio::test]
async fn real_pyiceberg_table_walks_and_classifies() {
    let (store, sidecar) = load_fixture().await;
    let bucket = sidecar.bucket.clone();
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));

    // The raw walk resolves both real snapshots to their data files, reading
    // format/size/partition from pyiceberg's v2 manifest entries.
    let plan = iceberg::walk_snapshot(
        &fetch,
        "managed-lakehouse",
        &bucket,
        &sidecar.metadata_key,
        sidecar.current_snapshot_id,
    )
    .await
    .expect("walk of real metadata");
    assert!(!plan.data_files.is_empty());
    for df in &plan.data_files {
        assert_eq!(df.format, FileFormat::Parquet);
        assert!(df.size > 0, "manifest size recorded");
        // Identity-partitioned on `category`: the field-id-labelled partition
        // record must surface as a (name, value) tuple.
        assert_eq!(df.partition.len(), 1, "one partition field");
        assert_eq!(df.partition[0].0, "category");
        assert!(df.partition[0].1.is_some(), "partition value present");
    }

    let (mapper, ident) = build_mapper(&store, &sidecar).await;
    let table = mapper.table_id(&ident).expect("table id");

    // Every real data file (both snapshots, per the sidecar) classifies as
    // TableData on this table, with a distinct file identity per path.
    let mut file_ids = Vec::new();
    for key in &sidecar.data_file_keys {
        match mapper.classify(&bucket, key, None) {
            Classify::TableData {
                table: t,
                file,
                chunk,
            } => {
                assert_eq!(t, table, "file attributed to the right table: {key}");
                assert_eq!(chunk, None, "no footer warmed yet");
                file_ids.push(file);
            }
            other => panic!("expected TableData for real data file {key}, got {other:?}"),
        }
    }
    file_ids.sort();
    file_ids.dedup();
    assert_eq!(
        file_ids.len(),
        sidecar.data_file_keys.len(),
        "each real data file has its own identity"
    );

    // The real metadata objects classify by kind.
    assert!(matches!(
        mapper.classify(&bucket, &sidecar.metadata_key, None),
        Classify::TableMetadata {
            kind: MetaKind::MetadataJson,
            ..
        }
    ));
    assert!(matches!(
        mapper.classify(&bucket, &plan.manifest_list_key, None),
        Classify::TableMetadata {
            kind: MetaKind::ManifestList,
            ..
        }
    ));
    for manifest_key in &plan.manifest_keys {
        assert!(matches!(
            mapper.classify(&bucket, manifest_key, None),
            Classify::TableMetadata {
                kind: MetaKind::Manifest,
                ..
            }
        ));
    }
}

#[tokio::test]
async fn real_parquet_footer_resolves_a_range_to_a_chunk() {
    let (store, sidecar) = load_fixture().await;
    let bucket = sidecar.bucket.clone();
    let (mapper, ident) = build_mapper(&store, &sidecar).await;
    let table = mapper.table_id(&ident).expect("table id");
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));

    let key = &sidecar.data_file_keys[0];
    let etag = store
        .head(&StorePath::from(key.as_str()))
        .await
        .expect("head")
        .e_tag
        .expect("etag");
    mapper
        .warm_footer(&fetch, table, key, &etag)
        .await
        .expect("real footer parses");

    let state = mapper.state();
    let index = state.table(table).expect("index");
    let spans = index
        .file_by_path(key)
        .expect("file")
        .chunks()
        .expect("chunks from the real footer")
        .clone();
    // Three columns (id, category, amount) in one row group.
    assert!(spans.len() >= 3, "real file has >= 3 column chunks");

    // A range starting inside a real chunk resolves to that chunk.
    let span = spans[1];
    match mapper.classify(&bucket, key, Some(span.start..span.start + 2)) {
        Classify::TableData {
            chunk: Some(chunk), ..
        } => {
            assert_eq!(chunk.column, span.column);
            assert_eq!(chunk.row_group, span.row_group);
        }
        other => panic!("expected chunk resolution on real footer, got {other:?}"),
    }
    // A read past the last chunk is the real footer region.
    let end = spans.last().expect("last").end;
    assert!(matches!(
        mapper.classify(&bucket, key, Some(end..end + 8)),
        Classify::TableMetadata {
            kind: MetaKind::Footer { .. },
            ..
        }
    ));
}

/// Re-encodes the fixture's real manifest WITHOUT the `data_file.file_format`
/// field — "delete one manifest field" — keeping everything else the walk
/// reads. Proves the by-name reader actually asserts on the field rather than
/// silently defaulting.
fn manifest_without_file_format(bytes: &[u8]) -> Vec<u8> {
    const CORRUPT_SCHEMA: &str = r#"{
      "type": "record", "name": "manifest_entry", "fields": [
        {"name": "status", "type": "int"},
        {"name": "data_file", "type": {
          "type": "record", "name": "data_file", "fields": [
            {"name": "file_path", "type": "string"},
            {"name": "file_size_in_bytes", "type": "long"}
          ]}}]}"#;
    let reader = apache_avro::Reader::new(bytes).expect("real manifest reads");
    let schema = apache_avro::Schema::parse_str(CORRUPT_SCHEMA).expect("schema");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for value in reader {
        let value = value.expect("record");
        let (status, data_file) = match &value {
            apache_avro::types::Value::Record(fields) => {
                let get = |name: &str| {
                    fields
                        .iter()
                        .find(|(k, _)| k == name)
                        .map(|(_, v)| v.clone())
                        .expect("field present in real manifest")
                };
                (get("status"), get("data_file"))
            }
            other => panic!("manifest entry is a record, got {other:?}"),
        };
        let (file_path, size) = match &data_file {
            apache_avro::types::Value::Record(fields) => {
                let get = |name: &str| {
                    fields
                        .iter()
                        .find(|(k, _)| k == name)
                        .map(|(_, v)| v.clone())
                        .expect("data_file field present")
                };
                (get("file_path"), get("file_size_in_bytes"))
            }
            other => panic!("data_file is a record, got {other:?}"),
        };
        writer
            .append(apache_avro::types::Value::Record(vec![
                ("status".to_owned(), status),
                (
                    "data_file".to_owned(),
                    apache_avro::types::Value::Record(vec![
                        ("file_path".to_owned(), file_path),
                        ("file_size_in_bytes".to_owned(), size),
                    ]),
                ),
            ]))
            .expect("append");
    }
    writer.into_inner().expect("finalize")
}

#[tokio::test]
async fn corrupted_manifest_missing_file_format_fails_the_walk() {
    let (store, sidecar) = load_fixture().await;
    let bucket = sidecar.bucket.clone();
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));

    // Find the current snapshot's manifest via the (valid) walk first.
    let plan = iceberg::walk_snapshot(
        &fetch,
        "managed-lakehouse",
        &bucket,
        &sidecar.metadata_key,
        sidecar.current_snapshot_id,
    )
    .await
    .expect("valid walk");
    let manifest_key = plan.manifest_keys.first().expect("a manifest").clone();

    // Overwrite that manifest with a field-deleted copy.
    let original = store
        .get(&StorePath::from(manifest_key.as_str()))
        .await
        .expect("get manifest")
        .bytes()
        .await
        .expect("bytes");
    let corrupted = manifest_without_file_format(&original);
    store
        .put(
            &StorePath::from(manifest_key.as_str()),
            PutPayload::from(corrupted),
        )
        .await
        .expect("put corrupted");

    // The same walk must now FAIL on the missing field — the positive tests
    // are asserting real content, not vacuously passing.
    let err = iceberg::walk_snapshot(
        &fetch,
        "managed-lakehouse",
        &bucket,
        &sidecar.metadata_key,
        sidecar.current_snapshot_id,
    )
    .await
    .expect_err("walk of corrupted manifest fails");
    assert!(
        err.to_string().contains("file_format"),
        "error names the deleted field: {err}"
    );
}

#[tokio::test]
async fn corrupted_metadata_json_missing_location_fails_the_build() {
    let (store, sidecar) = load_fixture().await;
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));

    // Drop `location` from the real metadata.json.
    let key = StorePath::from(sidecar.metadata_key.as_str());
    let original = store
        .get(&key)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    let mut doc: serde_json::Value = serde_json::from_slice(&original).expect("json");
    doc.as_object_mut().expect("object").remove("location");
    store
        .put(
            &key,
            PutPayload::from(serde_json::to_vec(&doc).expect("encode")),
        )
        .await
        .expect("put corrupted");

    let mapper = Mapper::new();
    let err = mapper
        .apply_table(
            &fetch,
            &BuildInputs {
                storage_binding_id: "managed-lakehouse".to_owned(),
                ident: TableIdent::new(&["db"], "events"),
                bucket: sidecar.bucket.clone(),
                metadata_key: sidecar.metadata_key.clone(),
                snapshot_ids: sidecar.snapshot_ids.clone(),
                current_snapshot_id: Some(sidecar.current_snapshot_id),
            },
        )
        .await
        .expect_err("build without location fails");
    assert!(
        err.to_string().contains("location"),
        "error names the field: {err}"
    );
}
