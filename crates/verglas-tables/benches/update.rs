//! Incremental update cost on a 10k-file commit.
//!
//! Builds a table at snapshot 1 (10k files), then measures the cost of applying
//! snapshot 2 (mostly the same files, 10% new) — a walk of the changed
//! manifests plus a clone-on-write rebuild that shares the unchanged
//! `Arc<FileMeta>` entries, then an atomic swap.

use std::sync::Arc;

use apache_avro::types::Value as AvroValue;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path as StorePath,
};
use verglas_tables::fetch::ObjectStoreFetch;
use verglas_tables::mapper::{BuildInputs, Mapper};

const BUCKET: &str = "lake";
const ROOT: &str = "warehouse/db/t";
const FILES: usize = 10_000;

/// Writes a snapshot's manifest, manifest list, and (once) the metadata.json
/// naming both snapshots. Returns nothing; keys are deterministic.
async fn write_snapshot(store: &Arc<dyn ObjectStore>, snapshot_id: i64, start: usize) {
    let manifest = mk_manifest(start, FILES);
    let manifest_key = format!("{ROOT}/metadata/snap-{snapshot_id}-m0.avro");
    let list = mk_manifest_list(&manifest_key, manifest.len() as u64);
    let list_key = format!("{ROOT}/metadata/snap-{snapshot_id}-manifest-list.avro");
    for (key, bytes) in [(&manifest_key, manifest), (&list_key, list)] {
        store
            .put(&StorePath::from(key.as_str()), PutPayload::from(bytes))
            .await
            .expect("put");
    }
}

/// Builds the two-snapshot store: snapshot 1 files [0,10k), snapshot 2 files
/// [1k, 11k) — a 90% overlap, 10% new.
fn build_store() -> Arc<dyn ObjectStore> {
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    rt.block_on(async {
        write_snapshot(&store, 1, 0).await;
        write_snapshot(&store, 2, 1_000).await;
        let metadata_key = format!("{ROOT}/metadata/v1.metadata.json");
        let metadata = serde_json::json!({
            "location": format!("s3://{BUCKET}/{ROOT}"),
            "current-snapshot-id": 2,
            "snapshots": [
                {"snapshot-id": 1, "manifest-list": format!("s3://{BUCKET}/{ROOT}/metadata/snap-1-manifest-list.avro")},
                {"snapshot-id": 2, "manifest-list": format!("s3://{BUCKET}/{ROOT}/metadata/snap-2-manifest-list.avro")}
            ]
        })
        .to_string();
        store
            .put(&StorePath::from(metadata_key.as_str()), PutPayload::from(metadata.into_bytes()))
            .await
            .expect("put metadata");
    });
    store
}

/// Inputs to (re)build the table at a given current snapshot.
fn inputs(current: i64, snapshot_ids: Vec<i64>) -> BuildInputs {
    BuildInputs {
        ident: verglas_tables::catalog::TableIdent::new(&["db"], "t"),
        bucket: BUCKET.to_owned(),
        metadata_key: format!("{ROOT}/metadata/v1.metadata.json"),
        snapshot_ids,
        current_snapshot_id: Some(current),
    }
}

/// A manifest with `count` entries starting at key index `start`.
fn mk_manifest(start: usize, count: usize) -> Vec<u8> {
    const SCHEMA: &str = r#"{"type":"record","name":"manifest_entry","fields":[
      {"name":"status","type":"int"},
      {"name":"data_file","type":{"type":"record","name":"data_file","fields":[
        {"name":"file_path","type":"string"},
        {"name":"file_format","type":"string"},
        {"name":"file_size_in_bytes","type":"long"}]}}]}"#;
    let schema = apache_avro::Schema::parse_str(SCHEMA).expect("schema");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for i in start..start + count {
        let data_file = AvroValue::Record(vec![
            (
                "file_path".to_owned(),
                AvroValue::String(format!("s3://{BUCKET}/{ROOT}/data/f{i}.parquet")),
            ),
            (
                "file_format".to_owned(),
                AvroValue::String("PARQUET".to_owned()),
            ),
            ("file_size_in_bytes".to_owned(), AvroValue::Long(1 << 20)),
        ]);
        let entry = AvroValue::Record(vec![
            ("status".to_owned(), AvroValue::Int(1)),
            ("data_file".to_owned(), data_file),
        ]);
        writer.append(entry).expect("append");
    }
    writer.into_inner().expect("finalize")
}

/// A manifest-list Avro naming one manifest.
fn mk_manifest_list(manifest_key: &str, len: u64) -> Vec<u8> {
    const SCHEMA: &str = r#"{"type":"record","name":"manifest_file","fields":[
      {"name":"manifest_path","type":"string"},
      {"name":"manifest_length","type":"long"}]}"#;
    let schema = apache_avro::Schema::parse_str(SCHEMA).expect("schema");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    writer
        .append(AvroValue::Record(vec![
            (
                "manifest_path".to_owned(),
                AvroValue::String(format!("s3://{BUCKET}/{manifest_key}")),
            ),
            ("manifest_length".to_owned(), AvroValue::Long(len as i64)),
        ]))
        .expect("append");
    writer.into_inner().expect("finalize")
}

/// Benches applying the 10k-file second-snapshot commit onto a mapper already
/// built at snapshot 1.
fn bench_update(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let store = build_store();
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));

    c.bench_function("update_commit_10k", |b| {
        b.iter_batched(
            || {
                let mapper = Arc::new(Mapper::new());
                rt.block_on(mapper.apply_table(&fetch, &inputs(1, vec![1])))
                    .expect("seed snap1");
                mapper
            },
            |mapper| {
                rt.block_on(mapper.apply_table(&fetch, &inputs(2, vec![1, 2])))
                    .expect("apply snap2");
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_update);
criterion_main!(benches);
