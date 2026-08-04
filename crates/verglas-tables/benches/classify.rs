//! `classify()` latency at scale.
//!
//! Builds synthetic tables of 1k and 100k live files and measures the hot-path
//! lookup: prefix match → hash lookup → (optional) column-chunk binary search.
//! The acceptance budget is <10µs p99 at 100k live files. No real data files are
//! written — `classify()` reads only the in-memory maps, so a hand-built
//! manifest with N entries is enough to populate them.

use std::hint::black_box;
use std::sync::Arc;

use apache_avro::types::Value as AvroValue;
use criterion::{Criterion, criterion_group, criterion_main};
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path as StorePath,
};
use verglas_tables::fetch::ObjectStoreFetch;
use verglas_tables::mapper::{BuildInputs, Mapper};

const BUCKET: &str = "lake";
const ROOT: &str = "warehouse/db/t";

/// Builds a mapper whose single table has `n` Parquet data files, via a
/// hand-built manifest read through an in-memory object store.
fn build_mapper(n: usize) -> (Arc<Mapper>, Vec<String>) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let manifest = mk_manifest(n);
    let manifest_key = format!("{ROOT}/metadata/snap-1-m0.avro");
    let list = mk_manifest_list(&manifest_key, manifest.len() as u64);
    let list_key = format!("{ROOT}/metadata/snap-1-manifest-list.avro");
    let metadata = mk_metadata(&list_key);
    let metadata_key = format!("{ROOT}/metadata/v1.metadata.json");

    rt.block_on(async {
        for (key, bytes) in [
            (&manifest_key, manifest),
            (&list_key, list),
            (&metadata_key, metadata.into_bytes()),
        ] {
            store
                .put(&StorePath::from(key.as_str()), PutPayload::from(bytes))
                .await
                .expect("put");
        }
    });

    let mapper = Arc::new(Mapper::new());
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));
    let inputs = BuildInputs {
        ident: verglas_tables::catalog::TableIdent::new(&["db"], "t"),
        bucket: BUCKET.to_owned(),
        metadata_key,
        snapshot_ids: vec![1],
        current_snapshot_id: Some(1),
    };
    rt.block_on(mapper.apply_table(&fetch, &inputs))
        .expect("build");

    let keys = (0..n)
        .step_by((n / 64).max(1))
        .map(|i| format!("{ROOT}/data/f{i}.parquet"))
        .collect();
    (mapper, keys)
}

/// Builds a manifest Avro with `n` live Parquet data-file entries.
fn mk_manifest(n: usize) -> Vec<u8> {
    const SCHEMA: &str = r#"{"type":"record","name":"manifest_entry","fields":[
      {"name":"status","type":"int"},
      {"name":"data_file","type":{"type":"record","name":"data_file","fields":[
        {"name":"file_path","type":"string"},
        {"name":"file_format","type":"string"},
        {"name":"file_size_in_bytes","type":"long"}]}}]}"#;
    let schema = apache_avro::Schema::parse_str(SCHEMA).expect("schema");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for i in 0..n {
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

/// Builds a manifest-list Avro naming one manifest.
fn mk_manifest_list(manifest_key: &str, len: u64) -> Vec<u8> {
    const SCHEMA: &str = r#"{"type":"record","name":"manifest_file","fields":[
      {"name":"manifest_path","type":"string"},
      {"name":"manifest_length","type":"long"}]}"#;
    let schema = apache_avro::Schema::parse_str(SCHEMA).expect("schema");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    let record = AvroValue::Record(vec![
        (
            "manifest_path".to_owned(),
            AvroValue::String(format!("s3://{BUCKET}/{manifest_key}")),
        ),
        ("manifest_length".to_owned(), AvroValue::Long(len as i64)),
    ]);
    writer.append(record).expect("append");
    writer.into_inner().expect("finalize")
}

/// Builds a metadata.json naming one snapshot with the given manifest list.
fn mk_metadata(list_key: &str) -> String {
    serde_json::json!({
        "location": format!("s3://{BUCKET}/{ROOT}"),
        "current-snapshot-id": 1,
        "snapshots": [{"snapshot-id": 1, "manifest-list": format!("s3://{BUCKET}/{list_key}")}]
    })
    .to_string()
}

/// Benches classify at 1k and 100k live files: a data-file hit (no range) and a
/// data-file hit with a byte range.
fn bench_classify(c: &mut Criterion) {
    for &n in &[1_000usize, 100_000] {
        let (mapper, keys) = build_mapper(n);
        let key = &keys[keys.len() / 2];

        c.bench_function(&format!("classify_hit_norange_{n}"), |b| {
            b.iter(|| black_box(mapper.classify(black_box(BUCKET), black_box(key), None)));
        });
        c.bench_function(&format!("classify_hit_range_{n}"), |b| {
            b.iter(|| {
                black_box(mapper.classify(black_box(BUCKET), black_box(key), Some(1024..4096)))
            });
        });
        c.bench_function(&format!("classify_unknown_{n}"), |b| {
            b.iter(|| black_box(mapper.classify(black_box("other"), black_box("nope/x"), None)));
        });
    }
}

criterion_group!(benches, bench_classify);
criterion_main!(benches);
