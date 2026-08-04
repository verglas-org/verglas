//! Acceptance tests for gzip-compressed table metadata objects (#336).
//!
//! A catalog with `write.metadata.compression-codec=gzip` (some managed catalogs
//! do) stores the table's `metadata.json` object gzip-compressed on the object
//! store, named `NNNNN-<uuid>.gz.metadata.json`. Both metadata consumers —
//! eager warming (`warming::coordinator`) and the mapper rebuild
//! (`mapper::updater`) — fetch that object and parse it. They must gunzip it
//! first. This is distinct from the catalog-gateway gzip handling
//! (`catalog_gateway_gzip.rs`, #307/#310), which decodes gzip *HTTP transfer
//! encoding* on catalog responses; here the stored object bytes are themselves
//! gzip, independent of any transfer encoding.
//!
//! Coverage:
//! - warming a table whose current metadata object is `*.gz.metadata.json`
//!   succeeds (fixture: gzip a valid `metadata.json`);
//! - plain `.metadata.json` still warms (no regression);
//! - a mapper rebuild succeeds against a gzip metadata object;
//! - a mapper rebuild still succeeds against plain metadata (no regression).

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStoreExt, PutPayload};
use verglas_core::CacheKey;
use verglas_core::read::{ReadError, ReadRange};
use verglas_tables::catalog::TableIdent;
use verglas_tables::fetch::ObjectStoreFetch;
use verglas_tables::mapper::{BuildInputs, Classify, Mapper};
use verglas_tables::warming::budget::TokenBucket;
use verglas_tables::warming::{WarmConfig, WarmSource, WarmTarget, Warmer};

/// The checked-in pyiceberg-v2 fixture directory.
fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pyiceberg-v2")
}

/// The fixture sidecar (bucket, current metadata key, current snapshot).
#[derive(serde::Deserialize)]
struct Sidecar {
    bucket: String,
    metadata_key: String,
    current_snapshot_id: i64,
}

/// Loads every fixture object into an in-memory `key -> bytes` map, keyed
/// relative to `objects/`, plus the parsed sidecar.
fn load_objects() -> (HashMap<String, Vec<u8>>, Sidecar) {
    let dir = fixture_dir();
    let sidecar: Sidecar =
        serde_json::from_slice(&std::fs::read(dir.join("fixture.json")).expect("fixture.json"))
            .expect("sidecar parses");
    let objects_root = dir.join("objects");
    let mut objects = HashMap::new();
    let mut stack = vec![objects_root.clone()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).expect("fixture dir readable") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let key = path
                .strip_prefix(&objects_root)
                .expect("under objects/")
                .to_string_lossy()
                .into_owned();
            objects.insert(key, std::fs::read(&path).expect("object readable"));
        }
    }
    (objects, sidecar)
}

/// gzip-compresses a byte slice into a single member — the on-object-store
/// framing a `write.metadata.compression-codec=gzip` writer produces.
fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).expect("gzip write");
    encoder.finish().expect("gzip finish")
}

/// Rewrites a plain metadata object into its gzip form: the bytes are gzipped
/// and re-keyed with the Iceberg `.gz.metadata.json` suffix. Returns the new
/// key. The original plain object is left in place so tests can drive either.
fn install_gzip_metadata(objects: &mut HashMap<String, Vec<u8>>, plain_key: &str) -> String {
    let plain = objects.get(plain_key).expect("plain metadata present");
    let gz_key = plain_key
        .strip_suffix(".metadata.json")
        .map(|stem| format!("{stem}.gz.metadata.json"))
        .expect("metadata key ends with .metadata.json");
    let compressed = gzip(plain);
    objects.insert(gz_key.clone(), compressed);
    gz_key
}

/// Builds an in-memory object store holding every object in `objects`.
async fn store_from(objects: &HashMap<String, Vec<u8>>) -> Arc<InMemory> {
    let store = Arc::new(InMemory::new());
    for (key, bytes) in objects {
        store
            .put(
                &StorePath::from(key.as_str()),
                PutPayload::from(bytes.clone()),
            )
            .await
            .expect("put");
    }
    store
}

/// A warming reader backed by an in-memory `key -> bytes` map. Resolves the
/// three range forms warming issues (`Full` for metadata, `Suffix` for
/// footers) so a warm walk exercises the real parse path without a cache engine.
struct MapWarmSource {
    /// Object key -> whole-object bytes.
    objects: HashMap<String, Vec<u8>>,
}

#[async_trait]
impl WarmSource for MapWarmSource {
    /// Returns `range` of `key`, or [`ReadError::NoSuchKey`] if absent.
    async fn read(&self, key: &CacheKey, range: ReadRange) -> Result<Bytes, ReadError> {
        let bytes = self.objects.get(&key.key).ok_or(ReadError::NoSuchKey)?;
        let len = bytes.len() as u64;
        let slice = match range {
            ReadRange::Full => bytes.clone(),
            ReadRange::From(start) => bytes[(start.min(len) as usize)..].to_vec(),
            ReadRange::Bounded(first, last) => {
                let end = (last.saturating_add(1)).min(len) as usize;
                bytes[(first.min(len) as usize)..end].to_vec()
            }
            ReadRange::Suffix(n) => bytes[(len.saturating_sub(n) as usize)..].to_vec(),
        };
        Ok(Bytes::from(slice))
    }
}

/// Acceptance #1: warming a table whose current metadata object is
/// `*.gz.metadata.json` succeeds — the gzip object is gunzipped before parse.
#[tokio::test]
async fn warming_gzip_metadata_object_succeeds() {
    let (mut objects, sidecar) = load_objects();
    let gz_key = install_gzip_metadata(&mut objects, &sidecar.metadata_key);
    let warmer = Warmer::new(
        Arc::new(MapWarmSource { objects }),
        WarmConfig::default(),
        Arc::new(TokenBucket::unlimited()),
    );

    let summary = warmer
        .warm_table(&WarmTarget {
            bucket: sidecar.bucket.clone(),
            metadata_key: gz_key,
            snapshot_id: sidecar.current_snapshot_id,
        })
        .await
        .expect("warm a gzip metadata object");
    assert!(
        summary.block_objects_warmed >= 1,
        "warming a gzip metadata table pins at least the metadata + manifest-list blocks"
    );
}

/// Acceptance #2: a plain `.metadata.json` object still warms unchanged.
#[tokio::test]
async fn warming_plain_metadata_object_still_succeeds() {
    let (objects, sidecar) = load_objects();
    let warmer = Warmer::new(
        Arc::new(MapWarmSource { objects }),
        WarmConfig::default(),
        Arc::new(TokenBucket::unlimited()),
    );

    let summary = warmer
        .warm_table(&WarmTarget {
            bucket: sidecar.bucket.clone(),
            metadata_key: sidecar.metadata_key.clone(),
            snapshot_id: sidecar.current_snapshot_id,
        })
        .await
        .expect("warm a plain metadata object");
    assert!(summary.block_objects_warmed >= 1);
}

/// Acceptance #3: a mapper rebuild succeeds against a gzip metadata object and
/// indexes the table's data files.
#[tokio::test]
async fn mapper_rebuild_over_gzip_metadata_succeeds() {
    let (mut objects, sidecar) = load_objects();
    let gz_key = install_gzip_metadata(&mut objects, &sidecar.metadata_key);
    let store = store_from(&objects).await;
    let fetch = ObjectStoreFetch::new(store as Arc<dyn ObjectStore>);
    let mapper = Mapper::new();
    let ident = TableIdent::new(&["db"], "events");

    mapper
        .apply_table(
            &fetch,
            &BuildInputs {
                ident: ident.clone(),
                bucket: sidecar.bucket.clone(),
                metadata_key: gz_key,
                snapshot_ids: vec![sidecar.current_snapshot_id],
                current_snapshot_id: Some(sidecar.current_snapshot_id),
            },
        )
        .await
        .expect("mapper rebuild over a gzip metadata object");

    let id = mapper.table_id(&ident).expect("table indexed");
    let state = mapper.state();
    let index = state.table(id).expect("index present");
    assert!(
        index.file_count() > 0,
        "rebuild from gzip metadata indexed the table's data files"
    );
    let data_key =
        "warehouse/db/events/data/category=a/00000-0-648b56f6-33a3-44a2-a346-18aeb807a036.parquet";
    assert!(
        matches!(
            mapper.classify(&sidecar.bucket, data_key, None),
            Classify::TableData { .. }
        ),
        "a known data file classifies to the rebuilt table"
    );
}

/// Acceptance #4: a mapper rebuild still succeeds against plain metadata (no
/// regression from the gunzip guard).
#[tokio::test]
async fn mapper_rebuild_over_plain_metadata_still_succeeds() {
    let (objects, sidecar) = load_objects();
    let store = store_from(&objects).await;
    let fetch = ObjectStoreFetch::new(store as Arc<dyn ObjectStore>);
    let mapper = Mapper::new();
    let ident = TableIdent::new(&["db"], "events");

    mapper
        .apply_table(
            &fetch,
            &BuildInputs {
                ident: ident.clone(),
                bucket: sidecar.bucket.clone(),
                metadata_key: sidecar.metadata_key.clone(),
                snapshot_ids: vec![sidecar.current_snapshot_id],
                current_snapshot_id: Some(sidecar.current_snapshot_id),
            },
        )
        .await
        .expect("mapper rebuild over a plain metadata object");

    let id = mapper.table_id(&ident).expect("table indexed");
    let state = mapper.state();
    assert!(state.table(id).expect("index present").file_count() > 0);
}
