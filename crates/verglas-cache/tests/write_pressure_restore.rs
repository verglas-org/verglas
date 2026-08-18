//! Drain-side restoration: after fragments persist to the origin and release
//! their bytes, the cache grows back into the returned space. Companion to
//! the frozen `write_pressure_reclaim` evaluator (claim on demand — this file
//! covers return on drain).

use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt as _;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStoreExt as _, PutPayload};
use tempfile::TempDir;
use verglas_cache::HybridCacheEngine;
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_s3::{BackendStore, ObjectMeta, ObjectRead, PassthroughRead, ReadError, ReadRange};

const BUCKET: &str = "test-bucket";
const MIB: u64 = 1024 * 1024;
const TEST_BLOCK_BYTES: u64 = 8 * MIB;

/// Deterministic payload: byte `i` is `(i + seed) % 251`.
fn pattern(seed: u64, len: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(len as usize);
    for i in 0..len {
        buf.extend_from_slice(&[(((i + seed) % 251) & 0xFF) as u8]);
    }
    buf.freeze()
}

/// Drains a full response body.
async fn read_all<R: ObjectRead>(
    reader: &R,
    key: &CacheKey,
    range: ReadRange,
) -> Result<(ObjectMeta, Range<u64>, Bytes), ReadError> {
    let got = reader.get(key, range).await?;
    let mut buf = BytesMut::new();
    let mut body = got.body;
    while let Some(chunk) = body.try_next().await? {
        buf.extend_from_slice(&chunk);
    }
    Ok((got.meta, got.range, buf.freeze()))
}

fn key(name: &str) -> CacheKey {
    CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: BUCKET.to_owned(),
        key: name.to_owned(),
    }
}

/// After a deep reclaim, `restore_disk` grows the store back and subsequent
/// fills physically land beyond the shrunken watermark.
#[tokio::test(flavor = "multi_thread")]
async fn restore_after_drain_lets_the_cache_grow_back() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let backend = PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone()));
    let config = CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(96 * MIB),
        dram_bytes: ByteSize(128 * MIB),
        data_block_bytes: ByteSize(TEST_BLOCK_BYTES),
        ..CacheConfig::default()
    };
    let engine = HybridCacheEngine::single_node(backend, &config)
        .await
        .expect("build engine");

    // Warm enough cold data that a reclaim has real bytes to free.
    for i in 0..8u64 {
        let name = format!("cold-{i}.parquet");
        let body = pattern(100 + i, TEST_BLOCK_BYTES);
        store
            .put(
                &StorePath::from(name.as_str()),
                PutPayload::from(body.clone()),
            )
            .await
            .expect("seed");
        let (_, _, got) = read_all(&engine, &key(&name), ReadRange::Full)
            .await
            .expect("fill");
        assert_eq!(got, body);
        engine.flush().await;
    }

    let reclaimed = engine
        .reclaim_disk_for_writeback(4 * TEST_BLOCK_BYTES)
        .await;
    assert!(reclaimed >= 4 * TEST_BLOCK_BYTES, "reclaim {reclaimed}");
    let room_shrunk = engine.disk_growth_room_bytes();

    // Drain finished: fragments released their bytes; keep no reserve.
    let restored = engine.restore_disk(0).await;
    assert!(
        restored >= 4 * TEST_BLOCK_BYTES,
        "restore must return the yielded footprint, got {restored}"
    );

    // The restored store accepts and physically lands new fills again.
    for i in 0..4u64 {
        let name = format!("fresh-{i}.parquet");
        let body = pattern(500 + i, TEST_BLOCK_BYTES);
        store
            .put(
                &StorePath::from(name.as_str()),
                PutPayload::from(body.clone()),
            )
            .await
            .expect("seed fresh");
        let (_, _, got) = read_all(&engine, &key(&name), ReadRange::Full)
            .await
            .expect("fresh fill");
        assert_eq!(got, body);
        engine.flush().await;
    }
    let room_regrown = engine.disk_growth_room_bytes();
    assert!(
        room_regrown < room_shrunk,
        "post-restore fills must consume the returned space: shrunk-room {room_shrunk}, now {room_regrown}"
    );
}

/// Restoring with a reserve keeps that many bytes yielded.
#[tokio::test(flavor = "multi_thread")]
async fn restore_honors_the_requested_reserve() {
    let dir = TempDir::new().expect("temp dir");
    let store = Arc::new(InMemory::new());
    let backend = PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone()));
    let config = CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(96 * MIB),
        dram_bytes: ByteSize(128 * MIB),
        data_block_bytes: ByteSize(TEST_BLOCK_BYTES),
        ..CacheConfig::default()
    };
    let engine = HybridCacheEngine::single_node(backend, &config)
        .await
        .expect("build engine");
    for i in 0..8u64 {
        let name = format!("cold-{i}.parquet");
        let body = pattern(100 + i, TEST_BLOCK_BYTES);
        store
            .put(
                &StorePath::from(name.as_str()),
                PutPayload::from(body.clone()),
            )
            .await
            .expect("seed");
        let (_, _, got) = read_all(&engine, &key(&name), ReadRange::Full)
            .await
            .expect("fill");
        assert_eq!(got, body);
        engine.flush().await;
    }
    let reclaimed = engine
        .reclaim_disk_for_writeback(4 * TEST_BLOCK_BYTES)
        .await;
    assert!(reclaimed >= 4 * TEST_BLOCK_BYTES);

    let reserve = 2 * TEST_BLOCK_BYTES;
    let restored_partial = engine.restore_disk(reserve).await;
    let restored_rest = engine.restore_disk(0).await;
    assert!(
        restored_rest >= TEST_BLOCK_BYTES,
        "the reserve must have stayed yielded until released, second restore {restored_rest}"
    );
    assert!(restored_partial > 0);
}
