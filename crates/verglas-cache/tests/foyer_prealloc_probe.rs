//! Empirical probe: does foyer's file device physically preallocate its full
//! logical capacity at build, or does on-disk (allocated-block) usage grow with
//! admitted data? The answer decides whether one NVMe budget can be shared
//! first-come-first-served between the block cache and the write-back fragment
//! store (#223). Prints logical vs physical sizes; run with `--nocapture`.

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use tempfile::TempDir;
use verglas_cache::HybridCacheEngine;
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_s3::{BackendStore, ObjectRead, PassthroughRead, ReadRange};

/// One mebibyte.
const MIB: u64 = 1024 * 1024;

/// Physically allocated bytes of a file (`st_blocks` × 512), as opposed to its
/// logical length — the figure that matters for a shared disk budget.
#[cfg(unix)]
fn physical_bytes(path: &std::path::Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path)
        .map(|m| m.blocks() * 512)
        .unwrap_or(0)
}

/// Logical file length.
fn logical_bytes(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Builds a 256 MiB engine, measures both foyer files' logical and physical
/// sizes at build, admits ~64 MiB, and measures again. The assertion encodes
/// the sharing precondition: physical usage at build is a small fraction of the
/// logical capacity and grows with admitted data.
#[tokio::test(flavor = "multi_thread")]
async fn foyer_disk_usage_grows_with_admissions() {
    let dir = TempDir::new().expect("temp dir");
    let capacity = 256 * MIB;
    let config = CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(capacity),
        dram_bytes: ByteSize(80 * MIB),
        data_block_bytes: ByteSize(8 * MIB),
        admission: verglas_core::config::Admission {
            enabled: false,
            ..verglas_core::config::Admission::default()
        },
        ..CacheConfig::default()
    };
    let store = Arc::new(InMemory::new());
    let backend = PassthroughRead::new(BackendStore::single(
        "default",
        "test-bucket",
        store.clone(),
    ));
    let engine = HybridCacheEngine::single_node(backend, &config)
        .await
        .expect("build engine");

    let blocks = dir.path().join("blocks.data");
    let meta = dir.path().join("meta.data");
    let at_build = (physical_bytes(&blocks), physical_bytes(&meta));
    println!(
        "AT BUILD: blocks.data logical={} physical={} | meta.data logical={} physical={}",
        logical_bytes(&blocks),
        at_build.0,
        logical_bytes(&meta),
        at_build.1
    );

    // Admit ~64 MiB of blocks (8 objects x 8 MiB).
    for i in 0..8u64 {
        let name = format!("probe-{i}.bin");
        let body = Bytes::from(vec![(i % 251) as u8; (8 * MIB) as usize]);
        store
            .put(&Path::from(name.as_str()), PutPayload::from(body))
            .await
            .expect("seed");
        let key = CacheKey {
            storage_binding_id: "default".to_owned(),
            bucket: "test-bucket".into(),
            key: name,
        };
        let got = engine.get(&key, ReadRange::Full).await.expect("read");
        let mut body = got.body;
        use futures::TryStreamExt;
        while body.try_next().await.expect("chunk").is_some() {}
    }
    engine.flush().await;

    let after = (physical_bytes(&blocks), physical_bytes(&meta));
    println!(
        "AFTER 64MiB ADMITTED: blocks.data logical={} physical={} | meta.data logical={} physical={}",
        logical_bytes(&blocks),
        after.0,
        logical_bytes(&meta),
        after.1
    );

    // The sharing precondition (#223): the device is sparse at build (physical
    // well under logical capacity) and physical usage grows with admissions.
    assert!(
        at_build.0 < capacity / 4,
        "blocks.data physically preallocated at build: {} of {capacity}",
        at_build.0
    );
    assert!(
        after.0 > at_build.0,
        "physical usage did not grow with admissions: {} -> {}",
        at_build.0,
        after.0
    );
}

/// The engine reports its remaining physical growth room — the bytes of its
/// logical disk capacity it has not yet physically consumed (#223). Full at a
/// fresh build (the device is sparse), shrinking as blocks are admitted. The
/// disk poll reads this to size the fragment store's live ceiling: fragments
/// may take exactly what the block cache is not using.
#[tokio::test(flavor = "multi_thread")]
async fn growth_room_starts_full_and_shrinks_with_admissions() {
    let dir = TempDir::new().expect("temp dir");
    let capacity = 256 * MIB;
    let config = CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(capacity),
        dram_bytes: ByteSize(80 * MIB),
        data_block_bytes: ByteSize(8 * MIB),
        admission: verglas_core::config::Admission {
            enabled: false,
            ..verglas_core::config::Admission::default()
        },
        ..CacheConfig::default()
    };
    let store = Arc::new(InMemory::new());
    let backend = PassthroughRead::new(BackendStore::single(
        "default",
        "test-bucket",
        store.clone(),
    ));
    let engine = HybridCacheEngine::single_node(backend, &config)
        .await
        .expect("build engine");

    let fresh = engine.disk_growth_room_bytes();
    let fresh_used = engine.disk_usage_bytes();
    assert!(
        fresh > capacity / 2,
        "a fresh sparse device has most of the budget as growth room, got {fresh} of {capacity}"
    );
    assert!(
        fresh <= capacity,
        "growth room can never exceed the budget: {fresh} > {capacity}"
    );

    // Admit blocks; growth room shrinks as the device physically fills.
    for i in 0..8u64 {
        let name = format!("gr-{i}.bin");
        let body = Bytes::from(vec![(i % 251) as u8; (8 * MIB) as usize]);
        store
            .put(&Path::from(name.as_str()), PutPayload::from(body))
            .await
            .expect("seed");
        let key = CacheKey {
            storage_binding_id: "default".to_owned(),
            bucket: "test-bucket".into(),
            key: name,
        };
        let got = engine.get(&key, ReadRange::Full).await.expect("read");
        let mut body = got.body;
        use futures::TryStreamExt;
        while body.try_next().await.expect("chunk").is_some() {}
    }
    engine.flush().await;

    let after = engine.disk_growth_room_bytes();
    let after_used = engine.disk_usage_bytes();
    assert!(
        after < fresh,
        "growth room must shrink as blocks are admitted: {fresh} -> {after}"
    );
    assert!(
        after_used > fresh_used,
        "reported disk usage must grow as blocks are admitted: {fresh_used} -> {after_used}"
    );
}
