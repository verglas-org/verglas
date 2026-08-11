//! Shared-cache regressions for versioned Neon pages. These tests require
//! reconstructed pages and ordinary object blocks to compete in one bounded
//! hybrid cache without a sequential scan displacing the reused page set.

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use tempfile::TempDir;
use verglas_cache::{HybridCacheEngine, MaterializedPageKey};
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_s3::{BackendStore, ObjectRead, PassthroughRead, ReadRange};

const MIB: u64 = 1024 * 1024;
const PAGE_BYTES: usize = 8192;
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Builds one engine with the production admission policy and the smallest
/// supported hybrid-cache budgets.
async fn engine(
    dir: &TempDir,
) -> (
    Arc<InMemory>,
    HybridCacheEngine<
        PassthroughRead,
        verglas_core::peer::NoopPeerFetch,
        verglas_core::ring::RendezvousRing,
    >,
) {
    let store = Arc::new(InMemory::new());
    let backend = PassthroughRead::new(BackendStore::single("default", "origin", store.clone()));
    let config = CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(96 * MIB),
        dram_bytes: ByteSize(80 * MIB),
        data_block_bytes: ByteSize(2 * MIB),
        ..CacheConfig::default()
    };
    let cache = HybridCacheEngine::single_node(backend, &config)
        .await
        .expect("build cache");
    (store, cache)
}

/// Returns one exact page identity at a fixed relation version.
fn page_key(block: u32, lsn: u64) -> MaterializedPageKey {
    MaterializedPageKey {
        tenant_shard_id: "11111111111111111111111111111111".to_owned(),
        timeline_id: "22222222222222222222222222222222".to_owned(),
        spcnode: 1663,
        dbnode: 16384,
        relnode: 24576,
        forknum: 0,
        block,
        lsn,
    }
}

/// Returns deterministic page bytes that identify their block.
fn page(block: u32) -> Bytes {
    Bytes::from(vec![(block % 251) as u8; PAGE_BYTES])
}

/// Versioned page identities never alias and invalid page bodies are rejected.
#[tokio::test(flavor = "multi_thread")]
async fn materialized_pages_are_exact_and_page_sized() {
    let _guard = TEST_LOCK.lock().await;
    let dir = TempDir::new().expect("temp dir");
    let (_, cache) = engine(&dir).await;
    let old = page_key(7, 100);
    let new = page_key(7, 200);

    assert!(cache.get_materialized_page(&old).await.is_none());
    assert!(
        cache
            .put_materialized_page(old.clone(), page(7))
            .await
            .expect("admit old")
    );
    assert_eq!(cache.get_materialized_page(&old).await, Some(page(7)));
    assert!(cache.get_materialized_page(&new).await.is_none());
    assert!(
        cache
            .put_materialized_page(new.clone(), page(8))
            .await
            .expect("admit new")
    );
    assert_eq!(cache.get_materialized_page(&old).await, Some(page(7)));
    assert_eq!(cache.get_materialized_page(&new).await, Some(page(8)));

    let error = cache
        .put_materialized_page(page_key(9, 200), Bytes::from_static(b"short"))
        .await
        .expect_err("non-page body must fail");
    assert!(error.to_string().contains("8192"));
}

/// A one-touch scan larger than the cache must not displace a repeatedly read
/// Neon working set. This also covers mixed workloads and variable-size
/// objects: 8 KiB Neon pages compete directly with 2 MiB Iceberg blocks.
#[tokio::test(flavor = "multi_thread")]
async fn hot_neon_pages_survive_a_one_touch_scan() {
    let _guard = TEST_LOCK.lock().await;
    let dir = TempDir::new().expect("temp dir");
    let (store, cache) = engine(&dir).await;
    let hot_pages = 1536_u32; // 12 MiB
    let cold_pages = 13_312_u32; // 104 MiB

    for block in 0..hot_pages {
        assert!(
            cache
                .put_materialized_page(page_key(block, 100), page(block))
                .await
                .expect("warm hot page")
        );
    }
    for _ in 0..2 {
        for block in 0..hot_pages {
            assert_eq!(
                cache.get_materialized_page(&page_key(block, 100)).await,
                Some(page(block)),
                "hot page {block} must hit while warming"
            );
        }
    }

    let scan_objects = cold_pages as usize * PAGE_BYTES / (2 * MIB as usize);
    for object in 0..scan_objects {
        let name = format!("iceberg/data/scan-{object}.parquet");
        let body = Bytes::from(vec![(object % 251) as u8; 2 * MIB as usize]);
        store
            .put(&Path::from(name.clone()), PutPayload::from(body.clone()))
            .await
            .expect("seed scan object");
        let read = cache
            .get(
                &CacheKey {
                    storage_binding_id: "default".to_owned(),
                    bucket: "origin".to_owned(),
                    key: name,
                },
                ReadRange::Full,
            )
            .await
            .expect("read scan object");
        let got = read
            .body
            .try_collect::<bytes::BytesMut>()
            .await
            .expect("body");
        assert_eq!(got.freeze(), body);
    }

    let mut hits = 0_u32;
    for block in 0..hot_pages {
        hits += u32::from(
            cache.get_materialized_page(&page_key(block, 100)).await == Some(page(block)),
        );
    }
    assert!(
        hits >= hot_pages * 9 / 10,
        "at least 90% of the reused Neon pages must survive one-touch churn; got {hits}/{hot_pages}"
    );
}

/// When the workload shifts, repeated demand for the new set must replace the
/// formerly hot set instead of pinning historical frequency forever.
#[tokio::test(flavor = "multi_thread")]
async fn materialized_page_heat_ages_toward_a_shifted_working_set() {
    let _guard = TEST_LOCK.lock().await;
    let dir = TempDir::new().expect("temp dir");
    let (_, cache) = engine(&dir).await;
    let set_pages = 1024_u32;

    for epoch in 0..2_u32 {
        let start = epoch * set_pages;
        for _ in 0..3 {
            for block in start..start + set_pages {
                if cache
                    .get_materialized_page(&page_key(block, 100))
                    .await
                    .is_none()
                {
                    let _ = cache
                        .put_materialized_page(page_key(block, 100), page(block))
                        .await
                        .expect("admit shifted page");
                }
            }
        }
    }

    let mut shifted_hits = 0_usize;
    for block in set_pages..2 * set_pages {
        shifted_hits += usize::from(
            cache
                .get_materialized_page(&page_key(block, 100))
                .await
                .is_some(),
        );
    }
    assert!(shifted_hits >= (set_pages as usize * 9 / 10));
}

/// A cyclic scanner has reuse, but its broad working set must not erase a much
/// smaller and more frequently reused OLTP set.
#[tokio::test(flavor = "multi_thread")]
async fn hot_pages_survive_a_cyclic_scan_larger_than_capacity() {
    let _guard = TEST_LOCK.lock().await;
    let dir = TempDir::new().expect("temp dir");
    let (_, cache) = engine(&dir).await;
    let hot_pages = 512_u32;
    let scan_start = 10_000_u32;
    let scan_pages = 12_288_u32; // 96 MiB, larger than the 80 MiB DRAM tier.

    for _ in 0..5 {
        for block in 0..hot_pages {
            let key = page_key(block, 100);
            if cache.get_materialized_page(&key).await.is_none() {
                let _ = cache
                    .put_materialized_page(key, page(block))
                    .await
                    .expect("admit hot page");
            }
        }
    }

    for _ in 0..3 {
        for block in scan_start..scan_start + scan_pages {
            let key = page_key(block, 100);
            if cache.get_materialized_page(&key).await.is_none() {
                let _ = cache
                    .put_materialized_page(key, page(block))
                    .await
                    .expect("admit cyclic scan page");
            }
        }
    }

    let mut hits = 0_u32;
    for block in 0..hot_pages {
        hits += u32::from(
            cache.get_materialized_page(&page_key(block, 100)).await == Some(page(block)),
        );
    }
    assert!(
        hits >= hot_pages * 4 / 5,
        "at least 80% of the OLTP set must survive cyclic churn; got {hits}/{hot_pages}"
    );
}
