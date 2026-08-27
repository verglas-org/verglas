//! Acceptance tests for S3-CAS Turso authority and Foyer-only local caching.

use std::sync::Arc;

use object_store::memory::InMemory;
use tempfile::TempDir;
use verglas_backend::{BackendStore, BackendStores, MultipartObjectStore};
use verglas_cache::HybridCacheEngine;
use verglas_core::config::{Admission, ByteSize, Cache as CacheConfig};
use verglas_do_turso::{TursoCasStorage, TursoStore};
use verglas_s3::PassthroughRead;

const BINDING: &str = "do-storage";
const BUCKET: &str = "do-01";

/// Builds the one Foyer engine used for Turso and object/Iceberg reads.
async fn storage(
    origin: Arc<InMemory>,
    cache_dir: &TempDir,
) -> Result<TursoCasStorage, Box<dyn std::error::Error>> {
    let typed: Arc<dyn MultipartObjectStore> = origin;
    let stores: Arc<dyn BackendStores> = BackendStore::single(BINDING, BUCKET, typed);
    let cache = HybridCacheEngine::new(
        PassthroughRead::new(Arc::clone(&stores)),
        &CacheConfig {
            dir: cache_dir.path().to_path_buf(),
            capacity_bytes: ByteSize(8 * 1024 * 1024),
            dram_bytes: ByteSize(8 * 1024 * 1024),
            data_block_bytes: ByteSize(1024 * 1024),
            admission: Admission {
                enabled: false,
                ..Admission::default()
            },
        },
    )
    .await?;
    Ok(TursoCasStorage::new(
        stores, cache, BINDING, BUCKET, "turso",
    )?)
}

/// Recovery retains Foyer and advances its cached baseline from the remote CAS head.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_checks_cas_head_and_applies_only_the_missing_delta()
-> Result<(), Box<dyn std::error::Error>> {
    let origin = Arc::new(InMemory::new());
    let cache = tempfile::tempdir()?;
    let storage = storage(origin, &cache).await?;
    let first = TursoStore::open_cas(storage.clone(), "do-01").await?;
    let event = first.begin_event().await?;
    event
        .execute("CREATE TABLE tenant_rows (id INTEGER PRIMARY KEY, payload TEXT)")
        .await?;
    event
        .execute("INSERT INTO tenant_rows VALUES (1, 'survives')")
        .await?;
    event.commit().await?;
    drop(first);

    let recovered = TursoStore::open_cas(storage, "do-01").await?;
    let recovery = recovered.recovery_stats();
    assert!(recovery.remote_generation >= recovery.local_generation);
    assert_eq!(
        recovery.applied_segments,
        recovery.remote_generation - recovery.local_generation,
        "recovery must apply only the CAS-head delta after the cached baseline"
    );
    let event = recovered.begin_event().await?;
    assert_eq!(
        event
            .query_json("SELECT id, payload FROM tenant_rows ORDER BY id")
            .await?,
        serde_json::json!([{ "id": 1, "payload": "survives" }])
    );
    event.rollback().await?;
    Ok(())
}
