//! Focused acceptance tests for the runtime-owned Iceberg origin storage.
//!
//! These tests use real `object_store::InMemory` origins behind `BackendStore`.
//! They prove that Iceberg reads fill and hit Foyer, survive an NVMe reopen, and
//! cannot escape the one configured binding and bucket.

use std::sync::Arc;

use bytes::Bytes;
use iceberg::io::{FileRead, Storage, StorageConfig, StorageFactory};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use tempfile::TempDir;
use verglas_backend::{BackendStore, BackendStores, MultipartObjectStore};
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_runtime::{OriginStorageConfig, OriginStorageFactory};

/// Builds a small valid Foyer configuration rooted at `directory`.
fn cache_config(directory: &TempDir) -> CacheConfig {
    CacheConfig {
        dir: directory.path().to_path_buf(),
        capacity_bytes: ByteSize(8 * 1024 * 1024),
        dram_bytes: ByteSize(64 * 1024 * 1024),
        data_block_bytes: ByteSize(1024 * 1024),
        admission: verglas_core::config::Admission {
            enabled: false,
            ..Default::default()
        },
    }
}

/// Creates a one-binding backend over one in-memory bucket and keeps the origin
/// handle so tests can observe durable bytes independently of the cache.
fn backend(binding: &str, bucket: &str, origin: Arc<InMemory>) -> Arc<dyn BackendStores> {
    let store: Arc<dyn MultipartObjectStore> = origin;
    BackendStore::single(binding, bucket, store)
}

/// Writes one object directly to the in-memory origin before opening the cache.
async fn origin_put(origin: &InMemory, key: &str, bytes: &'static [u8]) {
    origin
        .put(
            &Path::from(key),
            PutPayload::from_bytes(Bytes::from_static(bytes)),
        )
        .await
        .expect("origin put");
}

/// Reads all bytes from an Iceberg input file.
async fn read(storage: &Arc<dyn Storage>, location: &str) -> iceberg::Result<Bytes> {
    storage.new_input(location)?.read().await
}

/// Opening one explicit binding and bucket routes the first read to origin and
/// the repeated read to the DRAM Foyer block cache.
#[tokio::test]
async fn origin_fill_then_cache_hit_uses_exact_iceberg_location()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let origin = Arc::new(InMemory::new());
    origin_put(&origin, "table/data.parquet", b"origin-bytes").await;
    let stores = backend("managed", "lake", origin);
    let factory = OriginStorageFactory::new(
        stores,
        OriginStorageConfig::new("managed", "lake", cache_config(&directory)),
    )
    .await?;
    let storage = factory.build(&StorageConfig::new())?;

    assert_eq!(
        read(&storage, "s3://lake/table/data.parquet").await?,
        Bytes::from_static(b"origin-bytes")
    );
    assert_eq!(
        read(&storage, "s3://lake/table/data.parquet").await?,
        Bytes::from_static(b"origin-bytes")
    );
    let counters = factory.cache().counters().snapshot();
    assert_eq!(counters.backend_fills, 1);
    assert!(counters.dram_hits >= 1);
    Ok(())
}

/// Foyer's persistent block tier is reopened under the same directory and
/// serves the exact object version without a second origin GET/fill.
#[tokio::test]
async fn persistent_nvme_reopen_serves_the_same_object_version()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let origin = Arc::new(InMemory::new());
    origin_put(&origin, "table/metadata.json", b"persistent-metadata").await;
    let stores = backend("managed", "lake", origin);
    let config = cache_config(&directory);
    {
        let factory = OriginStorageFactory::new(
            stores.clone(),
            OriginStorageConfig::new("managed", "lake", config.clone()),
        )
        .await?;
        let storage = factory.build(&StorageConfig::new())?;
        assert_eq!(
            read(&storage, "s3://lake/table/metadata.json").await?,
            Bytes::from_static(b"persistent-metadata")
        );
        factory.cache().flush().await;
    }

    let reopened =
        OriginStorageFactory::new(stores, OriginStorageConfig::new("managed", "lake", config))
            .await?;
    let storage = reopened.build(&StorageConfig::new())?;
    assert_eq!(
        read(&storage, "s3://lake/table/metadata.json").await?,
        Bytes::from_static(b"persistent-metadata")
    );
    let counters = reopened.cache().counters().snapshot();
    assert_eq!(counters.backend_fills, 0);
    assert!(counters.disk_hits >= 1);
    Ok(())
}

/// A retry of an immutable object invalidates its old mapping only after the
/// origin is known durable, and a new object is served after its durable write.
#[tokio::test]
async fn durable_write_then_invalidation_prevents_stale_cache_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let origin = Arc::new(InMemory::new());
    origin_put(&origin, "table/metadata.json", b"old-metadata").await;
    let stores = backend("managed", "lake", origin.clone());
    let factory = OriginStorageFactory::new(
        stores,
        OriginStorageConfig::new("managed", "lake", cache_config(&directory)),
    )
    .await?;
    let storage = factory.build(&StorageConfig::new())?;
    let location = "s3://lake/table/metadata.json";
    assert_eq!(
        read(&storage, location).await?,
        Bytes::from_static(b"old-metadata")
    );

    let output = storage.new_output(location)?;
    let mut writer = output.writer().await?;
    writer.write(Bytes::from_static(b"old-metadata")).await?;
    writer.close().await?;
    assert_eq!(
        origin
            .get(&Path::from("table/metadata.json"))
            .await?
            .bytes()
            .await?,
        Bytes::from_static(b"old-metadata")
    );
    assert_eq!(
        read(&storage, location).await?,
        Bytes::from_static(b"old-metadata")
    );
    let conflict = storage
        .new_output(location)?
        .write(Bytes::from_static(b"different-metadata"))
        .await
        .expect_err("immutable paths reject different retry bytes");
    assert!(conflict.to_string().contains("immutable Iceberg object"));

    let new_location = "s3://lake/table/metadata-v2.json";
    storage
        .new_output(new_location)?
        .write(Bytes::from_static(b"new-metadata"))
        .await?;
    assert_eq!(
        origin
            .get(&Path::from("table/metadata-v2.json"))
            .await?
            .bytes()
            .await?,
        Bytes::from_static(b"new-metadata")
    );
    assert_eq!(
        read(&storage, new_location).await?,
        Bytes::from_static(b"new-metadata")
    );
    assert_eq!(factory.cache().counters().snapshot().backend_fills, 2);
    Ok(())
}

/// A factory fixes both identity dimensions. Equal object names in another
/// binding or bucket are never reachable through its Iceberg storage surface.
#[tokio::test]
async fn binding_and_bucket_are_exactly_isolated() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let managed_origin = Arc::new(InMemory::new());
    let customer_origin = Arc::new(InMemory::new());
    origin_put(&managed_origin, "same/key", b"managed-bytes").await;
    origin_put(&customer_origin, "same/key", b"customer-bytes").await;
    let managed = BackendStore::single(
        "managed",
        "lake",
        Arc::clone(&managed_origin) as Arc<dyn MultipartObjectStore>,
    );
    let customer = BackendStore::single(
        "customer",
        "lake",
        Arc::clone(&customer_origin) as Arc<dyn MultipartObjectStore>,
    );
    let registry = verglas_backend::BackendRegistry::new(vec![managed, customer])?;
    let factory = OriginStorageFactory::new(
        registry,
        OriginStorageConfig::new("managed", "lake", cache_config(&directory)),
    )
    .await?;
    let storage = factory.build(&StorageConfig::new())?;

    assert_eq!(
        read(&storage, "s3://lake/same/key").await?,
        Bytes::from_static(b"managed-bytes")
    );
    assert!(storage.new_input("s3://other/same/key").is_err());
    assert!(storage.new_input("s3://customer/same/key").is_err());
    assert!(storage.new_input("file:///tmp/same/key").is_err());
    Ok(())
}

/// Range reads use the same fixed binding, bucket, object version, and block
/// geometry as full reads, and an identical range is served from Foyer.
#[tokio::test]
async fn ranged_reads_reuse_the_exact_cached_block() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let origin = Arc::new(InMemory::new());
    origin_put(&origin, "table/data.bin", b"0123456789abcdef").await;
    let factory = OriginStorageFactory::new(
        backend("managed", "lake", origin),
        OriginStorageConfig::new("managed", "lake", cache_config(&directory)),
    )
    .await?;
    let storage = factory.build(&StorageConfig::new())?;
    let input = storage.new_input("s3://lake/table/data.bin")?;
    let first = input.reader().await?.read(2..8).await?;
    let second = input.reader().await?.read(2..8).await?;
    assert_eq!(first, Bytes::from_static(b"234567"));
    assert_eq!(second, Bytes::from_static(b"234567"));
    assert_eq!(factory.cache().counters().snapshot().backend_fills, 1);
    Ok(())
}

/// Ambiguous or traversal-bearing object keys fail before any origin access.
#[tokio::test]
async fn object_locations_reject_ambiguous_path_segments() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let origin = Arc::new(InMemory::new());
    let factory = OriginStorageFactory::new(
        backend("managed", "lake", origin),
        OriginStorageConfig::new("managed", "lake", cache_config(&directory)),
    )
    .await?;
    let storage = factory.build(&StorageConfig::new())?;

    for location in [
        "s3://lake/table/../escape",
        "s3://lake/table/./data",
        "s3://lake/table//data",
        "s3://lake/table\\data",
        "s3://lake/table/data?version=other",
        "s3://lake/table/data#fragment",
        "s3://lake/table/\u{0}data",
    ] {
        assert!(
            storage.new_input(location).is_err(),
            "ambiguous location was accepted: {location:?}"
        );
    }
    Ok(())
}
