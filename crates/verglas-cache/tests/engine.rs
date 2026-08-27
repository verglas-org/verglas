//! Focused behavior tests for the single-node origin cache.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::Future;
use futures::stream::{self, StreamExt, TryStreamExt};
use tempfile::tempdir;
use verglas_cache::{EngineError, HybridCacheEngine};
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_core::read::{
    BodyStream, ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange, ServedTier, TierCell,
};
use verglas_core::write::Invalidation;

/// A deterministic origin with observable HEAD and GET counts.
#[derive(Clone)]
struct MockOrigin {
    bytes: Arc<std::sync::Mutex<Bytes>>,
    etag: Arc<std::sync::Mutex<String>>,
    heads: Arc<AtomicU64>,
    gets: Arc<AtomicU64>,
    delay: Option<Duration>,
}

impl MockOrigin {
    /// Creates an origin serving one immutable object.
    fn new(bytes: &[u8], etag: &str) -> Self {
        Self {
            bytes: Arc::new(std::sync::Mutex::new(Bytes::copy_from_slice(bytes))),
            etag: Arc::new(std::sync::Mutex::new(etag.to_owned())),
            heads: Arc::new(AtomicU64::new(0)),
            gets: Arc::new(AtomicU64::new(0)),
            delay: None,
        }
    }

    /// Adds an origin delay so concurrent requests overlap at the fill seam.
    fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Replaces the origin version for invalidation testing.
    fn replace(&self, bytes: &[u8], etag: &str) {
        *self.bytes.lock().expect("bytes lock") = Bytes::copy_from_slice(bytes);
        *self.etag.lock().expect("etag lock") = etag.to_owned();
    }

    /// Returns the number of origin GETs issued.
    fn get_count(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }
}

impl ObjectRead for MockOrigin {
    /// Serves the requested range from the current origin version.
    fn get(
        &self,
        _key: &CacheKey,
        requested: ReadRange,
    ) -> impl Future<Output = Result<ObjectGet, ReadError>> + Send {
        let bytes = self.bytes.clone();
        let etag = self.etag.clone();
        let gets = self.gets.clone();
        let delay = self.delay;
        async move {
            gets.fetch_add(1, Ordering::Relaxed);
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let bytes = bytes.lock().expect("bytes lock").clone();
            let etag = etag.lock().expect("etag lock").clone();
            let range = match requested {
                ReadRange::Full => 0..bytes.len() as u64,
                ReadRange::Bounded(first, last) => first..(last + 1).min(bytes.len() as u64),
                ReadRange::From(first) => first..bytes.len() as u64,
                ReadRange::Suffix(length) => {
                    bytes.len().saturating_sub(length as usize) as u64..bytes.len() as u64
                }
            };
            if range.start >= range.end && !bytes.is_empty() {
                return Err(ReadError::InvalidRange);
            }
            let meta = ObjectMeta {
                size: bytes.len() as u64,
                e_tag: Some(etag),
                ..ObjectMeta::default()
            };
            let body =
                stream::once(
                    async move { Ok(bytes.slice(range.start as usize..range.end as usize)) },
                )
                .boxed();
            Ok(ObjectGet {
                meta,
                range,
                body,
                served_from: TierCell::new(),
            })
        }
    }

    /// Returns current origin metadata.
    fn head(&self, _key: &CacheKey) -> impl Future<Output = Result<ObjectMeta, ReadError>> + Send {
        let bytes = self.bytes.clone();
        let etag = self.etag.clone();
        let heads = self.heads.clone();
        async move {
            heads.fetch_add(1, Ordering::Relaxed);
            Ok(ObjectMeta {
                size: bytes.lock().expect("bytes lock").len() as u64,
                e_tag: Some(etag.lock().expect("etag lock").clone()),
                ..ObjectMeta::default()
            })
        }
    }
}

/// Builds a small but valid cache configuration for one test directory.
fn config(dir: &Path) -> CacheConfig {
    CacheConfig {
        dir: dir.to_path_buf(),
        capacity_bytes: ByteSize(8 * 1024 * 1024),
        dram_bytes: ByteSize(64 * 1024 * 1024),
        data_block_bytes: ByteSize(1024 * 1024),
        admission: verglas_core::config::Admission {
            enabled: false,
            ..Default::default()
        },
    }
}

/// Reads one body stream into one byte buffer.
async fn body_bytes(body: BodyStream) -> Bytes {
    let chunks = body.try_collect::<Vec<_>>().await.expect("body");
    let mut result = Vec::new();
    for chunk in chunks {
        result.extend_from_slice(&chunk);
    }
    Bytes::from(result)
}

/// The ETag and range geometry produce a reusable DRAM block hit.
#[tokio::test]
async fn range_reads_fill_and_hit() {
    let directory = tempdir().expect("cache directory");
    let origin = MockOrigin::new(b"0123456789abcdef", "v1");
    let engine = HybridCacheEngine::new(origin.clone(), &config(directory.path()))
        .await
        .expect("cache");
    let key = CacheKey {
        storage_binding_id: "binding".into(),
        bucket: "bucket".into(),
        key: "object".into(),
    };

    let first = engine
        .get(&key, ReadRange::Bounded(2, 7))
        .await
        .expect("first read");
    assert_eq!(body_bytes(first.body).await, Bytes::from_static(b"234567"));
    let second = engine
        .get(&key, ReadRange::Bounded(2, 7))
        .await
        .expect("second read");
    assert_eq!(body_bytes(second.body).await, Bytes::from_static(b"234567"));
    assert_eq!(
        origin.get_count(),
        1,
        "the second read is served from cache"
    );
    assert_eq!(engine.counters().snapshot().backend_fills, 1);
    assert!(engine.counters().snapshot().dram_hits >= 1);
}

/// Invalidation drops the mapping, so a changed origin version cannot reuse old
/// bytes even though old ETag-keyed blocks remain physically present.
#[tokio::test]
async fn invalidation_forces_a_fresh_etag_fill() {
    let directory = tempdir().expect("cache directory");
    let origin = MockOrigin::new(b"old", "v1");
    let engine = HybridCacheEngine::new(origin.clone(), &config(directory.path()))
        .await
        .expect("cache");
    let key = CacheKey {
        storage_binding_id: "binding".into(),
        bucket: "bucket".into(),
        key: "object".into(),
    };
    let first = engine.get(&key, ReadRange::Full).await.expect("first read");
    assert_eq!(body_bytes(first.body).await, Bytes::from_static(b"old"));
    origin.replace(b"new", "v2");
    engine
        .invalidate(std::slice::from_ref(&key))
        .await
        .expect("invalidate");
    let second = engine
        .get(&key, ReadRange::Full)
        .await
        .expect("second read");
    assert_eq!(body_bytes(second.body).await, Bytes::from_static(b"new"));
    assert_eq!(origin.get_count(), 2);
}

/// Persistent blocks survive a clean close and are reused after a new engine
/// opens the same cache directory.
#[tokio::test]
async fn persistent_blocks_recover_without_an_origin_get() {
    let directory = tempdir().expect("cache directory");
    let origin = MockOrigin::new(b"persistent", "v1");
    let key = CacheKey {
        storage_binding_id: "binding".into(),
        bucket: "bucket".into(),
        key: "object".into(),
    };
    {
        let engine = HybridCacheEngine::new(origin.clone(), &config(directory.path()))
            .await
            .expect("cache");
        let response = engine.get(&key, ReadRange::Full).await.expect("first read");
        assert_eq!(
            body_bytes(response.body).await,
            Bytes::from_static(b"persistent")
        );
        engine.flush().await;
    }
    let recovered = HybridCacheEngine::new(origin.clone(), &config(directory.path()))
        .await
        .expect("recovered cache");
    let response = recovered
        .get(&key, ReadRange::Full)
        .await
        .expect("recovered read");
    let served_from = response.served_from.clone();
    assert_eq!(
        body_bytes(response.body).await,
        Bytes::from_static(b"persistent")
    );
    assert_eq!(served_from.get(), ServedTier::Nvme);
    assert_eq!(
        origin.get_count(),
        1,
        "the recovered block avoids a second GET"
    );
}

/// Recovered bytes from an old ETag are not addressable by a newer mapping.
#[tokio::test]
async fn etag_version_isolation_survives_reopen() {
    let directory = tempdir().expect("cache directory");
    let origin = MockOrigin::new(b"version-one", "v1");
    let key = CacheKey {
        storage_binding_id: "binding".into(),
        bucket: "bucket".into(),
        key: "object".into(),
    };
    {
        let engine = HybridCacheEngine::new(origin.clone(), &config(directory.path()))
            .await
            .expect("cache");
        let response = engine.get(&key, ReadRange::Full).await.expect("first read");
        assert_eq!(
            body_bytes(response.body).await,
            Bytes::from_static(b"version-one")
        );
        engine.flush().await;
    }
    origin.replace(b"version-two", "v2");
    let recovered = HybridCacheEngine::new(origin.clone(), &config(directory.path()))
        .await
        .expect("recovered cache");
    let response = recovered
        .get(&key, ReadRange::Full)
        .await
        .expect("new version");
    assert_eq!(
        body_bytes(response.body).await,
        Bytes::from_static(b"version-two")
    );
    assert_eq!(
        origin.get_count(),
        2,
        "the new ETag requires one fresh fill"
    );
}

/// Concurrent cold readers share one origin GET for the same immutable block.
#[tokio::test]
async fn concurrent_fills_are_coalesced() {
    let directory = tempdir().expect("cache directory");
    let origin = MockOrigin::new(b"coalesced", "v1").with_delay(Duration::from_millis(25));
    let engine = HybridCacheEngine::new(origin.clone(), &config(directory.path()))
        .await
        .expect("cache");
    let key = CacheKey {
        storage_binding_id: "binding".into(),
        bucket: "bucket".into(),
        key: "object".into(),
    };
    let tasks = (0..8)
        .map(|_| {
            let engine = engine.clone();
            let key = key.clone();
            tokio::spawn(async move {
                let response = engine.get(&key, ReadRange::Full).await.expect("read");
                body_bytes(response.body).await
            })
        })
        .collect::<Vec<_>>();
    for task in tasks {
        assert_eq!(
            task.await.expect("reader task"),
            Bytes::from_static(b"coalesced")
        );
    }
    assert_eq!(origin.get_count(), 1, "one origin GET serves all readers");
    assert!(engine.counters().snapshot().deduped_fills >= 1);
}

/// DRAM and persistent budgets reject configurations that cannot hold the
/// minimum mapping and block working sets.
#[tokio::test]
async fn undersized_budgets_are_rejected() {
    let directory = tempdir().expect("cache directory");
    let origin = MockOrigin::new(b"bytes", "v1");
    let mut tiny_dram = config(directory.path());
    tiny_dram.dram_bytes = ByteSize(4096);
    assert!(matches!(
        HybridCacheEngine::new(origin.clone(), &tiny_dram).await,
        Err(EngineError::DramBudgetTooSmall(_))
    ));
    let mut tiny_disk = config(directory.path());
    tiny_disk.capacity_bytes = ByteSize(1024 * 1024);
    assert!(matches!(
        HybridCacheEngine::new(origin, &tiny_disk).await,
        Err(EngineError::DiskBudgetTooSmall(_))
    ));
}
