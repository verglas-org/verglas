//! Restart-recovery integration tests for the hybrid cache engine (issue #16),
//! mapped to the acceptance criteria: a server restart on the same directory
//! cache re-opens foyer's disk tier and rebuilds the in-memory index, so
//! previously cached keys serve from NVMe with **zero backend block re-fills**;
//! an unclean crash (a torn write, simulated here by corrupting the
//! backing file) drops the partial entries silently and never serves wrong
//! bytes; and the recovery time for a representative fill is measured and
//! recorded. Origin is `object_store`'s in-memory backend; the cache directory
//! lives in a temp dir and is deliberately reused across engine instances to
//! model the warm restart.
//!
//! The subtlety this file pins (from #121's docs): the key→ETag mapping is
//! DRAM-only and must NOT be resurrected on restart — every test proves the
//! first post-restart read re-resolves metadata against the origin (one backend
//! round trip) and only then reuses the surviving disk blocks.

use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use tempfile::TempDir;
use verglas_cache::{BLOCK_SIZE_BYTES, HybridCacheEngine};
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_s3::{
    BackendStore, ObjectGet, ObjectMeta, ObjectRead, PassthroughRead, ReadError, ReadRange,
};

/// The bucket the in-memory origin serves.
const BUCKET: &str = "restart-bucket";

/// Deterministic payload: byte `i` is `(i + seed) % 251` (prime, so block
/// alignment can never mask offset bugs).
fn pattern(seed: u64, len: u64) -> Bytes {
    (0..len)
        .map(|i| ((i + seed) % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// The logical key for an object in the test bucket.
fn key(k: &str) -> CacheKey {
    CacheKey {
        bucket: BUCKET.to_owned(),
        key: k.to_owned(),
    }
}

/// Cache config over a fixed directory with explicit budgets. The directory is
/// passed in (not a fresh `TempDir` per call) so two engine instances can share
/// one on-disk cache — that reuse is exactly what warm restart depends on.
fn cache_config(dir: &std::path::Path, capacity: u64, dram: u64) -> CacheConfig {
    CacheConfig {
        dir: dir.to_path_buf(),
        capacity_bytes: ByteSize(capacity),
        dram_bytes: ByteSize(dram),
        ..CacheConfig::default()
    }
}

/// Seeds an object into the in-memory origin.
async fn seed(store: &Arc<InMemory>, k: &str, bytes: Bytes) {
    store
        .put(&Path::from(k), PutPayload::from(bytes))
        .await
        .expect("seed object");
}

/// Independent counts of the backend requests an engine actually issued — the
/// ground truth the engine's own counters are checked against.
#[derive(Clone, Default)]
struct BackendCalls {
    /// Number of `get` calls (fills and passthroughs alike).
    gets: Arc<AtomicU64>,
    /// Number of `head` calls.
    heads: Arc<AtomicU64>,
}

impl BackendCalls {
    /// Loads `(gets, heads)`.
    fn snapshot(&self) -> (u64, u64) {
        (
            self.gets.load(Ordering::Relaxed),
            self.heads.load(Ordering::Relaxed),
        )
    }
}

/// An `ObjectRead` that counts calls before delegating.
struct CountingRead<R> {
    /// The wrapped reader.
    inner: R,
    /// Shared call counts.
    calls: BackendCalls,
}

impl<R: ObjectRead> ObjectRead for CountingRead<R> {
    /// Counts, then delegates.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.calls.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key, range).await
    }

    /// Counts, then delegates.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.calls.heads.fetch_add(1, Ordering::Relaxed);
        self.inner.head(key).await
    }
}

/// The concrete engine type these tests build: a counting single-node engine.
type Engine = HybridCacheEngine<
    CountingRead<PassthroughRead>,
    verglas_core::peer::NoopPeerFetch,
    verglas_core::ring::RendezvousRing,
>;

/// Builds a counting single-node engine over the given origin and config,
/// returning the engine and the independent backend call counts. Every call
/// with the same `dir` in the config opens the same on-disk cache — the reuse
/// warm restart depends on.
async fn engine_over(store: Arc<InMemory>, config: &CacheConfig) -> (Engine, BackendCalls) {
    let calls = BackendCalls::default();
    let backend = CountingRead {
        inner: PassthroughRead::new(BackendStore::single(BUCKET, store)),
        calls: calls.clone(),
    };
    let engine = HybridCacheEngine::single_node(backend, config)
        .await
        .expect("build engine");
    (engine, calls)
}

/// Reads a range through the engine, collecting the streamed body.
async fn read_all(
    reader: &Engine,
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

/// Returns the sole data-tier backing file that contains every Foyer region.
fn block_data_file(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("blocks.data")
}

/// The core acceptance criterion (#16): populate the cache, drop the engine
/// (a clean restart), re-open the same directory, and prove previously cached
/// keys serve from the recovered NVMe tier with **zero backend block re-fills**
/// — while the DRAM-only key→ETag mapping is *not* resurrected, so each object
/// costs exactly one metadata round trip (a HEAD) before its blocks are reused.
#[tokio::test(flavor = "multi_thread")]
async fn restart_recovers_disk_blocks_with_one_metadata_roundtrip_and_zero_block_refills() {
    let dir = TempDir::new().expect("temp dir");
    // DRAM floor (two-block memory tier) so the working set spills to disk and
    // recovery is genuinely reloading from NVMe, not the memory tier. Disk
    // budget = comfortable headroom over the eight blocks so foyer's reclaimer
    // never evicts a live block during the warm phase.
    let config = cache_config(dir.path(), 256 * BLOCK_SIZE_BYTES, 80 * 1024 * 1024);
    let store = Arc::new(InMemory::new());

    // Eight distinct single-block objects, seeded and warmed into the cache.
    let objects: Vec<(String, Bytes)> = (0..8)
        .map(|i| {
            (
                format!("warm-{i}.parquet"),
                pattern(100 + i, BLOCK_SIZE_BYTES),
            )
        })
        .collect();
    for (name, body) in &objects {
        seed(&store, name, body.clone()).await;
    }

    // --- Warm phase: fill the disk tier, then drop the engine (clean stop). ---
    {
        let (engine, calls) = engine_over(store.clone(), &config).await;
        for (name, body) in &objects {
            let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
                .await
                .expect("warm read");
            assert_eq!(&got, body, "warm read must be byte-identical");
            engine.flush().await;
        }
        assert_eq!(
            engine.counters().snapshot().backend_fills,
            8,
            "one fill per block during the warm phase"
        );
        assert_eq!(calls.snapshot(), (8, 0), "warm phase: eight GETs, no HEADs");
        // Drop closes foyer, flushing the disk tier durably.
    }

    // --- Restart phase: a fresh engine over the SAME directory. ---
    let (engine, calls) = engine_over(store.clone(), &config).await;

    for (name, body) in &objects {
        // The DRAM-only mapping is gone: HEAD must re-resolve it against the
        // origin (one backend round trip). If the mapping had been wrongly
        // resurrected, this HEAD would be served from cache with zero backend
        // HEADs — so the head-count assertion below is the "not resurrected"
        // proof.
        engine.head(&key(name)).await.expect("re-resolve mapping");
        let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("post-restart read");
        assert_eq!(
            &got, body,
            "recovered block must be byte-identical to origin"
        );
    }

    let counters = engine.counters().snapshot();
    assert_eq!(
        counters.backend_fills, 0,
        "recovered blocks must be reused with zero backend block re-fills"
    );
    assert_eq!(
        counters.disk_hits, 8,
        "every block must be served from the recovered NVMe tier"
    );
    assert_eq!(
        counters.backend_heads, 8,
        "each object re-resolves its DRAM-only mapping with exactly one HEAD"
    );
    assert_eq!(
        calls.snapshot(),
        (0, 8),
        "post-restart: zero backend GETs (no block re-fills), eight HEADs (metadata re-resolve)"
    );
}

/// Recovery covers BOTH foyer stores (#16): the dedicated metadata store (#50)
/// also persists its NVMe tier and recovers it on restart. Warm enough pinned
/// metadata to overflow the small meta DRAM front so it spills to the meta NVMe
/// tier, drop, re-open, and prove the spilled metadata comes back served from
/// the recovered meta store (a `meta_hit`) with byte-identical content — after
/// the same one-metadata-round-trip mapping re-resolution.
#[tokio::test(flavor = "multi_thread")]
async fn restart_recovers_pinned_metadata_from_the_meta_store() {
    let dir = TempDir::new().expect("temp dir");
    // Large data disk so the meta tier (a fraction of it) is comfortably sized;
    // DRAM floor keeps the meta DRAM front to ~1 MiB so the metadata set spills.
    let config = cache_config(dir.path(), 256 * BLOCK_SIZE_BYTES, 80 * 1024 * 1024);
    let store = Arc::new(InMemory::new());

    // 24 metadata.json objects (256 KiB each = 6 MiB) — far over the meta DRAM
    // front, so foyer evicts the front and spills them to the meta NVMe tier
    // during warming (deterministic eviction; the classifier routes these to the
    // pinned store by the `**/metadata/*.json` heuristic).
    let metas: Vec<(String, Bytes)> = (0..24)
        .map(|i| {
            (
                format!("db/t/metadata/v{i}.metadata.json"),
                pattern(400 + i, 256 * 1024),
            )
        })
        .collect();
    for (name, body) in &metas {
        seed(&store, name, body.clone()).await;
    }

    {
        let (engine, _) = engine_over(store.clone(), &config).await;
        // Two passes: the first pins each object into the meta store, the second
        // confirms it is a meta-store hit (and keeps the front churning so the
        // early objects spill to the meta NVMe tier).
        for _ in 0..2 {
            for (name, body) in &metas {
                let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
                    .await
                    .expect("warm metadata");
                assert_eq!(&got, body);
            }
        }
        engine.flush().await;
    }

    // --- Restart: a fresh engine over the same directory. ---
    let (engine, calls) = engine_over(store.clone(), &config).await;

    for (name, body) in &metas {
        engine.head(&key(name)).await.expect("re-resolve mapping");
        let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("post-restart metadata read");
        assert_eq!(&got, body, "recovered metadata must be byte-identical");
    }

    let counters = engine.counters().snapshot();
    assert!(
        counters.meta_hits > 0,
        "the metadata store must recover pinned metadata from its NVMe tier \
         (got {} meta hits after restart)",
        counters.meta_hits
    );
    // The spilled metadata was served from the recovered meta store, not the
    // backend: only the un-spilled DRAM-front tail (if any) re-fetches.
    let (gets, heads) = calls.snapshot();
    assert_eq!(
        heads, 24,
        "each object re-resolves its mapping with one HEAD"
    );
    assert!(
        gets < 24,
        "most metadata must recover from NVMe rather than refill (got {gets} backend GETs)"
    );
}

/// Metadata that remains in the DRAM front must still be durable on NVMe.
/// A large DRAM budget deliberately prevents eviction before restart; recovery
/// therefore proves write-through persistence rather than an incidental spill.
#[tokio::test(flavor = "multi_thread")]
async fn restart_recovers_metadata_that_never_evicted_from_dram() {
    let dir = TempDir::new().expect("temp dir");
    let config = cache_config(dir.path(), 256 * BLOCK_SIZE_BYTES, 128 * 1024 * 1024);
    let store = Arc::new(InMemory::new());
    let name = "db/t/metadata/v1.metadata.json";
    let body = pattern(901, 256 * 1024);
    seed(&store, name, body.clone()).await;

    {
        let (engine, _) = engine_over(store.clone(), &config).await;
        let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("warm metadata");
        assert_eq!(got, body);
        engine.flush().await;
    }

    let (engine, calls) = engine_over(store, &config).await;
    engine.head(&key(name)).await.expect("re-resolve mapping");
    let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
        .await
        .expect("post-restart metadata read");
    assert_eq!(got, body, "recovered metadata must be byte-identical");
    assert_eq!(
        engine.counters().snapshot().meta_hits,
        1,
        "metadata still hot in DRAM before restart must recover from NVMe"
    );
    assert_eq!(
        calls.snapshot(),
        (0, 1),
        "restart may re-resolve the mapping but must not refill metadata bytes"
    );
}

/// Crash safety (#16): an unclean stop can leave a torn write. Simulated here
/// by corrupting the backing file, then re-opening. foyer's
/// `RecoverMode::Quiet` + per-entry checksums drop the corrupted entries
/// silently (recovery still completes), the untouched earlier regions recover
/// and serve from NVMe, and — the invariant that matters — no read ever returns
/// wrong bytes: a dropped/corrupted block is a clean miss that refills from the
/// origin correctly.
#[tokio::test(flavor = "multi_thread")]
async fn restart_after_torn_tail_write_drops_partial_entries_never_serves_wrong_bytes() {
    let dir = TempDir::new().expect("temp dir");
    let config = cache_config(dir.path(), 256 * BLOCK_SIZE_BYTES, 80 * 1024 * 1024);
    let store = Arc::new(InMemory::new());

    let objects: Vec<(String, Bytes)> = (0..8)
        .map(|i| {
            (
                format!("crash-{i}.parquet"),
                pattern(200 + i, BLOCK_SIZE_BYTES),
            )
        })
        .collect();
    for (name, body) in &objects {
        seed(&store, name, body.clone()).await;
    }

    // Warm every object onto the disk tier, then drop (clean flush).
    {
        let (engine, _) = engine_over(store.clone(), &config).await;
        for (name, body) in &objects {
            let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
                .await
                .expect("warm read");
            assert_eq!(&got, body);
            engine.flush().await;
        }
    }

    // Simulate a torn write: flip bytes in the first Foyer region, where the
    // first block was written. This is a partial corruption, not a wipe;
    // later regions remain available for recovery.
    let backing_file = block_data_file(dir.path());
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&backing_file)
        .expect("open backing file");
    file.seek(SeekFrom::Start(4 * 1024 * 1024))
        .expect("seek to written region");
    let mut bytes = [0_u8; 64 * 1024];
    file.read_exact(&mut bytes).expect("read backing bytes");
    for b in &mut bytes {
        *b ^= 0xff;
    }
    file.seek(SeekFrom::Start(4 * 1024 * 1024))
        .expect("seek to corrupt region");
    file.write_all(&bytes)
        .expect("write corrupted backing bytes");

    // --- Restart over the corrupted directory: recovery must still complete. ---
    let (engine, _) = engine_over(store.clone(), &config).await;

    let mut disk_hits_seen = 0u64;
    for (name, body) in &objects {
        // HEAD re-resolves the DRAM-only mapping; GET then either hits the
        // recovered disk block or (for a corrupted/dropped block) refills.
        engine.head(&key(name)).await.expect("re-resolve mapping");
        let before = engine.counters().snapshot().disk_hits;
        let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("post-crash read");
        // The one invariant that must hold for EVERY object, corrupted or not:
        // never wrong bytes.
        assert_eq!(
            &got, body,
            "post-crash read of {name} must be byte-identical to the origin — \
             a corrupted entry is dropped and refilled, never served"
        );
        disk_hits_seen += engine.counters().snapshot().disk_hits - before;
    }

    // Recovery was not a no-op: entries outside the damaged region came back
    // from NVMe (some blocks served from disk, not all refilled).
    assert!(
        disk_hits_seen > 0,
        "the intact earlier regions must recover and serve from NVMe (got {disk_hits_seen} disk hits)"
    );
}

/// Purge survives a restart (#178): a generation-epoch purge persists the
/// bumped generation to `cache.dir`, so entries physically resident on the disk
/// tier from *before* the purge recover as **unreachable garbage** — a
/// post-restart read is a cold backend fill, never a disk hit of the purged
/// version. This is the recovery-correctness the generation-in-the-codec buys:
/// without persistence a restart would resurrect the purged blocks (a fresh
/// process at generation 0 would match the resident generation-0 keys).
#[tokio::test(flavor = "multi_thread")]
async fn purge_survives_restart_old_generation_recovers_as_unreachable_garbage() {
    let dir = TempDir::new().expect("temp dir");
    let config = cache_config(dir.path(), 256 * BLOCK_SIZE_BYTES, 80 * 1024 * 1024);
    let store = Arc::new(InMemory::new());

    let objects: Vec<(String, Bytes)> = (0..8)
        .map(|i| {
            (
                format!("purged-{i}.parquet"),
                pattern(600 + i, BLOCK_SIZE_BYTES),
            )
        })
        .collect();
    for (name, body) in &objects {
        seed(&store, name, body.clone()).await;
    }

    // Warm every object onto the disk tier, then purge (bumps + persists the
    // generation), then drop the engine (a clean restart on the same dir).
    {
        let (engine, _) = engine_over(store.clone(), &config).await;
        for (name, body) in &objects {
            let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
                .await
                .expect("warm read");
            assert_eq!(&got, body);
            engine.flush().await;
        }
        let report = engine.purge().await;
        assert_eq!(
            report.generation, 1,
            "purge bumped and persisted generation 1"
        );
    }
    // The generation is durable on disk.
    let persisted = std::fs::read(dir.path().join("PURGE_GENERATION")).expect("gen file");
    assert_eq!(
        u64::from_le_bytes(persisted.as_slice().try_into().expect("8 bytes")),
        1,
        "the bumped generation must be persisted for restart recovery"
    );

    // --- Restart over the SAME directory: the pre-purge blocks are recovered by
    // foyer but keyed at generation 0, unreachable at the recovered generation 1.
    let (engine, calls) = engine_over(store.clone(), &config).await;
    for (name, body) in &objects {
        engine.head(&key(name)).await.expect("re-resolve mapping");
        let (_, _, got) = read_all(&engine, &key(name), ReadRange::Full)
            .await
            .expect("post-restart read");
        assert_eq!(&got, body, "still byte-identical (refilled from origin)");
    }

    let counters = engine.counters().snapshot();
    assert_eq!(
        counters.disk_hits, 0,
        "no purged (old-generation) block may be served from the recovered disk tier"
    );
    assert_eq!(
        counters.backend_fills, 8,
        "every read must refill from the backend — the purge held across the restart"
    );
    assert_eq!(
        calls.snapshot(),
        (8, 8),
        "post-restart: eight cold GET fills and eight metadata HEADs"
    );
}

/// Measures and records the recovery time for a representative fill (#16's
/// acceptance criterion: "recovery time is measured and recorded"). Fills the
/// cache to a representative size, drops the engine, and times re-opening it —
/// the interval during which foyer rescans the disk regions and rebuilds the
/// index. The number is printed (run with `--nocapture`) for the PR/issue
/// record; the assertion is a loose upper bound so this stays a real test, not
/// a timing flake.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_time_for_a_representative_fill_is_measured_and_recorded() {
    let dir = TempDir::new().expect("temp dir");
    // A representative fill: 32 blocks (~256 MiB) across the disk tier. Recovery
    // cost is dominated by the region rescan, which is linear in the number of
    // regions, so this extrapolates directly to production sizings.
    let blocks = 32u64;
    let config = cache_config(dir.path(), 128 * BLOCK_SIZE_BYTES, 128 * 1024 * 1024);
    let store = Arc::new(InMemory::new());

    for i in 0..blocks {
        let name = format!("fill-{i}.parquet");
        seed(&store, &name, pattern(300 + i, BLOCK_SIZE_BYTES)).await;
    }

    {
        let (engine, _) = engine_over(store.clone(), &config).await;
        for i in 0..blocks {
            let name = format!("fill-{i}.parquet");
            read_all(&engine, &key(&name), ReadRange::Full)
                .await
                .expect("warm read");
            engine.flush().await;
        }
    }

    // Time only the re-open (recovery): building the engine over a populated
    // directory rescans the disk regions and rebuilds the in-memory index.
    let start = std::time::Instant::now();
    let (engine, _) = engine_over(store.clone(), &config).await;
    let recovery = start.elapsed();

    let filled_mib = blocks * BLOCK_SIZE_BYTES / (1024 * 1024);
    eprintln!(
        "#16 recovery-time: re-opened a {filled_mib} MiB / {blocks}-block directory cache in {recovery:?}"
    );

    // Sanity: recovery actually reloaded the blocks (a re-read serves from disk
    // with zero backend fills after the one metadata round trip).
    engine.head(&key("fill-0.parquet")).await.expect("head");
    let (_, _, _) = read_all(&engine, &key("fill-0.parquet"), ReadRange::Full)
        .await
        .expect("post-recovery read");
    assert_eq!(
        engine.counters().snapshot().backend_fills,
        0,
        "recovered block served without a backend re-fill"
    );

    // Loose upper bound: a few dozen regions must recover in well under the
    // seconds-scale target the issue sets for a full cache.
    assert!(
        recovery < std::time::Duration::from_secs(30),
        "recovery of a {filled_mib} MiB cache took {recovery:?}, far over the seconds-scale target"
    );
}
