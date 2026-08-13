//! Write-through ordering property tests (issue #21): the one correctness rule
//! of the write path is that **after Verglas acks a write, no reader may ever
//! see the old bytes.** These tests pin that invariant across *every* mutating
//! op type — overwrite PUT, DELETE, server-side CopyObject, and
//! CompleteMultipartUpload — under randomized read/write interleavings and
//! under sustained concurrent load.
//!
//! The front-end's ordering (backend durable → invalidate → ack) is only
//! observable through a cache that *would* serve stale bytes if the order were
//! wrong. The real [`verglas_cache::HybridCacheEngine`] cannot be used here (it
//! dev-depends on this crate, so depending back would cycle the graph), so
//! [`ModelCache`] below is a faithful stand-in: a key→object mapping that
//! serves cached (possibly stale) bytes on a hit, fills read-through on a miss,
//! and drops the mapping on `invalidate` — with the same per-key epoch fence
//! the engine uses (#124) so an in-flight miss that overlapped a write cannot
//! reinstate the stale mapping. If a handler acked before invalidating, a
//! post-ack read of this cache would return the pre-write bytes and the
//! assertions here would fail.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::{ObjectStoreExt, PutPayload, path::Path};
use verglas_core::CacheKey;
use verglas_s3::{
    BackendStore, BodyStream, Invalidation, ObjectGet, ObjectMeta, ObjectRead, PassthroughList,
    PassthroughWrite, ReadError, ReadRange,
};

/// The bucket every test in this file writes through.
const BUCKET: &str = "ordering-bucket";

/// Deterministic payload: byte `i` is `(i + salt) % 251` (prime, so no chunk
/// alignment can mask an offset bug). `salt` distinguishes "old" from "new"
/// content for the same key.
fn payload(salt: u64, len: u64) -> Bytes {
    (0..len)
        .map(|i| ((i + salt) % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// One cached object as the model remembers it: the backend ETag plus the
/// full bytes at the version that was resolved. A hit serves these bytes
/// verbatim — stale ones included, which is exactly what a broken ack order
/// would leak.
#[derive(Clone)]
struct CachedObject {
    /// ETag the backend reported for this version.
    e_tag: Option<String>,
    /// Full object bytes at the resolved version.
    bytes: Bytes,
}

impl CachedObject {
    /// Metadata-only view, for HEAD and for the `ObjectGet` header fields.
    fn meta(&self) -> ObjectMeta {
        ObjectMeta {
            size: self.bytes.len() as u64,
            e_tag: self.e_tag.clone(),
            last_modified: None,
            content_type: None,
            ..ObjectMeta::default()
        }
    }

    /// Builds the read result for `range`, slicing the cached bytes to the
    /// resolved byte window and streaming them as a single chunk.
    fn to_get(&self, range: ReadRange) -> ObjectGet {
        let total = self.bytes.len() as u64;
        let resolved = resolve_range(range, total);
        let slice = self
            .bytes
            .slice(resolved.start as usize..resolved.end as usize);
        let body: BodyStream = futures::stream::once(async move { Ok(slice) }).boxed();
        ObjectGet {
            meta: self.meta(),
            range: resolved,
            body,
            served_from: verglas_s3::TierCell::new(),
        }
    }
}

/// Resolves an HTTP-shaped [`ReadRange`] against a known object size into a
/// concrete half-open byte range, clamped to the object.
fn resolve_range(range: ReadRange, total: u64) -> Range<u64> {
    match range {
        ReadRange::Full => 0..total,
        ReadRange::From(first) => first.min(total)..total,
        ReadRange::Bounded(first, last) => first.min(total)..last.saturating_add(1).min(total),
        ReadRange::Suffix(len) => total.saturating_sub(len)..total,
    }
}

/// Shared state behind [`ModelCache`]: the key→object mappings and the per-key
/// invalidation epoch fence, guarded by one mutex so admission and
/// invalidation are atomic with respect to each other (the #124 discipline).
#[derive(Default)]
struct ModelState {
    /// Cached objects by key. A present entry is a cache hit.
    mappings: HashMap<CacheKey, CachedObject>,
    /// Per-key epoch, bumped by every invalidation. A miss captures the epoch
    /// before its read-through and only admits if it is still current.
    epochs: HashMap<CacheKey, u64>,
}

/// A minimal but faithful model of the read cache the front-end sits in front
/// of: reads fill read-through from `store` and cache the mapping; writes never
/// touch it except through [`Invalidation`]. Cloning shares the state, so the
/// same instance can be handed to the front-end as both reader and invalidator.
#[derive(Clone)]
struct ModelCache {
    /// The backend the front-end's writer also writes to; the read-through
    /// source of truth on a miss.
    store: Arc<InMemory>,
    /// Mappings + epoch fence, shared across clones.
    state: Arc<Mutex<ModelState>>,
}

impl ModelCache {
    /// Wraps a backend store in a fresh, empty cache.
    fn new(store: Arc<InMemory>) -> Self {
        ModelCache {
            store,
            state: Arc::new(Mutex::new(ModelState::default())),
        }
    }

    /// Locks the shared state, recovering from poisoning (no code panics under
    /// the lock, so the data is always intact).
    fn state(&self) -> MutexGuard<'_, ModelState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Returns the cached object for `key` if the mapping is present (a hit).
    fn lookup(&self, key: &CacheKey) -> Option<CachedObject> {
        self.state().mappings.get(key).cloned()
    }

    /// Captures the key's current epoch before a read-through begins.
    fn epoch(&self, key: &CacheKey) -> u64 {
        self.state().epochs.get(key).copied().unwrap_or(0)
    }

    /// Admits a freshly resolved object into the mapping, but only if no
    /// invalidation bumped the epoch since [`Self::epoch`] was captured —
    /// otherwise the resolution overlapped a write and must not reinstate a
    /// mapping the write just dropped (#124).
    fn admit(&self, key: &CacheKey, captured: u64, object: CachedObject) {
        let mut state = self.state();
        if state.epochs.get(key).copied().unwrap_or(0) == captured {
            state.mappings.insert(key.clone(), object);
        }
    }

    /// Reads the full object through to the backend, returning its bytes and
    /// ETag. A missing key surfaces as [`ReadError::NoSuchKey`], exactly as a
    /// deleted object would.
    async fn read_through(&self, key: &CacheKey) -> Result<CachedObject, ReadError> {
        let result = self
            .store
            .get(&Path::from(key.key.as_str()))
            .await
            .map_err(|error| match error {
                object_store::Error::NotFound { .. } => ReadError::NoSuchKey,
                other => ReadError::Backend(other.to_string()),
            })?;
        let e_tag = result.meta.e_tag.clone();
        let bytes = result
            .bytes()
            .await
            .map_err(|e| ReadError::Backend(e.to_string()))?;
        Ok(CachedObject { e_tag, bytes })
    }
}

impl ObjectRead for ModelCache {
    /// Serves from the mapping on a hit; on a miss, resolves read-through and
    /// admits under the epoch fence. The two `yield_now` points widen the
    /// resolve window so a concurrent write's invalidation reliably interleaves
    /// with an in-flight miss under the multi-thread runtime.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        if let Some(object) = self.lookup(key) {
            return Ok(object.to_get(range));
        }
        let captured = self.epoch(key);
        tokio::task::yield_now().await;
        let object = self.read_through(key).await?;
        tokio::task::yield_now().await;
        self.admit(key, captured, object.clone());
        Ok(object.to_get(range))
    }

    /// Metadata-only counterpart to [`Self::get`], with the same hit/miss and
    /// fence discipline.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        if let Some(object) = self.lookup(key) {
            return Ok(object.meta());
        }
        let captured = self.epoch(key);
        tokio::task::yield_now().await;
        let object = self.read_through(key).await?;
        self.admit(key, captured, object.clone());
        Ok(object.meta())
    }
}

#[async_trait::async_trait]
impl Invalidation for ModelCache {
    /// Bumps each key's epoch and drops its mapping under one lock, so a
    /// concurrent in-flight miss cannot re-admit the stale version. Returning
    /// `Ok` means every listed key is guaranteed to miss (and thus re-resolve
    /// against the backend) on its next read.
    async fn invalidate(&self, keys: &[CacheKey]) -> Result<(), String> {
        let mut state = self.state();
        for key in keys {
            *state.epochs.entry(key.clone()).or_insert(0) += 1;
            state.mappings.remove(key);
        }
        Ok(())
    }
}

/// Serves an axum router on an ephemeral loopback port, returning its base URL.
async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

/// Boots the front-end with the [`ModelCache`] as both reader and invalidator
/// and a real passthrough writer over the same in-memory backend, so a write
/// genuinely lands in the store the cache reads through. Returns the base URL
/// and a handle to the store for direct out-of-band seeding.
async fn serve_model() -> (String, Arc<InMemory>, ModelCache) {
    let store = Arc::new(InMemory::new());
    let model = ModelCache::new(store.clone());
    let base = serve(verglas_s3::router(
        "default",
        model.clone(),
        PassthroughWrite::new(BackendStore::single("default", BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default",
            BUCKET,
            store.clone(),
        ))),
        Arc::new(model.clone()),
        None,
    ))
    .await;
    (base, store, model)
}

/// The mutating op under test. Every variant makes the same key hold `new`
/// bytes (or removes it), and must invalidate the key before acking.
#[derive(Clone, Copy, Debug)]
enum WriteOp {
    /// Overwrite the key with a single atomic PUT.
    Put,
    /// Delete the key.
    Delete,
    /// Server-side copy `new` content from a sibling source onto the key.
    Copy,
    /// Replace the key via the multipart lifecycle (create/upload/complete).
    Multipart,
}

impl WriteOp {
    /// Picks one of the four op types from a seed (`% 4`).
    fn from_seed(seed: u64) -> Self {
        match seed % 4 {
            0 => WriteOp::Put,
            1 => WriteOp::Delete,
            2 => WriteOp::Copy,
            _ => WriteOp::Multipart,
        }
    }
}

/// A SplitMix64 PRNG: tiny, dependency-free, and fully deterministic from its
/// seed, so a failing randomized scenario reproduces exactly.
struct Rng(u64);

impl Rng {
    /// Seeds the generator.
    fn new(seed: u64) -> Self {
        Rng(seed)
    }

    /// Next 64-bit value (SplitMix64 finalizer).
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform integer in `lo..=hi`.
    fn in_range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next_u64() % (hi - lo + 1)
    }
}

/// PUTs `bytes` to `key` over HTTP, asserting a 200 ack.
async fn put(client: &reqwest::Client, base: &str, key: &str, bytes: Bytes) {
    let response = client
        .put(format!("{base}/{BUCKET}/{key}"))
        .body(bytes)
        .send()
        .await
        .expect("PUT");
    assert_eq!(response.status(), 200, "PUT {key} must ack");
}

/// Performs `op` so that `key` ends up holding `new` (or is deleted),
/// returning only once the front-end has acked — the moment after which no
/// read may see pre-write bytes. `src_key` is a scratch sibling used by Copy.
async fn apply_write(
    client: &reqwest::Client,
    base: &str,
    op: WriteOp,
    key: &str,
    src_key: &str,
    new: Bytes,
) {
    match op {
        WriteOp::Put => put(client, base, key, new).await,
        WriteOp::Delete => {
            let response = client
                .delete(format!("{base}/{BUCKET}/{key}"))
                .send()
                .await
                .expect("DELETE");
            assert_eq!(response.status(), 204, "DELETE {key} must ack");
        }
        WriteOp::Copy => {
            // Stage the new content at a sibling, then server-side copy it on.
            put(client, base, src_key, new).await;
            let response = client
                .put(format!("{base}/{BUCKET}/{key}"))
                .header("x-amz-copy-source", format!("{BUCKET}/{src_key}"))
                .send()
                .await
                .expect("COPY");
            assert_eq!(response.status(), 200, "COPY onto {key} must ack");
        }
        WriteOp::Multipart => {
            let url = format!("{base}/{BUCKET}/{key}");
            let response = client
                .post(format!("{url}?uploads"))
                .send()
                .await
                .expect("CreateMultipartUpload");
            assert_eq!(response.status(), 200);
            let body = response.text().await.expect("body");
            let upload_id = xml_tag(&body, "UploadId");
            let response = client
                .put(format!("{url}?partNumber=1&uploadId={upload_id}"))
                .body(new)
                .send()
                .await
                .expect("UploadPart");
            assert_eq!(response.status(), 200);
            let e_tag = response
                .headers()
                .get("etag")
                .expect("part ETag")
                .to_str()
                .expect("ascii")
                .to_owned();
            let manifest = format!(
                "<CompleteMultipartUpload>\
                   <Part><PartNumber>1</PartNumber><ETag>{e_tag}</ETag></Part>\
                 </CompleteMultipartUpload>"
            );
            let response = client
                .post(format!("{url}?uploadId={upload_id}"))
                .header("content-type", "application/xml")
                .body(manifest)
                .send()
                .await
                .expect("CompleteMultipartUpload");
            assert_eq!(response.status(), 200, "Complete on {key} must ack");
        }
    }
}

/// Extracts the text of the first `<tag>...</tag>` in an XML body.
fn xml_tag(body: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body
        .find(&open)
        .unwrap_or_else(|| panic!("no <{tag}> in: {body}"))
        + open.len();
    let end = start + body[start..].find(&close).expect("closing tag");
    body[start..end].to_owned()
}

/// One observed read: its status and (for a 200) its body bytes.
struct Observation {
    /// HTTP status of the GET.
    status: u16,
    /// Body bytes when the GET returned 200.
    body: Option<Bytes>,
}

/// GETs `key` once over HTTP, capturing the status and (on 200) the body.
async fn observe(client: &reqwest::Client, base: &str, key: &str) -> Observation {
    let response = client
        .get(format!("{base}/{BUCKET}/{key}"))
        .send()
        .await
        .expect("GET");
    let status = response.status().as_u16();
    let body = if status == 200 {
        Some(response.bytes().await.expect("body"))
    } else {
        None
    };
    Observation { status, body }
}

/// Successful object-producing writes allocate the committed version into the
/// read cache before the client receives its acknowledgement. This keeps the
/// common write-then-query path local for atomic PUTs, copies, and completed
/// multipart uploads instead of forcing the first query to refill from origin.
#[tokio::test]
async fn successful_writes_leave_the_new_version_resident() {
    let (base, _store, cache) = serve_model().await;
    let client = reqwest::Client::new();

    for (index, op) in [WriteOp::Put, WriteOp::Copy, WriteOp::Multipart]
        .into_iter()
        .enumerate()
    {
        let key = format!("resident/{index}.bin");
        let source = format!("resident/{index}.src");
        let expected = payload(70 + index as u64, 32_000 + index as u64);
        apply_write(&client, &base, op, &key, &source, expected.clone()).await;

        let cache_key = CacheKey {
            storage_binding_id: "default".to_owned(),
            bucket: BUCKET.to_owned(),
            key: key.clone(),
        };
        let resident = cache
            .lookup(&cache_key)
            .unwrap_or_else(|| panic!("{op:?} acknowledged without cache residency"));
        assert_eq!(resident.bytes, expected, "{op:?} cached different bytes");
    }
}

/// The property (#24), covering every op type: for a key already warm in the
/// cache with `old` bytes, a durable write that replaces it must be invalidated
/// before it is acked, so the **first read issued after the ack never returns
/// the old bytes**. Concurrent readers running across the write may see old or
/// new, but never anything else — and never old after the ack.
///
/// Randomized over 64 seeded scenarios: op type, payload sizes, reader fan-out,
/// and reads per reader all vary, and the model cache yields inside its miss
/// path so reads genuinely interleave with the write's invalidation. A handler
/// that acked before invalidating would leave the stale mapping in place and
/// the post-ack read would return `old`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_post_ack_read_returns_pre_write_bytes_across_op_types() {
    let (base, _store, _cache) = serve_model().await;
    let client = reqwest::Client::new();

    for scenario in 0..64u64 {
        let mut rng = Rng::new(0x5121_0000 ^ scenario);
        let op = WriteOp::from_seed(rng.next_u64());
        let key = format!("k/{scenario}.bin");
        let src_key = format!("k/{scenario}.src");
        let old_len = rng.in_range(1, 40_000);
        let new_len = rng.in_range(1, 40_000);
        let old = payload(7, old_len);
        let new = payload(200, new_len);

        // Warm the cache with the old version: write it, then read it so the
        // mapping caches `old` (making a stale post-ack read *possible* if the
        // ordering were wrong).
        put(&client, &base, &key, old.clone()).await;
        let warm = observe(&client, &base, &key).await;
        assert_eq!(warm.status, 200);
        assert_eq!(warm.body.as_ref(), Some(&old), "warm read should see old");

        // Fan out concurrent readers that race the write.
        let readers = rng.in_range(2, 8);
        let reads_each = rng.in_range(4, 20);
        let mut tasks = Vec::new();
        for _ in 0..readers {
            let client = client.clone();
            let base = base.clone();
            let key = key.clone();
            tasks.push(tokio::spawn(async move {
                let mut seen = Vec::new();
                for _ in 0..reads_each {
                    seen.push(observe(&client, &base, &key).await);
                    tokio::task::yield_now().await;
                }
                seen
            }));
        }

        // The write, concurrent with the readers.
        apply_write(&client, &base, op, &key, &src_key, new.clone()).await;

        // THE INVARIANT: the first read strictly after the ack.
        let post_ack = observe(&client, &base, &key).await;
        match op {
            WriteOp::Delete => assert_eq!(
                post_ack.status, 404,
                "scenario {scenario} ({op:?}): post-ack read must not find the deleted key"
            ),
            _ => {
                assert_eq!(post_ack.status, 200, "scenario {scenario} ({op:?})");
                assert_eq!(
                    post_ack.body.as_ref(),
                    Some(&new),
                    "scenario {scenario} ({op:?}): post-ack read returned stale bytes"
                );
            }
        }

        // Every concurrent observation must have been consistent: old or the
        // post-write outcome, never a torn or third value.
        for task in tasks {
            for obs in task.await.expect("reader task") {
                let ok = match op {
                    WriteOp::Delete => {
                        obs.status == 404 || (obs.status == 200 && obs.body.as_ref() == Some(&old))
                    }
                    _ => {
                        obs.status == 200
                            && (obs.body.as_ref() == Some(&old) || obs.body.as_ref() == Some(&new))
                    }
                };
                assert!(
                    ok,
                    "scenario {scenario} ({op:?}): concurrent read saw an inconsistent value \
                     (status {})",
                    obs.status
                );
            }
        }
    }
}

/// Concurrent read/write under sustained load on a single key: many writer
/// rounds, each racing a burst of readers, all on the same key. After every
/// round's ack the canonical read must reflect that round's write — the load
/// version of the invariant, exercising the mapping/fence under contention
/// rather than one write at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_load_never_exposes_stale_post_ack_reads() {
    let (base, _store, _cache) = serve_model().await;
    let client = reqwest::Client::new();
    let key = "hot/contended.bin";

    // Seed and warm.
    put(&client, &base, key, payload(0, 8_000)).await;
    let warm = observe(&client, &base, key).await;
    assert_eq!(warm.status, 200);

    const ROUNDS: u64 = 40;
    for round in 1..=ROUNDS {
        let new = payload(round, 8_000 + round * 37);

        // A burst of background readers hammering the key during the write.
        let mut readers = Vec::new();
        for _ in 0..6 {
            let client = client.clone();
            let base = base.clone();
            let key = key.to_owned();
            readers.push(tokio::spawn(async move {
                for _ in 0..8 {
                    let obs = observe(&client, &base, &key).await;
                    // A hit is always a whole object (never torn); staleness is
                    // caught by the post-ack read below, not here.
                    assert!(obs.status == 200, "reader saw status {}", obs.status);
                    tokio::task::yield_now().await;
                }
            }));
        }

        put(&client, &base, key, new.clone()).await;

        // Post-ack read must be this round's bytes.
        let post_ack = observe(&client, &base, key).await;
        assert_eq!(post_ack.status, 200, "round {round}");
        assert_eq!(
            post_ack.body.as_ref(),
            Some(&new),
            "round {round}: post-ack read returned stale bytes under load"
        );

        for reader in readers {
            reader.await.expect("reader task");
        }
    }
}

/// Directly seeds `bytes` into the backend store, bypassing the front-end.
/// Used where a test needs an object present without the front-end's write
/// (and its invalidation) running.
async fn seed_store(store: &InMemory, key: &str, bytes: Bytes) {
    store
        .put(&Path::from(key), PutPayload::from(bytes))
        .await
        .expect("seed store");
}

/// A cross-check that the model actually *can* serve stale bytes when
/// invalidation is skipped — proving the property tests above have teeth. It
/// drives the [`ModelCache`] directly (no front-end): warm `old`, overwrite the
/// backend to `new` *without* invalidating, and confirm the cache still serves
/// `old`; then invalidate and confirm it flips to `new`.
#[tokio::test]
async fn model_cache_serves_stale_until_invalidated() {
    let store = Arc::new(InMemory::new());
    let cache = ModelCache::new(store.clone());
    let key = CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: BUCKET.to_owned(),
        key: "proof.bin".to_owned(),
    };
    let old = payload(1, 5_000);
    let new = payload(2, 5_000);

    seed_store(&store, &key.key, old.clone()).await;
    let got = read_full(&cache, &key).await.expect("warm read");
    assert_eq!(got, old, "cold read fills old");

    // Backend now holds new bytes, but no invalidation ran.
    seed_store(&store, &key.key, new.clone()).await;
    let got = read_full(&cache, &key).await.expect("stale read");
    assert_eq!(
        got, old,
        "without invalidation the cache serves stale bytes"
    );

    cache
        .invalidate(std::slice::from_ref(&key))
        .await
        .expect("invalidate");
    let got = read_full(&cache, &key).await.expect("fresh read");
    assert_eq!(got, new, "after invalidation the cache re-resolves to new");
}

/// Reads the whole object from an [`ObjectRead`], concatenating the body
/// stream into a single buffer.
async fn read_full(reader: &impl ObjectRead, key: &CacheKey) -> Result<Bytes, ReadError> {
    let got = reader.get(key, ReadRange::Full).await?;
    let mut buf = Vec::new();
    let mut body = got.body;
    while let Some(chunk) = body.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(Bytes::from(buf))
}
