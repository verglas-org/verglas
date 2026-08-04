//! The cache-managed shadow store for Verglas-derived Puffin artifacts (#95):
//! put/get/list/latest round-trip, the budget as a hard ceiling (evict the
//! least-recently-written, refuse an over-budget single blob), durability across
//! a reopen, and the standing invariant — the store never writes anywhere but
//! its own directory under the cache dir, so no customer table or bucket is ever
//! touched.

use verglas_cache::shadow::{ArtifactKey, ArtifactKind, ShadowError, ShadowStore};

fn key(target: &str, field: &str) -> ArtifactKey {
    ArtifactKey::new(ArtifactKind::VectorIndex, format!("tbl:{target}"), field)
}

/// A generous budget so nothing evicts.
const ROOMY: u64 = 1 << 30;

#[tokio::test]
async fn put_get_list_latest_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ShadowStore::open(dir.path(), ROOMY).expect("open");
    let k = key("default.docs", "embedding");

    // Empty store: nothing under the key.
    assert!(store.latest(&k).await.expect("latest").is_none());
    assert!(store.get(&k, 111).await.expect("get").is_none());
    assert!(store.snapshots(&k).await.expect("snapshots").is_empty());

    // Snapshot ids are deliberately non-monotonic (random i64) — latest tracks
    // write order, not id order.
    let put_a = store
        .put(&k, 900, b"index-A".to_vec())
        .await
        .expect("put a");
    assert_eq!(put_a.snapshot, 900);
    let put_b = store
        .put(&k, 100, b"index-B".to_vec())
        .await
        .expect("put b");
    assert_eq!(put_b.snapshot, 100);

    // get by exact snapshot round-trips the bytes.
    let got_a = store.get(&k, 900).await.expect("get a").expect("present a");
    assert_eq!(got_a.bytes, b"index-A");
    let got_b = store.get(&k, 100).await.expect("get b").expect("present b");
    assert_eq!(got_b.bytes, b"index-B");

    // latest is the most recently WRITTEN (snapshot 100), not the highest id.
    let latest = store.latest(&k).await.expect("latest").expect("present");
    assert_eq!(latest.snapshot, 100);
    assert_eq!(latest.bytes, b"index-B");

    // snapshots lists both, ascending.
    assert_eq!(store.snapshots(&k).await.expect("snaps"), vec![100, 900]);

    // A second write to the same snapshot overwrites it (latest bytes win) and
    // does not double-count the snapshot.
    store
        .put(&k, 900, b"index-A2".to_vec())
        .await
        .expect("put a2");
    let got_a2 = store.get(&k, 900).await.expect("get a2").expect("present");
    assert_eq!(got_a2.bytes, b"index-A2");
    assert_eq!(store.snapshots(&k).await.expect("snaps"), vec![100, 900]);
    // The rewrite is now the latest.
    assert_eq!(
        store.latest(&k).await.expect("latest").expect("p").snapshot,
        900
    );
}

#[tokio::test]
async fn budget_is_a_hard_ceiling_and_evicts_the_oldest() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Budget for exactly two ~100-byte blobs.
    let store = ShadowStore::open(dir.path(), 250).expect("open");
    let k = key("default.docs", "embedding");
    let blob = |b: u8| vec![b; 100];

    store.put(&k, 1, blob(1)).await.expect("put 1");
    store.put(&k, 2, blob(2)).await.expect("put 2");
    assert!(store.used_bytes().expect("used") <= 250, "within ceiling");
    assert_eq!(store.snapshots(&k).await.expect("snaps"), vec![1, 2]);

    // A third blob pushes over the ceiling: the least-recently-written (snapshot
    // 1) is evicted, never breaching the budget.
    store.put(&k, 3, blob(3)).await.expect("put 3");
    assert!(
        store.used_bytes().expect("used") <= 250,
        "ceiling held under pressure"
    );
    assert_eq!(
        store.snapshots(&k).await.expect("snaps"),
        vec![2, 3],
        "oldest write evicted"
    );
    assert!(
        store.get(&k, 1).await.expect("get 1").is_none(),
        "evicted blob is gone"
    );
    // The evicted blob's file is deleted from disk, not merely dropped from the
    // index — the ceiling is real bytes, not bookkeeping.
    let on_disk = walk_files(dir.path());
    assert_eq!(on_disk.len(), 2, "only the two surviving blobs remain");
}

#[tokio::test]
async fn single_blob_over_budget_is_refused_not_admitted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ShadowStore::open(dir.path(), 100).expect("open");
    let k = key("default.docs", "embedding");

    let err = store
        .put(&k, 1, vec![0u8; 200])
        .await
        .expect_err("over-budget blob must be refused");
    assert!(matches!(err, ShadowError::TooLarge { .. }));
    // Nothing admitted, nothing on disk — never unbounded.
    assert_eq!(store.used_bytes().expect("used"), 0);
    assert!(walk_files(dir.path()).is_empty());
}

#[tokio::test]
async fn durable_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let k = key("default.docs", "embedding");
    {
        let store = ShadowStore::open(dir.path(), ROOMY).expect("open 1");
        store.put(&k, 900, b"first".to_vec()).await.expect("put 1");
        store.put(&k, 100, b"second".to_vec()).await.expect("put 2");
    }
    // Reopen the same directory: the index, byte total, and next sequence are
    // rebuilt from disk.
    let store = ShadowStore::open(dir.path(), ROOMY).expect("reopen");
    assert_eq!(store.snapshots(&k).await.expect("snaps"), vec![100, 900]);
    let latest = store.latest(&k).await.expect("latest").expect("present");
    assert_eq!(latest.snapshot, 100, "write recency survives reopen");
    assert_eq!(latest.bytes, b"second");
    assert_eq!(
        store.used_bytes().expect("used"),
        (b"first".len() + b"second".len()) as u64
    );

    // A new write after reopen keeps the sequence monotonic (it is the latest).
    store.put(&k, 500, b"third".to_vec()).await.expect("put 3");
    assert_eq!(
        store.latest(&k).await.expect("latest").expect("p").snapshot,
        500
    );
}

#[tokio::test]
async fn never_writes_outside_its_own_root() {
    // The store's ONLY sink is its root under the cache dir. Model a customer
    // bucket as a sibling directory and prove that no put touches it: the store
    // has no bucket/catalog/origin handle to reach one with, and every byte it
    // writes lands under its own root.
    let base = tempfile::tempdir().expect("tempdir");
    let cache_dir = base.path().join("cache");
    let customer_bucket = base.path().join("customer-bucket");
    std::fs::create_dir_all(&cache_dir).expect("cache dir");
    std::fs::create_dir_all(&customer_bucket).expect("customer dir");

    let store = ShadowStore::under_cache_dir(&cache_dir, ROOMY).expect("open");
    let k = key("default.docs", "embedding");
    let blob = store.put(&k, 1, b"derived".to_vec()).await.expect("put");

    // The blob lives under the cache dir's shadow root, nowhere near the bucket.
    let shadow_root = cache_dir.join("shadow");
    assert!(
        std::path::Path::new(&blob.location).starts_with(&shadow_root),
        "blob {} escaped the shadow root {}",
        blob.location,
        shadow_root.display()
    );
    // The customer bucket is untouched: zero files written to it.
    assert!(
        walk_files(&customer_bucket).is_empty(),
        "the shadow store wrote into the customer bucket"
    );
    // Every file the store created is under its shadow root.
    for f in walk_files(cache_dir.as_path()) {
        assert!(
            f.starts_with(&shadow_root),
            "store wrote {} outside its shadow root",
            f.display()
        );
    }
}

/// Every regular file under `dir`, recursively.
fn walk_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk_files(&p));
        } else {
            out.push(p);
        }
    }
    out
}
