//! Hermetic tests for the Vamana engine: recall vs brute force, incremental
//! insert/delete/consolidate, and blob round-trips.

use verglas_vector::codec;
use verglas_vector::store::{InMemoryShadowStore, IndexKey, LocalDirShadowStore, ShadowBlobStore};
use verglas_vector::vamana::{VamanaIndex, VamanaParams};
use verglas_vector::{Metric, brute_force_search, puffin};

/// Deterministic pseudo-random vectors (SplitMix64), so tests are reproducible.
fn synth(n: usize, dim: usize, seed: u64) -> Vec<(i64, Vec<f32>)> {
    let mut state = seed | 1;
    let mut next = || {
        state = state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^= z >> 31;
        (z as f64 / u64::MAX as f64) as f32
    };
    (0..n)
        .map(|i| {
            let v: Vec<f32> = (0..dim).map(|_| next() * 2.0 - 1.0).collect();
            (i as i64, v)
        })
        .collect()
}

fn recall_at_k(
    index: &VamanaIndex,
    data: &[(i64, Vec<f32>)],
    queries: &[Vec<f32>],
    k: usize,
    l: usize,
) -> f32 {
    let metric = index.metric();
    let dim = index.dim();
    let mut hit = 0usize;
    let mut total = 0usize;
    for q in queries {
        let approx: Vec<i64> = index
            .search(q, k, l)
            .expect("vector test op")
            .into_iter()
            .map(|n| n.id)
            .collect();
        let truth: Vec<i64> = brute_force_search(metric, dim, data, q, k)
            .expect("vector test op")
            .into_iter()
            .map(|n| n.id)
            .collect();
        let tset: std::collections::HashSet<i64> = truth.iter().copied().collect();
        hit += approx.iter().filter(|id| tset.contains(id)).count();
        total += truth.len();
    }
    hit as f32 / total as f32
}

#[test]
fn recall_at_10_beats_threshold_l2() {
    let dim = 32;
    let data = synth(2000, dim, 42);
    let queries: Vec<Vec<f32>> = synth(100, dim, 7).into_iter().map(|(_, v)| v).collect();

    let mut index =
        VamanaIndex::new(Metric::L2, dim, VamanaParams::default(), 1).expect("vector test op");
    index.build(&data).expect("vector test op");

    let recall = recall_at_k(&index, &data, &queries, 10, 100);
    assert!(recall >= 0.9, "recall@10 (L2) = {recall}, want >= 0.9");
}

#[test]
fn recall_at_10_beats_threshold_cosine() {
    let dim = 48;
    let data = synth(1500, dim, 99);
    let queries: Vec<Vec<f32>> = synth(80, dim, 5).into_iter().map(|(_, v)| v).collect();

    let mut index =
        VamanaIndex::new(Metric::Cosine, dim, VamanaParams::default(), 3).expect("vector test op");
    index.build(&data).expect("vector test op");

    let recall = recall_at_k(&index, &data, &queries, 10, 100);
    assert!(recall >= 0.9, "recall@10 (cosine) = {recall}, want >= 0.9");
}

#[test]
fn incremental_inserts_are_findable_and_recall_stays_high() {
    let dim = 32;
    let all = synth(2000, dim, 11);
    let (initial, added) = all.split_at(1500);

    let mut index =
        VamanaIndex::new(Metric::L2, dim, VamanaParams::default(), 2).expect("vector test op");
    index.build(initial).expect("vector test op");
    for (id, v) in added {
        index.insert(*id, v).expect("vector test op");
    }
    assert_eq!(index.len(), 2000);

    // Every inserted vector is its own nearest neighbor (findable).
    for (id, v) in added {
        let got = index.search(v, 1, 64).expect("vector test op");
        assert_eq!(got[0].id, *id, "inserted id {id} not findable");
    }

    let queries: Vec<Vec<f32>> = synth(100, dim, 8).into_iter().map(|(_, v)| v).collect();
    let recall = recall_at_k(&index, &all, &queries, 10, 100);
    assert!(
        recall >= 0.9,
        "post-insert recall@10 = {recall}, want >= 0.9"
    );
}

#[test]
fn deletes_then_consolidate_never_return_deleted_ids() {
    let dim = 24;
    let data = synth(1000, dim, 21);
    let mut index =
        VamanaIndex::new(Metric::L2, dim, VamanaParams::default(), 4).expect("vector test op");
    index.build(&data).expect("vector test op");

    // Delete every 3rd id.
    let deleted: Vec<i64> = (0..1000).filter(|i| i % 3 == 0).collect();
    for &id in &deleted {
        assert!(index.delete(id));
    }
    let dset: std::collections::HashSet<i64> = deleted.iter().copied().collect();

    // Before consolidation: deleted ids are filtered from every query.
    let queries: Vec<Vec<f32>> = synth(60, dim, 6).into_iter().map(|(_, v)| v).collect();
    for q in &queries {
        for n in index.search(q, 20, 100).expect("vector test op") {
            assert!(
                !dset.contains(&n.id),
                "deleted id {} returned pre-consolidate",
                n.id
            );
        }
    }

    let tombstones_before = index.tombstone_count();
    assert!(tombstones_before > 0);
    index.consolidate();
    assert_eq!(
        index.tombstone_count(),
        0,
        "consolidation clears tombstones"
    );
    assert_eq!(index.len(), 1000 - deleted.len());

    // After consolidation: still no deleted id, and recall over the live set
    // stays high.
    let live: Vec<(i64, Vec<f32>)> = data
        .iter()
        .filter(|(id, _)| !dset.contains(id))
        .cloned()
        .collect();
    for q in &queries {
        for n in index.search(q, 20, 100).expect("vector test op") {
            assert!(
                !dset.contains(&n.id),
                "deleted id {} returned post-consolidate",
                n.id
            );
            assert!(index.contains(n.id));
        }
    }
    let recall = recall_at_k(&index, &live, &queries, 10, 100);
    assert!(
        recall >= 0.9,
        "post-consolidate recall@10 = {recall}, want >= 0.9"
    );
}

#[test]
fn update_replaces_vector() {
    let dim = 16;
    let data = synth(500, dim, 33);
    let mut index =
        VamanaIndex::new(Metric::L2, dim, VamanaParams::default(), 5).expect("vector test op");
    index.build(&data).expect("vector test op");

    // Move id 0 to a fresh region and re-insert.
    let far = vec![9.0f32; dim];
    index.insert(0, &far).expect("vector test op");
    assert_eq!(index.len(), 500, "update keeps the live count");

    // id 0 now resolves at its new location...
    let got = index.search(&far, 1, 64).expect("vector test op");
    assert_eq!(got[0].id, 0);

    // ...and its old vector's exact match is no longer id 0 (the old slot is
    // tombstoned), though searching there still returns other live neighbors.
    let old = &data[0].1;
    let near_old = index.search(old, 1, 64).expect("vector test op");
    assert_ne!(near_old[0].id, 0, "id 0 should not sit at its old vector");

    // After consolidation the stale slot is physically gone.
    index.consolidate();
    assert_eq!(index.len(), 500);
    assert_eq!(index.tombstone_count(), 0);
    assert_eq!(index.search(&far, 1, 64).expect("vector test op")[0].id, 0);
}

#[test]
fn codec_round_trips_with_tombstones() {
    let dim = 20;
    let data = synth(300, dim, 77);
    let mut index =
        VamanaIndex::new(Metric::Cosine, dim, VamanaParams::default(), 6).expect("vector test op");
    index.build(&data).expect("vector test op");
    index.set_reflected_snapshot(123456789);
    for id in [1i64, 5, 9, 42] {
        index.delete(id);
    }

    let bytes = codec::encode(&index);
    let decoded = codec::decode(&bytes).expect("vector test op");
    assert_eq!(decoded.dim(), dim);
    assert_eq!(decoded.metric(), Metric::Cosine);
    assert_eq!(decoded.reflected_snapshot(), 123456789);
    assert_eq!(decoded.tombstone_count(), 4);
    assert!(!decoded.contains(5));
    assert!(decoded.contains(2));

    // Bad magic / truncation are errors, not panics.
    assert!(codec::decode(&bytes[..bytes.len() - 3]).is_err());
    assert!(codec::decode(b"not a vamana blob").is_err());
}

#[tokio::test]
async fn puffin_round_trip_through_shadow_store() {
    let dim = 20;
    let data = synth(400, dim, 88);
    let mut index =
        VamanaIndex::new(Metric::L2, dim, VamanaParams::default(), 7).expect("vector test op");
    index.build(&data).expect("vector test op");
    index.set_reflected_snapshot(555);

    let bytes = puffin::to_puffin_bytes(&index, Default::default())
        .await
        .expect("vector test op");

    // In-memory store: put, latest, snapshots.
    let store = InMemoryShadowStore::new();
    let key = IndexKey::table("default.docs", "embedding");
    store
        .put(&key, 555, bytes.clone())
        .await
        .expect("vector test op");
    let latest = store
        .latest(&key)
        .await
        .expect("vector test op")
        .expect("vector test op");
    assert_eq!(latest.snapshot, 555);
    let restored = puffin::from_puffin_bytes(&latest.bytes)
        .await
        .expect("vector test op");
    assert_eq!(restored.len(), 400);
    assert_eq!(restored.reflected_snapshot(), 555);

    // Local-dir store round-trip + version listing.
    let dir = tempfile::tempdir().expect("vector test op");
    let disk = LocalDirShadowStore::at(dir.path());
    disk.put(&key, 555, bytes.clone())
        .await
        .expect("vector test op");
    disk.put(&key, 600, bytes.clone())
        .await
        .expect("vector test op");
    assert_eq!(
        disk.snapshots(&key).await.expect("vector test op"),
        vec![555, 600]
    );
    assert_eq!(
        disk.latest(&key)
            .await
            .expect("vector test op")
            .expect("vector test op")
            .snapshot,
        600
    );
    assert!(disk.get(&key, 999).await.expect("vector test op").is_none());
}
