//! Integration tests for the rendezvous ring, mapped to issue #17's
//! acceptance criteria: lookups route by ownership, distribution across mock
//! 3/5/10-node rings is uniform within 5%, and removing a member moves only
//! ~1/N of the keyspace.
//!
//! The keyspace is generated deterministically (no RNG), so every run — local
//! or CI — exercises the exact same keys and the assertions are stable.

use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::ring::{RendezvousRing, Ring, RingError};

/// Number of deterministic keys sampled for the statistical assertions.
/// Large enough that a uniform hash lands well inside the 5% tolerance.
const KEYSPACE: usize = 100_000;

/// Builds `n` mock members named `node-0..node-n`.
fn members(n: usize) -> Vec<NodeId> {
    (0..n).map(|i| NodeId::new(format!("node-{i}"))).collect()
}

/// Generates the fixed, deterministic keyspace shared by all tests.
fn keyspace() -> impl Iterator<Item = CacheKey> {
    (0..KEYSPACE).map(|i| CacheKey {
        bucket: format!("lake-{}", i % 7),
        key: format!("warehouse/db/table/data/part-{i:06}.parquet"),
    })
}

#[test]
fn empty_membership_is_rejected() {
    assert_eq!(
        RendezvousRing::new(Vec::new()).err(),
        Some(RingError::EmptyMembership)
    );
}

#[test]
fn cluster_of_one_owns_everything() {
    let me = NodeId::new("node-solo");
    let ring = RendezvousRing::single(me.clone());
    assert_eq!(ring.members(), vec![me.clone()]);
    // A fixed ring never changes, so its epoch is the constant default.
    assert_eq!(ring.epoch(), 0);
    for key in keyspace().take(1_000) {
        assert_eq!(ring.owner(&key), me);
    }
}

#[test]
fn owner_is_always_a_member_and_deterministic() {
    for n in [3, 5, 10] {
        let ring = RendezvousRing::new(members(n)).expect("non-empty membership");
        for key in keyspace().take(1_000) {
            let owner = ring.owner(&key);
            assert!(ring.members().contains(&owner));
            assert_eq!(ring.owner(&key), owner, "ownership must be stable");
        }
    }
}

/// Rendezvous scores depend only on (key, node), so two nodes holding the
/// same member set in different orders must agree on every owner — this is
/// what makes ownership a pod-wide fact rather than a local one.
#[test]
fn ownership_is_independent_of_member_order() {
    let forward = RendezvousRing::new(members(5)).expect("non-empty membership");
    let mut reversed_members = members(5);
    reversed_members.reverse();
    let reversed = RendezvousRing::new(reversed_members).expect("non-empty membership");

    for key in keyspace().take(5_000) {
        assert_eq!(forward.owner(&key), reversed.owner(&key));
    }
}

/// Acceptance criterion: ownership distribution across mock nodes is uniform
/// within 5% (relative to the ideal 1/N share) for 3-, 5-, and 10-node rings.
#[test]
fn distribution_is_uniform_within_five_percent() {
    for n in [3usize, 5, 10] {
        let nodes = members(n);
        let ring = RendezvousRing::new(nodes.clone()).expect("non-empty membership");

        let mut counts = vec![0usize; n];
        for key in keyspace() {
            let owner = ring.owner(&key);
            let idx = nodes
                .iter()
                .position(|m| *m == owner)
                .expect("owner is a member");
            counts[idx] += 1;
        }

        let ideal = KEYSPACE as f64 / n as f64;
        for (idx, count) in counts.iter().enumerate() {
            let deviation = (*count as f64 - ideal).abs() / ideal;
            assert!(
                deviation <= 0.05,
                "{n}-node ring: node-{idx} owns {count} of {KEYSPACE} keys \
                 (ideal {ideal:.0}, deviation {:.2}%)",
                deviation * 100.0
            );
        }
    }
}

/// Acceptance criterion: membership change moves ~1/N of keys. Rendezvous
/// gives the strong form — removing a member moves exactly the keys that
/// member owned (each within 5% relative of the 1/N share) and no others.
#[test]
fn member_removal_moves_about_one_nth_of_ownership() {
    for n in [3usize, 5, 10] {
        let nodes = members(n);
        let before = RendezvousRing::new(nodes.clone()).expect("non-empty membership");

        let removed = nodes[n - 1].clone();
        let after = RendezvousRing::new(nodes[..n - 1].to_vec()).expect("non-empty membership");

        let mut moved = 0usize;
        for key in keyspace() {
            let old_owner = before.owner(&key);
            let new_owner = after.owner(&key);
            if old_owner == new_owner {
                continue;
            }
            moved += 1;
            assert_eq!(
                old_owner, removed,
                "only keys owned by the removed member may change owner"
            );
        }

        let ideal = KEYSPACE as f64 / n as f64;
        let deviation = (moved as f64 - ideal).abs() / ideal;
        assert!(
            deviation <= 0.05,
            "removing 1 of {n} members moved {moved} keys \
             (ideal {ideal:.0}, deviation {:.2}%)",
            deviation * 100.0
        );
    }
}

/// A fixed-membership ring never transitions ownership, so it never names a
/// warm donor (#30/#31): warm-from-peers is a live-ring behavior only. The
/// default keeps the cluster-of-one and the mock ring free of any peer hop.
#[test]
fn fixed_ring_names_no_warm_donor() {
    let solo = RendezvousRing::single(NodeId::new("node-solo"));
    for key in keyspace().take(1_000) {
        assert_eq!(solo.warm_donor(&key, &NodeId::new("node-solo")), None);
    }
    let ring = RendezvousRing::new(members(5)).expect("non-empty membership");
    for key in keyspace().take(1_000) {
        let owner = ring.owner(&key);
        assert_eq!(ring.warm_donor(&key, &owner), None);
    }
}
