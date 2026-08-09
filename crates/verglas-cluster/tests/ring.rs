//! Integration tests for the live weighted rendezvous ring (#28), mapped to
//! the issue's acceptance criteria: determinism across nodes, capacity-
//! proportional ownership within 5%, and minimal ownership movement on
//! join/leave. The keyspace is generated deterministically so every run
//! exercises identical keys and the statistical assertions are stable.

use verglas_cluster::ring::{LiveRing, RingMember, WeightedMember};
use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::ring::{RendezvousRing, Ring};

/// Number of deterministic keys sampled for the statistical assertions.
const KEYSPACE: usize = 100_000;

/// Builds a weighted member with an equal (unit) weight.
fn unit(id: &str) -> WeightedMember {
    WeightedMember::new(NodeId::new(id), 1)
}

/// `n` equal-weight members named `node-0..node-n`.
fn equal_members(n: usize) -> Vec<WeightedMember> {
    (0..n).map(|i| unit(&format!("node-{i}"))).collect()
}

/// The fixed, deterministic keyspace shared by all tests.
fn keyspace() -> impl Iterator<Item = CacheKey> {
    (0..KEYSPACE).map(|i| CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: format!("lake-{}", i % 7),
        key: format!("warehouse/db/table/data/part-{i:06}.parquet"),
    })
}

/// Counts how many keys each named node owns across the whole keyspace.
fn ownership_counts(ring: &LiveRing, names: &[&str]) -> Vec<usize> {
    let mut counts = vec![0usize; names.len()];
    for key in keyspace() {
        let owner = ring.owner(&key);
        let idx = names
            .iter()
            .position(|n| owner.as_str() == *n)
            .expect("owner is a member");
        counts[idx] += 1;
    }
    counts
}

#[test]
fn empty_membership_is_rejected() {
    assert!(LiveRing::new(Vec::new()).is_err());
}

#[test]
fn single_member_owns_everything() {
    let ring = LiveRing::single(NodeId::new("solo"), 42);
    assert_eq!(ring.members(), vec![NodeId::new("solo")]);
    for key in keyspace().take(2_000) {
        assert_eq!(ring.owner(&key).as_str(), "solo");
    }
}

/// Determinism: two nodes holding the same member set in different orders must
/// agree on every owner — ownership is a pod-wide fact, not a local one.
#[test]
fn ownership_is_deterministic_across_member_order() {
    let forward = LiveRing::new(equal_members(5)).expect("non-empty");
    let mut rev = equal_members(5);
    rev.reverse();
    let reversed = LiveRing::new(rev).expect("non-empty");

    for key in keyspace() {
        assert_eq!(forward.owner(&key), reversed.owner(&key));
    }
}

/// Equal weights reduce to plain rendezvous: distribution across 3/5/10 nodes
/// is uniform within 5% of the ideal 1/N share.
#[test]
fn equal_weights_distribute_uniformly_within_five_percent() {
    for n in [3usize, 5, 10] {
        let names: Vec<String> = (0..n).map(|i| format!("node-{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let ring = LiveRing::new(equal_members(n)).expect("non-empty");
        let counts = ownership_counts(&ring, &refs);

        let ideal = KEYSPACE as f64 / n as f64;
        for (idx, count) in counts.iter().enumerate() {
            let deviation = (*count as f64 - ideal).abs() / ideal;
            assert!(
                deviation <= 0.05,
                "{n}-node ring: node-{idx} owns {count} (ideal {ideal:.0}, dev {:.2}%)",
                deviation * 100.0
            );
        }
    }
}

/// Capacity weighting (#28 addendum): a node advertising 2× capacity owns ~2×
/// the keyspace. With weights 1/2/1 (total 4) the shares are 25/50/25%, each
/// within 5% relative.
#[test]
fn ownership_is_proportional_to_weight_within_five_percent() {
    let members = vec![
        WeightedMember::new(NodeId::new("small-a"), 1),
        WeightedMember::new(NodeId::new("big"), 2),
        WeightedMember::new(NodeId::new("small-b"), 1),
    ];
    let ring = LiveRing::new(members).expect("non-empty");
    let counts = ownership_counts(&ring, &["small-a", "big", "small-b"]);

    let total_weight = 4.0;
    let expected = [1.0, 2.0, 1.0];
    for (idx, count) in counts.iter().enumerate() {
        let ideal = KEYSPACE as f64 * expected[idx] / total_weight;
        let deviation = (*count as f64 - ideal).abs() / ideal;
        assert!(
            deviation <= 0.05,
            "member {idx} owns {count} (ideal {ideal:.0}, dev {:.2}%)",
            deviation * 100.0
        );
    }
}

/// A larger heterogeneity gap still tracks weight: a 6 TB node beside an 800 GB
/// node owns ~6/6.8 of the keyspace (the whitepaper's colocation example).
#[test]
fn large_capacity_gap_tracks_weight() {
    let members = vec![
        WeightedMember::new(NodeId::new("nvme-800g"), 800),
        WeightedMember::new(NodeId::new("nvme-6t"), 6000),
    ];
    let ring = LiveRing::new(members).expect("non-empty");
    let counts = ownership_counts(&ring, &["nvme-800g", "nvme-6t"]);

    let total = 6800.0;
    for (idx, (count, weight)) in counts.iter().zip([800.0, 6000.0]).enumerate() {
        let ideal = KEYSPACE as f64 * weight / total;
        let deviation = (*count as f64 - ideal).abs() / ideal;
        assert!(
            deviation <= 0.05,
            "member {idx} owns {count} (ideal {ideal:.0}, dev {:.2}%)",
            deviation * 100.0
        );
    }
}

/// The weighted live ring and the unweighted core `RendezvousRing` share one
/// hash-framing wire commitment, so at equal weights they must agree on every
/// owner. This is what lets a single-member `LiveRing` be the drop-in for the
/// N=1 `RendezvousRing` — no key changes home just because the ring type did.
#[test]
fn equal_weight_live_ring_agrees_with_core_rendezvous_ring() {
    let ids: Vec<NodeId> = (0..5).map(|i| NodeId::new(format!("node-{i}"))).collect();
    let live = LiveRing::new(
        ids.iter()
            .cloned()
            .map(|id| WeightedMember::new(id, 1))
            .collect(),
    )
    .expect("non-empty");
    let core = RendezvousRing::new(ids).expect("non-empty");

    for key in keyspace() {
        assert_eq!(live.owner(&key), core.owner(&key));
    }
}

/// Minimal movement on leave: removing a member moves *only* that member's
/// keys, and no others change owner (the strong rendezvous property).
#[test]
fn member_leave_moves_only_the_departed_keys() {
    for n in [3usize, 5, 10] {
        let before = LiveRing::new(equal_members(n)).expect("non-empty");
        let removed = NodeId::new(format!("node-{}", n - 1));
        let after = LiveRing::new(equal_members(n - 1)).expect("non-empty");

        let mut moved = 0usize;
        for key in keyspace() {
            let old = before.owner(&key);
            let new = after.owner(&key);
            if old == new {
                continue;
            }
            moved += 1;
            assert_eq!(old, removed, "only the departed member's keys may move");
        }
        let ideal = KEYSPACE as f64 / n as f64;
        let deviation = (moved as f64 - ideal).abs() / ideal;
        assert!(
            deviation <= 0.05,
            "{n}-node leave moved {moved} (ideal {ideal:.0})"
        );
    }
}

/// Minimal movement on join: adding a member moves ~1/(N+1) of keys, and every
/// moved key moves *to* the new member (none shuffle among incumbents).
#[test]
fn member_join_moves_about_one_over_n_plus_one() {
    for n in [3usize, 5, 10] {
        let before = LiveRing::new(equal_members(n)).expect("non-empty");
        let joined = NodeId::new(format!("node-{n}"));
        let after = LiveRing::new(equal_members(n + 1)).expect("non-empty");

        let mut moved = 0usize;
        for key in keyspace() {
            let old = before.owner(&key);
            let new = after.owner(&key);
            if old == new {
                continue;
            }
            moved += 1;
            assert_eq!(new, joined, "keys may only move to the joining member");
        }
        let ideal = KEYSPACE as f64 / (n + 1) as f64;
        let deviation = (moved as f64 - ideal).abs() / ideal;
        assert!(
            deviation <= 0.05,
            "join to {n} moved {moved} (ideal {ideal:.0})"
        );
    }
}

/// The ring updates in place: `update` swaps the member set atomically, bumps
/// the epoch, and every subsequent `owner` reflects the new membership. This is
/// how a gossip membership change reroutes ownership without rebuilding the
/// engine.
#[test]
fn update_swaps_membership_and_bumps_epoch() {
    let ring = LiveRing::single(NodeId::new("node-0"), 1);
    let start_epoch = ring.epoch();
    // Sole member owns everything.
    assert!(
        keyspace()
            .take(500)
            .all(|k| ring.owner(&k).as_str() == "node-0")
    );

    ring.update(equal_members(3)).expect("non-empty update");
    assert!(ring.epoch() > start_epoch, "epoch must advance on change");
    assert_eq!(ring.members().len(), 3);
    // Now ownership is spread — some keys land off node-0.
    let off_node0 = keyspace()
        .filter(|k| ring.owner(k).as_str() != "node-0")
        .count();
    assert!(off_node0 > 0, "membership change must reroute some keys");

    // An empty update is rejected and leaves the ring (and epoch) unchanged.
    let epoch_after = ring.epoch();
    assert!(ring.update(Vec::new()).is_err());
    assert_eq!(ring.epoch(), epoch_after);
    assert_eq!(ring.members().len(), 3);
}

/// #30 warm-from-peers at the ring: when a node joins and takes a key, the ring
/// names the key's *previous* owner as its warm donor — a healthy incumbent, so
/// `draining` is false (the join warm-up is gated on the joiner's warm window).
/// Adding a member never reshuffles the incumbents' relative order, so the
/// previous owner is exactly the highest-scoring non-joiner: the runner-up the
/// joiner now outranks.
#[test]
fn join_names_the_previous_owner_as_warm_donor() {
    let before = LiveRing::new(equal_members(3)).expect("non-empty");
    let joined = NodeId::new("node-3");
    let after = LiveRing::new(equal_members(4)).expect("non-empty");

    let mut checked = 0usize;
    for key in keyspace() {
        if after.owner(&key) != joined {
            continue;
        }
        let previous_owner = before.owner(&key);
        let donor = after
            .warm_donor(&key, &joined)
            .expect("a key the joiner owns has a previous holder");
        assert_eq!(
            donor.node, previous_owner,
            "the warm donor is the key's previous owner"
        );
        assert!(!donor.draining, "an incumbent donor is not draining");
        checked += 1;
    }
    assert!(checked > 0, "the joiner owns part of the keyspace");
}

/// #31 drain donor at the ring: a draining node owns nothing (its keys move to
/// successors), but it stays a ring member, and for every key it shed it is the
/// *draining* warm donor of the successor that inherited it — so the successor
/// warms from the leaving node, not the backend, and the pull is unconditional.
#[test]
fn draining_node_is_the_unconditional_warm_donor_of_its_successors() {
    let active = LiveRing::new(equal_members(3)).expect("non-empty");
    let drained = NodeId::new("node-2");

    let ring = LiveRing::new(equal_members(3)).expect("non-empty");
    ring.update_states(vec![
        RingMember::active(NodeId::new("node-0"), 1),
        RingMember::active(NodeId::new("node-1"), 1),
        RingMember::draining(drained.clone(), 1),
    ])
    .expect("non-empty");

    // A draining node is no longer an owner.
    assert!(
        !ring.members().contains(&drained),
        "a draining node owns nothing"
    );

    let mut shed = 0usize;
    for key in keyspace() {
        if active.owner(&key) != drained {
            // Ownership unchanged: a survivor still owns it. The draining node
            // must not become an *unconditional* donor for keys it never owned.
            let owner = ring.owner(&key);
            if let Some(donor) = ring.warm_donor(&key, &owner) {
                assert!(
                    !donor.draining,
                    "a still-owned key has no unconditional drain donor"
                );
            }
            continue;
        }
        // A key the draining node owned: its successor is the current owner and
        // must warm unconditionally from the leaving node.
        let successor = ring.owner(&key);
        assert_ne!(successor, drained, "ownership moved off the draining node");
        let donor = ring
            .warm_donor(&key, &successor)
            .expect("a shed key has a donor");
        assert_eq!(donor.node, drained, "the successor warms from the leaver");
        assert!(donor.draining, "a shed key's donor is the draining node");
        shed += 1;
    }
    assert!(shed > 0, "the draining node shed part of the keyspace");
}

/// The peer-serve set for a key is exactly two nodes — its owner and the one
/// donor the owner warms from — so a transition warms from a single previous
/// holder and a plain third node never serves (no A→B→C fan-out). This holds
/// for a join (the incumbent runner-up serves) and, by construction, a drain.
#[test]
fn peer_serve_set_is_the_owner_and_its_one_donor() {
    // Five nodes so most keys have a distinct owner, runner-up, and a third
    // node that is neither.
    let ring = LiveRing::new(equal_members(5)).expect("non-empty");
    let names: Vec<NodeId> = (0..5).map(|i| NodeId::new(format!("node-{i}"))).collect();

    let mut saw_third_refused = 0usize;
    for key in keyspace().take(20_000) {
        let owner = ring.owner(&key);
        let donor = ring
            .warm_donor(&key, &owner)
            .expect("a 5-node ring always names a donor")
            .node;
        // The owner and its designated donor serve; nobody else does.
        for n in &names {
            let serves = ring.should_serve_peer(&key, n);
            let expected = *n == owner || *n == donor;
            assert_eq!(
                serves, expected,
                "node {n} serve={serves} but expected {expected} (owner {owner}, donor {donor})"
            );
            if !expected && !serves {
                saw_third_refused += 1;
            }
        }
    }
    assert!(
        saw_third_refused > 0,
        "some third node must be refused — proving no fan-out"
    );
}
