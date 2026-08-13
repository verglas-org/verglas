//! Acceptance tests for coded Raft agreement, recovery, and fencing.

use bytes::Bytes;
use verglas_consensus::{
    CommandKind, ConsensusError, GroupConfig, GroupId, NodeId, Proposal, ReplicationMode,
    RequestId, SimulatedGroup,
};

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId::from).collect()
}

fn request(id: u128) -> RequestId {
    RequestId::from_u128(id)
}

fn group(k: usize) -> SimulatedGroup {
    SimulatedGroup::new(
        GroupConfig::new(GroupId::from("warehouse/a"), nodes(&[1, 2, 3, 4, 5]), k)
            .expect("five voters with k <= 3 is legal"),
    )
}

fn proposal(id: u128, payload: &'static [u8]) -> Proposal {
    Proposal::new(
        request(id),
        CommandKind::Catalog,
        Bytes::from_static(payload),
    )
}

#[test]
fn coded_commit_requires_future_quorum_reconstructability() {
    let mut group = group(3);
    group
        .elect(NodeId::from(1), &nodes(&[1, 2, 3]))
        .expect("elect coded-write leader");

    let error = group
        .propose(
            NodeId::from(4),
            proposal(1, b"metadata-v1"),
            &nodes(&[1, 2, 3, 4]),
        )
        .expect_err("four fragments cannot satisfy coded durability");
    assert_eq!(
        error,
        ConsensusError::CodedQuorumUnavailable {
            required: 5,
            durable: 4,
        }
    );
    assert_eq!(group.commit_index(), 0);

    let committed = group
        .propose(
            NodeId::from(4),
            proposal(1, b"metadata-v1"),
            &nodes(&[1, 2, 3, 4, 5]),
        )
        .expect("five fragments satisfy coded durability");
    assert_eq!(committed.mode(), ReplicationMode::Coded);
    assert_eq!(committed.index(), 1);

    for quorum in [nodes(&[1, 2, 3]), nodes(&[1, 4, 5]), nodes(&[2, 4, 5])] {
        assert_eq!(
            group
                .reconstruct(1, &quorum)
                .expect("future quorum reconstructs committed payload"),
            b"metadata-v1"
        );
    }
}

#[test]
fn complete_entry_mode_preserves_majority_liveness() {
    let mut group = group(3);
    group
        .elect(NodeId::from(1), &nodes(&[1, 2, 3]))
        .expect("elect complete-write leader");

    let committed = group
        .propose_complete(
            NodeId::from(2),
            proposal(2, b"degraded-write"),
            &nodes(&[1, 2, 3]),
        )
        .expect("majority commits complete representation");
    assert_eq!(committed.mode(), ReplicationMode::Complete);
    assert_eq!(
        group
            .reconstruct(1, &nodes(&[1, 2, 3]))
            .expect("majority reconstructs complete representation"),
        b"degraded-write"
    );
}

#[test]
fn partitioned_old_leader_cannot_commit_a_conflict() {
    let mut group = group(2);
    let first_term = group
        .elect(NodeId::from(1), &nodes(&[1, 2, 3]))
        .expect("elect first leader");
    group
        .propose_complete(NodeId::from(1), proposal(10, b"first"), &nodes(&[1, 2, 3]))
        .expect("commit first-term entry");

    let second_term = group
        .elect(NodeId::from(4), &nodes(&[3, 4, 5]))
        .expect("elect replacement leader");
    assert!(second_term > first_term);
    let error = group
        .propose_complete(
            NodeId::from(1),
            proposal(11, b"conflict"),
            &nodes(&[1, 2, 3]),
        )
        .expect_err("displaced leader must be fenced");
    assert_eq!(
        error,
        ConsensusError::NotLeader {
            leader: NodeId::from(4)
        }
    );
    assert_eq!(
        group
            .reconstruct(1, &nodes(&[3, 4, 5]))
            .expect("new quorum reconstructs first entry"),
        b"first"
    );
}

#[test]
fn exact_retry_is_idempotent_and_conflicting_retry_fails_closed() {
    let mut group = group(2);
    group
        .elect(NodeId::from(1), &nodes(&[1, 2, 3]))
        .expect("elect retry-test leader");
    let first = group
        .propose_complete(NodeId::from(5), proposal(20, b"same"), &nodes(&[1, 2, 3]))
        .expect("commit initial request");
    let retry = group
        .propose_complete(NodeId::from(2), proposal(20, b"same"), &nodes(&[2, 3, 4]))
        .expect("accept exact retry");
    assert_eq!(retry.index(), first.index());

    let error = group
        .propose_complete(
            NodeId::from(2),
            proposal(20, b"different"),
            &nodes(&[2, 3, 4]),
        )
        .expect_err("reject conflicting retry");
    assert_eq!(error, ConsensusError::ConflictingRetry(request(20)));
}

#[test]
fn writer_epochs_fence_parallel_neon_ingresses() {
    let mut group = group(2);
    group
        .elect(NodeId::from(1), &nodes(&[1, 2, 3]))
        .expect("elect writer-epoch leader");
    let writer_a = group
        .acquire_writer(
            NodeId::from(4),
            request(30),
            "compute-a",
            &nodes(&[1, 2, 3]),
        )
        .expect("acquire first writer epoch");
    let writer_b = group
        .acquire_writer(
            NodeId::from(5),
            request(31),
            "compute-b",
            &nodes(&[2, 3, 4]),
        )
        .expect("acquire replacement writer epoch");
    assert!(writer_b.epoch() > writer_a.epoch());

    let stale = Proposal::new(request(32), CommandKind::Wal, Bytes::from_static(b"wal-a"))
        .with_writer_epoch(writer_a.epoch());
    assert_eq!(
        group
            .propose_complete(NodeId::from(4), stale, &nodes(&[1, 2, 3]))
            .expect_err("stale writer epoch must be fenced"),
        ConsensusError::StaleWriterEpoch {
            expected: writer_b.epoch(),
            actual: writer_a.epoch(),
        }
    );

    let current = Proposal::new(request(33), CommandKind::Wal, Bytes::from_static(b"wal-b"))
        .with_writer_epoch(writer_b.epoch());
    group
        .propose_complete(NodeId::from(5), current, &nodes(&[2, 3, 4]))
        .expect("current writer epoch commits WAL");
}

#[test]
fn linearizable_reads_refuse_an_isolated_replica() {
    let mut group = group(2);
    group
        .elect(NodeId::from(1), &nodes(&[1, 2, 3]))
        .expect("elect read-test leader");
    group
        .propose_complete(
            NodeId::from(5),
            proposal(40, b"catalog"),
            &nodes(&[1, 2, 3]),
        )
        .expect("commit catalog entry");

    assert_eq!(
        group
            .read_index(NodeId::from(5), &nodes(&[5]))
            .expect_err("isolated replica cannot establish ReadIndex"),
        ConsensusError::ReadQuorumUnavailable {
            required: 3,
            reached: 1
        }
    );
    assert_eq!(
        group
            .read_index(NodeId::from(4), &nodes(&[2, 3, 4]))
            .expect("majority establishes ReadIndex"),
        1
    );
}

#[test]
fn membership_change_uses_joint_quorums() {
    let mut group = group(2);
    group
        .elect(NodeId::from(1), &nodes(&[1, 2, 3]))
        .expect("elect membership-test leader");
    group
        .begin_membership_change(nodes(&[1, 2, 3, 4, 6]), &nodes(&[1, 2, 3, 4]))
        .expect("enter joint membership");

    assert_eq!(
        group
            .finish_membership_change(&nodes(&[1, 2, 3]))
            .expect_err("old-only acknowledgments cannot finish joint membership"),
        ConsensusError::JointQuorumUnavailable
    );
    group
        .finish_membership_change(&nodes(&[1, 2, 3, 4, 6]))
        .expect("joint quorum finishes membership change");
    assert_eq!(group.voters(), nodes(&[1, 2, 3, 4, 6]));
}

#[test]
fn geometry_rejects_a_code_that_no_future_majority_can_reconstruct() {
    let error = GroupConfig::new(GroupId::from("invalid"), nodes(&[1, 2, 3, 4, 5]), 4)
        .expect_err("k larger than a future majority cannot preserve Raft liveness");
    assert_eq!(error, ConsensusError::InvalidConfiguration);
}

#[test]
fn non_unanimous_coded_quorum_uses_the_general_intersection_bound() {
    let mut group = SimulatedGroup::new(
        GroupConfig::new(GroupId::from("seven"), nodes(&[1, 2, 3, 4, 5, 6, 7]), 2)
            .expect("seven voters with k=2 is legal"),
    );
    group
        .elect(NodeId::from(1), &nodes(&[1, 2, 3, 4]))
        .expect("elect seven-node leader");
    let committed = group
        .propose(
            NodeId::from(7),
            proposal(50, b"six-of-seven"),
            &nodes(&[1, 2, 3, 4, 5]),
        )
        .expect("general intersection bound accepts five holders");
    assert_eq!(committed.mode(), ReplicationMode::Coded);
    assert_eq!(
        group
            .reconstruct(1, &nodes(&[4, 5, 6, 7]))
            .expect("future majority reconstructs coded payload"),
        b"six-of-seven"
    );
}
