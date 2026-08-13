//! Durable replica tests for restart recovery and immediate reads.

use std::sync::Arc;

use bytes::Bytes;
use tempfile::TempDir;
use verglas_consensus::{
    CommandKind, ConsensusError, DurableGroup, FileReplica, GroupConfig, GroupId, NodeId, Proposal,
    ReplicationMode, RequestId,
};

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId::from).collect()
}

fn replicas(root: &TempDir, ids: &[u64]) -> Vec<Arc<FileReplica>> {
    ids.iter()
        .map(|id| {
            Arc::new(
                FileReplica::open(NodeId::from(*id), root.path().join(id.to_string()))
                    .expect("open replica"),
            )
        })
        .collect()
}

fn proposal(id: u128, payload: &'static [u8]) -> Proposal {
    Proposal::new(
        RequestId::from_u128(id),
        CommandKind::Catalog,
        Bytes::from_static(payload),
    )
}

#[test]
fn committed_coded_entry_recovers_from_a_future_election_quorum() {
    let root = TempDir::new().expect("temporary replica root");
    let config = GroupConfig::new(
        GroupId::from("warehouse/restart"),
        nodes(&[1, 2, 3, 4, 5]),
        3,
    )
    .expect("valid group");
    let all = replicas(&root, &[1, 2, 3, 4, 5]);
    {
        let mut group = DurableGroup::bootstrap(config.clone(), all.clone()).expect("bootstrap");
        group
            .elect(NodeId::from(1), &nodes(&[1, 2, 3]))
            .expect("elect leader");
        group
            .propose(
                NodeId::from(4),
                proposal(1, b"durable-catalog-command"),
                &nodes(&[1, 2, 3, 4, 5]),
            )
            .expect("commit coded entry");
    }

    let surviving = replicas(&root, &[1, 4, 5]);
    let recovered = DurableGroup::recover(config, surviving, &nodes(&[1, 4, 5]))
        .expect("recover from legal future quorum");
    assert_eq!(recovered.commit_index(), 1);
    assert_eq!(
        recovered.read_committed(1).expect("read recovered entry"),
        b"durable-catalog-command"
    );
}

#[test]
fn durable_headers_reject_a_conflicting_retry_after_restart() {
    let root = TempDir::new().expect("temporary replica root");
    let config = GroupConfig::new(GroupId::from("timeline/retry"), nodes(&[1, 2, 3]), 2)
        .expect("valid group");
    let all = replicas(&root, &[1, 2, 3]);
    {
        let mut group = DurableGroup::bootstrap(config.clone(), all.clone()).expect("bootstrap");
        group
            .elect(NodeId::from(1), &nodes(&[1, 2]))
            .expect("elect leader");
        group
            .propose(
                NodeId::from(3),
                proposal(9, b"original"),
                &nodes(&[1, 2, 3]),
            )
            .expect("commit original");
    }

    let mut recovered = DurableGroup::recover(config, all, &nodes(&[1, 2]))
        .expect("recover committed request identity");
    recovered
        .elect(NodeId::from(2), &nodes(&[1, 2]))
        .expect("elect replacement");
    let error = recovered
        .propose(
            NodeId::from(3),
            proposal(9, b"different"),
            &nodes(&[1, 2, 3]),
        )
        .expect_err("conflicting retry must fail closed");
    assert!(error.to_string().contains("conflicting retry"));
}

#[test]
fn complete_mode_is_durable_with_only_a_majority() {
    let root = TempDir::new().expect("temporary replica root");
    let config = GroupConfig::new(GroupId::from("degraded"), nodes(&[1, 2, 3, 4, 5]), 3)
        .expect("valid group");
    let all = replicas(&root, &[1, 2, 3, 4, 5]);
    let mut group = DurableGroup::bootstrap(config.clone(), all).expect("bootstrap");
    group
        .elect(NodeId::from(1), &nodes(&[1, 2, 3]))
        .expect("elect leader");
    let committed = group
        .propose_complete(
            NodeId::from(5),
            proposal(12, b"complete-majority"),
            &nodes(&[1, 2, 3]),
        )
        .expect("commit complete entry");
    assert_eq!(committed.mode(), ReplicationMode::Complete);

    let recovered = DurableGroup::recover(config, replicas(&root, &[1, 2, 3]), &nodes(&[1, 2, 3]))
        .expect("recover complete entry");
    assert_eq!(
        recovered
            .read_committed(1)
            .expect("recovered majority reads complete entry"),
        b"complete-majority"
    );
}

#[test]
fn durable_writer_epoch_survives_restart_and_fences_stale_wal() {
    let root = TempDir::new().expect("temporary replica root");
    let config = GroupConfig::new(GroupId::from("timeline/writer"), nodes(&[1, 2, 3]), 2)
        .expect("valid group");
    let all = replicas(&root, &[1, 2, 3]);
    let epoch = {
        let mut group = DurableGroup::bootstrap(config.clone(), all.clone()).expect("bootstrap");
        group
            .elect(NodeId::from(1), &nodes(&[1, 2]))
            .expect("elect initial durable writer leader");
        group
            .acquire_writer(
                NodeId::from(3),
                RequestId::from_u128(20),
                "compute-a",
                &nodes(&[1, 2]),
            )
            .expect("commit writer lease")
            .epoch()
    };

    let mut recovered =
        DurableGroup::recover(config, all, &nodes(&[1, 2])).expect("recover writer lease");
    recovered
        .elect(NodeId::from(2), &nodes(&[1, 2]))
        .expect("elect recovered durable writer leader");
    let stale = Proposal::new(
        RequestId::from_u128(21),
        CommandKind::Wal,
        Bytes::from_static(b"wal"),
    )
    .with_writer_epoch(epoch - 1);
    assert_eq!(
        recovered
            .propose_complete(NodeId::from(3), stale, &nodes(&[1, 2]))
            .expect_err("recovered epoch fences stale WAL writer"),
        ConsensusError::StaleWriterEpoch {
            expected: epoch,
            actual: epoch - 1,
        }
    );
}
