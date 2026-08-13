//! Contract tests for the small value replicated by the real Raft layer.

use verglas_consensus::{
    CommandKind, EntryHeader, PayloadCertificate, RaftCommand, ReplicationMode, RequestId,
};

#[test]
fn raft_command_contains_a_payload_certificate_not_the_large_payload() {
    let marker = "large-payload-must-not-enter-raft-log";
    let header = EntryHeader::new(
        "timeline/tenant-a/timeline-a",
        7,
        CommandKind::Wal,
        RequestId::from_u128(41),
        8 * 1024 * 1024 * 1024,
        [0x5a; 32],
        Some(9),
        PayloadCertificate::new(ReplicationMode::Coded, 3, 2, vec![1, 2, 3, 4, 5])
            .expect("valid coded certificate"),
    )
    .expect("valid header");

    let encoded = serde_json::to_vec(&RaftCommand::Commit(header)).expect("serialize command");
    assert!(
        encoded.len() < 4096,
        "Raft entries must remain small headers"
    );
    assert!(
        !encoded
            .windows(marker.len())
            .any(|bytes| bytes == marker.as_bytes())
    );
}

#[test]
fn coded_certificate_enforces_the_future_quorum_intersection_bound() {
    let error = PayloadCertificate::new(ReplicationMode::Coded, 3, 2, vec![1, 2, 3, 4])
        .expect_err("four fragments cannot intersect every five-voter majority in three shards");
    assert_eq!(error.required(), 5);

    PayloadCertificate::new(ReplicationMode::Coded, 3, 2, vec![1, 2, 3, 4, 5])
        .expect("all five fragments satisfy F+k");
    PayloadCertificate::new(ReplicationMode::Complete, 3, 2, vec![1, 2, 3])
        .expect("complete entries require an ordinary majority");
}
