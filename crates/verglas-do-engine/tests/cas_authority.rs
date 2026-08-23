//! Lease-fenced conditional S3 CAS is the default single-worker commit authority.

use std::sync::Arc;

use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use uuid::Uuid;
use verglas_do_engine::{
    CasCommitAuthority, CommitAuthority, IsolationLevel, LeaseIdentity, TransactionEnvelope,
};

#[tokio::test]
async fn commit_acks_only_after_immutable_transaction_and_head_cas_verify() {
    let store = Arc::new(InMemory::new());
    let authority = CasCommitAuthority::acquire(
        store.clone(),
        "tenant-a",
        "agent-1",
        LeaseIdentity::new("opaque-lock-token", 4),
    )
    .await
    .expect("acquire empty DO");
    let envelope =
        TransactionEnvelope::new("agent-1", Uuid::from_u128(11), 0, IsolationLevel::Snapshot);

    let receipt = authority.commit(&envelope).await.expect("CAS commit");
    assert_eq!(receipt.commit_sequence(), 1);
    let transaction_path = Path::from(format!(
        "tenant-a/agent-1/transactions/{:020}-{}.arrow",
        1,
        envelope.transaction_id()
    ));
    let archived = store
        .get(&transaction_path)
        .await
        .expect("transaction object")
        .bytes()
        .await
        .expect("transaction bytes");
    assert_eq!(
        archived.as_ref(),
        envelope.canonical_bytes().expect("canonical")
    );
    let head = store
        .get(&Path::from("tenant-a/agent-1/head"))
        .await
        .expect("head object")
        .bytes()
        .await
        .expect("head bytes");
    assert!(
        head.windows(16)
            .any(|window| window == Uuid::from_u128(11).as_bytes())
    );
}

#[tokio::test]
async fn launcher_handoff_fences_old_worker_and_preserves_sequence() {
    let store = Arc::new(InMemory::new());
    let old = CasCommitAuthority::acquire(
        store.clone(),
        "tenant-a",
        "agent-1",
        LeaseIdentity::new("old-token", 3),
    )
    .await
    .expect("initial acquire");
    let first =
        TransactionEnvelope::new("agent-1", Uuid::from_u128(50), 0, IsolationLevel::Snapshot);
    old.commit(&first).await.expect("first commit");
    let previous = old.lease_grant().expect("old grant");

    let successor = CasCommitAuthority::handoff(
        store,
        "tenant-a",
        "agent-1",
        previous,
        LeaseIdentity::new("new-token", 4),
    )
    .await
    .expect("lease handoff");
    assert_eq!(successor.lease_grant().expect("new grant").sequence(), 1);
    let stale =
        TransactionEnvelope::new("agent-1", Uuid::from_u128(51), 1, IsolationLevel::Snapshot);
    assert!(old.commit(&stale).await.is_err());
    let next =
        TransactionEnvelope::new("agent-1", Uuid::from_u128(52), 1, IsolationLevel::Snapshot);
    assert_eq!(
        successor
            .commit(&next)
            .await
            .expect("successor commit")
            .commit_sequence(),
        2
    );
}

#[tokio::test]
async fn stale_lease_version_cannot_ack_after_another_worker_advances_head() {
    let store = Arc::new(InMemory::new());
    let first = CasCommitAuthority::acquire(
        store.clone(),
        "tenant-a",
        "agent-1",
        LeaseIdentity::new("opaque-lock-token", 9),
    )
    .await
    .expect("acquire");
    let stale = CasCommitAuthority::from_grant(
        store,
        "tenant-a",
        "agent-1",
        first.lease_grant().expect("lease grant"),
    )
    .expect("clone stale holder view");
    let winner =
        TransactionEnvelope::new("agent-1", Uuid::from_u128(21), 0, IsolationLevel::Snapshot);
    first.commit(&winner).await.expect("winner CAS");
    let loser =
        TransactionEnvelope::new("agent-1", Uuid::from_u128(22), 0, IsolationLevel::Snapshot);

    let error = stale.commit(&loser).await.expect_err("stale CAS must fail");
    assert!(error.to_string().contains("lease CAS failed"));
}
