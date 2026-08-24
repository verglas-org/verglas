//! Optional authority delegates one transaction to one externally durable replica service.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use uuid::Uuid;
use verglas_do_engine::{
    CommitAuthority, IsolationLevel, LeaseIdentity, ReplicaCommitAuthority, ReplicaSink, Result,
    TransactionEnvelope,
};

struct Sink {
    succeeds: bool,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ReplicaSink for Sink {
    async fn persist(
        &self,
        _lease: &LeaseIdentity,
        _sequence: u64,
        _transaction_id: Uuid,
        _canonical: &[u8],
    ) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.succeeds {
            Ok(())
        } else {
            Err(verglas_do_engine::Error::Authority(
                "replica unavailable".to_owned(),
            ))
        }
    }
}

#[tokio::test]
async fn one_durable_replica_service_ack_advances_the_worker() {
    let calls = Arc::new(AtomicUsize::new(0));
    let authority = ReplicaCommitAuthority::new(
        "agent-1",
        LeaseIdentity::new("held-token", 7),
        0,
        Arc::new(Sink {
            succeeds: true,
            calls: calls.clone(),
        }),
    );
    let envelope =
        TransactionEnvelope::new("agent-1", Uuid::from_u128(31), 0, IsolationLevel::Snapshot);

    let receipt = authority.commit(&envelope).await.expect("replica ACK");
    assert_eq!(receipt.commit_sequence(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_replica_service_never_acknowledges_or_advances_sequence() {
    let authority = ReplicaCommitAuthority::new(
        "agent-1",
        LeaseIdentity::new("held-token", 8),
        0,
        Arc::new(Sink {
            succeeds: false,
            calls: Arc::new(AtomicUsize::new(0)),
        }),
    );
    let failed =
        TransactionEnvelope::new("agent-1", Uuid::from_u128(41), 0, IsolationLevel::Snapshot);
    assert!(authority.commit(&failed).await.is_err());
    let retry =
        TransactionEnvelope::new("agent-1", Uuid::from_u128(42), 0, IsolationLevel::Snapshot);
    assert!(authority.commit(&retry).await.is_err());
}
