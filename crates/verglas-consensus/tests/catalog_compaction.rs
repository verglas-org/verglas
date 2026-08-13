//! Catalog checkpoints bound retained consensus history without losing state or retries.

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use verglas_consensus::{
    AppliedOutcome, CatalogAction, CatalogBatch, CatalogRequirement, CommandKind, EntryHeader,
    PayloadCertificate, PersistentStateMachine, RaftCommand, ReplicationMode, RequestId,
    VerglasRaftConfig,
};

/// Builds one coded catalog header with deterministic payload identity.
fn header(
    kind: CommandKind,
    request: u128,
    payload: &[u8],
) -> Result<EntryHeader, Box<dyn std::error::Error>> {
    Ok(EntryHeader::new(
        "warehouse/tenant/warehouse",
        1,
        kind,
        RequestId::from_u128(request),
        payload.len() as u64,
        Sha256::digest(payload).into(),
        None,
        PayloadCertificate::new(ReplicationMode::Coded, 3, 2, vec![1, 2, 3, 4, 5])?,
    )?)
}

/// Wraps one application command in an exact committed Raft identity.
fn entry(index: u64, command: RaftCommand) -> Entry<VerglasRaftConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(command),
    }
}

#[tokio::test]
async fn checkpointed_catalog_history_is_compacted_without_losing_state_or_retry_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new()?;
    let path = root.path().join("state.json");
    let mut state = PersistentStateMachine::open(path.clone()).await?;
    let batch = CatalogBatch::new(
        vec![CatalogRequirement::NamespaceAbsent {
            namespace: "analytics".to_owned(),
        }],
        vec![CatalogAction::CreateNamespace {
            namespace: "analytics".to_owned(),
        }],
    )?;
    let catalog_header =
        header(CommandKind::Catalog, 7, b"catalog")?.with_catalog_batch(batch.clone())?;
    let catalog_response = state
        .apply(vec![entry(1, RaftCommand::Commit(catalog_header.clone()))])
        .await?;
    assert_eq!(catalog_response[0].outcome, AppliedOutcome::Committed);

    let object_header = header(CommandKind::Object, 9, b"object")?;
    state
        .apply(vec![entry(2, RaftCommand::Commit(object_header.clone()))])
        .await?;
    let checkpoint_header =
        header(CommandKind::CatalogCheckpoint, 8, b"checkpoint")?.with_catalog_checkpoint(2)?;
    state
        .apply(vec![entry(3, RaftCommand::Commit(checkpoint_header))])
        .await?;

    state.build_snapshot().await?;
    assert!(state.committed_header(1).await.is_none());
    assert_eq!(state.committed_header(2).await, Some(object_header.clone()));
    assert_eq!(state.catalog_namespaces().await, vec!["analytics"]);

    drop(state);
    let mut recovered = PersistentStateMachine::open(path).await?;
    assert!(recovered.committed_header(1).await.is_none());
    assert_eq!(recovered.committed_header(2).await, Some(object_header));
    assert_eq!(recovered.catalog_namespaces().await, vec!["analytics"]);

    let retry = recovered
        .apply(vec![entry(4, RaftCommand::Commit(catalog_header))])
        .await?;
    assert_eq!(retry[0].outcome, AppliedOutcome::Duplicate);

    let later_batch = CatalogBatch::new(
        vec![CatalogRequirement::NamespaceAbsent {
            namespace: "fresh".to_owned(),
        }],
        vec![CatalogAction::CreateNamespace {
            namespace: "fresh".to_owned(),
        }],
    )?;
    let later_header =
        header(CommandKind::Catalog, 10, b"fresh")?.with_catalog_batch(later_batch)?;
    recovered
        .apply(vec![entry(5, RaftCommand::Commit(later_header.clone()))])
        .await?;
    recovered.build_snapshot().await?;
    assert_eq!(recovered.committed_header(5).await, Some(later_header));
    Ok(())
}
