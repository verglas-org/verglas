//! Catalog snapshot compaction releases only checkpoint-covered internal payloads.

use std::sync::Arc;

use bytes::Bytes;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use tempfile::TempDir;
use verglas_consensus::{
    AppliedOutcome, CatalogAction, CatalogBatch, CatalogRequirement, CommandKind, EntryHeader,
    FilePayloadReplica, PayloadCertificate, PayloadError, PayloadSet, PayloadStore,
    PersistentStateMachine, RaftCommand, ReconstructRequest, ReleaseRequest, RepairRequest,
    ReplicationMode, RequestId, SealRequest, StagedPayload, VerglasRaftConfig,
};

/// Store used to prove compaction never outruns failed physical reclamation.
struct RejectRelease;

#[async_trait::async_trait]
impl PayloadStore for RejectRelease {
    async fn set_voters(&self, _voters: Vec<u64>) -> Result<(), PayloadError> {
        Ok(())
    }

    async fn stage(
        &self,
        _request: RequestId,
        _group: &str,
        _configuration_generation: u64,
        _mode: ReplicationMode,
        _body: &[u8],
        _holders: &[u64],
    ) -> Result<StagedPayload, PayloadError> {
        Err(PayloadError::Transport(
            "stage is outside this test".to_owned(),
        ))
    }

    async fn reconstruct(&self, _read: ReconstructRequest<'_>) -> Result<Bytes, PayloadError> {
        Err(PayloadError::ReconstructionUnavailable)
    }

    async fn repair(&self, _repair: RepairRequest<'_>) -> Result<PayloadCertificate, PayloadError> {
        Err(PayloadError::Transport(
            "repair is outside this test".to_owned(),
        ))
    }

    async fn seal(&self, _seal: SealRequest<'_>) -> Result<(), PayloadError> {
        Err(PayloadError::Transport(
            "seal is outside this test".to_owned(),
        ))
    }

    async fn release(&self, _release: ReleaseRequest<'_>) -> Result<(), PayloadError> {
        Err(PayloadError::Transport(
            "injected release failure".to_owned(),
        ))
    }
}

/// Wraps one application command in an exact committed Raft identity.
fn entry(index: u64, command: RaftCommand) -> Entry<VerglasRaftConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(command),
    }
}

#[tokio::test]
async fn snapshot_releases_checkpointed_catalog_payloads_but_keeps_other_cache_entries()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new()?;
    let replicas = (1..=5)
        .map(|node| FilePayloadReplica::open(node, root.path().join(node.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let payloads = Arc::new(PayloadSet::new(3, 2, replicas)?);
    let mut state = PersistentStateMachine::open(root.path().join("state.json")).await?;
    state.attach_payload_store(payloads.clone()).await?;

    let catalog_request = RequestId::from_u128(71);
    let catalog_body = Bytes::from_static(b"catalog payload to release");
    let catalog = payloads
        .stage(
            catalog_request,
            "warehouse/tenant/warehouse",
            1,
            ReplicationMode::Complete,
            &catalog_body,
            &[1, 2, 3],
        )
        .await?;
    payloads
        .seal(SealRequest {
            hash: catalog.hash(),
            group: "warehouse/tenant/warehouse",
            configuration_generation: 1,
            request: catalog_request,
            term: 1,
            index: 1,
            certificate: catalog.certificate(),
        })
        .await?;
    let batch = CatalogBatch::new(
        vec![CatalogRequirement::NamespaceAbsent {
            namespace: "analytics".to_owned(),
        }],
        vec![CatalogAction::CreateNamespace {
            namespace: "analytics".to_owned(),
        }],
    )?;
    let catalog_header = EntryHeader::new(
        "warehouse/tenant/warehouse",
        1,
        CommandKind::Catalog,
        catalog_request,
        catalog.length(),
        catalog.hash(),
        None,
        catalog.certificate().clone(),
    )?
    .with_catalog_batch(batch)?;
    assert_eq!(
        state
            .apply(vec![entry(1, RaftCommand::Commit(catalog_header))])
            .await?[0]
            .outcome,
        AppliedOutcome::Committed
    );

    let object_request = RequestId::from_u128(72);
    let object_body = Bytes::from_static(b"unrelated object payload");
    let object = payloads
        .stage(
            object_request,
            "warehouse/tenant/warehouse",
            1,
            ReplicationMode::Complete,
            &object_body,
            &[1, 2, 3],
        )
        .await?;
    payloads
        .seal(SealRequest {
            hash: object.hash(),
            group: "warehouse/tenant/warehouse",
            configuration_generation: 1,
            request: object_request,
            term: 1,
            index: 2,
            certificate: object.certificate(),
        })
        .await?;
    let object_header = EntryHeader::new(
        "warehouse/tenant/warehouse",
        1,
        CommandKind::Object,
        object_request,
        object.length(),
        object.hash(),
        None,
        object.certificate().clone(),
    )?;
    state
        .apply(vec![entry(2, RaftCommand::Commit(object_header))])
        .await?;

    let checkpoint = EntryHeader::new(
        "warehouse/tenant/warehouse",
        1,
        CommandKind::CatalogCheckpoint,
        RequestId::from_u128(73),
        10,
        [3; 32],
        None,
        object.certificate().clone(),
    )?
    .with_catalog_checkpoint(2)?;
    state
        .apply(vec![entry(3, RaftCommand::Commit(checkpoint))])
        .await?;

    state.build_snapshot().await?;

    assert!(
        payloads
            .reconstruct(ReconstructRequest {
                hash: catalog.hash(),
                group: "warehouse/tenant/warehouse",
                configuration_generation: 1,
                request: catalog_request,
                length: catalog.length(),
                term: 1,
                index: 1,
                certificate: catalog.certificate(),
            })
            .await
            .is_err()
    );
    assert_eq!(
        payloads
            .reconstruct(ReconstructRequest {
                hash: object.hash(),
                group: "warehouse/tenant/warehouse",
                configuration_generation: 1,
                request: object_request,
                length: object.length(),
                term: 1,
                index: 2,
                certificate: object.certificate(),
            })
            .await?,
        object_body
    );
    assert_eq!(state.catalog_namespaces().await, vec!["analytics"]);
    Ok(())
}

#[tokio::test]
async fn failed_cache_release_keeps_catalog_history_for_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new()?;
    let mut state = PersistentStateMachine::open(root.path().join("state.json")).await?;
    state.attach_payload_store(Arc::new(RejectRelease)).await?;
    let certificate =
        PayloadCertificate::new(ReplicationMode::Complete, 3, 2, vec![1, 2, 3, 4, 5])?;
    let catalog = EntryHeader::new(
        "warehouse/tenant/warehouse",
        1,
        CommandKind::Catalog,
        RequestId::from_u128(81),
        7,
        [8; 32],
        None,
        certificate.clone(),
    )?
    .with_catalog_batch(CatalogBatch::new(
        vec![CatalogRequirement::NamespaceAbsent {
            namespace: "durable".to_owned(),
        }],
        vec![CatalogAction::CreateNamespace {
            namespace: "durable".to_owned(),
        }],
    )?)?;
    state
        .apply(vec![entry(1, RaftCommand::Commit(catalog))])
        .await?;
    let checkpoint = EntryHeader::new(
        "warehouse/tenant/warehouse",
        1,
        CommandKind::CatalogCheckpoint,
        RequestId::from_u128(82),
        10,
        [9; 32],
        None,
        certificate,
    )?
    .with_catalog_checkpoint(1)?;
    state
        .apply(vec![entry(2, RaftCommand::Commit(checkpoint))])
        .await?;

    assert!(state.build_snapshot().await.is_err());
    assert!(state.committed_header(1).await.is_some());
    assert_eq!(state.catalog_namespaces().await, vec!["durable"]);
    Ok(())
}

#[tokio::test]
async fn follower_snapshot_install_releases_catalog_payloads_pruned_by_the_leader()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new()?;
    let replicas = (1..=5)
        .map(|node| FilePayloadReplica::open(node, root.path().join(node.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let payloads = Arc::new(PayloadSet::new(3, 2, replicas)?);
    let request = RequestId::from_u128(91);
    let body = Bytes::from_static(b"follower catalog payload");
    let staged = payloads
        .stage(
            request,
            "warehouse/tenant/warehouse",
            1,
            ReplicationMode::Complete,
            &body,
            &[1, 2, 3],
        )
        .await?;
    payloads
        .seal(SealRequest {
            hash: staged.hash(),
            group: "warehouse/tenant/warehouse",
            configuration_generation: 1,
            request,
            term: 1,
            index: 1,
            certificate: staged.certificate(),
        })
        .await?;
    let catalog = EntryHeader::new(
        "warehouse/tenant/warehouse",
        1,
        CommandKind::Catalog,
        request,
        staged.length(),
        staged.hash(),
        None,
        staged.certificate().clone(),
    )?
    .with_catalog_batch(CatalogBatch::new(
        vec![CatalogRequirement::NamespaceAbsent {
            namespace: "follower".to_owned(),
        }],
        vec![CatalogAction::CreateNamespace {
            namespace: "follower".to_owned(),
        }],
    )?)?;
    let checkpoint = EntryHeader::new(
        "warehouse/tenant/warehouse",
        1,
        CommandKind::CatalogCheckpoint,
        RequestId::from_u128(92),
        10,
        [10; 32],
        None,
        staged.certificate().clone(),
    )?
    .with_catalog_checkpoint(1)?;

    let mut leader = PersistentStateMachine::open(root.path().join("leader.json")).await?;
    leader
        .apply(vec![
            entry(1, RaftCommand::Commit(catalog.clone())),
            entry(2, RaftCommand::Commit(checkpoint.clone())),
        ])
        .await?;
    let snapshot = leader.build_snapshot().await?;

    let mut follower = PersistentStateMachine::open(root.path().join("follower.json")).await?;
    follower.attach_payload_store(payloads.clone()).await?;
    follower
        .apply(vec![
            entry(1, RaftCommand::Commit(catalog)),
            entry(2, RaftCommand::Commit(checkpoint)),
        ])
        .await?;
    follower
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await?;

    assert!(
        payloads
            .reconstruct(ReconstructRequest {
                hash: staged.hash(),
                group: "warehouse/tenant/warehouse",
                configuration_generation: 1,
                request,
                length: staged.length(),
                term: 1,
                index: 1,
                certificate: staged.certificate(),
            })
            .await
            .is_err()
    );
    assert_eq!(follower.catalog_namespaces().await, vec!["follower"]);
    Ok(())
}
