//! Catalog snapshot compaction releases only checkpoint-covered internal payloads.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::sync::Notify;
use verglas_consensus::{
    AppliedOutcome, CatalogAction, CatalogBatch, CatalogRequirement, CommandKind, EntryHeader,
    FilePayloadReplica, PayloadCertificate, PayloadError, PayloadSet, PayloadStore,
    PersistentStateMachine, RaftCommand, ReconstructRequest, ReleaseRequest, RepairRequest,
    ReplicationMode, RequestId, SealRequest, StagedPayload, VerglasRaftConfig, WalArchiveSegment,
};

/// Store used to prove compaction never outruns failed physical reclamation.
struct RejectRelease;

/// Store that exposes whether physical reclamation blocks Raft state application.
struct BlockingRelease {
    entered: Arc<Notify>,
    continue_release: Arc<Notify>,
}

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

#[async_trait::async_trait]
impl PayloadStore for BlockingRelease {
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
        self.entered.notify_one();
        self.continue_release.notified().await;
        Ok(())
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
async fn archive_checkpoint_releases_and_prunes_only_covered_wal_payloads()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new()?;
    let replicas = (1..=5)
        .map(|node| FilePayloadReplica::open(node, root.path().join(node.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let payloads = Arc::new(PayloadSet::new(3, 2, replicas)?);
    let mut state = PersistentStateMachine::open(root.path().join("state.json")).await?;
    state.attach_payload_store(payloads.clone()).await?;
    let certificate =
        PayloadCertificate::new(ReplicationMode::Complete, 3, 2, vec![1, 2, 3, 4, 5])?;
    let binding_bucket = "tenant-db";
    let binding = EntryHeader::new(
        "timeline/tenant/timeline",
        1,
        CommandKind::WalArchiveBinding,
        RequestId::from_u128(100),
        binding_bucket.len() as u64,
        Sha256::digest(binding_bucket).into(),
        None,
        certificate.clone(),
    )?
    .with_wal_archive_bucket(binding_bucket.to_owned())?;
    state
        .apply(vec![entry(0, RaftCommand::Commit(binding))])
        .await?;

    let opened = EntryHeader::new(
        "timeline/tenant/timeline",
        1,
        CommandKind::TimelineOpen,
        RequestId::from_u128(101),
        0,
        Sha256::digest([]).into(),
        None,
        certificate.clone(),
    )?
    .with_wal_start(0)?;
    state
        .apply(vec![entry(1, RaftCommand::Commit(opened))])
        .await?;
    let lease = EntryHeader::new(
        "timeline/tenant/timeline",
        1,
        CommandKind::WriterLease,
        RequestId::from_u128(102),
        0,
        Sha256::digest([]).into(),
        Some(1),
        certificate.clone(),
    )?;
    state
        .apply(vec![entry(2, RaftCommand::Commit(lease))])
        .await?;

    let wal_request = RequestId::from_u128(103);
    let wal_body = Bytes::from_static(b"checkpoint-covered WAL");
    let staged = payloads
        .stage(
            wal_request,
            "timeline/tenant/timeline",
            1,
            ReplicationMode::Complete,
            &wal_body,
            &[1, 2, 3],
        )
        .await?;
    payloads
        .seal(SealRequest {
            hash: staged.hash(),
            group: "timeline/tenant/timeline",
            configuration_generation: 1,
            request: wal_request,
            term: 1,
            index: 3,
            certificate: staged.certificate(),
        })
        .await?;
    let wal = EntryHeader::new(
        "timeline/tenant/timeline",
        1,
        CommandKind::Wal,
        wal_request,
        staged.length(),
        staged.hash(),
        Some(1),
        staged.certificate().clone(),
    )?
    .with_wal_range(0, staged.length())?;
    state
        .apply(vec![entry(3, RaftCommand::Commit(wal.clone()))])
        .await?;

    let object_hash = [7; 32];
    let key = format!(
        "archive/timeline/{:016x}-{:016x}-{}",
        0,
        staged.length(),
        hex::encode(object_hash)
    );
    let segment = WalArchiveSegment::from_key(key.clone())?;
    let archive = EntryHeader::new(
        "timeline/tenant/timeline",
        1,
        CommandKind::ArchiveCheckpoint,
        RequestId::from_u128(104),
        key.len() as u64,
        Sha256::digest(key.as_bytes()).into(),
        None,
        certificate,
    )?
    .with_archive_segment(segment)?;
    state
        .apply(vec![entry(4, RaftCommand::Commit(archive))])
        .await?;

    state.release_checkpointed_wal_payloads().await?;
    state
        .apply(vec![entry(
            5,
            RaftCommand::ReleaseWal {
                through_lsn: staged.length(),
            },
        )])
        .await?;

    assert!(state.committed_header(3).await.is_none());
    assert!(state.committed_header(4).await.is_some());
    assert!(
        payloads
            .reconstruct(ReconstructRequest {
                hash: staged.hash(),
                group: "timeline/tenant/timeline",
                configuration_generation: 1,
                request: wal_request,
                length: staged.length(),
                term: 1,
                index: 3,
                certificate: staged.certificate(),
            })
            .await
            .is_err()
    );
    assert_eq!(
        state
            .apply(vec![entry(6, RaftCommand::Commit(wal))])
            .await?[0]
            .outcome,
        AppliedOutcome::Duplicate
    );
    Ok(())
}

#[tokio::test]
async fn wal_payload_reclamation_does_not_hold_the_raft_state_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new()?;
    let mut state = PersistentStateMachine::open(root.path().join("state.json")).await?;
    let entered = Arc::new(Notify::new());
    let continue_release = Arc::new(Notify::new());
    state
        .attach_payload_store(Arc::new(BlockingRelease {
            entered: Arc::clone(&entered),
            continue_release: Arc::clone(&continue_release),
        }))
        .await?;
    let certificate =
        PayloadCertificate::new(ReplicationMode::Complete, 3, 2, vec![1, 2, 3, 4, 5])?;
    let binding_bucket = "tenant-db";
    let binding = EntryHeader::new(
        "timeline/tenant/timeline",
        1,
        CommandKind::WalArchiveBinding,
        RequestId::from_u128(108),
        binding_bucket.len() as u64,
        Sha256::digest(binding_bucket).into(),
        None,
        certificate.clone(),
    )?
    .with_wal_archive_bucket(binding_bucket.to_owned())?;
    state
        .apply(vec![entry(0, RaftCommand::Commit(binding))])
        .await?;
    let opened = EntryHeader::new(
        "timeline/tenant/timeline",
        1,
        CommandKind::TimelineOpen,
        RequestId::from_u128(109),
        0,
        Sha256::digest([]).into(),
        None,
        certificate.clone(),
    )?
    .with_wal_start(0)?;
    state
        .apply(vec![entry(1, RaftCommand::Commit(opened))])
        .await?;
    let initial_lease = EntryHeader::new(
        "timeline/tenant/timeline",
        1,
        CommandKind::WriterLease,
        RequestId::from_u128(110),
        0,
        Sha256::digest([]).into(),
        Some(1),
        certificate.clone(),
    )?;
    state
        .apply(vec![entry(2, RaftCommand::Commit(initial_lease))])
        .await?;
    let wal = EntryHeader::new(
        "timeline/tenant/timeline",
        1,
        CommandKind::Wal,
        RequestId::from_u128(111),
        16,
        [11; 32],
        Some(1),
        certificate.clone(),
    )?
    .with_wal_range(0, 16)?;
    state
        .apply(vec![entry(3, RaftCommand::Commit(wal))])
        .await?;
    let archive_key = format!(
        "archive/timeline/{:016x}-{:016x}-{}",
        0,
        16,
        hex::encode([12; 32])
    );
    let archive = EntryHeader::new(
        "timeline/tenant/timeline",
        1,
        CommandKind::ArchiveCheckpoint,
        RequestId::from_u128(112),
        archive_key.len() as u64,
        Sha256::digest(archive_key.as_bytes()).into(),
        None,
        certificate.clone(),
    )?
    .with_archive_segment(WalArchiveSegment::from_key(archive_key)?)?;
    state
        .apply(vec![entry(4, RaftCommand::Commit(archive))])
        .await?;
    assert_eq!(state.wal_archive_state().await, (16, 16));
    assert!(state.committed_header(3).await.is_some());

    let releasing = {
        let state = state.clone();
        tokio::spawn(async move { state.release_checkpointed_wal_payloads().await })
    };
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .expect("checkpoint-covered WAL must enter physical release");
    let lease = EntryHeader::new(
        "timeline/tenant/timeline",
        1,
        CommandKind::WriterLease,
        RequestId::from_u128(113),
        0,
        Sha256::digest([]).into(),
        Some(2),
        certificate,
    )?;
    let applied = tokio::time::timeout(
        Duration::from_secs(2),
        state.apply(vec![entry(5, RaftCommand::Commit(lease))]),
    )
    .await;
    continue_release.notify_one();
    releasing.await??;
    applied.expect("Raft apply must not wait for remote payload deletion")?;
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
