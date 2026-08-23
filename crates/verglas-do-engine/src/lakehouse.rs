//! Durable Lakehouse Object boundary for covered derived artifact publication.

use std::sync::Arc;

use crate::{
    ArtifactReceipt, DerivedArtifact, Error, IcebergCommitReceipt, IcebergCommitter,
    IcebergIndexCoverage, ObjectStoreDerivedArtifactPublisher, OffloadBatch, OffloadBatchArchive,
    Result, VerifiedIcebergArchive,
};

/// Ownership class of the destination receiving lake artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBinding {
    /// Verglas owns the object layout and all reads route through Verglas.
    Managed,
    /// The customer owns the layout and retains independent access.
    Customer,
}

/// Authority under which one materialization publication was requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationAuthorization {
    /// A managed background coordinator requested publication.
    Autonomous,
    /// The customer explicitly invoked this publication.
    Explicit,
}

/// Durable Lakehouse Object publication primitive for cloud composition.
pub struct LakehouseObject {
    do_id: String,
    binding: StorageBinding,
    publisher: Arc<ObjectStoreDerivedArtifactPublisher>,
}

impl LakehouseObject {
    /// Binds one durable Lakehouse identity to an explicitly classified destination.
    pub fn new(
        do_id: impl Into<String>,
        binding: StorageBinding,
        publisher: Arc<ObjectStoreDerivedArtifactPublisher>,
    ) -> Self {
        Self {
            do_id: do_id.into(),
            binding,
            publisher,
        }
    }

    /// Publishes covered bytes while enforcing customer-storage authorization.
    pub async fn publish(
        &self,
        artifact: &DerivedArtifact,
        authorization: PublicationAuthorization,
    ) -> Result<ArtifactReceipt> {
        if artifact.do_id() != self.do_id {
            return Err(Error::WrongDo {
                expected: self.do_id.clone(),
                actual: artifact.do_id().to_owned(),
            });
        }
        if self.binding == StorageBinding::Customer
            && authorization != PublicationAuthorization::Explicit
        {
            return Err(Error::Materialization(
                "customer storage publication requires explicit invocation".to_owned(),
            ));
        }
        self.publisher.publish(artifact).await
    }

    /// Commits one shared offload batch under the binding's publication authority.
    pub async fn commit_batch(
        &self,
        batch: &OffloadBatch,
        committer: &IcebergCommitter,
        authorization: PublicationAuthorization,
    ) -> Result<IcebergCommitReceipt> {
        self.commit_batch_with_coverage(
            batch,
            committer,
            authorization,
            IcebergIndexCoverage::none(),
        )
        .await
    }

    /// Commits a shared batch and records verified index coverage in its snapshot.
    pub async fn commit_batch_with_coverage(
        &self,
        batch: &OffloadBatch,
        committer: &IcebergCommitter,
        authorization: PublicationAuthorization,
        coverage: IcebergIndexCoverage,
    ) -> Result<IcebergCommitReceipt> {
        let first = batch
            .transactions()
            .first()
            .ok_or_else(|| Error::Materialization("cannot commit an empty batch".to_owned()))?;
        if first.do_id() != self.do_id {
            return Err(Error::WrongDo {
                expected: self.do_id.clone(),
                actual: first.do_id().to_owned(),
            });
        }
        if self.binding == StorageBinding::Customer
            && authorization != PublicationAuthorization::Explicit
        {
            return Err(Error::Materialization(
                "customer storage publication requires explicit invocation".to_owned(),
            ));
        }
        committer
            .commit_batch_authorized(batch, coverage, self.binding, authorization, &self.do_id)
            .await
    }

    /// Creates an offload sink whose receipt is returned only after Iceberg verification.
    pub fn verified_offload_archive(
        &self,
        archive: Arc<dyn OffloadBatchArchive>,
        committer: Arc<IcebergCommitter>,
        authorization: PublicationAuthorization,
    ) -> VerifiedIcebergArchive {
        self.verified_offload_archive_with_coverage(
            archive,
            committer,
            authorization,
            IcebergIndexCoverage::none(),
        )
    }

    /// Creates a verified offload sink that records index coverage in each snapshot.
    pub fn verified_offload_archive_with_coverage(
        &self,
        archive: Arc<dyn OffloadBatchArchive>,
        committer: Arc<IcebergCommitter>,
        authorization: PublicationAuthorization,
        coverage: IcebergIndexCoverage,
    ) -> VerifiedIcebergArchive {
        VerifiedIcebergArchive::new_with_coverage(
            archive,
            committer,
            self.binding,
            authorization,
            self.do_id.clone(),
            coverage,
        )
    }
}
