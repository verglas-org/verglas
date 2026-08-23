//! Durable Lakehouse Object boundary for covered derived artifact publication.

use std::sync::Arc;

use crate::{ArtifactReceipt, DerivedArtifact, Error, ObjectStoreDerivedArtifactPublisher, Result};

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
}
