//! Lakehouse objects enforce managed versus customer publication authority.

use std::sync::Arc;

use object_store::memory::InMemory;
use verglas_do_engine::{
    ArtifactCoverage, ArtifactKind, DerivedArtifact, LakehouseObject,
    ObjectStoreDerivedArtifactPublisher, PublicationAuthorization, StorageBinding, TableId,
};

fn artifact(do_id: &str, through: u64) -> DerivedArtifact {
    DerivedArtifact::new(
        do_id.to_owned(),
        TableId::new("events"),
        ArtifactKind::Parquet,
        ArtifactCoverage::new(through - 1, through).expect("coverage"),
        b"PAR1-derived".to_vec(),
    )
}

#[tokio::test]
async fn customer_binding_requires_explicit_publication_invocation() {
    let publisher = Arc::new(ObjectStoreDerivedArtifactPublisher::new(
        Arc::new(InMemory::new()),
        "customer-prefix",
    ));
    let lakehouse = LakehouseObject::new("lake-1", StorageBinding::Customer, publisher);

    assert!(
        lakehouse
            .publish(&artifact("lake-1", 1), PublicationAuthorization::Autonomous)
            .await
            .is_err()
    );
    lakehouse
        .publish(&artifact("lake-1", 1), PublicationAuthorization::Explicit)
        .await
        .expect("explicit customer publication");
}

#[tokio::test]
async fn managed_binding_may_publish_covered_artifact_autonomously() {
    let publisher = Arc::new(ObjectStoreDerivedArtifactPublisher::new(
        Arc::new(InMemory::new()),
        "managed-prefix",
    ));
    let lakehouse = LakehouseObject::new("lake-2", StorageBinding::Managed, publisher);
    let receipt = lakehouse
        .publish(&artifact("lake-2", 4), PublicationAuthorization::Autonomous)
        .await
        .expect("managed publication");
    assert_eq!(receipt.coverage().through(), 4);
}
