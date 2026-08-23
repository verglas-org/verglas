//! Verified provider-neutral publication of covered derived artifacts.

use std::sync::Arc;

use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use verglas_do_engine::{
    ArtifactCoverage, ArtifactKind, DerivedArtifact, ObjectStoreDerivedArtifactPublisher, TableId,
};

#[test]
fn empty_or_reversed_coverage_is_rejected() {
    assert!(ArtifactCoverage::new(4, 4).is_err());
    assert!(ArtifactCoverage::new(5, 4).is_err());
}

#[tokio::test]
async fn publisher_writes_and_reads_back_covered_immutable_bytes() {
    let store = Arc::new(InMemory::new());
    let publisher = ObjectStoreDerivedArtifactPublisher::new(store.clone(), "tenant-a");
    let artifact = DerivedArtifact::new(
        "agent-7".to_owned(),
        TableId::new("edges"),
        ArtifactKind::GraphPuffin,
        ArtifactCoverage::new(0, 9).expect("coverage"),
        b"puffin-bytes".to_vec(),
    );

    let receipt = publisher
        .publish(&artifact)
        .await
        .expect("publish artifact");
    assert_eq!(receipt.coverage(), artifact.coverage());
    assert_eq!(receipt.sha256().len(), 64);
    assert!(receipt.object_path().ends_with("graph-puffin/9.puffin"));
    let bytes = store
        .get(&Path::from(receipt.object_path()))
        .await
        .expect("read artifact")
        .bytes()
        .await
        .expect("artifact bytes");
    assert_eq!(bytes.as_ref(), artifact.bytes());
    publisher.publish(&artifact).await.expect("exact retry");
    let conflicting = DerivedArtifact::new(
        "agent-7".to_owned(),
        TableId::new("edges"),
        ArtifactKind::GraphPuffin,
        ArtifactCoverage::new(0, 9).expect("coverage"),
        b"different-puffin-bytes".to_vec(),
    );
    assert!(publisher.publish(&conflicting).await.is_err());
}
