//! Documentation boundary checks for the two supported self-hosted deployments.

/// Self-hosting documentation must cover direct-origin and managed-edge layouts.
#[test]
fn documents_standalone_and_edge_cache_topologies() {
    let guide = include_str!("../../docs/get-started/self-host.mdx");

    assert!(guide.contains("Standalone runtime node"));
    assert!(guide.contains("Edge cache"));
    assert!(guide.contains("VERGLAS_STORAGE_BUCKET"));
    assert!(guide.contains("VERGLAS_CATALOG_URI"));
    assert!(guide.contains("Apache Polaris"));
    assert!(guide.contains("AWS Glue"));
    assert!(guide.contains("S3 Tables"));
    assert!(guide.contains("push notifications"));
    assert!(guide.contains("compaction"));
    assert!(guide.contains("serve-craft"));
    assert!(guide.contains("CRaft ring"));
    assert!(!guide.contains("catalog service's PostgreSQL database"));
}
