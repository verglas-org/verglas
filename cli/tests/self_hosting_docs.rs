//! Documentation boundary checks for the supported self-hosted deployment.

/// Self-hosting documentation must cover the one-node provider-backed layout.
#[test]
fn documents_one_node_and_all_managed_catalog_profiles() {
    let guide = include_str!("../../docs/get-started/self-host.mdx");

    for required in [
        "one disposable `verglas-cache-node`",
        "Verglas Cloud",
        "Cloudflare R2 Data Catalog",
        "Amazon S3 Tables",
        "VERGLAS_PROVIDER",
        "VERGLAS_CATALOG_URI",
        "Tables",
        "Graphs",
        "Vectors",
        "/admin/catalog/events",
    ] {
        assert!(
            guide.contains(required),
            "missing self-host documentation: {required}"
        );
    }
    assert!(!guide.to_ascii_lowercase().contains("minio"));
}
