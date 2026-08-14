//! Repository-boundary checks for the independently published Rust SDK.

#[test]
fn public_sdk_does_not_import_cloud_authorization_implementation() {
    let manifest = include_str!("../Cargo.toml");
    assert!(
        !manifest.contains("verglas-authz"),
        "the public SDK must own its wire DTOs instead of importing private Cloud authorization code"
    );
}
