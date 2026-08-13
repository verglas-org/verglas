//! Acceptance tests for the engine-owned memory grant contract.

use verglas_core::grant::{LocalGrantHost, MemoryGrantHost, MemoryGrantRequest};

/// A standalone engine role gets the requested budget and can grow it without
/// depending on a client SDK or hosted scheduler implementation.
#[tokio::test]
async fn standalone_role_owns_its_memory_budget_contract() {
    let host = LocalGrantHost;
    let initial = host
        .request(MemoryGrantRequest::new(128, 64))
        .await
        .expect("local grant");
    assert_eq!(initial.bytes, 128);

    let grown = host.grow(initial, 32).await.expect("local growth");
    assert_eq!(grown.bytes, 160);
    host.release(grown).await;
}
