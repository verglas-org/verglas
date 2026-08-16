//! Contract tests for the native S3 Vectors and Graph REST-JSON SDK clients.

use verglas_sdk::{S3VectorsClient, SigV4Credentials, VerglasGraphsClient};

/// Keeps every checked-in S3 Vectors operation in the public Rust client surface.
#[allow(clippy::too_many_lines)]
/// The clients use the services the cache listener verifies, not bearer routes.
#[test]
fn semantic_clients_select_the_listener_sigv4_services() {
    let credentials = SigV4Credentials::new("key", "secret");
    assert_eq!(
        S3VectorsClient::new("http://127.0.0.1:8333", credentials.clone()).signing_name(),
        "s3vectors"
    );
    assert_eq!(
        VerglasGraphsClient::new("http://127.0.0.1:8333", credentials).signing_name(),
        "verglasgraphs"
    );
}
