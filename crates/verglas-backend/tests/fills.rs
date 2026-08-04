//! Fills against the in-process stub S3 origin — the default `cargo test`
//! coverage of the streamed-fill contract (issue #18), with no MinIO, no
//! docker, and no network beyond loopback.
//!
//! The store is built exactly the way production builds it
//! ([`BackendStore::from_config`]: endpoint resolution, the per-bucket limiter
//! stack, file-sourced credentials) and then run through the same assertion
//! helper the opt-in real-S3 test (`tests/minio.rs`) uses, so the default path
//! covers the same logic rather than skipping it.

mod common;

use verglas_backend::{BackendStore, BackendStores};
use verglas_core::config::Backend;

#[tokio::test]
async fn fills_stream_from_an_in_process_s3_stub() {
    let endpoint = common::spawn_stub_origin();

    // Credentials come from a file, exercising the config-named credentials
    // path (issue #18: no credentials in Verglas config) without touching the
    // process environment.
    let dir = std::env::temp_dir().join(format!("verglas-fill-stub-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let creds = dir.join("credentials");
    std::fs::write(
        &creds,
        "[default]\naws_access_key_id = stub-access-key\naws_secret_access_key = stub-secret-key\n",
    )
    .expect("write creds");

    // The store builds one S3 client for the configured bucket behind its
    // limiter (#226: single-bucket serving), pointed at the stub via config —
    // endpoint, plaintext-http opt-in, and path-style addressing (#221).
    let bucket = "verglas-test".to_owned();
    let backend = Backend {
        max_concurrent_requests: 4,
        bucket: Some(bucket.clone()),
        endpoint: Some(endpoint),
        allow_http: true,
        credentials_file: Some(creds.display().to_string()),
        ..Backend::default()
    };
    let registry = BackendStore::from_config(&backend);
    let store = registry
        .store_for(&bucket)
        .expect("build stub client for the bucket");

    common::assert_streamed_fill(store).await;

    let _ = std::fs::remove_dir_all(&dir);
}
