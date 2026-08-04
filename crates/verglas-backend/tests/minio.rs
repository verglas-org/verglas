//! Fills against a real S3-compatible store (issue #18 acceptance criterion:
//! "fills work against a real S3 bucket ... integration test ... can use
//! MinIO with static creds"). Opt-in only: the test is `#[ignore]`d and gated
//! on `VERGLAS_MINIO_URL`, so neither the default `cargo test` nor CI ever
//! needs a MinIO — the same streamed-fill contract is covered on every run by
//! `tests/fills.rs` against the in-process stub origin, through the same
//! shared assertion helper. The IMDS instance-role path cannot be exercised
//! off an EC2 instance and is verified manually.
//!
//! To run it for real:
//!
//! ```text
//! VERGLAS_MINIO_URL=http://127.0.0.1:9000 \
//!   cargo test -p verglas-backend --test minio -- --ignored
//! ```
//!
//! Required env when `VERGLAS_MINIO_URL` is set:
//!   VERGLAS_MINIO_URL         endpoint, e.g. http://127.0.0.1:9000
//!   VERGLAS_MINIO_ACCESS_KEY  access key id      (default: minioadmin)
//!   VERGLAS_MINIO_SECRET_KEY  secret access key  (default: minioadmin)
//!   VERGLAS_MINIO_BUCKET      an existing bucket  (default: verglas-test)

mod common;

use verglas_backend::{BackendStore, BackendStores};
use verglas_core::config::Backend;

/// Reads an env var or falls back to `default`.
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::test]
#[ignore = "needs a real S3 origin: set VERGLAS_MINIO_URL (see the file docs) and run with --ignored"]
async fn fills_stream_from_a_real_s3_store() {
    let Ok(endpoint) = std::env::var("VERGLAS_MINIO_URL") else {
        eprintln!("VERGLAS_MINIO_URL unset; skipping the real-S3 fill test");
        return;
    };
    let bucket = env_or("VERGLAS_MINIO_BUCKET", "verglas-test");

    // The client sources creds and endpoint from the environment (issue #18:
    // no credentials in Verglas config). Point object_store at MinIO with
    // static keys, http, and path-style addressing.
    unsafe {
        std::env::set_var(
            "AWS_ACCESS_KEY_ID",
            env_or("VERGLAS_MINIO_ACCESS_KEY", "minioadmin"),
        );
        std::env::set_var(
            "AWS_SECRET_ACCESS_KEY",
            env_or("VERGLAS_MINIO_SECRET_KEY", "minioadmin"),
        );
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
        std::env::set_var("AWS_ENDPOINT", &endpoint);
        std::env::set_var("AWS_ALLOW_HTTP", "true");
        std::env::set_var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false");
    }

    // The store builds one S3 client for the configured bucket behind its
    // limiter (#226: single-bucket serving).
    let backend = Backend {
        max_concurrent_requests: 4,
        bucket: Some(bucket.clone()),
        ..Backend::default()
    };
    let registry = BackendStore::from_config(&backend);
    let store = registry
        .store_for(&bucket)
        .expect("build MinIO client for the bucket");

    common::assert_streamed_fill(store).await;
}
