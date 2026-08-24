//! Assertion-carrying tests for component digest verification and disk fetches.

use verglas_do_wasm::{ArtifactError, ArtifactStore, ComponentDigest, DirArtifactStore};

/// The digest renders as 64 hex characters and parses back to itself.
#[test]
fn digest_round_trips_hex() {
    let digest = ComponentDigest::new([0xab; 32]);
    let rendered = digest.to_string();
    assert_eq!(rendered.len(), 64);

    let parsed = rendered.parse::<ComponentDigest>().expect("parse digest");
    assert_eq!(parsed, digest);
    assert_eq!(
        ComponentDigest::from_hex(&rendered).expect("parse hex"),
        digest
    );
}

/// Verification accepts the exact bytes and rejects any tampering.
#[test]
fn verify_accepts_matching_bytes_and_rejects_tampering() {
    let bytes = b"component bytes";
    let digest = ComponentDigest::compute(bytes);

    digest.verify(bytes).expect("matching bytes verify");
    let error = digest
        .verify(b"tampered component bytes")
        .expect_err("tampered bytes must fail verification");
    assert!(matches!(
        error,
        ArtifactError::DigestMismatch { expected, actual }
            if expected == digest && actual != digest
    ));
}

/// The directory store returns stored bytes whose digest matches.
#[tokio::test]
async fn directory_store_fetches_verified_bytes() {
    let root = tempfile::tempdir().expect("artifact root");
    let bytes = b"stored component".to_vec();
    let digest = ComponentDigest::compute(&bytes);
    let path = root.path().join(format!("{digest}.wasm"));
    std::fs::write(&path, &bytes).expect("write component");

    let store = DirArtifactStore::new(root.path().to_path_buf());
    let fetched = store.fetch(digest).await.expect("fetch component");
    assert_eq!(fetched, bytes);
}

/// The directory store fails closed when stored bytes are corrupted.
#[tokio::test]
async fn directory_store_fails_closed_on_digest_mismatch() {
    let root = tempfile::tempdir().expect("artifact root");
    let expected = b"expected component";
    let digest = ComponentDigest::compute(expected);
    let path = root.path().join(format!("{digest}.wasm"));
    std::fs::write(&path, b"corrupted component").expect("write corrupted component");

    let store = DirArtifactStore::new(root.path().to_path_buf());
    let error = store
        .fetch(digest)
        .await
        .expect_err("corrupted bytes must fail closed");
    assert!(matches!(
        error,
        ArtifactError::DigestMismatch { expected: demanded, actual }
            if demanded == digest && actual != digest
    ));
}
