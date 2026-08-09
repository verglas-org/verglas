//! Typed, scoped secret lifecycle and authorization semantics.

use std::collections::BTreeSet;
use std::sync::Arc;

use verglas_authz::{
    Action, AeadSecretCipher, Authorizer, CreateSecret, Grant, MemoryAuthorizer,
    MemorySecretRepository, Principal, PrincipalKind, ReplaceSecret, ResolveSecret, ResourceKind,
    SecretCipher, SecretError, SecretKind, SecretService,
};

/// Deterministic cipher proving that repositories receive only sealed values.
struct TestCipher;

impl SecretCipher for TestCipher {
    fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, SecretError> {
        Ok(plaintext.iter().map(|byte| byte ^ 0x5a).collect())
    }

    fn open(&self, ciphertext: &[u8]) -> Result<Vec<u8>, SecretError> {
        self.seal(ciphertext)
    }
}

#[test]
fn production_cipher_authenticates_every_envelope() {
    let cipher = AeadSecretCipher::new(&[7_u8; 32]).expect("cipher");
    let first = cipher.seal(b"secret-value").expect("first");
    let second = cipher.seal(b"secret-value").expect("second");
    assert_ne!(first, second);
    assert_eq!(cipher.open(&first).expect("open"), b"secret-value");

    let mut tampered = first;
    let last = tampered.len() - 1;
    tampered[last] ^= 1;
    assert!(matches!(
        cipher.open(&tampered),
        Err(SecretError::Backend(_))
    ));
}

/// Creates a service with one principal that owns the supplied secret resources.
async fn service() -> (Arc<MemoryAuthorizer>, SecretService) {
    let authorizer = Arc::new(MemoryAuthorizer::new());
    authorizer
        .create_principal(Principal::new(
            "tenant-a",
            "runtime",
            PrincipalKind::Application,
        ))
        .await
        .expect("principal");
    authorizer
        .create_principal(Principal::new(
            "tenant-a",
            "creator",
            PrincipalKind::Application,
        ))
        .await
        .expect("creator");
    let service = SecretService::new(
        authorizer.clone(),
        Arc::new(MemorySecretRepository::new()),
        Arc::new(TestCipher),
    );
    (authorizer, service)
}

/// Grants use-secret on one stable secret resource.
async fn grant_use(authorizer: &MemoryAuthorizer, resource_id: &str, grant_id: &str) {
    authorizer
        .create_grant(Grant::new(
            grant_id,
            "tenant-a",
            "runtime",
            resource_id,
            BTreeSet::from([Action::UseSecret]),
        ))
        .await
        .expect("grant");
}

#[tokio::test]
async fn resolves_longest_authorized_uri_scope_and_never_lists_plaintext() {
    let (_authorizer, service) = service().await;
    for (id, scope, value) in [
        ("bucket", "s3://customer-bucket", b"bucket-value".as_slice()),
        (
            "team",
            "s3://customer-bucket/team",
            b"team-value".as_slice(),
        ),
    ] {
        let metadata = service
            .create(CreateSecret::new(
                "tenant-a",
                "runtime",
                id,
                SecretKind::S3,
                scope,
                value,
            ))
            .await
            .expect("create");
        assert_eq!(metadata.resource_kind, ResourceKind::Secret);
    }
    let resolved = service
        .resolve(ResolveSecret::new(
            "tenant-a",
            "runtime",
            SecretKind::S3,
            "s3://customer-bucket/team/table/data.parquet",
        ))
        .await
        .expect("resolve");
    assert_eq!(resolved.resource_id, "team");
    assert_eq!(resolved.expose(), b"team-value");
    let unauthorized = service
        .resolve(ResolveSecret::new(
            "tenant-a",
            "creator",
            SecretKind::S3,
            "s3://customer-bucket/team/table/data.parquet",
        ))
        .await;
    assert!(matches!(unauthorized, Err(SecretError::Forbidden(_))));

    let listed = service.list("tenant-a").await.expect("list");
    let json = serde_json::to_string(&listed).expect("json");
    assert!(!json.contains("bucket-value"));
    assert!(!json.contains("team-value"));
}

#[tokio::test]
async fn replacement_keeps_resource_identity_and_advances_version() {
    let (_authorizer, service) = service().await;
    service
        .create(CreateSecret::new(
            "tenant-a",
            "runtime",
            "catalog",
            SecretKind::IcebergRest,
            "https://catalog.customer.com",
            b"old-token",
        ))
        .await
        .expect("create");
    let replaced = service
        .replace(ReplaceSecret::new(
            "tenant-a",
            "runtime",
            "catalog",
            b"new-token",
        ))
        .await
        .expect("replace");
    assert_eq!(replaced.id, "catalog");
    assert_eq!(replaced.current_version, 2);
    service
        .create(CreateSecret::new(
            "tenant-a",
            "runtime",
            "catalog-specific",
            SecretKind::IcebergRest,
            "https://catalog.customer.com/v1",
            b"different-token",
        ))
        .await
        .expect("more specific secret");
    let resolved = service
        .resolve_by_id("tenant-a", "runtime", "catalog")
        .await
        .expect("resolve stable binding");
    assert_eq!(resolved.version, 2);
    assert_eq!(resolved.expose(), b"new-token");
}

#[tokio::test]
async fn resolution_fails_closed_when_missing_unauthorized_or_ambiguous() {
    let (authorizer, service) = service().await;
    let missing = service
        .resolve(ResolveSecret::new(
            "tenant-a",
            "runtime",
            SecretKind::S3,
            "s3://missing/path",
        ))
        .await;
    assert!(matches!(missing, Err(SecretError::NotFound(_))));

    for id in ["first", "second"] {
        service
            .create(CreateSecret::new(
                "tenant-a",
                "creator",
                id,
                SecretKind::S3,
                "s3://same/scope",
                id.as_bytes(),
            ))
            .await
            .expect("create");
    }
    let unauthorized = service
        .resolve(ResolveSecret::new(
            "tenant-a",
            "runtime",
            SecretKind::S3,
            "s3://same/scope/object",
        ))
        .await;
    assert!(matches!(unauthorized, Err(SecretError::Forbidden(_))));

    grant_use(&authorizer, "first", "grant-first").await;
    grant_use(&authorizer, "second", "grant-second").await;
    let ambiguous = service
        .resolve(ResolveSecret::new(
            "tenant-a",
            "runtime",
            SecretKind::S3,
            "s3://same/scope/object",
        ))
        .await;
    assert!(matches!(ambiguous, Err(SecretError::Conflict(_))));
}

#[tokio::test]
async fn rejects_malformed_scope_and_cross_type_resolution() {
    let (_authorizer, service) = service().await;
    let malformed = service
        .create(CreateSecret::new(
            "tenant-a",
            "runtime",
            "bad",
            SecretKind::S3,
            "not a uri",
            b"value",
        ))
        .await;
    assert!(matches!(malformed, Err(SecretError::Invalid(_))));

    service
        .create(CreateSecret::new(
            "tenant-a",
            "runtime",
            "catalog",
            SecretKind::IcebergRest,
            "https://catalog.customer.com",
            b"token",
        ))
        .await
        .expect("create");
    let wrong_type = service
        .resolve(ResolveSecret::new(
            "tenant-a",
            "runtime",
            SecretKind::S3,
            "s3://catalog.customer.com/path",
        ))
        .await;
    assert!(matches!(wrong_type, Err(SecretError::NotFound(_))));
}
