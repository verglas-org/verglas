//! Access-token minting, validation, and revocation behavior.

use verglas_authz::{
    AccessTokenRegistry, AccessTokenService, AccessTokenSigner, AuthzError,
    MemoryAccessTokenRegistry, TargetJwtRequest, TargetJwtSigner, TokenMintRequest,
    verifying_key_from_jwk,
};

/// Mints a process token that is valid for the test data plane.
fn request() -> TokenMintRequest {
    TokenMintRequest::new(
        "token-cli-alice-id",
        "tenant-a",
        "user-alice",
        "token-cli-alice",
        "Local CLI",
        "verglas-data-plane",
        7,
        1_000,
        2_000,
    )
}

#[tokio::test]
async fn minted_tokens_authenticate_only_while_durably_active() {
    let registry = MemoryAccessTokenRegistry::new();
    let signer = AccessTokenSigner::new([9; 32]);
    let service = AccessTokenService::new(signer, std::sync::Arc::new(registry.clone()));
    let minted = service.mint(request()).await.expect("mint");

    let claims = service
        .authenticate(
            minted.token.expose(),
            "tenant-a",
            "verglas-data-plane",
            1_500,
        )
        .await
        .expect("authenticate");
    assert_eq!(claims.principal_id, "token-cli-alice");
    assert_eq!(claims.token_id, minted.metadata.id);
    assert_eq!(
        registry
            .get_token("tenant-a", &claims.token_id)
            .await
            .expect("metadata")
            .last_used_at,
        Some(1_500)
    );

    service
        .revoke("tenant-a", &claims.token_id, 1_600)
        .await
        .expect("revoke");
    let denied = service
        .authenticate(
            minted.token.expose(),
            "tenant-a",
            "verglas-data-plane",
            1_700,
        )
        .await;
    assert!(matches!(denied, Err(AuthzError::Token(_))));
}

#[tokio::test]
async fn signing_material_and_plaintext_never_enter_token_metadata() {
    let registry = MemoryAccessTokenRegistry::new();
    let service = AccessTokenService::new(
        AccessTokenSigner::new([5; 32]),
        std::sync::Arc::new(registry.clone()),
    );
    let minted = service.mint(request()).await.expect("mint");
    let serialized = serde_json::to_string(&minted.metadata).expect("serialize metadata");

    assert!(!serialized.contains(minted.token.expose()));
    assert!(!format!("{:?}", minted.token).contains(minted.token.expose()));
    assert_eq!(
        registry
            .list_tokens("tenant-a", "user-alice")
            .await
            .expect("list")
            .len(),
        1
    );
}

#[tokio::test]
async fn tampered_and_expired_tokens_fail_before_registry_use() {
    let registry = MemoryAccessTokenRegistry::new();
    let service = AccessTokenService::new(
        AccessTokenSigner::new([3; 32]),
        std::sync::Arc::new(registry.clone()),
    );
    let minted = service.mint(request()).await.expect("mint");
    let tampered = format!("{}x", minted.token.expose());

    assert!(matches!(
        service
            .authenticate(&tampered, "tenant-a", "verglas-data-plane", 1_500)
            .await,
        Err(AuthzError::Token(_))
    ));
    assert!(matches!(
        service
            .authenticate(
                minted.token.expose(),
                "tenant-a",
                "verglas-data-plane",
                2_001
            )
            .await,
        Err(AuthzError::Token(_))
    ));
    assert_eq!(
        registry
            .get_token("tenant-a", &minted.metadata.id)
            .await
            .expect("metadata")
            .last_used_at,
        None
    );
}

#[test]
fn signer_requires_a_256_bit_base64_key() {
    let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [7_u8; 32]);
    assert!(AccessTokenSigner::from_base64(&encoded).is_ok());
    assert!(AccessTokenSigner::from_base64("not-base64").is_err());
    assert!(AccessTokenSigner::from_base64("dG9vLXNob3J0").is_err());
}

#[test]
fn target_jwt_is_ed25519_signed_and_has_a_public_jwks_key() {
    let signer = TargetJwtSigner::new("tenant-jwt-1", [4; 32]).expect("signer");
    let jwt = signer
        .mint(TargetJwtRequest::new(
            "neon-postgres",
            "user/alice@example.com",
            "credential-1",
            "tenant-a",
            "database/operations",
            1_000,
            2_000,
        ))
        .expect("mint");
    let claims = signer
        .verify(jwt.expose(), "neon-postgres", "database/operations", 1_500)
        .expect("verify");
    assert_eq!(claims.subject, "user/alice@example.com");
    assert_eq!(claims.token_id, "credential-1");
    assert_eq!(claims.tenant_id, "tenant-a");
    assert_eq!(claims.database_id, "database/operations");
    let jwks = serde_json::to_value(signer.jwks()).expect("jwks");
    assert_eq!(jwks["keys"][0]["kty"], "OKP");
    assert_eq!(jwks["keys"][0]["crv"], "Ed25519");
    assert_eq!(jwks["keys"][0]["kid"], "tenant-jwt-1");
    assert!(jwks["keys"][0].get("d").is_none());
    assert!(verifying_key_from_jwk(&signer.jwks().keys[0]).is_ok());
}

#[test]
fn target_jwt_rejects_audience_database_and_signature_mismatch() {
    let signer = TargetJwtSigner::new("tenant-jwt-1", [6; 32]).expect("signer");
    let jwt = signer
        .mint(TargetJwtRequest::new(
            "neon-postgres",
            "user/alice@example.com",
            "credential-1",
            "tenant-a",
            "database/operations",
            1_000,
            2_000,
        ))
        .expect("mint");
    assert!(
        signer
            .verify(jwt.expose(), "other", "database/operations", 1_500)
            .is_err()
    );
    assert!(
        signer
            .verify(jwt.expose(), "neon-postgres", "database/other", 1_500)
            .is_err()
    );
    assert!(
        signer
            .verify(
                &format!("{}x", jwt.expose()),
                "neon-postgres",
                "database/operations",
                1_500
            )
            .is_err()
    );
}

#[test]
fn target_jwt_derived_key_id_comes_from_its_public_key() {
    let first = TargetJwtSigner::from_base64_derived(&base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        [8_u8; 32],
    ))
    .expect("first signer");
    let second = TargetJwtSigner::from_base64_derived(&base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        [8_u8; 32],
    ))
    .expect("second signer");
    assert_eq!(first.jwks().keys[0].kid, second.jwks().keys[0].kid);
    assert!(first.jwks().keys[0].kid.starts_with("ed25519-"));
}
