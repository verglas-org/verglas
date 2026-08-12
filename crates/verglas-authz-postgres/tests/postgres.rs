//! Persistence and hierarchy checks against an operator-supplied test Postgres.

use std::collections::BTreeSet;

use verglas_authz::{
    AccessCheck, AccessTokenMetadata, AccessTokenRegistry, Action, AuthorizationRepository, Grant,
    Principal, PrincipalKind, Resource, ResourceKind, SecretKind, SecretMetadata, SecretRepository,
};
use verglas_authz_postgres::PostgresAuthorizationRepository;

#[tokio::test]
#[ignore = "requires VERGLAS_TEST_POSTGRES_URL"]
async fn grants_survive_reconnect_and_inherit_through_resources() {
    let url = std::env::var("VERGLAS_TEST_POSTGRES_URL").expect("VERGLAS_TEST_POSTGRES_URL");
    let tenant = format!("test-{}", uuid::Uuid::new_v4());
    let repository = PostgresAuthorizationRepository::connect(&url)
        .await
        .expect("connect");
    repository
        .create_principal(Principal::new(&tenant, "job-1", PrincipalKind::Job))
        .await
        .expect("principal");
    repository
        .create_resource(Resource::new(&tenant, "db-1", ResourceKind::Database))
        .await
        .expect("database");
    repository
        .create_resource(Resource::new(&tenant, "table-1", ResourceKind::Table).with_parent("db-1"))
        .await
        .expect("table");
    repository
        .create_grant(Grant::new(
            "grant-1",
            &tenant,
            "job-1",
            "db-1",
            BTreeSet::from([Action::Query]),
        ))
        .await
        .expect("grant");
    let version = repository.policy_version(&tenant).await.expect("version");
    drop(repository);

    let reconnected = PostgresAuthorizationRepository::connect(&url)
        .await
        .expect("reconnect");
    let (grant, matched_resource) = reconnected
        .matching_grant(&AccessCheck::new(
            &tenant,
            "job-1",
            "table-1",
            Action::Query,
        ))
        .await
        .expect("check")
        .expect("matching grant");
    assert_eq!(grant.id, "grant-1");
    assert_eq!(matched_resource, "db-1");
    assert_eq!(
        reconnected.policy_version(&tenant).await.expect("version"),
        version
    );
}

#[tokio::test]
#[ignore = "requires VERGLAS_TEST_POSTGRES_URL"]
async fn encrypted_secret_versions_survive_reconnect_without_plaintext_metadata() {
    let url = std::env::var("VERGLAS_TEST_POSTGRES_URL").expect("VERGLAS_TEST_POSTGRES_URL");
    let tenant = format!("test-{}", uuid::Uuid::new_v4());
    let repository = PostgresAuthorizationRepository::connect(&url)
        .await
        .expect("connect");
    repository
        .create_resource(Resource::new(&tenant, "secret-1", ResourceKind::Secret))
        .await
        .expect("resource");
    repository
        .create(
            SecretMetadata {
                tenant_id: tenant.clone(),
                id: "secret-1".to_owned(),
                kind: SecretKind::S3,
                scope: "s3://bucket/team".to_owned(),
                current_version: 1,
                resource_kind: ResourceKind::Secret,
            },
            b"sealed-v1".to_vec(),
        )
        .await
        .expect("secret");
    repository
        .replace(&tenant, "secret-1", b"sealed-v2".to_vec())
        .await
        .expect("replace");
    drop(repository);

    let reconnected = PostgresAuthorizationRepository::connect(&url)
        .await
        .expect("reconnect");
    let metadata = reconnected
        .get(&tenant, "secret-1")
        .await
        .expect("metadata");
    assert_eq!(metadata.current_version, 2);
    let stored = reconnected
        .candidates(&tenant, SecretKind::S3)
        .await
        .expect("candidates");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].ciphertext, b"sealed-v2");
    assert!(
        !serde_json::to_string(&metadata)
            .expect("json")
            .contains("sealed")
    );
}

#[tokio::test]
#[ignore = "requires VERGLAS_TEST_POSTGRES_URL"]
async fn token_registry_persists_metadata_and_revocation_without_bearer_material() {
    let url = std::env::var("VERGLAS_TEST_POSTGRES_URL").expect("VERGLAS_TEST_POSTGRES_URL");
    let tenant = format!("test-{}", uuid::Uuid::new_v4());
    let repository = PostgresAuthorizationRepository::connect(&url)
        .await
        .expect("connect");
    for id in ["user-alice", "token/cli-alice"] {
        repository
            .create_principal(Principal::new(&tenant, id, PrincipalKind::ServiceAccount))
            .await
            .expect("principal");
    }
    let metadata = AccessTokenMetadata {
        id: "cli-token-1".to_owned(),
        tenant_id: tenant.clone(),
        principal_id: "token/cli-alice".to_owned(),
        parent_principal_id: "user-alice".to_owned(),
        name: "Local CLI".to_owned(),
        audience: "verglas-data-plane".to_owned(),
        policy_version: 2,
        run_id: None,
        created_at: 1_000,
        expires_at: 2_000,
        last_used_at: None,
        revoked_at: None,
    };
    repository
        .create_token(metadata)
        .await
        .expect("create metadata");
    repository
        .record_token_use(&tenant, "cli-token-1", 1_500)
        .await
        .expect("use");
    let revoked = repository
        .revoke_token(&tenant, "cli-token-1", 1_600)
        .await
        .expect("revoke");
    assert_eq!(revoked.last_used_at, Some(1_500));
    assert_eq!(revoked.revoked_at, Some(1_600));
    assert_eq!(
        repository
            .list_tokens(&tenant, "user-alice")
            .await
            .expect("list")
            .len(),
        1
    );
}
