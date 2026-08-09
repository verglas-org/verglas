//! Persistence and hierarchy checks against an operator-supplied test Postgres.

use std::collections::BTreeSet;

use verglas_authz::{
    AccessCheck, Action, AuthorizationRepository, Grant, Principal, PrincipalKind, Resource,
    ResourceKind,
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
