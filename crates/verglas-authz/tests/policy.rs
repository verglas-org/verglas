//! Universal authorization behavior independent of a storage backend.

use std::collections::BTreeSet;

use verglas_authz::{
    AccessCheck, Action, Authorizer, Grant, GrantDelegation, GrantRevocation, MemoryAuthorizer,
    Principal, PrincipalKind, Resource, ResourceKind, ScopedTokenClaims,
};

/// Builds a deterministic set without depending on insertion order.
fn actions(values: impl IntoIterator<Item = Action>) -> BTreeSet<Action> {
    values.into_iter().collect()
}

#[tokio::test]
async fn delegation_requires_pass_grants_and_every_delegated_action() {
    let authorizer = MemoryAuthorizer::new();
    for id in ["owner", "job"] {
        authorizer
            .create_principal(Principal::new(
                "tenant-a",
                id,
                if id == "owner" {
                    PrincipalKind::User
                } else {
                    PrincipalKind::Job
                },
            ))
            .await
            .expect("create principal");
    }
    authorizer
        .create_resource(Resource::new("tenant-a", "table-1", ResourceKind::Table))
        .await
        .expect("create table");
    let requested = || {
        Grant::new(
            "delegated-query",
            "tenant-a",
            "job",
            "table-1",
            actions([Action::Query]),
        )
    };

    let denied = authorizer
        .delegate_grant(GrantDelegation::new("owner", requested()))
        .await;
    assert!(matches!(
        denied,
        Err(verglas_authz::AuthzError::Forbidden(_))
    ));

    authorizer
        .create_grant(Grant::new(
            "owner-capabilities",
            "tenant-a",
            "owner",
            "table-1",
            actions([Action::Query, Action::PassGrants, Action::ManageGrants]),
        ))
        .await
        .expect("grant owner capabilities");
    authorizer
        .delegate_grant(GrantDelegation::new("owner", requested()))
        .await
        .expect("delegate query");
    assert!(
        authorizer
            .check(AccessCheck::new(
                "tenant-a",
                "job",
                "table-1",
                Action::Query
            ))
            .await
            .expect("check delegated query")
            .allowed
    );
    authorizer
        .revoke_grant(GrantRevocation {
            tenant_id: "tenant-a".to_owned(),
            actor_principal_id: "owner".to_owned(),
            grant_id: "delegated-query".to_owned(),
        })
        .await
        .expect("revoke delegated query");
    assert!(
        !authorizer
            .check(AccessCheck::new(
                "tenant-a",
                "job",
                "table-1",
                Action::Query
            ))
            .await
            .expect("check revoked query")
            .allowed
    );
}

#[tokio::test]
async fn exact_and_parent_grants_allow_while_absence_denies() {
    let authorizer = MemoryAuthorizer::new();
    authorizer
        .create_principal(Principal::new("tenant-a", "worker-1", PrincipalKind::Job))
        .await
        .expect("create principal");
    authorizer
        .create_resource(Resource::new(
            "tenant-a",
            "database-1",
            ResourceKind::Database,
        ))
        .await
        .expect("create database");
    authorizer
        .create_resource(
            Resource::new("tenant-a", "table-1", ResourceKind::Table).with_parent("database-1"),
        )
        .await
        .expect("create table");

    let denied = authorizer
        .check(AccessCheck::new(
            "tenant-a",
            "worker-1",
            "table-1",
            Action::Query,
        ))
        .await
        .expect("deny check");
    assert!(!denied.allowed);
    assert_eq!(denied.reason.as_str(), "no_matching_grant");

    authorizer
        .create_grant(Grant::new(
            "grant-1",
            "tenant-a",
            "worker-1",
            "database-1",
            actions([Action::Query]),
        ))
        .await
        .expect("create grant");

    let inherited = authorizer
        .check(AccessCheck::new(
            "tenant-a",
            "worker-1",
            "table-1",
            Action::Query,
        ))
        .await
        .expect("inherited check");
    assert!(inherited.allowed);
    assert_eq!(inherited.reason.as_str(), "inherited_grant");
    assert_eq!(inherited.grant_id.as_deref(), Some("grant-1"));

    let write = authorizer
        .check(AccessCheck::new(
            "tenant-a",
            "worker-1",
            "table-1",
            Action::Append,
        ))
        .await
        .expect("write check");
    assert!(!write.allowed);
}

#[tokio::test]
async fn tenant_boundaries_are_enforced_before_policy_evaluation() {
    let authorizer = MemoryAuthorizer::new();
    authorizer
        .create_principal(Principal::new("tenant-a", "user-1", PrincipalKind::User))
        .await
        .expect("create principal");
    authorizer
        .create_resource(Resource::new("tenant-b", "table-1", ResourceKind::Table))
        .await
        .expect("create resource");

    let decision = authorizer
        .check(AccessCheck::new(
            "tenant-a",
            "user-1",
            "table-1",
            Action::Query,
        ))
        .await
        .expect("cross tenant check");
    assert!(!decision.allowed);
    assert_eq!(decision.reason.as_str(), "tenant_mismatch");
}

#[tokio::test]
async fn one_grant_owns_each_principal_resource_tuple_set() {
    let authorizer = MemoryAuthorizer::new();
    authorizer
        .create_principal(Principal::new("tenant-a", "job-1", PrincipalKind::Job))
        .await
        .expect("create principal");
    authorizer
        .create_resource(Resource::new("tenant-a", "table-1", ResourceKind::Table))
        .await
        .expect("create resource");
    authorizer
        .create_grant(Grant::new(
            "grant-1",
            "tenant-a",
            "job-1",
            "table-1",
            actions([Action::Query]),
        ))
        .await
        .expect("create grant");
    let duplicate = authorizer
        .create_grant(Grant::new(
            "grant-2",
            "tenant-a",
            "job-1",
            "table-1",
            actions([Action::Append]),
        ))
        .await;
    assert!(duplicate.is_err());
}

#[test]
fn scoped_claims_validate_audience_tenant_and_lifetime() {
    let claims = ScopedTokenClaims::new("tenant-a", "job-1", "data-plane-west", 12, 1_000, 1_100)
        .with_run("run-9");
    assert!(
        claims
            .validate("tenant-a", "data-plane-west", 1_050)
            .is_ok()
    );
    assert!(
        claims
            .validate("tenant-b", "data-plane-west", 1_050)
            .is_err()
    );
    assert!(
        claims
            .validate("tenant-a", "data-plane-east", 1_050)
            .is_err()
    );
    assert!(
        claims
            .validate("tenant-a", "data-plane-west", 1_101)
            .is_err()
    );
}
