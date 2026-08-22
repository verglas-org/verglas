use verglas_catalog_authz::{CredentialAuthzConfig, VerglasAuthorizer};
use verglas_catalog_core::{
    api::RequestMetadataTestBuilder,
    service::{
        ServerId,
        authn::UserId,
        authz::{Authorizer, CatalogUserAction, UserOrRole},
    },
};

#[tokio::test]
async fn caller_cannot_select_a_different_principal_for_authorization() {
    let authorizer = VerglasAuthorizer::try_new(
        ServerId::new_random(),
        CredentialAuthzConfig {
            issuer: "https://control.example.test".to_owned(),
            jwks: r#"{"keys":[]}"#.to_owned(),
            tenant_id: "tenant-a".to_owned(),
        },
    )
    .expect("authorizer");
    let metadata = RequestMetadataTestBuilder::builder()
        .verglas_bearer_token("opaque-caller-token")
        .build();
    let selected_user = UserId::new_unchecked("oidc", "bob");
    let selected = UserOrRole::User(selected_user.clone());
    let result = authorizer
        .are_allowed_user_actions_impl(
            &metadata,
            Some(&selected),
            &[(&selected_user, CatalogUserAction::Read)],
        )
        .await;

    assert!(
        result.is_err(),
        "caller-selected principals must be rejected"
    );
}
