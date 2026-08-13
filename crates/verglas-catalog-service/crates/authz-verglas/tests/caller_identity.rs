use lakekeeper::{
    api::RequestMetadataTestBuilder,
    service::{
        ServerId,
        authn::UserId,
        authz::{Authorizer, CatalogUserAction, UserOrRole},
    },
};
use lakekeeper_authz_verglas::{VerglasAuthorizer, VerglasAuthzConfig};

#[tokio::test]
async fn caller_cannot_select_a_different_principal_for_authorization() {
    let credentials = tempfile::NamedTempFile::new().expect("credential file");
    let authorizer = VerglasAuthorizer::try_new(
        ServerId::new_random(),
        VerglasAuthzConfig {
            endpoint: url::Url::parse("http://127.0.0.1:9/").expect("endpoint"),
            workload_credential_file: credentials.path().to_owned(),
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
