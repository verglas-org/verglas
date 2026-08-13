//! Builds and serves the hosted Iceberg API over Verglas `CRaft` catalog groups.

/// Serves a `CRaft` warehouse through its ordered ingress set and S3 metadata store.
pub(crate) async fn serve_craft(
    bind_addr: std::net::SocketAddr,
    endpoints: Vec<String>,
    tenant: String,
    warehouse: String,
    metadata_s3_profile: String,
) -> anyhow::Result<()> {
    if endpoints.is_empty() || endpoints.iter().any(|endpoint| endpoint.trim().is_empty()) {
        anyhow::bail!("--endpoints must contain one or more nonempty ingress URLs");
    }

    let profile: lakekeeper::service::storage::S3Profile =
        serde_json::from_str(&metadata_s3_profile)
            .map_err(|error| anyhow::anyhow!("invalid VERGLAS_METADATA_S3_PROFILE: {error}"))?;
    let catalog_root = profile
        .base_location()
        .map_err(|error| anyhow::anyhow!("invalid metadata S3 base location: {error}"))?
        .to_string();
    let storage_profile = lakekeeper::service::storage::StorageProfile::S3(profile.clone());
    let metadata_credential = metadata_s3_credential();
    let io = profile
        .lakekeeper_io(Some(&metadata_credential))
        .await
        .map_err(|error| anyhow::anyhow!("cannot construct metadata S3 FileIO: {error}"))?;
    let metadata = std::sync::Arc::new(
        lakekeeper_storage_verglas::metadata_store::FileIoMetadataStore::new(
            std::sync::Arc::new(io),
            catalog_root,
        ),
    );
    let catalog = lakekeeper_storage_verglas::VerglasCatalog::from_parts(
        endpoints,
        tenant.clone(),
        warehouse.clone(),
        metadata,
    )?;
    let server_id = lakekeeper::service::ServerId::new_random();
    let authorizer =
        lakekeeper_authz_verglas::new_authorizer_from_default_config(server_id).await?;
    let warehouse_id = lakekeeper::service::WarehouseId::new(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("verglas:warehouse:{warehouse}").as_bytes(),
    ));
    let project_id = lakekeeper::service::ProjectId::from(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("verglas:project:{tenant}").as_bytes(),
    ));
    let warehouse_context = lakekeeper::service::ResolvedWarehouse::for_hosted_craft(
        warehouse_id,
        project_id,
        warehouse,
        storage_profile,
        true,
    );
    let state = lakekeeper_storage_verglas::AuthorizedVerglasCatalog::new(
        catalog,
        authorizer.clone(),
        warehouse_context,
    );
    let hosted_router = lakekeeper::api::iceberg::v1::new_v1_hosted_router::<
        lakekeeper_storage_verglas::AuthorizedVerglasCatalog<_>,
        lakekeeper_storage_verglas::AuthorizedVerglasCatalog<_>,
    >();
    let router = lakekeeper::axum::Router::new().nest("/catalog/v1", hosted_router);

    lakekeeper::serve::serve_hosted(
        lakekeeper::serve::HostedServeConfiguration::builder()
            .bind_addr(bind_addr)
            .router(router)
            .state(state)
            .authorizer(authorizer)
            .build(),
    )
    .await
}

/// Selects the deployment-scoped AWS identity chain for immutable metadata IO.
fn metadata_s3_credential() -> lakekeeper::service::storage::S3Credential {
    lakekeeper::service::storage::S3Credential::AwsSystemIdentity(
        lakekeeper::service::storage::s3::S3AwsSystemIdentityCredential { external_id: None },
    )
}

#[cfg(test)]
mod tests {
    //! Constructor tests for the hosted immutable metadata authority.

    /// Hosted metadata uses the scoped ambient identity supplied by its deployment.
    #[test]
    fn hosted_metadata_uses_the_system_identity_credential_chain() {
        assert!(matches!(
            super::metadata_s3_credential(),
            lakekeeper::service::storage::S3Credential::AwsSystemIdentity(_)
        ));
    }
}
