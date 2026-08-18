//! Builds and serves the hosted Iceberg API over Verglas `CRaft` catalog groups.

use lakekeeper::axum::{
    Router,
    http::header,
    response::{Html, IntoResponse},
    routing::get,
};

const CATALOG_OPENAPI_YAML: &str =
    include_str!("../../../docs/docs/api/rest-catalog-open-api.yaml");

/// Builds documentation routes for the sole hosted catalog API.
fn documentation_router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/api-docs/catalog/v1/openapi.json", get(catalog_openapi))
        .route("/swagger-ui", get(swagger_ui))
}

/// Serves the canonical Iceberg REST catalog specification at its mounted paths.
async fn catalog_openapi() -> impl IntoResponse {
    let mut document: serde_json::Value =
        serde_norway::from_str(CATALOG_OPENAPI_YAML).expect("checked-in catalog OpenAPI is valid");
    let paths = document
        .get_mut("paths")
        .and_then(serde_json::Value::as_object_mut)
        .expect("checked-in catalog OpenAPI has paths");
    let mounted_paths = std::mem::take(paths)
        .into_iter()
        .map(|(path, item)| (format!("/catalog{path}"), item))
        .collect();
    *paths = mounted_paths;
    (
        [(header::CONTENT_TYPE, "application/json")],
        lakekeeper::axum::Json(document),
    )
}

/// Serves a small self-contained Swagger UI entry point bound only to the catalog spec.
async fn swagger_ui() -> Html<&'static str> {
    Html(
        r##"<!doctype html><title>Verglas catalog API</title><main id="swagger-ui"></main><script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script><script>SwaggerUIBundle({url:"/api-docs/catalog/v1/openapi.json",dom_id:"#swagger-ui"});</script>"##,
    )
}

/// Serves a `CRaft` warehouse through its ordered ingress set and S3 metadata store.
pub(crate) async fn serve_craft(
    bind_addr: std::net::SocketAddr,
    endpoints: Vec<String>,
    tenant: String,
    warehouse: String,
    managed_s3_profile: String,
) -> anyhow::Result<()> {
    if endpoints.is_empty() || endpoints.iter().any(|endpoint| endpoint.trim().is_empty()) {
        anyhow::bail!("--endpoints must contain one or more nonempty ingress URLs");
    }

    let profile: lakekeeper::service::storage::S3Profile =
        serde_json::from_str(&managed_s3_profile)
            .map_err(|error| anyhow::anyhow!("invalid VERGLAS_MANAGED_S3_PROFILE: {error}"))?;
    let storage_profile = lakekeeper::service::storage::StorageProfile::S3(profile.clone());
    let metadata_access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .map_err(|_| anyhow::anyhow!("AWS_ACCESS_KEY_ID is required for metadata S3 access"))?;
    let metadata_secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .map_err(|_| anyhow::anyhow!("AWS_SECRET_ACCESS_KEY is required for metadata S3 access"))?;
    let metadata_credential = metadata_s3_credential(metadata_access_key, metadata_secret_key);
    let io = profile
        .lakekeeper_io(Some(&metadata_credential))
        .await
        .map_err(|error| anyhow::anyhow!("cannot construct metadata S3 FileIO: {error}"))?;
    let roots = lakekeeper_storage_verglas::metadata_store::MetadataRoots::default();
    // The served warehouse must exist in the profile registry before the
    // first request, or every metadata operation 424s with
    // WarehouseProfileUnavailable. serve-craft hosts exactly one warehouse,
    // so it binds its own: the metadata profile's bucket is the root, and the
    // tenant doubles as the database id (one hosted database per tenant on
    // this topology).
    roots
        .bind(&warehouse, &tenant, &profile.bucket)
        .map_err(|error| anyhow::anyhow!("cannot bind warehouse profile: {}", error.message))?;
    let metadata = std::sync::Arc::new(
        lakekeeper_storage_verglas::metadata_store::FileIoMetadataStore::new(
            std::sync::Arc::new(io),
            roots,
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
    let router = documentation_router().nest("/catalog/v1", hosted_router);

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

/// Builds the explicit deployment-scoped credential for immutable metadata IO.
fn metadata_s3_credential(
    access_key_id: String,
    secret_access_key: String,
) -> lakekeeper::service::storage::S3Credential {
    lakekeeper::service::storage::S3Credential::AccessKey(
        lakekeeper::service::storage::s3::S3AccessKeyCredential {
            access_key_id,
            secret_access_key,
            external_id: None,
        },
    )
}

#[cfg(test)]
mod tests {
    //! Constructor tests for the hosted immutable metadata authority.

    use lakekeeper::axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    /// Hosted metadata uses the explicit scoped identity supplied by its deployment.
    #[test]
    fn hosted_metadata_uses_the_explicit_deployment_credential() {
        let credential = super::metadata_s3_credential(
            "warehouse-key".to_owned(),
            "warehouse-secret".to_owned(),
        );
        assert!(matches!(
            credential,
            lakekeeper::service::storage::S3Credential::AccessKey(
                lakekeeper::service::storage::s3::S3AccessKeyCredential {
                    access_key_id,
                    secret_access_key,
                    external_id: None,
                }
            ) if access_key_id == "warehouse-key" && secret_access_key == "warehouse-secret"
        ));
    }

    /// Documentation advertises only catalog paths before the hosted auth layer is applied.
    #[tokio::test]
    async fn hosted_documentation_routes_are_complete_without_catalog_state() {
        let response = super::documentation_router::<()>()
            .oneshot(
                Request::builder()
                    .uri("/api-docs/catalog/v1/openapi.json")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("documentation response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("OpenAPI body");
        let document: serde_json::Value = serde_json::from_slice(&body).expect("OpenAPI JSON");
        let paths = document["paths"].as_object().expect("OpenAPI paths");
        assert!(!paths.is_empty());
        assert!(paths.keys().all(|path| path.starts_with("/catalog/v1")));
        assert!(paths.keys().all(|path| !path.contains("management")));
        let swagger = super::documentation_router::<()>()
            .oneshot(
                Request::builder()
                    .uri("/swagger-ui")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("swagger response");
        assert_eq!(swagger.status(), StatusCode::OK);
    }
}
