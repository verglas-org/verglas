//! The one construction of the hosted Iceberg catalog.
//!
//! The catalog runs inside the ring node that serves it. There is no second
//! catalog process. How it reaches its consensus plane is the injected
//! [`ManagedCatalogTransport`] — the one seam a different deployment shape
//! would vary.

use std::sync::Arc;

use verglas_catalog::ManagedCatalogTransport;
use verglas_catalog_core::service::{
    ProjectId, ResolvedWarehouse, ServerId, WarehouseId,
    authz::Authorizer,
    storage::{S3Credential, S3Profile, StorageProfile, s3::S3AccessKeyCredential},
};

use crate::{
    AuthorizedVerglasCatalog, VerglasCatalog, metadata_store::FileIoMetadataStore,
    metadata_store::MetadataRoots,
};

/// Everything a hosted catalog needs that is not its transport or authorizer.
#[derive(Clone, Debug)]
pub struct HostedDeployment {
    /// Tenant owning the CRaft catalog groups.
    pub tenant: String,
    /// The single warehouse this deployment serves.
    pub warehouse: String,
    /// Catalog S3 storage profile for immutable table metadata.
    pub managed_s3_profile: S3Profile,
    /// Access key the catalog presents for metadata object IO.
    pub metadata_access_key_id: String,
    /// Secret key the catalog presents for metadata object IO.
    pub metadata_secret_access_key: String,
}

impl HostedDeployment {
    /// Derives this deployment's server identity from its tenant.
    ///
    /// Deterministic on purpose: the co-located topology runs one stateless
    /// catalog beside every ring node, so a per-process random identity would
    /// give peers different answers for one deployment.
    #[must_use]
    pub fn server_id(&self) -> ServerId {
        ServerId::new(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!("verglas:server:{}", self.tenant).as_bytes(),
        ))
    }

    /// Derives the stable warehouse identity from its name.
    #[must_use]
    pub fn warehouse_id(&self) -> WarehouseId {
        WarehouseId::new(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!("verglas:warehouse:{}", self.warehouse).as_bytes(),
        ))
    }

    /// Derives the stable project identity from the tenant.
    #[must_use]
    pub fn project_id(&self) -> ProjectId {
        ProjectId::from(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!("verglas:project:{}", self.tenant).as_bytes(),
        ))
    }
}

/// Assembles the authorized hosted catalog state for one deployment.
///
/// `transport` decides how the catalog reaches consensus: HTTP ingresses for a
/// standalone process, or a direct in-process binding for a catalog embedded
/// in a ring node.
///
/// # Errors
///
/// Returns an error when the metadata `FileIO` cannot be constructed from the
/// storage profile, or when the warehouse profile cannot be bound.
pub async fn hosted_catalog_state<A: Authorizer + Clone>(
    transport: Arc<dyn ManagedCatalogTransport>,
    deployment: &HostedDeployment,
    authorizer: A,
) -> Result<AuthorizedVerglasCatalog<A>, String> {
    let credential = S3Credential::AccessKey(S3AccessKeyCredential {
        access_key_id: deployment.metadata_access_key_id.clone(),
        secret_access_key: deployment.metadata_secret_access_key.clone(),
        external_id: None,
    });
    let io = deployment
        .managed_s3_profile
        .verglas_catalog_io(Some(&credential))
        .await
        .map_err(|error| format!("cannot construct metadata S3 FileIO: {error}"))?;

    // The served warehouse must exist in the profile registry before the first
    // request, or every metadata operation 424s with
    // WarehouseProfileUnavailable. One hosted warehouse per deployment, so it
    // binds its own: the metadata profile's bucket is the root and the tenant
    // doubles as the database id.
    let roots = MetadataRoots::default();
    roots
        .bind(
            &deployment.warehouse,
            &deployment.tenant,
            &deployment.managed_s3_profile.bucket,
        )
        .map_err(|error| format!("cannot bind warehouse profile: {}", error.message))?;

    let metadata = Arc::new(FileIoMetadataStore::new(Arc::new(io), roots));
    let catalog = VerglasCatalog::with_transport(transport, metadata);
    let warehouse_context = ResolvedWarehouse::for_hosted_craft(
        deployment.warehouse_id(),
        deployment.project_id(),
        deployment.warehouse.clone(),
        StorageProfile::S3(deployment.managed_s3_profile.clone()),
        true,
    );
    Ok(AuthorizedVerglasCatalog::new(
        catalog,
        authorizer,
        warehouse_context,
    ))
}

#[cfg(test)]
mod tests {
    use super::HostedDeployment;
    use verglas_catalog_core::service::storage::S3Profile;

    /// Builds a deployment whose storage coordinates are never dereferenced.
    fn deployment(tenant: &str, warehouse: &str) -> HostedDeployment {
        let profile: S3Profile = serde_json::from_str(
            r#"{"bucket":"b","region":"auto","endpoint":"http://s3.invalid","path-style-access":true,"sts-enabled":false}"#,
        )
        .expect("fixture profile");
        HostedDeployment {
            tenant: tenant.to_owned(),
            warehouse: warehouse.to_owned(),
            managed_s3_profile: profile,
            metadata_access_key_id: "k".to_owned(),
            metadata_secret_access_key: "s".to_owned(),
        }
    }

    /// Every identity a hosted deployment exposes must be a function of its
    /// names, never of the process that happens to be serving it.
    ///
    /// Four ring nodes each embed one catalog for the same deployment; if any
    /// of these were per-process, those peers would disagree about the server,
    /// warehouse, or project they collectively serve.
    #[test]
    fn hosted_identities_are_derived_from_names_not_the_process() {
        let first = deployment("tenant-a", "lite");
        let second = deployment("tenant-a", "lite");
        assert_eq!(first.server_id(), second.server_id());
        assert_eq!(first.warehouse_id(), second.warehouse_id());
        assert_eq!(first.project_id(), second.project_id());

        let other_tenant = deployment("tenant-b", "lite");
        assert_ne!(first.server_id(), other_tenant.server_id());
        assert_ne!(first.project_id(), other_tenant.project_id());

        let other_warehouse = deployment("tenant-a", "heavy");
        assert_ne!(first.warehouse_id(), other_warehouse.warehouse_id());
    }
}
