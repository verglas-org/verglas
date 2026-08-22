//! CRaft-backed Catalog catalog storage primitives.
//!
//! This crate owns the boundary from Catalog domain records to the
//! tenant-rooted Verglas consensus catalog. It deliberately has no SQL store.

use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};
use verglas_catalog::{
    ManagedCatalogClient, ManagedCatalogRequest, ManagedCatalogResponse, ManagedCatalogTransport,
};
use verglas_consensus::{CatalogAction, CatalogBatch, CatalogEntity, CatalogRequirement};

pub mod authorized;
pub mod domain;
mod hosted;
pub mod hosted_deployment;
pub(crate) mod idempotency;
pub mod metadata_store;
mod table_commit_queue;
mod transaction;

pub use authorized::AuthorizedVerglasCatalog;
pub use transaction::VerglasTransaction;

impl verglas_catalog_core::api::ThreadSafe for VerglasCatalog {}

/// The immutable object authority used by hosted Iceberg table and view routes.
///
/// Implementations must verify every read object identity and publish only a
/// newly immutable metadata object. They never own catalog names or pointers:
/// the subsequent CRaft batch is the only authority for those transitions.
#[async_trait::async_trait]
pub trait ImmutableMetadataStore: Send + Sync + 'static {
    /// Confirms the routed warehouse has an active storage binding.
    fn ensure_warehouse(&self, warehouse: &str) -> Result<(), MetadataStoreError>;
    /// Returns the durable database resource identity for warehouse authorization.
    fn warehouse_database_id(&self, warehouse: &str) -> Result<String, MetadataStoreError>;
    /// Allocates a deterministic catalog-managed table root for an omitted client location.
    fn table_root(
        &self,
        warehouse: &str,
        request_id: u128,
        namespace: &[String],
        name: &str,
    ) -> Result<String, MetadataStoreError>;
    /// Allocates a deterministic catalog-managed view root for an omitted client location.
    fn view_root(
        &self,
        warehouse: &str,
        request_id: u128,
        namespace: &[String],
        name: &str,
    ) -> Result<String, MetadataStoreError>;
    /// Rejects a client-supplied location that escapes its registered warehouse root.
    fn validate_location(&self, warehouse: &str, location: &str) -> Result<(), MetadataStoreError>;
    /// Loads and verifies the immutable metadata object at a table location.
    async fn load_table(
        &self,
        location: &str,
    ) -> Result<verglas_catalog_core::iceberg::spec::TableMetadataRef, MetadataStoreError>;
    /// Loads and verifies the immutable metadata object at a view location.
    async fn load_view(
        &self,
        location: &str,
    ) -> Result<verglas_catalog_core::iceberg::spec::ViewMetadataRef, MetadataStoreError>;
    /// Publishes a table creation metadata object and returns its immutable identity.
    async fn create_table(
        &self,
        request_id: u128,
        location: &str,
        request: &verglas_catalog_core::api::CreateTableRequest,
    ) -> Result<
        (
            String,
            verglas_catalog_core::iceberg::spec::TableMetadataRef,
        ),
        MetadataStoreError,
    >;
    /// Applies a validated table commit and publishes a new immutable metadata object.
    async fn commit_table(
        &self,
        request_id: u128,
        location: &str,
        request: &verglas_catalog_core::api::CommitTableRequest,
    ) -> Result<
        (
            String,
            verglas_catalog_core::iceberg::spec::TableMetadataRef,
        ),
        MetadataStoreError,
    >;
    /// Atomically publishes every metadata object for an Iceberg table transaction.
    async fn commit_tables(
        &self,
        request_id: u128,
        changes: &[(String, verglas_catalog_core::api::CommitTableRequest)],
    ) -> Result<
        Vec<(
            String,
            verglas_catalog_core::iceberg::spec::TableMetadataRef,
        )>,
        MetadataStoreError,
    >;
    /// Publishes a view creation metadata object and returns its immutable identity.
    async fn create_view(
        &self,
        request_id: u128,
        location: &str,
        request: &verglas_catalog_core::api::CreateViewRequest,
    ) -> Result<(String, verglas_catalog_core::iceberg::spec::ViewMetadataRef), MetadataStoreError>;
    /// Applies a validated view commit and publishes a new immutable metadata object.
    async fn commit_view(
        &self,
        request_id: u128,
        location: &str,
        request: &verglas_catalog_core::api::CommitViewRequest,
    ) -> Result<(String, verglas_catalog_core::iceberg::spec::ViewMetadataRef), MetadataStoreError>;
}

/// Failure from the required immutable metadata authority.
#[derive(Debug, thiserror::Error)]
#[error("immutable metadata operation failed: {message}")]
pub struct MetadataStoreError {
    /// Safe diagnostic detail returned by the metadata authority.
    pub message: String,
}

/// Composable persistence contract for the hosted Iceberg warehouse surface.
///
/// Catalog REST handlers can depend on this contract without inheriting
/// management, authorization, task, or SQL storage operations.
#[async_trait::async_trait]
pub trait HostedIcebergStore: Clone + Send + Sync + 'static {
    /// Reads a warehouse document by its stable identifier.
    async fn warehouse(
        &self,
        id: &str,
    ) -> Result<Option<domain::WarehouseDocument>, VerglasCatalogError>;
    /// Reads a namespace document by its stable identifier.
    async fn namespace(
        &self,
        id: &str,
    ) -> Result<Option<domain::NamespaceDocument>, VerglasCatalogError>;
    /// Reads a table document by its stable identifier.
    async fn table(&self, id: &str) -> Result<Option<domain::TableDocument>, VerglasCatalogError>;
    /// Reads a view document by its stable identifier.
    async fn view(&self, id: &str) -> Result<Option<domain::ViewDocument>, VerglasCatalogError>;
    /// Starts one atomic hosted-Iceberg mutation transaction.
    fn transaction(&self, request_id: u128) -> VerglasTransaction;
}

/// One Catalog storage handle bound to a single tenant and warehouse group.
#[derive(Clone)]
pub struct VerglasCatalog {
    /// How this adapter reaches the authoritative plane. A trait object so a
    /// process that already hosts the plane can serve catalog requests
    /// in-process instead of over HTTP to itself.
    client: Arc<dyn ManagedCatalogTransport>,
    metadata: Arc<dyn ImmutableMetadataStore>,
}

impl std::fmt::Debug for VerglasCatalog {
    /// Formats the handle without exposing ingress or tenant credentials.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("VerglasCatalog")
    }
}

#[async_trait::async_trait]
impl verglas_catalog_core::service::health::HealthExt for VerglasCatalog {
    /// Reports the CRaft catalog adapter as ready; each request still fences at the ingress.
    async fn health(&self) -> Vec<verglas_catalog_core::service::health::Health> {
        vec![verglas_catalog_core::service::health::Health::now(
            "verglas-craft-catalog",
            verglas_catalog_core::service::health::HealthStatus::Healthy,
        )]
    }

    /// Leaves health stateless because CRaft does not use a local connection pool.
    async fn update_health(&self) {}
}

impl VerglasCatalog {
    /// Creates a handle that routes through an ordered nonempty CRaft ingress set.
    pub fn with_ingresses<M: ImmutableMetadataStore>(
        ingresses: impl IntoIterator<Item = String>,
        tenant: impl Into<String>,
        warehouse: impl Into<String>,
        metadata: M,
    ) -> Result<Self, VerglasCatalogError> {
        Ok(Self {
            client: Arc::new(ManagedCatalogClient::with_ingresses(
                ingresses, tenant, warehouse,
            )?),
            metadata: Arc::new(metadata),
        })
    }

    /// Creates a handle over an explicit transport.
    ///
    /// The co-located topology uses this to bind the catalog directly to the
    /// consensus plane already running in the same process, which removes an
    /// HTTP hop per catalog operation.
    pub fn with_transport(
        transport: Arc<dyn ManagedCatalogTransport>,
        metadata: Arc<dyn ImmutableMetadataStore>,
    ) -> Self {
        Self {
            client: transport,
            metadata,
        }
    }

    /// Creates a handle from a shared production metadata authority.
    pub fn from_parts(
        ingresses: impl IntoIterator<Item = String>,
        tenant: impl Into<String>,
        warehouse: impl Into<String>,
        metadata: Arc<dyn ImmutableMetadataStore>,
    ) -> Result<Self, VerglasCatalogError> {
        Ok(Self {
            client: Arc::new(ManagedCatalogClient::with_ingresses(
                ingresses, tenant, warehouse,
            )?),
            metadata,
        })
    }

    /// Returns the required immutable metadata authority for hosted table and view routes.
    pub fn metadata(&self) -> &Arc<dyn ImmutableMetadataStore> {
        &self.metadata
    }

    /// Returns the authoritative warehouse route used by every CRaft request.
    pub fn warehouse(&self) -> &str {
        self.client.warehouse()
    }

    /// Reads one typed domain record after the consensus group's linearizable read fence.
    pub async fn read<T: DeserializeOwned>(
        &self,
        entity: CatalogEntity,
        id: &str,
    ) -> Result<Option<T>, VerglasCatalogError> {
        let response = self
            .client
            .execute(&ManagedCatalogRequest::Record {
                entity,
                id: id.to_owned(),
            })
            .await?;
        let document = match response {
            ManagedCatalogResponse::Record(document) => document,
            _ => return Err(VerglasCatalogError::WrongResponse),
        };
        document
            .map(|value| serde_json::from_str(&value).map_err(VerglasCatalogError::Decode))
            .transpose()
    }

    /// Lists one typed domain collection after the consensus group's linearizable read fence.
    pub async fn list<T: DeserializeOwned>(
        &self,
        entity: CatalogEntity,
    ) -> Result<Vec<(String, T)>, VerglasCatalogError> {
        let response = self
            .client
            .execute(&ManagedCatalogRequest::Records { entity })
            .await?;
        let records = match response {
            ManagedCatalogResponse::Records(records) => records,
            _ => return Err(VerglasCatalogError::WrongResponse),
        };
        records
            .into_iter()
            .map(|(id, document)| {
                serde_json::from_str(&document)
                    .map(|record| (id, record))
                    .map_err(VerglasCatalogError::Decode)
            })
            .collect()
    }

    /// Atomically writes a typed domain record through the warehouse consensus group.
    pub async fn create<T: Serialize>(
        &self,
        request_id: u128,
        entity: CatalogEntity,
        id: String,
        record: &T,
    ) -> Result<(), VerglasCatalogError> {
        let document = serde_json::to_string(record).map_err(VerglasCatalogError::Encode)?;
        let batch = CatalogBatch::new(
            vec![CatalogRequirement::RecordAbsent {
                entity,
                id: id.clone(),
            }],
            vec![CatalogAction::PutRecord {
                entity,
                id,
                document,
            }],
        )
        .map_err(|_| VerglasCatalogError::InvalidBatch)?;
        self.commit(request_id, batch).await
    }

    /// Commits one prevalidated atomic domain transaction through CRaft.
    pub async fn commit(
        &self,
        request_id: u128,
        batch: CatalogBatch,
    ) -> Result<(), VerglasCatalogError> {
        match self
            .client
            .execute(&ManagedCatalogRequest::Commit { request_id, batch })
            .await?
        {
            ManagedCatalogResponse::Applied(_) => Ok(()),
            _ => Err(VerglasCatalogError::WrongResponse),
        }
    }

    /// Starts one buffered deterministic transaction for a Catalog handler.
    pub fn transaction(&self, request_id: u128) -> VerglasTransaction {
        VerglasTransaction::new(self.clone(), request_id)
    }
}

#[async_trait::async_trait]
impl HostedIcebergStore for VerglasCatalog {
    /// Reads a CRaft-owned warehouse declaration.
    async fn warehouse(
        &self,
        id: &str,
    ) -> Result<Option<domain::WarehouseDocument>, VerglasCatalogError> {
        self.read(CatalogEntity::Warehouse, id).await
    }

    /// Reads a CRaft-owned namespace declaration.
    async fn namespace(
        &self,
        id: &str,
    ) -> Result<Option<domain::NamespaceDocument>, VerglasCatalogError> {
        self.read(CatalogEntity::Namespace, id).await
    }

    /// Reads a CRaft-owned Iceberg table declaration.
    async fn table(&self, id: &str) -> Result<Option<domain::TableDocument>, VerglasCatalogError> {
        self.read(CatalogEntity::Table, id).await
    }

    /// Reads a CRaft-owned Iceberg view declaration.
    async fn view(&self, id: &str) -> Result<Option<domain::ViewDocument>, VerglasCatalogError> {
        self.read(CatalogEntity::View, id).await
    }

    /// Starts one CRaft-backed atomic hosted-Iceberg mutation transaction.
    fn transaction(&self, request_id: u128) -> VerglasTransaction {
        VerglasCatalog::transaction(self, request_id)
    }
}

/// A failure at the sole Catalog-to-CRaft catalog authority boundary.
#[derive(Debug, thiserror::Error)]
pub enum VerglasCatalogError {
    /// The CRaft ingress rejected or could not serve the request.
    #[error(transparent)]
    Client(#[from] verglas_catalog::ManagedCatalogError),
    /// A persisted document did not match the requested Catalog wire type.
    #[error("invalid consensus catalog document: {0}")]
    Decode(serde_json::Error),
    /// A Catalog wire object could not be made into a durable document.
    #[error("cannot encode consensus catalog document: {0}")]
    Encode(serde_json::Error),
    /// The request could not form a valid deterministic catalog transaction.
    #[error("invalid consensus catalog transaction")]
    InvalidBatch,
    /// CRaft rejected an optimistic requirement or an idempotency key was reused with different input.
    #[error("catalog mutation conflicts with durable state")]
    IdempotencyConflict,
    /// The ingress returned a response for another catalog operation.
    #[error("unexpected consensus catalog response")]
    WrongResponse,
}

impl VerglasCatalogError {
    /// Returns whether the error is a final typed conflict rather than an unavailable ingress.
    pub(crate) fn is_conflict(&self) -> bool {
        matches!(
            self,
            Self::Client(verglas_catalog::ManagedCatalogError::Conflict)
                | Self::IdempotencyConflict
        )
    }
}

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests {
    //! Unit tests for typed durable-record decoding.

    use serde::Deserialize;

    use super::VerglasCatalogError;

    /// A representative Catalog wire record stored as a consensus JSON document.
    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct WarehouseRecord {
        name: String,
    }

    /// The adapter must serve a catalog read over a transport that is not
    /// HTTP, with no ingress URLs involved.
    ///
    /// This is what lets one stateless catalog run inside the same process as
    /// the consensus plane it reads, instead of making an HTTP request to its
    /// own address space.
    #[tokio::test]
    async fn the_adapter_reads_over_a_non_http_transport() {
        use std::sync::Arc;
        use verglas_catalog::{
            ManagedCatalogError, ManagedCatalogRequest, ManagedCatalogResponse,
            ManagedCatalogTransport,
        };
        use verglas_consensus::CatalogEntity;

        /// Answers directly, the way an in-process plane binding would.
        struct InProcessTransport;

        #[async_trait::async_trait]
        impl ManagedCatalogTransport for InProcessTransport {
            fn warehouse(&self) -> &str {
                "lite"
            }

            async fn execute(
                &self,
                request: &ManagedCatalogRequest,
            ) -> Result<ManagedCatalogResponse, ManagedCatalogError> {
                match request {
                    ManagedCatalogRequest::Record { entity, id }
                        if *entity == CatalogEntity::Warehouse && id == "warehouse/default" =>
                    {
                        Ok(ManagedCatalogResponse::Record(Some(
                            r#"{"name":"default"}"#.to_owned(),
                        )))
                    }
                    _ => Ok(ManagedCatalogResponse::Record(None)),
                }
            }
        }

        let catalog = super::VerglasCatalog::with_transport(
            Arc::new(InProcessTransport),
            Arc::new(crate::test_support::UnusedMetadataStore),
        );
        assert_eq!(catalog.warehouse(), "lite");
        let record: Option<WarehouseRecord> = catalog
            .read(CatalogEntity::Warehouse, "warehouse/default")
            .await
            .expect("read over the in-process transport");
        assert_eq!(
            record,
            Some(WarehouseRecord {
                name: "default".to_owned()
            })
        );
    }

    /// A document returned by the transport must decode into the requested domain type.
    #[test]
    fn durable_record_document_decodes_to_requested_type() {
        let record: WarehouseRecord = serde_json::from_str(r#"{"name":"default"}"#)
            .map_err(VerglasCatalogError::Decode)
            .expect("valid persisted domain document");
        assert_eq!(
            record,
            WarehouseRecord {
                name: "default".to_owned()
            }
        );
    }

    /// A corrupt durable document fails closed instead of becoming a partial domain object.
    #[test]
    fn malformed_durable_record_document_fails_closed() {
        let result: Result<WarehouseRecord, VerglasCatalogError> =
            serde_json::from_str("not-json").map_err(VerglasCatalogError::Decode);
        assert!(matches!(result, Err(VerglasCatalogError::Decode(_))));
    }
}
