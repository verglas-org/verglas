//! CRaft-backed Lakekeeper catalog storage primitives.
//!
//! This crate owns the boundary from Lakekeeper domain records to the
//! tenant-rooted Verglas consensus catalog. It deliberately has no SQL store.

use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};
use verglas_catalog::{ManagedCatalogClient, ManagedCatalogRequest, ManagedCatalogResponse};
use verglas_consensus::{CatalogAction, CatalogBatch, CatalogEntity, CatalogRequirement};

pub mod authorized;
pub mod domain;
mod hosted;
pub mod metadata_store;
mod transaction;

pub use authorized::AuthorizedVerglasCatalog;
pub use transaction::VerglasTransaction;

impl lakekeeper::api::ThreadSafe for VerglasCatalog {}

/// The immutable object authority used by hosted Iceberg table and view routes.
///
/// Implementations must verify every read object identity and publish only a
/// newly immutable metadata object. They never own catalog names or pointers:
/// the subsequent CRaft batch is the only authority for those transitions.
#[async_trait::async_trait]
pub trait ImmutableMetadataStore: Send + Sync + 'static {
    /// Allocates a deterministic catalog-managed table root for an omitted client location.
    fn table_root(&self, request_id: u128, namespace: &[String], name: &str) -> String;
    /// Allocates a deterministic catalog-managed view root for an omitted client location.
    fn view_root(&self, request_id: u128, namespace: &[String], name: &str) -> String;
    /// Loads and verifies the immutable metadata object at a table location.
    async fn load_table(
        &self,
        location: &str,
    ) -> Result<lakekeeper::iceberg::spec::TableMetadataRef, MetadataStoreError>;
    /// Loads and verifies the immutable metadata object at a view location.
    async fn load_view(
        &self,
        location: &str,
    ) -> Result<lakekeeper::iceberg::spec::ViewMetadataRef, MetadataStoreError>;
    /// Publishes a table creation metadata object and returns its immutable identity.
    async fn create_table(
        &self,
        request_id: u128,
        location: &str,
        request: &lakekeeper::api::CreateTableRequest,
    ) -> Result<(String, lakekeeper::iceberg::spec::TableMetadataRef), MetadataStoreError>;
    /// Applies a validated table commit and publishes a new immutable metadata object.
    async fn commit_table(
        &self,
        request_id: u128,
        location: &str,
        request: &lakekeeper::api::CommitTableRequest,
    ) -> Result<(String, lakekeeper::iceberg::spec::TableMetadataRef), MetadataStoreError>;
    /// Atomically publishes every metadata object for an Iceberg table transaction.
    async fn commit_tables(
        &self,
        request_id: u128,
        changes: &[(String, lakekeeper::api::CommitTableRequest)],
    ) -> Result<Vec<(String, lakekeeper::iceberg::spec::TableMetadataRef)>, MetadataStoreError>;
    /// Publishes a view creation metadata object and returns its immutable identity.
    async fn create_view(
        &self,
        request_id: u128,
        location: &str,
        request: &lakekeeper::api::CreateViewRequest,
    ) -> Result<(String, lakekeeper::iceberg::spec::ViewMetadataRef), MetadataStoreError>;
    /// Applies a validated view commit and publishes a new immutable metadata object.
    async fn commit_view(
        &self,
        request_id: u128,
        location: &str,
        request: &lakekeeper::api::CommitViewRequest,
    ) -> Result<(String, lakekeeper::iceberg::spec::ViewMetadataRef), MetadataStoreError>;
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
/// Lakekeeper REST handlers can depend on this contract without inheriting
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

/// One Lakekeeper storage handle bound to a single tenant and warehouse group.
#[derive(Clone)]
pub struct VerglasCatalog {
    client: Arc<ManagedCatalogClient>,
    metadata: Arc<dyn ImmutableMetadataStore>,
}

impl std::fmt::Debug for VerglasCatalog {
    /// Formats the handle without exposing ingress or tenant credentials.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("VerglasCatalog")
    }
}

#[async_trait::async_trait]
impl lakekeeper::service::health::HealthExt for VerglasCatalog {
    /// Reports the CRaft catalog adapter as ready; each request still fences at the ingress.
    async fn health(&self) -> Vec<lakekeeper::service::health::Health> {
        vec![lakekeeper::service::health::Health::now(
            "verglas-craft-catalog",
            lakekeeper::service::health::HealthStatus::Healthy,
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

    /// Starts one buffered deterministic transaction for a Lakekeeper handler.
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

/// A failure at the sole Lakekeeper-to-CRaft catalog authority boundary.
#[derive(Debug, thiserror::Error)]
pub enum VerglasCatalogError {
    /// The CRaft ingress rejected or could not serve the request.
    #[error(transparent)]
    Client(#[from] verglas_catalog::ManagedCatalogError),
    /// A persisted document did not match the requested Lakekeeper wire type.
    #[error("invalid consensus catalog document: {0}")]
    Decode(serde_json::Error),
    /// A Lakekeeper wire object could not be made into a durable document.
    #[error("cannot encode consensus catalog document: {0}")]
    Encode(serde_json::Error),
    /// The request could not form a valid deterministic catalog transaction.
    #[error("invalid consensus catalog transaction")]
    InvalidBatch,
    /// The ingress returned a response for another catalog operation.
    #[error("unexpected consensus catalog response")]
    WrongResponse,
}

#[cfg(test)]
mod tests {
    //! Unit tests for typed durable-record decoding.

    use serde::Deserialize;

    use super::VerglasCatalogError;

    /// A representative Lakekeeper wire record stored as a consensus JSON document.
    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct WarehouseRecord {
        name: String,
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
