//! Test-only doubles shared across this crate's unit tests.

use crate::{ImmutableMetadataStore, MetadataStoreError};

/// A metadata authority that answers nothing.
///
/// Shared by tests whose subject is the catalog transport or transaction
/// sequencing, never object IO: every method either returns a fixed identity
/// or refuses, so a test that accidentally reaches metadata fails loudly
/// instead of silently exercising a second subsystem.
pub(crate) struct UnusedMetadataStore;

#[async_trait::async_trait]
impl ImmutableMetadataStore for UnusedMetadataStore {
    /// Accepts every test warehouse because this fixture never accesses IO.
    fn ensure_warehouse(&self, _warehouse: &str) -> Result<(), MetadataStoreError> {
        Ok(())
    }

    /// Returns one valid fixture identity for authorization-only tests.
    fn warehouse_database_id(&self, _warehouse: &str) -> Result<String, MetadataStoreError> {
        Ok("00000000-0000-0000-0000-000000000000".to_owned())
    }

    /// Returns a deterministic unused table root.
    fn table_root(
        &self,
        _warehouse: &str,
        request_id: u128,
        _namespace: &[String],
        _name: &str,
    ) -> Result<String, MetadataStoreError> {
        Ok(format!("unused/tables/{request_id}"))
    }
    /// Returns a deterministic unused view root.
    fn view_root(
        &self,
        _warehouse: &str,
        request_id: u128,
        _namespace: &[String],
        _name: &str,
    ) -> Result<String, MetadataStoreError> {
        Ok(format!("unused/views/{request_id}"))
    }
    /// Accepts locations because this fixture never exercises object IO.
    fn validate_location(
        &self,
        _warehouse: &str,
        _location: &str,
    ) -> Result<(), MetadataStoreError> {
        Ok(())
    }
    /// Rejects an unexpected table metadata read.
    async fn load_table(
        &self,
        _location: &str,
    ) -> Result<verglas_catalog_core::iceberg::spec::TableMetadataRef, MetadataStoreError> {
        Err(MetadataStoreError {
            message: "unused".into(),
        })
    }
    /// Rejects an unexpected view metadata read.
    async fn load_view(
        &self,
        _location: &str,
    ) -> Result<verglas_catalog_core::iceberg::spec::ViewMetadataRef, MetadataStoreError> {
        Err(MetadataStoreError {
            message: "unused".into(),
        })
    }
    /// Rejects an unexpected table metadata publication.
    async fn create_table(
        &self,
        _request_id: u128,
        _location: &str,
        _request: &verglas_catalog_core::api::CreateTableRequest,
    ) -> Result<
        (
            String,
            verglas_catalog_core::iceberg::spec::TableMetadataRef,
        ),
        MetadataStoreError,
    > {
        Err(MetadataStoreError {
            message: "unused".into(),
        })
    }
    /// Rejects an unexpected table metadata commit.
    async fn commit_table(
        &self,
        _request_id: u128,
        _location: &str,
        _request: &verglas_catalog_core::api::CommitTableRequest,
    ) -> Result<
        (
            String,
            verglas_catalog_core::iceberg::spec::TableMetadataRef,
        ),
        MetadataStoreError,
    > {
        Err(MetadataStoreError {
            message: "unused".into(),
        })
    }
    /// Rejects an unexpected multi-table metadata commit.
    async fn commit_tables(
        &self,
        _request_id: u128,
        _changes: &[(String, verglas_catalog_core::api::CommitTableRequest)],
    ) -> Result<
        Vec<(
            String,
            verglas_catalog_core::iceberg::spec::TableMetadataRef,
        )>,
        MetadataStoreError,
    > {
        Err(MetadataStoreError {
            message: "unused".into(),
        })
    }
    /// Rejects an unexpected view metadata publication.
    async fn create_view(
        &self,
        _request_id: u128,
        _location: &str,
        _request: &verglas_catalog_core::api::CreateViewRequest,
    ) -> Result<(String, verglas_catalog_core::iceberg::spec::ViewMetadataRef), MetadataStoreError>
    {
        Err(MetadataStoreError {
            message: "unused".into(),
        })
    }
    /// Rejects an unexpected view metadata commit.
    async fn commit_view(
        &self,
        _request_id: u128,
        _location: &str,
        _request: &verglas_catalog_core::api::CommitViewRequest,
    ) -> Result<(String, verglas_catalog_core::iceberg::spec::ViewMetadataRef), MetadataStoreError>
    {
        Err(MetadataStoreError {
            message: "unused".into(),
        })
    }
}
