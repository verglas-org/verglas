//! Immutable Iceberg metadata publication over a configured Catalog FileIO.
//!
//! CRaft owns catalog pointers while this adapter owns immutable metadata bytes.
//! The deterministic UUID path means a collision is detected before write and
//! fails closed; a future conditional-put primitive belongs at this boundary.

use std::{
    collections::BTreeMap,
    str::FromStr as _,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use verglas_catalog_core::{
    api::{CommitTableRequest, CommitViewRequest, CreateTableRequest, CreateViewRequest},
    iceberg::spec::{TableMetadata, TableMetadataRef, ViewMetadata, ViewMetadataRef},
    service::{AllowedFormatVersions, TableId},
};
use verglas_catalog_io::{CatalogStorage, Location};

use crate::{ImmutableMetadataStore, MetadataStoreError};

/// Live warehouse-prefix to immutable object-root bindings owned by access.
/// The durable database declarations are replayed after every Catalog start.
#[derive(Debug, Clone, Default)]
pub struct MetadataRoots {
    roots: Arc<RwLock<BTreeMap<String, WarehouseBinding>>>,
}

/// One durable database identity and its isolated metadata root.
#[derive(Debug, Clone)]
struct WarehouseBinding {
    database_id: String,
    root: String,
}

impl MetadataRoots {
    /// Binds a Catalog warehouse prefix to its isolated managed bucket.
    pub fn bind(
        &self,
        warehouse: &str,
        database_id: &str,
        bucket: &str,
    ) -> Result<(), MetadataStoreError> {
        if warehouse.trim().is_empty() || bucket.trim().is_empty() {
            return Err(MetadataStoreError {
                message: "warehouse profile is incomplete".to_owned(),
            });
        }
        let root = format!("s3://{}/iceberg", bucket.trim());
        let mut roots = self.roots.write().map_err(|_| MetadataStoreError {
            message: "warehouse profile registry is unavailable".to_owned(),
        })?;
        let binding = WarehouseBinding {
            database_id: database_id.to_owned(),
            root,
        };
        if let Some(existing) = roots.get(warehouse) {
            if existing.database_id == binding.database_id && existing.root == binding.root {
                return Ok(());
            }
            return Err(MetadataStoreError {
                message: format!(
                    "warehouse profile {warehouse} conflicts with its existing binding"
                ),
            });
        }
        roots.insert(warehouse.to_owned(), binding);
        Ok(())
    }

    /// Removes a warehouse root after its Catalog profile is detached.
    pub fn remove(&self, warehouse: &str) -> Result<bool, MetadataStoreError> {
        let mut roots = self.roots.write().map_err(|_| MetadataStoreError {
            message: "warehouse profile registry is unavailable".to_owned(),
        })?;
        Ok(roots.remove(warehouse).is_some())
    }

    /// Returns the exact root for a requested Iceberg warehouse prefix.
    pub fn root(&self, warehouse: &str) -> Result<String, MetadataStoreError> {
        let roots = self.roots.read().map_err(|_| MetadataStoreError {
            message: "warehouse profile registry is unavailable".to_owned(),
        })?;
        roots
            .get(warehouse)
            .map(|binding| binding.root.clone())
            .ok_or_else(|| MetadataStoreError {
                message: format!("warehouse profile {warehouse} is not registered"),
            })
    }

    /// Returns the database resource identity bound to a routed warehouse.
    pub fn database_id(&self, warehouse: &str) -> Result<String, MetadataStoreError> {
        let roots = self.roots.read().map_err(|_| MetadataStoreError {
            message: "warehouse profile registry is unavailable".to_owned(),
        })?;
        roots
            .get(warehouse)
            .map(|binding| binding.database_id.clone())
            .ok_or_else(|| MetadataStoreError {
                message: format!("warehouse profile {warehouse} is not registered"),
            })
    }

    /// Allocates an immutable table root within one exact warehouse profile.
    pub fn table_root(
        &self,
        warehouse: &str,
        request_id: u128,
        namespace: &[String],
        name: &str,
    ) -> Result<String, MetadataStoreError> {
        Ok(allocated_root(
            &self.root(warehouse)?,
            "tables",
            request_id,
            namespace,
            name,
        ))
    }

    /// Allocates an immutable view root within one exact warehouse profile.
    pub fn view_root(
        &self,
        warehouse: &str,
        request_id: u128,
        namespace: &[String],
        name: &str,
    ) -> Result<String, MetadataStoreError> {
        Ok(allocated_root(
            &self.root(warehouse)?,
            "views",
            request_id,
            namespace,
            name,
        ))
    }

    /// Rejects locations outside the exact root registered for this warehouse.
    pub fn validate_location(
        &self,
        warehouse: &str,
        location: &str,
    ) -> Result<(), MetadataStoreError> {
        let root = self.root(warehouse)?;
        let rooted = format!("{}/", root.trim_end_matches('/'));
        if location.starts_with(&rooted) {
            return Ok(());
        }
        Err(MetadataStoreError {
            message: format!("location is outside warehouse profile {warehouse}"),
        })
    }
}

/// Production metadata authority backed by a Catalog-configured object store.
#[derive(Debug, Clone)]
pub struct FileIoMetadataStore {
    io: Arc<dyn CatalogStorage>,
    roots: MetadataRoots,
}

impl FileIoMetadataStore {
    /// Binds immutable metadata publication to an already configured FileIO.
    #[must_use]
    pub fn new(io: Arc<dyn CatalogStorage>, roots: MetadataRoots) -> Self {
        Self { io, roots }
    }

    /// Makes a replay-stable immutable metadata path below an Iceberg root.
    fn metadata_path(location: &str, request_id: u128) -> String {
        format!(
            "{}/metadata/{}.metadata.json",
            location.trim_end_matches('/'),
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                request_id.to_be_bytes().as_ref()
            )
        )
    }

    /// Publishes bytes only after proving the generated immutable path is absent.
    async fn publish<T: serde::Serialize>(
        &self,
        root: &str,
        request_id: u128,
        metadata: &T,
    ) -> Result<String, MetadataStoreError> {
        let path = Self::metadata_path(root, request_id);
        let bytes = serde_json::to_vec(metadata).map_err(|error| MetadataStoreError {
            message: format!("cannot encode metadata: {error}"),
        })?;
        if self.io.exists(&path).await.map_err(read_error)? {
            let existing = self.io.read_single(&path).await.map_err(read_error)?;
            if existing.as_ref() == bytes.as_slice() {
                return Ok(path);
            }
            return Err(MetadataStoreError {
                message: format!("immutable metadata identity conflicts: {path}"),
            });
        }
        // Extension point: use an object-store conditional PUT here when the
        // Catalog FileIO contract gains one. The generated UUID makes a
        // collision fail closed today instead of overwriting a prior object.
        self.io
            .write(&path, bytes.clone().into())
            .await
            .map_err(write_error)?;
        let written = self.io.read_single(&path).await.map_err(read_error)?;
        if written.as_ref() != bytes.as_slice() {
            return Err(MetadataStoreError {
                message: format!("metadata read-back identity mismatch: {path}"),
            });
        }
        Ok(path)
    }

    /// Loads one JSON metadata object and fails closed on malformed bytes.
    async fn load<T: serde::de::DeserializeOwned>(
        &self,
        location: &str,
    ) -> Result<T, MetadataStoreError> {
        let bytes = self.io.read_single(location).await.map_err(read_error)?;
        serde_json::from_slice(&bytes).map_err(|error| MetadataStoreError {
            message: format!("invalid immutable metadata at {location}: {error}"),
        })
    }
}

/// Creates a deterministic table or view root below one verified bucket root.
fn allocated_root(
    root: &str,
    kind: &str,
    request_id: u128,
    namespace: &[String],
    name: &str,
) -> String {
    format!(
        "{}/{}/{}/{}/{}",
        root.trim_end_matches('/'),
        kind,
        namespace.join("/"),
        name,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            request_id.to_be_bytes().as_ref()
        )
    )
}

/// Converts a FileIO read failure into a safe hosted metadata diagnostic.
fn read_error(error: impl std::fmt::Display) -> MetadataStoreError {
    MetadataStoreError {
        message: format!("metadata read failed: {error}"),
    }
}

/// Converts a FileIO write failure into a safe hosted metadata diagnostic.
fn write_error(error: impl std::fmt::Display) -> MetadataStoreError {
    MetadataStoreError {
        message: format!("metadata publication failed: {error}"),
    }
}

/// Binds the catalog-managed root into the request consumed by Catalog's builder.
fn table_request_with_location(request: &CreateTableRequest, location: &str) -> CreateTableRequest {
    let mut bound = request.clone();
    bound.location = Some(location.to_owned());
    bound
}

#[async_trait]
impl ImmutableMetadataStore for FileIoMetadataStore {
    /// Confirms a request prefix resolves to an explicit profile before routing.
    fn ensure_warehouse(&self, warehouse: &str) -> Result<(), MetadataStoreError> {
        self.roots.root(warehouse).map(|_| ())
    }

    /// Returns the durable database resource identity for authorization routing.
    fn warehouse_database_id(&self, warehouse: &str) -> Result<String, MetadataStoreError> {
        self.roots.database_id(warehouse)
    }

    /// Allocates a stable managed root for a table without a requested location.
    fn table_root(
        &self,
        warehouse: &str,
        request_id: u128,
        namespace: &[String],
        name: &str,
    ) -> Result<String, MetadataStoreError> {
        self.roots
            .table_root(warehouse, request_id, namespace, name)
    }

    /// Allocates a stable managed root for a view without a requested location.
    fn view_root(
        &self,
        warehouse: &str,
        request_id: u128,
        namespace: &[String],
        name: &str,
    ) -> Result<String, MetadataStoreError> {
        self.roots.view_root(warehouse, request_id, namespace, name)
    }

    /// Rejects a supplied location that escapes the registered isolated bucket.
    fn validate_location(&self, warehouse: &str, location: &str) -> Result<(), MetadataStoreError> {
        self.roots.validate_location(warehouse, location)
    }
    /// Loads a table object after the FileIO has returned its exact immutable bytes.
    async fn load_table(&self, location: &str) -> Result<TableMetadataRef, MetadataStoreError> {
        Ok(Arc::new(self.load::<TableMetadata>(location).await?))
    }

    /// Loads a view object after the FileIO has returned its exact immutable bytes.
    async fn load_view(&self, location: &str) -> Result<ViewMetadataRef, MetadataStoreError> {
        Ok(Arc::new(self.load::<ViewMetadata>(location).await?))
    }

    /// Creates the first immutable table metadata version using Catalog's shared builder.
    async fn create_table(
        &self,
        request_id: u128,
        location: &str,
        request: &CreateTableRequest,
    ) -> Result<(String, TableMetadataRef), MetadataStoreError> {
        let request = table_request_with_location(request, location);
        let metadata =
            verglas_catalog_core::server::tables::create_table::create_table_request_into_table_metadata(
                TableId::from(uuid::Uuid::new_v5(
                    &uuid::Uuid::NAMESPACE_URL,
                    request_id.to_be_bytes().as_ref(),
                )),
                request,
                &AllowedFormatVersions::default(),
                None,
            )
            .map_err(|error| MetadataStoreError {
                message: format!("table metadata validation failed: {error}"),
            })?;
        let path = self.publish(location, request_id, &metadata).await?;
        Ok((path, Arc::new(metadata)))
    }

    /// Validates requirements and applies standard Iceberg table updates before publication.
    async fn commit_table(
        &self,
        request_id: u128,
        location: &str,
        request: &CommitTableRequest,
    ) -> Result<(String, TableMetadataRef), MetadataStoreError> {
        let previous = self.load::<TableMetadata>(location).await?;
        let current = Location::from_str(location).map_err(|error| MetadataStoreError {
            message: format!("invalid metadata location: {error}"),
        })?;
        let build = verglas_catalog_core::server::commit_tables::apply_commit(
            previous,
            Some(&current),
            &request.requirements,
            request.updates.clone(),
        )
        .map_err(|error| MetadataStoreError {
            message: format!("table commit validation failed: {error}"),
        })?;
        let path = self
            .publish(build.metadata.location(), request_id, &build.metadata)
            .await?;
        Ok((path, Arc::new(build.metadata)))
    }

    /// Commits table metadata serially; CRaft publishes all pointers atomically afterwards.
    async fn commit_tables(
        &self,
        request_id: u128,
        changes: &[(String, CommitTableRequest)],
    ) -> Result<Vec<(String, TableMetadataRef)>, MetadataStoreError> {
        let mut published = Vec::with_capacity(changes.len());
        for (location, request) in changes {
            published.push(self.commit_table(request_id, location, request).await?);
        }
        Ok(published)
    }

    /// Creates immutable view metadata using Catalog's canonical builder.
    async fn create_view(
        &self,
        request_id: u128,
        location: &str,
        request: &CreateViewRequest,
    ) -> Result<(String, ViewMetadataRef), MetadataStoreError> {
        let view_id = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            request_id.to_be_bytes().as_ref(),
        );
        let metadata = verglas_catalog_core::server::views::build_view_metadata(
            request,
            location.to_owned(),
            view_id,
        )
        .map_err(|error| MetadataStoreError {
            message: format!("view metadata validation failed: {error}"),
        })?;
        let path = self.publish(location, request_id, &metadata).await?;
        Ok((path, Arc::new(metadata)))
    }

    /// Applies canonical Iceberg view updates before immutable publication.
    async fn commit_view(
        &self,
        request_id: u128,
        location: &str,
        request: &CommitViewRequest,
    ) -> Result<(String, ViewMetadataRef), MetadataStoreError> {
        let previous = self.load::<ViewMetadata>(location).await?;
        let metadata = verglas_catalog_core::server::views::apply_view_metadata_commit(
            request.clone(),
            previous,
        )
        .map_err(|error| MetadataStoreError {
            message: format!("view commit validation failed: {error}"),
        })?;
        let path = self
            .publish(metadata.location(), request_id, &metadata)
            .await?;
        Ok((path, Arc::new(metadata)))
    }
}

#[cfg(test)]
mod tests {
    //! Regression tests for catalog-managed immutable metadata locations.

    use verglas_catalog_core::{api::CreateTableRequest, iceberg::spec::Schema};

    /// A generated table root becomes the location consumed by the canonical builder.
    #[test]
    fn generated_table_location_is_bound_into_the_create_request() {
        let request = CreateTableRequest {
            name: "region".to_owned(),
            location: None,
            schema: Schema::builder().with_schema_id(0).build().expect("schema"),
            partition_spec: None,
            write_order: None,
            stage_create: None,
            properties: None,
        };
        let bound = super::table_request_with_location(&request, "s3://warehouse/region");
        assert_eq!(bound.location.as_deref(), Some("s3://warehouse/region"));
        assert!(request.location.is_none());
    }

    /// Each registered Catalog warehouse prefix receives an independent
    /// bucket root; no request may silently reuse another database's profile.
    #[test]
    fn warehouse_profiles_resolve_to_distinct_metadata_roots() {
        let roots = super::MetadataRoots::default();
        roots
            .bind(
                "analytics",
                "00000000-0000-0000-0000-000000000001",
                "analytics-bucket",
            )
            .expect("analytics binding");
        roots
            .bind(
                "reporting",
                "00000000-0000-0000-0000-000000000002",
                "reporting-bucket",
            )
            .expect("reporting binding");

        let analytics = roots
            .table_root("analytics", 7, &["sales".to_owned()], "daily")
            .expect("analytics root");
        let reporting = roots
            .table_root("reporting", 7, &["sales".to_owned()], "daily")
            .expect("reporting root");
        assert!(analytics.starts_with("s3://analytics-bucket/iceberg/tables/sales/daily/"));
        assert!(reporting.starts_with("s3://reporting-bucket/iceberg/tables/sales/daily/"));
        assert_ne!(analytics, reporting);
        assert!(roots.table_root("missing", 7, &[], "daily").is_err());
        assert!(
            roots
                .validate_location(
                    "analytics",
                    "s3://reporting-bucket/iceberg/tables/sales/daily"
                )
                .is_err()
        );
        assert!(
            roots
                .bind(
                    "analytics",
                    "00000000-0000-0000-0000-000000000002",
                    "reporting-bucket",
                )
                .is_err()
        );
    }
}
