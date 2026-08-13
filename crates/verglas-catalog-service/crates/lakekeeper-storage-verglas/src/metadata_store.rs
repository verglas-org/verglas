//! Immutable Iceberg metadata publication over a configured Lakekeeper FileIO.
//!
//! CRaft owns catalog pointers while this adapter owns immutable metadata bytes.
//! The deterministic UUID path means a collision is detected before write and
//! fails closed; a future conditional-put primitive belongs at this boundary.

use std::{str::FromStr as _, sync::Arc};

use async_trait::async_trait;
use lakekeeper::{
    api::{CommitTableRequest, CommitViewRequest, CreateTableRequest, CreateViewRequest},
    iceberg::spec::{TableMetadata, TableMetadataRef, ViewMetadata, ViewMetadataRef},
    service::{AllowedFormatVersions, TableId},
};
use lakekeeper_io::{LakekeeperStorage, Location};

use crate::{ImmutableMetadataStore, MetadataStoreError};

/// Production metadata authority backed by a Lakekeeper-configured object store.
#[derive(Debug, Clone)]
pub struct FileIoMetadataStore {
    io: Arc<dyn LakekeeperStorage>,
    catalog_root: String,
}

impl FileIoMetadataStore {
    /// Binds immutable metadata publication to an already configured FileIO.
    #[must_use]
    pub fn new(io: Arc<dyn LakekeeperStorage>, catalog_root: impl Into<String>) -> Self {
        Self {
            io,
            catalog_root: catalog_root.into().trim_end_matches('/').to_owned(),
        }
    }

    /// Creates a deterministic root below the configured warehouse catalog root.
    fn allocated_root(
        &self,
        kind: &str,
        request_id: u128,
        namespace: &[String],
        name: &str,
    ) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            self.catalog_root,
            kind,
            namespace.join("/"),
            name,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                request_id.to_be_bytes().as_ref()
            )
        )
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
        // Lakekeeper FileIO contract gains one. The generated UUID makes a
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

/// Binds the catalog-managed root into the request consumed by Lakekeeper's builder.
fn table_request_with_location(request: &CreateTableRequest, location: &str) -> CreateTableRequest {
    let mut bound = request.clone();
    bound.location = Some(location.to_owned());
    bound
}

#[async_trait]
impl ImmutableMetadataStore for FileIoMetadataStore {
    /// Allocates a stable managed root for a table without a requested location.
    fn table_root(&self, request_id: u128, namespace: &[String], name: &str) -> String {
        self.allocated_root("tables", request_id, namespace, name)
    }

    /// Allocates a stable managed root for a view without a requested location.
    fn view_root(&self, request_id: u128, namespace: &[String], name: &str) -> String {
        self.allocated_root("views", request_id, namespace, name)
    }
    /// Loads a table object after the FileIO has returned its exact immutable bytes.
    async fn load_table(&self, location: &str) -> Result<TableMetadataRef, MetadataStoreError> {
        Ok(Arc::new(self.load::<TableMetadata>(location).await?))
    }

    /// Loads a view object after the FileIO has returned its exact immutable bytes.
    async fn load_view(&self, location: &str) -> Result<ViewMetadataRef, MetadataStoreError> {
        Ok(Arc::new(self.load::<ViewMetadata>(location).await?))
    }

    /// Creates the first immutable table metadata version using Lakekeeper's shared builder.
    async fn create_table(
        &self,
        request_id: u128,
        location: &str,
        request: &CreateTableRequest,
    ) -> Result<(String, TableMetadataRef), MetadataStoreError> {
        let request = table_request_with_location(request, location);
        let metadata =
            lakekeeper::server::tables::create_table::create_table_request_into_table_metadata(
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
        let build = lakekeeper::server::commit_tables::apply_commit(
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

    /// Creates immutable view metadata using Lakekeeper's canonical builder.
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
        let metadata =
            lakekeeper::server::views::build_view_metadata(request, location.to_owned(), view_id)
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
        let metadata =
            lakekeeper::server::views::apply_view_metadata_commit(request.clone(), previous)
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

    use lakekeeper::{api::CreateTableRequest, iceberg::spec::Schema};

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
}
