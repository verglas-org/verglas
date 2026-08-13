//! Writer-facing routing for the EC durability tier.
//!
//! Eligible mutations never call origin COPY or multipart methods.  They first
//! become immutable `w`-proven fragment-ring state; the coordinator alone owns
//! any later origin propagation.

use std::sync::Arc;

use verglas_core::CacheKey;
use verglas_core::write::{
    CompletedPartRef, CopyOutcome, MultipartCreation, ObjectWrite, PartInfo, PartUpload,
    PutOutcome, WriteBodyStream, WriteChecksum, WriteError, WriteMetadata,
};

use crate::coordinator::{
    EcWriteGeometry, MultipartCompleteRequest, MultipartPartRequest, WriteCoordinator,
};
use crate::policy::WritebackPolicy;

pub struct WritebackWriter<W: ObjectWrite> {
    coordinator: Arc<WriteCoordinator<W>>,
    /// Only ordinary opted-out PUTs use the origin directly.  All mutation
    /// methods below route through the coordinator.
    origin: Arc<W>,
    policy: Arc<WritebackPolicy>,
}

impl<W: ObjectWrite> WritebackWriter<W> {
    pub fn new(
        coordinator: Arc<WriteCoordinator<W>>,
        origin: Arc<W>,
        policy: Arc<WritebackPolicy>,
    ) -> Self {
        Self {
            coordinator,
            origin,
            policy,
        }
    }

    fn geometry(&self, key: &CacheKey) -> Result<(usize, usize, usize), WriteError> {
        self.policy.geometry_for(key).ok_or_else(|| {
            WriteError::Unsupported("write-back mutation requires an enabled prefix".to_owned())
        })
    }
}

impl<W: ObjectWrite> ObjectWrite for WritebackWriter<W> {
    async fn put(
        &self,
        key: &CacheKey,
        metadata: WriteMetadata,
        body: WriteBodyStream,
    ) -> Result<PutOutcome, WriteError> {
        match self.policy.geometry_for(key) {
            Some((k, m, w)) => {
                self.coordinator
                    .put_stream(key, &metadata, body, k, m, w)
                    .await
            }
            None => self.origin.put(key, metadata, body).await,
        }
    }

    async fn delete(&self, key: &CacheKey) -> Result<(), WriteError> {
        match self.policy.geometry_for(key) {
            Some((k, m, w)) => self.coordinator.delete_ec(key, k, m, w).await,
            None => self.coordinator.delete(key).await,
        }
    }

    async fn delete_batch(
        &self,
        keys: &[CacheKey],
    ) -> Result<Vec<Result<(), WriteError>>, WriteError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(match self.geometry(key) {
                Ok((k, m, w)) => self.coordinator.delete_ec(key, k, m, w).await,
                Err(_) => self.coordinator.delete(key).await,
            });
        }
        Ok(results)
    }

    async fn copy(
        &self,
        source: &CacheKey,
        dest: &CacheKey,
        metadata: Option<WriteMetadata>,
    ) -> Result<CopyOutcome, WriteError> {
        let (k, m, w) = self.geometry(dest)?;
        self.coordinator
            .copy_ec(source, dest, metadata, k, m, w)
            .await
    }

    async fn create_multipart(
        &self,
        key: &CacheKey,
        metadata: WriteMetadata,
    ) -> Result<MultipartCreation, WriteError> {
        let (k, m, w) = self.geometry(key)?;
        self.coordinator
            .create_multipart_ec(key, metadata, k, m, w)
            .await
    }

    async fn upload_part(
        &self,
        key: &CacheKey,
        upload_id: &str,
        part_number: u16,
        checksum: WriteChecksum,
        body: WriteBodyStream,
    ) -> Result<PartUpload, WriteError> {
        let (k, m, w) = self.geometry(key)?;
        self.coordinator
            .upload_part_ec(MultipartPartRequest {
                key,
                upload_id,
                part_number,
                checksum,
                body,
                geometry: EcWriteGeometry { k, m, w },
            })
            .await
    }

    async fn complete_multipart(
        &self,
        key: &CacheKey,
        upload_id: &str,
        parts: Vec<CompletedPartRef>,
        object_checksum: WriteChecksum,
    ) -> Result<PutOutcome, WriteError> {
        let (k, m, w) = self.geometry(key)?;
        self.coordinator
            .complete_multipart_ec(MultipartCompleteRequest {
                key,
                upload_id,
                parts,
                checksum: object_checksum,
                geometry: EcWriteGeometry { k, m, w },
            })
            .await
    }

    async fn abort_multipart(&self, key: &CacheKey, upload_id: &str) -> Result<(), WriteError> {
        self.coordinator.abort_multipart_ec(key, upload_id).await
    }

    async fn list_parts(
        &self,
        key: &CacheKey,
        upload_id: &str,
    ) -> Result<Vec<PartInfo>, WriteError> {
        self.coordinator.list_parts_ec(key, upload_id)
    }
}
