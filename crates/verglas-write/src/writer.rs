//! Write-back writer wrapper (#180).
//!
//! Intercepts PUT: a key that a prefix opts in is streamed through the quorum
//! coordinator, which erasure-codes it stripe by stripe (one stripe resident,
//! regardless of object size) and places the fragments; a key that opts out
//! delegates to the origin unchanged. There is no size cap: the write-back size
//! limit is NVMe headroom, enforced inside the coordinator, not a DRAM buffer
//! cap. Every other write operation (delete, copy, multipart) delegates to the
//! origin unchanged. Multipart write-back is out of scope for this tier — large
//! multipart uploads already stream to the origin, where the ack-latency win is
//! marginal — so multipart is deliberately write-through (a noted extension
//! point).

use std::sync::Arc;

use verglas_core::CacheKey;
use verglas_core::write::{
    CompletedPartRef, CopyOutcome, MultipartCreation, ObjectWrite, PartInfo, PartUpload,
    PutOutcome, WriteBodyStream, WriteChecksum, WriteError, WriteMetadata,
};

use crate::coordinator::WriteCoordinator;
use crate::policy::WritebackPolicy;

/// Intercepts PUTs for write-back-eligible prefixes; delegates everything else.
pub struct WritebackWriter<W: ObjectWrite> {
    /// The quorum coordinator.
    coordinator: Arc<WriteCoordinator<W>>,
    /// Direct-to-origin writer for delegated writes.
    origin: Arc<W>,
    /// Per-prefix opt-in rules.
    policy: Arc<WritebackPolicy>,
}

impl<W: ObjectWrite> WritebackWriter<W> {
    /// Builds the writer over the shared coordinator, origin, and policy.
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
}

impl<W: ObjectWrite> ObjectWrite for WritebackWriter<W> {
    /// Routes an eligible PUT through the quorum coordinator, streaming the body
    /// so nothing is buffered whole; delegates an opted-out key to the origin.
    async fn put(
        &self,
        key: &CacheKey,
        metadata: WriteMetadata,
        body: WriteBodyStream,
    ) -> Result<PutOutcome, WriteError> {
        let Some((k, m, w)) = self.policy.geometry_for(key) else {
            return self.origin.put(key, metadata, body).await;
        };
        self.coordinator
            .put_stream(key, &metadata, body, k, m, w)
            .await
    }

    /// Orders DELETE after any earlier quorum-acked PUT for the same key.
    async fn delete(&self, key: &CacheKey) -> Result<(), WriteError> {
        self.coordinator.delete(key).await
    }

    /// Applies the same per-key ordering to every batch DELETE member.
    async fn delete_batch(
        &self,
        keys: &[CacheKey],
    ) -> Result<Vec<Result<(), WriteError>>, WriteError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.coordinator.delete(key).await);
        }
        Ok(results)
    }

    /// Delegates CopyObject to the origin.
    async fn copy(
        &self,
        source: &CacheKey,
        dest: &CacheKey,
        metadata: Option<WriteMetadata>,
    ) -> Result<CopyOutcome, WriteError> {
        self.origin.copy(source, dest, metadata).await
    }

    /// Delegates multipart creation to the origin (multipart is write-through).
    async fn create_multipart(
        &self,
        key: &CacheKey,
        metadata: WriteMetadata,
    ) -> Result<MultipartCreation, WriteError> {
        self.origin.create_multipart(key, metadata).await
    }

    /// Delegates part upload to the origin.
    async fn upload_part(
        &self,
        key: &CacheKey,
        upload_id: &str,
        part_number: u16,
        checksum: WriteChecksum,
        body: WriteBodyStream,
    ) -> Result<PartUpload, WriteError> {
        self.origin
            .upload_part(key, upload_id, part_number, checksum, body)
            .await
    }

    /// Delegates multipart completion to the origin.
    async fn complete_multipart(
        &self,
        key: &CacheKey,
        upload_id: &str,
        parts: Vec<CompletedPartRef>,
        object_checksum: WriteChecksum,
    ) -> Result<PutOutcome, WriteError> {
        self.origin
            .complete_multipart(key, upload_id, parts, object_checksum)
            .await
    }

    /// Delegates multipart abort to the origin.
    async fn abort_multipart(&self, key: &CacheKey, upload_id: &str) -> Result<(), WriteError> {
        self.origin.abort_multipart(key, upload_id).await
    }

    /// Delegates part listing to the origin.
    async fn list_parts(
        &self,
        key: &CacheKey,
        upload_id: &str,
    ) -> Result<Vec<PartInfo>, WriteError> {
        self.origin.list_parts(key, upload_id).await
    }
}
