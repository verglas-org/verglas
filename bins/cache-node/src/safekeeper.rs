//! Embedded Neon safekeeper wiring. PostgreSQL talks to this listener; WAL is
//! committed to the cache fleet's shared EC fragment ring before it is acked.

use std::net::SocketAddr;
use std::sync::Arc;

use verglas_core::CacheKey;
use verglas_core::read::{ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange};
use verglas_core::write::{
    CompletedPartRef, CopyOutcome, MultipartCreation, ObjectWrite, PartInfo, PartUpload,
    PutOutcome, WriteBodyStream, WriteChecksum, WriteError, WriteMetadata,
};
use verglas_s3::{BackendStore, BackendStores, PassthroughRead, PassthroughWrite};
use verglas_safekeeper::{AppendGeometry, SafekeeperServer};

use crate::VERSION;
use crate::ring::RingPlane;

/// Default PostgreSQL wire address for the embedded safekeeper.
const DEFAULT_SAFEKEEPER_ADDR: &str = "0.0.0.0:5454";

/// One origin facade combining the cache node's separate read and write
/// passthrough implementations for WAL segment drain and recovery.
struct Origin {
    /// Direct origin reader.
    read: PassthroughRead,
    /// Durable direct origin writer.
    write: PassthroughWrite,
}

impl Origin {
    /// Builds both passthrough paths over the cache node's backend registry.
    fn new(stores: Arc<BackendStore>) -> Self {
        let stores: Arc<dyn BackendStores> = stores;
        Self {
            read: PassthroughRead::new(Arc::clone(&stores)),
            write: PassthroughWrite::new(stores),
        }
    }
}

impl ObjectRead for Origin {
    /// Reads a flushed WAL range directly from origin.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.read.get(key, range).await
    }

    /// Reads flushed WAL metadata directly from origin.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.read.head(key).await
    }
}

impl ObjectWrite for Origin {
    /// Drains one completed WAL segment durably to origin.
    async fn put(
        &self,
        key: &CacheKey,
        metadata: WriteMetadata,
        body: WriteBodyStream,
    ) -> Result<PutOutcome, WriteError> {
        self.write.put(key, metadata, body).await
    }

    /// Deletes a truncated WAL segment from origin.
    async fn delete(&self, key: &CacheKey) -> Result<(), WriteError> {
        self.write.delete(key).await
    }

    /// Forwards batch deletion for completeness of the origin contract.
    async fn delete_batch(
        &self,
        keys: &[CacheKey],
    ) -> Result<Vec<Result<(), WriteError>>, WriteError> {
        self.write.delete_batch(keys).await
    }

    /// Forwards an origin-side copy.
    async fn copy(
        &self,
        source: &CacheKey,
        dest: &CacheKey,
        metadata: Option<WriteMetadata>,
    ) -> Result<CopyOutcome, WriteError> {
        self.write.copy(source, dest, metadata).await
    }

    /// Starts a multipart origin upload.
    async fn create_multipart(
        &self,
        key: &CacheKey,
        metadata: WriteMetadata,
    ) -> Result<MultipartCreation, WriteError> {
        self.write.create_multipart(key, metadata).await
    }

    /// Uploads one multipart origin part.
    async fn upload_part(
        &self,
        key: &CacheKey,
        upload_id: &str,
        part_number: u16,
        checksum: WriteChecksum,
        body: WriteBodyStream,
    ) -> Result<PartUpload, WriteError> {
        self.write
            .upload_part(key, upload_id, part_number, checksum, body)
            .await
    }

    /// Completes a multipart origin upload.
    async fn complete_multipart(
        &self,
        key: &CacheKey,
        upload_id: &str,
        parts: Vec<CompletedPartRef>,
        object_checksum: WriteChecksum,
    ) -> Result<PutOutcome, WriteError> {
        self.write
            .complete_multipart(key, upload_id, parts, object_checksum)
            .await
    }

    /// Aborts a multipart origin upload.
    async fn abort_multipart(&self, key: &CacheKey, upload_id: &str) -> Result<(), WriteError> {
        self.write.abort_multipart(key, upload_id).await
    }

    /// Lists parts belonging to an in-flight origin upload.
    async fn list_parts(
        &self,
        key: &CacheKey,
        upload_id: &str,
    ) -> Result<Vec<PartInfo>, WriteError> {
        self.write.list_parts(key, upload_id).await
    }
}

/// Chooses an EC layout that uses the whole configured ring while retaining
/// two parity fragments once at least four cache nodes exist.
pub(crate) fn geometry(nodes: usize) -> AppendGeometry {
    match nodes {
        0 | 1 => AppendGeometry { k: 1, m: 0, w: 1 },
        2 => AppendGeometry { k: 1, m: 1, w: 2 },
        3 => AppendGeometry { k: 2, m: 1, w: 3 },
        count => AppendGeometry {
            k: count - 2,
            m: 2,
            w: count - 1,
        },
    }
}

/// Serves the Neon protocol from this cache-node process until its listener
/// fails. The supplied ring plane remains alive for the process lifetime.
pub async fn serve(
    stores: Arc<BackendStore>,
    bucket: String,
    cache_dir: std::path::PathBuf,
    ring: Arc<RingPlane>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::var("VERGLAS_SAFEKEEPER_ADDR")
        .unwrap_or_else(|_| DEFAULT_SAFEKEEPER_ADDR.to_owned())
        .parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let layout = geometry(ring.node_count());
    let mut server = SafekeeperServer::new(
        ring.safekeeper_id(),
        Arc::new(Origin::new(stores)),
        bucket,
        "neon-wal",
        ring.transport(),
        ring.membership(),
        cache_dir.join("safekeeper"),
        layout,
    );
    if let Ok(endpoint) = std::env::var("VERGLAS_SAFEKEEPER_BROKER_ENDPOINT") {
        let advertise = std::env::var("VERGLAS_SAFEKEEPER_ADVERTISE_ADDR")
            .map_err(|_| "VERGLAS_SAFEKEEPER_ADVERTISE_ADDR is required with broker endpoint")?;
        server = server.with_broker(endpoint, advertise);
    }
    eprintln!(
        "verglas-cache-node {VERSION} embedded safekeeper listening on {} (EC k={}, m={}, ack quorum={})",
        listener.local_addr()?,
        layout.k,
        layout.m,
        layout.w,
    );
    server
        .serve(listener)
        .await
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Geometry grows from replicated single-node mode to two-parity fleet EC.
    #[test]
    fn geometry_tracks_ring_growth() {
        assert_eq!(geometry(1), AppendGeometry { k: 1, m: 0, w: 1 });
        assert_eq!(geometry(3), AppendGeometry { k: 2, m: 1, w: 3 });
        assert_eq!(geometry(4), AppendGeometry { k: 2, m: 2, w: 3 });
        assert_eq!(geometry(5), AppendGeometry { k: 3, m: 2, w: 4 });
    }
}
