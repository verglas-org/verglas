//! Fixed-part-size multipart uploads for Iceberg data-file writes.
//!
//! `iceberg-storage-opendal` creates every data-file writer with `op.writer(path)`
//! and no chunk size, so OpenDAL runs its S3 multipart writer in non-exact mode:
//! each `FileWrite::write` call from the Parquet writer becomes one upload part,
//! and those calls are variable-sized (one per Parquet flush). AWS S3 accepts
//! variable non-final parts; stricter S3-compatible stores reject them with `InvalidPart: All
//! non-trailing parts must have the same length`, which fails any object over a
//! few MiB.
//!
//! This module wraps that storage so every data-file writer buffers to a fixed
//! [`PART_SIZE`] boundary before forwarding bytes to OpenDAL. Each forwarded
//! write is exactly `PART_SIZE`, so OpenDAL emits equal-size parts and only the
//! final part is short — valid on S3 and required by stricter S3-compatible stores. Equal parts are correct
//! for both backends, so there is no backend-specific branch.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use iceberg::Result;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde::{Deserialize, Serialize};
use verglas_ring_proxy::EndpointPool;

/// Fixed multipart part size: 8 MiB. Comfortably above S3's 5 MiB minimum for
/// non-final parts and a clean power-of-two boundary. Every non-final part is
/// exactly this size; only the trailing part may be shorter.
pub const PART_SIZE: usize = 8 * 1024 * 1024;

/// A [`StorageFactory`] that builds an S3 storage whose data-file writers emit
/// fixed-size multipart parts. Operator construction is delegated to
/// [`OpenDalStorageFactory`]; only the writer path is wrapped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixedPartStorageFactory {
    /// The wrapped OpenDAL factory that builds the real S3 operator.
    inner: OpenDalStorageFactory,
    /// Every S3 ingress assigned to this database or lakehouse.
    endpoints: Vec<String>,
}

impl FixedPartStorageFactory {
    /// Wraps every ring endpoint built by `inner` so object placement and fixed
    /// multipart sizing apply below Iceberg FileIO.
    pub fn new(inner: OpenDalStorageFactory, endpoints: Vec<String>) -> Result<Self> {
        if !endpoints.is_empty() {
            validate_endpoint_pool(&endpoints)?;
        }
        Ok(Self { inner, endpoints })
    }
}

#[typetag::serde(name = "FixedPartStorageFactory")]
impl StorageFactory for FixedPartStorageFactory {
    /// Builds the inner OpenDAL storage and wraps it so multipart parts are
    /// fixed size.
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        if self.endpoints.is_empty() {
            let inner = self.inner.build(config)?;
            return Ok(Arc::new(FixedPartStorage { inner }));
        }
        let mut members = Vec::with_capacity(self.endpoints.len());
        for endpoint in &self.endpoints {
            let mut props = config.props().clone();
            props.insert(iceberg::io::S3_ENDPOINT.to_owned(), endpoint.clone());
            members.push(self.inner.build(&StorageConfig::from_props(props))?);
        }
        let inner = Arc::new(RingStorage::new(self.endpoints.clone(), members)?);
        Ok(Arc::new(FixedPartStorage { inner }))
    }
}

/// Validates that a workload has a complete, unambiguous endpoint pool.
fn validate_endpoint_pool(endpoints: &[String]) -> Result<()> {
    EndpointPool::new(endpoints.to_vec())
        .map(|_| ())
        .map_err(|error| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("invalid S3 endpoint pool: {error}"),
            )
        })
}

/// Iceberg storage that assigns every operation for one object to one cache ingress.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RingStorage {
    /// Stable placement policy shared by reads, writes, and multipart writers.
    pool: EndpointPool,
    /// Storage clients in the same order as the pool endpoints.
    members: Vec<Arc<dyn Storage>>,
}

impl RingStorage {
    /// Builds a storage pool with exactly one client per endpoint.
    fn new(endpoints: Vec<String>, members: Vec<Arc<dyn Storage>>) -> Result<Self> {
        if endpoints.len() != members.len() {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "the S3 endpoint and storage-client counts differ",
            ));
        }
        let pool = EndpointPool::new(endpoints).map_err(|error| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("invalid S3 endpoint pool: {error}"),
            )
        })?;
        Ok(Self { pool, members })
    }

    /// Returns the one storage client assigned to `path`.
    fn member(&self, path: &str) -> &Arc<dyn Storage> {
        &self.members[self.pool.index_for_path(path)]
    }
}

#[async_trait]
#[typetag::serde(name = "RingStorage")]
impl Storage for RingStorage {
    /// Checks existence through the object's assigned ingress.
    async fn exists(&self, path: &str) -> Result<bool> {
        self.member(path).exists(path).await
    }

    /// Loads metadata through the object's assigned ingress.
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.member(path).metadata(path).await
    }

    /// Reads the whole object through its assigned ingress.
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.member(path).read(path).await
    }

    /// Opens a range reader through the object's assigned ingress.
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.member(path).reader(path).await
    }

    /// Writes a one-shot object through its assigned ingress.
    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.member(path).write(path, bs).await
    }

    /// Opens a multipart writer on one ingress for the complete object lifetime.
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.member(path).writer(path).await
    }

    /// Deletes an object through the same ingress used by reads and writes.
    async fn delete(&self, path: &str) -> Result<()> {
        self.member(path).delete(path).await
    }

    /// Deletes a prefix through its stable assigned ingress.
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.member(path).delete_prefix(path).await
    }

    /// Creates an input handle bound to the object's assigned ingress.
    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.member(path).new_input(path)
    }

    /// Creates an output handle bound to the object's assigned ingress.
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.member(path).new_output(path)
    }
}

/// Storage that delegates every operation to an inner [`Storage`] except the
/// writer path, which it wraps so multipart parts are fixed size. Reads and
/// metadata are unaffected.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixedPartStorage {
    /// The wrapped OpenDAL storage that does the real I/O.
    inner: Arc<dyn Storage>,
}

#[async_trait]
#[typetag::serde(name = "FixedPartStorage")]
impl Storage for FixedPartStorage {
    /// Delegates to the inner storage.
    async fn exists(&self, path: &str) -> Result<bool> {
        self.inner.exists(path).await
    }

    /// Delegates to the inner storage.
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.inner.metadata(path).await
    }

    /// Delegates to the inner storage.
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.inner.read(path).await
    }

    /// Delegates to the inner storage.
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.inner.reader(path).await
    }

    /// Delegates to the inner storage. A one-shot `write` is a single PUT, not a
    /// multipart upload, so it needs no slicing.
    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.inner.write(path, bs).await
    }

    /// Wraps the inner writer so its multipart parts are fixed size.
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let inner = self.inner.writer(path).await?;
        Ok(Box::new(FixedPartWriter::new(inner, PART_SIZE)))
    }

    /// Delegates to the inner storage.
    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path).await
    }

    /// Delegates to the inner storage.
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.inner.delete_prefix(path).await
    }

    /// Reads are unaffected, so the input file may point at the inner storage.
    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.inner.new_input(path)
    }

    /// The output file must carry *this* storage so its writer is the fixed-part
    /// one. Pointing at the inner storage would bypass the slicing.
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// A [`FileWrite`] that buffers bytes to a fixed `part_size` boundary before
/// forwarding them to `inner`. Every forwarded write is exactly `part_size`
/// except the final one (flushed on [`FileWrite::close`]), so the underlying
/// multipart upload sees equal-size parts with only a short trailing part.
struct FixedPartWriter {
    /// The wrapped writer that performs the real multipart upload.
    inner: Box<dyn FileWrite>,
    /// The fixed size of every non-final part.
    part_size: usize,
    /// Bytes accumulated toward the next full part.
    buffer: BytesMut,
}

impl FixedPartWriter {
    /// Wraps `inner`, forwarding writes in `part_size`-byte parts.
    fn new(inner: Box<dyn FileWrite>, part_size: usize) -> Self {
        Self {
            inner,
            part_size,
            buffer: BytesMut::new(),
        }
    }
}

#[async_trait]
impl FileWrite for FixedPartWriter {
    /// Accumulates `bs` and forwards every whole `part_size` prefix to the inner
    /// writer, so each forwarded part is exactly `part_size`. Any remainder stays
    /// buffered for the next call or for `close`.
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.buffer.extend_from_slice(&bs);
        while self.buffer.len() >= self.part_size {
            let part = self.buffer.split_to(self.part_size).freeze();
            self.inner.write(part).await?;
        }
        Ok(())
    }

    /// Flushes the buffered remainder as the final (possibly short) part, then
    /// closes the inner writer to complete the upload.
    async fn close(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            let part = std::mem::take(&mut self.buffer).freeze();
            self.inner.write(part).await?;
        }
        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::io::MemoryStorage;
    use std::sync::Mutex;

    /// A [`FileWrite`] that records the size of every forwarded write so a test
    /// can assert the part sizing.
    struct RecordingWriter {
        /// Sizes of each forwarded write, in order.
        sizes: Arc<Mutex<Vec<usize>>>,
    }

    #[async_trait]
    impl FileWrite for RecordingWriter {
        /// Records the length of the forwarded part.
        async fn write(&mut self, bs: Bytes) -> Result<()> {
            self.sizes.lock().expect("lock").push(bs.len());
            Ok(())
        }

        /// Nothing to do on close for the recorder.
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// Feeds `writes` through a [`FixedPartWriter`] and returns the sizes of the
    /// parts it forwarded to the inner writer.
    async fn forwarded_part_sizes(part_size: usize, writes: &[usize]) -> Vec<usize> {
        let sizes = Arc::new(Mutex::new(Vec::new()));
        let recorder = RecordingWriter {
            sizes: Arc::clone(&sizes),
        };
        let mut writer = FixedPartWriter::new(Box::new(recorder), part_size);
        for &n in writes {
            writer
                .write(Bytes::from(vec![0u8; n]))
                .await
                .expect("write");
        }
        writer.close().await.expect("close");
        sizes.lock().expect("lock").clone()
    }

    /// A total larger than twice the part size, delivered in irregular chunks
    /// (as the Parquet writer does), is forwarded as equal-size parts with only
    /// the trailing part short. This is the strict-store requirement; before the fix
    /// OpenDAL forwarded the irregular chunks verbatim as variable-size parts.
    #[tokio::test]
    async fn slices_into_equal_parts_with_short_tail() {
        let part_size = 1024;
        // Irregular writes summing to 2.5 parts (2560 bytes).
        let writes = [700usize, 900, 960];
        let total: usize = writes.iter().sum();

        let sizes = forwarded_part_sizes(part_size, &writes).await;

        assert!(
            sizes.len() >= 2,
            "expected multiple parts for a >2x-part write, got {sizes:?}"
        );
        let (last, non_final) = sizes.split_last().expect("at least one part");
        for s in non_final {
            assert_eq!(
                *s, part_size,
                "every non-final part must equal part_size, got {sizes:?}"
            );
        }
        assert!(
            *last > 0 && *last <= part_size,
            "trailing part must be non-empty and no larger than part_size, got {sizes:?}"
        );
        assert_eq!(
            sizes.iter().sum::<usize>(),
            total,
            "all bytes must be forwarded, got {sizes:?}"
        );
    }

    /// A single write far larger than the part size is split into equal parts,
    /// not forwarded as one oversized part.
    #[tokio::test]
    async fn splits_one_large_write() {
        let part_size = 1024;
        let sizes = forwarded_part_sizes(part_size, &[part_size * 3 + 100]).await;
        assert_eq!(sizes, vec![1024, 1024, 1024, 100], "got {sizes:?}");
    }

    /// A total that is an exact multiple of the part size yields all equal parts
    /// and no empty trailing part.
    #[tokio::test]
    async fn exact_multiple_has_no_empty_tail() {
        let part_size = 1024;
        let sizes = forwarded_part_sizes(part_size, &[1500, 548]).await;
        assert_eq!(sizes, vec![1024, 1024], "got {sizes:?}");
    }

    /// A total smaller than the part size is forwarded as a single part.
    #[tokio::test]
    async fn small_object_is_one_part() {
        let part_size = 1024;
        let sizes = forwarded_part_sizes(part_size, &[200, 300]).await;
        assert_eq!(sizes, vec![500], "got {sizes:?}");
    }

    /// Every Iceberg object class is readable through the same ring member that
    /// accepted its write before any catalog pointer is involved.
    #[tokio::test]
    async fn ring_storage_covers_all_iceberg_file_io_objects() {
        let endpoints = (0..4)
            .map(|member| format!("http://cache-{member}:8333"))
            .collect::<Vec<_>>();
        let concrete = (0..4)
            .map(|_| Arc::new(MemoryStorage::new()))
            .collect::<Vec<_>>();
        let members = concrete
            .iter()
            .cloned()
            .map(|storage| storage as Arc<dyn Storage>)
            .collect();
        let ring = RingStorage::new(endpoints, members).expect("ring storage");
        let object_kinds = [
            "data",
            "delete",
            "manifest",
            "manifest-list",
            "metadata-json",
            "puffin",
        ];

        for object in 0..256 {
            let kind = object_kinds[object % object_kinds.len()];
            let path = format!("memory:///warehouse/db/table/{kind}/object-{object:04}.bin");
            let value = Bytes::from(format!("value-{object}"));
            ring.write(&path, value.clone()).await.expect("ring write");
            assert_eq!(ring.read(&path).await.expect("ring read"), value);

            let owners =
                futures::future::join_all(concrete.iter().map(|storage| storage.exists(&path)))
                    .await
                    .into_iter()
                    .filter(|exists| matches!(exists, Ok(true)))
                    .count();
            assert_eq!(owners, 1, "one ingress owns {path}");
        }

        let used = futures::future::join_all(concrete.iter().map(|storage| async move {
            for object in 0..256 {
                let kind = object_kinds[object % object_kinds.len()];
                let path = format!("memory:///warehouse/db/table/{kind}/object-{object:04}.bin");
                if storage.exists(&path).await.unwrap_or(false) {
                    return true;
                }
            }
            false
        }))
        .await;
        assert!(used.into_iter().all(|member| member));
    }
}
