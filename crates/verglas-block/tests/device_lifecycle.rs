//! Integration tests mapping to issue #382's acceptance criteria, driven
//! through the real `object_store` durability adapter (`InMemory`), not the
//! crate-internal test backend.
//!
//! Covered: two devices sharing content dedup in the backend; writes flushed on
//! one "box" read back from another box attaching the same device over the same
//! backend; and only written chunks are stored (zero ranges are sparse).

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;

use verglas_block::{BlockDevice, CHUNK_SIZE_BYTES, ChunkStore, ObjectBackend, ObjectStoreBackend};

/// The number of stored chunk objects under the `blocks/` prefix.
async fn chunk_object_count(store: &Arc<InMemory>) -> usize {
    let mut stream = store.list(Some(&Path::from("blocks")));
    let mut count = 0;
    while let Some(entry) = stream.next().await {
        entry.expect("list entry");
        count += 1;
    }
    count
}

/// Opens a device on a fresh local NVMe dir over the shared backend.
async fn open_box(
    id: &str,
    size: u64,
    backend: Arc<dyn ObjectBackend>,
) -> (BlockDevice, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ChunkStore::open(dir.path(), backend).await.expect("store");
    let dev = BlockDevice::ensure(id, size, store).await.expect("ensure");
    (dev, dir)
}

#[tokio::test]
async fn two_devices_sharing_content_dedup_in_the_backend() {
    let object_store = Arc::new(InMemory::new());
    let backend: Arc<dyn ObjectBackend> = Arc::new(ObjectStoreBackend::new(object_store.clone()));

    // Identical content written to the first chunk of two independent devices.
    let payload = vec![0xABu8; CHUNK_SIZE_BYTES as usize];
    let (dev_a, _a) = open_box("dev-a", CHUNK_SIZE_BYTES, backend.clone()).await;
    let (dev_b, _b) = open_box("dev-b", CHUNK_SIZE_BYTES, backend.clone()).await;
    dev_a.write_at(0, &payload).await.expect("write a");
    dev_b.write_at(0, &payload).await.expect("write b");
    dev_a.flush().await.expect("flush a");
    dev_b.flush().await.expect("flush b");

    assert_eq!(
        chunk_object_count(&object_store).await,
        1,
        "identical chunks share one backend object"
    );
}

#[tokio::test]
async fn flushed_writes_readable_from_another_box() {
    let object_store = Arc::new(InMemory::new());
    let backend: Arc<dyn ObjectBackend> = Arc::new(ObjectStoreBackend::new(object_store.clone()));

    let (dev_a, _a) = open_box("shared", 4 * CHUNK_SIZE_BYTES, backend.clone()).await;
    dev_a
        .write_at(CHUNK_SIZE_BYTES + 123, b"cross-box payload")
        .await
        .expect("write");
    dev_a.flush().await.expect("flush");

    // A different box: a fresh local NVMe dir, same device id and backend.
    let (dev_b, _b) = open_box("shared", 4 * CHUNK_SIZE_BYTES, backend.clone()).await;
    let read = dev_b
        .read_at(CHUNK_SIZE_BYTES + 123, b"cross-box payload".len() as u64)
        .await
        .expect("read b");
    assert_eq!(read, Bytes::from_static(b"cross-box payload"));
}

#[tokio::test]
async fn only_written_chunks_are_stored() {
    let object_store = Arc::new(InMemory::new());
    let backend: Arc<dyn ObjectBackend> = Arc::new(ObjectStoreBackend::new(object_store.clone()));

    // An 8-chunk device with writes to only two chunks (0 and 5).
    let (dev, _dir) = open_box("sparse", 8 * CHUNK_SIZE_BYTES, backend.clone()).await;
    dev.write_at(0, b"first").await.expect("w0");
    dev.write_at(5 * CHUNK_SIZE_BYTES, b"sixth")
        .await
        .expect("w5");
    dev.flush().await.expect("flush");

    assert_eq!(
        chunk_object_count(&object_store).await,
        2,
        "only the two written chunks are stored; the six zero chunks are sparse"
    );
    // The unwritten chunks still read back as zeros.
    assert_eq!(
        dev.read_at(3 * CHUNK_SIZE_BYTES, 16)
            .await
            .expect("read hole"),
        Bytes::from(vec![0u8; 16])
    );
}
