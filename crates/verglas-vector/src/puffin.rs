//! Puffin container framing for the Vamana index.
//!
//! Issue #95's standing rule — "any persisted artifact that is (a) derived from
//! a specific table snapshot and (b) outlives one process is serialized as a
//! Puffin blob with a versioned `verglas-*` blob type" — applies to the vector
//! index. We reuse the upstream `iceberg::puffin` writer/reader exactly as the
//! graph adjacency index does (`crates/verglas-graph/src/index.rs`), so the
//! index is a first-class Puffin artifact inspectable by the same tooling.
//!
//! **Divergence from #91 (deliberate, operator-directed).** #91 binds the index
//! blob to the source table's Iceberg snapshot via a `StatisticsFile` and
//! commits it through the catalog. We do NOT: the index is a cluster-local,
//! derived, serving artifact (#90 says a serving ANN index is NVMe/cluster
//! bound), and the standing invariant forbids Verglas writing customer tables
//! autonomously (#95). So the same Puffin blob is produced here but persisted to
//! the cluster-local shadow store (see `store.rs`), keyed by
//! `(table-or-graph, field, reflected-snapshot)`, and never referenced from the
//! source snapshot or the catalog. The blob still carries its `snapshot_id`
//! (the reflected watermark) so it is self-describing about what it indexes.

use std::collections::HashMap;

use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter};

pub use crate::codec::BLOB_TYPE;
use crate::error::{Result, VectorError};
use crate::vamana::VamanaIndex;

/// A fixed in-memory path used to round-trip the Puffin bytes through the
/// upstream writer/reader without touching disk. The shadow store owns real
/// placement.
const MEM_PATH: &str = "memory:///verglas-vamana.puffin";

/// Serializes `index` into a complete Puffin file (as bytes) carrying one
/// `verglas-vamana-v1` blob. `properties` become Puffin blob properties (metric,
/// dim, target, field) so `verglas artifacts inspect` can render it without
/// decoding the graph. The blob's `snapshot_id` is the index's reflected
/// snapshot.
pub async fn to_puffin_bytes(
    index: &VamanaIndex,
    properties: HashMap<String, String>,
) -> Result<Vec<u8>> {
    let body = crate::codec::encode(index);
    let io = FileIO::new_with_memory();
    let output = io
        .new_output(MEM_PATH)
        .map_err(|e| VectorError::Puffin(e.to_string()))?;
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
        .await
        .map_err(|e| VectorError::Puffin(e.to_string()))?;
    let blob = Blob::builder()
        .r#type(BLOB_TYPE.to_string())
        .fields(Vec::new())
        .snapshot_id(index.reflected_snapshot())
        .sequence_number(0)
        .data(body)
        .properties(properties)
        .build();
    writer
        .add(blob, CompressionCodec::Zstd)
        .await
        .map_err(|e| VectorError::Puffin(e.to_string()))?;
    writer
        .close()
        .await
        .map_err(|e| VectorError::Puffin(e.to_string()))?;
    let input = io
        .new_input(MEM_PATH)
        .map_err(|e| VectorError::Puffin(e.to_string()))?;
    let read = input
        .read()
        .await
        .map_err(|e| VectorError::Puffin(e.to_string()))?;
    let _ = io.delete(MEM_PATH).await;
    Ok(read.to_vec())
}

/// Parses a Puffin file (bytes) and decodes its `verglas-vamana-v1` blob back
/// into an index. A missing blob or a corrupt body is an error, so the caller
/// falls back to brute force (the turn-off path).
pub async fn from_puffin_bytes(bytes: &[u8]) -> Result<VamanaIndex> {
    let io = FileIO::new_with_memory();
    let output = io
        .new_output(MEM_PATH)
        .map_err(|e| VectorError::Puffin(e.to_string()))?;
    output
        .write(Bytes::from(bytes.to_vec()))
        .await
        .map_err(|e| VectorError::Puffin(e.to_string()))?;
    let input = io
        .new_input(MEM_PATH)
        .map_err(|e| VectorError::Puffin(e.to_string()))?;
    let reader = PuffinReader::new(input);
    let file_metadata = reader
        .file_metadata()
        .await
        .map_err(|e| VectorError::Puffin(e.to_string()))?;
    let blob_meta = file_metadata
        .blobs()
        .iter()
        .find(|b| b.blob_type() == BLOB_TYPE)
        .ok_or_else(|| VectorError::CorruptBlob("no verglas-vamana-v1 blob".to_owned()))?;
    let blob = reader
        .blob(blob_meta)
        .await
        .map_err(|e| VectorError::Puffin(e.to_string()))?;
    let index = crate::codec::decode(blob.data())?;
    let _ = io.delete(MEM_PATH).await;
    Ok(index)
}
