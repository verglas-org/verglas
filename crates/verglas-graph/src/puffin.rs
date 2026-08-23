//! In-memory Puffin framing for live DO graph adjacency artifacts.

use std::collections::HashMap;

use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter};

use crate::csr::AdjacencyIndex;
use crate::error::{GraphError, Result};
use crate::index::BLOB_TYPE;

const MEM_PATH: &str = "memory:///verglas-graph-adjacency.puffin";

/// Serializes one adjacency index into an Iceberg Puffin file.
pub async fn to_puffin_bytes(index: &AdjacencyIndex) -> Result<Vec<u8>> {
    let io = FileIO::new_with_memory();
    let output = io.new_output(MEM_PATH)?;
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false).await?;
    let blob = Blob::builder()
        .r#type(BLOB_TYPE.to_owned())
        .fields(Vec::new())
        .snapshot_id(index.snapshot_id())
        .sequence_number(0)
        .data(index.encode())
        .properties(HashMap::new())
        .build();
    writer.add(blob, CompressionCodec::zstd_default()).await?;
    writer.close().await?;
    let bytes = io.new_input(MEM_PATH)?.read().await?;
    let _ = io.delete(MEM_PATH).await;
    Ok(bytes.to_vec())
}

/// Decodes one graph adjacency blob from an Iceberg Puffin file.
pub async fn from_puffin_bytes(bytes: &[u8]) -> Result<AdjacencyIndex> {
    let io = FileIO::new_with_memory();
    io.new_output(MEM_PATH)?
        .write(Bytes::copy_from_slice(bytes))
        .await?;
    let input = io.new_input(MEM_PATH)?;
    let reader = PuffinReader::new(input);
    let metadata = reader.file_metadata().await?;
    let blob_metadata = metadata
        .blobs()
        .iter()
        .find(|blob| blob.blob_type() == BLOB_TYPE)
        .ok_or_else(|| GraphError::CorruptIndex("graph Puffin blob is missing".to_owned()))?;
    let blob = reader.blob(blob_metadata).await?;
    let index = AdjacencyIndex::decode(blob.data())?;
    let _ = io.delete(MEM_PATH).await;
    Ok(index)
}
