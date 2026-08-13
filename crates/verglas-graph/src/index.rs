//! Building the CSR adjacency index into a Puffin sidecar bound to the edge
//! table's snapshot, and loading it back.
//!
//! This reuses the existing Iceberg Puffin plumbing exactly: the blob is written
//! with `PuffinWriter` and bound to the snapshot through a `StatisticsFile`
//! entry (`UpdateStatisticsAction`) — the same snapshot-keyed attach path
//! deletion-vector and statistics blobs use. The blob type is
//! `verglas-graph-adjacency-v1`.
//!
//! [`build_adjacency_index`] is the index-builder seam: a callable engine
//! function the platform's MV/index-builder Job will drive later (§7.7). The Job
//! wiring is intentionally not here.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter};
use iceberg::spec::{BlobMetadata, StatisticsFile};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};

use crate::csr::AdjacencyIndex;
use crate::error::Result;
use crate::scan::{live_edges, load_edges};

/// The Puffin blob type for the CSR adjacency index. The `v1` suffix is a format
/// extension point only; per AGENTS.md no version negotiation is built — a break
/// rebuilds this derived artifact.
pub const BLOB_TYPE: &str = "verglas-graph-adjacency-v1";

/// How the index is (re)built. Full today; incremental is the noted extension
/// point — because the edge table is append-only, the edges since the last
/// indexed snapshot are exactly the new data files, so a future incremental
/// build merges that delta into the prior CSR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexBuildMode {
    /// Rebuild the whole CSR from the live edge set at the target snapshot.
    Full,
}

/// The outcome of an index build: what snapshot it bound to, the graph size, and
/// the blob it wrote.
#[derive(Debug, Clone)]
pub struct IndexBuildReport {
    /// The edge-table snapshot the index is bound to.
    pub snapshot_id: i64,
    /// The number of distinct nodes indexed.
    pub node_count: usize,
    /// The number of live edges indexed.
    pub edge_count: usize,
    /// The path of the Puffin blob written.
    pub blob_path: String,
    /// The size of the Puffin file in bytes.
    pub blob_bytes: u64,
    /// How the index was built.
    pub mode: IndexBuildMode,
}

/// How many commit attempts the statistics attach gets before a conflict
/// surfaces. Mirrors the write path's optimistic-concurrency retry.
const COMMIT_ATTEMPTS: u32 = 8;

/// Builds the adjacency index for `edges_ident` as of a snapshot (the current
/// one when `at` is `None`), writes it as a `verglas-graph-adjacency-v1` Puffin
/// blob, and binds it to the snapshot. Returns `None` when the table has no
/// snapshot yet (nothing to index).
///
/// This is the engine function the memory pipeline and a future index-builder
/// Job call; it does not assume local ownership — reads and writes go through
/// the catalog and the table's FileIO.
pub async fn build_adjacency_index(
    catalog: Arc<dyn Catalog>,
    edges_ident: &TableIdent,
    at: Option<i64>,
) -> Result<Option<IndexBuildReport>> {
    let (edges, snapshot) = load_edges(catalog.clone(), edges_ident, at).await?;
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    let live = live_edges(edges);
    let index = AdjacencyIndex::from_edges(&live, snapshot);
    let bytes = index.encode();

    let table = catalog.load_table(edges_ident).await?;
    let sequence_number = table
        .metadata()
        .snapshot_by_id(snapshot)
        .map(|s| s.sequence_number())
        .unwrap_or(0);

    // A unique path beside the table metadata.
    let blob_path = format!(
        "{}/metadata/verglas-graph-{}-{}.puffin",
        table.metadata().location(),
        snapshot,
        uuid::Uuid::new_v4()
    );

    let output = table.file_io().new_output(&blob_path)?;
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false).await?;
    let blob = Blob::builder()
        .r#type(BLOB_TYPE.to_string())
        .fields(Vec::new())
        .snapshot_id(snapshot)
        .sequence_number(sequence_number)
        .data(bytes)
        .properties(HashMap::new())
        .build();
    writer.add(blob, CompressionCodec::Zstd).await?;
    writer.close().await?;

    let (file_size, footer_size) = puffin_sizes(&table, &blob_path).await?;
    let statistics = StatisticsFile {
        snapshot_id: snapshot,
        statistics_path: blob_path.clone(),
        file_size_in_bytes: file_size,
        file_footer_size_in_bytes: footer_size,
        key_metadata: None,
        blob_metadata: vec![BlobMetadata {
            r#type: BLOB_TYPE.to_string(),
            snapshot_id: snapshot,
            sequence_number,
            fields: Vec::new(),
            properties: HashMap::new(),
        }],
    };

    attach_statistics(catalog.as_ref(), &table, statistics).await?;

    Ok(Some(IndexBuildReport {
        snapshot_id: snapshot,
        node_count: index.node_count(),
        edge_count: live.len(),
        blob_path,
        blob_bytes: file_size as u64,
        mode: IndexBuildMode::Full,
    }))
}

/// Reads the written Puffin file back to report its total size and footer size
/// for the `StatisticsFile`. The footer payload length is stored just before the
/// trailing flags and magic; these fields are informational (readers parse the
/// footer independently).
async fn puffin_sizes(table: &iceberg::table::Table, path: &str) -> Result<(i64, i64)> {
    let bytes = table.file_io().new_input(path)?.read().await?;
    let file_size = bytes.len() as i64;
    // Tail layout: [payload_len(4)][flags(4)][MAGIC(4)]. Footer = leading
    // MAGIC(4) + payload + those 12 trailing bytes.
    let footer_size = if bytes.len() >= 12 {
        let start = bytes.len() - 12;
        let payload_len = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
        i64::from(payload_len) + 16
    } else {
        file_size
    };
    Ok((file_size, footer_size))
}

/// Commits the `StatisticsFile` binding with optimistic-concurrency retry, so a
/// concurrent table writer does not lose the index attach.
async fn attach_statistics(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    statistics: StatisticsFile,
) -> Result<()> {
    let mut current = table.clone();
    let mut attempt = 1u32;
    loop {
        let tx = Transaction::new(&current);
        let tx = tx
            .update_statistics()
            .set_statistics(statistics.clone())
            .apply(tx)?;
        match tx.commit(catalog).await {
            Ok(_) => return Ok(()),
            Err(e)
                if e.kind() == iceberg::ErrorKind::CatalogCommitConflicts
                    && attempt < COMMIT_ATTEMPTS =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(100 * u64::from(attempt)))
                    .await;
                current = catalog.load_table(table.identifier()).await?;
                attempt += 1;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Loads the adjacency index bound to `snapshot` of `edges_ident`, or `None`
/// when no `verglas-graph-adjacency-v1` blob is bound to it. A present-but-corrupt
/// blob is a [`crate::error::GraphError::CorruptIndex`]; the caller then falls
/// back to a scan (the turn-off path).
pub async fn load_adjacency_index(
    catalog: Arc<dyn Catalog>,
    edges_ident: &TableIdent,
    snapshot: i64,
) -> Result<Option<AdjacencyIndex>> {
    let table = catalog.load_table(edges_ident).await?;
    let Some(stat) = table.metadata().statistics_for_snapshot(snapshot) else {
        return Ok(None);
    };
    if !stat.blob_metadata.iter().any(|b| b.r#type == BLOB_TYPE) {
        return Ok(None);
    }

    let input = table.file_io().new_input(&stat.statistics_path)?;
    let reader = PuffinReader::new(input);
    let file_metadata = reader.file_metadata().await?;
    let Some(blob_meta) = file_metadata
        .blobs()
        .iter()
        .find(|b| b.blob_type() == BLOB_TYPE)
    else {
        return Ok(None);
    };
    let blob = reader.blob(blob_meta).await?;
    let index = AdjacencyIndex::decode(blob.data())?;
    Ok(Some(index))
}
