//! # verglas-vector — a real-time-maintained ANN index for Verglas
//!
//! A streaming Vamana (DiskANN) vector index over an embedding field of a
//! Verglas table or graph node table, maintained incrementally by a
//! materialized view and served from the cluster-local shadow store.
//!
//! ## What this crate is
//!
//! - [`VamanaIndex`] — the pure engine ([`vamana`]): batch build, `search`
//!   (GreedySearch), streaming `insert` (RobustPrune + backward-edge repair),
//!   lazy `delete`, and `consolidate` (FreshDiskANN StreamingMerge /
//!   delete-consolidation). No IO, hermetic, deterministic.
//! - [`codec`] — the `verglas-vamana-v1` binary layout (flat CSR).
//! - [`puffin`] — wraps/unwraps that layout in a Puffin blob, reusing the
//!   upstream `iceberg::puffin` writer/reader (the same container the graph
//!   adjacency index uses).
//! - [`store`] — the [`store::ShadowBlobStore`] trait and two implementations
//!   (in-memory; local directory under `cache.dir`). Placement is injected so
//!   the engine stays testable.
//! - [`maintenance`] — the index-maintenance MV: reads the source table's
//!   Iceberg delta since its watermark, applies inserts/deletes, consolidates on
//!   a threshold, and persists the updated blob. The MV's watermark IS the
//!   snapshot the index reflects.
//! - [`arrow`] — extracting `(id, vector)` rows from Arrow batches.
//!
//! ## Algorithmic basis
//!
//! - GreedySearch / RobustPrune / the `alpha`,`R`,`L` parameters / the medoid
//!   entry point — Subramanya et al., *DiskANN* (NeurIPS 2019).
//! - Streaming insert, lazy delete + delete-consolidation — Singh et al.,
//!   *FreshDiskANN* (arXiv:2105.09613, 2021).
//! - Puffin-as-container for the shard layout — Borycki, arXiv:2606.04196 (the
//!   whitepaper's reference [7]); the binding, however, **diverges** (see next).
//!
//! ## Placement divergence from issue #91 (deliberate, operator-directed)
//!
//! #91 commits the index blob to the source table's Iceberg snapshot via a
//! `StatisticsFile`, distributed through the catalog. This crate does NOT. A
//! vector index over a customer table is a derived, cluster-local *serving*
//! artifact (#90: NVMe/cluster bound), and the standing invariant forbids
//! Verglas writing customer tables autonomously (#95). So the same Puffin blob
//! is produced but persisted to the cluster-local shadow store, keyed by
//! `(table-or-graph, field, reflected-snapshot)` — never referenced from the
//! source snapshot or catalog. See [`puffin`] and [`store`].
//!
//! ## Dependency decision
//!
//! Depends on `iceberg` (Puffin container, memory FileIO, delta scan) +
//! `arrow-*` + `verglas-iceberg` (`tables_api` delta/rows). It does NOT depend
//! on `verglas-cache`: the shadow store is injected through the
//! [`store::ShadowBlobStore`] trait, so the daemon wires real placement while
//! tests use the in-memory store.

pub mod arrow;
pub mod codec;
pub mod error;
pub mod maintenance;
pub mod metric;
pub mod puffin;
pub mod service;
pub mod store;
pub mod vamana;

pub use codec::BLOB_TYPE;
pub use error::{Result, VectorError};
pub use maintenance::{IdEncoding, MaintenanceConfig, uuid_hash_id};
pub use metric::Metric;
pub use store::{InMemoryShadowStore, IndexKey, LocalDirShadowStore, ShadowBlobStore, StoredBlob};
pub use vamana::{DEFAULT_ALPHA, DEFAULT_L, DEFAULT_R, Neighbor, VamanaIndex, VamanaParams};

/// Brute-force k-nearest search over `(id, vector)` rows under `metric` — the
/// ground truth for recall tests and the fallback the daemon serves when a field
/// has no index (the turn-off path). Returns nearest-first `(id, distance)`.
pub fn brute_force_search(
    metric: Metric,
    dim: usize,
    rows: &[(i64, Vec<f32>)],
    query: &[f32],
    k: usize,
) -> Result<Vec<Neighbor>> {
    if query.len() != dim {
        return Err(VectorError::DimMismatch {
            expected: dim,
            got: query.len(),
        });
    }
    let q = metric.prepare(query);
    let mut scored: Vec<Neighbor> = rows
        .iter()
        .filter(|(_, v)| v.len() == dim)
        .map(|(id, v)| {
            let prepared = metric.prepare(v);
            Neighbor {
                id: *id,
                distance: metric.report_distance(&q, &prepared),
            }
        })
        .collect();
    scored.sort_by(|a, b| a.distance.total_cmp(&b.distance));
    scored.truncate(k);
    Ok(scored)
}
