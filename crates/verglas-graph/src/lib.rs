//! The graph-over-Iceberg engine (#258).
//!
//! A directed labeled property graph engineered on Iceberg tables with a
//! snapshot-bound Puffin adjacency index. Two append-only tables — nodes and
//! edges (triplets: subject `src_id`, predicate, object `dst_id`, plus
//! confidence, provenance, and a `supersedes` belief-revision pointer) — are the
//! source of truth; a CSR adjacency structure serialized into a
//! `verglas-graph-adjacency-v1` Puffin blob, bound to the edge table's snapshot,
//! makes K-hop traversal read an index instead of the edge table. When no index
//! is bound (or it is stale/corrupt), traversal falls back to a scan of the
//! plain tables and returns the same answers — the standing turn-off test.
//!
//! # Crate placement
//!
//! This is a separate crate, `verglas-graph`, depending only on
//! `verglas-iceberg` (plus `iceberg`/`arrow`). It is a pure engine with zero
//! cache or server dependencies, so any process that already depends on
//! `verglas-iceberg` can embed it. It is not folded into `verglas-iceberg`
//! because the
//! graph model (nodes/edges/CSR/traversal) is a distinct capability with its own
//! surface; keeping it a sibling crate leaves `verglas-iceberg` the minimal table
//! engine and gives the later CLI/SDK/pipeline a cleanly separable dependency.
//!
//! # The engine API the next tasks call
//!
//! - [`Graph`] — open over a namespace, `ensure_tables`, `insert_nodes`,
//!   `insert_edges`, `build_index` (the index-builder seam), and open a
//!   [`GraphReader`].
//! - [`GraphReader`] — `get_neighbors`, `k_hop`, `neighborhood`, `paths`, over
//!   the index or the scan fallback transparently.
//! - [`index::build_adjacency_index`] — the standalone index-builder function a
//!   platform MV/index-builder Job will drive later (the Job is not wired here).
//!
//! No memory-pipeline wiring and no CLI/SDK surface live here — those are the
//! next two tasks.

pub mod csr;
pub mod error;
pub mod graph;
pub mod index;
pub mod model;
pub mod precedent;
pub mod scan;
pub mod schema;
pub mod traverse;

pub use error::{GraphError, Result};
pub use graph::{Graph, GraphReader, ReaderBackend};
pub use index::{BLOB_TYPE, IndexBuildMode, IndexBuildReport, build_adjacency_index};
pub use model::{Direction, Edge, Neighbor, Node, Path, Reached, Subgraph, TripletReceipt};
pub use precedent::{BM25_B, BM25_K1, bm25_scores, tokenize};
pub use traverse::TraversalFilter;
