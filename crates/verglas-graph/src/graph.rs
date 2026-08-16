//! The engine API surface: a `Graph` handle over a namespace's node and edge
//! tables, and a `GraphReader` that answers traversal queries from either the
//! Puffin index or a table scan.
//!
//! This is what the later graph-insert/query CLI+SDK task and the memory
//! pipeline call. Writes go through the one CAS commit path; a reader is opened
//! for a snapshot and transparently uses the adjacency index when it is bound to
//! that snapshot, falling back to a scan of the plain tables otherwise (the
//! turn-off path — correct, just slower). Nothing assumes local ownership: every
//! read and write goes through the catalog and the table's FileIO.

use std::{collections::HashMap, sync::Arc};

use iceberg::{Catalog, TableIdent};
use verglas_iceberg::parse_table_ident;
use verglas_iceberg::tables_api::{self, CommitRequest};
use verglas_iceberg::write;

use crate::csr::AdjacencyIndex;
use crate::error::Result;
use crate::index::{self, IndexBuildReport};
use crate::model::{Direction, Edge, Neighbor, Node, Path, Reached, Subgraph};
use crate::scan::{latest_nodes_by_id, live_edges, load_edges, load_nodes};
use crate::schema::{
    EDGES_TABLE, NODES_TABLE, edge_to_row, edges_schema, node_to_row, nodes_schema,
};
use crate::traverse::{self, TraversalFilter};

/// A handle to a property graph stored as a nodes table and an edges table in
/// one namespace.
#[derive(Clone)]
pub struct Graph {
    /// The catalog every operation goes through.
    catalog: Arc<dyn Catalog>,
    /// The nodes table identifier.
    nodes_ident: TableIdent,
    /// The edges (triplet) table identifier.
    edges_ident: TableIdent,
}

/// Which path served a reader's queries — the bound Puffin index, or a table
/// scan. Exposed so callers (and tests) can confirm the fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderBackend {
    /// Served from the snapshot-bound `verglas-graph-adjacency-v1` Puffin index.
    Index,
    /// Served from a scan of the edge table (no usable index for the snapshot).
    Scan,
}

impl Graph {
    /// Opens the graph in `namespace`: its tables are `<namespace>.nodes` and
    /// `<namespace>.edges`. Does not create them — call [`Graph::ensure_tables`].
    pub fn open(catalog: Arc<dyn Catalog>, namespace: &str) -> Result<Self> {
        let nodes_ident = parse_table_ident(&format!("{namespace}.{NODES_TABLE}"))?;
        let edges_ident = parse_table_ident(&format!("{namespace}.{EDGES_TABLE}"))?;
        Ok(Self {
            catalog,
            nodes_ident,
            edges_ident,
        })
    }

    /// The edges (triplet) table identifier — the one the index builder binds to.
    pub fn edges_ident(&self) -> &TableIdent {
        &self.edges_ident
    }

    /// The nodes table identifier.
    pub fn nodes_ident(&self) -> &TableIdent {
        &self.nodes_ident
    }

    /// Creates the node and edge tables if they do not already exist. Idempotent:
    /// an existing table is left as is.
    pub async fn ensure_tables(&self) -> Result<()> {
        self.ensure_tables_with_properties(HashMap::new()).await
    }

    /// Creates both graph tables with the same durable graph metadata.
    pub async fn ensure_tables_with_properties(
        &self,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.ensure_table(&self.nodes_ident, &nodes_schema(), &properties)
            .await?;
        self.ensure_table(&self.edges_ident, &edges_schema(), &properties)
            .await?;
        Ok(())
    }

    /// Creates one table from its schema when the catalog does not have it yet.
    async fn ensure_table(
        &self,
        ident: &TableIdent,
        schema: &arrow_schema::SchemaRef,
        properties: &HashMap<String, String>,
    ) -> Result<()> {
        if self.catalog.load_table(ident).await.is_ok() {
            return Ok(());
        }
        write::create_table_with_partitions_and_properties(
            self.catalog.as_ref(),
            ident,
            schema,
            &[],
            properties.clone(),
        )
        .await?;
        Ok(())
    }

    /// Appends nodes and returns the new nodes-table snapshot id (or `None` when
    /// no rows were given).
    pub async fn insert_nodes(&self, nodes: &[Node]) -> Result<Option<i64>> {
        let rows = nodes.iter().map(node_to_row).collect();
        self.commit_rows(&self.nodes_ident, rows).await
    }

    /// Appends edges and returns the new edges-table snapshot id (or `None` when
    /// no rows were given). That snapshot is what an index build binds to and
    /// what a reader reads as of.
    pub async fn insert_edges(&self, edges: &[Edge]) -> Result<Option<i64>> {
        let rows = edges.iter().map(edge_to_row).collect();
        self.commit_rows(&self.edges_ident, rows).await
    }

    /// Commits JSON rows through the engine's one CAS append path and returns the
    /// resulting snapshot id.
    async fn commit_rows(
        &self,
        ident: &TableIdent,
        rows: Vec<serde_json::Value>,
    ) -> Result<Option<i64>> {
        let response = tables_api::commit(
            self.catalog.as_ref(),
            ident,
            CommitRequest {
                rows,
                idempotency_key: None,
            },
        )
        .await?;
        Ok(response.snapshot_id.parse::<i64>().ok())
    }

    /// The current edges-table snapshot id, or `None` when the table has no data.
    pub async fn current_edges_snapshot(&self) -> Result<Option<i64>> {
        let table = self.catalog.load_table(&self.edges_ident).await?;
        Ok(table.metadata().current_snapshot_id())
    }

    /// Loads every node as of a snapshot (the current one when `at` is
    /// `None`), reduced to one row per id (`PutNodes` is append-only, so a
    /// repeated id is the node's latest edit — see [`latest_nodes_by_id`]).
    /// Used by precedent search (#148) to read `Decision` node objectives;
    /// there is no adjacency index for node properties, so this always scans
    /// the plain nodes table.
    pub async fn load_nodes(&self, at: Option<i64>) -> Result<Vec<Node>> {
        let (nodes, _snapshot) = load_nodes(self.catalog.clone(), &self.nodes_ident, at).await?;
        Ok(latest_nodes_by_id(nodes))
    }

    /// Builds the adjacency index as of a snapshot (the current one when `at` is
    /// `None`) and binds it as a Puffin blob. The index-builder seam.
    pub async fn build_index(&self, at: Option<i64>) -> Result<Option<IndexBuildReport>> {
        index::build_adjacency_index(self.catalog.clone(), &self.edges_ident, at).await
    }

    /// Opens a reader for a snapshot (the current one when `at` is `None`),
    /// preferring the bound Puffin index and falling back to a table scan when
    /// none is usable. The result is the same either way.
    pub async fn reader(&self, at: Option<i64>) -> Result<GraphReader> {
        let snapshot = match self.resolve_snapshot(at).await? {
            Some(id) => id,
            None => return Ok(GraphReader::empty()),
        };
        // Prefer the index; a missing or corrupt one falls back to the scan.
        match index::load_adjacency_index(self.catalog.clone(), &self.edges_ident, snapshot).await {
            Ok(Some(indexed)) => Ok(GraphReader::new(indexed, ReaderBackend::Index)),
            _ => self.scan_reader(Some(snapshot)).await,
        }
    }

    /// Opens a reader that always scans the edge table (the source-of-truth /
    /// turn-off path), ignoring any bound index. Same results as [`Graph::reader`],
    /// just without the index.
    pub async fn reader_scan_only(&self, at: Option<i64>) -> Result<GraphReader> {
        self.scan_reader(at).await
    }

    /// Builds a scan-backed reader over the live edge set as of a snapshot.
    async fn scan_reader(&self, at: Option<i64>) -> Result<GraphReader> {
        let (edges, snapshot) = load_edges(self.catalog.clone(), &self.edges_ident, at).await?;
        let Some(snapshot) = snapshot else {
            return Ok(GraphReader::empty());
        };
        let live = live_edges(edges);
        let index = AdjacencyIndex::from_edges(&live, snapshot);
        Ok(GraphReader::new(index, ReaderBackend::Scan))
    }

    /// Resolves the snapshot to read: the requested one, or the edge table's
    /// current snapshot, or `None` when empty.
    async fn resolve_snapshot(&self, at: Option<i64>) -> Result<Option<i64>> {
        match at {
            Some(id) => Ok(Some(id)),
            None => self.current_edges_snapshot().await,
        }
    }
}

/// A reader over one snapshot of the graph. It holds a materialized
/// [`AdjacencyIndex`] — decoded from Puffin or built from a scan — and answers
/// traversal queries over it.
pub struct GraphReader {
    /// The materialized adjacency structure.
    index: AdjacencyIndex,
    /// Which path produced it.
    backend: ReaderBackend,
}

impl GraphReader {
    /// Wraps a materialized index and records which path produced it.
    fn new(index: AdjacencyIndex, backend: ReaderBackend) -> Self {
        Self { index, backend }
    }

    /// A reader over an empty graph (no snapshot yet). Reports the scan backend.
    fn empty() -> Self {
        Self {
            index: AdjacencyIndex::from_edges(&[], 0),
            backend: ReaderBackend::Scan,
        }
    }

    /// Which path served this reader.
    pub fn backend(&self) -> ReaderBackend {
        self.backend
    }

    /// The snapshot the reader reflects.
    pub fn snapshot_id(&self) -> i64 {
        self.index.snapshot_id()
    }

    /// The number of distinct nodes with at least one incident edge.
    pub fn node_count(&self) -> usize {
        self.index.node_count()
    }

    /// Direct neighbors of a node in a direction, filtered by predicate, each
    /// with its edge receipt.
    pub fn get_neighbors(
        &self,
        node_id: &str,
        direction: Direction,
        filter: &TraversalFilter,
    ) -> Vec<Neighbor> {
        traverse::get_neighbors(&self.index, node_id, direction, filter)
    }

    /// Every node reached within `k` hops of `start`, with hop distance and path
    /// confidence.
    pub fn k_hop(
        &self,
        start: &str,
        k: u32,
        direction: Direction,
        filter: &TraversalFilter,
    ) -> Vec<Reached> {
        traverse::k_hop(&self.index, start, k, direction, filter)
    }

    /// The bounded neighborhood within `k` hops: reached nodes and traversed
    /// edges.
    pub fn neighborhood(
        &self,
        start: &str,
        k: u32,
        direction: Direction,
        filter: &TraversalFilter,
    ) -> Subgraph {
        traverse::neighborhood(&self.index, start, k, direction, filter)
    }

    /// The shortest path from `src` to `dst` within `max_hops`, if any.
    pub fn paths(
        &self,
        src: &str,
        dst: &str,
        max_hops: u32,
        direction: Direction,
        filter: &TraversalFilter,
    ) -> Vec<Path> {
        traverse::paths(&self.index, src, dst, max_hops, direction, filter)
    }
}
