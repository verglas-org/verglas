//! Live graph edge state and deterministic adjacency projection.

use std::collections::BTreeMap;

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use verglas_graph::csr::AdjacencyIndex;
use verglas_graph::{Direction, Edge, Neighbor};

use crate::error::{Error, Result};
use crate::transaction::TableId;

/// Declares one standard edge table as a live graph projection source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphIndexConfig {
    table: TableId,
}

impl GraphIndexConfig {
    /// Uses the graph crate's standard edge column names for one table.
    pub fn new(table: TableId) -> Self {
        Self { table }
    }

    /// Returns the source edge table.
    pub fn table(&self) -> &TableId {
        &self.table
    }
}

/// Live edge map and its forward/reverse adjacency representation.
pub(crate) struct LiveGraphProjection {
    config: GraphIndexConfig,
    edges: BTreeMap<String, Edge>,
    adjacency: AdjacencyIndex,
    through: u64,
}

impl LiveGraphProjection {
    /// Creates an empty graph projection.
    pub(crate) fn new(config: GraphIndexConfig) -> Self {
        Self {
            config,
            edges: BTreeMap::new(),
            adjacency: AdjacencyIndex::from_edges(&[], 0),
            through: 0,
        }
    }

    /// Applies append-only assertions and supersession tombstones.
    pub(crate) fn apply(&mut self, sequence: u64, batch: &RecordBatch) -> Result<()> {
        for edge in parse_edges(batch)? {
            if let Some(superseded) = &edge.supersedes {
                self.edges.remove(superseded);
            }
            self.edges.insert(edge.edge_id.clone(), edge);
        }
        let live = self.edges.values().cloned().collect::<Vec<_>>();
        self.adjacency = AdjacencyIndex::from_edges(&live, sequence as i64);
        self.through = sequence;
        Ok(())
    }

    /// Validates one batch against an isolated copy of the live edge set.
    pub(crate) fn validate(&self, sequence: u64, batch: &RecordBatch) -> Result<()> {
        let mut candidate = Self {
            config: self.config.clone(),
            edges: self.edges.clone(),
            adjacency: self.adjacency.clone(),
            through: self.through,
        };
        candidate.apply(sequence, batch)
    }

    /// Returns deterministic neighbors from forward, reverse, or both adjacency slices.
    pub(crate) fn neighbors(
        &self,
        node_id: &str,
        direction: Direction,
        predicate: Option<&str>,
    ) -> Vec<Neighbor> {
        let Some(node) = self.adjacency.node_index(node_id) else {
            return Vec::new();
        };
        let mut output = Vec::new();
        if matches!(direction, Direction::Out | Direction::Both) {
            self.collect(node, Direction::Out, predicate, &mut output);
        }
        if matches!(direction, Direction::In | Direction::Both) {
            self.collect(node, Direction::In, predicate, &mut output);
        }
        output
    }

    /// Converts one CSR adjacency slice into public graph neighbors.
    fn collect(
        &self,
        node: u32,
        direction: Direction,
        predicate: Option<&str>,
        output: &mut Vec<Neighbor>,
    ) {
        let entries = match direction {
            Direction::Out => self.adjacency.out_entries(node),
            Direction::In => self.adjacency.in_entries(node),
            Direction::Both => return,
        };
        for entry in entries {
            let Some(meta) = self.adjacency.edge_meta(entry.edge) else {
                continue;
            };
            let Some(edge_predicate) = self.adjacency.predicate_name(meta.predicate) else {
                continue;
            };
            if predicate.is_some_and(|expected| expected != edge_predicate) {
                continue;
            }
            let Some(adjacent) = self.adjacency.node_id(entry.neighbor) else {
                continue;
            };
            output.push(Neighbor {
                node_id: adjacent.to_owned(),
                predicate: edge_predicate.to_owned(),
                confidence: meta.confidence,
                edge_id: meta.edge_id.clone(),
                provenance: meta.provenance.clone(),
                direction,
            });
        }
    }

    /// Clones the snapshot-bound CSR for asynchronous Puffin framing.
    pub(crate) fn adjacency_for_puffin(&self) -> AdjacencyIndex {
        self.adjacency.clone()
    }

    /// Returns the highest transaction sequence represented by adjacency.
    pub(crate) fn through(&self) -> u64 {
        self.through
    }
}

/// Parses the standard Arrow edge table into graph-domain assertions.
fn parse_edges(batch: &RecordBatch) -> Result<Vec<Edge>> {
    let edge_ids = string_column(batch, "edge_id")?;
    let sources = string_column(batch, "src_id")?;
    let predicates = string_column(batch, "predicate")?;
    let destinations = string_column(batch, "dst_id")?;
    let provenance = string_column(batch, "provenance")?;
    let supersedes = string_column(batch, "supersedes")?;
    let confidence = batch
        .column_by_name("confidence")
        .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| Error::GraphProjection("confidence must be Float64".to_owned()))?;
    let mut edges = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        for (name, array) in [
            ("edge_id", edge_ids),
            ("src_id", sources),
            ("predicate", predicates),
            ("dst_id", destinations),
            ("provenance", provenance),
        ] {
            if array.is_null(row) {
                return Err(Error::GraphProjection(format!(
                    "{name} cannot be null at row {row}"
                )));
            }
        }
        if confidence.is_null(row) {
            return Err(Error::GraphProjection(format!(
                "confidence cannot be null at row {row}"
            )));
        }
        let mut edge = Edge::new(
            sources.value(row),
            predicates.value(row),
            destinations.value(row),
            provenance.value(row),
        );
        edge.edge_id = edge_ids.value(row).to_owned();
        edge.confidence = confidence.value(row);
        edge.supersedes = (!supersedes.is_null(row)).then(|| supersedes.value(row).to_owned());
        edges.push(edge);
    }
    Ok(edges)
}

/// Resolves one required UTF-8 column by name.
fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| Error::GraphProjection(format!("{name} must be Utf8")))
}
