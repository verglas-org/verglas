//! Live Vamana projection backed by canonical vector mutation batches.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use verglas_vector::arrow::extract_rows;
use verglas_vector::{Metric, Neighbor, VamanaIndex, VamanaParams, brute_force_search};

use crate::error::Result;
use crate::transaction::TableId;

/// Declares how one SQL table projects into its live Vamana delta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexConfig {
    table: TableId,
    id_column: String,
    vector_column: String,
    dimension: usize,
    metric: Metric,
}

impl VectorIndexConfig {
    /// Creates a vector projection over one fixed-size Arrow embedding column.
    pub fn new(
        table: TableId,
        id_column: impl Into<String>,
        vector_column: impl Into<String>,
        dimension: usize,
        metric: Metric,
    ) -> Self {
        Self {
            table,
            id_column: id_column.into(),
            vector_column: vector_column.into(),
            dimension,
            metric,
        }
    }

    /// Returns the source table identity.
    pub fn table(&self) -> &TableId {
        &self.table
    }
}

/// Mutable exact rows and Vamana graph for one registered vector projection.
pub(crate) struct LiveVectorProjection {
    config: VectorIndexConfig,
    index: VamanaIndex,
    rows: BTreeMap<i64, Vec<f32>>,
    through: u64,
}

impl LiveVectorProjection {
    /// Creates an empty deterministic Vamana delta.
    pub(crate) fn new(config: VectorIndexConfig) -> Result<Self> {
        let index = VamanaIndex::new(
            config.metric,
            config.dimension,
            VamanaParams::default(),
            0x0056_4552_474c_4153,
        )?;
        Ok(Self {
            config,
            index,
            rows: BTreeMap::new(),
            through: 0,
        })
    }

    /// Applies inserts and null-vector tombstones at one commit sequence.
    pub(crate) fn apply(&mut self, sequence: u64, batch: &RecordBatch) -> Result<()> {
        for row in extract_rows(batch, &self.config.id_column, &self.config.vector_column)? {
            match row.vector {
                Some(vector) => {
                    self.index.insert(row.id, &vector)?;
                    self.rows.insert(row.id, vector);
                }
                None => {
                    self.index.delete(row.id);
                    self.rows.remove(&row.id);
                }
            }
        }
        self.through = self.through.max(sequence);
        Ok(())
    }

    /// Validates a batch by applying it to an isolated projection clone.
    pub(crate) fn validate(&self, sequence: u64, batch: &RecordBatch) -> Result<()> {
        let mut candidate = Self {
            config: self.config.clone(),
            index: self.index.clone(),
            rows: self.rows.clone(),
            through: self.through,
        };
        candidate.apply(sequence, batch)
    }

    /// Exact-reranks every live row so no Vamana coverage tail can be omitted.
    pub(crate) fn search(&self, query: &[f32], k: usize) -> Result<Vec<Neighbor>> {
        let rows = self
            .rows
            .iter()
            .map(|(id, vector)| (*id, vector.clone()))
            .collect::<Vec<_>>();
        Ok(brute_force_search(
            self.config.metric,
            self.config.dimension,
            &rows,
            query,
            k,
        )?)
    }

    /// Clones the Vamana delta and binds it to its covered DO sequence.
    pub(crate) fn index_for_puffin(&self) -> VamanaIndex {
        let mut index = self.index.clone();
        index.set_reflected_snapshot(self.through as i64);
        index
    }

    /// Returns the highest vector mutation represented by both exact rows and Vamana.
    pub(crate) fn through(&self) -> u64 {
        self.through
    }
}
