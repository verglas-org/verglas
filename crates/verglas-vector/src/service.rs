//! Server-facing declaration, refresh, discovery, and ANN search over
//! snapshot-bound Iceberg Vamana attachments.
//!
//! The table metadata is the only registry. Search loads the attachment for the
//! table's exact current snapshot and keeps a decoded in-process copy keyed by
//! `(table, field, snapshot)`. Missing attachments are errors; this prototype
//! has no brute-force, shadow-store, or rebuild compatibility path.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use iceberg::{Catalog, TableIdent};

use crate::attachment::{list_indexes_for_snapshot, load_index_for_snapshot};
use crate::error::{Result, VectorError};
use crate::maintenance::{MaintenanceConfig, MaintenanceReport, run_maintenance};
use crate::metric::Metric;
use crate::vamana::{Neighbor, VamanaIndex};

/// The logical identity of a vector index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey {
    /// Stable logical source id, such as `tbl:default.docs` or `graph:kg`.
    pub target: String,
    /// The embedding field stored in the Vamana blob properties.
    pub field: String,
}

impl IndexKey {
    /// Constructs a key for a table embedding field.
    pub fn table(ident: &str, field: &str) -> Self {
        Self {
            target: format!("tbl:{ident}"),
            field: field.to_owned(),
        }
    }

    /// Constructs a key for a graph node-table embedding field.
    pub fn graph(namespace: &str, field: &str) -> Self {
        Self {
            target: format!("graph:{namespace}"),
            field: field.to_owned(),
        }
    }
}

/// ANN search parameters.
#[derive(Debug, Clone)]
pub struct SearchOptions {
    /// Number of neighbors to return.
    pub k: usize,
    /// Candidate-list size used by Vamana GreedySearch.
    pub l: usize,
}

/// A successful indexed search.
#[derive(Debug, Clone)]
pub struct SearchOutcome {
    /// Nearest-first neighbors from Vamana.
    pub neighbors: Vec<Neighbor>,
}

/// One index advertised by the current table snapshot.
#[derive(Debug, Clone)]
pub struct IndexDescriptor {
    /// The logical target supplied by the route.
    pub target: String,
    /// The indexed embedding field.
    pub field: String,
    /// The configured distance metric.
    pub metric: Metric,
    /// The exact source snapshot carrying the attachment.
    pub reflected_snapshot: i64,
    /// Number of live vectors in the attached index.
    pub live_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    table: String,
    field: String,
    snapshot: i64,
}

/// The vector service. Durable state lives exclusively in Iceberg; this owns
/// only disposable decoded indexes for the running process.
#[derive(Default)]
pub struct VectorService {
    cache: Mutex<HashMap<CacheKey, Arc<VamanaIndex>>>,
}

impl VectorService {
    /// Creates an empty disposable serving cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds and attaches an index to the table's current snapshot.
    pub async fn declare(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        key: IndexKey,
        config: MaintenanceConfig,
    ) -> Result<Option<MaintenanceReport>> {
        if key.field != config.vec_field {
            return Err(VectorError::Field(format!(
                "index key field '{}' differs from config field '{}'",
                key.field, config.vec_field
            )));
        }
        let report = run_maintenance(catalog, ident, &config).await?;
        if let Some(report) = &report {
            self.load_and_cache(catalog, ident, &key.field, report.reflected_snapshot)
                .await?;
        }
        Ok(report)
    }

    /// Refreshes an existing attached index to the table's current snapshot.
    /// The nearest attached ancestor supplies the build configuration.
    pub async fn refresh(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        key: &IndexKey,
    ) -> Result<Option<MaintenanceReport>> {
        let table = catalog.load_table(ident).await?;
        let snapshot =
            table
                .metadata()
                .current_snapshot_id()
                .ok_or_else(|| VectorError::IndexNotFound {
                    table: ident.to_string(),
                    field: key.field.clone(),
                    snapshot: None,
                })?;
        let attached = crate::attachment::load_latest_ancestor_index(&table, snapshot, &key.field)
            .await?
            .ok_or_else(|| VectorError::IndexNotFound {
                table: ident.to_string(),
                field: key.field.clone(),
                snapshot: Some(snapshot),
            })?;
        let report = run_maintenance(catalog, ident, &attached.config).await?;
        if let Some(report) = &report {
            self.load_and_cache(catalog, ident, &key.field, report.reflected_snapshot)
                .await?;
        }
        Ok(report)
    }

    /// Searches the Vamana attachment bound to the exact current snapshot.
    /// Missing or stale attachments are returned as errors.
    pub async fn search(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        key: &IndexKey,
        query: &[f32],
        options: &SearchOptions,
    ) -> Result<SearchOutcome> {
        let table = catalog.load_table(ident).await?;
        let snapshot =
            table
                .metadata()
                .current_snapshot_id()
                .ok_or_else(|| VectorError::IndexNotFound {
                    table: ident.to_string(),
                    field: key.field.clone(),
                    snapshot: None,
                })?;
        let cache_key = CacheKey {
            table: ident.to_string(),
            field: key.field.clone(),
            snapshot,
        };
        let cached = self
            .cache
            .lock()
            .map_err(|error| VectorError::Cache(error.to_string()))?
            .get(&cache_key)
            .cloned();
        let index = match cached {
            Some(index) => index,
            None => {
                let attached = load_index_for_snapshot(&table, snapshot, &key.field)
                    .await?
                    .ok_or_else(|| VectorError::IndexNotFound {
                        table: ident.to_string(),
                        field: key.field.clone(),
                        snapshot: Some(snapshot),
                    })?;
                self.cache_index(cache_key, attached.index)?
            }
        };
        Ok(SearchOutcome {
            neighbors: index.search(query, options.k, options.l)?,
        })
    }

    /// Returns whether the exact current snapshot advertises `field`.
    pub async fn served(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        field: &str,
    ) -> Result<bool> {
        let table = catalog.load_table(ident).await?;
        let Some(snapshot) = table.metadata().current_snapshot_id() else {
            return Ok(false);
        };
        Ok(load_index_for_snapshot(&table, snapshot, field)
            .await?
            .is_some())
    }

    /// Lists every Vamana blob attached to the exact current snapshot.
    pub async fn list(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        target: &str,
    ) -> Result<Vec<IndexDescriptor>> {
        let table = catalog.load_table(ident).await?;
        let Some(snapshot) = table.metadata().current_snapshot_id() else {
            return Ok(Vec::new());
        };
        Ok(list_indexes_for_snapshot(&table, snapshot)
            .await?
            .into_iter()
            .map(|attached| IndexDescriptor {
                target: target.to_owned(),
                field: attached.config.vec_field,
                metric: attached.index.metric(),
                reflected_snapshot: snapshot,
                live_count: attached.index.len(),
            })
            .collect())
    }

    /// Loads the exact attachment and inserts its decoded index into the
    /// disposable serving cache.
    async fn load_and_cache(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        field: &str,
        snapshot: i64,
    ) -> Result<Arc<VamanaIndex>> {
        let table = catalog.load_table(ident).await?;
        let attached = load_index_for_snapshot(&table, snapshot, field)
            .await?
            .ok_or_else(|| VectorError::IndexNotFound {
                table: ident.to_string(),
                field: field.to_owned(),
                snapshot: Some(snapshot),
            })?;
        self.cache_index(
            CacheKey {
                table: ident.to_string(),
                field: field.to_owned(),
                snapshot,
            },
            attached.index,
        )
    }

    /// Inserts one decoded index into the process-local serving cache.
    fn cache_index(&self, key: CacheKey, index: VamanaIndex) -> Result<Arc<VamanaIndex>> {
        let index = Arc::new(index);
        self.cache
            .lock()
            .map_err(|error| VectorError::Cache(error.to_string()))?
            .insert(key, index.clone());
        Ok(index)
    }
}
