//! The daemon-facing index service: declare, refresh, and search over the
//! shadow store, with a brute-force fallback.
//!
//! This is the thin layer the daemon routes call. It owns:
//! - a [`ShadowBlobStore`] (injected placement),
//! - a registry of declared indexes (field → [`MaintenanceConfig`]), and
//! - an in-memory cache of decoded indexes for serving.
//!
//! Declaring an index registers it and runs the initial build (a full
//! [`run_maintenance`]). Searching loads the index from the cache/shadow store
//! and runs GreedySearch; when no index blob exists it falls back to brute force
//! over the column (the turn-off path).
//!
//! The in-memory registry is a PROJECTION of the durable `verglas_sys.indexes`
//! registry, not the source of truth. On daemon boot [`VectorService::rehydrate`]
//! replays each declared row: it restores the maintenance config into this
//! in-memory registry and either loads the present shadow-store blob (serve
//! immediately) or rebuilds a missing one. The durable write of the registry row
//! lives in the daemon (which owns the `SystemCatalog`); this crate stays free of
//! the platform control-plane dependency and takes the reconstructed config back
//! through `rehydrate`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use iceberg::{Catalog, TableIdent};

use crate::error::{Result, VectorError};
use crate::maintenance::{
    MaintenanceConfig, MaintenanceReport, load_latest_index, run_maintenance,
};
use crate::metric::Metric;
use crate::store::{IndexKey, ShadowBlobStore};
use crate::vamana::{Neighbor, VamanaIndex};

/// Whether a search was served from the ANN index or the brute-force fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchSource {
    /// Served from the maintained Vamana index.
    Index,
    /// Served by brute force over the column (no index for this field).
    BruteForce,
}

/// Search parameters. `default_metric`/`default_id_field` are used only on the
/// brute-force fallback for a field that was never declared (the index path
/// takes metric/id from the blob and registry).
#[derive(Debug, Clone)]
pub struct SearchOptions {
    /// Number of neighbors to return.
    pub k: usize,
    /// Candidate-list size `L`.
    pub l: usize,
    /// Metric for the brute-force fallback when the field is undeclared.
    pub default_metric: Metric,
    /// Id column for the brute-force fallback when the field is undeclared.
    pub default_id_field: String,
}

/// The result of a search: neighbors and how they were produced.
#[derive(Debug, Clone)]
pub struct SearchOutcome {
    /// Nearest-first neighbors.
    pub neighbors: Vec<Neighbor>,
    /// Index vs brute-force fallback.
    pub source: SearchSource,
}

/// The outcome of rehydrating one declared index from the durable registry on
/// boot: either the cluster-local blob was present and loaded (served without a
/// rebuild), or no blob existed and the index was rebuilt from the source table.
#[derive(Debug, Clone)]
pub enum RehydrateOutcome {
    /// The shadow-store blob was present and loaded into the serving cache — the
    /// index serves immediately, no rebuild.
    ServedFromBlob {
        /// The snapshot the loaded blob reflects.
        reflected_snapshot: i64,
        /// Live vectors in the loaded blob.
        live_count: usize,
    },
    /// No blob was present, so the index was rebuilt via the maintenance MV.
    /// `None` when the source table had no rows yet (nothing to build).
    Rebuilt(Option<MaintenanceReport>),
}

/// A declared index, for listing.
#[derive(Debug, Clone)]
pub struct IndexDescriptor {
    /// The logical target (`tbl:...` / `graph:...`).
    pub target: String,
    /// The embedding field.
    pub field: String,
    /// The distance metric.
    pub metric: Metric,
    /// The source snapshot the current blob reflects, if built.
    pub reflected_snapshot: Option<i64>,
    /// Live vectors in the current blob, if built.
    pub live_count: Option<usize>,
}

/// The index service backing the daemon's declare/search routes.
pub struct VectorService {
    store: Arc<dyn ShadowBlobStore>,
    registry: Mutex<HashMap<IndexKey, MaintenanceConfig>>,
    cache: Mutex<HashMap<IndexKey, Arc<VamanaIndex>>>,
}

impl VectorService {
    /// A service over `store`.
    pub fn new(store: Arc<dyn ShadowBlobStore>) -> Self {
        VectorService {
            store,
            registry: Mutex::new(HashMap::new()),
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn lock_registry(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<IndexKey, MaintenanceConfig>>> {
        self.registry
            .lock()
            .map_err(|e| VectorError::Store(format!("registry lock: {e}")))
    }

    fn cache_put(&self, key: &IndexKey, index: VamanaIndex) -> Result<Arc<VamanaIndex>> {
        let arc = Arc::new(index);
        self.cache
            .lock()
            .map_err(|e| VectorError::Store(format!("cache lock: {e}")))?
            .insert(key.clone(), arc.clone());
        Ok(arc)
    }

    fn cache_get(&self, key: &IndexKey) -> Result<Option<Arc<VamanaIndex>>> {
        Ok(self
            .cache
            .lock()
            .map_err(|e| VectorError::Store(format!("cache lock: {e}")))?
            .get(key)
            .cloned())
    }

    /// Declares an index on a field and runs the initial build. Registers the
    /// maintenance config so later [`VectorService::refresh`] runs know how to
    /// read the source, and warms the serving cache. Returns the build report
    /// (`None` when the source has no rows yet).
    pub async fn declare(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        key: IndexKey,
        config: MaintenanceConfig,
    ) -> Result<Option<MaintenanceReport>> {
        self.lock_registry()?.insert(key.clone(), config.clone());
        let report = run_maintenance(catalog, ident, &key, self.store.as_ref(), &config).await?;
        if report.is_some()
            && let Some(index) = load_latest_index(self.store.as_ref(), &key).await?
        {
            self.cache_put(&key, index)?;
        }
        Ok(report)
    }

    /// Registers a maintenance config into the in-memory registry WITHOUT
    /// building — the config-only half of [`VectorService::declare`]. The daemon
    /// uses it at boot to register the memory index (whose source table may not
    /// exist until the first consolidation) so a later [`VectorService::refresh`]
    /// knows how to read the source; the build lands when the table has rows.
    pub fn register(&self, key: IndexKey, config: MaintenanceConfig) -> Result<()> {
        self.lock_registry()?.insert(key, config);
        Ok(())
    }

    /// Rehydrates one index from the durable registry on daemon boot: restores
    /// its maintenance config into the in-memory projection, then either loads
    /// the present shadow-store blob into the serving cache (serve immediately,
    /// no rebuild) or, when no blob is present, rebuilds it via the maintenance
    /// MV. The registry is the source of truth; the in-memory registry/cache is
    /// the projection this rebuilds after a restart.
    pub async fn rehydrate(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        key: IndexKey,
        config: MaintenanceConfig,
    ) -> Result<RehydrateOutcome> {
        self.lock_registry()?.insert(key.clone(), config.clone());
        if let Some(index) = load_latest_index(self.store.as_ref(), &key).await? {
            let reflected_snapshot = index.reflected_snapshot();
            let live_count = index.len();
            self.cache_put(&key, index)?;
            return Ok(RehydrateOutcome::ServedFromBlob {
                reflected_snapshot,
                live_count,
            });
        }
        // No blob in the shadow store — rebuild from the source table.
        let report = run_maintenance(catalog, ident, &key, self.store.as_ref(), &config).await?;
        if report.is_some()
            && let Some(index) = load_latest_index(self.store.as_ref(), &key).await?
        {
            self.cache_put(&key, index)?;
        }
        Ok(RehydrateOutcome::Rebuilt(report))
    }

    /// Runs a maintenance pass for a previously declared index (a cron/manual
    /// trigger). Errors if the field was never declared in this process.
    pub async fn refresh(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        key: &IndexKey,
    ) -> Result<Option<MaintenanceReport>> {
        let config = self
            .lock_registry()?
            .get(key)
            .cloned()
            .ok_or_else(|| VectorError::Field(format!("index '{}' not declared", key.field)))?;
        let report = run_maintenance(catalog, ident, key, self.store.as_ref(), &config).await?;
        if report.is_some()
            && let Some(index) = load_latest_index(self.store.as_ref(), key).await?
        {
            self.cache_put(key, index)?;
        }
        Ok(report)
    }

    /// Searches `field` for the `k` nearest neighbors of `query`. Serves from the
    /// maintained index when a blob exists (cache first, then shadow store), else
    /// falls back to brute force over the column. `default_metric` is used only
    /// for the brute-force path when the field was never declared.
    pub async fn search(
        &self,
        catalog: &dyn Catalog,
        ident: &TableIdent,
        key: &IndexKey,
        query: &[f32],
        opts: &SearchOptions,
    ) -> Result<SearchOutcome> {
        // Cache, then shadow store.
        let cached = self.cache_get(key)?;
        let index = match cached {
            Some(idx) => Some(idx),
            None => match load_latest_index(self.store.as_ref(), key).await? {
                Some(idx) => Some(self.cache_put(key, idx)?),
                None => None,
            },
        };
        if let Some(index) = index {
            let neighbors = index.search(query, opts.k, opts.l)?;
            return Ok(SearchOutcome {
                neighbors,
                source: SearchSource::Index,
            });
        }

        // Brute-force fallback (turn-off path): metric/id from the registry if
        // declared, else the caller's defaults.
        let (metric, id_field, encoding) = {
            let reg = self.lock_registry()?;
            match reg.get(key) {
                Some(cfg) => (cfg.metric, cfg.id_field.clone(), cfg.id_encoding),
                None => (
                    opts.default_metric,
                    opts.default_id_field.clone(),
                    crate::maintenance::IdEncoding::default(),
                ),
            }
        };
        let rows = read_all_vectors(catalog, ident, &id_field, &key.field, encoding).await?;
        let neighbors = crate::brute_force_search(metric, query.len(), &rows, query, opts.k)?;
        Ok(SearchOutcome {
            neighbors,
            source: SearchSource::BruteForce,
        })
    }

    /// Whether a built index blob is available to serve `key` — in the serving
    /// cache, or loadable from the shadow store. A recall seed source consults
    /// this before searching so the no-index case takes its own brute-force
    /// path (the turn-off) rather than paying for the search's whole-table
    /// fallback scan.
    pub async fn served(&self, key: &IndexKey) -> Result<bool> {
        if self.cache_get(key)?.is_some() {
            return Ok(true);
        }
        Ok(self.store.latest(key).await?.is_some())
    }

    /// Lists declared indexes with their current build state.
    pub async fn list(&self) -> Result<Vec<IndexDescriptor>> {
        let entries: Vec<(IndexKey, Metric)> = {
            let reg = self.lock_registry()?;
            reg.iter().map(|(k, c)| (k.clone(), c.metric)).collect()
        };
        let mut out = Vec::with_capacity(entries.len());
        for (key, metric) in entries {
            let (reflected_snapshot, live_count) =
                match load_latest_index(self.store.as_ref(), &key).await? {
                    Some(idx) => (Some(idx.reflected_snapshot()), Some(idx.len())),
                    None => (None, None),
                };
            out.push(IndexDescriptor {
                target: key.target,
                field: key.field,
                metric,
                reflected_snapshot,
                live_count,
            });
        }
        Ok(out)
    }
}

/// Reads every `(id, vector)` pair from the current snapshot for the brute-force
/// fallback. Skips rows with a null embedding.
async fn read_all_vectors(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    id_field: &str,
    vec_field: &str,
    encoding: crate::maintenance::IdEncoding,
) -> Result<Vec<(i64, Vec<f32>)>> {
    let resp = verglas_iceberg::tables_api::rows(catalog, ident, None, None).await?;
    let mut out = Vec::with_capacity(resp.rows.len());
    for row in &resp.rows {
        if let Some((id, Some(vector))) =
            crate::maintenance::parse_row(row, id_field, vec_field, encoding)?
        {
            out.push((id, vector));
        }
    }
    Ok(out)
}
