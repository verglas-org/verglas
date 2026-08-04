//! Plan-based memory estimation for a query, computed without executing it.
//!
//! A standalone query worker's memory grant should be *predicted* from the
//! query before it runs, not just grown reactively after the fact — that's
//! the primary sizing mechanism; growth (`MemoryGrantHost::grow`) is the
//! escape hatch for when the estimate falls short, not the main plan.
//!
//! [`estimate`] plans `sql` the same way [`crate::query::query`] does — same
//! catalog/time-travel context, same DataFusion session — but stops after
//! `SessionContext::sql`, which only parses and plans; it never calls
//! `DataFrame::collect`, so no data is read. It then walks the resolved
//! logical plan, pulling real per-table byte totals from the Iceberg scan
//! planner (the same `table.scan().build()?.plan_files()` call
//! [`crate::inspect::show`] uses) and combining them with fixed heuristics for
//! projection, filtering, and join/aggregate/sort/window working sets.
//!
//! Every term beyond the raw per-table scan size is a documented
//! approximation — see [`QueryMemoryEstimate::notes`] on every result. This is
//! a clean seam, not a finished cost model: the constants below are the place
//! a follow-up plugs in real column-level stats (Iceberg's manifest
//! `DataFile::column_sizes`, not consumed here) and cardinality-aware
//! join/aggregate costing.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::common::TableReference;
use datafusion::logical_expr::{LogicalPlan, TableScan};
use futures::TryStreamExt;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use crate::error::{AgentError, Result};
use crate::ident::parse_table_ident;
use crate::query::{TimeTravel, catalog_context, resolve_reference};
use crate::report::QueryMemoryEstimate;

/// Fixed baseline for the query engine's own runtime (DataFusion session +
/// tokio), independent of the query. Not measured — a round number chosen to
/// be in the right neighborhood for this engine; a follow-up could calibrate
/// it from the role binary's own observed idle RSS.
const BASE_OVERHEAD_BYTES: u64 = 64 * 1024 * 1024;

/// Per-filter-predicate selectivity assumed above a table scan, applied once
/// per filter up to [`MAX_FILTER_TERMS`]. Approximate: real selectivity needs
/// column value/null-count/distinct-count statistics this estimator does not
/// consume yet; a bare filter count is treated as a weak but directionally
/// correct signal (more predicates likely prune more rows).
const FILTER_SELECTIVITY_FACTOR: f64 = 0.5;

/// Filter terms beyond this stop reducing the estimate further — a scan under
/// enough predicates should bottom out near "small", not asymptote to zero.
const MAX_FILTER_TERMS: usize = 3;

/// Hash-table overhead for an `Aggregate` node, as a fraction of its input's
/// scan bytes. Approximate: a real cost would key off group-by cardinality
/// and the aggregate row width, not input size.
const AGGREGATE_WORKING_SET_FACTOR: f64 = 0.3;

/// Hash-table build-side overhead for a `Join` node, as a multiple of the
/// smaller input's scan bytes (the side a hash join builds from). Approximate:
/// assumes the smaller *scan* is the smaller *build side*, which join
/// reordering may not preserve.
const JOIN_BUILD_SIDE_FACTOR: f64 = 1.5;

/// Materialization overhead for a `Sort` node, as a multiple of its input's
/// scan bytes — an in-memory sort must hold its input. Approximate: DataFusion
/// may spill to disk under memory pressure rather than holding everything, so
/// this is a worst-case-ish upper bound, not a measured figure.
const SORT_WORKING_SET_FACTOR: f64 = 1.0;

/// Materialization overhead for a `Window` node, as a multiple of its input's
/// scan bytes. Same shape and same caveats as [`SORT_WORKING_SET_FACTOR`].
const WINDOW_WORKING_SET_FACTOR: f64 = 1.0;

/// Estimates the memory `sql` will need, without executing it. Mirrors
/// [`crate::query::query`]'s context selection (catalog-wide, or one
/// time-traveled table via `at`), so the estimate reflects exactly what a
/// subsequent real run of the same request would scan.
pub async fn estimate(
    catalog: Arc<dyn Catalog>,
    sql: &str,
    at: Option<TimeTravel>,
) -> Result<QueryMemoryEstimate> {
    let (ctx, precomputed_scan_bytes) = match at {
        Some(travel) => {
            let ident = parse_table_ident(&travel.table)?;
            let table = catalog.load_table(&ident).await?;
            let snapshot_id = resolve_reference(&table, &travel.reference)?;
            let bytes = table_bytes(&table, Some(snapshot_id)).await?;
            let provider =
                iceberg_datafusion::IcebergStaticTableProvider::try_new_from_table_snapshot(
                    table,
                    snapshot_id,
                )
                .await
                .map_err(AgentError::Iceberg)?;
            let ctx = datafusion::prelude::SessionContext::new();
            ctx.register_table(ident.name(), Arc::new(provider))
                .map_err(|e| AgentError::Query(e.to_string()))?;
            (ctx, Some(bytes))
        }
        None => (catalog_context(catalog.clone()).await?, None),
    };

    // `.sql()` parses and plans only; `into_optimized_plan()` runs the logical
    // optimizer (including the projection/filter pushdown into `TableScan`
    // this estimator relies on) but still stops short of physical planning or
    // `.collect()` — no data is read from the cache/backend here.
    let dataframe = ctx
        .sql(sql)
        .await
        .map_err(|e| AgentError::Query(e.to_string()))?;
    let plan = dataframe
        .into_optimized_plan()
        .map_err(|e| AgentError::Query(e.to_string()))?;
    let cost = walk(&plan, catalog.as_ref(), precomputed_scan_bytes).await?;

    let estimated_total_bytes = cost
        .scan_bytes
        .saturating_add(cost.working_set_bytes)
        .saturating_add(BASE_OVERHEAD_BYTES);

    Ok(QueryMemoryEstimate {
        scan_bytes: cost.scan_bytes,
        working_set_bytes: cost.working_set_bytes,
        base_overhead_bytes: BASE_OVERHEAD_BYTES,
        estimated_total_bytes,
        approximate: true,
        notes: caveats(cost.warnings),
    })
}

/// The always-true caveats about what this estimator approximates, plus any
/// table-resolution warnings hit while walking this particular plan.
fn caveats(warnings: Vec<String>) -> Vec<String> {
    let mut notes = vec![
        "scan size is the table's live-file size at snapshot grain (post partition pruning \
         via the current or pinned snapshot only); predicate ranges are not pushed into the \
         Iceberg scan planner, so no manifest/file pruning beyond that happens here"
            .to_owned(),
        format!(
            "predicate selectivity is a fixed {FILTER_SELECTIVITY_FACTOR}x-per-filter \
             heuristic (capped at {MAX_FILTER_TERMS} filters), not derived from column \
             value/null-count statistics"
        ),
        "projection sizing uses a column-COUNT ratio (selected / total columns), not \
         per-column byte stats — Iceberg's manifest DataFile::column_sizes is not consumed yet"
            .to_owned(),
        format!(
            "join/aggregate/sort/window working-set terms are fixed multipliers on input scan \
             bytes (aggregate {AGGREGATE_WORKING_SET_FACTOR}x, join build-side \
             {JOIN_BUILD_SIDE_FACTOR}x, sort/window {SORT_WORKING_SET_FACTOR}x), not \
             cardinality- or row-width-aware costing"
        ),
        format!(
            "base runtime overhead is a fixed {BASE_OVERHEAD_BYTES}-byte constant, not measured"
        ),
        "this estimate has no term for the query's OUTPUT/result size — the query role streams \
         its response as batches are produced (never collects the whole result), so a large \
         result does not need a larger grant; only operator working sets (join/aggregate hash \
         tables, sort/window buffers) do"
            .to_owned(),
    ];
    notes.extend(warnings);
    notes
}

/// The two cost terms accumulated while walking a logical plan, plus any
/// warnings hit resolving table stats along the way.
struct PlanCost {
    scan_bytes: u64,
    working_set_bytes: u64,
    warnings: Vec<String>,
}

impl PlanCost {
    fn zero() -> Self {
        PlanCost {
            scan_bytes: 0,
            working_set_bytes: 0,
            warnings: Vec::new(),
        }
    }

    fn merge(mut self, other: PlanCost) -> Self {
        self.scan_bytes = self.scan_bytes.saturating_add(other.scan_bytes);
        self.working_set_bytes = self
            .working_set_bytes
            .saturating_add(other.working_set_bytes);
        self.warnings.extend(other.warnings);
        self
    }
}

type CostFuture<'a> = Pin<Box<dyn Future<Output = Result<PlanCost>> + Send + 'a>>;

/// Walks `plan` bottom-up, summing scan bytes at the leaves and adding
/// working-set terms at aggregate/join/sort/window nodes. `precomputed_scan_bytes`
/// is `Some` only on the time-travel path, where the whole plan scans exactly
/// the one table already loaded and sized by the caller; on the catalog path
/// it is `None` and every `TableScan` resolves (and sizes) its own table.
fn walk<'a>(
    plan: &'a LogicalPlan,
    catalog: &'a dyn Catalog,
    precomputed_scan_bytes: Option<u64>,
) -> CostFuture<'a> {
    Box::pin(async move {
        match plan {
            LogicalPlan::TableScan(scan) => scan_cost(scan, catalog, precomputed_scan_bytes).await,
            LogicalPlan::Aggregate(agg) => {
                let input = walk(&agg.input, catalog, precomputed_scan_bytes).await?;
                let extra = scaled(input.scan_bytes, AGGREGATE_WORKING_SET_FACTOR);
                Ok(PlanCost {
                    scan_bytes: input.scan_bytes,
                    working_set_bytes: input.working_set_bytes.saturating_add(extra),
                    warnings: input.warnings,
                })
            }
            LogicalPlan::Sort(sort) => {
                let input = walk(&sort.input, catalog, precomputed_scan_bytes).await?;
                let extra = scaled(input.scan_bytes, SORT_WORKING_SET_FACTOR);
                Ok(PlanCost {
                    scan_bytes: input.scan_bytes,
                    working_set_bytes: input.working_set_bytes.saturating_add(extra),
                    warnings: input.warnings,
                })
            }
            LogicalPlan::Window(window) => {
                let input = walk(&window.input, catalog, precomputed_scan_bytes).await?;
                let extra = scaled(input.scan_bytes, WINDOW_WORKING_SET_FACTOR);
                Ok(PlanCost {
                    scan_bytes: input.scan_bytes,
                    working_set_bytes: input.working_set_bytes.saturating_add(extra),
                    warnings: input.warnings,
                })
            }
            LogicalPlan::Join(join) => {
                let left = walk(&join.left, catalog, precomputed_scan_bytes).await?;
                let right = walk(&join.right, catalog, precomputed_scan_bytes).await?;
                let build_side = left.scan_bytes.min(right.scan_bytes);
                let extra = scaled(build_side, JOIN_BUILD_SIDE_FACTOR);
                let mut warnings = left.warnings;
                warnings.extend(right.warnings);
                Ok(PlanCost {
                    scan_bytes: left.scan_bytes.saturating_add(right.scan_bytes),
                    working_set_bytes: left
                        .working_set_bytes
                        .saturating_add(right.working_set_bytes)
                        .saturating_add(extra),
                    warnings,
                })
            }
            other => {
                let mut total = PlanCost::zero();
                for input in other.inputs() {
                    let child = walk(input, catalog, precomputed_scan_bytes).await?;
                    total = total.merge(child);
                }
                Ok(total)
            }
        }
    })
}

/// `bytes * factor`, rounded, saturating rather than overflowing on huge
/// inputs.
fn scaled(bytes: u64, factor: f64) -> u64 {
    ((bytes as f64) * factor).round().max(0.0) as u64
}

/// The scan-size term for one `TableScan` node: its table's live-file bytes
/// (precomputed on the time-travel path, resolved fresh here otherwise),
/// reduced by a column-count projection ratio and a per-filter selectivity
/// factor. A table whose stats cannot be resolved contributes zero bytes with
/// a warning, rather than failing the whole estimate.
async fn scan_cost(
    scan: &TableScan,
    catalog: &dyn Catalog,
    precomputed_scan_bytes: Option<u64>,
) -> Result<PlanCost> {
    let mut warnings = Vec::new();
    let size_bytes = match precomputed_scan_bytes {
        Some(bytes) => bytes,
        None => match resolve_table_bytes(catalog, &scan.table_name).await {
            Ok(bytes) => bytes,
            Err(e) => {
                warnings.push(format!(
                    "could not resolve stats for `{}` ({e}); its scan contributes 0 bytes to \
                     the estimate",
                    scan.table_name
                ));
                0
            }
        },
    };

    let total_cols = scan.source.schema().fields().len().max(1) as f64;
    let projected_cols = scan
        .projection
        .as_ref()
        .map(|p| p.len())
        .unwrap_or(scan.source.schema().fields().len())
        .max(1) as f64;
    let projection_ratio = (projected_cols / total_cols).min(1.0);

    let filter_terms = scan.filters.len().min(MAX_FILTER_TERMS);
    let filter_factor = FILTER_SELECTIVITY_FACTOR.powi(filter_terms as i32);

    let scan_bytes = ((size_bytes as f64) * projection_ratio * filter_factor).round() as u64;
    Ok(PlanCost {
        scan_bytes,
        working_set_bytes: 0,
        warnings,
    })
}

/// Resolves `table_name` (as it appears in a planned `TableScan`, already
/// case-resolved by DataFusion's catalog machinery) back to the Iceberg table
/// it names, and sums its live-file bytes. Exact-cased lookup first, then a
/// case-insensitive scan of the namespace's tables — mirroring
/// `query::CaseInsensitiveSchemaProvider` so an estimate resolves any table a
/// real run of the same SQL would.
async fn resolve_table_bytes(catalog: &dyn Catalog, table_name: &TableReference) -> Result<u64> {
    let schema = table_name.schema().unwrap_or("default").to_owned();
    let table_local = table_name.table().to_owned();
    let namespace = NamespaceIdent::new(schema.clone());

    let ident = TableIdent::new(namespace.clone(), table_local.clone());
    let table = match catalog.load_table(&ident).await {
        Ok(table) => table,
        Err(_) => {
            let candidates = catalog.list_tables(&namespace).await?;
            let matched = candidates
                .into_iter()
                .find(|c| c.name().eq_ignore_ascii_case(&table_local))
                .ok_or_else(|| {
                    AgentError::Query(format!("no table matches `{schema}.{table_local}`"))
                })?;
            catalog.load_table(&matched).await?
        }
    };
    table_bytes(&table, None).await
}

/// Sums the live-file bytes of `table` at `snapshot_id` (or its current
/// snapshot when `None`), deduping files split into multiple scan tasks.
/// Reuses the same `table.scan().build()?.plan_files()` pattern
/// [`crate::inspect::show`] uses — the snapshot summary's own counters are not
/// cumulative for fast-append, so the scan plan is the authoritative source.
async fn table_bytes(table: &iceberg::table::Table, snapshot_id: Option<i64>) -> Result<u64> {
    if snapshot_id.is_none() && table.metadata().current_snapshot().is_none() {
        return Ok(0);
    }
    let mut builder = table.scan();
    if let Some(id) = snapshot_id {
        builder = builder.snapshot_id(id);
    }
    let scan = builder.build()?;
    let tasks: Vec<_> = scan.plan_files().await?.try_collect().await?;
    let mut seen = HashSet::new();
    let mut bytes = 0u64;
    for task in &tasks {
        if seen.insert(task.data_file_path.clone()) {
            bytes = bytes.saturating_add(task.file_size_in_bytes);
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Write;
    use std::path::PathBuf;

    use iceberg::CatalogBuilder;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};

    use super::*;
    use crate::{inspect, write};

    async fn memory_catalog() -> Arc<dyn Catalog> {
        let warehouse = tempfile::tempdir().expect("warehouse tempdir");
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.path().to_str().expect("utf8 path").to_string(),
                )]),
            )
            .await
            .expect("memory catalog");
        std::mem::forget(warehouse);
        Arc::new(catalog)
    }

    fn source_file(name: &str, contents: &str) -> PathBuf {
        let dir = tempfile::tempdir().expect("source tempdir");
        let path = dir.path().join(name);
        let mut file = std::fs::File::create(&path).expect("create source");
        file.write_all(contents.as_bytes()).expect("write source");
        file.flush().expect("flush source");
        std::mem::forget(dir);
        path
    }

    /// A big-enough CSV fixture that the scan-bytes term dominates the fixed
    /// base overhead, so the estimate's shape (not just its floor) is
    /// testable.
    fn wide_orders_csv(rows: u64) -> String {
        let mut body = "id,amount,customer,region,note\n".to_owned();
        for i in 0..rows {
            body.push_str(&format!(
                "{i},{amt}.00,customer-{i},region-{r},a padding note to give this row some width\n",
                amt = i % 1000,
                r = i % 8,
            ));
        }
        body
    }

    /// A full unfiltered, unprojected scan estimate's `scan_bytes` exactly
    /// equals the table's live-file size (`inspect::show`'s `size_bytes`) —
    /// no projection or filter factor applies, so this term should track the
    /// real, measured on-disk size precisely, not just approximately.
    #[tokio::test]
    async fn full_scan_estimate_matches_measured_table_size() {
        let catalog = memory_catalog().await;
        let ident = parse_table_ident("sales.orders").expect("ident");
        let src = source_file("orders.csv", &wide_orders_csv(500));
        write::create_table(catalog.as_ref(), &ident, &src, None)
            .await
            .expect("create");

        let show = inspect::show(catalog.as_ref(), &ident).await.expect("show");
        let measured = show.size_bytes.expect("size_bytes present");
        assert!(measured > 0, "fixture should have written real bytes");

        let est = estimate(catalog.clone(), "SELECT * FROM sales.orders", None)
            .await
            .expect("estimate");
        assert_eq!(est.scan_bytes, measured);
        assert!(
            est.estimated_total_bytes > est.scan_bytes,
            "total should add working-set/base overhead on top of the scan"
        );
        assert!(est.approximate);
        assert!(
            !est.notes.is_empty(),
            "estimate must say what's approximate"
        );
    }

    /// A large result set with no join/aggregate/sort/window contributes
    /// zero working-set bytes, however many rows it returns — the estimate
    /// has no output/result-size term because the query role streams its
    /// response as batches are produced instead of collecting it, so result
    /// size is never what a grant needs to cover. Only operator working sets
    /// (this test's table has none) do. A bigger table changes `scan_bytes`
    /// (real bytes read), never `working_set_bytes`.
    #[tokio::test]
    async fn large_plain_scan_has_zero_working_set_bytes() {
        let catalog = memory_catalog().await;
        let ident = parse_table_ident("sales.huge").expect("ident");
        let src = source_file("huge.csv", &wide_orders_csv(20_000));
        write::create_table(catalog.as_ref(), &ident, &src, None)
            .await
            .expect("create");

        let est = estimate(catalog.clone(), "SELECT * FROM sales.huge", None)
            .await
            .expect("estimate");
        assert!(est.scan_bytes > 0, "a real table has real scan bytes");
        assert_eq!(
            est.working_set_bytes, 0,
            "a plain scan with no operators needs no working set, regardless of result size"
        );
        assert!(
            est.notes
                .iter()
                .any(|n| n.contains("no term for the query's OUTPUT")),
            "the estimate must say explicitly that result size is not modeled"
        );
    }

    /// Projecting fewer columns shrinks the scan-bytes term relative to
    /// `SELECT *`, in the direction and rough proportion the column-count
    /// heuristic predicts (a sane factor, not an exact match — this is a
    /// documented approximation).
    #[tokio::test]
    async fn projection_shrinks_the_scan_estimate() {
        let catalog = memory_catalog().await;
        let ident = parse_table_ident("sales.wide").expect("ident");
        let src = source_file("wide.csv", &wide_orders_csv(500));
        write::create_table(catalog.as_ref(), &ident, &src, None)
            .await
            .expect("create");

        let full = estimate(catalog.clone(), "SELECT * FROM sales.wide", None)
            .await
            .expect("full estimate");
        let projected = estimate(catalog.clone(), "SELECT id FROM sales.wide", None)
            .await
            .expect("projected estimate");

        assert!(
            projected.scan_bytes < full.scan_bytes,
            "projected {} should be smaller than full {}",
            projected.scan_bytes,
            full.scan_bytes
        );
        // 5 columns total, 1 selected: expect roughly a fifth, within a sane
        // (generous) factor since this is a column-count, not byte, ratio.
        let expected = full.scan_bytes / 5;
        assert!(
            projected.scan_bytes <= expected * 3 && projected.scan_bytes >= expected / 3,
            "projected {} should be within a sane factor of the {}/5 column-ratio expectation \
             ({})",
            projected.scan_bytes,
            full.scan_bytes,
            expected
        );
    }

    /// A `WHERE` filter shrinks the scan estimate relative to the unfiltered
    /// query — direction only (the fixed selectivity factor is a heuristic,
    /// not a real predicate cost), which is exactly what this estimator can
    /// honestly claim.
    #[tokio::test]
    async fn filter_shrinks_the_scan_estimate() {
        let catalog = memory_catalog().await;
        let ident = parse_table_ident("sales.filtered").expect("ident");
        let src = source_file("filtered.csv", &wide_orders_csv(500));
        write::create_table(catalog.as_ref(), &ident, &src, None)
            .await
            .expect("create");

        let full = estimate(catalog.clone(), "SELECT * FROM sales.filtered", None)
            .await
            .expect("full estimate");
        let filtered = estimate(
            catalog.clone(),
            "SELECT * FROM sales.filtered WHERE id > 100",
            None,
        )
        .await
        .expect("filtered estimate");

        assert!(
            filtered.scan_bytes < full.scan_bytes,
            "filtered {} should be smaller than full {}",
            filtered.scan_bytes,
            full.scan_bytes
        );
    }

    /// A join across two tables adds a positive working-set term on top of
    /// the sum of both scans — an aggregate-free, join-free single-table
    /// query has zero working-set bytes, so this is a real, checkable
    /// difference, not just "greater than zero" noise.
    #[tokio::test]
    async fn join_adds_a_working_set_term() {
        let catalog = memory_catalog().await;
        let left = parse_table_ident("sales.a").expect("ident");
        let right = parse_table_ident("sales.b").expect("ident");
        write::create_table(
            catalog.as_ref(),
            &left,
            &source_file("a.csv", &wide_orders_csv(200)),
            None,
        )
        .await
        .expect("create a");
        write::create_table(
            catalog.as_ref(),
            &right,
            &source_file("b.csv", &wide_orders_csv(200)),
            None,
        )
        .await
        .expect("create b");

        let single = estimate(catalog.clone(), "SELECT * FROM sales.a", None)
            .await
            .expect("single-table estimate");
        assert_eq!(single.working_set_bytes, 0, "no join/agg/sort here");

        let joined = estimate(
            catalog.clone(),
            "SELECT sales.a.id FROM sales.a JOIN sales.b ON sales.a.id = sales.b.id",
            None,
        )
        .await
        .expect("join estimate");
        assert!(
            joined.working_set_bytes > 0,
            "a join should add a positive working-set term"
        );
    }

    /// A query naming a table that does not exist fails the same way
    /// `estimate` and `query` both plan through DataFusion — `estimate`
    /// should never claim success for a request that could never actually
    /// run.
    #[tokio::test]
    async fn unplannable_query_errors_like_query_does() {
        let catalog = memory_catalog().await;
        let ident = parse_table_ident("sales.known").expect("ident");
        write::create_table(
            catalog.as_ref(),
            &ident,
            &source_file("known.csv", &wide_orders_csv(10)),
            None,
        )
        .await
        .expect("create");

        let err = estimate(catalog.clone(), "SELECT * FROM sales.nonexistent", None)
            .await
            .expect_err("unplannable query should error, matching query()'s own behavior");
        assert!(matches!(err, AgentError::Query(_)));
    }
}
