//! The idempotent batch commit the worker harness uses.
//!
//! A harness commits a worker's output under an idempotency key. Correctness
//! under retry rests on that key alone: the keyed append is written first, and a
//! crash-and-replay re-commits the same rows under the same key so the table
//! itself detects the duplicate. The key rides on the committed snapshot's
//! summary — exactly the property names [`verglas_iceberg::tables_api`] writes —
//! so the table itself detects the replay, with no external dedup store.
//!
//! There is no deployment watermark store. Cross-run progress for cron workers
//! is the trigger's logical interval; table/queue cursors are separate read
//! positions, not a harness-owned cell.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use iceberg::{Catalog, TableIdent};

/// The snapshot-summary property recording a commit's idempotency key. Matches
/// `verglas_iceberg::tables_api` so a keyed harness commit and an SDK commit are
/// detected the same way.
const IDEMPOTENCY_KEY_PROP: &str = "verglas.commit.idempotency-key";

/// The snapshot-summary property recording how many rows a keyed commit wrote,
/// so a replay reports the original count without re-reading the data.
const IDEMPOTENCY_ROWS_PROP: &str = "verglas.commit.rows";

/// Errors a harness raises around the commit path.
#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    /// An Iceberg catalog, table, or write operation failed.
    #[error("iceberg: {0}")]
    Iceberg(#[from] iceberg::Error),
    /// The engine write path failed.
    #[error(transparent)]
    Write(#[from] verglas_iceberg::AgentError),
    /// The subprocess transport or a Job failed.
    #[error("job: {0}")]
    Job(String),
}

/// What one keyed commit did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOutcome {
    /// Rows written this commit (the original count on a replay).
    pub rows_committed: u64,
    /// The output table's snapshot id after the commit.
    pub snapshot_id: Option<i64>,
    /// True when the key matched an earlier commit and nothing was written.
    pub replayed: bool,
}

/// Appends `batches` to `ident` under `key`, replay-safe.
///
/// If a snapshot of the table already carries `key`, the append is a replay: the
/// original snapshot and row count are returned and nothing is written. Extra
/// `extra_props` ride on the same snapshot. The target table must already exist;
/// a source harness creates it from the connector's discovered schema before the
/// first commit.
pub async fn commit_batches_keyed(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    batches: Vec<RecordBatch>,
    key: &str,
    extra_props: HashMap<String, String>,
) -> Result<CommitOutcome, HarnessError> {
    let table = catalog.load_table(ident).await?;

    // Replay: the key already rode on a committed snapshot.
    for snapshot in table.metadata().snapshots() {
        let props = &snapshot.summary().additional_properties;
        if props.get(IDEMPOTENCY_KEY_PROP).map(String::as_str) == Some(key) {
            let rows_committed = props
                .get(IDEMPOTENCY_ROWS_PROP)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            return Ok(CommitOutcome {
                rows_committed,
                snapshot_id: Some(snapshot.snapshot_id()),
                replayed: true,
            });
        }
    }

    let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let mut props = extra_props;
    props.insert(IDEMPOTENCY_KEY_PROP.to_owned(), key.to_owned());
    props.insert(IDEMPOTENCY_ROWS_PROP.to_owned(), row_count.to_string());

    let report = verglas_iceberg::write::append_batches(catalog, ident, batches, props).await?;
    Ok(CommitOutcome {
        rows_committed: report.records_added,
        snapshot_id: report.snapshot_id,
        replayed: false,
    })
}
