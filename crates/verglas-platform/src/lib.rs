//! The local platform registry: the `verglas_sys` catalog of worker
//! declarations that describe the agent-data platform's dataflow.
//!
//! This crate is the local projection of the unified deployment record
//! (WHITEPAPER §7.1). It is not memory-specific — the server supervisor and the
//! CLI both read and write it — so it lives on its own, below the harnesses and
//! the memory workflow.
//!
//! The platform owns its control plane as Iceberg tables under the `verglas_sys`
//! namespace, written with the same append-only, revision-not-mutation discipline
//! the raw memory table uses. A declaration is a row keyed by name; pause and
//! resume append a new revision rather than rewriting a row, so the Iceberg
//! snapshot log is the audit trail of every state change.
//!
//! Single node only for v0: `placement` is always `local`; there is no leader,
//! no remote placement, no push delivery. Those are extension points, noted in
//! the schema (the `placement` column) but not built.
//!
//! The tables:
//! - `verglas_sys.workers` — the single deployment registry (code + triggers +
//!   output). What the worker runtime reads each tick.
//!
//! [`Deployment`] is the canonical projection of a worker row into the unified
//! deployment record.

mod rows;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use iceberg::spec::DataFileFormat;
use iceberg::spec::{FormatVersion, Schema};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;
use serde::{Serialize, Serializer};

pub use rows::{PLACEMENT_LOCAL, WORKERS_TABLE, WorkerRow, WorkerSpec};

pub const SYSTEM_NAMESPACE: &str = "verglas_sys";

/// The lifecycle state of a worker (or other system declaration).
///
/// `Completed` is for a bounded flow that ran to completion (a one-shot ingest
/// or a backfill); the unbounded flows (hook capture, a log tail) sit in
/// `Running` until paused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemState {
    /// Declared but not yet started.
    Created,
    /// Actively running (an unbounded flow follows; a bounded flow is in
    /// progress).
    Running,
    /// Paused by an operator. Execution stops until resumed.
    Paused,
    /// A bounded flow that ran to completion.
    Completed,
    /// Stopped on an error.
    Error,
    /// Removed from the active list but kept on the record. An archived worker
    /// is hidden from the default list; `resume` (or a re-register) brings it
    /// back by appending a `running` revision. Nothing is destroyed — the
    /// registry is append-only, so archiving is just another revision.
    Archived,
}

impl SystemState {
    /// The stable string stored in the `state` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            SystemState::Created => "created",
            SystemState::Running => "running",
            SystemState::Paused => "paused",
            SystemState::Completed => "completed",
            SystemState::Error => "error",
            SystemState::Archived => "archived",
        }
    }

    /// Parses a stored `state` string; an unknown value is a decode error so a
    /// corrupt row is visible rather than silently coerced.
    pub fn parse(s: &str) -> Result<SystemState, PlatformError> {
        match s {
            "created" => Ok(SystemState::Created),
            "running" => Ok(SystemState::Running),
            "paused" => Ok(SystemState::Paused),
            "completed" => Ok(SystemState::Completed),
            "error" => Ok(SystemState::Error),
            "archived" => Ok(SystemState::Archived),
            other => Err(PlatformError::Decode(format!("unknown state: {other}"))),
        }
    }
}

impl Serialize for SystemState {
    /// Serializes as the stable lowercase string.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

/// Errors from the system-table control plane.
#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    /// An Iceberg catalog, table, or write operation failed.
    #[error("iceberg: {0}")]
    Iceberg(#[from] iceberg::Error),
    /// A scanned row did not match the expected schema.
    #[error("decode: {0}")]
    Decode(String),
    /// A state change named a worker that does not exist.
    #[error("not found: no worker named {name}")]
    NotFound {
        /// The requested worker name.
        name: String,
    },
}

/// The control plane over one catalog's worker registry.
pub struct SystemCatalog {
    catalog: Arc<dyn Catalog>,
}

impl SystemCatalog {
    /// Wraps a catalog. The system tables are created on first write.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        SystemCatalog { catalog }
    }

    /// Ensures a system table exists (namespace and table), creating it from
    /// `schema` on first use. Idempotent: an existing table is loaded.
    async fn ensure_table(&self, table_name: &str, schema: Schema) -> Result<Table, PlatformError> {
        let namespace = NamespaceIdent::new(SYSTEM_NAMESPACE.to_owned());
        let ident = TableIdent::new(namespace.clone(), table_name.to_owned());
        if let Ok(table) = self.catalog.load_table(&ident).await {
            return Ok(table);
        }
        // Create the namespace, tolerating a concurrent create or a pre-existing
        // one (some REST catalogs answer the existence HEAD probe with 400).
        if let Err(e) = self
            .catalog
            .create_namespace(&namespace, HashMap::new())
            .await
        {
            tracing::debug!("create_namespace(verglas_sys) returned {e}; continuing");
        }
        let creation = TableCreation::builder()
            .name(table_name.to_owned())
            .schema(schema)
            .format_version(FormatVersion::V2)
            .build();
        match self.catalog.create_table(&namespace, creation).await {
            Ok(table) => Ok(table),
            // Lost a create race: the table now exists.
            Err(_) if self.catalog.load_table(&ident).await.is_ok() => {
                Ok(self.catalog.load_table(&ident).await?)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Commits `batch` as one fast-append to an unpartitioned system table.
    async fn append_batch(&self, table: &Table, batch: RecordBatch) -> Result<(), PlatformError> {
        let schema = table.metadata().current_schema().clone();
        let location = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name = DefaultFileNameGenerator::new(
            "verglas_system".to_owned(),
            Some(uuid::Uuid::new_v4().to_string()),
            DataFileFormat::Parquet,
        );
        let parquet = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            table.file_io().clone(),
            location,
            file_name,
        );
        let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
        writer.write(batch).await?;
        let data_files = writer.close().await?;

        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action.apply(tx)?;
        tx.commit(self.catalog.as_ref()).await?;
        Ok(())
    }

    /// Scans every row of a system table's current snapshot as record batches.
    async fn scan_all(&self, table: &Table) -> Result<Vec<RecordBatch>, PlatformError> {
        if table.metadata().current_snapshot().is_none() {
            return Ok(Vec::new());
        }
        let scan = table.scan().select_all().build()?;
        Ok(scan.to_arrow().await?.try_collect().await?)
    }

    /// Declares a worker, appending a new revision in `Running`. Redeclaring an
    /// existing worker bumps the revision and preserves its `created_at`. This is
    /// the idempotent declare the deploy path calls.
    pub async fn register_worker(&self, spec: WorkerSpec) -> Result<WorkerRow, PlatformError> {
        self.register_worker_state(spec, SystemState::Running).await
    }

    /// Declares a worker at an explicit state.
    pub async fn register_worker_state(
        &self,
        spec: WorkerSpec,
        state: SystemState,
    ) -> Result<WorkerRow, PlatformError> {
        let table = self
            .ensure_table(WORKERS_TABLE, rows::worker_schema())
            .await?;
        let existing = self.current_worker(&table, &spec.name).await?;
        let (revision, created_at) = match &existing {
            Some(r) => (r.revision + 1, r.created_at),
            None => (1, chrono::Utc::now()),
        };
        let row = WorkerRow {
            name: spec.name,
            code: spec.code,
            triggers: spec.triggers,
            output: spec.output,
            config: spec.config,
            state,
            placement: PLACEMENT_LOCAL.to_owned(),
            created_by: spec.created_by,
            created_at,
            revision,
        };
        let batch = rows::encode_workers(
            std::slice::from_ref(&row),
            table.metadata().current_schema(),
        );
        self.append_batch(&table, batch).await?;
        Ok(row)
    }

    /// The current view of all workers: the highest-revision row per name.
    pub async fn list_workers(&self) -> Result<Vec<WorkerRow>, PlatformError> {
        let table = self
            .ensure_table(WORKERS_TABLE, rows::worker_schema())
            .await?;
        let all = self.decode_worker_rows(&table).await?;
        Ok(current_view(all, |r| (r.name.clone(), r.revision)))
    }

    /// The active worker view: the current revision of every non-archived
    /// worker. This is what the supervisor reads each tick.
    pub async fn list_active_workers(&self) -> Result<Vec<WorkerRow>, PlatformError> {
        let mut rows = self.list_workers().await?;
        rows.retain(|r| r.state != SystemState::Archived);
        Ok(rows)
    }

    /// The current (highest-revision) row for one worker, or `None`.
    pub async fn get_worker(&self, name: &str) -> Result<Option<WorkerRow>, PlatformError> {
        let table = self
            .ensure_table(WORKERS_TABLE, rows::worker_schema())
            .await?;
        self.current_worker(&table, name).await
    }

    /// Pauses or resumes a worker by appending a revision that flips its state.
    /// Every other field is carried forward; an unknown worker is NotFound.
    pub async fn set_worker_state(
        &self,
        name: &str,
        state: SystemState,
    ) -> Result<WorkerRow, PlatformError> {
        let table = self
            .ensure_table(WORKERS_TABLE, rows::worker_schema())
            .await?;
        let current =
            self.current_worker(&table, name)
                .await?
                .ok_or_else(|| PlatformError::NotFound {
                    name: name.to_owned(),
                })?;
        let next = WorkerRow {
            state,
            revision: current.revision + 1,
            ..current
        };
        let batch = rows::encode_workers(
            std::slice::from_ref(&next),
            table.metadata().current_schema(),
        );
        self.append_batch(&table, batch).await?;
        Ok(next)
    }

    /// Decodes all worker rows.
    async fn decode_worker_rows(&self, table: &Table) -> Result<Vec<WorkerRow>, PlatformError> {
        let mut out = Vec::new();
        for batch in self.scan_all(table).await? {
            out.extend(rows::decode_workers(&batch)?);
        }
        Ok(out)
    }

    /// The highest-revision row for one worker name.
    async fn current_worker(
        &self,
        table: &Table,
        name: &str,
    ) -> Result<Option<WorkerRow>, PlatformError> {
        Ok(self
            .decode_worker_rows(table)
            .await?
            .into_iter()
            .filter(|r| r.name == name)
            .max_by_key(|r| r.revision))
    }

}

/// One deployment as a single record — the canonical shape §7.1 describes for
/// a `verglas_sys` worker row. A deployment is `kind × trigger × placement`
/// plus its code, config, and target tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Deployment {
    /// Always `"worker"` for local registry rows.
    pub kind: String,
    /// The deployment name (unique within a kind).
    pub name: String,
    /// How it is invoked: derived from triggers (`cron`, `webhook`, or `manual`).
    pub trigger: String,
    /// Always `"local"` for v0 (single-node only).
    pub placement: String,
    /// The Job code or an artifact reference.
    pub code: String,
    /// Unused for workers (schedule lives inside triggers JSON); always `None`.
    pub schedule: Option<String>,
    /// The target tables the deployment writes.
    pub target_tables: Vec<String>,
    /// The lifecycle state (`running`, `paused`, ...).
    pub status: String,
    /// The deployment config as a JSON string.
    pub config: String,
    /// When the deployment was first declared.
    pub created_at: DateTime<Utc>,
}

/// Picks a coarse trigger label from a worker's triggers JSON for the unified
/// deployment record. Cron wins over webhook when both are present; otherwise
/// the deployment is on-demand (`manual`).
fn trigger_from_worker(triggers_json: &str) -> String {
    let Ok(triggers) = serde_json::from_str::<Vec<serde_json::Value>>(triggers_json) else {
        return "manual".to_owned();
    };
    let mut has_webhook = false;
    for trigger in triggers {
        match trigger.get("type").and_then(|v| v.as_str()) {
            Some("cron") => return "cron".to_owned(),
            Some("webhook") => has_webhook = true,
            _ => {}
        }
    }
    if has_webhook {
        "webhook".to_owned()
    } else {
        "manual".to_owned()
    }
}

impl Deployment {
    /// Projects a worker row into the unified record.
    pub fn from_worker(row: &WorkerRow) -> Deployment {
        let mut target_tables = Vec::new();
        if let Some(output) = row.output.as_deref().filter(|s| !s.is_empty()) {
            target_tables.push(output.to_owned());
        }
        Deployment {
            kind: "worker".to_owned(),
            name: row.name.clone(),
            trigger: trigger_from_worker(&row.triggers),
            placement: row.placement.clone(),
            code: row.code.clone(),
            schedule: None,
            target_tables,
            status: row.state.as_str().to_owned(),
            config: row.config.clone(),
            created_at: row.created_at,
        }
    }
}

/// Reduces an append-only row set to the current view: the highest-revision row
/// per name. `key` yields `(name, revision)` for a row.
fn current_view<T, F>(rows: Vec<T>, key: F) -> Vec<T>
where
    F: Fn(&T) -> (String, i64),
{
    let mut best: HashMap<String, T> = HashMap::new();
    for row in rows {
        let (name, revision) = key(&row);
        match best.get(&name) {
            Some(existing) if key(existing).1 >= revision => {}
            _ => {
                best.insert(name, row);
            }
        }
    }
    let mut out: Vec<T> = best.into_values().collect();
    out.sort_by_key(|r| key(r).0);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every state string round-trips through parse/as_str.
    #[test]
    fn state_roundtrips() {
        for s in [
            SystemState::Created,
            SystemState::Running,
            SystemState::Paused,
            SystemState::Completed,
            SystemState::Error,
            SystemState::Archived,
        ] {
            assert_eq!(SystemState::parse(s.as_str()).expect("parse"), s);
        }
        assert!(SystemState::parse("bogus").is_err());
    }

    /// The current view keeps the highest revision per name and drops the rest.
    #[test]
    fn current_view_keeps_highest_revision() {
        let rows = vec![("a", 1), ("a", 3), ("a", 2), ("b", 1)];
        let view = current_view(rows, |(n, r)| (n.to_string(), *r));
        assert_eq!(view, vec![("a", 3), ("b", 1)]);
    }
}
