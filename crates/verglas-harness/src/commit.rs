//! The idempotent batch commit and the durable watermark store both harnesses
//! share.
//!
//! A harness commits a Job's output under an idempotency key and records the
//! consumed range in a durable watermark. Commit precedes acknowledgment: the
//! keyed append is written first, then the watermark advances, so a crash
//! between the two re-consumes the same range and the key makes the re-commit a
//! no-op rather than a duplicate. The key rides on the committed snapshot's
//! summary — exactly the property names [`verglas_iceberg::tables_api`] writes —
//! so the table itself detects the replay, with no external dedup store.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use iceberg::{Catalog, TableIdent};
use opendal::{ErrorKind, Operator};

/// The snapshot-summary property recording a commit's idempotency key. Matches
/// `verglas_iceberg::tables_api` so a keyed harness commit and an SDK commit are
/// detected the same way.
const IDEMPOTENCY_KEY_PROP: &str = "verglas.commit.idempotency-key";

/// The snapshot-summary property recording how many rows a keyed commit wrote,
/// so a replay reports the original count without re-reading the data.
const IDEMPOTENCY_ROWS_PROP: &str = "verglas.commit.rows";

/// Errors a harness raises around the commit and watermark path.
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
    /// A watermark could not be read or written.
    #[error("watermark store: {0}")]
    Watermark(String),
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
/// `extra_props` (an MV's consumed-range record, say) ride on the same snapshot.
/// The target table must already exist; a source harness creates it from the
/// connector's discovered schema before the first commit.
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

/// A deployment's durable watermark: the opaque cursor a harness resumes from.
///
/// The value is opaque to the store — a harness serializes whatever position it
/// needs (a snapshot id, a cron cursor) as a string. Locally this is the
/// `verglas_sys.watermarks` table behind the daemon's `/v1/watermark`; the
/// in-memory store here backs the tests.
#[async_trait]
pub trait WatermarkStore: Send + Sync {
    /// The current watermark for `deployment`, or `None` before the first set.
    async fn get(&self, deployment: &str) -> Result<Option<String>, HarnessError>;

    /// Stores `deployment`'s watermark. Called after the keyed commit, so the
    /// commit is always at least as far along as the watermark.
    async fn set(&self, deployment: &str, watermark: String) -> Result<(), HarnessError>;
}

/// An in-memory watermark store for tests and one-shot runs.
#[derive(Default, Clone)]
pub struct MemoryWatermarkStore {
    inner: Arc<Mutex<HashMap<String, String>>>,
}

impl MemoryWatermarkStore {
    /// A fresh empty store.
    pub fn new() -> MemoryWatermarkStore {
        MemoryWatermarkStore::default()
    }
}

#[async_trait]
impl WatermarkStore for MemoryWatermarkStore {
    /// Reads the in-memory watermark.
    async fn get(&self, deployment: &str) -> Result<Option<String>, HarnessError> {
        let map = self
            .inner
            .lock()
            .map_err(|e| HarnessError::Watermark(e.to_string()))?;
        Ok(map.get(deployment).cloned())
    }

    /// Writes the in-memory watermark.
    async fn set(&self, deployment: &str, watermark: String) -> Result<(), HarnessError> {
        let mut map = self
            .inner
            .lock()
            .map_err(|e| HarnessError::Watermark(e.to_string()))?;
        map.insert(deployment.to_owned(), watermark);
        Ok(())
    }
}

/// Where a durable watermark store keeps its objects: the warehouse bucket
/// reached over the same S3/R2 keypair the catalog FileIO uses, under a
/// tenant-scoped key prefix.
///
/// The harness already carries these values in its launch spec's catalog half;
/// the store reuses them so a fleet run needs no extra credential. Path-style
/// addressing is used (what R2 and MinIO serve), matching the catalog's FileIO.
#[derive(Debug, Clone)]
pub struct S3WatermarkConfig {
    /// S3/R2 data endpoint, e.g. the account's `*.r2.cloudflarestorage.com`.
    pub endpoint: String,
    /// SigV4 signing region (R2 uses `auto`).
    pub region: String,
    /// S3/R2 access key id.
    pub access_key_id: String,
    /// S3/R2 secret access key.
    pub secret_access_key: String,
    /// The warehouse bucket the watermark objects live in.
    pub bucket: String,
    /// The tenant-scoped key prefix, e.g. `catalog/<tenant>/_watermarks`. Each
    /// deployment's cursor is one `<deployment>.json` object under it.
    pub prefix: String,
}

/// A durable [`WatermarkStore`] backed by an S3/R2 object per deployment.
///
/// A cron run resumes from the object rather than re-bootstrapping: `set` writes
/// the deployment's cursor after each committed batch, `get` reads it back at the
/// next run's start. The object body is the opaque watermark string the executor
/// produced; the store never interprets it. A missing object (`NotFound`) reads
/// as `None`, so the first run starts fresh; any other transport error surfaces
/// as [`HarnessError::Watermark`].
///
/// Extension point: only S3-shaped services are wired here (R2/MinIO/S3). A
/// future non-S3 warehouse would add a constructor; the trait surface stays.
#[derive(Clone)]
pub struct S3WatermarkStore {
    /// The OpenDAL operator rooted at the warehouse bucket.
    op: Operator,
    /// The key prefix every deployment's object hangs under, no trailing slash.
    prefix: String,
}

impl S3WatermarkStore {
    /// Builds an S3-backed store from `config`, constructing an OpenDAL S3
    /// operator over the endpoint, region, and keypair in path-style addressing.
    pub fn new(config: S3WatermarkConfig) -> Result<S3WatermarkStore, HarnessError> {
        let builder = opendal::services::S3::default()
            .bucket(&config.bucket)
            .endpoint(&config.endpoint)
            .region(&config.region)
            .access_key_id(&config.access_key_id)
            .secret_access_key(&config.secret_access_key);
        let op = Operator::new(builder)
            .map_err(|e| HarnessError::Watermark(format!("building S3 watermark store: {e}")))?
            .finish();
        Ok(S3WatermarkStore::from_operator(op, config.prefix))
    }

    /// Wraps an already-built OpenDAL `op` rooted at the warehouse bucket. Lets a
    /// test drive the get/set/NotFound contract over an in-memory operator.
    pub fn from_operator(op: Operator, prefix: String) -> S3WatermarkStore {
        S3WatermarkStore {
            op,
            prefix: prefix.trim_end_matches('/').to_owned(),
        }
    }

    /// The object key for `deployment`: `<prefix>/<deployment>.json`.
    fn key(&self, deployment: &str) -> String {
        format!("{}/{}.json", self.prefix, deployment)
    }
}

#[async_trait]
impl WatermarkStore for S3WatermarkStore {
    /// Reads the deployment's watermark object. A `NotFound` object is `None`
    /// (the deployment has never committed); any other error is a store failure.
    async fn get(&self, deployment: &str) -> Result<Option<String>, HarnessError> {
        match self.op.read(&self.key(deployment)).await {
            Ok(buf) => {
                let text = String::from_utf8(buf.to_vec()).map_err(|e| {
                    HarnessError::Watermark(format!("watermark object is not UTF-8: {e}"))
                })?;
                Ok(Some(text))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(HarnessError::Watermark(format!(
                "reading watermark for {deployment}: {e}"
            ))),
        }
    }

    /// Overwrites the deployment's watermark object with `watermark`.
    async fn set(&self, deployment: &str, watermark: String) -> Result<(), HarnessError> {
        self.op
            .write(&self.key(deployment), watermark.into_bytes())
            .await
            .map_err(|e| {
                HarnessError::Watermark(format!("writing watermark for {deployment}: {e}"))
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An in-memory OpenDAL operator standing in for the warehouse bucket, so the
    /// durable store's contract is exercised without a live S3/R2 endpoint.
    fn memory_store(prefix: &str) -> S3WatermarkStore {
        let op = Operator::new(opendal::services::Memory::default())
            .expect("memory operator")
            .finish();
        S3WatermarkStore::from_operator(op, prefix.to_owned())
    }

    /// A deployment with no object reads as `None` — the first run starts fresh.
    #[tokio::test]
    async fn get_missing_is_none() {
        let store = memory_store("catalog/tenant/_watermarks");
        assert_eq!(store.get("fred").await.expect("get"), None);
    }

    /// `set` then `get` round-trips the exact watermark string durably in the
    /// object store, and `set` again overwrites it — the cron resume contract.
    #[tokio::test]
    async fn set_get_roundtrip_and_overwrite() {
        let store = memory_store("catalog/tenant/_watermarks");
        store
            .set("fred", "{\"pos\":42}".to_owned())
            .await
            .expect("set");
        assert_eq!(
            store.get("fred").await.expect("get"),
            Some("{\"pos\":42}".to_owned())
        );
        store
            .set("fred", "{\"pos\":99}".to_owned())
            .await
            .expect("overwrite");
        assert_eq!(
            store.get("fred").await.expect("get"),
            Some("{\"pos\":99}".to_owned())
        );
    }

    /// Deployments are isolated by key, and one deployment's watermark never
    /// leaks into another's `get`.
    #[tokio::test]
    async fn deployments_are_isolated() {
        let store = memory_store("catalog/tenant/_watermarks/");
        store.set("a", "one".to_owned()).await.expect("set a");
        store.set("b", "two".to_owned()).await.expect("set b");
        assert_eq!(store.get("a").await.expect("get a"), Some("one".to_owned()));
        assert_eq!(store.get("b").await.expect("get b"), Some("two".to_owned()));
        assert_eq!(store.get("c").await.expect("get c"), None);
    }

    /// A trailing slash on the prefix is trimmed so the key has exactly one
    /// separator before `<deployment>.json`.
    #[test]
    fn key_trims_trailing_slash() {
        let store = memory_store("catalog/tenant/_watermarks/");
        assert_eq!(store.key("fred"), "catalog/tenant/_watermarks/fred.json");
    }
}
