//! The index-maintenance materialized view.
//!
//! This is the MV Job the platform's MV harness drives (§7.2): per run it reads
//! the source table's Iceberg delta since the index's watermark, applies the
//! streaming Vamana updates, consolidates past a tombstone threshold, and
//! commits the updated Puffin blob as a snapshot-bound Iceberg statistics file.
//!
//! ## Contract
//!
//! - **Delta read.** `verglas_iceberg::tables_api::delta(catalog, ident, since,
//!   _)` returns the rows appended since `since` (a snapshot-id watermark). The
//!   write path is append-only, so a delta is exactly the new rows. Deletes
//!   ride the same append stream as a **null embedding** for an id (a row
//!   re-appended with a null vector is a delete) — this keeps the maintenance
//!   path fully incremental instead of diffing full row sets. An
//!   insert/update is a row with a non-null embedding (same id replaces).
//! - **Apply.** Non-null embedding → [`VamanaIndex::insert`] (RobustPrune +
//!   backward-edge repair; a replace tombstones the old vector). Null embedding
//!   → [`VamanaIndex::delete`] (lazy tombstone).
//! - **Consolidate.** When the tombstone fraction reaches
//!   [`MaintenanceConfig::consolidate_threshold`], run
//!   [`VamanaIndex::consolidate`] (FreshDiskANN StreamingMerge) to rewire and
//!   physically drop tombstones. Otherwise tombstones persist in the blob (the
//!   on-disk DeleteList) and are filtered at query time.
//! - **Watermark = the reflected snapshot.** The updated index's
//!   `reflected_snapshot` is the delta's new tip. Persisting the blob (whose
//!   body encodes that snapshot) and attaching it through the catalog IS the
//!   commit; there is no separate registry. On the next run the attachment's
//!   reflected snapshot is read back as `since`. A crash
//!   before the persist re-consumes the same delta on restart — inserts are
//!   upserts and deletes idempotent, so the live result set is unchanged
//!   (commit-before-ack, kill-and-resume equivalent).
//! - **Initial run.** No prior blob → `since = None` → the delta is the whole
//!   table → a full build.

use iceberg::{Catalog, TableIdent};
use serde_json::Value;

use crate::error::{Result, VectorError};
use crate::metric::Metric;
use crate::vamana::{VamanaIndex, VamanaParams};

/// The default tombstone fraction that triggers a consolidation. Not a user
/// knob — a maintenance-internal threshold (FreshDiskANN consolidates
/// periodically, not per-delete). At 0.2, a fifth of the index being dead
/// triggers the rewrite.
pub const DEFAULT_CONSOLIDATE_THRESHOLD: f32 = 0.2;

/// Fixed RNG seed for deterministic, reproducible index construction across
/// maintenance runs.
const BUILD_SEED: u64 = 0x5645_5247_4C41_5331; // "VERGLAS1"

/// How the maintenance MV reads a source row's identity column into the `i64`
/// key the Vamana index stores. The index engine keys every vector by an `i64`;
/// this says how to derive that `i64` from the source column so a table keyed by
/// a non-integer identity (the memory table, keyed by a `uuid` `memory_id`) is
/// indexable without a schema migration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IdEncoding {
    /// The id column is an integer (number, or a string that parses to `i64`).
    /// The default — every table declared through the server's generic index
    /// route.
    #[default]
    Integer,
    /// The id column is a `uuid` string, folded to a stable `i64` by
    /// [`uuid_hash_id`]. The recall path re-derives the same `i64` from a
    /// candidate's `memory_id` to map an ANN neighbor back to its memory.
    UuidHash,
}

impl IdEncoding {
    /// The wire token stored in the durable registry row's params JSON.
    pub fn as_str(self) -> &'static str {
        match self {
            IdEncoding::Integer => "integer",
            IdEncoding::UuidHash => "uuidHash",
        }
    }

    /// Parses the wire token back; unknown/absent tokens are [`IdEncoding::Integer`].
    pub fn parse(s: &str) -> IdEncoding {
        match s {
            "uuidHash" => IdEncoding::UuidHash,
            _ => IdEncoding::Integer,
        }
    }
}

/// Folds a `uuid` string to a stable, sign-agnostic `i64` key: the two 64-bit
/// halves of the 128-bit value XOR-ed together. Deterministic, so the
/// maintenance build and the recall mapping agree on the same key for a row.
/// Returns `None` when `s` is not a valid uuid.
pub fn uuid_hash_id(s: &str) -> Option<i64> {
    let u = uuid::Uuid::parse_str(s).ok()?;
    let bits = u.as_u128();
    Some(((bits >> 64) as u64 ^ bits as u64) as i64)
}

/// How the maintenance MV reads and indexes a source field.
#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// The identity column, keying vectors. Read into the index's `i64` key per
    /// [`MaintenanceConfig::id_encoding`].
    pub id_field: String,
    /// The embedding column to index.
    pub vec_field: String,
    /// The distance metric (recorded in the blob, never inferred).
    pub metric: Metric,
    /// How the id column is read into the index's `i64` key.
    pub id_encoding: IdEncoding,
    /// Vamana construction/insert parameters.
    pub params: VamanaParams,
    /// Tombstone fraction that triggers consolidation.
    pub consolidate_threshold: f32,
}

impl MaintenanceConfig {
    /// A config for `(id_field, vec_field, metric)` with default params,
    /// threshold, and integer id encoding.
    pub fn new(id_field: impl Into<String>, vec_field: impl Into<String>, metric: Metric) -> Self {
        MaintenanceConfig {
            id_field: id_field.into(),
            vec_field: vec_field.into(),
            metric,
            id_encoding: IdEncoding::default(),
            params: VamanaParams::default(),
            consolidate_threshold: DEFAULT_CONSOLIDATE_THRESHOLD,
        }
    }

    /// Sets the id encoding (builder style), for the memory index's `uuid` key.
    pub fn with_id_encoding(mut self, encoding: IdEncoding) -> Self {
        self.id_encoding = encoding;
        self
    }
}

/// The outcome of one maintenance run.
#[derive(Debug, Clone)]
pub struct MaintenanceReport {
    /// The distance metric encoded in the attached index.
    pub metric: Metric,
    /// The source snapshot the index now reflects.
    pub reflected_snapshot: i64,
    /// The snapshot the index reflected before this run, if any.
    pub prior_snapshot: Option<i64>,
    /// Whether this run built the index from empty (initial or forced rebuild).
    pub full_build: bool,
    /// Vectors inserted or updated this run.
    pub inserts: usize,
    /// Vectors deleted (tombstoned) this run.
    pub deletes: usize,
    /// Whether a consolidation ran.
    pub consolidated: bool,
    /// Live vectors after the run.
    pub live_count: usize,
    /// Tombstones remaining in the persisted blob after the run.
    pub tombstones: usize,
    /// Where the blob was persisted.
    pub blob_location: String,
    /// The persisted Puffin file size.
    pub blob_bytes: usize,
}

/// Runs one maintenance pass. Returns `None` when the source table has no rows
/// yet (nothing to index, no blob written). Otherwise persists the updated blob
/// and returns the report.
pub async fn run_maintenance(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    config: &MaintenanceConfig,
) -> Result<Option<MaintenanceReport>> {
    let table = catalog.load_table(ident).await?;
    let Some(source_snapshot) = table.metadata().current_snapshot_id() else {
        return Ok(None);
    };
    // Load the nearest attached ancestor index and use its snapshot as the
    // incremental watermark.
    let prior =
        crate::attachment::load_latest_ancestor_index(&table, source_snapshot, &config.vec_field)
            .await?
            .map(|attached| attached.index);
    let prior_snapshot = prior.as_ref().map(|i| i.reflected_snapshot());
    let since = prior_snapshot.filter(|&s| s >= 0).map(|s| s.to_string());

    // Read exactly the delta after the attached ancestor. An invalid or expired
    // watermark is an error; this prototype has no implicit rebuild path.
    let response = verglas_iceberg::tables_api::delta(catalog, ident, since.clone(), None).await?;
    let (rows, watermark_str, full_build, mut index) =
        (response.rows, response.watermark, since.is_none(), prior);

    // An empty watermark means the table has no snapshot yet.
    if watermark_str.is_empty() {
        return Ok(None);
    }
    let reflected_snapshot: i64 = watermark_str.parse().map_err(|_| {
        VectorError::Iceberg(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            format!("non-numeric watermark '{watermark_str}'"),
        ))
    })?;

    let mut inserts = 0usize;
    let mut deletes = 0usize;
    let mut skipped_unreadable_ids = 0usize;
    for row in &rows {
        let Some((id, vector)) =
            parse_row(row, &config.id_field, &config.vec_field, config.id_encoding)?
        else {
            // Id missing or not decodable under the active encoding.
            skipped_unreadable_ids += 1;
            continue;
        };
        match vector {
            Some(v) => {
                let idx = ensure_index(&mut index, config, v.len())?;
                idx.insert(id, &v)?;
                inserts += 1;
            }
            None => {
                if let Some(idx) = index.as_mut()
                    && idx.delete(id)
                {
                    deletes += 1;
                }
            }
        }
    }

    // Full build that scanned rows but indexed none because every id was
    // unreadable under the active encoding is a configuration error — not a
    // successful empty index (HTTP 200 / inserts: 0). A truly empty table
    // (no rows) still returns `None` below.
    if full_build && inserts == 0 && skipped_unreadable_ids > 0 {
        return Err(VectorError::Field(
            "full build indexed zero rows: no source row yielded a usable id under the active encoding; \
             id must be an integer, or a UUID with uuidHash"
                .to_owned(),
        ));
    }

    // Nothing was ever indexed (no non-null embeddings across the whole table).
    let Some(mut index) = index else {
        return Ok(None);
    };
    index.set_reflected_snapshot(reflected_snapshot);

    // Consolidate past the tombstone threshold.
    let total = index.len() + index.tombstone_count();
    let frac = if total == 0 {
        0.0
    } else {
        index.tombstone_count() as f32 / total as f32
    };
    let consolidated = frac >= config.consolidate_threshold && index.tombstone_count() > 0;
    if consolidated {
        index.consolidate();
    }

    // Persist — this is the commit. The blob body encodes reflected_snapshot, so
    // the persisted blob is the durable watermark.
    let blob_location =
        crate::attachment::attach_index(catalog, &table, reflected_snapshot, config, &index)
            .await?;
    let blob_bytes = table
        .file_io()
        .new_input(&blob_location)?
        .read()
        .await?
        .len();

    Ok(Some(MaintenanceReport {
        metric: index.metric(),
        reflected_snapshot,
        prior_snapshot,
        full_build,
        inserts,
        deletes,
        consolidated,
        live_count: index.len(),
        tombstones: index.tombstone_count(),
        blob_location,
        blob_bytes,
    }))
}

/// Loads the index attached to the table's current snapshot for `field`.
/// `None` means the snapshot has no Vamana attachment for that field.
pub async fn load_latest_index(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    field: &str,
) -> Result<Option<VamanaIndex>> {
    let table = catalog.load_table(ident).await?;
    let Some(snapshot) = table.metadata().current_snapshot_id() else {
        return Ok(None);
    };
    Ok(
        crate::attachment::load_index_for_snapshot(&table, snapshot, field)
            .await?
            .map(|attached| attached.index),
    )
}

/// Lazily creates the index once the first vector's dimensionality is known.
fn ensure_index<'a>(
    index: &'a mut Option<VamanaIndex>,
    config: &MaintenanceConfig,
    dim: usize,
) -> Result<&'a mut VamanaIndex> {
    if index.is_none() {
        *index = Some(VamanaIndex::new(
            config.metric,
            dim,
            config.params,
            BUILD_SEED,
        )?);
    }
    Ok(index.as_mut().expect("index just set"))
}

/// Parses one JSON row into `(id, Option<vector>)`. `None` return = row has no
/// usable id (skipped). Inner `None` vector = null embedding (a delete). The
/// `encoding` decides how the id column is read into the `i64` key: an integer
/// column directly, or a `uuid` column folded by [`uuid_hash_id`].
pub(crate) fn parse_row(
    row: &Value,
    id_field: &str,
    vec_field: &str,
    encoding: IdEncoding,
) -> Result<Option<(i64, Option<Vec<f32>>)>> {
    let Some(obj) = row.as_object() else {
        return Ok(None);
    };
    let id = match (encoding, obj.get(id_field)) {
        (IdEncoding::Integer, Some(Value::Number(n))) => n.as_i64(),
        (IdEncoding::Integer, Some(Value::String(s))) => s.parse::<i64>().ok(),
        (IdEncoding::UuidHash, Some(Value::String(s))) => uuid_hash_id(s),
        _ => None,
    };
    let Some(id) = id else { return Ok(None) };

    let vector = match obj.get(vec_field) {
        None | Some(Value::Null) => None,
        Some(Value::Array(arr)) => {
            let mut v = Vec::with_capacity(arr.len());
            for x in arr {
                let f = x.as_f64().ok_or_else(|| {
                    VectorError::Field(format!("embedding '{vec_field}' has non-numeric element"))
                })?;
                v.push(f as f32);
            }
            Some(v)
        }
        Some(other) => {
            return Err(VectorError::Field(format!(
                "embedding '{vec_field}' must be an array or null, got {other}"
            )));
        }
    };
    Ok(Some((id, vector)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A uuid folds to a stable, repeatable `i64`; a non-uuid string yields none.
    #[test]
    fn uuid_hash_is_stable() {
        let a = uuid_hash_id("00000000-0000-0000-0000-000000000001");
        let b = uuid_hash_id("00000000-0000-0000-0000-000000000001");
        assert_eq!(a, b);
        assert!(a.is_some());
        assert_ne!(a, uuid_hash_id("00000000-0000-0000-0000-000000000002"));
        assert_eq!(uuid_hash_id("not-a-uuid"), None);
    }

    /// The id encoding token round-trips through the registry wire form.
    #[test]
    fn encoding_round_trips() {
        assert_eq!(
            IdEncoding::parse(IdEncoding::UuidHash.as_str()),
            IdEncoding::UuidHash
        );
        assert_eq!(
            IdEncoding::parse(IdEncoding::Integer.as_str()),
            IdEncoding::Integer
        );
        assert_eq!(IdEncoding::parse("bogus"), IdEncoding::Integer);
    }

    /// `parse_row` reads an integer id directly and a uuid id via the hash, and a
    /// uuid id under integer encoding is (correctly) unusable.
    #[test]
    fn parse_row_honors_encoding() {
        let int_row = json!({ "id": 7, "embedding": [0.1, 0.2] });
        let got = parse_row(&int_row, "id", "embedding", IdEncoding::Integer)
            .expect("parse int row")
            .expect("int row has an id");
        assert_eq!(got.0, 7);

        let uuid = "00000000-0000-0000-0000-000000000001";
        let expected = uuid_hash_id(uuid).expect("uuid folds");
        let uuid_row = json!({ "memory_id": uuid, "embedding": [0.1, 0.2] });
        let got = parse_row(&uuid_row, "memory_id", "embedding", IdEncoding::UuidHash)
            .expect("parse uuid row")
            .expect("uuid row has an id");
        assert_eq!(got.0, expected);

        // A uuid string under integer encoding does not parse to an i64 → skipped.
        assert!(
            parse_row(&uuid_row, "memory_id", "embedding", IdEncoding::Integer)
                .expect("parse under integer encoding")
                .is_none()
        );
    }
}
