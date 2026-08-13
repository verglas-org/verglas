//! Snapshot commit classification and changed-manifests-only diffing.
//!
//! Iceberg records the *kind* of every commit in the snapshot's
//! `summary.operation` field (spec §Snapshots). [`SnapshotOp::classify`] reads
//! it; `replace` is compaction — data logically unchanged, files rewritten —
//! which is the heat-carry case #51 exists for. The file-set delta comes from
//! manifest-entry `status` fields (ADDED / DELETED) in the *changed manifests
//! only* — the manifests a commit rewrote, found by their `added_snapshot_id`
//! — never by diffing whole file listings.

use crate::fetch::{MetadataFetch, ObjectPath};
use crate::iceberg::{self, DataFileEntry, EntryStatus, MetadataDoc, SnapshotRef};
use crate::mapper::MapError;

/// The shape of a commit, from the snapshot's `summary.operation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotOp {
    /// New data appended; history is untouched. New data is cold by definition,
    /// so no data prefetch — only the added files' (cheap) footers.
    Append,
    /// Files rewritten, data logically unchanged: **compaction**. The heat-carry
    /// case — prefetch the replacement files' hot chunks before queries ask.
    Replace,
    /// Rows overwritten: logical content changed, but carry heat and expect real
    /// shifts.
    Overwrite,
    /// Rows deleted: logical content changed; carry heat, expect shifts.
    Delete,
    /// An operation the classifier does not recognize (or none recorded). Treated
    /// conservatively — footers only, like an append.
    Other,
}

impl SnapshotOp {
    /// Classifies a snapshot's `summary.operation` string (case-insensitively).
    /// An unrecognized or absent operation maps to [`SnapshotOp::Other`].
    pub fn classify(operation: Option<&str>) -> SnapshotOp {
        match operation.map(|o| o.to_ascii_lowercase()).as_deref() {
            Some("append") => SnapshotOp::Append,
            Some("replace") => SnapshotOp::Replace,
            Some("overwrite") => SnapshotOp::Overwrite,
            Some("delete") => SnapshotOp::Delete,
            _ => SnapshotOp::Other,
        }
    }

    /// Whether this commit should trigger *data* prefetch of the added files'
    /// hot chunks. True for the content-preserving-or-shifting operations
    /// (`replace`/`overwrite`/`delete`) whose partitions carry heat across the
    /// rewrite; false for `append` (new data is cold) and `other`.
    pub fn prefetches_data(self) -> bool {
        matches!(
            self,
            SnapshotOp::Replace | SnapshotOp::Overwrite | SnapshotOp::Delete
        )
    }

    /// Whether this commit is pure compaction (`replace`): data logically
    /// unchanged, so heat carries cleanly by `(partition, column)` identity.
    pub fn is_compaction(self) -> bool {
        matches!(self, SnapshotOp::Replace)
    }
}

/// The result of diffing one commit: its operation and the files it added and
/// removed, read from the changed manifests only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotDiff {
    /// The snapshot this diff resolves.
    pub snapshot_id: i64,
    /// The commit's classified operation.
    pub op: SnapshotOp,
    /// Files this commit added (manifest status ADDED). The prefetch targets.
    pub added: Vec<DataFileEntry>,
    /// Files this commit removed (manifest status DELETED). The retirement
    /// targets.
    pub removed: Vec<DataFileEntry>,
}

impl SnapshotDiff {
    /// A diff carrying no changes for `snapshot_id` and the given op. Used when a
    /// commit rewrote no manifests we can attribute (e.g. the first snapshot).
    fn empty(snapshot_id: i64, op: SnapshotOp) -> SnapshotDiff {
        SnapshotDiff {
            snapshot_id,
            op,
            added: Vec::new(),
            removed: Vec::new(),
        }
    }
}

/// Diffs the commit that produced `snapshot_id` against its predecessor by
/// reading only the manifests that commit rewrote.
///
/// Reads `metadata.json` for the snapshot's operation and manifest-list key,
/// then reads the manifest list and selects the manifests whose
/// `added_snapshot_id` equals `snapshot_id` — the ones this commit wrote — and
/// reads only those, bucketing their entries by ADDED / DELETED. Never lists or
/// diffs the full file set. Off the request hot path (does IO).
pub async fn diff_snapshot(
    fetch: &dyn MetadataFetch,
    storage_binding_id: &str,
    bucket: &str,
    metadata_key: &str,
    snapshot_id: i64,
) -> Result<SnapshotDiff, MapError> {
    let doc = read_metadata(fetch, storage_binding_id, bucket, metadata_key).await?;
    let snap = doc
        .snapshots
        .iter()
        .find(|s| s.snapshot_id == snapshot_id)
        .ok_or_else(|| MapError::Malformed {
            object: metadata_key.to_owned(),
            detail: format!("metadata.json names no snapshot {snapshot_id}"),
        })?;
    diff_from_snapshot_ref(fetch, storage_binding_id, bucket, snap).await
}

/// Diffs a specific [`SnapshotRef`] already resolved from a `metadata.json`.
/// The core the server calls when it has parsed the doc once and holds the ref.
pub async fn diff_from_snapshot_ref(
    fetch: &dyn MetadataFetch,
    storage_binding_id: &str,
    bucket: &str,
    snap: &SnapshotRef,
) -> Result<SnapshotDiff, MapError> {
    let op = SnapshotOp::classify(snap.operation.as_deref());
    let list_bytes =
        read_object(fetch, storage_binding_id, bucket, &snap.manifest_list_key).await?;
    let refs = iceberg::parse_manifest_list_refs(&list_bytes, bucket, &snap.manifest_list_key)?;

    // Only the manifests this commit wrote carry its ADDED/DELETED entries. A
    // manifest whose list entry records no `added_snapshot_id` is ambiguous, so
    // we skip it rather than re-read every manifest — the delta lives in the
    // ones the commit stamped.
    let mut added = Vec::new();
    let mut removed = Vec::new();
    for r in &refs {
        if r.added_snapshot_id != Some(snap.snapshot_id) {
            continue;
        }
        let bytes = read_object(fetch, storage_binding_id, bucket, &r.manifest_key).await?;
        let mut entries = Vec::new();
        iceberg::parse_manifest_entries(&bytes, bucket, &r.manifest_key, &mut entries)?;
        for entry in entries {
            match entry.status {
                EntryStatus::Added => added.push(entry.file),
                EntryStatus::Deleted => removed.push(entry.file),
                EntryStatus::Existing => {}
            }
        }
    }

    if added.is_empty() && removed.is_empty() {
        return Ok(SnapshotDiff::empty(snap.snapshot_id, op));
    }
    Ok(SnapshotDiff {
        snapshot_id: snap.snapshot_id,
        op,
        added,
        removed,
    })
}

/// Reads and parses the table's `metadata.json` through the fetch interface.
pub(crate) async fn read_metadata(
    fetch: &dyn MetadataFetch,
    storage_binding_id: &str,
    bucket: &str,
    metadata_key: &str,
) -> Result<MetadataDoc, MapError> {
    let bytes = read_object(fetch, storage_binding_id, bucket, metadata_key).await?;
    iceberg::parse_metadata_json(&bytes, bucket, metadata_key)
}

/// Reads a whole (small) metadata object through the fetch interface with a
/// generous suffix window — manifests and manifest lists comfortably fit.
pub(crate) async fn read_object(
    fetch: &dyn MetadataFetch,
    storage_binding_id: &str,
    bucket: &str,
    key: &str,
) -> Result<bytes::Bytes, MapError> {
    let path = ObjectPath {
        storage_binding_id: storage_binding_id.to_owned(),
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    };
    // 16 MiB covers any manifest / manifest list; larger is pathological.
    fetch
        .fetch_suffix(&path, 16 * 1024 * 1024)
        .await
        .map_err(MapError::Fetch)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The operation classifier is case-insensitive and maps the four spec
    /// operations, defaulting unknown/absent to `Other`.
    #[test]
    fn classifies_the_four_spec_operations() {
        assert_eq!(SnapshotOp::classify(Some("append")), SnapshotOp::Append);
        assert_eq!(SnapshotOp::classify(Some("REPLACE")), SnapshotOp::Replace);
        assert_eq!(
            SnapshotOp::classify(Some("Overwrite")),
            SnapshotOp::Overwrite
        );
        assert_eq!(SnapshotOp::classify(Some("delete")), SnapshotOp::Delete);
        assert_eq!(SnapshotOp::classify(Some("cherrypick")), SnapshotOp::Other);
        assert_eq!(SnapshotOp::classify(None), SnapshotOp::Other);
    }

    /// Only `replace`/`overwrite`/`delete` prefetch data; `append` does not, and
    /// only `replace` is pure compaction.
    #[test]
    fn prefetch_and_compaction_predicates() {
        assert!(!SnapshotOp::Append.prefetches_data());
        assert!(SnapshotOp::Replace.prefetches_data());
        assert!(SnapshotOp::Overwrite.prefetches_data());
        assert!(SnapshotOp::Delete.prefetches_data());
        assert!(!SnapshotOp::Other.prefetches_data());

        assert!(SnapshotOp::Replace.is_compaction());
        assert!(!SnapshotOp::Overwrite.is_compaction());
    }
}
