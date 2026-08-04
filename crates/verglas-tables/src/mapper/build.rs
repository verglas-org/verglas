//! Building and incrementally refreshing [`MapperState`].
//!
//! This is the single-writer side of the mapper. It walks a table's snapshots
//! through the fetch interface, builds a fresh [`TableIndex`] that shares unchanged
//! `Arc<FileMeta>` with the previous one (clone-on-write: maps are rebuilt,
//! per-file metadata is shared so parsed footers survive), assembles a new
//! [`MapperState`], and swaps it in atomically. Reads never see a partial
//! update.

use std::collections::BTreeMap;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use super::{
    FileId, FileMeta, FileSet, IdState, MapError, Mapper, MapperState, MetaKind, TableId,
    TableIndex,
};
use crate::catalog::TableIdent;
use crate::fetch::{MetadataFetch, ObjectPath};
use crate::iceberg::{self, SnapshotPlan};

/// The inputs the updater hands the builder to (re)build one table: its
/// identity, where it lives, which snapshots to retain, and which is current.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildInputs {
    /// Catalog identity of the table.
    pub ident: TableIdent,
    /// Origin bucket the table's objects live in.
    pub bucket: String,
    /// The current `metadata.json` object key.
    pub metadata_key: String,
    /// Snapshot ids to retain (ascending, ending at the current snapshot).
    /// Retaining recent lineage is what makes time-travel reads resolve.
    pub snapshot_ids: Vec<i64>,
    /// The current snapshot id (`None` for a table with no snapshots yet).
    pub current_snapshot_id: Option<i64>,
}

impl Mapper {
    /// (Re)builds one table from the catalog and swaps a new [`MapperState`]
    /// in. Single-writer: callers must serialize `apply_table`/`remove_table`
    /// (the updater task does). Off the hot path — it does IO and allocates.
    ///
    /// Shares unchanged files with the table's previous index, so a commit that
    /// touches a few manifests reuses every untouched `Arc<FileMeta>` (and its
    /// parsed footer).
    pub async fn apply_table(
        &self,
        fetch: &dyn MetadataFetch,
        inputs: &BuildInputs,
    ) -> Result<(), MapError> {
        // Parse metadata.json once for the table location and per-snapshot
        // manifest-list keys.
        let doc_bytes = read_whole(fetch, &inputs.bucket, &inputs.metadata_key).await?;
        let doc = iceberg::parse_metadata_json(&doc_bytes, &inputs.bucket, &inputs.metadata_key)?;

        // Walk each retained snapshot into a plan (parsing metadata.json only
        // the once, above).
        let mut plans: Vec<SnapshotPlan> = Vec::new();
        for &snapshot_id in &inputs.snapshot_ids {
            let Some(list_key) = doc
                .snapshots
                .iter()
                .find(|s| s.snapshot_id == snapshot_id)
                .map(|s| s.manifest_list_key.clone())
            else {
                // A snapshot the catalog lineage names but this metadata.json
                // does not — skip it rather than fail the whole rebuild.
                continue;
            };
            let (manifest_keys, data_files) =
                iceberg::walk_manifest_list(fetch, &inputs.bucket, &list_key).await?;
            plans.push(SnapshotPlan {
                snapshot_id,
                metadata_key: inputs.metadata_key.clone(),
                manifest_list_key: list_key,
                manifest_keys,
                data_files,
            });
        }

        let table_id = self.assign_table_id(&inputs.ident);
        let current = self.state.load_full();
        let prev = current.tables.get(&table_id).cloned();
        let index = self.build_index(
            table_id,
            inputs.ident.clone(),
            &inputs.bucket,
            &doc.table_location,
            &inputs.metadata_key,
            &plans,
            inputs.current_snapshot_id,
            prev.as_deref(),
        );
        let next = state_with_table(&current, Arc::new(index));
        self.state.store(Arc::new(next));
        Ok(())
    }

    /// Removes a table from the map (catalog dropped it or it left the filter),
    /// swapping a new state without it. Its id mapping is retained so a
    /// reappearance keeps the same id.
    pub fn remove_table(&self, ident: &TableIdent) {
        let Some(table_id) = self.table_id(ident) else {
            return;
        };
        let current = self.state.load_full();
        if !current.tables.contains_key(&table_id) {
            return;
        }
        let mut tables = current.tables.clone();
        tables.remove(&table_id);
        let next = state_from_tables(tables);
        self.state.store(Arc::new(next));
    }

    /// Parses the Parquet footer for `key` in `table` and installs the column
    /// chunks, so subsequent `classify()` range calls resolve to chunks.
    ///
    /// Guarded on format: a non-Parquet file returns `Ok(())` without ever
    /// touching the footer path (the addendum's rule — never discover format by
    /// failing to parse). Idempotent. Off the hot path.
    pub async fn warm_footer(
        &self,
        fetch: &dyn MetadataFetch,
        table: TableId,
        key: &str,
        etag: &str,
    ) -> Result<(), MapError> {
        let current = self.state.load_full();
        let Some(index) = current.tables.get(&table) else {
            return Ok(());
        };
        let Some(file) = index.by_path.get(key).cloned() else {
            return Ok(());
        };
        if !matches!(file.format, super::FileFormat::Parquet) || file.chunks().is_some() {
            return Ok(());
        }
        let path = ObjectPath {
            bucket: index.bucket.to_string(),
            key: key.to_owned(),
        };
        let spans = self.footer_cache.get_or_parse(fetch, &path, etag).await?;
        // Installing chunks mutates the shared Arc<FileMeta> in place (OnceLock):
        // it only *adds* resolution, never changes identity, so the live state
        // stays consistent without a swap.
        file.set_chunks(spans);
        Ok(())
    }

    /// Assigns (or reuses) the stable [`TableId`] for `ident`.
    fn assign_table_id(&self, ident: &TableIdent) -> TableId {
        let mut ids = self.ids.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(id) = ids.idents.get(ident) {
            return *id;
        }
        let id = TableId(ids.next_table_id);
        ids.next_table_id += 1;
        ids.idents.insert(ident.clone(), id);
        id
    }

    /// Builds a fresh [`TableIndex`], sharing unchanged `Arc<FileMeta>` with
    /// `prev` and interning paths. Allocates file ids under the writer lock.
    #[allow(clippy::too_many_arguments)]
    fn build_index(
        &self,
        table_id: TableId,
        ident: TableIdent,
        bucket: &str,
        location: &str,
        metadata_key: &str,
        plans: &[SnapshotPlan],
        current_snapshot_id: Option<i64>,
        prev: Option<&TableIndex>,
    ) -> TableIndex {
        let root = location.trim_end_matches('/');
        let prefix: Arc<str> = Arc::from(format!("{root}/"));
        let metadata_prefix: Arc<str> = Arc::from(format!("{root}/metadata/"));

        let mut by_path: FxHashMap<Arc<str>, Arc<FileMeta>> = FxHashMap::default();
        let mut meta_paths: FxHashMap<Arc<str>, MetaKind> = FxHashMap::default();
        let mut snapshots: BTreeMap<i64, Arc<FileSet>> = BTreeMap::new();

        // The current metadata.json is always a catalogued metadata object,
        // even for a table with no snapshots.
        meta_paths.insert(Arc::from(metadata_key), MetaKind::MetadataJson);

        let mut ids = self.ids.lock().unwrap_or_else(|p| p.into_inner());
        for plan in plans {
            meta_paths.insert(
                Arc::from(plan.manifest_list_key.as_str()),
                MetaKind::ManifestList,
            );
            for manifest_key in &plan.manifest_keys {
                meta_paths.insert(Arc::from(manifest_key.as_str()), MetaKind::Manifest);
            }

            let mut files: Vec<Arc<FileMeta>> = Vec::with_capacity(plan.data_files.len());
            for df in &plan.data_files {
                let file = reuse_or_new(&mut by_path, &mut ids, prev, df);
                files.push(file);
            }
            snapshots.insert(
                plan.snapshot_id,
                Arc::new(FileSet {
                    snapshot_id: plan.snapshot_id,
                    files,
                }),
            );
        }

        TableIndex {
            table: table_id,
            ident: Arc::new(ident),
            bucket: Arc::from(bucket),
            prefix,
            metadata_prefix,
            by_path,
            meta_paths,
            snapshots,
            current_snapshot_id,
        }
    }
}

/// Reuses an existing `Arc<FileMeta>` for `df` — from this build's map (a file
/// shared across retained snapshots) or from `prev` (a file unchanged across
/// the commit) — or mints a new one with a fresh id and interned path. Reusing
/// `prev` preserves the parsed footer.
fn reuse_or_new(
    by_path: &mut FxHashMap<Arc<str>, Arc<FileMeta>>,
    ids: &mut IdState,
    prev: Option<&TableIndex>,
    df: &iceberg::DataFileEntry,
) -> Arc<FileMeta> {
    if let Some(existing) = by_path.get(df.path.as_str()).cloned() {
        return existing;
    }
    // Reuse across the commit only when identity matches (same size + format);
    // a rewritten file at the same path is a new identity.
    let reused = prev
        .and_then(|p| p.by_path.get(df.path.as_str()))
        .filter(|f| f.size == df.size && f.format == df.format)
        .cloned();
    let file = match reused {
        Some(existing) => existing,
        None => {
            let id = FileId(ids.next_file_id);
            ids.next_file_id += 1;
            let path: Arc<str> = Arc::from(df.path.as_str());
            let partition: Arc<[(String, Option<String>)]> = Arc::from(df.partition.clone());
            Arc::new(FileMeta::new(id, path, df.size, df.format, partition))
        }
    };
    by_path.insert(Arc::clone(&file.path), Arc::clone(&file));
    file
}

/// Reads a whole small metadata object through the fetch interface (growing suffix).
async fn read_whole(
    fetch: &dyn MetadataFetch,
    bucket: &str,
    key: &str,
) -> Result<bytes::Bytes, MapError> {
    // Reuse the iceberg reader's whole-object strategy via a suffix read; a
    // metadata.json is small, so one suffix window covers it.
    let path = ObjectPath {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    };
    // 8 MiB comfortably covers any metadata.json; larger docs are pathological.
    fetch
        .fetch_suffix(&path, 8 * 1024 * 1024)
        .await
        .map_err(MapError::Fetch)
}

/// Builds a new state that is `current` with `index` inserted/replaced.
fn state_with_table(current: &MapperState, index: Arc<TableIndex>) -> MapperState {
    let mut tables = current.tables.clone();
    tables.insert(index.table, index);
    state_from_tables(tables)
}

/// Builds a state from a table map, recomputing the sorted prefix vector.
fn state_from_tables(tables: FxHashMap<TableId, Arc<TableIndex>>) -> MapperState {
    let mut table_prefixes: Vec<(Arc<str>, TableId)> = tables
        .values()
        .map(|t| (Arc::clone(&t.prefix), t.table))
        .collect();
    table_prefixes.sort_by(|a, b| a.0.cmp(&b.0));
    MapperState {
        table_prefixes,
        tables,
    }
}
