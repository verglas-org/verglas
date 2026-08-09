//! The single-writer map updater: turns catalog [`TableChanged`] events into
//! incremental [`Mapper`] rebuilds.
//!
//! One background task owns all writes to the map. It subscribes to the catalog
//! watcher (#47), and on each commit reads the changed table's current pointer
//! and lineage from the watcher, rebuilds that one table (sharing unchanged
//! files with the prior index), and swaps. Because there is exactly one writer,
//! the load-build-store sequence needs no coordination beyond the atomic swap;
//! concurrent readers see the old or the new state, never a mix.

use std::sync::Arc;

use tokio::sync::broadcast::error::RecvError;

use super::{BuildInputs, Mapper};
use crate::catalog::{CatalogWatcher, TableChanged, TableIdent};
use crate::fetch::MetadataFetch;
use crate::iceberg;

/// Drives map rebuilds off catalog events. Generic over the watcher and fetch
/// implementations so the server (REST watcher + through-cache fetch) and tests
/// (mock watcher + object_store fetch) share it.
pub struct MapUpdater<W: CatalogWatcher, F: MetadataFetch> {
    /// Immutable storage binding for the watched database.
    storage_binding_id: Arc<str>,
    /// The map to keep current.
    mapper: Arc<Mapper>,
    /// Source of last-known table pointers and lineage.
    watcher: Arc<W>,
    /// How metadata is read (through the cache in the server).
    fetch: Arc<F>,
}

impl<W: CatalogWatcher + 'static, F: MetadataFetch + 'static> MapUpdater<W, F> {
    /// Builds an updater over a mapper, watcher, and fetch interface.
    pub fn new(
        storage_binding_id: impl Into<Arc<str>>,
        mapper: Arc<Mapper>,
        watcher: Arc<W>,
        fetch: Arc<F>,
    ) -> MapUpdater<W, F> {
        MapUpdater {
            storage_binding_id: storage_binding_id.into(),
            mapper,
            watcher,
            fetch,
        }
    }

    /// Spawns the updater loop and returns its task handle (aborted on drop by
    /// the caller if it wraps the handle). Seeds the map from the watcher's
    /// current state first, then consumes events.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut events = self.watcher.subscribe();
            // Seed from whatever the watcher already knows (it may have polled
            // before we subscribed).
            self.resync_all().await;
            loop {
                match events.recv().await {
                    Ok(change) => self.apply_change(&change).await,
                    // Lagged past the ring: the watcher's state is the truth,
                    // so resynchronize every table rather than chase events.
                    Err(RecvError::Lagged(_)) => self.resync_all().await,
                    Err(RecvError::Closed) => break,
                }
            }
        })
    }

    /// Applies one catalog change: drop the table if it disappeared, otherwise
    /// rebuild it from the watcher's current pointer and lineage.
    pub async fn apply_change(&self, change: &TableChanged) {
        if change.new_snapshot.is_none() {
            self.mapper.remove_table(&change.table);
            return;
        }
        self.rebuild(&change.table).await;
    }

    /// Rebuilds every currently watched table. Used to seed and to recover from
    /// a lagged event ring.
    pub async fn resync_all(&self) {
        for table in self.watcher.watched_tables() {
            self.rebuild(&table).await;
        }
    }

    /// Rebuilds one table from the watcher's state, logging (not propagating)
    /// build failures — a failed rebuild leaves the last-good index in place.
    async fn rebuild(&self, table: &TableIdent) {
        let Some(inputs) = self.build_inputs(table) else {
            tracing::warn!(table = %table, "no resolvable pointer for table; skipping rebuild");
            return;
        };
        if let Err(error) = self.mapper.apply_table(self.fetch.as_ref(), &inputs).await {
            tracing::warn!(table = %table, %error, "table map rebuild failed; keeping last-good index");
        }
    }

    /// Assembles [`BuildInputs`] for a table from the watcher's last-known
    /// pointer and lineage. `None` if the table is unknown or its metadata
    /// location is not an absolute object URI (no bucket to resolve).
    fn build_inputs(&self, table: &TableIdent) -> Option<BuildInputs> {
        let state = self.watcher.table_state(table)?;
        let (bucket, metadata_key) = iceberg::parse_object_uri(&state.metadata_location)?;
        let current_snapshot_id = state.current_snapshot_id;

        // Retain recent lineage plus the current snapshot, deduped ascending.
        let mut snapshot_ids: Vec<i64> = self
            .watcher
            .lineage(table)
            .iter()
            .map(|entry| entry.snapshot_id)
            .collect();
        if let Some(current) = current_snapshot_id {
            snapshot_ids.push(current);
        }
        snapshot_ids.sort_unstable();
        snapshot_ids.dedup();

        Some(BuildInputs {
            ident: table.clone(),
            storage_binding_id: self.storage_binding_id.to_string(),
            bucket,
            metadata_key,
            snapshot_ids,
            current_snapshot_id,
        })
    }
}
