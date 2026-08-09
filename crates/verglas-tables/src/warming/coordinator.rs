//! Watcher-driven warming (#168): bind the eager [`Warmer`] to a
//! [`CatalogWatcher`] so watched tables are warmed on onboarding and re-warmed
//! whenever a commit swings the catalog pointer.
//!
//! On a commit the watcher fires a [`TableChanged`]; the coordinator resolves
//! the table's *new* pointer (metadata.json + current snapshot) and warms that
//! snapshot's manifests and footers, so the refresh tracks the moved pointer
//! within one watch interval. A dropped table (new snapshot `None`) is left
//! alone — its pinned bytes age out naturally.

use std::sync::Arc;

use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

use crate::catalog::{CatalogWatcher, TableChanged, TableIdent};
use crate::iceberg::parse_object_uri;

use super::{WarmTarget, Warmer};

/// Drives a [`Warmer`] from a [`CatalogWatcher`]'s events.
pub struct WarmingCoordinator<W> {
    /// Immutable storage binding for the watched database.
    storage_binding_id: Arc<str>,
    /// The warming job (shared so the server can read its progress).
    warmer: Arc<Warmer>,
    /// The catalog watcher whose pointer swings trigger refreshes.
    watcher: Arc<W>,
}

impl<W: CatalogWatcher + 'static> WarmingCoordinator<W> {
    /// Binds `warmer` to `watcher`.
    pub fn new(
        storage_binding_id: impl Into<Arc<str>>,
        warmer: Arc<Warmer>,
        watcher: Arc<W>,
    ) -> WarmingCoordinator<W> {
        WarmingCoordinator {
            storage_binding_id: storage_binding_id.into(),
            warmer,
            watcher,
        }
    }

    /// The shared warmer (for progress reads and testing).
    pub fn warmer(&self) -> &Arc<Warmer> {
        &self.warmer
    }

    /// Warms every currently-watched table once (onboarding / resync).
    pub async fn warm_all(&self) {
        warm_all(
            Arc::clone(&self.storage_binding_id),
            Arc::clone(&self.warmer),
            Arc::clone(&self.watcher),
        )
        .await;
    }

    /// Handles one catalog change: re-warm the table's new pointer. A dropped
    /// table (`new_snapshot` is `None`) is skipped.
    pub async fn on_change(&self, change: &TableChanged) {
        if change.new_snapshot.is_none() {
            return;
        }
        warm_table(
            Arc::clone(&self.storage_binding_id),
            Arc::clone(&self.warmer),
            Arc::clone(&self.watcher),
            change.table.clone(),
        )
        .await;
    }

    /// Spawns the background loop: wait for the watcher's first successful
    /// poll to seed the watched set, warm every watched table once (the
    /// startup pass that onboards pre-existing tables, #168), then re-warm on
    /// each catalog change. A lagged subscription triggers a full resync
    /// (the watcher retains last-known state; see the catalog module docs).
    pub fn spawn(self) -> JoinHandle<()> {
        let warmer = self.warmer;
        let watcher = self.watcher;
        let storage_binding_id = self.storage_binding_id;
        let mut events = watcher.subscribe();
        let mut seeded = watcher.seeded();
        tokio::spawn(async move {
            // Before the first successful poll the watched set is empty
            // because it has not been read yet — an immediate warm_all would
            // silently skip every pre-existing table (they would wait for
            // their next commit). Events arriving during the wait sit in the
            // already-open subscription and are drained after the startup
            // pass; warming an already-warm snapshot is all cache hits, so a
            // double warm costs no backend traffic.
            while !*seeded.borrow() {
                if seeded.changed().await.is_err() {
                    // Watcher dropped before ever seeding: nothing to warm,
                    // and the event loop below ends on Closed.
                    break;
                }
            }
            warm_all(
                Arc::clone(&storage_binding_id),
                Arc::clone(&warmer),
                Arc::clone(&watcher),
            )
            .await;
            loop {
                match events.recv().await {
                    Ok(change) if change.new_snapshot.is_some() => {
                        warm_table(
                            Arc::clone(&storage_binding_id),
                            Arc::clone(&warmer),
                            Arc::clone(&watcher),
                            change.table,
                        )
                        .await;
                    }
                    Ok(_) => {}
                    Err(RecvError::Lagged(_)) => {
                        warm_all(
                            Arc::clone(&storage_binding_id),
                            Arc::clone(&warmer),
                            Arc::clone(&watcher),
                        )
                        .await;
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        })
    }
}

/// Resolves a table's current catalog pointer into a warm target, or `None`
/// when the table has no snapshot yet or an unparseable location.
fn target_for<W: CatalogWatcher>(
    storage_binding_id: &str,
    watcher: &W,
    table: &TableIdent,
) -> Option<WarmTarget> {
    let state = watcher.table_state(table)?;
    let snapshot_id = state.current_snapshot_id?;
    let (bucket, metadata_key) = parse_object_uri(&state.metadata_location)?;
    Some(WarmTarget {
        storage_binding_id: storage_binding_id.to_owned(),
        bucket,
        metadata_key,
        snapshot_id,
    })
}

/// Warms every watched table once. Owns its `Arc`s so the future is `Send`.
async fn warm_all<W: CatalogWatcher>(
    storage_binding_id: Arc<str>,
    warmer: Arc<Warmer>,
    watcher: Arc<W>,
) {
    for table in watcher.watched_tables() {
        warm_table(
            Arc::clone(&storage_binding_id),
            Arc::clone(&warmer),
            Arc::clone(&watcher),
            table,
        )
        .await;
    }
}

/// Warms one table's current pointer if it resolves. Errors are logged and
/// skipped — warming is best-effort background work.
async fn warm_table<W: CatalogWatcher>(
    storage_binding_id: Arc<str>,
    warmer: Arc<Warmer>,
    watcher: Arc<W>,
    table: TableIdent,
) {
    let Some(target) = target_for(&storage_binding_id, watcher.as_ref(), &table) else {
        return;
    };
    if let Err(error) = warmer.warm_table_owned(target).await {
        tracing::warn!(%table, %error, "table warm failed; will retry on next commit");
    }
}
