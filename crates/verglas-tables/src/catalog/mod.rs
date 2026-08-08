//! Catalog watching (#47): the `CatalogWatcher` trait, its event and identity
//! types, and the polling machinery that turns catalog-pointer swings into
//! `TableChanged` events for downstream consumers (the mapper #49, the
//! prefetcher #51).
//!
//! Change discovery is Iceberg REST polling only. Push notification from a
//! hosted catalog (for example Lakekeeper) is a separate cloud integration; this
//! crate does not open a Verglas-owned websocket to the catalog origin.
//!
//! # Event-channel semantics
//!
//! Events fan out over a [`tokio::sync::broadcast`] channel so several
//! independent consumers can subscribe. The channel is a bounded ring
//! ([`EVENT_CHANNEL_CAPACITY`]): a subscriber that falls more than the
//! capacity behind observes `RecvError::Lagged(n)` and has *lost those n
//! events*. That is deliberate — events are change *notifications*, not the
//! source of truth. The watcher always retains the last-known state, so a
//! lagged (or freshly attached) consumer resynchronizes by re-reading
//! [`CatalogWatcher::table_state`] / [`CatalogWatcher::lineage`] and then
//! continues consuming events. Nothing downstream may rely on seeing every
//! event exactly once.

mod watcher;

use tokio::sync::{broadcast, watch};

pub use verglas_catalog::{
    CatalogError, CatalogGateway, CatalogResponse, CatalogSource, RestCatalogSource, TableIdent,
    TableState,
};
pub use watcher::{PollingWatcher, WatcherOptions};

/// Capacity of the broadcast ring. Commits are seconds-to-minutes apart per
/// table; a consumer that falls 1024 events behind is resynchronizing from
/// state anyway (see the module docs).
pub const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// One observed step in a table's snapshot lineage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEntry {
    /// The snapshot id current at this step.
    pub snapshot_id: i64,
    /// The metadata.json location that named it.
    pub metadata_location: String,
    /// When the watcher observed this pointer (observation time, not commit
    /// time — polling cannot know the latter without reading metadata).
    pub observed_at: std::time::SystemTime,
}

/// A catalog-pointer swing on one table.
///
/// `old_snapshot`/`new_snapshot` are `None` at the endpoints of a table's
/// life: `(None, Some)` is a table appearing after watch start, `(Some,
/// None)` is a table dropped from the catalog. The initial seeding poll
/// emits nothing — consumers that enumerate the pre-existing table set at
/// startup await [`CatalogWatcher::seeded`] first, then read the seeded
/// state via [`CatalogWatcher::table_state`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableChanged {
    /// Which table changed.
    pub table: TableIdent,
    /// Snapshot id before the change (`None`: table just appeared).
    pub old_snapshot: Option<i64>,
    /// Snapshot id after the change (`None`: table was dropped).
    pub new_snapshot: Option<i64>,
}

/// The interface downstream consumers hold: last-known catalog state plus a
/// subscription to change events.
///
/// All state reads are served from the watcher's memory of the last
/// successful poll — never a live catalog call — so consumers keep working
/// through a catalog outage (resilience invariant of #47).
pub trait CatalogWatcher: Send + Sync {
    /// Tables currently watched (present in the catalog and passing the
    /// filters), sorted by dotted name.
    fn watched_tables(&self) -> Vec<TableIdent>;

    /// Last-known pointer state for one table; `None` if unwatched/unknown.
    fn table_state(&self, table: &TableIdent) -> Option<TableState>;

    /// Recent snapshot lineage for one table, oldest → newest, ending at the
    /// current snapshot. Depth is bounded by [`WatcherOptions::history_depth`].
    fn lineage(&self, table: &TableIdent) -> Vec<SnapshotEntry>;

    /// Subscribes to change events (see the module docs for lag semantics).
    fn subscribe(&self) -> broadcast::Receiver<TableChanged>;

    /// A receiver that reads `true` once the watcher's first successful poll
    /// has seeded the watched-table set. Before that point,
    /// [`watched_tables`](CatalogWatcher::watched_tables) is empty because the
    /// catalog *has not been read yet* — not because it is empty — so any
    /// consumer that enumerates pre-existing tables at startup (the warming
    /// coordinator, #168) must await this signal before its first enumeration.
    fn seeded(&self) -> watch::Receiver<bool>;
}

/// Include/exclude patterns over dotted table names. `*` matches any run of
/// characters (including dots). Empty `include` means every table; `exclude`
/// always wins over `include`.
#[derive(Debug, Clone, Default)]
pub struct TableFilter {
    /// Patterns a table must match one of (empty = all tables).
    include: Vec<String>,
    /// Patterns that reject a table even when included.
    exclude: Vec<String>,
}

impl TableFilter {
    /// Builds a filter from pattern slices (test/server convenience).
    pub fn new(include: &[&str], exclude: &[&str]) -> TableFilter {
        TableFilter {
            include: include.iter().map(|s| (*s).to_owned()).collect(),
            exclude: exclude.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    /// Builds a filter from owned pattern lists (config path).
    pub fn from_patterns(include: Vec<String>, exclude: Vec<String>) -> TableFilter {
        TableFilter { include, exclude }
    }

    /// Whether the table passes the filter: it must match some `include`
    /// pattern (or `include` is empty) and no `exclude` pattern. Uses the
    /// shared glob matcher (`verglas_core::glob`) so a table pattern and a
    /// bucket pattern (#235) agree on what `*` means.
    pub fn matches(&self, table: &TableIdent) -> bool {
        let name = table.dotted();
        let included = self.include.is_empty()
            || self
                .include
                .iter()
                .any(|p| verglas_core::glob::matches(p, &name));
        included
            && !self
                .exclude
                .iter()
                .any(|p| verglas_core::glob::matches(p, &name))
    }
}
