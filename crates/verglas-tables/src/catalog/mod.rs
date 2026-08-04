//! Catalog watching (#47): the `CatalogWatcher` trait, its event and identity
//! types, and the polling machinery that turns catalog-pointer swings into
//! `TableChanged` events for downstream consumers (the mapper #49, the
//! prefetcher #51).
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

mod feed;
mod rest;
mod watcher;
mod websocket;

use std::fmt;

use async_trait::async_trait;
use tokio::sync::{broadcast, watch};

pub use rest::{CatalogGateway, CatalogResponse, RestCatalogSource};
pub use watcher::{CatalogFeed, PollingWatcher, WatcherOptions};
pub use websocket::WsFeedConfig;

/// Capacity of the broadcast ring. Commits are seconds-to-minutes apart per
/// table; a consumer that falls 1024 events behind is resynchronizing from
/// state anyway (see the module docs).
pub const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// A table's identity in the catalog: a (possibly nested) namespace plus a
/// table name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableIdent {
    /// Namespace levels, outermost first (e.g. `["db"]` or `["db", "sub"]`).
    pub namespace: Vec<String>,
    /// Table name within the namespace.
    pub name: String,
}

impl TableIdent {
    /// Builds an identity from namespace levels and a name.
    pub fn new(namespace: &[&str], name: &str) -> TableIdent {
        TableIdent {
            namespace: namespace.iter().map(|s| (*s).to_owned()).collect(),
            name: name.to_owned(),
        }
    }

    /// The dotted full name (`db.sub.table`) filters match against.
    pub fn dotted(&self) -> String {
        let mut out = self.namespace.join(".");
        if !out.is_empty() {
            out.push('.');
        }
        out.push_str(&self.name);
        out
    }
}

impl fmt::Display for TableIdent {
    /// Renders the dotted full name.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.dotted())
    }
}

/// A table's current catalog pointer: where the metadata.json lives and
/// which snapshot it names. This pair is everything a cheap poll compares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableState {
    /// Object-store location of the current `metadata.json`.
    pub metadata_location: String,
    /// The metadata's current snapshot id; `None` for a table with no
    /// snapshots yet (freshly created, never written).
    pub current_snapshot_id: Option<i64>,
}

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

/// Error talking to or decoding a catalog.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// The HTTP request itself failed (connect, timeout, TLS).
    #[error("catalog request failed: {0}")]
    Http(#[from] reqwest::Error),
    /// The catalog answered with a non-success status.
    #[error("catalog returned HTTP {status} for {url}")]
    Status {
        /// The HTTP status code returned.
        status: u16,
        /// The URL that produced it.
        url: String,
    },
    /// The catalog answered 200 but the body was not the expected shape.
    #[error("malformed catalog response from {url}: {detail}")]
    Malformed {
        /// The URL that produced the body.
        url: String,
        /// What was wrong with it.
        detail: String,
    },
    /// SigV4 signing could not resolve credentials or build the signature (#236).
    #[error("catalog request signing failed: {detail}")]
    Auth {
        /// What went wrong (credential resolution or signing).
        detail: String,
    },
}

/// A catalog protocol the polling loop can drive: enumerate tables and read
/// one table's current pointer. This is the trait the Glue watcher (#48)
/// implements; everything above it (filtering, diffing, lineage, backoff,
/// events) is shared via [`PollingWatcher`].
#[async_trait]
pub trait CatalogSource: Send + Sync + 'static {
    /// Enumerates every table visible in the catalog (unfiltered — the
    /// polling loop applies [`TableFilter`] before touching any table).
    async fn list_tables(&self) -> Result<Vec<TableIdent>, CatalogError>;

    /// Reads one table's current pointer. Must be cheap: pointer fields
    /// only, no manifest reads.
    async fn table_pointer(&self, table: &TableIdent) -> Result<TableState, CatalogError>;
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
    /// Builds a filter from pattern slices (test/daemon convenience).
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
