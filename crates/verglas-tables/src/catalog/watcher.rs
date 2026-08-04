//! The generic polling loop: drives any [`CatalogSource`] on an interval,
//! diffs pointers against last-known state, tracks lineage, and broadcasts
//! [`TableChanged`] events. Shared by the REST watcher (#47) and the future
//! Glue watcher (#48), which only needs to implement [`CatalogSource`].

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{broadcast, watch};
use verglas_core::config;

use super::websocket::WsFeedConfig;
use super::{
    CatalogSource, CatalogWatcher, EVENT_CHANNEL_CAPACITY, SnapshotEntry, TableChanged,
    TableFilter, TableIdent, TableState,
};

/// Lineage entries retained per table by default: enough for downstream
/// consumers to diff across a burst of commits between their reads.
const DEFAULT_HISTORY_DEPTH: usize = 16;

/// Tuning knobs for the polling loop.
#[derive(Debug, Clone)]
pub struct WatcherOptions {
    /// Time between successful poll cycles.
    pub interval: Duration,
    /// Maximum random delay added to each cycle (de-synchronizes a fleet of
    /// servers polling the same catalog).
    pub jitter: Duration,
    /// Ceiling for the exponential backoff while the catalog is unreachable.
    pub max_backoff: Duration,
    /// How many lineage entries to retain per table.
    pub history_depth: usize,
    /// Which tables to watch.
    pub filter: TableFilter,
}

impl Default for WatcherOptions {
    /// The documented defaults: 30s interval, 10% jitter, 5min backoff cap,
    /// 16 lineage entries, all tables.
    fn default() -> WatcherOptions {
        WatcherOptions {
            interval: Duration::from_secs(30),
            jitter: Duration::from_secs(3),
            max_backoff: Duration::from_secs(300),
            history_depth: DEFAULT_HISTORY_DEPTH,
            filter: TableFilter::default(),
        }
    }
}

impl WatcherOptions {
    /// Maps the server's `[catalog]` config section onto options: the
    /// configured interval, 10% of it as jitter, and the config's filters.
    pub fn from_config(config: &config::Catalog) -> WatcherOptions {
        let interval = Duration::from_secs(config.poll_interval_secs);
        WatcherOptions {
            interval,
            jitter: interval / 10,
            // The backoff cap never sits below the interval itself.
            max_backoff: Duration::from_secs(300).max(interval),
            history_depth: DEFAULT_HISTORY_DEPTH,
            filter: TableFilter::from_patterns(config.include.clone(), config.exclude.clone()),
        }
    }
}

/// Per-table record the loop maintains: current pointer plus recent lineage.
#[derive(Debug, Clone)]
struct TableRecord {
    /// Last-known pointer.
    state: TableState,
    /// Recent lineage, oldest first, bounded by `history_depth`.
    lineage: Vec<SnapshotEntry>,
}

impl TableRecord {
    /// Appends a lineage step for `state` (when it names a snapshot) and
    /// trims the front to `depth`.
    fn push_lineage(&mut self, state: &TableState, depth: usize) {
        if let Some(snapshot_id) = state.current_snapshot_id {
            self.lineage.push(SnapshotEntry {
                snapshot_id,
                metadata_location: state.metadata_location.clone(),
                observed_at: SystemTime::now(),
            });
            if self.lineage.len() > depth {
                let excess = self.lineage.len() - depth;
                self.lineage.drain(..excess);
            }
        }
    }
}

/// State shared between a feed task (polling or websocket) and the
/// [`CatalogWatcher`] surface. Both transports write last-known state here and
/// fan out [`TableChanged`] events; consumers read the same memory regardless
/// of which transport produced it.
pub(crate) struct Shared {
    /// Last-known state per watched table.
    tables: Mutex<HashMap<TableIdent, TableRecord>>,
    /// Event fan-out (see the module docs in `catalog` for lag semantics).
    events: broadcast::Sender<TableChanged>,
    /// Flips to `true` once the first successful poll has seeded `tables` —
    /// the signal startup consumers (`CatalogWatcher::seeded`) wait on before
    /// enumerating pre-existing tables (#168).
    seeded: watch::Sender<bool>,
}

impl Shared {
    /// Creates empty shared state with a fresh event ring and an un-seeded
    /// signal.
    pub(crate) fn new() -> Shared {
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let (seeded, _) = watch::channel(false);
        Shared {
            tables: Mutex::new(HashMap::new()),
            events,
            seeded,
        }
    }

    /// Locks the table map, tolerating poisoning — a panicked cycle must not
    /// take last-known state down with it.
    fn tables(&self) -> MutexGuard<'_, HashMap<TableIdent, TableRecord>> {
        self.tables
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Marks state seeded, waking startup consumers. Idempotent.
    pub(crate) fn mark_seeded(&self) {
        self.seeded.send_replace(true);
    }

    /// Watched tables from last-known state, sorted by identity.
    fn watched_tables(&self) -> Vec<TableIdent> {
        let mut tables: Vec<TableIdent> = self.tables().keys().cloned().collect();
        tables.sort();
        tables
    }

    /// Last-known pointer for one table.
    fn table_state(&self, table: &TableIdent) -> Option<TableState> {
        self.tables().get(table).map(|record| record.state.clone())
    }

    /// Recent lineage for one table (empty if unknown).
    fn lineage(&self, table: &TableIdent) -> Vec<SnapshotEntry> {
        self.tables()
            .get(table)
            .map(|record| record.lineage.clone())
            .unwrap_or_default()
    }

    /// A fresh event subscription.
    fn subscribe(&self) -> broadcast::Receiver<TableChanged> {
        self.events.subscribe()
    }

    /// A receiver on the seeded signal.
    fn seeded_rx(&self) -> watch::Receiver<bool> {
        self.seeded.subscribe()
    }
}

/// A [`CatalogWatcher`] driven by polling a [`CatalogSource`].
pub struct PollingWatcher {
    /// State shared with the background task.
    shared: Arc<Shared>,
    /// The background polling task; aborted on drop.
    task: tokio::task::JoinHandle<()>,
}

impl PollingWatcher {
    /// Starts watching: spawns the polling task and returns the handle
    /// consumers query and subscribe through.
    pub fn spawn<S: CatalogSource>(source: S, options: WatcherOptions) -> PollingWatcher {
        let shared = Arc::new(Shared::new());
        let task = tokio::spawn(run(source, options, Arc::clone(&shared)));
        PollingWatcher { shared, task }
    }
}

impl Drop for PollingWatcher {
    /// Stops the background task when the handle goes away.
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CatalogWatcher for PollingWatcher {
    /// Watched tables from last-known state, sorted by identity.
    fn watched_tables(&self) -> Vec<TableIdent> {
        self.shared.watched_tables()
    }

    /// Last-known pointer for one table.
    fn table_state(&self, table: &TableIdent) -> Option<TableState> {
        self.shared.table_state(table)
    }

    /// Recent lineage for one table (empty if unknown).
    fn lineage(&self, table: &TableIdent) -> Vec<SnapshotEntry> {
        self.shared.lineage(table)
    }

    /// A fresh event subscription.
    fn subscribe(&self) -> broadcast::Receiver<TableChanged> {
        self.shared.subscribe()
    }

    /// `true` once the first successful poll has populated the watched set.
    fn seeded(&self) -> watch::Receiver<bool> {
        self.shared.seeded_rx()
    }
}

/// The catalog change feed the server runs (#47 transport selection): the same
/// [`CatalogWatcher`] surface, driven by either the websocket transport or
/// polling.
///
/// With a [`WsFeedConfig`] the feed attempts a websocket upgrade at the catalog
/// origin; on success the websocket drives changes and polling only covers
/// reconnect gaps, and on a non-upgradeable endpoint (a third-party catalog) it
/// falls back to polling and retries the upgrade periodically. Without a config
/// (SigV4 / AWS catalogs, which never speak the feed) polling is the only mode.
pub struct CatalogFeed {
    /// State shared with the background feed task.
    shared: Arc<Shared>,
    /// The background feed task; aborted on drop.
    task: tokio::task::JoinHandle<()>,
}

impl CatalogFeed {
    /// Spawns the feed. `ws` selects the transport: `Some` attempts websocket
    /// with polling fallback; `None` polls only.
    pub fn spawn<S: CatalogSource>(
        source: S,
        options: WatcherOptions,
        ws: Option<WsFeedConfig>,
    ) -> CatalogFeed {
        let shared = Arc::new(Shared::new());
        let task = match ws {
            Some(config) => tokio::spawn(super::websocket::run(
                source,
                options,
                Arc::clone(&shared),
                config,
            )),
            None => tokio::spawn(run(source, options, Arc::clone(&shared))),
        };
        CatalogFeed { shared, task }
    }
}

impl Drop for CatalogFeed {
    /// Stops the background task when the handle goes away.
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CatalogWatcher for CatalogFeed {
    /// Watched tables from last-known state, sorted by identity.
    fn watched_tables(&self) -> Vec<TableIdent> {
        self.shared.watched_tables()
    }

    /// Last-known pointer for one table.
    fn table_state(&self, table: &TableIdent) -> Option<TableState> {
        self.shared.table_state(table)
    }

    /// Recent lineage for one table (empty if unknown).
    fn lineage(&self, table: &TableIdent) -> Vec<SnapshotEntry> {
        self.shared.lineage(table)
    }

    /// A fresh event subscription.
    fn subscribe(&self) -> broadcast::Receiver<TableChanged> {
        self.shared.subscribe()
    }

    /// `true` once the first successful poll/attach has populated the watched
    /// set.
    fn seeded(&self) -> watch::Receiver<bool> {
        self.shared.seeded_rx()
    }
}

/// The poll-forever task: sleep-with-jitter between successful cycles,
/// exponential backoff (state intact) while the catalog is unreachable.
/// Never panics, never exits — resilience is this loop's whole job.
async fn run<S: CatalogSource>(source: S, options: WatcherOptions, shared: Arc<Shared>) {
    // The first successful cycle seeds state silently (no event flood on
    // startup); only later cycles emit appearance events.
    let mut seeded = false;
    let mut failures: u32 = 0;
    loop {
        match poll_once(&source, &options, &shared, seeded).await {
            Ok(()) => {
                if failures > 0 {
                    tracing::info!(failures, "catalog poll recovered; watching resumed");
                }
                seeded = true;
                // Wake startup consumers: the watched set now reflects a real
                // read of the catalog (send_replace never fails, receivers or
                // not). Idempotent on every later successful cycle.
                shared.seeded.send_replace(true);
                failures = 0;
                tokio::time::sleep(options.interval + jitter(options.jitter)).await;
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                let delay = backoff_delay(options.interval, options.max_backoff, failures);
                tracing::warn!(
                    %error,
                    failures,
                    ?delay,
                    "catalog poll failed; backing off with last-known state intact"
                );
                tokio::time::sleep(delay + jitter(options.jitter)).await;
            }
        }
    }
}

/// Diffs one table's freshly-read `state` against last-known state, updating
/// the record and returning the [`TableChanged`] to emit (or `None` when the
/// pointer is unchanged). A brand-new table emits only when `seeded` — the
/// initial seeding pass stays silent. Shared by the polling loop and the
/// websocket single-table refresh so both diff identically.
fn apply_one(
    tables: &mut HashMap<TableIdent, TableRecord>,
    ident: TableIdent,
    state: TableState,
    history_depth: usize,
    seeded: bool,
) -> Option<TableChanged> {
    match tables.get_mut(&ident) {
        Some(record) if record.state == state => None,
        Some(record) => {
            let event = TableChanged {
                table: ident,
                old_snapshot: record.state.current_snapshot_id,
                new_snapshot: state.current_snapshot_id,
            };
            record.push_lineage(&state, history_depth);
            record.state = state;
            Some(event)
        }
        None => {
            let mut record = TableRecord {
                state: state.clone(),
                lineage: Vec::new(),
            };
            record.push_lineage(&state, history_depth);
            let event = seeded.then(|| TableChanged {
                table: ident.clone(),
                old_snapshot: None,
                new_snapshot: state.current_snapshot_id,
            });
            tables.insert(ident, record);
            event
        }
    }
}

/// One cheap poll cycle: list tables, filter, read each watched table's
/// pointer, diff against last-known state, emit events. No manifest reads
/// ever happen here. Errors on a single table are logged and skipped (its
/// state is kept); an error listing tables fails the cycle for backoff.
///
/// Shared by the polling watcher and the websocket transport: the websocket
/// runs one pass to seed the watched set on attach, to catch up on reconnect,
/// and to service a `resync`.
pub(crate) async fn poll_once<S: CatalogSource>(
    source: &S,
    options: &WatcherOptions,
    shared: &Shared,
    seeded: bool,
) -> Result<(), super::CatalogError> {
    let listed = source.list_tables().await?;
    let watched: BTreeSet<TableIdent> = listed
        .into_iter()
        .filter(|table| options.filter.matches(table))
        .collect();

    // Read pointers before taking the lock — no lock is held across awaits.
    let mut pointers: Vec<(TableIdent, TableState)> = Vec::with_capacity(watched.len());
    for table in &watched {
        match source.table_pointer(table).await {
            Ok(state) => pointers.push((table.clone(), state)),
            Err(error) => {
                tracing::warn!(table = %table, %error, "table pointer read failed; keeping last-known state");
            }
        }
    }

    let mut emitted: Vec<TableChanged> = Vec::new();
    {
        let mut tables = shared.tables();
        for (ident, state) in pointers {
            if let Some(event) = apply_one(&mut tables, ident, state, options.history_depth, seeded)
            {
                emitted.push(event);
            }
        }
        // Tables that left the catalog (or the filter set): drop state and
        // tell consumers via `new_snapshot: None`.
        let gone: Vec<TableIdent> = tables
            .keys()
            .filter(|ident| !watched.contains(*ident))
            .cloned()
            .collect();
        for ident in gone {
            if let Some(record) = tables.remove(&ident) {
                emitted.push(TableChanged {
                    table: ident,
                    old_snapshot: record.state.current_snapshot_id,
                    new_snapshot: None,
                });
            }
        }
    }
    for event in emitted {
        // Err means no subscribers exist right now; state is still updated,
        // so late subscribers resynchronize from it (module-doc semantics).
        let _ = shared.events.send(event);
    }
    Ok(())
}

/// Refreshes one table after a websocket `change` frame: read its current
/// pointer, diff against last-known state, and emit downstream — the same
/// handling the poller drives, but for a single named table instead of a full
/// scan. Tables outside the filter set are ignored; a pointer-read error keeps
/// last-known state (the reconnect catch-up poll will retry).
pub(crate) async fn refresh_table<S: CatalogSource>(
    source: &S,
    options: &WatcherOptions,
    shared: &Shared,
    table: &TableIdent,
) {
    if !options.filter.matches(table) {
        return;
    }
    let state = match source.table_pointer(table).await {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(table = %table, %error, "feed change: table pointer read failed; keeping last-known state");
            return;
        }
    };
    let event = {
        let mut tables = shared.tables();
        // A change frame names a table the server knows has committed, so the
        // refresh always treats the table as seeded (a new appearance emits).
        apply_one(
            &mut tables,
            table.clone(),
            state,
            options.history_depth,
            true,
        )
    };
    if let Some(event) = event {
        let _ = shared.events.send(event);
    }
}

/// Exponential backoff for consecutive failures: `interval * 2^(n-1)`,
/// capped. Failure #1 retries after one interval.
fn backoff_delay(interval: Duration, cap: Duration, failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(10);
    interval.saturating_mul(1 << exponent).min(cap)
}

/// A uniform-ish random delay in `[0, max)`. Cheap clock-seeded xorshift —
/// jitter needs spread across a fleet, not statistical quality, so this
/// avoids a `rand` dependency.
fn jitter(max: Duration) -> Duration {
    if max.is_zero() {
        return Duration::ZERO;
    }
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| (d.as_secs() << 32) | u64::from(d.subsec_nanos()))
        .unwrap_or(0x9E37_79B9_7F4A_7C15);
    let mut x = seed | 1;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    let span = max.as_nanos().min(u128::from(u64::MAX)) as u64;
    Duration::from_nanos(x % span)
}
