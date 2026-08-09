//! Incremental-update acceptance (issue #49): atomic swaps, time travel, and
//! the event-driven updater.
//!
//! Covers: a concurrent `classify()` during a swap always sees a consistent
//! (all-old or all-new) state; an old snapshot's files still resolve after a
//! commit (time travel); and the single-writer `MapUpdater` rebuilds the map
//! from catalog events.

mod support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use support::{BUCKET, FileSpec, SnapshotSpec};
use tokio::sync::broadcast;
use verglas_tables::catalog::{
    CatalogWatcher, SnapshotEntry, TableChanged, TableIdent, TableState,
};
use verglas_tables::fetch::ObjectStoreFetch;
use verglas_tables::mapper::updater::MapUpdater;
use verglas_tables::mapper::{BuildInputs, Classify, FileFormat, Mapper};

/// A hand-driven `CatalogWatcher` for the updater test: tests set a table's
/// pointer + lineage and emit change events explicitly.
struct MockWatcher {
    /// ident -> (current pointer, lineage).
    inner: Mutex<HashMap<TableIdent, (TableState, Vec<SnapshotEntry>)>>,
    /// Event fan-out.
    events: broadcast::Sender<TableChanged>,
}

impl MockWatcher {
    /// A fresh empty watcher.
    fn new() -> Arc<MockWatcher> {
        let (events, _) = broadcast::channel(64);
        Arc::new(MockWatcher {
            inner: Mutex::new(HashMap::new()),
            events,
        })
    }

    /// Sets a table's current pointer and snapshot lineage.
    fn set(&self, ident: &TableIdent, metadata_uri: &str, current: i64, lineage_ids: &[i64]) {
        let state = TableState {
            metadata_location: metadata_uri.to_owned(),
            current_snapshot_id: Some(current),
        };
        let lineage = lineage_ids
            .iter()
            .map(|id| SnapshotEntry {
                snapshot_id: *id,
                metadata_location: metadata_uri.to_owned(),
                observed_at: SystemTime::now(),
            })
            .collect();
        self.inner
            .lock()
            .expect("present")
            .insert(ident.clone(), (state, lineage));
    }

    /// Emits a change event.
    fn emit(&self, change: TableChanged) {
        let _ = self.events.send(change);
    }
}

impl CatalogWatcher for MockWatcher {
    fn watched_tables(&self) -> Vec<TableIdent> {
        self.inner
            .lock()
            .expect("present")
            .keys()
            .cloned()
            .collect()
    }
    fn table_state(&self, table: &TableIdent) -> Option<TableState> {
        self.inner
            .lock()
            .expect("present")
            .get(table)
            .map(|(s, _)| s.clone())
    }
    fn lineage(&self, table: &TableIdent) -> Vec<SnapshotEntry> {
        self.inner
            .lock()
            .expect("present")
            .get(table)
            .map(|(_, l)| l.clone())
            .unwrap_or_default()
    }
    fn subscribe(&self) -> broadcast::Receiver<TableChanged> {
        self.events.subscribe()
    }
    fn seeded(&self) -> tokio::sync::watch::Receiver<bool> {
        // Hand-driven state is set before consumers bind, so the mock is
        // seeded from birth.
        let (tx, rx) = tokio::sync::watch::channel(true);
        drop(tx);
        rx
    }
}

/// Polls `f` until it returns true or the deadline passes.
async fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not met within deadline");
}

/// Builds a two-snapshot store (snap1 file v1, snap2 file v2) with metadata
/// naming both, current = 2.
async fn two_snapshot_store() -> (Arc<dyn object_store::ObjectStore>, support::FixtureTable) {
    let store = support::new_store();
    let snap1 = SnapshotSpec {
        snapshot_id: 1,
        files: vec![FileSpec {
            rel_path: "data/v1.parquet".to_owned(),
            format: FileFormat::Parquet,
            bytes: support::parquet_flat(0),
            partition: vec![],
        }],
    };
    let snap2 = SnapshotSpec {
        snapshot_id: 2,
        files: vec![FileSpec {
            rel_path: "data/v2.parquet".to_owned(),
            format: FileFormat::Parquet,
            bytes: support::parquet_flat(1),
            partition: vec![],
        }],
    };
    let fixture = support::build_table(&store, "warehouse/db/t", &[snap1, snap2], 2).await;
    (store, fixture)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_classify_during_swap_is_consistent() {
    let (store, fixture) = two_snapshot_store().await;
    let ident = TableIdent::new(&["db"], "t");
    let mapper = Arc::new(Mapper::new());
    let fetch = Arc::new(ObjectStoreFetch::new(Arc::clone(&store)));

    let inputs = BuildInputs {
        storage_binding_id: "managed-lakehouse".to_owned(),
        ident: ident.clone(),
        bucket: BUCKET.to_owned(),
        metadata_key: fixture.metadata_key.clone(),
        snapshot_ids: vec![1, 2],
        current_snapshot_id: Some(2),
    };
    mapper
        .apply_table(fetch.as_ref(), &inputs)
        .await
        .expect("present");

    let v1 = format!("{}/data/v1.parquet", fixture.root);
    let v2 = format!("{}/data/v2.parquet", fixture.root);

    // Writer: repeatedly remove and re-add the whole table. Each swap flips both
    // files present <-> both absent atomically.
    let writer = {
        let mapper = Arc::clone(&mapper);
        let fetch = Arc::clone(&fetch);
        let ident = ident.clone();
        let inputs = inputs.clone();
        tokio::spawn(async move {
            for _ in 0..200 {
                mapper.remove_table(&ident);
                mapper
                    .apply_table(fetch.as_ref(), &inputs)
                    .await
                    .expect("present");
            }
        })
    };

    // Readers: within a single loaded state, v1 and v2 must agree — a torn swap
    // would show one present and the other absent.
    let mut readers = Vec::new();
    for _ in 0..3 {
        let mapper = Arc::clone(&mapper);
        let (v1, v2) = (v1.clone(), v2.clone());
        readers.push(tokio::spawn(async move {
            for _ in 0..50_000 {
                let state = mapper.state();
                let a = matches!(
                    state.classify(BUCKET, &v1, None),
                    Classify::TableData { .. }
                );
                let b = matches!(
                    state.classify(BUCKET, &v2, None),
                    Classify::TableData { .. }
                );
                assert_eq!(a, b, "torn swap: v1 and v2 disagreed within one state");
            }
        }));
    }

    writer.await.expect("present");
    for r in readers {
        r.await.expect("present");
    }
}

#[tokio::test]
async fn old_snapshot_files_resolve_after_commit_time_travel() {
    let (store, fixture) = two_snapshot_store().await;
    let ident = TableIdent::new(&["db"], "t");
    let mapper = Arc::new(Mapper::new());
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));

    // Build at the current snapshot while retaining snapshot 1 in the lineage.
    mapper
        .apply_table(
            &fetch,
            &BuildInputs {
                storage_binding_id: "managed-lakehouse".to_owned(),
                ident: ident.clone(),
                bucket: BUCKET.to_owned(),
                metadata_key: fixture.metadata_key.clone(),
                snapshot_ids: vec![1, 2],
                current_snapshot_id: Some(2),
            },
        )
        .await
        .expect("present");

    let v1 = format!("{}/data/v1.parquet", fixture.root);
    // The old-snapshot file (not in the current live set) still classifies.
    assert!(matches!(
        mapper.classify(BUCKET, &v1, None),
        Classify::TableData { .. }
    ));
}

#[tokio::test]
async fn updater_seeds_then_rebuilds_on_commit_event() {
    let (store, fixture) = two_snapshot_store().await;
    let ident = TableIdent::new(&["db"], "t");
    let mapper = Arc::new(Mapper::new());
    let fetch = Arc::new(ObjectStoreFetch::new(Arc::clone(&store)));
    let watcher = MockWatcher::new();

    // Watcher initially knows the table at snapshot 1 only.
    watcher.set(&ident, &fixture.metadata_uri, 1, &[1]);

    let updater = MapUpdater::new(
        "managed-lakehouse",
        Arc::clone(&mapper),
        Arc::clone(&watcher),
        Arc::clone(&fetch),
    );
    let handle = updater.spawn();

    let v1 = format!("{}/data/v1.parquet", fixture.root);
    let v2 = format!("{}/data/v2.parquet", fixture.root);

    // After seeding, the snapshot-1 file resolves and the snapshot-2 file does
    // not yet exist in the map.
    {
        let mapper = Arc::clone(&mapper);
        let v1 = v1.clone();
        wait_until(move || {
            matches!(
                mapper.classify(BUCKET, &v1, None),
                Classify::TableData { .. }
            )
        })
        .await;
    }
    assert_eq!(mapper.classify(BUCKET, &v2, None), Classify::Unknown);

    // A commit to snapshot 2: watcher advances, emits an event, updater rebuilds.
    watcher.set(&ident, &fixture.metadata_uri, 2, &[1, 2]);
    watcher.emit(TableChanged {
        table: ident.clone(),
        old_snapshot: Some(1),
        new_snapshot: Some(2),
    });

    {
        let mapper = Arc::clone(&mapper);
        let v2 = v2.clone();
        wait_until(move || {
            matches!(
                mapper.classify(BUCKET, &v2, None),
                Classify::TableData { .. }
            )
        })
        .await;
    }
    // Snapshot 1 remains resolvable (retained lineage / time travel).
    assert!(matches!(
        mapper.classify(BUCKET, &v1, None),
        Classify::TableData { .. }
    ));

    handle.abort();
}

#[tokio::test]
async fn updater_drops_table_on_removal_event() {
    let (store, fixture) = two_snapshot_store().await;
    let ident = TableIdent::new(&["db"], "t");
    let mapper = Arc::new(Mapper::new());
    let fetch = Arc::new(ObjectStoreFetch::new(Arc::clone(&store)));
    let watcher = MockWatcher::new();
    watcher.set(&ident, &fixture.metadata_uri, 2, &[1, 2]);

    let updater = MapUpdater::new(
        "managed-lakehouse",
        Arc::clone(&mapper),
        Arc::clone(&watcher),
        Arc::clone(&fetch),
    );
    let handle = updater.spawn();

    let v2 = format!("{}/data/v2.parquet", fixture.root);
    {
        let mapper = Arc::clone(&mapper);
        let v2 = v2.clone();
        wait_until(move || {
            matches!(
                mapper.classify(BUCKET, &v2, None),
                Classify::TableData { .. }
            )
        })
        .await;
    }

    // Table dropped from the catalog: new_snapshot None.
    watcher.emit(TableChanged {
        table: ident.clone(),
        old_snapshot: Some(2),
        new_snapshot: None,
    });
    {
        let mapper = Arc::clone(&mapper);
        let v2 = v2.clone();
        wait_until(move || matches!(mapper.classify(BUCKET, &v2, None), Classify::Unknown)).await;
    }

    handle.abort();
}
