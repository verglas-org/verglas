//! Acceptance tests for the durable tenant-scoped KV engine.

use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};

use bytes::Bytes;
use verglas_kv::{DeleteOptions, Error, PutOptions, ReadTier, Scope, Store, StoreConfig};

/// Opens a test store with bounded but roomy tiers.
fn open(path: &std::path::Path) -> Store {
    Store::open(
        path,
        StoreConfig {
            capacity_bytes: 1024 * 1024,
            ram_bytes: 1024,
        },
    )
    .expect("open store")
}

/// Returns the common tenant/namespace used by isolated tests.
fn scope() -> Scope {
    Scope::new("tenant-a", "workshop.blueprints").expect("valid scope")
}

/// The store exposes every issue-defined KV metric as a bounded Prometheus family.
#[test]
fn metrics_exposition_covers_requests_tiers_storage_and_maintenance() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path());
    store
        .put(
            &scope(),
            "metric",
            Bytes::from_static(b"value"),
            PutOptions::default(),
        )
        .expect("put");
    assert!(
        store.used_bytes() > 0,
        "durable log usage must be observable"
    );
    store.get(&scope(), "metric").expect("get").expect("live");

    let rendered = store.render_metrics();
    for family in [
        "verglas_kv_requests_total",
        "verglas_kv_hits_total",
        "verglas_kv_misses_total",
        "verglas_kv_expired_reads_total",
        "verglas_kv_nvme_read_seconds_total",
        "verglas_kv_bytes_total",
        "verglas_kv_capacity_failures_total",
        "verglas_kv_cas_conflicts_total",
        "verglas_kv_recovery_seconds",
        "verglas_kv_compaction_failures_total",
        "verglas_kv_cleanup_failures_total",
    ] {
        assert!(rendered.contains(family), "missing {family}");
    }
}

/// A committed write is hot immediately and cold-but-present after restart.
#[test]
fn hot_and_cold_reads_survive_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path());
    let result = store
        .put(
            &scope(),
            "featured",
            Bytes::from_static(b"blue"),
            PutOptions::default(),
        )
        .expect("put");
    let hot = store.get(&scope(), "featured").expect("get").expect("live");
    assert_eq!(hot.bytes, Bytes::from_static(b"blue"));
    assert_eq!(hot.version, result.version);
    assert_eq!(hot.tier, ReadTier::Ram);
    drop(store);

    let recovered = open(dir.path());
    let cold = recovered
        .get(&scope(), "featured")
        .expect("get")
        .expect("live");
    assert_eq!(cold.bytes, Bytes::from_static(b"blue"));
    assert_eq!(cold.version, result.version);
    assert_eq!(cold.tier, ReadTier::Nvme);
    assert_eq!(
        recovered
            .get(&scope(), "featured")
            .expect("get")
            .expect("live")
            .tier,
        ReadTier::Ram
    );
}

/// Overwrite, compare-and-set, and create-only checks are atomic.
#[test]
fn conditional_writes_and_overwrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path());
    let first = store
        .put(
            &scope(),
            "key",
            Bytes::from_static(b"one"),
            PutOptions::default(),
        )
        .expect("first");
    let conflict = store.put(
        &scope(),
        "key",
        Bytes::from_static(b"wrong"),
        PutOptions {
            if_match: Some("not-the-version".to_owned()),
            ..PutOptions::default()
        },
    );
    assert_eq!(conflict, Err(Error::Precondition));
    let second = store
        .put(
            &scope(),
            "key",
            Bytes::from_static(b"two"),
            PutOptions {
                if_match: Some(first.version),
                ..PutOptions::default()
            },
        )
        .expect("cas");
    assert_ne!(second.version, "not-the-version");
    assert_eq!(
        store.put(
            &scope(),
            "key",
            Bytes::new(),
            PutOptions {
                create_only: true,
                ..PutOptions::default()
            },
        ),
        Err(Error::Precondition)
    );
    assert_eq!(
        store
            .get(&scope(), "key")
            .expect("get")
            .expect("live")
            .bytes,
        Bytes::from_static(b"two")
    );
}

/// Concurrent create-only writers produce exactly one committed version.
#[test]
fn conditional_write_race_has_one_winner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(open(dir.path()));
    let barrier = Arc::new(Barrier::new(8));
    let mut joins = Vec::new();
    for n in 0..8 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        joins.push(std::thread::spawn(move || {
            barrier.wait();
            store.put(
                &scope(),
                "race",
                Bytes::from(format!("value-{n}")),
                PutOptions {
                    create_only: true,
                    ..PutOptions::default()
                },
            )
        }));
    }
    let results: Vec<_> = joins.into_iter().map(|j| j.join().expect("join")).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == Err(Error::Precondition))
            .count(),
        7
    );
}

/// An idempotency key replays one logical version across restart.
#[test]
fn duplicate_idempotency_key_replays_version() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path());
    let options = PutOptions {
        idempotency_key: Some("request-7".to_owned()),
        ..PutOptions::default()
    };
    let first = store
        .put(&scope(), "key", Bytes::from_static(b"one"), options.clone())
        .expect("first");
    let replay = store
        .put(&scope(), "key", Bytes::from_static(b"two"), options.clone())
        .expect("replay");
    assert_eq!(replay.version, first.version);
    assert!(replay.idempotent);
    drop(store);
    let recovered = open(dir.path());
    let replay = recovered
        .put(&scope(), "key", Bytes::from_static(b"three"), options)
        .expect("replay after restart");
    assert_eq!(replay.version, first.version);
    assert_eq!(
        recovered
            .get(&scope(), "key")
            .expect("get")
            .expect("live")
            .bytes,
        Bytes::from_static(b"one")
    );
}

/// Deletes are idempotent, conditional, and durable as tombstones.
#[test]
fn delete_and_tombstone_survive_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path());
    let put = store
        .put(
            &scope(),
            "key",
            Bytes::from_static(b"value"),
            PutOptions::default(),
        )
        .expect("put");
    assert_eq!(
        store.delete(
            &scope(),
            "key",
            DeleteOptions {
                if_match: Some("wrong".to_owned())
            }
        ),
        Err(Error::Precondition)
    );
    assert!(
        store
            .delete(
                &scope(),
                "key",
                DeleteOptions {
                    if_match: Some(put.version)
                }
            )
            .expect("delete")
            .removed
    );
    assert!(
        !store
            .delete(&scope(), "key", DeleteOptions::default())
            .expect("repeat")
            .removed
    );
    drop(store);
    assert!(
        open(dir.path())
            .get(&scope(), "key")
            .expect("get")
            .is_none()
    );
}

/// Expiration suppresses bytes before any physical cleanup runs.
#[test]
fn expired_values_are_never_returned_or_listed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path());
    store
        .put(
            &scope(),
            "expired",
            Bytes::from_static(b"secret"),
            PutOptions {
                expires_at_ms: Some(1),
                ..PutOptions::default()
            },
        )
        .expect("put");
    assert!(store.get(&scope(), "expired").expect("get").is_none());
    assert!(
        store
            .list(&scope(), "", 100, None)
            .expect("list")
            .entries
            .is_empty()
    );
    assert_eq!(store.metrics().expired_reads, 1);
}

/// Tenant and namespace are part of lookup and list resolution.
#[test]
fn tenant_and_namespace_are_isolated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path());
    store
        .put(
            &scope(),
            "shared",
            Bytes::from_static(b"a"),
            PutOptions::default(),
        )
        .expect("put");
    let other_tenant = Scope::new("tenant-b", "workshop.blueprints").expect("scope");
    let other_namespace = Scope::new("tenant-a", "other").expect("scope");
    assert!(store.get(&other_tenant, "shared").expect("get").is_none());
    assert!(
        store
            .get(&other_namespace, "shared")
            .expect("get")
            .is_none()
    );
    assert!(
        store
            .list(&other_tenant, "", 10, None)
            .expect("list")
            .entries
            .is_empty()
    );
}

/// Listing is metadata-only, prefix-bounded, deterministic, and paginated.
#[test]
fn list_is_ordered_prefix_bounded_and_cursor_checked() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path());
    for key in ["user/c", "other", "user/a", "user/b"] {
        store
            .put(
                &scope(),
                key,
                Bytes::from_static(b"must-not-be-listed"),
                PutOptions {
                    content_type: Some("text/plain".to_owned()),
                    metadata: BTreeMap::from([("kind".to_owned(), "demo".to_owned())]),
                    ..PutOptions::default()
                },
            )
            .expect("put");
    }
    let first = store.list(&scope(), "user/", 2, None).expect("first");
    assert_eq!(
        first
            .entries
            .iter()
            .map(|e| e.key.as_str())
            .collect::<Vec<_>>(),
        ["user/a", "user/b"]
    );
    assert!(first.entries.iter().all(|e| e.metadata["kind"] == "demo"));
    let cursor = first.next_cursor.expect("cursor");
    let second = store
        .list(&scope(), "user/", 2, Some(&cursor))
        .expect("second");
    assert_eq!(
        second
            .entries
            .iter()
            .map(|e| e.key.as_str())
            .collect::<Vec<_>>(),
        ["user/c"]
    );
    assert!(second.next_cursor.is_none());
    assert!(matches!(
        store.list(&scope(), "other", 2, Some(&cursor)),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        store.list(&scope(), "", 2, Some("not-a-cursor")),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        store.list(&scope(), "", 0, None),
        Err(Error::Invalid(_))
    ));
}

/// Capacity refusal does not evict or overwrite a live value.
#[test]
fn capacity_exhaustion_is_explicit_and_preserves_live_values() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::open(
        dir.path(),
        StoreConfig {
            capacity_bytes: 700,
            ram_bytes: 128,
        },
    )
    .expect("open");
    store
        .put(
            &scope(),
            "kept",
            Bytes::from(vec![7; 128]),
            PutOptions::default(),
        )
        .expect("first");
    let mut refused = false;
    for n in 0..20 {
        match store.put(
            &scope(),
            &format!("fill-{n}"),
            Bytes::from(vec![n as u8; 128]),
            PutOptions::default(),
        ) {
            Ok(_) => {}
            Err(Error::Capacity) => {
                refused = true;
                break;
            }
            Err(error) => panic!("unexpected error: {error}"),
        }
    }
    assert!(refused);
    assert_eq!(
        store
            .get(&scope(), "kept")
            .expect("get")
            .expect("kept")
            .bytes,
        Bytes::from(vec![7; 128])
    );
    assert!(store.metrics().capacity_failures > 0);
}

/// Shared-disk accounting can close unused headroom without evicting committed KV.
#[test]
fn dynamic_capacity_ceiling_refuses_growth_but_preserves_committed_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path());
    store
        .put(
            &scope(),
            "kept",
            Bytes::from_static(b"value"),
            PutOptions::default(),
        )
        .expect("put");
    store.set_capacity_ceiling(store.used_bytes());
    assert_eq!(
        store.put(
            &scope(),
            "new",
            Bytes::from_static(b"more"),
            PutOptions::default(),
        ),
        Err(Error::Capacity)
    );
    assert_eq!(
        store
            .get(&scope(), "kept")
            .expect("get")
            .expect("live")
            .bytes,
        Bytes::from_static(b"value")
    );
}

/// Compaction preserves live versions and tombstone semantics across restart.
#[test]
fn compaction_preserves_latest_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path());
    let old = store
        .put(
            &scope(),
            "live",
            Bytes::from_static(b"old"),
            PutOptions::default(),
        )
        .expect("old");
    let live = store
        .put(
            &scope(),
            "live",
            Bytes::from_static(b"new"),
            PutOptions {
                if_match: Some(old.version),
                ..PutOptions::default()
            },
        )
        .expect("new");
    store
        .put(
            &scope(),
            "gone",
            Bytes::from_static(b"gone"),
            PutOptions::default(),
        )
        .expect("put gone");
    store
        .delete(&scope(), "gone", DeleteOptions::default())
        .expect("delete gone");
    store.compact().expect("compact");
    drop(store);
    let recovered = open(dir.path());
    let value = recovered.get(&scope(), "live").expect("get").expect("live");
    assert_eq!(value.bytes, Bytes::from_static(b"new"));
    assert_eq!(value.version, live.version);
    assert!(recovered.get(&scope(), "gone").expect("get").is_none());
}

/// Invalid scopes, keys, metadata, and oversized values fail before persistence.
#[test]
fn malformed_inputs_are_rejected() {
    assert!(matches!(Scope::new("", "ok"), Err(Error::Invalid(_))));
    assert!(matches!(
        Scope::new("ok", "bad/namespace"),
        Err(Error::Invalid(_))
    ));
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path());
    assert!(matches!(
        store.put(&scope(), "", Bytes::new(), PutOptions::default()),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        store.put(
            &scope(),
            &"k".repeat(2049),
            Bytes::new(),
            PutOptions::default()
        ),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        store.put(
            &scope(),
            "key",
            Bytes::new(),
            PutOptions {
                metadata: BTreeMap::from([("x".repeat(129), "v".to_owned())]),
                ..PutOptions::default()
            }
        ),
        Err(Error::Invalid(_))
    ));
}

/// A path that cannot become a directory reports a storage failure explicitly.
#[test]
fn storage_open_failure_is_not_reported_as_a_miss() {
    let dir = tempfile::tempdir().expect("tempdir");
    let occupied = dir.path().join("occupied");
    std::fs::write(&occupied, "not a directory").expect("write marker");
    let result = Store::open(
        &occupied,
        StoreConfig {
            capacity_bytes: 1024,
            ram_bytes: 128,
        },
    );
    assert!(matches!(result, Err(Error::Storage(_))));
}
