//! Orphan retirement: demote a compaction's removed files without breaking time
//! travel.
//!
//! When a commit removes files, they are not destroyed — an older snapshot may
//! still be queried (time travel), so the entries are **demoted** (marked
//! evict-first), not evicted, and only hard-evicted after a grace window tied to
//! the table's snapshot-expiry policy. The window is read from table properties
//! (`history.expire.max-snapshot-age-ms`, default 7 days): recent time-travel
//! reads stay fast, and the space cost is bounded by normal eviction pressure.
//!
//! # Scope (this PR)
//!
//! This module computes the retirement schedule — which keys to demote and when
//! each may be hard-evicted — and tracks it. Wiring the *evict-first bit* into
//! the data store's eviction policy needs a per-entry flag the two-store cache
//! (#50) does not yet expose; that is a tracked follow-up. Until then the
//! scheduler is the source of truth a future eviction hook consults, and the
//! mapper's snapshot retention already keeps a removed file resolvable for time
//! travel while its snapshot is retained.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::iceberg::DataFileEntry;

/// The Iceberg table property naming the maximum snapshot age before expiry.
pub const EXPIRE_MAX_AGE_PROP: &str = "history.expire.max-snapshot-age-ms";
/// The default grace window when the table sets no expiry policy: 7 days.
pub const DEFAULT_GRACE_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Reads the retirement grace window (milliseconds) from parsed table
/// properties, falling back to [`DEFAULT_GRACE_MS`] when unset or unparseable.
pub fn grace_ms_from_properties(properties: &HashMap<String, String>) -> u64 {
    properties
        .get(EXPIRE_MAX_AGE_PROP)
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(DEFAULT_GRACE_MS)
}

/// Parses the `properties` map from a `metadata.json` document. Lenient: a
/// missing or malformed `properties` object yields an empty map.
pub fn parse_table_properties(metadata_json: &[u8]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Ok(serde_json::Value::Object(doc)) = serde_json::from_slice(metadata_json)
        && let Some(serde_json::Value::Object(props)) = doc.get("properties")
    {
        for (k, v) in props {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_owned());
            }
        }
    }
    out
}

/// One demoted object: its cache key, the wall-clock time (Unix ms) after
/// which it may be hard-evicted, and the enumeration facts the engine captured
/// at demotion time (#305) — the ETag, size, and admission generation physical
/// removal needs once the grace window closes. Serialized into the retire
/// state file so a restart cannot amnesty dead bytes.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Demotion {
    /// Bucket the file lives in.
    pub bucket: String,
    /// Object key of the removed data file.
    pub key: String,
    /// Unix-milliseconds after which the entry may be hard-evicted.
    pub hard_evict_at_ms: u64,
    /// The ETag the cached blocks were admitted under, when the mapping was
    /// still warm at demotion time. `None` means the blocks cannot be
    /// enumerated for physical removal and are left to LRU aging.
    pub etag: Option<String>,
    /// The object size in bytes, bounding the block enumeration.
    pub size: u64,
    /// The admission generation the blocks live under (#178).
    pub generation: u64,
}

/// The per-entry facts the scheduler retains alongside each deadline (#305).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Retained {
    /// See [`Demotion::hard_evict_at_ms`].
    deadline_ms: u64,
    /// See [`Demotion::etag`].
    etag: Option<String>,
    /// See [`Demotion::size`].
    size: u64,
    /// See [`Demotion::generation`].
    generation: u64,
}

/// Tracks demoted (evict-first) entries, their hard-evict deadlines, and the
/// physical-removal facts (#305). The demotion set is what keeps
/// recently-orphaned bytes serving time-travel reads through the grace window;
/// the sweep drains it into physical hard evictions.
pub struct RetirementScheduler {
    /// Demoted key → retained facts, keyed by `(bucket, key)`.
    demoted: Mutex<HashMap<(String, String), Retained>>,
}

impl Default for RetirementScheduler {
    /// An empty scheduler.
    fn default() -> RetirementScheduler {
        RetirementScheduler::new()
    }
}

impl RetirementScheduler {
    /// A fresh scheduler with no demotions.
    pub fn new() -> RetirementScheduler {
        RetirementScheduler {
            demoted: Mutex::new(HashMap::new()),
        }
    }

    /// Records `demotions` (idempotent per key: a re-demotion refreshes the
    /// deadline and enumeration facts, except that a known ETag is never
    /// overwritten by `None` — the first warm capture is the one that can
    /// physically remove the blocks). Off the request hot path.
    pub fn record(&self, demotions: &[Demotion]) {
        let mut set = self.demoted.lock().unwrap_or_else(|p| p.into_inner());
        for d in demotions {
            let entry = Retained {
                deadline_ms: d.hard_evict_at_ms,
                etag: d.etag.clone(),
                size: d.size,
                generation: d.generation,
            };
            match set.entry((d.bucket.clone(), d.key.clone())) {
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    let keep_etag = if entry.etag.is_none() {
                        o.get().etag.clone()
                    } else {
                        entry.etag.clone()
                    };
                    let merged = Retained {
                        etag: keep_etag,
                        ..entry
                    };
                    o.insert(merged);
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(entry);
                }
            }
        }
    }

    /// Whether `(bucket, key)` is currently demoted (evict-first).
    pub fn is_demoted(&self, bucket: &str, key: &str) -> bool {
        self.demoted
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&(bucket.to_owned(), key.to_owned()))
    }

    /// Removes and returns the demotions whose grace window closed at or before
    /// `now_ms` — the entries a hard-eviction pass may now physically drop.
    /// Clearing them keeps the demotion set from growing without bound.
    pub fn drain_expired(&self, now_ms: u64) -> Vec<Demotion> {
        let mut set = self.demoted.lock().unwrap_or_else(|p| p.into_inner());
        let expired: Vec<(String, String)> = set
            .iter()
            .filter(|(_, r)| r.deadline_ms <= now_ms)
            .map(|((b, k), _)| (b.clone(), k.clone()))
            .collect();
        let mut out = Vec::with_capacity(expired.len());
        for (bucket, key) in expired {
            if let Some(r) = set.remove(&(bucket.clone(), key.clone())) {
                out.push(Demotion {
                    bucket,
                    key,
                    hard_evict_at_ms: r.deadline_ms,
                    etag: r.etag,
                    size: r.size,
                    generation: r.generation,
                });
            }
        }
        out
    }

    /// Every currently-demoted entry, for persistence and startup restore
    /// (#305). Off the hot path.
    pub fn all(&self) -> Vec<Demotion> {
        let set = self.demoted.lock().unwrap_or_else(|p| p.into_inner());
        set.iter()
            .map(|((bucket, key), r)| Demotion {
                bucket: bucket.clone(),
                key: key.clone(),
                hard_evict_at_ms: r.deadline_ms,
                etag: r.etag.clone(),
                size: r.size,
                generation: r.generation,
            })
            .collect()
    }

    /// The number of currently-demoted entries (for metrics/tests).
    pub fn demoted_count(&self) -> usize {
        self.demoted.lock().unwrap_or_else(|p| p.into_inner()).len()
    }
}

/// Builds the enriched demotions for one commit's removed files (#305): the
/// manifest's path and size joined with the engine's demotion receipt for the
/// same object (ETag + generation), stamped with the grace deadline.
pub fn demotions_for_commit(
    bucket: &str,
    removed: &[DataFileEntry],
    receipts: &[verglas_cache::DemoteReceipt],
    grace_ms: u64,
    now_ms: u64,
) -> Vec<Demotion> {
    let deadline = now_ms.saturating_add(grace_ms);
    removed
        .iter()
        .map(|file| {
            let receipt = receipts
                .iter()
                .find(|r| r.key.bucket == bucket && r.key.key == file.path);
            Demotion {
                bucket: bucket.to_owned(),
                key: file.path.clone(),
                hard_evict_at_ms: deadline,
                etag: receipt.and_then(|r| r.etag.clone()),
                size: if file.size > 0 {
                    file.size
                } else {
                    receipt.map(|r| r.size).unwrap_or(0)
                },
                generation: receipt.map(|r| r.generation).unwrap_or(0),
            }
        })
        .collect()
}

/// The persisted retirement state (#305): the demotion schedule plus the
/// per-table replay watermarks the coordinator maintains. Written atomically
/// (tmp + rename) after every change; loaded at startup so a restart cannot
/// amnesty dead bytes and commits made while the server was down are replayed
/// rather than lost.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RetireState {
    /// Every grace-pending demotion, enriched for physical removal.
    pub demotions: Vec<Demotion>,
    /// Per-table replay state, keyed by the table identity string.
    pub tables: HashMap<String, TableWatermark>,
}

/// Per-table replay state (#305): where retirement processing last stopped.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableWatermark {
    /// The last snapshot id whose commit was diffed and retired.
    pub last_snapshot_id: i64,
    /// The metadata.json key current as of `last_snapshot_id`. A commit
    /// supersedes it, and the superseded object is itself retired.
    pub metadata_key: String,
    /// Manifest-list key per retained snapshot. A snapshot that vanishes from
    /// the metadata between commits (expiry) retires its manifest list.
    /// Manifest *data* files are deliberately not tracked here: manifests are
    /// shared across snapshots, so retiring one without refcounting could evict
    /// live metadata — those age out under LRU instead.
    pub manifest_lists: HashMap<i64, String>,
}

impl RetireState {
    /// Loads the state from `path`. Lenient: a missing or malformed file is an
    /// empty state (at worst, pre-existing dead bytes age out under LRU as they
    /// did before persistence existed) — never a startup failure.
    pub fn load(path: &std::path::Path) -> RetireState {
        std::fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    /// Atomically persists the state to `path` (write to a sibling temp file,
    /// then rename). Best-effort at the call sites: a failed save costs
    /// durability of the schedule across a restart, never correctness.
    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        let bytes = serde_json::to_vec(self).map_err(std::io::Error::other)?;
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapper::FileFormat;

    /// A removed data file for tests.
    fn file(path: &str) -> DataFileEntry {
        DataFileEntry {
            path: path.to_owned(),
            format: FileFormat::Parquet,
            size: 1024,
            partition: vec![],
        }
    }

    /// The grace window reads the table property, and defaults to 7 days.
    #[test]
    fn grace_reads_property_or_defaults() {
        let mut props = HashMap::new();
        assert_eq!(grace_ms_from_properties(&props), DEFAULT_GRACE_MS);
        props.insert(EXPIRE_MAX_AGE_PROP.to_owned(), "3600000".to_owned());
        assert_eq!(grace_ms_from_properties(&props), 3_600_000);
        // A zero/garbage value falls back to the default.
        props.insert(EXPIRE_MAX_AGE_PROP.to_owned(), "0".to_owned());
        assert_eq!(grace_ms_from_properties(&props), DEFAULT_GRACE_MS);
        props.insert(EXPIRE_MAX_AGE_PROP.to_owned(), "abc".to_owned());
        assert_eq!(grace_ms_from_properties(&props), DEFAULT_GRACE_MS);
    }

    /// Table properties parse out of a real metadata.json body.
    #[test]
    fn properties_parse_from_metadata_json() {
        let json = serde_json::json!({
            "location": "s3://lake/t",
            "properties": {"history.expire.max-snapshot-age-ms": "123", "other": "x"}
        })
        .to_string();
        let props = parse_table_properties(json.as_bytes());
        assert_eq!(
            props.get(EXPIRE_MAX_AGE_PROP).map(String::as_str),
            Some("123")
        );
        assert_eq!(grace_ms_from_properties(&props), 123);
        // A body with no properties yields an empty map (and the default grace).
        assert!(parse_table_properties(b"{}").is_empty());
    }

    /// Demotion records a deadline; the entry stays demoted through the grace
    /// window and drains out only once it closes, carrying the enumeration
    /// facts (#305) captured at demotion time.
    #[test]
    fn demotion_holds_through_grace_then_drains() {
        let sched = RetirementScheduler::new();
        let removed = vec![file("t/data/old-0.parquet"), file("t/data/old-1.parquet")];
        let receipts = vec![verglas_cache::DemoteReceipt {
            key: verglas_core::CacheKey {
                bucket: "lake".to_owned(),
                key: "t/data/old-0.parquet".to_owned(),
            },
            etag: Some("\"abc\"".to_owned()),
            size: 1024,
            generation: 3,
        }];
        let demotions = demotions_for_commit("lake", &removed, &receipts, 1_000, 10_000);
        assert_eq!(demotions.len(), 2);
        assert_eq!(demotions[0].hard_evict_at_ms, 11_000);
        assert_eq!(demotions[0].etag.as_deref(), Some("\"abc\""));
        assert_eq!(demotions[0].generation, 3);
        // The second file had no receipt: no etag, but the manifest size holds.
        assert_eq!(demotions[1].etag, None);
        assert_eq!(demotions[1].size, 1024);
        sched.record(&demotions);
        assert!(sched.is_demoted("lake", "t/data/old-0.parquet"));
        assert_eq!(sched.demoted_count(), 2);

        // Before the deadline nothing drains.
        assert!(sched.drain_expired(10_500).is_empty());
        assert_eq!(sched.demoted_count(), 2);

        // At/after the deadline both drain, enrichment intact, and the set
        // empties.
        let expired = sched.drain_expired(11_000);
        assert_eq!(expired.len(), 2);
        assert!(
            expired
                .iter()
                .any(|d| d.etag.as_deref() == Some("\"abc\"") && d.generation == 3)
        );
        assert_eq!(sched.demoted_count(), 0);
        assert!(!sched.is_demoted("lake", "t/data/old-0.parquet"));
    }

    /// A re-demotion never downgrades a known ETag to `None` (#305): the first
    /// warm capture is what makes physical removal possible.
    #[test]
    fn re_demotion_keeps_the_known_etag() {
        let sched = RetirementScheduler::new();
        let with_etag = Demotion {
            bucket: "lake".to_owned(),
            key: "t/f.parquet".to_owned(),
            hard_evict_at_ms: 1_000,
            etag: Some("\"e1\"".to_owned()),
            size: 10,
            generation: 0,
        };
        sched.record(std::slice::from_ref(&with_etag));
        let without = Demotion {
            etag: None,
            hard_evict_at_ms: 2_000,
            ..with_etag.clone()
        };
        sched.record(std::slice::from_ref(&without));
        let drained = sched.drain_expired(5_000);
        assert_eq!(drained.len(), 1);
        assert_eq!(
            drained[0].etag.as_deref(),
            Some("\"e1\""),
            "the refreshed deadline holds but the warm-captured etag survives"
        );
        assert_eq!(drained[0].hard_evict_at_ms, 2_000);
    }

    /// The retire state round-trips through its file (#305): demotions with
    /// their enrichment and per-table watermarks survive a save/load — the
    /// restart-amnesty fix.
    #[test]
    fn retire_state_roundtrips_through_disk() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("retire-state.json");
        let mut state = RetireState::default();
        state.demotions.push(Demotion {
            bucket: "lake".to_owned(),
            key: "t/data/dead.parquet".to_owned(),
            hard_evict_at_ms: 42,
            etag: Some("\"e\"".to_owned()),
            size: 4096,
            generation: 1,
        });
        state.tables.insert(
            "db.events".to_owned(),
            TableWatermark {
                last_snapshot_id: 1002,
                metadata_key: "t/metadata/00002.metadata.json".to_owned(),
                manifest_lists: HashMap::from([(1002_i64, "t/metadata/snap-1002.avro".to_owned())]),
            },
        );
        state.save(&path).expect("save");
        let loaded = RetireState::load(&path);
        assert_eq!(loaded, state);

        // A missing or malformed file is an empty state, never an error.
        assert_eq!(
            RetireState::load(&dir.path().join("absent.json")),
            RetireState::default()
        );
        std::fs::write(&path, b"not json").expect("clobber");
        assert_eq!(RetireState::load(&path), RetireState::default());
    }
}
