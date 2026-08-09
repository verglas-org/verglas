//! Evict-first demotion of retired objects (issue #198).
//!
//! When a compaction commit orphans a file, retirement (verglas-tables'
//! `RetirementScheduler`) demotes it: it stays resolvable for time travel while
//! its snapshot is retained, but its cached blocks must become **evict-first**
//! so normal pressure drops them before live data, and they must never be
//! re-admitted while demoted.
//!
//! This module holds the demotion set the engine consults. It reuses the
//! generation-epoch machinery of #178: a demoted object's block lookups resolve
//! at a **poisoned generation** ([`DEMOTED_GENERATION_BIT`]) that no live entry
//! was ever written under, so a demoted block is an unreachable miss by
//! construction — a lookup at the poisoned generation cannot find the
//! live-generation entry. The stale live-generation entry is never touched
//! again, so LRU ages it out before any entry the working set keeps warm: that
//! is the evict-first behavior, achieved with an O(1) key stamp and no
//! selective-eviction pass. On the fill path the engine consults this set to
//! skip admission, so a demoted object refills straight through the cache
//! without ever being re-admitted.
//!
//! # Hot path
//!
//! The warm read path pays exactly one relaxed atomic load ([`Demotions::any`]):
//! when nothing is demoted (the overwhelming common case) the set is never
//! consulted and key resolution is identical to before this module existed. A
//! demoted object is cold by construction (always a miss), so the sharded-set
//! lookup it pays lands only on the slow backend-fill path, never on a warm hit.

use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use verglas_core::CacheKey;

/// The high bit a demoted object's block lookups set in their generation stamp.
/// The purge generation (#178) counts up from 0, so a live entry never carries
/// this bit; OR-ing it in yields a generation no live entry was written under,
/// which is what makes a demoted block an unreachable miss.
pub(crate) const DEMOTED_GENERATION_BIT: u64 = 1 << 63;

/// The set of demoted (evict-first) objects the engine consults on the miss
/// ladder. Keyed by whole-object identity ([`CacheKey`]): retirement demotes a
/// removed *file*, so every block/ETag of that object is demoted together.
///
/// Guarded by a mutex, not a sharded lock-free map: demotion mutations are a
/// low-frequency background operation (a compaction commit demotes a batch, a
/// server sweep clears expired ones), never on the request hot path. The hot
/// path only ever reads [`Demotions::any`] (a relaxed atomic) and, when that is
/// non-zero, takes the lock for a single contains-check on the cold miss path.
pub(crate) struct Demotions {
    /// The demoted object identities.
    set: Mutex<HashSet<CacheKey>>,
    /// The current size of `set`, mirrored as an atomic so the hot path can
    /// short-circuit without taking the lock. Relaxed: an approximate gauge is
    /// fine — a demotion that is a hair late to be observed only costs one extra
    /// warm hit before the object goes cold, never a correctness violation.
    count: AtomicU64,
}

impl Demotions {
    /// An empty demotion set.
    pub(crate) fn new() -> Demotions {
        Demotions {
            set: Mutex::new(HashSet::new()),
            count: AtomicU64::new(0),
        }
    }

    /// Whether any object is currently demoted. A single relaxed atomic load:
    /// the hot-path guard that keeps warm reads paying nothing when nothing is
    /// retired.
    pub(crate) fn any(&self) -> bool {
        self.count.load(Ordering::Relaxed) != 0
    }

    /// Whether `key` is demoted. Callers gate this behind [`Demotions::any`] so
    /// the lock is only taken when something is demoted (a cold path).
    pub(crate) fn contains(&self, key: &CacheKey) -> bool {
        self.set
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(key)
    }

    /// Demotes `keys` (idempotent). After this returns, every block of each key
    /// is an unreachable miss until [`Demotions::clear`] undemotes it. Returns
    /// the keys that were *newly* demoted, so the caller's byte gauge (#305)
    /// counts each object once no matter how often a commit re-demotes it. Off
    /// the request hot path.
    pub(crate) fn demote(&self, keys: &[CacheKey]) -> Vec<CacheKey> {
        let mut set = self.set.lock().unwrap_or_else(|p| p.into_inner());
        let mut newly = Vec::new();
        for key in keys {
            if set.insert(key.clone()) {
                newly.push(key.clone());
            }
        }
        self.count.store(set.len() as u64, Ordering::Relaxed);
        newly
    }

    /// Undemotes `keys` (the hard-eviction step, run after the grace window):
    /// the object becomes cacheable again on its next read. Returns the keys
    /// that were actually demoted, so the caller's byte gauge (#305) subtracts
    /// only what demotion added. Off the hot path.
    pub(crate) fn clear(&self, keys: &[CacheKey]) -> Vec<CacheKey> {
        let mut set = self.set.lock().unwrap_or_else(|p| p.into_inner());
        let mut removed = Vec::new();
        for key in keys {
            if set.remove(key) {
                removed.push(key.clone());
            }
        }
        self.count.store(set.len() as u64, Ordering::Relaxed);
        removed
    }

    /// The number of currently-demoted objects (for metrics and tests).
    pub(crate) fn len(&self) -> usize {
        self.set.lock().unwrap_or_else(|p| p.into_inner()).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A key in a fixed bucket.
    fn key(k: &str) -> CacheKey {
        CacheKey {
            storage_binding_id: "default".to_owned(),
            bucket: "b".to_owned(),
            key: k.to_owned(),
        }
    }

    /// Demote sets membership and the atomic gauge; clear removes them.
    #[test]
    fn demote_and_clear_track_membership() {
        let d = Demotions::new();
        assert!(!d.any());
        assert!(!d.contains(&key("a")));

        d.demote(&[key("a"), key("b")]);
        assert!(d.any());
        assert_eq!(d.len(), 2);
        assert!(d.contains(&key("a")));
        assert!(!d.contains(&key("c")));

        // Idempotent: re-demoting does not double-count.
        d.demote(&[key("a")]);
        assert_eq!(d.len(), 2);

        d.clear(&[key("a")]);
        assert!(!d.contains(&key("a")));
        assert!(d.contains(&key("b")));
        assert!(d.any());

        d.clear(&[key("b")]);
        assert!(!d.any());
        assert_eq!(d.len(), 0);
    }

    /// The poisoned generation bit is above any plausible purge count, so a
    /// live entry (generation counting up from 0) never collides with it.
    #[test]
    fn poison_bit_is_above_live_generations() {
        assert_eq!(DEMOTED_GENERATION_BIT, 1u64 << 63);
        // A live generation OR the bit differs from the same live generation.
        for g in [0u64, 1, 42, 1_000_000] {
            assert_ne!(g, g | DEMOTED_GENERATION_BIT);
        }
    }
}
