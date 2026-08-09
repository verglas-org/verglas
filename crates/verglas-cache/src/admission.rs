//! Scan-resistant block admission (#15): a compact frequency sketch that keeps
//! a one-touch bulk read (a full-table scan larger than the cache) from
//! evicting the frequently-reused working set.
//!
//! # Why admission, and why here
//!
//! foyer owns eviction; it does not expose a TinyLFU-style admission hook for
//! its memory tier (its `StorageFilter` sees only a hash, a size, and IO
//! statistics — no per-key frequency, no key), so the admission *decision*
//! lives at the engine's fill sites, gating every `blocks.insert`. A rejected
//! block is still served to the client — the scan's bytes flow through — it is
//! simply not cached, so it never becomes an eviction that displaces the
//! working set.
//!
//! # The policy: frequency doorkeeper under pressure
//!
//! The canonical TinyLFU rule is "admit a candidate only if its estimated
//! frequency beats the eviction victim's." foyer does not hand us the victim,
//! so we approximate the rule with the facts we *can* observe cheaply:
//!
//! 1. **Is there a victim at all?** Only once the configured NVMe cache reaches
//!    pressure does an admission evict a cached block; below that the cache is
//!    still filling cold, so every block is admitted (this keeps a working set
//!    that *fits* fully cached and preserves the pre-#15 cold-fill behavior the
//!    #12 tests pin). DRAM is the hot tier, not the cache capacity: when it is
//!    full, foyer demotes blocks to the configured NVMe tier. foyer 0.22.3
//!    exposes no *live* disk-tier residency gauge (only cumulative IO
//!    statistics), so capacity pressure is sensed by a cumulative-admitted-bytes
//!    proxy crossing a fraction ([`PRESSURE_RATIO`]) of the disk byte budget.
//!    A live disk occupancy hook is the natural upstream foyer ask; noted as a
//!    seam.
//! 2. **Is the candidate worth a victim's slot?** Under pressure, a block is
//!    admitted only if a count-min sketch estimates it has been seen at least
//!    `frequency_threshold` times (default 2). A one-touch scan block is seen
//!    exactly once, so it never clears the bar; a working-set block that keeps
//!    being re-read does.
//! 3. **Resident-biased thinning under sustained churn (#164).** The frequency
//!    gate stops a *one-touch* scan but not a **cyclic** one: a query sweeping
//!    tables larger than the cache re-touches every block each cycle, so every
//!    block clears the frequency bar by its second sighting, admission passes
//!    the whole sweep, and LRU-family eviction degenerates to ~0% hits (the
//!    classic cyclic-scan pathology, #164). So once under pressure, a candidate
//!    that clears the frequency gate is admitted only with probability
//!    `churn_admit_probability`. Thinning the sweep to a fraction `p` lets a
//!    stable resident subset survive rather than being cyclically overwritten,
//!    converging on the theoretical cyclic-scan hit ratio (≈ cache/footprint).
//!    `p = 1.0` (the default) disables the bias and recovers the pre-#164
//!    doorkeeper. The fractional admitter is deterministic (a Bresenham-style
//!    accumulator, not an RNG) so it is reproducible and lock-free.
//!
//! # Sketch sizing (reasoned, per the issue)
//!
//! The sketch is a classic count-min sketch: [`SKETCH_DEPTH`] rows of `width`
//! 4-bit-range counters (stored one per byte, saturating at 255), width a power
//! of two derived from the cache's block capacity ([`CountMinSketch::sized_for`]).
//! Count-min never *under*-counts, so a working-set key is never wrongly seen
//! as rare; collisions only *over*-count, which at worst admits a scan block
//! early, and taking the min across [`SKETCH_DEPTH`] independent rows drives
//! that probability down. Counters are periodically halved (the TinyLFU aging
//! step) once total increments reach a sampling window, so the estimate tracks
//! *recent* frequency and cannot saturate. Memory is bounded and tiny — a few
//! KiB for small caches up to a few MiB for very large ones — and fixed at
//! build time, so it is engine bookkeeping outside the block/DRAM weighters
//! (the same category as the per-entry overhead the module comment documents).
//!
//! # Hot-path discipline
//!
//! Every method here is lock-free (plain relaxed atomics). Both hits and misses
//! update the sketch, so admission reflects observed demand instead of refill
//! count. The aging sweep is O(width × depth), amortized over the sampling
//! window.

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use verglas_core::config::Admission as AdmissionConfig;

use crate::entry::BlockEntryKey;

/// Rows in the count-min sketch. Four independent hashes: the estimate is the
/// minimum across rows, so four rows make a collision inflate an estimate only
/// when it collides in *every* row — vanishingly rare at the widths we size to.
const SKETCH_DEPTH: usize = 4;

/// Fraction of the byte budget at which the cache is treated as under pressure
/// (admission starts frequency-gating). Set below 1.0 because foyer's on-disk
/// framing and its reclaimer's clean-region headroom mean live blocks start
/// being evicted before cumulative admissions reach the nominal budget; a half
/// leaves ample room for a working set that fits to stay fully cached while
/// engaging scan resistance well before the cache actually overflows.
const PRESSURE_RATIO: u64 = 2; // budget / PRESSURE_RATIO = pressure threshold.

/// Fixed-point scale for the deterministic fractional admitter: a probability
/// is carried as parts-per-million of a resident's slot. A million is finer
/// than any admission fraction worth configuring and keeps the accumulator
/// arithmetic in exact integers.
const PROBABILITY_SCALE: u64 = 1_000_000;

/// Smallest sketch width (counters per row). Floors the derived width so even a
/// tiny cache tracks enough distinct keys to tell a scan from a working set.
const MIN_SKETCH_WIDTH: usize = 4096;

/// Largest sketch width (counters per row). Caps sketch memory at
/// `SKETCH_DEPTH × MAX_SKETCH_WIDTH` bytes (4 MiB) for very large caches.
const MAX_SKETCH_WIDTH: usize = 1 << 20;

/// A lock-free count-min sketch with TinyLFU aging. Counters are bytes
/// (saturating at 255); the estimate for a key is the minimum of its counter in
/// each row. All operations use relaxed atomics — the sketch is approximate by
/// design, so a torn read across a concurrent aging sweep only ever perturbs an
/// estimate, never corrupts memory.
struct CountMinSketch {
    /// `SKETCH_DEPTH × width` counters, row-major. Boxed slice: sized once at
    /// build time, never resized.
    counters: Box<[AtomicU8]>,
    /// Counters per row; a power of two so the column index is a mask.
    width: usize,
    /// `width - 1`, the column mask.
    mask: u64,
    /// Increments since the last aging sweep.
    additions: AtomicU64,
    /// Increment count that triggers an aging sweep (halve every counter).
    sample_size: u64,
}

impl CountMinSketch {
    /// Builds a sketch sized for a cache that holds about `block_capacity`
    /// blocks: width is the block capacity times a slack factor, clamped to
    /// [`MIN_SKETCH_WIDTH`]..=[`MAX_SKETCH_WIDTH`] and rounded up to a power of
    /// two. The sampling window is ten widths — Caffeine's ratio — so aging is
    /// frequent enough to track recency without churning the counters.
    fn sized_for(block_capacity: u64) -> Self {
        let target = block_capacity
            .saturating_mul(8)
            .max(MIN_SKETCH_WIDTH as u64);
        let width = (target as usize)
            .clamp(MIN_SKETCH_WIDTH, MAX_SKETCH_WIDTH)
            .next_power_of_two();
        let counters = (0..width * SKETCH_DEPTH)
            .map(|_| AtomicU8::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        CountMinSketch {
            counters,
            width,
            mask: width as u64 - 1,
            additions: AtomicU64::new(0),
            sample_size: (width as u64) * 10,
        }
    }

    /// Column index of `hash` in row `row`, derived by double hashing (a
    /// per-row odd multiplier mixes the base hash so the rows are independent).
    fn index(&self, hash: u64, row: usize) -> usize {
        // Odd, well-mixed per-row constants (fractional bits of φ-derived
        // primes); multiplying folds the row's identity into the base hash.
        const SEEDS: [u64; SKETCH_DEPTH] = [
            0x9e37_79b9_7f4a_7c15,
            0xc2b2_ae3d_27d4_eb4f,
            0x1656_67b1_9e37_79f9,
            0xff51_afd7_ed55_8ccd,
        ];
        let mixed = hash.wrapping_mul(SEEDS[row]);
        // Fold the high bits down so distinct rows use distinct bit ranges.
        let folded = mixed ^ (mixed >> 32);
        (row * self.width) + (folded & self.mask) as usize
    }

    /// Records one access to `hash` and returns its (post-increment) estimated
    /// frequency: increments the counter in every row (saturating) and returns
    /// the minimum. Triggers an aging sweep when the sampling window fills.
    fn increment_and_estimate(&self, hash: u64) -> u32 {
        let mut min = u8::MAX;
        for row in 0..SKETCH_DEPTH {
            let counter = &self.counters[self.index(hash, row)];
            // Saturating increment via a relaxed CAS loop; contention here is
            // between cold fills only, and a lost race just drops one count
            // from an approximate sketch.
            let mut cur = counter.load(Ordering::Relaxed);
            loop {
                if cur == u8::MAX {
                    break;
                }
                match counter.compare_exchange_weak(
                    cur,
                    cur + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        cur += 1;
                        break;
                    }
                    Err(actual) => cur = actual,
                }
            }
            min = min.min(cur);
        }
        // One thread that pushes `additions` across the window runs the sweep.
        if self.additions.fetch_add(1, Ordering::Relaxed) + 1 >= self.sample_size {
            self.age();
        }
        min as u32
    }

    /// The TinyLFU aging step: halve every counter and reset the window. Halving
    /// preserves relative frequencies while letting stale keys decay, and caps
    /// counters so a long-lived hot key cannot pin the sketch. Off the warm
    /// path; amortized one sweep per [`Self::sample_size`] increments.
    fn age(&self) {
        // Reset the window first so a concurrent incrementer does not trigger a
        // second overlapping sweep; a few extra increments landing mid-sweep is
        // harmless for an approximate sketch.
        self.additions.store(0, Ordering::Relaxed);
        for counter in self.counters.iter() {
            let cur = counter.load(Ordering::Relaxed);
            if cur != 0 {
                counter.store(cur >> 1, Ordering::Relaxed);
            }
        }
    }

    /// Zeroes every counter and the window — a cold reset, used when the cache
    /// is purged so post-purge admission starts from a clean frequency slate.
    fn clear(&self) {
        self.additions.store(0, Ordering::Relaxed);
        for counter in self.counters.iter() {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

/// The scan-resistant admission policy: the frequency sketch plus the pressure
/// bookkeeping and tuning. Constructed once per engine; consulted at every data
/// block fill site (never for metadata mappings, which are always admitted).
pub(crate) struct Admission {
    /// Recent-access frequency estimator.
    sketch: CountMinSketch,
    /// Whether the policy gates at all; off admits every block (pre-#15).
    enabled: bool,
    /// Estimated frequency a block must reach to be admitted under pressure.
    threshold: u32,
    /// Byte budget above which the cache is under pressure (an admission then
    /// implies an eviction). `budget / PRESSURE_RATIO`.
    pressure_threshold_bytes: u64,
    /// Cumulative bytes admitted since the last [`Self::reset`] — the disk-tier
    /// "is there a victim" proxy (foyer exposes no live disk residency gauge).
    /// Monotonic within an epoch; never decremented, because once the cache has
    /// filled it stays full in steady state.
    admitted_bytes: AtomicU64,
    /// Probability, in parts-per-million ([`PROBABILITY_SCALE`]), that a
    /// candidate which clears the frequency gate is admitted *while under
    /// pressure* — the resident-biased thinning of a cyclic scan (#164).
    /// [`PROBABILITY_SCALE`] (1.0) disables the bias.
    admit_probability_ppm: u64,
    /// Bresenham-style accumulator for the deterministic fractional admitter:
    /// each churned candidate adds [`Self::admit_probability_ppm`]; a candidate
    /// is admitted exactly when the running sum crosses a multiple of
    /// [`PROBABILITY_SCALE`], so over `k` candidates exactly `⌊k·p⌋` are
    /// admitted — no RNG, and lock-free because `fetch_add` hands each caller a
    /// disjoint interval, so each crossing is claimed by exactly one caller.
    admit_accumulator: AtomicU64,
}

impl Admission {
    /// Builds the policy from config and the cache's disk byte budget (the
    /// whole-cache ceiling — blocks ultimately live on disk, DRAM holds a hot
    /// subset). The sketch is sized to the number of blocks that budget holds.
    pub(crate) fn new(config: &AdmissionConfig, disk_budget_bytes: u64, block_bytes: u64) -> Self {
        let block_capacity = (disk_budget_bytes / block_bytes).max(1);
        Admission {
            sketch: CountMinSketch::sized_for(block_capacity),
            enabled: config.enabled,
            threshold: config.frequency_threshold,
            pressure_threshold_bytes: (disk_budget_bytes / PRESSURE_RATIO).max(1),
            admitted_bytes: AtomicU64::new(0),
            admit_probability_ppm: probability_to_ppm(config.churn_admit_probability),
            admit_accumulator: AtomicU64::new(0),
        }
    }

    /// Records a fill of `key` (weight `weight` bytes) and decides whether it
    /// should be admitted to the configured cache capacity. Cold path only.
    ///
    /// Always records the access (so frequency accumulates across misses, which
    /// is what lets a repeatedly-read working-set block clear the bar even after
    /// a transient eviction). Admits unconditionally while the policy is off or
    /// the cache is not yet under pressure; otherwise the candidate must clear
    /// the frequency gate *and* win the resident-biased probabilistic draw
    /// (#164). Updates the cumulative admitted-bytes proxy on admission.
    pub(crate) fn admit(&self, key: &BlockEntryKey, weight: u64) -> bool {
        if !self.enabled {
            self.admitted_bytes.fetch_add(weight, Ordering::Relaxed);
            return true;
        }
        let frequency = self.sketch.increment_and_estimate(hash_key(key));
        let admit = if !self.under_pressure() {
            true
        } else {
            // One-touch scans die at the frequency gate; a cyclic scan clears it
            // by its second sighting, so the probabilistic draw is what thins it
            // to a stable resident subset.
            frequency >= self.threshold && self.churn_admit()
        };
        if admit {
            self.admitted_bytes.fetch_add(weight, Ordering::Relaxed);
        }
        admit
    }

    /// Records a successful DRAM or NVMe lookup. Foyer's W-TinyLFU records the
    /// same access for DRAM victim comparison; this sketch also carries that
    /// heat into the outer disk-admission gate.
    pub(crate) fn record_hit(&self, key: &BlockEntryKey) {
        if self.enabled {
            self.sketch.increment_and_estimate(hash_key(key));
        }
    }

    /// Records a partial-read candidate and returns whether it has demonstrated
    /// enough reuse to justify aligned background overfetch. This doorkeeper is
    /// independent of occupancy: user-requested exact bytes flow on first
    /// sight, while only repeated blocks spend extra origin bandwidth.
    pub(crate) fn repeated(&self, key: &BlockEntryKey) -> bool {
        !self.enabled || self.sketch.increment_and_estimate(hash_key(key)) >= self.threshold
    }

    /// Whether an admission now would evict a cached block from the configured
    /// NVMe capacity. A full DRAM tier is not pressure: it demotes blocks into
    /// the still-available NVMe tier.
    fn under_pressure(&self) -> bool {
        self.admitted_bytes.load(Ordering::Relaxed) >= self.pressure_threshold_bytes
    }

    /// The deterministic fractional admitter (#164): returns `true` for a
    /// `p`-fraction of calls, spread evenly, where `p = admit_probability_ppm /
    /// PROBABILITY_SCALE`. `p = 1.0` short-circuits to always-admit. Otherwise
    /// each call advances a shared accumulator by `admit_probability_ppm` and
    /// admits exactly when the sum steps across a multiple of
    /// [`PROBABILITY_SCALE`] — so `k` calls admit `⌊k·p⌋`, with no lock and no
    /// RNG. (The accumulator wraps at `u64::MAX`, which is not a multiple of the
    /// scale; the one mis-step per ~1.8e13 admissions is immaterial for an
    /// approximate admission policy — noted as the arithmetic's one seam.)
    fn churn_admit(&self) -> bool {
        if self.admit_probability_ppm >= PROBABILITY_SCALE {
            return true;
        }
        let prev = self
            .admit_accumulator
            .fetch_add(self.admit_probability_ppm, Ordering::Relaxed);
        (prev % PROBABILITY_SCALE) + self.admit_probability_ppm >= PROBABILITY_SCALE
    }

    /// Resets the pressure proxy, frequency sketch, and probabilistic-admitter
    /// accumulator to cold — called when the cache is purged (#138) so a
    /// repeat-cold leg starts admitting freely again, exactly as a fresh server
    /// would.
    pub(crate) fn reset(&self) {
        self.admitted_bytes.store(0, Ordering::Relaxed);
        self.admit_accumulator.store(0, Ordering::Relaxed);
        self.sketch.clear();
    }
}

/// Converts a configured probability in `(0, 1]` to parts-per-million
/// ([`PROBABILITY_SCALE`]), rounding to the nearest and clamping into
/// `1..=PROBABILITY_SCALE` so a validated config never yields a zero (admit
/// nothing) or an over-unity scale. Config validation already rejects values
/// outside `(0, 1]`; the clamp is defence in depth for a directly-built policy.
fn probability_to_ppm(probability: f64) -> u64 {
    let scaled = (probability * PROBABILITY_SCALE as f64).round();
    (scaled as u64).clamp(1, PROBABILITY_SCALE)
}

/// Hashes a block key to the 64-bit value the sketch indexes on. Uses the
/// standard library's default hasher — allocation-free and good enough for a
/// frequency sketch (only the low bits, well mixed, are used).
fn hash_key(key: &BlockEntryKey) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::BLOCK_SIZE_BYTES;
    use verglas_core::{BlockKey, CacheKey};

    /// Builds a block key for object `name`, block `index`, at a fixed ETag.
    fn block_key(name: &str, index: u64) -> BlockEntryKey {
        BlockEntryKey {
            block: BlockKey {
                object: CacheKey {
                    bucket: "b".into(),
                    key: name.into(),
                },
                etag: "e".into(),
                block_bytes: BLOCK_SIZE_BYTES,
                block_index: index,
            },
            generation: 0,
        }
    }

    /// Admits `key` using the configured cache-capacity pressure proxy, the
    /// shape most of the #15 tests exercise.
    fn admit(policy: &Admission, key: &BlockEntryKey) -> bool {
        policy.admit(key, BLOCK_SIZE_BYTES)
    }

    /// A disabled policy admits every block and never consults the sketch.
    #[test]
    fn disabled_admits_everything() {
        let cfg = AdmissionConfig {
            enabled: false,
            ..AdmissionConfig::default()
        };
        let policy = Admission::new(&cfg, 8 * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
        for _ in 0..100 {
            assert!(admit(&policy, &block_key("scan", 0)));
        }
    }

    /// Below the pressure threshold every first-touch block is admitted, so a
    /// working set that fits fills the cache cold exactly as before #15.
    #[test]
    fn admits_first_touch_until_under_pressure() {
        let cfg = AdmissionConfig::default();
        // Budget of 8 blocks → pressure at 4 blocks' worth of admissions.
        let policy = Admission::new(&cfg, 8 * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
        for i in 0..4 {
            assert!(
                admit(&policy, &block_key("ws", i)),
                "block {i} below pressure must be admitted on first touch"
            );
        }
        // Now at the pressure threshold: a fresh one-touch block is rejected.
        assert!(
            !admit(&policy, &block_key("scan", 99)),
            "a first-touch block under pressure is rejected"
        );
    }

    /// Under pressure, a block re-read enough times clears the frequency bar
    /// and is admitted (at the default `p = 1.0`), while a one-touch block never
    /// is.
    #[test]
    fn under_pressure_admits_only_repeated_keys() {
        let cfg = AdmissionConfig::default();
        let policy = Admission::new(&cfg, 8 * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
        // Drive the cache under pressure with cold fills.
        for i in 0..4 {
            admit(&policy, &block_key("fill", i));
        }
        // A one-touch key: seen once, below threshold 2, rejected.
        assert!(!admit(&policy, &block_key("scan", 1)));
        // A hot key: first sighting rejected, second sighting admitted.
        assert!(!admit(&policy, &block_key("hot", 1)));
        assert!(admit(&policy, &block_key("hot", 1)));
    }

    /// A pure one-touch scan of many distinct blocks admits (almost) none of
    /// them once the cache is under pressure — the core scan-resistance
    /// property, isolated from the engine.
    #[test]
    fn one_touch_scan_is_mostly_rejected() {
        let cfg = AdmissionConfig::default();
        let policy = Admission::new(&cfg, 8 * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
        // Warm to pressure.
        for i in 0..4 {
            admit(&policy, &block_key("ws", i));
        }
        let mut admitted = 0;
        for i in 0..1000 {
            if admit(&policy, &block_key("scan", i)) {
                admitted += 1;
            }
        }
        assert_eq!(admitted, 0, "no one-touch scan block should be admitted");
    }

    /// A full DRAM tier does not put a hybrid cache under admission pressure
    /// while the configured NVMe capacity remains available. DRAM is the hot
    /// tier, so foyer must demote a first-touch block rather than reject it.
    #[test]
    fn spare_nvme_capacity_admits_when_dram_is_full() {
        let cfg = AdmissionConfig::default(); // p = 1.0
        // Disk budget so large the byte proxy never trips inside this test.
        let policy = Admission::new(&cfg, 1_000_000 * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
        // DRAM is intentionally absent from the policy: it is a hot-tier
        // budget, while this test has ample configured NVMe capacity.
        assert!(
            policy.admit(&block_key("cold", 0), BLOCK_SIZE_BYTES),
            "spare NVMe capacity admits a first-touch block"
        );
        // Memory tier full: the candidate must be admitted into the configured
        // NVMe cache instead of treating the hot tier as the whole cache.
        assert!(
            policy.admit(&block_key("scan", 0), BLOCK_SIZE_BYTES),
            "spare NVMe capacity admits a one-touch block despite full DRAM"
        );
    }

    /// Resident-biased thinning (#164): under sustained pressure a *cyclic*
    /// scan — every block re-touched, so all clear the frequency gate — is
    /// admitted only at the configured fraction, deterministically. With
    /// `p = 0.1`, exactly one in ten qualifying candidates is admitted.
    #[test]
    fn churn_probability_thins_cyclic_scan() {
        let cfg = AdmissionConfig {
            churn_admit_probability: 0.1,
            ..AdmissionConfig::default()
        };
        // Budget of 8 blocks → pressure after 4 blocks' worth of admissions.
        let policy = Admission::new(&cfg, 8 * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
        for i in 0..4 {
            admit(&policy, &block_key("fill", i));
        }
        // First cycle: every key seen once (freq 1 < 2) → all rejected at the
        // frequency gate, the probabilistic draw never consulted.
        for i in 0..100 {
            assert!(
                !admit(&policy, &block_key("cyc", i)),
                "freq-1 pass rejected"
            );
        }
        // Second cycle: every key now clears the frequency gate, so the draw
        // governs — exactly ⌊100 · 0.1⌋ = 10 are admitted.
        let admitted = (0..100)
            .filter(|i| admit(&policy, &block_key("cyc", *i)))
            .count();
        assert_eq!(
            admitted, 10,
            "p=0.1 must admit exactly a tenth of the cyclic sweep, got {admitted}"
        );
    }

    /// The default `p = 1.0` disables the resident bias: under pressure every
    /// candidate that clears the frequency gate is admitted (pre-#164 behavior).
    #[test]
    fn probability_one_admits_every_qualifying_candidate() {
        let cfg = AdmissionConfig::default();
        let policy = Admission::new(&cfg, 8 * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
        for i in 0..4 {
            admit(&policy, &block_key("fill", i));
        }
        // Prime each key to freq 2, then confirm the second sighting admits.
        for i in 0..50 {
            let k = block_key("cyc", i);
            assert!(
                !admit(&policy, &k),
                "first sighting under pressure rejected"
            );
            assert!(admit(&policy, &k), "p=1.0 admits every second sighting");
        }
    }

    /// Reset returns the policy to admitting first-touch blocks (post-purge).
    #[test]
    fn reset_restores_cold_admission() {
        let cfg = AdmissionConfig::default();
        let policy = Admission::new(&cfg, 8 * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
        for i in 0..8 {
            admit(&policy, &block_key("fill", i));
        }
        assert!(!admit(&policy, &block_key("cold", 0)));
        policy.reset();
        assert!(
            admit(&policy, &block_key("cold", 0)),
            "after reset the cache is cold again and admits first touch"
        );
    }

    /// Background overfetch starts only after the default two-touch gate.
    #[test]
    fn repeated_gate_requires_two_touches_by_default() {
        let policy = Admission::new(
            &AdmissionConfig::default(),
            8 * BLOCK_SIZE_BYTES,
            BLOCK_SIZE_BYTES,
        );
        let key = block_key("partial", 0);
        assert!(!policy.repeated(&key));
        assert!(policy.repeated(&key));
    }

    /// The fixed-point conversion rounds to the nearest part-per-million and
    /// clamps a directly-built policy into `1..=PROBABILITY_SCALE`.
    #[test]
    fn probability_to_ppm_rounds_and_clamps() {
        assert_eq!(probability_to_ppm(1.0), PROBABILITY_SCALE);
        assert_eq!(probability_to_ppm(0.1), 100_000);
        assert_eq!(probability_to_ppm(0.5), 500_000);
        // A vanishing probability clamps to one part rather than zero.
        assert_eq!(probability_to_ppm(1e-9), 1);
    }

    /// The aging sweep halves counters and cannot panic on a saturated sketch.
    #[test]
    fn aging_halves_counters() {
        let sketch = CountMinSketch::sized_for(1);
        // Hammer one key past the sampling window to force at least one sweep.
        let mut last = 0;
        for _ in 0..sketch.sample_size + 10 {
            last = sketch.increment_and_estimate(0xabcd);
        }
        // After a halving the estimate is well below the raw number of hits.
        assert!(last < 255, "counters must not saturate under aging");
    }
}
