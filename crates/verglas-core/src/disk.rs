//! Free-space probing and the runtime disk decisions (#96, #223).
//!
//! Two concerns share this module. Disk safety (#96): at startup the config
//! refuses a `cache.capacity_bytes` larger than the free space here (see
//! [`free_bytes`] and `Config::validate`), and at runtime a background poll
//! pauses cache admission when the filesystem itself nears full. Budget sharing
//! (#223): there is **one** NVMe budget — `cache.capacity_bytes` — shared first
//! come, first served by the block cache, the metadata store, and the
//! write-back fragment store and durable KV log. There is no carve and no
//! fraction; enforcement is accounting. KV and acknowledged write-back bytes
//! are non-evictable; every other cached byte remains heat-managed. The foyer
//! stores get the full budget as their *logical* capacity
//! but their device files are sparse, so their *physical* usage grows only with
//! admitted data; [`disk_decision`] lets fragments take exactly the bytes the
//! block cache has not physically grown into, and pauses block admission before
//! it would grow into bytes protected durable state already holds. The server
//! aggregates KV and fragment usage for this decision, then translates the
//! returned grant back into a fragment-only ceiling. The poll runs off every
//! hot path; the serve path only reads the resulting atomics.

use std::path::Path;

/// Bytes free on the filesystem backing `dir`, as an unprivileged writer sees
/// them (`statvfs`/`statfs` available blocks). `None` when the platform has no
/// probe wired up or the syscall fails — the caller then does not gate on free
/// space (an unknown free figure never refuses a start or pauses caching; wrong
/// is worse than slow). Never called on a hot path.
pub fn free_bytes(dir: &Path) -> Option<u64> {
    free_bytes_impl(dir)
}

/// Real `statfs`-backed implementation on macOS and Linux: available blocks
/// times the fragment size.
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn free_bytes_impl(dir: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(dir.as_os_str().as_bytes()).ok()?;
    // SAFETY: `c_path` is a valid NUL-terminated string for the call, and `stat`
    // is a properly aligned, sized output buffer.
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    let rc = unsafe { libc::statfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: statfs returned success, so the buffer is initialized.
    let stat = unsafe { stat.assume_init() };
    // `f_bavail` is blocks available to a non-root writer; `f_bsize` is the
    // transfer block size. Both are the honest "how much can I still write". The
    // integer types differ by platform (macOS `f_bsize` is u32, Linux is a signed
    // long), so the widening cast is a no-op on one target and real on another —
    // hence the local allow.
    #[allow(clippy::unnecessary_cast)]
    let bavail = stat.f_bavail as u64;
    #[allow(clippy::unnecessary_cast)]
    let bsize = stat.f_bsize as u64;
    bavail.checked_mul(bsize)
}

/// Platforms without a wired-up probe report `None`, so free-space gating is
/// simply disabled there rather than guessing.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn free_bytes_impl(_dir: &Path) -> Option<u64> {
    None
}

/// Physical bytes already held by files under `dir`, recursively (#298). The
/// startup capacity gate adds this to the filesystem's free space: on restart
/// a warm cache's own files are what consumed the disk, and a budget the
/// server booted with cold must still validate warm. Sums allocated blocks
/// (`st_blocks * 512`), not logical lengths — the foyer device files are
/// sparse and the budget tracks physical usage. Best-effort: unreadable
/// entries are skipped (a file vanishing mid-walk must not fail a boot), and
/// symlinks are not followed. Startup-only, never called on a hot path.
pub fn allocated_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total: u64 = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.is_dir() {
            total = total.saturating_add(allocated_bytes(&path));
        } else if meta.is_file() {
            total = total.saturating_add(physical_size(&meta));
        }
    }
    total
}

/// Allocated on-disk size of one file: blocks on Unix (sparse-aware), logical
/// length elsewhere (no better probe exists there, and those platforms do not
/// gate on free space anyway).
#[cfg(unix)]
fn physical_size(meta: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    // `st_blocks` is always in 512-byte units, independent of the
    // filesystem's block size.
    meta.blocks().saturating_mul(512)
}

/// Non-Unix fallback: logical length.
#[cfg(not(unix))]
fn physical_size(meta: &std::fs::Metadata) -> u64 {
    meta.len()
}

/// The tuning that turns the poll's readings into runtime decisions (#96/#223).
/// Both fields are bytes, derived by the server from `cache.capacity_bytes`.
/// The same marks gate the filesystem's free space and the shared budget's
/// headroom — one reserve, one hysteresis band, no extra knobs.
#[derive(Debug, Clone, Copy)]
pub struct DiskParams {
    /// Headroom low-water mark. Below it, block admission pauses and the
    /// fragment store gets no new room — the node stops adding to the disk. It
    /// also bounds how far foyer's in-flight flush pipeline can overshoot
    /// between polls, so it must comfortably exceed one poll interval's worth
    /// of flush writes.
    pub low_water: u64,
    /// Headroom high-water mark. Admission resumes only once headroom climbs
    /// back above it. The gap to `low_water` is the hysteresis band, so a disk
    /// hovering at the edge does not flap admission on and off.
    pub high_water: u64,
}

/// The outcome the background poll publishes for the hot paths to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskState {
    /// When true, the cache stops admitting new blocks and serves from what is
    /// resident plus origin fills. Reads never fail; the disk simply stops
    /// growing.
    pub caching_paused: bool,
    /// The current ceiling the fragment store enforces: how many total fragment
    /// bytes it may hold right now — what it already holds plus the shared
    /// budget headroom the block cache is not using. Never below `frag_used`:
    /// an acked fragment is never dropped, a spent budget only refuses *new*
    /// fragments (the write degrades to write-through).
    pub fragment_max: u64,
}

/// Decides the runtime disk state from the poll's latest readings (#96/#223).
///
/// `free` is [`free_bytes`] (None when unprobeable). `foyer_growth_room` is the
/// block+metadata stores' logical capacity minus their physical (allocated)
/// bytes — how much more of the shared budget they can still consume; at their
/// plateau they recycle regions in place and it stays zero. `frag_used` is the
/// fragment store's current bytes; `was_paused` the previous decision (for
/// hysteresis).
///
/// First come, first served over one budget:
/// - Shared headroom is `foyer_growth_room - frag_used` — the budget bytes
///   nobody holds yet. Fragments may take all of it (`fragment_max`), bounded
///   also by the real free space above the reserve; when it is spent,
///   `fragment_max == frag_used`, so new fragment reservations are refused and
///   write-back degrades to write-through — never an error, and stored
///   fragments are never dropped.
/// - Block admission pauses (with hysteresis) when fragments hold most of the
///   remaining growth room, so the block cache never grows into bytes fragments
///   already own. With no fragments the budget is foyer's alone and admission
///   never pauses on budget grounds — a full read cache recycles in place.
/// - The #96 filesystem gate runs alongside: free space below the low-water
///   mark pauses admission and stops new fragments regardless of budget
///   accounting (a shared disk can fill from outside). Unknown free space never
///   gates — wrong is worse than slow.
pub fn disk_decision(
    free: Option<u64>,
    foyer_growth_room: u64,
    frag_used: u64,
    was_paused: bool,
    params: &DiskParams,
) -> DiskState {
    // #96: the filesystem itself running out of space, budget aside. The gate
    // only applies while the device files can still grow the filesystem
    // footprint (#300): once they hold their full logical size physically —
    // preallocated at boot, or a sparse file at its plateau — admission
    // recycles bytes the files already own and cannot fill the disk, so low
    // free space (typically caused by the preallocation itself) must not stop
    // the cache from caching. New fragments still consume real disk and stay
    // gated through `fs_headroom` below.
    let fs_paused = foyer_growth_room > 0
        && match free {
            None => false,
            Some(f) if f < params.low_water => true,
            Some(f) if f >= params.high_water => false,
            Some(_) => was_paused,
        };
    // #223: the shared budget. Only fragment-held bytes can push the combined
    // total over the budget (foyer alone is bounded by its own logical
    // capacity), so with no fragments there is nothing to guard.
    let shared_headroom = foyer_growth_room.saturating_sub(frag_used);
    let budget_paused = if frag_used == 0 {
        false
    } else if shared_headroom < params.low_water {
        true
    } else if shared_headroom >= params.high_water {
        false
    } else {
        was_paused
    };
    let fs_headroom = free.map_or(u64::MAX, |f| f.saturating_sub(params.low_water));
    let fragment_max = frag_used.saturating_add(shared_headroom.min(fs_headroom));
    DiskState {
        caching_paused: fs_paused || budget_paused,
        fragment_max,
    }
}

/// The physical growth room of one sparse device file: its logical length minus
/// its allocated bytes (`st_blocks` × 512). Zero for a missing file or one that
/// has physically filled its logical size. This is the unit of the shared
/// budget's accounting (#223): a foyer device consumes real disk only as it
/// admits data, so `logical - physical` is the budget it has not yet claimed.
/// Never called on a hot path — the background poll reads it once a tick.
#[cfg(unix)]
pub fn file_growth_room(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(path) {
        Ok(meta) => meta.len().saturating_sub(meta.blocks() * 512),
        Err(_) => 0,
    }
}

/// Non-Unix platforms cannot read allocated blocks; report zero growth room,
/// which refuses new fragments rather than risking the budget (a supported
/// platform probe is trivial to add when one is targeted).
#[cfg(not(unix))]
pub fn file_growth_room(_path: &Path) -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A parameter set with a clear hysteresis band.
    fn params() -> DiskParams {
        DiskParams {
            low_water: 100,
            high_water: 200,
        }
    }

    /// Free space on a real temp directory probes to some positive figure.
    #[test]
    fn free_bytes_reports_positive_on_temp_dir() {
        let free = free_bytes(&std::env::temp_dir());
        // On the supported platforms this is Some and non-zero; on an
        // unsupported platform it is None and gating is simply off.
        if let Some(bytes) = free {
            assert!(bytes > 0, "a writable temp dir must report free space");
        }
    }

    /// `allocated_bytes` sums physical allocation recursively and counts a
    /// sparse file at its allocated size, not its logical length (#298) — the
    /// foyer device files are sparse and the budget tracks physical usage.
    #[test]
    #[cfg(unix)]
    fn allocated_bytes_is_sparse_aware_and_recursive() {
        let dir = std::env::temp_dir().join(format!("verglas-disk-alloc-{}", std::process::id()));
        let sub = dir.join("nested");
        std::fs::create_dir_all(&sub).expect("create test dirs");
        // A dense 1 MiB file in a subdirectory: fully allocated.
        let dense = vec![7u8; 1024 * 1024];
        std::fs::write(sub.join("dense"), &dense).expect("write dense file");
        // A 1 GiB logical file with no written bytes: sparse on every
        // supported filesystem, so its allocation is far below its length.
        let sparse_len: u64 = 1 << 30;
        let sparse = std::fs::File::create(dir.join("sparse")).expect("create sparse file");
        sparse.set_len(sparse_len).expect("extend sparse file");
        drop(sparse);

        let total = allocated_bytes(&dir);
        assert!(
            total >= dense.len() as u64,
            "the dense file in the subdirectory must be counted: {total}"
        );
        assert!(
            total < sparse_len / 2,
            "a hole-only sparse file must count near zero, not its {sparse_len}-byte length: {total}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A missing directory contributes zero rather than failing the caller —
    /// the startup gate must never refuse a boot over an unreadable walk.
    #[test]
    fn allocated_bytes_of_missing_dir_is_zero() {
        let missing = std::env::temp_dir().join("verglas-disk-alloc-definitely-missing");
        assert_eq!(allocated_bytes(&missing), 0);
    }

    /// One shared budget, first come first serve (#223): with the block cache
    /// grown into little of its logical capacity, fragments may take everything
    /// the blocks are not using — the whole growth room, not a fraction of it.
    #[test]
    fn fragments_can_use_headroom_blocks_are_not_using() {
        // Blocks have 10_000 bytes of un-grown room; no fragments yet.
        let state = disk_decision(Some(u64::MAX / 2), 10_000, 0, false, &params());
        assert!(!state.caching_paused);
        assert_eq!(
            state.fragment_max, 10_000,
            "fragments may take all the room blocks are not using"
        );
    }

    /// A write burst that consumes the shared headroom pauses block admission
    /// (blocks would otherwise grow into bytes fragments already hold) and caps
    /// fragments at what they hold — the next write degrades to write-through,
    /// never an error, and no stored fragment is dropped.
    #[test]
    fn exhausted_shared_budget_pauses_blocks_and_freezes_fragments() {
        // Fragments hold 9_950 of the 10_000-byte growth room: headroom 50 is
        // below the low-water margin.
        let state = disk_decision(Some(u64::MAX / 2), 10_000, 9_950, false, &params());
        assert!(
            state.caching_paused,
            "blocks pause before growing into fragment-held bytes"
        );
        assert_eq!(
            state.fragment_max, 10_000,
            "fragments keep what they hold plus the last sliver, no more"
        );
    }

    /// Propagation frees fragments and the blocks reclaim the space: the pause
    /// clears and admission resumes — sharing works in both directions.
    #[test]
    fn blocks_reclaim_space_after_propagation_frees_fragments() {
        // Paused under fragment pressure...
        let full = disk_decision(Some(u64::MAX / 2), 10_000, 9_950, true, &params());
        assert!(full.caching_paused);
        // ...then propagation deletes the fragments: unpaused immediately.
        let freed = disk_decision(Some(u64::MAX / 2), 10_000, 0, true, &params());
        assert!(
            !freed.caching_paused,
            "admission resumes once fragments propagate"
        );
        assert_eq!(freed.fragment_max, 10_000);
    }

    /// A block cache at its physical plateau (zero growth room) with no
    /// fragments never pauses — foyer recycles its regions in place, so there is
    /// nothing to guard — and new fragments are refused (write-through): blocks
    /// came first and the budget is spent.
    #[test]
    fn read_plateau_never_pauses_and_refuses_new_fragments() {
        let state = disk_decision(Some(u64::MAX / 2), 0, 0, false, &params());
        assert!(
            !state.caching_paused,
            "a full read cache recycles in place; admission never pauses"
        );
        assert_eq!(
            state.fragment_max, 0,
            "a spent budget refuses new fragments — write-back degrades to write-through"
        );
    }

    /// Budget-side hysteresis: once paused under fragment pressure, headroom
    /// inside the band stays paused; only clearing the high-water mark resumes.
    #[test]
    fn budget_pause_clears_only_above_high_water() {
        let mid = disk_decision(Some(u64::MAX / 2), 10_000, 9_850, true, &params());
        assert!(mid.caching_paused, "headroom 150 is inside the band");
        let recovered = disk_decision(Some(u64::MAX / 2), 10_000, 9_800, true, &params());
        assert!(!recovered.caching_paused, "headroom 200 clears the band");
    }

    /// The #96 filesystem gate is unchanged: free space below the low-water mark
    /// pauses caching and stops new fragments even when the budget has headroom
    /// (a shared disk filled by someone else), and an unknown free reading never
    /// gates.
    #[test]
    /// A fully preallocated device cannot be fs-paused (#300): with zero
    /// growth room, admission recycles bytes the files already own and cannot
    /// consume filesystem space — the scarce free space (which the
    /// preallocation itself caused) must not stop the cache from caching.
    fn preallocated_device_cannot_be_fs_paused() {
        let state = disk_decision(Some(40), 0, 0, false, &params());
        assert!(
            !state.caching_paused,
            "zero growth room means admission cannot fill the disk"
        );
        assert_eq!(state.fragment_max, 0, "no headroom for new fragments");
        // Hysteresis must not trap it either: a node paused under the old
        // reading unpauses once the device is fully preallocated.
        let stuck = disk_decision(Some(40), 0, 0, true, &params());
        assert!(!stuck.caching_paused, "a preallocated device unpauses");
    }

    #[test]
    fn fs_free_space_still_gates_independently() {
        let full_disk = disk_decision(Some(40), 10_000, 250, false, &params());
        assert!(full_disk.caching_paused, "paused below fs low water");
        assert_eq!(
            full_disk.fragment_max, 250,
            "no new fragment room on a full disk, stored fragments kept"
        );

        let unknown = disk_decision(None, 10_000, 250, true, &params());
        assert!(!unknown.caching_paused, "unknown free space never pauses");
        assert_eq!(
            unknown.fragment_max, 10_000,
            "unknown free space leaves the budget accounting in charge"
        );
    }

    /// #96 hysteresis on the filesystem side is unchanged: inside the band the
    /// previous decision holds; above the high-water mark caching resumes.
    #[test]
    fn fs_pause_clears_only_above_high_water() {
        let mid = disk_decision(Some(150), 10_000, 0, true, &params());
        assert!(mid.caching_paused, "still paused inside the band");
        let recovered = disk_decision(Some(200), 10_000, 0, true, &params());
        assert!(!recovered.caching_paused, "resumes above high water");
    }

    /// Simulated interleaving: blocks grow only while unpaused (by at most the
    /// low-water margin per tick — the poll's staleness bound) and fragments
    /// reserve only up to `fragment_max`. Combined physical usage never exceeds
    /// the shared budget. This is the accounting form of the hard-ceiling
    /// invariant under a concurrent write burst plus scan.
    #[test]
    fn combined_usage_never_exceeds_the_budget_under_interleaving() {
        let capacity: u64 = 100_000; // the block cache's logical size
        let p = params();
        // Deterministic pseudo-random interleaving.
        let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut step = || {
            rng ^= rng >> 12;
            rng ^= rng << 25;
            rng ^= rng >> 27;
            rng.wrapping_mul(0x2545_F491_4F6C_DD1D)
        };
        let mut blocks_phys: u64 = 0; // scan filling the read cache
        let mut frag_used: u64 = 0; // write burst staging fragments
        let mut paused = false;
        for _ in 0..10_000 {
            let growth_room = capacity - blocks_phys;
            let state = disk_decision(Some(u64::MAX / 2), growth_room, frag_used, paused, &p);
            paused = state.caching_paused;
            match step() % 3 {
                // The scan admits blocks: physical growth, gated by the pause
                // and bounded per tick by the low-water margin.
                0 if !paused => {
                    let grow = (step() % p.low_water).min(capacity - blocks_phys);
                    blocks_phys += grow;
                }
                // The write burst stages fragments up to the live ceiling.
                1 => {
                    let want = step() % 5_000;
                    let admit = want.min(state.fragment_max.saturating_sub(frag_used));
                    frag_used += admit;
                }
                // Propagation completes for some object: fragments release.
                2 => {
                    frag_used -= step() % (frag_used + 1);
                }
                _ => {}
            }
            assert!(
                blocks_phys + frag_used <= capacity,
                "combined usage {} + {} exceeds the {capacity}-byte budget",
                blocks_phys,
                frag_used
            );
        }
    }
}
