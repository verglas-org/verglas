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
//! admitted data; the space broker lets fragments take exactly the bytes the
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

/// Bytes a sparse file physically occupies (allocated blocks, not logical
/// length). The write-pressure accounting subtracts this from the configured
/// budget, so a truncation after a live shrink is observed immediately.
/// Never called on a hot path.
#[cfg(unix)]
pub fn file_allocated_bytes(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(path) {
        Ok(meta) => meta.blocks() * 512,
        Err(_) => 0,
    }
}

/// Non-Unix platforms cannot read allocated blocks; report the logical length
/// as allocated, which refuses new fragments rather than risking the budget.
#[cfg(not(unix))]
pub fn file_allocated_bytes(path: &Path) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
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

    /// One shared budget, both directions (#223, event-driven): the fragment
    /// ceiling is what fragments already hold plus the budget the block cache
    /// is not physically using. Blocks grow only inside the growth room;
    /// fragments admit only up to the ceiling; releases return space. Under a
    /// deterministic interleaving of scan growth, write bursts, and drain
    /// releases, combined physical usage never exceeds the shared budget —
    /// the accounting form of the hard-ceiling invariant.
    #[test]
    fn combined_usage_never_exceeds_the_budget_under_interleaving() {
        let capacity: u64 = 100_000;
        let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut step = || {
            rng ^= rng >> 12;
            rng ^= rng << 25;
            rng ^= rng >> 27;
            rng.wrapping_mul(0x2545_F491_4F6C_DD1D)
        };
        let mut blocks_phys: u64 = 0;
        let mut frag_used: u64 = 0;
        for _ in 0..10_000 {
            // The broker's ceiling formula: held bytes plus unspent budget.
            let growth_room = capacity - blocks_phys - frag_used;
            let fragment_max = frag_used + growth_room;
            match step() % 3 {
                // The scan admits blocks inside the growth room only.
                0 => {
                    let grow = (step() % 5_000).min(growth_room);
                    blocks_phys += grow;
                }
                // The write burst stages fragments up to the live ceiling.
                1 => {
                    let want = step() % 5_000;
                    frag_used += want.min(fragment_max.saturating_sub(frag_used));
                }
                // Propagation completes for some object: fragments release.
                2 => {
                    frag_used -= step() % (frag_used + 1);
                }
                _ => {}
            }
            assert!(
                blocks_phys + frag_used <= capacity,
                "combined usage {blocks_phys} + {frag_used} exceeds the {capacity}-byte budget"
            );
        }
    }
}
