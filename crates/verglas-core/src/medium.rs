//! Cache-medium detection: the hardware context Verglas discloses so its
//! numbers carry their platform.
//!
//! Warm-path performance depends on what backs the cache directory (laptop
//! SSD vs workstation NVMe vs instance-store) and on whether the OS serves the
//! cache device with `O_DIRECT` or buffered IO. Local diagnostics print this at
//! startup and the benchmarks under `benchmarks/` record it
//! alongside their results, so two runs on different machines are never
//! conflated.

use std::path::{Path, PathBuf};
use std::time::Instant;

/// How the operating system serves reads of the cache device for this path.
///
/// This reports the platform *capability*, not a per-request choice: macOS has
/// no `O_DIRECT` at all, so the cache engine there is buffered by construction;
/// on Linux an `O_DIRECT` open of a scratch file under the directory succeeds
/// on device-backed filesystems and fails on some others (older tmpfs, certain
/// network filesystems).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    /// The OS/filesystem accepts `O_DIRECT` opens on this path.
    Direct,
    /// Reads go through the page cache (macOS always; Linux where `O_DIRECT`
    /// is rejected).
    Buffered,
}

impl IoMode {
    /// The short label used in banners and JSON.
    pub fn label(self) -> &'static str {
        match self {
            IoMode::Direct => "direct",
            IoMode::Buffered => "buffered",
        }
    }
}

/// A quick, approximate read-throughput baseline of the cache device, measured
/// by writing then reading a scratch file. Reads are page-cache-warmed (we do
/// not force a cache drop), so these are an upper bound on cold-device speed —
/// enough to distinguish a laptop SSD from instance-store NVMe, not a storage
/// benchmark.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DeviceBaseline {
    /// Sequential read throughput, MiB/s, reading the scratch file end to end.
    pub sequential_mib_per_s: f64,
    /// Random read throughput, MiB/s, from fixed-size reads at random offsets.
    pub random_mib_per_s: f64,
}

/// The resolved facts about the filesystem backing a cache directory.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheMedium {
    /// The directory that was probed.
    pub path: PathBuf,
    /// The filesystem type name (`apfs`, `ext4`, `tmpfs`, …), or a hex magic
    /// when the platform reports an unrecognized one.
    pub filesystem: String,
    /// How the OS serves reads of this path's device.
    pub io_mode: IoMode,
}

impl CacheMedium {
    /// Detects the filesystem type and IO mode for `dir`. Never fails: an
    /// undetectable filesystem is reported as `"unknown"` so the banner and
    /// bench output always have a value to print.
    pub fn detect(dir: &Path) -> CacheMedium {
        CacheMedium {
            path: dir.to_path_buf(),
            filesystem: detect_filesystem(dir).unwrap_or_else(|| "unknown".to_owned()),
            io_mode: detect_io_mode(dir),
        }
    }

    /// A one-line human-readable disclosure of path, filesystem, and IO mode,
    /// naming why the mode is what it is so an operator knows what they are
    /// measuring.
    pub fn note(&self) -> String {
        let reason = match self.io_mode {
            IoMode::Direct => "O_DIRECT-capable",
            #[cfg(target_os = "macos")]
            IoMode::Buffered => "macOS has no O_DIRECT; cache uses buffered IO",
            #[cfg(not(target_os = "macos"))]
            IoMode::Buffered => "O_DIRECT rejected on this filesystem; cache uses buffered IO",
        };
        format!(
            "{} on {} — IO mode: {} ({reason})",
            self.path.display(),
            self.filesystem,
            self.io_mode.label(),
        )
    }

    /// Runs the quick read-throughput baseline under `dir`. Returns an error
    /// only if the scratch file cannot be created or read — the caller treats
    /// that as "baseline unavailable" rather than fatal.
    pub fn measure_baseline(dir: &Path) -> std::io::Result<DeviceBaseline> {
        measure_baseline(dir)
    }
}

/// Bytes of the scratch file the baseline writes: large enough to time
/// meaningfully, small enough to stay well under a second.
const BASELINE_FILE_BYTES: usize = 16 * 1024 * 1024;
/// Per-read size for the random-read phase.
const RANDOM_READ_BYTES: usize = 4096;
/// Number of random reads performed in the random phase.
const RANDOM_READ_COUNT: usize = 4096;

/// Writes a scratch file under `dir`, then times a full sequential read and a
/// batch of random-offset reads. The file is removed before returning.
fn measure_baseline(dir: &Path) -> std::io::Result<DeviceBaseline> {
    use std::io::{Read, Seek, SeekFrom, Write};

    let path = dir.join(".verglas-baseline-probe");
    // A simple non-repeating byte pattern; content is irrelevant, only IO is.
    let mut buf = vec![0u8; BASELINE_FILE_BYTES];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }

    let result = (|| {
        let mut file = std::fs::File::create(&path)?;
        file.write_all(&buf)?;
        file.sync_all()?;

        // Sequential: read the whole file back start to finish.
        let mut file = std::fs::File::open(&path)?;
        let mut sink = vec![0u8; BASELINE_FILE_BYTES];
        let seq_start = Instant::now();
        file.read_exact(&mut sink)?;
        let seq_elapsed = seq_start.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
        let sequential_mib_per_s = (BASELINE_FILE_BYTES as f64 / (1024.0 * 1024.0)) / seq_elapsed;

        // Random: fixed-size reads at deterministic pseudo-random offsets so
        // the measurement is reproducible run to run.
        let max_offset = (BASELINE_FILE_BYTES - RANDOM_READ_BYTES) as u64;
        let mut small = vec![0u8; RANDOM_READ_BYTES];
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let rand_start = Instant::now();
        for _ in 0..RANDOM_READ_COUNT {
            // xorshift* for cheap, dependency-free deterministic offsets.
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            let offset = state.wrapping_mul(0x2545_F491_4F6C_DD1D) % max_offset;
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut small)?;
        }
        let rand_elapsed = rand_start.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
        let random_bytes = (RANDOM_READ_BYTES * RANDOM_READ_COUNT) as f64;
        let random_mib_per_s = (random_bytes / (1024.0 * 1024.0)) / rand_elapsed;

        Ok(DeviceBaseline {
            sequential_mib_per_s,
            random_mib_per_s,
        })
    })();

    let _ = std::fs::remove_file(&path);
    result
}

/// Detects the filesystem type name for `dir` via `statfs`. Returns `None`
/// when the syscall fails (the caller substitutes `"unknown"`).
#[cfg(target_os = "macos")]
fn detect_filesystem(dir: &Path) -> Option<String> {
    let stat = statfs(dir)?;
    // `f_fstypename` is a NUL-padded C string like "apfs".
    let bytes: Vec<u8> = stat
        .f_fstypename
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

/// Detects the filesystem type name for `dir` by mapping the `statfs` magic
/// number to a well-known name, falling back to its hex form.
#[cfg(target_os = "linux")]
fn detect_filesystem(dir: &Path) -> Option<String> {
    let stat = statfs(dir)?;
    let magic = stat.f_type as u64;
    let name = match magic {
        0xEF53 => "ext4",
        0x5846_5342 => "xfs",
        0x9123_683E => "btrfs",
        0xF2F5_2010 => "f2fs",
        0x0102_1994 => "tmpfs",
        0x2FC1_2FC1 => "zfs",
        0x794C_7630 => "overlayfs",
        0x6969 => "nfs",
        0x0102_1997 => "v9fs",
        _ => return Some(format!("0x{magic:x}")),
    };
    Some(name.to_owned())
}

/// Neither macOS nor Linux: filesystem detection is not wired up.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn detect_filesystem(_dir: &Path) -> Option<String> {
    None
}

/// Calls `statfs(2)` on `dir`, returning the filled struct or `None` on error.
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn statfs(dir: &Path) -> Option<libc::statfs> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(dir.as_os_str().as_bytes()).ok()?;
    // SAFETY: `c_path` is a valid NUL-terminated string for the duration of
    // the call, and `stat` is a properly aligned, sized output buffer.
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    let rc = unsafe { libc::statfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc == 0 {
        // SAFETY: statfs returned success, so the buffer is initialized.
        Some(unsafe { stat.assume_init() })
    } else {
        None
    }
}

/// macOS has no `O_DIRECT`; the cache is buffered there by construction.
#[cfg(target_os = "macos")]
fn detect_io_mode(_dir: &Path) -> IoMode {
    IoMode::Buffered
}

/// Probes `O_DIRECT` support by opening a scratch file with the flag; success
/// means the device serves direct IO, an `EINVAL`/`ENOTSUP` means buffered.
#[cfg(target_os = "linux")]
fn detect_io_mode(dir: &Path) -> IoMode {
    use std::os::unix::fs::OpenOptionsExt;

    let path = dir.join(".verglas-odirect-probe");
    let opened = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(&path);
    let mode = match opened {
        Ok(_) => IoMode::Direct,
        Err(_) => IoMode::Buffered,
    };
    let _ = std::fs::remove_file(&path);
    mode
}

/// Platforms without a wired-up probe report buffered IO — the safe, honest
/// default (we never claim direct IO we cannot verify).
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn detect_io_mode(_dir: &Path) -> IoMode {
    IoMode::Buffered
}
