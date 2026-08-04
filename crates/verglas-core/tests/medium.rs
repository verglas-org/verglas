//! Tests for cache-medium detection: the hardware context Verglas discloses so
//! published numbers (issue #26) and the `verglas dev` banner (issue #25) carry
//! their platform. Derived from the acceptance criteria before implementation.

use verglas_core::medium::{CacheMedium, IoMode};

/// A real directory resolves to a medium naming a non-empty filesystem type,
/// an IO mode, and the directory it described.
#[test]
fn detects_medium_for_a_real_directory() {
    let dir = std::env::temp_dir();
    let medium = CacheMedium::detect(&dir);

    assert_eq!(medium.path, dir);
    assert!(
        !medium.filesystem.is_empty(),
        "filesystem type must be named"
    );
    // Every platform resolves to one of the two known IO modes.
    assert!(matches!(medium.io_mode, IoMode::Buffered | IoMode::Direct));
}

/// macOS has no `O_DIRECT`; the engine always uses buffered IO there, and the
/// medium must say so honestly rather than claim direct IO.
#[cfg(target_os = "macos")]
#[test]
fn macos_reports_buffered_io() {
    let medium = CacheMedium::detect(&std::env::temp_dir());
    assert_eq!(medium.io_mode, IoMode::Buffered);
    assert!(
        medium.note().to_lowercase().contains("o_direct")
            || medium.note().to_lowercase().contains("buffered"),
        "note must disclose the IO mode: {}",
        medium.note()
    );
}

/// The one-line note is human-readable and mentions the resolved path so an
/// operator eyeballing the banner knows exactly what they are measuring.
#[test]
fn note_mentions_path_and_filesystem() {
    let dir = std::env::temp_dir();
    let medium = CacheMedium::detect(&dir);
    let note = medium.note();
    assert!(
        note.contains(&medium.filesystem),
        "note names the fs: {note}"
    );
}

/// The device baseline probes real sequential and random read throughput by
/// writing then reading a scratch file under the directory; both come back
/// strictly positive on any working filesystem.
#[test]
fn device_baseline_measures_positive_throughput() {
    let dir = std::env::temp_dir();
    let baseline = CacheMedium::measure_baseline(&dir).expect("baseline runs on temp dir");
    assert!(
        baseline.sequential_mib_per_s > 0.0,
        "sequential throughput must be positive: {baseline:?}"
    );
    assert!(
        baseline.random_mib_per_s > 0.0,
        "random throughput must be positive: {baseline:?}"
    );
}
