//! The block model: objects are cached as fixed-size blocks, and a range read
//! maps to the covering blocks. This module owns the pure range/block
//! arithmetic the engine builds on.
//!
//! # Data-block geometry
//!
//! A configured data-block size is the unit of caching, admission, eviction,
//! and backend fills. It echoes through range-serving, the future
//! miss-ratio-curve math, and the benchmark suite, so the rationale
//! is recorded here once:
//!
//! - **Fill economics.** A cold read costs at most one origin GET of the
//!   configured block size per touched block. The 2 MiB default matched the
//!   observed 1–2 MiB DuckDB Parquet ranges in local TPC-DS SF1000: it cut
//!   origin bytes from 70.67 GB to 33.29 GB and cold wall time from 658.142 s
//!   to 329.440 s. Operators can trade request count for read amplification
//!   over the validated 1–8 MiB range.
//! - **DRAM granularity.** Smaller data blocks make the DRAM tier a finer hot
//!   set; the configured geometry is carried into every budget calculation so
//!   the hard ceiling remains exact.
//! - **Disk layout.** Foyer packs logical cache entries into larger physical
//!   eviction segments. The engine chooses that segment geometry independently
//!   so a 1 MiB logical block does not create hundreds of 1 MiB files.
//! - **Power of two.** Block index and intra-block offset are shift/mask
//!   operations, and MRC bucket math (issue #62 territory) stays exact.
//!
//! Changing `cache.data_block_bytes` reinterprets existing block indices, so
//! operators must start with an empty cache directory. This is a pre-release
//! flag day: no on-disk compatibility machinery is built (see AGENTS.md).

use std::ops::Range;

use verglas_core::config::DEFAULT_DATA_BLOCK_BYTES;
use verglas_core::read::{ReadError, ReadRange};

/// Default data-cache block size, re-exported for tests and callers that use
/// the default config. Engine paths always read `cache.data_block_bytes`.
pub const BLOCK_SIZE_BYTES: u64 = DEFAULT_DATA_BLOCK_BYTES;

/// Resolves an HTTP-shaped [`ReadRange`] against the object's total size into
/// an absolute half-open byte range, mirroring S3 semantics exactly:
///
/// - `Full` → the whole object, even when empty.
/// - `From(first)` → `first..size`; `first >= size` is unsatisfiable (416).
/// - `Bounded(first, last)` → inclusive ends, `last` clamped to the object;
///   unsatisfiable when `first >= size` or the range is inverted.
/// - `Suffix(len)` → the last `len` bytes; `len == 0` is unsatisfiable (S3
///   rejects `bytes=-0`), `len >= size` serves the whole object.
/// - Any explicit range against an empty object is unsatisfiable.
pub(crate) fn resolve_range(range: ReadRange, size: u64) -> Result<Range<u64>, ReadError> {
    match range {
        ReadRange::Full => Ok(0..size),
        ReadRange::From(first) if first < size => Ok(first..size),
        ReadRange::From(_) => Err(ReadError::InvalidRange),
        ReadRange::Bounded(first, last) if first <= last && first < size => {
            Ok(first..last.saturating_add(1).min(size))
        }
        ReadRange::Bounded(..) => Err(ReadError::InvalidRange),
        ReadRange::Suffix(len) if len > 0 && size > 0 => Ok(size.saturating_sub(len)..size),
        ReadRange::Suffix(_) => Err(ReadError::InvalidRange),
    }
}

/// Returns the inclusive `(first, last)` block indices covering a non-empty
/// absolute byte range. Callers must not pass an empty range — an empty read
/// covers no blocks and is served as an empty stream before block math runs.
pub(crate) fn covering_blocks(range: &Range<u64>, block_bytes: u64) -> (u64, u64) {
    debug_assert!(!range.is_empty());
    (range.start / block_bytes, (range.end - 1) / block_bytes)
}

/// Length in bytes of block `index` of an object of `size` bytes: a full
/// block everywhere except the final block, which is the remainder. Callers
/// only pass indices that lie within the object (guaranteed by
/// [`covering_blocks`] over a size-clamped range), so the result is never 0.
pub(crate) fn block_len(size: u64, index: u64, block_bytes: u64) -> usize {
    debug_assert!(index * block_bytes < size);
    (size - index * block_bytes).min(block_bytes) as usize
}
