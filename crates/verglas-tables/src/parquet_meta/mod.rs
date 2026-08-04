//! Parquet footer parsing: the file-internal map from byte offset to column
//! chunk.
//!
//! Every Parquet data file carries a Thrift `FileMetaData` footer at its tail
//! giving the byte range of every row group's every column chunk. This module
//! reads that footer through the [`MetadataFetch`](crate::fetch::MetadataFetch)
//! interface and flattens it into a sorted `Vec<ChunkSpan>` the mapper binary-searches
//! on the hot path. Parses are memoized by `(path, etag)` — the footer of an
//! immutable object never changes, so it is read and decoded at most once per
//! version.
//!
//! This capability is **Parquet-only** (the addendum): Avro and ORC data files
//! are never handed to this module. The mapper guards on `FileFormat` before
//! ever calling here — a footer parse is never used to *discover* whether a
//! file is Parquet.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::ParquetMetaDataReader;

use crate::fetch::{MetadataFetch, ObjectPath};
use crate::mapper::{ChunkSpan, ColumnId, MapError};

/// Reads and decodes a Parquet footer into column-chunk spans, sorted by start
/// offset. Two suffix reads: the 8-byte footer tail (length + magic), then the
/// footer body.
///
/// Off the hot path (does IO). The mapper calls this from the background
/// updater / warmer, never from `classify()`.
pub async fn parse_footer(
    fetch: &dyn MetadataFetch,
    path: &ObjectPath,
) -> Result<Vec<ChunkSpan>, MapError> {
    // The last 8 bytes hold the metadata length and the `PAR1` magic.
    let tail = fetch
        .fetch_suffix(path, FOOTER_SIZE as u64)
        .await
        .map_err(MapError::Fetch)?;
    if tail.len() < FOOTER_SIZE {
        return Err(MapError::Parquet {
            object: path.key.clone(),
            detail: format!("truncated footer tail: {} bytes", tail.len()),
        });
    }
    let mut footer = [0u8; FOOTER_SIZE];
    footer.copy_from_slice(&tail[tail.len() - FOOTER_SIZE..]);
    let footer_tail =
        ParquetMetaDataReader::decode_footer_tail(&footer).map_err(|e| MapError::Parquet {
            object: path.key.clone(),
            detail: e.to_string(),
        })?;
    let metadata_len = footer_tail.metadata_length();

    // Read the metadata bytes plus the 8-byte tail, then decode the metadata
    // portion (the tail is stripped before decoding).
    let want = (metadata_len + FOOTER_SIZE) as u64;
    let suffix = fetch
        .fetch_suffix(path, want)
        .await
        .map_err(MapError::Fetch)?;
    if suffix.len() < metadata_len + FOOTER_SIZE {
        return Err(MapError::Parquet {
            object: path.key.clone(),
            detail: format!(
                "footer read short: got {} of {} bytes",
                suffix.len(),
                metadata_len + FOOTER_SIZE
            ),
        });
    }
    let start = suffix.len() - FOOTER_SIZE - metadata_len;
    let metadata_bytes = &suffix[start..suffix.len() - FOOTER_SIZE];
    let metadata =
        ParquetMetaDataReader::decode_metadata(metadata_bytes).map_err(|e| MapError::Parquet {
            object: path.key.clone(),
            detail: e.to_string(),
        })?;

    Ok(spans_from_metadata(&metadata))
}

/// Flattens a decoded `FileMetaData` into column-chunk spans sorted by start
/// offset — the shape the mapper binary-searches. Shared by [`parse_footer`]
/// and [`parse_footer_from_tail`] so both produce identical spans.
fn spans_from_metadata(metadata: &parquet::file::metadata::ParquetMetaData) -> Vec<ChunkSpan> {
    let mut spans = Vec::new();
    for (row_group_index, row_group) in metadata.row_groups().iter().enumerate() {
        for (column_index, column) in row_group.columns().iter().enumerate() {
            let (chunk_start, chunk_len) = column.byte_range();
            spans.push(ChunkSpan {
                start: chunk_start,
                end: chunk_start + chunk_len,
                // ColumnId is the leaf-column ordinal within the row group.
                // Mapping to Iceberg field ids is a future refinement (#60).
                column: ColumnId(column_index as u32),
                row_group: row_group_index as u32,
            });
        }
    }
    spans.sort_by_key(|span| span.start);
    spans
}

/// The result of parsing a Parquet footer from a single speculative tail read.
///
/// The warming pipeline (#168) reads the last ~64 KiB of a file in one GET;
/// most footers fit, so it decodes them without a second round trip. When the
/// declared footer is larger than the window, it reports the exact byte count a
/// second read must fetch — the ≤2-GET rule from the issue.
#[derive(Debug)]
pub enum FooterFromTail {
    /// The footer fit within the tail read; the decoded, sorted spans.
    Complete(Vec<ChunkSpan>),
    /// The footer is larger than the tail read: this many trailing bytes
    /// (footer metadata plus the 8-byte trailer) must be fetched to decode it.
    NeedLarger {
        /// Total trailing bytes required for a complete footer read.
        needed: u64,
    },
}

/// Parses a Parquet footer from `tail`, the trailing bytes of the object from a
/// single speculative suffix read.
///
/// Returns [`FooterFromTail::Complete`] when the footer's declared length fits
/// inside `tail` (the common case — one GET), or [`FooterFromTail::NeedLarger`]
/// naming the exact byte count a second read must fetch. Never issues IO: the
/// caller owns the reads (and their cache side effects). Parquet-only by
/// construction — the mapper guards on `FileFormat` before a footer is ever
/// read, so this is never used to *discover* format.
pub fn parse_footer_from_tail(object: &str, tail: &[u8]) -> Result<FooterFromTail, MapError> {
    if tail.len() < FOOTER_SIZE {
        return Err(MapError::Parquet {
            object: object.to_owned(),
            detail: format!("truncated footer tail: {} bytes", tail.len()),
        });
    }
    let mut footer = [0u8; FOOTER_SIZE];
    footer.copy_from_slice(&tail[tail.len() - FOOTER_SIZE..]);
    let footer_tail =
        ParquetMetaDataReader::decode_footer_tail(&footer).map_err(|e| MapError::Parquet {
            object: object.to_owned(),
            detail: e.to_string(),
        })?;
    let metadata_len = footer_tail.metadata_length();
    let needed = metadata_len + FOOTER_SIZE;
    if tail.len() < needed {
        return Ok(FooterFromTail::NeedLarger {
            needed: needed as u64,
        });
    }
    let start = tail.len() - FOOTER_SIZE - metadata_len;
    let metadata_bytes = &tail[start..tail.len() - FOOTER_SIZE];
    let metadata =
        ParquetMetaDataReader::decode_metadata(metadata_bytes).map_err(|e| MapError::Parquet {
            object: object.to_owned(),
            detail: e.to_string(),
        })?;
    Ok(FooterFromTail::Complete(spans_from_metadata(&metadata)))
}

/// A process-wide memo of parsed footers keyed by `(path, etag)`. A parsed
/// footer is shared as an `Arc<Vec<ChunkSpan>>` so rebuilding a `TableIndex`
/// across a commit reuses it without re-reading the object.
///
/// The lock here is off the hot path — `classify()` reads the already-populated
/// `OnceLock` on `FileMeta`, never this cache.
#[derive(Default)]
pub struct FooterCache {
    /// `(key, etag) → parsed spans`.
    entries: Mutex<HashMap<FooterKey, Arc<Vec<ChunkSpan>>>>,
}

/// Memo key for a parsed footer: the object key and its ETag (version).
type FooterKey = (String, String);

impl FooterCache {
    /// A fresh, empty cache.
    pub fn new() -> FooterCache {
        FooterCache::default()
    }

    /// Returns the parsed footer for `(path, etag)`, reading and decoding it
    /// through `fetch` on the first request and serving the memoized
    /// `Arc<Vec<ChunkSpan>>` thereafter. Off the hot path.
    pub async fn get_or_parse(
        &self,
        fetch: &dyn MetadataFetch,
        path: &ObjectPath,
        etag: &str,
    ) -> Result<Arc<Vec<ChunkSpan>>, MapError> {
        let memo_key = (path.key.clone(), etag.to_owned());
        if let Some(hit) = self.lookup(&memo_key) {
            return Ok(hit);
        }
        let spans = Arc::new(parse_footer(fetch, path).await?);
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        // Another task may have parsed concurrently; keep whichever landed
        // first so all callers share one Arc.
        Ok(Arc::clone(
            entries
                .entry(memo_key)
                .or_insert_with(|| Arc::clone(&spans)),
        ))
    }

    /// Looks up a memoized parse without holding the lock across the await.
    fn lookup(&self, key: &(String, String)) -> Option<Arc<Vec<ChunkSpan>>> {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
            .cloned()
    }
}
