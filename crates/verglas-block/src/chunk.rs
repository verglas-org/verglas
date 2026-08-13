//! Content addressing and chunk geometry for the block-device store.
//!
//! A device is a flat byte range cut into fixed [`CHUNK_SIZE_BYTES`] chunks.
//! Each stored chunk is named by the SHA-256 of its bytes ([`ChunkHash`]), so
//! two chunks with identical content share one name and therefore one backend
//! object — dedup across devices and versions falls out of the addressing.
//! Unwritten ranges are never stored: a device version maps such a chunk index
//! to "no chunk", which reads back as zeros.

use sha2::{Digest, Sha256};

/// The fixed chunk size: 2 MiB, aligned with the cache's block unit (#382).
/// Every offset/length calculation in this crate is in terms of this constant;
/// it is not configurable (prototype: one size, no size classes).
pub const CHUNK_SIZE_BYTES: u64 = 2 * 1024 * 1024;

/// The backend key prefix every chunk object lives under. Chunks are pooled
/// across all devices in this one prefix so identical content dedups globally.
pub const CHUNK_PREFIX: &str = "blocks";

/// The SHA-256 content address of one chunk's bytes. Two byte-identical chunks
/// hash equal, so the store writes and reads them as a single object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChunkHash([u8; 32]);

impl ChunkHash {
    /// Hashes `bytes` into its content address.
    pub fn of(bytes: &[u8]) -> ChunkHash {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        ChunkHash(hasher.finalize().into())
    }

    /// The lowercase hex rendering of the digest (64 characters). This is the
    /// stable, filesystem- and object-key-safe name of the chunk.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parses a 64-character lowercase-hex digest back into a [`ChunkHash`], or
    /// `None` if the string is not a valid 32-byte hex digest. Used when a
    /// manifest is read back from the backend.
    pub fn from_hex(s: &str) -> Option<ChunkHash> {
        let bytes = hex::decode(s).ok()?;
        let array: [u8; 32] = bytes.try_into().ok()?;
        Some(ChunkHash(array))
    }

    /// The backend object key this chunk is stored at: `<CHUNK_PREFIX>/<hex>`.
    pub fn object_key(&self) -> String {
        format!("{CHUNK_PREFIX}/{}", self.to_hex())
    }

    /// The raw 32-byte digest. Used by the ring write-back plane to frame a
    /// chunk's identity compactly inside a flush bundle (32 raw bytes, not 64
    /// hex characters).
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Rebuilds a hash from its raw 32-byte digest — the inverse of
    /// [`ChunkHash::to_bytes`], used when a flush bundle is decoded.
    pub fn from_bytes(bytes: [u8; 32]) -> ChunkHash {
        ChunkHash(bytes)
    }
}

/// One chunk's participation in a byte request: which chunk index it is, and the
/// half-open `[start, end)` sub-range within that chunk the request touches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkSpan {
    /// The zero-based chunk index within the device.
    pub index: u64,
    /// The first byte within the chunk the request touches (inclusive).
    pub start: usize,
    /// One past the last byte within the chunk the request touches (exclusive).
    pub end: usize,
}

impl ChunkSpan {
    /// The number of bytes this span covers within its chunk.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Whether this span covers no bytes. Present so clippy's `len_without_is_empty`
    /// is satisfied; a real span always has `end > start`.
    pub fn is_empty(&self) -> bool {
        self.end == self.start
    }
}

/// The number of chunks a device of `device_size` bytes is cut into: the
/// ceiling of `device_size / CHUNK_SIZE_BYTES`. A zero-sized device has zero
/// chunks.
pub fn chunk_count(device_size: u64) -> u64 {
    device_size.div_ceil(CHUNK_SIZE_BYTES)
}

/// The logical byte length of chunk `index` in a device of `device_size` bytes.
/// Every chunk is [`CHUNK_SIZE_BYTES`] except possibly the last, which is the
/// remainder when the device size is not a chunk multiple. `index` is assumed in
/// range (`< chunk_count(device_size)`); an out-of-range index yields 0.
pub fn chunk_len(index: u64, device_size: u64) -> usize {
    let base = index.saturating_mul(CHUNK_SIZE_BYTES);
    if base >= device_size {
        return 0;
    }
    (device_size - base).min(CHUNK_SIZE_BYTES) as usize
}

/// Decomposes a half-open byte request `[offset, offset + len)` into the chunks
/// it touches, each with the sub-range within that chunk. The request is assumed
/// to lie within the device (`offset + len <= device_size`); the caller clamps
/// to the device size before calling. An empty request yields no spans.
pub fn covering_spans(offset: u64, len: u64) -> Vec<ChunkSpan> {
    if len == 0 {
        return Vec::new();
    }
    let end = offset + len;
    let first = offset / CHUNK_SIZE_BYTES;
    let last = (end - 1) / CHUNK_SIZE_BYTES;
    let mut spans = Vec::new();
    for index in first..=last {
        let chunk_base = index * CHUNK_SIZE_BYTES;
        let chunk_top = chunk_base + CHUNK_SIZE_BYTES;
        let span_start = offset.max(chunk_base) - chunk_base;
        let span_end = end.min(chunk_top) - chunk_base;
        spans.push(ChunkSpan {
            index,
            start: span_start as usize,
            end: span_end as usize,
        });
    }
    spans
}

/// Whether every byte in `bytes` is zero. A chunk that writes back to all-zero
/// is dropped rather than stored, so an all-zero region costs no backend object
/// (the sparse invariant).
pub fn is_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|b| *b == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_bytes_hash_to_one_address() {
        let a = ChunkHash::of(b"the same bytes");
        let b = ChunkHash::of(b"the same bytes");
        let c = ChunkHash::of(b"different bytes");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn hash_round_trips_through_hex() {
        let h = ChunkHash::of(b"payload");
        let hex = h.to_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(ChunkHash::from_hex(&hex), Some(h));
        assert_eq!(h.object_key(), format!("blocks/{hex}"));
        assert_eq!(ChunkHash::from_hex("not-hex"), None);
        assert_eq!(ChunkHash::from_hex("ab"), None);
    }

    #[test]
    fn chunk_count_is_ceiling() {
        assert_eq!(chunk_count(0), 0);
        assert_eq!(chunk_count(1), 1);
        assert_eq!(chunk_count(CHUNK_SIZE_BYTES), 1);
        assert_eq!(chunk_count(CHUNK_SIZE_BYTES + 1), 2);
        assert_eq!(chunk_count(3 * CHUNK_SIZE_BYTES), 3);
    }

    #[test]
    fn last_chunk_carries_the_remainder() {
        let size = CHUNK_SIZE_BYTES + 100;
        assert_eq!(chunk_len(0, size), CHUNK_SIZE_BYTES as usize);
        assert_eq!(chunk_len(1, size), 100);
        assert_eq!(chunk_len(2, size), 0);
    }

    #[test]
    fn covering_spans_splits_at_chunk_boundaries() {
        // A request straddling the first boundary touches two chunks.
        let spans = covering_spans(CHUNK_SIZE_BYTES - 10, 20);
        assert_eq!(
            spans,
            vec![
                ChunkSpan {
                    index: 0,
                    start: (CHUNK_SIZE_BYTES - 10) as usize,
                    end: CHUNK_SIZE_BYTES as usize,
                },
                ChunkSpan {
                    index: 1,
                    start: 0,
                    end: 10,
                },
            ]
        );
        assert_eq!(spans[0].len(), 10);
        assert_eq!(spans[1].len(), 10);
    }

    #[test]
    fn empty_request_touches_no_chunks() {
        assert!(covering_spans(0, 0).is_empty());
    }

    #[test]
    fn zero_detection() {
        assert!(is_zero(&[0, 0, 0]));
        assert!(is_zero(&[]));
        assert!(!is_zero(&[0, 1, 0]));
    }
}
