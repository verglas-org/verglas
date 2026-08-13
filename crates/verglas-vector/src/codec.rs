//! The `verglas-vamana-v1` binary layout: the compact, mmap-friendly serialized
//! form of a consolidated Vamana graph. This is the payload carried inside a
//! Puffin blob (see `puffin.rs`); Puffin is the container, this is the blob
//! body.
//!
//! Layout (all little-endian; the DiskANN-style flat CSR the paper's shard
//! layout also uses — offsets + neighbor arrays, so "the neighbors of node i"
//! is a contiguous slice):
//!
//! ```text
//!   MAGIC              8 bytes  b"VGVAMN\x01\x00"
//!   metric             1 byte   0 = L2, 1 = Cosine
//!   _pad               3 bytes
//!   dim                u32
//!   R                  u32
//!   L_build            u32
//!   alpha              f32
//!   reflected_snapshot i64
//!   node_count (N)     u64
//!   edge_count (E)     u64
//!   ids                N * i64
//!   vectors            N * dim * f32
//!   adj_offsets        (N+1) * u64   (prefix sums into adj_neighbors)
//!   adj_neighbors      E * u32
//!   deleted_bits       ceil(N/8) bytes  (tombstone bitmap, LSB-first)
//! ```
//!
//! The tombstone bitmap is FreshDiskANN's on-disk DeleteList: lazily-deleted
//! points stay in the graph (traversal is unbroken) and survive across
//! maintenance runs until a consolidation rewrites the blob without them.
//!
//! Per AGENTS.md there is no version negotiation: a format break rebuilds this
//! derived artifact. The trailing `v1` byte in the magic is the extension point.

use crate::error::{Result, VectorError};
use crate::metric::Metric;
use crate::vamana::{VamanaIndex, VamanaParams};

/// Magic + format version prefix.
const MAGIC: [u8; 8] = *b"VGVAMN\x01\x00";

/// The Puffin blob type string for a Vamana index. Follows the `verglas-*`
/// blob-type convention (#95); `v1` is the layout version.
pub const BLOB_TYPE: &str = "verglas-vamana-v1";

/// Serializes a (consolidated) index to the `verglas-vamana-v1` layout.
///
/// The caller is expected to `consolidate` first so no tombstones are present;
/// this encodes only the live graph.
pub fn encode(index: &VamanaIndex) -> Vec<u8> {
    let (metric, dim, params, snapshot, vectors, ids, adj, deleted) = index.parts();
    let n = ids.len();
    let e: usize = adj.iter().map(|a| a.len()).sum();

    let mut buf: Vec<u8> = Vec::with_capacity(
        8 + 4 + 4 + 12 + 8 + 16 + n * 8 + vectors.len() * 4 + (n + 1) * 8 + e * 4 + n.div_ceil(8),
    );
    buf.extend_from_slice(&MAGIC);
    buf.push(match metric {
        Metric::L2 => 0,
        Metric::Cosine => 1,
    });
    buf.extend_from_slice(&[0u8; 3]);
    buf.extend_from_slice(&(dim as u32).to_le_bytes());
    buf.extend_from_slice(&(params.r as u32).to_le_bytes());
    buf.extend_from_slice(&(params.l_build as u32).to_le_bytes());
    buf.extend_from_slice(&params.alpha.to_le_bytes());
    buf.extend_from_slice(&snapshot.to_le_bytes());
    buf.extend_from_slice(&(n as u64).to_le_bytes());
    buf.extend_from_slice(&(e as u64).to_le_bytes());

    for &id in ids {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    for &x in vectors {
        buf.extend_from_slice(&x.to_le_bytes());
    }
    let mut offset = 0u64;
    buf.extend_from_slice(&offset.to_le_bytes());
    for a in adj {
        offset += a.len() as u64;
        buf.extend_from_slice(&offset.to_le_bytes());
    }
    for a in adj {
        for &nb in a {
            buf.extend_from_slice(&nb.to_le_bytes());
        }
    }
    // Tombstone bitmap, LSB-first within each byte.
    let mut bitmap = vec![0u8; n.div_ceil(8)];
    for (i, &d) in deleted.iter().enumerate() {
        if d {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    buf.extend_from_slice(&bitmap);
    buf
}

/// Deserializes a `verglas-vamana-v1` blob body back into an index. A truncated
/// blob, bad magic, or inconsistent lengths is a [`VectorError::CorruptBlob`],
/// never a panic (the caller then falls back to brute force — the turn-off
/// path).
pub fn decode(bytes: &[u8]) -> Result<VamanaIndex> {
    let mut cur = Cursor::new(bytes);
    let magic = cur.take(8)?;
    if magic != MAGIC {
        return Err(VectorError::CorruptBlob("bad magic".to_owned()));
    }
    let metric = match cur.take(1)?[0] {
        0 => Metric::L2,
        1 => Metric::Cosine,
        other => {
            return Err(VectorError::CorruptBlob(format!("unknown metric {other}")));
        }
    };
    cur.take(3)?; // padding
    let dim = cur.u32()? as usize;
    let r = cur.u32()? as usize;
    let l_build = cur.u32()? as usize;
    let alpha = cur.f32()?;
    let snapshot = cur.i64()?;
    let n = cur.u64()? as usize;
    let e = cur.u64()? as usize;
    if dim == 0 {
        return Err(VectorError::CorruptBlob("zero dim".to_owned()));
    }

    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        ids.push(cur.i64()?);
    }
    let mut vectors = Vec::with_capacity(n * dim);
    for _ in 0..n * dim {
        vectors.push(cur.f32()?);
    }
    let mut offsets = Vec::with_capacity(n + 1);
    for _ in 0..n + 1 {
        offsets.push(cur.u64()?);
    }
    if offsets.first().copied() != Some(0) || offsets.last().copied() != Some(e as u64) {
        return Err(VectorError::CorruptBlob("bad adjacency offsets".to_owned()));
    }
    let mut neighbors = Vec::with_capacity(e);
    for _ in 0..e {
        neighbors.push(cur.u32()?);
    }
    let mut adj: Vec<Vec<u32>> = Vec::with_capacity(n);
    for w in offsets.windows(2) {
        let (a, b) = (w[0] as usize, w[1] as usize);
        if b < a || b > neighbors.len() {
            return Err(VectorError::CorruptBlob("adjacency overrun".to_owned()));
        }
        adj.push(neighbors[a..b].to_vec());
    }

    let bitmap = cur.take(n.div_ceil(8))?;
    let deleted: Vec<bool> = (0..n)
        .map(|i| bitmap[i / 8] & (1 << (i % 8)) != 0)
        .collect();

    VamanaIndex::from_parts(
        metric,
        dim,
        VamanaParams { r, l_build, alpha },
        snapshot,
        vectors,
        ids,
        adj,
        deleted,
    )
}

/// A tiny forward-only reader over a byte slice that returns
/// [`VectorError::CorruptBlob`] on truncation instead of panicking.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Cursor { bytes, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.bytes.len())
            .ok_or_else(|| VectorError::CorruptBlob("truncated".to_owned()))?;
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }
    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }
    fn f32(&mut self) -> Result<f32> {
        let b = self.take(4)?;
        Ok(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
}
