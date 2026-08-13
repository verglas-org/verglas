//! The in-memory CSR (Compressed Sparse Row) adjacency index and its compact
//! binary form.
//!
//! This is the structure the Puffin blob carries and the structure the scan
//! fallback builds directly — both paths converge here, which is why
//! index-vs-scan traversal is equivalent by construction. Nodes are mapped to
//! dense internal indices; out-adjacency (ordered by source) and in-adjacency
//! (ordered by destination) are stored as offset + neighbor arrays, the
//! GraphAr/Kùzu layout, so "all neighbors of a node" is a contiguous slice
//! rather than a per-edge lookup. Neighbor slices are sorted by
//! (predicate, neighbor) so a predicate filter is a bounded scan and a future
//! delta merge / delta-encoding is cheap.

use crate::error::{GraphError, Result};
use crate::model::Edge;

/// The magic + version prefix of a serialized adjacency blob. The trailing byte
/// is a format version; per AGENTS.md we do not build version negotiation — a
/// format break rebuilds the blob (it is a derived artifact). Extension point.
const MAGIC: [u8; 8] = *b"VGGRAPH\x01";

/// The per-edge payload stored once and referenced from both adjacency
/// directions.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeMeta {
    /// The subject node's dense index.
    pub src: u32,
    /// The object node's dense index.
    pub dst: u32,
    /// The predicate, as an index into [`AdjacencyIndex::predicates`].
    pub predicate: u32,
    /// The edge confidence.
    pub confidence: f64,
    /// The originating edge's id (a traversal receipt).
    pub edge_id: String,
    /// The originating edge's provenance (a traversal receipt).
    pub provenance: String,
}

/// One entry in a neighbor slice: the adjacent node (dense index) and the edge
/// that connects it (index into [`AdjacencyIndex::edges`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdjEntry {
    /// The adjacent node's dense index.
    pub neighbor: u32,
    /// The connecting edge's index into the edge-meta table.
    pub edge: u32,
}

/// A snapshot-bound CSR adjacency index over a set of live edges.
#[derive(Debug, Clone, PartialEq)]
pub struct AdjacencyIndex {
    /// The edge-table snapshot this index reflects.
    snapshot_id: i64,
    /// Dense index → external node id, sorted for binary-search lookup.
    node_ids: Vec<String>,
    /// Predicate index → predicate string, sorted and unique.
    predicates: Vec<String>,
    /// The per-edge payloads, referenced by both adjacency directions.
    edges: Vec<EdgeMeta>,
    /// Out-adjacency start offsets, length `node_ids.len() + 1`.
    out_offsets: Vec<u32>,
    /// Out-adjacency neighbor entries (node is the subject).
    out_adj: Vec<AdjEntry>,
    /// In-adjacency start offsets, length `node_ids.len() + 1`.
    in_offsets: Vec<u32>,
    /// In-adjacency neighbor entries (node is the object).
    in_adj: Vec<AdjEntry>,
}

impl AdjacencyIndex {
    /// Builds the CSR index from a set of live edges (superseded edges already
    /// removed) reflecting `snapshot_id`. This is the shared core: the index
    /// builder serializes the result to Puffin, and the scan fallback uses it
    /// directly.
    pub fn from_edges(edges: &[Edge], snapshot_id: i64) -> Self {
        // Dense node ids: the sorted unique set of every subject and object.
        let mut node_ids: Vec<String> = Vec::with_capacity(edges.len() * 2);
        for edge in edges {
            node_ids.push(edge.src_id.clone());
            node_ids.push(edge.dst_id.clone());
        }
        node_ids.sort();
        node_ids.dedup();

        // Predicate dictionary.
        let mut predicates: Vec<String> = edges.iter().map(|e| e.predicate.clone()).collect();
        predicates.sort();
        predicates.dedup();

        let index_of = |ids: &[String], id: &str| -> u32 {
            // Every id was inserted above, so the search always hits; the
            // fallback keeps this total without an unwrap.
            match ids.binary_search_by(|candidate| candidate.as_str().cmp(id)) {
                Ok(i) => i as u32,
                Err(_) => 0,
            }
        };

        let mut edge_metas: Vec<EdgeMeta> = Vec::with_capacity(edges.len());
        // Per-node buckets before flattening to CSR.
        let mut out_buckets: Vec<Vec<AdjEntry>> = vec![Vec::new(); node_ids.len()];
        let mut in_buckets: Vec<Vec<AdjEntry>> = vec![Vec::new(); node_ids.len()];

        for edge in edges {
            let src = index_of(&node_ids, &edge.src_id);
            let dst = index_of(&node_ids, &edge.dst_id);
            let predicate = index_of(&predicates, &edge.predicate);
            let meta_idx = edge_metas.len() as u32;
            edge_metas.push(EdgeMeta {
                src,
                dst,
                predicate,
                confidence: edge.confidence,
                edge_id: edge.edge_id.clone(),
                provenance: edge.provenance.clone(),
            });
            if let Some(bucket) = out_buckets.get_mut(src as usize) {
                bucket.push(AdjEntry {
                    neighbor: dst,
                    edge: meta_idx,
                });
            }
            if let Some(bucket) = in_buckets.get_mut(dst as usize) {
                bucket.push(AdjEntry {
                    neighbor: src,
                    edge: meta_idx,
                });
            }
        }

        // Sort each neighbor slice by (predicate, neighbor) for stable,
        // filter-friendly order, then flatten to offset + entry arrays.
        let flatten =
            |buckets: Vec<Vec<AdjEntry>>, metas: &[EdgeMeta]| -> (Vec<u32>, Vec<AdjEntry>) {
                let mut offsets = Vec::with_capacity(buckets.len() + 1);
                let mut entries = Vec::new();
                offsets.push(0u32);
                for mut bucket in buckets {
                    bucket.sort_by(|a, b| {
                        let pa = metas.get(a.edge as usize).map(|m| m.predicate).unwrap_or(0);
                        let pb = metas.get(b.edge as usize).map(|m| m.predicate).unwrap_or(0);
                        (pa, a.neighbor).cmp(&(pb, b.neighbor))
                    });
                    entries.extend(bucket);
                    offsets.push(entries.len() as u32);
                }
                (offsets, entries)
            };

        let (out_offsets, out_adj) = flatten(out_buckets, &edge_metas);
        let (in_offsets, in_adj) = flatten(in_buckets, &edge_metas);

        Self {
            snapshot_id,
            node_ids,
            predicates,
            edges: edge_metas,
            out_offsets,
            out_adj,
            in_offsets,
            in_adj,
        }
    }

    /// The edge-table snapshot this index reflects.
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// The number of distinct nodes with at least one incident edge.
    pub fn node_count(&self) -> usize {
        self.node_ids.len()
    }

    /// The dense index of an external node id, or `None` if it has no edges.
    pub fn node_index(&self, id: &str) -> Option<u32> {
        self.node_ids
            .binary_search_by(|candidate| candidate.as_str().cmp(id))
            .ok()
            .map(|i| i as u32)
    }

    /// The external id of a dense index.
    pub fn node_id(&self, index: u32) -> Option<&str> {
        self.node_ids.get(index as usize).map(String::as_str)
    }

    /// The predicate string of a predicate index.
    pub fn predicate_name(&self, index: u32) -> Option<&str> {
        self.predicates.get(index as usize).map(String::as_str)
    }

    /// The predicate index of a predicate string, if the graph uses it.
    pub fn predicate_index(&self, name: &str) -> Option<u32> {
        self.predicates
            .binary_search_by(|candidate| candidate.as_str().cmp(name))
            .ok()
            .map(|i| i as u32)
    }

    /// The edge-meta payload of an edge index.
    pub fn edge_meta(&self, index: u32) -> Option<&EdgeMeta> {
        self.edges.get(index as usize)
    }

    /// The out-adjacency slice of a node (edges where it is the subject).
    pub fn out_entries(&self, node: u32) -> &[AdjEntry] {
        Self::slice(&self.out_offsets, &self.out_adj, node)
    }

    /// The in-adjacency slice of a node (edges where it is the object).
    pub fn in_entries(&self, node: u32) -> &[AdjEntry] {
        Self::slice(&self.in_offsets, &self.in_adj, node)
    }

    /// The contiguous neighbor slice for `node` given an offset/entry pair.
    fn slice<'a>(offsets: &[u32], entries: &'a [AdjEntry], node: u32) -> &'a [AdjEntry] {
        let start = offsets.get(node as usize).copied().unwrap_or(0) as usize;
        let end = offsets.get(node as usize + 1).copied().unwrap_or(0) as usize;
        entries.get(start..end).unwrap_or(&[])
    }

    /// Serializes the index to its compact little-endian binary form (the bytes
    /// of the Puffin blob).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&self.snapshot_id.to_le_bytes());
        put_str_vec(&mut buf, &self.node_ids);
        put_str_vec(&mut buf, &self.predicates);
        put_u32(&mut buf, self.edges.len() as u32);
        for meta in &self.edges {
            put_u32(&mut buf, meta.src);
            put_u32(&mut buf, meta.dst);
            put_u32(&mut buf, meta.predicate);
            buf.extend_from_slice(&meta.confidence.to_le_bytes());
            put_str(&mut buf, &meta.edge_id);
            put_str(&mut buf, &meta.provenance);
        }
        put_u32_vec(&mut buf, &self.out_offsets);
        put_adj_vec(&mut buf, &self.out_adj);
        put_u32_vec(&mut buf, &self.in_offsets);
        put_adj_vec(&mut buf, &self.in_adj);
        buf
    }

    /// Decodes an index from its binary form. A truncated blob, bad magic, or an
    /// out-of-range length is a [`GraphError::CorruptIndex`]; the caller then
    /// treats the index as absent and falls back to a scan.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cur = Cursor::new(bytes);
        let magic = cur.take(MAGIC.len())?;
        if magic != MAGIC {
            return Err(GraphError::CorruptIndex("bad magic".to_owned()));
        }
        let snapshot_id = cur.read_i64()?;
        let node_ids = cur.read_str_vec()?;
        let predicates = cur.read_str_vec()?;
        let edge_count = cur.read_u32()? as usize;
        let mut edges = Vec::with_capacity(edge_count);
        for _ in 0..edge_count {
            let src = cur.read_u32()?;
            let dst = cur.read_u32()?;
            let predicate = cur.read_u32()?;
            let confidence = cur.read_f64()?;
            let edge_id = cur.read_str()?;
            let provenance = cur.read_str()?;
            edges.push(EdgeMeta {
                src,
                dst,
                predicate,
                confidence,
                edge_id,
                provenance,
            });
        }
        let out_offsets = cur.read_u32_vec()?;
        let out_adj = cur.read_adj_vec()?;
        let in_offsets = cur.read_u32_vec()?;
        let in_adj = cur.read_adj_vec()?;
        Ok(Self {
            snapshot_id,
            node_ids,
            predicates,
            edges,
            out_offsets,
            out_adj,
            in_offsets,
            in_adj,
        })
    }
}

/// Appends a u32 as little-endian.
fn put_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Appends a length-prefixed UTF-8 string.
fn put_str(buf: &mut Vec<u8>, value: &str) {
    put_u32(buf, value.len() as u32);
    buf.extend_from_slice(value.as_bytes());
}

/// Appends a length-prefixed vector of strings.
fn put_str_vec(buf: &mut Vec<u8>, values: &[String]) {
    put_u32(buf, values.len() as u32);
    for value in values {
        put_str(buf, value);
    }
}

/// Appends a length-prefixed vector of u32s.
fn put_u32_vec(buf: &mut Vec<u8>, values: &[u32]) {
    put_u32(buf, values.len() as u32);
    for value in values {
        put_u32(buf, *value);
    }
}

/// Appends a length-prefixed vector of adjacency entries.
fn put_adj_vec(buf: &mut Vec<u8>, values: &[AdjEntry]) {
    put_u32(buf, values.len() as u32);
    for entry in values {
        put_u32(buf, entry.neighbor);
        put_u32(buf, entry.edge);
    }
}

/// A bounds-checked reader over the blob bytes. Every read that would run past
/// the end yields a [`GraphError::CorruptIndex`] rather than panicking.
struct Cursor<'a> {
    /// The full blob.
    bytes: &'a [u8],
    /// The read position.
    pos: usize,
}

impl<'a> Cursor<'a> {
    /// A cursor at the start of `bytes`.
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    /// Takes `n` bytes, advancing the position.
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| GraphError::CorruptIndex("length overflow".to_owned()))?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| GraphError::CorruptIndex("unexpected end of blob".to_owned()))?;
        self.pos = end;
        Ok(slice)
    }

    /// Reads a little-endian u32.
    fn read_u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Reads a little-endian i64.
    fn read_i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        let arr: [u8; 8] = b
            .try_into()
            .map_err(|_| GraphError::CorruptIndex("bad i64".to_owned()))?;
        Ok(i64::from_le_bytes(arr))
    }

    /// Reads a little-endian f64.
    fn read_f64(&mut self) -> Result<f64> {
        let b = self.take(8)?;
        let arr: [u8; 8] = b
            .try_into()
            .map_err(|_| GraphError::CorruptIndex("bad f64".to_owned()))?;
        Ok(f64::from_le_bytes(arr))
    }

    /// Reads a length-prefixed UTF-8 string.
    fn read_str(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| GraphError::CorruptIndex("bad utf-8 string".to_owned()))
    }

    /// Reads a length-prefixed vector of strings.
    fn read_str_vec(&mut self) -> Result<Vec<String>> {
        let count = self.read_u32()? as usize;
        let mut out = Vec::with_capacity(count.min(1 << 20));
        for _ in 0..count {
            out.push(self.read_str()?);
        }
        Ok(out)
    }

    /// Reads a length-prefixed vector of u32s.
    fn read_u32_vec(&mut self) -> Result<Vec<u32>> {
        let count = self.read_u32()? as usize;
        let mut out = Vec::with_capacity(count.min(1 << 20));
        for _ in 0..count {
            out.push(self.read_u32()?);
        }
        Ok(out)
    }

    /// Reads a length-prefixed vector of adjacency entries.
    fn read_adj_vec(&mut self) -> Result<Vec<AdjEntry>> {
        let count = self.read_u32()? as usize;
        let mut out = Vec::with_capacity(count.min(1 << 20));
        for _ in 0..count {
            let neighbor = self.read_u32()?;
            let edge = self.read_u32()?;
            out.push(AdjEntry { neighbor, edge });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Edge;

    /// A fixed edge for deterministic assertions.
    fn edge(id: &str, src: &str, pred: &str, dst: &str, conf: f64) -> Edge {
        let mut e = Edge::new(src, pred, dst, "prov");
        e.edge_id = id.to_owned();
        e.confidence = conf;
        e
    }

    /// from_edges builds a CSR whose out/in slices match the input edges.
    #[test]
    fn builds_out_and_in_adjacency() {
        let edges = vec![
            edge("e1", "a", "knows", "b", 0.9),
            edge("e2", "a", "likes", "c", 0.5),
            edge("e3", "b", "knows", "c", 0.7),
        ];
        let index = AdjacencyIndex::from_edges(&edges, 42);
        assert_eq!(index.snapshot_id(), 42);
        assert_eq!(index.node_count(), 3);

        let a = index.node_index("a").expect("a present");
        assert_eq!(index.out_entries(a).len(), 2);
        assert!(index.in_entries(a).is_empty());

        let c = index.node_index("c").expect("c present");
        assert_eq!(index.in_entries(c).len(), 2);
        assert!(index.out_entries(c).is_empty());

        // A node with no edges resolves to None.
        assert!(index.node_index("zzz").is_none());
    }

    /// encode → decode round-trips to an identical index.
    #[test]
    fn encode_decode_round_trips() {
        let edges = vec![
            edge("e1", "a", "knows", "b", 0.9),
            edge("e2", "b", "knows", "c", 0.7),
            edge("e3", "c", "knows", "a", 0.6),
        ];
        let index = AdjacencyIndex::from_edges(&edges, 7);
        let bytes = index.encode();
        let decoded = AdjacencyIndex::decode(&bytes).expect("decode");
        assert_eq!(index, decoded);
    }

    /// A truncated or bad-magic blob decodes to a CorruptIndex error, not a panic.
    #[test]
    fn corrupt_blob_is_rejected() {
        assert!(AdjacencyIndex::decode(b"not-a-graph-blob").is_err());
        assert!(AdjacencyIndex::decode(&[]).is_err());

        let good = AdjacencyIndex::from_edges(&[edge("e1", "a", "p", "b", 1.0)], 1).encode();
        // Truncating the body past the header must be rejected.
        assert!(AdjacencyIndex::decode(&good[..good.len() - 3]).is_err());
    }
}
