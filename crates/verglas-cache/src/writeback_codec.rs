//! Streaming Reed-Solomon fragment codec for the write-back tier (#180).
//!
//! An object is erasure-coded into `k` data plus `m` parity fragments so any
//! `k` of the `k+m` fragments reconstruct it. Encoding is **striped**: the
//! object is cut into stripes of `k * chunk` bytes, each stripe is encoded on
//! its own, and fragment `i` is the concatenation of chunk `i` from every
//! stripe. Striping is what lets encoding overlap the client upload — a stripe
//! is encoded as soon as its `k * chunk` bytes have arrived, so the encoder
//! never needs the whole object in memory at once ([`StripeEncoder`]).
//!
//! This module owns only byte splitting and reassembly. Placement, journals,
//! peer transport, quorum, and origin propagation live above it in
//! `verglas-writeback`.
//!
//! Backend: `reed-solomon-simd` (Leopard, O(n log n), SIMD on x86-64 and
//! AArch64). The comparison against `reed-solomon-erasure` that justifies the
//! choice is `examples/codec_throughput.rs`.

use bytes::Bytes;
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

/// Largest per-fragment stripe chunk. Bounds the encoder's working buffer to
/// one stripe (`k * chunk`) so a large streamed upload never buffers whole.
/// Even, as `reed-solomon-simd` requires an even shard size.
pub const MAX_STRIPE_CHUNK: usize = 4 * 1024 * 1024;

/// The per-fragment integrity checksum over the fragment's whole payload.
///
/// CRC32C (Castagnoli) is the choice: it is a bit-flip detector, not an
/// error-correcting code, which is exactly what is wanted here — Reed-Solomon is
/// an *erasure* code, so a fragment that fails this check is treated as missing
/// (an erasure) and the object is reconstructed from the good fragments. CRC32C
/// is hardware-accelerated (SSE4.2 / ARM CRC instructions with a software
/// fallback) and is the standard storage checksum (iSCSI, SCTP, ext4, Ceph), so
/// it is cheap enough to compute on the write path. It is not a cryptographic
/// hash: it defends against disk/transit bit-rot, not a malicious rewrite.
pub fn checksum(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// Errors from the write-back fragment codec.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// The requested data/parity geometry is not usable.
    #[error("invalid write-back codec geometry k={k} m={m}: {reason}")]
    InvalidGeometry {
        /// Number of data fragments.
        k: usize,
        /// Number of parity fragments.
        m: usize,
        /// Why the geometry is rejected.
        reason: &'static str,
    },
    /// The Reed-Solomon backend rejected an encode or decode call.
    #[error("reed-solomon codec failed: {0}")]
    Backend(String),
    /// Fewer than `k` distinct fragments were available for reconstruction.
    #[error("need at least {needed} distinct fragments, got {available}")]
    InsufficientFragments {
        /// Fragments required (the data width `k`).
        needed: usize,
        /// Distinct fragments supplied.
        available: usize,
    },
    /// A fragment index is outside `0..k+m`.
    #[error("fragment index {index} is outside 0..{total}")]
    FragmentIndex {
        /// Offending fragment index.
        index: usize,
        /// Total fragment count `k+m`.
        total: usize,
    },
    /// A supplied fragment has the wrong length for the stripe layout.
    #[error("fragment {index} has {actual} bytes, expected a multiple of chunk {chunk}")]
    FragmentLength {
        /// Offending fragment index.
        index: usize,
        /// Supplied byte count.
        actual: usize,
        /// Per-stripe chunk size.
        chunk: usize,
    },
}

/// One encoded data or parity fragment.
///
/// `index` in `0..k` is a data fragment, `k..k+m` is a parity fragment. `bytes`
/// is `num_stripes * chunk` long: the fragment's chunk from each stripe,
/// concatenated in stripe order. `checksum` is the CRC32C of `bytes`, carried
/// alongside so [`reassemble`] can reject a bit-flipped fragment before it is
/// fed to the erasure decoder (#220).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fragment {
    /// Zero-based fragment index. `0..k` data, `k..k+m` parity.
    pub index: usize,
    /// The fragment payload (stripe chunks concatenated).
    pub bytes: Bytes,
    /// CRC32C of `bytes`, the integrity check verified before reassembly.
    pub checksum: u32,
}

impl Fragment {
    /// Builds a fragment for `index` over `bytes`, computing its CRC32C. The
    /// only way to mint a fragment, so `checksum` always describes `bytes`
    /// unless the bytes are later corrupted on disk or in transit.
    pub fn new(index: usize, bytes: Bytes) -> Self {
        let checksum = checksum(&bytes);
        Self {
            index,
            bytes,
            checksum,
        }
    }

    /// True when the fragment's bytes still match its recorded checksum. A
    /// `false` means the bytes were corrupted after the checksum was taken; the
    /// fragment is then treated as an erasure, not as data.
    pub fn verify(&self) -> bool {
        checksum(&self.bytes) == self.checksum
    }
}

/// The fixed geometry and stripe sizing shared by an object's fragments.
///
/// Held in the journal so a later read can reassemble with the exact chunk the
/// write used. Round up `object_len/k` to an even chunk, capped at
/// [`MAX_STRIPE_CHUNK`], so small objects get small fragments and large ones
/// stream in bounded stripes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    /// Data fragments.
    pub k: usize,
    /// Parity fragments.
    pub m: usize,
    /// Bytes per fragment per stripe.
    pub chunk: usize,
}

impl Geometry {
    /// Builds a geometry for `object_len`, choosing the stripe chunk. Rejects
    /// `k == 0` or `m == 0`.
    pub fn for_object(k: usize, m: usize, object_len: usize) -> Result<Self, CodecError> {
        Self::validate(k, m)?;
        let target = object_len.div_ceil(k.max(1)).max(2);
        let chunk = round_up_even(target.min(MAX_STRIPE_CHUNK));
        Ok(Self { k, m, chunk })
    }

    /// Builds a geometry with an explicit chunk (streaming, unknown length).
    /// The chunk is rounded up to even and floored at 2.
    pub fn streaming(k: usize, m: usize, chunk: usize) -> Result<Self, CodecError> {
        Self::validate(k, m)?;
        Ok(Self {
            k,
            m,
            chunk: round_up_even(chunk.max(2)),
        })
    }

    /// Rejects geometries the codec cannot use. `k == 0` is always invalid.
    /// `m == 0` is the degenerate **identity code**: `k` data fragments, no
    /// parity, so any lost data fragment is unrecoverable. It carries no
    /// redundancy, which is exactly the single-node write-back contract (#286) —
    /// one local fragment whose only durability is the local disk until it
    /// propagates. Multi-node quorum geometry still requires `m >= 1` at the
    /// configuration layer; the identity code is selected internally only for a
    /// one-node deployment.
    fn validate(k: usize, m: usize) -> Result<(), CodecError> {
        let _ = m;
        if k == 0 {
            return Err(CodecError::InvalidGeometry {
                k,
                m,
                reason: "k must be at least 1",
            });
        }
        Ok(())
    }

    /// Total fragments `k + m`.
    pub fn total(&self) -> usize {
        self.k + self.m
    }

    /// Bytes of original data covered by one stripe (`k * chunk`).
    fn stripe_data_len(&self) -> usize {
        self.k * self.chunk
    }
}

/// Rounds `n` up to the next even number (shard size must be even).
fn round_up_even(n: usize) -> usize {
    if n.is_multiple_of(2) { n } else { n + 1 }
}

/// A streaming stripe encoder: push object bytes in arbitrary-sized slices, and
/// each time a full stripe (`k * chunk` bytes) accumulates it is encoded and
/// appended to the fragment buffers. [`StripeEncoder::finish`] flushes the last
/// (zero-padded) stripe and returns the fragments.
///
/// Encoding overlaps the upload: the caller feeds bytes as they arrive from the
/// client and only the current stripe is resident.
pub struct StripeEncoder {
    /// Geometry and chunk sizing.
    geometry: Geometry,
    /// `k + m` fragment buffers, grown one chunk per stripe.
    fragments: Vec<Vec<u8>>,
    /// Bytes of the current, not-yet-full stripe.
    stripe_buf: Vec<u8>,
    /// Total original bytes pushed so far.
    object_len: usize,
}

impl StripeEncoder {
    /// Starts an encoder for `geometry`.
    pub fn new(geometry: Geometry) -> Self {
        Self {
            geometry,
            fragments: vec![Vec::new(); geometry.total()],
            stripe_buf: Vec::with_capacity(geometry.stripe_data_len()),
            object_len: 0,
        }
    }

    /// Feeds `data` into the encoder, encoding any stripes it completes.
    pub fn push(&mut self, data: &[u8]) -> Result<(), CodecError> {
        self.object_len += data.len();
        self.stripe_buf.extend_from_slice(data);
        let stripe = self.geometry.stripe_data_len();
        while self.stripe_buf.len() >= stripe {
            let rest = self.stripe_buf.split_off(stripe);
            let full = std::mem::replace(&mut self.stripe_buf, rest);
            self.encode_stripe(&full)?;
        }
        Ok(())
    }

    /// Flushes the trailing partial stripe (zero-padded) and returns the
    /// finished fragments plus the geometry and true object length.
    pub fn finish(mut self) -> Result<Encoded, CodecError> {
        if !self.stripe_buf.is_empty() {
            let mut last = std::mem::take(&mut self.stripe_buf);
            last.resize(self.geometry.stripe_data_len(), 0);
            self.encode_stripe(&last)?;
        }
        let fragments = self
            .fragments
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| Fragment::new(index, Bytes::from(bytes)))
            .collect();
        Ok(Encoded {
            geometry: self.geometry,
            object_len: self.object_len as u64,
            fragments,
        })
    }

    /// Encodes one full `k * chunk` stripe and appends each shard to its
    /// fragment buffer.
    fn encode_stripe(&mut self, stripe: &[u8]) -> Result<(), CodecError> {
        let shards = encode_one_stripe(self.geometry, stripe)?;
        for (index, shard) in shards.into_iter().enumerate() {
            self.fragments[index].extend_from_slice(&shard);
        }
        Ok(())
    }
}

/// Encodes one full `k * chunk` stripe into its `k + m` shards (the `k` data
/// chunks followed by the `m` parity chunks). The shared stripe kernel behind
/// both the accumulating [`StripeEncoder`] and the streaming
/// [`StreamingStripeEncoder`].
fn encode_one_stripe(geometry: Geometry, stripe: &[u8]) -> Result<Vec<Vec<u8>>, CodecError> {
    let Geometry { k, m, chunk } = geometry;
    // Identity code (`m == 0`): the shards are just the `k` data chunks, no
    // parity. The Reed-Solomon backend requires `m >= 1`, and there is nothing
    // to encode anyway, so bypass it. `reassemble` needs all `k` data shards
    // present for such an object (there is no parity to reconstruct from), which
    // is the single-node contract: the one local fragment is the only copy.
    if m == 0 {
        return Ok((0..k)
            .map(|i| stripe[i * chunk..(i + 1) * chunk].to_vec())
            .collect());
    }
    let mut encoder = ReedSolomonEncoder::new(k, m, chunk)
        .map_err(|error| CodecError::Backend(error.to_string()))?;
    let mut shards: Vec<Vec<u8>> = Vec::with_capacity(k + m);
    for i in 0..k {
        let shard = &stripe[i * chunk..(i + 1) * chunk];
        encoder
            .add_original_shard(shard)
            .map_err(|error| CodecError::Backend(error.to_string()))?;
        shards.push(shard.to_vec());
    }
    let result = encoder
        .encode()
        .map_err(|error| CodecError::Backend(error.to_string()))?;
    for shard in result.recovery_iter() {
        shards.push(shard.to_vec());
    }
    Ok(shards)
}

/// A streaming stripe encoder that never accumulates the fragments.
///
/// Like [`StripeEncoder`] it consumes object bytes in arbitrary-sized slices and
/// encodes each full `k * chunk` stripe. Unlike it, each completed stripe's
/// `k + m` shards are handed to a caller-supplied sink and then dropped, so the
/// encoder's resident memory is one stripe (`k * chunk` input plus one stripe of
/// shards), independent of object size. The coordinator uses this to append
/// fragment shards to NVMe/peers stripe by stripe, so a large object never
/// buffers whole — the write-back size limit becomes NVMe headroom, not DRAM
/// (#180).
pub struct StreamingStripeEncoder {
    /// Geometry and chunk sizing.
    geometry: Geometry,
    /// Bytes of the current, not-yet-full stripe.
    stripe_buf: Vec<u8>,
    /// Total original bytes pushed so far.
    object_len: u64,
}

impl StreamingStripeEncoder {
    /// Starts a streaming encoder for `geometry`.
    pub fn new(geometry: Geometry) -> Self {
        Self {
            geometry,
            stripe_buf: Vec::with_capacity(geometry.stripe_data_len()),
            object_len: 0,
        }
    }

    /// Feeds `data` in, calling `sink` once per completed stripe with that
    /// stripe's `k + m` shards in fragment-index order. The shards are dropped
    /// after `sink` returns, so nothing accumulates.
    pub fn push<S, E>(&mut self, data: &[u8], mut sink: S) -> Result<(), E>
    where
        S: FnMut(&[Vec<u8>]) -> Result<(), E>,
        E: From<CodecError>,
    {
        self.object_len += data.len() as u64;
        self.stripe_buf.extend_from_slice(data);
        let stripe = self.geometry.stripe_data_len();
        while self.stripe_buf.len() >= stripe {
            let rest = self.stripe_buf.split_off(stripe);
            let full = std::mem::replace(&mut self.stripe_buf, rest);
            let shards = encode_one_stripe(self.geometry, &full)?;
            sink(&shards)?;
        }
        Ok(())
    }

    /// Flushes the trailing partial stripe (zero-padded), calling `sink` with
    /// its shards if any bytes remain, and returns the true object length.
    pub fn finish<S, E>(mut self, mut sink: S) -> Result<u64, E>
    where
        S: FnMut(&[Vec<u8>]) -> Result<(), E>,
        E: From<CodecError>,
    {
        if !self.stripe_buf.is_empty() {
            let mut last = std::mem::take(&mut self.stripe_buf);
            last.resize(self.geometry.stripe_data_len(), 0);
            let shards = encode_one_stripe(self.geometry, &last)?;
            sink(&shards)?;
        }
        Ok(self.object_len)
    }

    /// The geometry this encoder streams with.
    pub fn geometry(&self) -> Geometry {
        self.geometry
    }
}

/// The output of [`StripeEncoder::finish`]: the fragments, their geometry, and
/// the original object length (fragments are padded to a stripe boundary).
#[derive(Debug, Clone)]
pub struct Encoded {
    /// Geometry the fragments were produced with.
    pub geometry: Geometry,
    /// Original object length before stripe padding.
    pub object_len: u64,
    /// The `k + m` fragments in index order.
    pub fragments: Vec<Fragment>,
}

/// Encodes a whole in-memory object in one call (tests and small objects).
pub fn encode(k: usize, m: usize, body: &[u8]) -> Result<Encoded, CodecError> {
    let geometry = Geometry::for_object(k, m, body.len())?;
    let mut encoder = StripeEncoder::new(geometry);
    encoder.push(body)?;
    encoder.finish()
}

/// Reassembles the original object from any `k` distinct, verified fragments.
///
/// Each fragment must be `num_stripes * chunk` bytes. Before a fragment is used
/// it is checksum-verified (#220): Reed-Solomon is an *erasure* code, not an
/// error-correcting one, so a bit-flipped fragment fed to the decoder produces
/// silently wrong bytes. A fragment that fails its CRC32C is therefore dropped
/// and treated as missing (an erasure), and the object is reconstructed from the
/// good fragments. Stripes are decoded independently: a stripe with all `k` data
/// fragments present is copied directly; otherwise it is reconstructed. Fails
/// loudly if fewer than `k` distinct fragments verify — it never returns
/// reconstructed garbage.
pub fn reassemble(
    geometry: Geometry,
    object_len: u64,
    fragments: &[Fragment],
) -> Result<Bytes, CodecError> {
    let Geometry { k, m, chunk } = geometry;
    let total = geometry.total();

    // Index the supplied fragments, rejecting bad indices and lengths. A
    // fragment that fails its checksum is corrupt: skip it so it is treated as
    // an erasure the parity fragments cover, never as data.
    let mut slots: Vec<Option<&Fragment>> = vec![None; total];
    let mut num_stripes = 0usize;
    let mut available = 0usize;
    for fragment in fragments {
        if fragment.index >= total {
            return Err(CodecError::FragmentIndex {
                index: fragment.index,
                total,
            });
        }
        if !fragment.bytes.len().is_multiple_of(chunk) {
            return Err(CodecError::FragmentLength {
                index: fragment.index,
                actual: fragment.bytes.len(),
                chunk,
            });
        }
        // Verify before use: a corrupt fragment is an erasure, not data.
        if !fragment.verify() {
            continue;
        }
        let stripes = fragment.bytes.len() / chunk;
        num_stripes = num_stripes.max(stripes);
        if slots[fragment.index].is_none() {
            available += 1;
            slots[fragment.index] = Some(fragment);
        }
    }
    if available < k {
        return Err(CodecError::InsufficientFragments {
            needed: k,
            available,
        });
    }

    let mut body = Vec::with_capacity(num_stripes * k * chunk);
    for stripe in 0..num_stripes {
        let span = |f: &Fragment| f.bytes.slice(stripe * chunk..(stripe + 1) * chunk);
        let data_slots = &slots[..k];
        let parity_slots = &slots[k..];
        // Fast path: every data fragment present, no decode needed.
        if data_slots.iter().all(Option::is_some) {
            for fragment in data_slots.iter().flatten() {
                body.extend_from_slice(&span(fragment));
            }
            continue;
        }
        // Reconstruct this stripe from whatever k+ fragments are present.
        let mut decoder = ReedSolomonDecoder::new(k, m, chunk)
            .map_err(|error| CodecError::Backend(error.to_string()))?;
        for (i, fragment) in data_slots.iter().enumerate() {
            if let Some(fragment) = fragment {
                decoder
                    .add_original_shard(i, span(fragment))
                    .map_err(|error| CodecError::Backend(error.to_string()))?;
            }
        }
        for (parity, fragment) in parity_slots.iter().enumerate() {
            if let Some(fragment) = fragment {
                decoder
                    .add_recovery_shard(parity, span(fragment))
                    .map_err(|error| CodecError::Backend(error.to_string()))?;
            }
        }
        let result = decoder
            .decode()
            .map_err(|error| CodecError::Backend(error.to_string()))?;
        let restored: std::collections::HashMap<usize, &[u8]> =
            result.restored_original_iter().collect();
        for (i, fragment) in data_slots.iter().enumerate() {
            match fragment {
                Some(fragment) => body.extend_from_slice(&span(fragment)),
                None => {
                    let shard = restored.get(&i).ok_or(CodecError::InsufficientFragments {
                        needed: k,
                        available,
                    })?;
                    body.extend_from_slice(shard);
                }
            }
        }
    }
    body.truncate(object_len as usize);
    Ok(Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic bytes whose pattern crosses shard and stripe boundaries.
    fn body(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// All k+m fragments are produced and data fragments equal the input split.
    #[test]
    fn encode_produces_k_plus_m_fragments() {
        let encoded = encode(4, 2, &body(1000)).expect("encode");
        assert_eq!(encoded.fragments.len(), 6);
        assert_eq!(encoded.object_len, 1000);
        assert!(
            encoded
                .fragments
                .iter()
                .all(|f| f.bytes.len() == encoded.geometry.chunk)
        );
    }

    /// Any k of k+m fragments reconstruct the exact original, single stripe.
    #[test]
    fn reassembles_from_any_k_single_stripe() {
        let original = body(4096 + 7);
        let encoded = encode(4, 2, &original).expect("encode");
        let total = encoded.geometry.total();
        // Drop each possible pair of fragments (m=2) and rebuild from the rest.
        for drop_a in 0..total {
            for drop_b in (drop_a + 1)..total {
                let surviving: Vec<Fragment> = encoded
                    .fragments
                    .iter()
                    .filter(|f| f.index != drop_a && f.index != drop_b)
                    .cloned()
                    .collect();
                let got = reassemble(encoded.geometry, encoded.object_len, &surviving)
                    .expect("reassemble");
                assert_eq!(
                    got.as_ref(),
                    original.as_slice(),
                    "dropped {drop_a},{drop_b}"
                );
            }
        }
    }

    /// A multi-stripe object (larger than one k*chunk stripe) round-trips, and
    /// streaming push in odd-sized slices matches a single push.
    #[test]
    fn multi_stripe_streaming_round_trips() {
        let original = body(MAX_STRIPE_CHUNK * 4 * 2 + 123);
        let geometry = Geometry::streaming(4, 2, MAX_STRIPE_CHUNK).expect("geometry");
        let mut encoder = StripeEncoder::new(geometry);
        // Feed in irregular slices to exercise the stripe buffer boundaries.
        let mut offset = 0;
        for step in [1, 7, 4095, MAX_STRIPE_CHUNK, 999_999] {
            let end = (offset + step).min(original.len());
            encoder.push(&original[offset..end]).expect("push");
            offset = end;
        }
        encoder.push(&original[offset..]).expect("push tail");
        let encoded = encoder.finish().expect("finish");
        assert!(encoded.fragments[0].bytes.len() >= 2 * geometry.chunk);

        // Drop the two parity fragments; rebuild from the k data fragments.
        let surviving: Vec<Fragment> = encoded
            .fragments
            .iter()
            .filter(|f| f.index < 4)
            .cloned()
            .collect();
        let got = reassemble(encoded.geometry, encoded.object_len, &surviving).expect("reassemble");
        assert_eq!(got.as_ref(), original.as_slice());
    }

    /// Fewer than k distinct fragments cannot reconstruct.
    #[test]
    fn rejects_too_few_fragments() {
        let encoded = encode(4, 2, &body(500)).expect("encode");
        let err = reassemble(
            encoded.geometry,
            encoded.object_len,
            &encoded.fragments[..3],
        )
        .expect_err("three fragments cannot rebuild k=4");
        assert!(err.to_string().contains("need at least 4"));
    }

    /// A single corrupted fragment (one flipped byte) is caught by its checksum
    /// and treated as an erasure, so reassembly reconstructs the exact original
    /// from the good fragments — never silently wrong bytes (#220).
    #[test]
    fn corrupt_fragment_is_treated_as_erasure() {
        let original = body(4096 + 7);
        let encoded = encode(4, 2, &original).expect("encode");
        // Corrupt exactly one data fragment: flip a byte in fragment 0. Its
        // recorded checksum no longer matches its bytes.
        let mut fragments = encoded.fragments.clone();
        let mut corrupt = fragments[0].bytes.to_vec();
        corrupt[3] ^= 0xff;
        fragments[0].bytes = Bytes::from(corrupt);
        // The checksum still describes the ORIGINAL bytes, so the flip is
        // detectable — this is the whole point of storing it.
        assert_ne!(
            fragments[0].checksum,
            checksum(&fragments[0].bytes),
            "the flip makes the fragment fail its own checksum"
        );
        // Reassembly drops the corrupt fragment (m=2 parity covers it) and still
        // returns byte-identical output.
        let got = reassemble(encoded.geometry, encoded.object_len, &fragments).expect("reassemble");
        assert_eq!(got.as_ref(), original.as_slice(), "corrupt fragment erased");
    }

    /// More corrupt-or-missing fragments than `m` cannot be reconstructed: the
    /// read fails loudly rather than returning reconstructed garbage (#220).
    #[test]
    fn more_than_m_corrupt_fails_loudly() {
        let original = body(4096 + 7);
        let encoded = encode(4, 2, &original).expect("encode"); // m=2 tolerance
        let mut fragments = encoded.fragments.clone();
        // Corrupt three fragments (> m=2). Fewer than k=4 remain verifiable.
        for fragment in fragments.iter_mut().take(3) {
            let mut corrupt = fragment.bytes.to_vec();
            corrupt[0] ^= 0xff;
            fragment.bytes = Bytes::from(corrupt);
        }
        let err = reassemble(encoded.geometry, encoded.object_len, &fragments)
            .expect_err("three corrupt of k=4/m=2 cannot rebuild");
        assert!(
            err.to_string().contains("need at least 4"),
            "fails loudly, never returns wrong bytes: {err}"
        );
    }

    /// A rebuilt fragment (via `Fragment::new`) computes a checksum that matches
    /// its bytes, so a re-encode round-trips through verification.
    #[test]
    fn fragment_new_computes_matching_checksum() {
        let f = Fragment::new(1, Bytes::from_static(b"some fragment bytes"));
        assert_eq!(f.checksum, checksum(&f.bytes));
    }

    /// Zero data fragments is an invalid geometry; zero parity is the valid
    /// degenerate identity code used by single-node write-back (#286).
    #[test]
    fn rejects_invalid_geometry() {
        assert!(Geometry::for_object(0, 2, 100).is_err());
        assert!(
            Geometry::for_object(4, 0, 100).is_ok(),
            "m=0 is the identity code"
        );
    }

    /// The degenerate `k=1, m=0` identity code produces one fragment equal to the
    /// object (no parity, no redundancy) and reassembles it byte-identically —
    /// the single-node write-back coding (#286).
    #[test]
    fn identity_code_k1_m0_round_trips() {
        let original = body(5000);
        let encoded = encode(1, 0, &original).expect("identity encode");
        assert_eq!(encoded.fragments.len(), 1, "one fragment, no parity");
        assert_eq!(encoded.object_len, 5000);
        let got =
            reassemble(encoded.geometry, encoded.object_len, &encoded.fragments).expect("rebuild");
        assert_eq!(got.as_ref(), original.as_slice());
    }

    /// A wider `k, m=0` identity code splits the object into `k` data fragments
    /// with no parity and reassembles from all `k`.
    #[test]
    fn identity_code_k_m0_round_trips() {
        let original = body(4096 + 13);
        let encoded = encode(3, 0, &original).expect("identity encode");
        assert_eq!(encoded.fragments.len(), 3);
        let got =
            reassemble(encoded.geometry, encoded.object_len, &encoded.fragments).expect("rebuild");
        assert_eq!(got.as_ref(), original.as_slice());
    }

    /// An empty object still produces reconstructable fragments.
    #[test]
    fn handles_empty_object() {
        let encoded = encode(2, 1, &[]).expect("encode empty");
        assert_eq!(encoded.object_len, 0);
        let got =
            reassemble(encoded.geometry, encoded.object_len, &encoded.fragments).expect("rebuild");
        assert_eq!(got.len(), 0);
    }

    /// Streaming into a sink emits each stripe's k+m shards as the stripe
    /// completes, never holding more than one stripe of fragment bytes, and the
    /// concatenated per-fragment output matches the whole-object encode.
    #[test]
    fn stream_into_sink_matches_whole_encode() {
        let original = body(MAX_STRIPE_CHUNK * 4 * 3 + 77);
        let geometry = Geometry::streaming(4, 2, MAX_STRIPE_CHUNK).expect("geometry");

        // A sink that concatenates shards per fragment index, and records the
        // largest number of shard bytes it ever held for a single stripe.
        let mut collected: Vec<Vec<u8>> = vec![Vec::new(); geometry.total()];
        let mut max_stripe_bytes = 0usize;
        let mut encoder = StreamingStripeEncoder::new(geometry);
        let mut feed = |data: &[u8], collected: &mut Vec<Vec<u8>>, max: &mut usize| {
            encoder
                .push(data, |shards: &[Vec<u8>]| {
                    let mut stripe_bytes = 0;
                    for (index, shard) in shards.iter().enumerate() {
                        collected[index].extend_from_slice(shard);
                        stripe_bytes += shard.len();
                    }
                    *max = (*max).max(stripe_bytes);
                    Ok::<(), CodecError>(())
                })
                .expect("push");
        };
        let mut offset = 0;
        for step in [1, 7, 4095, MAX_STRIPE_CHUNK, 999_999] {
            let end = (offset + step).min(original.len());
            feed(
                &original[offset..end],
                &mut collected,
                &mut max_stripe_bytes,
            );
            offset = end;
        }
        feed(&original[offset..], &mut collected, &mut max_stripe_bytes);
        let object_len = encoder
            .finish(|shards: &[Vec<u8>]| {
                for (index, shard) in shards.iter().enumerate() {
                    collected[index].extend_from_slice(shard);
                }
                Ok::<(), CodecError>(())
            })
            .expect("finish");

        // The streamed output equals the whole-object encode.
        let whole = {
            let mut e = StripeEncoder::new(geometry);
            e.push(&original).expect("push");
            e.finish().expect("finish")
        };
        assert_eq!(object_len, whole.object_len);
        for (index, fragment) in whole.fragments.iter().enumerate() {
            assert_eq!(
                collected[index].as_slice(),
                fragment.bytes.as_ref(),
                "fragment {index} streamed bytes match whole encode"
            );
        }
        // The sink never saw more than one stripe of shards at a time
        // (k+m shards, each `chunk` bytes).
        assert_eq!(max_stripe_bytes, geometry.total() * geometry.chunk);
    }
}
