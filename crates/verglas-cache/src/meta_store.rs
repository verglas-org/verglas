//! The dedicated metadata store (#50): a second, small foyer **hybrid**
//! instance holding the raw bytes of table metadata — `metadata.json`,
//! manifest lists, manifests, and Parquet footers — in its **own eviction
//! domain**, hard isolated from data-block pressure. A full-table scan flowing
//! through the big hybrid cache structurally *cannot* evict a footer here:
//! they do not share a cache, a weighter, or a shard.
//!
//! # DRAM front + NVMe long tail
//!
//! Metadata is ~1% of a table's bytes and disproportionately valuable, so the
//! store is hybrid: a small DRAM front for the hot planning set and an NVMe
//! tier (carved from inside the engine's disk ceiling) for the long tail and
//! warm restarts. Metadata objects are KB-scale, so the disk tier uses a
//! [`META_DISK_BLOCK_BYTES`]-sized region — far smaller than the data store's
//! MiB-scale regions — which is what lets a few-MB metadata budget still hold the
//! four regions foyer's engine needs. An entry larger than one region cannot
//! persist to disk (foyer skips it); it stays served from the DRAM front —
//! pinning still holds, only the restart warmth is lost for that entry.
//!
//! Entries are keyed by [`MetaEntryKey`]: block-aligned metadata objects use
//! the same `(object, etag, block_bytes, block_index)` identity as data blocks, and Parquet
//! footers are pinned at **suffix granularity** — the exact tail bytes an
//! engine's speculative footer read fetches, keyed `(object, etag, len)`. That
//! is what keeps a sub-data-block-size data file's *body* out of this store: only the
//! footer bytes are pinned, never block 0 of the file.

use std::io::{Read, Write};
use std::path::Path;

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Code, DeviceBuilder, Error as FoyerError, FileDeviceBuilder, HybridCache,
    HybridCacheBuilder, RecoverMode, Result as FoyerResult,
};
use verglas_core::{BlockKey, CacheKey};

use crate::entry::BlockEntryKey;

/// Per-entry DRAM bookkeeping charged on top of the encoded entry size, so the
/// store's configured capacity is a byte ceiling rather than an entry-count
/// ceiling. Matches the data store's accounting.
const ENTRY_OVERHEAD_BYTES: usize = 256;

/// Disk region size for the metadata tier. Metadata objects are KB-scale, so
/// 1 MiB regions keep a few-MB disk budget above foyer's four-region floor
/// while still fitting every realistic manifest/footer; a rare entry above
/// ~1 MiB stays DRAM-only (see module docs).
pub(crate) const META_DISK_BLOCK_BYTES: usize = 1024 * 1024;

/// Maximum number of Foyer logical regions per disk tier. Foyer creates every
/// region at startup, so this keeps multi-TiB cache construction bounded.
const MAX_FOYER_PARTITIONS: usize = 32_768;

/// Smallest useful metadata disk tier: foyer's engine needs several regions to
/// rotate through. Carved from inside the engine's disk ceiling.
pub(crate) const META_DISK_MIN_BYTES: u64 = 4 * META_DISK_BLOCK_BYTES as u64;

/// DRAM the metadata tier's disk-write pipeline reserves (buffer pool + submit
/// queue below), accounted inside the engine's DRAM ceiling exactly like the
/// data store's pipeline reservation.
pub(crate) const META_PIPELINE_RESERVED_BYTES: usize = 2 * 1024 * 1024;
/// Flush buffer pool for the metadata tier (small regions, low write rate).
const META_FLUSH_BUFFER_BYTES: usize = 1024 * 1024;
/// Submit queue ceiling for the metadata tier.
const META_SUBMIT_QUEUE_BYTES: usize = 1024 * 1024;

/// Returns a Foyer region size that can hold one minimum entry and bounds the
/// number of regions Foyer creates for a tier.
pub(crate) fn foyer_block_size(capacity: usize, minimum: usize) -> usize {
    capacity.div_ceil(MAX_FOYER_PARTITIONS).max(minimum)
}

/// Identity of one pinned metadata entry. Every variant carries the cache
/// **generation** it was pinned under (issue #178): a purge bumps the engine's
/// generation, so old-generation entries become instantly unreachable without a
/// racy `clear()` on this store (see [`BlockEntryKey`] for the mechanism).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum MetaEntryKey {
    /// A block-aligned metadata object (metadata.json, manifest list,
    /// manifest): same identity as a data block, plus the generation.
    Block {
        /// The logical block identity.
        block: BlockKey,
        /// The cache generation this entry was pinned under.
        generation: u64,
    },
    /// A Parquet footer pinned at suffix granularity: the exact last `len`
    /// bytes of `(object, etag)` — never the file's body.
    Suffix {
        /// The object whose tail is pinned.
        object: CacheKey,
        /// The version the bytes belong to (ETag discipline, #14).
        etag: String,
        /// The suffix length the engine requested.
        len: u64,
        /// The cache generation this entry was pinned under.
        generation: u64,
    },
}

/// Tag bytes distinguishing the two key shapes on disk.
const TAG_BLOCK: u8 = 0;
/// See [`TAG_BLOCK`].
const TAG_SUFFIX: u8 = 1;

impl Code for MetaEntryKey {
    /// Encodes a tag byte then the variant's fields, length-prefixed —
    /// faithful inverse of [`Code::decode`] for disk recovery.
    fn encode(&self, writer: &mut impl Write) -> FoyerResult<()> {
        match self {
            MetaEntryKey::Block { block, generation } => {
                writer
                    .write_all(&[TAG_BLOCK])
                    .map_err(FoyerError::io_error)?;
                BlockEntryKey {
                    block: block.clone(),
                    generation: *generation,
                }
                .encode(writer)
            }
            MetaEntryKey::Suffix {
                object,
                etag,
                len,
                generation,
            } => {
                writer
                    .write_all(&[TAG_SUFFIX])
                    .map_err(FoyerError::io_error)?;
                crate::entry::write_bytes(writer, object.bucket.as_bytes())?;
                crate::entry::write_bytes(writer, object.key.as_bytes())?;
                crate::entry::write_bytes(writer, etag.as_bytes())?;
                writer
                    .write_all(&len.to_le_bytes())
                    .map_err(FoyerError::io_error)?;
                writer
                    .write_all(&generation.to_le_bytes())
                    .map_err(FoyerError::io_error)
            }
        }
    }

    /// Decodes what [`Code::encode`] wrote.
    fn decode(reader: &mut impl Read) -> FoyerResult<Self> {
        let mut tag = [0u8; 1];
        reader.read_exact(&mut tag).map_err(FoyerError::io_error)?;
        match tag[0] {
            TAG_BLOCK => {
                let entry = BlockEntryKey::decode(reader)?;
                Ok(MetaEntryKey::Block {
                    block: entry.block,
                    generation: entry.generation,
                })
            }
            _ => {
                let bucket = crate::entry::read_string(reader)?;
                let key = crate::entry::read_string(reader)?;
                let etag = crate::entry::read_string(reader)?;
                let mut len = [0u8; 8];
                reader.read_exact(&mut len).map_err(FoyerError::io_error)?;
                let mut generation = [0u8; 8];
                reader
                    .read_exact(&mut generation)
                    .map_err(FoyerError::io_error)?;
                Ok(MetaEntryKey::Suffix {
                    object: CacheKey { bucket, key },
                    etag,
                    len: u64::from_le_bytes(len),
                    generation: u64::from_le_bytes(generation),
                })
            }
        }
    }

    /// Serialized size, exact: the tag plus the variant's encoding.
    fn estimated_size(&self) -> usize {
        1 + match self {
            MetaEntryKey::Block { block, generation } => BlockEntryKey {
                block: block.clone(),
                generation: *generation,
            }
            .estimated_size(),
            MetaEntryKey::Suffix { object, etag, .. } => {
                8 + object.bucket.len() + 8 + object.key.len() + 8 + etag.len() + 8 + 8
            }
        }
    }
}

/// The dedicated metadata store: a small foyer hybrid (DRAM front + NVMe long
/// tail) in its own eviction domain. Cheap to share by `Arc` inside the engine.
pub(crate) struct MetaStore {
    /// The hybrid cache of pinned metadata entries.
    blocks: HybridCache<MetaEntryKey, Bytes>,
}

impl MetaStore {
    /// Builds a metadata store with `dram_bytes` of memory front and
    /// `disk_bytes` of NVMe tier in `path`. Both budgets are carved from
    /// *inside* the engine's ceilings (see the engine's budget arithmetic),
    /// never added on top.
    pub(crate) async fn new(
        dram_bytes: usize,
        disk_bytes: usize,
        path: &Path,
    ) -> Result<MetaStore, String> {
        let device = FileDeviceBuilder::new(path)
            .with_capacity(disk_bytes)
            .build()
            .map_err(|e| e.to_string())?;
        let blocks = HybridCacheBuilder::new()
            .with_name("verglas-meta-store")
            // Keep Foyer's memory-stable WriteOnEviction policy. `insert`
            // explicitly enqueues every entry to storage as well, preserving
            // unclean-restart durability without letting storage churn demote
            // the pinned DRAM planning set.
            .memory(dram_bytes)
            // One shard: the store is small and low-traffic (planning reads,
            // not the data hot path), and foyer evicts per shard — one shard
            // makes the configured capacity a single, exact eviction domain
            // (the #122 stranding hazard cannot apply).
            .with_shards(1)
            // Charge each entry its exact encoded size plus fixed bookkeeping,
            // identical to the data store, so the capacity is a byte ceiling.
            .with_weighter(|key: &MetaEntryKey, value: &Bytes| {
                key.estimated_size() + value.len() + ENTRY_OVERHEAD_BYTES
            })
            .storage()
            // Disk-tier recovery on startup (#16): rebuild the index from the
            // on-disk Foyer metadata, dropping any checksum-failed (torn) tail
            // entry silently. Pinned explicitly so it can never regress to
            // `None` and leave metadata cold after every restart.
            .with_recover_mode(RecoverMode::Quiet)
            .with_engine_config(
                BlockEngineConfig::new(device)
                    .with_block_size(foyer_block_size(disk_bytes, META_DISK_BLOCK_BYTES))
                    .with_buffer_pool_size(META_FLUSH_BUFFER_BYTES)
                    .with_submit_queue_size_threshold(META_SUBMIT_QUEUE_BYTES),
            )
            .build()
            .await
            .map_err(|e| e.to_string())?;
        Ok(MetaStore { blocks })
    }

    /// Looks up a pinned metadata entry — DRAM front first, NVMe tail second.
    pub(crate) async fn get(&self, key: &MetaEntryKey) -> Option<Bytes> {
        self.blocks
            .get(key)
            .await
            .ok()
            .flatten()
            .map(|entry| entry.value().clone())
    }

    /// Pins a metadata entry. Metadata is always admitted (it bypasses the
    /// scan-resistant admission policy entirely — tiny and disproportionately
    /// valuable, #15), so there is no admission gate here by construction.
    pub(crate) fn insert(&self, key: MetaEntryKey, value: Bytes) {
        let entry = self.blocks.insert(key, value);
        self.blocks.storage().enqueue(entry.piece(), false);
    }

    /// Waits for every queued metadata write to reach the NVMe tier.
    pub(crate) async fn flush(&self) {
        self.blocks.storage().wait().await;
    }

    /// Physically removes one pinned entry from both tiers (#305): the
    /// grace-window hard-eviction sweep drops a retired metadata object's
    /// blocks so their space returns to the shared budget. A per-key remove,
    /// never a `clear()` (see the module note on foyer-rs/foyer#1305). Off the
    /// hot path.
    pub(crate) fn remove(&self, key: &MetaEntryKey) {
        self.blocks.remove(key);
    }

    /// DRAM bytes currently resident in the memory front, as charged by the
    /// weighter — the metadata store's contribution to the DRAM ceiling gauge.
    pub(crate) fn usage(&self) -> u64 {
        self.blocks.memory().usage() as u64
    }

    // No `clear()`: purge is a generation-epoch bump, not a physical wipe (issue
    // #178). `MetaEntryKey` carries the generation, so a purge makes every pinned
    // entry unreachable at O(1) and LRU reclaims them lazily. foyer's `clear()`
    // is banned on this store because it panics against a concurrent `insert`
    // (foyer-rs/foyer#1305) — the exact race this rework removes by construction.
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `MetaEntryKey` codec (foyer's on-disk framing) must round-trip both
    /// variants exactly — including the #178 generation — and `estimated_size`
    /// must equal the encoded length (the DRAM weighter charges it). Restart
    /// recovery decodes this framing to rebuild the pinned-metadata index, so an
    /// old-generation entry must decode with its old generation and stay
    /// unreachable against the current one.
    fn round_trip(key: &MetaEntryKey) {
        let mut buf = Vec::new();
        key.encode(&mut buf).expect("encode");
        assert_eq!(
            buf.len(),
            key.estimated_size(),
            "estimated_size must equal the encoded length for {key:?}"
        );
        let decoded = MetaEntryKey::decode(&mut buf.as_slice()).expect("decode");
        assert_eq!(&decoded, key, "decode must invert encode");
    }

    #[test]
    fn meta_entry_keys_round_trip_with_generation() {
        let block = BlockKey {
            object: CacheKey {
                bucket: "warehouse".to_owned(),
                key: "db/t/metadata/v3.metadata.json".to_owned(),
            },
            etag: "\"abc\"".to_owned(),
            block_bytes: 2 * 1024 * 1024,
            block_index: 0,
        };
        round_trip(&MetaEntryKey::Block {
            block: block.clone(),
            generation: 7,
        });
        round_trip(&MetaEntryKey::Suffix {
            object: CacheKey {
                bucket: "warehouse".to_owned(),
                key: "db/t/data/f.parquet".to_owned(),
            },
            etag: "\"def\"".to_owned(),
            len: 64 * 1024,
            generation: 7,
        });

        // Two suffix keys differing only by generation encode differently and are
        // distinct identities — the restart-correctness property.
        let base = MetaEntryKey::Suffix {
            object: CacheKey {
                bucket: "b".to_owned(),
                key: "k".to_owned(),
            },
            etag: "\"e\"".to_owned(),
            len: 128,
            generation: 0,
        };
        let bumped = MetaEntryKey::Suffix {
            object: CacheKey {
                bucket: "b".to_owned(),
                key: "k".to_owned(),
            },
            etag: "\"e\"".to_owned(),
            len: 128,
            generation: 1,
        };
        assert_ne!(base, bumped, "generation is part of key identity");
        let (mut a, mut b) = (Vec::new(), Vec::new());
        base.encode(&mut a).expect("encode");
        bumped.encode(&mut b).expect("encode");
        assert_ne!(a, b, "generation must change the on-disk encoding");
    }
}
