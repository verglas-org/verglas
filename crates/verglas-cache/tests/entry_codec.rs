//! Round-trip tests for the block-key codec (`Code` impl): the encoding is
//! foyer's on-disk framing, and recovery decodes it to rebuild the index, so
//! encode/decode must be exact inverses and `estimated_size` must match the
//! encoded length exactly (the DRAM weighter charges it).

use foyer::Code;
use verglas_cache::entry::BlockEntryKey;
use verglas_core::{BlockKey, CacheKey};

/// Encodes, asserts the exact size, decodes, and asserts equality.
fn round_trip(value: &BlockEntryKey) {
    let mut buf = Vec::new();
    value.encode(&mut buf).expect("encode");
    assert_eq!(
        buf.len(),
        value.estimated_size(),
        "estimated_size must equal the encoded length for {value:?}"
    );
    let decoded = BlockEntryKey::decode(&mut buf.as_slice()).expect("decode");
    assert_eq!(&decoded, value, "decode must invert encode");
}

#[test]
fn block_keys_round_trip() {
    round_trip(&BlockEntryKey {
        block: BlockKey {
            object: CacheKey {
                storage_binding_id: "default".to_owned(),
                bucket: "lake".to_owned(),
                key: "warehouse/db/t/data/part-00000.parquet".to_owned(),
            },
            etag: "\"9bb58f26192e4ba00f01e2e7b136bbd8\"".to_owned(),
            block_bytes: 2 * 1024 * 1024,
            block_index: u64::MAX,
        },
        generation: 42,
    });
    // Degenerate fields still frame unambiguously (length prefixes).
    round_trip(&BlockEntryKey {
        block: BlockKey {
            object: CacheKey {
                storage_binding_id: "default".to_owned(),
                bucket: String::new(),
                key: String::new(),
            },
            etag: String::new(),
            block_bytes: 0,
            block_index: 0,
        },
        generation: 0,
    });
}

/// The generation is part of the on-disk framing (issue #178): two keys that
/// differ only by generation must encode differently and each round-trip to
/// itself. This is what makes restart recovery correct — an entry written under
/// an old generation recovers with that old generation and so stays an
/// unreachable miss against the current generation.
#[test]
fn generation_is_encoded_and_distinguishes_keys() {
    let base = BlockKey {
        object: CacheKey {
            storage_binding_id: "default".to_owned(),
            bucket: "lake".to_owned(),
            key: "t/data/part.parquet".to_owned(),
        },
        etag: "\"e\"".to_owned(),
        block_bytes: 2 * 1024 * 1024,
        block_index: 3,
    };
    let gen0 = BlockEntryKey {
        block: base.clone(),
        generation: 0,
    };
    let gen1 = BlockEntryKey {
        block: base,
        generation: 1,
    };
    round_trip(&gen0);
    round_trip(&gen1);
    let mut a = Vec::new();
    let mut b = Vec::new();
    gen0.encode(&mut a).expect("encode");
    gen1.encode(&mut b).expect("encode");
    assert_ne!(a, b, "generation must change the on-disk encoding");
    assert_ne!(gen0, gen1, "generation is part of key identity");
}

#[test]
fn storage_binding_is_encoded_and_distinguishes_blocks() {
    let managed = BlockEntryKey {
        block: BlockKey {
            object: CacheKey {
                storage_binding_id: "managed".to_owned(),
                bucket: "lake".to_owned(),
                key: "same.parquet".to_owned(),
            },
            etag: "\"same-etag\"".to_owned(),
            block_bytes: 2 * 1024 * 1024,
            block_index: 0,
        },
        generation: 0,
    };
    let customer = BlockEntryKey {
        block: BlockKey {
            object: CacheKey {
                storage_binding_id: "customer".to_owned(),
                ..managed.block.object.clone()
            },
            ..managed.block.clone()
        },
        generation: managed.generation,
    };
    let (mut managed_bytes, mut customer_bytes) = (Vec::new(), Vec::new());
    managed.encode(&mut managed_bytes).expect("managed encode");
    customer
        .encode(&mut customer_bytes)
        .expect("customer encode");
    assert_ne!(managed_bytes, customer_bytes);
    round_trip(&managed);
    round_trip(&customer);
}

#[test]
fn truncated_input_fails_to_decode() {
    let key = BlockEntryKey {
        block: BlockKey {
            object: CacheKey {
                storage_binding_id: "default".to_owned(),
                bucket: "lake".to_owned(),
                key: "k".to_owned(),
            },
            etag: "\"e\"".to_owned(),
            block_bytes: 2 * 1024 * 1024,
            block_index: 7,
        },
        generation: 5,
    };
    let mut buf = Vec::new();
    key.encode(&mut buf).expect("encode");
    let truncated = &buf[..buf.len() - 1];
    assert!(BlockEntryKey::decode(&mut &truncated[..]).is_err());
}
