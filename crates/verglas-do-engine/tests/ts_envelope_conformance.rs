//! Cross-language conformance: a canonical TransactionEnvelope encoded by the
//! TypeScript SDK (`sdks/typescript/src/do-protocol.ts`) must decode byte-exactly
//! in the engine, proving the SDK's COMMIT payloads are real engine envelopes.

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use verglas_do_engine::{IsolationLevel, MutationDomain, MutationKind, TransactionEnvelope};

/// Hex bytes produced by the TypeScript encoder for a fixed two-row upsert
/// transaction with one schema declaration. Regenerate with:
/// `npx tsx -e "import {encodeCanonicalTransaction, encodeHex} from './src/do-protocol.ts'; ..."`
/// from `sdks/typescript` using the exact field values asserted below.
const TS_ENVELOPE_HEX: &str = "0e00000000000000636f6e666f726d616e63652d646f018f3b2a1c4d4e5f8a9b0c1d2e3f4a5b070000000000000002010000000000000006000000000000006576656e7473b000000000000000ffffffffa00000001000000000000a000c000a00090004000a0000001000000000010400080008000000040008000000040000000200000044000000100000000c0010000c000b000a0004000c0000001c0000000000050104000000050000006c6162656c00000004000400040000000c0010000c0000000b0004000c0000001c0000000000000204000000020000006964000008000c0008000700080000000000000140000000ffffffff000000000100000000000000030106000000000000006576656e7473b001000000000000ffffffffa00000001000000000000a000c000a00090004000a0000001000000000010400080008000000040008000000040000000200000044000000100000000c0010000c000b000a0004000c0000001c0000000000050104000000050000006c6162656c00000004000400040000000c0010000c0000000b0004000c0000001c0000000000000204000000020000006964000008000c0008000700080000000000000140000000ffffffffc800000014000000000000000c001600140013000c0004000c0000003000000000000000140000000000000304000a0018000c00080004000a0000003c00000010000000020000000000000000000000020000000200000000000000000000000000000002000000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000010000000000000001000000000000000000000000000000010000000000000000c00000000000000200000000000000009000000000000000100000000000000020000000000000000000000050000000900000000000000616c7068616265746100000000000000ffffffff00000000";

/// Decodes the TypeScript-produced envelope and asserts every semantic field
/// the SDK claims to encode, including Arrow row contents.
#[test]
fn typescript_envelope_decodes_byte_exactly_in_the_engine() {
    let bytes = hex::decode(TS_ENVELOPE_HEX).expect("fixture hex must decode");
    let envelope =
        TransactionEnvelope::from_canonical_bytes(&bytes).expect("engine must accept TS envelope");

    assert_eq!(envelope.do_id(), "conformance-do");
    assert_eq!(
        envelope.transaction_id().to_string(),
        "018f3b2a-1c4d-4e5f-8a9b-0c1d2e3f4a5b"
    );
    assert_eq!(envelope.base_commit_sequence(), 7);
    assert_eq!(envelope.isolation(), IsolationLevel::Serializable);

    let schemas = envelope.schema_changes();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].table().as_str(), "events");
    let schema = schemas[0].schema();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "label");

    let mutations = envelope.mutations();
    assert_eq!(mutations.len(), 1);
    let mutation = &mutations[0];
    assert_eq!(mutation.kind(), MutationKind::Upsert);
    assert_eq!(mutation.domain(), MutationDomain::Relational);
    assert_eq!(mutation.table().as_str(), "events");

    let batch = mutation.batch();
    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<Int64Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    let labels = batch.column(1).as_string::<i32>();
    assert_eq!(labels.value(0), "alpha");
    assert_eq!(labels.value(1), "beta");
}
