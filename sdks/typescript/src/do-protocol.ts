//! Canonical Durable Object endpoint commands and transaction envelopes.
//!
//! The worker socket accepts REGISTER, QUERY, and COMMIT lines. COMMIT carries
//! the exact length-prefixed envelope consumed by verglas-do-engine, including
//! Arrow IPC schema and mutation streams.

import { encodeArrowSchema, encodeArrowStream, type ArrowColumn } from "./arrow-ipc";

/** A schema declaration included in a canonical transaction envelope. */
export interface CanonicalSchemaChange {
  /** SQL-visible table name. */
  table: string;
  /** Arrow schema fields. */
  columns: ArrowColumn[];
}

/** Mutation operation tags accepted by the engine envelope. */
export type CanonicalMutationKind = "insert" | "replace" | "upsert";

/** Mutation state domains accepted by the engine envelope. */
export type CanonicalMutationDomain = "relational" | "vector" | "graph";

/** One Arrow mutation included in a canonical commit. */
export interface CanonicalMutation {
  /** Atomic operation applied to the table. */
  kind: CanonicalMutationKind;
  /** Engine state domain updated by this mutation. */
  domain: CanonicalMutationDomain;
  /** SQL-visible table name. */
  table: string;
  /** Arrow columns matching each row object. */
  columns: ArrowColumn[];
  /** Rows encoded as one Arrow record batch. */
  rows: Record<string, unknown>[];
}

/** Fields needed to serialize one engine TransactionEnvelope. */
export interface CanonicalTransactionEnvelope {
  /** Durable Object ID string owned by the worker endpoint. */
  doId: string;
  /** Retry-stable UUID in canonical textual form or 16 raw bytes. */
  transactionId: string | Uint8Array;
  /** Commit sequence that formed the transaction's snapshot. */
  baseCommitSequence: number | bigint;
  /** Snapshot or serializable validation policy. */
  isolation?: "snapshot" | "serializable";
  /** Durable table declarations before mutations. */
  schemaChanges?: CanonicalSchemaChange[];
  /** Mutations in SQL statement order. */
  mutations?: CanonicalMutation[];
}

const domainTag: Record<CanonicalMutationDomain, number> = { relational: 1, vector: 2, graph: 3 };
const kindTag: Record<CanonicalMutationKind, number> = { insert: 1, replace: 2, upsert: 3 };

/** Encodes one exact engine TransactionEnvelope canonical byte sequence. */
export function encodeCanonicalTransaction(envelope: CanonicalTransactionEnvelope): Uint8Array {
  const output: number[] = [];
  putBytes(output, new TextEncoder().encode(envelope.doId));
  const transactionId = typeof envelope.transactionId === "string"
    ? parseUuid(envelope.transactionId)
    : envelope.transactionId;
  if (transactionId.length !== 16) throw new Error("transaction ID must contain exactly 16 bytes");
  output.push(...transactionId);
  putU64(output, envelope.baseCommitSequence);
  output.push(envelope.isolation === "serializable" ? 2 : 1);
  const schemaChanges = envelope.schemaChanges ?? [];
  putU64(output, schemaChanges.length);
  for (const schema of schemaChanges) {
    putBytes(output, new TextEncoder().encode(schema.table));
    putBytes(output, encodeArrowSchema(schema.columns));
  }
  const mutations = envelope.mutations ?? [];
  putU64(output, mutations.length);
  for (const mutation of mutations) {
    output.push(kindTag[mutation.kind], domainTag[mutation.domain]);
    putBytes(output, new TextEncoder().encode(mutation.table));
    putBytes(output, encodeArrowStream(mutation.columns, mutation.rows));
  }
  return Uint8Array.from(output);
}

/** Converts one engine command payload into lower-case hexadecimal text. */
export function encodeHex(bytes: Uint8Array): string {
  let output = "";
  for (const byte of bytes) output += byte.toString(16).padStart(2, "0");
  return output;
}

/** Converts UTF-8 SQL text into the endpoint's hex command token. */
export function encodeUtf8Hex(value: string): string {
  return encodeHex(new TextEncoder().encode(value));
}

/** Writes an engine u64 length-prefixed byte string. */
function putBytes(output: number[], bytes: Uint8Array): void {
  putU64(output, bytes.length);
  output.push(...bytes);
}

/** Writes a little-endian u64 without relying on Node Buffer APIs. */
function putU64(output: number[], value: number | bigint): void {
  let remaining = BigInt(value);
  if (remaining < 0n) throw new Error("canonical u64 values cannot be negative");
  for (let i = 0; i < 8; i += 1) {
    output.push(Number(remaining & 0xffn));
    remaining >>= 8n;
  }
  if (remaining !== 0n) throw new Error("canonical u64 value exceeds 64 bits");
}

/** Parses a hyphenated or compact UUID into the engine's 16-byte order. */
function parseUuid(value: string): Uint8Array {
  const compact = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(compact)) throw new Error(`invalid transaction UUID: ${value}`);
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 1) bytes[i] = Number.parseInt(compact.slice(i * 2, i * 2 + 2), 16);
  return bytes;
}
