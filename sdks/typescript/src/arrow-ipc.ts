//! Minimal Arrow IPC stream codec for the Durable Objects worker endpoint.
//!
//! The endpoint speaks Arrow IPC streams, not JSON. This module implements the
//! primitive column types used by Durable Object storage without adding an
//! Arrow runtime dependency to the SDK.

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/** Primitive Arrow types supported by the worker storage bridge. */
export type ArrowPrimitiveType = "utf8" | "int32" | "int64" | "float64" | "bool";

/** One top-level Arrow column and its row values. */
export interface ArrowColumn {
  /** SQL-visible column name. */
  name: string;
  /** Arrow physical type. */
  type: ArrowPrimitiveType;
  /** Whether null values are allowed. */
  nullable?: boolean;
  /** Values in row order. */
  values?: unknown[];
}

/** Decoded primitive Arrow stream. */
export interface DecodedArrowStream {
  /** Column names in schema order. */
  columns: ArrowColumn[];
  /** Rows represented as objects keyed by column name. */
  rows: Record<string, unknown>[];
  /** Number of rows read from the record batches. */
  rowsRead: number;
  /** Number of rows written, always zero for a query response. */
  rowsWritten: number;
}

/** Encodes arbitrary bytes as a lower-case hexadecimal command token. */
export function encodeHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** Encodes UTF-8 SQL as the endpoint QUERY token. */
export function encodeUtf8Hex(value: string): string {
  return encodeHex(textEncoder.encode(value));
}

/** A tiny backwards FlatBuffers builder sufficient for Arrow metadata tables. */
class FlatBufferBuilder {
  #bytes: Uint8Array;
  #space: number;
  #minimumAlignment = 1;
  #vtable: number[] | undefined;
  #vtableFields = 0;
  #nested = false;
  #objectStart = 0;
  #vectorElements = 0;

  /** Creates a builder with a growable backing buffer. */
  constructor(initialSize = 1024) {
    this.#bytes = new Uint8Array(initialSize);
    this.#space = initialSize;
  }

  /** Returns the finished FlatBuffer bytes. */
  finish(rootOffset: number): Uint8Array {
    this.#prep(this.#minimumAlignment, 4);
    this.#addOffset(rootOffset);
    return this.#bytes.slice(this.#space);
  }

  /** Returns the current backwards offset. */
  offset(): number {
    return this.#bytes.length - this.#space;
  }

  /** Adds a signed byte with FlatBuffers alignment. */
  addInt8(value: number): void {
    this.#prep(1, 0);
    this.#writeInt8(value);
  }

  /** Adds a signed 16-bit scalar with FlatBuffers alignment. */
  addInt16(value: number): void {
    this.#prep(2, 0);
    this.#writeInt16(value);
  }

  /** Adds a signed 32-bit scalar with FlatBuffers alignment. */
  addInt32(value: number): void {
    this.#prep(4, 0);
    this.#writeInt32(value);
  }

  /** Adds a signed 64-bit scalar with FlatBuffers alignment. */
  addInt64(value: bigint): void {
    this.#prep(8, 0);
    this.#writeInt64(value);
  }

  /** Starts a FlatBuffers table with the declared number of fields. */
  startObject(fieldCount: number): void {
    if (this.#nested) throw new Error("Arrow metadata object is nested");
    this.#vtable = Array.from({ length: fieldCount }, () => 0);
    this.#vtableFields = fieldCount;
    this.#nested = true;
    this.#objectStart = this.offset();
  }

  /** Records the current location for one table field. */
  slot(field: number): void {
    if (this.#vtable) this.#vtable[field] = this.offset();
  }

  /** Adds a scalar field when it differs from its FlatBuffers default. */
  addFieldInt8(field: number, value: number, defaultValue: number): void {
    if (value !== defaultValue) {
      this.addInt8(value);
      this.slot(field);
    }
  }

  /** Adds a 16-bit scalar field when it differs from its default. */
  addFieldInt16(field: number, value: number, defaultValue: number): void {
    if (value !== defaultValue) {
      this.addInt16(value);
      this.slot(field);
    }
  }

  /** Adds a 32-bit scalar field when it differs from its default. */
  addFieldInt32(field: number, value: number, defaultValue: number): void {
    if (value !== defaultValue) {
      this.addInt32(value);
      this.slot(field);
    }
  }

  /** Adds a 64-bit scalar field when it differs from its default. */
  addFieldInt64(field: number, value: bigint, defaultValue: bigint): void {
    if (value !== defaultValue) {
      this.addInt64(value);
      this.slot(field);
    }
  }

  /** Adds an offset field when it differs from its default. */
  addFieldOffset(field: number, value: number, defaultValue = 0): void {
    if (value !== defaultValue) {
      this.#addOffset(value);
      this.slot(field);
    }
  }

  /** Ends the current table and returns its offset. */
  endObject(): number {
    if (!this.#nested || !this.#vtable) throw new Error("Arrow metadata table is not open");
    this.addInt32(0);
    const vtableLocation = this.offset();
    let last = this.#vtableFields - 1;
    while (last >= 0 && this.#vtable[last] === 0) last -= 1;
    for (let i = last; i >= 0; i -= 1) {
      this.addInt16(this.#vtable[i] === 0 ? 0 : vtableLocation - this.#vtable[i]);
    }
    this.addInt16(vtableLocation - this.#objectStart);
    this.addInt16((last + 3) * 2);
    const tableLocation = this.#bytes.length - vtableLocation;
    const vtableOffset = this.offset() - vtableLocation;
    this.#writeInt32At(tableLocation, vtableOffset);
    this.#nested = false;
    this.#vtable = undefined;
    return vtableLocation;
  }

  /** Starts a scalar or offset vector. */
  startVector(elementSize: number, count: number, alignment: number): void {
    if (this.#nested) throw new Error("Arrow metadata vector is nested");
    this.#vectorElements = count;
    this.#prep(4, elementSize * count);
    this.#prep(alignment, elementSize * count);
  }

  /** Ends the current vector and returns its offset. */
  endVector(): number {
    this.#writeInt32(this.#vectorElements);
    return this.offset();
  }

  /** Writes an offset element into a vector or table. */
  addOffset(offset: number): void {
    this.#addOffset(offset);
  }

  /** Writes a UTF-8 string object. */
  createString(value: string): number {
    const bytes = textEncoder.encode(value);
    this.addInt8(0);
    this.startVector(1, bytes.length, 1);
    this.#space -= bytes.length;
    this.#bytes.set(bytes, this.#space);
    return this.endVector();
  }

  /** Writes one inline Arrow struct into a vector. */
  createFieldNode(length: number, nullCount: number): number {
    this.#prep(8, 16);
    this.#writeInt64(BigInt(nullCount));
    this.#writeInt64(BigInt(length));
    return this.offset();
  }

  /** Writes one inline Arrow buffer descriptor into a vector. */
  createBuffer(offset: number, length: number): number {
    this.#prep(8, 16);
    this.#writeInt64(BigInt(length));
    this.#writeInt64(BigInt(offset));
    return this.offset();
  }

  /** Aligns and reserves one scalar or vector. */
  #prep(size: number, additional: number): void {
    if (size > this.#minimumAlignment) this.#minimumAlignment = size;
    const alignment = (~(this.#bytes.length - this.#space + additional) + 1) & (size - 1);
    while (this.#space < alignment + size + additional) this.#grow();
    for (let i = 0; i < alignment; i += 1) this.#bytes[--this.#space] = 0;
  }

  /** Doubles the backing buffer while preserving backwards offsets. */
  #grow(): void {
    const old = this.#bytes;
    const next = new Uint8Array(old.length * 2);
    const used = old.length - this.#space;
    next.set(old.subarray(this.#space), next.length - used);
    this.#space += next.length - old.length;
    this.#bytes = next;
  }

  /** Writes one byte at the backwards cursor. */
  #writeInt8(value: number): void {
    this.#bytes[--this.#space] = value & 0xff;
  }

  /** Writes one little-endian 16-bit value at the backwards cursor. */
  #writeInt16(value: number): void {
    this.#space -= 2;
    new DataView(this.#bytes.buffer).setInt16(this.#space, value, true);
  }

  /** Writes one little-endian 32-bit value at the backwards cursor. */
  #writeInt32(value: number): void {
    this.#space -= 4;
    new DataView(this.#bytes.buffer).setInt32(this.#space, value, true);
  }

  /** Writes one little-endian 64-bit value at the backwards cursor. */
  #writeInt64(value: bigint): void {
    this.#space -= 8;
    new DataView(this.#bytes.buffer).setBigInt64(this.#space, value, true);
  }

  /** Writes one little-endian 32-bit value at an existing table location. */
  #writeInt32At(position: number, value: number): void {
    new DataView(this.#bytes.buffer).setInt32(position, value, true);
  }

  /** Writes a relative offset from the current write location. */
  #addOffset(offset: number): void {
    this.#prep(4, 0);
    this.#writeInt32(this.offset() - offset + 4);
  }
}

const arrowTypeTag: Record<ArrowPrimitiveType, number> = {
  utf8: 5,
  int32: 2,
  int64: 2,
  float64: 3,
  bool: 6,
};

/** Encodes a primitive type table used by an Arrow Field union. */
function createType(builder: FlatBufferBuilder, type: ArrowPrimitiveType): number {
  if (type === "int32" || type === "int64") {
    builder.startObject(2);
    builder.addFieldInt32(0, type === "int64" ? 64 : 32, 0);
    builder.addFieldInt8(1, 1, 0);
    return builder.endObject();
  }
  if (type === "float64") {
    builder.startObject(1);
    builder.addFieldInt16(0, 2, 0);
    return builder.endObject();
  }
  builder.startObject(0);
  return builder.endObject();
}

/** Encodes one Arrow Field metadata table. */
function createField(builder: FlatBufferBuilder, column: ArrowColumn, typeOffset: number): number {
  const name = builder.createString(column.name);
  builder.startObject(7);
  builder.addFieldOffset(0, name);
  builder.addFieldInt8(1, column.nullable === false ? 0 : 1, 0);
  builder.addFieldInt8(2, arrowTypeTag[column.type], 0);
  builder.addFieldOffset(3, typeOffset);
  return builder.endObject();
}

/** Encodes an Arrow Schema metadata table. */
function createSchema(builder: FlatBufferBuilder, fields: number[]): number {
  builder.startVector(4, fields.length, 4);
  for (let i = fields.length - 1; i >= 0; i -= 1) builder.addOffset(fields[i]);
  const fieldsOffset = builder.endVector();
  builder.startObject(4);
  builder.addFieldOffset(1, fieldsOffset);
  return builder.endObject();
}

/** Encodes a Message metadata table. */
function createMessage(
  builder: FlatBufferBuilder,
  headerType: number,
  headerOffset: number,
  bodyLength: number,
): number {
  builder.startObject(5);
  builder.addFieldInt16(0, 4, 0);
  builder.addFieldInt8(1, headerType, 0);
  builder.addFieldOffset(2, headerOffset);
  builder.addFieldInt64(3, BigInt(bodyLength), 0n);
  return builder.endObject();
}

/** Builds an Arrow schema stream with no record batches. */
export function encodeArrowSchema(columns: ArrowColumn[]): Uint8Array {
  const builder = new FlatBufferBuilder();
  const fields = columns.map((column) => createField(builder, column, createType(builder, column.type)));
  const schema = createSchema(builder, fields);
  const message = createMessage(builder, 1, schema, 0);
  const metadata = builder.finish(message);
  return concat(frame(metadata), endOfStream());
}

/** Builds an Arrow record-batch stream containing one primitive batch. */
export function encodeArrowStream(columns: ArrowColumn[], rows: Record<string, unknown>[]): Uint8Array {
  const body = encodeBody(columns, rows);
  const schemaBuilder = new FlatBufferBuilder();
  const fields = columns.map((column) => createField(schemaBuilder, column, createType(schemaBuilder, column.type)));
  const schema = createSchema(schemaBuilder, fields);
  const schemaMessage = createMessage(schemaBuilder, 1, schema, 0);
  const schemaMetadata = schemaBuilder.finish(schemaMessage);
  const batchBuilder = new FlatBufferBuilder();
  const batch = createRecordBatch(batchBuilder, columns, rows.length, body.buffers, body.nullCounts);
  const batchMessage = createMessage(batchBuilder, 3, batch, body.bytes.length);
  const batchMetadata = batchBuilder.finish(batchMessage);
  return concat(frame(schemaMetadata), frame(batchMetadata, body.bytes), endOfStream());
}

interface BodyLayout {
  bytes: Uint8Array;
  buffers: Array<{ offset: number; length: number }>;
  nullCounts: number[];
}

/** Encodes validity and value buffers in Arrow's flattened field order. */
function encodeBody(columns: ArrowColumn[], rows: Record<string, unknown>[]): BodyLayout {
  const chunks: Uint8Array[] = [];
  const buffers: Array<{ offset: number; length: number }> = [];
  const nullCounts: number[] = [];
  let offset = 0;
  const append = (chunk: Uint8Array): void => {
    const aligned = (offset + 7) & ~7;
    if (aligned > offset) chunks.push(new Uint8Array(aligned - offset));
    offset = aligned;
    buffers.push({ offset, length: chunk.length });
    chunks.push(chunk);
    offset += chunk.length;
  };
  for (const column of columns) {
    const values = rows.map((row) => row[column.name]);
    const validity = new Uint8Array(Math.ceil(values.length / 8));
    let nullCount = 0;
    for (let i = 0; i < values.length; i += 1) {
      if (values[i] === null || values[i] === undefined) nullCount += 1;
      else validity[i >> 3] |= 1 << (i & 7);
    }
    nullCounts.push(nullCount);
    append(nullCount === 0 ? new Uint8Array() : validity);
    if (column.type === "utf8") {
      const encoded = values.map((value) => (value === null || value === undefined ? new Uint8Array() : textEncoder.encode(String(value))));
      const offsets = new Uint8Array((values.length + 1) * 4);
      const offsetView = new DataView(offsets.buffer);
      let total = 0;
      offsetView.setInt32(0, 0, true);
      for (let i = 0; i < encoded.length; i += 1) {
        total += encoded[i].length;
        offsetView.setInt32((i + 1) * 4, total, true);
      }
      append(offsets);
      const data = new Uint8Array(total);
      let cursor = 0;
      for (const value of encoded) {
        data.set(value, cursor);
        cursor += value.length;
      }
      append(data);
    } else if (column.type === "bool") {
      const valuesBuffer = new Uint8Array(Math.ceil(values.length / 8));
      values.forEach((value, i) => {
        if (value) valuesBuffer[i >> 3] |= 1 << (i & 7);
      });
      append(valuesBuffer);
    } else {
      const width = column.type === "int32" ? 4 : 8;
      const valuesBuffer = new Uint8Array(values.length * width);
      const view = new DataView(valuesBuffer.buffer);
      values.forEach((value, i) => {
        const number = Number(value ?? 0);
        if (column.type === "int32") view.setInt32(i * width, number, true);
        else if (column.type === "int64") view.setBigInt64(i * width, BigInt(number), true);
        else view.setFloat64(i * width, number, true);
      });
      append(valuesBuffer);
    }
  }
  const total = (offset + 7) & ~7;
  const body = new Uint8Array(total);
  let cursor = 0;
  for (const chunk of chunks) {
    body.set(chunk, cursor);
    cursor += chunk.length;
  }
  return { bytes: body, buffers, nullCounts };
}

/** Encodes a RecordBatch metadata table and its struct vectors. */
function createRecordBatch(
  builder: FlatBufferBuilder,
  columns: ArrowColumn[],
  rowCount: number,
  buffers: Array<{ offset: number; length: number }>,
  nullCounts: number[],
): number {
  builder.startVector(16, buffers.length, 8);
  for (let i = buffers.length - 1; i >= 0; i -= 1) builder.createBuffer(buffers[i].offset, buffers[i].length);
  const bufferVector = builder.endVector();
  builder.startVector(16, columns.length, 8);
  for (let i = columns.length - 1; i >= 0; i -= 1) builder.createFieldNode(rowCount, nullCounts[i]);
  const nodeVector = builder.endVector();
  builder.startObject(5);
  builder.addFieldInt64(0, BigInt(rowCount), 0n);
  builder.addFieldOffset(1, nodeVector);
  builder.addFieldOffset(2, bufferVector);
  return builder.endObject();
}

/** Frames one Arrow metadata message and optional body with continuation markers. */
function frame(metadata: Uint8Array<ArrayBufferLike>, body: Uint8Array<ArrayBufferLike> = new Uint8Array()): Uint8Array {
  const metadataPaddedLength = (metadata.length + 7) & ~7;
  const bodyPaddedLength = (body.length + 7) & ~7;
  const output = new Uint8Array(8 + metadataPaddedLength + bodyPaddedLength);
  const view = new DataView(output.buffer);
  view.setInt32(0, -1, true);
  view.setInt32(4, metadataPaddedLength, true);
  output.set(metadata, 8);
  output.set(body, 8 + metadataPaddedLength);
  return output;
}

/** Returns Arrow's continuation-marker end-of-stream frame. */
function endOfStream(): Uint8Array {
  const output = new Uint8Array(8);
  new DataView(output.buffer).setInt32(0, -1, true);
  return output;
}

/** Concatenates typed-array chunks without relying on Node buffers. */
function concat(...chunks: Uint8Array[]): Uint8Array {
  const output = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.length;
  }
  return output;
}

/** Decodes an Arrow IPC stream containing primitive top-level fields. */
export function decodeArrowStream(bytes: Uint8Array): DecodedArrowStream {
  let position = 0;
  let columns: ArrowColumn[] = [];
  const batches: Array<{ rows: Record<string, unknown>[]; count: number }> = [];
  while (position + 8 <= bytes.length) {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const marker = view.getInt32(position, true);
    if (marker !== -1) throw new Error("invalid Arrow IPC continuation marker");
    const metadataLength = view.getInt32(position + 4, true);
    position += 8;
    if (metadataLength === 0) break;
    const metadataEnd = position + metadataLength;
    if (metadataEnd > bytes.length) throw new Error("truncated Arrow IPC metadata");
    const message = new FlatBufferReader(bytes, position);
    const headerType = message.uint8Field(6);
    if (headerType === 1) {
      const schema = message.indirectField(8);
      columns = schemaVector(schema, bytes);
    } else if (headerType === 3) {
      const batch = message.indirectField(8);
      const rowCount = Number(batch.int64Field(4));
      const nodes = batch.structVector(6, 16);
      const buffers = batch.structVector(8, 16);
      const bodyStart = (metadataEnd + 7) & ~7;
      const decoded = decodeBatch(columns, rowCount, nodes, buffers, bytes, bodyStart);
      batches.push({ rows: decoded, count: rowCount });
      const bodyLength = Number(message.int64Field(10));
      position = (bodyStart + bodyLength + 7) & ~7;
      continue;
    }
    position = (metadataEnd + 7) & ~7;
  }
  const rows = batches.flatMap((batch) => batch.rows);
  return { columns, rows, rowsRead: batches.reduce((sum, batch) => sum + batch.count, 0), rowsWritten: 0 };
}

/** Reads schema fields from an Arrow Schema table. */
function schemaVector(schema: FlatBufferReader, bytes: Uint8Array): ArrowColumn[] {
  return schema.tableVector(6).map((field) => {
    const name = field.stringField(4) ?? "";
    const nullable = field.boolField(6);
    const typeTag = field.uint8Field(8);
    const type = field.indirectField(10);
    return { name, nullable, type: readArrowType(typeTag, type, bytes) };
  });
}

/** Maps an Arrow field union to the primitive decoder type. */
function readArrowType(tag: number, type: FlatBufferReader, _bytes: Uint8Array): ArrowPrimitiveType {
  if (tag === 5) return "utf8";
  if (tag === 6) return "bool";
  if (tag === 2) return Number(type.int32Field(4)) === 32 ? "int32" : "int64";
  if (tag === 3) return "float64";
  throw new Error(`unsupported Arrow field type tag ${tag}`);
}

/** Decodes one primitive record batch from its body descriptors. */
function decodeBatch(
  columns: ArrowColumn[],
  rowCount: number,
  nodes: FlatBufferReader[],
  buffers: FlatBufferReader[],
  bytes: Uint8Array,
  bodyStart: number,
): Record<string, unknown>[] {
  const rows = Array.from({ length: rowCount }, () => ({} as Record<string, unknown>));
  let bufferIndex = 0;
  for (let columnIndex = 0; columnIndex < columns.length; columnIndex += 1) {
    const column = columns[columnIndex];
    const node = nodes[columnIndex];
    const nullCount = Number(node.int64At(8));
    const validity = buffers[bufferIndex++];
    const valueBuffers = column.type === "utf8" ? [buffers[bufferIndex++], buffers[bufferIndex++]] : [buffers[bufferIndex++]];
    const validityOffset = bodyStart + Number(validity.int64At(0));
    const validityLength = Number(validity.int64At(8));
    const isValid = (row: number): boolean => nullCount === 0 || (validityLength > 0 && (bytes[validityOffset + (row >> 3)] & (1 << (row & 7))) !== 0);
    const values = decodeValues(column.type, rowCount, valueBuffers, bodyStart, bytes);
    for (let row = 0; row < rowCount; row += 1) rows[row][column.name] = isValid(row) ? values[row] : null;
  }
  return rows;
}

/** Decodes one Arrow primitive value buffer. */
function decodeValues(
  type: ArrowPrimitiveType,
  rowCount: number,
  buffers: FlatBufferReader[],
  bodyStart: number,
  bytes: Uint8Array,
): unknown[] {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (type === "utf8") {
    const offsetsStart = bodyStart + Number(buffers[0].int64At(0));
    const dataStart = bodyStart + Number(buffers[1].int64At(0));
    const offsets = Array.from({ length: rowCount + 1 }, (_, i) => view.getInt32(offsetsStart + i * 4, true));
    return Array.from({ length: rowCount }, (_, i) => textDecoder.decode(bytes.subarray(dataStart + offsets[i], dataStart + offsets[i + 1])));
  }
  const start = bodyStart + Number(buffers[0].int64At(0));
  if (type === "bool") return Array.from({ length: rowCount }, (_, i) => (bytes[start + (i >> 3)] & (1 << (i & 7))) !== 0);
  if (type === "int32") return Array.from({ length: rowCount }, (_, i) => view.getInt32(start + i * 4, true));
  if (type === "int64") return Array.from({ length: rowCount }, (_, i) => Number(view.getBigInt64(start + i * 8, true)));
  return Array.from({ length: rowCount }, (_, i) => view.getFloat64(start + i * 8, true));
}

/** Reads Arrow FlatBuffers tables, vectors, strings, and fixed structs. */
class FlatBufferReader {
  readonly #bytes: Uint8Array;
  readonly #position: number;

  /** Creates a reader at a FlatBuffer table root or an inline struct. */
  constructor(bytes: Uint8Array, position: number, absolute = false) {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    this.#bytes = bytes;
    this.#position = absolute ? position : position + view.getInt32(position, true);
  }

  /** Reads one optional uint8 field by vtable byte offset. */
  uint8Field(vtableOffset: number): number {
    const offset = this.fieldOffset(vtableOffset);
    return offset === 0 ? 0 : this.#bytes[this.#position + offset];
  }

  /** Reads one optional boolean field. */
  boolField(vtableOffset: number): boolean {
    return this.uint8Field(vtableOffset) !== 0;
  }

  /** Reads one optional int32 field. */
  int32Field(vtableOffset: number): number {
    const offset = this.fieldOffset(vtableOffset);
    return offset === 0 ? 0 : new DataView(this.#bytes.buffer, this.#bytes.byteOffset, this.#bytes.byteLength).getInt32(this.#position + offset, true);
  }

  /** Reads one optional int64 field. */
  int64Field(vtableOffset: number): bigint {
    const offset = this.fieldOffset(vtableOffset);
    return offset === 0 ? 0n : new DataView(this.#bytes.buffer, this.#bytes.byteOffset, this.#bytes.byteLength).getBigInt64(this.#position + offset, true);
  }

  /** Reads one inline int64 from a struct. */
  int64At(offset: number): bigint {
    return new DataView(this.#bytes.buffer, this.#bytes.byteOffset, this.#bytes.byteLength).getBigInt64(this.#position + offset, true);
  }

  /** Reads an optional string field. */
  stringField(vtableOffset: number): string | undefined {
    const offset = this.fieldOffset(vtableOffset);
    if (offset === 0) return undefined;
    const address = this.#position + offset;
    const view = new DataView(this.#bytes.buffer, this.#bytes.byteOffset, this.#bytes.byteLength);
    const target = address + view.getInt32(address, true);
    const length = view.getInt32(target, true);
    return textDecoder.decode(this.#bytes.subarray(target + 4, target + 4 + length));
  }

  /** Follows an optional table offset field. */
  indirectField(vtableOffset: number): FlatBufferReader {
    const offset = this.fieldOffset(vtableOffset);
    if (offset === 0) return new FlatBufferReader(this.#bytes, 0);
    return new FlatBufferReader(this.#bytes, this.#position + offset);
  }

  /** Reads a vector of table offsets. */
  tableVector(vtableOffset: number): FlatBufferReader[] {
    const vector = this.vectorPosition(vtableOffset);
    if (vector === undefined) return [];
    const view = new DataView(this.#bytes.buffer, this.#bytes.byteOffset, this.#bytes.byteLength);
    const count = view.getInt32(vector, true);
    return Array.from({ length: count }, (_, i) => new FlatBufferReader(this.#bytes, vector + 4 + i * 4));
  }

  /** Reads a vector of fixed-width structs. */
  structVector(vtableOffset: number, width: number): FlatBufferReader[] {
    const vector = this.vectorPosition(vtableOffset);
    if (vector === undefined) return [];
    const view = new DataView(this.#bytes.buffer, this.#bytes.byteOffset, this.#bytes.byteLength);
    const count = view.getInt32(vector, true);
    return Array.from({ length: count }, (_, i) => new FlatBufferReader(this.#bytes, vector + 4 + i * width, true));
  }

  /** Finds a field's object-relative offset from the vtable. */
  private fieldOffset(vtableOffset: number): number {
    const view = new DataView(this.#bytes.buffer, this.#bytes.byteOffset, this.#bytes.byteLength);
    const vtable = this.#position - view.getInt32(this.#position, true);
    const length = view.getUint16(vtable, true);
    return vtableOffset < length ? view.getUint16(vtable + vtableOffset, true) : 0;
  }

  /** Finds a vector's absolute position from a table field. */
  private vectorPosition(vtableOffset: number): number | undefined {
    const offset = this.fieldOffset(vtableOffset);
    if (offset === 0) return undefined;
    const address = this.#position + offset;
    const view = new DataView(this.#bytes.buffer, this.#bytes.byteOffset, this.#bytes.byteLength);
    return address + view.getInt32(address, true);
  }
}
