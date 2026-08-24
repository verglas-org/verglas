/**
 * Cloudflare Durable Objects classes and namespace primitives for the Worker
 * component shim. Storage operations are deliberately transport-only: this
 * module never creates a local persistence fallback.
 */

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const STORAGE_CONFIGURATION_ERROR =
  'Durable Object storage requires the WIT storage capability; an in-memory fallback is forbidden';
const ZERO_ID = '0'.repeat(64);

/** A stable Durable Object identity. */
export class DurableObjectId {
  /** @param {string} hex @param {string} [name] */
  constructor(hex, name) {
    if (typeof hex !== 'string' || !/^[0-9a-fA-F]{64}$/u.test(hex)) {
      throw new Error('DurableObjectId must be a 64-character hexadecimal string');
    }
    this.name = name;
    this.#hex = hex.toLowerCase();
  }

  #hex;

  /** @returns {string} */
  toString() {
    return this.#hex;
  }

  /** @param {unknown} other @returns {boolean} */
  equals(other) {
    return other instanceof DurableObjectId && this.#hex === other.#hex;
  }
}

/** A Cloudflare-shaped SQL cursor over JSON rows returned by the WIT host. */
export class SqlStorageCursor {
  /** @param {object} [result] @param {Promise<object>} [pending] */
  constructor(result = {}, pending) {
    this.#pending = pending;
    if (pending) {
      this.columnNames = [];
      this.rowsRead = 0;
      this.rowsWritten = 0;
      this.#rows = [];
      this.#rawRows = [];
      Object.defineProperty(this, 'then', {
        enumerable: false,
        value: (onFulfilled, onRejected) => pending.then(
          (value) => {
            const cursor = new SqlStorageCursor(value);
            return onFulfilled ? onFulfilled(cursor) : cursor;
          },
          onRejected,
        ),
      });
      return;
    }
    const normalized = normalizeSqlResult(result);
    this.columnNames = normalized.columns;
    this.rowsRead = Number(result.rowsRead ?? normalized.rows.length);
    this.rowsWritten = Number(result.rowsWritten ?? 0);
    this.#rows = normalized.rows;
    this.#rawRows = normalized.rawRows;
  }

  #pending;
  #rows;
  #rawRows;

  /** @param {Promise<object>} promise @returns {SqlStorageCursor} */
  static pending(promise) {
    return new SqlStorageCursor({}, promise);
  }

  /** @returns {Array<object>} */
  toArray() {
    this.#assertHydrated();
    return [...this.#rows];
  }

  /** @returns {object} */
  one() {
    this.#assertHydrated();
    if (this.#rows.length !== 1) {
      throw new Error(`SQL cursor expected one row, received ${this.#rows.length}`);
    }
    return this.#rows[0];
  }

  /** @returns {Array<Array<unknown>>} */
  raw() {
    this.#assertHydrated();
    return this.#rawRows.map((row) => [...row]);
  }

  /** Throws when a remote cursor has not been awaited. */
  #assertHydrated() {
    if (this.#pending) {
      throw new Error('SQL cursor is pending; await storage.sql.exec(...) before reading it');
    }
  }
}

/**
 * The storage bridge for one Durable Object state. The host owns the event
 * transaction; the guest never emits a canonical transaction envelope.
 */
export class DurableObjectStorage {
  /** @param {DurableObjectId|object} idOrOptions @param {object} [transport] */
  constructor(idOrOptions, transport) {
    const options = idOrOptions instanceof DurableObjectId
      ? { id: idOrOptions, transport }
      : idOrOptions ?? {};
    this.id = options.id ?? new DurableObjectId(ZERO_ID);
    this.transport = options.transport ?? options.bridge;
    this.#onAlarmChange = options.onAlarmChange;
    this.#now = options.now ?? Date.now;
    this.sql = new SqlStorage((query, bindings) => this.#executeSql(query, bindings));
  }

  #onAlarmChange;
  #now;

  /** @param {string} operation @returns {object} */
  #requireTransport(operation) {
    if (!this.transport || typeof this.transport[operation] !== 'function') {
      throw new Error(`${STORAGE_CONFIGURATION_ERROR}: missing ${operation}`);
    }
    return this.transport;
  }

  /** @param {string|string[]} keyOrKeys @param {object} [_options] @returns {Promise<unknown|Map<string, unknown>>} */
  async get(keyOrKeys, _options) {
    const transport = this.#requireTransport('get');
    if (Array.isArray(keyOrKeys)) {
      const result = new Map();
      for (const key of keyOrKeys) {
        const value = await this.get(key);
        if (value !== undefined) result.set(key, value);
      }
      return result;
    }
    const bytes = normalizeOptional(await transport.get(String(keyOrKeys)));
    return bytes === undefined ? undefined : decodeStructuredClone(bytes);
  }

  /** @param {string|Record<string, unknown>} keyOrEntries @param {unknown|object} valueOrOptions @param {object} [maybeOptions] @returns {Promise<void>} */
  async put(keyOrEntries, valueOrOptions, maybeOptions) {
    const transport = this.#requireTransport('put');
    const entries = typeof keyOrEntries === 'string'
      ? { [keyOrEntries]: valueOrOptions }
      : keyOrEntries;
    const options = typeof keyOrEntries === 'string' ? maybeOptions : valueOrOptions;
    validatePutOptions(options);
    if (!entries || typeof entries !== 'object' || Array.isArray(entries)) {
      throw new TypeError('Durable Object storage.put entries must be an object');
    }
    for (const [key, value] of Object.entries(entries)) {
      await transport.put(String(key), encodeStructuredClone(value));
    }
  }

  /** @param {string|string[]} keyOrKeys @param {object} [_options] @returns {Promise<boolean|number>} */
  async delete(keyOrKeys, _options) {
    const transport = this.#requireTransport('delete');
    if (Array.isArray(keyOrKeys)) {
      let deleted = 0;
      for (const key of keyOrKeys) if (await this.delete(key)) deleted += 1;
      return deleted;
    }
    return Boolean(await transport.delete(String(keyOrKeys)));
  }

  /** @param {object} [options] @returns {Promise<Map<string, unknown>>} */
  async list(options = {}) {
    const transport = this.#requireTransport('list');
    validateListOptions(options);
    const limit = options.limit === undefined ? 1000 : options.limit;
    if (limit === 0) return new Map();
    const prefix = options.prefix ?? '';
    const hostLimit = options.start !== undefined || options.startAfter !== undefined || options.end !== undefined || options.reverse
      ? 0xffffffff
      : limit;
    const keys = await transport.list(String(prefix), hostLimit);
    let selected = [...keys].map(String);
    selected = selected.filter((key) => {
      if (options.start !== undefined && key < options.start) return false;
      if (options.startAfter !== undefined && key <= options.startAfter) return false;
      if (options.end !== undefined && key >= options.end) return false;
      return true;
    });
    selected.sort((left, right) => left < right ? -1 : left > right ? 1 : 0);
    if (options.reverse) selected.reverse();
    selected = selected.slice(0, limit);
    const result = new Map();
    for (const key of selected) {
      const value = await this.get(key);
      if (value !== undefined) result.set(key, value);
    }
    return result;
  }

  /** @returns {Promise<void>} */
  async deleteAll() {
    const values = await this.list({ limit: 0xffffffff });
    for (const key of values.keys()) await this.delete(key);
  }

  /** @param {(storage: DurableObjectStorage) => unknown|Promise<unknown>} callback @returns {Promise<unknown>} */
  async transaction(callback) {
    return await callback(new DurableObjectTransaction(this));
  }

  /** @param {number|Date} scheduledTime @param {object} [_options] @returns {Promise<void>} */
  async setAlarm(scheduledTime, _options) {
    const at = scheduledTime instanceof Date ? scheduledTime.getTime() : scheduledTime;
    if (!Number.isFinite(at) || at < 0) throw new Error('Durable Object alarm time must be a finite non-negative number');
    const transport = this.#requireTransport('setAlarm');
    await transport.setAlarm(toU64(at));
    this.#onAlarmChange?.(at);
  }

  /** @param {object} [_options] @returns {Promise<number|undefined>} */
  async getAlarm(_options) {
    const transport = this.#requireTransport('getAlarm');
    const value = normalizeOptional(await transport.getAlarm());
    return value === undefined ? undefined : Number(value);
  }

  /** @param {object} [_options] @returns {Promise<void>} */
  async deleteAlarm(_options) {
    const transport = this.#requireTransport('deleteAlarm');
    await transport.deleteAlarm();
    this.#onAlarmChange?.(undefined);
  }

  /** @returns {number|undefined} */
  alarmTime() {
    return undefined;
  }

  /** @param {string} query @param {Array<unknown>} bindings @returns {SqlStorageCursor} */
  #executeSql(query, bindings) {
    const transport = this.#requireTransport('sqlRows');
    const statement = renderSqlBindings(query, bindings);
    const result = transport.sqlRows(statement);
    if (isThenable(result)) return SqlStorageCursor.pending(Promise.resolve(result).then(decodeSqlRows));
    return new SqlStorageCursor(decodeSqlRows(result));
  }
}

/** A transaction view whose writes remain event-scoped host mutations. */
export class DurableObjectTransaction extends DurableObjectStorage {
  /** @param {DurableObjectStorage} parent */
  constructor(parent) {
    super({ id: parent.id, transport: parent.transport });
  }
}

/** SQL API exposed by DurableObjectStorage. */
class SqlStorage {
  /** @param {(query: string, bindings: Array<unknown>) => SqlStorageCursor} execute */
  constructor(execute) {
    this.#execute = execute;
  }

  #execute;

  /** @param {string} query @param {...unknown} bindings @returns {SqlStorageCursor} */
  exec(query, ...bindings) {
    if (typeof query !== 'string' || query.trim() === '') {
      throw new TypeError('Durable Object SQL query must be a non-empty string');
    }
    return this.#execute(query, bindings);
  }
}

/** State passed to one Durable Object constructor. */
export class DurableObjectState {
  /** @param {DurableObjectId} id @param {DurableObjectStorage|object} [storageOrTransport] */
  constructor(id, storageOrTransport) {
    this.id = id;
    this.storage = storageOrTransport instanceof DurableObjectStorage
      ? storageOrTransport
      : new DurableObjectStorage(id, storageOrTransport);
  }

  #tail = Promise.resolve();
  #waitUntilTasks = new Set();
  #pendingWebSocket;
  #acceptedWebSocket;
  #socketFactory;

  /** @param {() => unknown|Promise<unknown>} callback @returns {Promise<unknown>} */
  blockConcurrencyWhile(callback) {
    const run = this.#tail.then(callback);
    this.#tail = run.then(() => undefined, () => undefined);
    return run;
  }

  /** @param {Promise<unknown>} promise */
  waitUntil(promise) {
    const task = Promise.resolve(promise);
    this.#waitUntilTasks.add(task);
    void task.then(() => this.#waitUntilTasks.delete(task), () => this.#waitUntilTasks.delete(task));
  }

  /** @returns {Promise<void>} */
  async waitForConcurrency() {
    await this.#tail;
  }

  /** @returns {Promise<void>} */
  async waitForWaitUntil() {
    await Promise.all([...this.#waitUntilTasks]);
  }

  /** @param {number|undefined} websocketId @param {(id: number|bigint) => object} [socketFactory] */
  beginRequest(websocketId, socketFactory) {
    this.#pendingWebSocket = websocketId;
    this.#acceptedWebSocket = undefined;
    this.#socketFactory = socketFactory;
  }

  /** @returns {number|bigint|undefined} */
  acceptedWebSocketId() {
    return this.#acceptedWebSocket?.id;
  }

  /** @param {object} server */
  acceptWebSocket(server) {
    if (this.#pendingWebSocket === undefined || this.#pendingWebSocket === null) {
      throw new Error('ctx.acceptWebSocket requires a pending WebSocket upgrade request');
    }
    this.#acceptedWebSocket = { id: normalizeNumber(this.#pendingWebSocket), server };
  }

  /** @param {number|bigint} id @returns {object} */
  getWebSocket(id) {
    return this.#socketFactory ? this.#socketFactory(id) : { id };
  }

  /** @returns {Promise<object[]>} */
  async getWebSockets() {
    const transport = this.storage.transport;
    if (!transport || typeof transport.attached !== 'function') return [];
    const ids = await transport.attached();
    return ids.map((id) => this.getWebSocket(id));
  }
}

/** The base class imported from `cloudflare:workers`. */
export class DurableObject {
  /** @param {DurableObjectState} ctx @param {unknown} env */
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
  }

  /** @param {Request} _request @returns {Response|Promise<Response>} */
  fetch(_request) {
    throw new Error('DurableObject.fetch must be overridden by the user object');
  }

  /** @returns {Promise<void>} */
  alarm() {
    return Promise.resolve();
  }
}

/** A stub for one Durable Object address. */
export class DurableObjectStub {
  /** @param {DurableObjectId} id @param {Function} fetch @param {Function} [invoke] */
  constructor(id, fetch, invoke = async () => {
    throw new Error('Durable Object RPC is not part of the Worker v2 binding surface');
  }) {
    this.id = id;
    this.#fetch = fetch;
    this.#invoke = invoke;
  }

  #fetch;
  #invoke;

  /** @param {RequestInfo|URL|string} input @param {RequestInit} [init] @returns {Promise<Response>} */
  fetch(input, init) {
    return this.#fetch(input, init);
  }

  /** @param {string} method @param {Array<unknown>} args @returns {Promise<unknown>} */
  invoke(method, args) {
    return this.#invoke(method, args);
  }
}

/** A local namespace useful to tests and host-side runtime callers. */
export class DurableObjectNamespace {
  /** @param {Function|object} objectConstructorOrBinding @param {object} [suppliedOptions] */
  constructor(objectConstructorOrBinding, suppliedOptions = {}) {
    const isConstructor = typeof objectConstructorOrBinding === 'function';
    const binding = isConstructor ? {} : objectConstructorOrBinding ?? {};
    const options = isConstructor ? suppliedOptions : { ...binding, ...suppliedOptions };
    this.#objectConstructor = isConstructor ? objectConstructorOrBinding : binding.class;
    if (typeof this.#objectConstructor !== 'function') throw new TypeError('DurableObjectNamespace requires a Durable Object class');
    this.#transport = options.transport;
    this.#env = options.env;
    this.#now = options.now ?? Date.now;
  }

  #objectConstructor;
  #transport;
  #env;
  #now;
  #instances = new Map();
  #idsByName = new Map();
  #alarmTimers = new Map();

  /** @param {unknown} env */
  configureEnvironment(env) {
    this.#env = env;
  }

  /** @param {string} name @returns {DurableObjectId} */
  idFromName(name) {
    if (typeof name !== 'string') throw new TypeError('DurableObjectNamespace.idFromName requires a string name');
    if (!this.#idsByName.has(name)) this.#idsByName.set(name, new DurableObjectId(sha256Hex(name), name));
    return this.#idsByName.get(name);
  }

  /** @param {string} hex @returns {DurableObjectId} */
  idFromString(hex) {
    return new DurableObjectId(hex);
  }

  /** @returns {DurableObjectId} */
  newUniqueId() {
    const bytes = new Uint8Array(32);
    if (!globalThis.crypto || typeof globalThis.crypto.getRandomValues !== 'function') throw new Error('DurableObjectNamespace.newUniqueId requires Web Crypto getRandomValues');
    globalThis.crypto.getRandomValues(bytes);
    return new DurableObjectId(bytesToHex(bytes));
  }

  /** @param {DurableObjectId} id @returns {DurableObjectStub} */
  get(id) {
    if (!(id instanceof DurableObjectId)) throw new TypeError('DurableObjectNamespace.get requires a DurableObjectId');
    const stub = new DurableObjectStub(id, (input, init) => this.#dispatchFetch(id, input, init), (method, args) => this.#dispatchRpc(id, method, args));
    return new Proxy(stub, {
      get: (target, property, receiver) => {
        if (property === 'then') return undefined;
        if (typeof property !== 'string' || property in target) {
          const value = Reflect.get(target, property, receiver);
          return typeof value === 'function' ? value.bind(target) : value;
        }
        return (...args) => target.invoke(property, args);
      },
    });
  }

  /** @param {DurableObjectId} id @returns {object} */
  #getOrCreate(id) {
    const key = id.toString();
    const existing = this.#instances.get(key);
    if (existing) return existing;
    const storage = new DurableObjectStorage({
      id,
      transport: this.#transport,
      onAlarmChange: (at) => this.scheduleAlarm(id, at),
      now: this.#now,
    });
    const state = new DurableObjectState(id, storage);
    const object = new this.#objectConstructor(state, this.#env);
    const record = { id, storage, state, object };
    this.#instances.set(key, record);
    return record;
  }

  /** @param {DurableObjectId} id @param {RequestInfo|URL|string} input @param {RequestInit} [init] @returns {Promise<Response>} */
  async #dispatchFetch(id, input, init) {
    const record = this.#getOrCreate(id);
    await record.state.waitForConcurrency();
    const request = input instanceof Request && init === undefined ? input : new Request(input, init);
    return await record.object.fetch(request);
  }

  /** @param {DurableObjectId} id @param {string} method @param {Array<unknown>} args @returns {Promise<unknown>} */
  async #dispatchRpc(id, method, args) {
    if (method === 'fetch' || method === 'alarm') throw new Error(`Durable Object RPC method ${method} is reserved`);
    const record = this.#getOrCreate(id);
    await record.state.waitForConcurrency();
    const candidate = record.object[method];
    if (typeof candidate !== 'function') throw new Error(`Durable Object does not expose public RPC method ${method}`);
    return await candidate.apply(record.object, args);
  }

  /** @param {DurableObjectId} id @param {number|undefined} at */
  scheduleAlarm(id, at) {
    const key = id.toString();
    const prior = this.#alarmTimers.get(key);
    if (prior !== undefined) clearTimeout(prior);
    if (at === undefined) {
      this.#alarmTimers.delete(key);
      return;
    }
    const record = this.#getOrCreate(id);
    const delay = Math.max(0, Math.min(at - this.#now(), 2_147_483_647));
    this.#alarmTimers.set(key, setTimeout(async () => {
      this.#alarmTimers.delete(key);
      await record.state.waitForConcurrency();
      await record.object.alarm();
    }, delay));
  }
}

/** Encodes one value using the TypeScript SDK's structured-clone JSON tags. */
export function encodeStructuredClone(value) {
  const json = JSON.stringify(value, (_key, current) => encodeCloneValue(current));
  if (json === undefined) throw new TypeError('Durable Object storage value is not structured-cloneable');
  return textEncoder.encode(json);
}

/** Decodes one value from the WIT byte boundary. */
export function decodeStructuredClone(bytes) {
  return JSON.parse(textDecoder.decode(bytes), (_key, current) => decodeCloneValue(current));
}

/** Computes a synchronous SHA-256 digest for `idFromName`. */
export function sha256Hex(value) {
  return bytesToHex(sha256(textEncoder.encode(String(value))));
}

/** @param {unknown} value @returns {unknown} */
function encodeCloneValue(value) {
  if (typeof value === 'bigint') return { __verglas_bigint__: value.toString() };
  if (value instanceof ArrayBuffer) return { __verglas_array_buffer__: bytesToHex(new Uint8Array(value)) };
  if (ArrayBuffer.isView(value)) return { __verglas_array_buffer__: bytesToHex(new Uint8Array(value.buffer, value.byteOffset, value.byteLength)) };
  if (typeof value === 'function' || typeof value === 'symbol') throw new TypeError('Durable Object storage values must be structured-cloneable');
  return value;
}

/** @param {unknown} value @returns {unknown} */
function decodeCloneValue(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return value;
  if (typeof value.__verglas_bigint__ === 'string') return BigInt(value.__verglas_bigint__);
  if (typeof value.__verglas_array_buffer__ === 'string') return hexToBytes(value.__verglas_array_buffer__).buffer;
  return value;
}

/** @param {object|undefined} options */
function validatePutOptions(options) {
  if (options === undefined) return;
  if (!options || typeof options !== 'object' || Array.isArray(options)) throw new TypeError('Durable Object storage.put options must be an object');
  if (options.expiration !== undefined || options.expirationTtl !== undefined) throw new Error('Durable Object storage expiration is not supported by the WIT storage surface');
}

/** @param {object} options */
function validateListOptions(options) {
  if (!options || typeof options !== 'object' || Array.isArray(options)) throw new TypeError('Durable Object storage.list options must be an object');
  if (options.limit !== undefined && (!Number.isSafeInteger(options.limit) || options.limit < 0 || options.limit > 0xffffffff)) throw new RangeError('Durable Object storage.list limit must be an integer between 0 and 4294967295');
  for (const key of ['start', 'startAfter', 'end', 'prefix']) if (options[key] !== undefined && typeof options[key] !== 'string') throw new TypeError(`storage.list ${key} must be a string`);
  if (options.reverse !== undefined && typeof options.reverse !== 'boolean') throw new TypeError('storage.list reverse must be boolean');
}

/** @param {unknown} value @returns {unknown} */
function normalizeOptional(value) {
  if (value && typeof value === 'object' && 'tag' in value) {
    if (value.tag === 'none') return undefined;
    if (value.tag === 'some') return value.val;
  }
  return value === null ? undefined : value;
}

/** @param {unknown} value @returns {number|bigint} */
function normalizeNumber(value) {
  return typeof value === 'bigint' ? value : Number(value);
}

/** @param {number} value @returns {bigint} */
function toU64(value) {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError('u64 values must be non-negative safe integers');
  return BigInt(value);
}

/** @param {unknown} raw @returns {object} */
function decodeSqlRows(raw) {
  let value = raw;
  if (typeof value === 'string') {
    try { value = JSON.parse(value); } catch (error) { throw new Error(`WIT sql-rows returned invalid JSON: ${error.message}`); }
  }
  if (Array.isArray(value)) return { rows: value };
  throw new TypeError('WIT sql-rows must return a JSON array of rows');
}

/** @param {object} result @returns {{columns:string[],rows:Array<object>,rawRows:Array<Array<unknown>>}} */
function normalizeSqlResult(result) {
  const sourceRows = result.rows ?? [];
  const first = sourceRows[0];
  const declared = result.columns ?? result.columnNames;
  const columns = declared ? [...declared] : first && !Array.isArray(first) ? Object.keys(first) : [];
  const rows = sourceRows.map((row) => Array.isArray(row) ? Object.fromEntries(columns.map((column, index) => [column, row[index]])) : row);
  const rawRows = sourceRows.map((row) => Array.isArray(row) ? row.map(toSqlValue) : columns.map((column) => toSqlValue(row[column])));
  return { columns, rows, rawRows };
}

/** @param {unknown} value @returns {unknown} */
function toSqlValue(value) {
  if (value === null || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return value;
  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) return value;
  return JSON.stringify(value);
}

/** @param {string} query @param {Array<unknown>} bindings @returns {string} */
function renderSqlBindings(query, bindings) {
  let index = 0;
  const rendered = query.replace(/\?/gu, () => {
    if (index >= bindings.length) throw new Error('SQL query has fewer bindings than placeholders');
    return sqlLiteral(bindings[index++]);
  });
  if (index !== bindings.length) throw new Error('SQL query has more bindings than placeholders');
  return rendered;
}

/** @param {unknown} value @returns {string} */
function sqlLiteral(value) {
  if (value === null) return 'NULL';
  if (typeof value === 'string') return `'${value.replaceAll("'", "''")}'`;
  if (typeof value === 'boolean') return value ? '1' : '0';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new Error('SQL bindings must contain finite numbers');
    return String(value);
  }
  if (typeof value === 'bigint') return `${value}`;
  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
    const bytes = value instanceof ArrayBuffer ? new Uint8Array(value) : new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    return `X'${bytesToHex(bytes)}'`;
  }
  throw new TypeError('SQL bindings must be scalar Cloudflare SQL values');
}

/** @param {unknown} value @returns {boolean} */
function isThenable(value) {
  return Boolean(value && (typeof value === 'object' || typeof value === 'function') && typeof value.then === 'function');
}

/** @param {string} value @returns {Uint8Array} */
function hexToBytes(value) {
  if (!/^(?:[0-9a-f]{2})*$/iu.test(value)) throw new TypeError('invalid hexadecimal storage value');
  const bytes = new Uint8Array(value.length / 2);
  for (let index = 0; index < bytes.length; index += 1) bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  return bytes;
}

/** @param {Uint8Array} bytes @returns {string} */
function bytesToHex(bytes) {
  return Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
}

/** @param {unknown} value @returns {Uint8Array} */
function sha256(value) {
  const bytes = new Uint8Array(value);
  const bitLength = BigInt(bytes.length) * 8n;
  const paddedLength = Math.ceil((bytes.length + 9) / 64) * 64;
  const input = new Uint8Array(paddedLength);
  input.set(bytes);
  input[bytes.length] = 0x80;
  const view = new DataView(input.buffer);
  view.setBigUint64(paddedLength - 8, bitLength, false);
  let h0 = 0x6a09e667;
  let h1 = 0xbb67ae85;
  let h2 = 0x3c6ef372;
  let h3 = 0xa54ff53a;
  let h4 = 0x510e527f;
  let h5 = 0x9b05688c;
  let h6 = 0x1f83d9ab;
  let h7 = 0x5be0cd19;
  const k = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
  ];
  const schedule = new Uint32Array(64);
  for (let offset = 0; offset < paddedLength; offset += 64) {
    for (let index = 0; index < 16; index += 1) schedule[index] = view.getUint32(offset + index * 4, false);
    for (let index = 16; index < 64; index += 1) {
      const value1 = schedule[index - 15];
      const value2 = schedule[index - 2];
      const sigma0 = ((value1 >>> 7) | (value1 << 25)) ^ ((value1 >>> 18) | (value1 << 14)) ^ (value1 >>> 3);
      const sigma1 = ((value2 >>> 17) | (value2 << 15)) ^ ((value2 >>> 19) | (value2 << 13)) ^ (value2 >>> 10);
      schedule[index] = (schedule[index - 16] + sigma0 + schedule[index - 7] + sigma1) >>> 0;
    }
    let [a, b, c, d, e, f, g, h] = [h0, h1, h2, h3, h4, h5, h6, h7];
    for (let index = 0; index < 64; index += 1) {
      const sigma1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
      const choice = (e & f) ^ (~e & g);
      const temporary1 = (h + sigma1 + choice + k[index] + schedule[index]) >>> 0;
      const sigma0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const temporary2 = (sigma0 + majority) >>> 0;
      [h, g, f, e, d, c, b, a] = [g, f, e, (d + temporary1) >>> 0, c, b, a, (temporary1 + temporary2) >>> 0];
    }
    h0 = (h0 + a) >>> 0;
    h1 = (h1 + b) >>> 0;
    h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0;
    h4 = (h4 + e) >>> 0;
    h5 = (h5 + f) >>> 0;
    h6 = (h6 + g) >>> 0;
    h7 = (h7 + h) >>> 0;
  }
  const output = new Uint8Array(32);
  const outputView = new DataView(output.buffer);
  [h0, h1, h2, h3, h4, h5, h6, h7].forEach((value, index) => outputView.setUint32(index * 4, value, false));
  return output;
}
