//! Cloudflare Durable Objects-compatible classes and the local worker host.
//!
//! The local host keeps object instances in one process and sends every durable
//! storage operation through the configured Verglas worker transport. It never
//! substitutes an in-memory storage implementation for a missing engine.

import { decodeArrowStream, encodeArrowSchema, type ArrowColumn } from "./arrow-ipc";
import {
  encodeCanonicalTransaction,
  encodeHex,
  encodeUtf8Hex,
  type CanonicalMutation,
} from "./do-protocol";

/** A value accepted by the Durable Object SQL bridge. */
export type SqlStorageValue = string | number | boolean | null | ArrayBuffer | ArrayBufferView;

/**
 * The narrow command transport used by the Durable Objects bridge.
 *
 * A future WASM or microVM host can implement this interface without changing
 * the storage and namespace APIs.
 */
// Future WASM/microVM hosts can implement this interface without changing storage.
export interface DurableObjectTransport {
  /** Sends one command line without its trailing newline and returns one response line. */
  send(commandLine: string): string | Promise<string>;
}

/** Creates a transport for one object identity when the endpoint is per object. */
export type DurableObjectTransportFactory =
  | DurableObjectTransport
  | ((id: DurableObjectId) => DurableObjectTransport);

/** Options shared by storage operations that may yield to other requests. */
export interface DurableObjectStorageOptions {
  /** Allows the operation to run while another request is active. */
  allowConcurrency?: boolean;
}

/** Options accepted by `storage.get`. */
export interface DurableObjectStorageGetOptions extends DurableObjectStorageOptions {
  /** Skips the runtime cache when the engine supports one. */
  noCache?: boolean;
}

/** Options accepted by `storage.put`. */
export interface DurableObjectStoragePutOptions extends DurableObjectStorageOptions {
  /** Absolute expiration time in seconds since the Unix epoch. */
  expiration?: number;
  /** Relative expiration duration in seconds. */
  expirationTtl?: number;
}

/** Options accepted by `storage.list`. */
export interface DurableObjectStorageListOptions extends DurableObjectStorageGetOptions {
  /** Includes keys greater than or equal to this key. */
  start?: string;
  /** Includes keys strictly greater than this key. */
  startAfter?: string;
  /** Includes keys strictly less than this key. */
  end?: string;
  /** Restricts results to keys with this prefix. */
  prefix?: string;
  /** Maximum number of entries to return. */
  limit?: number;
  /** Returns keys in descending lexicographic order. */
  reverse?: boolean;
}

/** The result shape returned by a scripted or engine SQL statement. */
export interface SqlStorageResult<T = Record<string, unknown>> {
  /** Column names in engine order. */
  columns?: string[];
  /** Alternate wire spelling accepted from engine adapters. */
  columnNames?: string[];
  /** Object rows or rows in column order. */
  rows?: T[] | unknown[][];
  /** Number of rows read by the engine. */
  rowsRead?: number;
  /** Number of rows written by the engine. */
  rowsWritten?: number;
}

/**
 * A cursor matching Cloudflare's synchronous SQL cursor shape.
 *
 * The bridge also makes a cursor thenable while a remote response is in flight.
 * This keeps the public cursor methods synchronous for local transports while
 * allowing a socket-backed caller to `await storage.sql.exec(...)` before reading.
 */
export class SqlStorageCursor<T = Record<string, unknown>> {
  /** Names of columns returned by the statement. */
  columnNames: string[];
  /** Number of rows read by the statement. */
  rowsRead: number;
  /** Number of rows written by the statement. */
  rowsWritten: number;
  #rows: T[];
  #rawRows: SqlStorageValue[][];
  #pending: Promise<SqlStorageResult<T>> | undefined;

  /** Builds a hydrated or pending cursor from one decoded engine response. */
  constructor(result: SqlStorageResult<T> = {}, pending?: Promise<SqlStorageResult<T>>) {
    this.#pending = pending;
    if (pending) {
      this.columnNames = [];
      this.#rows = [];
      this.#rawRows = [];
      this.rowsRead = 0;
      this.rowsWritten = 0;
      Object.defineProperty(this, "then", {
        enumerable: false,
        value: <TResult1 = SqlStorageCursor<T>, TResult2 = never>(
          onfulfilled?: ((value: SqlStorageCursor<T>) => TResult1 | PromiseLike<TResult1>) | null,
          onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
        ) => pending.then(
          (remoteResult) => {
            const hydrated = new SqlStorageCursor(remoteResult);
            return onfulfilled ? onfulfilled(hydrated) : (hydrated as TResult1Placeholder<T>);
          },
          onrejected ?? undefined,
        ),
      });
      return;
    }
    const normalized = normalizeSqlResult(result);
    this.columnNames = normalized.columns;
    this.#rows = normalized.rows;
    this.#rawRows = normalized.rawRows;
    this.rowsRead = result.rowsRead ?? normalized.rows.length;
    this.rowsWritten = result.rowsWritten ?? 0;
  }

  /** Builds a cursor whose methods become available after a remote response. */
  static pending<T>(pending: Promise<SqlStorageResult<T>>): SqlStorageCursor<T> {
    return new SqlStorageCursor({}, pending);
  }

  /** Returns all rows in object form. */
  toArray(): T[] {
    this.assertHydrated();
    return [...this.#rows];
  }

  /** Returns the sole row and throws unless exactly one row exists. */
  one(): T {
    this.assertHydrated();
    if (this.#rows.length !== 1) {
      throw new Error(`SQL cursor expected one row, received ${this.#rows.length}`);
    }
    return this.#rows[0] as T;
  }

  /** Returns rows as arrays in the order named by `columnNames`. */
  raw(): SqlStorageValue[][] {
    this.assertHydrated();
    return this.#rawRows.map((row) => [...row]);
  }

  /** Throws a useful error instead of exposing an incompletely populated cursor. */
  private assertHydrated(): void {
    if (this.#pending) {
      throw new Error("SQL cursor is pending; await sql.exec(...) before reading it");
    }
  }
}

// A private cast target keeps the pending cursor's generic callback independent
// of the public cursor class's type parameter.
type TResult1Placeholder<T> = SqlStorageCursor<T>;

/** SQL API exposed by `DurableObjectStorage.sql`. */
export interface SqlStorage {
  /** Executes SQL with positional bindings and returns a Cloudflare-style cursor. */
  exec<T = Record<string, unknown>>(query: string, ...bindings: SqlStorageValue[]): SqlStorageCursor<T>;
}

/** A durable object constructor accepted by a namespace binding. */
export type DurableObjectConstructor<Env = unknown, Instance extends DurableObject<Env> = DurableObject<Env>> =
  new (ctx: DurableObjectState, env: Env) => Instance;

/** The base class user Durable Objects extend. */
export class DurableObject<Env = unknown> {
  /** State and storage belonging to this object identity. */
  readonly ctx: DurableObjectState;
  /** Environment bindings configured for the worker. */
  readonly env: Env;

  /** Constructs a user object with its stable state and environment. */
  constructor(ctx: DurableObjectState, env: Env) {
    this.ctx = ctx;
    this.env = env;
  }

  /** Handles an HTTP request addressed to this object. */
  fetch(_request: Request): Response | Promise<Response> {
    throw new Error("DurableObject.fetch must be overridden by the user object");
  }

  /** Handles the local timer callback for a scheduled alarm. */
  alarm(): Promise<void> {
    return Promise.resolve();
  }
}

/** The state object passed to one Durable Object instance. */
export class DurableObjectState {
  /** Stable identity for this instance. */
  readonly id: DurableObjectId;
  /** Durable storage bridge owned by this instance. */
  readonly storage: DurableObjectStorage;
  #tail: Promise<void> = Promise.resolve();
  readonly #waitUntilTasks = new Set<Promise<unknown>>();

  /** Creates state around one ID and storage bridge. */
  constructor(id: DurableObjectId, storage?: DurableObjectStorage) {
    this.id = id;
    this.storage = storage ?? createUnconfiguredStorage(id);
  }

  /** Runs initialization or migration work before later object activity proceeds. */
  blockConcurrencyWhile<T>(callback: () => T | Promise<T>): Promise<T> {
    const run = this.#tail.then(callback);
    this.#tail = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  /** Extends the lifetime of work started by this object. */
  waitUntil(promise: Promise<unknown>): void {
    this.#waitUntilTasks.add(promise);
    void promise.then(
      () => this.#waitUntilTasks.delete(promise),
      () => this.#waitUntilTasks.delete(promise),
    );
  }

  /** Waits for all concurrency blocks currently queued for this object. */
  async waitForConcurrency(): Promise<void> {
    await this.#tail;
  }

  /** Waits for work retained by `waitUntil` without making it part of storage commit. */
  async waitForWaitUntil(): Promise<void> {
    await Promise.all([...this.#waitUntilTasks]);
  }
}

/** A stable 64-hex Durable Object identity. */
export class DurableObjectId {
  /** Optional human-readable name used to derive this ID. */
  readonly name: string | undefined;
  readonly #hex: string;

  /** Creates an ID from its canonical 64-character hexadecimal form. */
  constructor(hex: string, name?: string) {
    if (!/^[0-9a-fA-F]{64}$/.test(hex)) {
      throw new Error("DurableObjectId must be a 64-character hexadecimal string");
    }
    this.#hex = hex.toLowerCase();
    this.name = name;
  }

  /** Returns the canonical lower-case hexadecimal ID. */
  toString(): string {
    return this.#hex;
  }

  /** Compares two IDs by canonical identity. */
  equals(other: DurableObjectId): boolean {
    return other instanceof DurableObjectId && this.#hex === other.#hex;
  }
}

/** A proxy surface for one Durable Object address. */
export type DurableObjectStubRpc<Instance extends DurableObject<any>> = {
  [Key in keyof Instance as Key extends keyof DurableObject<any>
    ? never
    : Instance[Key] extends (...args: any[]) => any
      ? Key
      : never]: Instance[Key] extends (...args: infer A) => infer R
    ? (...args: A) => Promise<Awaited<R>>
    : never;
};

/** A Durable Object stub with HTTP fetch and typed public RPC methods. */
export class DurableObjectStub<Instance extends DurableObject<any> = DurableObject<any>> {
  /** Address used by this stub. */
  readonly id: DurableObjectId;
  readonly #invoke: (method: string & (keyof DurableObjectStubRpc<Instance> | string), args: unknown[]) => Promise<unknown>;
  readonly #fetch: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

  /** Creates one stub connected to a namespace's local dispatcher. */
  constructor(
    id: DurableObjectId,
    fetch: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>,
    invoke: (method: string & (keyof DurableObjectStubRpc<Instance> | string), args: unknown[]) => Promise<unknown>,
  ) {
    this.id = id;
    this.#fetch = fetch;
    this.#invoke = invoke;
  }

  /** Sends an HTTP request to the addressed object. */
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    return this.#fetch(input, init);
  }

  /** Invokes one public RPC method on the addressed object. */
  invoke(method: string, args: unknown[]): Promise<unknown> {
    return this.#invoke(method, args);
  }
}

/** Options for one local namespace binding. */
export interface DurableObjectNamespaceOptions<Env = unknown> {
  /** Injects a test transport or a host-provided engine transport. */
  transport?: DurableObjectTransportFactory;
  /** Configures a Unix socket path or an ID-dependent socket path. */
  endpoint?: string | ((id: DurableObjectId) => string);
  /** Environment passed to each object constructor. */
  env?: Env;
  /** Millisecond clock used by local alarm scheduling. */
  now?: () => number;
}

/** A declarative namespace binding accepted by the local host. */
export interface DurableObjectNamespaceBinding<Instance extends DurableObject<any> = DurableObject<any>>
  extends DurableObjectNamespaceOptions {
  /** User Durable Object constructor for this binding. */
  class: DurableObjectConstructor<any, Instance>;
}

/** A namespace that creates IDs and routes HTTP/RPC calls to local instances. */
export class DurableObjectNamespace<Instance extends DurableObject<any> = DurableObject<any>> {
  readonly #objectConstructor: DurableObjectConstructor<any, Instance>;
  readonly #transport?: DurableObjectTransportFactory;
  readonly #endpoint?: string | ((id: DurableObjectId) => string);
  readonly #instances = new Map<string, ObjectRecord<Instance>>();
  readonly #pendingAlarmChanges = new Map<string, number | undefined>();
  readonly #idsByName = new Map<string, DurableObjectId>();
  readonly #now: () => number;
  #env: unknown;

  /** Creates a namespace for one user Durable Object class. */
  constructor(
    objectConstructor: DurableObjectConstructor<any, Instance>,
    options?: DurableObjectNamespaceOptions,
  );
  /** Creates a namespace from a declarative `{ class, transport }` binding. */
  constructor(binding: DurableObjectNamespaceBinding<Instance>);
  constructor(
    objectConstructorOrBinding: DurableObjectConstructor<any, Instance> | DurableObjectNamespaceBinding<Instance>,
    suppliedOptions: DurableObjectNamespaceOptions = {},
  ) {
    const objectConstructor = typeof objectConstructorOrBinding === "function"
      ? objectConstructorOrBinding
      : objectConstructorOrBinding.class;
    const options = typeof objectConstructorOrBinding === "function"
      ? suppliedOptions
      : { ...objectConstructorOrBinding, ...suppliedOptions };
    this.#objectConstructor = objectConstructor;
    this.#transport = options.transport;
    this.#endpoint = options.endpoint;
    this.#env = options.env;
    this.#now = options.now ?? Date.now;
    if (this.#transport && this.#endpoint) {
      throw new Error("DurableObjectNamespace accepts transport or endpoint, not both");
    }
  }

  /** Rebinds the environment after all worker namespace bindings are assembled. */
  configureEnvironment(env: unknown): void {
    this.#env = env;
  }

  /** Derives a stable ID from a human-readable name. */
  idFromName(name: string): DurableObjectId {
    if (typeof name !== "string") throw new Error("DurableObjectNamespace.idFromName requires a string name");
    const prior = this.#idsByName.get(name);
    if (prior) return prior;
    const id = new DurableObjectId(hashName(name), name);
    this.#idsByName.set(name, id);
    return id;
  }

  /** Parses a canonical hexadecimal ID supplied by another worker. */
  idFromString(hex: string): DurableObjectId {
    return new DurableObjectId(hex);
  }

  /** Allocates a cryptographically random ID with no associated name. */
  newUniqueId(): DurableObjectId {
    const bytes = new Uint8Array(32);
    const cryptoApi = globalThis.crypto;
    if (!cryptoApi?.getRandomValues) {
      throw new Error("DurableObjectNamespace.newUniqueId requires Web Crypto getRandomValues");
    }
    cryptoApi.getRandomValues(bytes);
    return new DurableObjectId(bytesToHex(bytes));
  }

  /** Returns a stub for one stable object identity. */
  get(id: DurableObjectId): DurableObjectStub<Instance> & DurableObjectStubRpc<Instance> {
    if (!(id instanceof DurableObjectId)) throw new Error("DurableObjectNamespace.get requires a DurableObjectId");
    const target = new DurableObjectStub<Instance>(
      id,
      (input, init) => this.dispatchFetch(id, input, init),
      (method, args) => this.dispatchRpc(id, method, args),
    );
    return new Proxy(target, {
      get: (object, property) => {
        if (property === "then") return undefined;
        if (typeof property !== "string" || property in object) {
          const value = Reflect.get(object, property, object);
          return typeof value === "function" ? value.bind(object) : value;
        }
        return (...args: unknown[]) => object.invoke(property, args);
      },
    }) as DurableObjectStub<Instance> & DurableObjectStubRpc<Instance>;
  }

  /** Returns or constructs the in-process object record for one ID. */
  private getOrCreate(id: DurableObjectId): ObjectRecord<Instance> {
    const key = id.toString();
    const existing = this.#instances.get(key);
    if (existing) return existing;
    const transport = this.makeTransport(id);
    const storage = new DurableObjectStorage({
      id,
      transport,
      onAlarmChange: (at) => this.scheduleAlarm(id, at),
      now: this.#now,
    });
    const state = new DurableObjectState(id, storage);
    const object = new this.#objectConstructor(state, this.#env);
    const record: ObjectRecord<Instance> = { id, state, storage, object, alarmTimer: undefined };
    this.#instances.set(key, record);
    if (this.#pendingAlarmChanges.has(key)) {
      const pendingAlarm = this.#pendingAlarmChanges.get(key);
      this.#pendingAlarmChanges.delete(key);
      this.scheduleAlarm(id, pendingAlarm);
    }
    return record;
  }

  /** Creates one object-specific transport or leaves it absent for configuration errors. */
  private makeTransport(id: DurableObjectId): DurableObjectTransport | undefined {
    if (this.#transport) {
      return typeof this.#transport === "function" ? this.#transport(id) : this.#transport;
    }
    if (this.#endpoint) {
      const path = typeof this.#endpoint === "function" ? this.#endpoint(id) : this.#endpoint;
      return createUnixSocketTransport(path);
    }
    return undefined;
  }

  /** Routes one normalized request through the object's fetch method. */
  private async dispatchFetch(id: DurableObjectId, input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const record = this.getOrCreate(id);
    await record.state.waitForConcurrency();
    const request = normalizeRequest(input, init);
    return await record.object.fetch(request);
  }

  /** Routes one RPC call and rejects methods that are not public functions. */
  private async dispatchRpc(id: DurableObjectId, method: string, args: unknown[]): Promise<unknown> {
    if (method === "fetch" || method === "alarm") {
      throw new Error(`Durable Object RPC method ${method} is reserved`);
    }
    const record = this.getOrCreate(id);
    await record.state.waitForConcurrency();
    const candidate = (record.object as unknown as Record<string, unknown>)[method];
    if (typeof candidate !== "function") {
      throw new Error(`Durable Object does not expose public RPC method ${method}`);
    }
    return await (candidate as (...values: unknown[]) => unknown).apply(record.object, args);
  }

  /** Installs or cancels a local timer for one object alarm. */
  private scheduleAlarm(id: DurableObjectId, at: number | undefined): void {
    const key = id.toString();
    const record = this.#instances.get(key);
    if (!record) {
      this.#pendingAlarmChanges.set(key, at);
      return;
    }
    if (record.alarmTimer !== undefined) clearTimeout(record.alarmTimer);
    if (at === undefined) {
      record.alarmTimer = undefined;
      return;
    }
    const delay = Math.max(0, Math.min(at - this.#now(), 2_147_483_647));
    record.alarmTimer = setTimeout(() => {
      record.alarmTimer = undefined;
      void this.dispatchAlarm(record);
    }, delay);
  }

  /** Dispatches one timer callback after the object's concurrency blocks. */
  private async dispatchAlarm(record: ObjectRecord<Instance>): Promise<void> {
    const alarmAt = record.storage.alarmTime();
    if (alarmAt === undefined || alarmAt > this.#now()) {
      this.scheduleAlarm(record.id, alarmAt);
      return;
    }
    record.storage.clearAlarmAfterDispatch();
    await record.state.waitForConcurrency();
    await record.object.alarm();
  }
}

interface ObjectRecord<Instance extends DurableObject<any>> {
  readonly id: DurableObjectId;
  readonly state: DurableObjectState;
  readonly storage: DurableObjectStorage;
  readonly object: Instance;
  alarmTimer: ReturnType<typeof setTimeout> | undefined;
}

/** The private relational table used to implement Cloudflare KV methods. */
const KV_TABLE = "__verglas_do_kv";

/** The durable KV table's exact Arrow schema declaration. */
const KV_COLUMNS: ArrowColumn[] = [
  { name: "key", type: "utf8", nullable: false },
  { name: "value_json", type: "utf8", nullable: false },
  { name: "expires_at", type: "int64", nullable: true },
];

/** Client-side mutation state submitted as one canonical engine envelope. */
interface ClientTransactionBuffer {
  readonly id: string;
  readonly baseCommitSequence: number;
  readonly mutations: CanonicalMutation[];
}

/** A storage bridge backed by the authoritative worker endpoint. */
export class DurableObjectStorage {
  /** SQL interface exposed by this storage. */
  readonly sql: SqlStorage;
  protected readonly bridge: StorageBridge;
  protected readonly transactionId: string | undefined;
  protected readonly transactionBuffer: ClientTransactionBuffer | undefined;
  readonly #onAlarmChange: ((at: number | undefined) => void) | undefined;
  readonly #now: () => number;
  #alarmAt: number | undefined;

  /** Creates storage around an object ID and optional engine transport. */
  constructor(id: DurableObjectId, transport?: DurableObjectTransport);
  /** Creates storage with host-only alarm scheduling and a configured bridge. */
  constructor(options: DurableObjectStorageInternalOptions);
  constructor(
    idOrOptions: DurableObjectId | DurableObjectStorageInternalOptions,
    transport?: DurableObjectTransport,
  ) {
    const options: DurableObjectStorageInternalOptions =
      idOrOptions instanceof DurableObjectId
        ? { id: idOrOptions, transport }
        : idOrOptions;
    this.bridge = options.bridge ?? new StorageBridge(options.transport, options.id);
    this.transactionBuffer = options.transactionBuffer;
    this.transactionId = options.transactionId ?? options.transactionBuffer?.id;
    this.#onAlarmChange = options.onAlarmChange;
    this.#now = options.now ?? Date.now;
    this.#alarmAt = undefined;
    this.sql = new SqlStorageImpl((query, bindings) => this.executeStatement(query, bindings));
  }

  /** Reads one value or a batch of values from the private KV table. */
  get<T = unknown>(key: string, options?: DurableObjectStorageGetOptions): Promise<T | undefined>;
  /** Reads a batch of values and preserves the requested key order in a map. */
  get<T = unknown>(keys: string[], options?: DurableObjectStorageGetOptions): Promise<Map<string, T>>;
  async get<T = unknown>(keyOrKeys: string | string[], _options?: DurableObjectStorageGetOptions): Promise<T | undefined | Map<string, T>> {
    if (Array.isArray(keyOrKeys)) {
      const map = new Map<string, T>();
      for (const key of keyOrKeys) {
        const value = await this.get<T>(key);
        if (value !== undefined) map.set(key, value);
      }
      return map;
    }
    const cursor = await this.readCursor<{ value_json?: string; value?: T; expires_at?: number }>(
      `SELECT key, value_json, expires_at FROM "__verglas_do_kv" WHERE key = ?`,
      [keyOrKeys],
    );
    const row = cursor.toArray()[0];
    if (!row) return undefined;
    if (typeof row.expires_at === "number" && row.expires_at <= Math.floor(this.#now() / 1000)) {
      await this.delete(keyOrKeys);
      return undefined;
    }
    return row.value_json === undefined ? row.value : decodeStoredValue(row.value_json) as T;
  }

  /** Writes one value with optional absolute or relative expiration. */
  put<T>(key: string, value: T, options?: DurableObjectStoragePutOptions): Promise<void>;
  /** Writes a record of values with one shared expiration policy. */
  put<T>(entries: Record<string, T>, options?: DurableObjectStoragePutOptions): Promise<void>;
  async put<T>(keyOrEntries: string | Record<string, T>, valueOrOptions?: T | DurableObjectStoragePutOptions, maybeOptions?: DurableObjectStoragePutOptions): Promise<void> {
    const entries: Record<string, T> =
      typeof keyOrEntries === "string"
        ? { [keyOrEntries]: valueOrOptions as T }
        : keyOrEntries;
    const options =
      typeof keyOrEntries === "string" ? maybeOptions : (valueOrOptions as DurableObjectStoragePutOptions | undefined);
    const expiration = expirationSeconds(options, this.#now);
    const rows = Object.entries(entries).map(([key, value]) => ({
      key,
      value_json: encodeStoredValue(value),
      expires_at: expiration,
    }));
    await this.appendOrCommit({
      kind: "upsert",
      domain: "relational",
      table: KV_TABLE,
      columns: KV_COLUMNS,
      rows,
    });
  }

  /** Deletes one key and reports whether the key existed. */
  delete(key: string, options?: DurableObjectStorageOptions): Promise<boolean>;
  /** Deletes several keys and reports the number deleted. */
  delete(keys: string[], options?: DurableObjectStorageOptions): Promise<number>;
  async delete(keyOrKeys: string | string[], _options?: DurableObjectStorageOptions): Promise<boolean | number> {
    if (Array.isArray(keyOrKeys)) {
      let deleted = 0;
      for (const key of keyOrKeys) if (await this.delete(key)) deleted += 1;
      return deleted;
    }
    const rows = await this.readKvRows();
    const existed = rows.some((row) => row.key === keyOrKeys);
    if (existed) {
      await this.appendOrCommit({
        kind: "replace",
        domain: "relational",
        table: KV_TABLE,
        columns: KV_COLUMNS,
        rows: rows.filter((row) => row.key !== keyOrKeys),
      });
    }
    return existed;
  }

  /** Lists keys and values according to Cloudflare's lexicographic options. */
  async list<T = unknown>(_options: DurableObjectStorageListOptions = {}): Promise<Map<string, T>> {
    const options = _options;
    const conditions: string[] = [];
    const bindings: SqlStorageValue[] = [];
    if (options.start !== undefined) {
      conditions.push("key >= ?");
      bindings.push(options.start);
    }
    if (options.startAfter !== undefined) {
      conditions.push("key > ?");
      bindings.push(options.startAfter);
    }
    if (options.end !== undefined) {
      conditions.push("key < ?");
      bindings.push(options.end);
    }
    if (options.prefix !== undefined) {
      conditions.push("key LIKE ?");
      bindings.push(`${escapeLike(options.prefix)}%`);
    }
    const where = conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
    const order = options.reverse ? "DESC" : "ASC";
    const limit = options.limit === undefined ? "" : " LIMIT ?";
    if (options.limit !== undefined) bindings.push(options.limit);
    const cursor = await this.readCursor<{ key: string; value_json?: string; value?: T; expires_at?: number }>(
      `SELECT key, value_json, expires_at FROM "__verglas_do_kv"${where} ORDER BY key ${order}${limit}`,
      bindings,
    );
    const result = new Map<string, T>();
    for (const row of cursor.toArray()) {
      if (typeof row.expires_at === "number" && row.expires_at <= Math.floor(this.#now() / 1000)) continue;
      result.set(row.key, row.value_json === undefined ? row.value as T : decodeStoredValue(row.value_json) as T);
    }
    return result;
  }

  /** Deletes every key in the private KV table. */
  async deleteAll(_options?: DurableObjectStorageOptions): Promise<void> {
    await this.appendOrCommit({
      kind: "replace",
      domain: "relational",
      table: KV_TABLE,
      columns: KV_COLUMNS,
      rows: [],
    });
  }

  /** Runs a callback in one engine transaction with rollback on failure. */
  async transaction<T>(callback: (txn: DurableObjectTransaction) => T | Promise<T>): Promise<T> {
    if (this.transactionBuffer) return await callback(this as unknown as DurableObjectTransaction);
    const buffer: ClientTransactionBuffer = {
      id: nextTransactionId(),
      baseCommitSequence: this.bridge.currentSequence,
      mutations: [],
    };
    const txn = new DurableObjectTransaction(this.objectId(), this.bridge, buffer);
    try {
      const result = await callback(txn);
      await txn.commit();
      return result;
    } catch (error) {
      await txn.rollback();
      throw error;
    }
  }

  /** Schedules a local timer alarm; durable engine alarm persistence is not available yet. */
  async setAlarm(scheduledTime: number | Date, _options?: DurableObjectStorageOptions): Promise<void> {
    const at = scheduledTime instanceof Date ? scheduledTime.getTime() : scheduledTime;
    if (!Number.isFinite(at)) throw new Error("Durable Object alarm time must be finite");
    this.#alarmAt = at;
    this.#onAlarmChange?.(at);
  }

  /** Returns the currently scheduled local alarm timestamp. */
  async getAlarm(_options?: DurableObjectStorageOptions): Promise<number | undefined> {
    return this.#alarmAt;
  }

  /** Cancels a local timer alarm. */
  async deleteAlarm(_options?: DurableObjectStorageOptions): Promise<void> {
    this.#alarmAt = undefined;
    this.#onAlarmChange?.(undefined);
  }

  /** Reads the timestamp for the namespace timer without crossing the public async API. */
  alarmTime(): number | undefined {
    return this.#alarmAt;
  }

  /** Clears an alarm immediately before its local callback is dispatched. */
  clearAlarmAfterDispatch(): void {
    this.#alarmAt = undefined;
  }

  /** Tracks statements issued by transactions and returns their original cursor. */
  protected trackStatement<T>(cursor: SqlStorageCursor<T>): SqlStorageCursor<T> {
    return cursor;
  }

  /** Returns the object identity represented by this storage instance. */
  protected objectId(): DurableObjectId {
    return this.bridge.id;
  }

  /** Executes one SQL query through the endpoint's snapshot-isolated QUERY command. */
  protected executeStatement(query: string, bindings: SqlStorageValue[]): SqlStorageCursor {
    const trimmed = query.trim();
    if (/^(SELECT|WITH|PRAGMA|EXPLAIN)\b/i.test(trimmed)) {
      const table = queryTable(trimmed);
      const cursor = this.bridge.query(table, renderSqlBindings(trimmed, bindings));
      return this.trackStatement(cursor);
    }
    throw new Error("SQL mutations must be submitted as canonical Arrow mutations; use storage.put/delete or a transaction buffer");
  }

  /** Reads one cursor through the authoritative QUERY command. */
  private async readCursor<T>(query: string, bindings: SqlStorageValue[]): Promise<SqlStorageCursor<T>> {
    return (await this.executeStatement(query, bindings)) as SqlStorageCursor<T>;
  }

  /** Reads the complete private KV table for client-side replace mutations. */
  private async readKvRows(): Promise<Array<{ key: string; value_json: string; expires_at: number | null }>> {
    const cursor = await this.readCursor<{ key: string; value_json: string; expires_at: number | null }>(
      `SELECT key, value_json, expires_at FROM "${KV_TABLE}"`,
      [],
    );
    return cursor.toArray();
  }

  /** Appends a mutation to an explicit buffer or commits one implicit envelope. */
  private async appendOrCommit(mutation: CanonicalMutation): Promise<void> {
    if (this.transactionBuffer) {
      this.transactionBuffer.mutations.push(mutation);
      return;
    }
    const buffer: ClientTransactionBuffer = {
      id: nextTransactionId(),
      baseCommitSequence: this.bridge.currentSequence,
      mutations: [mutation],
    };
    await this.bridge.commit(buffer);
  }
}

interface DurableObjectStorageInternalOptions {
  id: DurableObjectId;
  transport?: DurableObjectTransport;
  bridge?: StorageBridge;
  transactionId?: string;
  transactionBuffer?: ClientTransactionBuffer;
  onAlarmChange?: (at: number | undefined) => void;
  now?: () => number;
}

/** A transaction callback surface matching Durable Object storage methods. */
export class DurableObjectTransaction extends DurableObjectStorage {
  readonly #buffer: ClientTransactionBuffer;
  #committed = false;

  /** Creates one storage view bound to an uncommitted client-side envelope. */
  constructor(id: DurableObjectId, bridge: StorageBridge, buffer: ClientTransactionBuffer) {
    super({ id, bridge, transactionBuffer: buffer });
    this.#buffer = buffer;
  }

  /** Commits all buffered mutations as one canonical engine envelope. */
  async commit(): Promise<void> {
    if (this.#committed) throw new Error("Durable Object transaction already completed");
    await this.bridge.commit(this.#buffer);
    this.#committed = true;
  }

  /** Discards buffered mutations after a callback exception. */
  async rollback(): Promise<void> {
    this.#buffer.mutations.length = 0;
    this.#committed = true;
  }
}

/** SQL adapter that uses one callback for both local and remote transports. */
class SqlStorageImpl implements SqlStorage {
  readonly #execute: (query: string, bindings: SqlStorageValue[]) => SqlStorageCursor;

  /** Binds SQL execution to one Durable Object storage instance. */
  constructor(execute: (query: string, bindings: SqlStorageValue[]) => SqlStorageCursor) {
    this.#execute = execute;
  }

  /** Executes one SQL statement with positional bindings. */
  exec<T = Record<string, unknown>>(query: string, ...bindings: SqlStorageValue[]): SqlStorageCursor<T> {
    if (!query.trim()) throw new Error("Durable Object SQL query must not be empty");
    return this.#execute(query, bindings) as SqlStorageCursor<T>;
  }
}

/** Internal command bridge shared by storage and transaction views. */
export class StorageBridge {
  readonly #transport: DurableObjectTransport | undefined;
  readonly id: DurableObjectId;
  #currentSequence = 0;
  readonly #registeredTables = new Set<string>();

  /** Creates a bridge that fails clearly when no endpoint was configured. */
  constructor(transport?: DurableObjectTransport, id = new DurableObjectId("0000000000000000000000000000000000000000000000000000000000000000")) {
    this.#transport = transport;
    this.id = id;
  }

  /** Returns the last commit sequence acknowledged by this bridge. */
  get currentSequence(): number {
    return this.#currentSequence;
  }

  /** Sends one snapshot-isolated QUERY and decodes its Arrow IPC response. */
  query<T = Record<string, unknown>>(table: string, sql: string): SqlStorageCursor<T> {
    const response = this.send(`QUERY ${table} ${encodeUtf8Hex(sql)}`);
    const decode = (line: string): SqlStorageResult<T> => decodeArrowResponse(line) as SqlStorageResult<T>;
    if (isThenable(response)) return SqlStorageCursor.pending(Promise.resolve(response).then(decode));
    return new SqlStorageCursor(decode(response));
  }

  /** Declares one durable table schema through the authoritative endpoint. */
  async register(table: string, columns: ArrowColumn[]): Promise<void> {
    if (this.#registeredTables.has(table)) return;
    const response = await toPromise(this.send(`REGISTER ${table} ${encodeHex(encodeArrowSchema(columns))}`));
    assertProtocolOk(response);
    this.#registeredTables.add(table);
  }

  /** Registers mutation tables and submits exactly one canonical COMMIT envelope. */
  async commit(buffer: ClientTransactionBuffer): Promise<void> {
    const schemas = new Map<string, ArrowColumn[]>();
    for (const mutation of buffer.mutations) if (!schemas.has(mutation.table)) schemas.set(mutation.table, mutation.columns);
    for (const [table, columns] of schemas) await this.register(table, columns);
    const canonical = encodeCanonicalTransaction({
      doId: this.id.toString(),
      transactionId: buffer.id,
      baseCommitSequence: buffer.baseCommitSequence,
      isolation: "snapshot",
      mutations: buffer.mutations,
    });
    const response = (await toPromise(this.send(`COMMIT ${encodeHex(canonical)}`))).trim();
    assertProtocolOk(response);
    const payload = response.startsWith("OK ") ? response.slice(3).trim() : "";
    if (!/^\d+$/.test(payload)) throw new Error(`invalid COMMIT sequence response: ${response}`);
    this.#currentSequence = Number(payload);
  }

  /** Sends one line through the configured transport, never creating local storage. */
  private send(command: string): string | Promise<string> {
    if (!this.#transport) throw new Error(STORAGE_CONFIGURATION_ERROR);
    return this.#transport.send(command);
  }
}

/** Stable configuration error shared by KV and SQL operations. */
const STORAGE_CONFIGURATION_ERROR =
  "Durable Object storage/sql requires a configured Verglas engine endpoint or an injected test transport; in-memory fallback is forbidden";


/** Options accepted by the local worker host. */
export interface WorkerRuntimeOptions<Env = Record<string, unknown>> {
  /** User module object, default export, or an import specifier. */
  module: WorkerModule<Env> | WorkerEntrypoint<Env> | string | Promise<WorkerModule<Env> | WorkerEntrypoint<Env>>;
  /** Non-DO bindings copied into the worker environment. */
  env?: Partial<Env>;
  /** Explicit bindings, including prebuilt namespaces. */
  bindings?: Record<string, unknown>;
  /** Constructor bindings converted into local Durable Object namespaces. */
  durableObjects?: Record<
    string,
    DurableObjectConstructor<any, any> | DurableObjectNamespace<any> | DurableObjectNamespaceBinding<any>
  >;
  /** Shared scripted or engine transport for object storage. */
  transport?: DurableObjectTransportFactory;
  /** Endpoint path or ID-dependent endpoint path for object sockets. */
  endpoint?: string | ((id: DurableObjectId) => string);
  /** Clock injected into all namespace alarm schedulers. */
  now?: () => number;
}

/** A worker module's default fetch entrypoint. */
export interface WorkerEntrypoint<Env = Record<string, unknown>> {
  /** Handles one worker request. */
  fetch(request: Request, env: Env, ctx: ExecutionContext): Response | Promise<Response>;
}

/** Shape accepted from a dynamically loaded worker module. */
export interface WorkerModule<Env = Record<string, unknown>> {
  /** Default-exported fetch entrypoint. */
  default: WorkerEntrypoint<Env>;
}

/** Cloudflare-compatible execution context passed to worker fetch. */
export interface ExecutionContext {
  /** Keeps asynchronous work alive after the response is returned. */
  waitUntil(promise: Promise<unknown>): void;
  /** Requests pass-through behavior when the worker throws. */
  passThroughOnException(): void;
}

/** Local execution-context implementation used by the worker shell. */
export class LocalExecutionContext implements ExecutionContext {
  readonly #tasks = new Set<Promise<unknown>>();
  #passThrough = false;

  /** Retains one promise until it settles. */
  waitUntil(promise: Promise<unknown>): void {
    this.#tasks.add(promise);
    void promise.then(
      () => this.#tasks.delete(promise),
      () => this.#tasks.delete(promise),
    );
  }

  /** Records the pass-through request for host integrations. */
  passThroughOnException(): void {
    this.#passThrough = true;
  }

  /** Reports whether pass-through was requested by the worker. */
  get passThroughRequested(): boolean {
    return this.#passThrough;
  }

  /** Waits for all promises retained by this request. */
  async waitUntilSettled(): Promise<void> {
    await Promise.all([...this.#tasks]);
  }
}

/** A local Node worker runtime that injects Durable Object namespace bindings. */
export class WorkerRuntime<Env = Record<string, unknown>> {
  readonly #moduleInput: WorkerRuntimeOptions<Env>["module"];
  readonly #baseEnv: Record<string, unknown>;
  readonly #bindings: Record<string, unknown>;
  readonly #durableObjects: Record<
    string,
    DurableObjectConstructor<any, any> | DurableObjectNamespace<any> | DurableObjectNamespaceBinding<any>
  >;
  readonly #transport?: DurableObjectTransportFactory;
  readonly #endpoint?: string | ((id: DurableObjectId) => string);
  readonly #now: () => number;
  readonly #namespaces = new Map<string, DurableObjectNamespace<any>>();
  readonly #env: Record<string, unknown>;
  #module?: WorkerEntrypoint<Env>;
  #modulePromise?: Promise<WorkerEntrypoint<Env>>;

  /** Builds a worker host from a default export and configured bindings. */
  constructor(options: WorkerRuntimeOptions<Env>);
  /** Builds a worker host from a module and separate binding options. */
  constructor(module: WorkerRuntimeOptions<Env>["module"], options?: Omit<WorkerRuntimeOptions<Env>, "module">);
  constructor(
    optionsOrModule: WorkerRuntimeOptions<Env> | WorkerRuntimeOptions<Env>["module"],
    suppliedOptions: Omit<WorkerRuntimeOptions<Env>, "module"> = {},
  ) {
    const options = typeof optionsOrModule === "object" && optionsOrModule !== null && "module" in optionsOrModule
      ? optionsOrModule as WorkerRuntimeOptions<Env>
      : { ...suppliedOptions, module: optionsOrModule } as WorkerRuntimeOptions<Env>;
    this.#moduleInput = options.module;
    this.#baseEnv = { ...(options.env ?? {}) };
    this.#bindings = { ...(options.bindings ?? {}) };
    this.#durableObjects = { ...(options.durableObjects ?? {}) };
    this.#transport = options.transport;
    this.#endpoint = options.endpoint;
    this.#now = options.now ?? Date.now;
    this.#env = { ...this.#baseEnv };
    this.initializeBindings();
  }

  /** Handles one request through the user's default worker export. */
  fetch(request: Request, context?: ExecutionContext): Promise<Response>;
  /** Handles one request with explicit Cloudflare-style environment and context. */
  fetch(request: Request, env: Env, context: ExecutionContext): Promise<Response>;
  async fetch(
    request: Request,
    envOrContext?: Env | ExecutionContext,
    suppliedContext?: ExecutionContext,
  ): Promise<Response> {
    const entrypoint = await this.loadEntrypoint();
    const hasExplicitEnv = suppliedContext !== undefined && envOrContext !== undefined && !isExecutionContext(envOrContext);
    const env = hasExplicitEnv ? (envOrContext as Env) : (this.#env as Env);
    const context = suppliedContext ?? (isExecutionContext(envOrContext) ? envOrContext : undefined) ?? new LocalExecutionContext();
    return await entrypoint.fetch(request, env, context);
  }

  /** Returns the fully assembled environment used by subsequent fetch calls. */
  get env(): Env {
    return this.#env as Env;
  }

  /** Creates namespace objects before publishing the final circular environment. */
  private initializeBindings(): void {
    for (const [name, binding] of Object.entries(this.#bindings)) {
      this.#env[name] = binding;
    }
    for (const [name, binding] of Object.entries(this.#durableObjects)) {
      const namespace = binding instanceof DurableObjectNamespace
        ? binding
        : isNamespaceBinding(binding)
          ? new DurableObjectNamespace({
              ...binding,
              transport: binding.transport ?? this.#transport,
              endpoint: binding.endpoint ?? this.#endpoint,
              env: binding.env ?? this.#env,
              now: binding.now ?? this.#now,
            })
          : new DurableObjectNamespace(binding, {
              transport: this.#transport,
              endpoint: this.#endpoint,
              env: this.#env,
              now: this.#now,
            });
      this.#namespaces.set(name, namespace);
      this.#env[name] = namespace;
    }
    for (const namespace of this.#namespaces.values()) namespace.configureEnvironment(this.#env);
  }

  /** Loads and validates a default worker fetch entrypoint exactly once. */
  private loadEntrypoint(): Promise<WorkerEntrypoint<Env>> {
    if (this.#module) return Promise.resolve(this.#module);
    if (!this.#modulePromise) {
      this.#modulePromise = this.resolveModule(this.#moduleInput).then((module) => {
        const entrypoint = isWorkerEntrypoint(module) ? module : module.default;
        if (!entrypoint || typeof entrypoint.fetch !== "function") {
          throw new Error("Worker module must default-export an object with fetch(request, env, ctx)");
        }
        this.#module = entrypoint as WorkerEntrypoint<Env>;
        return this.#module;
      });
    }
    return this.#modulePromise;
  }

  /** Resolves object, promise, or dynamic import worker module forms. */
  private async resolveModule(
    input: WorkerRuntimeOptions<Env>["module"],
  ): Promise<WorkerModule<Env> | WorkerEntrypoint<Env>> {
    if (typeof input === "string") return (await import(input)) as WorkerModule<Env>;
    return await input;
  }
}

/** Creates a local worker runtime from options. */
export function createWorkerRuntime<Env = Record<string, unknown>>(
  options: WorkerRuntimeOptions<Env>,
): WorkerRuntime<Env>;
/** Creates a local worker runtime from a module and separate options. */
export function createWorkerRuntime<Env = Record<string, unknown>>(
  module: WorkerRuntimeOptions<Env>["module"],
  options?: Omit<WorkerRuntimeOptions<Env>, "module">,
): WorkerRuntime<Env>;
export function createWorkerRuntime<Env>(
  optionsOrModule: WorkerRuntimeOptions<Env> | WorkerRuntimeOptions<Env>["module"],
  options: Omit<WorkerRuntimeOptions<Env>, "module"> = {},
): WorkerRuntime<Env> {
  if (typeof optionsOrModule === "object" && optionsOrModule !== null && "module" in optionsOrModule) {
    return new WorkerRuntime(optionsOrModule as WorkerRuntimeOptions<Env>);
  }
  return new WorkerRuntime({ ...options, module: optionsOrModule } as WorkerRuntimeOptions<Env>);
}

/** Alias used by hosts that call the local shell a Durable Object runtime. */
export const createDurableObjectRuntime = createWorkerRuntime;
/** Alias class for hosts that prefer an explicit Durable Object runtime name. */
export class DurableObjectRuntime<Env = Record<string, unknown>> extends WorkerRuntime<Env> {}

/** Creates a newline transport connected to one Unix worker socket. */
export function createUnixSocketTransport(socketPath: string): DurableObjectTransport {
  return {
    async send(commandLine: string): Promise<string> {
      const net = await import("node:net");
      return await new Promise<string>((resolve, reject) => {
        const socket = net.createConnection(socketPath);
        let response = "";
        socket.setEncoding("utf8");
        socket.on("data", (chunk: string) => {
          response += chunk;
        });
        socket.once("error", reject);
        socket.once("end", () => resolve(response.trim()));
        socket.once("connect", () => socket.end(`${commandLine}\n`));
      });
    },
  };
}

/** Converts one request input into a Request understood by user objects. */
function normalizeRequest(input: RequestInfo | URL, init?: RequestInit): Request {
  if (input instanceof Request && init === undefined) return input;
  return new Request(input, init);
}

/** Detects a Cloudflare execution context supplied to the host fetch. */
function isExecutionContext(value: unknown): value is ExecutionContext {
  return typeof value === "object" && value !== null &&
    typeof (value as ExecutionContext).waitUntil === "function" &&
    typeof (value as ExecutionContext).passThroughOnException === "function";
}

/** Detects one declarative namespace binding object. */
function isNamespaceBinding(
  value: unknown,
): value is DurableObjectNamespaceBinding<any> {
  return typeof value === "object" && value !== null && "class" in value && typeof value.class === "function";
}

/** Detects a default-export wrapper versus an entrypoint object. */
function isWorkerEntrypoint<Env>(
  value: WorkerModule<Env> | WorkerEntrypoint<Env>,
): value is WorkerEntrypoint<Env> {
  return typeof (value as WorkerEntrypoint<Env>).fetch === "function";
}

/** Returns true for a Promise-like command response. */
function isThenable<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return typeof value === "object" && value !== null && typeof (value as PromiseLike<T>).then === "function";
}

type MaybePromise<T> = T | PromiseLike<T>;

/** Normalizes a command response into a Promise for transaction lifecycle calls. */
function toPromise<T>(value: MaybePromise<T>): Promise<T> {
  return isThenable(value) ? Promise.resolve(value) : Promise.resolve(value);
}

/** Rejects a lifecycle response that carries an engine error. */
function assertProtocolOk(response: string): void {
  const trimmed = response.trim();
  if (trimmed.startsWith("ERR ")) throw new Error(trimmed.slice(4));
}

/** Decodes one endpoint OK/ERR line containing a hex Arrow IPC stream. */
function decodeArrowResponse(response: string): SqlStorageResult<Record<string, unknown>> {
  const trimmed = response.trim();
  if (trimmed.startsWith("ERR ")) throw new Error(trimmed.slice(4));
  const payload = trimmed === "OK" ? "" : trimmed.startsWith("OK ") ? trimmed.slice(3).trim() : "";
  if (!payload) return {};
  const decoded = decodeArrowStream(hexToBytes(payload));
  return {
    columns: decoded.columns.map((column) => column.name),
    rows: decoded.rows,
    rowsRead: decoded.rowsRead,
    rowsWritten: decoded.rowsWritten,
  };
}

/** Normalizes object and array rows into both cursor views. */
function normalizeSqlResult<T>(result: SqlStorageResult<T>): {
  columns: string[];
  rows: T[];
  rawRows: SqlStorageValue[][];
} {
  const sourceRows = result.rows ?? [];
  const first = sourceRows[0];
  const declaredColumns = result.columns ?? result.columnNames;
  const columns = declaredColumns
    ? [...declaredColumns]
    : first && !Array.isArray(first)
      ? Object.keys(first as Record<string, unknown>)
      : [];
  const rows = sourceRows.map((row) => {
    if (Array.isArray(row)) {
      return Object.fromEntries(columns.map((column, index) => [column, row[index]])) as T;
    }
    return row as T;
  });
  const rawRows = sourceRows.map((row) =>
    Array.isArray(row)
      ? row.map(toSqlStorageValue)
      : columns.map((column) => toSqlStorageValue((row as Record<string, unknown>)[column])),
  );
  return { columns, rows, rawRows };
}

/** Encodes values returned by a row object for `raw()`. */
function toSqlStorageValue(value: unknown): SqlStorageValue {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    value instanceof ArrayBuffer ||
    ArrayBuffer.isView(value)
  ) return value as SqlStorageValue;
  return JSON.stringify(value);
}

/** Encodes JSON values used by the private KV table. */
function encodeStoredValue(value: unknown): string {
  return JSON.stringify(value, (_key, current) => {
    if (typeof current === "bigint") return { __verglas_bigint__: current.toString() };
    if (current instanceof ArrayBuffer) return { __verglas_array_buffer__: bytesToHex(new Uint8Array(current)) };
    if (ArrayBuffer.isView(current)) return { __verglas_array_buffer__: bytesToHex(new Uint8Array(current.buffer, current.byteOffset, current.byteLength)) };
    return current;
  });
}

/** Decodes values stored in the private KV table. */
function decodeStoredValue(value: string | undefined): unknown {
  if (value === undefined) return undefined;
  return JSON.parse(value, (_key, current) => {
    if (isRecord(current) && typeof current.__verglas_bigint__ === "string") return BigInt(current.__verglas_bigint__);
    if (isRecord(current) && typeof current.__verglas_array_buffer__ === "string") return hexToArrayBuffer(current.__verglas_array_buffer__);
    return current;
  });
}

/** Selects the table token required by the endpoint QUERY command. */
function queryTable(sql: string): string {
  const match = /\b(?:FROM|INTO|UPDATE|JOIN)\s+(?:["`]([^"`]+)["`]|([A-Za-z_][A-Za-z0-9_.$]*))/i.exec(sql);
  const table = match?.[1] ?? match?.[2] ?? KV_TABLE;
  if (/\s/.test(table)) throw new Error(`invalid SQL table token: ${table}`);
  return table;
}

/** Substitutes positional SQL bindings because QUERY has one SQL token only. */
function renderSqlBindings(sql: string, bindings: SqlStorageValue[]): string {
  let index = 0;
  const rendered = sql.replace(/\?/g, () => {
    if (index >= bindings.length) throw new Error("SQL query has fewer bindings than placeholders");
    return sqlLiteral(bindings[index++]);
  });
  if (index !== bindings.length) throw new Error("SQL query has more bindings than placeholders");
  return rendered;
}

/** Renders one SQL literal without introducing protocol whitespace. */
function sqlLiteral(value: SqlStorageValue): string {
  if (value === null) return "NULL";
  if (typeof value === "string") return `'${value.replaceAll("'", "''")}'`;
  if (typeof value === "boolean") return value ? "1" : "0";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("SQL bindings must contain finite numbers");
    return String(value);
  }
  return `X'${bytesToHex(value instanceof ArrayBuffer ? new Uint8Array(value) : new Uint8Array(value.buffer, value.byteOffset, value.byteLength))}'`;
}

/** Computes one absolute expiration value from Cloudflare put options. */
function expirationSeconds(options: DurableObjectStoragePutOptions | undefined, now: () => number): number | null {
  if (!options) return null;
  if (options.expiration !== undefined && options.expirationTtl !== undefined) {
    throw new Error("Durable Object put cannot set both expiration and expirationTtl");
  }
  if (options.expiration !== undefined) return options.expiration;
  if (options.expirationTtl !== undefined) return Math.floor(now() / 1000) + options.expirationTtl;
  return null;
}

/** Escapes SQL LIKE metacharacters in a prefix binding. */
function escapeLike(value: string): string {
  return value.replace(/[\\%_]/g, (character) => `\\${character}`);
}

let transactionSequence = 0;

/** Allocates a process-unique UUID accepted by the engine envelope parser. */
function nextTransactionId(): string {
  transactionSequence += 1;
  const suffix = transactionSequence.toString(16).padStart(12, "0");
  return `00000000-0000-4000-8000-${suffix}`;
}

/** Derives a deterministic 256-bit ID without a runtime crypto dependency. */
function hashName(name: string): string {
  const bytes = new TextEncoder().encode(name);
  const prime = 1_099_511_628_211n;
  const mask = 0xffffffffffffffffn;
  const words: string[] = [];
  for (let seed = 0n; seed < 4n; seed += 1n) {
    let hash = (1469598103934665603n ^ (seed * 0x9e3779b97f4a7c15n)) & mask;
    for (const byte of bytes) {
      hash ^= BigInt(byte);
      hash = (hash * prime) & mask;
    }
    words.push(hash.toString(16).padStart(16, "0"));
  }
  return words.join("");
}

/** Converts random bytes into lower-case hexadecimal. */
function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** Narrows a decoded JSON object for the KV value reviver. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Decodes one stored hexadecimal byte array. */
function hexToArrayBuffer(hex: string): ArrayBuffer {
  const bytes = hexToBytes(hex);
  return bytes.slice().buffer as ArrayBuffer;
}

/** Creates state storage that fails on its first engine operation instead of falling back. */
function createUnconfiguredStorage(id: DurableObjectId): DurableObjectStorage {
  return new DurableObjectStorage(id);
}

/** Decodes a protocol hexadecimal string into bytes. */
function hexToBytes(hex: string): Uint8Array {
  if (!/^(?:[0-9a-fA-F]{2})*$/.test(hex)) throw new Error("invalid hexadecimal value");
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}
