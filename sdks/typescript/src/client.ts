// The client and Table handle: the whole read/write surface an artifact uses.

import { makeTransport, type Transport } from "./http";
import { VerglasHttpError } from "./http";
import { CatalogFeed, feedUrl, globalWebSocket } from "./feed";
import { NamespaceRuntime } from "./namespace";
import type {
  ChangeHandler,
  CommitOptions,
  CommitResult,
  ConnectOptions,
  CreateTableResult,
  EnsureTableResult,
  DeltaResult,
  FeedSubscription,
  FollowFeedOptions,
  FollowRowsOptions,
  DynamicNamespaceRegistry,
  NamespaceBindings,
  NamespaceManifest,
  NamespaceRegistry,
  KvDeleteResult,
  KvListOptions,
  KvListPage,
  KvPutOptions,
  KvPutResult,
  KvValue,
  FollowHandler,
  QueueEnqueueResult,
  QueueDelivery,
  QueueMessage,
  QueuePollResult,
  QueueReceipt,
  Row,
  ScanOptions,
  ScanResult,
  Snapshot,
  TableDefinition,
  Watermark,
} from "./types";

const DEFAULT_TIMEOUT_MS = 30_000;

/**
 * Opens a client against a Verglas endpoint (the self-hosted server's base URL).
 */
export function connect<Namespaces extends NamespaceRegistry = DynamicNamespaceRegistry>(
  opts: ConnectOptions,
): VerglasClient<Namespaces> {
  if (!opts.endpoint) throw new Error("connect: endpoint is required");
  if (!opts.token) throw new Error("connect: token is required");
  const fetchImpl = opts.fetch ?? globalThis.fetch;
  if (typeof fetchImpl !== "function") {
    throw new Error("connect: no global fetch; pass one via ConnectOptions.fetch");
  }
  const transport = makeTransport(opts.endpoint, opts.token, fetchImpl, opts.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  const queueTransport = makeTransport(opts.accessEndpoint ?? opts.endpoint, opts.token, fetchImpl, opts.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  return new VerglasClient<Namespaces>(transport, queueTransport, opts.endpoint, opts.token, opts.catalogUri, fetchImpl, opts.timeoutMs ?? DEFAULT_TIMEOUT_MS);
}

/** A connected Verglas client. Cheap to hold; makes no requests until used. */
export class VerglasClient<Namespaces extends NamespaceRegistry = DynamicNamespaceRegistry> {
  /** The shared change-feed socket, opened lazily on the first `follow`. */
  private feed?: CatalogFeed;
  readonly #namespaces: NamespaceRuntime<Namespaces>;
  /** Resolved Iceberg REST catalog base, discovered lazily when unset. */
  #catalogUri?: string;
  #catalogTransport?: Transport;

  /** Integration APIs composed into this client through reflection. */
  readonly namespace: NamespaceBindings<Namespaces>;

  /** @internal */
  constructor(
    private readonly transport: Transport,
    private readonly queueTransport: Transport,
    /** The endpoint this client is bound to (for logging/diagnostics). */
    readonly endpoint: string,
    /** Bearer token, reused to authenticate the change-feed websocket. */
    private readonly token: string,
    catalogUri: string | undefined,
    private readonly fetchImpl: typeof fetch,
    private readonly timeoutMs: number,
  ) {
    this.#namespaces = new NamespaceRuntime<Namespaces>(transport);
    this.namespace = this.#namespaces.namespace;
    this.#catalogUri = catalogUri;
  }

  /** Lists all Integration namespace manifests visible to this principal. */
  reflect(): Promise<NamespaceManifest[]>;
  /** Reads one Integration namespace manifest and caches it for later invocations. */
  reflect(namespace: string): Promise<NamespaceManifest>;
  reflect(namespace?: string): Promise<NamespaceManifest | NamespaceManifest[]> {
    return namespace === undefined ? this.#namespaces.reflect() : this.#namespaces.manifest(namespace);
  }

  /** Returns a thin handle to one tenant-authorized KV namespace. */
  kv(namespace: string): Kv {
    if (!namespace) throw new Error("kv: namespace is required");
    return new Kv(this.transport, namespace);
  }

  /**
   * Follows table-commit notifications over the platform's edge change feed and
   * invokes `handler` for each commit to the named table(s). One websocket per
   * client carries every follow (multiplexed and filtered client-side), so this
   * never opens a long-lived connection to a tenant backend — the backend scales
   * to zero and the edge holds the socket while it sleeps.
   *
   * A `ChangeEvent` is a *notification* (seq, table, snapshot id, commit time),
   * not the rows. To read what changed, `delta` the table from a watermark. Pass
   * `opts.cursor` to replay past changes (an int seq) or omit it for live-only;
   * pass `opts.onResync` to learn when the edge drops replay history and the feed
   * falls back to live. Returns a handle — call `close()` (or abort
   * `opts.signal`) to end this follow; the socket closes with the last one.
   *
   * This is distinct from `Table.follow`, which polls the backend for *rows*.
   */
  follow(table: string | string[], handler: ChangeHandler, opts?: FollowFeedOptions): FeedSubscription {
    const tables = Array.isArray(table) ? table : [table];
    if (tables.length === 0 || tables.some((t) => !t)) {
      throw new Error("follow: at least one non-empty table name is required");
    }
    if (!this.feed) {
      this.feed = new CatalogFeed(feedUrl(this.endpoint), this.token, globalWebSocket());
    }
    return this.feed.follow(tables, handler, opts);
  }

  /**
   * Follows a table's ROWS, driven by the change feed instead of interval polling.
   * Subscribes to the table's commit notifications (`follow` above) and, on each
   * commit, delta-reads the newly committed rows and invokes
   * `handler(newRows, watermark)`. This is the row subscription primitive workers
   * and consumers use — a commit notification wakes a bounded `delta` read, so an
   * idle table costs nothing (the edge holds the socket while the backend sleeps).
   *
   * Starts from `opts.fromWatermark`, or the table's current snapshot when omitted
   * (only rows committed from here on are delivered). Batches are delivered in
   * commit order and `handler` is awaited before the next drain, so a slow handler
   * applies natural backpressure. If `handler` (or a delta read) throws and no
   * `onError` is given, the subscription closes and `closed` rejects. Call
   * `close()` (or abort `opts.signal`) to stop.
   */
  followRows<T extends Row = Row>(
    table: string,
    handler: FollowHandler<T>,
    opts?: FollowRowsOptions,
  ): FeedSubscription {
    if (!table) throw new Error("followRows: a non-empty table name is required");
    const handle = this.table<T>(table);

    // The tracked position. When no starting watermark is given, capture the
    // current snapshot EAGERLY (at call time, before any later commit lands), so
    // we deliver exactly the rows committed from here on — not from first change.
    let watermark: Watermark | undefined = opts?.fromWatermark;
    const started: Promise<void> =
      opts?.fromWatermark !== undefined
        ? Promise.resolve()
        : handle.snapshot().then((s) => void (watermark = s.watermark));

    // Serialize drains so commits are processed one at a time, in order, with the
    // handler awaited (backpressure). A pending drain coalesces further changes.
    let draining: Promise<void> = Promise.resolve();
    let closed = false;
    let rejectClosed: ((err: unknown) => void) | undefined;

    const drain = async (): Promise<void> => {
      await started;
      for (;;) {
        if (closed) return;
        const d: DeltaResult<T> = await handle.delta(watermark as Watermark, { limit: opts?.batchSize });
        watermark = d.watermark;
        if (d.rows.length === 0) return;
        await handler(d.rows, d.watermark);
      }
    };

    const onChange: ChangeHandler = () => {
      draining = draining.then(drain).catch((err) => {
        if (opts?.onError) {
          opts.onError(err);
          return;
        }
        closed = true;
        sub.close();
        rejectClosed?.(err);
      });
    };

    const sub = this.follow(table, onChange, {
      cursor: opts?.cursor,
      onResync: opts?.onResync,
      signal: opts?.signal,
    });

    // Wrap the feed subscription's `closed` so an unhandled error rejects it, the
    // way the old row-poller's `done` promise rejected.
    const closedPromise = new Promise<void>((resolve, reject) => {
      rejectClosed = reject;
      sub.closed.then(resolve, reject);
    });
    return {
      close: () => {
        closed = true;
        sub.close();
      },
      closed: closedPromise,
    };
  }

  /** A handle to one table by fully-qualified name (e.g. `demo.job_runs`). */
  table<T extends Row = Row>(name: string): Table<T> {
    if (!name) throw new Error("table: name is required");
    return new Table<T>(this.transport, name);
  }

  /**
   * A handle to one explicitly provisioned PostgreSQL-backed queue.
   */
  queue<T extends Row = Row>(name: string): Queue<T> {
    if (!name) throw new Error("queue: name is required");
    return new Queue<T>(this.queueTransport, name);
  }

  /**
   * Creates a table from an explicit schema and partition spec. Use this when the
   * table needs exact column types (decimals, dates), per-column nullability, or
   * a partition spec (month transform, several columns) that the schema inference
   * on the first commit cannot express. The SDK does not build the table in JS —
   * it POSTs the definition to the endpoint, which owns the catalog. Returns the
   * table name and its final column list.
   */
  /**
   * Creates a table in the Iceberg REST catalog from an explicit schema and
   * partition spec. Prefer `ensureTable` when the table may already exist.
   */
  async createTable(name: string, def: TableDefinition): Promise<CreateTableResult> {
    if (!name) throw new Error("createTable: name is required");
    if (!def?.schema?.length) throw new Error("createTable: schema is required");
    const expected = { schema: def.schema, partitions: def.partitions ?? [] };
    await this.#createCatalogTable(name, expected);
    return { table: name, columns: expected.schema.map((column) => column.name) };
  }

  /** Creates a missing table or verifies its exact existing definition via Iceberg REST. */
  async ensureTable(name: string, def: TableDefinition): Promise<EnsureTableResult> {
    if (!name) throw new Error("ensureTable: name is required");
    const expected = { schema: def.schema, partitions: def.partitions ?? [] };
    const catalog = await this.#catalog();
    const { namespacePath, tableName } = splitTableName(name);
    try {
      const loaded = await catalog.request<unknown>(
        "GET",
        `/v1/namespaces/${namespacePath}/tables/${encodeURIComponent(tableName)}`,
      );
      const actual = definitionFromLoadResponse(loaded);
      if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        throw new Error(`ensureTable: ${name} definition mismatch`);
      }
      return "existing";
    } catch (error) {
      if (!(error instanceof VerglasHttpError) || error.status !== 404) throw error;
      await this.#createCatalogTable(name, expected);
      return "created";
    }
  }

  /** Resolves the Iceberg REST transport, discovering the catalog URI when needed. */
  async #catalog(): Promise<Transport> {
    if (this.#catalogTransport) return this.#catalogTransport;
    if (!this.#catalogUri) {
      const access = await this.transport.request<{ catalog_uri?: string }>("GET", "/admin/access");
      if (!access?.catalog_uri) {
        throw new Error("connect: catalog URI is required; pass ConnectOptions.catalogUri or configure /admin/access");
      }
      this.#catalogUri = access.catalog_uri;
    }
    this.#catalogTransport = makeTransport(
      this.#catalogUri,
      this.token,
      this.fetchImpl,
      this.timeoutMs,
    );
    return this.#catalogTransport;
  }

  /** Creates the Iceberg namespace (if needed) and table for `name`. */
  async #createCatalogTable(name: string, def: TableDefinition): Promise<void> {
    const catalog = await this.#catalog();
    const { namespace, namespacePath, tableName } = splitTableName(name);
    try {
      await catalog.request("POST", "/v1/namespaces", {
        body: { namespace, properties: {} },
      });
    } catch (error) {
      if (!(error instanceof VerglasHttpError) || (error.status !== 409 && error.status !== 400)) {
        throw error;
      }
    }
    await catalog.request("POST", `/v1/namespaces/${namespacePath}/tables`, {
      body: catalogCreateRequest(tableName, def),
    });
  }

  /** Executes SQL through the configured database query endpoint. */
}

/** A thin raw-byte handle to one KV namespace. */
export class Kv {
  /** @internal */
  constructor(
    private readonly transport: Transport,
    readonly namespace: string,
  ) {}

  private base(): string {
    return `/v1/kv/${encodeURIComponent(this.namespace)}`;
  }

  private path(key: string): string {
    if (!key) throw new Error("KV key is required");
    return `${this.base()}/${encodeURIComponent(key)}`;
  }

  /** Durably sets raw bytes and returns the server-owned version. */
  async put(key: string, bytes: Uint8Array, opts?: KvPutOptions): Promise<KvPutResult> {
    if (opts?.ttlSeconds !== undefined && opts.expiresAtMs !== undefined) {
      throw new Error("KV put accepts ttlSeconds or expiresAtMs, not both");
    }
    const headers: Record<string, string> = {};
    if (opts?.ttlSeconds !== undefined) headers["x-verglas-ttl-seconds"] = String(opts.ttlSeconds);
    if (opts?.expiresAtMs !== undefined) headers["x-verglas-expires-at-ms"] = String(opts.expiresAtMs);
    if (opts?.contentType) headers["content-type"] = opts.contentType;
    if (opts?.ifMatch) headers["if-match"] = opts.ifMatch;
    if (opts?.ifNoneMatch) headers["if-none-match"] = "*";
    if (opts?.idempotencyKey) headers["idempotency-key"] = opts.idempotencyKey;
    for (const [name, value] of Object.entries(opts?.metadata ?? {})) {
      headers[`x-verglas-meta-${name}`] = value;
    }
    const response = await this.transport.requestRaw("PUT", this.path(key), {
      body: Uint8Array.from(bytes).buffer,
      headers,
    });
    const version = response.headers.get("etag");
    if (!version) throw new Error("KV put response has no ETag");
    return {
      version,
      idempotent: response.headers.get("x-verglas-idempotent") === "true",
    };
  }

  /** Gets raw bytes, or null when the key is absent or expired. */
  async get(key: string): Promise<KvValue | null> {
    let response: Response;
    try {
      response = await this.transport.requestRaw("GET", this.path(key));
    } catch (error) {
      if (error instanceof VerglasHttpError && error.status === 404) return null;
      throw error;
    }
    const version = response.headers.get("etag");
    const modified = response.headers.get("x-verglas-modified-at-ms");
    if (!version || !modified) throw new Error("KV get response is missing metadata headers");
    const metadata: Record<string, string> = {};
    response.headers.forEach((value, name) => {
      if (name.startsWith("x-verglas-meta-")) metadata[name.slice("x-verglas-meta-".length)] = value;
    });
    const expires = response.headers.get("x-verglas-expires-at-ms");
    return {
      bytes: new Uint8Array(await response.arrayBuffer()),
      version,
      contentType: response.headers.get("content-type") ?? undefined,
      modifiedAtMs: Number(modified),
      expiresAtMs: expires === null ? undefined : Number(expires),
      metadata,
      tier: (() => {
        const tier = response.headers.get("x-verglas-kv-tier");
        return tier === "ram" || tier === "nvme" ? tier : "unspecified";
      })(),
    };
  }

  /** Deletes one key idempotently with an optional expected version. */
  async delete(key: string, opts?: { ifMatch?: string }): Promise<KvDeleteResult> {
    const response = await this.transport.requestRaw("DELETE", this.path(key), {
      headers: opts?.ifMatch ? { "if-match": opts.ifMatch } : undefined,
    });
    return (await response.json()) as KvDeleteResult;
  }

  /** Lists one bounded metadata-only prefix page without interpreting its cursor. */
  async list(opts?: KvListOptions): Promise<KvListPage> {
    const page = await this.transport.request<{
      entries: Array<{
        key: string;
        version: string;
        modified_at_ms: number;
        expires_at_ms?: number;
        content_type?: string;
        metadata: Record<string, string>;
      }>;
      next_cursor?: string;
    }>("GET", this.base(), {
      query: { prefix: opts?.prefix, limit: opts?.limit, cursor: opts?.cursor },
    });
    return {
      entries: page.entries.map((entry) => ({
        key: entry.key,
        version: entry.version,
        modifiedAtMs: entry.modified_at_ms,
        expiresAtMs: entry.expires_at_ms,
        contentType: entry.content_type,
        metadata: entry.metadata,
      })),
      nextCursor: page.next_cursor,
    };
  }
}

/** A read/write handle to a single Verglas table. */
export class Table<T extends Row = Row> {
  /** @internal */
  constructor(
    private readonly transport: Transport,
    readonly name: string,
  ) {}

  private base(): string {
    return `/v1/tables/${encodeURIComponent(this.name)}`;
  }

  /** The current snapshot's metadata — a cheap poll, reads no rows. */
  snapshot(): Promise<Snapshot> {
    return this.transport.request<Snapshot>("GET", `${this.base()}/snapshot`);
  }

  /** Reads a page of rows from the current snapshot. */
  scan(opts?: ScanOptions): Promise<ScanResult<T>> {
    return this.transport.request<ScanResult<T>>("GET", `${this.base()}/rows`, {
      query: { limit: opts?.limit, cursor: opts?.cursor },
    });
  }

  /**
   * Reads rows committed after `sinceWatermark`. Pass the previous
   * `DeltaResult.watermark` (or a `ScanResult.watermark`) to walk forward. When
   * nothing new has committed, `rows` is empty and the watermark is unchanged.
   */
  delta(sinceWatermark: Watermark, opts?: { limit?: number }): Promise<DeltaResult<T>> {
    return this.transport.request<DeltaResult<T>>("GET", `${this.base()}/delta`, {
      query: { since: sinceWatermark, limit: opts?.limit },
    });
  }


  /**
   * Appends a batch of rows as JSONL through `POST /v1/ingest/{name}`. The SDK
   * does not build Parquet or commit Iceberg metadata in JS — the write worker
   * owns that path.
   */
  async append(rows: T[], opts?: CommitOptions): Promise<CommitResult> {
    const jsonl = rows.map((row) => JSON.stringify(row)).join("\n");
    const response = await this.transport.requestRaw(
      "POST",
      `/v1/ingest/${encodeURIComponent(this.name)}`,
      {
        query: { mode: "append", format: "jsonl" },
        body: jsonl,
        headers: {
          "content-type": "application/x-ndjson",
          ...(opts?.idempotencyKey ? { "idempotency-key": opts.idempotencyKey } : {}),
        },
      },
    );
    const text = await response.text();
    return (text ? JSON.parse(text) : undefined) as CommitResult;
  }
}

/** Splits `ns.table` into Iceberg REST namespace segments and the table name. */
function splitTableName(table: string): { namespace: string[]; namespacePath: string; tableName: string } {
  const dot = table.lastIndexOf(".");
  if (dot <= 0 || dot === table.length - 1) {
    throw new Error(`table '${table}' must include a namespace and table name`);
  }
  const namespace = table.slice(0, dot).split(".");
  const tableName = table.slice(dot + 1);
  if (namespace.some((part) => !part) || !tableName) {
    throw new Error(`table '${table}' contains an empty identifier`);
  }
  return { namespace, namespacePath: namespace.map(encodeURIComponent).join("%1F"), tableName };
}

/** Builds the Iceberg REST create-table body from a Verglas table definition. */
function catalogCreateRequest(name: string, definition: TableDefinition): unknown {
  const ids = new Map<string, number>();
  const fields = definition.schema.map((column, index) => {
    const id = index + 1;
    ids.set(column.name, id);
    return {
      id,
      name: column.name,
      required: column.nullable === false,
      type: catalogType(column.type),
    };
  });
  const partitionFields = (definition.partitions ?? []).map((partition, index) => {
    const sourceId = ids.get(partition.source);
    if (sourceId === undefined) {
      throw new Error(`partition source '${partition.source}' is not a table column`);
    }
    return {
      "source-id": sourceId,
      "field-id": 1000 + index,
      name: `${partition.source}_${partition.transform}`,
      transform: partition.transform,
    };
  });
  return {
    name,
    schema: { type: "struct", "schema-id": 0, fields },
    "partition-spec": { "spec-id": 0, fields: partitionFields },
  };
}

/** Maps a Verglas/Arrow type name onto an Iceberg REST primitive type string. */
function catalogType(typeName: string): string {
  switch (typeName) {
    case "int64":
      return "long";
    case "int32":
      return "int";
    case "float64":
    case "double":
      return "double";
    case "float32":
    case "float":
      return "float";
    case "utf8":
    case "string":
      return "string";
    case "bool":
    case "boolean":
      return "boolean";
    case "date32":
      return "date";
    default:
      if (typeName.startsWith("decimal")) return typeName.replace("decimal128", "decimal");
      return typeName;
  }
}

/** Extracts a Verglas table definition from an Iceberg REST load-table response. */
function definitionFromLoadResponse(loaded: unknown): TableDefinition {
  const root = loaded as {
    metadata?: { schemas?: Array<{ fields?: CatalogField[] }>; "partition-specs"?: Array<{ fields?: CatalogPartition[] }> };
    schema?: { fields?: CatalogField[] };
    "partition-spec"?: { fields?: CatalogPartition[] };
  };
  const fields = root.metadata?.schemas?.[0]?.fields ?? root.schema?.fields ?? [];
  const partitions = root.metadata?.["partition-specs"]?.[0]?.fields ?? root["partition-spec"]?.fields ?? [];
  const idToName = new Map<number, string>();
  for (const [index, field] of fields.entries()) {
    idToName.set(field.id ?? index + 1, field.name);
  }
  return {
    schema: fields.map((field) => ({
      name: field.name,
      type: reverseCatalogType(String(field.type)),
      nullable: field.required !== true,
    })),
    partitions: partitions.map((field) => ({
      source: idToName.get(field["source-id"]) ?? String(field["source-id"]),
      transform: field.transform === "month" ? ("month" as const) : ("identity" as const),
    })),
  };
}

/** One Iceberg REST schema field. */
type CatalogField = { id?: number; name: string; required?: boolean; type: string };
/** One Iceberg REST partition-spec field. */
type CatalogPartition = { "source-id": number; transform: string };

/** Maps an Iceberg primitive type name back to the Verglas/Arrow name used in definitions. */
function reverseCatalogType(typeName: string): string {
  switch (typeName) {
    case "long":
      return "int64";
    case "int":
      return "int32";
    case "double":
      return "float64";
    case "float":
      return "float32";
    case "string":
      return "utf8";
    case "boolean":
      return "bool";
    case "date":
      return "date32";
    default:
      return typeName;
  }
}

/**
 * A read/write handle to a single Verglas queue — the queue output type.
 *
 * A queue is an independently scalable service over a dedicated Neon database.
 * Poll returns exclusive expiring deliveries and ack requires the exact receipt.
 *
 * # Semantics: at-least-once with consumer-side idempotency
 *
 * A crash before ack lets the lease expire and redelivers the message under a
 * higher generation. A stale receipt is rejected rather than acknowledging a
 * newer consumer's delivery.
 */
export class Queue<T extends Row = Row> {
  /** @internal */
  constructor(
    private readonly transport: Transport,
    readonly name: string,
  ) {}

  private base(): string {
    return `/v1/queues/${encodeURIComponent(this.name)}`;
  }

  /** Appends messages to the queue. */
  enqueue(messages: QueueMessage<T>[]): Promise<QueueEnqueueResult> {
    return this.transport.request<QueueEnqueueResult>("POST", `${this.base()}/enqueue`, {
      body: { messages },
    });
  }

  /**
   * Claims up to `max` exclusive messages for one consumer process.
   */
  poll(group: string, opts: { owner: string; topics: string[]; max?: number; leaseSeconds: number }): Promise<QueuePollResult<T>> {
    if (!group) throw new Error("poll: group is required");
    if (!opts.owner) throw new Error("poll: owner is required");
    if (opts.topics.length === 0) throw new Error("poll: topics are required");
    return this.transport.request<QueuePollResult<T>>("POST", `${this.base()}/poll`, {
      body: { group, owner: opts.owner, topics: opts.topics, max: opts.max ?? 256, leaseSeconds: opts.leaseSeconds },
    });
  }

  /** Pushes matching deliveries and reconnects without issuing poll requests. */
  async *subscribe(
    group: string,
    opts: { owner: string; topics: string[]; max?: number; leaseSeconds: number },
  ): AsyncGenerator<QueueDelivery<T>> {
    if (!group) throw new Error("subscribe: group is required");
    if (!opts.owner) throw new Error("subscribe: owner is required");
    if (opts.topics.length === 0) throw new Error("subscribe: topics are required");
    let delay = 250;
    for (;;) {
      try {
        const response = await this.transport.requestRaw("POST", `${this.base()}/subscribe`, {
          headers: { "content-type": "application/json", accept: "application/x-ndjson" },
          body: JSON.stringify({
            group,
            owner: opts.owner,
            topics: opts.topics,
            max: opts.max ?? 256,
            leaseSeconds: opts.leaseSeconds,
          }),
        });
        delay = 250;
        const reader = response.body?.getReader();
        if (!reader) throw new Error("subscribe: response body is unavailable");
        const decoder = new TextDecoder();
        let buffered = "";
        for (;;) {
          const chunk = await reader.read();
          if (chunk.done) break;
          buffered += decoder.decode(chunk.value, { stream: true });
          let newline = buffered.indexOf("\n");
          while (newline >= 0) {
            const frame = buffered.slice(0, newline);
            buffered = buffered.slice(newline + 1);
            if (frame) yield JSON.parse(frame) as QueueDelivery<T>;
            newline = buffered.indexOf("\n");
          }
        }
      } catch (error) {
        if (error instanceof VerglasHttpError && error.status < 500) throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
      delay = Math.min(delay * 2, 30_000);
    }
  }

  /**
   * Acknowledges exactly the live delivery generation after committing work.
   */
  ack(group: string, receipt: QueueReceipt): Promise<void> {
    if (!group) throw new Error("ack: group is required");
    return this.transport.request<void>("POST", `${this.base()}/ack`, {
      body: { group, receipt },
    });
  }
}
