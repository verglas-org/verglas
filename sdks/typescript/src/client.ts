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
  FollowHandler,
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
  return new VerglasClient<Namespaces>(transport, opts.endpoint, opts.token, opts.catalogUri, fetchImpl, opts.timeoutMs ?? DEFAULT_TIMEOUT_MS);
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
    "stage-create": false,
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

