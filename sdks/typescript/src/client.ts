// The client and Table handle: the whole read/write surface an artifact uses.

import { makeTransport, type Transport } from "./http";
import { VerglasHttpError } from "./http";
import type {
  CommitOptions,
  CommitResult,
  ConnectOptions,
  DeltaResult,
  CreateTableResult,
  EnsureTableResult,
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
export function connect(opts: ConnectOptions): VerglasClient {
  if (!opts.endpoint) throw new Error("connect: endpoint is required");
  if (!opts.token) throw new Error("connect: token is required");
  const fetchImpl = opts.fetch ?? globalThis.fetch;
  if (typeof fetchImpl !== "function") {
    throw new Error("connect: no global fetch; pass one via ConnectOptions.fetch");
  }
  const transport = makeTransport(opts.endpoint, opts.token, fetchImpl, opts.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  return new VerglasClient(transport, opts.endpoint, opts.token, opts.catalogUri, fetchImpl, opts.timeoutMs ?? DEFAULT_TIMEOUT_MS);
}

/** A connected Verglas client. Cheap to hold; makes no requests until used. */
export class VerglasClient {
  /** Resolved Iceberg REST catalog base, discovered lazily when unset. */
  #catalogUri?: string;
  #catalogTransport?: Transport;

  /** @internal */
  constructor(
    private readonly transport: Transport,
    /** The endpoint this client is bound to (for logging/diagnostics). */
    readonly endpoint: string,
    /** Bearer token used for catalog discovery and requests. */
    private readonly token: string,
    catalogUri: string | undefined,
    private readonly fetchImpl: typeof fetch,
    private readonly timeoutMs: number,
  ) {
    this.#catalogUri = catalogUri;
  }

  /** A handle to one table by fully-qualified name (e.g. `demo.job_runs`). */
  table<T extends Row = Row>(name: string): Table<T> {
    if (!name) throw new Error("table: name is required");
    return new Table<T>(this.transport, name);
  }

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

