import {
  extractWorkerSource,
  VerglasHttpError,
  type CreateDatabaseRequest,
  type DatabaseView,
  type JobSummary,
  type WorkerRow,
} from "@verglas/sdk";
import type {
  VerglasCatalogSnapshot,
  VerglasCreateDatabaseInput,
  VerglasCreateTableInput,
  VerglasDatabaseCapabilities,
  VerglasDatabaseDefinition,
  VerglasDatabaseDetail,
  VerglasDatabaseSummary,
  VerglasGraphSummary,
  VerglasIntegrationConfiguration,
  VerglasQueryResult,
  VerglasTableSummary,
  VerglasVesselSummary,
  VerglasWorkerDetail,
  VerglasWorkerRunSummary,
  VerglasWorkerSummary,
} from "@verglas/workshop-shared/api";
import {
  resolveLocalContainerRuntimeConfigured,
  verglasAdmin,
  verglasDatabaseAccess,
  verglasRuntime,
  verglasScheduler,
  type VerglasClientEnv,
} from "./verglas-clients";

/** Environment values used by the local Verglas catalog adapter. */
export type VerglasCatalogEnv = VerglasClientEnv;

type IcebergTableIdentifier = {
  namespace: string[];
  name: string;
};

function mapWorker(
  row: WorkerRow,
  runs?: VerglasWorkerRunSummary[],
): VerglasWorkerSummary {
  return {
    name: row.name,
    state: row.state,
    placement: row.placement,
    output: row.output ?? "",
    triggers: row.triggers,
    createdBy: row.created_by,
    revision: row.revision,
    createdAt: row.created_at,
    recentRuns: runs,
    activeRun: runs?.some(
      (run) => run.state === "running" || run.state === "pending",
    ),
  };
}

function mapRun(job: JobSummary): VerglasWorkerRunSummary {
  return {
    jobId: job.job_id,
    state: job.state,
    createdAt: job.created_at,
    completedAt: job.completed_at,
    rowsProduced: job.rows_produced,
    errorMessage: job.error_message,
  };
}

/** Reads local worker and Vessel metadata through the official Verglas SDK. */
export class VerglasCatalogClient {
  readonly #env: VerglasCatalogEnv;
  readonly #fetch: typeof fetch;
  readonly #accessToken?: string | Promise<string>;
  readonly #catalogBases = new Map<string, Promise<string>>();

  constructor(
    env: VerglasCatalogEnv,
    fetcher: typeof fetch = fetch,
    accessToken?: string | Promise<string>,
  ) {
    resolveLocalContainerRuntimeConfigured(env);
    this.#env = env;
    this.#fetch = fetcher.bind(globalThis);
    this.#accessToken = accessToken;
  }

  /** Lists active workers, optionally enriched with recent run dots. */
  async listWorkers(opts?: {
    withRuns?: boolean;
  }): Promise<VerglasWorkerSummary[]> {
    const scheduler = verglasScheduler(this.#env, this.#fetch);
    const rows = await scheduler.listWorkers("active");
    if (!opts?.withRuns) return rows.map((row) => mapWorker(row));
    return await Promise.all(
      rows.map(async (row) => {
        try {
          const jobs = await scheduler.listWorkerJobs(row.name, 12);
          return mapWorker(row, jobs.map(mapRun));
        } catch {
          return mapWorker(row);
        }
      }),
    );
  }

  /** Full worker detail for the Jobs page. */
  async getWorker(name: string): Promise<VerglasWorkerDetail> {
    const scheduler = verglasScheduler(this.#env, this.#fetch);
    const row = await scheduler.getWorker(name);
    let recentRuns: VerglasWorkerRunSummary[] | undefined;
    try {
      recentRuns = (await scheduler.listWorkerJobs(name, 20)).map(mapRun);
    } catch {
      // Scheduler may be briefly unavailable; detail still useful.
    }
    return {
      ...mapWorker(row, recentRuns),
      sourceCode: extractWorkerSource(row.config),
      config: row.config,
    };
  }

  async listWorkerJobs(
    name: string,
    limit = 20,
  ): Promise<VerglasWorkerRunSummary[]> {
    return (
      await verglasScheduler(this.#env, this.#fetch).listWorkerJobs(name, limit)
    ).map(mapRun);
  }

  async runWorker(
    name: string,
    idempotencyKey: string,
  ): Promise<{ jobId: string; created: boolean }> {
    const result = await verglasScheduler(this.#env, this.#fetch).runWorker(
      name,
      idempotencyKey,
    );
    return { jobId: result.job_id, created: result.created };
  }

  async setWorkerState(
    name: string,
    state: "running" | "paused" | "archived",
  ): Promise<void> {
    await verglasScheduler(this.#env, this.#fetch).setWorkerState(name, state);
  }

  async listTables(): Promise<VerglasTableSummary[]> {
    return (await this.getCatalog()).tables;
  }

  /** Lists bounded namespaces and tables from one Lakehouse's database-scoped catalog. */
  async #listCatalogTables(
    database: Extract<VerglasDatabaseDefinition, { type: "lakehouse" }>,
  ): Promise<{ namespaces: string[][]; tables: VerglasTableSummary[] }> {
    const admin = verglasAdmin(
      this.#env,
      await this.#requireAccessToken(),
      this.#fetch,
    );
    const catalogBase = await this.#catalogBase(database.name);
    const namespaceBody = await admin.getJson<{ namespaces?: string[][] }>(
      `${catalogBase}/namespaces`,
    );
    const namespaces = (namespaceBody.namespaces ?? []).slice(0, 100);
    const identifiers: IcebergTableIdentifier[] = [];
    for (const namespace of namespaces) {
      const encoded = encodeURIComponent(namespace.join("\u001f"));
      const tableBody = await admin.getJson<{
        identifiers?: IcebergTableIdentifier[];
      }>(`${catalogBase}/namespaces/${encoded}/tables`);
      const remaining = 1000 - identifiers.length;
      identifiers.push(
        ...(tableBody.identifiers ?? []).slice(0, Math.min(500, remaining)),
      );
      if (identifiers.length === 1000) break;
    }

    const tables = identifiers
      .map(({ namespace, name }) => ({
        database: database.name,
        namespace,
        name,
        qualifiedName: [...namespace, name].map(quoteIdentifier).join("."),
      }))
      .toSorted((a, b) => a.qualifiedName.localeCompare(b.qualifiedName));
    return { namespaces, tables };
  }

  /** Resolves the Iceberg REST prefix advertised for one database warehouse. */
  async #catalogBase(name: string): Promise<string> {
    const database = validateIdentifier(name, "Database name");
    let pending = this.#catalogBases.get(database);
    if (!pending) {
      pending = this.#resolveCatalogBase(database);
      this.#catalogBases.set(database, pending);
    }
    return await pending;
  }

  /** Negotiates one mounted Iceberg catalog rather than assuming an unprefixed warehouse. */
  async #resolveCatalogBase(database: string): Promise<string> {
    const admin = verglasAdmin(
      this.#env,
      await this.#requireAccessToken(),
      this.#fetch,
    );
    const mount = databaseCatalogMount(database);
    type CatalogConfig = {
      defaults?: { prefix?: string };
      overrides?: { prefix?: string };
    };
    let config: CatalogConfig | undefined;
    for (let attempt = 0; attempt < 6; attempt++) {
      try {
        config = await admin.getJson<CatalogConfig>(`${mount}/v1/config`);
        break;
      } catch (error) {
        if (
          !(error instanceof VerglasHttpError) ||
          error.status !== 404 ||
          attempt === 5
        )
          throw error;
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }
    if (!config)
      throw new Error(
        `Database '${database}' catalog did not become available.`,
      );
    const prefix = config.overrides?.prefix ?? config.defaults?.prefix;
    return prefix ? `${mount}/v1/${encodeURIComponent(prefix)}` : `${mount}/v1`;
  }

  /** Lists public database definitions without exposing tenant or secret resource IDs. */
  async #listDatabases(): Promise<VerglasDatabaseDefinition[]> {
    const result = await verglasDatabaseAccess(
      this.#env,
      await this.#requireAccessToken(),
      this.#fetch,
    ).listDatabases();
    return result
      .map(mapDatabaseDefinition)
      .toSorted((left, right) => left.name.localeCompare(right.name));
  }

  /** Reads one public database definition by tenant-local name. */
  async #getDatabaseDefinition(
    name: string,
  ): Promise<VerglasDatabaseDefinition> {
    const database = validateIdentifier(name, "Database name");
    const result = await verglasDatabaseAccess(
      this.#env,
      await this.#requireAccessToken(),
      this.#fetch,
    ).getDatabase(database);
    return mapDatabaseDefinition(result);
  }

  async getCatalog(): Promise<VerglasCatalogSnapshot> {
    const definitions = await this.#listDatabases();
    const catalogs = await Promise.all(
      definitions.map(async (database) =>
        database.type === "lakehouse"
          ? await this.#listCatalogTables(database)
          : { namespaces: [], tables: [] },
      ),
    );
    const tables = catalogs
      .flatMap((catalog) => catalog.tables)
      .toSorted(
        (left, right) =>
          left.database.localeCompare(right.database) ||
          left.qualifiedName.localeCompare(right.qualifiedName),
      );
    const graphs = inferGraphs(tables);
    const vectors: VerglasCatalogSnapshot["vectors"] = [];
    return {
      databases: summarizeDatabases(definitions, tables, vectors, graphs),
      tables,
      vectors,
      graphs,
    };
  }

  /** Returns the selected resource and only operations safe for its database kind. */
  async getDatabase(name: string): Promise<VerglasDatabaseDetail> {
    const definition = await this.#getDatabaseDefinition(name);
    if (definition.type === "postgres")
      return { ...summarizeDatabase(definition, [], [], []), tables: [] };
    const { tables } = await this.#listCatalogTables(definition);
    const graphs = inferGraphs(tables);
    return { ...summarizeDatabase(definition, tables, [], graphs), tables };
  }

  /** Creates one top-level database resource through the tenant access service. */
  async createDatabase(
    input: VerglasCreateDatabaseInput,
  ): Promise<VerglasDatabaseSummary> {
    const request = createDatabaseRequest(input);
    const result = await verglasDatabaseAccess(
      this.#env,
      await this.#requireAccessToken(),
      this.#fetch,
    ).createDatabase(request);
    return summarizeDatabase(mapDatabaseDefinition(result), [], [], []);
  }

  /** Deletes a database resource after proving a Lakehouse contains no tables. */
  async deleteDatabase(name: string): Promise<void> {
    const database = await this.#getDatabaseDefinition(name);
    if (database.type === "lakehouse") {
      const { tables } = await this.#listCatalogTables(database);
      if (tables.length > 0) {
        throw new Error(
          `Database '${database.name}' contains ${tables.length} ${tables.length === 1 ? "table" : "tables"}.`,
        );
      }
    }
    await verglasDatabaseAccess(
      this.#env,
      await this.#requireAccessToken(),
      this.#fetch,
    ).deleteDatabase(database.name);
  }

  /** Requires the authenticated caller's short-lived access bearer for database-resource routes. */
  async #requireAccessToken(): Promise<string> {
    const token = (await this.#accessToken)?.trim();
    if (!token)
      throw new Error("A user-scoped Verglas access token is required.");
    return token;
  }

  /** Creates one explicitly-schemaed Iceberg table. */
  async createTable(
    input: VerglasCreateTableInput,
  ): Promise<VerglasTableSummary> {
    const database = await this.#requireLakehouse(
      input.database,
      "Iceberg table management",
    );
    const namespace = validateNamespace(input.namespace);
    const name = validateIdentifier(input.name, "Table name");
    if (!input.columns.length)
      throw new Error("A table requires at least one column.");
    const admin = verglasAdmin(
      this.#env,
      await this.#requireAccessToken(),
      this.#fetch,
    );
    const catalogBase = await this.#catalogBase(database.name);
    const namespaceBody = await admin.getJson<{ namespaces?: string[][] }>(
      `${catalogBase}/namespaces`,
    );
    if (
      !(namespaceBody.namespaces ?? []).some((candidate) =>
        sameNamespace(candidate, namespace),
      )
    ) {
      await admin.postJson<void>(`${catalogBase}/namespaces`, {
        namespace,
        properties: {},
      });
    }
    await admin.postJson<void>(
      `${catalogBase}/namespaces/${encodeNamespace(namespace)}/tables`,
      createTableRequest(name, input),
    );
    return {
      database: database.name,
      namespace,
      name,
      qualifiedName: [...namespace, name].map(quoteIdentifier).join("."),
    };
  }

  /** Deletes one Iceberg table. */
  async deleteTable(
    databaseName: string,
    namespace: string[],
    name: string,
  ): Promise<void> {
    const database = await this.#requireLakehouse(
      databaseName,
      "Iceberg table management",
    );
    const validatedNamespace = validateNamespace(namespace);
    const validatedName = validateIdentifier(name, "Table name");
    const admin = verglasAdmin(
      this.#env,
      await this.#requireAccessToken(),
      this.#fetch,
    );
    const catalogBase = await this.#catalogBase(database.name);
    await admin.deleteJson<void>(
      `${catalogBase}/namespaces/${encodeNamespace(validatedNamespace)}/tables/${encodeURIComponent(validatedName)}`,
    );
  }

  /** Requires an existing Lakehouse before issuing catalog mutations. */
  async #requireLakehouse(
    name: string,
    operation: "Iceberg table management" | "SQL query execution",
  ): Promise<Extract<VerglasDatabaseDefinition, { type: "lakehouse" }>> {
    const database = await this.#getDatabaseDefinition(name);
    if (database.type !== "lakehouse") {
      throw new Error(
        `Postgres database '${database.name}' does not expose ${operation}.`,
      );
    }
    return database;
  }

  async query(
    databaseName: string,
    sql: string,
    maxRows = 100,
  ): Promise<VerglasQueryResult> {
    const database = await this.#requireLakehouse(
      databaseName,
      "SQL query execution",
    );
    const normalizedSql = sql.trim().replace(/;+\s*$/, "");
    if (!normalizedSql) throw new Error("SQL query is required.");
    const boundedRows = Math.min(Math.max(Math.trunc(maxRows), 1), 500);
    const boundedSql = `SELECT * FROM (${normalizedSql}) AS __verglas_query LIMIT ${boundedRows + 1}`;
    const result = await verglasAdmin(
      this.#env,
      await this.#requireAccessToken(),
      this.#fetch,
    ).query(database.name, boundedSql);
    const truncated = result.rows.length > boundedRows;
    const rows = result.rows.slice(0, boundedRows);
    return { columns: result.columns, rows, rowCount: rows.length, truncated };
  }

  async listVessels(): Promise<VerglasVesselSummary[]> {
    const runtime = verglasRuntime(this.#env, this.#fetch);
    const vessels = await runtime.listVessels<VerglasVesselSummary>();
    return await Promise.all(
      vessels.map(async (vessel) => {
        if (vessel.role === "application") {
          return { ...vessel, previewUrl: runtime.previewUrl(vessel.name) };
        }
        try {
          const schema = await runtime.vesselHttp<
            Pick<VerglasIntegrationConfiguration, "title" | "description">
          >(vessel.name, "/v1/config/schema");
          return {
            ...vessel,
            title: schema.title,
            description: schema.description,
          };
        } catch {
          return vessel;
        }
      }),
    );
  }

  async getIntegrationConfiguration(
    name: string,
  ): Promise<VerglasIntegrationConfiguration> {
    const runtime = verglasRuntime(this.#env, this.#fetch);
    const [schema, state] = await Promise.all([
      runtime.vesselHttp<Omit<VerglasIntegrationConfiguration, "configured">>(
        name,
        "/v1/config/schema",
      ),
      runtime.vesselHttp<{ configured: boolean }>(name, "/v1/config"),
    ]);
    return { ...schema, configured: state.configured };
  }

  async configureIntegration(
    name: string,
    values: Record<string, string>,
  ): Promise<void> {
    await verglasRuntime(this.#env, this.#fetch).vesselHttp(
      name,
      "/v1/config",
      {
        method: "PUT",
        body: values,
      },
    );
  }

  async deleteVessel(name: string): Promise<void> {
    try {
      await verglasRuntime(this.#env, this.#fetch).deleteVessel(name);
    } catch (error) {
      if (
        error &&
        typeof error === "object" &&
        "status" in error &&
        error.status === 404
      )
        return;
      throw error;
    }
  }

  /** Persists the lifecycle state of an OSS Application Vessel. */
  async setApplicationState(
    name: string,
    state: "running" | "stopped",
  ): Promise<void> {
    const runtime = verglasRuntime(this.#env, this.#fetch);
    const vessel = (await runtime.listVessels<VerglasVesselSummary>()).find(
      (candidate) => candidate.name === name,
    );
    if (!vessel) throw new Error(`Application '${name}' was not found.`);
    if (vessel.role !== "application")
      throw new Error(`Vessel '${name}' is not an Application.`);
    if (state === "running") await runtime.resumeVessel(name);
    else await runtime.stopVessel(name);
  }
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

function validateNamespace(namespace: string[]): string[] {
  if (!namespace.length) throw new Error("Table namespace is required.");
  return namespace.map((part) => validateIdentifier(part, "Table namespace"));
}

function validateIdentifier(value: string, label: string): string {
  const normalized = value.trim();
  if (!normalized) throw new Error(`${label} is required.`);
  if (normalized.includes("\u001f"))
    throw new Error(`${label} cannot contain a unit separator.`);
  return normalized;
}

function encodeNamespace(namespace: string[]): string {
  return encodeURIComponent(namespace.join("\u001f"));
}

function sameNamespace(left: string[], right: string[]): boolean {
  return (
    left.length === right.length &&
    left.every((part, index) => part === right[index])
  );
}

function databaseCatalogMount(name: string): string {
  return `/v1/databases/${encodeURIComponent(validateIdentifier(name, "Database name"))}/catalog`;
}

function mapDatabaseDefinition(
  database: DatabaseView,
): VerglasDatabaseDefinition {
  if (database.type === "postgres") {
    return {
      type: "postgres",
      name: database.name,
      engine: { mode: "managed-neon" },
    };
  }
  return {
    type: "lakehouse",
    name: database.name,
    storage:
      database.storage.mode === "managed"
        ? { mode: "managed" }
        : { mode: "scoped-secret", dataPath: database.storage.data_path },
    catalog: database.catalog,
  };
}

function createDatabaseRequest(
  input: VerglasCreateDatabaseInput,
): CreateDatabaseRequest {
  const name = validateIdentifier(input.name, "Database name");
  if (input.type === "postgres") {
    return { type: "postgres", name, engine: { mode: "managed-neon" } };
  }
  return {
    type: "lakehouse",
    name,
    storage:
      input.storage.mode === "managed"
        ? { mode: "managed" }
        : { mode: "scoped-secret", data_path: input.storage.dataPath },
    catalog: input.catalog,
  };
}

function createTableRequest(
  name: string,
  input: VerglasCreateTableInput,
): unknown {
  const columns = new Map<string, number>();
  const fields = input.columns.map((column, index) => {
    const columnName = validateIdentifier(column.name, "Column name");
    const type = validateIdentifier(column.type, "Column type");
    if (columns.has(columnName))
      throw new Error(`Duplicate column '${columnName}'.`);
    const id = index + 1;
    columns.set(columnName, id);
    return {
      id,
      name: columnName,
      required: column.nullable === false,
      type: catalogType(type),
    };
  });
  const partitionFields = (input.partitions ?? []).map((partition, index) => {
    const source = validateIdentifier(partition.source, "Partition source");
    const sourceId = columns.get(source);
    if (sourceId === undefined)
      throw new Error(`Partition source '${source}' is not a table column.`);
    return {
      "source-id": sourceId,
      "field-id": 1000 + index,
      name: `${source}_${partition.transform}`,
      transform: partition.transform,
    };
  });
  return {
    name,
    schema: { type: "struct", "schema-id": 0, fields },
    "partition-spec": { "spec-id": 0, fields: partitionFields },
  };
}

function catalogType(type: string): string {
  switch (type) {
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
      return type.startsWith("decimal")
        ? type.replace("decimal128", "decimal")
        : type;
  }
}

function summarizeDatabases(
  definitions: VerglasDatabaseDefinition[],
  tables: VerglasTableSummary[],
  vectors: VerglasCatalogSnapshot["vectors"],
  graphs: VerglasGraphSummary[],
): VerglasDatabaseSummary[] {
  return definitions.map((database) =>
    summarizeDatabase(
      database,
      tables.filter((table) => table.database === database.name),
      vectors.filter((vector) => vector.database === database.name),
      graphs.filter((graph) => graph.database === database.name),
    ),
  );
}

function summarizeDatabase(
  database: VerglasDatabaseDefinition,
  tables: VerglasTableSummary[],
  vectors: VerglasCatalogSnapshot["vectors"],
  graphs: VerglasGraphSummary[],
): VerglasDatabaseSummary {
  return {
    ...database,
    capabilities: databaseCapabilities(database.type),
    tableCount: tables.length,
    vectorCount: vectors.length,
    graphCount: graphs.length,
  };
}

function databaseCapabilities(
  type: VerglasDatabaseDefinition["type"],
): VerglasDatabaseCapabilities {
  if (type === "lakehouse") {
    return {
      catalog: true,
      tableCrud: true,
      tableMetrics: false,
      vectors: false,
      graphs: true,
      query: true,
    };
  }
  return {
    catalog: false,
    tableCrud: false,
    tableMetrics: false,
    vectors: false,
    graphs: false,
    query: false,
  };
}

function inferGraphs(tables: VerglasTableSummary[]): VerglasGraphSummary[] {
  const byNamespace = new Map<string, Map<string, VerglasTableSummary>>();
  for (const table of tables) {
    if (table.namespace.length !== 1) continue;
    const namespace = table.namespace[0];
    const key = `${table.database}\u0000${namespace}`;
    const entries =
      byNamespace.get(key) ?? new Map<string, VerglasTableSummary>();
    entries.set(table.name, table);
    byNamespace.set(key, entries);
  }
  const graphs: VerglasGraphSummary[] = [];
  for (const [key, entries] of byNamespace) {
    const nodes = entries.get("nodes");
    const edges = entries.get("edges");
    if (nodes && edges) {
      const [database, namespace] = key.split("\u0000") as [string, string];
      graphs.push({
        database,
        namespace,
        nodesTable: nodes.qualifiedName,
        edgesTable: edges.qualifiedName,
      });
    }
  }
  return graphs.toSorted(
    (a, b) =>
      a.database.localeCompare(b.database) ||
      a.namespace.localeCompare(b.namespace),
  );
}
