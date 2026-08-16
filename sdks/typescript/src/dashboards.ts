export type DashboardSource =
  | { type: "table"; table: string; follow?: boolean }
  | { type: "graph"; graph: string; view: "k-hop" | "neighbors" | "paths"; from?: string; hops?: number; to?: string }
  | { type: "vector"; watch: string; index: string; queryPath?: string };

export interface DashboardElement {
  type: string;
  props?: Record<string, unknown>;
  children?: string[];
}

export interface DashboardSpec {
  spec_version: number;
  name: string;
  title: string;
  sources: Record<string, DashboardSource>;
  root: string;
  elements: Record<string, DashboardElement>;
  state?: Record<string, unknown>;
}

export interface DashboardDataClient {
  followRows(
    table: string,
    handler: (rows: unknown[]) => void,
  ): { close(): void; closed: Promise<unknown> };
  graph(): {
    kHop(from: string, hops: number): Promise<unknown[]>;
    neighbors(from: string): Promise<unknown[]>;
    paths(from: string, to: string): Promise<unknown[]>;
  };
  vectorSearch(index: string, query: unknown): Promise<unknown[]>;
}

export interface DashboardStateStore {
  set(path: string, value: unknown): void;
  get(path?: string): unknown;
}

async function refreshGraphSource(
  client: DashboardDataClient,
  source: Extract<DashboardSource, { type: "graph" }>,
  store: DashboardStateStore,
  sourceName: string,
): Promise<void> {
  const handle = client.graph();
  const from = source.from ?? "";
  if (source.view === "k-hop") {
    store.set(`/sources/${sourceName}/nodes`, await handle.kHop(from, source.hops ?? 1));
    return;
  }
  if (source.view === "neighbors") {
    store.set(`/sources/${sourceName}/nodes`, await handle.neighbors(from));
    return;
  }
  store.set(`/sources/${sourceName}/paths`, await handle.paths(from, source.to ?? ""));
}

async function refreshVectorSource(
  client: DashboardDataClient,
  source: Extract<DashboardSource, { type: "vector" }>,
  store: DashboardStateStore,
  sourceName: string,
  spec: DashboardSpec,
): Promise<void> {
  let query: unknown;
  if (source.queryPath) {
    query = store.get(source.queryPath) ?? spec.state?.[source.queryPath.replace(/^\//, "")];
  }
  store.set(`/sources/${sourceName}/hits`, await client.vectorSearch(source.index, query));
}

/** Binds live table/graph/vector feeds into a json-render state store. */
export async function bindDashboardSources(
  client: DashboardDataClient,
  spec: DashboardSpec,
  store: DashboardStateStore,
): Promise<{ close(): void }> {
  const subscriptions: Array<{ close(): void }> = [];

  for (const [sourceName, source] of Object.entries(spec.sources)) {
    if (source.type === "table") {
      if (source.follow === false) continue;
      subscriptions.push(client.followRows(source.table, (rows) => {
        store.set(`/sources/${sourceName}/rows`, rows);
      }));
      continue;
    }

    if (source.type === "graph") {
      await refreshGraphSource(client, source, store, sourceName);
      continue;
    }

    await refreshVectorSource(client, source, store, sourceName, spec);
    subscriptions.push(client.followRows(source.watch, () => {
      void refreshVectorSource(client, source, store, sourceName, spec);
    }));
  }

  return {
    close() {
      for (const sub of subscriptions) sub.close();
    },
  };
}
