import { createFileRoute, useNavigate } from "@tanstack/react-router";
import {
  ArrowRight,
  CirclesThreePlus,
  Database,
  DatabaseIcon,
  MagnifyingGlass,
  Plus,
  SpinnerGap,
  Table,
  Trash,
  VectorThree,
} from "@phosphor-icons/react";
import type {
  VerglasCatalogSnapshot,
  VerglasCreateDatabaseInput,
  VerglasDatabaseDetail,
  VerglasDatabaseSummary,
  VerglasGraphSummary,
  VerglasTableDetail,
  VerglasTableSummary,
  VerglasVectorSummary,
} from "@verglas/workshop-shared/api";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useAuthenticatedApi } from "../AuthContext";
import { getStoredSelectedModel } from "../modelSelection";
import { useDocumentTitle } from "../useDocumentTitle";
import { useAutomaticRefresh } from "../useAutomaticRefresh";
import {
  databaseCapabilityLabels,
  databaseKindLabel,
  databaseResourceDescription,
} from "../databasePresentation";
import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "../components/ui/alert-dialog";
import { Badge } from "../components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "../components/ui/dialog";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "../components/ui/tabs";

export const Route = createFileRoute("/data")({ component: DatabasesPage });

type CatalogKind = "table" | "vector" | "graph";
export type CatalogItem =
  | { kind: "table"; id: string; value: VerglasTableSummary }
  | { kind: "vector"; id: string; value: VerglasVectorSummary }
  | { kind: "graph"; id: string; value: VerglasGraphSummary };

export type NamespaceGroup = {
  name: string;
  namespace: string[];
  tables: VerglasTableSummary[];
};

type DatabaseAssets = {
  tables: VerglasTableSummary[];
  vectors: VerglasVectorSummary[];
  graphs: VerglasGraphSummary[];
};

type Metric = { label: string; value: string };

const EMPTY_CATALOG: VerglasCatalogSnapshot = {
  databases: [],
  tables: [],
  vectors: [],
  graphs: [],
};

function DatabasesPage() {
  useDocumentTitle("Databases");
  const { authenticatedApi } = useAuthenticatedApi();
  const navigate = useNavigate();
  const [catalog, setCatalog] = useState(EMPTY_CATALOG);
  const [selectedDatabaseName, setSelectedDatabaseName] = useState<
    string | null
  >(null);
  const [selectedAsset, setSelectedAsset] = useState<CatalogItem | null>(null);
  const [detail, setDetail] = useState<VerglasDatabaseDetail | null>(null);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [createDatabaseOpen, setCreateDatabaseOpen] = useState(false);
  const [createTableOpen, setCreateTableOpen] = useState(false);
  const [deleteDatabase, setDeleteDatabase] =
    useState<VerglasDatabaseSummary | null>(null);
  const [deleteTable, setDeleteTable] = useState<VerglasTableSummary | null>(
    null,
  );

  const database =
    catalog.databases.find(
      (candidate) => candidate.name === selectedDatabaseName,
    ) ??
    catalog.databases[0] ??
    null;
  const assets = useMemo(
    () => databaseAssets(catalog, database?.name ?? ""),
    [catalog, database?.name],
  );

  const loadCatalog = useCallback(
    async (showLoading = false) => {
      if (showLoading) setLoading(true);
      setError(null);
      try {
        const next = await authenticatedApi.getVerglasCatalog();
        setCatalog(next);
        setSelectedDatabaseName((current) =>
          next.databases.some((candidate) => candidate.name === current)
            ? current
            : (next.databases[0]?.name ?? null),
        );
        setSelectedAsset((current) =>
          current && catalogContains(next, current) ? current : null,
        );
      } catch (reason) {
        setError(errorMessage(reason));
      } finally {
        if (showLoading) setLoading(false);
      }
    },
    [authenticatedApi],
  );

  useEffect(() => {
    void loadCatalog(true);
  }, [loadCatalog]);
  useAutomaticRefresh(loadCatalog, 15_000);

  useEffect(() => {
    if (!database) {
      setDetail(null);
      return;
    }
    setDetail(null);
    let cancelled = false;
    void authenticatedApi
      .getVerglasDatabase(database.name)
      .then((next) => {
        if (!cancelled) setDetail(next);
      })
      .catch((reason) => {
        if (!cancelled) setError(errorMessage(reason));
      });
    return () => {
      cancelled = true;
    };
  }, [authenticatedApi, database?.name]);

  const createDatabase = useCallback(
    async (input: VerglasCreateDatabaseInput) => {
      setBusy(true);
      setError(null);
      try {
        const created = await authenticatedApi.createVerglasDatabase(input);
        setCreateDatabaseOpen(false);
        await loadCatalog();
        setSelectedDatabaseName(created.name);
        setSelectedAsset(null);
      } catch (reason) {
        setError(errorMessage(reason));
      } finally {
        setBusy(false);
      }
    },
    [authenticatedApi, loadCatalog],
  );

  const createTable = useCallback(
    async (
      namespace: string[],
      name: string,
      columns: Array<{ name: string; type: string; nullable?: boolean }>,
    ) => {
      if (
        !database ||
        database.type !== "lakehouse" ||
        !database.capabilities.tableCrud
      )
        return;
      setBusy(true);
      setError(null);
      try {
        await authenticatedApi.createVerglasTable({
          database: database.name,
          namespace,
          name,
          columns,
        });
        setCreateTableOpen(false);
        await loadCatalog();
      } catch (reason) {
        setError(errorMessage(reason));
      } finally {
        setBusy(false);
      }
    },
    [authenticatedApi, database, loadCatalog],
  );

  const removeDatabase = useCallback(async () => {
    if (!deleteDatabase) return;
    setBusy(true);
    setError(null);
    try {
      await authenticatedApi.deleteVerglasDatabase(deleteDatabase.name);
      setDeleteDatabase(null);
      setSelectedAsset(null);
      await loadCatalog();
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setBusy(false);
    }
  }, [authenticatedApi, deleteDatabase, loadCatalog]);

  const removeTable = useCallback(async () => {
    if (!deleteTable) return;
    setBusy(true);
    setError(null);
    try {
      await authenticatedApi.deleteVerglasTable(
        deleteTable.database,
        deleteTable.namespace,
        deleteTable.name,
      );
      setDeleteTable(null);
      setSelectedAsset(null);
      await loadCatalog();
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setBusy(false);
    }
  }, [authenticatedApi, deleteTable, loadCatalog]);

  const openQueryWorkspace = useCallback(
    async (item: CatalogItem) => {
      if (busy) return;
      setBusy(true);
      setError(null);
      const overseer = authenticatedApi.newWorkspace();
      try {
        const models = await authenticatedApi.listModels();
        const modelId = getStoredSelectedModel(models);
        const metadata = await overseer.getMetadata();
        await overseer.newChat(workspacePromptForCatalogItem(item), modelId);
        await navigate({ to: "/workspace/$id", params: { id: metadata.id } });
      } catch (reason) {
        setError(errorMessage(reason));
      } finally {
        overseer[Symbol.dispose]();
        setBusy(false);
      }
    },
    [authenticatedApi, busy, navigate],
  );

  return (
    <div className="flex h-full min-h-0 flex-col bg-kumo-base">
      <PageHeader onCreate={() => setCreateDatabaseOpen(true)} />
      <div className="grid min-h-0 flex-1 grid-cols-[250px_minmax(0,1fr)_320px]">
        <DatabaseSidebar
          databases={catalog.databases}
          selected={database?.name ?? null}
          loading={loading}
          onSelect={(name) => {
            setSelectedDatabaseName(name);
            setSelectedAsset(null);
            setSearch("");
          }}
        />
        <main className="min-h-0 min-w-0 overflow-auto border-x border-kumo-line">
          {error && <ErrorBanner message={error} />}
          {loading ? (
            <EmptyState
              icon={<SpinnerGap size={22} className="animate-spin" />}
              title="Loading databases"
              detail="Reading tenant database resources and their scoped catalogs."
            />
          ) : !database ? (
            <EmptyState
              icon={<Database size={24} weight="duotone" />}
              title="No databases yet"
              detail="Create a managed Lakehouse or Postgres database to begin."
            />
          ) : (
            <DatabaseOverview
              database={database}
              detail={detail}
              assets={assets}
              search={search}
              selected={selectedAsset}
              busy={busy}
              onSearch={setSearch}
              onSelect={setSelectedAsset}
              onCreateTable={() => setCreateTableOpen(true)}
            />
          )}
        </main>
        <aside className="min-h-0 overflow-auto bg-kumo-elevated/30">
          {selectedAsset ? (
            <AssetDetails
              item={selectedAsset}
              tableDetail={
                selectedAsset.kind === "table"
                  ? detail?.tables.find(
                      (table) =>
                        table.qualifiedName ===
                        selectedAsset.value.qualifiedName,
                    )
                  : undefined
              }
              busy={busy}
              onQuery={
                database?.capabilities.query
                  ? () => void openQueryWorkspace(selectedAsset)
                  : undefined
              }
              onDelete={
                selectedAsset.kind === "table"
                  ? () => setDeleteTable(selectedAsset.value)
                  : undefined
              }
            />
          ) : (
            <DatabaseDetails
              database={database}
              detail={detail}
              onDelete={
                database ? () => setDeleteDatabase(database) : undefined
              }
            />
          )}
        </aside>
      </div>
      <CreateDatabaseDialog
        open={createDatabaseOpen}
        busy={busy}
        onOpenChange={setCreateDatabaseOpen}
        onCreate={(input) => void createDatabase(input)}
      />
      <CreateTableDialog
        open={createTableOpen}
        database={database}
        busy={busy}
        onOpenChange={setCreateTableOpen}
        onCreate={(namespace, name, columns) =>
          void createTable(namespace, name, columns)
        }
      />
      <ConfirmDialog
        open={deleteTable !== null}
        busy={busy}
        title={`Delete ${deleteTable?.name ?? "table"}?`}
        description="This permanently deletes the table and its data from the selected Lakehouse."
        onOpenChange={(open) => {
          if (!open) setDeleteTable(null);
        }}
        onConfirm={() => void removeTable()}
      />
      <ConfirmDialog
        open={deleteDatabase !== null}
        busy={busy}
        title={`Delete ${deleteDatabase?.name ?? "database"}?`}
        description={
          deleteDatabase?.type === "lakehouse"
            ? "The Lakehouse must be empty. Delete its tables first."
            : "This removes the managed Postgres database resource."
        }
        onOpenChange={(open) => {
          if (!open) setDeleteDatabase(null);
        }}
        onConfirm={() => void removeDatabase()}
      />
    </div>
  );
}

function PageHeader({ onCreate }: { onCreate: () => void }) {
  return (
    <header className="flex shrink-0 items-center justify-between border-b border-kumo-line px-6 py-4">
      <div className="flex items-center gap-3">
        <span className="flex h-9 w-9 items-center justify-center rounded-xl border border-kumo-line bg-kumo-elevated text-kumo-brand">
          <Database size={19} weight="duotone" />
        </span>
        <div>
          <h1 className="text-lg font-semibold tracking-tight text-kumo-default">
            Databases
          </h1>
          <p className="text-[12px] text-kumo-subtle">
            Manage Lakehouse and Postgres resources for this tenant.
          </p>
        </div>
      </div>
      <div className="flex items-center gap-2">
        <button
          type="button"
          onClick={onCreate}
          className="inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg bg-kumo-brand px-3 text-[12px] font-semibold text-white"
        >
          <Plus size={15} weight="bold" /> Create database
        </button>
      </div>
    </header>
  );
}

function DatabaseSidebar({
  databases,
  selected,
  loading,
  onSelect,
}: {
  databases: VerglasDatabaseSummary[];
  selected: string | null;
  loading: boolean;
  onSelect: (name: string) => void;
}) {
  return (
    <aside className="min-h-0 overflow-auto bg-kumo-elevated/30 p-3">
      <div className="mb-2 px-2 text-[10px] font-semibold uppercase tracking-[0.12em] text-kumo-inactive">
        Databases
      </div>
      {loading ? (
        <div className="px-2 py-3 text-[12px] text-kumo-subtle">
          Loading resources…
        </div>
      ) : (
        databases.map((database) => (
          <button
            key={database.name}
            type="button"
            onClick={() => onSelect(database.name)}
            className={`mb-1.5 w-full cursor-pointer rounded-xl border p-3 text-left transition-colors ${selected === database.name ? "border-kumo-brand bg-kumo-brand/5" : "border-transparent hover:border-kumo-line hover:bg-kumo-tint"}`}
          >
            <div className="flex items-center gap-2">
              <DatabaseIcon
                size={16}
                weight={selected === database.name ? "fill" : "duotone"}
                className={
                  selected === database.name
                    ? "text-kumo-brand"
                    : "text-kumo-inactive"
                }
              />
              <span className="min-w-0 flex-1 truncate text-[12px] font-medium text-kumo-default">
                {database.name}
              </span>
              <Badge
                variant={database.type === "lakehouse" ? "info" : "secondary"}
              >
                {databaseKindLabel(database)}
              </Badge>
            </div>
            <div className="mt-2 flex items-center justify-between text-[10px] text-kumo-inactive">
              <span>Status</span>
              <span className="font-medium text-kumo-success">Registered</span>
            </div>
            {database.capabilities.catalog && (
              <div className="mt-1 flex items-center justify-between text-[10px] text-kumo-inactive">
                <span>Tables</span>
                <span className="font-mono text-kumo-subtle">
                  {database.tableCount.toLocaleString()}
                </span>
              </div>
            )}
          </button>
        ))
      )}
    </aside>
  );
}

function DatabaseOverview({
  database,
  detail,
  assets,
  search,
  selected,
  busy,
  onSearch,
  onSelect,
  onCreateTable,
}: {
  database: VerglasDatabaseSummary;
  detail: VerglasDatabaseDetail | null;
  assets: DatabaseAssets;
  search: string;
  selected: CatalogItem | null;
  busy: boolean;
  onSearch: (value: string) => void;
  onSelect: (item: CatalogItem) => void;
  onCreateTable: () => void;
}) {
  const capabilities = databaseCapabilityLabels(database);
  const metrics = databaseMetrics(database, detail);
  return (
    <div className="p-5">
      <Card>
        <CardHeader className="flex-row items-start justify-between gap-4">
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <CardTitle className="text-lg">{database.name}</CardTitle>
              <Badge
                variant={database.type === "lakehouse" ? "info" : "secondary"}
              >
                {databaseKindLabel(database)}
              </Badge>
              <Badge variant="success">Registered</Badge>
            </div>
            <CardDescription className="mt-1">
              {databaseResourceDescription(database)}
            </CardDescription>
          </div>
          {database.type === "lakehouse" && database.capabilities.tableCrud && (
            <button
              type="button"
              onClick={onCreateTable}
              disabled={busy}
              className="inline-flex h-9 shrink-0 cursor-pointer items-center gap-1.5 rounded-lg bg-kumo-brand px-3 text-[12px] font-semibold text-white disabled:opacity-50"
            >
              <Plus size={15} weight="bold" /> Add table
            </button>
          )}
        </CardHeader>
        <CardContent>
          {capabilities.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {capabilities.map((capability) => (
                <Badge key={capability} variant="outline">
                  {capability}
                </Badge>
              ))}
            </div>
          ) : (
            <p className="text-[11px] text-kumo-inactive">
              No data operations are exposed by the current OS runtime.
            </p>
          )}
          {metrics.length > 0 && (
            <div className="mt-4 grid grid-cols-2 gap-2 xl:grid-cols-4">
              {metrics.map((metric) => (
                <MetricCard key={metric.label} metric={metric} />
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {database.type === "postgres" ? (
        <PostgresOverview database={database} />
      ) : (
        <LakehouseCatalog
          database={database}
          assets={assets}
          search={search}
          selected={selected}
          onSearch={onSearch}
          onSelect={onSelect}
        />
      )}
    </div>
  );
}

function PostgresOverview({
  database,
}: {
  database: Extract<VerglasDatabaseSummary, { type: "postgres" }>;
}) {
  return (
    <Card className="mt-4">
      <CardHeader>
        <CardTitle>Managed Postgres</CardTitle>
        <CardDescription>
          Verglas provisions this database on{" "}
          {database.engine.mode === "managed-neon"
            ? "managed Neon"
            : database.engine.mode}
          .
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="rounded-lg border border-dashed border-kumo-line bg-kumo-elevated/30 p-8 text-center">
          <Database
            size={24}
            weight="duotone"
            className="mx-auto text-kumo-inactive"
          />
          <p className="mt-3 text-[12px] font-medium text-kumo-default">
            Database resource is registered
          </p>
          <p className="mx-auto mt-1 max-w-md text-[11px] leading-5 text-kumo-subtle">
            This runtime does not yet expose Postgres schema browsing, metrics,
            or query controls to the OS. Those surfaces are intentionally
            hidden.
          </p>
        </div>
      </CardContent>
    </Card>
  );
}

function LakehouseCatalog({
  database,
  assets,
  search,
  selected,
  onSearch,
  onSelect,
}: {
  database: Extract<VerglasDatabaseSummary, { type: "lakehouse" }>;
  assets: DatabaseAssets;
  search: string;
  selected: CatalogItem | null;
  onSearch: (value: string) => void;
  onSelect: (item: CatalogItem) => void;
}) {
  const normalized = search.trim().toLocaleLowerCase();
  const tables = assets.tables.filter(
    (table) =>
      !normalized ||
      `${table.namespace.join(".")} ${table.name}`
        .toLocaleLowerCase()
        .includes(normalized),
  );
  const groups = namespaceGroups(tables);
  const defaultTab = database.capabilities.catalog
    ? "tables"
    : database.capabilities.graphs
      ? "graphs"
      : "overview";
  return (
    <Tabs defaultValue={defaultTab} className="mt-4">
      <TabsList>
        {database.capabilities.catalog && (
          <TabsTrigger value="tables">
            Tables{" "}
            <span className="ml-1 font-mono text-[10px]">
              {database.tableCount}
            </span>
          </TabsTrigger>
        )}
        {database.capabilities.vectors && (
          <TabsTrigger value="vectors">
            Vectors{" "}
            <span className="ml-1 font-mono text-[10px]">
              {database.vectorCount}
            </span>
          </TabsTrigger>
        )}
        {database.capabilities.graphs && (
          <TabsTrigger value="graphs">
            Graphs{" "}
            <span className="ml-1 font-mono text-[10px]">
              {database.graphCount}
            </span>
          </TabsTrigger>
        )}
      </TabsList>
      {database.capabilities.catalog && (
        <TabsContent value="tables">
          <div className="mb-4 flex justify-end">
            <label className="flex h-9 w-full max-w-[280px] items-center gap-2 rounded-lg border border-kumo-line bg-kumo-elevated px-3">
              <MagnifyingGlass
                size={14}
                className="shrink-0 text-kumo-inactive"
              />
              <input
                value={search}
                onChange={(event) => onSearch(event.target.value)}
                placeholder="Search namespaces and tables…"
                className="min-w-0 flex-1 bg-transparent text-[12px] text-kumo-default outline-none placeholder:text-kumo-inactive"
              />
            </label>
          </div>
          {groups.length === 0 ? (
            <CatalogEmpty
              title={search ? "No matching tables" : "No tables yet"}
              detail={
                search
                  ? "Try a different table or namespace."
                  : "Add a table to create the first namespace and schema."
              }
            />
          ) : (
            groups.map((group) => (
              <section key={group.name} className="mb-6">
                <div className="mb-2 flex items-center gap-2">
                  <span className="font-mono text-[11px] font-medium text-kumo-default">
                    {group.name}
                  </span>
                  <Badge variant="secondary">{group.tables.length}</Badge>
                </div>
                <div className="grid grid-cols-[repeat(auto-fill,minmax(210px,1fr))] gap-3">
                  {group.tables.map((table) => {
                    const item: CatalogItem = {
                      kind: "table",
                      id: assetId("table", table),
                      value: table,
                    };
                    return (
                      <CatalogCard
                        key={item.id}
                        item={item}
                        selected={selected?.id === item.id}
                        onSelect={() => onSelect(item)}
                      />
                    );
                  })}
                </div>
              </section>
            ))
          )}
        </TabsContent>
      )}
      {database.capabilities.vectors && (
        <TabsContent value="vectors">
          <AssetGrid
            items={assets.vectors.map((value): CatalogItem => ({
              kind: "vector",
              id: assetId("vector", value),
              value,
            }))}
            selected={selected}
            onSelect={onSelect}
            emptyTitle="No vector indexes"
          />
        </TabsContent>
      )}
      {database.capabilities.graphs && (
        <TabsContent value="graphs">
          <AssetGrid
            items={assets.graphs.map((value): CatalogItem => ({
              kind: "graph",
              id: assetId("graph", value),
              value,
            }))}
            selected={selected}
            onSelect={onSelect}
            emptyTitle="No graph spaces"
          />
        </TabsContent>
      )}
    </Tabs>
  );
}

function AssetGrid({
  items,
  selected,
  onSelect,
  emptyTitle,
}: {
  items: CatalogItem[];
  selected: CatalogItem | null;
  onSelect: (item: CatalogItem) => void;
  emptyTitle: string;
}) {
  if (items.length === 0)
    return (
      <CatalogEmpty
        title={emptyTitle}
        detail="This database has no resources of this type."
      />
    );
  return (
    <div className="grid grid-cols-[repeat(auto-fill,minmax(210px,1fr))] gap-3">
      {items.map((item) => (
        <CatalogCard
          key={item.id}
          item={item}
          selected={selected?.id === item.id}
          onSelect={() => onSelect(item)}
        />
      ))}
    </div>
  );
}

function CatalogCard({
  item,
  selected,
  onSelect,
}: {
  item: CatalogItem;
  selected: boolean;
  onSelect: () => void;
}) {
  const presentation = itemPresentation(item);
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`group min-h-[112px] cursor-pointer rounded-xl border p-4 text-left transition-all ${selected ? "border-kumo-brand bg-kumo-brand/5 shadow-sm" : "border-kumo-line bg-kumo-base hover:-translate-y-0.5 hover:border-kumo-line-strong hover:shadow-sm"}`}
    >
      <div className="flex items-start justify-between gap-3">
        <span
          className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-lg ${selected ? "bg-kumo-brand text-white" : "bg-kumo-elevated text-kumo-brand"}`}
        >
          {kindIcon(item.kind, 16)}
        </span>
        <Badge variant={selected ? "info" : "secondary"}>
          {singularKind(item.kind)}
        </Badge>
      </div>
      <div className="mt-3 truncate text-[13px] font-medium text-kumo-default">
        {presentation.title}
      </div>
      <div className="mt-0.5 truncate font-mono text-[10px] text-kumo-inactive">
        {presentation.subtitle}
      </div>
    </button>
  );
}

function DatabaseDetails({
  database,
  detail,
  onDelete,
}: {
  database: VerglasDatabaseSummary | null;
  detail: VerglasDatabaseDetail | null;
  onDelete?: () => void;
}) {
  if (!database)
    return (
      <EmptyState
        icon={<Database size={20} />}
        title="Select a database"
        detail="Inspect its engine, capabilities, and resources."
      />
    );
  return (
    <div className="flex min-h-full flex-col p-6">
      <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-kumo-brand text-white">
        <Database size={20} weight="duotone" />
      </span>
      <div className="mt-4 text-[10px] font-semibold uppercase tracking-[0.12em] text-kumo-brand">
        {databaseKindLabel(database)} database
      </div>
      <h2 className="mt-1 break-words text-lg font-semibold tracking-tight text-kumo-default">
        {database.name}
      </h2>
      <p className="mt-1 break-words text-[11px] leading-5 text-kumo-subtle">
        {databaseResourceDescription(database)}
      </p>
      <dl className="mt-6 divide-y divide-kumo-line border-y border-kumo-line">
        <DetailRow label="Status" value="Registered" />
        <DetailRow label="Engine" value={databaseKindLabel(database)} />
        {database.type === "lakehouse" && (
          <DetailRow
            label="Tables"
            value={database.tableCount.toLocaleString()}
          />
        )}
        {database.capabilities.tableMetrics && detail && (
          <DetailRow
            label="Storage"
            value={formatBytes(totalPhysicalBytes(detail))}
          />
        )}
      </dl>
      <div className="mt-6">
        <div className="text-[10px] font-semibold uppercase tracking-[0.1em] text-kumo-inactive">
          Capabilities
        </div>
        <div className="mt-2 flex flex-wrap gap-1.5">
          {databaseCapabilityLabels(database).map((capability) => (
            <Badge key={capability} variant="outline">
              {capability}
            </Badge>
          ))}
          {databaseCapabilityLabels(database).length === 0 && (
            <span className="text-[11px] text-kumo-inactive">
              Management only
            </span>
          )}
        </div>
      </div>
      <div className="mt-auto pt-8">
        <button
          type="button"
          onClick={onDelete}
          className="inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg border border-kumo-danger/40 px-3 text-[12px] text-kumo-danger hover:bg-kumo-danger-tint"
        >
          <Trash size={14} /> Delete database
        </button>
        {database.type === "lakehouse" && (
          <p className="mt-2 text-[10px] leading-4 text-kumo-inactive">
            Delete all tables before deleting this Lakehouse.
          </p>
        )}
      </div>
    </div>
  );
}

function AssetDetails({
  item,
  tableDetail,
  busy,
  onQuery,
  onDelete,
}: {
  item: CatalogItem;
  tableDetail?: VerglasTableDetail;
  busy: boolean;
  onQuery?: () => void;
  onDelete?: () => void;
}) {
  const presentation = itemPresentation(item);
  const facts = itemFacts(item);
  if (tableDetail?.physical) {
    facts.push(["Rows", tableDetail.physical.rowCount.toLocaleString()]);
    facts.push(["Files", tableDetail.physical.fileCount.toLocaleString()]);
    facts.push(["Size", formatBytes(tableDetail.physical.sizeBytes)]);
  }
  if (tableDetail?.usage) {
    facts.push(["Cache hits", tableDetail.usage.hits.toLocaleString()]);
    facts.push(["Bytes served", formatBytes(tableDetail.usage.bytesServed)]);
  }
  return (
    <div className="flex min-h-full flex-col p-6">
      <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-kumo-brand text-white">
        {kindIcon(item.kind, 20)}
      </span>
      <div className="mt-4 text-[10px] font-semibold uppercase tracking-[0.12em] text-kumo-brand">
        {singularKind(item.kind)}
      </div>
      <h2 className="mt-1 break-words text-lg font-semibold tracking-tight text-kumo-default">
        {presentation.title}
      </h2>
      <p className="mt-1 break-all font-mono text-[11px] leading-5 text-kumo-subtle">
        {presentation.subtitle}
      </p>
      <dl className="mt-6 divide-y divide-kumo-line border-y border-kumo-line">
        {facts.map(([label, value]) => (
          <DetailRow key={label} label={label} value={value} />
        ))}
      </dl>
      <div className="mt-auto pt-8">
        {onQuery && (
          <button
            type="button"
            disabled={busy}
            onClick={onQuery}
            className="mb-2 flex h-9 w-full cursor-pointer items-center justify-center gap-2 rounded-lg bg-kumo-brand px-4 text-[12px] font-semibold text-white disabled:opacity-50"
          >
            Query in workspace <ArrowRight size={14} />
          </button>
        )}
        {onDelete && (
          <button
            type="button"
            disabled={busy}
            onClick={onDelete}
            className="flex h-9 w-full cursor-pointer items-center justify-center gap-2 rounded-lg border border-kumo-danger/40 px-4 text-[12px] font-medium text-kumo-danger hover:bg-kumo-danger-tint disabled:opacity-50"
          >
            <Trash size={14} /> Delete table
          </button>
        )}
        {!onQuery && (
          <p className="mt-3 text-[10px] leading-4 text-kumo-inactive">
            This database does not expose scoped SQL queries.
          </p>
        )}
      </div>
    </div>
  );
}

function MetricCard({ metric }: { metric: Metric }) {
  return (
    <div className="rounded-xl border border-kumo-line bg-kumo-elevated/45 px-3 py-2.5">
      <div className="text-[10px] font-medium uppercase tracking-wide text-kumo-inactive">
        {metric.label}
      </div>
      <div className="mt-1 text-[14px] font-semibold text-kumo-default">
        {metric.value}
      </div>
    </div>
  );
}

function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="grid grid-cols-[90px_minmax(0,1fr)] gap-3 py-3">
      <dt className="text-[11px] text-kumo-inactive">{label}</dt>
      <dd className="break-all text-right font-mono text-[11px] text-kumo-default">
        {value}
      </dd>
    </div>
  );
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="mx-5 mt-5 rounded-lg border border-kumo-danger/25 bg-kumo-danger-tint px-3 py-2 text-[12px] text-kumo-danger">
      {message}
    </div>
  );
}

function CatalogEmpty({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="rounded-xl border border-dashed border-kumo-line px-4 py-10 text-center">
      <div className="text-[12px] font-medium text-kumo-default">{title}</div>
      <div className="mt-1 text-[11px] text-kumo-subtle">{detail}</div>
    </div>
  );
}

function EmptyState({
  icon,
  title,
  detail,
}: {
  icon: React.ReactNode;
  title: string;
  detail: string;
}) {
  return (
    <div className="flex min-h-[360px] flex-col items-center justify-center px-8 text-center">
      <span className="text-kumo-inactive">{icon}</span>
      <h2 className="mt-3 text-[13px] font-medium text-kumo-default">
        {title}
      </h2>
      <p className="mt-1 text-[11px] text-kumo-subtle">{detail}</p>
    </div>
  );
}

function CreateDatabaseDialog({
  open,
  busy,
  onOpenChange,
  onCreate,
}: {
  open: boolean;
  busy: boolean;
  onOpenChange: (open: boolean) => void;
  onCreate: (input: VerglasCreateDatabaseInput) => void;
}) {
  const [name, setName] = useState("");
  const [type, setType] = useState<"lakehouse" | "postgres">("lakehouse");
  const submit = () =>
    onCreate(
      type === "lakehouse"
        ? {
            type,
            name: name.trim(),
            storage: { mode: "managed" },
            catalog: { mode: "managed-lakekeeper" },
          }
        : { type, name: name.trim(), engine: { mode: "managed-neon" } },
    );
  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!busy) onOpenChange(next);
      }}
    >
      <DialogContent className="w-[min(36rem,calc(100vw-2rem))]">
        <DialogHeader>
          <DialogTitle>Create database</DialogTitle>
          <DialogDescription>
            Create an explicit tenant database resource. Each database has
            independent backing services and capabilities.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 px-5">
          <label className="block text-[12px] font-medium text-kumo-default">
            Database name
            <input
              autoFocus
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="analytics"
              className="mt-1.5 h-9 w-full rounded-lg border border-kumo-line bg-kumo-elevated px-3 text-[13px] outline-none focus:border-kumo-brand"
            />
          </label>
          <fieldset>
            <legend className="text-[12px] font-medium text-kumo-default">
              Database type
            </legend>
            <div className="mt-2 grid grid-cols-2 gap-3">
              <DatabaseTypeChoice
                selected={type === "lakehouse"}
                title="Lakehouse"
                description="Managed object storage and a Lakekeeper warehouse."
                onSelect={() => setType("lakehouse")}
              />
              <DatabaseTypeChoice
                selected={type === "postgres"}
                title="Postgres"
                description="An independent managed Neon database."
                onSelect={() => setType("postgres")}
              />
            </div>
          </fieldset>
          <div className="rounded-lg border border-kumo-line bg-kumo-elevated/40 p-3 text-[11px] leading-5 text-kumo-subtle">
            {type === "lakehouse"
              ? "Configuration: managed storage · managed Lakekeeper"
              : "Configuration: managed Neon"}
          </div>
        </div>
        <DialogFooter>
          <button
            type="button"
            onClick={() => onOpenChange(false)}
            className="h-9 rounded-lg px-3 text-[12px] text-kumo-subtle"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={busy || !name.trim()}
            onClick={submit}
            className="h-9 rounded-lg bg-kumo-brand px-3 text-[12px] font-semibold text-white disabled:opacity-50"
          >
            {busy ? "Creating…" : "Create database"}
          </button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function DatabaseTypeChoice({
  selected,
  title,
  description,
  onSelect,
}: {
  selected: boolean;
  title: string;
  description: string;
  onSelect: () => void;
}) {
  return (
    <button
      type="button"
      role="radio"
      aria-checked={selected}
      onClick={onSelect}
      className={`rounded-xl border p-3 text-left ${selected ? "border-kumo-brand bg-kumo-brand/5" : "border-kumo-line bg-kumo-base hover:bg-kumo-tint"}`}
    >
      <div className="text-[12px] font-semibold text-kumo-default">{title}</div>
      <div className="mt-1 text-[10px] leading-4 text-kumo-subtle">
        {description}
      </div>
    </button>
  );
}

function CreateTableDialog({
  open,
  database,
  busy,
  onOpenChange,
  onCreate,
}: {
  open: boolean;
  database: VerglasDatabaseSummary | null;
  busy: boolean;
  onOpenChange: (open: boolean) => void;
  onCreate: (
    namespace: string[],
    name: string,
    columns: Array<{ name: string; type: string; nullable?: boolean }>,
  ) => void;
}) {
  const [namespace, setNamespace] = useState("main");
  const [name, setName] = useState("");
  const [schema, setSchema] = useState("id string\ncreated_at timestamp");
  const columns = schema
    .split("\n")
    .map((line) => line.trim().split(/\s+/, 2))
    .filter(([column, type]) => column && type)
    .map(([column, type]) => ({ name: column, type }));
  const namespaceParts = namespace
    .split(".")
    .map((part) => part.trim())
    .filter(Boolean);
  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!busy) onOpenChange(next);
      }}
    >
      <DialogContent className="w-[min(36rem,calc(100vw-2rem))]">
        <DialogHeader>
          <DialogTitle>Add table</DialogTitle>
          <DialogDescription>
            Create an Iceberg table inside{" "}
            {database?.name ?? "the selected Lakehouse"}. Namespaces are scoped
            within this database.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 px-5">
          <div className="grid grid-cols-2 gap-3">
            <label className="block text-[12px] font-medium text-kumo-default">
              Namespace
              <input
                value={namespace}
                onChange={(event) => setNamespace(event.target.value)}
                placeholder="main"
                className="mt-1.5 h-9 w-full rounded-lg border border-kumo-line bg-kumo-elevated px-3 font-mono text-[12px] outline-none focus:border-kumo-brand"
              />
            </label>
            <label className="block text-[12px] font-medium text-kumo-default">
              Table name
              <input
                autoFocus
                value={name}
                onChange={(event) => setName(event.target.value)}
                placeholder="events"
                className="mt-1.5 h-9 w-full rounded-lg border border-kumo-line bg-kumo-elevated px-3 font-mono text-[12px] outline-none focus:border-kumo-brand"
              />
            </label>
          </div>
          <label className="block text-[12px] font-medium text-kumo-default">
            Columns
            <textarea
              value={schema}
              onChange={(event) => setSchema(event.target.value)}
              rows={5}
              className="mt-1.5 w-full rounded-lg border border-kumo-line bg-kumo-elevated p-3 font-mono text-[12px] outline-none focus:border-kumo-brand"
            />
            <span className="mt-1 block text-[10px] text-kumo-inactive">
              One column per line: name type
            </span>
          </label>
        </div>
        <DialogFooter>
          <button
            type="button"
            onClick={() => onOpenChange(false)}
            className="h-9 rounded-lg px-3 text-[12px] text-kumo-subtle"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={
              busy ||
              !name.trim() ||
              namespaceParts.length === 0 ||
              columns.length === 0
            }
            onClick={() => onCreate(namespaceParts, name.trim(), columns)}
            className="h-9 rounded-lg bg-kumo-brand px-3 text-[12px] font-semibold text-white disabled:opacity-50"
          >
            {busy ? "Creating…" : "Add table"}
          </button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ConfirmDialog({
  open,
  busy,
  title,
  description,
  onOpenChange,
  onConfirm,
}: {
  open: boolean;
  busy: boolean;
  title: string;
  description: string;
  onOpenChange: (open: boolean) => void;
  onConfirm: () => void;
}) {
  return (
    <AlertDialog
      open={open}
      onOpenChange={(next) => {
        if (!busy) onOpenChange(next);
      }}
    >
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          <AlertDialogDescription>{description}</AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <button
            type="button"
            onClick={() => onOpenChange(false)}
            className="h-9 rounded-lg px-3 text-[12px] text-kumo-subtle"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={onConfirm}
            className="h-9 rounded-lg bg-kumo-danger px-3 text-[12px] font-semibold text-white disabled:opacity-50"
          >
            {busy ? "Deleting…" : "Delete"}
          </button>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}

/** Filters a combined catalog to assets owned by one top-level database resource. */
export function databaseAssets(
  catalog: VerglasCatalogSnapshot,
  database: string,
): DatabaseAssets {
  return {
    tables: catalog.tables.filter((table) => table.database === database),
    vectors: catalog.vectors.filter((vector) => vector.database === database),
    graphs: catalog.graphs.filter((graph) => graph.database === database),
  };
}

/** Groups a Lakehouse database's tables by namespace without changing resource identity. */
export function namespaceGroups(
  tables: VerglasTableSummary[],
): NamespaceGroup[] {
  const groups = new Map<string, NamespaceGroup>();
  for (const table of tables) {
    const name = table.namespace.join(".") || "default";
    const group = groups.get(name) ?? {
      name,
      namespace: table.namespace,
      tables: [],
    };
    group.tables.push(table);
    groups.set(name, group);
  }
  return [...groups.values()]
    .map((group) => ({
      ...group,
      tables: group.tables.toSorted((left, right) =>
        left.name.localeCompare(right.name),
      ),
    }))
    .toSorted((left, right) => left.name.localeCompare(right.name));
}

/** Builds the first chat turn for a scoped catalog asset query. */
export function workspacePromptForCatalogItem(item: CatalogItem): string {
  if (item.kind === "table") {
    return `Query table \`${item.value.qualifiedName}\` in database \`${item.value.database}\`. Inspect its schema and data, then help me analyze or visualize it. Always run SQL against the selected database.`;
  }
  if (item.kind === "vector") {
    return `Explore vector index \`${item.value.field}\` on \`${item.value.target}\` in database \`${item.value.database}\`. Use only capabilities exposed for this database.`;
  }
  return `Explore graph \`${item.value.namespace}\` in database \`${item.value.database}\`. Inspect its node and edge tables and use only capabilities exposed for this database.`;
}

function databaseMetrics(
  database: VerglasDatabaseSummary,
  detail: VerglasDatabaseDetail | null,
): Metric[] {
  const metrics: Metric[] = [];
  if (database.capabilities.catalog)
    metrics.push({
      label: "Tables",
      value: database.tableCount.toLocaleString(),
    });
  if (database.capabilities.graphs)
    metrics.push({
      label: "Graphs",
      value: database.graphCount.toLocaleString(),
    });
  if (database.capabilities.vectors)
    metrics.push({
      label: "Vectors",
      value: database.vectorCount.toLocaleString(),
    });
  if (database.capabilities.tableMetrics && detail) {
    metrics.push({
      label: "Storage",
      value: formatBytes(totalPhysicalBytes(detail)),
    });
    metrics.push({ label: "Rows", value: totalRows(detail).toLocaleString() });
  }
  return metrics;
}

function totalPhysicalBytes(detail: VerglasDatabaseDetail): number {
  return detail.tables.reduce(
    (total, table) => total + (table.physical?.sizeBytes ?? 0),
    0,
  );
}

function totalRows(detail: VerglasDatabaseDetail): number {
  return detail.tables.reduce(
    (total, table) => total + (table.physical?.rowCount ?? 0),
    0,
  );
}

function catalogContains(
  catalog: VerglasCatalogSnapshot,
  item: CatalogItem,
): boolean {
  return [...catalog.tables, ...catalog.vectors, ...catalog.graphs].some(
    (candidate) => {
      if ("qualifiedName" in candidate)
        return item.id === assetId("table", candidate);
      if ("field" in candidate) return item.id === assetId("vector", candidate);
      return item.id === assetId("graph", candidate);
    },
  );
}

function assetId(kind: "table", value: VerglasTableSummary): string;
function assetId(kind: "vector", value: VerglasVectorSummary): string;
function assetId(kind: "graph", value: VerglasGraphSummary): string;
function assetId(
  kind: CatalogKind,
  value: VerglasTableSummary | VerglasVectorSummary | VerglasGraphSummary,
): string {
  if (kind === "table") {
    const table = value as VerglasTableSummary;
    return `table:${table.database}:${table.qualifiedName}`;
  }
  if (kind === "vector") {
    const vector = value as VerglasVectorSummary;
    return `vector:${vector.database}:${vector.target}:${vector.field}`;
  }
  const graph = value as VerglasGraphSummary;
  return `graph:${graph.database}:${graph.namespace}`;
}

function itemPresentation(item: CatalogItem): {
  title: string;
  subtitle: string;
} {
  if (item.kind === "table")
    return {
      title: item.value.name,
      subtitle: item.value.namespace.join(".") || "default",
    };
  if (item.kind === "vector")
    return { title: item.value.field, subtitle: item.value.target };
  return {
    title: item.value.namespace,
    subtitle: `${item.value.nodesTable} + ${item.value.edgesTable}`,
  };
}

function itemFacts(item: CatalogItem): Array<[string, string]> {
  if (item.kind === "table")
    return [
      ["Database", item.value.database],
      ["Namespace", item.value.namespace.join(".") || "default"],
      ["SQL name", item.value.qualifiedName],
    ];
  if (item.kind === "vector")
    return [
      ["Database", item.value.database],
      ["Target", item.value.target],
      ["Field", item.value.field],
      ["Metric", item.value.metric],
      ["Vectors", item.value.liveCount?.toLocaleString() ?? "Not reported"],
    ];
  return [
    ["Database", item.value.database],
    ["Graph", item.value.namespace],
    ["Nodes", item.value.nodesTable],
    ["Edges", item.value.edgesTable],
  ];
}

function singularKind(kind: CatalogKind): string {
  return kind === "table"
    ? "Table"
    : kind === "vector"
      ? "Vector index"
      : "Graph space";
}

function kindIcon(kind: CatalogKind, size: number) {
  if (kind === "table") return <Table size={size} weight="duotone" />;
  if (kind === "vector") return <VectorThree size={size} weight="duotone" />;
  return <CirclesThreePlus size={size} weight="duotone" />;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(1)} GB`;
}

function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}
