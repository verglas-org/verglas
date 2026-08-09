import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { ArrowRight, ArrowsClockwise, CirclesThreePlus, Database, MagnifyingGlass, SpinnerGap, Table, VectorThree } from '@phosphor-icons/react'
import type { VerglasCatalogSnapshot, VerglasGraphSummary, VerglasTableSummary, VerglasVectorSummary } from '@verglas/workshop-shared/api'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { useAuthenticatedApi } from '../AuthContext'
import { getStoredSelectedModel } from '../modelSelection'
import { useDocumentTitle } from '../useDocumentTitle'

export const Route = createFileRoute('/data')({ component: LakehousePage })

type CatalogKind = 'table' | 'vector' | 'graph'
type CatalogItem =
  | {kind: 'table'; id: string; value: VerglasTableSummary}
  | {kind: 'vector'; id: string; value: VerglasVectorSummary}
  | {kind: 'graph'; id: string; value: VerglasGraphSummary}

const EMPTY_CATALOG: VerglasCatalogSnapshot = {tables: [], vectors: [], graphs: []}

function LakehousePage() {
  useDocumentTitle('Lakehouse')
  const {authenticatedApi} = useAuthenticatedApi()
  const navigate = useNavigate()
  const [catalog, setCatalog] = useState(EMPTY_CATALOG)
  const [activeKind, setActiveKind] = useState<CatalogKind>('table')
  const [selected, setSelected] = useState<CatalogItem | null>(null)
  const [search, setSearch] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [launching, setLaunching] = useState(false)

  const loadCatalog = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const next = await authenticatedApi.getVerglasCatalog()
      setCatalog(next)
      setSelected((current) => current && catalogContains(next, current) ? current : null)
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason))
    } finally {
      setLoading(false)
    }
  }, [authenticatedApi])

  useEffect(() => { void loadCatalog() }, [loadCatalog])

  const items = useMemo(() => {
    const normalized = search.trim().toLocaleLowerCase()
    return itemsForKind(catalog, activeKind).filter((item) =>
      !normalized || searchableText(item).toLocaleLowerCase().includes(normalized))
  }, [activeKind, catalog, search])

  const querySelected = useCallback(async () => {
    if (!selected || launching) return
    setLaunching(true)
    const overseer = authenticatedApi.newWorkspace()
    try {
      const models = await authenticatedApi.listModels()
      const modelId = getStoredSelectedModel(models)
      const [chat, metadata] = await Promise.all([
        overseer.newChat(workspacePrompt(selected), modelId),
        overseer.getMetadata(),
      ])
      await navigate({to: '/workspace/$id', params: {id: metadata.id}, search: {chat}})
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason))
    } finally {
      overseer[Symbol.dispose]()
      setLaunching(false)
    }
  }, [authenticatedApi, launching, navigate, selected])

  return <div className="flex h-full min-h-0 flex-col bg-kumo-base">
    <header className="flex shrink-0 items-center justify-between border-b border-kumo-line px-6 py-4">
      <div className="flex items-center gap-3">
        <span className="flex h-9 w-9 items-center justify-center rounded-xl border border-kumo-line bg-kumo-elevated text-kumo-brand"><Database size={19} weight="duotone" /></span>
        <div><h1 className="text-lg font-semibold tracking-tight text-kumo-default">Lakehouse</h1><p className="text-[12px] text-kumo-subtle">Explore the data and relationships available to your agents.</p></div>
      </div>
      <button type="button" onClick={() => void loadCatalog()} disabled={loading} className="inline-flex h-9 cursor-pointer items-center gap-2 rounded-lg border border-kumo-line px-3 text-[12px] font-medium text-kumo-default hover:bg-kumo-tint disabled:opacity-50">
        <ArrowsClockwise size={14} className={loading ? 'animate-spin' : ''} /> Refresh
      </button>
    </header>

    <div className="grid min-h-0 flex-1 grid-cols-[minmax(0,1fr)_340px]">
      <main className="flex min-h-0 min-w-0 flex-col border-r border-kumo-line">
        <div className="flex shrink-0 items-center gap-2 border-b border-kumo-line px-5 py-3">
          {(['table', 'vector', 'graph'] as const).map((kind) => <KindButton key={kind} kind={kind} active={activeKind === kind} count={catalogCount(catalog, kind)} onClick={() => {setActiveKind(kind); setSelected(null)}} />)}
          <label className="ml-auto flex h-9 w-[280px] items-center gap-2 rounded-lg border border-kumo-line bg-kumo-elevated px-3 focus-within:border-kumo-brand">
            <MagnifyingGlass size={14} className="shrink-0 text-kumo-inactive" />
            <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={`Search ${kindLabel(activeKind).toLocaleLowerCase()}…`} className="min-w-0 flex-1 bg-transparent text-[12px] text-kumo-default outline-none placeholder:text-kumo-inactive" />
          </label>
        </div>

        <div className="min-h-0 flex-1 overflow-auto p-5">
          {error && <div className="mb-4 rounded-lg border border-kumo-danger/25 bg-kumo-danger-tint px-3 py-2 text-[12px] text-kumo-danger">{error}</div>}
          {loading ? <EmptyState icon={<SpinnerGap size={22} className="animate-spin" />} title="Loading lakehouse" detail="Discovering Tables, Vectors, and Graphs from Verglas." />
            : items.length === 0 ? <EmptyState icon={kindIcon(activeKind, 22)} title={`No ${kindLabel(activeKind).toLocaleLowerCase()} found`} detail={search ? 'Try a different search.' : `Verglas did not report any ${kindLabel(activeKind).toLocaleLowerCase()} in this catalog.`} />
            : <div className="grid grid-cols-[repeat(auto-fill,minmax(240px,1fr))] gap-3">{items.map((item) => <CatalogCard key={item.id} item={item} selected={selected?.id === item.id} onSelect={() => setSelected(item)} />)}</div>}
        </div>
      </main>

      <aside className="min-h-0 overflow-auto bg-kumo-elevated/30">
        {selected ? <AssetDetails item={selected} launching={launching} onQuery={() => void querySelected()} /> : <div className="flex h-full min-h-[360px] flex-col items-center justify-center px-8 text-center">
          <span className="mb-4 flex h-12 w-12 items-center justify-center rounded-2xl border border-kumo-line bg-kumo-base text-kumo-inactive">{kindIcon(activeKind, 24)}</span>
          <h2 className="text-[14px] font-medium text-kumo-default">Select {indefiniteKind(activeKind)}</h2>
          <p className="mt-1.5 max-w-[240px] text-[12px] leading-5 text-kumo-subtle">Inspect its identity and open a Workspace with the asset already in context.</p>
        </div>}
      </aside>
    </div>
  </div>
}

function KindButton({kind, count, active, onClick}: {kind: CatalogKind; count: number; active: boolean; onClick: () => void}) {
  return <button type="button" onClick={onClick} className={`inline-flex h-9 cursor-pointer items-center gap-2 rounded-lg px-3 text-[12px] font-medium transition-colors ${active ? 'bg-kumo-fill text-kumo-default' : 'text-kumo-subtle hover:bg-kumo-tint hover:text-kumo-default'}`}>
    {kindIcon(kind, 15)} {kindLabel(kind)} <span className={`rounded-md px-1.5 py-0.5 font-mono text-[10px] ${active ? 'bg-kumo-base text-kumo-default' : 'bg-kumo-elevated text-kumo-inactive'}`}>{count}</span>
  </button>
}

function CatalogCard({item, selected, onSelect}: {item: CatalogItem; selected: boolean; onSelect: () => void}) {
  const presentation = itemPresentation(item)
  return <button type="button" onClick={onSelect} className={`group min-h-[116px] cursor-pointer rounded-xl border p-4 text-left transition-all ${selected ? 'border-kumo-brand bg-kumo-brand/5 shadow-sm' : 'border-kumo-line bg-kumo-base hover:-translate-y-0.5 hover:border-kumo-line-strong hover:shadow-sm'}`}>
    <div className="flex items-start justify-between gap-3"><span className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-lg ${selected ? 'bg-kumo-brand text-white' : 'bg-kumo-elevated text-kumo-brand'}`}>{kindIcon(item.kind, 16)}</span><ArrowRight size={14} className={`mt-2 transition-transform ${selected ? 'translate-x-0 text-kumo-brand' : '-translate-x-1 text-kumo-inactive opacity-0 group-hover:translate-x-0 group-hover:opacity-100'}`} /></div>
    <div className="mt-3 truncate text-[13px] font-medium text-kumo-default">{presentation.title}</div>
    <div className="mt-0.5 truncate font-mono text-[10px] text-kumo-inactive">{presentation.subtitle}</div>
  </button>
}

function AssetDetails({item, launching, onQuery}: {item: CatalogItem; launching: boolean; onQuery: () => void}) {
  const presentation = itemPresentation(item)
  const facts = itemFacts(item)
  return <div className="flex min-h-full flex-col p-6">
    <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-kumo-brand text-white">{kindIcon(item.kind, 20)}</span>
    <div className="mt-4 text-[10px] font-semibold uppercase tracking-[0.12em] text-kumo-brand">{singularKind(item.kind)}</div>
    <h2 className="mt-1 break-words text-lg font-semibold tracking-tight text-kumo-default">{presentation.title}</h2>
    <p className="mt-1 break-all font-mono text-[11px] leading-5 text-kumo-subtle">{presentation.subtitle}</p>
    <dl className="mt-6 divide-y divide-kumo-line border-y border-kumo-line">{facts.map(([label, value]) => <div key={label} className="grid grid-cols-[100px_minmax(0,1fr)] gap-3 py-3"><dt className="text-[11px] text-kumo-inactive">{label}</dt><dd className="break-all text-right font-mono text-[11px] text-kumo-default">{value}</dd></div>)}</dl>
    <div className="mt-auto pt-8">
      <button type="button" disabled={launching} onClick={onQuery} className="flex h-10 w-full cursor-pointer items-center justify-center gap-2 rounded-lg bg-kumo-brand px-4 text-[12px] font-semibold text-white shadow-sm hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60">
        {launching ? <SpinnerGap size={15} className="animate-spin" /> : <ArrowRight size={15} weight="bold" />} Query in Workspace
      </button>
      <p className="mt-2 text-center text-[10px] leading-4 text-kumo-inactive">Opens a new agent workspace with this asset selected.</p>
    </div>
  </div>
}

function EmptyState({icon, title, detail}: {icon: React.ReactNode; title: string; detail: string}) {
  return <div className="flex min-h-[360px] flex-col items-center justify-center text-center"><span className="text-kumo-inactive">{icon}</span><h2 className="mt-3 text-[13px] font-medium text-kumo-default">{title}</h2><p className="mt-1 text-[11px] text-kumo-subtle">{detail}</p></div>
}

function itemsForKind(catalog: VerglasCatalogSnapshot, kind: CatalogKind): CatalogItem[] {
  if (kind === 'table') return catalog.tables.map((value) => ({kind, id: `table:${value.qualifiedName}`, value}))
  if (kind === 'vector') return catalog.vectors.map((value) => ({kind, id: `vector:${value.target}:${value.field}`, value}))
  return catalog.graphs.map((value) => ({kind, id: `graph:${value.namespace}`, value}))
}

function catalogContains(catalog: VerglasCatalogSnapshot, item: CatalogItem): boolean {
  return itemsForKind(catalog, item.kind).some((candidate) => candidate.id === item.id)
}

function searchableText(item: CatalogItem): string {
  const presentation = itemPresentation(item)
  return `${presentation.title} ${presentation.subtitle}`
}

function itemPresentation(item: CatalogItem): {title: string; subtitle: string} {
  if (item.kind === 'table') return {title: item.value.name, subtitle: item.value.namespace.join('.') || 'default'}
  if (item.kind === 'vector') return {title: item.value.field, subtitle: item.value.target}
  return {title: item.value.namespace, subtitle: `${item.value.nodesTable} + ${item.value.edgesTable}`}
}

function itemFacts(item: CatalogItem): Array<[string, string]> {
  if (item.kind === 'table') return [['Namespace', item.value.namespace.join('.') || 'default'], ['Table', item.value.name], ['SQL name', item.value.qualifiedName]]
  if (item.kind === 'vector') return [['Target', item.value.target], ['Field', item.value.field], ['Metric', item.value.metric], ['Vectors', item.value.liveCount?.toLocaleString() ?? 'Not reported'], ['Snapshot', item.value.reflectedSnapshot?.toString() ?? 'Not reported']]
  return [['Namespace', item.value.namespace], ['Nodes', item.value.nodesTable], ['Edges', item.value.edgesTable]]
}

function workspacePrompt(item: CatalogItem): string {
  if (item.kind === 'table') return `I selected the Verglas lakehouse table ${item.value.qualifiedName}. Help me explore and query this table. Start by running this exact bounded sample query through the Verglas SDK: SELECT * FROM ${item.value.qualifiedName} LIMIT 100. Verglas supports LIMIT, not FETCH FIRST. Log the structured query result so the Workspace renders it as an interactive data widget, then explain what the data contains.`
  if (item.kind === 'vector') return `I selected the Verglas vector index ${item.value.target} on field ${item.value.field} using ${item.value.metric} distance. Help me inspect and query this vector index through the Verglas SDK. Show structured results in the Workspace data widget and explain useful searches I can run.`
  return `I selected the Verglas property graph ${item.value.namespace}, backed by ${item.value.nodesTable} and ${item.value.edgesTable}. Help me explore this graph through the Verglas SDK. Begin with a bounded overview of its nodes and relationships and return structured results for the Workspace data widget.`
}

function catalogCount(catalog: VerglasCatalogSnapshot, kind: CatalogKind): number {
  return kind === 'table' ? catalog.tables.length : kind === 'vector' ? catalog.vectors.length : catalog.graphs.length
}

function kindLabel(kind: CatalogKind): string { return kind === 'table' ? 'Tables' : kind === 'vector' ? 'Vectors' : 'Graphs' }
function singularKind(kind: CatalogKind): string { return kind === 'table' ? 'Table' : kind === 'vector' ? 'Vector index' : 'Graph' }
function indefiniteKind(kind: CatalogKind): string { return kind === 'table' ? 'a table' : kind === 'vector' ? 'a vector index' : 'a graph' }
function kindIcon(kind: CatalogKind, size: number) {
  if (kind === 'table') return <Table size={size} weight="duotone" />
  if (kind === 'vector') return <VectorThree size={size} weight="duotone" />
  return <CirclesThreePlus size={size} weight="duotone" />
}
