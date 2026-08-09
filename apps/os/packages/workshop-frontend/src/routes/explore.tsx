import { createFileRoute, Link } from '@tanstack/react-router'
import { Browser, FlowArrow, Globe, MagnifyingGlass, PlugsConnected, UsersThree } from '@phosphor-icons/react'
import { useEffect, useMemo, useState, type ReactNode } from 'react'
import type { BlueprintPublicInfo } from '@verglas/workshop-shared/api'
import { useAuthenticatedApi } from '../AuthContext'
import { useDocumentTitle } from '../useDocumentTitle'

export const Route = createFileRoute('/explore')({ component: ExplorePage })

type Scope = 'organization' | 'global'
type Kind = 'workflow' | 'application' | 'integration'

const kinds: Array<{id: Kind; label: string; icon: ReactNode}> = [
  {id: 'workflow', label: 'Workers', icon: <FlowArrow size={15} />},
  {id: 'application', label: 'Applications', icon: <Browser size={15} />},
  {id: 'integration', label: 'Integrations', icon: <PlugsConnected size={15} />},
]

function ExplorePage() {
  useDocumentTitle('Explore')
  const { authenticatedApi } = useAuthenticatedApi()
  const [scope, setScope] = useState<Scope>('organization')
  const [kind, setKind] = useState<Kind>('application')
  const [search, setSearch] = useState('')
  const [globalApplications, setGlobalApplications] = useState<BlueprintPublicInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    authenticatedApi.listFeaturedBlueprints()
      .then((items) => { if (!cancelled) setGlobalApplications(items) })
      .catch((err) => { if (!cancelled) setError(err instanceof Error ? err.message : String(err)) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [authenticatedApi])

  const applications = useMemo(() => {
    if (scope !== 'global' || kind !== 'application') return []
    const query = search.trim().toLowerCase()
    if (!query) return globalApplications
    return globalApplications.filter((item) =>
      item.metadata.title.toLowerCase().includes(query) ||
      (item.metadata.description ?? '').toLowerCase().includes(query))
  }, [globalApplications, kind, scope, search])

  return <div className="mx-auto flex h-full w-full max-w-5xl flex-col px-6 pb-12 pt-10 sm:px-10">
    <header>
      <h1 className="text-2xl font-semibold tracking-tight text-kumo-default">Explore</h1>
      <p className="mt-1 text-[13px] leading-5 text-kumo-subtle">Discover reusable Workers, Applications, and Integrations exported by your organization or published globally.</p>
    </header>

    <div className="mt-6 flex flex-col gap-4 border-b border-kumo-line pb-4 sm:flex-row sm:items-center sm:justify-between">
      <div className="inline-flex w-fit rounded-lg bg-kumo-tint p-1">
        <ScopeButton active={scope === 'organization'} onClick={() => setScope('organization')} icon={<UsersThree size={14} />}>Organization</ScopeButton>
        <ScopeButton active={scope === 'global'} onClick={() => setScope('global')} icon={<Globe size={14} />}>Global</ScopeButton>
      </div>
      <label className="relative sm:w-64">
        <MagnifyingGlass size={15} className="absolute left-3 top-1/2 -translate-y-1/2 text-kumo-inactive" />
        <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search exports…" className="h-9 w-full rounded-lg border border-kumo-line bg-kumo-base pl-9 pr-3 text-[13px] text-kumo-default outline-none focus:border-kumo-brand" />
      </label>
    </div>

    <nav aria-label="Export type" className="mt-4 flex gap-1">
      {kinds.map((item) => <button key={item.id} onClick={() => setKind(item.id)} className={`inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg px-3 text-[13px] transition-colors ${kind === item.id ? 'bg-kumo-fill font-medium text-kumo-strong' : 'text-kumo-subtle hover:bg-kumo-tint hover:text-kumo-default'}`}>{item.icon}{item.label}</button>)}
    </nav>

    <div className="mt-5 min-h-0 flex-1 overflow-y-auto">
      {error ? <Empty title="Marketplace unavailable" message={error} /> : loading ? <Empty title="Loading exports…" message="Reading the marketplace catalog." /> : applications.length > 0 ? (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">{applications.map((application) => <ApplicationExportCard key={application.id} application={application} />)}</div>
      ) : (
        <Empty
          title={search ? 'No exports match' : `No ${scope} ${kind} exports yet`}
          message={search ? 'Try a different search term.' : scope === 'organization'
            ? `Export ${kind === 'workflow' ? 'a' : 'an'} ${kind} from its detail page to make it reusable across your organization.`
            : `Globally published ${kind}s will appear here after registry review.`}
        />
      )}
    </div>
  </div>
}

function ScopeButton({active, onClick, icon, children}: {active: boolean; onClick: () => void; icon: ReactNode; children: ReactNode}) {
  return <button onClick={onClick} className={`inline-flex h-8 cursor-pointer items-center gap-1.5 rounded-md px-3 text-[12px] ${active ? 'bg-kumo-base font-medium text-kumo-default shadow-sm' : 'text-kumo-subtle'}`}>{icon}{children}</button>
}

function ApplicationExportCard({application}: {application: BlueprintPublicInfo}) {
  return <article className="relative overflow-hidden rounded-xl border border-kumo-line bg-kumo-base transition-colors hover:border-kumo-fill">
    <Link to="/blueprint/$id" params={{id: application.id}} aria-label={`Open ${application.metadata.title}`} className="absolute inset-0 z-10" />
    <div className="flex aspect-[16/9] items-center justify-center border-b border-kumo-line bg-kumo-tint">{application.screenshotUrl ? <img src={application.screenshotUrl} alt="" className="h-full w-full object-cover" /> : <Browser size={28} className="text-kumo-inactive" />}</div>
    <div className="p-4"><div className="flex items-center gap-2"><span className="rounded-md bg-kumo-fill px-2 py-0.5 text-[10px] font-semibold uppercase text-kumo-subtle">Application</span><span className="text-[10px] text-kumo-inactive">Global</span></div><h2 className="mt-3 truncate text-sm font-medium text-kumo-default">{application.metadata.title}</h2><p className="mt-1 line-clamp-2 text-[12px] leading-5 text-kumo-subtle">{application.metadata.description || 'No description provided.'}</p></div>
  </article>
}

function Empty({title, message}: {title: string; message: string}) {
  return <div className="rounded-xl border border-dashed border-kumo-line px-6 py-14 text-center"><h2 className="text-sm font-medium text-kumo-default">{title}</h2><p className="mx-auto mt-1 max-w-md text-[12px] leading-5 text-kumo-subtle">{message}</p></div>
}
