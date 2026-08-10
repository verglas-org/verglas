import { Button, useKumoToastManager } from '@cloudflare/kumo'
import { Database, FlowArrow, Key, ShieldCheck, Stack, UserCircleGear } from '@phosphor-icons/react'
import type { RpcStub } from 'capnweb'
import { useEffect, useMemo, useState } from 'react'
import type {
  AdminApi,
  VerglasAccessAction,
  VerglasAccessSnapshot,
} from '@verglas/workshop-shared/api'
import { buildResourceTree, formatAccessAction } from '../accessPresentation'

const DELEGATABLE_ACTIONS: VerglasAccessAction[] = [
  'discover', 'describe', 'query', 'append', 'modify', 'create_child', 'execute',
  'use_secret', 'deploy', 'pass_grants', 'manage_grants',
]

const INPUT = 'h-10 w-full rounded-lg border border-kumo-line bg-kumo-base px-3 text-sm text-kumo-default outline-none transition focus:border-kumo-ring focus:ring-2 focus:ring-kumo-ring/15'

function ResourceKindBadge({ kind }: { kind: string }) {
  return <span className="rounded-md bg-kumo-tint px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-[0.08em] text-kumo-subtle">{kind.replaceAll('_', ' ')}</span>
}

/** Tenant principal, hierarchical resource, and grant management for deployment administrators. */
export default function AdminAccessPanel({ admin }: { admin: RpcStub<AdminApi> }) {
  const toasts = useKumoToastManager()
  const [snapshot, setSnapshot] = useState<VerglasAccessSnapshot | null>(null)
  const [principalId, setPrincipalId] = useState('')
  const [resourceId, setResourceId] = useState('')
  const [actions, setActions] = useState<VerglasAccessAction[]>(['query'])
  const [saving, setSaving] = useState(false)

  const load = async () => {
    const next = await admin.getAccessSnapshot()
    setSnapshot(next)
    setPrincipalId((value) => value || next.principals.find((principal) => principal.kind !== 'user')?.id || '')
    setResourceId((value) => value || next.resources.find((resource) => resource.kind === 'database')?.id || next.resources[0]?.id || '')
  }

  useEffect(() => {
    void load().catch((error) => {
      toasts.add({ title: error instanceof Error ? error.message : 'Failed to load access state', variant: 'error' })
    })
  }, [admin])

  const processPrincipals = useMemo(
    () => snapshot?.principals.filter((principal) => principal.kind !== 'user') ?? [],
    [snapshot],
  )
  const resources = useMemo(() => snapshot ? buildResourceTree(snapshot.resources) : [], [snapshot])
  const selectedResource = snapshot?.resources.find((resource) => resource.id === resourceId)

  const toggleAction = (action: VerglasAccessAction) => {
    setActions((current) => current.includes(action)
      ? current.filter((item) => item !== action)
      : [...current, action])
  }

  const delegate = async () => {
    if (!principalId || !resourceId || actions.length === 0) return
    setSaving(true)
    try {
      await admin.delegateAccess({ principalId, resourceId, actions })
      await load()
      toasts.add({ title: 'Access delegated', variant: 'success' })
    } catch (error) {
      toasts.add({
        title: error instanceof Error ? error.message : 'Unable to delegate access',
        variant: 'error',
      })
    } finally {
      setSaving(false)
    }
  }

  const revoke = async (grantId: string) => {
    try {
      await admin.revokeAccess(grantId)
      await load()
      toasts.add({ title: 'Grant revoked', variant: 'success' })
    } catch (error) {
      toasts.add({ title: error instanceof Error ? error.message : 'Unable to revoke grant', variant: 'error' })
    }
  }

  if (!snapshot) {
    return <div className="rounded-xl border border-kumo-line bg-kumo-elevated p-6 text-sm text-kumo-subtle">Loading tenant access…</div>
  }

  return (
    <div className="space-y-5">
      <div className="grid gap-3 sm:grid-cols-3">
        <Summary icon={<UserCircleGear size={18} />} label="Principals" value={snapshot.principals.length} detail="People and workloads" />
        <Summary icon={<Database size={18} />} label="Resources" value={snapshot.resources.length} detail="Databases and runtimes" />
        <Summary icon={<Key size={18} />} label="Explicit grants" value={snapshot.grants.length} detail="Inherited access is live" />
      </div>

      <div className="grid gap-5 xl:grid-cols-[minmax(0,0.9fr)_minmax(360px,1.1fr)]">
        <section className="overflow-hidden rounded-xl border border-kumo-line bg-kumo-elevated">
          <div className="border-b border-kumo-line px-5 py-4">
            <div className="flex items-center gap-2">
              <Stack size={17} className="text-kumo-brand" />
              <h2 className="text-sm font-semibold text-kumo-default">Resource hierarchy</h2>
            </div>
            <p className="mt-1 text-xs leading-5 text-kumo-subtle">Database grants inherit down to namespaces, tables, graph, and vector resources.</p>
          </div>
          <div className="max-h-[352px] overflow-y-auto p-2">
            {resources.map(({ resource, depth }) => {
              const selected = resource.id === resourceId
              return (
                <button
                  key={resource.id}
                  type="button"
                  onClick={() => setResourceId(resource.id)}
                  style={{ paddingLeft: `${12 + depth * 18}px` }}
                  className={`flex w-full items-center gap-2 rounded-lg py-2 pr-2 text-left transition ${selected ? 'bg-kumo-brand/10 text-kumo-brand' : 'text-kumo-default hover:bg-kumo-tint'}`}
                >
                  <span className={`h-1.5 w-1.5 shrink-0 rounded-full ${selected ? 'bg-kumo-brand' : 'bg-kumo-line'}`} />
                  <span className="min-w-0 flex-1 truncate font-mono text-xs">{resource.id}</span>
                  <ResourceKindBadge kind={resource.kind} />
                </button>
              )
            })}
            {resources.length === 0 && <p className="px-3 py-8 text-center text-sm text-kumo-subtle">No protected resources registered yet.</p>}
          </div>
        </section>

        <section className="rounded-xl border border-kumo-line bg-kumo-elevated p-5">
          <div className="flex items-center gap-2">
            <FlowArrow size={17} className="text-kumo-brand" />
            <h2 className="text-sm font-semibold text-kumo-default">Delegate access</h2>
          </div>
          <p className="mt-1 text-xs leading-5 text-kumo-subtle">The grant is accepted only when your principal can pass every selected action on this resource.</p>
          <div className="mt-5 grid gap-4 sm:grid-cols-2">
            <label className="text-xs font-medium text-kumo-subtle">
              Workload principal
              <select value={principalId} onChange={(event) => setPrincipalId(event.target.value)} className={`mt-1.5 ${INPUT}`}>
                <option value="">Select a Job, Vessel, or service</option>
                {processPrincipals.map((principal) => <option key={principal.id} value={principal.id}>{principal.id} · {principal.kind}</option>)}
              </select>
            </label>
            <label className="text-xs font-medium text-kumo-subtle">
              Resource
              <select value={resourceId} onChange={(event) => setResourceId(event.target.value)} className={`mt-1.5 ${INPUT}`}>
                <option value="">Select a resource</option>
                {resources.map(({ resource, depth }) => <option key={resource.id} value={resource.id}>{'  '.repeat(depth)}{resource.id}</option>)}
              </select>
            </label>
          </div>
          {selectedResource && <p className="mt-2 font-mono text-[11px] text-kumo-inactive">Selected {selectedResource.kind}: {selectedResource.id}</p>}
          <fieldset className="mt-5">
            <legend className="text-xs font-medium text-kumo-subtle">Actions</legend>
            <div className="mt-2 flex flex-wrap gap-2">
              {DELEGATABLE_ACTIONS.map((action) => {
                const selected = actions.includes(action)
                return <button key={action} type="button" aria-pressed={selected} onClick={() => toggleAction(action)} className={`rounded-full border px-2.5 py-1 text-xs transition ${selected ? 'border-kumo-brand bg-kumo-brand/10 text-kumo-brand' : 'border-kumo-line text-kumo-subtle hover:bg-kumo-tint'}`}>{formatAccessAction(action)}</button>
              })}
            </div>
          </fieldset>
          <div className="mt-5 flex justify-end">
            <Button variant="primary" size="sm" loading={saving} disabled={!principalId || !resourceId || actions.length === 0} onClick={delegate}>Delegate access</Button>
          </div>
        </section>
      </div>

      <section className="overflow-hidden rounded-xl border border-kumo-line bg-kumo-elevated">
        <div className="flex items-start justify-between gap-4 border-b border-kumo-line px-5 py-4">
          <div>
            <h2 className="text-sm font-semibold text-kumo-default">Active grants</h2>
            <p className="mt-0.5 text-xs text-kumo-subtle">Revoking a grant takes effect for the next request. Root owner grants cannot be removed here.</p>
          </div>
          <ShieldCheck size={19} className="mt-0.5 shrink-0 text-kumo-brand" />
        </div>
        {snapshot.grants.length === 0 ? <p className="px-5 py-8 text-center text-sm text-kumo-subtle">No explicit grants.</p> : (
          <div className="divide-y divide-kumo-line">
            {snapshot.grants.map((grant) => (
              <div key={grant.id} className="flex items-center gap-4 px-5 py-3.5">
                <div className="min-w-0 flex-1">
                  <p className="truncate font-mono text-xs text-kumo-default">{grant.principalId} <span className="text-kumo-inactive">→</span> {grant.resourceId}</p>
                  <div className="mt-2 flex flex-wrap gap-1.5">{grant.actions.map((action) => <span key={action} className="rounded bg-kumo-tint px-1.5 py-0.5 text-[10px] font-medium text-kumo-subtle">{formatAccessAction(action)}</span>)}</div>
                </div>
                {!grant.id.startsWith('local-owner/') && <Button variant="ghost" size="sm" onClick={() => void revoke(grant.id)}>Revoke</Button>}
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="overflow-hidden rounded-xl border border-kumo-line bg-kumo-elevated">
        <div className="flex items-start justify-between gap-4 border-b border-kumo-line px-5 py-4">
          <div>
            <h2 className="text-sm font-semibold text-kumo-default">Token inventory</h2>
            <p className="mt-0.5 text-xs text-kumo-subtle">Credential values are never retained or displayed here.</p>
          </div>
          <Key size={19} className="mt-0.5 shrink-0 text-kumo-brand" />
        </div>
        {snapshot.tokens.length === 0 ? <p className="px-5 py-8 text-center text-sm text-kumo-subtle">No scoped credentials issued.</p> : (
          <div className="divide-y divide-kumo-line">
            {snapshot.tokens.map((token) => {
              const active = !token.revokedAt && token.expiresAt * 1_000 > Date.now()
              return <div key={token.id} className="flex items-center gap-3 px-5 py-3.5"><span className={`h-2 w-2 shrink-0 rounded-full ${active ? 'bg-kumo-success' : 'bg-kumo-inactive'}`} /><div className="min-w-0 flex-1"><p className="truncate text-sm font-medium text-kumo-default">{token.name}</p><p className="mt-0.5 truncate font-mono text-[11px] text-kumo-subtle">{token.parentPrincipalId} → {token.principalId}</p><p className="mt-1 text-xs text-kumo-subtle">{token.audience} · {active ? `Expires ${new Date(token.expiresAt * 1_000).toLocaleDateString()}` : token.revokedAt ? 'Revoked' : 'Expired'}</p></div></div>
            })}
          </div>
        )}
      </section>
    </div>
  )
}

function Summary({ icon, label, value, detail }: { icon: React.ReactNode; label: string; value: number; detail: string }) {
  return (
    <div className="flex items-center gap-3 rounded-xl border border-kumo-line bg-kumo-elevated px-4 py-3.5">
      <div className="grid h-9 w-9 place-items-center rounded-lg bg-kumo-tint text-kumo-brand">{icon}</div>
      <div>
        <p className="text-lg font-semibold leading-5 text-kumo-default">{value}</p>
        <p className="text-xs text-kumo-subtle">{label} · {detail}</p>
      </div>
    </div>
  )
}
