import { Button, useKumoToastManager } from '@cloudflare/kumo'
import type { RpcStub } from 'capnweb'
import { useEffect, useMemo, useState } from 'react'
import type {
  AdminApi,
  VerglasAccessAction,
  VerglasAccessSnapshot,
} from '@verglas/workshop-shared/api'

const DELEGATABLE_ACTIONS: VerglasAccessAction[] = [
  'discover', 'describe', 'query', 'append', 'modify', 'create_child', 'execute',
  'use_secret', 'deploy', 'pass_grants', 'manage_grants',
]

/** Tenant principal, resource, and delegated-grant management for deployment admins. */
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
    setPrincipalId((value) => value || next.principals.find((p) => p.kind !== 'user')?.id || '')
    setResourceId((value) => value || next.resources.find((r) => r.kind !== 'tenant')?.id || 'tenant')
  }

  useEffect(() => {
    void load().catch((error) => {
      toasts.add({title: error instanceof Error ? error.message : 'Failed to load access state', variant: 'error'})
    })
  }, [admin])

  const processPrincipals = useMemo(
    () => snapshot?.principals.filter((principal) => principal.kind !== 'user') ?? [],
    [snapshot],
  )

  const delegate = async () => {
    if (!principalId || !resourceId || actions.length === 0) return
    setSaving(true)
    try {
      await admin.delegateAccess({principalId, resourceId, actions})
      await load()
      toasts.add({title: 'Access delegated', variant: 'success'})
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
      toasts.add({title: 'Grant revoked', variant: 'success'})
    } catch (error) {
      toasts.add({title: error instanceof Error ? error.message : 'Unable to revoke grant', variant: 'error'})
    }
  }

  if (!snapshot) {
    return <div className="rounded-xl border border-kumo-line bg-kumo-elevated p-6 text-sm text-kumo-subtle">Loading tenant access…</div>
  }

  return (
    <div className="space-y-4">
      <div className="rounded-xl border border-kumo-line bg-kumo-elevated p-6">
        <h2 className="text-lg font-semibold text-kumo-strong">Tenant permissions</h2>
        <p className="mt-1 text-sm text-kumo-subtle">
          Delegate access to Jobs and Vessels. Verglas rejects any action you do not already hold
          or cannot pass on this resource.
        </p>
        <div className="mt-5 grid gap-4 md:grid-cols-2">
          <label className="text-xs font-medium text-kumo-subtle">
            Process
            <select value={principalId} onChange={(event) => setPrincipalId(event.target.value)} className="mt-1.5 h-10 w-full rounded-lg border border-kumo-line bg-kumo-base px-3 text-sm text-kumo-default">
              <option value="">Select a Job or Vessel</option>
              {processPrincipals.map((principal) => <option key={principal.id} value={principal.id}>{principal.id} · {principal.kind}</option>)}
            </select>
          </label>
          <label className="text-xs font-medium text-kumo-subtle">
            Resource
            <select value={resourceId} onChange={(event) => setResourceId(event.target.value)} className="mt-1.5 h-10 w-full rounded-lg border border-kumo-line bg-kumo-base px-3 text-sm text-kumo-default">
              {snapshot.resources.map((resource) => <option key={resource.id} value={resource.id}>{resource.id} · {resource.kind}</option>)}
            </select>
          </label>
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          {DELEGATABLE_ACTIONS.map((action) => {
            const selected = actions.includes(action)
            return <button key={action} type="button" onClick={() => setActions((current) => selected ? current.filter((item) => item !== action) : [...current, action])} className={`rounded-full border px-3 py-1.5 text-xs ${selected ? 'border-kumo-brand bg-kumo-brand/10 text-kumo-brand' : 'border-kumo-line text-kumo-subtle hover:bg-kumo-tint'}`}>{action.replaceAll('_', ' ')}</button>
          })}
        </div>
        <div className="mt-5 flex justify-end">
          <Button variant="primary" size="sm" loading={saving} disabled={!principalId || !resourceId || actions.length === 0} onClick={delegate}>Delegate access</Button>
        </div>
      </div>

      <div className="overflow-hidden rounded-xl border border-kumo-line bg-kumo-elevated">
        <div className="border-b border-kumo-line px-5 py-4">
          <h3 className="text-sm font-semibold text-kumo-default">Explicit grants</h3>
          <p className="mt-0.5 text-xs text-kumo-subtle">Inherited permissions are evaluated at request time.</p>
        </div>
        {snapshot.grants.length === 0 ? <p className="px-5 py-8 text-center text-sm text-kumo-subtle">No explicit grants.</p> : snapshot.grants.map((grant) => (
          <div key={grant.id} className="flex items-center gap-4 border-b border-kumo-line px-5 py-3 last:border-b-0">
            <div className="min-w-0 flex-1">
              <p className="truncate font-mono text-xs text-kumo-default">{grant.principalId} → {grant.resourceId}</p>
              <p className="mt-1 text-xs text-kumo-subtle">{grant.actions.join(', ')}</p>
            </div>
            {!grant.id.startsWith('local-owner/') && <Button variant="ghost" size="sm" onClick={() => void revoke(grant.id)}>Revoke</Button>}
          </div>
        ))}
      </div>
    </div>
  )
}
