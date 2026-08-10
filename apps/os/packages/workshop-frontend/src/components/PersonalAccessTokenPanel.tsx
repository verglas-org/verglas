import { Button, useKumoToastManager } from '@cloudflare/kumo'
import { CheckCircle, Copy, Key, Plus, ShieldCheck, Trash } from '@phosphor-icons/react'
import type { RpcStub } from 'capnweb'
import { useEffect, useMemo, useState } from 'react'
import type { AuthenticatedApi, VerglasAccessAction, VerglasAccessResource } from '@verglas/workshop-shared/api'
import { buildResourceTree, formatAccessAction, isTokenRequestComplete, toTokenGrants } from '../accessPresentation'

const ACTIONS: VerglasAccessAction[] = ['discover', 'describe', 'query', 'append', 'modify', 'create_child', 'execute', 'use_secret', 'deploy', 'connect']
const EXPIRATIONS = [
  { label: '24 hours', seconds: 60 * 60 * 24 },
  { label: '7 days', seconds: 60 * 60 * 24 * 7 },
  { label: '30 days', seconds: 60 * 60 * 24 * 30 },
  { label: '90 days', seconds: 60 * 60 * 24 * 90 },
]
const INPUT = 'h-9 w-full rounded-lg border border-kumo-line bg-kumo-base px-3 text-[13px] text-kumo-default outline-none transition focus:border-kumo-ring focus:ring-2 focus:ring-kumo-ring/15'

type AccessToken = Awaited<ReturnType<RpcStub<AuthenticatedApi>['listAccessTokens']>>[number]
type CreateTokenInput = Parameters<RpcStub<AuthenticatedApi>['createAccessToken']>[0]

/** Creates, displays once, and revokes the current user's delegated local development tokens. */
export default function PersonalAccessTokenPanel({
  api,
  resources,
}: {
  api: RpcStub<AuthenticatedApi>
  resources: VerglasAccessResource[]
}) {
  const toasts = useKumoToastManager()
  const [tokens, setTokens] = useState<AccessToken[]>([])
  const [loading, setLoading] = useState(true)
  const [creating, setCreating] = useState(false)
  const [name, setName] = useState('Local development')
  const [audience, setAudience] = useState('verglas-cli')
  const [expiration, setExpiration] = useState(EXPIRATIONS[2].seconds)
  const [resourceIds, setResourceIds] = useState<string[]>([])
  const [actions, setActions] = useState<VerglasAccessAction[]>(['discover', 'describe', 'query'])
  const [revealedToken, setRevealedToken] = useState<string | null>(null)

  const resourceTree = useMemo(() => buildResourceTree(resources), [resources])
  const expiresAt = useMemo(() => new Date(Date.now() + expiration * 1_000), [expiration])
  const canCreate = isTokenRequestComplete({ name, expiresAt, resourceIds, actions }) && audience.trim().length > 0

  const load = async () => {
    setLoading(true)
    try {
      setTokens(await api.listAccessTokens())
    } catch (error) {
      toasts.add({ title: error instanceof Error ? error.message : 'Failed to load access tokens', variant: 'error' })
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { void load() }, [api])

  const toggleResource = (resourceId: string) => {
    setResourceIds((current) => current.includes(resourceId)
      ? current.filter((item) => item !== resourceId)
      : [...current, resourceId])
  }

  const toggleAction = (action: VerglasAccessAction) => {
    setActions((current) => current.includes(action)
      ? current.filter((item) => item !== action)
      : [...current, action])
  }

  const create = async () => {
    if (!canCreate) return
    setCreating(true)
    setRevealedToken(null)
    try {
      const input: CreateTokenInput = {
        name: name.trim(),
        audience: audience.trim(),
        expiresInSeconds: expiration,
        grants: toTokenGrants(resourceIds, actions),
      }
      const { token, ...summary } = await api.createAccessToken(input)
      setRevealedToken(token)
      setTokens((current) => [summary, ...current])
      toasts.add({ title: 'Access token created', variant: 'success' })
    } catch (error) {
      toasts.add({ title: error instanceof Error ? error.message : 'Unable to create access token', variant: 'error' })
    } finally {
      setCreating(false)
    }
  }

  const revoke = async (token: AccessToken) => {
    try {
      await api.revokeAccessToken(token.id)
      setTokens((current) => current.map((item) => item.id === token.id ? { ...item, revokedAt: Math.floor(Date.now() / 1_000) } : item))
      toasts.add({ title: 'Access token revoked', variant: 'success' })
    } catch (error) {
      toasts.add({ title: error instanceof Error ? error.message : 'Unable to revoke access token', variant: 'error' })
    }
  }

  const copyToken = async () => {
    if (!revealedToken) return
    try {
      await navigator.clipboard.writeText(revealedToken)
      toasts.add({ title: 'Token copied to clipboard', variant: 'success' })
    } catch {
      toasts.add({ title: 'Could not copy token', variant: 'error' })
    }
  }

  return (
    <section className="flex flex-col gap-3">
      <div className="flex items-end justify-between gap-4 px-1">
        <div>
          <h2 className="text-[12px] font-medium uppercase tracking-[0.08em] text-kumo-inactive">Developer access</h2>
          <p className="mt-1 text-[12px] leading-5 text-kumo-subtle">Create a scoped token for the Verglas CLI or SDK. Tokens receive only the permissions you can delegate.</p>
        </div>
      </div>

      <div className="overflow-hidden rounded-xl border border-kumo-line bg-kumo-base">
        <div className="border-b border-kumo-line px-5 py-4">
          <div className="flex items-center gap-2">
            <div className="grid h-8 w-8 place-items-center rounded-lg bg-kumo-tint text-kumo-brand"><Plus size={16} /></div>
            <div><h3 className="text-[14px] font-medium text-kumo-default">Create access token</h3><p className="mt-0.5 text-[12px] text-kumo-subtle">Choose a narrow scope and a bounded lifetime.</p></div>
          </div>
        </div>
        <div className="space-y-5 p-5">
          <div className="grid gap-4 sm:grid-cols-2">
            <label className="text-[12px] font-medium text-kumo-subtle">Token name<input value={name} onChange={(event) => setName(event.target.value)} className={`mt-1.5 ${INPUT}`} placeholder="Local development" /></label>
            <label className="text-[12px] font-medium text-kumo-subtle">Audience<select value={audience} onChange={(event) => setAudience(event.target.value)} className={`mt-1.5 ${INPUT}`}><option value="verglas-cli">CLI and SDK</option><option value="data-plane">Data plane only</option></select></label>
          </div>
          <div>
            <p className="text-[12px] font-medium text-kumo-subtle">Expires</p>
            <div className="mt-2 flex flex-wrap gap-2">{EXPIRATIONS.map((option) => <button key={option.seconds} type="button" onClick={() => setExpiration(option.seconds)} className={`rounded-full border px-3 py-1.5 text-xs transition ${expiration === option.seconds ? 'border-kumo-brand bg-kumo-brand/10 text-kumo-brand' : 'border-kumo-line text-kumo-subtle hover:bg-kumo-tint'}`}>{option.label}</button>)}</div>
          </div>
          <div>
            <p className="text-[12px] font-medium text-kumo-subtle">Resources</p>
            <div className="mt-2 max-h-48 overflow-y-auto rounded-lg border border-kumo-line bg-kumo-elevated p-1.5">
              {resourceTree.map(({ resource, depth }) => {
                const selected = resourceIds.includes(resource.id)
                return <label key={resource.id} style={{ paddingLeft: `${8 + depth * 16}px` }} className={`flex cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 text-xs transition ${selected ? 'bg-kumo-brand/10 text-kumo-brand' : 'text-kumo-default hover:bg-kumo-tint'}`}><input checked={selected} onChange={() => toggleResource(resource.id)} type="checkbox" className="accent-[var(--color-kumo-brand)]" /><span className="min-w-0 flex-1 truncate font-mono">{resource.id}</span><span className="text-[10px] uppercase tracking-wide text-kumo-inactive">{resource.kind.replaceAll('_', ' ')}</span></label>
              })}
              {resourceTree.length === 0 && <p className="px-3 py-4 text-center text-xs text-kumo-subtle">No resources are currently delegable.</p>}
            </div>
          </div>
          <div>
            <p className="text-[12px] font-medium text-kumo-subtle">Actions</p>
            <div className="mt-2 flex flex-wrap gap-2">{ACTIONS.map((action) => { const selected = actions.includes(action); return <button key={action} type="button" aria-pressed={selected} onClick={() => toggleAction(action)} className={`rounded-full border px-2.5 py-1 text-xs transition ${selected ? 'border-kumo-brand bg-kumo-brand/10 text-kumo-brand' : 'border-kumo-line text-kumo-subtle hover:bg-kumo-tint'}`}>{formatAccessAction(action)}</button> })}</div>
          </div>
          <div className="flex justify-end"><Button variant="primary" size="sm" loading={creating} disabled={!canCreate} onClick={create}><Key size={14} /> Create token</Button></div>
        </div>
      </div>

      {revealedToken && <div className="rounded-xl border border-kumo-success/40 bg-kumo-success/10 p-4"><div className="flex gap-3"><CheckCircle size={19} className="mt-0.5 shrink-0 text-kumo-success" /><div className="min-w-0 flex-1"><p className="text-sm font-medium text-kumo-default">Copy this token now</p><p className="mt-0.5 text-xs leading-5 text-kumo-subtle">For security, Verglas will not show this value again.</p><div className="mt-3 flex gap-2"><code className="min-w-0 flex-1 overflow-x-auto rounded-md border border-kumo-line bg-kumo-base px-3 py-2 text-xs text-kumo-default">{revealedToken}</code><Button variant="secondary" size="sm" onClick={() => void copyToken()}><Copy size={14} /> Copy</Button></div></div></div></div>}

      <div className="overflow-hidden rounded-xl border border-kumo-line bg-kumo-base">
        <div className="flex items-center gap-2 border-b border-kumo-line px-5 py-4"><ShieldCheck size={17} className="text-kumo-brand" /><div><h3 className="text-[14px] font-medium text-kumo-default">Your access tokens</h3><p className="mt-0.5 text-[12px] text-kumo-subtle">Revoke a token immediately if a device or credential is no longer trusted.</p></div></div>
        {loading ? <p className="px-5 py-8 text-center text-sm text-kumo-subtle">Loading access tokens…</p> : tokens.length === 0 ? <p className="px-5 py-8 text-center text-sm text-kumo-subtle">No personal access tokens yet.</p> : <div className="divide-y divide-kumo-line">{tokens.map((token) => { const active = !token.revokedAt && token.expiresAt * 1_000 > Date.now(); return <div key={token.id} className="flex items-center gap-3 px-5 py-3.5"><div className={`grid h-8 w-8 shrink-0 place-items-center rounded-lg ${active ? 'bg-kumo-tint text-kumo-brand' : 'bg-kumo-fill text-kumo-inactive'}`}><Key size={15} /></div><div className="min-w-0 flex-1"><p className="truncate text-sm font-medium text-kumo-default">{token.name}</p><p className="mt-0.5 text-xs text-kumo-subtle">{token.audience} · {active ? `Expires ${new Date(token.expiresAt * 1_000).toLocaleDateString()}` : token.revokedAt ? 'Revoked' : 'Expired'}</p></div>{active && <Button variant="ghost" size="sm" onClick={() => void revoke(token)}><Trash size={14} /> Revoke</Button>}</div> })}</div>}
      </div>
    </section>
  )
}
