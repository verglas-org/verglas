import { Button, useKumoToastManager } from '@cloudflare/kumo'
import { CheckCircle, Copy, Key, ShieldCheck, Trash } from '@phosphor-icons/react'
import type { RpcStub } from 'capnweb'
import { useEffect, useMemo, useState } from 'react'
import type { AuthenticatedApi, VerglasAccessAction, VerglasAccessResource } from '@verglas/workshop-shared/api'
import {
  buildResourceTree,
  describeAccessAction,
  formatAccessAction,
  isTokenRequestComplete,
  personalAccessTokens,
  simpleTokenResources,
  type TokenAccessLevel,
  toPresetTokenGrants,
  toTokenGrants,
} from '../accessPresentation'

const ACTIONS: VerglasAccessAction[] = ['discover', 'describe', 'query', 'append', 'modify', 'create_child', 'execute', 'use_secret', 'deploy', 'connect']
const EXPIRATIONS = [
  { label: '24 hours', seconds: 60 * 60 * 24 },
  { label: '7 days', seconds: 60 * 60 * 24 * 7 },
  { label: '30 days', seconds: 60 * 60 * 24 * 30 },
  { label: '90 days', seconds: 60 * 60 * 24 * 90 },
  { label: '1 year', seconds: 60 * 60 * 24 * 366 },
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
  const [showCreate, setShowCreate] = useState(false)
  const [name, setName] = useState('My computer')
  const [expiration, setExpiration] = useState(EXPIRATIONS[2].seconds)
  const [simpleResourceId, setSimpleResourceId] = useState('')
  const [accessLevel, setAccessLevel] = useState<TokenAccessLevel>('read')
  const [advanced, setAdvanced] = useState(false)
  const [resourceIds, setResourceIds] = useState<string[]>([])
  const [actions, setActions] = useState<VerglasAccessAction[]>(['discover', 'describe', 'query'])
  const [revealedToken, setRevealedToken] = useState<string | null>(null)

  const resourceTree = useMemo(() => buildResourceTree(resources), [resources])
  const simpleResources = useMemo(() => simpleTokenResources(resources), [resources])
  const expiresAt = useMemo(() => new Date(Date.now() + expiration * 1_000), [expiration])
  const canCreate = advanced
    ? isTokenRequestComplete({ name, expiresAt, resourceIds, actions })
    : name.trim().length > 0 && simpleResourceId.length > 0

  const load = async () => {
    setLoading(true)
    try {
      setTokens(personalAccessTokens(await api.listAccessTokens()))
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
        audience: 'verglas-cli',
        expiresInSeconds: expiration,
        grants: advanced
          ? toTokenGrants(resourceIds, actions)
          : toPresetTokenGrants(simpleResourceId, accessLevel),
      }
      const { token, ...summary } = await api.createAccessToken(input)
      setRevealedToken(token)
      setShowCreate(false)
      setTokens((current) => personalAccessTokens([summary, ...current]))
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
          <p className="mt-1 text-[12px] leading-5 text-kumo-subtle">Connect the Verglas CLI, an SDK, or a local script without using your account password.</p>
        </div>
        {!showCreate && <Button variant="primary" size="sm" onClick={() => { setRevealedToken(null); setShowCreate(true) }}><Key size={14} /> Create token</Button>}
      </div>

      {showCreate && <div className="overflow-hidden rounded-xl border border-kumo-line bg-kumo-base">
        <div className="border-b border-kumo-line px-5 py-4">
          <div className="flex items-start justify-between gap-4">
            <div><h3 className="text-[14px] font-medium text-kumo-default">Create a token</h3><p className="mt-0.5 text-[12px] text-kumo-subtle">Choose what this device can access. You can revoke it at any time.</p></div>
            <Button variant="ghost" size="sm" onClick={() => setShowCreate(false)}>Cancel</Button>
          </div>
        </div>
        <div className="space-y-5 p-5">
          <label className="block text-[12px] font-medium text-kumo-subtle">
            Name this device or script
            <input value={name} onChange={(event) => setName(event.target.value)} className={`mt-1.5 ${INPUT}`} placeholder="My laptop" />
          </label>

          {!advanced && <>
            <label className="block text-[12px] font-medium text-kumo-subtle">
              Access to
              <select value={simpleResourceId} onChange={(event) => setSimpleResourceId(event.target.value)} className={`mt-1.5 ${INPUT}`}>
                <option value="">Choose a database…</option>
                {simpleResources.map((resource) => (
                  <option key={resource.id} value={resource.id}>
                    {resource.kind === 'tenant' ? 'All databases in this tenant' : resource.id.replace(/^database\//, '')}
                  </option>
                ))}
              </select>
              <span className="mt-1 block font-normal leading-5 text-kumo-inactive">Database access automatically includes its tables.</span>
            </label>

            <fieldset>
              <legend className="text-[12px] font-medium text-kumo-subtle">Permission</legend>
              <div className="mt-2 grid gap-2 sm:grid-cols-2">
                <PermissionChoice
                  checked={accessLevel === 'read'}
                  title="Read only"
                  description="List, query, and connect to data."
                  onChange={() => setAccessLevel('read')}
                />
                <PermissionChoice
                  checked={accessLevel === 'write'}
                  title="Read and write"
                  description="Also create, append, update, and delete data."
                  onChange={() => setAccessLevel('write')}
                />
              </div>
            </fieldset>
          </>}

          <label className="block text-[12px] font-medium text-kumo-subtle">
            Expires
            <select value={expiration} onChange={(event) => setExpiration(Number(event.target.value))} className={`mt-1.5 ${INPUT}`}>
              {EXPIRATIONS.map((option) => <option key={option.seconds} value={option.seconds}>{option.label}</option>)}
            </select>
            <span className="mt-1 block font-normal leading-5 text-kumo-inactive">One year is the maximum. Expiring tokens limit the damage from forgotten or leaked credentials.</span>
          </label>

          <details open={advanced} onToggle={(event) => setAdvanced(event.currentTarget.open)} className="rounded-lg border border-kumo-line bg-kumo-elevated">
            <summary className="cursor-pointer px-3 py-2.5 text-[12px] font-medium text-kumo-subtle">Custom permissions</summary>
            <div className="space-y-4 border-t border-kumo-line p-4">
              <div>
                <p className="text-[12px] font-medium text-kumo-subtle">Resources</p>
                <div className="mt-2 max-h-48 overflow-y-auto rounded-lg border border-kumo-line bg-kumo-base p-1.5">
                  {resourceTree.map(({ resource, depth }) => {
                    const selected = resourceIds.includes(resource.id)
                    return <label key={resource.id} style={{ paddingLeft: `${8 + depth * 16}px` }} className={`flex cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 text-xs transition ${selected ? 'bg-kumo-brand/10 text-kumo-brand' : 'text-kumo-default hover:bg-kumo-tint'}`}><input checked={selected} onChange={() => toggleResource(resource.id)} type="checkbox" className="accent-[var(--color-kumo-brand)]" /><span className="min-w-0 flex-1 truncate font-mono">{resource.id}</span><span className="text-[10px] uppercase tracking-wide text-kumo-inactive">{resource.kind.replaceAll('_', ' ')}</span></label>
                  })}
                  {resourceTree.length === 0 && <p className="px-3 py-4 text-center text-xs text-kumo-subtle">No resources are currently delegable.</p>}
                </div>
              </div>
              <div>
                <p className="text-[12px] font-medium text-kumo-subtle">Capabilities</p>
                <div className="mt-2 grid gap-2 sm:grid-cols-2">{ACTIONS.map((action) => { const selected = actions.includes(action); return <label key={action} className={`flex cursor-pointer gap-2.5 rounded-lg border p-3 transition ${selected ? 'border-kumo-brand bg-kumo-brand/5' : 'border-kumo-line bg-kumo-base hover:bg-kumo-tint'}`}><input type="checkbox" checked={selected} onChange={() => toggleAction(action)} className="mt-0.5 accent-[var(--color-kumo-brand)]" /><span><span className="block text-[12px] font-medium text-kumo-default">{formatAccessAction(action)}</span><span className="mt-0.5 block text-[11px] leading-4 text-kumo-subtle">{describeAccessAction(action)}</span></span></label> })}</div>
              </div>
            </div>
          </details>

          <div className="flex items-center justify-between gap-4 border-t border-kumo-line pt-4">
            <p className="text-[11px] leading-4 text-kumo-inactive">The token is shown once after creation.</p>
            <Button variant="primary" size="sm" loading={creating} disabled={!canCreate} onClick={create}><Key size={14} /> Create token</Button>
          </div>
        </div>
      </div>}

      {revealedToken && <div className="rounded-xl border border-kumo-success/40 bg-kumo-success/10 p-4"><div className="flex gap-3"><CheckCircle size={19} weight="fill" className="mt-0.5 shrink-0 text-kumo-success" /><div className="min-w-0 flex-1"><p className="text-sm font-medium text-kumo-default">Your token is ready</p><p className="mt-0.5 text-xs leading-5 text-kumo-subtle">Copy it now. Verglas cannot show it again after you leave this page.</p><div className="mt-3 flex gap-2"><code className="min-w-0 flex-1 overflow-x-auto rounded-md border border-kumo-line bg-kumo-base px-3 py-2 text-xs text-kumo-default">{revealedToken}</code><Button variant="secondary" size="sm" onClick={() => void copyToken()}><Copy size={14} /> Copy token</Button></div><p className="mt-2 text-[11px] text-kumo-inactive">Use it as the <code className="font-mono">VERGLAS_TOKEN</code> environment variable.</p></div></div></div>}

      <div className="overflow-hidden rounded-xl border border-kumo-line bg-kumo-base">
        <div className="flex items-center gap-2 border-b border-kumo-line px-5 py-4"><ShieldCheck size={17} className="text-kumo-brand" /><div><h3 className="text-[14px] font-medium text-kumo-default">Your tokens</h3><p className="mt-0.5 text-[12px] text-kumo-subtle">Tokens you created for devices and scripts. Browser login sessions are not shown here.</p></div></div>
        {loading ? <p className="px-5 py-8 text-center text-sm text-kumo-subtle">Loading tokens…</p> : tokens.length === 0 ? <div className="px-5 py-8 text-center"><Key size={22} className="mx-auto text-kumo-inactive" /><p className="mt-2 text-sm text-kumo-subtle">You have not created any tokens.</p></div> : <div className="divide-y divide-kumo-line">{tokens.map((token) => { const active = !token.revokedAt && token.expiresAt * 1_000 > Date.now(); return <div key={token.id} className="flex items-center gap-3 px-5 py-3.5"><div className={`grid h-8 w-8 shrink-0 place-items-center rounded-lg ${active ? 'bg-kumo-tint text-kumo-brand' : 'bg-kumo-fill text-kumo-inactive'}`}><Key size={15} /></div><div className="min-w-0 flex-1"><p className="truncate text-sm font-medium text-kumo-default">{token.name}</p><p className="mt-0.5 text-xs text-kumo-subtle">{active ? `Expires ${new Date(token.expiresAt * 1_000).toLocaleDateString()}` : token.revokedAt ? 'Revoked' : 'Expired'}{token.lastUsedAt ? ` · Last used ${new Date(token.lastUsedAt * 1_000).toLocaleDateString()}` : ''}</p></div>{active && <Button variant="ghost" size="sm" onClick={() => void revoke(token)}><Trash size={14} /> Revoke</Button>}</div> })}</div>}
      </div>
    </section>
  )
}

/** One plain-language permission option in the default token flow. */
function PermissionChoice({
  checked,
  title,
  description,
  onChange,
}: {
  checked: boolean
  title: string
  description: string
  onChange: () => void
}) {
  return (
    <label className={`flex cursor-pointer gap-3 rounded-lg border p-3 transition ${checked ? 'border-kumo-brand bg-kumo-brand/5' : 'border-kumo-line hover:bg-kumo-tint'}`}>
      <input type="radio" name="token-permission" checked={checked} onChange={onChange} className="mt-0.5 accent-[var(--color-kumo-brand)]" />
      <span><span className="block text-[13px] font-medium text-kumo-default">{title}</span><span className="mt-0.5 block text-[11px] leading-4 text-kumo-subtle">{description}</span></span>
    </label>
  )
}
