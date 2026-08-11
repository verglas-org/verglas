import type { VerglasAccessAction, VerglasAccessResource, VerglasAccessTokenSummary } from '@verglas/workshop-shared/api'

const sortResources = (left: VerglasAccessResource, right: VerglasAccessResource) => left.id.localeCompare(right.id)

const ACCESS_ACTION_COPY: Record<VerglasAccessAction, { label: string; description: string }> = {
  discover: { label: 'List resources', description: 'See that a resource exists in lists and search results.' },
  describe: { label: 'View details', description: 'Read names, schemas, configuration, and other metadata.' },
  query: { label: 'Read data', description: 'Query or read the contents of a database, table, or data resource.' },
  append: { label: 'Add data', description: 'Add new records without changing or deleting existing data.' },
  modify: { label: 'Edit data and settings', description: 'Read, add, change, or delete existing data and metadata.' },
  create_child: { label: 'Create databases or tables', description: 'Create a new resource inside the selected tenant or database.' },
  execute: { label: 'Run jobs or integrations', description: 'Start a Worker, Job, Integration operation, or other callable resource.' },
  use_secret: { label: 'Use saved credentials', description: 'Use a saved credential without revealing its value.' },
  deploy: { label: 'Deploy applications', description: 'Deploy or update an Application, Worker, or other executable resource.' },
  connect: { label: 'Connect to databases', description: 'Open a database or service connection. This does not grant data access by itself.' },
  pass_grants: { label: 'Delegate existing access', description: 'Pass along permissions already held on the selected resource.' },
  manage_grants: { label: 'Manage access', description: 'Add or remove permissions on the selected resource.' },
  own: { label: 'Own resource', description: 'Perform every operation available on the selected resource.' },
}

/** A resource with the depth at which it belongs in the authorization tree. */
export type AccessResourceTreeNode = {
  resource: VerglasAccessResource
  depth: number
}

/** The minimum information required before a user can mint a scoped access token. */
export type AccessTokenDraft = {
  name: string
  expiresAt: Date | null
  resourceIds: string[]
  actions: VerglasAccessAction[]
}

/** Plain-language permission levels offered by the default token flow. */
export type TokenAccessLevel = 'read' | 'write'

/** Keeps short-lived login sessions and internal credentials out of the personal-token list. */
export function personalAccessTokens(tokens: VerglasAccessTokenSummary[]): VerglasAccessTokenSummary[] {
  return tokens.filter(({ principalId }) => principalId.startsWith('token/'))
}

/** Returns scopes whose inherited permissions are useful without understanding the resource graph. */
export function simpleTokenResources(resources: VerglasAccessResource[]): VerglasAccessResource[] {
  return resources
    .filter(({ kind }) => kind === 'database' || kind === 'tenant')
    .toSorted(sortResources)
}

/** Converts one simple access level into the minimum inherited database/tenant grant. */
export function toPresetTokenGrants(resourceId: string, level: TokenAccessLevel): {
  resourceId: string
  actions: VerglasAccessAction[]
}[] {
  return [{
    resourceId,
    actions: level === 'read'
      ? ['query', 'connect']
      : ['modify', 'create_child', 'connect'],
  }]
}

/** Returns a stable, depth-first resource tree while retaining malformed/orphaned resources. */
export function buildResourceTree(resources: VerglasAccessResource[]): AccessResourceTreeNode[] {
  const children = new Map<string, VerglasAccessResource[]>()
  const ids = new Set(resources.map((resource) => resource.id))
  const roots: VerglasAccessResource[] = []

  for (const resource of resources) {
    if (!resource.parentId || !ids.has(resource.parentId)) {
      roots.push(resource)
      continue
    }
    const siblings = children.get(resource.parentId) ?? []
    siblings.push(resource)
    children.set(resource.parentId, siblings)
  }

  const result: AccessResourceTreeNode[] = []
  const visited = new Set<string>()
  const visit = (resource: VerglasAccessResource, depth: number) => {
    if (visited.has(resource.id)) return
    visited.add(resource.id)
    result.push({ resource, depth })
    for (const child of (children.get(resource.id) ?? []).toSorted(sortResources)) visit(child, depth + 1)
  }
  for (const root of roots.toSorted(sortResources)) visit(root, 0)
  for (const resource of resources.toSorted(sortResources)) visit(resource, 0)
  return result
}

/** Converts a stable RBAC action identifier into a readable control label. */
export function formatAccessAction(action: VerglasAccessAction): string {
  return ACCESS_ACTION_COPY[action].label
}

/** Explains the concrete capability represented by one authorization action. */
export function describeAccessAction(action: VerglasAccessAction): string {
  return ACCESS_ACTION_COPY[action].description
}

/** Ensures token creation has a bounded scope and an expiration after the current time. */
export function isTokenRequestComplete(draft: AccessTokenDraft, now = new Date()): boolean {
  return draft.name.trim().length > 0
    && draft.expiresAt !== null
    && draft.expiresAt > now
    && draft.resourceIds.length > 0
    && draft.actions.length > 0
}

/** Expands a compact UI selection into one least-privilege grant per protected resource. */
export function toTokenGrants(resourceIds: string[], actions: VerglasAccessAction[]): {
  resourceId: string
  actions: VerglasAccessAction[]
}[] {
  return resourceIds.map((resourceId) => ({ resourceId, actions: [...actions] }))
}
