import type { VerglasAccessAction, VerglasAccessResource } from '@verglas/workshop-shared/api'

const sortResources = (left: VerglasAccessResource, right: VerglasAccessResource) => left.id.localeCompare(right.id)

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
  return action.replaceAll('_', ' ').replace(/^./, (character) => character.toUpperCase())
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
