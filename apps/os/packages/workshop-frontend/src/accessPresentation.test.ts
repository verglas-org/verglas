import { describe, expect, it } from 'vitest'
import {
  buildResourceTree,
  formatAccessAction,
  isTokenRequestComplete,
  toTokenGrants,
} from './accessPresentation'

describe('access presentation', () => {
  it('orders database children underneath their database regardless of API order', () => {
    const tree = buildResourceTree([
      { tenantId: 'acme', id: 'table:events', kind: 'table', parentId: 'database:analytics' },
      { tenantId: 'acme', id: 'database:operations', kind: 'database', parentId: 'tenant:acme' },
      { tenantId: 'acme', id: 'tenant:acme', kind: 'tenant' },
      { tenantId: 'acme', id: 'database:analytics', kind: 'database', parentId: 'tenant:acme' },
      { tenantId: 'acme', id: 'namespace:market', kind: 'namespace', parentId: 'database:analytics' },
    ])

    expect(tree.map((node) => `${node.depth}:${node.resource.id}`)).toEqual([
      '0:tenant:acme',
      '1:database:analytics',
      '2:namespace:market',
      '2:table:events',
      '1:database:operations',
    ])
  })

  it('keeps orphaned resources visible instead of dropping an invalid hierarchy', () => {
    const tree = buildResourceTree([
      { tenantId: 'acme', id: 'table:orphan', kind: 'table', parentId: 'database:gone' },
    ])

    expect(tree).toEqual([{ resource: { tenantId: 'acme', id: 'table:orphan', kind: 'table', parentId: 'database:gone' }, depth: 0 }])
  })

  it('retains cyclic resource records without recursing forever', () => {
    const tree = buildResourceTree([
      { tenantId: 'acme', id: 'namespace:a', kind: 'namespace', parentId: 'namespace:b' },
      { tenantId: 'acme', id: 'namespace:b', kind: 'namespace', parentId: 'namespace:a' },
    ])

    expect(tree.map(({ resource }) => resource.id)).toEqual(['namespace:a', 'namespace:b'])
  })

  it('requires a name, future expiration, resource, and action before creating a token', () => {
    const future = new Date('2030-01-02T00:00:00.000Z')
    const now = new Date('2030-01-01T00:00:00.000Z')

    expect(isTokenRequestComplete({ name: '', expiresAt: future, resourceIds: ['database:analytics'], actions: ['query'] }, now)).toBe(false)
    expect(isTokenRequestComplete({ name: 'cli', expiresAt: now, resourceIds: ['database:analytics'], actions: ['query'] }, now)).toBe(false)
    expect(isTokenRequestComplete({ name: 'cli', expiresAt: future, resourceIds: [], actions: ['query'] }, now)).toBe(false)
    expect(isTokenRequestComplete({ name: 'cli', expiresAt: future, resourceIds: ['database:analytics'], actions: [] }, now)).toBe(false)
    expect(isTokenRequestComplete({ name: 'cli', expiresAt: future, resourceIds: ['database:analytics'], actions: ['query'] }, now)).toBe(true)
  })

  it('formats action identifiers for UI labels', () => {
    expect(formatAccessAction('create_child')).toBe('Create child')
  })

  it('applies selected actions to every selected resource when building a token request', () => {
    expect(toTokenGrants(['database:analytics', 'database:operations'], ['discover', 'query'])).toEqual([
      { resourceId: 'database:analytics', actions: ['discover', 'query'] },
      { resourceId: 'database:operations', actions: ['discover', 'query'] },
    ])
  })
})
