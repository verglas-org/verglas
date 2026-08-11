import { describe, expect, it } from 'vitest'
import {
  buildResourceTree,
  describeAccessAction,
  formatAccessAction,
  isTokenRequestComplete,
  personalAccessTokens,
  simpleTokenResources,
  toPresetTokenGrants,
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
    expect(formatAccessAction('create_child')).toBe('Create databases or tables')
    expect(describeAccessAction('connect')).toBe('Open a database or service connection. This does not grant data access by itself.')
    expect(describeAccessAction('use_secret')).toBe('Use a saved credential without revealing its value.')
  })

  it('applies selected actions to every selected resource when building a token request', () => {
    expect(toTokenGrants(['database:analytics', 'database:operations'], ['discover', 'query'])).toEqual([
      { resourceId: 'database:analytics', actions: ['discover', 'query'] },
      { resourceId: 'database:operations', actions: ['discover', 'query'] },
    ])
  })

  it('keeps OS login sessions out of the personal token inventory', () => {
    const tokens = [
      { id: 'session-record', name: 'OS session', principalId: 'session/browser', parentPrincipalId: 'user/dev@example.com', audience: 'access', createdAt: 1, expiresAt: 2 },
      { id: 'personal-record', name: 'Laptop', principalId: 'token/laptop', parentPrincipalId: 'user/dev@example.com', audience: 'verglas-cli', createdAt: 1, expiresAt: 2 },
    ]

    expect(personalAccessTokens(tokens)).toEqual([tokens[1]])
  })

  it('offers only meaningful inherited scopes in the simple resource picker', () => {
    const resources = [
      { tenantId: 'acme', id: 'table/analytics/events', kind: 'table' as const, parentId: 'database/analytics' },
      { tenantId: 'acme', id: 'database/analytics', kind: 'database' as const, parentId: 'tenant' },
      { tenantId: 'acme', id: 'job/nightly', kind: 'job' as const, parentId: 'tenant' },
      { tenantId: 'acme', id: 'tenant', kind: 'tenant' as const },
    ]

    expect(simpleTokenResources(resources).map(({ id }) => id)).toEqual([
      'database/analytics',
      'tenant',
    ])
  })

  it('turns plain-language access levels into bounded inherited grants', () => {
    expect(toPresetTokenGrants('database/analytics', 'read')).toEqual([{
      resourceId: 'database/analytics',
      actions: ['query', 'connect'],
    }])
    expect(toPresetTokenGrants('database/analytics', 'write')).toEqual([{
      resourceId: 'database/analytics',
      actions: ['modify', 'create_child', 'connect'],
    }])
    expect(toPresetTokenGrants('tenant', 'read')).toEqual([{
      resourceId: 'tenant',
      actions: ['query', 'connect'],
    }])
  })
})
