import { describe, expect, it } from 'vitest'
import type { VerglasCatalogSnapshot } from '@verglas/workshop-shared/api'
import { databaseAssets, namespaceGroups, workspacePromptForCatalogItem } from './routes/data'

const catalog: VerglasCatalogSnapshot = {
  databases: [{
    type: 'lakehouse',
    name: 'analytics',
    storage: {mode: 'managed'},
    catalog: {mode: 'managed-lakekeeper'},
    capabilities: {catalog: true, tableCrud: true, tableMetrics: false, vectors: false, graphs: true, query: true},
    tableCount: 2,
    vectorCount: 0,
    graphCount: 1,
  }, {
    type: 'postgres',
    name: 'operations',
    engine: {mode: 'managed-neon'},
    capabilities: {catalog: false, tableCrud: false, tableMetrics: false, vectors: false, graphs: false, query: false},
    tableCount: 0,
    vectorCount: 0,
    graphCount: 0,
  }],
  tables: [
    { database: 'analytics', namespace: ['events'], name: 'log', qualifiedName: '"events"."log"' },
    { database: 'analytics', namespace: ['knowledge'], name: 'nodes', qualifiedName: '"knowledge"."nodes"' },
  ],
  vectors: [],
  graphs: [{ database: 'analytics', namespace: 'knowledge', nodesTable: '"knowledge"."nodes"', edgesTable: '"knowledge"."edges"' }],
}

describe('dynamic database catalog presentation', () => {
  it('filters catalog assets by their top-level database resource', () => {
    expect(databaseAssets(catalog, 'analytics')).toMatchObject({tables: [{name: 'log'}, {name: 'nodes'}], graphs: [{namespace: 'knowledge'}], vectors: []})
    expect(databaseAssets(catalog, 'operations')).toEqual({tables: [], vectors: [], graphs: []})
  })

  it('groups Lakehouse tables into namespaces without promoting namespaces to databases', () => {
    expect(namespaceGroups(databaseAssets(catalog, 'analytics').tables)).toEqual([
      {name: 'events', namespace: ['events'], tables: [expect.objectContaining({name: 'log'})]},
      {name: 'knowledge', namespace: ['knowledge'], tables: [expect.objectContaining({name: 'nodes'})]},
    ])
  })

  it('carries the selected database and table into a query workspace prompt', () => {
    const table = catalog.tables[0]
    expect(workspacePromptForCatalogItem({kind: 'table', id: 'table', value: table}))
      .toContain('database `analytics`')
    expect(workspacePromptForCatalogItem({kind: 'table', id: 'table', value: table}))
      .toContain('`"events"."log"`')
  })
})
