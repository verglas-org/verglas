import { describe, expect, it } from 'vitest'
import type { VerglasCatalogSnapshot } from '@verglas/workshop-shared/api'
import { databaseGroups, databaseMetrics } from './routes/data'

const catalog: VerglasCatalogSnapshot = {
  databases: [],
  tables: [
    { namespace: ['analytics'], name: 'events', qualifiedName: '"analytics"."events"' },
    { namespace: ['analytics'], name: 'sessions', qualifiedName: '"analytics"."sessions"' },
    { namespace: [], name: 'notes', qualifiedName: '"notes"' },
  ],
  vectors: [{ target: 'tbl:analytics.events', field: 'embedding', metric: 'cosine', liveCount: 42 }],
  graphs: [{ namespace: 'analytics', nodesTable: '"analytics"."nodes"', edgesTable: '"analytics"."edges"' }],
}

describe('database catalog presentation', () => {
  it('keeps an empty database returned by the catalog selectable', () => {
    const emptyCatalog: VerglasCatalogSnapshot = {...catalog, databases: [{name: 'staging', tableCount: 0, vectorCount: 0, graph: false}]}

    expect(databaseGroups(emptyCatalog).find((database) => database.name === 'staging')).toMatchObject({
      namespace: ['staging'],
      tables: [],
      vectors: [],
      graphs: [],
    })
  })

  it('groups catalog entries by database namespace and keeps the default database', () => {
    expect(databaseGroups(catalog).map((database) => ({name: database.name, tables: database.tables.length, vectors: database.vectors.length, graphs: database.graphs.length}))).toEqual([
      {name: 'analytics', tables: 2, vectors: 1, graphs: 1},
      {name: 'default', tables: 1, vectors: 0, graphs: 0},
    ])
  })

  it('uses reported vector count but does not invent storage or usage metrics', () => {
    expect(databaseMetrics(databaseGroups(catalog)[0]).at(-1)).toEqual({label: 'Usage', value: 'Not reported', reported: false})
  })

  it('does not misrepresent missing table measurements as zero', () => {
    const database = databaseGroups(catalog)[0]
    const detail = {name: 'analytics', tableCount: 2, vectorCount: 1, graph: true, tables: database.tables}

    expect(databaseMetrics(database, detail).slice(-2)).toEqual([
      {label: 'Storage', value: 'Not reported', reported: false},
      {label: 'Usage', value: 'Not reported', reported: false},
    ])
  })
})
