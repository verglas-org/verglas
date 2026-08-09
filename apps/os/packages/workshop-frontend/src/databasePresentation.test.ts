import { describe, expect, it } from 'vitest'
import {
  databaseCapabilityLabels,
  databaseKindLabel,
  databaseResourceDescription,
} from './databasePresentation'
import type { VerglasDatabaseDefinition, VerglasDatabaseSummary } from '@verglas/workshop-shared/api'

const managedLakehouse: VerglasDatabaseDefinition = {
  type: 'lakehouse',
  name: 'analytics',
  storage: {mode: 'managed'},
  catalog: {mode: 'managed-lakekeeper'},
}

const managedPostgres: VerglasDatabaseDefinition = {
  type: 'postgres',
  name: 'operations',
  engine: {mode: 'managed-neon'},
}

describe('dynamic database presentation', () => {
  it('presents top-level database engine kinds instead of treating namespaces as databases', () => {
    expect(databaseKindLabel(managedLakehouse)).toBe('Lakehouse')
    expect(databaseKindLabel(managedPostgres)).toBe('Postgres')
  })

  it('only advertises surfaces that the OS can operate for each engine', () => {
    const lakehouse: VerglasDatabaseSummary = {
      ...managedLakehouse,
      capabilities: {catalog: true, tableCrud: true, tableMetrics: false, vectors: false, graphs: true, query: true},
      tableCount: 2,
      vectorCount: 0,
      graphCount: 1,
    }
    const postgres: VerglasDatabaseSummary = {
      ...managedPostgres,
      capabilities: {catalog: false, tableCrud: false, tableMetrics: false, vectors: false, graphs: false, query: false},
      tableCount: 0,
      vectorCount: 0,
      graphCount: 0,
    }

    expect(databaseCapabilityLabels(lakehouse)).toEqual(['Catalog', 'Table CRUD', 'Graphs', 'SQL'])
    expect(databaseCapabilityLabels(postgres)).toEqual([])
  })

  it('describes the selected managed implementation without exposing secrets or fake metrics', () => {
    expect(databaseResourceDescription(managedLakehouse)).toBe('Managed storage · Managed Lakekeeper')
    expect(databaseResourceDescription(managedPostgres)).toBe('Managed Neon')
  })
})
