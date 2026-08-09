import type { VerglasDatabaseDefinition, VerglasDatabaseSummary } from '@verglas/workshop-shared/api'

/** Returns the concise engine name used throughout database management. */
export function databaseKindLabel(database: VerglasDatabaseDefinition): 'Lakehouse' | 'Postgres' {
  return database.type === 'lakehouse' ? 'Lakehouse' : 'Postgres'
}

/** Lists only database surfaces currently exposed by the OS control plane. */
export function databaseCapabilityLabels(database: VerglasDatabaseSummary): string[] {
  const capabilities: string[] = []
  if (database.capabilities.catalog) capabilities.push('Catalog')
  if (database.capabilities.tableCrud) capabilities.push('Table CRUD')
  if (database.capabilities.tableMetrics) capabilities.push('Metrics')
  if (database.capabilities.vectors) capabilities.push('Vectors')
  if (database.capabilities.graphs) capabilities.push('Graphs')
  return capabilities
}

/** Describes the selected backing services without including credentials or internal IDs. */
export function databaseResourceDescription(database: VerglasDatabaseDefinition): string {
  if (database.type === 'postgres') return 'Managed Neon'
  const storage = database.storage.mode === 'managed' ? 'Managed storage' : database.storage.dataPath
  const catalog = database.catalog.mode === 'managed-lakekeeper'
    ? 'Managed Lakekeeper'
    : `${database.catalog.uri} · ${database.catalog.warehouse}`
  return `${storage} · ${catalog}`
}
