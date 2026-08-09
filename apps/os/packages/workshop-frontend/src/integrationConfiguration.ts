import type { VerglasIntegrationConfigurationField } from '@verglas/workshop-shared/api'

export type IntegrationFieldGroup = {
  id: 'authentication' | 'settings'
  title: string
  fields: VerglasIntegrationConfigurationField[]
}

/** Splits the generic Vessel schema into compact settings-page sections. */
export function groupIntegrationFields(fields: VerglasIntegrationConfigurationField[]): IntegrationFieldGroup[] {
  const authentication = fields.filter((field) => field.secret || field.type === 'password')
  const settings = fields.filter((field) => !authentication.includes(field))

  return [
    ...(authentication.length ? [{ id: 'authentication' as const, title: 'Authentication', fields: authentication }] : []),
    ...(settings.length ? [{ id: 'settings' as const, title: 'Connection settings', fields: settings }] : []),
  ]
}

/** Returns labels for values that must be supplied before a new connection can be verified. */
export function missingRequiredIntegrationFields(
  fields: VerglasIntegrationConfigurationField[],
  values: Record<string, string>,
  configured: boolean,
): string[] {
  if (configured) return []
  return fields
    .filter((field) => field.required && !values[field.name]?.trim())
    .map((field) => field.label)
}
