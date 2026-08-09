import { describe, expect, it } from 'vitest'
import type { VerglasIntegrationConfigurationField } from '@verglas/workshop-shared/api'
import { groupIntegrationFields, missingRequiredIntegrationFields } from './integrationConfiguration'

const fields: VerglasIntegrationConfigurationField[] = [
  { name: 'apiKey', label: 'API key', type: 'text', required: true, secret: true },
  { name: 'endpoint', label: 'Endpoint', type: 'url', required: true, secret: false },
  { name: 'project', label: 'Project', type: 'text', required: false, secret: false },
]

describe('integration configuration presentation', () => {
  it('groups secret fields as authentication and leaves connection settings together', () => {
    expect(groupIntegrationFields(fields)).toEqual([
      { id: 'authentication', title: 'Authentication', fields: [fields[0]] },
      { id: 'settings', title: 'Connection settings', fields: [fields[1], fields[2]] },
    ])
  })

  it('requires values for a new connection but preserves configured secrets', () => {
    expect(missingRequiredIntegrationFields(fields, { endpoint: 'https://api.example.com' }, false))
      .toEqual(['API key'])
    expect(missingRequiredIntegrationFields(fields, {}, true)).toEqual([])
  })
})
