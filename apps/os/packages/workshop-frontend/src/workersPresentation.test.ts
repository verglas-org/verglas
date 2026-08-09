import { describe, expect, it } from 'vitest'
import type { VerglasWorkerSummary } from '@verglas/workshop-shared/api'
import { summarizeWorkers, workerLifecycleLabel, workerScheduleSummary } from './workersPresentation'

const worker = (overrides: Partial<VerglasWorkerSummary> = {}): VerglasWorkerSummary => ({
  name: 'daily-orders', state: 'running', placement: 'scheduler', output: 'lake.orders',
  triggers: '[{"type":"cron","schedule":"0 9 * * *"}]', createdBy: 'alex', revision: 4,
  createdAt: '2026-08-09T10:00:00.000Z', ...overrides,
})

describe('workers presentation', () => {
  it('does not present worker lifecycle as a running job', () => {
    expect(workerLifecycleLabel('running')).toBe('Active')
    expect(workerLifecycleLabel('paused')).toBe('Disabled')
    expect(workerLifecycleLabel('archived')).toBe('Disabled')
  })

  it('renders portable trigger declarations as a concise schedule', () => {
    expect(workerScheduleSummary(worker())).toEqual({ label: 'Cron · 0 9 * * *', kind: 'scheduled' })
    expect(workerScheduleSummary(worker({ triggers: 'invalid' }))).toEqual({ label: 'Invalid trigger declaration', kind: 'unknown' })
  })

  it('derives operational totals from workers and their bounded history', () => {
    expect(summarizeWorkers([
      worker({ activeRun: true, recentRuns: [{ jobId: '1', state: 'running', createdAt: 'now' }] }),
      worker({ name: 'hourly', triggers: '[]', recentRuns: [{ jobId: '2', state: 'failed', createdAt: 'now' }] }),
    ])).toEqual({ total: 2, active: 1, scheduled: 1, failed: 1 })
  })
})
