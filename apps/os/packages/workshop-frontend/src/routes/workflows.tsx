import { createFileRoute } from '@tanstack/react-router'
import { FlowArrow, Pause, Play, Trash } from '@phosphor-icons/react'
import { useCallback, useEffect, useState } from 'react'
import type { VerglasWorkerDetail, VerglasWorkerRunSummary, VerglasWorkerSummary } from '@verglas/workshop-shared/api'
import { useAuthenticatedApi } from '../AuthContext'
import DeleteConfirmationDialog from '../components/DeleteConfirmationDialog'
import {
  CatalogDetailCard,
  CatalogEmpty,
  CatalogError,
  CatalogPage,
  CatalogStatus,
  CatalogTable,
} from '../components/CatalogTable'
import { RunHistoryDots } from '../components/RunHistoryDots'
import { useDocumentTitle } from '../useDocumentTitle'

export const Route = createFileRoute('/workflows')({ component: JobsPage })

function triggerLabels(raw: string): string[] {
  try {
    const triggers = JSON.parse(raw) as Array<{type?: string; schedule?: string; path?: string; eventType?: string}>
    return triggers.map((trigger) => {
      if (trigger.type === 'cron') return trigger.schedule ? `Schedule · ${trigger.schedule}` : 'Schedule'
      if (trigger.type === 'webhook') return trigger.path ? `Webhook · ${trigger.path}` : 'Webhook'
      if (trigger.type === 'event') return trigger.eventType ? `Event · ${trigger.eventType}` : 'Event'
      return trigger.type || 'Manual'
    })
  } catch {
    return ['Invalid trigger declaration']
  }
}

function JobsPage() {
  useDocumentTitle('Jobs')
  const { authenticatedApi } = useAuthenticatedApi()
  const [workers, setWorkers] = useState<VerglasWorkerSummary[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [detail, setDetail] = useState<VerglasWorkerDetail | null>(null)
  const [selectedRun, setSelectedRun] = useState<VerglasWorkerRunSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [detailLoading, setDetailLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      setWorkers(await authenticatedApi.listVerglasWorkers())
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [authenticatedApi])

  const loadDetail = useCallback(async (name: string) => {
    setDetailLoading(true)
    setError(null)
    try {
      const next = await authenticatedApi.getVerglasWorker(name)
      setDetail(next)
      setSelectedRun((current) => {
        if (!current) return next.recentRuns?.[0] ?? null
        return next.recentRuns?.find((run) => run.jobId === current.jobId) ?? next.recentRuns?.[0] ?? null
      })
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setDetailLoading(false)
    }
  }, [authenticatedApi])

  useEffect(() => { void load() }, [load])

  useEffect(() => {
    if (!selected) {
      setDetail(null)
      setSelectedRun(null)
      return
    }
    void loadDetail(selected)
  }, [selected, loadDetail])

  useEffect(() => {
    if (!selected || !detail?.activeRun) return
    const id = window.setInterval(() => { void loadDetail(selected) }, 5000)
    return () => window.clearInterval(id)
  }, [selected, detail?.activeRun, loadDetail])

  const open = (name: string) => setSelected(name)
  const close = () => setSelected(null)

  const runNow = async () => {
    if (!selected) return
    setBusy(true)
    setError(null)
    try {
      await authenticatedApi.runVerglasWorker(selected)
      await loadDetail(selected)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  const togglePause = async () => {
    if (!selected || !detail) return
    setBusy(true)
    setError(null)
    try {
      const next = detail.state === 'paused' ? 'running' : 'paused'
      await authenticatedApi.setVerglasWorkerState(selected, next)
      await loadDetail(selected)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  const remove = async () => {
    if (!confirmDelete) return
    setBusy(true)
    setError(null)
    try {
      await authenticatedApi.setVerglasWorkerState(confirmDelete, 'archived')
      setConfirmDelete(null)
      if (selected === confirmDelete) close()
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  const worker = detail
  const triggers = worker ? triggerLabels(worker.triggers) : []

  return (
    <CatalogPage title="Jobs" description="Workers that ingest, transform, or act on lakehouse data." onRefresh={() => void load()}>
      {error && <CatalogError message={error} />}
      {selected ? (
        <CatalogDetailCard
          open
          title={worker?.name ?? selected}
          subtitle={worker?.output || 'No output declared'}
          meta={
            <div className="flex flex-wrap items-center gap-2">
              {worker && <CatalogStatus value={worker.state} good={worker.state === 'running'} />}
              {worker?.activeRun && (
                <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-semibold uppercase text-amber-600">
                  Live
                </span>
              )}
            </div>
          }
          onBack={close}
          footer={
            <>
              <button
                type="button"
                onClick={() => setConfirmDelete(selected)}
                className="mr-auto inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg border border-kumo-line px-3 text-[13px] text-kumo-subtle hover:border-kumo-danger/40 hover:bg-kumo-danger-tint hover:text-kumo-danger"
              >
                <Trash size={14} />
                Delete
              </button>
              <button
                type="button"
                disabled={busy || !worker}
                onClick={() => void togglePause()}
                className="inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg border border-kumo-line px-3 text-[13px] text-kumo-default disabled:opacity-40"
              >
                {worker?.state === 'paused' ? <Play size={14} /> : <Pause size={14} />}
                {worker?.state === 'paused' ? 'Resume' : 'Pause'}
              </button>
              <button
                type="button"
                disabled={busy || !worker || worker.state === 'paused'}
                onClick={() => void runNow()}
                className="inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg bg-kumo-brand px-3 text-[13px] font-medium text-white disabled:opacity-40"
              >
                <Play size={14} />
                Run now
              </button>
            </>
          }
        >
          {detailLoading && !worker ? (
            <div className="py-10 text-center text-sm text-kumo-subtle">Loading job…</div>
          ) : worker ? (
            <div className="space-y-6">
              <section>
                <h3 className="mb-2 text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">Schedule</h3>
                <div className="flex flex-wrap gap-1.5">
                  {triggers.map((trigger, index) => (
                    <span key={`${trigger}-${index}`} className="rounded-md bg-kumo-tint px-2 py-1 text-[11px] text-kumo-subtle">
                      {trigger}
                    </span>
                  ))}
                </div>
                <dl className="mt-4 grid gap-3 text-[13px] sm:grid-cols-3">
                  <div>
                    <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">Revision</dt>
                    <dd className="mt-1 text-kumo-default">{worker.revision}</dd>
                  </div>
                  <div>
                    <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">Placement</dt>
                    <dd className="mt-1 text-kumo-default">{worker.placement}</dd>
                  </div>
                  <div>
                    <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">Output</dt>
                    <dd className="mt-1 break-all font-mono text-[12px] text-kumo-subtle">{worker.output || '—'}</dd>
                  </div>
                </dl>
              </section>

              <section>
                <div className="mb-2 flex items-center justify-between gap-3">
                  <h3 className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">Runs</h3>
                  <RunHistoryDots
                    runs={worker.recentRuns}
                    selectedJobId={selectedRun?.jobId}
                    onSelect={setSelectedRun}
                  />
                </div>
                {selectedRun ? (
                  <div className="rounded-xl border border-kumo-line bg-kumo-tint/30 px-4 py-3 text-[13px]">
                    <div className="flex flex-wrap items-center gap-2">
                      <CatalogStatus
                        value={selectedRun.state}
                        good={selectedRun.state === 'succeeded'}
                      />
                      <span className="font-mono text-[11px] text-kumo-inactive">{selectedRun.jobId.slice(0, 16)}…</span>
                    </div>
                    <div className="mt-2 text-kumo-subtle">
                      Started {new Date(selectedRun.createdAt).toLocaleString()}
                      {selectedRun.completedAt && ` · Finished ${new Date(selectedRun.completedAt).toLocaleString()}`}
                    </div>
                    <div className="mt-3">
                      <div className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">Result</div>
                      <pre className="mt-1 whitespace-pre-wrap break-words font-mono text-[12px] text-kumo-default">
                        {selectedRun.errorMessage
                          || (selectedRun.rowsProduced != null
                            ? `Succeeded — ${selectedRun.rowsProduced} rows written`
                            : selectedRun.state)}
                      </pre>
                    </div>
                  </div>
                ) : (
                  <p className="text-sm text-kumo-subtle">No runs recorded for this worker yet.</p>
                )}
              </section>

              <section>
                <h3 className="mb-2 text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">Code</h3>
                {worker.sourceCode ? (
                  <pre className="max-h-[28rem] overflow-auto rounded-xl border border-kumo-line bg-kumo-base p-4 font-mono text-[12px] leading-5 text-kumo-default">
                    {worker.sourceCode}
                  </pre>
                ) : (
                  <p className="text-sm text-kumo-subtle">No TypeScript source was bundled in this worker revision.</p>
                )}
              </section>
            </div>
          ) : null}
        </CatalogDetailCard>
      ) : loading ? (
        <CatalogEmpty>Loading Verglas workers…</CatalogEmpty>
      ) : (
        <CatalogTable
          empty="No workers are registered in Verglas."
          cards={workers.map((entry) => ({
            id: entry.name,
            icon: <FlowArrow size={18} />,
            primary: entry.name,
            secondary: entry.output || 'No output declared',
            tertiary: triggerLabels(entry.triggers).join(' · '),
            meta: (
              <div className="flex flex-col items-end gap-1.5">
                <div className="flex items-center gap-1.5">
                  {entry.activeRun && (
                    <span className="rounded-full bg-amber-500/15 px-1.5 py-0.5 text-[9px] font-semibold uppercase text-amber-600">
                      Live
                    </span>
                  )}
                  <CatalogStatus value={entry.state} good={entry.state === 'running'} />
                </div>
                <RunHistoryDots runs={entry.recentRuns} />
              </div>
            ),
            onOpen: () => open(entry.name),
          }))}
        />
      )}

      <DeleteConfirmationDialog
        open={confirmDelete !== null}
        title="Delete job"
        description={
          confirmDelete
            ? `Archive “${confirmDelete}” so it leaves the active worker list. Past runs remain in scheduler history.`
            : null
        }
        isDeleting={busy}
        onOpenChange={(open) => { if (!open) setConfirmDelete(null) }}
        onConfirm={() => void remove()}
      />
    </CatalogPage>
  )
}
