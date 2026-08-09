import type { VerglasWorkerRunSummary } from '@verglas/workshop-shared/api'

/** Airflow-style last-N run status circles (newest on the right). */
export function RunHistoryDots({
  runs,
  limit = 12,
  onSelect,
  selectedJobId,
}: {
  runs?: VerglasWorkerRunSummary[]
  limit?: number
  onSelect?: (run: VerglasWorkerRunSummary) => void
  selectedJobId?: string | null
}) {
  const slice = (runs ?? []).slice(0, limit).toReversed()
  if (slice.length === 0) {
    return <span className="text-[11px] text-kumo-inactive">No runs yet</span>
  }

  return (
    <div className="flex items-center gap-1" aria-label="Recent run history">
      {slice.map((run) => {
        const color =
          run.state === 'succeeded' ? 'bg-kumo-success' :
          run.state === 'failed' ? 'bg-kumo-danger' :
          run.state === 'running' || run.state === 'pending' || run.state === 'retryable'
            ? 'bg-amber-500' : 'bg-kumo-inactive'
        const selected = selectedJobId === run.jobId
        const title = [
          run.state,
          run.completedAt ?? run.createdAt,
          run.rowsProduced != null ? `${run.rowsProduced} rows` : null,
          run.errorMessage,
        ].filter(Boolean).join(' · ')
        return (
          <button
            key={run.jobId}
            type="button"
            title={title}
            disabled={!onSelect}
            onClick={(event) => {
              event.stopPropagation()
              onSelect?.(run)
            }}
            className={`h-2.5 w-2.5 rounded-full ${color} ${
              onSelect ? 'cursor-pointer hover:ring-2 hover:ring-kumo-brand/40' : 'cursor-default'
            } ${selected ? 'ring-2 ring-kumo-brand ring-offset-1 ring-offset-kumo-base' : ''}`}
          />
        )
      })}
    </div>
  )
}
