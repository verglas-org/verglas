import { Link } from '@tanstack/react-router'
import { CalendarDots, CaretRight, Clock, FlowArrow, Lightning, Plus, WarningCircle } from '@phosphor-icons/react'
import type { VerglasWorkerSummary } from '@verglas/workshop-shared/api'
import { useMemo } from 'react'
import type { ReactNode } from 'react'
import { CatalogStatus } from './CatalogTable'
import { summarizeWorkers, workerLifecycleLabel, workerScheduleSummary } from '../workersPresentation'

function time(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? 'Unknown time' : date.toLocaleString()
}

function runClass(state: string) {
  if (state === 'succeeded') return 'bg-kumo-success-tint text-kumo-success'
  if (state === 'failed') return 'bg-kumo-danger-tint text-kumo-danger'
  if (state === 'running') return 'bg-amber-500/15 text-amber-600'
  return 'bg-kumo-tint text-kumo-subtle'
}

function Metric({ label, value, icon, danger }: { label: string; value: number; icon: ReactNode; danger?: boolean }) {
  return <div className="rounded-xl border border-kumo-line bg-kumo-base p-4"><div className="flex items-center justify-between text-[11px] font-medium uppercase tracking-wide text-kumo-inactive"><span>{label}</span><span className={`flex h-7 w-7 items-center justify-center rounded-lg ${danger ? 'bg-kumo-danger-tint text-kumo-danger' : 'bg-kumo-brand/10 text-kumo-brand'}`}>{icon}</span></div><div className="mt-3 text-2xl font-semibold tracking-tight text-kumo-default">{value}</div></div>
}

/** Airflow-inspired operational board for registered Verglas workers. */
export function WorkersBoard({ workers, onOpen }: { workers: VerglasWorkerSummary[]; onOpen: (name: string) => void }) {
  const summary = useMemo(() => summarizeWorkers(workers), [workers])
  const recentRuns = useMemo(() => workers.flatMap((worker) => (worker.recentRuns ?? []).map((run) => ({ ...run, workerName: worker.name }))).toSorted((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt)).slice(0, 8), [workers])
  return <>
    <section aria-label="Worker health summary" className="grid grid-cols-2 gap-3 lg:grid-cols-4">
      <Metric label="Workers" value={summary.total} icon={<FlowArrow size={15} />} />
      <Metric label="Live runs" value={summary.active} icon={<Lightning size={15} weight="fill" />} />
      <Metric label="Scheduled" value={summary.scheduled} icon={<CalendarDots size={15} />} />
      <Metric label="Needs attention" value={summary.failed} icon={<WarningCircle size={15} weight="fill" />} danger={summary.failed > 0} />
    </section>
    <div className="mt-6 grid gap-5 lg:grid-cols-[minmax(0,1.45fr)_minmax(18rem,.8fr)]">
      <section className="overflow-hidden rounded-xl border border-kumo-line bg-kumo-base"><div className="flex items-center justify-between border-b border-kumo-line px-4 py-3"><div><h2 className="text-sm font-semibold text-kumo-default">Worker runs</h2><p className="mt-0.5 text-[11px] text-kumo-subtle">Select a worker to inspect its schedule and history.</p></div><span className="rounded-full bg-kumo-tint px-2 py-0.5 text-[10px] font-medium text-kumo-subtle">{workers.length} active</span></div><div className="divide-y divide-kumo-line">{workers.map((worker) => { const schedule = workerScheduleSummary(worker); const run = worker.recentRuns?.[0]; return <button type="button" key={worker.name} onClick={() => onOpen(worker.name)} className="group flex w-full cursor-pointer items-center gap-3 px-4 py-3.5 text-left hover:bg-kumo-tint/50"><span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-kumo-fill text-kumo-brand"><FlowArrow size={17} /></span><span className="min-w-0 flex-1"><span className="flex items-center gap-2"><span className="truncate text-[13px] font-medium text-kumo-default">{worker.name}</span><CatalogStatus value={workerLifecycleLabel(worker.state)} good={worker.state === 'running'} />{worker.activeRun && <span className="rounded-full bg-amber-500/15 px-1.5 py-0.5 text-[9px] font-semibold uppercase text-amber-600">Live</span>}</span><span className="mt-1 flex items-center gap-1.5 truncate font-mono text-[10px] text-kumo-inactive"><CalendarDots size={12} />{schedule.label}</span></span><span className="hidden text-right sm:block">{run ? <CatalogStatus value={run.state} good={run.state === 'succeeded'} /> : <span className="text-[10px] text-kumo-inactive">No runs</span>}<span className="mt-1 block text-[10px] text-kumo-inactive">{run ? time(run.createdAt) : 'No runs yet'}</span></span><CaretRight size={15} className="shrink-0 text-kumo-inactive transition-transform group-hover:translate-x-0.5" /></button> })}</div></section>
      <section className="overflow-hidden rounded-xl border border-kumo-line bg-kumo-base"><div className="flex items-center justify-between border-b border-kumo-line px-4 py-3"><div><h2 className="text-sm font-semibold text-kumo-default">Recent activity</h2><p className="mt-0.5 text-[11px] text-kumo-subtle">Latest scheduler attempts.</p></div><Clock size={16} className="text-kumo-inactive" /></div>{recentRuns.length ? <div className="divide-y divide-kumo-line">{recentRuns.map((run) => <button key={run.jobId} type="button" onClick={() => onOpen(run.workerName)} className="flex w-full cursor-pointer items-start gap-2 px-4 py-3 text-left hover:bg-kumo-tint/50"><span className={`mt-0.5 h-2 w-2 shrink-0 rounded-full ${runClass(run.state).split(' ')[0]}`} /><span className="min-w-0 flex-1"><span className="block truncate text-[12px] font-medium text-kumo-default">{run.workerName}</span><span className="mt-0.5 block text-[11px] text-kumo-subtle">{time(run.createdAt)}</span></span><span className={`rounded-full px-1.5 py-0.5 text-[9px] font-semibold uppercase ${runClass(run.state)}`}>{run.state}</span></button>)}</div> : <p className="px-4 py-8 text-center text-[12px] text-kumo-subtle">Run a worker to see scheduler activity here.</p>}</section>
    </div>
  </>
}

/** Empty board state links to the existing assistant-driven worker creation flow. */
export function WorkersEmptyState() {
  return <div className="rounded-2xl border border-dashed border-kumo-line bg-kumo-base px-6 py-14 text-center"><div className="mx-auto flex h-11 w-11 items-center justify-center rounded-xl bg-kumo-brand/10 text-kumo-brand"><FlowArrow size={22} /></div><h2 className="mt-4 text-base font-semibold text-kumo-default">No workers yet</h2><p className="mx-auto mt-1 max-w-md text-[13px] leading-5 text-kumo-subtle">Create a worker with the assistant to schedule an ingestion, transformation, or event-driven task.</p><Link to="/workspaces" className="mt-5 inline-flex h-9 items-center gap-1.5 rounded-lg bg-kumo-brand px-3 text-[13px] font-medium text-white hover:bg-kumo-brand-hover"><Plus size={15} /> Create worker</Link></div>
}
