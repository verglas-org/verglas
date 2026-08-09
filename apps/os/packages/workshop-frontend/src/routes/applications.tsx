import { createFileRoute } from '@tanstack/react-router'
import { ArrowSquareOut, Browser, Trash } from '@phosphor-icons/react'
import { useCallback, useEffect, useState } from 'react'
import type { VerglasVesselSummary } from '@verglas/workshop-shared/api'
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
import { useDocumentTitle } from '../useDocumentTitle'

export const Route = createFileRoute('/applications')({ component: ApplicationsPage })

function ApplicationsPage() {
  useDocumentTitle('Applications')
  const { authenticatedApi } = useAuthenticatedApi()
  const [applications, setApplications] = useState<VerglasVesselSummary[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [deleting, setDeleting] = useState<string | null>(null)
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      setApplications((await authenticatedApi.listVerglasVessels()).filter((v) => v.role === 'application'))
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [authenticatedApi])
  useEffect(() => { void load() }, [load])

  const app = applications.find((entry) => entry.name === selected) ?? null

  const remove = async () => {
    if (!confirmDelete) return
    setDeleting(confirmDelete)
    setError(null)
    try {
      await authenticatedApi.deleteVerglasApplication(confirmDelete)
      if (selected === confirmDelete) setSelected(null)
      setConfirmDelete(null)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setDeleting(null)
    }
  }

  return (
    <CatalogPage
      title="Applications"
      description="Full-stack local previews built over your lakehouse and integrations."
      onRefresh={() => void load()}
    >
      {error && <CatalogError message={error} />}
      {app ? (
        <CatalogDetailCard
          open
          title={app.name}
          subtitle={app.image}
          meta={<CatalogStatus value={app.health} good={app.health === 'ready'} />}
          screenshotUrl={app.screenshotUrl}
          onBack={() => setSelected(null)}
          footer={
            <>
              <button
                type="button"
                onClick={() => setConfirmDelete(app.name)}
                className="mr-auto inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg border border-kumo-line px-3 text-[13px] text-kumo-subtle hover:border-kumo-danger/40 hover:bg-kumo-danger-tint hover:text-kumo-danger"
              >
                <Trash size={14} />
                Delete
              </button>
              {app.previewUrl && (
                <a
                  href={app.previewUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex h-9 items-center gap-1.5 rounded-lg bg-kumo-brand px-3 text-[13px] font-medium text-white hover:bg-kumo-brand-hover"
                >
                  Open preview <ArrowSquareOut size={14} />
                </a>
              )}
            </>
          }
        >
          <dl className="grid gap-3 text-[13px] sm:grid-cols-2">
            <div>
              <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">Vessel</dt>
              <dd className="mt-1 font-mono text-kumo-default">{app.name}</dd>
            </div>
            <div>
              <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">Health</dt>
              <dd className="mt-1 text-kumo-default">{app.health}</dd>
            </div>
            <div className="sm:col-span-2">
              <dt className="text-[11px] font-medium uppercase tracking-wide text-kumo-inactive">Image</dt>
              <dd className="mt-1 break-all font-mono text-[12px] text-kumo-subtle">{app.image}</dd>
            </div>
          </dl>
        </CatalogDetailCard>
      ) : loading ? (
        <CatalogEmpty>Loading applications…</CatalogEmpty>
      ) : (
        <CatalogTable
          empty="No Application Vessels are running."
          cards={applications.map((entry) => ({
            id: entry.name,
            icon: <Browser size={18} />,
            primary: entry.name,
            secondary: entry.image,
            meta: <CatalogStatus value={entry.health} good={entry.health === 'ready'} />,
            onOpen: () => setSelected(entry.name),
          }))}
        />
      )}

      <DeleteConfirmationDialog
        open={confirmDelete !== null}
        title="Delete application"
        description={
          confirmDelete
            ? `Stop and remove “${confirmDelete}” from the local runtime. This cannot be undone.`
            : null
        }
        isDeleting={deleting !== null}
        onOpenChange={(open) => { if (!open) setConfirmDelete(null) }}
        onConfirm={() => void remove()}
      />
    </CatalogPage>
  )
}
