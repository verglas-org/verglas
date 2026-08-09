import { createFileRoute } from '@tanstack/react-router'
import { CheckCircle, PlugsConnected, Trash } from '@phosphor-icons/react'
import { useCallback, useEffect, useState } from 'react'
import type { IntegrationSetupInstruction, VerglasIntegrationConfiguration, VerglasVesselSummary } from '@verglas/workshop-shared/api'
import { useAuthenticatedApi } from '../AuthContext'
import DeleteConfirmationDialog from '../components/DeleteConfirmationDialog'
import {
  CatalogDetailCard,
  CatalogError,
  CatalogPage,
  CatalogStatus,
  CatalogTable,
} from '../components/CatalogTable'
import { useDocumentTitle } from '../useDocumentTitle'

export const Route = createFileRoute('/integrations')({ component: IntegrationsPage })

function IntegrationsPage() {
  useDocumentTitle('Integrations')
  const { authenticatedApi } = useAuthenticatedApi()
  const [integrations, setIntegrations] = useState<VerglasVesselSummary[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [configuration, setConfiguration] = useState<VerglasIntegrationConfiguration | null>(null)
  const [values, setValues] = useState<Record<string, string>>({})
  const [error, setError] = useState<string | null>(null)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [deleting, setDeleting] = useState<string | null>(null)
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)

  const load = useCallback(async () => {
    setError(null)
    try {
      setIntegrations((await authenticatedApi.listVerglasVessels()).filter((v) => v.role === 'integration'))
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }, [authenticatedApi])
  useEffect(() => { void load() }, [load])

  const open = async (name: string) => {
    setSelected(name)
    setConfiguration(null)
    setValues({})
    setDetailError(null)
    try {
      setConfiguration(await authenticatedApi.getVerglasIntegrationConfiguration(name))
    } catch (err) {
      setDetailError(err instanceof Error ? err.message : String(err))
    }
  }

  const close = () => {
    setSelected(null)
    setConfiguration(null)
    setValues({})
    setDetailError(null)
  }

  const save = async () => {
    if (!selected || !configuration) return
    setSaving(true)
    setDetailError(null)
    try {
      await authenticatedApi.configureVerglasIntegration(selected, values)
      setConfiguration(await authenticatedApi.getVerglasIntegrationConfiguration(selected))
      setValues({})
      await load()
    } catch (err) {
      setDetailError(err instanceof Error ? err.message : String(err))
    } finally {
      setSaving(false)
    }
  }

  const remove = async () => {
    if (!confirmDelete) return
    setDeleting(confirmDelete)
    setError(null)
    try {
      await authenticatedApi.deleteVerglasIntegration(confirmDelete)
      if (selected === confirmDelete) close()
      setConfirmDelete(null)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setDeleting(null)
    }
  }

  const selectedVessel = integrations.find((entry) => entry.name === selected)

  return (
    <CatalogPage
      title="Integrations"
      description="API containers that connect external systems. Configuration stays inside each integration."
      onRefresh={() => void load()}
    >
      {error && <CatalogError message={error} />}
      {selected !== null ? (
        <CatalogDetailCard
          open
          title={configuration?.title ?? selectedVessel?.title ?? selected ?? 'Integration'}
          subtitle={configuration?.description ?? selectedVessel?.description}
          meta={configuration?.configured ? (
            <span className="inline-flex items-center gap-1 rounded-full bg-kumo-success-tint px-2 py-1 text-[10px] font-semibold uppercase text-kumo-success">
              <CheckCircle size={12} weight="fill" />
              Connected
            </span>
          ) : selectedVessel ? (
            <CatalogStatus value={selectedVessel.health} good={selectedVessel.health === 'ready'} />
          ) : undefined}
          onBack={close}
          footer={
            <>
              <button
                type="button"
                onClick={() => selected && setConfirmDelete(selected)}
                className="mr-auto inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg border border-kumo-line px-3 text-[13px] text-kumo-subtle hover:border-kumo-danger/40 hover:bg-kumo-danger-tint hover:text-kumo-danger"
              >
                <Trash size={14} />
                Delete
              </button>
              <button
                type="button"
                disabled={!configuration || saving || Object.values(values).every((value) => !value)}
                onClick={() => void save()}
                className="h-9 cursor-pointer rounded-lg bg-kumo-brand px-3 text-[13px] font-medium text-white disabled:cursor-not-allowed disabled:opacity-40"
              >
                {saving ? 'Saving and testing…' : 'Save and test'}
              </button>
            </>
          }
        >
          {detailError && <CatalogError message={detailError} />}
          {!configuration && !detailError ? (
            <div className="py-10 text-center text-sm text-kumo-subtle">Loading configuration…</div>
          ) : configuration ? (
            <>
              <p className="mb-4 font-mono text-[11px] text-kumo-inactive">{selected}</p>
              {configuration.instructions.length > 0 && (
                <ol className="mb-5 list-decimal space-y-2 pl-5 text-[12px] leading-5 text-kumo-subtle">
                  {configuration.instructions.map((instruction, index) => (
                    <SetupInstruction
                      key={typeof instruction === 'string' ? instruction : `${instruction.title}-${index}`}
                      instruction={instruction}
                    />
                  ))}
                </ol>
              )}
              <div className="space-y-4">
                {configuration.fields.map((field) => (
                  <label key={field.name} className="block">
                    <span className="mb-1.5 block text-[12px] font-medium text-kumo-default">{field.label}</span>
                    <input
                      type={field.secret ? 'password' : field.type === 'password' ? 'password' : 'text'}
                      required={field.required}
                      placeholder={configuration.configured && field.secret ? 'Leave blank to keep current value' : field.placeholder}
                      value={values[field.name] ?? ''}
                      onChange={(event) => setValues((current) => ({ ...current, [field.name]: event.target.value }))}
                      className="h-10 w-full rounded-lg border border-kumo-line bg-kumo-base px-3 text-sm text-kumo-default outline-none focus:border-kumo-brand"
                    />
                    {field.description && (
                      <span className="mt-1 block text-[11px] text-kumo-inactive">{field.description}</span>
                    )}
                  </label>
                ))}
              </div>
              {configuration.helpUrl && (
                <a href={configuration.helpUrl} target="_blank" rel="noreferrer" className="mt-4 inline-flex text-[12px] text-kumo-brand hover:underline">
                  Setup documentation
                </a>
              )}
            </>
          ) : null}
        </CatalogDetailCard>
      ) : (
        <CatalogTable
          empty="No Integration Vessels are running."
          cards={integrations.map((integration) => ({
            id: integration.name,
            icon: <PlugsConnected size={18} />,
            primary: integration.title ?? integration.name,
            secondary: integration.description ?? integration.image,
            tertiary: integration.title ? integration.name : undefined,
            meta: <CatalogStatus value={integration.health} good={integration.health === 'ready'} />,
            onOpen: () => void open(integration.name),
          }))}
        />
      )}

      <DeleteConfirmationDialog
        open={confirmDelete !== null}
        title="Delete integration"
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

function SetupInstruction({ instruction }: { instruction: string | IntegrationSetupInstruction }) {
  if (typeof instruction === 'string') return <li>{instruction}</li>
  return (
    <li>
      <span className="font-medium text-kumo-default">{instruction.title}</span>
      <span className="block">{instruction.description}</span>
      {isSafeExternalUrl(instruction.url) && (
        <a href={instruction.url} target="_blank" rel="noreferrer" className="text-kumo-brand hover:underline">
          Open setup page
        </a>
      )}
    </li>
  )
}

function isSafeExternalUrl(value: string | undefined): value is string {
  if (!value) return false
  try {
    return new URL(value).protocol === 'https:'
  } catch {
    return false
  }
}
