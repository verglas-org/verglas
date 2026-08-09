import { createFileRoute, Link as RouterLink } from '@tanstack/react-router'
import {
  ArrowSquareOut,
  CheckCircle,
  CircleNotch,
  Key,
  Link as LinkIcon,
  LockKey,
  PlugsConnected,
  ShieldCheck,
  Trash,
  Plus,
} from '@phosphor-icons/react'
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
import { groupIntegrationFields, missingRequiredIntegrationFields } from '../integrationConfiguration'
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
  const [validationError, setValidationError] = useState<string | null>(null)
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
    setValidationError(null)
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
    setValidationError(null)
  }

  const save = async () => {
    if (!selected || !configuration) return
    const missing = missingRequiredIntegrationFields(configuration.fields, values, configuration.configured)
    if (missing.length) {
      setValidationError(`Enter ${missing.join(', ')} before verifying this connection.`)
      return
    }
    setSaving(true)
    setDetailError(null)
    setValidationError(null)
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
  const hasChanges = Object.values(values).some((value) => value.trim())

  return (
    <CatalogPage
      title="Integrations"
      description="Manage the credentials and connection settings for your external systems."
      onRefresh={() => void load()}
      actions={(
        <RouterLink
          to="/"
          search={{prompt: 'Create an integration that connects to '}}
          className="inline-flex h-9 items-center gap-1.5 rounded-lg bg-kumo-brand px-3 text-[13px] font-medium text-white hover:bg-kumo-brand-hover"
        >
          <Plus size={15} weight="bold" />
          New integration
        </RouterLink>
      )}
    >
      {error && <CatalogError message={error} />}
      {selected !== null ? (
        <CatalogDetailCard
          open
          title={configuration?.title ?? selectedVessel?.title ?? selected ?? 'Integration'}
          subtitle={configuration?.description ?? selectedVessel?.description}
          meta={configuration?.configured ? (
            <span className="inline-flex items-center gap-1 rounded-full bg-kumo-success-tint px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.06em] text-kumo-success">
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
                className="mr-auto inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg border border-kumo-line px-3 text-[13px] text-kumo-subtle transition-colors hover:border-kumo-danger/40 hover:bg-kumo-danger-tint hover:text-kumo-danger"
              >
                <Trash size={14} />
                Delete
              </button>
              <button
                type="button"
                disabled={!configuration || saving || !hasChanges}
                onClick={() => void save()}
                className="inline-flex h-9 cursor-pointer items-center gap-1.5 rounded-lg bg-kumo-brand px-3.5 text-[13px] font-medium text-white transition-colors hover:bg-kumo-brand-hover disabled:cursor-not-allowed disabled:opacity-40"
              >
                {saving && <CircleNotch size={14} className="animate-spin" />}
                {saving ? 'Verifying connection…' : 'Save & verify'}
              </button>
            </>
          }
        >
          {detailError && <CatalogError message={detailError} />}
          {validationError && <CatalogError message={validationError} />}
          {!configuration && !detailError ? (
            <div className="py-10 text-center text-sm text-kumo-subtle">Loading configuration…</div>
          ) : configuration ? (
            <div className="grid gap-7 lg:grid-cols-[minmax(0,1fr)_13rem]">
              <div className="min-w-0 space-y-6">
                <IntegrationIdentity vessel={selectedVessel} configured={configuration.configured} />

                {configuration.fields.length > 0 ? (
                  <div className="space-y-5">
                    {groupIntegrationFields(configuration.fields).map((group) => (
                      <section key={group.id} aria-labelledby={`${group.id}-settings`}>
                        <div className="mb-2 flex items-center gap-2 px-1">
                          {group.id === 'authentication' ? <LockKey size={14} className="text-kumo-inactive" /> : <LinkIcon size={14} className="text-kumo-inactive" />}
                          <h3 id={`${group.id}-settings`} className="text-[12px] font-medium uppercase tracking-[0.08em] text-kumo-inactive">{group.title}</h3>
                        </div>
                        <div className="overflow-hidden rounded-xl border border-kumo-line bg-kumo-base">
                          {group.fields.map((field, index) => (
                            <IntegrationField
                              key={field.name}
                              field={field}
                              value={values[field.name] ?? ''}
                              configured={configuration.configured}
                              bordered={index > 0}
                              onChange={(value) => {
                                setValidationError(null)
                                setValues((current) => ({ ...current, [field.name]: value }))
                              }}
                            />
                          ))}
                        </div>
                      </section>
                    ))}
                  </div>
                ) : (
                  <div className="rounded-xl border border-dashed border-kumo-line px-4 py-5 text-[13px] leading-5 text-kumo-subtle">
                    This integration does not need any connection settings.
                  </div>
                )}
              </div>

              <aside className="space-y-4 lg:border-l lg:border-kumo-line lg:pl-5">
                <section>
                  <h3 className="text-[12px] font-medium uppercase tracking-[0.08em] text-kumo-inactive">Connection</h3>
                  <p className="mt-2 text-[12px] leading-5 text-kumo-subtle">
                    Secret values are masked and never shown again after they are saved.
                  </p>
                </section>
                {configuration.instructions.length > 0 && (
                  <section>
                    <h3 className="text-[12px] font-medium uppercase tracking-[0.08em] text-kumo-inactive">Setup guide</h3>
                    <ol className="mt-2 space-y-3 text-[12px] leading-5 text-kumo-subtle">
                      {configuration.instructions.map((instruction, index) => (
                        <SetupInstruction
                          key={typeof instruction === 'string' ? instruction : `${instruction.title}-${index}`}
                          number={index + 1}
                          instruction={instruction}
                        />
                      ))}
                    </ol>
                  </section>
                )}
                {isSafeExternalUrl(configuration.helpUrl) && (
                  <a href={configuration.helpUrl} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1.5 text-[12px] font-medium text-kumo-brand hover:underline">
                    Setup documentation
                    <ArrowSquareOut size={13} />
                  </a>
                )}
              </aside>
            </div>
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

function IntegrationIdentity({ vessel, configured }: { vessel?: VerglasVesselSummary; configured: boolean }) {
  const health = vessel?.health ?? 'unknown'
  const status = configured ? 'Configured' : health === 'ready' ? 'Ready to configure' : 'Awaiting setup'

  return (
    <section className="rounded-xl border border-kumo-line bg-kumo-tint/30 px-4 py-3.5">
      <div className="flex items-start gap-3">
        <div className={`grid h-8 w-8 shrink-0 place-items-center rounded-lg ${configured ? 'bg-kumo-success-tint text-kumo-success' : 'bg-kumo-fill text-kumo-brand'}`}>
          {configured ? <ShieldCheck size={17} /> : <Key size={17} />}
        </div>
        <div className="min-w-0 flex-1">
          <p className="text-[13px] font-medium text-kumo-default">{status}</p>
          <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-[11px]">
            <div><dt className="text-kumo-inactive">Vessel</dt><dd className="mt-0.5 truncate font-mono text-kumo-subtle">{vessel?.name ?? 'Loading'}</dd></div>
            <div><dt className="text-kumo-inactive">Runtime</dt><dd className="mt-0.5 capitalize text-kumo-subtle">{vessel?.state ?? health}</dd></div>
          </dl>
        </div>
      </div>
    </section>
  )
}

function IntegrationField({
  field,
  value,
  configured,
  bordered,
  onChange,
}: {
  field: VerglasIntegrationConfiguration['fields'][number]
  value: string
  configured: boolean
  bordered: boolean
  onChange: (value: string) => void
}) {
  const secret = field.secret || field.type === 'password'
  return (
    <label className={`block px-4 py-3.5 ${bordered ? 'border-t border-kumo-line' : ''}`}>
      <span className="flex flex-wrap items-center gap-1.5 text-[13px] font-medium text-kumo-default">
        {field.label}
        {field.required && <span className="text-kumo-danger" aria-label="Required">*</span>}
        {secret && <span className="rounded bg-kumo-fill px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-[0.06em] text-kumo-inactive">Secret</span>}
      </span>
      {field.description && <span className="mt-1 block text-[12px] leading-4 text-kumo-subtle">{field.description}</span>}
      <input
        type={inputType(field.type, secret)}
        required={field.required && !configured}
        autoComplete={secret ? 'new-password' : undefined}
        placeholder={configured && secret ? 'Stored securely — enter a new value to replace it' : field.placeholder}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="mt-2 h-9 w-full rounded-lg border border-kumo-line bg-kumo-base px-3 text-[13px] tracking-[-0.15px] text-kumo-default outline-none transition-[border-color,box-shadow] placeholder:text-kumo-inactive focus:border-kumo-ring focus:ring-[3px] focus:ring-kumo-ring/15"
      />
    </label>
  )
}

function inputType(type: string, secret: boolean): 'email' | 'number' | 'password' | 'text' | 'url' {
  if (secret) return 'password'
  return ['email', 'number', 'url'].includes(type) ? type as 'email' | 'number' | 'url' : 'text'
}

function SetupInstruction({ instruction, number }: { instruction: string | IntegrationSetupInstruction; number: number }) {
  if (typeof instruction === 'string') return <li className="flex gap-2"><span className="text-kumo-inactive">{number}.</span><span>{instruction}</span></li>
  return (
    <li className="flex gap-2">
      <span className="text-kumo-inactive">{number}.</span>
      <span>
        <span className="font-medium text-kumo-default">{instruction.title}</span>
        <span className="block">{instruction.description}</span>
        {isSafeExternalUrl(instruction.url) && (
          <a href={instruction.url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-kumo-brand hover:underline">
            Open setup page
            <ArrowSquareOut size={12} />
          </a>
        )}
      </span>
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
