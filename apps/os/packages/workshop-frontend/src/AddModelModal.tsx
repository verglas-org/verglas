import { useEffect, useState } from 'react'
import { Button, Dialog, Input, Select, SensitiveInput, useKumoToastManager } from '@cloudflare/kumo'
import {
  type AiChatAuthorInfo,
  type AiModelConfig,
  type AiModelProvider,
  type AuthenticatedApi,
  type ModelRuntimeId,
  type ModelRuntimeLoginResult,
  type ModelRuntimeWizardStep,
  SUGGESTED_MODELS,
} from '@verglas/workshop-shared/api'
import type { RpcStub } from 'capnweb'
import { ArrowLeft, ArrowSquareOut, Check, Key, SignIn } from '@phosphor-icons/react'

interface AddModelModalProps {
  visible: boolean
  onCancel: () => void
  onSuccess: () => void
  authenticatedApi: RpcStub<AuthenticatedApi>
}

type RuntimeBrand = {
  id: ModelRuntimeId
  name: string
  accountName: string
  tokenName: string
  tokenPlaceholder: string
  logo: string
  accent: string
  description: string
}

const RUNTIMES: RuntimeBrand[] = [
  {
    id: 'codex',
    name: 'Codex',
    accountName: 'ChatGPT account',
    tokenName: 'OpenAI API key',
    tokenPlaceholder: 'sk-...',
    logo: '/provider-icons/codex.svg',
    accent: '#10a37f',
    description: 'Use Codex with your ChatGPT subscription or an OpenAI API key.',
  },
  {
    id: 'claude-code',
    name: 'Claude Code',
    accountName: 'Claude account',
    tokenName: 'Anthropic API key',
    tokenPlaceholder: 'sk-ant-...',
    logo: '/provider-icons/claude.svg',
    accent: '#d97757',
    description: 'Use Claude Code with your Claude subscription or an Anthropic API key.',
  },
  {
    id: 'cursor',
    name: 'Cursor',
    accountName: 'Cursor account',
    tokenName: 'Cursor API key',
    tokenPlaceholder: 'key_...',
    logo: '/provider-icons/cursor.svg',
    accent: '#1f2937',
    description: 'Use the Cursor agent runtime with your Cursor subscription or API key.',
  },
]

const DIRECT_PROVIDERS: Exclude<AiModelProvider, 'local-runtime'>[] = [
  'openai', 'anthropic', 'google', 'cloudflare', 'ollama',
]

const PROVIDER_LABELS: Record<Exclude<AiModelProvider, 'local-runtime'>, string> = {
  anthropic: 'Anthropic API',
  openai: 'OpenAI API',
  google: 'Google AI',
  cloudflare: 'Cloudflare Workers AI',
  ollama: 'Ollama',
}

type View = 'choose' | 'runtime' | 'token' | 'wizard' | 'direct'

function RuntimeLogo({ runtime, size = 42 }: { runtime: RuntimeBrand, size?: number }) {
  return (
    <div
      className="flex shrink-0 items-center justify-center rounded-xl"
      style={{ width: size, height: size, background: runtime.accent }}
    >
      <img src={runtime.logo} alt="" className="h-[68%] w-[68%] object-contain" />
    </div>
  )
}

export default function AddModelModal({
  visible, onCancel, onSuccess, authenticatedApi,
}: AddModelModalProps) {
  const toasts = useKumoToastManager()
  const [view, setView] = useState<View>('choose')
  const [runtime, setRuntime] = useState<RuntimeBrand | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [apiToken, setApiToken] = useState('')
  const [login, setLogin] = useState<ModelRuntimeLoginResult | null>(null)
  const [wizardValue, setWizardValue] = useState<unknown>('')

  const [directProvider, setDirectProvider] = useState<Exclude<AiModelProvider, 'local-runtime'>>('openai')
  const [directModel, setDirectModel] = useState('')
  const [directName, setDirectName] = useState('')
  const [directToken, setDirectToken] = useState('')
  const [directUrl, setDirectUrl] = useState('')
  const [accountId, setAccountId] = useState('')

  useEffect(() => {
    if (!visible) {
      setView('choose')
      setRuntime(null)
      setLoading(false)
      setError('')
      setApiToken('')
      setLogin(null)
      setWizardValue('')
    }
  }, [visible])

  const selectRuntime = (selected: RuntimeBrand) => {
    setRuntime(selected)
    setError('')
    setView('runtime')
  }

  const complete = (title: string) => {
    toasts.add({ title, variant: 'success' })
    onSuccess()
  }

  const linkSubscription = async () => {
    if (!runtime) return
    setLoading(true)
    setError('')
    try {
      const detection = await authenticatedApi.detectModelRuntimes()
      const status = detection.runtimes.find(item => item.id === runtime.id)
      if (status?.linked) {
        await authenticatedApi.linkSubscriptionRuntime(runtime.id)
        complete(`${runtime.name} linked`)
        return
      }
      const sessionId = crypto.randomUUID()
      const result = await authenticatedApi.startModelRuntimeLogin(runtime.id, sessionId)
      setLogin(result)
      setWizardValue(result.step?.initialValue ?? '')
      setView('wizard')
      if (result.done) await finishLogin(result)
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught))
    } finally {
      setLoading(false)
    }
  }

  const finishLogin = async (result: ModelRuntimeLoginResult) => {
    if (!runtime) return
    if (result.error || result.status === 'error') {
      setError(result.error || `${runtime.name} login failed.`)
      return
    }
    await authenticatedApi.linkSubscriptionRuntime(runtime.id)
    complete(`${runtime.name} linked`)
  }

  const advanceWizard = async () => {
    if (!login?.step) return
    setLoading(true)
    setError('')
    try {
      const step = login.step
      const needsAnswer = ['text', 'select', 'confirm', 'multiselect'].includes(step.type)
      const result = await authenticatedApi.continueModelRuntimeLogin(
        login.sessionId,
        needsAnswer ? { stepId: step.id, value: wizardValue } : undefined,
      )
      setLogin(result)
      setWizardValue(result.step?.initialValue ?? '')
      if (result.done) await finishLogin(result)
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught))
    } finally {
      setLoading(false)
    }
  }

  const linkToken = async () => {
    if (!runtime || !apiToken.trim()) return
    setLoading(true)
    setError('')
    try {
      await authenticatedApi.linkTokenRuntime(runtime.id, apiToken.trim())
      complete(`${runtime.name} linked`)
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught))
    } finally {
      setLoading(false)
    }
  }

  const addDirectModel = async () => {
    const model = directModel.trim()
    if (!model || !directName.trim()) {
      setError('Model ID and display name are required.')
      return
    }
    if (directProvider !== 'ollama' && !directToken.trim()) {
      setError('API token is required.')
      return
    }
    if (directProvider === 'cloudflare' && !accountId.trim()) {
      setError('Cloudflare account ID is required.')
      return
    }
    setLoading(true)
    setError('')
    try {
      const profile: AiChatAuthorInfo = {
        type: 'agent', id: model, name: directName.trim(),
      }
      const config: AiModelConfig = {
        provider: directProvider,
        model,
        apiToken: directToken.trim(),
        ...(directUrl.trim() ? { apiUrl: directUrl.trim() } : {}),
        ...(accountId.trim() ? { accountId: accountId.trim() } : {}),
      }
      await authenticatedApi.addModel(profile, config)
      complete('AI model added')
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught))
    } finally {
      setLoading(false)
    }
  }

  const back = async () => {
    if (view === 'wizard' && login && !login.done) {
      try { await authenticatedApi.cancelModelRuntimeLogin(login.sessionId) } catch { /* best effort */ }
    }
    setError('')
    setApiToken('')
    setLogin(null)
    setWizardValue('')
    setView(view === 'runtime' || view === 'direct' ? 'choose' : 'runtime')
  }

  return (
    <Dialog.Root open={visible} onOpenChange={(open) => { if (!open) onCancel() }}>
      <Dialog className="p-6" size="lg">
        <div className="mb-5 flex items-center gap-2">
          {view !== 'choose' && (
            <button
              type="button"
              aria-label="Back"
              onClick={() => void back()}
              className="rounded-md p-1 text-kumo-subtle hover:bg-kumo-fill hover:text-kumo-default"
            >
              <ArrowLeft size={18} />
            </button>
          )}
          <Dialog.Title className="text-lg font-semibold">
            {view === 'choose' ? 'Link an AI runtime' : runtime?.name || 'Add API model'}
          </Dialog.Title>
        </div>

        {view === 'choose' && (
          <div className="space-y-5">
            <div>
              <p className="mb-3 text-sm text-kumo-subtle">
                Sign in with an existing coding-agent subscription or connect with an API key.
              </p>
              <div className="grid gap-3 sm:grid-cols-3">
                {RUNTIMES.map(item => (
                  <button
                    key={item.id}
                    type="button"
                    onClick={() => selectRuntime(item)}
                    className="group rounded-xl border border-kumo-line p-4 text-left transition hover:border-kumo-brand hover:bg-kumo-tint"
                  >
                    <RuntimeLogo runtime={item} />
                    <div className="mt-3 text-sm font-semibold text-kumo-default">Link {item.name}</div>
                    <div className="mt-1 text-xs leading-4 text-kumo-subtle">{item.description}</div>
                  </button>
                ))}
              </div>
            </div>
            <div className="border-t border-kumo-line pt-4">
              <button
                type="button"
                onClick={() => { setRuntime(null); setError(''); setView('direct') }}
                className="text-sm font-medium text-kumo-brand hover:underline"
              >
                Add another API provider
              </button>
            </div>
          </div>
        )}

        {view === 'runtime' && runtime && (
          <div className="space-y-4">
            <div className="flex items-center gap-3 rounded-xl border border-kumo-line bg-kumo-tint p-4">
              <RuntimeLogo runtime={runtime} size={48} />
              <div>
                <div className="font-semibold text-kumo-default">{runtime.name}</div>
                <div className="mt-0.5 text-sm text-kumo-subtle">{runtime.description}</div>
              </div>
            </div>
            <button
              type="button"
              onClick={() => void linkSubscription()}
              disabled={loading}
              className="flex w-full items-center gap-3 rounded-xl border border-kumo-line p-4 text-left transition hover:border-kumo-brand hover:bg-kumo-tint disabled:opacity-60"
            >
              <SignIn size={22} className="text-kumo-brand" />
              <div className="flex-1">
                <div className="text-sm font-semibold text-kumo-default">Continue with subscription</div>
                <div className="mt-0.5 text-xs text-kumo-subtle">Sign in with your {runtime.accountName}</div>
              </div>
            </button>
            <button
              type="button"
              onClick={() => { setError(''); setView('token') }}
              className="flex w-full items-center gap-3 rounded-xl border border-kumo-line p-4 text-left transition hover:border-kumo-brand hover:bg-kumo-tint"
            >
              <Key size={22} className="text-kumo-brand" />
              <div className="flex-1">
                <div className="text-sm font-semibold text-kumo-default">Use API key</div>
                <div className="mt-0.5 text-xs text-kumo-subtle">Connect with {runtime.tokenName}</div>
              </div>
            </button>
          </div>
        )}

        {view === 'token' && runtime && (
          <div className="space-y-4">
            <div className="flex items-center gap-3">
              <RuntimeLogo runtime={runtime} />
              <div>
                <div className="font-semibold text-kumo-default">Use {runtime.tokenName}</div>
                <div className="text-sm text-kumo-subtle">The token is stored with your Workshop model.</div>
              </div>
            </div>
            <SensitiveInput
              label={runtime.tokenName}
              placeholder={runtime.tokenPlaceholder}
              value={apiToken}
              onValueChange={setApiToken}
            />
            <Button variant="primary" className="w-full" loading={loading}
              disabled={!apiToken.trim()} onClick={() => void linkToken()}>
              Link {runtime.name}
            </Button>
          </div>
        )}

        {view === 'wizard' && runtime && login?.step && (
          <WizardStepView
            runtime={runtime}
            step={login.step}
            value={wizardValue}
            onValueChange={setWizardValue}
            onContinue={() => void advanceWizard()}
            loading={loading}
          />
        )}

        {view === 'direct' && (
          <div className="space-y-4">
            <Select label="Provider" value={directProvider}
              onValueChange={(value) => setDirectProvider(value as typeof directProvider)}>
              {DIRECT_PROVIDERS.map(provider => (
                <Select.Option key={provider} value={provider}>{PROVIDER_LABELS[provider]}</Select.Option>
              ))}
            </Select>
            <Input label="Model ID" placeholder={Object.keys(SUGGESTED_MODELS[directProvider])[0] || 'model-id'}
              value={directModel} onChange={event => setDirectModel(event.target.value)} />
            <Input label="Display name" placeholder="Shown in chats"
              value={directName} onChange={event => setDirectName(event.target.value)} />
            {directProvider === 'cloudflare' && (
              <Input label="Cloudflare account ID" value={accountId}
                onChange={event => setAccountId(event.target.value)} />
            )}
            <SensitiveInput label="API token" value={directToken} onValueChange={setDirectToken}
              description={directProvider === 'ollama' ? 'Optional for local Ollama' : undefined} />
            {(directProvider === 'ollama') && (
              <Input label="API URL" placeholder="http://localhost:11434" value={directUrl}
                onChange={event => setDirectUrl(event.target.value)} />
            )}
            <Button variant="primary" className="w-full" loading={loading}
              onClick={() => void addDirectModel()}>Add model</Button>
          </div>
        )}

        {error && (
          <div className="mt-4 rounded-lg border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-700">
            {error}
          </div>
        )}

        <div className="mt-6 flex justify-end">
          <Dialog.Close render={(props) => (
            <Button variant="secondary" {...props} disabled={loading}>Cancel</Button>
          )} />
        </div>
      </Dialog>
    </Dialog.Root>
  )
}

function WizardStepView({
  runtime, step, value, onValueChange, onContinue, loading,
}: {
  runtime: RuntimeBrand
  step: ModelRuntimeWizardStep
  value: unknown
  onValueChange: (value: unknown) => void
  onContinue: () => void
  loading: boolean
}) {
  useEffect(() => {
    if (step.type !== 'progress' || loading) return
    const timer = window.setTimeout(onContinue, 1_500)
    return () => window.clearTimeout(timer)
  }, [loading, onContinue, step.message, step.type])

  const openExternal = () => {
    if (step.externalUrl) window.open(step.externalUrl, '_blank', 'noopener,noreferrer')
  }
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <RuntimeLogo runtime={runtime} />
        <div>
          <div className="font-semibold text-kumo-default">{step.title || `Sign in to ${runtime.name}`}</div>
          {step.message && <div className="mt-0.5 text-sm text-kumo-subtle">{step.message}</div>}
        </div>
      </div>
      {step.deviceCode && (
        <div className="rounded-xl border border-kumo-line bg-kumo-tint p-4 text-center">
          <div className="text-xs uppercase tracking-wide text-kumo-subtle">Device code</div>
          <div className="mt-1 select-all font-mono text-xl font-semibold text-kumo-default">
            {step.deviceCode.code}
          </div>
        </div>
      )}
      {step.externalUrl && (
        <Button variant="secondary" className="w-full" onClick={openExternal}>
          Open {runtime.name} sign in <ArrowSquareOut size={15} />
        </Button>
      )}
      {step.type === 'text' && (step.sensitive ? (
        <SensitiveInput label={step.title || 'Value'} placeholder={step.placeholder}
          value={String(value ?? '')} onValueChange={onValueChange} />
      ) : (
        <Input label={step.title || 'Value'} placeholder={step.placeholder}
          value={String(value ?? '')} onChange={event => onValueChange(event.target.value)} />
      ))}
      {step.type === 'select' && step.options && (
        <Select label={step.title || 'Choose an option'} value={String(value ?? '')}
          onValueChange={selected => onValueChange(selected)}>
          {step.options.map(option => (
            <Select.Option key={String(option.value)} value={String(option.value)}>
              {option.label}
            </Select.Option>
          ))}
        </Select>
      )}
      {step.type === 'confirm' && (
        <label className="flex items-center gap-2 text-sm text-kumo-default">
          <input type="checkbox" checked={Boolean(value)}
            onChange={event => onValueChange(event.target.checked)} />
          Confirm
        </label>
      )}
      <Button variant="primary" className="w-full" loading={loading} onClick={onContinue}>
        {step.type === 'progress' ? 'Check status' : <><Check size={15} /> Continue</>}
      </Button>
    </div>
  )
}
