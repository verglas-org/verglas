import type { AiChatAuthorInfo, ModelRuntimeId } from '@verglas/workshop-shared/api'

const RUNTIME_LABELS: Record<ModelRuntimeId, string> = {
  codex: 'Codex',
  'claude-code': 'Claude Code',
  cursor: 'Cursor',
}

export type ModelGroup = {
  id: string
  label: string
  models: AiChatAuthorInfo[]
}

export function modelRuntime(modelId: string): ModelRuntimeId | null {
  const match = modelId.match(/^runtime:(codex|claude-code|cursor)(?::|$)/)
  return match?.[1] as ModelRuntimeId | undefined ?? null
}

export function groupModels(models: AiChatAuthorInfo[]): ModelGroup[] {
  const groups = new Map<string, ModelGroup>()
  for (const model of models) {
    const runtime = modelRuntime(model.id)
    const id = runtime ?? 'api'
    const group = groups.get(id) ?? {
      id,
      label: runtime ? RUNTIME_LABELS[runtime] : 'API models',
      models: [],
    }
    group.models.push(model)
    groups.set(id, group)
  }
  return [...groups.values()]
}

export function modelRuntimeLabel(modelId: string): string | null {
  const runtime = modelRuntime(modelId)
  return runtime ? RUNTIME_LABELS[runtime] : null
}
