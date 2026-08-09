import { createFileRoute } from '@tanstack/react-router'
import WorkspaceChatPage from '../WorkspaceChatPage'

type WorkspaceSearch = {
  chat?: number
  /** Legacy workpiece id search param; ignored (Workspace editor removed). */
  w?: number
}

function parseIntParam(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isInteger(value)) return value
  if (typeof value === 'string' && value !== '') {
    const parsed = Number(value)
    if (Number.isInteger(parsed)) return parsed
  }
  return undefined
}

export const Route = createFileRoute('/workspace/$id')({
  component: WorkspaceChatPage,
  validateSearch: (search: Record<string, unknown>): WorkspaceSearch => ({
    chat: typeof search.chat === 'number' ? search.chat
      : typeof search.chat === 'string' ? Number(search.chat) || undefined
      : undefined,
    w: parseIntParam(search.w),
  }),
})
