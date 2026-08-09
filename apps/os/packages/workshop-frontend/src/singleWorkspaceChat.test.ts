import { describe, expect, it } from 'vitest'
import type { AiChatMetadata } from '@verglas/workshop-shared/api'
import { selectSingleWorkspaceChatId } from './ChatInterface'

function chat(id: number): AiChatMetadata {
  return {
    id,
    title: `Chat ${id}`,
    started: new Date(0),
    lastActive: new Date(0),
  }
}

describe('selectSingleWorkspaceChatId', () => {
  it('keeps a Workspace on its first chat regardless of list order', () => {
    expect(selectSingleWorkspaceChatId([chat(9), chat(3), chat(6)])).toBe(3)
  })

  it('has no selection until the first chat is created', () => {
    expect(selectSingleWorkspaceChatId([])).toBeNull()
  })
})
