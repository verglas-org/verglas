// @vitest-environment jsdom
/* eslint-disable react/react-in-jsx-scope */

import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { RpcStub } from 'capnweb'
import type { AiChatAuthorInfo, AuthenticatedApi, PublicApi } from '@verglas/workshop-shared/api'
import { useAuth } from './useAuth'

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true

function Probe({ publicApi }: { publicApi: RpcStub<PublicApi> }) {
  const auth = useAuth(publicApi)
  return <p>{auth.isLoading ? 'loading' : auth.isAuthenticated ? 'authenticated' : 'signed-out'}</p>
}

describe('useAuth', () => {
  let root: Root | undefined
  let container: HTMLDivElement | undefined

  beforeEach(() => {
    const values = new Map<string, string>()
    Object.defineProperty(window, 'localStorage', {
      configurable: true,
      value: {
        clear: () => values.clear(),
        getItem: (key: string) => values.get(key) ?? null,
        removeItem: (key: string) => values.delete(key),
        setItem: (key: string, value: string) => values.set(key, value),
      },
    })
  })

  afterEach(() => {
    act(() => root?.unmount())
    container?.remove()
    window.localStorage.clear()
    vi.restoreAllMocks()
  })

  it('clears a stale local session before authenticated routes can use it', async () => {
    window.localStorage.setItem('authToken', 'old-user:old-session')
    const dispose = vi.fn<() => void>()
    const authenticatedApi = Object.assign({
      whoami: vi.fn<() => Promise<AiChatAuthorInfo>>().mockRejectedValue(
        new Error('invalid session token'),
      ),
    }, { [Symbol.dispose]: dispose }) as unknown as RpcStub<AuthenticatedApi>
    const publicApi = {
      authenticate: vi.fn<() => RpcStub<AuthenticatedApi>>(() => authenticatedApi),
    } as unknown as RpcStub<PublicApi>

    container = document.createElement('div')
    document.body.append(container)
    root = createRoot(container)
    await act(async () => {
      root!.render(<Probe publicApi={publicApi} />)
      await Promise.resolve()
    })

    expect(container.textContent).toBe('signed-out')
    expect(window.localStorage.getItem('authToken')).toBeNull()
    expect(dispose).toHaveBeenCalledOnce()
  })
})
