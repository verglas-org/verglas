import { useState, useEffect, useRef } from 'react'
import { RpcStub } from 'capnweb'
import { PublicApi, AuthenticatedApi } from '@verglas/workshop-shared/api'

const CF_ACCESS_MODE = import.meta.env.VITE_CF_ACCESS_MODE === 'true'

interface AuthState {
  token: string | null
  authenticatedApi: RpcStub<AuthenticatedApi> | null
  isLoading: boolean
  error: string | null
}

export { CF_ACCESS_MODE }

export function useAuth(publicApi: RpcStub<PublicApi>) {
  const [authState, setAuthState] = useState<AuthState>({
    token: null,
    authenticatedApi: null,
    isLoading: true,
    error: null
  })

  // Track current authenticated API stub for cleanup on unmount.
  // State closures go stale in cleanup functions, so we use a ref.
  const authenticatedApiRef = useRef<RpcStub<AuthenticatedApi> | null>(null)
  const pendingApiRef = useRef<RpcStub<AuthenticatedApi> | null>(null)
  const authenticationAttemptRef = useRef(0)
  authenticatedApiRef.current = authState.authenticatedApi

  useEffect(() => {
    if (CF_ACCESS_MODE) {
      authenticateWithCfAccess()
    } else {
      const storedToken = localStorage.getItem('authToken')
      if (storedToken) {
        authenticateWithToken(storedToken)
      } else {
        setAuthState(prev => ({ ...prev, isLoading: false }))
      }
    }
    return () => {
      authenticationAttemptRef.current += 1
      pendingApiRef.current?.[Symbol.dispose]()
      pendingApiRef.current = null
      // The authenticateWithXxx functions also dispose the old stub via their setAuthState
      // updater, so this may double-dispose on reconnect. That's fine — dispose is idempotent.
      authenticatedApiRef.current?.[Symbol.dispose]()
    }
  }, [publicApi])

  const authenticateWithCfAccess = () => {
    setAuthState(prev => {
      if (prev.authenticatedApi) {
        prev.authenticatedApi[Symbol.dispose]()
      }
      return { ...prev, authenticatedApi: null, isLoading: true, error: null }
    })

    const attempt = ++authenticationAttemptRef.current
    const authenticatedApi = publicApi.authenticateFromCfAccess()
    pendingApiRef.current = authenticatedApi
    authenticatedApi.whoami().then(() => {
      if (authenticationAttemptRef.current !== attempt) {
        authenticatedApi[Symbol.dispose]()
        return
      }
      pendingApiRef.current = null
      setAuthState({ token: null, authenticatedApi, isLoading: false, error: null })
    }).catch((err: unknown) => {
      authenticatedApi[Symbol.dispose]()
      if (authenticationAttemptRef.current !== attempt) return
      pendingApiRef.current = null
      setAuthState({
        token: null,
        authenticatedApi: null,
        isLoading: false,
        error: err instanceof Error ? err.message : 'Authentication failed'
      })
    })
  }

  const authenticateWithToken = (token: string) => {
    setAuthState(prev => {
      // Dispose the previous authenticated API stub if it exists
      if (prev.authenticatedApi) {
        prev.authenticatedApi[Symbol.dispose]()
      }
      return {
        ...prev,
        authenticatedApi: null, // Clear the disposed stub
        isLoading: true,
        error: null
      }
    })

    const attempt = ++authenticationAttemptRef.current
    const authenticatedApi = publicApi.authenticate(token)
    pendingApiRef.current = authenticatedApi
    authenticatedApi.whoami().then(() => {
      if (authenticationAttemptRef.current !== attempt) {
        authenticatedApi[Symbol.dispose]()
        return
      }
      pendingApiRef.current = null
      setAuthState({ token, authenticatedApi, isLoading: false, error: null })
    }).catch(() => {
      authenticatedApi[Symbol.dispose]()
      if (authenticationAttemptRef.current !== attempt) return
      pendingApiRef.current = null
      localStorage.removeItem('authToken')
      setAuthState({ token: null, authenticatedApi: null, isLoading: false, error: null })
    })
  }

  const login = (token: string) => {
    authenticateWithToken(token)
  }

  const logout = () => {
    authenticationAttemptRef.current += 1
    pendingApiRef.current?.[Symbol.dispose]()
    pendingApiRef.current = null
    // Use functional updater to read current state (avoids stale closure).
    setAuthState(prev => {
      if (prev.authenticatedApi) {
        prev.authenticatedApi[Symbol.dispose]()
      }
      return {
        token: null,
        authenticatedApi: null,
        isLoading: false,
        error: null
      }
    })

    if (!CF_ACCESS_MODE) {
      localStorage.removeItem('authToken')
    }
  }

  return {
    ...authState,
    login,
    logout,
    isAuthenticated: !!authState.authenticatedApi
  }
}
