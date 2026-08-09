import { useEffect } from 'react'

/** Reconciles server-owned state while a catalog surface is visible. */
export function useAutomaticRefresh(refresh: () => void | Promise<void>, intervalMs = 10_000) {
  useEffect(() => {
    let disposed = false
    let running = false
    const run = () => {
      if (disposed || running || document.visibilityState === 'hidden') return
      running = true
      void Promise.resolve(refresh())
        .catch(() => undefined)
        .finally(() => { running = false })
    }
    const onVisibilityChange = () => {
      if (document.visibilityState === 'visible') run()
    }
    const interval = window.setInterval(run, intervalMs)
    window.addEventListener('focus', run)
    document.addEventListener('visibilitychange', onVisibilityChange)
    return () => {
      disposed = true
      window.clearInterval(interval)
      window.removeEventListener('focus', run)
      document.removeEventListener('visibilitychange', onVisibilityChange)
    }
  }, [intervalMs, refresh])
}
