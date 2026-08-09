/**
 * Application containers are directly manageable only when the deployment has
 * explicitly advertised its local OSS container runtime.
 */
export function applicationLifecycleAvailable(localContainerRuntime: boolean | undefined): boolean {
  return localContainerRuntime === true
}

/** Chooses the next persisted lifecycle intent from the runtime's desired state. */
export function nextApplicationLifecycleState(running: boolean | undefined): 'running' | 'stopped' {
  return running === false ? 'running' : 'stopped'
}
