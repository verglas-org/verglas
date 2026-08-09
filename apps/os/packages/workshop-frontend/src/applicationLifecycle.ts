/**
 * Application containers are directly manageable only when the deployment has
 * explicitly advertised its local OSS container runtime.
 */
export function applicationLifecycleAvailable(localContainerRuntime: boolean | undefined): boolean {
  return localContainerRuntime === true
}
