/**
 * Env → official @verglas/sdk control clients. Workshop must not hand-roll
 * fetch to VERGLAS_* /v1 paths; go through these factories.
 */
import {
  connectAccess,
  connectAdmin,
  connectRuntime,
  connectScheduler,
  type VerglasAccessClient,
  type VerglasAdminClient,
  type VerglasRuntimeClient,
  type VerglasSchedulerClient,
} from "@verglas/sdk";

/** Env vars used to construct Verglas SDK control clients. */
export type VerglasClientEnv = {
  VERGLAS_ACCESS_URI?: string;
  VERGLAS_ADMIN_URL?: string;
  VERGLAS_SCHEDULER_URL?: string;
  VERGLAS_SCHEDULER_CONTROL_TOKEN?: string;
  VERGLAS_CONTAINER_RUNTIME_URL?: string;
  VERGLAS_CONTAINER_RUNTIME_TOKEN?: string;
  VERGLAS_DATA_ENDPOINT?: string;
};

/** Mandatory tenant access service for dynamic database resources. */
export function verglasDatabaseAccess(
  env: VerglasClientEnv,
  token: string,
  fetcher?: typeof fetch,
): VerglasAccessClient {
  const endpoint = env.VERGLAS_ACCESS_URI?.trim();
  if (!endpoint) throw new Error("VERGLAS_ACCESS_URI is not configured.");
  if (!token.trim())
    throw new Error("A user-scoped Verglas access token is required.");
  return connectAccess({ endpoint, token, fetch: fetcher });
}

/** Database-scoped data-query and catalog listener. */
export function verglasAdmin(
  env: VerglasClientEnv,
  token: string,
  fetcher?: typeof fetch,
): VerglasAdminClient {
  const endpoint = env.VERGLAS_ADMIN_URL?.trim();
  if (!endpoint)
    throw new Error("The local Verglas data endpoint is not configured.");
  if (!token.trim())
    throw new Error("A user-scoped Verglas data token is required.");
  return connectAdmin({ endpoint, token, fetch: fetcher });
}

/** Scheduler control (worker registry, runs, secrets, and job history). */
export function verglasScheduler(
  env: VerglasClientEnv,
  fetcher?: typeof fetch,
): VerglasSchedulerClient {
  const endpoint = env.VERGLAS_SCHEDULER_URL?.trim();
  const token = env.VERGLAS_SCHEDULER_CONTROL_TOKEN?.trim();
  if (!endpoint || !token) {
    throw new Error(
      "VERGLAS_SCHEDULER_URL and VERGLAS_SCHEDULER_CONTROL_TOKEN must be configured together.",
    );
  }
  return connectScheduler({ endpoint, token, fetch: fetcher });
}

/** Local container runtime (Vessels). */
export function verglasRuntime(
  env: VerglasClientEnv,
  fetcher?: typeof fetch,
): VerglasRuntimeClient {
  const endpoint = env.VERGLAS_CONTAINER_RUNTIME_URL?.trim();
  const token = env.VERGLAS_CONTAINER_RUNTIME_TOKEN?.trim();
  if (!endpoint || !token) {
    throw new Error(
      "VERGLAS_CONTAINER_RUNTIME_URL and VERGLAS_CONTAINER_RUNTIME_TOKEN must be configured together.",
    );
  }
  return connectRuntime({ endpoint, token, fetch: fetcher });
}

/** True when the local container runtime endpoint and credential are configured together. */
export function resolveLocalContainerRuntimeConfigured(
  env: VerglasClientEnv,
): boolean {
  const endpoint = Boolean(env.VERGLAS_CONTAINER_RUNTIME_URL?.trim());
  const token = Boolean(env.VERGLAS_CONTAINER_RUNTIME_TOKEN?.trim());
  if (endpoint !== token) {
    throw new Error(
      "VERGLAS_CONTAINER_RUNTIME_URL and VERGLAS_CONTAINER_RUNTIME_TOKEN must be configured together.",
    );
  }
  return endpoint;
}

/** True when scheduler worker control is configured. */
export function resolveWorkerControlConfigured(env: VerglasClientEnv): boolean {
  const sched = Boolean(env.VERGLAS_SCHEDULER_URL?.trim());
  const token = Boolean(env.VERGLAS_SCHEDULER_CONTROL_TOKEN?.trim());
  if (!sched && !token) return false;
  if (!sched || !token) {
    throw new Error(
      "VERGLAS_SCHEDULER_URL and VERGLAS_SCHEDULER_CONTROL_TOKEN must be configured together.",
    );
  }
  return true;
}
