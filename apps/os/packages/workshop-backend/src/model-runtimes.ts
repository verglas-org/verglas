import type {
  ModelRuntimeDetection,
  ModelRuntimeCatalogEntry,
  ModelRuntimeId,
  ModelRuntimeLoginResult,
  ModelRuntimeWizardAnswer,
} from "@verglas/workshop-shared/api";

type RuntimeAdapterConfig = {
  endpoint: string;
  token: string;
};

type RuntimeAdapterDetection = Pick<ModelRuntimeDetection, "runtimes">;

function resolveRuntimeAdapter(env: Cloudflare.Env): RuntimeAdapterConfig | null {
  const endpoint = env.LOCAL_MODEL_RUNTIME_URL?.trim();
  const token = env.LOCAL_MODEL_RUNTIME_TOKEN?.trim();
  if (!endpoint && !token) return null;
  if (!endpoint || !token) {
    throw new Error(
      "LOCAL_MODEL_RUNTIME_URL and LOCAL_MODEL_RUNTIME_TOKEN must be configured together.",
    );
  }
  return { endpoint: endpoint.replace(/\/+$/, ""), token };
}

const RUNTIME_IDS: ModelRuntimeId[] = ["codex", "claude-code", "cursor"];

/** Backend client for the loopback model-runtime adapter started by the open-source stack. */
export class ModelRuntimeManager {
  readonly #config: RuntimeAdapterConfig | null;

  /** Creates a manager from deployment-owned native runtime configuration. */
  constructor(env: Cloudflare.Env) {
    this.#config = resolveRuntimeAdapter(env);
  }

  /** Returns host CLI availability and login state for every branded runtime. */
  async detect(): Promise<ModelRuntimeDetection> {
    if (!this.#config) {
      return {
        runtimeAvailable: false,
        runtimeError: "The native model runtime adapter is not running for this deployment.",
        runtimes: this.#emptyStatuses(),
      };
    }
    try {
      const result = await this.#request<RuntimeAdapterDetection>("GET", "/v1/runtimes");
      return { runtimeAvailable: true, runtimes: result.runtimes };
    } catch (error) {
      return {
        runtimeAvailable: false,
        runtimeError: error instanceof Error ? error.message : String(error),
        runtimes: this.#emptyStatuses(),
      };
    }
  }

  /** Starts the selected CLI's own browser-based subscription login. */
  async startLogin(runtime: ModelRuntimeId, sessionId: string): Promise<ModelRuntimeLoginResult> {
    return await this.#request(
      "POST",
      `/v1/runtimes/${encodeURIComponent(runtime)}/login`,
      { sessionId },
    );
  }

  /** Polls the active native CLI login; answers are unused by browser-owned login flows. */
  async continueLogin(
      sessionId: string, _answer?: ModelRuntimeWizardAnswer): Promise<ModelRuntimeLoginResult> {
    return await this.#request(
      "POST",
      `/v1/login-sessions/${encodeURIComponent(sessionId)}`,
    );
  }

  /** Cancels an active native CLI login process. */
  async cancelLogin(sessionId: string): Promise<void> {
    await this.#request("DELETE", `/v1/login-sessions/${encodeURIComponent(sessionId)}`);
  }

  /** Verifies that a native subscription runtime is installed and signed in. */
  async requireLinked(runtime: ModelRuntimeId): Promise<void> {
    const detection = await this.detect();
    if (!detection.runtimeAvailable) {
      throw new Error(detection.runtimeError || "The native model runtime adapter is unavailable.");
    }
    const status = detection.runtimes.find(item => item.id === runtime);
    if (!status?.available) throw new Error(status?.detail || `${runtime} is not installed.`);
    if (!status.linked) throw new Error(status.detail || `${runtime} is not signed in.`);
  }

  /** Runs a real bounded inference before a runtime is exposed in the model picker. */
  async verifyLinked(runtime: ModelRuntimeId, model: string, apiToken?: string): Promise<void> {
    if (!apiToken) await this.requireLinked(runtime);
    await this.#request(
      "POST",
      `/v1/runtimes/${encodeURIComponent(runtime)}/verify`,
      { model, ...(apiToken ? { apiToken } : {}) },
      60_000,
    );
  }

  /** Lists the provider-owned models currently available through a native runtime. */
  async listModels(runtime: ModelRuntimeId, apiToken?: string): Promise<ModelRuntimeCatalogEntry[]> {
    const result = await this.#request<{models: ModelRuntimeCatalogEntry[]}>(
      apiToken ? "POST" : "GET",
      `/v1/runtimes/${encodeURIComponent(runtime)}/models`,
      apiToken ? {apiToken} : undefined,
      30_000,
    );
    return result.models;
  }

  async #request<T>(method: string, path: string, body?: unknown, timeoutMs = 15_000): Promise<T> {
    if (!this.#config) throw new Error("The native model runtime adapter is not configured.");
    const response = await fetch(`${this.#config.endpoint}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${this.#config.token}`,
        ...(body === undefined ? {} : { "Content-Type": "application/json" }),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.timeout(timeoutMs),
    });
    const result = await response.json() as T & { error?: string };
    if (!response.ok) {
      throw new Error(result.error || `Native runtime request failed: HTTP ${response.status}`);
    }
    return result;
  }

  #emptyStatuses(): ModelRuntimeDetection["runtimes"] {
    return RUNTIME_IDS.map(id => ({
      id,
      available: false,
      linked: false,
      detail: "Native runtime status is unavailable.",
      supportsGuidedLogin: false,
    }));
  }
}
