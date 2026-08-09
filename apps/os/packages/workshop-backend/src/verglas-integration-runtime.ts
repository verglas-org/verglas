import { connectRuntime, type VerglasRuntimeClient } from "@verglas/sdk";
import type {
  IntegrationSetupInstruction,
  IntegrationVerification,
  SourceConfigurationField,
} from "@verglas/workshop-shared/api";

/** Environment required to deploy generated Integration Vessels locally. */
export interface VerglasIntegrationRuntimeEnv {
  VERGLAS_CONTAINER_RUNTIME_URL?: string;
  VERGLAS_CONTAINER_RUNTIME_TOKEN?: string;
  VERGLAS_DATA_ENDPOINT?: string;
  VERGLAS_DATA_TOKEN?: string;
  VERGLAS_INTEGRATION_RUNTIME_IMAGE?: string;
}

/** Immutable generated Integration definition passed to its generic runtime. */
export interface GeneratedIntegrationDeployment {
  name: string;
  title: string;
  description: string;
  module: string;
  instructions: IntegrationSetupInstruction[];
  fields: SourceConfigurationField[];
}

/** Immutable standalone TypeScript Application project sent to the Verglas builder. */
export interface GeneratedApplicationDeployment {
  name: string;
  files: Record<string, string>;
}

/** Complete compositional Vessel source submitted through one atomic runtime apply. */
export interface GeneratedVesselDeployment {
  /** Stable name matching the YAML manifest. */
  name: string;
  /** Portable `verglas.io/v1alpha1` Vessel YAML. */
  manifest: string;
  /** Component projects keyed by each manifest-relative `project` path. */
  projects: Record<string, {files: Record<string, string>}>;
}

/** Integration component resolved from a compositional Vessel release. */
export interface AppliedVesselIntegration {
  name: string;
  version: string;
  runtimeName: string;
  config: {
    fields: Array<{
      name: string;
      label: string;
      type: SourceConfigurationField["type"];
      required: boolean;
      default?: unknown;
      help?: string;
      placeholder?: string;
    }>;
    setup: IntegrationSetupInstruction[];
  };
}

/** Non-secret result of applying one complete Vessel release. */
export interface AppliedVesselResult {
  name: string;
  version: string;
  digest: string;
  integrations: AppliedVesselIntegration[];
  interfaceRuntime: string;
  previewUrl: string;
  outcome: "created" | "upgraded" | "unchanged";
}

type IntegrationRuntimeResult = {
  configured: boolean;
  verification?: IntegrationVerification;
  error?: string;
};

/** Runtime status returned by an Integration Vessel's GET /v1/status (non-secret). */
export type IntegrationRuntimeStatus = {
  configured?: boolean;
  running?: boolean;
  verification?: IntegrationVerification;
  lastVerify?: Record<string, string | number | boolean | null>;
  recentVerifyAttempts?: Array<Record<string, string | number | boolean | null>>;
};

/**
 * Failed live verification that still carries the structured IntegrationVerification payload.
 * Callers must persist `verification` (including `details`) instead of only the Error message.
 */
export class IntegrationVerificationFailed extends Error {
  readonly verification: IntegrationVerification;

  constructor(verification: IntegrationVerification, fallbackMessage?: string) {
    super(verification.message || fallbackMessage || "Integration verification failed");
    this.name = "IntegrationVerificationFailed";
    this.verification = verification;
  }
}

/** Validates the executable surface expected by the generic Integration runtime. */
export function validateGeneratedIntegrationModule(module: string): void {
  if (module.length === 0 || module.length > 256 * 1024) {
    throw new Error("An Integration module must contain between 1 byte and 256 KiB.");
  }
  if (!/export\s+default\s+\{/.test(module)) {
    throw new Error("An Integration module must default-export an object literal.");
  }
  if (!/\bverify\s*\(/.test(module)) {
    throw new Error("An Integration module must implement verify(ctx).");
  }
  if (!/\b(?:start|fetch)\s*\(/.test(module)) {
    throw new Error("An Integration module must implement start(ctx) or fetch(request, ctx).");
  }
  if (/\b(?:eval|Function)\s*\(/.test(module)) {
    throw new Error("Generated Integration modules cannot evaluate additional source text.");
  }
}

/** Deploys, configures, and verifies generated Integration Vessels via @verglas/sdk. */
export class VerglasIntegrationRuntimeClient {
  readonly #runtime: VerglasRuntimeClient;
  readonly #dataEndpoint: string;
  readonly #dataToken: string;
  readonly #image: string;

  /** Resolves an all-or-nothing local Integration runtime configuration. */
  constructor(env: VerglasIntegrationRuntimeEnv, fetcher: typeof fetch = fetch) {
    const runtimeEndpoint = env.VERGLAS_CONTAINER_RUNTIME_URL?.trim();
    const runtimeToken = env.VERGLAS_CONTAINER_RUNTIME_TOKEN?.trim();
    const dataEndpoint = env.VERGLAS_DATA_ENDPOINT?.trim();
    const dataToken = env.VERGLAS_DATA_TOKEN?.trim();
    if (!runtimeEndpoint || !runtimeToken || !dataEndpoint || !dataToken) {
      throw new Error(
        "Generated Integrations require VERGLAS_CONTAINER_RUNTIME_URL, " +
        "VERGLAS_CONTAINER_RUNTIME_TOKEN, VERGLAS_DATA_ENDPOINT, and VERGLAS_DATA_TOKEN.",
      );
    }
    this.#runtime = connectRuntime({
      endpoint: runtimeEndpoint,
      token: runtimeToken,
      fetch: fetcher,
    });
    this.#dataEndpoint = dataEndpoint.replace(/\/+$/, "");
    this.#dataToken = dataToken;
    this.#image = env.VERGLAS_INTEGRATION_RUNTIME_IMAGE?.trim() ||
      "verglas/verglas-container-runtime:local";
  }

  /** Creates one isolated Integration Vessel from generated source and setup metadata. */
  async deploy(deployment: GeneratedIntegrationDeployment): Promise<void> {
    validateGeneratedIntegrationModule(deployment.module);
    const definition = {
      title: deployment.title,
      description: deployment.description,
      instructions: deployment.instructions,
      fields: deployment.fields,
    };
    await this.#runtime.putVessel(deployment.name, {
      name: deployment.name,
      role: "integration",
      image: this.#image,
      entrypoint: [
        "/usr/local/bin/bun",
        "/opt/verglas-integration-runtime/runtime.mjs",
      ],
      environment: {
        VERGLAS_INTEGRATION_NAME: deployment.name,
        VERGLAS_INTEGRATION_PORT: "8370",
        VERGLAS_INTEGRATION_MODULE: encodeBase64(deployment.module),
        VERGLAS_INTEGRATION_DEFINITION: encodeBase64(JSON.stringify(definition)),
        VERGLAS_DATA_ENDPOINT: this.#dataEndpoint,
        VERGLAS_DATA_TOKEN: this.#dataToken,
      },
      http: {port: 8370, healthPath: "/health"},
    });
    await this.#waitForHealth(deployment.name);
  }

  /** Builds and starts one standalone TypeScript Application Vessel. */
  async deployApplication(deployment: GeneratedApplicationDeployment): Promise<string> {
    validateApplicationProject(deployment.files);
    await this.#runtime.putVesselProject(deployment.name, {
      name: deployment.name,
      role: "application",
      project: {files: deployment.files},
      environment: {
        VERGLAS_DATA_ENDPOINT: this.#dataEndpoint,
        VERGLAS_DATA_TOKEN: this.#dataToken,
        PORT: "8380",
      },
      http: {port: 8380, healthPath: "/health"},
    });
    await this.#waitForHealth(deployment.name);
    return this.#runtime.previewUrl(deployment.name);
  }

  /** Builds and reconciles every component of one versioned Vessel release atomically. */
  async deployVessel(deployment: GeneratedVesselDeployment): Promise<AppliedVesselResult> {
    if (!deployment.name.trim() || !deployment.manifest.trim()) {
      throw new Error("A Vessel requires a name and manifest.");
    }
    const result = await this.#runtime.putVesselComposition(deployment.name, {
      manifest: deployment.manifest,
      projects: deployment.projects,
      dataEndpoint: this.#dataEndpoint,
      dataToken: this.#dataToken,
    }) as AppliedVesselResult;
    return {
      ...result,
      previewUrl: new URL(result.previewUrl, `${this.#runtime.endpoint}/`).toString(),
    };
  }

  /** Stores user configuration and returns the mandatory live verification result. */
  async configure(name: string, values: Record<string, string>): Promise<IntegrationVerification> {
    const response = await this.#runtime.fetch(
      `/v1/vessels/${encodeURIComponent(name)}/http/v1/config`,
      {method: "PUT", headers: {"content-type": "application/json"}, body: JSON.stringify(values)},
    );
    return await verificationResult(response, "configure and verify Integration");
  }

  /** Exercises the Integration's live verifier without changing configuration. */
  async test(name: string): Promise<IntegrationVerification> {
    const response = await this.#runtime.fetch(
      `/v1/vessels/${encodeURIComponent(name)}/http/v1/test`,
      {method: "POST"},
    );
    return await verificationResult(response, "test Integration");
  }

  /** Best-effort health probe for an Integration Vessel HTTP surface. */
  async health(name: string): Promise<{ok: boolean; status: number; body?: string}> {
    const response = await this.#runtime.fetch(
      `/v1/vessels/${encodeURIComponent(name)}/http/health`,
      {method: "GET"},
    );
    const body = (await response.text()).slice(0, 500);
    return {ok: response.ok, status: response.status, body: body || undefined};
  }

  /** Non-secret runtime status including recent verify diagnostics when the Vessel supports them. */
  async status(name: string): Promise<IntegrationRuntimeStatus> {
    const response = await this.#runtime.fetch(
      `/v1/vessels/${encodeURIComponent(name)}/http/v1/status`,
      {method: "GET"},
    );
    if (!response.ok) {
      return {configured: false};
    }
    return await response.json() as IntegrationRuntimeStatus;
  }

  /** Stops and removes a Vessel container from the local runtime. */
  async deleteVessel(name: string): Promise<void> {
    const response = await this.#runtime.fetch(
      `/v1/vessels/${encodeURIComponent(name)}`,
      {method: "DELETE"},
    );
    if (response.status === 404) return;
    await expectSuccess(response, "delete Vessel");
  }

  async #waitForHealth(name: string): Promise<void> {
    let lastStatus = 0;
    for (let attempt = 0; attempt < 60; attempt++) {
      const response = await this.#runtime.fetch(
        `/v1/vessels/${encodeURIComponent(name)}/http/health`,
        {method: "GET"},
      );
      lastStatus = response.status;
      if (response.ok) return;
      await new Promise(resolve => setTimeout(resolve, 250));
    }
    throw new Error(`Integration Vessel did not become healthy; last HTTP status was ${lastStatus}.`);
  }
}

/** Enforces the minimum standalone project contract before invoking a Docker build. */
export function validateApplicationProject(files: Record<string, string>): void {
  const packageJson = files["package.json"];
  if (!packageJson) throw new Error("An Application project must include package.json.");
  let packageDefinition: {scripts?: {start?: unknown}};
  try {
    packageDefinition = JSON.parse(packageJson) as {scripts?: {start?: unknown}};
  } catch {
    throw new Error("Application package.json must be valid JSON.");
  }
  if (typeof packageDefinition.scripts?.start !== "string") {
    throw new Error("Application package.json must define scripts.start.");
  }
  if (!Object.keys(files).some(path => /^src\/.*\.(?:ts|tsx)$/.test(path))) {
    throw new Error("An Application project must include TypeScript source under src/.");
  }
}

function encodeBase64(value: string): string {
  let binary = "";
  for (const byte of new TextEncoder().encode(value)) binary += String.fromCodePoint(byte);
  return btoa(binary);
}

/** Exported for unit tests — parses a configure/test JSON body into a verification result. */
export function parseIntegrationVerificationResult(
  result: IntegrationRuntimeResult,
  operation: string,
  responseOk: boolean,
): IntegrationVerification {
  const verification = result.verification;
  if (responseOk && verification?.ok) return verification;
  const failed: IntegrationVerification = verification && typeof verification === "object"
    ? {
      ok: false,
      message: verification.message || result.error || `Verglas failed to ${operation}`,
      testedAt: verification.testedAt || new Date().toISOString(),
      ...(verification.latencyMs !== undefined ? {latencyMs: verification.latencyMs} : {}),
      ...(verification.details ? {details: verification.details} : {}),
    }
    : {
      ok: false,
      message: result.error || `Verglas failed to ${operation}`,
      testedAt: new Date().toISOString(),
    };
  throw new IntegrationVerificationFailed(failed);
}

async function verificationResult(response: Response, operation: string): Promise<IntegrationVerification> {
  const result = await response.json() as IntegrationRuntimeResult;
  return parseIntegrationVerificationResult(result, operation, response.ok);
}

async function expectSuccess(response: Response, operation: string): Promise<void> {
  if (response.ok) return;
  const detail = (await response.text()).slice(0, 1000);
  throw new Error(`Verglas failed to ${operation}: HTTP ${response.status}${detail ? ` — ${detail}` : ""}`);
}
