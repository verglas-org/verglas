//! Cloudflare Workers-shaped management client for a celld host.
//!
//! Routes are intentionally account-prefix-free because celld already scopes the
//! listener to one tenant cell. The client unwraps Cloudflare response envelopes
//! and preserves structured API errors for callers.

/** A message returned in a Cloudflare-style success or error envelope. */
export interface WorkersManagementMessage {
  /** Stable HTTP/API error code when supplied by celld. */
  code: number | string;
  /** Human-readable message. */
  message: string;
}

/** Error thrown when a celld management request is not successful. */
export class WorkersManagementError extends Error {
  /** Structured errors exactly as returned by the API. */
  readonly errors: WorkersManagementMessage[];
  /** HTTP status observed for the failed request. */
  readonly status: number;

  /** Creates a typed management failure with the API error list. */
  constructor(errors: WorkersManagementMessage[], status: number) {
    const message = errors.map((error) => error.message).join("; ") || `management request failed with status ${status}`;
    super(message);
    this.name = "WorkersManagementError";
    this.errors = errors;
    this.status = status;
  }
}

/** Script metadata accepted by the celld multipart upload route. */
export interface WorkerScriptMetadata {
  /** Module file that is the Worker entrypoint. */
  main_module: string;
  /** Opaque binding metadata owned by the Worker management API. */
  bindings: Record<string, unknown>[];
}

/** Source accepted for one multipart Worker module. */
export type WorkerModuleSource = string | Blob | ArrayBuffer | ArrayBufferView;

/** Complete module-syntax script upload request. */
export interface WorkerScriptUpload extends WorkerScriptMetadata {
  /** Module path to source content. */
  modules: Record<string, WorkerModuleSource>;
}

/** Alternate upload shape separating the metadata part from module parts. */
export interface WorkerScriptUploadParts {
  /** Metadata JSON sent as the multipart metadata part. */
  metadata: WorkerScriptMetadata;
  /** Module path to source content. */
  modules: Record<string, WorkerModuleSource>;
}

/** Script metadata returned by the celld management API. */
export interface WorkerScript extends WorkerScriptMetadata {
  /** Stable script identifier. */
  id: string;
  /** Script name in the management path. */
  name: string;
  /** Stored module path names. */
  modules: string[];
}

/** Constructor fetch seam for browsers, Node, and captured-fetch tests. */
export type ManagementFetch = typeof fetch;

interface WorkersEnvelope<T> {
  success: boolean;
  errors?: WorkersManagementMessage[];
  messages?: WorkersManagementMessage[];
  result?: T | null;
}

/** Builds the module-syntax multipart body expected by celld. */
export function buildWorkerScriptFormData(upload: WorkerScriptUpload | WorkerScriptUploadParts): FormData {
  const form = new FormData();
  const metadata: WorkerScriptMetadata = "metadata" in upload
    ? upload.metadata
    : { main_module: upload.main_module, bindings: upload.bindings };
  form.append("metadata", new Blob([JSON.stringify(metadata)], { type: "application/json" }));
  for (const [moduleName, source] of Object.entries(upload.modules)) {
    form.append(moduleName, moduleBlob(source), moduleName);
  }
  return form;
}

/** Short alias for callers that name the multipart helper after the script form. */
export const buildScriptFormData = buildWorkerScriptFormData;

/** Client for the account-prefix-free celld Workers management API. */
export class WorkersManagementClient {
  readonly #baseUrl: string;
  readonly #fetch: ManagementFetch;

  /** Binds the client to one celld base URL and optional fetch implementation. */
  constructor(baseUrl: string, fetchImpl: ManagementFetch = globalThis.fetch) {
    if (!baseUrl) throw new Error("WorkersManagementClient: baseUrl is required");
    if (typeof fetchImpl !== "function") throw new Error("WorkersManagementClient: fetch implementation is required");
    this.#baseUrl = baseUrl.replace(/\/+$/, "");
    this.#fetch = fetchImpl;
  }

  /** Uploads one Worker script using Cloudflare module-syntax multipart fields. */
  uploadScript(name: string, upload: WorkerScriptUpload | WorkerScriptUploadParts): Promise<WorkerScript> {
    return this.request<WorkerScript>("PUT", `/workers/scripts/${segment(name)}`, buildWorkerScriptFormData(upload));
  }

  /** Lists all stored Worker scripts. */
  listScripts(): Promise<WorkerScript[]> {
    return this.request<WorkerScript[]>("GET", "/workers/scripts");
  }

  /** Returns one stored Worker script. */
  getScript(name: string): Promise<WorkerScript> {
    return this.request<WorkerScript>("GET", `/workers/scripts/${segment(name)}`);
  }

  /** Deletes one stored Worker script and returns the API result. */
  deleteScript(name: string): Promise<boolean> {
    return this.request<boolean>("DELETE", `/workers/scripts/${segment(name)}`);
  }

  /** Sends one request and unwraps its Cloudflare response envelope. */
  private async request<T>(method: string, path: string, body?: BodyInit | object): Promise<T> {
    const init: RequestInit = { method };
    if (body instanceof FormData || typeof body === "string" || body instanceof Blob) {
      init.body = body;
    } else if (body !== undefined) {
      init.body = JSON.stringify(body);
      init.headers = { "content-type": "application/json" };
    }
    const response = await this.#fetch(`${this.#baseUrl}${path}`, init);
    let envelope: WorkersEnvelope<T>;
    try {
      envelope = await response.json() as WorkersEnvelope<T>;
    } catch {
      throw new WorkersManagementError(
        [{ code: response.status, message: `management API returned non-JSON status ${response.status}` }],
        response.status,
      );
    }
    if (!response.ok || envelope.success !== true) {
      throw new WorkersManagementError(envelope.errors ?? [], response.status);
    }
    return envelope.result as T;
  }
}

/** Converts one supported module source into a Blob without Node-only APIs. */
function moduleBlob(source: WorkerModuleSource): Blob {
  if (source instanceof Blob) return source;
  if (typeof source === "string") return new Blob([source], { type: "application/javascript" });
  if (source instanceof ArrayBuffer) return new Blob([source]);
  const bytes = new Uint8Array(source.byteLength);
  bytes.set(new Uint8Array(source.buffer, source.byteOffset, source.byteLength));
  return new Blob([bytes.buffer]);
}

/** Escapes one path segment without allowing names to alter route structure. */
function segment(value: string): string {
  return encodeURIComponent(value);
}
