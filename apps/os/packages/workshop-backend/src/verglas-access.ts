import type {
  VerglasAccessAction,
  VerglasAccessGrant,
  VerglasAccessGrantInput,
  VerglasAccessIdentity,
  VerglasAccessPrincipal,
  VerglasAccessResource,
  VerglasAccessSnapshot,
  VerglasPrincipalKind,
  VerglasResourceKind,
} from "@verglas/workshop-shared/api";

/** Server-only configuration for the tenant authorization service. */
export interface VerglasAccessEnv {
  VERGLAS_ACCESS_URI?: string;
  VERGLAS_ACCESS_SERVICE_TOKEN?: string;
  VERGLAS_TENANT_ID?: string;
  VERGLAS_LOCAL_OWNER_BOOTSTRAP?: string;
}

type PrincipalWire = {
  tenant_id: string;
  id: string;
  kind: VerglasPrincipalKind;
  parent_id?: string;
};

type ResourceWire = {
  tenant_id: string;
  id: string;
  kind: VerglasResourceKind;
  parent_id?: string;
};

type GrantWire = {
  id: string;
  tenant_id: string;
  principal_id: string;
  resource_id: string;
  actions: VerglasAccessAction[];
};

type DecisionWire = {
  allowed: boolean;
  policy_version: number;
};

/** Resolved all-or-nothing access-service configuration. */
export type VerglasAccessConfig = {
  endpoint: string;
  serviceToken: string;
  tenantId: string;
  bootstrapLocalOwners: boolean;
};

/** Resolves the mandatory pair of access-service endpoint and backend-only credential. */
export function resolveVerglasAccessConfig(env: VerglasAccessEnv): VerglasAccessConfig | null {
  const endpoint = env.VERGLAS_ACCESS_URI?.trim();
  const serviceToken = env.VERGLAS_ACCESS_SERVICE_TOKEN?.trim();
  if (!endpoint && !serviceToken) return null;
  if (!endpoint || !serviceToken) {
    throw new Error("VERGLAS_ACCESS_URI and VERGLAS_ACCESS_SERVICE_TOKEN must be configured together.");
  }
  return {
    endpoint: endpoint.replace(/\/+$/, ""),
    serviceToken,
    tenantId: env.VERGLAS_TENANT_ID?.trim() || "local",
    bootstrapLocalOwners: env.VERGLAS_LOCAL_OWNER_BOOTSTRAP === "true",
  };
}

/** Maps an authenticated OS account to one stable tenant principal identifier. */
export function userPrincipalId(userId: string): string {
  return `user/${encodeURIComponent(userId.toLowerCase())}`;
}

/** Trusted Workshop adapter for identity bootstrap, decisions, and delegated grants. */
export class VerglasAccessClient {
  readonly #config: VerglasAccessConfig;
  readonly #fetch: typeof fetch;

  constructor(config: VerglasAccessConfig, fetcher: typeof fetch = fetch) {
    this.#config = config;
    this.#fetch = fetcher.bind(globalThis);
  }

  /** Ensures an OS user exists in the tenant and returns their effective tenant-root identity. */
  async ensureUser(userId: string): Promise<VerglasAccessIdentity> {
    const principalId = userPrincipalId(userId);
    await this.#ensure("/v1/access/principals", {
      tenant_id: this.#config.tenantId,
      id: principalId,
      kind: "user",
    });
    await this.#ensure("/v1/access/resources", {
      tenant_id: this.#config.tenantId,
      id: "tenant",
      kind: "tenant",
    });
    if (this.#config.bootstrapLocalOwners) {
      await this.#ensure("/v1/access/grants", {
        id: `local-owner/${encodeURIComponent(userId.toLowerCase())}`,
        tenant_id: this.#config.tenantId,
        principal_id: principalId,
        resource_id: "tenant",
        actions: ["own"],
      });
    }
    const decision = await this.#request<DecisionWire>("/v1/access/check", {
      method: "POST",
      body: JSON.stringify({
        tenant_id: this.#config.tenantId,
        principal_id: principalId,
        resource_id: "tenant",
        action: "own",
      }),
    });
    return {
      tenantId: this.#config.tenantId,
      principalId,
      tenantOwner: decision.allowed,
      policyVersion: decision.policy_version,
    };
  }

  /** Idempotently registers a process identity discovered by the OS. */
  async ensurePrincipal(id: string, kind: VerglasPrincipalKind, parentId?: string): Promise<void> {
    await this.#ensure("/v1/access/principals", {
      tenant_id: this.#config.tenantId,
      id,
      kind,
      ...(parentId ? {parent_id: parentId} : {}),
    });
  }

  /** Idempotently registers a protected resource beneath the tenant root. */
  async ensureResource(id: string, kind: VerglasResourceKind, parentId = "tenant"): Promise<void> {
    await this.#ensure("/v1/access/resources", {
      tenant_id: this.#config.tenantId,
      id,
      kind,
      ...(parentId ? {parent_id: parentId} : {}),
    });
  }

  /** Returns the bounded tenant registry used by the local permissions UI. */
  async snapshot(): Promise<VerglasAccessSnapshot> {
    const query = `?tenant_id=${encodeURIComponent(this.#config.tenantId)}`;
    const [principals, resources, grants] = await Promise.all([
      this.#request<PrincipalWire[]>(`/v1/access/principals${query}`),
      this.#request<ResourceWire[]>(`/v1/access/resources${query}`),
      this.#request<GrantWire[]>(`/v1/access/grants${query}`),
    ]);
    return {
      tenantId: this.#config.tenantId,
      principals: principals.map(mapPrincipal),
      resources: resources.map(mapResource),
      grants: grants.map(mapGrant),
    };
  }

  /** Evaluates one action on a registered resource for the mapped OS user. */
  async checkUser(userId: string, resourceId: string, action: VerglasAccessAction): Promise<boolean> {
    return await this.checkPrincipal(userPrincipalId(userId), resourceId, action);
  }

  /** Evaluates one action for any registered human or process principal. */
  async checkPrincipal(
      principalId: string, resourceId: string, action: VerglasAccessAction): Promise<boolean> {
    const decision = await this.#request<DecisionWire>("/v1/access/check", {
      method: "POST",
      body: JSON.stringify({
        tenant_id: this.#config.tenantId,
        principal_id: principalId,
        resource_id: resourceId,
        action,
      }),
    });
    return decision.allowed;
  }

  /** Delegates only actions the approving user already holds and is allowed to pass. */
  async delegate(actorUserId: string, input: VerglasAccessGrantInput): Promise<VerglasAccessGrant> {
    if (input.actions.length === 0) throw new Error("At least one access action is required.");
    const wire = await this.#request<GrantWire>("/v1/access/delegations", {
      method: "POST",
      body: JSON.stringify({
        actor_principal_id: userPrincipalId(actorUserId),
        grant: {
          id: `delegated/${crypto.randomUUID()}`,
          tenant_id: this.#config.tenantId,
          principal_id: input.principalId,
          resource_id: input.resourceId,
          actions: [...new Set(input.actions)],
        },
      }),
    });
    return mapGrant(wire);
  }

  /** Revokes one explicit grant through the trusted access service. */
  async revoke(actorUserId: string, grantId: string): Promise<void> {
    await this.#request("/v1/access/revocations", {
      method: "POST",
      body: JSON.stringify({
        tenant_id: this.#config.tenantId,
        actor_principal_id: userPrincipalId(actorUserId),
        grant_id: grantId,
      }),
    });
  }

  async #ensure(path: string, body: unknown): Promise<void> {
    const response = await this.#fetch(`${this.#config.endpoint}${path}`, {
      method: "POST",
      headers: this.#headers(),
      body: JSON.stringify(body),
    });
    if (response.ok || response.status === 409) return;
    throw await accessError(path, response);
  }

  async #request<T = unknown>(path: string, init: RequestInit = {}): Promise<T> {
    const response = await this.#fetch(`${this.#config.endpoint}${path}`, {
      ...init,
      headers: this.#headers(init.headers),
    });
    if (!response.ok) throw await accessError(path, response);
    if (response.status === 204) return undefined as T;
    return await response.json<T>();
  }

  #headers(existing?: HeadersInit): Headers {
    const headers = new Headers(existing);
    headers.set("authorization", `Bearer ${this.#config.serviceToken}`);
    headers.set("content-type", "application/json");
    return headers;
  }
}

function mapPrincipal(value: PrincipalWire): VerglasAccessPrincipal {
  return {tenantId: value.tenant_id, id: value.id, kind: value.kind, parentId: value.parent_id};
}

function mapResource(value: ResourceWire): VerglasAccessResource {
  return {tenantId: value.tenant_id, id: value.id, kind: value.kind, parentId: value.parent_id};
}

function mapGrant(value: GrantWire): VerglasAccessGrant {
  return {
    id: value.id,
    tenantId: value.tenant_id,
    principalId: value.principal_id,
    resourceId: value.resource_id,
    actions: value.actions,
  };
}

async function accessError(path: string, response: Response): Promise<Error> {
  const detail = (await response.text()).slice(0, 1000);
  return new Error(`Verglas access ${path} failed: HTTP ${response.status}${detail ? ` — ${detail}` : ""}`);
}
