import type {
  VerglasAccessAction,
  VerglasAccessGrant,
  VerglasAccessGrantInput,
  VerglasAccessIdentity,
  VerglasAccessPrincipal,
  VerglasAccessResource,
  VerglasAccessSnapshot,
  VerglasAccessTokenSummary,
  VerglasCreatedAccessToken,
  VerglasCreateAccessTokenInput,
  VerglasPrincipalKind,
  VerglasResourceKind,
} from "@verglas/workshop-shared/api";

/** Server-only configuration for the tenant authorization service. */
export interface VerglasAccessEnv {
  VERGLAS_ACCESS_URI?: string;
  VERGLAS_IDENTITY_ASSERTION_KEY?: string;
  VERGLAS_TENANT_ID?: string;
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

type AuthorizationWire = {
  identity: {
    tenant_id: string;
    principal_id: string;
    token_id: string;
    audience: string;
  };
  decision: DecisionWire;
};

type SessionWire = {
  token: string;
  expires_at: number;
};

type AccessTokenWire = {
  id: string;
  name: string;
  principal_id: string;
  parent_principal_id: string;
  audience: string;
  created_at: number;
  expires_at: number;
  last_used_at?: number;
  revoked_at?: number;
};

type CreatedAccessTokenWire = AccessTokenWire & {token: string};

/** Resolved authorization endpoint and assertion issuer configuration. */
export type VerglasAccessConfig = {
  endpoint: string;
  identityAssertionKey: string;
  tenantId: string;
};

/** Resolves the identity-assertion configuration as one mandatory pair. */
export function resolveVerglasAccessConfig(env: VerglasAccessEnv): VerglasAccessConfig | null {
  const endpoint = env.VERGLAS_ACCESS_URI?.trim();
  const identityAssertionKey = env.VERGLAS_IDENTITY_ASSERTION_KEY?.trim();
  if (!endpoint && !identityAssertionKey) return null;
  if (!endpoint || !identityAssertionKey) {
    throw new Error("VERGLAS_ACCESS_URI and the identity assertion key must be configured together.");
  }
  if (!/^[0-9a-fA-F]{64}$/.test(identityAssertionKey)) {
    throw new Error("VERGLAS_IDENTITY_ASSERTION_KEY must contain exactly 64 hexadecimal characters.");
  }
  return {
    endpoint: endpoint.replace(/\/+$/, ""),
    identityAssertionKey,
    tenantId: env.VERGLAS_TENANT_ID?.trim() || "local",
  };
}

/** Maps an authenticated OS account to one stable tenant principal identifier. */
export function userPrincipalId(userId: string): string {
  return `user/${userId.trim().toLowerCase()}`;
}

/** User-bound adapter for access decisions, delegation, and scoped credentials. */
export class VerglasAccessClient {
  readonly #config: VerglasAccessConfig;
  readonly #userId: string;
  readonly #fetch: typeof fetch;
  readonly #sessions = new Map<string, Promise<SessionWire>>();

  /** Binds every operation to the authenticated OS identity supplied by the server. */
  constructor(config: VerglasAccessConfig, userId: string, fetcher: typeof fetch = fetch) {
    this.#config = config;
    this.#userId = userId;
    this.#fetch = fetcher.bind(globalThis);
  }

  /** Returns the mapped principal and its effective tenant-root ownership decision. */
  async identity(): Promise<VerglasAccessIdentity> {
    const authorization = await this.#authorize("tenant", "own");
    return {
      tenantId: authorization.identity.tenant_id,
      principalId: authorization.identity.principal_id,
      tenantOwner: authorization.decision.allowed,
      policyVersion: authorization.decision.policy_version,
    };
  }

  /** Idempotently registers a process identity under the caller's current tenant authority. */
  async ensurePrincipal(id: string, kind: VerglasPrincipalKind, parentId?: string): Promise<void> {
    await this.#ensure("/v1/access/principals", {
      id,
      kind,
      ...(parentId ? {parent_id: parentId} : {}),
    });
  }

  /** Idempotently registers a protected resource beneath the tenant root. */
  async ensureResource(id: string, kind: VerglasResourceKind, parentId = "tenant"): Promise<void> {
    await this.#ensure("/v1/access/resources", {
      id,
      kind,
      ...(parentId ? {parent_id: parentId} : {}),
    });
  }

  /** Returns the bounded tenant registry visible to the current tenant owner. */
  async snapshot(): Promise<VerglasAccessSnapshot> {
    const [principals, resources, grants] = await Promise.all([
      this.#request<PrincipalWire[]>("/v1/access/principals"),
      this.#request<ResourceWire[]>("/v1/access/resources"),
      this.#request<GrantWire[]>("/v1/access/grants"),
    ]);
    const tokenLists = await Promise.all(principals.map((principal) =>
      this.#request<AccessTokenWire[]>(
        `/v1/access/tokens?principal_id=${encodeURIComponent(principal.id)}`,
      )));
    const tokens = new Map(tokenLists.flat().map((token) => [token.id, token]));
    return {
      tenantId: this.#config.tenantId,
      principals: principals.map(mapPrincipal),
      resources: resources.map(mapResource),
      grants: grants.map(mapGrant),
      tokens: [...tokens.values()].map(mapAccessToken),
    };
  }

  /** Lists resources on which the authenticated user can pass grants. */
  async listDelegableResources(): Promise<VerglasAccessResource[]> {
    const resources = await this.#request<ResourceWire[]>("/v1/access/resources");
    const decisions = await Promise.all(resources.map(async (resource) => ({
      resource,
      allowed: (await this.#authorize(resource.id, "pass_grants")).decision.allowed,
    })));
    return decisions.filter(({allowed}) => allowed).map(({resource}) => mapResource(resource));
  }

  /** Lists explicit grants assigned to one child process visible to the current user. */
  async listPrincipalGrants(principalId: string): Promise<VerglasAccessGrant[]> {
    const grants = await this.#request<GrantWire[]>(
      `/v1/access/grants?principal_id=${encodeURIComponent(principalId)}`,
    );
    return grants.filter((grant) => grant.principal_id === principalId).map(mapGrant);
  }

  /** Evaluates one action on a registered resource for this authenticated user. */
  async checkUser(_userId: string, resourceId: string, action: VerglasAccessAction): Promise<boolean> {
    return (await this.#authorize(resourceId, action)).decision.allowed;
  }

  /** Delegates only actions the authenticated user already holds and may pass. */
  async delegate(input: VerglasAccessGrantInput): Promise<VerglasAccessGrant> {
    if (input.actions.length === 0) throw new Error("At least one access action is required.");
    const wire = await this.#request<GrantWire>("/v1/access/delegations", {
      method: "POST",
      body: JSON.stringify({
        grant: {
          id: `delegated/${crypto.randomUUID()}`,
          principal_id: input.principalId,
          resource_id: input.resourceId,
          actions: [...new Set(input.actions)],
        },
      }),
    });
    return mapGrant(wire);
  }

  /** Revokes one explicit grant through the authenticated principal. */
  async revoke(grantId: string): Promise<void> {
    await this.#request("/v1/access/revocations", {
      method: "POST",
      body: JSON.stringify({grant_id: grantId}),
    });
  }

  /** Lists non-secret token metadata visible to the authenticated principal. */
  async listTokens(principalId?: string): Promise<VerglasAccessTokenSummary[]> {
    const query = principalId ? `?principal_id=${encodeURIComponent(principalId)}` : "";
    return (await this.#request<AccessTokenWire[]>(`/v1/access/tokens${query}`)).map(mapAccessToken);
  }

  /** Creates a delegated token and returns its plaintext bearer exactly once. */
  async createToken(input: VerglasCreateAccessTokenInput): Promise<VerglasCreatedAccessToken> {
    validateTokenInput(input);
    const wire = await this.#request<CreatedAccessTokenWire>("/v1/access/tokens", {
      method: "POST",
      body: JSON.stringify({
        name: input.name.trim(),
        audience: input.audience.trim(),
        expires_in_seconds: input.expiresInSeconds,
        grants: input.grants.map((grant) => ({
          resource_id: grant.resourceId,
          actions: [...new Set(grant.actions)],
        })),
      }),
    });
    return {...mapAccessToken(wire), token: wire.token};
  }

  /** Revokes a user-owned token, or any tenant token when called through an owner capability. */
  async revokeToken(tokenId: string): Promise<void> {
    const id = tokenId.trim();
    if (!id) throw new Error("Token ID is required.");
    await this.#request(`/v1/access/tokens/${encodeURIComponent(id)}`, {method: "DELETE"});
  }

  /** Returns the short-lived bearer used for user-scoped Verglas control calls. */
  async sessionToken(audience: "access" | "data-plane" = "access"): Promise<string> {
    const session = await this.#getSession(audience);
    return session.token;
  }

  /** Evaluates an action while allowing the service to derive tenant and actor from the bearer. */
  async #authorize(resourceId: string, action: VerglasAccessAction): Promise<AuthorizationWire> {
    return await this.#request<AuthorizationWire>("/v1/access/authorize", {
      method: "POST",
      body: JSON.stringify({audience: "access", resource_id: resourceId, action}),
    });
  }

  /** Treats conflict as successful idempotent registration and rejects every other failure. */
  async #ensure(path: string, body: unknown): Promise<void> {
    const response = await this.#fetch(`${this.#config.endpoint}${path}`, {
      method: "POST",
      headers: await this.#headers(),
      body: JSON.stringify(body),
    });
    if (response.ok || response.status === 409) return;
    throw await accessError(path, response);
  }

  /** Sends one authenticated JSON request using the current short-lived user session. */
  async #request<T = unknown>(path: string, init: RequestInit = {}): Promise<T> {
    const response = await this.#fetch(`${this.#config.endpoint}${path}`, {
      ...init,
      headers: await this.#headers(init.headers),
    });
    if (!response.ok) throw await accessError(path, response);
    if (response.status === 204) return undefined as T;
    return await response.json<T>();
  }

  /** Constructs request headers without exposing the assertion signing key outside this adapter. */
  async #headers(existing?: HeadersInit): Promise<Headers> {
    const headers = new Headers(existing);
    headers.set("authorization", `Bearer ${await this.sessionToken()}`);
    headers.set("content-type", "application/json");
    return headers;
  }

  /** Reuses an unexpired access session and mints a replacement before its safety window. */
  async #getSession(audience: "access" | "data-plane"): Promise<SessionWire> {
    const existing = this.#sessions.get(audience);
    const current = existing ? await existing : undefined;
    if (current && current.expires_at > Math.floor(Date.now() / 1000) + 30) return current;
    const pending = this.#exchangeIdentityAssertion(audience);
    this.#sessions.set(audience, pending);
    try {
      return await pending;
    } catch (error) {
      if (this.#sessions.get(audience) === pending) this.#sessions.delete(audience);
      throw error;
    }
  }

  /** Exchanges a 60-second signed OS identity assertion for an access-session bearer. */
  async #exchangeIdentityAssertion(audience: "access" | "data-plane"): Promise<SessionWire> {
    const assertion = await signIdentityAssertion(this.#config, this.#userId);
    const path = "/v1/access/sessions";
    const response = await this.#fetch(`${this.#config.endpoint}${path}`, {
      method: "POST",
      headers: {"content-type": "application/json"},
      body: JSON.stringify({assertion, audience}),
    });
    if (!response.ok) throw await accessError(path, response);
    return await response.json<SessionWire>();
  }
}

/** Signs a compact HS256 identity assertion for one OS-authenticated user. */
async function signIdentityAssertion(config: VerglasAccessConfig, userId: string): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  const header = base64UrlJson({alg: "HS256", typ: "JWT"});
  const payload = base64UrlJson({
    sub: userPrincipalId(userId),
    tenant_id: config.tenantId,
    aud: "verglas-access",
    iat: now,
    exp: now + 60,
    jti: crypto.randomUUID(),
  });
  const unsigned = `${header}.${payload}`;
  const key = await crypto.subtle.importKey(
    "raw",
    decodeHexKey(config.identityAssertionKey),
    {name: "HMAC", hash: "SHA-256"},
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(unsigned));
  return `${unsigned}.${base64UrlBytes(new Uint8Array(signature))}`;
}

/** Decodes the configured 256-bit hex secret into its HMAC key bytes. */
function decodeHexKey(value: string): Uint8Array {
  const bytes = new Uint8Array(32);
  for (let index = 0; index < bytes.length; index++) {
    bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

/** Encodes one JSON object using the unpadded base64url form required by compact JWTs. */
function base64UrlJson(value: unknown): string {
  return base64UrlBytes(new TextEncoder().encode(JSON.stringify(value)));
}

/** Encodes bytes without padding or non-URL-safe alphabet characters. */
function base64UrlBytes(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

/** Maps one service principal record into the browser-safe API contract. */
function mapPrincipal(value: PrincipalWire): VerglasAccessPrincipal {
  return {tenantId: value.tenant_id, id: value.id, kind: value.kind, parentId: value.parent_id};
}

/** Maps one service resource record into the browser-safe API contract. */
function mapResource(value: ResourceWire): VerglasAccessResource {
  return {tenantId: value.tenant_id, id: value.id, kind: value.kind, parentId: value.parent_id};
}

/** Maps one explicit service grant into the browser-safe API contract. */
function mapGrant(value: GrantWire): VerglasAccessGrant {
  return {
    id: value.id,
    tenantId: value.tenant_id,
    principalId: value.principal_id,
    resourceId: value.resource_id,
    actions: value.actions,
  };
}

/** Maps token metadata while deliberately excluding its one-time plaintext value. */
function mapAccessToken(value: AccessTokenWire): VerglasAccessTokenSummary {
  return {
    id: value.id,
    name: value.name,
    principalId: value.principal_id,
    parentPrincipalId: value.parent_principal_id,
    audience: value.audience,
    createdAt: value.created_at,
    expiresAt: value.expires_at,
    lastUsedAt: value.last_used_at,
    revokedAt: value.revoked_at,
  };
}

/** Rejects malformed or unbounded token requests before they cross the RPC boundary. */
function validateTokenInput(input: VerglasCreateAccessTokenInput): void {
  if (!input.name.trim() || input.name.length > 100) throw new Error("Token name is required and must not exceed 100 characters.");
  if (!input.audience.trim() || input.audience.length > 100) throw new Error("Token audience is required and must not exceed 100 characters.");
  if (!Number.isSafeInteger(input.expiresInSeconds) || input.expiresInSeconds < 60) {
    throw new Error("Token expiration must be at least 60 seconds.");
  }
  if (input.grants.length > 100) {
    throw new Error("A token may include at most 100 resource grants.");
  }
  for (const grant of input.grants) {
    if (!grant.resourceId.trim() || grant.actions.length === 0) {
      throw new Error("Every token grant requires a resource and at least one action.");
    }
  }
}

/** Converts a failed access-service response into one bounded diagnostic error. */
async function accessError(path: string, response: Response): Promise<Error> {
  const detail = (await response.text()).slice(0, 1000);
  return new Error(`Verglas access ${path} failed: HTTP ${response.status}${detail ? ` — ${detail}` : ""}`);
}
