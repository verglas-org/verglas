// This file defines the API spoken between the Verglas OS service and the front-end UI.
//
// The UI is a good old "fat client" SPA. Why not use SSR? Because:
// - Users of this UI are likely to have it open often, maybe even all the time. Startup time is
//   less of a concern than with sites you visit only briefly, and assets are likely to be in cache
//   in any case.
// - The Workspaces themselves are sandboxed on the client side in addition to the server side. This
//   sandboxing requires running code in the browser. It is not plausible to server-side render
//   a Workspace itself.
// - By providing a really clean API boundary between client and server, we make it easier to build
//   alternative clients.
// - SPA is just easier to think about.
//
// The entire API between the client and server is an RPC API, using Cloudflare's JavaScript RPC,
// which essentially allows natural JavaScript / TypeScript interfaces to be exposed over the
// network.
//
// The RPC interface operates over a WebSocket, which the client starts immediately at startup and
// keeps open for the entire lifetime of the session, reconnecting if needed.
//
// Workspaces run inside a sandboxed iframe which has no ability to talk to the outside world at all,
// except postMessage() to the parent frame. Through postMessage() exchanges, the Workspace can speak
// RPC to the Workshop. Among other things, through this interface, the Workshop provides the
// Workspace a stub pointing to the Workspace's server-side Durable Object interface.

import { RpcCompatible, RpcStub, RpcTarget } from "capnweb";
import { AccountDescription, ActionKind, ActionDescription, AvatarImage, GatekeeperUiFrame, ObservationDescription, ResourceDescription, ResourceConfiguratorFrame, SupportedResource, VendorDescription, HookDescription } from "./gatekeeper.js";
import type { UiFeatureFlags } from "./feature-flags.js";

export const SERVICE_SALT = new Uint8Array([
  0xd9, 0x4e, 0x54, 0x1d, 0x29, 0xc1, 0x03, 0x74, 0x73, 0x7e, 0xb3, 0xe3, 0x34, 0x6d, 0x8f, 0x21
]);

// A pending gatekeeper sign-in attempt, returned by `PublicApi.startGatekeeperLogin()`. Holding this
// stub is the capability to receive the resulting session token; dispose it to abandon the attempt.
export interface LoginAttempt extends RpcTarget {
  // Resolves with a session token (to store and pass to `authenticate()`, same format as `login()`)
  // once the gatekeeper popup completes, or rejects if the attempt fails or is abandoned. Safe to
  // call immediately after `startGatekeeperLogin()`.
  wait(): Promise<string>;
}

// Public API exposed to the internet.
export interface PublicApi extends RpcTarget {
  // Returns deployment-level configuration the client needs at boot (authentication and branding).
  // Contains no secrets.
  getServerConfig(): Promise<ServerConfig>;

  // Begin a sign-in via an authentication gatekeeper (e.g. "google", "github", "cloudflare").
  // Returns a `url` the client opens in a new tab (the gatekeeper's OAuth popup, which self-closes)
  // and an `attempt` stub whose `wait()` resolves once the popup completes. The vendor must be
  // auth-capable and allowlisted (see ServerConfig.authVendors); throws otherwise.
  //
  // Dispose `attempt` to abandon the sign-in (e.g. the user closed the popup); this cancels the wait
  // server-side.
  startGatekeeperLogin(vendorId: string): Promise<{ url: string; attempt: RpcStub<LoginAttempt> }>;

  // Authenticates the user using an auth token (typically stored in localStorage).
  authenticate(token: string): Promise<AuthenticatedApi>;

  // Like authenticate() but the server is expected to be sitting behind Cloudflare Access, and the
  // client is expected to have already authenticated with Access (before they could load the
  // application in their browser at all). The credentials from the Cloudflare Access session will
  // be used to authenticate the user.
  authenticateFromCfAccess(): Promise<AuthenticatedApi>;

  // Login with username and password.
  //
  // Returns a token to store in local storage and pass to `authenticate()` in the future.
  //
  // Returns null if login failed (no such user or wrong password).
  //
  // `passwordHash` is derived from the user's password as follows:
  //
  //     argon2id({
  //       password,
  //       salt: SERVICE_SALT + encode(username, 'utf8'),
  //       parallelism: 1,
  //       iterations: 3,
  //       memorySize: 64MiB,
  //       hashLength: 32,
  //     });
  //
  // Note that the `passwordHash` itself is NOT stored plaintext by the server -- additional
  // hashing is performed server-side. The overall scheme achieves roughly the same security
  // guarantees as traditional server-side password hashing, but with the added benefit that the
  // server never sees the user's password at all, and also the benefit of performing the expensive
  // hash on the client which tends to have more resources available than a busy server.
  //
  // This API may be disabled when the server uses SSO for authentication.
  login(username: string, passwordHash: Uint8Array): Promise<string | null>;

  // Create a new account. Returns a token to store in local storage and pass to `authenticate()`
  // in the future.
  //
  // Returns null if the username already exists. (Other kinds of errors may throw exceptions.)
  //
  // See login() (above) for an explanation of the password hashing algorithm.
  //
  // This API may be disabled when the server uses SSO for authentication.
  createAccount(username: string, displayName: string, passwordHash: Uint8Array)
      : Promise<string | null>;

  // Fetch blueprint metadata by ID. Returns null if the blueprint doesn't exist. No
  // authentication required (knowing the ID is sufficient, since a blueprint is "just data").
  getBlueprint(id: string): Promise<BlueprintPublicInfo | null>;

  // Download a blueprint as a `.blueprint` archive stream. The archive contains only
  // BlueprintMetadata plus the current blueprint code snapshot, not the full KV record.
  downloadBlueprint(id: string): Promise<ReadableStream<Uint8Array>>;
}

// Subscription callback for AuthenticatedApi.subscribeConnectedAccounts().
export interface ConnectedAccountsSubscriber {
  // If `credentialsValid` is false, the account's credentials are known to be expired, and the
  // UI should call reconnectAccount() to fix this if the user tries to select this account.
  add(id: number, description: AccountDescription, vendor: VendorDescription,
      supportedResources: SupportedResource[], credentialsValid: boolean, vendorId: string): void;
  remove(id: number): void;

  // Called after add() has been called for all accounts known so far.
  ready(): void;
}

// When listing gatekeeper vendors or connected accounts, you can filter to only vendors/accounts
// that support certain features. This type specifies the filter.
export type GatekeeperVendorFilter = {
  // Filter for vendors that can connect to the given resource.
  resourceUrl?: string,
};

/** Options for subscribing to connected accounts. */
export type ConnectedAccountsFilter = GatekeeperVendorFilter & {
  /** Ensure and include auto-provisioned accounts forced by deployment policy. */
  includeForcedAutoProvisionedAccounts?: boolean;
};

// Identifies a workpiece within a workspace. A workpiece is a numbered thing the user (or agent)
// is working on inside the workspace -- currently a workspace or a gatekeeper (connection), with
// more types expected later. All workpiece types share one sequential per-workspace ID namespace,
// so a bare number unambiguously identifies a workpiece of any type, and derived names (Yjs file
// roots, facet names) can never collide across types.
export type WorkpieceId = number;

// Matches an ASCII JavaScript identifier, excluding `$`. Deliberately conservative: binding
// names are typed by agents and rendered as `env.NAME`, so full Unicode identifier support buys
// nothing; and while `$` is technically legal in identifiers, it is conventionally reserved for
// special circumstances like code generators, so agents shouldn't be using it.
const IDENTIFIER_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;

// ECMAScript reserved words (including strict-mode reservations and literals), which are valid
// per IDENTIFIER_REGEX but cannot follow `.` in all contexts and would confuse both agents and
// humans as binding names.
const RESERVED_WORDS = new Set([
  "break", "case", "catch", "class", "const", "continue", "debugger", "default", "delete", "do",
  "else", "enum", "export", "extends", "false", "finally", "for", "function", "if", "import",
  "in", "instanceof", "new", "null", "return", "super", "switch", "this", "throw", "true", "try",
  "typeof", "var", "void", "while", "with",
  // Strict-mode / contextual reservations.
  "await", "implements", "interface", "let", "package", "private", "protected", "public",
  "static", "yield",
]);

// Validates a binding name, throwing a descriptive Error if it is unacceptable. This is the one
// shared validator applied at every chokepoint that writes a binding name (workspace binding edges,
// the workspace default binding list, chat binding maps, spawner env configs, and the agent
// tools), wherever the map is keyed.
//
// A valid name is a JavaScript identifier (see IDENTIFIER_REGEX; reserved words excluded) that is
// not a dangerous or confusing property name: anything that exists on `Object.prototype`
// (`__proto__`, `constructor`, `hasOwnProperty`, `toString`, etc.) or `prototype` is rejected,
// since binding maps are used as plain objects where such names would collide with inherited
// members -- or worse, mutate the prototype chain.
//
// ALL_CAPS_WITH_UNDERSCORES is style guidance only (recommended in tool descriptions and used by
// generated names), not enforced here.
export function validateBindingName(name: string): void {
  if (!IDENTIFIER_REGEX.test(name)) {
    throw new Error(
        `Invalid binding name "${name}": binding names must be JavaScript identifiers ` +
        `(letters, digits, and '_', not starting with a digit).`);
  }
  if (RESERVED_WORDS.has(name)) {
    throw new Error(`Invalid binding name "${name}": this is a reserved word in JavaScript.`);
  }
  if (name === "prototype" || name in Object.prototype) {
    throw new Error(
        `Invalid binding name "${name}": this name collides with a built-in object property.`);
  }
}

// Why a previously-configured observer binding failed verification on this open attempt. Attached to
// the ObserverBindingNeed the overseer re-prompts with, so the client can explain what went wrong
// instead of dead-ending the open.
export type ObserverBindingFailure = {
  // The account that was tried and rejected (a ConnectedAccountRecord id in the opening user's own
  // User DO). Re-authenticating it in place is usually the fix, so the client should pre-select it
  // and aim its re-authenticate affordance at it. May no longer exist, if the user disconnected it.
  accountId: number;
  // Human-readable explanation for display: either the error the gatekeeper threw when it refused
  // the account, or a message the overseer authored itself for a cause it can see directly (e.g.
  // the chosen account is no longer connected). Free text: MUST NOT be parsed or matched on.
  reason: string;
};

/**
 * Describes one connection a non-owner must verify using one of their own accounts. Passed to
 * ObserverConfigCallback.configure() when the opening user needs to choose an account; its result
 * echoes `gatekeeperId` in an ObserverAccountChoice.
 */
export type ObserverBindingNeed = {
  // The overseer-assigned gatekeeper id (a workpiece id).
  gatekeeperId: WorkpieceId;
  // The vendor the user must have a connected account for (e.g. "google"). The frontend filters
  // the user's connected accounts by this to find candidates.
  vendorId: string;
  // Human-readable resource title, for display in the configuration modal.
  resourceTitle: string;
  // Canonical resource URL, if known, for display.
  resourceUrl?: string;
  // Set only when this binding was already configured but its chosen account failed verification on
  // this attempt (expired credentials, a revoked grant, an upstream outage, or a genuine denial).
  // Absent for a binding that has simply never been configured. Deliberately carries no
  // "credentials valid" flag: the client already has that live from subscribeConnectedAccounts, and
  // a copy on the wire would go stale while the modal is open across an OAuth round trip.
  failure?: ObserverBindingFailure;
};

// The opening user's chosen account for a single gatekeeper binding. Returned from
// ObserverConfigCallback.configure().
export type ObserverAccountChoice = {
  // Matches the ObserverBindingNeed.gatekeeperId being satisfied.
  gatekeeperId: WorkpieceId;
  // An account in the opening user's own User DO (a ConnectedAccountRecord id).
  accountId: number;
};

// Provided by the client when opening a workspace. Invoked by the overseer ONLY if the opening user
// must choose connected accounts for one or more gatekeeper bindings before they can observe the
// workspace. In the common case (owner, or an already-configured observer) this is never called and
// open() resolves without an extra round trip. The overseer does not resolve open() until this
// returns. If the user cannot or will not provide the needed accounts, the callback should reject,
// and the overseer denies the open.
//
// `configure()` may be called a second time within one open, for the subset of bindings that failed
// verification (each carrying an ObserverBindingNeed.failure). This lets the user repair a binding --
// typically by re-authenticating the account whose credentials expired -- without leaving the flow.
// The overseer bounds the number of such re-prompts, so a client that resubmits an account that
// keeps failing eventually gets a denial rather than an endless loop.
export interface ObserverConfigCallback extends RpcTarget {
  configure(needs: ObserverBindingNeed[]): Promise<ObserverAccountChoice[]>;
}

/** Stable error codes attached to expected failures from `AuthenticatedApi.openWorkspace()`. */
export const OPEN_WORKSPACE_ERROR_CODES = {
  workspaceNotFound: "WORKSPACE_NOT_FOUND",
  workspaceAccessDenied: "WORKSPACE_ACCESS_DENIED",
} as const;

/** An expected failure code from `AuthenticatedApi.openWorkspace()`. */
export type OpenWorkspaceErrorCode =
    typeof OPEN_WORKSPACE_ERROR_CODES[keyof typeof OPEN_WORKSPACE_ERROR_CODES];

const OPEN_WORKSPACE_ERROR_MESSAGES: Record<OpenWorkspaceErrorCode, string> = {
  [OPEN_WORKSPACE_ERROR_CODES.workspaceNotFound]: "Workspace not found.",
  [OPEN_WORKSPACE_ERROR_CODES.workspaceAccessDenied]: "You don't have access to this workspace.",
};

/** Creates an expected `openWorkspace()` error with a machine-readable code. */
export function createOpenWorkspaceError(
    code: OpenWorkspaceErrorCode): Error & { code: OpenWorkspaceErrorCode } {
  return Object.assign(new Error(OPEN_WORKSPACE_ERROR_MESSAGES[code]), { code });
}

/** Reads the machine-readable code from an expected `openWorkspace()` error. */
export function getOpenWorkspaceErrorCode(error: unknown): OpenWorkspaceErrorCode | undefined {
  if (typeof error !== "object" || error === null) return undefined;

  const candidate = "code" in error ? error.code : undefined;
  return isOpenWorkspaceErrorCode(candidate) ? candidate : undefined;
}

function isOpenWorkspaceErrorCode(value: unknown): value is OpenWorkspaceErrorCode {
  return value === OPEN_WORKSPACE_ERROR_CODES.workspaceNotFound ||
      value === OPEN_WORKSPACE_ERROR_CODES.workspaceAccessDenied;
}

/** Stable authorization actions shared by lakehouse and runtime resources. */
export type VerglasAccessAction =
    "discover" | "describe" | "query" | "append" | "modify" | "create_child" |
    "execute" | "use_secret" | "deploy" | "pass_grants" | "manage_grants" | "own";

/** Human or process category registered with the tenant authorization service. */
export type VerglasPrincipalKind =
    "user" | "service_account" | "agent" | "job" | "job_run" | "vessel" |
    "application" | "integration";

/** Protected resource category understood across Verglas data and runtime backends. */
export type VerglasResourceKind =
    "tenant" | "database" | "warehouse" | "namespace" | "table" | "view" |
    "vector_index" | "graph" | "object_prefix" | "integration" |
    "integration_operation" | "secret" | "job" | "vessel" | "application" |
    "model" | "queue";

/** One tenant-scoped human or process identity. */
export type VerglasAccessPrincipal = {
  /** Tenant that owns the identity. */
  tenantId: string;
  /** Stable principal identifier within the tenant. */
  id: string;
  /** Identity lifecycle category. */
  kind: VerglasPrincipalKind;
  /** Durable parent identity for derived processes. */
  parentId?: string;
};

/** One protected object in the tenant resource hierarchy. */
export type VerglasAccessResource = {
  /** Tenant that owns the resource. */
  tenantId: string;
  /** Stable resource identifier within the tenant. */
  id: string;
  /** Backend-neutral resource category. */
  kind: VerglasResourceKind;
  /** Resource from which grants are inherited. */
  parentId?: string;
};

/** One additive assignment of actions to a principal on a resource. */
export type VerglasAccessGrant = {
  /** Stable grant identifier. */
  id: string;
  /** Tenant containing both the principal and resource. */
  tenantId: string;
  /** Principal receiving the actions. */
  principalId: string;
  /** Resource on which the actions originate. */
  resourceId: string;
  /** Non-empty delegated action set. */
  actions: VerglasAccessAction[];
};

/** Current OS user's mapped authorization identity and effective tenant-root access. */
export type VerglasAccessIdentity = {
  /** Tenant selected for this OS deployment. */
  tenantId: string;
  /** Stable principal mapped from the authenticated OS account. */
  principalId: string;
  /** Whether this identity owns the tenant root. */
  tenantOwner: boolean;
  /** Policy revision observed while constructing this view. */
  policyVersion: number;
};

/** Bounded tenant authorization state shown to a tenant administrator. */
export type VerglasAccessSnapshot = {
  /** Tenant represented by every returned object. */
  tenantId: string;
  /** Registered human and process identities. */
  principals: VerglasAccessPrincipal[];
  /** Registered protected objects. */
  resources: VerglasAccessResource[];
  /** Explicit grants; inherited access is evaluated at use time. */
  grants: VerglasAccessGrant[];
};

/** A user-approved request to delegate access to a process principal. */
export type VerglasAccessGrantInput = {
  /** Process identity receiving access. */
  principalId: string;
  /** Protected object the process wants to use. */
  resourceId: string;
  /** Exact actions requested by the process. */
  actions: VerglasAccessAction[];
};

// Top-level API exposed to the user after they have authenticated.
export interface AuthenticatedApi extends RpcTarget {
  // Get profile info for the user who is logged in.
  whoami(): Promise<AiChatAuthorInfo>;

  /** Returns the tenant principal mapped from the current authenticated OS account. */
  getAccessIdentity(): Promise<VerglasAccessIdentity>;

  // Set the user's own display name, seen in chats, etc.
  setOwnDisplayName(name: string): Promise<void>;

  // Change the user's password, if using password-based authentication.
  //
  // See `PublicApi.login()` for an explanation of the hashing algorithm.
  changePassword(oldHash: Uint8Array, newHash: Uint8Array): Promise<void>;

  // Whether this account has a password set. False for accounts created via an OAuth provider, in
  // which case the change-password UI should be hidden.
  hasPasswordLogin(): Promise<boolean>;

  // List the user's configured AI models.
  //
  // Note that the list returned here could be different from a particular workspace's Overseer,
  // especially if the workspace is owned by someone else.
  listModels(): Promise<AiChatAuthorInfo[]>;

  // Adds a new model to the user's configured set. The ID must be unique among the user's
  // configured models.
  addModel(profile: AiChatAuthorInfo, config: AiModelConfig): Promise<void>;

  // Deletes a configured model.
  deleteModel(id: string): Promise<void>;

  /** Discovers coding-agent CLIs and subscription login state on the local runtime host. */
  detectModelRuntimes(): Promise<ModelRuntimeDetection>;

  /** Starts the selected coding-agent CLI's provider-owned subscription login flow. */
  startModelRuntimeLogin(runtime: ModelRuntimeId, sessionId: string): Promise<ModelRuntimeLoginResult>;

  /** Advances an active provider login flow with the answer to its current step. */
  continueModelRuntimeLogin(
      sessionId: string, answer?: ModelRuntimeWizardAnswer): Promise<ModelRuntimeLoginResult>;

  /** Cancels an active provider login flow. */
  cancelModelRuntimeLogin(sessionId: string): Promise<void>;

  /** Activates a detected subscription runtime and adds it to the user's model list. */
  linkSubscriptionRuntime(runtime: ModelRuntimeId): Promise<void>;

  /** Configures API-token access for a runtime and adds it to the user's model list. */
  linkTokenRuntime(runtime: ModelRuntimeId, apiToken: string): Promise<void>;

  // Set the model to use for simple quick tasks, like generating chat titles. Set null to
  // disable quick model use (e.g. chats will be titled "New Chat").
  setQuickModel(id: string | null): Promise<void>;

  // Get the quick model setting.
  getQuickModel(): Promise<null | string>;

  // Resolve UI feature flags for the authenticated user.
  getUiFeatureFlags(): Promise<UiFeatureFlags>;

  // Get the user's preferred model, chosen during onboarding. Returns null if the user has not
  // set a preference (or explicitly chose "No agent").
  getPreferredModel(): Promise<string | null>;

  // Set the user's preferred model. Pass null to indicate "No agent".
  setPreferredModel(id: string | null): Promise<void>;

  // Returns true if the user has completed the onboarding wizard.
  isOnboardingCompleted(): Promise<boolean>;

  // Mark the onboarding wizard as completed.
  completeOnboarding(): Promise<void>;

  // Upload a user avatar image. The data should be a compressed image (JPEG/PNG), ideally under
  // 50 KB. Pass null to remove the avatar.
  setAvatar(data: Uint8Array | null): Promise<void>;

  // Fetch a user's avatar image by user ID. Returns null if no avatar has been set.
  // Accepts any user ID so that other users' avatars can be displayed (e.g. in chat).
  getAvatar(userId: string): Promise<Uint8Array | null>;

  // Open an existing workspace.
  //
  // If `shareKey` is provided, the server redeems it before opening, adding the caller as a
  // collaborator. If the key is invalid or expired, the call throws an exception. This design
  // allows share-key redemption and workspace opening in a single round trip, and further calls
  // can be pipelined on the returned Overseer.
  //
  // To allow for pipelining, this throws an exception if the workspace doesn't exist. Expected
  // missing and authorization failures carry a code from `OPEN_WORKSPACE_ERROR_CODES`.
  //
  // `configureObservers` is invoked only when the opening user is a non-owner who must choose
  // connected accounts for one or more gatekeeper bindings before they can observe the workspace (see
  // ObserverConfigCallback). It is never called for the owner or an already-configured observer,
  // so the common-case open is still a single pipelined round trip.
  //
  // TODO(multi-vessel): This should be renamed to openWorkspace().
  openWorkspace(id: string, shareKey?: string,
             configureObservers?: RpcStub<ObserverConfigCallback>): Promise<RpcStub<Overseer>>;

  // Create a new workspace. It will start out titled "Untitled Workspace".
  //
  // Note: A workspace is considered "provisional" until it has some sort of activity, such as a
  //   chat message or code edit. Provisional workspaces do not appear on the home page and will be
  //   automatically deleted after some time. Note in particular that calling
  //   new*Gatekeeper() will not clear the provisional bit (as long as the gatekeeper isn't bound
  //   into a workspace), so provisional workspaces are useful to allow the user to write an initial
  //   chat message without explicitly creating a new workspace.
  //
  // TODO(multi-vessel): This should be renamed to newWorkspace().
  newWorkspace(): Promise<RpcStub<Overseer>>;

  // List metadata about all the user's Workspaces. Used to display the front-page listing.
  //
  // Provisional workspaces are hidden.
  //
  // TODO: Pagination, sort options.
  listWorkspaces(): Promise<WorkspaceMetadataWithTimestamps[]>;

  // List the outputs of all the user's workspaces. Used to display the Outputs page, which lets
  // the user find things they made without remembering which workspace they made them in.
  //
  // Served from an index in the user's own account which each workspace pushes to; a workspace
  // shared with the user contributes its outputs from the first time the user opens it (matching
  // when it appears in listWorkspaces()), and stops updating them if their access is revoked.
  // Provisional workspaces (still awaiting acceptance of a chat's changes) are never included.
  //
  // TODO: Pagination, sort options.
  listOutputs(): Promise<ListOutputsResult>;

  /** Lists active Verglas workers available to this local OS instance. */
  listVerglasWorkers(): Promise<VerglasWorkerSummary[]>;

  /** Full worker detail including extracted source and recent run history. */
  getVerglasWorker(name: string): Promise<VerglasWorkerDetail>;

  /** Recent scheduler jobs for one worker (newest first). */
  listVerglasWorkerJobs(name: string, limit?: number): Promise<VerglasWorkerRunSummary[]>;

  /** Queues an immediate run of a registered worker. */
  runVerglasWorker(name: string): Promise<{jobId: string, created: boolean}>;

  /** Updates worker lifecycle state (pause / resume / archive-as-delete). */
  setVerglasWorkerState(name: string, state: "running" | "paused" | "archived"): Promise<void>;

  /** Lists tables discoverable through the configured local Verglas catalog. */
  listVerglasTables(): Promise<VerglasTableSummary[]>;

  /** Returns the bounded Tables, Vectors, and Graphs catalog for the Lakehouse explorer. */
  getVerglasCatalog(): Promise<VerglasCatalogSnapshot>;

  /** Returns one database namespace with bounded physical and cache-usage metrics for its tables. */
  getVerglasDatabase(name: string): Promise<VerglasDatabaseDetail>;

  /** Creates an empty Iceberg namespace, presented in the OS as a database. */
  createVerglasDatabase(name: string): Promise<void>;

  /** Deletes an empty Iceberg namespace; non-empty databases are rejected by the catalog. */
  deleteVerglasDatabase(name: string): Promise<void>;

  /** Creates one table with an explicit Iceberg schema inside an existing or new database namespace. */
  createVerglasTable(input: VerglasCreateTableInput): Promise<VerglasTableSummary>;

  /** Deletes one Iceberg table from its namespace. */
  deleteVerglasTable(namespace: string[], name: string): Promise<void>;

  /** Lists local Verglas Vessels without exposing their secret configuration. */
  listVerglasVessels(): Promise<VerglasVesselSummary[]>;

  /** Reads the declarative configuration UI and readiness state for one Integration Vessel. */
  getVerglasIntegrationConfiguration(name: string): Promise<VerglasIntegrationConfiguration>;

  /**
   * Applies user-supplied configuration to one Integration Vessel.
   * When the Vessel is chat-owned, updates that chat card and resumes the waiting agent
   * on both successful and failed verification (same path as the chat Save and test card).
   */
  configureVerglasIntegration(name: string, values: Record<string, string>): Promise<void>;

  /** Deletes a local Integration Vessel and any Workshop chat records that own it. */
  deleteVerglasIntegration(name: string): Promise<void>;

  /** Deletes a local Application Vessel and any Workshop chat records that own it. */
  deleteVerglasApplication(name: string): Promise<void>;

  /** Starts or stops an OSS Application Vessel while preserving its desired runtime state. */
  setVerglasApplicationState(name: string, state: "running" | "stopped"): Promise<void>;

  // The deployment's standard output formats, in the order they should be offered -- what fills a
  // "New Document / New Slides / ..." menu. Empty when the deployment promotes none.
  //
  // These are ordinary blueprints an admin has promoted.
  listOutputFormats(): Promise<OutputFormatOffer[]>;

  // List all third-party services that this account can connect to.
  listGatekeeperVendors(filter?: GatekeeperVendorFilter): Promise<GatekeeperVendorInfo[]>;

  // Connect this account to a specific account on a third-party service. Returns the URL which
  // should be opened in a new tab in the user's browser to complete the authorization. When the
  // authorization flow completes, the account will be added to the list, which can be observed
  // through subscribeConnectedAccounts().
  //
  // `resourceUrlPatterns`, if given, limits the connection to the authorization needed for those
  // grantable resource types (those with `grantable`; see `SupportedResource`). If omitted,
  // authorization for all of the vendor's resource types is requested.
  connectAccount(vendorId: string, resourceUrlPatterns?: string[]): Promise<{url: string}>;

  // Ensure the authorization for the listed grantable resource types (by `urlPattern`) is granted
  // on a connected account, expanding if needed. Returns a URL to open in a new tab to authorize
  // them, or no url if nothing was needed. The updated grant is observable via
  // subscribeConnectedAccounts().
  ensureAccountResources(accountId: number, resourceUrlPatterns: string[]): Promise<{url?: string}>;

  // List the auto-provisioning ("ambient") gatekeepers the user can opt into right now: those set to
  // 'optional' by the admin that the user hasn't added yet. Rendered as an "Available" section on the
  // Connectors page. ('enabled' ones are already provisioned; 'disabled' ones aren't offered.) Returns
  // the same shape as listGatekeeperVendors (with no resources) so the connect UI handles both
  // identically, routing on `description.autoProvisionsAccount`.
  listAddableGatekeepers(): Promise<GatekeeperVendorInfo[]>;

  // Opt into an ambient gatekeeper: mint its connected account for this user (no OAuth flow). Only
  // works while the vendor's mode is 'optional' (or 'enabled') and the user has no account yet; the
  // new account then appears via subscribeConnectedAccounts(). Throws otherwise.
  provisionAmbientAccount(vendorId: string): Promise<void>;

  // Subscribe to the list of third-party accounts connected to the user's account.
  //
  // Dispose the returned stub to cancel the subscription.
  //
  // This is subscription-based because the flow to connect a new account completes in a separate
  // window. When it completes, we want the list of accounts in the Workshop UI to update
  // immediately, to give the user feedback that the account is now connected.
  subscribeConnectedAccounts(
      subscriber: RpcStub<ConnectedAccountsSubscriber>, filter?: ConnectedAccountsFilter)
      : Promise<RpcStub<{}>>;

  // Remove a connected account, revoking the token.
  disconnectAccount(accountId: number): Promise<void>;

  // Get the UI used to choose a specific resource from a connected account.
  //
  // `accountId` is the user's connected account that provides this resource.
  // `resourceUrlPattern` is the `urlPattern` associated with the supported resource.
  startResourceConfigurator(
    accountId: number,
    resourceUrlPattern: string,
  ): Promise<ResourceConfiguratorFrame>;

  // Remove a shared workspace from the user's home page listing. Does NOT revoke the user's
  // access -- if they open the workspace again (e.g., via link), it reappears on their home page.
  dismissSharedWorkspace(workspaceId: string): Promise<void>;

  // List all blueprints created by the current user (from User DO). Useful for an audit
  // view in Settings.
  listOwnBlueprints(): Promise<BlueprintUserSummary[]>;

  // Return a blueprint created by the current user, or null if it is not owned by this user.
  getOwnBlueprint(blueprintId: string): Promise<BlueprintUserSummary | null>;

  // List the blueprints currently in the user's library. This includes uploaded `.blueprint`
  // archives (stored locally) and blueprints saved by reference from other publishers.
  listLibraryBlueprints(): Promise<BlueprintLibrarySummary[]>;

  // Pin a blueprint for quick reuse on the home page. Pinning a public blueprint that isn't
  // already yours or in your library saves it to your library first.
  setBlueprintPinned(blueprintId: string, pinned: boolean): Promise<void>;

  // Returns whether the blueprint is pinned by the current user.
  isBlueprintPinned(blueprintId: string): Promise<boolean>;

  // List the deployment-wide featured blueprints. This is served from a KV snapshot rather
  // than directly from the AdminSettings durable object.
  listFeaturedBlueprints(): Promise<BlueprintPublicInfo[]>;

  // Add a blueprint to the user's library by reference, caching the current public metadata
  // snapshot for list rendering.
  addBlueprintToLibrary(blueprintId: string): Promise<void>;

  // Remove a blueprint from the user's library. If the library entry was uploaded by the
  // current user, this also deletes the backing blueprint content from storage.
  removeBlueprintFromLibrary(blueprintId: string): Promise<void>;

  // Returns info about whether the blueprint is in the user's library.
  // Returns null if not in library, or { uploaded } if it is.
  isBlueprintInLibrary(blueprintId: string): Promise<{ uploaded: boolean } | null>;

  // Legacy blueprint-to-workspace installation contract. New deployments reject this operation;
  // generated interfaces are standalone Application Vessels.
  //
  // Every required binding in the blueprint must have a corresponding entry in `bindings`,
  // keyed by binding name. Throws if any are missing or if accountId/modelId are invalid.
  //
  // The returned Overseer can be used immediately (pipelining-friendly).
  newWorkspaceFromBlueprint(
    blueprintId: string,
    bindings: Record<string, BlueprintBindingAssignment>
  ): Promise<RpcStub<Overseer>>;

  // Delete a blueprint that the user owns. Works even if the source workspace has been deleted
  // (operates on User DO + KV directly).
  deleteOrphanedBlueprint(blueprintId: string): Promise<void>;

  // Import a `.blueprint` archive from another Workshop instance. The imported blueprint is stored
  // as a local blueprint owned by the current user.
  importBlueprint(archive: ReadableStream<Uint8Array>): Promise<string>;

  // Re-authenticate a connected account whose credentials have expired (or may be about to
  // expire). Returns the URL to open in a new tab. When the OAuth flow completes, the account
  // is updated and subscribers are notified with credentialsValid: true.
  reconnectAccount(accountId: number): Promise<{url: string}>;

  // --- Gatekeeper management apps ---

  // List the gatekeepers that expose a full-page management UI (VendorDescription.providesUi) and are
  // available to this user. The Workshop renders a nav entry + page per entry. Independent of whether
  // the gatekeeper is a singleton.
  listGatekeeperApps(): Promise<GatekeeperAppInfo[]>;

  // Get the app frame (self-contained iframe HTML + the gatekeeper's `ui` capability) for the given
  // gatekeeper id, or null if there is no such UI-providing gatekeeper. The Workshop hosts the HTML
  // in a sandboxed iframe and exposes `ui` to it over a MessagePort RPC session.
  getGatekeeperApp(id: string): Promise<GatekeeperUiFrame | null>;

  // --- Deployment admin ---

  // Whether the current user is a deployment admin. Used by the client to decide whether to show
  // the admin UI.
  amIAdmin(): Promise<boolean>;

  // Returns a capability for managing deployment-wide admin settings, or null when the caller is not
  // an admin. The access check happens once here, so the returned stub's methods need no per-call
  // checks. (Authentication config — sign-in providers, password login — is intentionally not
  // managed here; it stays env-var driven.)
  getAdminApi(): Promise<RpcStub<AdminApi> | null>;

  // TODO:
  // - Edit permissions on a connected account.
}

// Describes a gatekeeper's management app, for the Workshop nav + page.
export type GatekeeperAppInfo = {
  // The vendor id (the GATEKEEPER_<ID> binding suffix, lowercased), used as the URL slug at
  // /gatekeepers/$id. This is the vendor, not a specific account: it assumes one management-UI
  // account per vendor per user, which holds for today's auto-provisioned singletons.
  id: string;
  // Title for the nav entry / page header.
  title: string;
  // Optional icon.
  icon?: AvatarImage;
};

// ---------------------------------------------------------------------------
// Context Library — pluggable separate worker (packages/gatekeeper-context)
// ---------------------------------------------------------------------------
//
// The Context Library lives in its own Worker, bound as the auto-provisioned gatekeeper
// GATEKEEPER_CONTEXT. Core implements none of its logic and owns none of its types (those live in
// the gatekeeper package): the account mints a management capability (the iframe app's `ui`, treated
// opaquely here) and a read session (the agent read-path, auto-provided as an unnamed capsule).

// Maximum length (characters) of the admin announcement / banner text.
export const MAX_ANNOUNCEMENT_LENGTH = 2000;

// Accent colors available for the full-width announcement banner. Soft status tints plus the brand
// color, so a banner need not look like an alert.
export type BannerColor = 'neutral' | 'info' | 'success' | 'warning' | 'danger' | 'brand';

export const BANNER_COLORS: BannerColor[] =
    ['neutral', 'info', 'success', 'warning', 'danger', 'brand'];

export const DEFAULT_BANNER_COLOR: BannerColor = 'info';

export function isBannerColor(value: unknown): value is BannerColor {
  return typeof value === 'string' && (BANNER_COLORS as string[]).includes(value);
}

// The deployment-wide full-width banner configuration.
export type BannerConfig = {
  // Banner text (Markdown supported). Empty string hides the banner.
  text: string;
  // Accent color.
  color: BannerColor;
};

// Whether `value` is a valid 3- or 6-digit hex color (e.g. "#abc" or "#aabbcc"). Used to validate
// the admin accent color before it's interpolated into CSS, preventing CSS injection.
export function isHexColor(value: unknown): value is string {
  return typeof value === 'string' && /^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(value);
}

// A single gatekeeper resource type in the admin resource-config UI.
export type AdminResource = {
  // The resource's urlPattern, used as its stable identifier.
  urlPattern: string;
  title: string;
  description: string;
  icon?: AvatarImage;
  // Whether this resource is currently enabled (not in the admin disabled set).
  enabled: boolean;
};

// Provisioning mode for an auto-provisioning ("ambient") gatekeeper — one that mints a connected
// account with no OAuth flow (VendorDescription.autoProvisionsAccount), e.g. the Context Library:
//   - 'disabled': not available; no account is provisioned and any existing one is dormant.
//   - 'optional': users opt in from the Connectors page; not forced on anyone (the default).
//   - 'enabled':  auto-provisioned for every user (forced); they can't remove it.
export const AMBIENT_GATEKEEPER_MODES = ['disabled', 'optional', 'enabled'] as const;
export type AmbientGatekeeperMode = typeof AMBIENT_GATEKEEPER_MODES[number];

export function isAmbientGatekeeperMode(value: unknown): value is AmbientGatekeeperMode {
  return AMBIENT_GATEKEEPER_MODES.includes(value as AmbientGatekeeperMode);
}

// A bound gatekeeper in the admin gatekeeper-config UI, discriminated by `autoProvisions`:
//   - an ordinary OAuth/resource gatekeeper has a binary `enabled` flag and `resources` to toggle;
//   - an auto-provisioning ("ambient") gatekeeper has a three-state `ambientMode` and no resources.
export type AdminResourceVendor = {
  vendorId: string;
  displayName: string;
  logo?: AvatarImage;
} & (
  | { autoProvisions: false; enabled: boolean; resources: AdminResource[] }
  | { autoProvisions: true; ambientMode: AmbientGatekeeperMode }
);

// A connectable third-party service: its vendor id, display metadata, and the resource types it
// offers (empty for an auto-provisioning gatekeeper like the Context Library). Returned by both
// listGatekeeperVendors and listAddableGatekeepers so the connect UI treats both uniformly.
export type GatekeeperVendorInfo = {
  id: string;
  description: VendorDescription;
  supportedResources: SupportedResource[];
  // Present when a bound gatekeeper could not be queried. UIs should surface this to the user but
  // not offer it as connectable.
  unavailable?: boolean;
};

// Maximum length (characters) of the admin-authored agent system-prompt instructions.
export const MAX_INSTANCE_INSTRUCTIONS_LENGTH = 8000;

// Maximum length (characters) of the admin-authored site name shown next to the top-bar logo.
export const MAX_SITE_NAME_LENGTH = 40;

// What this deployment calls itself when the admin has not set a custom `siteName`. Also the
// product's own name, so it appears in prose the server and UI address to the user.
export const DEFAULT_SITE_NAME = "Verglas";

// The name to display for this deployment. Accepts an unset or not-yet-loaded `siteName` so both
// the server (reading admin config) and the client (reading ServerConfig) resolve it identically.
export function resolveSiteName(siteName: string | undefined): string {
  return (siteName ?? "").trim() || DEFAULT_SITE_NAME;
}

/** Maximum byte length of an admin-uploaded site logo after browser-side PNG conversion. */
export const MAX_SITE_LOGO_BYTES = 256 * 1024;

/** Maximum width or height of an admin-uploaded site logo in pixels. */
export const MAX_SITE_LOGO_DIMENSION = 512;

// All admin-managed deployment settings, returned by AdminApi.getSettings() for the admin UI.
export type AdminSettingsView = {
  // Whether new account signups are allowed.
  signupsEnabled: boolean;
  // Site name shown next to the top-bar logo ("" falls back to DEFAULT_SITE_NAME).
  siteName: string;
  /** Custom deployment logo, or undefined to use the default Verglas mark. */
  siteLogo?: AvatarImage;
  // Agent system-prompt instructions ("" when unset).
  instanceInstructions: string;
  // Top-bar notice text ("" when unset).
  announcement: string;
  // Full-width banner (text + accent color).
  banner: BannerConfig;
  // Accent color hex, or "" for the default theme.
  accentColor: string;
  // Every bound gatekeeper and its resource types, with enabled state (not hidden when disabled).
  resourceVendors: AdminResourceVendor[];
  // The blueprints promoted as standard output formats, in menu order (including disabled ones).
  formats: AdminFormat[];
};

// One promoted blueprint, as the admin Formats panel sees it: the deployment's curation plus
// enough of the blueprint to show what is being curated.
export type AdminFormat = {
  blueprintId: string;

  blueprintTitle: string;

  // The blueprint's own description, which is the rest of the catalog entry the agent reads;
  // `agentHint` is only its last line.
  blueprintDescription: string;

  // Presentation after the deployment's overrides are applied.
  output?: BlueprintOutput;

  // What the blueprint itself declares, so the panel can show which fields are overridden.
  declared?: BlueprintOutput;

  // The deployment's presentation overrides, if any.
  overrides?: Partial<BlueprintOutput>;

  enabled: boolean;

  // One line telling the agent when to prefer this format.
  agentHint: string;

  // The promoted blueprint no longer exists (deleted after promotion). Such an entry is skipped
  // everywhere else; the panel surfaces it so the admin can remove it.
  missing: boolean;

  // The blueprint ships with the deployment (see format-blueprints/ and the FORMAT_BLUEPRINTS the
  // build generates from it), so an upgrade can replace its contents. Curation stays the admin's: an upgrade never re-promotes something they
  // removed, nor resets their overrides.
  bundled: boolean;
};

// Capability for managing deployment-wide admin settings, obtained via
// AuthenticatedApi.getAdminApi() (which is null for non-admins). The access check happens when the
// capability is minted, so these methods don't re-check. Covers branding, agent instructions, and
// which gatekeeper connectors/resources are offered — NOT authentication config (that's env-var
// driven). Each setter throws on invalid input.
export interface AdminApi {
  // Read all admin-managed settings for the admin UI in one call.
  getSettings(): Promise<AdminSettingsView>;

  /** Lists the tenant's principals, resources, and explicit grants. */
  getAccessSnapshot(): Promise<VerglasAccessSnapshot>;

  /** Delegates access as the current user, rejecting privileges the user cannot pass. */
  delegateAccess(input: VerglasAccessGrantInput): Promise<VerglasAccessGrant>;

  /** Revokes one explicit tenant grant. */
  revokeAccess(grantId: string): Promise<void>;

  // Enable or disable new account signups. Existing users can still log in while signups are closed.
  setSignupsEnabled(enabled: boolean): Promise<void>;

  // Set the site name shown next to the top-bar logo. Pass "" to reset to DEFAULT_SITE_NAME.
  // Rejects over MAX_SITE_NAME_LENGTH.
  setSiteName(name: string): Promise<void>;

  /** Set the deployment logo from browser-rasterized PNG bytes and return its canonical public
   * image, or undefined after reset. Pass null to restore the default Verglas mark. The
   * caller must supply decodable PNG data; the server enforces its header, size, and dimensions. */
  setSiteLogo(data: Uint8Array | null): Promise<AvatarImage | undefined>;

  // Replace the agent system-prompt instructions. Pass "" to clear. Rejects over MAX_INSTANCE_INSTRUCTIONS_LENGTH.
  setInstanceInstructions(text: string): Promise<void>;

  // Enable or disable a single gatekeeper resource type, keyed by vendor id + resource urlPattern.
  // Soft enforcement: disabling hides the resource from the connect UI, the resource picker, and the
  // agent; it doesn't revoke a capability a workspace already holds.
  setResourceEnabled(vendorId: string, urlPattern: string, enabled: boolean): Promise<void>;

  // Set a gatekeeper's availability. For an auto-provisioning ("ambient") gatekeeper, `mode` is the
  // full three-state (disabled / optional / enabled); for an ordinary gatekeeper only 'disabled' /
  // 'enabled' are valid ('optional' is rejected). Soft enforcement: it doesn't revoke a capability a
  // workspace already holds, and 'disabled' leaves an ambient account's data dormant rather than deleting
  // it.
  setGatekeeperMode(vendorId: string, mode: AmbientGatekeeperMode): Promise<void>;

  // Set the top-bar notice (centered text in the top navigation bar). Pass "" to clear. Rejects over
  // MAX_ANNOUNCEMENT_LENGTH.
  setAnnouncement(text: string): Promise<void>;

  // Set the full-width banner. Pass an empty text to hide it. Rejects over MAX_ANNOUNCEMENT_LENGTH or
  // an invalid color.
  setBanner(text: string, color: BannerColor): Promise<void>;

  // Set the deployment accent color (hex, e.g. "#3b82f6"). Pass "" to reset to the default theme.
  // Rejects an invalid hex color.
  setAccentColor(color: string): Promise<void>;

  // Returns whether the blueprint is featured on the deployment. Returns null when the blueprint
  // can't be featured (e.g. it isn't a listable blueprint).
  isBlueprintFeatured(blueprintId: string): Promise<boolean | null>;

  // Mark or unmark a blueprint as featured on the deployment.
  setBlueprintFeatured(blueprintId: string, featured: boolean): Promise<void>;

  // --- Standard output formats ---
  //
  // Promotion is what makes a blueprint one of the deployment's standard formats: offered in the
  // "New ..." menu and listed first for the agent. A blueprint declaring what it produces is
  // presentation, and never enough on its own.

  // Offer a blueprint as a standard format, appended last in menu order. Throws if the blueprint
  // doesn't exist. Promoting one that already is leaves its curation and menu position alone, so
  // that retrying a promotion whose mirror write failed repairs it rather than being refused.
  promoteFormat(blueprintId: string): Promise<void>;

  // Stop offering a blueprint as a standard format, and forget the deployment's curation of it.
  // The blueprint itself is untouched.
  //
  // Refused for a bundled blueprint (see `AdminFormat.bundled`), which the deployment installed
  // and will reinstall: `enabled: false` withdraws it without discarding the admin's overrides,
  // hint and menu position.
  removeFormat(blueprintId: string): Promise<void>;

  // Update one promoted format. Only the provided fields change. `agentHint: ""` clears the hint;
  // an `overrides` field set to null reverts that field to the blueprint's own declaration.
  updateFormat(blueprintId: string, patch: AdminFormatPatch): Promise<void>;

  // Reorder the menu. `blueprintIds` must be a permutation of the currently promoted ids.
  setFormatOrder(blueprintIds: string[]): Promise<void>;
}

// A partial edit to one promoted format. Absent fields are left alone.
export type AdminFormatPatch = {
  enabled?: boolean;
  agentHint?: string;
  // Per-field presentation overrides. A field set to null reverts to the blueprint's declaration;
  // a field left absent is unchanged.
  overrides?: {[K in keyof BlueprintOutput]?: BlueprintOutput[K] | null};
};

// A gatekeeper vendor offered as a sign-in method. The login/signup pages render a "Continue with
// ..." button per entry, alongside (never replacing) username/password. Built from auth-capable
// gatekeepers (VendorDescription.providesAuth) that are in the deployment's auth allowlist.
export type AuthVendorInfo = {
  // The gatekeeper vendor id (the GATEKEEPER_<NAME> binding suffix, lowercased), e.g. "google".
  vendorId: string;
  // Display name, logo, and brand color from the gatekeeper's VendorDescription.
  displayName: string;
  logo?: AvatarImage;
  color?: string;
};

// Deployment-level configuration that the client needs at boot to decide what UI to render.
// Returned by `PublicApi.getServerConfig()`. Contains no secrets.
export type ServerConfig = {
  // Auth-capable, allowlisted gatekeeper vendors offered as sign-in methods. Empty when none are
  // configured (password-only).
  authVendors: AuthVendorInfo[];

  // Whether username/password login is available. Defaults to true; an installation can disable it
  // (DISABLE_PASSWORD_AUTH) to be OAuth-only. Forced true if no auth vendor is configured, to avoid
  // locking everyone out.
  passwordAuthEnabled: boolean;

  // Whether new account signups are allowed (admin-configurable, default true). The signup page
  // hides the create-account form when false.
  signupsEnabled: boolean;

  // Site name shown next to the top-bar logo (admin-configurable). Empty falls back to
  // DEFAULT_SITE_NAME.
  siteName: string;

  /** Custom deployment logo, or undefined to use the default Verglas mark. */
  siteLogo?: AvatarImage;

  // Deployment-wide top-bar notice (centered text in the top navigation bar). Empty when none is set.
  announcement: string;

  // Deployment-wide full-width banner shown across the top of the app. Empty text hides it.
  banner: string;
  bannerColor: BannerColor;

  // Deployment accent (brand) color as a hex string, or "" to use the default theme. The client
  // overrides the brand CSS variables with this (and derived shades) at runtime.
  accentColor: string;

  /** Whether this OSS deployment has a fully configured local container runtime. */
  localContainerRuntime: boolean;
};

// Supported AI providers.
export type AiModelProvider =
    "openai" | "anthropic" | "google" | "cloudflare" | "ollama" | "local-runtime";

// Configuration specifying how to connect to an AI model provider.
export type AiModelConfig = {
  // Which AI provider hosts the model?
  provider: AiModelProvider;

  // Name of the specific model, as specified to the provider's API.
  model: string;

  // Secret API token for the respective provider. Subscription-backed local runtimes leave this
  // empty because their CLI owns its provider credential.
  apiToken: string;

  // Cloudflare account ID owning the Workers AI deployment the token authorizes. Required for
  // provider "cloudflare" (whose REST endpoint is account-scoped); unused for other providers.
  accountId?: string;

  // URL of the API. If not specified, use the default for the provider. Overriding the URL is
  // useful for local runtimes and alternative providers that expose a compatible API.
  apiUrl?: string;

  // Native coding-agent runtime used to execute this model. Required for newly-linked
  // "local-runtime" models; older saved configurations encode the runtime in `model`.
  runtime?: ModelRuntimeId;

  // Zero-based capability order reported by the provider's live model catalog. Lower values are
  // presented first; undefined is used for manually configured models without catalog metadata.
  catalogRank?: number;
};

/** Branded coding-agent runtimes that Verglas OS can execute through its native host adapter. */
export type ModelRuntimeId = "codex" | "claude-code" | "cursor";

/** One selectable model exposed by a branded coding-agent runtime. */
export type ModelRuntimeCatalogEntry = {
  /** Provider-owned model identifier passed unchanged to the runtime CLI. */
  id: string;
  /** Human-readable model name shown in model pickers. */
  name: string;
  /** Optional provider-owned explanation of the model. */
  description?: string;
  /** Whether the provider currently recommends this model by default. */
  isDefault?: boolean;
  /** Model context window when the runtime reports one. */
  contextWindow?: number;
};

/** One active workflow declaration in the Verglas worker registry. */
export type VerglasWorkerSummary = {
  /** Stable worker name. */
  name: string;
  /** Worker lifecycle state. */
  state: string;
  /** Placement selected by the worker declaration. */
  placement: string;
  /** Destination table or resource produced by the worker. */
  output: string;
  /** Portable JSON trigger declarations. */
  triggers: string;
  /** Identity that created the active revision. */
  createdBy: string;
  /** Active immutable revision number. */
  revision: number;
  /** Creation time of the active revision. */
  createdAt: string;
  /** Newest-first recent runs for list cards (bounded). */
  recentRuns?: VerglasWorkerRunSummary[];
  /** True when any recent run is currently executing. */
  activeRun?: boolean;
};

/** One scheduler job attempt summary for a worker. */
export type VerglasWorkerRunSummary = {
  /** Durable scheduler job id. */
  jobId: string;
  /** Queue state: pending, running, retryable, succeeded, failed. */
  state: string;
  /** When the job was enqueued. */
  createdAt: string;
  /** When the latest attempt completed, if finished. */
  completedAt?: string;
  /** Rows written on success. */
  rowsProduced?: number;
  /** Failure message from the latest attempt. */
  errorMessage?: string;
};

/** Worker registry row plus extracted source for the Jobs detail view. */
export type VerglasWorkerDetail = VerglasWorkerSummary & {
  /** TypeScript module extracted from the worker config files, when present. */
  sourceCode?: string;
  /** Raw worker config JSON (secrets by name only). */
  config: string;
};

/** One table discoverable through the local Verglas Iceberg catalog. */
export type VerglasTableSummary = {
  /** Namespace components containing the table. */
  namespace: string[];
  /** Unqualified Iceberg table name. */
  name: string;
  /** SQL-safe display name with every identifier quoted. */
  qualifiedName: string;
};

/** One explicit column in a table created through the OS management UI. */
export type VerglasTableColumn = {
  /** Stable column name within its table. */
  name: string;
  /** Verglas/Arrow type name accepted by the Iceberg catalog. */
  type: string;
  /** Whether the column accepts null values; omitted means true. */
  nullable?: boolean;
};

/** One ordered partition declaration in a table created through the OS management UI. */
export type VerglasTablePartition = {
  /** Existing source-column name. */
  source: string;
  /** Catalog partition transform. */
  transform: "identity" | "month";
};

/** Input required to create one explicit Iceberg table. */
export type VerglasCreateTableInput = {
  /** Namespace components containing the new table. */
  namespace: string[];
  /** New unqualified table name. */
  name: string;
  /** Ordered non-empty schema. */
  columns: VerglasTableColumn[];
  /** Optional ordered Iceberg partition specification. */
  partitions?: VerglasTablePartition[];
};

/** Cache-traffic usage attributed to one table by the local Verglas server. */
export type VerglasTableUsageMetrics = {
  /** Fully-qualified table name reported by the metering service. */
  table: string;
  /** Cache-served reads. */
  hits: number;
  /** Backend-fill reads. */
  misses: number;
  /** Total bytes served to readers, not physical table size. */
  bytesServed: number;
  /** Bytes served from cache tiers, not physical table size. */
  cacheBytes: number;
  /** Backend requests avoided by cache hits. */
  requestsAvoided: number;
  /** Estimated client latency saved by cache hits. */
  latencySavedSeconds: number;
};

/** Physical metadata from the table describe endpoint. */
export type VerglasTablePhysicalMetrics = {
  /** Current live row count. */
  rowCount: number;
  /** Current Iceberg data-file count. */
  fileCount: number;
  /** Current Iceberg data-file bytes. */
  sizeBytes: number;
  /** Current Iceberg snapshot identifier, when the table has a snapshot. */
  currentSnapshotId?: number;
};

/** A table plus the physical and cache-usage metrics available to the selected database view. */
export type VerglasTableDetail = VerglasTableSummary & {
  /** Physical metrics when the configured server exposes table inspection. */
  physical?: VerglasTablePhysicalMetrics;
  /** Cache-traffic metrics when the configured server exposes metering. */
  usage?: VerglasTableUsageMetrics;
};

/** One Iceberg namespace rendered by the OS as a database. */
export type VerglasDatabaseSummary = {
  /** Dot-separated namespace name. */
  name: string;
  /** Number of tables in the namespace. */
  tableCount: number;
  /** Number of maintained vector indexes targeting tables in the namespace. */
  vectorCount: number;
  /** Whether the namespace is a property graph with nodes and edges backing tables. */
  graph: boolean;
};

/** Bounded details returned after a user selects one database namespace. */
export type VerglasDatabaseDetail = VerglasDatabaseSummary & {
  /** Tables and their best-available metrics in the selected database. */
  tables: VerglasTableDetail[];
};

/** One maintained vector index discoverable through the local Verglas index registry. */
export type VerglasVectorSummary = {
  /** Stable index target, such as `tbl:rlean.runs` or `graph:agent_memory`. */
  target: string;
  /** Indexed embedding field. */
  field: string;
  /** Distance metric used by the index. */
  metric: string;
  /** Current Iceberg snapshot reflected by the index, when available. */
  reflectedSnapshot?: number;
  /** Number of live indexed vectors, when available. */
  liveCount?: number;
};

/** One property graph inferred from its paired Verglas node and edge tables. */
export type VerglasGraphSummary = {
  /** Graph namespace used by the Verglas graph SDK. */
  namespace: string;
  /** SQL-safe qualified name of the graph's node table. */
  nodesTable: string;
  /** SQL-safe qualified name of the graph's edge table. */
  edgesTable: string;
};

/** Bounded lakehouse catalog snapshot rendered by the Lakehouse explorer. */
export type VerglasCatalogSnapshot = {
  /** Database namespaces derived from the Iceberg catalog. */
  databases: VerglasDatabaseSummary[];
  /** Iceberg tables visible to this OS instance. */
  tables: VerglasTableSummary[];
  /** Maintained vector indexes visible to this OS instance. */
  vectors: VerglasVectorSummary[];
  /** Property graphs visible to this OS instance. */
  graphs: VerglasGraphSummary[];
};

/** A bounded SQL result returned by the Verglas query service. */
export type VerglasQueryResult = {
  /** Ordered result column names. */
  columns: string[];
  /** At most the requested number of JSON-compatible rows. */
  rows: Record<string, unknown>[];
  /** Number of rows returned in `rows`. */
  rowCount: number;
  /** Whether at least one additional row was omitted from the bounded result. */
  truncated: boolean;
};

/** One query execution visible in the Data page's live activity panel. */
export type VerglasQueryActivity = {
  /** Monotonically increasing sequence within the workspace runtime. */
  sequence: number;
  /** Whether the query originated from the Data editor or an agent tool call. */
  source: "data" | "agent";
  /** SQL submitted by the caller before bounded-result wrapping. */
  sql: string;
  /** Current execution state. */
  status: "running" | "succeeded" | "failed";
  /** ISO timestamp when execution began. */
  startedAt: string;
  /** ISO timestamp when execution finished, when terminal. */
  finishedAt?: string;
  /** Returned bounded row count, when successful. */
  rowCount?: number;
  /** Bounded failure text, when unsuccessful. */
  error?: string;
};

/** Product role served by a local Verglas Vessel. */
export type VerglasVesselRole = "integration" | "application";

/** Public runtime state for a local Verglas Vessel. */
export type VerglasVesselSummary = {
  /** Stable Vessel name used in runtime paths. */
  name: string;
  /** User-facing title declared by an Integration Vessel. */
  title?: string;
  /** User-facing purpose declared by an Integration Vessel. */
  description?: string;
  /** Whether the Vessel is an external API integration or a user-facing application. */
  role: VerglasVesselRole;
  /** OCI image backing the current local Vessel. */
  image: string;
  /** Observed container lifecycle state, when available. */
  state?: string;
  /** Persisted desired lifecycle state; false means the OSS runtime keeps this Vessel stopped. */
  running?: boolean;
  /** Result of the Vessel's bounded health probe. */
  health: "ready" | "unhealthy" | "unknown";
  /** Local preview URL for Application Vessels. */
  previewUrl?: string;
  /** Browser-loadable Application screenshot URL when one was captured at create time. */
  screenshotUrl?: string;
};

/** One field rendered by an Integration Vessel's generic configuration screen. */
export type VerglasIntegrationConfigurationField = {
  /** Stable field name passed back to the Integration Vessel. */
  name: string;
  /** Human-readable form label. */
  label: string;
  /** Input rendering hint declared by the integration. */
  type: string;
  /** Whether the integration requires a value before becoming ready. */
  required: boolean;
  /** Whether the value must be masked and never returned after submission. */
  secret: boolean;
  /** Optional input placeholder. */
  placeholder?: string;
  /** Optional field-level help text. */
  description?: string;
};

/** Declarative configuration UI and current readiness for one Integration Vessel. */
export type VerglasIntegrationConfiguration = {
  /** User-facing integration title. */
  title: string;
  /** Short explanation of the external connection. */
  description: string;
  /** Optional setup documentation URL. */
  helpUrl?: string;
  /** Ordered setup instructions, including richer steps generated by the agent. */
  instructions: Array<string | IntegrationSetupInstruction>;
  /** Ordered fields rendered by the OS. */
  fields: VerglasIntegrationConfigurationField[];
  /** Whether the Vessel reports that required configuration is present. */
  configured: boolean;
};

/** One selectable value in a provider-owned login step. */
export type ModelRuntimeWizardOption = {
  /** Opaque value returned to the provider login flow when the option is selected. */
  value: unknown;
  /** User-facing option label. */
  label: string;
  /** Optional explanation shown with the option. */
  hint?: string;
};

/** Device-code details supplied by a provider-owned login flow. */
export type ModelRuntimeDeviceCode = {
  /** Short code the user enters on the provider website. */
  code: string;
  /** Optional number of minutes before the code expires. */
  expiresInMinutes?: number;
  /** Optional provider instructions accompanying the code. */
  message?: string;
};

/** A single interactive step in a provider-owned subscription login flow. */
export type ModelRuntimeWizardStep = {
  /** Opaque step identifier returned with the user's answer. */
  id: string;
  /** Rendering behavior requested by the provider flow. */
  type: "note" | "select" | "text" | "confirm" | "multiselect" | "progress" | "action";
  /** Optional step heading. */
  title?: string;
  /** Optional explanatory body. */
  message?: string;
  /** Options for select and multiselect steps. */
  options?: ModelRuntimeWizardOption[];
  /** Initial input value supplied by the provider. */
  initialValue?: unknown;
  /** Placeholder for text input. */
  placeholder?: string;
  /** Whether text input must be visually masked. */
  sensitive?: boolean;
  /** Browser URL to open for OAuth, device-code, or action steps. */
  externalUrl?: string;
  /** Device-code information displayed next to an external URL. */
  deviceCode?: ModelRuntimeDeviceCode;
};

/** Answer submitted for the current provider-owned login step. */
export type ModelRuntimeWizardAnswer = {
  /** Step identifier from the current login result. */
  stepId: string;
  /** Provider-defined answer value. */
  value?: unknown;
};

/** Current result of starting or advancing a provider-owned login flow. */
export type ModelRuntimeLoginResult = {
  /** Stable login session identifier. */
  sessionId: string;
  /** True when the login flow reached a terminal state. */
  done: boolean;
  /** Current interactive step, when the flow needs user action. */
  step?: ModelRuntimeWizardStep;
  /** Terminal or running status reported by the native runtime adapter. */
  status?: "running" | "done" | "cancelled" | "error";
  /** Safe provider error text, when the flow failed. */
  error?: string;
};

/** Availability and authentication state for one branded runtime. */
export type ModelRuntimeStatus = {
  /** Stable runtime identifier used by the connection API. */
  id: ModelRuntimeId;
  /** True when the required CLI is installed on the runtime host. */
  available: boolean;
  /** True when the CLI reports reusable subscription credentials. */
  linked: boolean;
  /** Human-readable status from runtime discovery. */
  detail: string;
  /** Whether the host adapter can launch the CLI's guided login. */
  supportsGuidedLogin: boolean;
};

/** Result of discovering supported subscription runtimes through the native host adapter. */
export type ModelRuntimeDetection = {
  /** True when the deployment has a reachable native model-runtime adapter. */
  runtimeAvailable: boolean;
  /** Safe connection error when the native adapter is unavailable. */
  runtimeError?: string;
  /** Status for every branded runtime shown in the UI. */
  runtimes: ModelRuntimeStatus[];
};

// Workers AI adds the response cap to the prompt and rejects a request whose total exceeds the
// model's window, so every Cloudflare model reserves this much of it for the response.
export const WORKERS_AI_OUTPUT_LIMIT = 32768;

// Models offered in the picker. `contextWindow` is the maximum tokens one request may total.
// `outputLimit`, when present, is both the requested response cap and the space reserved for it,
// leaving the remainder as the prompt budget context compaction sizes against.
export const SUGGESTED_MODELS: Record<
  AiModelProvider,
  Record<string, {name: string, contextWindow: number, outputLimit?: number}>
> = {
  "cloudflare": {
    "@cf/moonshotai/kimi-k2.7-code": {
      name: "Kimi K2.7 Code (Workers AI)", contextWindow: 262144,
      outputLimit: WORKERS_AI_OUTPUT_LIMIT,
    },
    "@cf/zai-org/glm-5.2": {
      name: "GLM 5.2 (Workers AI)", contextWindow: 262144, outputLimit: WORKERS_AI_OUTPUT_LIMIT,
    },
  },
  "anthropic": {
    // TODO: Include Fable -- but we need an admin option to disable it, since many orgs don't
    //   allow it for ZDR reasons. It's sort of overkill for building workspaces anyway.
    "claude-opus-5": {name: "Claude Opus 5", contextWindow: 1000000},
    "claude-sonnet-5": {name: "Claude Sonnet 5", contextWindow: 1000000},
    "claude-haiku-4-5": {name: "Claude Haiku 4.5", contextWindow: 200000},
  },
  "openai": {
    "gpt-5.6-sol": {name: "GPT 5.6 Sol", contextWindow: 1050000, outputLimit: 128000},
    "gpt-5.6-luna": {name: "GPT 5.6 Luna", contextWindow: 1050000, outputLimit: 128000},
    "gpt-5.6-terra": {name: "GPT 5.6 Terra", contextWindow: 1050000, outputLimit: 128000},
  },
  "google": {
    "gemini-3.6-flash": {name: "Gemini 3.6 Flash", contextWindow: 1048576},
  },
  "ollama": {
  },
  "local-runtime": {
    "codex": {name: "Codex subscription", contextWindow: 128000},
    "claude-code": {name: "Claude Code subscription", contextWindow: 128000},
    "cursor": {name: "Cursor subscription", contextWindow: 128000},
  },
};

// Metadata about a Postgres-backed agent Workspace. Includes everything needed to render the
// Workspace list on the front page.
//
// TODO(multi-vessel): Rename `WorkspaceMetadata`.
export type WorkspaceMetadata = {
  // Unique ID for this workspace, used with `openWorkspace()`. This is a url-safe base64 value
  // chosen randomly when the workspace is created.
  id: string;

  // Human-readable workspace title. Can be modified. (Per-workspace titles live on the workspace
  // workpieces themselves; see WorkpieceSummary.)
  title: string;

  // Total cost of AI inference in dollars, if known.
  totalCost?: number;

  // Whether the user has pinned this workspace to the top of their list.
  pinned?: boolean;

  // Set when the workspace is not owned by the current user. Presence of this field indicates the
  // user is a collaborator, not the owner.
  owner?: AiChatAuthorInfo;

  // The viewing user's effective role for this workspace. The owner is always "build". Used by the
  // frontend to decide whether to render the full editor ("build") or the UI-only shell ("use").
  // Absent implies "build" for backwards compatibility.
  role?: CollaboratorRole;

  // True when the workspace has observed data marked as share-prohibited. Such workspaces can no longer
  // be shared with additional users or links.
  sharingProhibited?: boolean;

  // Various objects in the API specify a workspaceId, but make the property optional. When omitted,
  // the default workspace ID should be assumed. This is largely for backwards compatibility with
  // records that were stored before workspaces could have multiple workspaces.
  //
  // TODO(multi-vessel): Do a migration to backfill all workspace IDs, then eliminate the concept of
  // a default workspace from the API.
  defaultVesselId?: WorkpieceId;

  // TODO:
  // - created / modified / activity times
  // - icon? thumbnail?
}

// WorkspaceMetadata extended with timestamps. These are available when listing workspaces from the
// user's collection, but not from the Overseer (which doesn't track them).
export type WorkspaceMetadataWithTimestamps = WorkspaceMetadata & {
  created: Date;
  lastActive: Date;
}

// The icons an output format may be drawn with. A closed set because we want them to look consistent.
// The glyphs themselves live in the frontend, so only these keys ever cross the wire.
export const OUTPUT_ICONS = ["fileText", "gridNine", "presentation", "appWindow", "flowArrow",
    "kanban", "chartBar", "table", "notebook", "listChecks"] as const;

// One of `OUTPUT_ICONS`, naming a glyph the frontend knows how to draw.
export type OutputIcon = typeof OUTPUT_ICONS[number];

// Whether an unknown value names one of the icons this deployment can draw. Used wherever an icon
// arrives from outside the kernel: a published blueprint, an admin override, or the browser.
export function isOutputIcon(value: unknown): value is OutputIcon {
  return typeof value === "string" && (OUTPUT_ICONS as readonly string[]).includes(value);
}

// What instantiating a blueprint produces: a Document, a Spreadsheet, a Workflow, etc. Declared
// by the blueprint's author (see `BlueprintMetadata.output`), inherited by every workspace instantiated
// from it, and used wherever that workspace is shown in place of generic workspace.
//
// Declaring this is presentation only. Any user can publish a blueprint calling itself a
// Document; that must be harmless. Being offered as one of the deployment's standard formats (in
// the New menu, or the agent's preferred list) is a separate, admin-curated decision.
export type BlueprintOutput = {
  // Stable grouping slug, e.g. "document". Outputs sharing an id are grouped together on the
  // outputs page.
  id: string;

  noun: string;
  plural: string;

  icon: OutputIcon;
};

// One entry of the "New ..." menu, as returned by `listOutputFormats()`. This names a blueprint the
// deployment has promoted, instantiated with `newWorkspaceFromBlueprint(blueprintId, ...)` like any other.
export type OutputFormatOffer = {
  blueprintId: string;

  // How to name and draw it: the blueprint's own declaration with any deployment override
  // applied. Also what the created workspace inherits.
  output: BlueprintOutput;

  // The blueprint's description.
  description: string;

  // The blueprint needs bindings wired up before it can run.
  requiresSetup: boolean;
};

// The result of `AuthenticatedApi.listOutputs()`.
export type ListOutputsResult = {
  // Every output indexed so far.
  outputs: OutputSummary[];

  // Set while workspaces predating the index are still being swept into it, which happens once per
  // user after a deployment upgrades. Each call sweeps a bounded number of them, so a caller that
  // wants the rest calls again until this is false.
  //
  // False means stop asking, not that the index is complete. A workspace that couldn't be reached
  // is passed over rather than retried forever, and a sweep that reached none of them gives up for
  // now instead of spinning. Either way the gap closes when the workspace is next opened, and the
  // next call to this method resumes any sweep that was left unfinished.
  catchingUp: boolean;
};

// One entry in the user's output index: something a workspace produced that the user can open
// directly.
export type OutputSummary = {
  // The workspace that contains this output (an `openWorkspace()` id).
  workspaceId: string;

  // The workpiece within that workspace. `(workspaceId, workpieceId)` uniquely identifies an
  // output.
  workpieceId: WorkpieceId;

  // The format this output was built as, if it came from a blueprint declaring one. Absent for a
  // workspace built from scratch, which displays as a generic app.
  output?: BlueprintOutput;

  title: string;
  workspaceTitle: string;
  created: Date;

  // When the containing workspace was last active. Outputs have no activity timestamp of their
  // own yet, so all outputs of a workspace share this value.
  lastActive: Date;

  // Set when the containing workspace is owned by someone else (i.e. it was shared with the
  // caller).
  owner?: AiChatAuthorInfo;

  // The caller's role, cached on their last open and refreshed when a revocation downgrades them,
  // so a listing can offer only the actions it permits; the workspace still authorizes each one
  // when attempted. Absent for the caller's own workspaces, and for a shared one whose last open
  // predates this field.
  role?: CollaboratorRole;
}

// Describes the client-side UI code for a Workspace. Such code is intended to run inside an iframe
// sandbox with no access to the outside world except through an RPC interface to the Workshop
// and to the Workspace's server.
export type UiBundle = {
  // URL from which the main bundle of UI code can be downloaded. This download contains all the
  // Workspace's client-side assets. The URL is content-addressed to make it highly cacheable, even
  // across multiple Workspaces sharing the same implementation (blueprint).
  //
  // TODO: Specify the format of what this URL returns. A raw HTML page doesn't quite work because
  //   the client needs to initialize the sandbox with some platform libraries before loading the
  //   Workspace itself.
//  url: string;

  // Returns the raw JS code to execute in the Workspace iframe.
  // TODO: For now we just return the code but we should switch to serving over HTTP as described
  //   above, for caching. Or... maybe we should actually serve over RPC, but also employ the
  //   Cache API in the browser? Or some other local storage?
  jsCode: string;

  // Other metadata could be placed here in the future, e.g. to specify what version of support
  // libraries should be loaded.
};

// Represents an incremental update to the code.
export type CodeUpdate = {
  // Version number of the code AFTER this update has been applied.
  version: number;

  // Original timestamp of this update.
  timestamp: Date;

  // Yjs encoded update blob, encoding at least all changes since the previous version that the
  // client is known to already have.
  //
  // All encoded updates use V2 format.
  update: Uint8Array;
}

// Callback interface used to receive code updates from the server.
export interface CodeSubscriber {
  // Called any time the version on the server is newer than what the subscriber has.
  //
  // When the subscriber is multiple versions behind, the server may choose to send multiple
  // incremental updates or one big update. The server may make several calls in rapid succession
  // without waiting for previous calls to return. Cap'n Web guarantees that the calls will be
  // delivered in order, but it is important that the subscriber either applies the updates or
  // places them in some sort of queue synchronously to maintain ordering.
  update(up: CodeUpdate): void;

  // Called the first time the subscriber is up-to-date with the latest version known to the
  // server.
  ready(): void;
}

// Specifies the state of an action in the action log:
// * pending: Action has not been applied yet. It is waiting for approval.
// * approved: Action was approved and applied.
// * rejected: Action was rejected by the user.
export type ActionState = "pending" | "approved" | "rejected";

export type ActionLogEntry = {
  // Sequential ID number for the action. Counts up from when the workspace was created.
  id: number;

  // Which gatekeeper produced this action? Omitted if the log entry came from a non-gatekeeper
  // source (e.g. webFetch tool).
  gatekeeperId?: WorkpieceId;

  resourceTitle: string;
  resourceUrl?: string;

  createdAt: Date;
  appliedAt?: Date;

  state: ActionState;
} & ({
  type: "action";
  description: ActionDescription;
  // Who resolved the action (approved or rejected it). Set when the action leaves "pending"; absent
  // while still pending (or for legacy actions resolved before this was tracked). For an
  // auto-approved action this is the user who enabled the rule -- auto-approvals run under their
  // authority (see `autoApproved`).
  resolvedBy?: AiChatAuthorInfo;

  // True when the action was applied automatically by an auto-approval rule rather than by a human
  // clicking Approve. Only ever set alongside state "approved" (there is no automatic rejection).
  autoApproved?: boolean;
} | {
  type: "observation";
  description: ObservationDescription;
} | {
  type: "bindHook";

  description: HookDescription;

  // Hook that was created by this action. `undefined` if it was later deleted.
  hookId?: number;

  // Is the hook currently enabled?
  enabled: boolean;

  // Note that `state` is not meaningful for hooks. Instead of being "approved" or "rejected", they
  // are enabled/disabled, which the user can freely toggle as often as they want.
});

export type BoundHookInfo = {
  id: number;

  // The gatekeeper that delivers this hook.
  gatekeeperId: WorkpieceId;

  // The workspace whose code this hook wakes.
  workspaceId: WorkpieceId;

  resourceTitle?: string;
  resourceUrl?: string;
  description: HookDescription;
  enabled: boolean;
};

// Configuration for an AI spawner binding. This binding allows the workspace to programmatically
// create new agents, that is, start new agent chat threads, which appear in the workspace's agent
// chat UI as new conversations. Agents created this way don't typically edit the workspace code, but
// rather use the `executeCode` tool to directly invoke the workspace's bindings to perform tasks.
// Each agent can additionally be provide "props" which may include additional RPC stubs
// representing specific resources or callbacks relevant to that agent session.
//
// For example, a workspace that responds to emails might invoke an agent for each email message that
// arrives, with an RPC stub that allows it to reply to that email -- but prohibits the agent from
// seeing or replying to any other email, to guard against prompt injection or information leakage
// between email threads.
export type AgentSpawnerConfig = {
  // Display name for the binding, shown in the binding list.
  displayName: string,

  // Model ID to run, of the workspace owner's available models. Can be `null` to just create a chat
  // that doesn't actually run an agent -- the chat will be notified that the chat needs attention,
  // same as for an agent chat where the agent fails to mark the task complete.
  modelId: string | null,

  // The bindings available to agents spawned by this spawner: binding name (as it appears as
  // `env.NAME` in the spawned agent's executeCode environment) -> target workpiece. When an agent
  // is spawned, this map is snapshotted into the spawned chat's seed binding layer (entries whose
  // targets no longer exist are dropped); the spawned agent sees only these bindings, never the
  // workspace's default binding list.
  //
  // The entries are deliberately not limited to bindings held by the workspace that owns the
  // spawner: a spawner may define bindings of its own, with its own names and targets.
  env: Record<string, WorkpieceId>,
};

// Interface to a workspace's Overseer, used to display the Workspace Workshop shell UI around that
// workspace. Workspace-level concerns live here: the workspace registry, code sync (one Yjs doc for
// the whole workspace), chats, actions/hooks, sharing, and blueprint listing. Per-workspace
// operations live on the VesselClient sub-capability (see createVessel()/getVessel()).
export interface Overseer extends RpcTarget {
  // Get metadata describing this workspace.
  getMetadata(): Promise<WorkspaceMetadata>;

  /** Runs a bounded read query for the Data visual query surface. */
  queryVerglas(sql: string, maxRows?: number): Promise<VerglasQueryResult>;

  /** Lists live Data and agent query operations after an optional sequence cursor. */
  listVerglasQueryActivity(afterSequence?: number): Promise<VerglasQueryActivity[]>;

  // Get metadata describing this workspace and subscribe to changes.
  //
  // `callback` will be called once immediately with the current metadata, then again any time it
  // changes.
  //
  // Disposing the returned `RpcStub` will cancel the subscription.
  subscribeToMetadata(
      callback: RpcStub<(metadata: WorkspaceMetadata) => void>)
      : Promise<RpcStub<{}>>;

  // Receive the current viewer roster, then incremental updates as viewers come and go.
  // A viewer is present for the lifetime of the openWorkspace() session.
  subscribeToPresence(subscriber: RpcStub<PresenceSubscriber>): Promise<RpcStub<{}>>;

  // Change the workspace title.
  setTitle(title: string): Promise<void>;

  // Pin or unpin this workspace in the user's list.
  setPinned(pinned: boolean): Promise<void>;

  // Instruct the workspace to delete itself, removing it from the User's workspace list and
  // deleting all data. Further method calls will fail.
  //
  // TODO: Implement undelete, maybe using PITR...
  deleteSelf(): Promise<void>;

  // Subscribe to the workspace's workpiece list.
  //
  // The subscriber receives one entry() per existing workpiece, followed by ready(), then
  // incremental entry()/removed() calls as workpieces are created, renamed, or deleted. In v1
  // only workspace-type workpieces are delivered (see WorkpieceSummary).
  //
  // Disposing the returned `RpcStub` will cancel the subscription.
  subscribeToWorkpieces(subscriber: RpcStub<WorkpiecesSubscriber>): Promise<RpcStub<{}>>;

  // Create a new workspace workpiece in this workspace. `title` is required -- workspaces have no
  // default title. The new workspace starts with no files and no bindings.
  //
  // If `chatId` is provided, the creation is provisional to that chat, exactly like code edits
  // made with a chat open: a `changes` message records it in the chat log (see
  // `createdVessels`), and the workspace remains pending (see WorkpieceSummary.chatId) until
  // the user accepts the chat's changes through that message (merging deletes the pending marker;
  // reverting deletes the workspace). Without `chatId` the workspace is created permanently.
  //
  // `bindingName` is the name under which the workspace appears in chat envs and the workspace
  // default binding list (see validateBindingName()). When absent, the server chooses one from
  // the title (via the quick model when configured, else a generic fallback). Workspace binding
  // Legacy Dynamic Worker Workspaces removed — live calls throw. Kept for Cap'n Web history typing.
  createWorkpiece(title: string, chatId?: number, bindingName?: string)
      : Promise<RpcStub<VesselClient>>;

  // Get the workspace with the given workpiece ID. To allow for pipelining, this throws an
  // exception if there is no such workspace.
  getVessel(id: WorkpieceId): Promise<RpcStub<VesselClient>>;

  // Subscribe to code updates.
  //
  // Code is represented as a single Yjs doc shared by the whole workspace. Each workpiece that
  // owns files has its own root Y.Map (mapping file names to Y.Text instances) within the doc,
  // named per WorkpieceSummary.filesRoot. Updates are whole-doc and may span workpieces.
  //
  // `subscriber` will receive updates whenever it becomes out-of-date. `fromVersion` is the
  // version the subscriber already has before the subscription starts. To download the code from
  // scratch, omit the version (or pass zero).
  //
  // Disposing the returned `RpcStub` will cancel the subscription.
  subscribeToCode(subscriber: RpcStub<CodeSubscriber>, fromVersion?: number): Promise<RpcStub<{}>>;

  // Send a Yjs update to the server.
  //
  // If `chatId` is omitted, the update applies to the committed mainline code. If `chatId`
  // is provided, the update is recorded as a live draft edit for that chat's branch.
  updateCode(update: Uint8Array, chatId?: number): Promise<void>;

  // Get an existing gatekeeper by workpiece ID. Throws if the ID doesn't exist.
  getGatekeeperById(id: WorkpieceId): Promise<GatekeeperClient<any>>;

  // Try to create a new gatekeeper for this URL.
  //
  // `accountId` is the user's connected account to use to access this resource. To determine an
  // appropriate account, use `subscribeConnectedAccounts()` with a `filter` for this URL, then
  // let the user choose one.
  //
  // The new gatekeeper is a workspace-level workpiece; it is not bound into any workspace's `env` by
  // default. Use VesselClient.bind() / bindWithSuggestedName() to expose it to a workspace.
  newGatekeeper(accountId: number, resourceUrl: string): Promise<GatekeeperClient<any> | null>;

  // Create a new gatekeeper for an AI model binding. The model can be any returned by
  // listModels().
  newAiModelGatekeeper(modelId: string): Promise<GatekeeperClient<any>>;

  // Create a new gatekeeper for an agent spawner binding. This allows the workspace to
  // programmatically spawn AI agents to complete tasks.
  newAgentSpawnerGatekeeper(config: AgentSpawnerConfig): Promise<GatekeeperClient<any>>;

  // List history of actions.
  // TODO: This should be paginated.
  listActions(): Promise<ActionLogEntry[]>;

  // Approve an action that is currently in the "pending" state. The action will be performed on
  // approval.
  approveAction(id: number): Promise<void>;

  // Reject an action that is in the "pending" state. This notifies the gatekeeper that it will not
  // be approved in the future.
  rejectAction(id: number): Promise<void>;

  // List information about bound hooks (which could wake up a workspace asynchronously).
  //
  // The list spans the whole workspace; each entry names the workspace it wakes (see
  // BoundHookInfo.workspaceId), so a per-workspace view must filter on that.
  listHooks(): Promise<BoundHookInfo[]>;

  // Enable the hook with the given ID. Callbacks will begin flowing.
  enableHook(id: number): Promise<void>;

  // Disable the hook with the given ID. Callbacks will stop.
  disableHook(id: number): Promise<void>;

  // Permanently delete the hook. Implies disabling it.
  deleteHook(id: number): Promise<void>;

  // Enable auto-approval of actions carrying the given `actionKind` (the
  // ActionDescription.actionKind) on the gatekeeper identified by `gatekeeperId`. Future actions
  // with that kind's tag whose author marked them `autoApprovable` are then applied automatically
  // without manual approval, and any matching action(s) already pending are applied immediately.
  //
  // Auto-approval rules are workspace-wide per gatekeeper: approving an action kind approves it
  // no matter which workspace invokes it.
  setAutoApprovedActionKind(gatekeeperId: WorkpieceId, actionKind: ActionKind): Promise<void>;

  // Remove the auto-approval rule for `tag` on the given gatekeeper; matching actions then
  // require manual approval again.
  removeAutoApprovedActionKind(gatekeeperId: WorkpieceId, tag: string): Promise<void>;

  // List the currently-enabled auto-approval rules.
  listAutoApprovedActionKinds(): Promise<Array<{ gatekeeperId: WorkpieceId; actionKind: ActionKind }>>;

  // List the auto-approvable action kinds offered by gatekeepers bound in this workspace. Each
  // entry identifies its connection and reports whether a matching auto-approval rule is enabled.
  listPreApprovableActions(): Promise<PreApprovableAction[]>;

  // Accept an agent's pending connection request (a "connectionRequest" chat message). The caller
  // is responsible for having actually created the gatekeeper (via newGatekeeper()) and passes the
  // resulting gatekeeper id. The gatekeeper is surfaced to the agent as a named binding in the
  // chat's env, under the name the agent chose when it made the request (see
  // `connectionRequest.bindingName`). This marks the request accepted, updates the inline card,
  // and resumes the agent so it can use the resource.
  acceptConnectionRequest(requestId: string, result: {gatekeeperId: WorkpieceId}): Promise<void>;

  // Deny an agent's pending connection request. Updates the inline card. Does NOT resume the agent:
  // the turn stays ended so the user can decide what to tell the agent to do instead.
  denyConnectionRequest(requestId: string): Promise<void>;

  /** Approves a pending AI access request under the current user's delegable authority. */
  approvePermissionRequest(requestId: string): Promise<void>;

  /** Denies a pending AI access request without changing tenant policy. */
  denyPermissionRequest(requestId: string): Promise<void>;

  // Supplies generated Source configuration, stores secret fields in the Verglas scheduler,
  // and publishes the Source as a Verglas worker revision. This does not resume or block a chat.
  configureSource(requestId: string, values: Record<string, string>): Promise<void>;

  // Queues an immediate run of a configured Source and returns its durable Verglas job ID.
  runSource(requestId: string): Promise<{jobId: string, created: boolean}>;

  /** Saves generated Integration configuration and requires its live verification to pass. */
  configureIntegration(requestId: string, values: Record<string, string>): Promise<void>;

  /** Re-runs an Integration's generated live verification without changing its configuration. */
  testIntegration(requestId: string): Promise<IntegrationVerification>;

  /** Deletes a chat-owned Integration Vessel and removes its configuration card. */
  deleteIntegration(requestId: string): Promise<void>;

  /** Deletes a chat-owned Application Vessel and removes its preview card. */
  deleteApplication(vesselName: string): Promise<void>;

  // Subscribe to action adds/updates. Dispose the returned stub to unsubscribe.
  // If `startAfter` is set, replay actions changed after that timestamp.
  subscribeToActions(subscriber: RpcStub<ActionsSubscriber>, startAfter?: Date): Promise<RpcStub<{}>>;

  // List past AI chats.
  listChats(): Promise<AiChatMetadata[]>;

  // List available models. The first listed model should be the default, unless the user has
  // chosen something else.
  listModels(): Promise<AiChatAuthorInfo[]>;

  // Fetch one page of messages in the chat history for the given chat thread. If `beforeSequence`
  // is absent, fetch the current tail. Otherwise, fetch messages before that sequence.
  //
  // Note that if you plan to subscribe to updates, you should initiate the subscription first,
  // before fetching history. Otherwise, you could theoretically miss a message that is sent
  // between when you fetch the history and when you subscribe.
  //
  // In typical usage, the client subscribes to all chat activity upfront, but only fetches
  // histories if and when the user opens a specific.
  getChatHistory(chatId: number, beforeSequence?: number): Promise<AiChatHistoryPage>;

  // Fetch a single message from a chat thread.
  getChatMessage(chatId: number, sequence: number): Promise<AiChatMessage | undefined>;

  // Subscribe to all new chat messages (across all threads).
  //
  // If `startAt` is given, it must be a date in the past. All messages starting from that date
  // will be sent upfront. This is intended to allow resubscribing after being disconnected. If
  // `startAt` is omitted, only new messages will be sent.
  //
  // Generally, a client should subscribe to chats immediately on loading the workspace editor. If
  // the client needs to call any methods like `listChats()` to backfill content, it should make
  // these calls after `subscribeToChat()`, so that there's no chance of missing a message. (It is
  // not necessary to wait for `subscribeToChat()` to return -- only to initiate the call before
  // other read calls.)
  subscribeToChat(subscriber: RpcStub<AiChatSubscriber>, startAfter?: Date): Promise<RpcStub<{}>>;

  // Lists slash commands available from Gatekeepers currently attached to this Workspace, including
  // ambient ones.
  listSlashCommands(): Promise<SlashCommandChoice[]>;

  // Starts a new chat with the given initial message or slash-command request. A slash command
  // always creates a visible chat event, even when it does not produce a message for the agent.
  // Slash-command requests cannot include capsules or attachments.
  //
  // `modelId` is one of the IDs in the result of `listModels()`, or null to inhibit AI response
  // (useful when using chat to talk between humans).
  //
  // `formats` records where the message names one of the deployment's standard output formats, so
  // the transcript can draw it as a chip. Display only -- what the agent reads is the noun, which
  // is already in the text.
  newChat(initialMessage: string | SlashCommandRequest, modelId: string | null,
          capsules?: CapsuleSpecifier[], attachments?: ChatAttachmentHandle[],
          formats?: MessageFormatRef[]): Promise<number>;

  // Send a message to the chat from this client. Sending a message causes the LLM to start
  // running if it isn't already.
  // If a slash command produces no message, only its visible invocation event is committed and the
  // agent does not run.
  // Slash-command requests cannot include capsules or attachments.
  //
  // `modelId` is one of the IDs in the result of `listModels()`, or null to inhibit AI response
  // (useful when using chat to talk between humans).
  //
  sendChatMessage(chatId: number, message: string | SlashCommandRequest, modelId: string | null,
                  capsules?: CapsuleSpecifier[], attachments?: ChatAttachmentHandle[],
                  formats?: MessageFormatRef[]): Promise<void>;

  // Upload an attachment for use in a future chat message. This way by the time the user wants to
  // send the message, likely uploading is complete. `modelId` determines whether the
  // selected provider can receive a raw file attachment.
  //
  // Pass the returned handle to newChat() or sendChatMessage() to commit the attachment into chat history.
  uploadChatAttachment(attachment: ChatAttachmentUpload, modelId: string | null): Promise<ChatAttachmentHandle>;

  // Fetch the bytes of a committed chat attachment over RPC. The canonical metadata is already
  // present in the message's ChatAttachmentRef. Images are inlined there, so this is normally used
  // only to download non-image attachments on demand.
  getChatAttachmentContent(chatId: number, id: string): Promise<Uint8Array>;

  // Delete an uploaded attachment that the user explicitly removed before sending the message.
  deleteChatAttachment(id: string): Promise<void>;

  // Update the title of a chat. Usually not needed as a title is generated automatically from
  // the first message.
  setChatTitle(chatId: number, title: string): Promise<void>;

  // Indicates that the user has requested that proposed changes through the given sequence number
  // in the chat thread be merged into the mainline.
  //
  // If `options.includeDraft` is true, any current live draft for the chat is first materialized
  // into one durable `changes` message and included in the merge.
  mergeChanges(
      chatId: number, mergeThrough: number | null,
      options?: { includeDraft?: boolean }): Promise<void>;

  // Indicates that the user has requested that proposed changes starting from the given sequence
  // number in the chat thread be reverted.
  revertChanges(chatId: number, revertFrom: number): Promise<void>;

  // Materialize the current live draft for the chat into one durable `changes` message without
  // merging it into the mainline.
  finalizeChatDraft(chatId: number): Promise<void>;

  // Discard the current live draft for the chat without affecting any durable `changes` messages.
  discardChatDraftChanges(chatId: number): Promise<void>;

  // Delete a chat thread.
  deleteChat(chatId: number): Promise<void>;

  // Request that any ongoing LLM session in the given chat immediately stop.
  //
  // If an LLM is running, the session is canceled subscribers will receive a metadata update
  // reflecting this before `stop()` returns.
  //
  // If no LLM is running, `stop()` does nothing and returns immediately.
  stopAgent(chatId: number): Promise<void>;

  // Retry the agent on the given chat. This starts the agent without adding a new user message.
  // The agent will re-process the existing chat history using the specified model.
  //
  // If an agent is already running, this does nothing.
  retryAgent(chatId: number, modelId: string): Promise<void>;

  // Subscribe to the workspace worker's console logs. This allows the user to observe console logs
  // being produced by the workspace.
  //
  // At present, logs are not stored, so the only way to see them is to be subscribed when they
  // happen.
  //
  // To unsubscribe, dispose the returned stub.
  subscribeToConsoleLogs(subscriber: RpcStub<ConsoleLogSubscriber>): Promise<RpcStub<{}>>;

  // --- Blueprint management ---
  //
  // Blueprint listing and maintenance are workspace-level (each blueprint record remembers which
  // workspace it exports). Creating a blueprint is per-workspace: see VesselClient.createBlueprint().

  // List blueprints created from this workspace's workspaces.
  listBlueprints(): Promise<BlueprintVesselSummary[]>;

  // Update an existing blueprint. Any combination of metadata and code can be updated
  // atomically in a single call with one propagation pass.
  //
  // - `title` / `description`: if provided, update the respective field.
  // - `updateCode`: if true, snapshot the source workspace's current committed code into the
  //   blueprint and increment the blueprint version.
  // - `updateBindings`: if true, refresh the blueprint's connection annotations from
  //   the source workspace's current bindings without changing the code snapshot.
  //
  // At least one option must be provided.
  updateBlueprint(blueprintId: string, options: {
    title?: string;
    description?: string;
    updateCode?: boolean;
    updateBindings?: boolean;
    screenshot?: BlueprintScreenshotUpload | null;
  }): Promise<void>;

  // Delete a blueprint. Cleans up KV, R2, User DO, and local storage.
  deleteBlueprint(blueprintId: string): Promise<void>;

  // Retry publishing a blueprint whose `dirty` flag is set (meaning a previous propagation
  // to User DO / KV / R2 failed).
  retryBlueprintPublish(blueprintId: string): Promise<void>;

  // --- Collaborator management ---

  /**
   * List the connections a recipient with `role` must verify before opening this workspace, in
   * the order the connections were created. Reports what sharing will cost the recipient; it
   * grants nothing and mints no capability.
   */
  listObserverRequirements(role: CollaboratorRole): Promise<ObserverBindingNeed[]>;

  // List all collaborators. Available to owner and all collaborators.
  listCollaborators(): Promise<CollaboratorInfo[]>;

  // Add a collaborator by username/email. The caller must be the owner or an existing
  // collaborator. `role` is the access level to grant; the caller may not grant a role higher
  // than their own effective role. Returns the new collaborator's info, or null if the username
  // doesn't correspond to an existing account.
  addCollaborator(username: string, role: CollaboratorRole,
                  note?: string): Promise<CollaboratorInfo | null>;

  // Remove a collaborator (identified by profile.id).
  //
  // Owner can remove anyone. A non-owner collaborator can only remove their own edge(s)
  // from the target. If the target still has edges from other sources, they keep access
  // and the return is an empty array. If no edges remain, the target is fully removed.
  //
  // When a target is fully removed, `keepUsers` lists the profile.ids of users who would
  // lose access transitively but should be retained. Their PermissionEdges through the
  // removed user are replaced with new edges from the caller. Users reachable only through
  // the removed user who are NOT in `keepUsers` are also removed.
  //
  // Returns the list of users whose access actually changed (removed or downgraded), including
  // the primary target. An empty array means the caller's edge was removed but no one's effective
  // access changed (the target retained their role through other edges).
  removeCollaborator(profileId: string, keepUsers: string[]): Promise<AffectedCollaborator[]>;

  // Preview what would happen if a collaborator were removed. For a non-owner caller,
  // if the target has edges from other sources that would survive, returns an empty array
  // (the target would not actually be affected). Otherwise, returns the list of users whose
  // access would change (lose access or be downgraded to a lower role) as a consequence, so the
  // caller can present checkboxes for which to keep vs. remove.
  previewRemoveCollaborator(profileId: string): Promise<AffectedCollaborator[]>;

  // --- Share link management ---
  //
  // A share *link* may back several keys: `createShareLink` mints the first and `newShareLinkKey`
  // mints more on demand. Renaming, revoking, and grants apply to the link.

  // Create a share link. The server generates a random 128-bit key, stores its HMAC-SHA-256
  // hash, and returns the raw key (hex-encoded) along with the id of the link it created. The
  // caller constructs a URL from the key. The raw key is never stored server-side. `role` is the
  // access level granted to anyone who redeems the link; the caller may not grant a role higher
  // than their own effective role.
  createShareLink(role: CollaboratorRole, note?: string)
      : Promise<{ key: string; linkId: string }>;

  // Mint a fresh secret for an existing link so the user can copy a new URL without creating a
  // whole new link. The old secrets remain valid, and revoking the link revokes them all together.
  newShareLinkKey(linkId: string): Promise<{ key: string }>;

  // List active share links (for management UI).
  listShareLinks(): Promise<ShareLinkInfo[]>;

  // Update a share link's management metadata. The raw secrets are not available after creation;
  // this only edits the stored note used by the management UI.
  updateShareLink(linkId: string, note?: string): Promise<void>;

  // Revoke a share link by its `linkId`, which revokes every secret ever minted for it. Users who
  // gained access through the link may be transitively removed or downgraded. `keepUsers` lists
  // profile.ids of users who should be retained at their prior role with fresh edges from the
  // caller. Returns the list of users whose access actually changed (removed or downgraded).
  revokeShareLink(linkId: string, keepUsers: string[]): Promise<AffectedCollaborator[]>;

  // Preview what would happen if a share link were revoked. Returns the list of users whose access
  // would change (lose access or be downgraded to a lower role) as a consequence, so the caller
  // can present checkboxes for which to keep vs. remove.
  previewRevokeShareLink(linkId: string): Promise<AffectedCollaborator[]>;
}

export type AiChatMetadata = {
  id: number,
  title: string,
  started: Date,
  lastActive: Date,

  // If present, an LLM (described by the author info) is currently actively responding to the
  // chat.
  activeAgent?: AiChatAuthorInfo,

  // If true, this chat thread has proposed changes which have not been accepted yet,
  // including any live draft edits that have not yet been materialized into a durable
  // `changes` message.
  hasProposedChanges?: boolean;

  // If this was started from an agent spawner, the spawner's display name.
  spawnerName?: string;

  // Tokens the model reported for this conversation's last step, if known. Cleared when compaction
  // changes what the next prompt will contain, until a step measures it again.
  totalTokens?: number;

  // Total cost of this conversation so far, in dollars, if known.
  totalCost?: number;

  // First sequence this chat still replays. Everything before it is covered by a compaction
  // checkpoint; those messages remain in canonical history but no longer drive current-state reads.
  compactedTo?: number;
};

// One page of a chat's history, bounded below by a compaction checkpoint. Compaction doesn't delete
// messages, so a long thread is read one checkpoint-delimited page at a time.
export type AiChatHistoryPage = {
  // The page's messages, ascending by sequence.
  messages: AiChatMessage[];

  // The checkpoint bounding this page below, absent once the page reaches the thread's start.
  compacted?: {
    // First sequence in this page. Pass as `getChatHistory`'s `beforeSequence` for the page before.
    to: number;

    // Summary that replaces the messages before `to` in subsequent model prompts, exposed so the
    // user can inspect the context kept across the boundary.
    summary: string;

    // Changes still proposed before `to`, merged into one update, so the client can show pending
    // changes without loading the messages that recorded them.
    proposedChanges?: Uint8Array;
  };
};

export type AiChatAuthorInfo = {
  // Is the author a human, AI, or Workspace?
  //
  // "vessel" means this is a prompt sent to an agent spawner -- i.e. a workspace spawned an
  // agent programmatically. In this case `id` is the workspace's owner's ID (for accounting purposes)
  // and `name` is the workspace title.
  type: "user" | "agent" | "vessel";

  // Unique user identifier, e.g. "kenton@cloudflare.com" or "gpt-5.1-pro".
  id: string;

  // Display name for author, e.g. "Kenton Varda" or "GPT"
  name: string;

  // Note: the avatar is intentionally not included here to keep this type lightweight (it's
  // embedded in every chat message). Fetch user avatars separately via
  // `AuthenticatedApi.getAvatar(userId)`.
};

export type AiChatMessage = {
  chatId: number;
  sequence: number;
  timestamp: Date;
  author: AiChatAuthorInfo;
} & AiChatMessageBody;

export type AiChatMessageBody = {
  // A regular chat message.
  type: "message";
  message: string;

  // The message may contain "capsules", which are embedded capabilities that reference external
  // resources. See `CapsuleSpecifier` for more.
  capsules?: CapsuleSpecifier[];

  // Standard output formats the message names, e.g. "create a Doc for homework and Slides for the
  // presentation". See `MessageFormatRef`.
  formats?: MessageFormatRef[];

  // If the AI produces any thinking/reasoning text, this is it. This should be hidden by default
  // but the user should have the option to expand it.
  reasoning?: string;

  // Messages from an AI agent can invoke tools.
  toolCalls?: AiToolCall[];

  // Attachments that were sent with this message. Actual bytes stored separately.
  attachments?: ChatAttachmentRef[];

  // Sequence of the visible slash-command event that generated this agent-visible message.
  // Clients use this to group the two records for display.
  generatedBySlashCommandSequence?: number;
} | {
  // A generated Source (Gatekeeper) that will ingest external data through a Verglas worker.
  // The card remains independent of the Workspace draft lifecycle and never blocks the chat.
  type: "sourceConfiguration";

  // Stable request identifier used to configure or run this Source.
  requestId: string;

  // User-facing Source identity and purpose.
  title: string;
  description: string;

  // Verglas output table receiving successfully ingested rows.
  outputTable: string;

  // Generated, bounded configuration fields. Values are never included in chat messages.
  fields: SourceConfigurationField[];

  /** Deployment triggers for this Source. */
  triggers?: SourceTrigger[];

  /** Fully resolved public ingress URLs for webhook triggers, when configured. */
  webhookUrls?: string[];

  // Deployment lifecycle of the generated Source worker.
  state: "needs_configuration" | "ready" | "error";

  // Last deployment failure, safe for display and absent after a successful configuration.
  error?: string;
} | {
  /** A generated, isolated Integration Vessel and its user-assisted setup lifecycle. */
  type: "integrationConfiguration";

  /** Stable request identifier used to configure and re-test this Integration. */
  requestId: string;

  /** Runtime Vessel identity. */
  vesselName: string;

  /** User-facing Integration identity and purpose. */
  title: string;
  description: string;

  /** Generated setup work the user must complete outside Verglas. */
  instructions: IntegrationSetupInstruction[];

  /** Generated configuration fields. Values never enter chat storage. */
  fields: SourceConfigurationField[];

  /** Integration deployment and verification lifecycle. */
  state: "deploying" | "needs_configuration" | "ready" | "error";

  /** Most recent live verification result. */
  verification?: IntegrationVerification;

  /** Last safe deployment or verification failure. */
  error?: string;
} | {
  /**
   * Lightweight Jobs pipeline surface for a chat: workers, triggers, output tables, and
   * edges inferred from outputs feeding later jobs. Not a mini-app — links to `/workflows`.
   */
  type: "jobsPipeline";

  /** Jobs owned by this chat, newest last. */
  jobs: Array<{
    /** Source configuration request id when this job came from createSource. */
    requestId: string;
    /** Runtime worker name. */
    workerName: string;
    /** User-facing job title. */
    title: string;
    /** Destination lakehouse table. */
    outputTable: string;
    /** Declared triggers for this job. */
    triggers: SourceTrigger[];
    /** Deployment lifecycle. */
    state: "needs_configuration" | "ready" | "error";
  }>;

  /** Simple DAG edges: from job requestId to job requestId when outputs feed later jobs. */
  edges: Array<{from: string; to: string}>;
} | {
  /**
   * Lightweight Application preview card: title, description, and preview URL.
   * Screenshot is optional; primary CTA opens the preview.
   */
  type: "applicationPreview";

  /** Runtime Vessel identity. */
  vesselName: string;

  /** Local preview URL served by the container runtime. */
  previewUrl: string;

  /** User-facing Application identity. */
  title: string;
  description: string;

  /** Optional screenshot URL when capture is available. */
  screenshotUrl?: string;
} | {
  // A slash command exactly as requested by the client, retained for display and never included in
  // model context. A gatekeeper command does not itself start an agent turn -- the prompt it expands
  // to arrives as a separate `message`. A built-in command is handled by the Workshop, and this
  // record is what drives the turn it runs.
  type: "slashCommand";
  request: SlashCommandRequest;

  // Provider-supplied skill name for the display badge. Commands without one show no badge.
  skillName?: string;
} | {
  // Represents changes made to the code by an agent tool call or by a collaborating user as part
  // of a chat. These changes are provisional until they are accepted.
  type: "changes";

  // The code changes themselves, as a Yjs-encoded (V2) update against the workspace code Y.Doc.
  // Absent when the batch records only workspace creations and/or binding additions with no
  // accompanying code edits.
  update?: Uint8Array;

  // The workspace code version that `update` was built against. Once an agent session observes
  // the code at some version, the chat stays locked to that version (see
  // AiToolCall.observedCodeVersion), so history replay must learn each update's base version
  // *before* it reconstructs the session's code state. Present whenever `update` is, except in
  // messages persisted before this field existed. For user-authored batches this records the
  // mainline base at the time the user's edits were captured, which may legitimately differ
  // from the version an agent session is locked to (the user can accept changes -- advancing
  // mainline -- and keep editing); such stamps seed a session's version lock but are never
  // checked against it.
  observedCodeVersion?: number;

  // Workspaces created as part of this batch of changes (by the agent's `createVessel` tool, or by
  // the user via Overseer.createVessel() with a chat open -- in the latter case `update` is
  // omitted). Like the code changes themselves, the creations are provisional: a merge
  // through this message makes them permanent, and a revert covering it deletes them. Titles are
  // denormalized for display, since a reverted creation's registry record is gone. `bindingName`
  // is the name under which the workspace appears in the creating chat's env (and, once merged, the
  // workspace default binding list); recording it here lets the creating chat pick the name back
  // up on replay.
  createdVessels?: {workspaceId: WorkpieceId, title: string, bindingName: string}[];

  // Binding edges added to workspaces as part of this batch of changes (by the agent's
  // setVesselBinding tool, or by the user binding a connection with a chat open -- in the latter
  // case `update` is omitted). Like `createdVessels`, the additions are
  // provisional: the edge is visible only from this chat until a merge through this message
  // makes it permanent, and a revert covering it deletes the edge. `name` is the binding's name
  // within the workspace identified by `workspaceId`; `target` is the bound workpiece.
  addedBindings?: {workspaceId: WorkpieceId, name: string, target: WorkpieceId}[];
} | {
  // Indicates that at this point in the chat, the user chose to merge all (non-reverted) changes
  // in this chat up to and including the given sequence number.
  type: "merge";
  mergeThrough: number;

  // Code version at which the merge was applied. (A merge covering only workspace creations /
  // binding additions writes no new code version; this then records the bumped version counter.)
  version: number;
} | {
  // Indicates that at this point in the chat, the user chose to revert all changes starting at the
  // given sequence number through the end of the chat as of that time. These changes are
  // completely erased from the Yjs history. Subsequent changes will be based only on what existed
  // before this point, and any later merge will not include the reverted changes.
  type: "revert";
  revertFrom: number;
} | {
  // Indicates that the agent in this chat performed an action.
  type: "action",
  actionId: number;

  // Denormalized description of the action.
  //
  // This is inlined into the message at the time of query, so it is always present and always
  // current in messages delivered to the client. It is marked optional only because it is not
  // present in messages stored in the chat table on the server side.
  actionLog?: ActionLogEntry;
} | {
  // Indicates that the AI agent accessed the workspace one or more times. This is logged in order
  // to track whether information known to the workspace may have tainted the agent session.
  type: "useVessel";
} | {
  // Indicates that the agent run ended with an error (e.g. LLM API failure, abort, server
  // restart). This is displayed to the user with a "retry" button, but is NOT included in the
  // chat log sent to the LLM so the agent does not react to it.
  type: "error";
  message: string;
  // Optional machine-readable code so the client can react specially.
  code?: string;
} | {
  // Indicates that a callback was received on the agent's `self` object. When the agent uses
  // `executeCode`, the executed code receives a `self` parameter. Calling any method on `self`
  // (e.g., `self.onUpdate(data)`) delivers a callback message back to this chat thread and
  // activates the agent to respond.
  type: "agentCallback";

  // The method name that was called on `self`.
  methodName: string;

  // A depth-limited summary string of the arguments for the agent's context window.
  argsSummary: string;
} | {
  // A system-generated nudge message sent to the agent when it tries to end its turn while
  // agent callbacks are still unresolved. This is displayed as a user message to the LLM
  // so it can be prompted to continue.
  type: "agentNudge";
  text: string;
} | {
  // The agent requested that the user connect a gatekeeper (e.g. "I need ClickHouse cluster X").
  // Rendered inline in the chat as an accept/deny card. State is mutated in-place when the user
  // accepts or denies; the message is re-delivered to subscribers so the card updates. On accept the
  // agent is resumed with the outcome (see the history builder in agent.ts); on deny the agent is
  // not resumed (the user drives what happens next).
  type: "connectionRequest";

  // Unique id used by acceptConnectionRequest()/denyConnectionRequest().
  requestId: string;

  // The gatekeeper vendor the agent is requesting (id + denormalized display name).
  vendorId: string;
  vendorName: string;

  // Denormalized vendor logo URL, for the connection card icon.
  vendorLogoUrl?: string;

  // Denormalized human-readable resource type/scope being requested (e.g. "Home Assistant
  // Instance", "Gmail Mailbox"), resolved from the vendor's supported resources at request time.
  resourceTitle?: string;

  // A fully- or partially-specified resource URL, if the agent could infer one. When absent (or
  // incomplete) the accept flow opens the vendor's resource configurator to fill in the gaps.
  resourceUrl?: string;

  // The urlPattern of the supported resource this request resolved to at request time (one of the
  // vendor's SupportedResource.urlPattern values, e.g. "https://github.com/:owner/:repo" or the
  // whole-instance "https://*"). The backend guarantees every connection request resolves to a
  // concrete resource (see resolveRequestedResource), and the accept modal pre-selects exactly this
  // resource — so accepting never opens a blank "create new connection" picker.
  resourceUrlPattern?: string;

  // Why the agent wants this connection. Shown to the user to inform their decision.
  reason: string;

  // Lifecycle state. Starts "pending"; set by the user's accept/deny.
  state: "pending" | "accepted" | "denied";

  // Once accepted, the id of the created gatekeeper. The resource is surfaced to the agent as a
  // named binding in the chat's env; the agent can additionally bind it into a workspace via
  // setVesselBinding if its workspace code needs it.
  gatekeeperId?: WorkpieceId;

  // The name under which the resource will appear in the chat's env (`env.NAME` in executeCode)
  // once the request is accepted. Optional only for persisted legacy requests.
  bindingName?: string;

} | {
  /** The AI asks the user to delegate lakehouse or runtime access to a process principal. */
  type: "permissionRequest";
  /** Unique identifier used to approve or deny the request. */
  requestId: string;
  /** Job, Vessel, Application, Integration, or agent principal receiving access. */
  principalId: string;
  /** Protected Verglas resource the process wants to use. */
  resourceId: string;
  /** Exact actions requested by the process. */
  actions: VerglasAccessAction[];
  /** Agent-authored explanation shown to the approving user. */
  reason: string;
  /** User decision lifecycle. */
  state: "pending" | "approved" | "denied";
};

// Bytes to upload as a chat attachment.
//
// The server stores the bytes and returns the handle to pass when sending the message.
export type ChatAttachmentUpload = {
  mimeType: string;
  content: Uint8Array;
  name?: string;
};

// Handle for an attachment that has been uploaded but not yet sent as part of a message.
//
// Pass this back unchanged when sending the chat message.
export type ChatAttachmentHandle = {
  // Clients must not infer storage paths from this ID or construct handles by hand.
  id: string;
};

// Attachment metadata returned to clients.
//
// For image attachments, `content` carries the full image bytes inline so the client can render
// them in the chat without an extra round trip. For other attachments, fetch the bytes on demand
// via `Overseer.getChatAttachmentContent()`.
export type ChatAttachmentRef = ChatAttachmentHandle & {
  mimeType: string;
  name?: string;
  size: number;

  // Inlined bytes for small image attachments. Present only for images.
  content?: Uint8Array;
};

// Whether attachment bytes can be decoded and inlined into the agent's prompt as text.
export function isTextLikeAttachmentMimeType(mimeType: string): boolean {
  if (mimeType.startsWith("image/")) return false;
  return mimeType.startsWith("text/") ||
      /\b(json|javascript|typescript|xml|yaml|csv|markdown)\b/.test(mimeType);
}

// Describes a tool call performed by an AI agent as part of a message.
//
// The agent addresses workpieces by their chat binding name (the `workspace`/`workpiece` parameters
// on several variants), never by workpiece ID. Logs persisted before multi-vessel workspaces lack
// these names; when a name is absent, the workspace's `defaultVesselId` (from `WorkspaceMetadata`)
// is assumed, and it is an error for it to be omitted when there is no default.
export type AiToolCall = {
  // ID of the original tool call, useful to reproduce the model messages.
  toolCallId: string;

  // If present, this tool observed the code at the given version number.
  //
  // Note that generally once the agent observes code at a particular version, the server tries
  // to stay at that version for the rest of the thread, to avoid confusing the agent. ("changes"
  // messages record the base version of their code updates the same way; see AiChatMessageBody.)
  observedCodeVersion?: number;

  // If the tool failed, the error.
  error?: string;
} & ({
  // Any workpiece can potentially export files. Workspaces, in particular, export their source code
  // as files, but other workpieces may export other filesystems. Hence, a file is identified by
  // the pair of a workpiece reference (the `workpiece` chat binding name) and `filename`.
  toolName: "readFile";
  input: {workpiece?: string, filename: string};
} | {
  // Creates a generated Source backed by one Verglas worker deployment.
  toolName: "createSource";
  input: {
    title: string;
    description: string;
    outputTable: string;
    workerModule: string;
    triggers: SourceTrigger[];
    configFields: SourceConfigurationField[];
  };
  output?: {requestId: string};
} | {
  /** Creates a generated Integration Vessel with declarative setup and live verification. */
  toolName: "createIntegration";
  input: {
    title: string;
    description: string;
    module: string;
    instructions: IntegrationSetupInstruction[];
    configFields: SourceConfigurationField[];
  };
  output?: {requestId: string; vesselName: string; state: string};
} | {
  /** Inspects a chat-owned Integration's state, verification, and runtime health. */
  toolName: "inspectIntegration";
  input: {requestId: string};
  output?: Record<string, unknown>;
} | {
  /** Re-runs live verification for a chat-owned Integration. */
  toolName: "testIntegration";
  input: {requestId: string};
  output?: IntegrationVerification;
} | {
  /** Redeploys an Integration module onto the same Vessel. */
  toolName: "updateIntegration";
  input: {
    requestId: string;
    module: string;
    title?: string;
    description?: string;
    instructions?: IntegrationSetupInstruction[];
    configFields?: SourceConfigurationField[];
  };
  output?: {requestId: string; vesselName: string; state: string};
} | {
  /** Builds a standalone TypeScript Application Vessel with its own dependencies. */
  toolName: "createApplication";
  input: {
    title: string;
    description: string;
    files: Record<string, string>;
  };
  output?: {vesselName: string; previewUrl: string; screenshotUrl?: string};
} | {
  /** Builds one versioned composition of Integration, Worker, and Interface projects. */
  toolName: "createVessel";
  input: {
    /** Stable Vessel identity matching the manifest. */
    name: string;
    /** Portable compositional Vessel YAML. */
    manifest: string;
    /** Complete component projects keyed by manifest-relative project path. */
    projects: Record<string, {files: Record<string, string>}>;
  };
  output?: {vesselName: string; version: string; previewUrl: string};
} | {
  toolName: "writeFile";
  input: {
    workpiece?: string;
    filename: string;
    content: string;
  };
} | {
  toolName: "editFile";
  input: {
    workpiece?: string;
    filename: string;
    textToReplace: string;
    replacement: string;
  };
} | {
  // Describe one of the chat's bindings by name. Numeric names appear only in logs persisted
  // before named chat bindings (they were capsule indices).
  toolName: "describeBinding";
  input: {
    name: string | number;
  };
} | {
  toolName: "setBindingHook";
  input: {
    bindingName: string;
    entrypoint: string | null;
  };
} | {
  // Wire one of the chat's bindings into a workspace's own binding list. The addition is provisional
  // to the chat, recorded by a "changes" message (see `addedBindings`).
  toolName: "setVesselBinding";
  input: {
    // Chat binding name of the target workspace.
    workspace: string;
    // Chat binding name of the resource to wire into the workspace.
    source: string;
    // Name to bind the resource under within the workspace; defaults to `source`.
    name?: string;
  };

  // The added binding edge as resolved when the tool ran, recorded so crash recovery can re-adopt
  // an addition whose "changes" message never flushed (see `addedBindings`), mirroring
  // createVessel's recorded output. `changeId` is the change number of the batch that records the
  // addition. Absent only when the call failed (`error` is set).
  output?: {workspaceId: WorkpieceId, name: string, target: WorkpieceId, changeId: number};
} | {
  // Obsolete predecessor of `setVesselBinding`, from before named chat bindings; appears only in
  // old chat logs. Its additions were immediate and permanent (nothing provisional to recover),
  // so replay is a recorded no-op.
  toolName: "saveCapsuleAsBinding";
  input: {
    capsuleId: number;
    bindingName: string;
  };
} | {
  // Create a new workspace workpiece in the workspace, either empty or instantiated from a blueprint.
  toolName: "createWorkpiece";
  input: {
    // Human-readable title for the new workspace. Required: the agent always names its creations.
    title: string;

    // Name under which the workspace appears in the chat's env and, once merged, the workspace
    // default binding list (see validateBindingName()).
    bindingName: string;

    // If present, the new workspace starts with the named blueprint's files (copied into the chat's
    // proposed changes) instead of empty.
    blueprintId?: string;
  };

  // The created workspace's workpiece ID, recorded when the workspace was actually created. History
  // replay reconstructs tool outputs by re-running persisted calls, but a creation tool can't be
  // re-run; replay returns this recorded result without creating anything.
  //
  // `changeId` is the change number of the "changes" batch that records the creation (see
  // `createdVessels` on the "changes" message body), reported like writeFile/editFile report
  // theirs so reverts can be referred to precisely.
  //
  // `blueprintNotes` is present for blueprint instantiations: formatted text describing the files
  // copied in and the bindings the blueprint expects the agent to wire up. Recorded so replay
  // doesn't have to re-fetch the blueprint (whose content may have changed since).
  output?: {workspaceId: WorkpieceId, changeId?: number, blueprintNotes?: string};
} | {
  toolName: "executeCode";
  input: {
    code: string;
  };

  // Output, if the code actually ran. (Otherwise, `error` should be present.)
  output?: string;
} | {
  toolName: "giveUp";
  input: {
    error: string;
  };
} | {
  toolName: "webFetch";
  input: {
    url: string;
    // If true, return the raw response body without Markdown conversion.
    raw?: boolean;
  };

  // Output, if the fetch actually completed. (Otherwise, `error` should be present.) This is
  // stored so that the agent's chat history can be replayed without re-issuing the fetch.
  // Formatted as a YAML-frontmatter header followed by the body (see formatWebFetchResult).
  output?: string;
} | {
  // This actually shouldn't ever appear in logs unless the agent misunderstands the tool.
  toolName: "observeUserChanges";
  input: {};
} | {
  // List the blueprints the workspace owner could instantiate (their own blueprints, their
  // library, and the deployment's featured blueprints), so the agent can pass a blueprintId to
  // createVessel. The formatted text output is recorded so replay doesn't re-list.
  toolName: "listBlueprints";
  input: {};
  output?: string;
} | {
  // List the resource types a gatekeeper vendor offers, so the agent can construct a resourceUrl
  // for requestConnection. Resource patterns are only surfaced on demand (not in the system prompt).
  toolName: "listConnectableResources";
  input: {
    vendorId: string;
  };
  output?: string;
} | {
  // Ask the user to connect a gatekeeper, pre-configured as much as the agent can manage. Renders
  // an accept/deny card in the chat; non-blocking (the turn ends, and the agent is resumed if the
  // user accepts; on deny the agent is not resumed).
  toolName: "requestConnection";
  input: {
    vendorId: string;
    resourceUrl?: string;
    reason: string;

    // Name under which the resource will appear in the chat's env once accepted (see
    // `connectionRequest.bindingName`). Optional only because logs persisted before named chat
    // bindings lack it.
    bindingName?: string;
  };
  output?: string;
} | {
  /** Ask the user to delegate bounded Verglas access to a process. */
  toolName: "requestPermission";
  input: {
    principalId: string;
    resourceId: string;
    actions: VerglasAccessAction[];
    reason: string;
  };
  output?: string;
});

/** One generated Source configuration input mapped to a worker environment binding. */
export type SourceConfigurationField = {
  /** Worker environment variable name. */
  name: string;
  /** Human-readable label rendered in the setup card. */
  label: string;
  /** Input behavior; secret values are never persisted in workspace or chat storage. */
  type: "text" | "secret" | "url" | "number" | "boolean";
  /** Whether the user must provide a non-empty value before deployment. */
  required: boolean;
  /** Brief generated help text. */
  description?: string;
  /** Non-secret initial value for configuration known while generating the Source. */
  defaultValue?: string;
};

/** One concrete setup step generated for an Integration Vessel. */
export type IntegrationSetupInstruction = {
  /** Short imperative step title. */
  title: string;
  /** Exact work the user must perform and what value to bring back. */
  description: string;
  /** Optional trusted external documentation or account-setup URL. */
  url?: string;
};

/** Result of exercising an Integration's generated live verification method. */
export type IntegrationVerification = {
  /** Whether the external connection behaved as declared. */
  ok: boolean;
  /** Human-readable result safe to display. */
  message: string;
  /** ISO timestamp produced by the Integration runtime. */
  testedAt: string;
  /** End-to-end verification latency when available. */
  latencyMs?: number;
  /** Bounded, non-secret diagnostic facts returned by the generated verifier. */
  details?: Record<string, string | number | boolean | null>;
};

/** Portable trigger declaration accepted by the Verglas worker registry. */
export type SourceTrigger =
  | {type: "cron"; schedule: string; startDate?: string; catchup?: "none" | "sequential" | "parallel"}
  | {type: "webhook"; path: string}
  | {type: "event"; eventType: string};

// TODO: Extend AiToolCall for code-mode tool calls.
// - Includes inline audit logs from the action.
// - Actions can be approved or rejected inline.

// A standard output format named inline in a chat message, recorded so the message can be redrawn
// the way it was composed. Display only: the agent reads the noun as ordinary text and resolves it
// against the deployment's live catalog, so no blueprint id is carried here.
//
// Shaped like `CapsuleSpecifier`, but carries no authority: naming a format grants nothing, so
// there is no workpiece behind it.
export type MessageFormatRef = {
  // Position and length of the format's name within the message text. Exists so we can render as
  // format with icon in chat UI.
  position: number;
  length: number;

  // Denormalized so an old message still displays after the format is renamed or un-promoted.
  noun: string;
  icon: OutputIcon;
};

// Capsules are resource references that are embedded inline in a chat message. The name comes
// from the fact that they are represented as a pill-shaped inline element, and that they represent
// a capability (in the capability-based security sense).
//
// When the user is typing a chat message and inserts a link into the message, they will be
// prompted to turn the link into a capsule. Doing so implicitly creates a gatekeeper and grants
// the agent permission to use it.
export type CapsuleSpecifier = {
  // Position and length of the text within the chat message which should be replaced by the
  // capsule. The chat message contains some placeholder text which the capsule replaces. This
  // placeholder text exists mostly for ease of debugging -- it is never actually displayed to
  // the user nor the agent. Typically, the placeholder text should be an integer in square
  // brackets, where the integer is the position of the capsule within the message's capsule
  // list, e.g. `[0]`, `[1]`, etc. However, nothing should actually depend on the placeholder
  // text's content; the CapsuleSpecifier itself is all that matters.
  position: number;
  length: number;

  // ID of the workpiece, which should have been created using newGatekeeper() or similar.
  //
  // This can reference any workpiece, including workspaces. It should be called `workpieceId`, but
  // when it was introduced it could only point to gatekeepers, and a name change would break
  // existing storage.
  gatekeeperId: WorkpieceId;

  // Denormalized resource description from calling GatekeeperClient.describe() at the time of
  // insertion. We store this in the chat message to avoid the need to start up the gatekeeper
  // to ask for it again every time the message is displayed.
  description: ResourceDescription;

  // Vendor whose gatekeeper the resource came from, denormalized at insertion like `description`
  // and trusted no more than it is, so the message can show the vendor's logo without starting the
  // gatekeeper. Display metadata only, never authority.
  vendorId?: string;

  // The name under which the pasted resource appears in the chat's env (`env.NAME` in
  // executeCode). Stamped onto the persisted message at the turn-start naming chokepoint; absent
  // until then. Messages from before named chat bindings existed are stamped lazily the same
  // way. If the same workpiece already has a name in the chat's scope, that name is reused
  // rather than minting a new one.
  bindingName?: string;
};

// Identifies a Gatekeeper slash command or the built-in `/compact` command.
export type SlashCommandId = {
  gatekeeperId: WorkpieceId;
  commandId: string;
  builtin?: never;
} | {
  builtin: true;
  commandId: "compact";
};

// A slash command invocation parsed by the client.
export type SlashCommandRequest = {
  id: SlashCommandId;

  // Unparsed natural-language arguments surrounding the command, with the command itself removed.
  // The provider may consume or transform these into an agent-visible message.
  args: string;

  // Index in `args` where the user typed the command, so a transcript can show it where they put it
  // rather than implying it led the line. Display only.
  commandPosition?: number;
};

// One slash command as shown in the Workshop picker.
export type SlashCommandChoice = {
  // Selection to pass back when invoking this command.
  selection: SlashCommandId;

  // Name shown after `/`.
  name: string;

  // Short description shown in the picker.
  description: string;

  // Name of the command's provider: the offering Gatekeeper's title, or the Workshop itself for a
  // built-in command.
  providerLabel: string;

  // Optional resource label used when multiple commands share a name.
  resourceLabel?: string;

};

// One provisional streaming event emitted while an agent step is still in progress.
//
// At most one provisional stream is active per chat at a time. The client should not persist
// these events. Instead, it should display them temporarily and discard them as soon as the
// corresponding durable `message()` and/or `changes` message arrives, or when the agent stops
// running (`activeAgent` becomes unset in the chat metadata).
export type AiChatStreamEvent = {
  // The turn is summarizing older context before it can continue, or before `/compact` ends.
  type: "compacting";
} | {
  // The compaction attempt ended, whether it compacted, failed, was cancelled, or found nothing to
  // do.
  type: "compacted";

  // Set when the attempt made no checkpoint because nothing precedes the newest message to
  // summarize. Only `/compact` reports this, since an explicit command is otherwise silent.
  nothingToCompact?: boolean;
} | {
  type: "textDelta";
  delta: string;
} | {
  type: "reasoningDelta";
  delta: string;
} | {
  type: "toolCallStarted";
  toolCallId: string;
  toolName: AiToolCall["toolName"];
} | {
  // For the executeCode tool specifically, we stream the code as the AI writes it. (For all other
  // tool calls, the tool inputs are not streamed -- though writeFile and editFile separately
  // stream codeUpdate messages.)
  type: "toolCodeDelta";
  toolCallId: string;
  delta: string;
} | {
  // This is a provisional UI lifecycle event. For most tools it means the full tool call input has
  // been received, so the tool is no longer visually "in progress". executeCode does not emit this
  // during streaming; its provisional card is cleared when the final durable message arrives.
  type: "toolCallFinished";
  toolCallId: string;
} | {
  // Indicates which file the agent is currently editing, if any. This is emitted while a
  // writeFile/editFile call is streaming, and set to null when a non-edit tool becomes active.
  type: "setActiveFile";
  file: { workpieceId: WorkpieceId, filename: string } | null;
} | {
  // Streaming write/edit target file, used by the UI before the finalized tool call arrives.
  type: "toolCallTarget";
  toolCallId: string;
  file: { workpieceId: WorkpieceId, filename: string };
} | {
  // Streaming createVessel output format, used by the UI before the finalized tool call arrives.
  // Has the deployment's overrides applied, so it matches what the workspace is stamped with.
  type: "toolCallOutputFormat";
  toolCallId: string;
  output: BlueprintOutput;
} | {
  type: "toolOutputDelta";
  toolCallId: string;
  delta: string;
} | {
  type: "codeReset";
} | {
  type: "codeUpdate";
  update: Uint8Array;
};

// Interface implemented by the client to receive action-log upserts.
export interface ActionsSubscriber {
  entry(record: ActionLogEntry): void;
  ready(): void;
}

// Interface implemented by the client to receive callback notifications whenever there is new
// chat activity. Use the Workspace capability's subscribeToChat() to register a subscriber.
export interface AiChatSubscriber {
  // Sent exactly once, at the start of a subscription, before any other callbacks. Carries an
  // opaque value identifying the current chat stream generation. If a resubscribing client
  // sees a different value than on its previous subscription, any in-flight provisional stream
  // content was lost and will be re-streamed from scratch; the client should discard its
  // provisional streaming state. An unchanged value means provisional state should be kept.
  streamGeneration(generation: number): void;

  // Metadata for the given chat thread has changed, or a new chat thread was created.
  metadata(chat: AiChatMetadata): void;

  // Indicates the chat thread was deleted.
  deleted(chatId: number): void;

  // Adds a message to the chat.
  message(msg: AiChatMessage): void;

  // Delivers one persisted live-draft update for a chat branch. Subscriptions replay all currently
  // stored draft updates for a chat so newly-joined clients can reconstruct the editable branch
  // state without a separate fetch.
  draftUpdate(chatId: number, timestamp: Date, author: AiChatAuthorInfo, update: Uint8Array): void;

  // Indicates that all persisted live-draft updates for the given chat were cleared.
  draftCleared(chatId: number): void;

  // Delivers one provisional streaming event. Clients may ignore event types they don't support.
  stream(chatId: number, event: AiChatStreamEvent): void;
}

// Interface implemented by the client to receive callback notifications about console logs written
// by the workspace.
export interface ConsoleLogSubscriber {
  // Deliver a batch of logs. Often just one log is delivered at a time, but for efficiency they
  // may be batched.
  //
  // If `chatId` is non-null, then the logs were generated while running the version of the workspace
  // code including the changes in the given chat. This can be used to associate the logs with
  // an ongoing agent session and report them to that session.
  event(chatId: number | null, logs: ConsoleLogEvent[]): Promise<void>;
}

export type ConsoleLogEvent = {
  timestamp: Date;

  level: "debug" | "info" | "log" | "warn" | "error";

  // The parameters that were passed to the log function, represented as an array of serializable
  // values.
  message: any[];
}

// Summary of one workpiece, delivered via Overseer.subscribeToWorkpieces(). In v1 only
// workspace-type workpieces are published (gatekeeper workpieces -- chat capsules, ambient
// singletons, connections -- are not listed); `type` discriminates for future workpiece types.
export type WorkpieceSummary = {
  id: WorkpieceId;
  type: "vessel";

  // Display title. (For a workspace, its user-renamable title.)
  title: string;

  // The format this workpiece was built as, inherited from the blueprint it was instantiated
  // from. Absent means a generic app. The UI names and draws the workpiece from this.
  output?: BlueprintOutput;

  // The name of the Y.Doc root map that holds this workpiece's files, if it owns files (see
  // Overseer.subscribeToCode). For most workspaces this is the decimal workpiece ID; the workspace
  // migrated from before multi-vessel support keeps the legacy unnamed root "".
  filesRoot?: string;

  // If present, this workpiece exists only in the context of the given chat. The UI should display
  // it only while the given chat is open.
  //
  // For workspaces, this means the workspace is still provisional: it becomes permanent when the user
  // accepts the chat's changes through its creation message, and is deleted if those changes are
  // reverted (or the chat is deleted).
  chatId?: number;
};

// Callback interface used to receive workpiece-list updates. See Overseer.subscribeToWorkpieces().
export interface WorkpiecesSubscriber {
  // Upsert: called once per existing workpiece when the subscription starts, then again whenever
  // a workpiece is created or its summary changes (e.g. it is renamed).
  entry(summary: WorkpieceSummary): void;

  // The workpiece was deleted.
  removed(id: WorkpieceId): void;

  // Called after entry() has been called for all workpieces known so far.
  ready(): void;
}

// Information about one of a workspace's bindings, for display in the Connections tab. Returned by
// VesselClient.listBindings().
export type VesselBindingInfo = {
  // The binding name, as it appears in the workspace worker's `env`.
  name: string;

  // The workpiece that the binding points at.
  target: WorkpieceId;

  // Denormalized display info about the target.
  resourceTitle: string;
  vendorId?: string;

  // If present, this binding is still provisional to the given chat (which is necessarily the
  // `chatId` passed to listBindings(); edges pending in other chats are never listed). It becomes
  // permanent when the user accepts that chat's changes through the message that recorded it, and
  // is deleted if those changes are reverted.
  chatId?: number;
};

// An auto-approvable action kind offered by a specific connection. Aggregated from each bound
// gatekeeper's getAutoApprovableActions(); `alreadyEnabled` reports whether a matching rule exists.
export type PreApprovableAction = {
  gatekeeperId: WorkpieceId;
  resourceTitle: string;
  actionKind: ActionKind;
  alreadyEnabled: boolean;

  // Vendor of the gatekeeper holding this connection. Absent for vendorless gatekeepers.
  vendorId?: string;
};

// =======================================================================================
// Blueprint types
// =======================================================================================

// Describes how a gatekeeper was originally created. Stored on each GatekeeperRecord so that
// bindings can be recreated and blueprint metadata can be derived.
export type GatekeeperCreationSpec = {
  type: "gatekeeper";
  vendorId: string;        // identifies the gatekeeper adapter (e.g. "google")
  resourceUrl: string;
  typeUrlPattern: string;  // URL pattern from the vendor's SupportedResource (not the specific URL)
} | {
  type: "aiModel";
  modelId: string;         // the user's configured model ID
  provider: string;        // provider name (e.g. "anthropic")
  modelName: string;       // model name on the provider's API (e.g. "claude-sonnet-4-6")
} | {
  type: "agentSpawner";
  config: AgentSpawnerConfig;

  // Denormalized from the creating user's model config at binding creation time.
  // Absent when config.modelId is null. Used to populate blueprint suggestedModel
  // without requiring a live lookup.
  modelProvider?: string;
  modelName?: string;
} | {
  // A singleton gatekeeper account (e.g. the Context Library) auto-provided to every workspace as an
  // unnamed capsule so the agent can read/search it in code. Not user-configured, so excluded from
  // blueprints; re-added automatically if missing.
  type: "ambient";
  vendorId: string;        // the singleton gatekeeper's id (GATEKEEPER_<ID> suffix, lowercased)
  accountId: number;       // the owner's connected-account id for this singleton (in their user DO)
};

// User-provided metadata controlling how a gatekeeper binding should appear in blueprints.
// Stored on the binding edge (a workspace's binding-name -> gatekeeper mapping), not on the
// gatekeeper itself: two workspaces binding the same gatekeeper can annotate it differently for
// their respective blueprints. Optional: when absent, the binding is included in the blueprint
// with a generated title, empty description, and no resource suggestion.
//
// Legacy field `included` may still be present on records written by older versions of
// the workshop. The backend still honors `included: false`, but new writes omit it.
export type BlueprintBindingAnnotation = {
  title: string;           // friendly name shown to people using the blueprint
  description: string;     // explains what resource to connect (may be empty)
  suggestValue?: boolean;  // include the specific URL/model as a suggestion
};

// Symbolic target of one agent-spawner env entry in a blueprint. Workpiece IDs are
// workspace-local, so a spawner's `env` (see AgentSpawnerConfig.env) can't transfer into a
// blueprint as-is; instead each entry references either one of the blueprint's own bindings by
// name -- the user fills it at instantiation time like any other binding, and the spawner env
// entry resolves to the gatekeeper created for it -- or the blueprint's workspace itself, resolving
// to the newly instantiated workspace.
export type SpawnerEnvTarget = {
  type: "binding";

  // Key into BlueprintMetadata.bindings. May reference a binding that is also bound into the
  // workspace, or one synthesized purely to feed this spawner (see BlueprintBinding.spawnerOnly).
  name: string;
} | {
  // This spawner binding refers back to the workspace itself (the one instantiated from the
  // blueprint).
  type: "vessel";
};

// Describes one binding required by a blueprint. Stored in BlueprintMetadata.bindings as a
// Record keyed by binding name. Consumers identify bindings by their key (the binding name)
// while `title` and `description` provide user-facing text.
export type BlueprintBinding = {
  title: string;        // friendly name shown to people using the blueprint
  description: string;  // explains what resource to connect here (may be empty)

  // If true, this binding exists only to satisfy an agent spawner's env (it is referenced by
  // some spawner's `env` entry as a SpawnerEnvTarget). The user fills it at instantiation time
  // like any other binding, but the created gatekeeper is fed only to the spawner(s) referencing
  // it -- it is not bound into the workspace itself.
  spawnerOnly?: true;
} & ({
  // A regular external-resource gatekeeper binding.
  type: "gatekeeper";

  // Identifies the gatekeeper adapter (currently mapped to the workshop's
  // GATEKEEPER_<name> service binding).
  gatekeeperName: string;

  // URL pattern describing the type of resource this binding accepts.
  typeUrlPattern: string;

  // The specific resource URL from the source workspace (suggestion only).
  resourceUrl?: string;
} | {
  // An AI model binding. The user instantiating the blueprint picks one of their own
  // configured models.
  type: "aiModel";

  // The blueprint creator may suggest a particular model to use, or omit this to leave
  // it up to the recipient.
  suggestedModel?: {provider: string, modelName: string};
} | {
  // An agent spawner binding.
  type: "agentSpawner";

  // The blueprint creator may suggest a particular model to use, or omit this. (The
  // value is `null` if the suggestion is that AgentSpawnerConfig.modelId should be
  // configured as `null`. This is different from `undefined`, which means no suggestion.)
  suggestedModel?: {provider: string, modelName: string} | null;

  // Symbolic form of AgentSpawnerConfig.env: env name -> target, resolved to concrete workpiece
  // IDs at instantiation time (see SpawnerEnvTarget).
  env: Record<string, SpawnerEnvTarget>;
});

export type BlueprintScreenshotUpload = {
  mimeType: "image/jpeg" | "image/png";
  content: Uint8Array;
};

export const BLUEPRINT_SCREENSHOT_R2_PREFIX = 'screenshots/';
export const BLUEPRINT_SCREENSHOT_PATH_PREFIX = '/blueprint-screenshot/';

/** R2 key prefix for Application Vessel screenshots captured at create time. */
export const APPLICATION_SCREENSHOT_R2_PREFIX = 'application-screenshots/';

/** Public path prefix that serves Application Vessel screenshots. */
export const APPLICATION_SCREENSHOT_PATH_PREFIX = '/application-screenshot/';

/**
 * Browser-loadable URL for an Application screenshot stored under
 * {@link APPLICATION_SCREENSHOT_R2_PREFIX}.
 */
export function applicationScreenshotUrl(vesselName: string, versionMs?: number): string {
  const base = `${APPLICATION_SCREENSHOT_PATH_PREFIX}${encodeURIComponent(vesselName)}`;
  return versionMs === undefined ? base : `${base}?v=${versionMs}`;
}

export function blueprintScreenshotUrl(id: string, metadata: { screenshot?: true, lastUpdated: Date }): string | undefined {
  return metadata.screenshot ?
      `${BLUEPRINT_SCREENSHOT_PATH_PREFIX}${id}?v=${metadata.lastUpdated.valueOf()}` : undefined;
}

// General metadata about a blueprint. Stored (in slightly different wrapper records) in
// three locations: Workspace DO, User DO, and KV.
export type BlueprintMetadata = {
  title: string;
  description: string;  // longer-form description of what the blueprint does
  author: AiChatAuthorInfo;
  created: Date;

  version: number;       // increments every time the blueprint is updated
  lastUpdated: Date;

  // If present, a screenshot is stored separately from the metadata. The server uses this
  // to decide when to include a derived screenshotUrl.
  screenshot?: true;

  // What instantiating this blueprint produces. Absent means a generic app. Inherited by workspaces
  // created from this blueprint, and preserved when such a workspace is republished as a blueprint.
  output?: BlueprintOutput;

  // Key = binding name.
  bindings: Record<string, BlueprintBinding>;
};

// Public view (returned by PublicApi.getBlueprint).
export type BlueprintPublicInfo = {
  id: string;
  metadata: BlueprintMetadata;

  // If present, browser-loadable URL for the public screenshot.
  screenshotUrl?: string;
};

// Workspace-side summary (returned by Overseer.listBlueprints).
export type BlueprintVesselSummary = {
  id: string;
  title: string;
  description: string;
  version: number;
  codeVersionDate: Date;  // timestamp of the exported code version
  screenshotUrl?: string;
  dirty?: boolean;        // true if last publish failed and needs retry
};

// Where a blueprint the user owns came from. This distinguishes the case the UI cares about — the
// source workspace still exists, so it can be opened and it owns deletion of the blueprint — from
// the two cases where it does not, so no caller has to infer that from display text. `workspaceId`
// is reachable only in the case where opening it is meaningful.
export type BlueprintSource =
    // Published from a workspace that still exists. `workspaceTitle` is its current title.
    { type: "workspace"; workspaceId: string; workspaceTitle: string }
    // Published from a workspace that has since been deleted.
  | { type: "deletedWorkspace" }
    // Added to the user's library rather than published from one of their workspaces.
  | { type: "imported" };

// User-side summary (returned by AuthenticatedApi.listOwnBlueprints and getOwnBlueprint).
export type BlueprintUserSummary = {
  id: string;
  title: string;
  description: string;
  // Where this blueprint came from, and whether that origin is still openable.
  source: BlueprintSource;
  version: number;
  lastUpdated: Date;
  pinned?: boolean;
};

// User-side library summary (returned by AuthenticatedApi.listLibraryBlueprints).
export type BlueprintLibrarySummary = {
  id: string;
  metadata: BlueprintMetadata;
  addedAt: Date;
  uploaded: boolean;
  pinned?: boolean;
};

// Binding assignment (input to newWorkspaceFromBlueprint).
// When instantiating a blueprint, the user provides a Record mapping binding name ->
// assignment. Every required binding in the blueprint must have a corresponding entry.
export type BlueprintBindingAssignment = {
  type: "gatekeeper";
  accountId: number;      // user's connected account ID
  resourceUrl: string;
} | {
  type: "aiModel";
  modelId: string;        // one of the user's configured models
} | {
  type: "agentSpawner";
  modelId: string | null; // model to run, or null for no agent
};

// Common base interface for per-workpiece capabilities. Each workpiece type has its own
// subinterface (VesselClient, GatekeeperClient<T>) for type-specific operations; this base holds
// the shared identity/lifecycle surface.
export interface WorkpieceClient extends RpcTarget {
  // Get the workpiece's ID, unique among all workpieces in the workspace (of any type).
  getId(): Promise<WorkpieceId>;

  // Human-readable title, for display. For a workspace this is its user-renamable title; for a
  // gatekeeper it is the connected resource's title.
  getTitle(): Promise<string>;

  // Change the workpiece's title.
  //
  // (Note gatekeeper titles are initially based on the title of the underlying resource, but this
  // method does not change the remote resource, only the display name used locally within this
  // workspace.)
  setTitle(title: string): Promise<void>;

  // Permanently remove this workpiece from the workspace.
  //
  // For a workspace, this deletes its registry entry (including its binding map) and hooks and
  // clears its files; gatekeepers it bound survive, possibly no longer bound by any workspace. For
  // a gatekeeper, this destroys the connection itself -- distinct from merely unbinding it from
  // one workspace (VesselClient.unbind()).
  remove(): Promise<void>;
}

// Capability representing one workspace workpiece within a workspace. Obtained from
// Overseer.createVessel() or Overseer.getVessel(). Workspace-level concerns (code sync, chats,
// sharing, actions, blueprint listing) stay on Overseer; this covers the per-workspace surface.
export interface VesselClient extends WorkpieceClient {
  // Get the workspace's deployed UI code, to be run inside an iframe sandbox.
  //
  // Returns null if the workspace has no deployed UI code (e.g. if it's new, or if it's just an AI
  // agent with no code).
  getUiBundle(chatId?: number): Promise<UiBundle | null>;

  // Open an RPC interface to the workspace's server-side Durable Object facet. The frontend may pass
  // this stub into the workspace's iframe sandbox, so that the workspace UI can communicate with its
  // server side. It can also permit the coding agent to make direct calls.
  //
  // If `chatId` is specified, then the workspace will include changes currently proposed in the given
  // chat.
  //
  // @ts-ignore - TODO: Fix type instantiation issue
  connectToVessel(chatId?: number): Promise<RpcStub<any>>;

  /**
   * Renders the Workspace's UI as a PDF. If `chatId` is specified, the PDF includes changes currently
   * proposed in that chat.
   */
  exportPdf(chatId?: number): Promise<ReadableStream<Uint8Array>>;

  // --- Binding management ---
  //
  // A workspace's bindings are edges mapping a name (as it appears in the workspace worker's `env`) to
  // a target workpiece -- today always a gatekeeper. The same gatekeeper may be bound multiple
  // times in one workspace or by several workspaces, under independent names.

  // List this workspace's bindings.
  //
  // If `chatId` is specified, bindings which have been proposed but not yet accepted in the given
  // chat thread will be included.
  listBindings(chatId?: number): Promise<VesselBindingInfo[]>;

  // Get the gatekeeper bound under the given name, or null if there is no such binding.
  getBinding(name: string): Promise<GatekeeperClient<any> | null>;

  // Bind the given workpiece (a gatekeeper) into this workspace's `env` under `name`. Throws if the
  // name is invalid (see validateBindingName()), reserved, or already bound in this workspace
  // (including bound provisionally by another chat).
  //
  // If `chatId` is provided, the binding is treated like an edit made in the given chat -- it is
  // proposed, but someone needs to click "accept changes" (call `mergeChanges()`) to make it
  // final. Until then it exists only in the given chat.
  bind(name: string, target: WorkpieceId, chatId?: number): Promise<void>;

  // Like bind(), but if the target isn't already bound in this workspace, choose a name based on the
  // resource's own suggestion (deduplicated against this workspace's existing binding names). If the
  // target is already bound, does nothing. Either way, returns the target's binding name.
  bindWithSuggestedName(target: WorkpieceId, chatId?: number): Promise<string>;

  // Remove the binding with the given name. This only removes the edge from this workspace -- the
  // target gatekeeper itself survives (possibly no longer bound by any workspace); use
  // GatekeeperClient.remove() to destroy the connection itself.
  unbind(name: string): Promise<void>;

  // Rename a binding while preserving its target and blueprint annotation. Throws if `oldName`
  // does not exist or `newName` is reserved or already bound in this workspace.
  renameBinding(oldName: string, newName: string): Promise<void>;

  // Get the blueprint annotation for the named binding, if one has been set. Annotations live on
  // the binding edge, not on the target gatekeeper (see BlueprintBindingAnnotation).
  getBlueprintAnnotation(name: string): Promise<BlueprintBindingAnnotation | null>;

  // Set the blueprint annotation for the named binding.
  setBlueprintAnnotation(name: string, annotation: BlueprintBindingAnnotation): Promise<void>;

  // Create a new blueprint from this workspace's current committed code.
  // `title` defaults to the workspace's title if omitted.
  //
  // The blueprint is always owned by the workspace owner, regardless of who calls this method.
  //
  // Steps: generate ID, snapshot code, collect binding metadata, store locally, propagate
  // to User DO + KV + R2. Maintenance of existing blueprints stays on Overseer (see
  // Overseer.updateBlueprint() etc.).
  createBlueprint(title?: string, description?: string, screenshot?: BlueprintScreenshotUpload): Promise<BlueprintVesselSummary>;
}

// Capability representing one gatekeeper (connection) workpiece. Note that binding-edge
// operations -- binding names and blueprint annotations -- live on VesselClient, since a
// gatekeeper may be bound by several workspaces under different names.
export interface GatekeeperClient<Session extends RpcCompatible<Session>> extends WorkpieceClient {
  // Get the resource description, including the schema of its RPC interface.
  describe(): Promise<ResourceDescription>;

  // Open a direct session to this gatekeeper. Particularly useful when using the AI agent to talk
  // to the resource directly.
  openSession(): Promise<RpcStub<Session>>;

  // Get the creation spec describing how this gatekeeper was originally created.
  getCreationSpec(): Promise<GatekeeperCreationSpec>;

  // TODO: Get/set permissions.
}

// The level of access a collaborator (or share key) grants.
//
// - "build": full access -- edit code, use and participate in chats, manage bindings, etc. (the
//   same access the owner has, modulo the owner-only exceptions documented in sharing.md).
// - "use": may only render, interact with, and export the workspace's deployed UI (getUiBundle(),
//   connectToVessel(), and exportPdf()), plus read basic metadata.
//
// Roles are ordered build > use. A collaborator's effective role is the maximum role reachable
// from the owner through their valid permission edges, where each edge grants
// min(edge role, sharer's effective role). The owner is the implicit root at "build".
export type CollaboratorRole = "build" | "use";

// One person currently connected to a workspace.
export type PresenceParticipant = {
  // Opaque key matching this participant across add/remove events.
  key: string;
  user: AiChatAuthorInfo;
  role: CollaboratorRole;
};

// `init` delivers the full roster once on subscribe.
// `add`/`remove` (keyed by `key`) report changes thereafter.
export interface PresenceSubscriber {
  init(participants: PresenceParticipant[]): void;
  add(participant: PresenceParticipant): void;
  remove(key: string): void;
}

// Describes how one user came to have collaborator access.
export type PermissionEdge = {
  created: Date;

  // The role granted by this edge. Absent on edges created before roles were introduced; such
  // edges are treated as "build" for backwards compatibility.
  role?: CollaboratorRole;
} & ({
  // Granted directly by another user.
  type: "user";
  sharer: string;  // profile.id of the person who shared
  note?: string;
} | {
  // Gained by redeeming a share key.
  type: "shareKey";

  // The id of the share link that was redeemed (the hash of its first key). Every key of the link
  // resolves to this id, so redeeming any of them yields this one edge.
  keyId: string;
});

// Information about a single collaborator, returned by list/add operations.
export type CollaboratorInfo = {
  profile: AiChatAuthorInfo;
  addedBy: PermissionEdge[];

  // The collaborator's effective role (the maximum role reachable from the owner). Absent implies
  // "build" for backwards compatibility.
  role?: CollaboratorRole;
};

// Describes a collaborator whose access would change (or did change) as a result of a removal or
// share key revocation. Used by the preview/confirm flow, which must surface not only users who
// lose access entirely but also users who would be downgraded to a lower role.
export type AffectedCollaborator = {
  profile: AiChatAuthorInfo;
  addedBy: PermissionEdge[];

  // The effective role before the change.
  oldRole: CollaboratorRole;

  // The effective role after the change, or null if the user loses access entirely.
  newRole: CollaboratorRole | null;
};

// Information about a share link, for the management UI. Each link may have one or more keys.
// When users "copy" an existing link, they are getting a new key.
export type ShareLinkInfo = {
  linkId: string;
  note?: string;
  created: Date;
  createdBy: AiChatAuthorInfo;

  // The role granted to anyone who redeems this link. Absent implies "build" for links created
  // before roles were introduced.
  role?: CollaboratorRole;
};
