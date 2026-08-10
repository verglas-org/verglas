import { throwLegacyVesselsRemoved } from "./legacy-vessels";
import { RpcStub, RpcTarget, newWorkersRpcResponse } from "capnweb";
import { validateRpc } from "capnweb-validate";
import type { JWTPayload } from "jose";
import { PublicApi, AuthenticatedApi, Overseer, WorkspaceMetadataWithTimestamps, AiChatAuthorInfo, AiModelConfig, ConnectedAccountsSubscriber, ConnectedAccountsFilter, GatekeeperVendorFilter, ObserverConfigCallback, BlueprintLibrarySummary, BlueprintPublicInfo, BlueprintUserSummary, BlueprintBindingAssignment, APPLICATION_SCREENSHOT_PATH_PREFIX, APPLICATION_SCREENSHOT_R2_PREFIX, BLUEPRINT_SCREENSHOT_PATH_PREFIX, BLUEPRINT_SCREENSHOT_R2_PREFIX, blueprintScreenshotUrl, ServerConfig, LoginAttempt, GatekeeperAppInfo, AdminApi, GatekeeperVendorInfo, OutputFormatOffer, ListOutputsResult, SUGGESTED_MODELS, createOpenWorkspaceError, OPEN_WORKSPACE_ERROR_CODES, type ModelRuntimeCatalogEntry, type ModelRuntimeDetection, type ModelRuntimeId, type ModelRuntimeLoginResult, type ModelRuntimeWizardAnswer, type VerglasAccessAction, type VerglasAccessIdentity, type VerglasAccessResource, type VerglasAccessTokenSummary, type VerglasCreatedAccessToken, type VerglasCreateAccessTokenInput, type VerglasCatalogSnapshot, type VerglasCreateDatabaseInput, type VerglasCreateTableInput, type VerglasDatabaseDetail, type VerglasDatabaseSummary, type VerglasIntegrationConfiguration, type VerglasTableSummary, type VerglasVesselSummary, type VerglasWorkerSummary } from '@verglas/workshop-shared/api';
import type { UiFeatureFlags } from "@verglas/workshop-shared/feature-flags";
import { getServerConfig } from "./deployment-config.js";
import { isPasswordAuthEnabled, getAuthGatekeeperAllowlist } from "./auth/config.js";
import { getAuthVendorBinding } from "./auth/auth-vendors.js";
import { PendingLogin, LoginConnectCallbackImpl } from "./auth/login-flow.js";
import { listFormatOffers, readAdminConfig } from "./admin-config.js";

// Re-export the optional-feature Durable Objects + entrypoints so they can be bound in wrangler.
export { PendingLogin, LoginConnectCallbackImpl };
import { GatekeeperUiFrame } from "@verglas/workshop-shared/gatekeeper";
import { getModel, LanguageModelGatekeeper } from "./ai-models";
import { completeText } from "./ai-invoke.js";
import { AdminSettings, AdminApiImpl } from "./admin-settings.js";
import { BlueprintKvRecord, buildBlueprintArchiveStream, listFeaturedBlueprintsFromKv, parseBlueprintArchive, randomBlueprintId, readBlueprintKvRecord } from "./blueprint-archive.js";
import { GatekeeperConnectCallbackImpl, normalizeEmail, UserDurableObject } from "./user";
import { recordAnalytics } from "./analytics";
import { handleClientErrorRequest } from "./client-errors.js";
import { verifyCfAccessJwt } from "./access.js";
import { resolveUiFeatureFlags } from "./feature-flags";
import { serveSiteLogo, SITE_LOGO_PATH } from "./site-logo.js";
import { createWorkshopLogger } from "./observability";
import { ModelRuntimeManager } from "./model-runtimes.js";
import { AgentWorkspace } from "./verglas-agent-runtime.js";
import { VerglasCatalogClient } from "./verglas-catalog.js";
import { resolveVerglasAccessConfig, userPrincipalId, VerglasAccessClient } from "./verglas-access.js";

const logger = createWorkshopLogger("workshop.server");

// Set once we've asked the AdminSettings DO to install the bundled format blueprints (see the
// fetch handler), so later requests skip the call. The DO holds the real answer.
let formatBlueprintInstallStarted = false;

function publicBlueprintInfo(id: string, metadata: BlueprintPublicInfo['metadata']): BlueprintPublicInfo {
  return {
    id,
    metadata,
    screenshotUrl: blueprintScreenshotUrl(id, metadata),
  };
}

// Re-export entrypoint types from ai-models.ts.
export { LanguageModelGatekeeper };

// Re-export entrypoint types from admin-settings.ts.
export { AdminSettings };

// Re-export entrypoint types from user.ts.
export { UserDurableObject, GatekeeperConnectCallbackImpl };

// Declare optional environment variables here since they may be omitted from wrangler.jsonc.
type Env = Cloudflare.Env & {
  // Set these if using Cloudflare Access for authentication, otherwise email/password is used.
  CF_ACCESS_AUD?: string,  // audience
  CF_ACCESS_ISS?: string,  // team URL, i.e. https://<team>.cloudflareaccess.com
  DEV?: boolean;
  FLAGS?: Flagship;
}

// =======================================================================================

@validateRpc()
class AuthenticatedApiImpl extends RpcTarget implements AuthenticatedApi {
  constructor(private ctx: ExecutionContext, private env: Env,
      private user: DurableObjectStub<UserDurableObject>,
      private abortSession: (reason: Error) => void) {
    super();

    this.adminSettings = this.ctx.exports.AdminSettings;
    this.users = this.ctx.exports.UserDurableObject;
    const accessConfig = resolveVerglasAccessConfig(this.env);
    this.access = accessConfig
      ? new VerglasAccessClient(accessConfig, this.#userId())
      : null;
  }

  private adminSettings: DurableObjectNamespace<AdminSettings>;
  private users: DurableObjectNamespace<UserDurableObject>;
  private access: VerglasAccessClient | null;

  #isAdmin(): boolean {
    let name = this.user.id.name;
    let admins = this.env.ADMINS;

    if (!name || !admins) return false;

    if (typeof admins === "string") {
      // Admins should be a JSON binding of array type, but `.env` doesn't actually let you
      // specify JSON bindings, so we also support a string that parses as JSON array.
      admins = JSON.parse(admins);
    }

    if (!Array.isArray(admins)) {
      throw new TypeError("ADMINS must be configured as an array of email addresses.");
    }

    return admins.includes(name);
  }

  async whoami(): Promise<AiChatAuthorInfo> {
    const profile = await this.user.whoami();
    const access = this.#accessClient();
    if (access) await access.identity();
    return profile;
  }

  async getAccessIdentity(): Promise<VerglasAccessIdentity> {
    const access = this.#accessClient();
    if (!access) throw new Error("Verglas tenant authorization is not configured.");
    return await access.identity();
  }

  async listAccessibleAccessResources(): Promise<VerglasAccessResource[]> {
    const access = this.#accessClient();
    if (!access) throw new Error("Verglas tenant authorization is not configured.");
    return await access.listDelegableResources();
  }

  async listAccessTokens(): Promise<VerglasAccessTokenSummary[]> {
    const access = this.#accessClient();
    if (!access) throw new Error("Verglas tenant authorization is not configured.");
    return await access.listTokens();
  }

  async createAccessToken(input: VerglasCreateAccessTokenInput): Promise<VerglasCreatedAccessToken> {
    const access = this.#accessClient();
    if (!access) throw new Error("Verglas tenant authorization is not configured.");
    return await access.createToken(input);
  }

  async revokeAccessToken(tokenId: string): Promise<void> {
    const access = this.#accessClient();
    if (!access) throw new Error("Verglas tenant authorization is not configured.");
    await access.revokeToken(tokenId);
  }

  #userId(): string {
    const id = this.user.id.name;
    if (!id) throw new Error("The authenticated user has no stable identity.");
    return id;
  }

  #accessClient(): VerglasAccessClient | null {
    return this.access;
  }

  async #requireAccess(action: VerglasAccessAction): Promise<void> {
    const access = this.#accessClient();
    if (!access) throw new Error("Verglas tenant authorization is not configured.");
    if (!await access.checkUser(this.#userId(), "tenant", action)) {
      throw new Error(`Access denied: ${action} on tenant resource.`);
    }
  }

  #catalogClient(): VerglasCatalogClient {
    const accessToken = this.#accessClient()?.sessionToken("data-plane");
    return new VerglasCatalogClient(this.env, fetch, accessToken);
  }
  setOwnDisplayName(name: string): Promise<void> {
    return this.user.setOwnDisplayName(name);
  }
  changePassword(oldHash: Uint8Array, newHash: Uint8Array): Promise<void> {
    return this.user.changePassword(oldHash, newHash);
  }
  hasPasswordLogin(): Promise<boolean> {
    return this.user.hasPasswordLogin();
  }
  async listModels(): Promise<AiChatAuthorInfo[]> {
    const records = await this.user.listModelRecords();
    const refresh = new Map<ModelRuntimeId, typeof records[number]>();
    for (const record of records) {
      const match = record.profile.id.match(/^runtime:(codex|claude-code|cursor)(?::|$)/);
      if (!match ||
          (record.profile.id !== `runtime:${match[1]}` && record.config.catalogRank !== undefined)) {
        continue;
      }
      refresh.set(match[1] as ModelRuntimeId, record);
    }
    for (const [runtime, record] of refresh) {
      try {
        const apiToken = record.config.apiToken;
        const catalog = apiToken && runtime !== "cursor"
          ? this.#apiTokenModels(runtime)
          : await new ModelRuntimeManager(this.env).listModels(runtime);
        await this.#saveRuntimeModels(runtime, apiToken, catalog);
      } catch (error) {
        logger.warn("failed to migrate native runtime model catalog", {
          event: "model-runtime.catalog.migration.failed",
          modelId: record.profile.id,
          error,
        });
      }
    }
    return await this.user.listModels();
  }
  addModel(profile: AiChatAuthorInfo, config: AiModelConfig): Promise<void> {
    return this.user.addModel(profile, config);
  }
  deleteModel(id: string): Promise<void> {
    return this.user.deleteModel(id);
  }
  detectModelRuntimes(): Promise<ModelRuntimeDetection> {
    return new ModelRuntimeManager(this.env).detect();
  }
  startModelRuntimeLogin(
      runtime: ModelRuntimeId, sessionId: string): Promise<ModelRuntimeLoginResult> {
    return new ModelRuntimeManager(this.env).startLogin(runtime, sessionId);
  }
  continueModelRuntimeLogin(
      sessionId: string, answer?: ModelRuntimeWizardAnswer): Promise<ModelRuntimeLoginResult> {
    return new ModelRuntimeManager(this.env).continueLogin(sessionId, answer);
  }
  cancelModelRuntimeLogin(sessionId: string): Promise<void> {
    return new ModelRuntimeManager(this.env).cancelLogin(sessionId);
  }
  async linkSubscriptionRuntime(runtime: ModelRuntimeId): Promise<void> {
    const manager = new ModelRuntimeManager(this.env);
    const models = await manager.listModels(runtime);
    const defaultModel = models.find(model => model.isDefault) ?? models[0];
    if (!defaultModel) throw new Error(`${runtime} has no available models.`);
    await manager.verifyLinked(runtime, defaultModel.id);
    await this.#saveRuntimeModels(runtime, "", models);
  }
  async linkTokenRuntime(runtime: ModelRuntimeId, apiToken: string): Promise<void> {
    const token = apiToken.trim();
    if (!token) throw new Error("API token is required.");
    const models = runtime === "cursor"
      ? await new ModelRuntimeManager(this.env).listModels(runtime, token)
      : this.#apiTokenModels(runtime);
    const defaultModel = models.find(model => model.isDefault) ?? models[0];
    if (!defaultModel) throw new Error(`${runtime} has no available models.`);
    if (runtime === "cursor") {
      await new ModelRuntimeManager(this.env).verifyLinked(runtime, defaultModel.id, token);
    } else {
      const config = this.#runtimeModelConfig(runtime, token, defaultModel.id);
      const initiator = await this.user.whoami();
      await completeText(getModel(this.env, config, initiator), {
        prompt: "Reply with the single word ready.",
        maxTokens: 8,
      });
    }
    await this.#saveRuntimeModels(runtime, token, models);
  }
  setQuickModel(id: string | null): Promise<void> {
    return this.user.setQuickModel(id);
  }
  getQuickModel(): Promise<null | string> {
    return this.user.getQuickModel();
  }

  async #saveRuntimeModels(
      runtime: ModelRuntimeId, apiToken: string, models: ModelRuntimeCatalogEntry[]): Promise<void> {
    const prefix = `runtime:${runtime}`;
    await this.user.replaceModels(prefix, models.map((model, catalogRank) => ({
      profile: {
        type: "agent",
        id: `${prefix}:${model.id}`,
        name: model.name,
      },
      config: this.#runtimeModelConfig(runtime, apiToken, model.id, catalogRank),
    })));
  }

  #runtimeModelConfig(
      runtime: ModelRuntimeId, apiToken: string, model: string, catalogRank?: number): AiModelConfig {
    if (apiToken && runtime === "codex") {
      return { provider: "openai", model, apiToken, catalogRank };
    }
    if (apiToken && runtime === "claude-code") {
      return { provider: "anthropic", model, apiToken, catalogRank };
    }
    return { provider: "local-runtime", runtime, model, apiToken, catalogRank };
  }

  #apiTokenModels(runtime: Exclude<ModelRuntimeId, "cursor">): ModelRuntimeCatalogEntry[] {
    const provider = runtime === "codex" ? "openai" : "anthropic";
    const defaultId = runtime === "codex" ? "gpt-5.6-sol" : "claude-sonnet-5";
    return Object.entries(SUGGESTED_MODELS[provider]).map(([id, model]) => ({
      id,
      name: model.name,
      ...(id === defaultId ? { isDefault: true } : {}),
      contextWindow: model.contextWindow,
    }));
  }

  getPreferredModel(): Promise<string | null> {
    return this.user.getPreferredModel();
  }
  setPreferredModel(id: string | null): Promise<void> {
    return this.user.setPreferredModel(id);
  }
  isOnboardingCompleted(): Promise<boolean> {
    return this.user.isOnboardingCompleted();
  }
  completeOnboarding(): Promise<void> {
    return this.user.completeOnboarding();
  }

  async setAvatar(data: Uint8Array | null): Promise<void> {
    if (data) {
      if (data.byteLength > 100 * 1024) {
        throw new Error("Avatar too large (max 100 KB)");
      }
      // Verify the data starts with a known image magic-byte header.
      let isJpeg = data[0] === 0xFF && data[1] === 0xD8 && data[2] === 0xFF;
      let isPng = data[0] === 0x89 && data[1] === 0x50 && data[2] === 0x4E && data[3] === 0x47;
      if (!isJpeg && !isPng) {
        throw new Error("Avatar must be a JPEG or PNG image");
      }
    }
    // Avatar data lives in KV (global), not the user's DO storage, so we
    // read/write it directly here to avoid routing through the DO location.
    let userId = this.user.id.name!;
    if (data) {
      await this.env.AVATARS.put(userId, data);
    } else {
      await this.env.AVATARS.delete(userId);
    }
  }
  async getAvatar(userId: string): Promise<Uint8Array | null> {
    let result = await this.env.AVATARS.get(userId, "arrayBuffer");
    if (!result) return null;
    return new Uint8Array(result);
  }

  getUiFeatureFlags(): Promise<UiFeatureFlags> {
    return resolveUiFeatureFlags(this.env, this.user.id.name!);
  }

  async #openWorkspaceInternal(id: string, shareKey?: string,
                            configureObservers?: RpcStub<ObserverConfigCallback>)
      : Promise<AgentWorkspace> {
    if (shareKey || configureObservers) {
      throw createOpenWorkspaceError(OPEN_WORKSPACE_ERROR_CODES.workspaceAccessDenied);
    }
    const record = await this.user.getVessel(id);
    if (!record || record.owner) {
      throw createOpenWorkspaceError(OPEN_WORKSPACE_ERROR_CODES.workspaceNotFound);
    }
    const result = new AgentWorkspace(this.ctx, this.env, this.user, id);
    const access = this.#accessClient();
    await Promise.all([
      result.ensure(record.title),
      access?.ensurePrincipal(`agent/${id}`, "agent", userPrincipalId(this.#userId())),
    ]);
    recordAnalytics(this.ctx, this.env, {
      event_name: "workspace_opened",
      user_id: this.user.id.toString(),
      workspace_id: id,
      source: "direct",
    });
    return result;
  }

  async openWorkspace(id: string, shareKey?: string,
                   configureObservers?: RpcStub<ObserverConfigCallback>)
      : Promise<RpcStub<Overseer>> {
    // @ts-expect-error Cap'n Web RPC stubs and native RPC stubs are compatible but the type
    //     system doesn't know this.
    return this.#openWorkspaceInternal(id, shareKey, configureObservers);
  }

  async newWorkspace(): Promise<RpcStub<Overseer>> {
    const bytes = new Uint8Array(32);
    crypto.getRandomValues(bytes);
    const id = Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
    await this.user.newWorkspace(id, "Untitled Workspace");
    recordAnalytics(this.ctx, this.env, {
      event_name: "workspace_created",
      user_id: this.user.id.toString(),
      workspace_id: id,
      source: "blank",
    });
    let result = await this.openWorkspace(id);
    if (!result) {
      throw new Error("Open failed despite newly-created workspace?");
    }
    return result;
  }

  async listWorkspaces(): Promise<WorkspaceMetadataWithTimestamps[]> {
    return this.user.listWorkspaces();
  }

  listOutputs(): Promise<ListOutputsResult> {
    return this.user.listOutputs();
  }

  async listVerglasWorkers(): Promise<VerglasWorkerSummary[]> {
    await this.#requireAccess("discover");
    const workers = await this.#catalogClient().listWorkers({withRuns: true});
    const access = this.#accessClient();
    if (access) await Promise.all(workers.flatMap(worker => [
      access.ensurePrincipal(`job/${worker.name}`, "job"),
      access.ensureResource(`job/${worker.name}`, "job"),
    ]));
    return workers;
  }

  async getVerglasWorker(name: string) {
    await this.#requireAccess("describe");
    return await this.#catalogClient().getWorker(name);
  }

  async listVerglasWorkerJobs(name: string, limit?: number) {
    await this.#requireAccess("describe");
    return await this.#catalogClient().listWorkerJobs(name, limit);
  }

  async runVerglasWorker(name: string) {
    await this.#requireAccess("execute");
    return await this.#catalogClient().runWorker(name, crypto.randomUUID());
  }

  async setVerglasWorkerState(name: string, state: "running" | "paused" | "archived"): Promise<void> {
    await this.#requireAccess("modify");
    await this.#catalogClient().setWorkerState(name, state);
  }

  async listVerglasTables(): Promise<VerglasTableSummary[]> {
    await this.#requireAccess("discover");
    const tables = await this.#catalogClient().listTables();
    const access = this.#accessClient();
    if (access) await Promise.all(tables.map(table =>
      access.ensureResource(
        `table/${table.database}/${[...table.namespace, table.name].join(".")}`,
        "table",
      )));
    return tables;
  }

  async getVerglasCatalog(): Promise<VerglasCatalogSnapshot> {
    await this.#requireAccess("discover");
    const catalog = await this.#catalogClient().getCatalog();
    const access = this.#accessClient();
    if (access) await Promise.all([
      ...catalog.databases.map(database =>
        access.ensureResource(`database/${database.name}`, "database")),
      ...catalog.tables.map(table =>
        access.ensureResource(`table/${table.database}/${[...table.namespace, table.name].join(".")}`, "table")),
      ...catalog.vectors.map(vector =>
        access.ensureResource(`vector/${vector.database}/${vector.target}/${vector.field}`, "vector_index")),
      ...catalog.graphs.map(graph =>
        access.ensureResource(`graph/${graph.database}/${graph.namespace}`, "graph")),
    ]);
    return catalog;
  }

  async getVerglasDatabase(name: string): Promise<VerglasDatabaseDetail> {
    await this.#requireAccess("describe");
    return await this.#catalogClient().getDatabase(name);
  }

  async createVerglasDatabase(input: VerglasCreateDatabaseInput): Promise<VerglasDatabaseSummary> {
    await this.#requireAccess("create_child");
    return await this.#catalogClient().createDatabase(input);
  }

  async deleteVerglasDatabase(name: string): Promise<void> {
    await this.#requireAccess("modify");
    await this.#catalogClient().deleteDatabase(name);
  }

  async createVerglasTable(input: VerglasCreateTableInput): Promise<VerglasTableSummary> {
    await this.#requireAccess("create_child");
    const table = await this.#catalogClient().createTable(input);
    const access = this.#accessClient();
    if (access) await access.ensureResource(
      `table/${table.database}/${[...table.namespace, table.name].join(".")}`,
      "table",
    );
    return table;
  }

  async deleteVerglasTable(database: string, namespace: string[], name: string): Promise<void> {
    await this.#requireAccess("modify");
    await this.#catalogClient().deleteTable(database, namespace, name);
  }

  async listVerglasVessels(): Promise<VerglasVesselSummary[]> {
    await this.#requireAccess("discover");
    const vessels = await this.#catalogClient().listVessels();
    const access = this.#accessClient();
    if (access) await Promise.all(vessels.flatMap(vessel => {
      const kind = vessel.role === "integration" ? "integration" : "application";
      return [
        access.ensurePrincipal(`vessel/${vessel.name}`, kind),
        access.ensureResource(`vessel/${vessel.name}`, kind),
      ];
    }));
    return vessels;
  }

  async getVerglasIntegrationConfiguration(name: string): Promise<VerglasIntegrationConfiguration> {
    await this.#requireAccess("describe");
    return await this.#catalogClient().getIntegrationConfiguration(name);
  }

  async configureVerglasIntegration(name: string, values: Record<string, string>): Promise<void> {
    await this.#requireAccess("modify");
    await this.#catalogClient().configureIntegration(name, values);
  }

  async deleteVerglasIntegration(name: string): Promise<void> {
    await this.#requireAccess("modify");
    await this.#catalogClient().deleteVessel(name);
  }

  async deleteVerglasApplication(name: string): Promise<void> {
    await this.#requireAccess("modify");
    await this.#catalogClient().deleteVessel(name);
  }

  async setVerglasApplicationState(name: string, state: "running" | "stopped"): Promise<void> {
    await this.#requireAccess("modify");
    await this.#catalogClient().setApplicationState(name, state);
  }

  async listOutputFormats(): Promise<OutputFormatOffer[]> {
    let offers = await listFormatOffers(this.env, await readAdminConfig(this.env));
    // Neither the agent's hint nor the binding details are part of what a user is offered here.
    return offers.map(({agentHint: _agentHint, bindings: _bindings, ...offer}) => offer);
  }

  listGatekeeperVendors(filter?: GatekeeperVendorFilter): Promise<GatekeeperVendorInfo[]> {
    return this.user.listGatekeeperVendors(filter);
  }

  connectAccount(vendorId: string, resourceUrlPatterns?: string[]): Promise<{url: string}> {
    return this.user.connectAccount(vendorId, resourceUrlPatterns);
  }

  ensureAccountResources(accountId: number, resourceUrlPatterns: string[]): Promise<{url?: string}> {
    return this.user.ensureAccountResources(accountId, resourceUrlPatterns);
  }

  listAddableGatekeepers(): Promise<GatekeeperVendorInfo[]> {
    return this.user.listAddableGatekeepers();
  }

  provisionAmbientAccount(vendorId: string): Promise<void> {
    return this.user.provisionAmbientAccount(vendorId);
  }

  subscribeConnectedAccounts(
      subscriber: RpcStub<ConnectedAccountsSubscriber>, filter?: ConnectedAccountsFilter)
      : Promise<RpcStub<{}>> {
    return this.user.subscribeConnectedAccounts(subscriber, filter);
  }

  disconnectAccount(accountId: number): Promise<void> {
    return this.user.disconnectAccount(accountId);
  }

  reconnectAccount(accountId: number): Promise<{url: string}> {
    return this.user.reconnectAccount(accountId);
  }

  startResourceConfigurator(
      accountId: number,
      resourceUrlPattern: string) {
    return this.user.startResourceConfigurator(accountId, resourceUrlPattern);
  }

  async dismissSharedWorkspace(workspaceId: string): Promise<void> {
    return this.user.forgetSharedWorkspace(workspaceId);
  }

  async listOwnBlueprints(): Promise<BlueprintUserSummary[]> {
    return this.user.listBlueprints();
  }

  async getOwnBlueprint(blueprintId: string): Promise<BlueprintUserSummary | null> {
    return this.user.getBlueprint(blueprintId);
  }

  async listLibraryBlueprints(): Promise<BlueprintLibrarySummary[]> {
    return this.user.listLibraryBlueprints();
  }

  async setBlueprintPinned(blueprintId: string, pinned: boolean): Promise<void> {
    return this.user.setBlueprintPinned(blueprintId, pinned);
  }

  async isBlueprintPinned(blueprintId: string): Promise<boolean> {
    return this.user.isBlueprintPinned(blueprintId);
  }

  async listFeaturedBlueprints(): Promise<BlueprintPublicInfo[]> {
    return (await listFeaturedBlueprintsFromKv(this.env)).map(
        blueprint => publicBlueprintInfo(blueprint.id, blueprint.metadata));
  }

  async addBlueprintToLibrary(blueprintId: string): Promise<void> {
    return this.user.addBlueprintToLibrary(blueprintId);
  }

  async removeBlueprintFromLibrary(blueprintId: string): Promise<void> {
    return this.user.removeBlueprintFromLibrary(blueprintId);
  }

  isBlueprintInLibrary(blueprintId: string): Promise<{ uploaded: boolean } | null> {
    return this.user.isBlueprintInLibrary(blueprintId);
  }

  async importBlueprint(archive: ReadableStream<Uint8Array>): Promise<string> {
    let { metadata, contentLength, content } = await parseBlueprintArchive(archive);
    delete metadata.screenshot;
    let blueprintId = randomBlueprintId();
    let r2Key = `${blueprintId}/${metadata.version}`;

    try {
      let fixedLengthStream = new FixedLengthStream(contentLength);

      await Promise.all([
        content.pipeTo(fixedLengthStream.writable),
        this.env.BLUEPRINT_CONTENT.put(r2Key, fixedLengthStream.readable),
      ]);

      let kvRecord: BlueprintKvRecord = {
        metadata,
        ownerId: this.user.id.toString(),
      };

      await this.env.BLUEPRINTS.put(blueprintId, JSON.stringify(kvRecord));

      await this.user.importBlueprint(blueprintId, metadata);

      recordAnalytics(this.ctx, this.env, {
        event_name: "blueprint_imported",
        user_id: this.user.id.toString(),
        blueprint_id: blueprintId,
      });

      return blueprintId;
    } catch (err) {
      // Try to delete what we uploaded, but don't wait for results becasue there's nothing we
      // can do if they fail, and we already have an error to throw.
      this.env.BLUEPRINTS.delete(blueprintId);
      this.env.BLUEPRINT_CONTENT.delete(r2Key);
      throw err;
    }
  }

  async newWorkspaceFromBlueprint(
    _blueprintId: string,
    _bindings: Record<string, BlueprintBindingAssignment>
  ): Promise<RpcStub<Overseer>> {
    throwLegacyVesselsRemoved();
  }

  async deleteOrphanedBlueprint(blueprintId: string): Promise<void> {
    return this.user.deleteOwnedBlueprint(blueprintId);
  }

  // --- Gatekeeper management apps ---

  // The management apps available to the current user: their connected accounts that declare a
  // top-level UI (AccountDescription.providesUi). The app id is the gatekeeper's routing id (its
  // vendor id, e.g. "context"), so each app is hosted at /gatekeepers/<vendorId>. UI-providing
  // accounts are auto-provisioned singletons (one per vendor), so the vendor id identifies them.
  async listGatekeeperApps(): Promise<GatekeeperAppInfo[]> {
    // listProvidedAccounts provisions auto-provisioned accounts first (idempotent), so their apps
    // appear in the nav even before the user opens a workspace — in a single round trip.
    let accounts = await this.user.listProvidedAccounts();
    return accounts
        .filter(account => account.description.providesUi)
        .map(account => ({
          id: account.vendorId,
          title: account.description.providesUi!.title,
          icon: account.description.providesUi!.icon,
        }));
  }

  async getGatekeeperApp(id: string): Promise<GatekeeperUiFrame | null> {
    // Self-sufficient: listProvidedAccounts provisions auto-provisioned accounts first (idempotent),
    // so a direct URL load of /gatekeepers/$id works without racing the Header's listGatekeeperApps.
    let accounts = await this.user.listProvidedAccounts();
    let app = accounts.find(account => account.vendorId === id && account.description.providesUi);
    if (!app) return null;
    // isAdmin is supplied fresh per open so admin-gated features reflect the user's current status.
    return this.user.startAccountAppUi(app.accountId, { isAdmin: this.#isAdmin() });
  }

  // --- Deployment admin ---

  async amIAdmin(): Promise<boolean> {
    return this.#isAdmin();
  }

  async getAdminApi(): Promise<RpcStub<AdminApi> | null> {
    if (!this.#isAdmin()) return null;
    // #isAdmin() guarantees a non-empty user id name. Forwarded to gatekeepers when listing the
    // resource catalog so RBAC-gated ones still surface for this admin.
    let adminUserId = this.user.id.name!;
    // @ts-expect-error Cap'n Web RPC stubs and native RPC targets are compatible but the type
    //     system doesn't know this.
    return new AdminApiImpl(this.adminSettings.getByName(""), adminUserId, this.#accessClient());
  }
}

async function serveApplicationScreenshot(env: Env, vesselName: string): Promise<Response> {
  const object = await env.BLUEPRINT_CONTENT.get(`${APPLICATION_SCREENSHOT_R2_PREFIX}${vesselName}`);
  if (!object) return new Response("Not Found", {status: 404});
  let contentType = object.httpMetadata?.contentType;
  if (contentType !== "image/jpeg" && contentType !== "image/png" && contentType !== "image/svg+xml") {
    contentType = "image/jpeg";
  }
  return new Response(object.body, {
    headers: {
      "Content-Type": contentType,
      "Cache-Control": "public, max-age=3600",
    },
  });
}

async function serveBlueprintScreenshot(env: Env, blueprintId: string): Promise<Response> {
  let object = await env.BLUEPRINT_CONTENT.get(`${BLUEPRINT_SCREENSHOT_R2_PREFIX}${blueprintId}`);
  if (!object) return new Response("Not Found", {status: 404});

  let contentType = object.httpMetadata?.contentType;
  if (contentType !== "image/jpeg" && contentType !== "image/png") {
    contentType = "image/jpeg";
  }

  return new Response(object.body, {
    headers: {
      "Content-Type": contentType,
      "Cache-Control": "public, max-age=31536000, immutable",
    },
  });
}

// Returned by startGatekeeperLogin(). Wraps the PendingLogin DO so the client awaits the login
// result through a capability (this stub) rather than a guessable id — no login id is ever exposed
// to the client. Disposing the stub (e.g. when the pop-up closes or the component unmounts) cancels
// the in-flight wait and lets the DO be evicted.
@validateRpc()
class LoginAttemptImpl extends RpcTarget implements LoginAttempt {
  constructor(private pending: DurableObjectStub<PendingLogin>) {
    super();
  }

  async wait(): Promise<string> {
    return await this.pending.awaitResult();
  }
}

@validateRpc()
class PublicApiImpl extends RpcTarget implements PublicApi {
  users: DurableObjectNamespace<UserDurableObject>;

  constructor(private ctx: ExecutionContext, private env: Env,
      private abortSession: (reason: Error) => void,
      private accessPayload?: JWTPayload) {
    super();
    this.users = this.ctx.exports.UserDurableObject;
  }

  async getServerConfig(): Promise<ServerConfig> {
    return getServerConfig(this.env);
  }

  async startGatekeeperLogin(vendorId: string): Promise<{ url: string; attempt: RpcStub<LoginAttempt> }> {
    if (!getAuthGatekeeperAllowlist(this.env).includes(vendorId)) {
      throw new Error(`Sign-in via "${vendorId}" is not enabled on this deployment.`);
    }
    const vendor = getAuthVendorBinding(this.env, vendorId);
    if (!vendor) throw new Error(`No such auth gatekeeper: ${vendorId}`);
    const desc = await vendor.describe();
    if (!desc.providesAuth) throw new Error(`"${vendorId}" does not provide authentication.`);

    // The PendingLogin DO is the rendezvous between this request and the (separate) OAuth-callback
    // invocation. The client never sees its id — we hand back an `attempt` stub instead.
    const pendingId = this.ctx.exports.PendingLogin.newUniqueId();
    const pending = this.ctx.exports.PendingLogin.get(pendingId);
    const callback = this.ctx.exports.LoginConnectCallbackImpl(
        { props: { pendingId: pendingId.toString(), vendorId } });
    // Sign-in needs only minimal scopes to verify the user's email. Capability scopes are requested
    // later through an explicit connected account.
    const { url } = await vendor.connectAccount(callback, { scopes: "auth" });
    // @ts-expect-error Cap'n Web RPC stubs and native RPC targets are compatible but the type
    //     system doesn't know this.
    return { url, attempt: new LoginAttemptImpl(pending) };
  }

  async authenticate(token: string): Promise<AuthenticatedApi> {
    let split = token.split(':');
    if (split.length !== 2) {
      throw new Error("Invalid session token.");
    }

    let userId = this.users.idFromName(split[0]);
    let stub = this.users.get(userId);
    await stub.authenticate(split[1]);
    recordAnalytics(this.ctx, this.env, {
      event_name: "user_authenticated",
      user_id: userId.toString(),
      source: "session_token",
    });
    return new AuthenticatedApiImpl(this.ctx, this.env, stub, this.abortSession);
  }

  async authenticateFromCfAccess(): Promise<AuthenticatedApi> {
    if (!this.accessPayload) {
      throw new Error("Not authenticated with Access.");
    }

    let email = this.accessPayload.email as string;
    let userId = this.users.idFromName(email);
    let stub = this.users.get(userId);
    let signupsEnabled = (await readAdminConfig(this.env)).signupsEnabled;
    let accountCreated = await stub.authenticateFromCfAccess(email, signupsEnabled);
    if (accountCreated) {
      recordAnalytics(this.ctx, this.env, {
        event_name: "account_created",
        user_id: userId.toString(),
        source: "cf_access",
      });
    }
    recordAnalytics(this.ctx, this.env, {
      event_name: "user_authenticated",
      user_id: userId.toString(),
      source: "cf_access",
    });
    return new AuthenticatedApiImpl(this.ctx, this.env, stub, this.abortSession);
  }

  async login(email: string, passwordHash: Uint8Array): Promise<string | null> {
    if (this.env.CF_ACCESS_AUD) {
      throw new Error("This deployment requires Cloudflare Access authentication.");
    }
    if (!isPasswordAuthEnabled(this.env)) {
      throw new Error("Password login is disabled on this deployment. Use a sign-in option.");
    }

    email = normalizeEmail(email);

    let id = this.users.idFromName(email);
    let user = this.users.get(id);

    let token = await user.login(passwordHash);
    if (!token) return null;

    recordAnalytics(this.ctx, this.env, {
      event_name: "user_authenticated",
      user_id: id.toString(),
      source: "password",
    });

    return `${email}:${token}`;
  }

  async createAccount(email: string, passwordHash: Uint8Array): Promise<string | null> {
    if (this.env.CF_ACCESS_AUD) {
      throw new Error("This deployment requires Cloudflare Access authentication.");
    }
    if (!isPasswordAuthEnabled(this.env)) {
      throw new Error("Password signup is disabled on this deployment. Use a sign-in option.");
    }
    if (!(await readAdminConfig(this.env)).signupsEnabled) {
      throw new Error("New signups are currently disabled on this deployment.");
    }

    email = normalizeEmail(email);

    let id = this.users.idFromName(email);
    let user = this.users.get(id);

    let token = await user.createAccount(email, passwordHash);
    if (!token) return null;

    recordAnalytics(this.ctx, this.env, {
      event_name: "account_created",
      user_id: id.toString(),
      source: "password",
    });

    return `${email}:${token}`;
  }

  async getBlueprint(id: string): Promise<BlueprintPublicInfo | null> {
    let kvRecord = await readBlueprintKvRecord(this.env, id);
    if (!kvRecord) return null;

    return publicBlueprintInfo(id, kvRecord.metadata);
  }

  async downloadBlueprint(id: string): Promise<ReadableStream<Uint8Array>> {
    let kvRecord = await readBlueprintKvRecord(this.env, id);
    if (!kvRecord) throw new Error("Blueprint not found.");

    let r2Object = await this.env.BLUEPRINT_CONTENT.get(`${id}/${kvRecord.metadata.version}`);
    if (!r2Object) throw new Error("Blueprint content not found in R2.");

    let metadata = { ...kvRecord.metadata };
    delete metadata.screenshot;

    return buildBlueprintArchiveStream(metadata, r2Object.body, r2Object.size);
  }
}

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext) {
    let url = new URL(req.url);

    if (url.pathname === SITE_LOGO_PATH) {
      return serveSiteLogo(req, env.BLUEPRINT_CONTENT);
    }

    if (url.pathname.startsWith(BLUEPRINT_SCREENSHOT_PATH_PREFIX)) {
      let blueprintId = url.pathname.slice(BLUEPRINT_SCREENSHOT_PATH_PREFIX.length);
      return serveBlueprintScreenshot(env, blueprintId);
    }

    if (url.pathname.startsWith(APPLICATION_SCREENSHOT_PATH_PREFIX)) {
      const vesselName = decodeURIComponent(
          url.pathname.slice(APPLICATION_SCREENSHOT_PATH_PREFIX.length));
      if (!vesselName) return new Response("Not Found", {status: 404});
      return serveApplicationScreenshot(env, vesselName);
    }

    // Sign-in via authentication gatekeepers happens entirely within each gatekeeper Worker (the
    // OAuth redirect lands on `/gatekeeper/<name>/oauth`); the result is bridged back to the waiting
    // browser via the `attempt` stub from PublicApi.startGatekeeperLogin(). So the backend no longer
    // hosts /auth/* callbacks.

    if (url.pathname === "/api/client-errors") {
      return handleClientErrorRequest(req, env, ctx);
    }

    if (url.pathname === "/api") {
      // Make sure the bundled format blueprints are installed. The AdminSettings DO doesn't wake
      // merely because someone deployed, so the install needs a trigger; hanging it off API
      // traffic means a fresh deployment is provisioned by its first visitor. Fire-and-forget,
      // and the DO is idempotent.
      if (!formatBlueprintInstallStarted) {
        formatBlueprintInstallStarted = true;
        ctx.waitUntil(ctx.exports.AdminSettings.getByName("").ensureFormatBlueprintsInstalled()
            .then((complete: boolean) => {
              // A partial install resolves rather than throwing, and nothing else will call the DO
              // from here, so clearing this is the whole retry: one bad archive would otherwise
              // leave the deployment half-provisioned for as long as the isolate lives.
              if (!complete) formatBlueprintInstallStarted = false;
            })
            .catch((err: unknown) => {
              // Likewise let the next request try again. The DO coalesces concurrent callers, so a
              // retry costs one comparison once it succeeds.
              formatBlueprintInstallStarted = false;
              logger.warn("failed to install bundled format blueprints", {
                event: "formats.install.trigger.failed", error: err,
              });
            }));
      }

      let accessPayload: JWTPayload | undefined;

      if (env.CF_ACCESS_AUD) {
        if (req.headers.get("Origin") !== url.origin) {
          return new Response("Cross-origin API access not allowed.", { status: 403 });
        }

        const payload = await verifyCfAccessJwt(req, env);
        if (!payload) return new Response("Invalid CF access JWT.", { status: 403 });

        if (!payload.email) {
          return new Response("Access JWT didn't specify email address.", { status: 403 });
        }

        accessPayload = payload;
      }

      // HACK: Implement `abortSession` callback by closing the websocket.
      // TODO: When ctx.abort() becomes non-experimental, consider using that instead.
      let resp: Response | undefined;
      let aborted = false;
      let abortSession = (reason: Error) => {
        aborted = true;
        resp?.webSocket?.close();
      };

      resp = await newWorkersRpcResponse(req,
          new PublicApiImpl(ctx, env, abortSession, accessPayload));

      if (aborted) {
        // Oops, we missed the abortSession() call while awaiting, apply now.
        resp?.webSocket?.close();
      }
      return resp;
    }

    return new Response("Not Found", {status: 404});
  }
} satisfies ExportedHandler<Env>;
