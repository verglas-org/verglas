import { RpcStub as NativeRpcStub, RpcTarget } from "cloudflare:workers";
import type { RpcStub } from "capnweb";
import type {
  ActionLogEntry,
  ActionsSubscriber,
  AffectedCollaborator,
  AgentSpawnerConfig,
  AiChatAuthorInfo,
  AiChatHistoryPage,
  AiChatMessage,
  AiChatMetadata,
  AiChatSubscriber,
  BlueprintScreenshotUpload,
  BlueprintVesselSummary,
  BoundHookInfo,
  CapsuleSpecifier,
  ChatAttachmentHandle,
  ChatAttachmentUpload,
  CodeSubscriber,
  CollaboratorInfo,
  CollaboratorRole,
  ConsoleLogSubscriber,
  GatekeeperClient,
  IntegrationVerification,
  MessageFormatRef,
  ObserverBindingNeed,
  Overseer,
  PreApprovableAction,
  PresenceSubscriber,
  ShareLinkInfo,
  SlashCommandChoice,
  SlashCommandRequest,
  VesselClient,
  VerglasQueryActivity,
  VerglasQueryResult,
  WorkpieceId,
  WorkpiecesSubscriber,
  WorkspaceMetadata,
} from "@verglas/workshop-shared/api";
import type { ActionKind } from "@verglas/workshop-shared/gatekeeper";
import type { UserDurableObject, UserChatContext } from "./user";
import { VerglasCatalogClient } from "./verglas-catalog";
import {
  resolveVerglasAccessConfig,
  VerglasAccessClient,
} from "./verglas-access";

type AgentRuntimeConfig = {
  endpoint: string;
  token: string;
  tenantId: string;
};

type WorkspaceWire = {
  id: string;
  owner_id: string;
  title: string;
  pinned: boolean;
  created_at: string;
  updated_at: string;
};

type ChatWire = {
  id: string | number;
  title: string;
  model_profile: AiChatAuthorInfo | null;
  active: boolean;
  started_at: string;
  updated_at: string;
};

type MessageWire = {
  chat_id: string | number;
  sequence: string | number;
  timestamp: string;
  author: AiChatAuthorInfo;
  body: Omit<AiChatMessage, "chatId" | "sequence" | "timestamp" | "author">;
};

/** Resolves the standalone agent-runtime connection as an all-or-nothing deployment contract. */
export function resolveAgentRuntimeConfig(
  env: Cloudflare.Env,
): AgentRuntimeConfig {
  const endpoint = env.VERGLAS_AGENT_RUNTIME_URL?.trim();
  const token = env.VERGLAS_AGENT_RUNTIME_TOKEN?.trim();
  const tenantId = env.VERGLAS_TENANT_ID?.trim();
  if (!endpoint || !token || !tenantId) {
    throw new Error(
      "VERGLAS_AGENT_RUNTIME_URL, VERGLAS_AGENT_RUNTIME_TOKEN, and VERGLAS_TENANT_ID are required.",
    );
  }
  return { endpoint: endpoint.replace(/\/+$/, ""), token, tenantId };
}

/** Backend-only HTTP client for Postgres-backed Workspace and agent-run state. */
export class VerglasAgentRuntimeClient {
  /** Creates the client without exposing its service credential to the browser. */
  constructor(
    readonly config: AgentRuntimeConfig,
    readonly fetcher: typeof fetch = fetch,
  ) {}

  /** Calls one bounded agent-runtime JSON endpoint. */
  async request<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await this.fetcher.call(
      globalThis,
      `${this.config.endpoint}${path}`,
      {
        ...init,
        headers: {
          Authorization: `Bearer ${this.config.token}`,
          ...(init?.body === undefined
            ? {}
            : { "Content-Type": "application/json" }),
          ...init?.headers,
        },
      },
    );
    const text = await response.text();
    if (!response.ok) {
      let detail = text;
      try {
        detail = (JSON.parse(text) as { error?: string }).error ?? text;
      } catch {
        // Plain text remains useful.
      }
      throw new Error(
        `Verglas Agent Runtime ${path} failed: HTTP ${response.status} — ${detail}`,
      );
    }
    return (text ? JSON.parse(text) : undefined) as T;
  }

  /** Creates the durable Workspace record idempotently. */
  createWorkspace(
    id: string,
    ownerId: string,
    title: string,
  ): Promise<WorkspaceWire> {
    return this.request(`/v1/workspaces/${encodeURIComponent(id)}`, {
      method: "PUT",
      body: JSON.stringify({ tenantId: this.config.tenantId, ownerId, title }),
    });
  }

  /** Reads one owned Workspace. */
  getWorkspace(id: string, ownerId: string): Promise<WorkspaceWire> {
    return this.request(
      `/v1/workspaces/${encodeURIComponent(id)}?ownerId=${encodeURIComponent(ownerId)}`,
    );
  }
}

function mapChat(chat: ChatWire): AiChatMetadata {
  return {
    id: Number(chat.id),
    title: chat.title,
    started: new Date(chat.started_at),
    lastActive: new Date(chat.updated_at),
    ...(chat.active && chat.model_profile
      ? { activeAgent: chat.model_profile }
      : {}),
  };
}

function mapMessage(message: MessageWire): AiChatMessage {
  return {
    chatId: Number(message.chat_id),
    sequence: Number(message.sequence),
    timestamp: new Date(message.timestamp),
    author: message.author,
    ...message.body,
  } as AiChatMessage;
}

/** Workspace capability backed by Postgres and isolated Verglas agent runs rather than a DO. */
export class AgentWorkspace extends RpcTarget implements Overseer {
  readonly #runtime: VerglasAgentRuntimeClient;
  readonly #access: VerglasAccessClient | null;
  readonly #metadataSubscribers = new Set<
    RpcStub<(metadata: WorkspaceMetadata) => void>
  >();

  /** Creates one connection-scoped Workspace capability. */
  constructor(
    readonly ctx: ExecutionContext,
    readonly env: Cloudflare.Env,
    readonly user: DurableObjectStub<UserDurableObject>,
    readonly workspaceId: string,
  ) {
    super();
    this.#runtime = new VerglasAgentRuntimeClient(
      resolveAgentRuntimeConfig(env),
    );
    const accessConfig = resolveVerglasAccessConfig(env);
    this.#access = accessConfig
      ? new VerglasAccessClient(accessConfig, this.#ownerId())
      : null;
  }

  #ownerId(): string {
    const ownerId = this.user.id.name;
    if (!ownerId) throw new Error("The current user has no stable identity.");
    return ownerId;
  }

  #path(suffix = ""): string {
    return `/v1/workspaces/${encodeURIComponent(this.workspaceId)}${suffix}`;
  }

  #deny(): never {
    throw new Error(
      "This legacy Workspace operation is not available in the Verglas agent runtime.",
    );
  }

  async #chatContext(modelId: string | null): Promise<UserChatContext> {
    return await this.user.getChatContext(modelId);
  }

  /** Mints one short-lived bearer from the complete grants currently assigned to this agent. */
  async #agentScopedToken(): Promise<string> {
    if (!this.#access)
      throw new Error("Verglas tenant authorization is not configured.");
    const principalId = `agent/${this.workspaceId}`;
    const grants = await this.#access.listPrincipalGrants(principalId);
    const byResource = new Map<
      string,
      Set<(typeof grants)[number]["actions"][number]>
    >();
    for (const grant of grants) {
      const actions = byResource.get(grant.resourceId) ?? new Set();
      for (const action of grant.actions) actions.add(action);
      byResource.set(grant.resourceId, actions);
    }
    const created = await this.#access.createToken({
      name: `Workspace ${this.workspaceId} turn`,
      audience: "data-plane",
      expiresInSeconds: 15 * 60,
      grants: [...byResource].map(([resourceId, actions]) => ({
        resourceId,
        actions: [...actions],
      })),
    });
    return created.token;
  }

  async #emitMetadata(): Promise<void> {
    const metadata = await this.getMetadata();
    for (const subscriber of this.#metadataSubscribers)
      subscriber(metadata).catch(() => {});
  }

  /** Ensures the Postgres Workspace exists before returning it to a client. */
  async ensure(title = "Untitled Workspace"): Promise<void> {
    await this.#runtime.createWorkspace(
      this.workspaceId,
      this.#ownerId(),
      title,
    );
  }

  async getMetadata(): Promise<WorkspaceMetadata> {
    const [workspace, owner] = await Promise.all([
      this.#runtime.getWorkspace(this.workspaceId, this.#ownerId()),
      this.user.whoami(),
    ]);
    return { id: workspace.id, title: workspace.title, owner };
  }

  async subscribeToMetadata(
    callback: RpcStub<(metadata: WorkspaceMetadata) => void>,
  ): Promise<RpcStub<{}>> {
    const retained = callback.dup();
    this.#metadataSubscribers.add(retained);
    await retained(await this.getMetadata());
    const subscribers = this.#metadataSubscribers;
    // @ts-expect-error Native RPC targets implement the Cap'n Web disposal contract at runtime.
    return new NativeRpcStub<{}>({
      [Symbol.dispose]() {
        subscribers.delete(retained);
        retained[Symbol.dispose]();
      },
    });
  }

  async setTitle(title: string): Promise<void> {
    const normalized = title.trim();
    if (!normalized || normalized.length > 200)
      throw new Error("Workspace title is invalid.");
    await Promise.all([
      this.#runtime.request(
        this.#path(`?ownerId=${encodeURIComponent(this.#ownerId())}`),
        {
          method: "PATCH",
          body: JSON.stringify({ title: normalized }),
        },
      ),
      this.user.updateTitle(this.workspaceId, normalized),
    ]);
    await this.#emitMetadata();
  }

  async setPinned(pinned: boolean): Promise<void> {
    await Promise.all([
      this.#runtime.request(
        this.#path(`?ownerId=${encodeURIComponent(this.#ownerId())}`),
        {
          method: "PATCH",
          body: JSON.stringify({ pinned }),
        },
      ),
      this.user.updatePinned(this.workspaceId, pinned),
    ]);
  }

  async deleteSelf(): Promise<void> {
    await Promise.all([
      this.#runtime.request(
        this.#path(`?ownerId=${encodeURIComponent(this.#ownerId())}`),
        {
          method: "DELETE",
        },
      ),
      this.user.deleteWorkspace(this.workspaceId),
    ]);
  }

  async listChats(): Promise<AiChatMetadata[]> {
    const chats = await this.#runtime.request<ChatWire[]>(this.#path("/chats"));
    return chats.map(mapChat);
  }

  async listModels(): Promise<AiChatAuthorInfo[]> {
    return await this.user.listModels();
  }

  async getChatHistory(
    chatId: number,
    _beforeSequence?: number,
  ): Promise<AiChatHistoryPage> {
    const messages = await this.#runtime.request<MessageWire[]>(
      this.#path(`/chats/${chatId}/messages`),
    );
    return { messages: messages.map(mapMessage) };
  }

  async getChatMessage(
    chatId: number,
    sequence: number,
  ): Promise<AiChatMessage | undefined> {
    return (await this.getChatHistory(chatId)).messages.find(
      (message) => message.sequence === sequence,
    );
  }

  async subscribeToChat(
    subscriber: RpcStub<AiChatSubscriber>,
    _startAfter?: Date,
  ): Promise<RpcStub<{}>> {
    const retained = subscriber.dup();
    const sequences = new Map<number, number>();
    let disposed = false;
    let polling = false;
    const poll = async () => {
      if (disposed || polling) return;
      polling = true;
      try {
        const chats = await this.listChats();
        for (const chat of chats) {
          await retained.metadata(chat);
          const after = sequences.get(chat.id) ?? -1;
          const rows = await this.#runtime.request<MessageWire[]>(
            this.#path(`/chats/${chat.id}/messages?afterSequence=${after}`),
          );
          for (const row of rows) {
            const message = mapMessage(row);
            sequences.set(chat.id, message.sequence);
            await retained.message(message);
          }
        }
      } finally {
        polling = false;
      }
    };
    await retained.streamGeneration(1);
    await poll();
    const timer = setInterval(() => poll().catch(() => {}), 500);
    // @ts-expect-error Native RPC targets implement the Cap'n Web disposal contract at runtime.
    return new NativeRpcStub<{}>({
      [Symbol.dispose]() {
        disposed = true;
        clearInterval(timer);
        retained[Symbol.dispose]();
      },
    });
  }

  async newChat(
    initialMessage: string | SlashCommandRequest,
    modelId: string | null,
    capsules?: CapsuleSpecifier[],
    attachments?: ChatAttachmentHandle[],
    _formats?: MessageFormatRef[],
  ): Promise<number> {
    if (typeof initialMessage !== "string")
      throw new Error("Slash commands are not yet supported.");
    if (capsules?.length || attachments?.length) {
      throw new Error(
        "Capsules and attachments are not yet supported by the agent runtime.",
      );
    }
    const context = await this.#chatContext(modelId);
    const scopedToken = await this.#agentScopedToken();
    const result = await this.#runtime.request<{ chatId: number }>(
      this.#path("/chats"),
      {
        method: "POST",
        body: JSON.stringify({
          profile: context.profile,
          modelProfile: context.aiModel?.profile,
          modelConfig: context.aiModel?.config,
          prompt: initialMessage,
          principalId: `agent/${this.workspaceId}`,
          scopedToken,
        }),
      },
    );
    return result.chatId;
  }

  async sendChatMessage(
    chatId: number,
    message: string | SlashCommandRequest,
    modelId: string | null,
    capsules?: CapsuleSpecifier[],
    attachments?: ChatAttachmentHandle[],
    _formats?: MessageFormatRef[],
  ): Promise<void> {
    if (typeof message !== "string")
      throw new Error("Slash commands are not yet supported.");
    if (capsules?.length || attachments?.length) {
      throw new Error(
        "Capsules and attachments are not yet supported by the agent runtime.",
      );
    }
    const context = await this.#chatContext(modelId);
    const scopedToken = await this.#agentScopedToken();
    await this.#runtime.request(this.#path(`/chats/${chatId}/messages`), {
      method: "POST",
      body: JSON.stringify({
        profile: context.profile,
        modelProfile: context.aiModel?.profile,
        modelConfig: context.aiModel?.config,
        prompt: message,
        principalId: `agent/${this.workspaceId}`,
        scopedToken,
      }),
    });
  }

  async setChatTitle(chatId: number, title: string): Promise<void> {
    await this.#runtime.request(this.#path(`/chats/${chatId}`), {
      method: "PATCH",
      body: JSON.stringify({ title }),
    });
  }

  async deleteChat(chatId: number): Promise<void> {
    await this.#runtime.request(this.#path(`/chats/${chatId}`), {
      method: "DELETE",
    });
  }

  async #permissionRequest(
    requestId: string,
  ): Promise<AiChatMessage & { type: "permissionRequest" }> {
    const separator = requestId.indexOf(":");
    if (separator < 1)
      throw new Error(`Malformed permission request id: ${requestId}`);
    const chatId = Number(requestId.slice(0, separator));
    if (!Number.isInteger(chatId))
      throw new Error(`Malformed permission request id: ${requestId}`);
    const request = (await this.getChatHistory(chatId)).messages.find(
      (message) =>
        message.type === "permissionRequest" && message.requestId === requestId,
    );
    if (!request || request.type !== "permissionRequest") {
      throw new Error(`No such permission request: ${requestId}`);
    }
    if (request.principalId !== `agent/${this.workspaceId}`) {
      throw new Error(
        "Permission request principal does not belong to this Workspace.",
      );
    }
    return request;
  }

  async approvePermissionRequest(requestId: string): Promise<void> {
    const request = await this.#permissionRequest(requestId);
    if (request.state !== "pending") {
      throw new Error(`Permission request is not pending: ${requestId}`);
    }
    if (!this.#access)
      throw new Error("Verglas tenant authorization is not configured.");
    await this.#access.delegate({
      principalId: request.principalId,
      resourceId: request.resourceId,
      actions: request.actions,
    });
    const scopedToken = await this.#agentScopedToken();
    await this.#runtime.request(
      this.#path(`/permission-requests/${encodeURIComponent(requestId)}`),
      {
        method: "PATCH",
        body: JSON.stringify({ state: "approved", scopedToken }),
      },
    );
  }

  async denyPermissionRequest(requestId: string): Promise<void> {
    const request = await this.#permissionRequest(requestId);
    if (request.state !== "pending") {
      throw new Error(`Permission request is not pending: ${requestId}`);
    }
    await this.#runtime.request(
      this.#path(`/permission-requests/${encodeURIComponent(requestId)}`),
      { method: "PATCH", body: JSON.stringify({ state: "denied" }) },
    );
  }

  async stopAgent(chatId: number): Promise<void> {
    await this.#runtime.request(this.#path(`/chats/${chatId}/stop`), {
      method: "POST",
    });
  }

  async retryAgent(chatId: number, modelId: string): Promise<void> {
    const context = await this.#chatContext(modelId);
    if (!context.aiModel)
      throw new Error("Select an AI model before retrying the agent.");
    const scopedToken = await this.#agentScopedToken();
    await this.#runtime.request(this.#path(`/chats/${chatId}/retry`), {
      method: "POST",
      body: JSON.stringify({
        modelProfile: context.aiModel.profile,
        modelConfig: context.aiModel.config,
        principalId: `agent/${this.workspaceId}`,
        scopedToken,
      }),
    });
  }

  async queryVerglas(
    database: string,
    sql: string,
    maxRows?: number,
  ): Promise<VerglasQueryResult> {
    if (!this.#access)
      throw new Error("Verglas tenant authorization is not configured.");
    return await new VerglasCatalogClient(
      this.env,
      fetch,
      this.#access.sessionToken("data-plane"),
    ).query(database, sql, maxRows);
  }

  async listVerglasQueryActivity(
    _afterSequence?: number,
  ): Promise<VerglasQueryActivity[]> {
    return [];
  }

  async listSlashCommands(): Promise<SlashCommandChoice[]> {
    return [];
  }
  async subscribeToActions(
    subscriber: RpcStub<ActionsSubscriber>,
  ): Promise<RpcStub<{}>> {
    const retained = subscriber.dup();
    await retained.ready();
    // @ts-expect-error Native RPC targets implement the Cap'n Web disposal contract at runtime.
    return new NativeRpcStub<{}>({
      [Symbol.dispose]() {
        retained[Symbol.dispose]();
      },
    });
  }
  async listActions(): Promise<ActionLogEntry[]> {
    return [];
  }
  async listHooks(): Promise<BoundHookInfo[]> {
    return [];
  }
  async listPreApprovableActions(): Promise<PreApprovableAction[]> {
    return [];
  }
  async listAutoApprovedActionKinds(): Promise<
    Array<{ gatekeeperId: WorkpieceId; actionKind: ActionKind }>
  > {
    return [];
  }
  async listBlueprints(): Promise<BlueprintVesselSummary[]> {
    return [];
  }

  async subscribeToPresence(
    _subscriber: RpcStub<PresenceSubscriber>,
  ): Promise<RpcStub<{}>> {
    this.#deny();
  }
  async subscribeToWorkpieces(
    _subscriber: RpcStub<WorkpiecesSubscriber>,
  ): Promise<RpcStub<{}>> {
    this.#deny();
  }
  async createWorkpiece(_title: string): Promise<RpcStub<VesselClient>> {
    this.#deny();
  }
  async getVessel(_id: WorkpieceId): Promise<RpcStub<VesselClient>> {
    this.#deny();
  }
  async subscribeToCode(
    _subscriber: RpcStub<CodeSubscriber>,
    _fromVersion?: number,
  ): Promise<RpcStub<{}>> {
    this.#deny();
  }
  async updateCode(_update: Uint8Array, _chatId?: number): Promise<void> {
    this.#deny();
  }
  async getGatekeeperById(
    _id: WorkpieceId,
  ): Promise<GatekeeperClient<unknown>> {
    this.#deny();
  }
  async newGatekeeper(
    _accountId: number,
    _resourceUrl: string,
  ): Promise<GatekeeperClient<unknown> | null> {
    this.#deny();
  }
  async newAiModelGatekeeper(
    _modelId: string,
  ): Promise<GatekeeperClient<unknown>> {
    this.#deny();
  }
  async newAgentSpawnerGatekeeper(
    _config: AgentSpawnerConfig,
  ): Promise<GatekeeperClient<unknown>> {
    this.#deny();
  }
  async approveAction(_id: number): Promise<void> {
    this.#deny();
  }
  async rejectAction(_id: number): Promise<void> {
    this.#deny();
  }
  async enableHook(_id: number): Promise<void> {
    this.#deny();
  }
  async disableHook(_id: number): Promise<void> {
    this.#deny();
  }
  async deleteHook(_id: number): Promise<void> {
    this.#deny();
  }
  async setAutoApprovedActionKind(
    _gatekeeperId: WorkpieceId,
    _actionKind: ActionKind,
  ): Promise<void> {
    this.#deny();
  }
  async removeAutoApprovedActionKind(
    _gatekeeperId: WorkpieceId,
    _tag: string,
  ): Promise<void> {
    this.#deny();
  }
  async acceptConnectionRequest(
    _requestId: string,
    _result: { gatekeeperId: WorkpieceId },
  ): Promise<void> {
    this.#deny();
  }
  async denyConnectionRequest(_requestId: string): Promise<void> {
    this.#deny();
  }
  async configureSource(
    _requestId: string,
    _values: Record<string, string>,
  ): Promise<void> {
    this.#deny();
  }
  async runSource(
    _requestId: string,
  ): Promise<{ jobId: string; created: boolean }> {
    this.#deny();
  }
  async configureIntegration(
    _requestId: string,
    _values: Record<string, string>,
  ): Promise<void> {
    this.#deny();
  }
  async testIntegration(_requestId: string): Promise<IntegrationVerification> {
    this.#deny();
  }
  async deleteIntegration(_requestId: string): Promise<void> {
    this.#deny();
  }
  async deleteApplication(_vesselName: string): Promise<void> {
    this.#deny();
  }
  async uploadChatAttachment(
    _attachment: ChatAttachmentUpload,
    _modelId: string | null,
  ): Promise<ChatAttachmentHandle> {
    this.#deny();
  }
  async getChatAttachmentContent(
    _chatId: number,
    _id: string,
  ): Promise<Uint8Array> {
    this.#deny();
  }
  async deleteChatAttachment(_id: string): Promise<void> {
    this.#deny();
  }
  async mergeChanges(
    _chatId: number,
    _mergeThrough: number | null,
    _options?: { includeDraft?: boolean },
  ): Promise<void> {
    this.#deny();
  }
  async revertChanges(_chatId: number, _revertFrom: number): Promise<void> {
    this.#deny();
  }
  async finalizeChatDraft(_chatId: number): Promise<void> {
    this.#deny();
  }
  async discardChatDraftChanges(_chatId: number): Promise<void> {
    this.#deny();
  }
  async subscribeToConsoleLogs(
    _subscriber: RpcStub<ConsoleLogSubscriber>,
  ): Promise<RpcStub<{}>> {
    this.#deny();
  }
  async updateBlueprint(
    _blueprintId: string,
    _options: {
      title?: string;
      description?: string;
      updateCode?: boolean;
      updateBindings?: boolean;
      screenshot?: BlueprintScreenshotUpload | null;
    },
  ): Promise<void> {
    this.#deny();
  }
  async deleteBlueprint(_blueprintId: string): Promise<void> {
    this.#deny();
  }
  async retryBlueprintPublish(_blueprintId: string): Promise<void> {
    this.#deny();
  }
  async listObserverRequirements(
    _role: CollaboratorRole,
  ): Promise<ObserverBindingNeed[]> {
    return [];
  }
  async listCollaborators(): Promise<CollaboratorInfo[]> {
    return [];
  }
  async addCollaborator(
    _username: string,
    _role: CollaboratorRole,
    _note?: string,
  ): Promise<CollaboratorInfo | null> {
    this.#deny();
  }
  async removeCollaborator(
    _profileId: string,
    _keepUsers: string[],
  ): Promise<AffectedCollaborator[]> {
    this.#deny();
  }
  async previewRemoveCollaborator(
    _profileId: string,
  ): Promise<AffectedCollaborator[]> {
    this.#deny();
  }
  async createShareLink(
    _role: CollaboratorRole,
    _note?: string,
  ): Promise<{ key: string; linkId: string }> {
    this.#deny();
  }
  async newShareLinkKey(_linkId: string): Promise<{ key: string }> {
    this.#deny();
  }
  async listShareLinks(): Promise<ShareLinkInfo[]> {
    return [];
  }
  async updateShareLink(_linkId: string, _note?: string): Promise<void> {
    this.#deny();
  }
  async revokeShareLink(
    _linkId: string,
    _keepUsers: string[],
  ): Promise<AffectedCollaborator[]> {
    this.#deny();
  }
  async previewRevokeShareLink(
    _linkId: string,
  ): Promise<AffectedCollaborator[]> {
    this.#deny();
  }
}
