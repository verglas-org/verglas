import { DurableObject, RpcStub, RpcTarget } from "cloudflare:workers";
import { validateRpc } from "capnweb-validate";
import type {
  AnthropicMessagesCompat, Api, AssistantMessageEventStream, Context, Model, ModelCost,
  OpenAICompletionsCompat, ProviderHeaders, SimpleStreamOptions, StreamFunction,
} from "@earendil-works/pi-ai";
import { stream as anthropicMessagesStream } from "@earendil-works/pi-ai/api/anthropic-messages";
import { stream as googleGenerativeAiStream } from "@earendil-works/pi-ai/api/google-generative-ai";
import { stream as openaiCompletionsStream } from "@earendil-works/pi-ai/api/openai-completions";
import { stream as openaiResponsesStream } from "@earendil-works/pi-ai/api/openai-responses";
import { ANTHROPIC_MODELS } from "@earendil-works/pi-ai/providers/anthropic.models";
import { CLOUDFLARE_WORKERS_AI_MODELS } from "@earendil-works/pi-ai/providers/cloudflare-workers-ai.models";
import { GOOGLE_MODELS } from "@earendil-works/pi-ai/providers/google.models";
import { OPENAI_MODELS } from "@earendil-works/pi-ai/providers/openai.models";
import { ApprovalQueue, Gatekeeper, ResourceDescription } from '@verglas/workshop-shared/gatekeeper';
import { LanguageModelBinding } from "./ai-model-binding";
import AI_MODEL_BINDING_TYPES from "./ai-model-binding.txt";
import { AiChatAuthorInfo, AiModelConfig, SUGGESTED_MODELS, WORKERS_AI_OUTPUT_LIMIT }
  from "@verglas/workshop-shared/api";
import { completeText } from "./ai-invoke.js";
import { bridgePdfAttachments } from "./chat-attachment-pdf.js";

type GatewayMetadataContext = {
  source: "chat" | "thread-title" | "workspace-title" | "model-binding";
  workspaceId?: string;
  chatId?: number;
};

type ModelRoutingOptions = {
  sessionAffinity?: string;
  metadata?: GatewayMetadataContext;
};

/**
 * Per-call stream options accepted by a ModelHandle, extending pi's own options with
 * handle-level knobs.
 */
export type ModelStreamOptions = SimpleStreamOptions & {
  // When false, suppress the handle's per-API thinking/reasoning defaults so the request runs
  // without extended thinking (as far as the model allows). Used by completeText(): one-shot
  // calls -- titles, binding names, compaction summaries, workspace model bindings -- should be
  // quick, and none of them benefit from cross-step reasoning. Default: true.
  thinking?: boolean;
};

/**
 * A resolved model plus everything needed to stream from it: `stream` closes over the routing
 * (endpoint, auth headers, session affinity) chosen by getModel(),
 * so callers never handle credentials themselves. pi streams never throw/reject for provider
 * failures; failures surface as a final AssistantMessage with stopReason "error"/"aborted".
 */
export type ModelHandle = {
  // pi model descriptor (plain data; pi dispatches purely on `model.api`).
  model: Model<Api>;

  // Streams a response. Merges the handle's routing/auth and per-API options into whatever
  // per-call options the caller (e.g. the agent loop) passes. Assignable to pi-agent-core's
  // StreamFn (the extra ModelStreamOptions knobs are optional).
  stream: (model: Model<Api>, context: Context, options?: ModelStreamOptions)
      => AssistantMessageEventStream;

  // Status of the most recent HTTP response observed by `stream`. Reset at
  // the start of every request and set from pi's onResponse callback (which fires only once a
  // response arrives -- an SDK-level failure leaves this undefined), so consumers must read it
  // right after the request they care about completes. Turns run requests sequentially, so this
  // is safe.
  lastResponse?: { status: number };
};

// The pi API implementations we route through, keyed by `Model.api`. Import per-module (never
// `providers/all`, which drags ~30 providers into the bundle).
const API_STREAMS: Record<string, StreamFunction<Api, SimpleStreamOptions>> = {
  "anthropic-messages": anthropicMessagesStream as StreamFunction<Api, SimpleStreamOptions>,
  "openai-responses": openaiResponsesStream as StreamFunction<Api, SimpleStreamOptions>,
  "openai-completions": openaiCompletionsStream as StreamFunction<Api, SimpleStreamOptions>,
  "google-generative-ai": googleGenerativeAiStream as StreamFunction<Api, SimpleStreamOptions>,
};

const ZERO_COST: ModelCost = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };

// Consult pi's builtin catalog for cost/compat metadata of a known model id. Unknown models are
// fine (synthesized with zero cost). Import per-provider, not providers/all.
function catalogModel(provider: AiModelConfig["provider"], modelId: string): Model<Api> | undefined {
  switch (provider) {
    case "anthropic": return (ANTHROPIC_MODELS as Record<string, Model<Api>>)[modelId];
    case "openai": return (OPENAI_MODELS as Record<string, Model<Api>>)[modelId];
    case "google": return (GOOGLE_MODELS as Record<string, Model<Api>>)[modelId];
    case "cloudflare": return (CLOUDFLARE_WORKERS_AI_MODELS as Record<string, Model<Api>>)[modelId];
    case "ollama": case "local-runtime": return undefined;
    default: return undefined;
  }
}

// Token limits for a synthesized model. SUGGESTED_MODELS remains authoritative (compaction
// budgets in agent-compaction.ts are computed from it and must not change); pi's catalog fills
// gaps for models we don't list, and unknown models get conservative defaults.
function modelTokenWindow(config: AiModelConfig, catalog: Model<Api> | undefined)
    : { contextWindow: number, maxTokens: number } {
  const suggested = SUGGESTED_MODELS[config.provider]?.[config.model];
  return {
    contextWindow: suggested?.contextWindow ?? catalog?.contextWindow ?? 128_000,
    maxTokens: suggested?.outputLimit ??
        (config.provider === "cloudflare" ? WORKERS_AI_OUTPUT_LIMIT : undefined) ??
        catalog?.maxTokens ?? 4096,
  };
}

// Compat flags for a Workers AI model reached over its OpenAI-compatible REST endpoint. Matches
// pi's own generated Workers AI catalog entries.
function workersAiCompat(catalog: Model<Api> | undefined): OpenAICompletionsCompat {
  return {
    supportsStore: false,
    supportsDeveloperRole: false,
    supportsLongCacheRetention: false,
    ...(catalog?.compat as OpenAICompletionsCompat | undefined),
    sendSessionAffinityHeaders: true,
  };
}

type HandleArgs = {
  model: Model<Api>;
  // Provider auth: a plain API key (pi turns it into the SDK's native auth) and/or headers.
  apiKey?: string;
  headers?: ProviderHeaders;
  sessionAffinity?: string;
};

function makeHandle(args: HandleArgs): ModelHandle {
  const streamFn = API_STREAMS[args.model.api];
  if (!streamFn) {
    throw new Error(`Unsupported model API "${args.model.api}".`);
  }

  // Per-API extras:
  // - Anthropic: adaptive thinking (the model decides when/how much to think)` -- but only for
  //   models pi's catalog marks adaptive-capable (compat.forceAdaptiveThinking). For other
  //   Anthropic models (e.g. Haiku 4.5, which rejects the adaptive format) we pass nothing, so pi
  //   omits the `thinking` field and the provider default (no extended thinking) applies --
  //   matching the pre-pi quick-model behavior.
  // - OpenAI Responses: explicit medium reasoning effort. pi would otherwise *disable* reasoning
  //   when no effort is passed; effort selection also makes pi request encrypted reasoning
  //   content, which -- with pi's unconditional `store: false` -- preserves the old stateless
  //   ZDR behavior with reasoning carried between tool steps.
  // - Everything else: provider defaults.
  const anthropicCompat = args.model.compat as AnthropicMessagesCompat | undefined;
  const apiExtras: Record<string, unknown> =
      args.model.api === "anthropic-messages"
          ? (anthropicCompat?.forceAdaptiveThinking === true ? { thinkingEnabled: true } : {}) :
      args.model.api === "openai-responses" ? { reasoningEffort: "medium" } : {};

  const handle: ModelHandle = {
    model: args.model,
    stream: (model, context, { thinking = true, ...options } = {}) => {
      // Never let a failed request read a previous request's response metadata.
      handle.lastResponse = undefined;
      const headers: ProviderHeaders = {
        ...args.headers,
        ...options.headers,
      };
      const merged: SimpleStreamOptions = {
        // API defaults first, so an explicit per-call option can override them. `thinking: false`
        // replaces them with an explicit thinking-off request: for Anthropic pi sends
        // `thinking: {type:"disabled"}` (and knows to omit it for models that can't turn thinking
        // off, e.g. claude-fable-5); for OpenAI Responses, passing no reasoningEffort makes pi
        // disable reasoning.
        ...(thinking
            ? apiExtras
            : args.model.api === "anthropic-messages" ? { thinkingEnabled: false } : {}),
        ...options,
        ...(args.apiKey !== undefined ? { apiKey: args.apiKey } : {}),
        ...(Object.keys(headers).length > 0 ? { headers } : {}),
        // Session affinity: pi only sends it when caching isn't "none" (fine for us).
        sessionId: options.sessionId ?? args.sessionAffinity,
        onResponse: async (response, responseModel) => {
          handle.lastResponse = {
            status: response.status,
          };
          await options.onResponse?.(response, responseModel);
        },
        // PDF attachments ride pi image parts and are rewritten here into the provider's native
        // document blocks (no-op for payloads without one; see chat-attachment-pdf.ts).
        onPayload: async (payload, payloadModel) => {
          const replaced = await options.onPayload?.(payload, payloadModel);
          return bridgePdfAttachments(args.model.api, replaced ?? payload) ?? replaced;
        },
      };
      return streamFn(model, context, merged);
    },
  };
  return handle;
}

/** Resolve a model using its saved provider credential or the deployment-owned native runtime. */
export function getModel(env: Cloudflare.Env, config: AiModelConfig,
                         _initiator: AiChatAuthorInfo,
                         options: ModelRoutingOptions = {}): ModelHandle {
  return getModelDirect(env, config, options.sessionAffinity);
}

// Direct provider access using the credentials in the model config itself.
function getModelDirect(
    env: Cloudflare.Env, config: AiModelConfig, sessionAffinity?: string): ModelHandle {
  const catalog = catalogModel(config.provider, config.model);
  const window = modelTokenWindow(config, catalog);
  switch (config.provider) {
    case "anthropic":
      return makeHandle({
        model: {
          id: config.model,
          name: catalog?.name ?? config.model,
          api: "anthropic-messages",
          provider: "anthropic",
          baseUrl: config.apiUrl ?? "https://api.anthropic.com",
          reasoning: true,
          input: catalog?.input ?? ["text", "image"],
          cost: catalog?.cost ?? ZERO_COST,
          ...window,
          thinkingLevelMap: catalog?.thinkingLevelMap,
          // Catalog compat verbatim -- see the gateway-path comment on forceAdaptiveThinking.
          compat: catalog?.compat,
        },
        apiKey: config.apiToken,
        sessionAffinity,
      });
    case "cloudflare": {
      // Workers AI is fetch-only (no Workers-binding transport), so the user's own account ID and
      // API token come from the
      // model config. (The REST endpoint is account-scoped, hence the extra accountId field.)
      if (!config.accountId || !config.apiToken) {
        throw new Error(
            "This Workers AI model has no Cloudflare credentials. Re-add it with your " +
            "Cloudflare account ID and an API token that permits Workers AI.");
      }
      return makeHandle({
        model: {
          id: config.model,
          name: catalog?.name ?? config.model,
          api: "openai-completions",
          provider: "cloudflare-workers-ai",
          baseUrl: `https://api.cloudflare.com/client/v4/accounts/${config.accountId}/ai/v1`,
          reasoning: catalog?.reasoning ?? false,
          input: catalog?.input ?? ["text"],
          cost: catalog?.cost ?? ZERO_COST,
          ...window,
          compat: workersAiCompat(catalog),
        },
        apiKey: config.apiToken,
        sessionAffinity,
      });
    }
    case "google":
      return makeHandle({
        model: {
          id: config.model,
          name: catalog?.name ?? config.model,
          api: "google-generative-ai",
          provider: "google",
          baseUrl: config.apiUrl ?? "https://generativelanguage.googleapis.com/v1beta",
          reasoning: catalog?.reasoning ?? true,
          input: catalog?.input ?? ["text", "image"],
          cost: catalog?.cost ?? ZERO_COST,
          ...window,
          thinkingLevelMap: catalog?.thinkingLevelMap,
        },
        apiKey: config.apiToken,
        sessionAffinity,
      });
    case "ollama":
      // `apiUrl` is the Ollama server base; its OpenAI-compat endpoint lives under /v1. Accept
      // (and strip) a trailing `/api` or `/v1` path: configs saved before the pi migration store
      // the native-API base `http://host:11434/api` (the old ollama provider's convention), and
      // users may paste the /v1 endpoint directly. When no API key was configured we assume
      // local auth and send no Authorization header at all (as before the pi migration; a strict
      // local proxy may reject an unexpected bearer token): the OpenAI SDK requires *some* key,
      // so give it a placeholder while a null default header deletes the Authorization header
      // the SDK derives from it.
      return makeHandle({
        model: {
          id: config.model,
          name: config.model,
          api: "openai-completions",
          provider: "ollama",
          baseUrl: `${(config.apiUrl ?? "http://localhost:11434")
              .replace(/\/+$/, "").replace(/\/(api|v1)$/, "")}/v1`,
          reasoning: true,
          input: ["text", "image"],
          cost: ZERO_COST,
          ...window,
        },
        ...(config.apiToken === ""
            ? { apiKey: "unused", headers: { Authorization: null } }
            : { apiKey: config.apiToken }),
        sessionAffinity,
      });
    case "local-runtime": {
      const endpoint = config.apiUrl || env.LOCAL_MODEL_RUNTIME_URL?.trim();
      const token = env.LOCAL_MODEL_RUNTIME_TOKEN?.trim();
      if (!endpoint || !token) {
        throw new Error("The native model runtime adapter is not configured.");
      }
      const baseUrl = endpoint.replace(/\/+$/, "").replace(/\/v1$/, "");
      return makeHandle({
        model: {
          id: config.model,
          name: config.model,
          api: "openai-completions",
          provider: "local-runtime",
          baseUrl: `${baseUrl}/v1`,
          reasoning: true,
          input: ["text", "image"],
          cost: ZERO_COST,
          ...window,
        },
        apiKey: token,
        headers: {
          ...(sessionAffinity ? { "x-runtime-session-key": `workshop:${sessionAffinity}` } : {}),
          ...(config.runtime ? { "x-model-runtime": config.runtime } : {}),
          ...(config.apiToken ? { "x-provider-api-key": config.apiToken } : {}),
        },
        sessionAffinity,
      });
    }
    case "openai":
      return makeHandle({
        model: {
          id: config.model,
          name: catalog?.name ?? config.model,
          api: "openai-responses",
          provider: "openai",
          baseUrl: config.apiUrl ?? "https://api.openai.com/v1",
          reasoning: catalog?.reasoning ?? true,
          input: catalog?.input ?? ["text", "image"],
          cost: catalog?.cost ?? ZERO_COST,
          ...window,
          thinkingLevelMap: catalog?.thinkingLevelMap,
          compat: catalog?.compat,
        },
        apiKey: config.apiToken,
        sessionAffinity,
      });
    default:
      config.provider satisfies never;
      throw new Error(`Unknown provider "${config.provider}".`);
  }
}

// =======================================================================================

export type LanguageModelGatekeeperProps = {
  displayName: string,
  config: AiModelConfig,
  initiator: AiChatAuthorInfo,
  metadata?: GatewayMetadataContext,
};

export class LanguageModelGatekeeper
    extends DurableObject<Cloudflare.Env, LanguageModelGatekeeperProps>
    implements Gatekeeper<LanguageModelBinding> {
  async describe(): Promise<ResourceDescription> {
    let modelConfig = this.ctx.props.config;
    let displayName = this.ctx.props.displayName;

    return {
      // TODO: Decide if we need real URLs or if `url` should stop being part of the description.
      url: `http://models.local/${modelConfig.provider}/${modelConfig.model}`,

      title: displayName,
      snippet: "An AI large language model.",

      suggestedBindingName: "LLM",

      tsType: "LanguageModelBinding",
    };
  }

  async getTypeScriptTypes(): Promise<string> {
    return AI_MODEL_BINDING_TYPES;
  }

  async getAutoApprovableActions() {
    return [];
  }

  async startSession(approvalQueue: RpcStub<ApprovalQueue>)
      : Promise<LanguageModelBinding> {
    let model = getModel(this.env, this.ctx.props.config, this.ctx.props.initiator, {
      metadata: this.ctx.props.metadata,
    });
    return new LanguageModelBindingImpl(model);
  }

  applyAction(action: number): Promise<void> {
    throw new Error("This gatekeeper implements no actions.");
  }
  rejectAction(action: number): Promise<void | {restart?: boolean}> {
    throw new Error("This gatekeeper implements no actions.");
  }
  revertAction(action: number):
      Promise<void | {message?: string, canRetry?: boolean, restart?: boolean}> {
    throw new Error("This gatekeeper implements no actions.");
  }

  async addObserver(_id: string, _user: Fetcher): Promise<void> {
    // An AI model is not a restricted-access resource: nothing read through it identifies the
    // observer or leaks private data, so any observer is permitted. No-op (never throws).
  }

  async removeObserver(_id: string): Promise<void> {
    // No observer state is tracked (see addObserver). Idempotent no-op.
  }
}

@validateRpc()
class LanguageModelBindingImpl extends RpcTarget implements LanguageModelBinding {
  constructor(private model: ModelHandle) {
    super();
  }

  async run(options: {prompt: string, systemPrompt?: string}): Promise<string> {
    // TODO: Should we be calling authorizeObservation() here? It's not really observing anything,
    //   but you might want the audit logs?
    // TODO: Account LLM costs back to the calling workspace.
    return await completeText(this.model, {
      prompt: options.prompt,
      systemPrompt: options.systemPrompt,
    });
  }
}
