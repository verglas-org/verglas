import type { RpcStub, RpcTarget } from "cloudflare:workers";

/** A completed Workspace response that should be delivered back to the chat gateway. */
export type VesselResponse = {
  text: string;
};

/** RPC target provided by the chat gateway for the backend's eventual response. */
export interface ChatGatewayRpcTarget extends RpcTarget {
  /**
   * Deliver the completed Workspace response. Implementations must be idempotent because delivery is
   * at-least-once when response target acknowledgements fail.
   */
  onVesselResponse(response: VesselResponse): Promise<void>;
}

/** External message submission accepted by the backend gateway. */
export type SubmitExternalMessageInput = {
  // Selects the Workspaces account used to submit the message.
  // The backend trusts the gateway: supplying this email grants access as that account.
  callerEmail: string;
  // Selects the workspace to create or reuse.
  workspaceKey: string;
  // Selects the chat to create or reuse.
  chatKey: string;
  // Deduplicates the originating message and correlates the response target.
  messageKey: string;
  // Names the workspace if it must be created.
  workspaceTitle: string;
  // User text sent to Workspaces.
  prompt: string;
  // Persistent target invoked when the Workspace response is ready.
  chatGatewayRpcTarget: RpcStub<ChatGatewayRpcTarget>;
};

/** Submission result returned by the backend gateway. */
export type SubmitExternalMessageResult =
  | {
      accepted: true;
      chatPath: string;
    }
  | {
      accepted: false;
      // User-facing explanation of an actionable submission rejection.
      message: string;
    };

/** Service binding RPC interface used by chat gateway workers. */
export interface ExternalMessageGateway {
  /** Submit an external chat message for Workspace routing and execution. */
  submitExternalMessage(input: SubmitExternalMessageInput): Promise<SubmitExternalMessageResult>;
}
