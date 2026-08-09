/**
 * Sealed legacy chat-log / wire tokens from the pre-Vessel Workshop.
 *
 * ONLY this file may contain these historical spellings. Live code and new
 * tool names use Workspace / Vessel terminology; history replay maps through
 * these constants when reading old Durable Object chat logs.
 */

/** Historical agent tool name for creating an in-workspace vessel workpiece. */
export const LEGACY_TOOL_CREATE_VESSEL = "createGadget";

/** Historical agent tool name for binding a vessel workpiece. */
export const LEGACY_TOOL_SET_VESSEL_BINDING = "setGadgetBinding";

/** Historical workpiece type discriminator persisted in older chat/change logs. */
export const LEGACY_WORKPIECE_TYPE_VESSEL = "gadget";

/** Historical User DO listing collection prefix (now `workspaces:`). */
export const LEGACY_USER_WORKSPACE_KV_PREFIX = "gadgets:";

/** Historical Overseer vessel-registry collection prefix (now `vessels:`). */
export const LEGACY_OVERSEER_VESSEL_KV_PREFIX = "gadgets:";

/** True when a persisted tool name is the legacy create-vessel tool. */
export function isLegacyCreateVesselTool(toolName: string): boolean {
  return toolName === LEGACY_TOOL_CREATE_VESSEL || toolName === "createWorkpiece";
}

/** True when a persisted tool name is the legacy set-vessel-binding tool. */
export function isLegacySetVesselBindingTool(toolName: string): boolean {
  return toolName === LEGACY_TOOL_SET_VESSEL_BINDING || toolName === "setVesselBinding";
}

/**
 * Maps historical chat-log tool names onto the current Workspace/Vessel names
 * so replay switches only need the modern spellings.
 */
export function normalizeLegacyToolName(toolName: string): string {
  if (toolName === LEGACY_TOOL_CREATE_VESSEL) return "createWorkpiece";
  if (toolName === LEGACY_TOOL_SET_VESSEL_BINDING) return "setVesselBinding";
  return toolName;
}
