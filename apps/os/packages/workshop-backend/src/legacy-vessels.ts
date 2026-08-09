/** Thrown when any Dynamic Worker / facet Workspace path is invoked. */
export const LEGACY_VESSELS_REMOVED =
  "Legacy Workspaces have been removed; use createApplication / createSource.";

export function throwLegacyVesselsRemoved(): never {
  throw new Error(LEGACY_VESSELS_REMOVED);
}
