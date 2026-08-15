/** Maps a host workspace onto a project-scoped RIME graph namespace. */

const GRAPH_PREFIX = "rime_";
const MAX_NAMESPACE_LENGTH = 63;
const FALLBACK_NAME = "project";

/**
 * Derives the Iceberg-safe graph namespace for one project root.
 * Distinct projects never share a graph; `agent_memory` is never used.
 */
export function projectGraphNamespace(projectRoot) {
  const base = basename(typeof projectRoot === "string" ? projectRoot : "");
  const slug = slugify(base);
  const name = slug.length === 0 ? FALLBACK_NAME : slug;
  const namespace = `${GRAPH_PREFIX}${name}`;
  return namespace.length <= MAX_NAMESPACE_LENGTH
    ? namespace
    : namespace.slice(0, MAX_NAMESPACE_LENGTH);
}

/** Reads the project root from a Cursor hook payload. */
export function projectRootFromHookInput(input) {
  if (!input || typeof input !== "object") {
    return undefined;
  }
  const roots = firstStringList(input.workspace_roots) ?? firstStringList(input.workspaceRoots);
  if (roots?.[0]) {
    return roots[0];
  }
  if (typeof input.cwd === "string" && input.cwd.trim()) {
    return input.cwd.trim();
  }
  if (typeof input.root_path === "string" && input.root_path.trim()) {
    return input.root_path.trim();
  }
  return undefined;
}

/** Renders a bounded, metadata-only graph summary for coordinator injection. */
export function compactGraphContext({ namespace, show, error } = {}) {
  const graph = {
    namespace,
    role: "rime-project-graph",
    loop: "coordinator",
    ...(show && typeof show === "object"
      ? {
          nodesTable: show.nodesTable,
          edgesTable: show.edgesTable,
          nodeCount: show.nodeCount,
          edgeCount: show.edgeCount,
          indexed: show.indexed,
          snapshotId: show.snapshotId,
        }
      : {}),
    ...(error ? { unavailable: true } : {}),
  };
  return `RIME project graph ${JSON.stringify(graph)}. Persist experiment evidence here with VerglasGraphStore or \`verglas --json graph\`. Do not write RIME state to agent_memory. Style: reply in simplified technical English — short declarative sentences, active voice, no hedging or filler; state facts, numbers, next actions.`;
}

/** Last path segment, ignoring trailing slashes. */
function basename(path) {
  const trimmed = path.replace(/\/+$/, "");
  if (trimmed.length === 0) {
    return "";
  }
  const parts = trimmed.split("/");
  return parts[parts.length - 1] ?? "";
}

/** Lowercases and collapses a name into `[a-z0-9_]+`. */
function slugify(value) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

/** Returns the first non-empty string list, if any. */
function firstStringList(value) {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const items = value.filter((item) => typeof item === "string" && item.trim());
  return items.length > 0 ? items : undefined;
}
