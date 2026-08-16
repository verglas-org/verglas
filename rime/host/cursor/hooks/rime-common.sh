#!/bin/bash
# Shared Cursor hook helpers for the RIME coordinator and worker loops.
# Fail open: never block a session because Verglas is down.

rime_read_stdin() {
  python3 -c 'import sys; sys.stdout.write(sys.stdin.read())' 2>/dev/null || cat
}

rime_json_get() {
  local input="$1"
  local expr="$2"
  python3 -c '
import json, sys
raw = sys.stdin.read()
try:
    data = json.loads(raw) if raw.strip() else {}
except Exception:
    data = {}
expr = sys.argv[1]
cur = data
for part in expr.split("."):
    if isinstance(cur, list):
        try:
            cur = cur[int(part)]
        except Exception:
            cur = ""
            break
    elif isinstance(cur, dict):
        cur = cur.get(part, "")
    else:
        cur = ""
        break
if isinstance(cur, list):
    cur = cur[0] if cur else ""
if cur is None:
    cur = ""
print(cur if isinstance(cur, str) else str(cur))
' "$expr" <<<"$input" 2>/dev/null || true
}

rime_project_root() {
  local input="$1"
  local root
  root=$(rime_json_get "$input" "workspace_roots.0")
  if [[ -z "$root" ]]; then
    root=$(rime_json_get "$input" "workspaceRoots.0")
  fi
  if [[ -z "$root" ]]; then
    root=$(rime_json_get "$input" "cwd")
  fi
  if [[ -z "$root" ]]; then
    root=$(rime_json_get "$input" "root_path")
  fi
  printf '%s' "$root"
}

rime_graph_namespace() {
  local root="$1"
  local base slug namespace
  base="${root%/}"
  base="${base##*/}"
  slug=$(printf '%s' "$base" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]\{1,\}/_/g; s/^_\{1,\}//; s/_\{1,\}$//')
  if [[ -z "$slug" ]]; then
    slug="project"
  fi
  namespace="rime_${slug}"
  printf '%s' "${namespace:0:63}"
}

rime_verglas() {
  command -v verglas >/dev/null 2>&1 || return 1
  verglas --json "$@" 2>/dev/null
}

rime_emit_context() {
  local namespace="$1"
  local show="$2"
  python3 -c '
import json, sys
namespace = sys.argv[1]
raw = sys.argv[2]
show = {}
try:
    show = json.loads(raw) if raw.strip() else {}
except Exception:
    show = {}
graph = {
    "namespace": namespace,
    "role": "rime-project-graph",
    "loop": "coordinator",
}
for key in ("nodesTable", "edgesTable", "nodeCount", "edgeCount", "indexed", "snapshotId"):
    if key in show:
        graph[key] = show[key]
context = (
    "RIME project graph "
    + json.dumps(graph, separators=(",", ":"))
    + ". Persist experiment evidence here with VerglasGraphStore or `verglas --json graph`. Do not write RIME state to agent_memory."
    + " Style: reply in simplified technical English — short declarative sentences, active voice, no hedging or filler; state facts, numbers, next actions."
)
json.dump({"additional_context": context}, sys.stdout)
' "$namespace" "$show" 2>/dev/null || true
}
