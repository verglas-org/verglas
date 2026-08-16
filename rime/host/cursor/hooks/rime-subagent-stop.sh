#!/bin/bash
# Worker loop: tell the coordinator a rime-worker finished. Do not write the graph.
set -euo pipefail
trap 'exit 0' ERR

input=$(cat)
# shellcheck source=rime-common.sh
. "$(cd "$(dirname "$0")" && pwd)/rime-common.sh"

kind=$(rime_json_get "$input" "subagent_type")
if [[ -z "$kind" ]]; then
  kind=$(rime_json_get "$input" "subagentType")
fi
if [[ "$kind" != "rime-worker" && "$kind" != "best-of-n-runner" ]]; then
  exit 0
fi

status=$(rime_json_get "$input" "status")
root=$(rime_project_root "$input")
namespace=""
if [[ -n "$root" ]]; then
  namespace=$(rime_graph_namespace "$root")
fi
workspace=$(rime_json_get "$input" "workspace")
if [[ -z "$workspace" ]]; then
  workspace=$(rime_json_get "$input" "cwd")
fi

python3 -c '
import json, sys
kind, status, namespace, workspace = sys.argv[1:5]
message = (
    "RIME worker loop finished: agent="
    + kind
    + " status="
    + (status or "unknown")
    + " workspace="
    + (workspace or "unspecified")
    + " graph="
    + (namespace or "unspecified")
    + ". Coordinator must evaluate independently and persist evidence to the project graph. Do not treat this notice as a score."
)
json.dump({"followup_message": message}, sys.stdout)
' "$kind" "$status" "$namespace" "$workspace" 2>/dev/null || true
exit 0
