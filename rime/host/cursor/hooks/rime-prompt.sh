#!/bin/bash
# Coordinator loop: inject the project graph on each prompt. Fail open.
set -euo pipefail
trap 'exit 0' ERR

input=$(cat)
# shellcheck source=rime-common.sh
. "$(cd "$(dirname "$0")" && pwd)/rime-common.sh"

root=$(rime_project_root "$input")
[[ -n "$root" ]] || exit 0
namespace=$(rime_graph_namespace "$root")
rime_verglas graph create "$namespace" >/dev/null || exit 0
show=$(rime_verglas graph show "$namespace") || exit 0
rime_emit_context "$namespace" "$show"
exit 0
