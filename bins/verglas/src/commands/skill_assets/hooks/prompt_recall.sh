#!/usr/bin/env bash
# Verglas per-prompt recall injection, shared by Claude Code, Codex, and Cursor.
# Runs on UserPromptSubmit: it reads the user's prompt from the hook JSON on
# stdin, recalls the memories most relevant to it (cognee's `recall` tool at the
# tenant's memory MCP), and injects them as additional context for the turn.
#
# `recall` is cognee's structured retrieval (scored results with provenance —
# the CHUNKS/SUMMARIES-style mode, NOT a GRAPH_COMPLETION natural-language
# answer), which is what cognee recommends for injecting context into an agent
# prompt. Transport + config + fail-open behavior mirror session_start.sh.
#
# STRICTLY read-only against memory. Fail-open: any error/timeout emits nothing
# (exit 0) so a turn is never blocked.
[ -n "$VERGLAS_CONSOLIDATION_CHILD" ] && exit 0
set +e
HARNESS="${1:-claude}"
ENDPOINT="${VERGLAS_MCP_ENDPOINT:-$(cat "__VERGLAS_MCP_ENDPOINT_FILE__" 2>/dev/null)}"
BEARER="${VERGLAS_MCP_BEARER:-$(cat "__VERGLAS_MCP_BEARER_FILE__" 2>/dev/null)}"

INPUT="$(cat)"
[ -z "$ENDPOINT" ] && exit 0
[ -z "$BEARER" ] && exit 0

PROMPT="$(printf '%s' "$INPUT" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
    print(d.get("prompt") or d.get("user_prompt") or "")
except Exception:
    print("")
' 2>/dev/null)"
[ -z "$PROMPT" ] && exit 0

REQ="$(python3 - "$PROMPT" <<'PY' 2>/dev/null
import json, sys
print(json.dumps({
    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
    "params": {"name": "recall", "arguments": {"query": sys.argv[1], "k": 5}},
}))
PY
)"
[ -z "$REQ" ] && exit 0

RESP="$(curl -fsS --max-time "${VERGLAS_MCP_TIMEOUT:-5}" -X POST "$ENDPOINT" \
  -H "Authorization: Bearer $BEARER" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  --data "$REQ" 2>/dev/null)"
[ -z "$RESP" ] && exit 0

BLOCK="$(printf '%s' "$RESP" | python3 -c '
import sys, json
raw = sys.stdin.read()
def objs(raw):
    t = raw.strip()
    try:
        return [json.loads(t)]
    except Exception:
        pass
    out = []
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("data:"):
            line = line[5:].strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out
texts = []
for o in objs(raw):
    r = o.get("result") if isinstance(o, dict) else None
    if not isinstance(r, dict):
        continue
    for c in r.get("content", []):
        if isinstance(c, dict) and c.get("type") == "text" and c.get("text"):
            texts.append(c["text"])
print("\n".join(texts))
' 2>/dev/null)"
[ -z "$BLOCK" ] && exit 0

python3 - "$HARNESS" "$BLOCK" <<'PY' 2>/dev/null
import json, sys
harness, block = sys.argv[1], sys.argv[2]
if harness == "cursor":
    print(json.dumps({"additional_context": block}))
else:
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": block,
        }
    }))
PY
exit 0
