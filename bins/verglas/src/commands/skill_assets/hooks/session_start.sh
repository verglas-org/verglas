#!/usr/bin/env bash
# Verglas session-start memory injection, shared by Claude Code, Codex, and
# Cursor. Fetches a bounded session-context block from the tenant's memory MCP
# (cognee's `session_context` tool, reached at the tenant's container ingress)
# and injects it as additional context for the session.
#
# Transport: a single JSON-RPC `tools/call` over streamable HTTP (curl). The
# endpoint URL and the bearer are read from ~/.verglas/credentials (the files
# `verglas skills install` wrote from `verglas login`); env overrides win for
# tests/dev. Dependency-light: curl + python3 only.
#
# STRICTLY read-only against memory: it calls `session_context`, which assembles
# and returns a context block and writes nothing. Fail-open: any error/timeout
# emits nothing (exit 0) so a fresh session is never blocked.
#
# Child-hook suppression: a Verglas-spawned child gets no injection.
[ -n "$VERGLAS_CONSOLIDATION_CHILD" ] && exit 0
set +e
HARNESS="${1:-claude}"
ENDPOINT="${VERGLAS_MCP_ENDPOINT:-$(cat "__VERGLAS_MCP_ENDPOINT_FILE__" 2>/dev/null)}"
BEARER="${VERGLAS_MCP_BEARER:-$(cat "__VERGLAS_MCP_BEARER_FILE__" 2>/dev/null)}"
cat >/dev/null 2>&1 # drain stdin
[ -z "$ENDPOINT" ] && exit 0
[ -z "$BEARER" ] && exit 0

REQ='{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"session_context","arguments":{"max_tokens":1200}}}'
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
            "hookEventName": "SessionStart",
            "additionalContext": block,
        }
    }))
PY
exit 0
