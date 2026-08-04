#!/usr/bin/env bash
# Verglas session-close consolidation, shared by Claude Code, Codex, and Cursor.
# Launched on Stop / SessionEnd / PreCompact (or the harness equivalent). It reads
# the hook JSON on stdin, extracts the session's transcript, and posts the session
# content to the tenant's memory MCP via cognee's `remember` tool — the API-mode
# consolidation call (cognee builds the graph internally; we do NOT call the
# lower-level `cognify`, which cognee keeps internal and recommends against for
# integrations). The write runs DETACHED so session close is never blocked.
#
# Config + transport mirror the injection hooks. Fail-open: any error exits 0 and
# the host session close is never blocked.
[ -n "$VERGLAS_CONSOLIDATION_CHILD" ] && exit 0
set +e
HARNESS="${1:-claude}"
ENDPOINT="${VERGLAS_MCP_ENDPOINT:-$(cat "__VERGLAS_MCP_ENDPOINT_FILE__" 2>/dev/null)}"
BEARER="${VERGLAS_MCP_BEARER:-$(cat "__VERGLAS_MCP_BEARER_FILE__" 2>/dev/null)}"

INPUT="$(cat)"
[ -z "$ENDPOINT" ] && exit 0
[ -z "$BEARER" ] && exit 0

# Extract a bounded chunk of the session content: the harness transcript when a
# path is present (Claude Code / Codex), else the raw hook JSON. Capped so a long
# session posts one bounded memory, not the whole log.
CONTENT="$(printf '%s' "$INPUT" | python3 -c '
import json, sys
raw = sys.stdin.read()
try:
    d = json.loads(raw)
except Exception:
    d = {}
path = d.get("transcript_path") if isinstance(d, dict) else None
text = ""
if path:
    try:
        with open(path) as f:
            parts = []
            for line in f:
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                msg = ev.get("message") if isinstance(ev, dict) else None
                if isinstance(msg, dict):
                    c = msg.get("content")
                    if isinstance(c, str):
                        parts.append(c)
                    elif isinstance(c, list):
                        for b in c:
                            if isinstance(b, dict) and b.get("type") == "text" and b.get("text"):
                                parts.append(b["text"])
            text = "\n".join(parts)
    except Exception:
        text = ""
if not text:
    sid = d.get("session_id") or d.get("conversation_id") or ""
    text = ("session " + str(sid)).strip()
# Bound the memory: keep the tail, which holds the outcome of the session.
cap = 6000
if len(text) > cap:
    text = text[-cap:]
print(text)
' 2>/dev/null)"
[ -z "$CONTENT" ] && exit 0

REQ="$(python3 - "$CONTENT" <<'PY' 2>/dev/null
import json, sys
print(json.dumps({
    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
    "params": {
        "name": "remember",
        "arguments": {"content": sys.argv[1], "kind": "reflection"},
    },
}))
PY
)"
[ -z "$REQ" ] && exit 0

# Detach the write so session close returns immediately. Fail-open on its own.
nohup curl -fsS --max-time "${VERGLAS_MCP_TIMEOUT:-15}" -X POST "$ENDPOINT" \
  -H "Authorization: Bearer $BEARER" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  --data "$REQ" >/dev/null 2>&1 &
exit 0
