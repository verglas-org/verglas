#!/bin/bash
# Installs the Cursor RIME host: skill, worker agent, and two-loop hooks.
# Does not install a second Verglas executable.
set -euo pipefail

src="$(cd "$(dirname "$0")/../.." && pwd)"
cursor_home="${CURSOR_HOME:-$HOME/.cursor}"
skill_dst="$cursor_home/skills/rime"
agent_dst="$cursor_home/agents"
hook_dst="$cursor_home/hooks"
hooks_json="$cursor_home/hooks.json"

mkdir -p "$skill_dst" "$agent_dst" "$hook_dst"
rm -rf "$skill_dst"
mkdir -p "$skill_dst"
cp -R "$src/skills/rime/." "$skill_dst/"
cp "$src/host/cursor/rime-worker.md" "$agent_dst/rime-worker.md"
cp "$src/host/cursor/hooks/"*.sh "$hook_dst/"
chmod +x "$hook_dst"/rime-*.sh

python3 - "$hooks_json" "$src/host/cursor/hooks.json" <<'PY'
import json, sys
from pathlib import Path

target = Path(sys.argv[1])
fragment = json.loads(Path(sys.argv[2]).read_text())
current = {"version": 1, "hooks": {}}
if target.exists():
    current = json.loads(target.read_text())
    if not isinstance(current, dict):
        current = {"version": 1, "hooks": {}}
current.setdefault("version", 1)
hooks = current.setdefault("hooks", {})
for event, entries in fragment.get("hooks", {}).items():
    existing = hooks.setdefault(event, [])
    for entry in entries:
        command = entry.get("command", "")
        name = command.rsplit("/", 1)[-1]
        if any(name in item.get("command", "") for item in existing):
            continue
        existing.append(entry)
target.write_text(json.dumps(current, indent=2) + "\n")
PY

echo "Installed Cursor RIME host into $cursor_home"
