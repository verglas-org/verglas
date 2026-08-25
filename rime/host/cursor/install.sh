#!/bin/bash
# Installs the Cursor RIME skill and worker agent.
# Does not install a second Verglas executable.
set -euo pipefail

src="$(cd "$(dirname "$0")/../.." && pwd)"
cursor_home="${CURSOR_HOME:-$HOME/.cursor}"
skill_dst="$cursor_home/skills/rime"
agent_dst="$cursor_home/agents"

mkdir -p "$agent_dst"
rm -rf "$skill_dst"
mkdir -p "$skill_dst"
cp -R "$src/skills/rime/." "$skill_dst/"
cp "$src/host/cursor/rime-worker.md" "$agent_dst/rime-worker.md"

echo "Installed Cursor RIME host into $cursor_home"
