#!/usr/bin/env bash
# Reject product and control-plane source that belongs in sibling repositories.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

forbidden_paths=(
  apps/os
  bins/access-node
  bins/queue-service
  bins/scheduler
  bins/verglas
  bins/verglas-pgcdc
  bins/verglas-server
  crates/verglas-application-runtime
  crates/verglas-authz
  crates/verglas-authz-openfga
  crates/verglas-authz-postgres
  crates/verglas-container-runtime
  crates/verglas-database
  crates/verglas-harness
  crates/verglas-integration-runtime
  crates/verglas-pgcdc
  crates/verglas-platform
  crates/verglas-queue
  crates/verglas-rest
  crates/verglas-scheduler
  crates/verglas-vessel-contract
  sdks/rust
  sdks/typescript
)

status=0
for path in "${forbidden_paths[@]}"; do
  if [[ -e "$repo_root/$path" ]]; then
    printf 'repository boundary violation: %s\n' "$path" >&2
    status=1
  fi
done

exit "$status"
