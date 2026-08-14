#!/usr/bin/env bash
# Reject hosted product and control-plane source that belongs in sibling repositories.

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
  crates/verglas-catalog-service
  crates/verglas-database
  crates/verglas-harness
  crates/verglas-integration-runtime
  crates/verglas-pgcdc
  crates/verglas-platform
  crates/verglas-queue
  crates/verglas-rest
  crates/verglas-scheduler
  crates/verglas-vessel-contract
)

status=0
for path in "${forbidden_paths[@]}"; do
  if [[ -e "$repo_root/$path" ]]; then
    printf 'repository boundary violation: %s\n' "$path" >&2
    status=1
  fi
done

if rg -q 'lakekeeper-(bin|storage-verglas)|name = "lakekeeper"' \
  "$repo_root/Cargo.toml" \
  "$repo_root/bins"/*/Cargo.toml \
  "$repo_root/crates"/*/Cargo.toml; then
  printf 'repository boundary violation: Lakekeeper service code remains in the OSS engine\n' >&2
  status=1
fi

exit "$status"
