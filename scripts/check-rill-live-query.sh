#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)

grep -q 'olap_connector: verglas' "$root/deploy/rill/rill.yaml"
grep -q 'driver: verglas' "$root/deploy/rill/connectors/verglas.yaml"
grep -q 'dockerfile: integrations/rill/Dockerfile' "$root/docker-compose.yml"

if rg -q 'VERGLAS_RILL_|/v1/dashboards|DashboardCommand' \
  "$root/docker-compose.yml" \
  "$root/crates/verglas-rest/src" \
  "$root/crates/verglas-core/src" \
  "$root/bins/verglas/src" \
  "$root/bins/verglas-server/src"; then
  echo "Verglas still owns Rill dashboard lifecycle or configuration" >&2
  exit 1
fi
