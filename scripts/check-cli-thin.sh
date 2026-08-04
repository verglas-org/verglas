#!/usr/bin/env bash
set -euo pipefail

reject_dependency() {
  local package="$1"
  local forbidden="$2"
  local tree
  tree="$(cargo tree -p "$package" --edges normal)"
  if grep -q "$forbidden" <<<"$tree"; then
    echo "$package dependency graph unexpectedly contains $forbidden" >&2
    exit 1
  fi
}

for package in verglas verglas-sdk; do
  for forbidden in verglas-iceberg datafusion iceberg-datafusion; do
    reject_dependency "$package" "$forbidden"
  done
done
reject_dependency verglas-iceberg verglas-sdk
for forbidden in verglas-sdk verglas-iceberg datafusion iceberg-datafusion; do
  reject_dependency verglas-api "$forbidden"
done

echo "Dependency boundaries: API leaf; SDK and CLI engine-free; Iceberg independent of SDK"
