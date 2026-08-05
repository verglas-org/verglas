#!/usr/bin/env bash
# Cursor Cloud environment install step.
#
# Runs once when the environment image/snapshot is built. Installs the tooling
# the dev workflow needs, primes dependency caches, and pre-builds the server and
# CLI so the start step boots fast. Idempotent: every install is guarded so a
# rebuild or a re-run is a no-op when the tool is already present.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"

echo "== verglas cloud install: system tooling =="

# unzip — required to unpack the AWS CLI and Bun release zips below.
# jq — used by scripts/changed-cargo-packages.sh and release tooling.
need_apt=0
command -v unzip >/dev/null 2>&1 || need_apt=1
command -v jq >/dev/null 2>&1 || need_apt=1
if [ "$need_apt" -eq 1 ]; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq unzip jq
fi

# just — the documented task runner (just build/test/lint).
if ! command -v just >/dev/null 2>&1; then
  curl -fsSL https://github.com/casey/just/releases/download/1.36.0/just-1.36.0-x86_64-unknown-linux-musl.tar.gz \
    -o /tmp/just.tar.gz
  sudo tar -xzf /tmp/just.tar.gz -C /usr/local/bin just
fi

# MinIO server + client — the local S3 origin the start step serves through.
if ! command -v minio >/dev/null 2>&1; then
  sudo curl -fsSL https://dl.min.io/server/minio/release/linux-amd64/minio -o /usr/local/bin/minio
  sudo chmod +x /usr/local/bin/minio
fi
if ! command -v mc >/dev/null 2>&1; then
  sudo curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc
  sudo chmod +x /usr/local/bin/mc
fi

# AWS CLI v2 — drives PUT/GET through the cache in the documented examples.
if ! command -v aws >/dev/null 2>&1; then
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
  ( cd /tmp && unzip -oq awscliv2.zip && sudo ./aws/install >/dev/null )
fi

# Bun 1.3.8 — pinned runtime for the SDK parity / trajectory checks (matches CI).
if ! command -v bun >/dev/null 2>&1; then
  curl -fsSL https://github.com/oven-sh/bun/releases/download/bun-v1.3.8/bun-linux-x64.zip -o /tmp/bun.zip
  ( cd /tmp && unzip -oq bun.zip && sudo mv bun-linux-x64/bun /usr/local/bin/bun && sudo chmod +x /usr/local/bin/bun )
fi

echo "== verglas cloud install: rust deps + build =="
# rustup installs the pinned toolchain (rust-toolchain.toml) on first use.
cargo fetch --locked
# Pre-build the two binaries the start step runs, so boot is fast.
cargo build -p verglas-server -p verglas

echo "== verglas cloud install: typescript sdk =="
if [ -f sdks/typescript/package-lock.json ]; then
  npm --prefix sdks/typescript ci
fi

echo "== verglas cloud install: done =="
