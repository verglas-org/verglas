#!/bin/sh
# Verglas one-line installer (macOS / Linux).
#
# Fetches prebuilt binaries for all four Verglas tools from the latest GitHub
# Release and installs them into ~/.cargo/bin (no Rust toolchain required). Each
# tool has its own cargo-dist installer that verifies a SHA-256 checksum; this
# wrapper just runs all four so `curl ... install.sh | sh` gets you a working
# install of the whole suite in one command.
#
# cargo-dist emits one installer per crate (the four binaries live in four
# workspace packages), so there is no single native "install everything"
# script — this wrapper is that entry point. To install one tool only, run its
# own installer, e.g.:
#   curl -fsSL https://github.com/cascade-labs/verglas/releases/latest/download/verglas-installer.sh | sh
set -eu

BASE="${VERGLAS_RELEASE_BASE:-https://github.com/cascade-labs/verglas/releases/latest/download}"
APPS="verglas verglasd verglas-mcp verglas-consolidate"

for app in $APPS; do
    echo "==> installing ${app}"
    curl -fsSL "${BASE}/${app}-installer.sh" | sh
done

echo
echo "Verglas installed (verglas, verglasd, verglas-mcp, verglas-consolidate)."
echo "Next: verglas init --bucket <bucket> --region <region>"
