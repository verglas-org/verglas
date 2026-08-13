#!/bin/sh
# Run from any directory while keeping the Docker build context stable.
set -eu
exec python3 "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/benchmark.py" "$@"
