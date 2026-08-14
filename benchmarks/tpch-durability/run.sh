#!/bin/sh
# Run from any directory while keeping the checked-in topology authoritative.
set -eu
exec python3 "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/benchmark.py" "$@"
