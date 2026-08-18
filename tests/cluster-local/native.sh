#!/usr/bin/env bash
# Runs the four-node ring as host processes against the Dockerized MinIO.
#
# Containers are the intended shape, but the workspace currently patches
# `iceberg` to an absolute host path (Cargo.toml), which a Docker build cannot
# resolve. Once that fork is pushed and repinned to a SHA, docker-compose.yml
# builds and this script becomes redundant.
#
#   ./native.sh up     build the node binary and start four nodes
#   ./native.sh down   stop them
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
ROOT=$(cd ../.. && pwd)
RUN=${VERGLAS_LOCAL_RUN_DIR:-/tmp/verglas-local}
PEERS="node1=127.0.0.1:28335,node2=127.0.0.1:28345,node3=127.0.0.1:28355,node4=127.0.0.1:28365"

case "${1:-}" in
  up)
    cargo build --release -p verglas-cache-node --manifest-path "$ROOT/Cargo.toml"
    BIN="$ROOT/target/release/verglas-cache-node"
    mkdir -p "$RUN"
    i=0
    for node in node1 node2 node3 node4; do
      i=$((i+1))
      s3=$((18333 + (i-1)*10)); admin=$((18334 + (i-1)*10)); ring=$((28335 + (i-1)*10))
      dir="$RUN/$node"; mkdir -p "$dir/cache"
      # Per-node config: same policy, distinct ports and cache directory.
      sed -e "s|/var/lib/verglas/cache|$dir/cache|" \
          -e "s|s3_port = 8333|s3_port = $s3|" \
          -e "s|admin_port = 8334|admin_port = $admin|" \
          -e "s|http://minio:9000|http://127.0.0.1:19000|" \
          -e "s|/etc/verglas/backend-credentials|$PWD/credentials/backend|" \
          -e "s|/etc/verglas/endpoint-credentials|$PWD/credentials/endpoint|" \
          node1.toml > "$dir/config.toml"
      VERGLAS_NODE_ID="$node" \
      VERGLAS_RING_PEERS="$PEERS" \
      VERGLAS_RING_ADDR="127.0.0.1:$ring" \
      VERGLAS_CLUSTER_SECRET=verglas-local-ring-secret \
      VERGLAS_LOG_FORMAT=json \
        nohup "$BIN" --config "$dir/config.toml" > "$dir/node.log" 2>&1 &
      echo "$!" > "$dir/pid"
      echo "started $node pid=$(cat "$dir/pid") s3=:$s3 admin=:$admin ring=:$ring"
    done
    ;;
  down)
    for node in node1 node2 node3 node4; do
      [ -f "$RUN/$node/pid" ] && kill "$(cat "$RUN/$node/pid")" 2>/dev/null || true
    done
    echo "stopped"
    ;;
  *) echo "usage: $0 {up|down}" >&2; exit 64 ;;
esac
