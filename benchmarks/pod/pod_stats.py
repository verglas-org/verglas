#!/usr/bin/env python3
"""Per-node /admin/stats snapshot + evidence renderer for the pod profile.

The pod profile (issue #160, peer fetch #29) runs the constrained TPC-H legs
through node 0 of an N-node `verglas dev` pod. The proof that the pod behaves as
one cache — aggregate capacity N x per-node, cross-owner reads served by peers
rather than the backend — lives in each node's `/admin/stats` counters, not in
the latency table. This helper reads every node's admin surface and renders the
cross-node evidence.

Two modes, stdlib-only (no venv needed):

  snapshot --out FILE --admin URL [--admin URL ...]
      Fetch every node's /admin/stats now and write {admin_url: body} to FILE.

  report --before B.json --after A.json [--labels node-0,node-1,...]
      Diff the two snapshots per node and print the per-node counter deltas plus
      the pod aggregate, calling out peer traffic (peer_hits on the reader,
      peer_served_blocks on the owners) and aggregate backend_fills.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
import urllib.error

# Counters that carry the pod story, in display order.
COUNTERS = [
    "dram_hits",
    "disk_hits",
    "peer_hits",
    "peer_misses",
    "peer_errors",
    "peer_served_blocks",
    "peer_served_bytes",
    "backend_fills",
    "backend_fill_bytes",
    "meta_hits",
    "meta_misses",
]


def fetch(admin_url: str) -> dict | None:
    """Read one node's /admin/stats; None (never fabricated) if unreachable."""
    url = admin_url.rstrip("/") + "/admin/stats"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            if resp.status != 200:
                return None
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError) as e:
        print(f"[pod_stats] could not read {url}: {e}", file=sys.stderr)
        return None


def snapshot(admins: list[str], out: str) -> int:
    """Fetch every node's stats and write {admin_url: body} to ``out``."""
    body = {a: fetch(a) for a in admins}
    with open(out, "w") as f:
        json.dump(body, f, indent=2)
    missing = [a for a, v in body.items() if v is None]
    if missing:
        print(f"[pod_stats] WARNING unreachable admin(s): {missing}", file=sys.stderr)
    return 0


def _counters(body: dict | None) -> dict:
    """Pull the counters sub-object out of a stats body (empty if missing)."""
    return (body or {}).get("counters", {}) or {}


def report(before_path: str, after_path: str, labels: list[str]) -> int:
    """Diff before/after per node and print per-node deltas + the pod aggregate."""
    with open(before_path) as f:
        before = json.load(f)
    with open(after_path) as f:
        after = json.load(f)

    admins = list(after.keys())
    if labels and len(labels) == len(admins):
        names = dict(zip(admins, labels))
    else:
        names = {a: f"node-{i}" for i, a in enumerate(admins)}

    # Per-node deltas.
    per_node: dict[str, dict] = {}
    for a in admins:
        b, af = _counters(before.get(a)), _counters(after.get(a))
        per_node[a] = {k: af.get(k, 0) - b.get(k, 0) for k in COUNTERS}

    agg = {k: sum(per_node[a][k] for a in admins) for k in COUNTERS}

    lines = []
    lines.append("Pod per-node /admin/stats counter deltas (after - before)")
    lines.append("")
    header = f"  {'counter':>20}  " + "  ".join(f"{names[a]:>14}" for a in admins) + f"  {'POD TOTAL':>14}"
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for k in COUNTERS:
        row = f"  {k:>20}  " + "  ".join(f"{per_node[a][k]:>14,}" for a in admins)
        row += f"  {agg[k]:>14,}"
        lines.append(row)
    lines.append("")
    # Plain-English restatement of the cross-node counters. The caller (run.sh)
    # supplies each part's interpretation; these lines only say what the
    # numbers say, so they stay honest for both Part 1 and Part 2.
    reader = names[admins[0]]
    lines.append("  Cross-node summary:")
    lines.append(
        f"    - {reader}: peer_hits = {per_node[admins[0]]['peer_hits']:,} "
        f"(cross-owner reads a peer served), peer_misses = "
        f"{per_node[admins[0]]['peer_misses']:,} (cross-owner reads the ring "
        f"routed off-node but the owner lacked)."
    )
    donor_served = sum(per_node[a]["peer_served_blocks"] for a in admins[1:])
    lines.append(
        f"    - other nodes served {donor_served:,} blocks TO peers "
        f"(peer_served_blocks) — the donor side of the same fetches."
    )
    lines.append(
        f"    - pod aggregate backend_fills = {agg['backend_fills']:,} across "
        f"{len(admins)} nodes' independent caches."
    )
    out = "\n".join(lines)
    print(out)

    # Machine-readable sidecar for the PR/issue write-up.
    print("\n---JSON---")
    print(json.dumps({"per_node": {names[a]: per_node[a] for a in admins}, "pod_total": agg}, indent=2))
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="pod per-node /admin/stats evidence")
    sub = p.add_subparsers(dest="mode", required=True)
    s = sub.add_parser("snapshot")
    s.add_argument("--out", required=True)
    s.add_argument("--admin", action="append", required=True, dest="admins")
    r = sub.add_parser("report")
    r.add_argument("--before", required=True)
    r.add_argument("--after", required=True)
    r.add_argument("--labels", default="")
    args = p.parse_args()
    if args.mode == "snapshot":
        return snapshot(args.admins, args.out)
    labels = [x for x in args.labels.split(",") if x] if args.labels else []
    return report(args.before, args.after, labels)


if __name__ == "__main__":
    sys.exit(main())
