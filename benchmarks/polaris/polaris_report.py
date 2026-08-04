#!/usr/bin/env python3
"""Report driver for the Polaris-over-Verglas catalog benchmark (issue #171).

Two responsibilities, one per subcommand:

* ``extract`` — parse one Gatling simulation report directory
  (``build/reports/gatling/<sim>-<ts>/js/stats.json``) into a flat
  ``{operation -> {p50, p95, p99, count, ok, ko}}`` map, appended under a
  simulation label into a per-leg results JSON. Gatling is the source of the
  latency percentiles; this script never invents a number.

* ``table`` — merge the ``direct`` and ``verglas`` per-leg JSONs into the
  comparison the issue asks for: per-operation p50/p95/p99 for
  ``direct`` vs ``verglas-cold`` vs ``verglas-warm``, plus the daemon's
  ``/admin/stats`` meta counters for the warm leg (the mechanism evidence).

Nothing here runs in CI. It is pure stdlib so ``run.sh`` needs no venv.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

# Gatling 3.14's HTML report no longer ships a machine-readable js/stats.json;
# the numbers live in the per-request pages (req_*.html) as an HTML stats table
# keyed by row label. We read those labels directly, so the parser is robust to
# whichever percentile indicators the suite's gatling.conf selects (run.sh sets
# 50/75/95/99). p50/p95/p99 are surfaced per the issue.
_NAME_RE = re.compile(r"<title>\s*Gatling Stats\s*-\s*(.*?)\s*</title>", re.I | re.S)
_ROW_RE = re.compile(
    r'<td class="title">(?P<label>[^<]*)</td>\s*'
    r'<td class="total">(?P<total>[^<]*)</td>\s*'
    r'<td class="ok">(?P<ok>[^<]*)</td>\s*'
    r'<td class="ko">(?P<ko>[^<]*)</td>',
    re.I | re.S,
)


def _num(cell: str) -> Any:
    """A stats cell -> int/float, or None for the '-' placeholder Gatling emits."""
    cell = cell.strip().replace(",", "")
    if cell in ("", "-"):
        return None
    try:
        return int(cell)
    except ValueError:
        try:
            return float(cell)
        except ValueError:
            return None


def _parse_req_html(path: Path) -> tuple[str, dict[str, Any]] | None:
    """Parse one req_*.html into (request name, {p50,p95,p99,count,ok,ko}).

    Percentiles come from the "Nth percentile" rows' *total* column; count/ok/ko
    come from the "Total count" row's three columns (the ok/ko split is the
    correctness evidence). Returns None if the page carries no recognisable name.
    """
    html = path.read_text(errors="replace")
    m = _NAME_RE.search(html)
    if not m:
        return None
    name = m.group(1).strip()
    rows: dict[str, tuple[Any, Any, Any]] = {}
    for r in _ROW_RE.finditer(html):
        rows[r.group("label").strip().lower()] = (
            _num(r.group("total")), _num(r.group("ok")), _num(r.group("ko"))
        )

    def pct(label: str) -> Any:
        cell = rows.get(label)
        return cell[0] if cell else None

    count_row = rows.get("total count", (None, None, None))
    return name, {
        "p50": pct("50th percentile"),
        "p95": pct("95th percentile"),
        "p99": pct("99th percentile"),
        "count": count_row[0],
        "ok": count_row[1],
        "ko": count_row[2],
    }


def _load_report_dir(report_dir: Path) -> dict[str, dict[str, Any]]:
    """Read every req_*.html in a Gatling report dir into an op->percentiles map."""
    out: dict[str, dict[str, Any]] = {}
    for req_html in sorted(report_dir.glob("req_*.html")):
        parsed = _parse_req_html(req_html)
        if parsed:
            name, stats = parsed
            out[name] = stats
    if not out:
        raise SystemExit(f"error: no parseable req_*.html under {report_dir}")
    return out


def cmd_extract(args: argparse.Namespace) -> None:
    """Append one simulation's per-operation percentiles into a per-leg JSON."""
    results_path = Path(args.results)
    doc: dict[str, Any]
    if results_path.is_file():
        doc = json.loads(results_path.read_text())
    else:
        doc = {"leg": args.leg, "context": {}, "simulations": {}}
    doc.setdefault("simulations", {})[args.label] = _load_report_dir(Path(args.report_dir))
    results_path.write_text(json.dumps(doc, indent=2))
    print(f"[report] extracted {args.label} -> {results_path}", file=sys.stderr)


def _fmt(v: Any) -> str:
    """Render a latency cell (ms) or a dash when the op is absent on a leg."""
    return "-" if v is None else f"{v}"


def _speedup(direct: Any, warm: Any) -> str:
    """direct/warm ratio, guarded against the missing-op and zero cases."""
    try:
        if direct and warm and warm > 0:
            return f"{direct / warm:.1f}x"
    except (TypeError, ZeroDivisionError):
        pass
    return "-"


# Operations highlighted in the headline table, in workload order. The rest are
# still printed in the full per-op dump below the headline.
HEADLINE_OPS = [
    ("Create Table", "write metadata.json (create)"),
    ("Update Table", "commit metadata.json (update)"),
    ("Fetch single Table", "loadTable — read metadata.json"),
    ("Fetch children Tables", "listTables"),
    ("Create Namespace", "createNamespace"),
    ("Fetch single namespace", "loadNamespace"),
    ("Create View", "createView"),
    ("Fetch single View", "loadView"),
    ("Update View", "commit view metadata (update)"),
]


def _op_across(doc: dict[str, Any], sims: list[str], op: str, metric: str) -> Any:
    """First non-null metric for ``op`` across the given simulation labels."""
    for sim in sims:
        cell = doc.get("simulations", {}).get(sim, {}).get(op)
        if cell and cell.get(metric) is not None:
            return cell[metric]
    return None


def cmd_table(args: argparse.Namespace) -> None:
    """Render the direct vs Verglas-cold vs Verglas-warm comparison table."""
    direct = json.loads(Path(args.direct).read_text()) if Path(args.direct).is_file() else {}
    verglas = json.loads(Path(args.verglas).read_text()) if Path(args.verglas).is_file() else {}

    # Which simulation label carries each column's numbers.
    d_write = ["CreateTreeDataset", "ReadUpdateTreeDataset"]
    d_read = ["ReadTreeDataset", "ReadUpdateTreeDataset"]
    v_cold = ["ReadTreeDataset.cold"]
    v_warm = ["ReadTreeDataset.warm", "ReadUpdateTreeDataset"]
    v_write = ["CreateTreeDataset", "ReadUpdateTreeDataset"]

    lines: list[str] = []
    lines.append("# Polaris-over-Verglas — catalog IO benchmark (issue #171)")
    lines.append("")
    ctx_d = direct.get("context", {})
    ctx_v = verglas.get("context", {})
    ctx = ctx_v or ctx_d
    for key in ("machine", "origin", "bucket_prefix_direct", "bucket_prefix_verglas",
                "verglas_cache_budget", "verglas_endpoint", "polaris_image",
                "polaris_tools_commit", "polaris_cache_config", "dataset",
                "sampling", "run_label"):
        if ctx.get(key):
            lines.append(f"- **{key}**: {ctx[key]}")
    lines.append("")

    # Headline latency table (p50/p95/p99 ms), reads getting the cold/warm split.
    hdr = ("| operation | metric | direct | verglas-cold | verglas-warm | direct/warm |")
    sep = ("|---|---|---|---|---|---|")
    lines.append("## Latency percentiles (ms)")
    lines.append("")
    lines.append(hdr)
    lines.append(sep)
    is_read = {"Fetch single Table", "Fetch children Tables", "Fetch single namespace",
               "Fetch single View", "Fetch child namespaces", "Check Table Exists"}
    for op, desc in HEADLINE_OPS:
        for metric in ("p50", "p95", "p99"):
            if op in is_read or "Fetch" in op:
                dv = _op_across(direct, d_read, op, metric)
                cold = _op_across(verglas, v_cold, op, metric)
                warm = _op_across(verglas, v_warm, op, metric)
            else:
                dv = _op_across(direct, d_write, op, metric)
                cold = None
                warm = _op_across(verglas, v_write, op, metric)
            if dv is None and cold is None and warm is None:
                continue
            label = f"{op} ({desc})" if metric == "p50" else ""
            spd = _speedup(dv, warm) if metric == "p95" else ""
            lines.append(
                f"| {label} | {metric} | {_fmt(dv)} | {_fmt(cold)} | {_fmt(warm)} | {spd} |"
            )
    lines.append("")

    # Meta-counter evidence for the warm leg: the mechanism proof (#50 pinning).
    admin = verglas.get("admin_stats_warm", {})
    if admin:
        c = admin.get("counters", {})
        cache = admin.get("cache", {})
        mh, mm = c.get("meta_hits", 0), c.get("meta_misses", 0)
        total = mh + mm
        rate = f"{100 * mh / total:.1f}%" if total else "n/a"
        lines.append("## Verglas /admin/stats — warm leg (mechanism evidence)")
        lines.append("")
        lines.append(f"- **meta hit rate**: {rate}  (meta_hits={mh}, meta_misses={mm})")
        lines.append(f"- **meta_bytes_served**: {c.get('meta_bytes_served')}")
        lines.append(f"- **backend_heads**: {c.get('backend_heads')}  "
                     f"**backend_fills**: {c.get('backend_fills')}")
        lines.append(f"- **cache budget**: dram={cache.get('dram_bytes')}B "
                     f"disk={cache.get('capacity_bytes')}B  "
                     f"dram_usage={admin.get('dram_usage_bytes')}B")
        lines.append("")

    # Correctness gate: every leg/sim must have zero failed (ko) requests.
    lines.append("## Correctness (Gatling assertions)")
    lines.append("")
    any_ko = False
    for legname, doc in (("direct", direct), ("verglas", verglas)):
        for sim, ops in doc.get("simulations", {}).items():
            ko = sum((o.get("ko") or 0) for o in ops.values())
            n = sum((o.get("count") or 0) for o in ops.values())
            flag = "OK" if ko == 0 else f"FAILURES={ko}"
            any_ko = any_ko or ko != 0
            lines.append(f"- {legname} / {sim}: {n} requests, {flag}")
    lines.append("")
    lines.append("**Result-correctness deviations: "
                 + ("NONE" if not any_ko else "PRESENT — investigate") + "**")
    lines.append("")

    out = "\n".join(lines)
    if args.out:
        Path(args.out).write_text(out)
    print(out)


def main() -> None:
    """CLI entrypoint: dispatch to ``extract`` or ``table``."""
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("extract", help="parse a Gatling report dir into a per-leg JSON")
    e.add_argument("--leg", required=True)
    e.add_argument("--label", required=True, help="simulation label, e.g. ReadTreeDataset.cold")
    e.add_argument("--report-dir", required=True)
    e.add_argument("--results", required=True)
    e.set_defaults(func=cmd_extract)

    t = sub.add_parser("table", help="merge per-leg JSONs into the comparison table")
    t.add_argument("--direct", required=True)
    t.add_argument("--verglas", required=True)
    t.add_argument("--out", default="")
    t.set_defaults(func=cmd_table)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
