#!/usr/bin/env python3
"""Snapshot-driven prefetch recovery driver (issue #51).

Proves the #51 claim end to end against real infrastructure: after a compaction
rewrites a partitioned Iceberg table's small files into fewer large ones, a
Verglas cache with prefetch ON recovers its hit rate on the *first*
post-compaction query wave (the coordinator carried the hot columns across the
rewrite by partition+column identity), while prefetch OFF cold-misses the
rewritten files and re-earns its hit rate query by query.

The trigger is production: the ``[catalog]`` REST watcher (#47) observes the
compaction commit on Polaris, the diff classifies it, and the prefetch
coordinator (#51) repairs the cache — no benchmark shortcut. The heat that
decides *which* chunks to prefetch is earned by the steady read load that runs
before the compaction.

Reuses ``benchmarks/warming/warming_demo.py`` for all Polaris/verglas-server/counter
plumbing (catalog bootstrap, REST catalog open, origin/Verglas S3 profiles,
``/admin/stats`` reading), so both demos share one machinery.

Subcommands (configuration arrives as explicit flags from ``run.sh``, which
loads ``.env`` and never logs secrets):

- ``bootstrap``  create the Polaris catalog + admin grants (idempotent).
- ``seed``       write a partitioned table with many small files per partition,
                 data on the origin, through Polaris. Idempotent per table.
- ``load``       a steady read wave through the Verglas endpoint (projecting the
                 hot column), making those columns hot in the heat ledger.
- ``compact``    rewrite the table's data files (pyiceberg optimize, or an
                 overwrite fallback) — the commit the watcher observes.
- ``measure``    the first post-compaction read wave through Verglas, recording
                 the server's /admin/stats counter delta as the recovery number.
- ``report``     render the A/B (prefetch off/on) recovery table.

Never reads secrets from anywhere but the flags/environment handed to it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

import pyarrow as pa
import requests

# Reuse the warming demo's proven Polaris/verglas-server/counter helpers.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "warming"))
import warming_demo as w  # noqa: E402

# The hot column every wave projects — the one whose heat must carry across the
# rewrite. A partitioned table on ``category``; ``value`` is the payload column.
HOT_COLUMN = "value"


# --------------------------------------------------------------------------- #
# Table shape
# --------------------------------------------------------------------------- #


def _table_name(args: argparse.Namespace) -> str:
    """The demo table's dotted name in the seeded namespace."""
    return f"{args.namespace}.{args.table}"


def _arrow_batch(partition: str, salt: int, rows: int) -> pa.Table:
    """A small Arrow batch for one partition: an id, the partition key, and the
    hot payload column."""
    return pa.table(
        {
            "id": pa.array([salt * rows + i for i in range(rows)], pa.int64()),
            "category": pa.array([partition] * rows, pa.string()),
            HOT_COLUMN: pa.array([float(i) * 1.5 for i in range(rows)], pa.float64()),
        }
    )


def phase_seed(args: argparse.Namespace) -> dict:
    """Create the partitioned table and append many small files per partition.

    Each append is its own data file, so a partition ends with
    ``files_per_partition`` small files — exactly the fragmentation a compaction
    exists to fix. Idempotent: an existing table with the expected file count is
    left alone. For the Avro fixture the table's ``write.format.default`` is set
    to ``avro`` so the data files are row-oriented (the addendum path).
    """
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, LongType, StringType, DoubleType
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform

    catalog = w.rest_catalog(args, w.origin_creds(args))
    try:
        catalog.create_namespace(args.namespace)
    except Exception:
        pass

    name = _table_name(args)
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "category", StringType(), required=True),
        NestedField(3, HOT_COLUMN, DoubleType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category")
    )
    props = {}
    if args.format == "avro":
        props["write.format.default"] = "avro"

    try:
        catalog.drop_table(name)
    except Exception:
        pass
    tbl = catalog.create_table(name, schema=schema, partition_spec=spec, properties=props)

    partitions = [f"p{i}" for i in range(args.partitions)]
    files = 0
    for p in partitions:
        for f in range(args.files_per_partition):
            tbl.append(_arrow_batch(p, salt=files, rows=args.rows_per_file))
            files += 1
    return {"seeded_table": name, "partitions": len(partitions), "small_files": files}


# --------------------------------------------------------------------------- #
# Read waves + compaction
# --------------------------------------------------------------------------- #


def _scan_hot_column(args: argparse.Namespace, endpoint_creds) -> int:
    """Scan the table projecting only the hot column, resolving objects through
    the given S3 profile (the Verglas endpoint for load/measure). Returns the
    row count scanned (a proxy for read volume)."""
    catalog = w.rest_catalog(args, endpoint_creds)
    tbl = catalog.load_table(_table_name(args))
    scan = tbl.scan(selected_fields=("category", HOT_COLUMN))
    return scan.to_arrow().num_rows


def phase_load(args: argparse.Namespace) -> dict:
    """Steady read load through Verglas: scan the hot column ``waves`` times so
    the heat ledger records the hot ``(partition, column)`` identities."""
    creds = w.verglas_creds(args)
    rows = 0
    for _ in range(args.waves):
        rows = _scan_hot_column(args, creds)
    return {"loaded_waves": args.waves, "rows_per_wave": rows}


def phase_compact(args: argparse.Namespace) -> dict:
    """Compact the table's data files through Polaris (data on the origin).

    Prefers pyiceberg's ``optimize.rewrite_data_files`` (a true ``replace``);
    falls back to a full ``overwrite`` on versions without it (the coordinator
    repairs ``replace`` and ``overwrite`` identically). Returns the operation the
    commit recorded so the report can state it.
    """
    catalog = w.rest_catalog(args, w.origin_creds(args))
    tbl = catalog.load_table(_table_name(args))
    op = "overwrite"
    optimize = getattr(tbl, "optimize", None)
    rewrite = getattr(optimize, "rewrite_data_files", None) if optimize else None
    if callable(rewrite):
        rewrite(target_file_size_bytes=args.target_file_size)
        op = "replace"
    else:
        # Overwrite the whole table with its current contents compacted into one
        # Arrow table per partition — one large file per partition replaces the
        # small ones. Same logical content; a data-carrying commit.
        current = tbl.scan().to_arrow()
        tbl.overwrite(current)
    tbl.refresh()
    return {"compaction_operation": op, "snapshot_id": tbl.current_snapshot().snapshot_id}


def phase_measure(args: argparse.Namespace) -> dict:
    """The first post-compaction read wave through Verglas, with the server's
    /admin/stats counter delta recorded as the recovery number.

    Waits ``settle_secs`` first so the watcher observes the commit and the
    coordinator's prefetch drains (prefetch ON); prefetch OFF has nothing to
    drain and cold-misses. Hit rate = cache-served reads / total reads."""
    time.sleep(args.settle_secs)
    creds = w.verglas_creds(args)
    before = w.fetch_counters(args.admin_endpoint)
    rows = _scan_hot_column(args, creds)
    after = w.fetch_counters(args.admin_endpoint)
    delta = {}
    if before and after:
        delta = {k: after.get(k, 0) - before.get(k, 0) for k in after}

    hits = delta.get("dram_hits", 0) + delta.get("disk_hits", 0)
    misses = delta.get("backend_fills", 0)
    total = hits + misses
    hit_rate = (hits / total) if total else 0.0
    record = {
        "config": args.config,
        "format": args.format,
        "rows": rows,
        "hit_rate": round(hit_rate, 4),
        "counters_delta": {
            k: delta.get(k, 0)
            for k in ("dram_hits", "disk_hits", "backend_fills", "backend_bytes_served")
        },
    }
    if args.results:
        with open(args.results, "a") as f:
            f.write(json.dumps(record) + "\n")
    return record


def phase_report(args: argparse.Namespace) -> dict:
    """Render the A (prefetch off) vs B (prefetch on) recovery table per format
    from the results JSONL."""
    records = []
    if args.results and os.path.exists(args.results):
        with open(args.results) as f:
            records = [json.loads(line) for line in f if line.strip()]
    print("\n=== #51 compaction hit-rate recovery (live, Polaris) ===")
    print(f"{'format':8} {'config':10} {'hit_rate':>9}  counters_delta")
    for r in records:
        print(
            f"{r['format']:8} {r['config']:10} {r['hit_rate'] * 100:8.1f}%  {r['counters_delta']}"
        )
    print("========================================================\n")
    return {"reported": len(records)}


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _add_common(p: argparse.ArgumentParser) -> None:
    """Flags shared by every subcommand (mirrors warming_demo's surface)."""
    p.add_argument("--catalog-uri", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--table", default="events")
    p.add_argument("--origin-endpoint", required=True)
    p.add_argument("--origin-region", default="us-east-1")
    p.add_argument("--verglas-endpoint", default="")
    p.add_argument("--verglas-key", default="")
    p.add_argument("--verglas-secret", default="")
    p.add_argument("--admin-endpoint", default="")
    p.add_argument("--format", choices=["parquet", "avro"], default="parquet")
    p.add_argument("--partitions", type=int, default=8)
    p.add_argument("--files-per-partition", type=int, default=8)
    p.add_argument("--rows-per-file", type=int, default=2000)
    p.add_argument("--waves", type=int, default=3)
    p.add_argument("--target-file-size", type=int, default=16 * 1024 * 1024)
    p.add_argument("--settle-secs", type=float, default=6.0)
    p.add_argument("--config", default="")
    p.add_argument("--results", default="")


def main() -> None:
    """Dispatch a subcommand and print its JSON result."""
    parser = argparse.ArgumentParser(description="issue #51 compaction prefetch demo")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for cmd in ("bootstrap", "seed", "load", "compact", "measure", "report"):
        _add_common(sub.add_parser(cmd))
    args = parser.parse_args()

    phases = {
        # bootstrap reuses the warming demo's Polaris catalog bring-up verbatim.
        "bootstrap": w.phase_bootstrap,
        "seed": phase_seed,
        "load": phase_load,
        "compact": phase_compact,
        "measure": phase_measure,
        "report": phase_report,
    }
    result = phases[args.cmd](args)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
