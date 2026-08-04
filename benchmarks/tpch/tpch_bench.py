#!/usr/bin/env python3
"""TPC-H-over-Iceberg endpoint-vs-direct benchmark driver (issue #136).

Phases
------
- ``generate``  DuckDB's ``tpch`` extension produces the 8 TPC-H tables at a
                scale factor; each is written as an Iceberg table (Parquet data
                files) via pyiceberg using a *local* SQLite catalog whose
                warehouse is ``s3://<bucket>/<prefix>``. Every write goes
                THROUGH THE VERGLAS ENDPOINT (one-endpoint principle).
- ``query``     the 22 canonical TPC-H queries (their text comes from the
                extension's own ``tpch_queries()`` so they are authoritative)
                are run three ways: direct-to-origin, Verglas cold, Verglas
                warm. Each leg fetches the identical bytes over the identical
                path, so the comparison is apples-to-apples.
- ``teardown``  deletes every object under the (non-empty, guarded) prefix and
                removes the SQLite catalog file.

Read modes
----------
``--read-mode duckdb-iceberg`` reads each table through DuckDB's ``iceberg``
extension pointed at the pyiceberg-written ``metadata.json`` (full SQL pushdown
into the Parquet scan). ``--read-mode pyiceberg-arrow`` loads each table with
pyiceberg -> Arrow and registers it in DuckDB (measures scan+fetch, not
pushdown). ``auto`` (default) tries duckdb-iceberg and falls back to
pyiceberg-arrow if the iceberg extension cannot read the metadata. Both modes
route every byte through the leg's endpoint, so either is endpoint-vs-direct
comparable; the report records which mode ran.

All configuration arrives as explicit flags from ``run.sh`` (which resolves
defaults and loads the ``.env``). This module never reads customer secrets from
anywhere but the process environment / flags handed to it, and never logs them.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import statistics
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field, asdict
from typing import Optional
from urllib.parse import urlparse

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

# The 8 TPC-H tables, in dependency-free write order.
TABLES = [
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]

# Number of canonical TPC-H queries.
NUM_QUERIES = 22


@dataclass
class S3Target:
    """One S3 access profile: an endpoint plus the credentials to reach it.

    Used both for the Verglas endpoint (dev keys) and the direct origin
    (ambient AWS_* credentials). ``host_port`` and ``use_ssl`` are derived for
    DuckDB's httpfs, which wants the bare host:port and an explicit SSL flag.
    """

    endpoint: str
    access_key_id: str
    secret_access_key: str
    region: str

    @property
    def host_port(self) -> str:
        """Endpoint stripped of scheme, the form DuckDB's ``s3_endpoint`` wants."""
        p = urlparse(self.endpoint)
        return p.netloc or p.path

    @property
    def use_ssl(self) -> bool:
        """True when the endpoint speaks https (origin), false for local http (Verglas)."""
        return urlparse(self.endpoint).scheme == "https"

    def pyiceberg_props(self) -> dict:
        """pyiceberg FileIO properties selecting this endpoint with path-style access.

        OCI (and most S3-compatible origins) require path-style addressing; the
        local Verglas endpoint does too. pyiceberg's PyArrow FileIO maps these
        ``s3.*`` keys onto a ``pyarrow.fs.S3FileSystem``.
        """
        return {
            "s3.endpoint": self.endpoint,
            "s3.access-key-id": self.access_key_id,
            "s3.secret-access-key": self.secret_access_key,
            "s3.region": self.region,
            "s3.path-style-access": "true",
        }


@dataclass
class Config:
    """Everything a phase needs, resolved from flags by ``run.sh``."""

    scale: float
    bucket: str
    prefix: str
    namespace: str
    catalog_db: str
    verglas: S3Target
    origin: S3Target
    read_mode: str
    cache_note: str
    profile: str
    admin_endpoint: str
    as_json: bool

    def warehouse(self) -> str:
        """The catalog warehouse root — always ``s3://<bucket>/<prefix>``."""
        return f"s3://{self.bucket}/{self.prefix}"


def fetch_stats(admin_endpoint: str) -> Optional[dict]:
    """Read the daemon's ``/admin/stats`` (cache budgets + read-path counters).

    This is the single source of truth for a run's tier context: the report
    stamps the daemon's actual ``dram_bytes``/``capacity_bytes`` from here rather
    than guessing, and the nvme-resident/constrained profiles read the counters
    to prove where warm reads were served from and whether eviction happened.
    Returns ``None`` (never a fabricated value) if the admin surface is
    unreachable, so a missing endpoint degrades to "context unavailable".
    """
    if not admin_endpoint:
        return None
    url = admin_endpoint.rstrip("/") + "/admin/stats"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            if resp.status != 200:
                return None
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError) as e:
        print(f"[stats] could not read {url}: {e}", flush=True)
        return None


def counter_delta(before: Optional[dict], after: Optional[dict]) -> dict:
    """Per-counter difference ``after - before`` over the counters snapshots.

    Both arguments are full ``/admin/stats`` bodies (or ``None``); an empty dict
    is returned when either is missing so the report renders "unavailable".
    """
    if not before or not after:
        return {}
    b, a = before.get("counters", {}), after.get("counters", {})
    return {k: a.get(k, 0) - b.get(k, 0) for k in a}


def require_prefix(prefix: str) -> str:
    """Reject an empty (or slashes-only) prefix.

    Seeding at the bucket root would scatter tables across the customer bucket,
    and a prefix-scoped teardown of an empty prefix would delete the ENTIRE
    bucket. This guard mirrors ``verglas bench``'s discipline and is
    load-bearing for the teardown path.
    """
    trimmed = prefix.strip().strip("/")
    if not trimmed:
        raise SystemExit(
            "prefix must be a non-empty path within the bucket (never the bucket root)"
        )
    return trimmed


def make_catalog(cfg: Config, target: S3Target) -> SqlCatalog:
    """Open the SQLite catalog with a FileIO pointed at ``target``.

    The same catalog file is opened with different endpoints per leg: the
    catalog only records logical metadata *locations*; which network path
    resolves them is the FileIO's endpoint. That is exactly the integration point the
    endpoint-vs-direct comparison turns on. PyArrowFileIO serves reads and
    writes alike: the write path's existence precheck issues ListObjectsV2,
    which the endpoint serves since #8.
    """
    props = {
        "uri": f"sqlite:///{cfg.catalog_db}",
        "warehouse": cfg.warehouse(),
    }
    props.update(target.pyiceberg_props())
    props["py-io-impl"] = "pyiceberg.io.pyarrow.PyArrowFileIO"
    return SqlCatalog("bench", **props)


# --------------------------------------------------------------------------- #
# generate                                                                     #
# --------------------------------------------------------------------------- #


def phase_generate(cfg: Config) -> dict:
    """Generate TPC-H at the scale factor and write each table as Iceberg.

    dbgen runs locally in DuckDB; the resulting Arrow tables are appended into
    Iceberg tables whose data files land in ``s3://<bucket>/<prefix>`` through
    the Verglas endpoint. Idempotent: an existing table at the same identifier
    is dropped and rewritten.
    """
    require_prefix(cfg.prefix)
    print(f"[generate] dbgen at SF{cfg.scale} …", flush=True)
    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")
    t0 = time.monotonic()
    con.execute(f"CALL dbgen(sf={cfg.scale})")
    gen_s = time.monotonic() - t0
    print(f"[generate] dbgen done in {gen_s:.1f}s", flush=True)

    catalog = make_catalog(cfg, cfg.verglas)
    catalog.create_namespace_if_not_exists(cfg.namespace)

    written = {}
    for t in TABLES:
        ident = f"{cfg.namespace}.{t}"
        arrow = con.execute(f"SELECT * FROM {t}").fetch_arrow_table()
        if catalog.table_exists(ident):
            catalog.drop_table(ident)
        tbl = catalog.create_table(ident, schema=arrow.schema)
        w0 = time.monotonic()
        tbl.append(arrow)
        write_s = time.monotonic() - w0
        rows = arrow.num_rows
        written[t] = {"rows": rows, "write_s": round(write_s, 3)}
        print(
            f"[generate] wrote {ident}: {rows:,} rows in {write_s:.1f}s "
            f"(through the Verglas endpoint)",
            flush=True,
        )

    return {
        "scale": cfg.scale,
        "dbgen_seconds": round(gen_s, 3),
        "tables": written,
        "warehouse": cfg.warehouse(),
    }


# --------------------------------------------------------------------------- #
# query                                                                        #
# --------------------------------------------------------------------------- #


def canonical_queries() -> list[str]:
    """The 22 official TPC-H query texts, verbatim from DuckDB's extension.

    Using the extension's own text keeps the queries canonical rather than a
    hand-transcribed variant.
    """
    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")
    rows = con.execute(
        "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr"
    ).fetchall()
    con.close()
    return [q for (_nr, q) in rows]


def duckdb_conn_for(target: S3Target) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with httpfs/iceberg/tpch loaded and S3 pointed at ``target``.

    tpch is loaded only to supply the canonical query texts; no dbgen runs on
    this connection. The global S3 settings are per-connection, so each leg's
    connection reaches exactly one endpoint.
    """
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"SET s3_endpoint='{target.host_port}'")
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_use_ssl={'true' if target.use_ssl else 'false'}")
    con.execute(f"SET s3_region='{target.region}'")
    con.execute(f"SET s3_access_key_id='{target.access_key_id}'")
    con.execute(f"SET s3_secret_access_key='{target.secret_access_key}'")
    return con


def metadata_locations(cfg: Config) -> dict:
    """Return ``{table: metadata.json location}`` read once via the origin.

    The metadata *location* is endpoint-independent (it is the same ``s3://``
    path for every leg); only the network path that resolves it differs. Reading
    it through the origin (not Verglas) keeps the Verglas cache untouched until
    the cold leg, so the cold measurement stays honest. Existing-object reads
    never trigger a List, so this works against any origin.
    """
    catalog = make_catalog(cfg, cfg.origin)
    return {
        t: catalog.load_table(f"{cfg.namespace}.{t}").metadata_location for t in TABLES
    }


def setup_views_duckdb_iceberg(
    con: duckdb.DuckDBPyConnection, meta: dict
) -> None:
    """Register each TPC-H table as a DuckDB view over its Iceberg metadata.

    Points ``iceberg_scan`` straight at the pyiceberg-written ``metadata.json``
    — sidestepping any reliance on a ``version-hint.text`` file that pyiceberg
    does not emit. DuckDB plans the scan and pushes predicates/projections into
    the Parquet reads, fetched over this connection's endpoint. Raises if the
    iceberg extension cannot read the metadata, which the caller treats as the
    trigger to fall back to pyiceberg-arrow.
    """
    for t in TABLES:
        con.execute(
            f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM iceberg_scan('{meta[t]}')"
        )
    # Force the extension to actually touch the metadata now so an
    # incompatibility surfaces here (as a fallback trigger) rather than mid-run.
    con.execute("SELECT count(*) FROM region").fetchone()


# Arrow tables registered per DuckDB connection (see setup_views_pyiceberg_arrow).
_ARROW_REGISTRY: dict = {}


def setup_views_pyiceberg_arrow(
    con: duckdb.DuckDBPyConnection, cfg: Config, target: S3Target
) -> None:
    """Load each table with pyiceberg -> Arrow and register it in DuckDB.

    Fetches every data file through ``target``'s endpoint into an Arrow table,
    then registers it as a DuckDB relation. This measures scan+fetch over the
    endpoint plus in-memory SQL rather than pushed-down scans — still identical
    across legs, so still endpoint-vs-direct comparable. Uses the PyArrow FileIO
    (path-style), whose existing-object reads never trigger a List.
    """
    catalog = make_catalog(cfg, target)
    # Keep references alive for the connection's lifetime. DuckDB connection
    # objects reject attribute assignment, so the tables live in a module
    # registry keyed by connection id (registrations die with the process; a
    # bench run creates a handful of connections, so growth is bounded).
    tables = _ARROW_REGISTRY.setdefault(id(con), {})
    for t in TABLES:
        tbl = catalog.load_table(f"{cfg.namespace}.{t}")
        arrow = tbl.scan().to_arrow()
        tables[t] = arrow
        con.register(t, arrow)


def run_leg(
    cfg: Config,
    target: S3Target,
    queries: list[str],
    read_mode: str,
    meta: dict,
) -> tuple[str, list[dict]]:
    """Set up the tables for one leg then time all 22 queries.

    Returns the read mode actually used and a per-query list of
    ``{nr, ms, rows}``. In ``auto`` mode a duckdb-iceberg setup failure falls
    back to pyiceberg-arrow for the whole leg.
    """
    con = duckdb_conn_for(target)

    used = read_mode
    if read_mode in ("auto", "duckdb-iceberg"):
        try:
            setup_views_duckdb_iceberg(con, meta)
            used = "duckdb-iceberg"
        except Exception as e:  # noqa: BLE001 - fallback is the whole point
            if read_mode == "duckdb-iceberg":
                raise
            print(
                f"[query] duckdb-iceberg read failed ({e.__class__.__name__}: {e}); "
                f"falling back to pyiceberg-arrow",
                flush=True,
            )
            con.close()
            con = duckdb_conn_for(target)
            setup_views_pyiceberg_arrow(con, cfg, target)
            used = "pyiceberg-arrow"
    else:
        setup_views_pyiceberg_arrow(con, cfg, target)
        used = "pyiceberg-arrow"

    results = []
    for i, q in enumerate(queries, start=1):
        t0 = time.monotonic()
        rows = con.execute(q).fetchall()
        ms = (time.monotonic() - t0) * 1000.0
        results.append({"nr": i, "ms": round(ms, 2), "rows": len(rows)})
    con.close()
    return used, results


def phase_query(cfg: Config) -> dict:
    """Run the three legs (direct, Verglas cold, Verglas warm) and assemble a report.

    Order matters: the direct leg first (never touches Verglas), then the cold
    leg (first Verglas touch = genuine cold with an empty daemon cache), then
    the warm leg (immediate re-run, cache populated). This mirrors the
    cold-before-warm discipline of ``verglas bench``.
    """
    require_prefix(cfg.prefix)
    queries = canonical_queries()
    assert len(queries) == NUM_QUERIES, f"expected {NUM_QUERIES} queries, got {len(queries)}"

    # Resolve every table's metadata location once, via the origin, so the
    # Verglas cache is not warmed before the cold leg.
    meta = metadata_locations(cfg)

    print("[query] direct-to-origin leg …", flush=True)
    direct_mode, direct = run_leg(cfg, cfg.origin, queries, cfg.read_mode, meta)
    print("[query] Verglas cold leg …", flush=True)
    cold_mode, cold = run_leg(cfg, cfg.verglas, queries, cfg.read_mode, meta)
    # Snapshot the daemon counters between the cold and warm legs so the warm
    # leg's own deltas isolate where its reads came from (DRAM vs disk) and
    # whether eviction forced backend refills — the per-profile evidence.
    stats_pre_warm = fetch_stats(cfg.admin_endpoint)
    print("[query] Verglas warm leg …", flush=True)
    warm_mode, warm = run_leg(cfg, cfg.verglas, queries, cfg.read_mode, meta)
    stats_post_warm = fetch_stats(cfg.admin_endpoint)

    per_query = []
    for d, c, w in zip(direct, cold, warm):
        speedup = (d["ms"] / w["ms"]) if w["ms"] > 0 else 0.0
        per_query.append(
            {
                "nr": d["nr"],
                "rows": d["rows"],
                "direct_ms": d["ms"],
                "cold_ms": c["ms"],
                "warm_ms": w["ms"],
                "speedup_direct_over_warm": round(speedup, 2),
            }
        )

    totals = {
        "direct_ms": round(sum(q["direct_ms"] for q in per_query), 2),
        "cold_ms": round(sum(q["cold_ms"] for q in per_query), 2),
        "warm_ms": round(sum(q["warm_ms"] for q in per_query), 2),
    }
    totals["speedup_direct_over_warm"] = (
        round(totals["direct_ms"] / totals["warm_ms"], 2) if totals["warm_ms"] else 0.0
    )

    return {
        "scale": cfg.scale,
        "read_mode": {"direct": direct_mode, "cold": cold_mode, "warm": warm_mode},
        "per_query": per_query,
        "totals": totals,
        "machine": machine_context(cfg),
        "cache_stats": cache_stats_report(stats_pre_warm, stats_post_warm),
    }


def cache_stats_report(pre_warm: Optional[dict], post_warm: Optional[dict]) -> dict:
    """Assemble the tier-context evidence from the counter snapshots.

    Records the daemon's configured budgets, the DRAM-tier occupancy after the
    warm leg, and the warm-leg-only counter deltas. The warm-leg deltas are what
    prove the profile's claim: ``disk_hits > 0`` means warm reads were served
    from NVMe (nvme-resident), and ``backend_fills > 0`` on the warm leg means
    eviction forced refills (constrained). ``available`` is False when the admin
    surface could not be read, so the report says so rather than inventing.
    """
    if not post_warm:
        return {"available": False}
    cache = post_warm.get("cache", {})
    return {
        "available": True,
        "cache_config": {
            "dram_bytes": cache.get("dram_bytes"),
            "capacity_bytes": cache.get("capacity_bytes"),
        },
        "dram_usage_bytes_after_warm": post_warm.get("dram_usage_bytes"),
        "warm_leg_counter_delta": counter_delta(pre_warm, post_warm),
    }


def machine_context(cfg: Config) -> dict:
    """Machine + cache-medium context recorded alongside the numbers.

    The cache medium is a property of the daemon's ``--cache-dir``, which this
    process does not own; ``run.sh`` passes a note describing it (default: the
    daemon's ephemeral temp dir).
    """
    return {
        "platform": platform.platform(),
        "processor": platform.processor() or platform.machine(),
        "python": platform.python_version(),
        "duckdb": duckdb.__version__,
        "cpu_count": os.cpu_count(),
        "cache_medium_note": cfg.cache_note,
        "profile": cfg.profile,
    }


# --------------------------------------------------------------------------- #
# teardown                                                                     #
# --------------------------------------------------------------------------- #


def phase_teardown(cfg: Config) -> dict:
    """Delete every object under the prefix and remove the SQLite catalog file.

    Uses pyiceberg's S3 FileSystem (via PyArrow) pointed at the *origin* to list
    and delete, guarded by the non-empty-prefix check so it can never operate at
    the bucket root.
    """
    root = require_prefix(cfg.prefix)
    from pyiceberg.io.pyarrow import PyArrowFileIO
    from pyarrow.fs import FileSelector

    io = PyArrowFileIO(properties=cfg.origin.pyiceberg_props())
    fs = io.fs_by_scheme("s3", cfg.bucket)
    base = f"{cfg.bucket}/{root}"
    deleted = 0

    try:
        # Count real objects first (for the report), then delete the whole
        # subtree in one call.
        entries = fs.get_file_info(FileSelector(base, recursive=True, allow_not_found=True))
        deleted = sum(1 for e in entries if e.type.name == "File")
        fs.delete_dir(base)
    except Exception as e:  # noqa: BLE001
        print(f"[teardown] listing/delete note: {e}", flush=True)

    catalog_removed = False
    if os.path.exists(cfg.catalog_db):
        os.remove(cfg.catalog_db)
        catalog_removed = True

    print(f"[teardown] deleted {deleted} objects under s3://{base}", flush=True)
    return {
        "prefix": root,
        "objects_deleted": deleted,
        "catalog_removed": catalog_removed,
    }


# --------------------------------------------------------------------------- #
# reporting                                                                    #
# --------------------------------------------------------------------------- #


def _mib(n: Optional[int]) -> str:
    """Formats a byte count as MiB (or '?' when unknown) for the tier context."""
    if n is None:
        return "?"
    return f"{n / (1024 * 1024):.0f} MiB"


def render_cache_stats(stats: dict) -> list[str]:
    """Render the tier-context lines: the cache budget and the warm-leg evidence.

    No latency number is ever published without this context (issue #141). When
    the admin surface was unreachable the lines say "unavailable" rather than
    omitting the context or inventing a value.
    """
    if not stats or not stats.get("available"):
        return ["  cache budget: unavailable (daemon /admin/stats not reachable)"]
    cfg = stats.get("cache_config", {})
    delta = stats.get("warm_leg_counter_delta", {})
    lines = [
        f"  cache budget: DRAM {_mib(cfg.get('dram_bytes'))} / "
        f"disk {_mib(cfg.get('capacity_bytes'))} (from daemon /admin/stats)",
        f"  DRAM tier in use after warm: {_mib(stats.get('dram_usage_bytes_after_warm'))}",
    ]
    if delta:
        lines.append(
            "  warm-leg reads: "
            f"dram_hits={delta.get('dram_hits', 0)} "
            f"disk_hits={delta.get('disk_hits', 0)} "
            f"backend_fills={delta.get('backend_fills', 0)} "
            f"(disk_hits>0 ⇒ served from NVMe; backend_fills>0 ⇒ eviction refills)"
        )
    return lines


def render_query_table(report: dict) -> str:
    """Render the per-query direct/cold/warm table plus totals as text."""
    lines = []
    m = report["machine"]
    lines.append("TPC-H over Iceberg — endpoint vs direct")
    lines.append(
        f"  SF{report['scale']}  |  {m['platform']}  |  {m['cpu_count']} CPUs  "
        f"|  duckdb {m['duckdb']}"
    )
    lines.append(f"  profile: {m.get('profile', 'unspecified')}")
    lines.append(f"  cache medium: {m['cache_medium_note']}")
    for line in render_cache_stats(report.get("cache_stats", {})):
        lines.append(line)
    rm = report["read_mode"]
    lines.append(
        f"  read mode: direct={rm['direct']} cold={rm['cold']} warm={rm['warm']}"
    )
    lines.append("")
    lines.append(
        f"  {'Q':>3}  {'rows':>8}  {'direct_ms':>10}  {'cold_ms':>10}  "
        f"{'warm_ms':>10}  {'direct/warm':>11}"
    )
    lines.append("  " + "-" * 62)
    for q in report["per_query"]:
        lines.append(
            f"  {q['nr']:>3}  {q['rows']:>8}  {q['direct_ms']:>10.2f}  "
            f"{q['cold_ms']:>10.2f}  {q['warm_ms']:>10.2f}  "
            f"{q['speedup_direct_over_warm']:>10.2f}x"
        )
    t = report["totals"]
    lines.append("  " + "-" * 62)
    lines.append(
        f"  {'TOT':>3}  {'':>8}  {t['direct_ms']:>10.2f}  {t['cold_ms']:>10.2f}  "
        f"{t['warm_ms']:>10.2f}  {t['speedup_direct_over_warm']:>10.2f}x"
    )
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# entrypoint                                                                   #
# --------------------------------------------------------------------------- #


def derive_admin_endpoint(verglas_endpoint: str) -> str:
    """Derive the daemon admin URL from the Verglas S3 endpoint (next port).

    ``verglas dev`` binds the admin API on ``s3_port + 1``; mirroring that here
    keeps the report's tier context working with the default local setup and no
    extra flag. Returns ``""`` when the endpoint carries no parseable port.
    """
    p = urlparse(verglas_endpoint)
    if not p.hostname or p.port is None:
        return ""
    scheme = p.scheme or "http"
    return f"{scheme}://{p.hostname}:{p.port + 1}"


def build_config(args: argparse.Namespace) -> Config:
    """Assemble a :class:`Config` from parsed flags, validating the origin creds."""
    for k in ("origin_endpoint", "origin_access_key_id", "origin_secret_access_key"):
        if not getattr(args, k):
            raise SystemExit(f"missing required origin credential: --{k.replace('_', '-')}")
    admin_endpoint = args.admin_endpoint or derive_admin_endpoint(args.verglas_endpoint)
    verglas = S3Target(
        endpoint=args.verglas_endpoint,
        access_key_id=args.verglas_access_key_id,
        secret_access_key=args.verglas_secret_access_key,
        region=args.verglas_region,
    )
    origin = S3Target(
        endpoint=args.origin_endpoint,
        access_key_id=args.origin_access_key_id,
        secret_access_key=args.origin_secret_access_key,
        region=args.origin_region,
    )
    return Config(
        scale=args.scale,
        bucket=args.bucket,
        prefix=require_prefix(args.prefix),
        namespace=args.namespace,
        catalog_db=args.catalog_db,
        verglas=verglas,
        origin=origin,
        read_mode=args.read_mode,
        cache_note=args.cache_note,
        profile=args.profile,
        admin_endpoint=admin_endpoint,
        as_json=args.json,
    )


def main() -> int:
    """Parse flags, run the requested phase, and print text or JSON output."""
    p = argparse.ArgumentParser(description="TPC-H-over-Iceberg benchmark driver")
    p.add_argument("phase", choices=["generate", "query", "teardown"])
    p.add_argument("--scale", type=float, default=1.0)
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", required=True)
    p.add_argument("--namespace", default="tpch")
    p.add_argument("--catalog-db", required=True)
    p.add_argument("--verglas-endpoint", required=True)
    p.add_argument("--verglas-access-key-id", required=True)
    p.add_argument("--verglas-secret-access-key", required=True)
    p.add_argument("--verglas-region", default="us-east-1")
    p.add_argument("--origin-endpoint", default=os.environ.get("AWS_ENDPOINT", ""))
    p.add_argument(
        "--origin-access-key-id", default=os.environ.get("AWS_ACCESS_KEY_ID", "")
    )
    p.add_argument(
        "--origin-secret-access-key",
        default=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    )
    p.add_argument("--origin-region", default=os.environ.get("AWS_REGION", "us-east-1"))
    p.add_argument(
        "--read-mode",
        choices=["auto", "duckdb-iceberg", "pyiceberg-arrow"],
        default="auto",
    )
    p.add_argument("--cache-note", default="daemon --cache-dir (medium unspecified)")
    p.add_argument(
        "--profile",
        default="unspecified",
        help="tier profile label: dram-resident | nvme-resident | constrained",
    )
    p.add_argument(
        "--admin-endpoint",
        default="",
        help="daemon admin URL for /admin/stats (cache budgets + counters); "
        "empty derives it from the Verglas endpoint's next port",
    )
    p.add_argument("--json", action="store_true")
    args = p.parse_args()

    cfg = build_config(args)

    if args.phase == "generate":
        report = phase_generate(cfg)
    elif args.phase == "query":
        report = phase_query(cfg)
    else:
        report = phase_teardown(cfg)

    if cfg.as_json:
        print(json.dumps(report, indent=2))
    elif args.phase == "query":
        print(render_query_table(report))
    else:
        print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
