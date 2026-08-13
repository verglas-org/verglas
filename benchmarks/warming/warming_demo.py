#!/usr/bin/env python3
"""Eager-warming live demonstration driver (issue #168).

Proves the #168 claim end to end against real infrastructure: a **watched**
Iceberg table's *cold* planning walk collapses toward a *warm* one because the
server warmed the table's metadata (``metadata.json`` -> manifest list ->
manifests -> Parquet footers) into #50's pinned store **before any client
asked**. The trigger is the ``[catalog]`` REST watcher (#47) driving the warming
coordinator (#168) — the exact production path — not a benchmark shortcut.

The plumbing this closes: ``benchmarks/tpch`` seeds through a *local SQLite*
catalog, which the REST watcher cannot see, so it never exercises warming. Here
an **Apache Polaris** container is the REST catalog verglas-cache-node watches; the table
is seeded *through Polaris* (pyiceberg as the client) with its data on the same
origin S3 the server fills from, so a commit the watcher observes drives a real
warming walk.

Subcommands (all configuration arrives as explicit flags from ``run.sh``, which
loads ``.env`` and never logs secrets):

- ``bootstrap``  create the Polaris catalog + admin grants (idempotent).
- ``seed``       dbgen at a scale factor -> write each TPC-H table as Iceberg
                 through Polaris, data on the origin. Idempotent per table.
- ``walk``       the planning walk through the Verglas endpoint
                 (metadata.json -> manifest list -> manifests -> footers),
                 timed, with the server's /admin/stats meta counters recorded as
                 mechanism evidence. Emits one JSON record appended to a results
                 file.
- ``report``     render the A/B/C comparison table (+ counters) from the
                 results file.
- ``teardown``   drop the tables, delete the prefix, drop the catalog.

This module never reads customer secrets from anywhere but the flags/environment
handed to it, and never logs them.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Optional

import requests

# The 8 TPC-H tables, in dependency-free write order. A subset is selectable via
# --tables for a quicker seed; the planning walk covers whatever was seeded.
ALL_TABLES = [
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]

# Speculative footer window the client reads, byte-identical to the warmer's
# default (crates/verglas-tables: DEFAULT_FOOTER_READ_BYTES). Warming pins the
# footer as an HTTP *suffix* range (`bytes=-N`); the client MUST issue the same
# suffix range for its read to hit the pinned suffix-granular entry.
FOOTER_WINDOW = 64 * 1024

# Fixed Polaris root principal (airgapped-bench convention; mirrors
# benchmarks/polaris). The realm defaults to the single configured realm, so no
# Polaris-Realm header is needed once a bearer token is presented.
POLARIS_CLIENT_ID = "root"
POLARIS_CLIENT_SECRET = "s3cr3t"
POLARIS_SCOPE = "PRINCIPAL_ROLE:ALL"


# --------------------------------------------------------------------------- #
# Polaris REST helpers
# --------------------------------------------------------------------------- #


def polaris_token(catalog_uri: str) -> str:
    """Fetch a Polaris OAuth2 client-credentials token for the root principal.

    pyiceberg's own token exchange asks for a scope Polaris rejects, so the
    token is minted here and handed to pyiceberg (and to verglas-cache-node) as a ready
    bearer credential. Tokens are short-lived; a demo run finishes well inside
    the lifetime, and each server start re-mints one.
    """
    resp = requests.post(
        f"{catalog_uri}/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": POLARIS_CLIENT_ID,
            "client_secret": POLARIS_CLIENT_SECRET,
            "scope": POLARIS_SCOPE,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _mgmt_headers(token: str) -> dict:
    """Auth + JSON headers for the Polaris management API."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def phase_bootstrap(args: argparse.Namespace) -> dict:
    """Create the Polaris catalog and the admin grants the seed needs.

    Idempotent: an already-present catalog/role/grant returns a conflict that is
    treated as success, so re-runs are safe. The catalog's ``storageConfigInfo``
    points at the *origin* S3 (``stsUnavailable`` so Polaris uses the static
    credentials directly — no STS on an S3-compatible origin), and a catalog
    admin role with ``CATALOG_MANAGE_CONTENT`` is bound to the root principal's
    ``service_admin`` role so pyiceberg can create namespaces and tables.
    """
    token = polaris_token(args.catalog_uri)
    h = _mgmt_headers(token)
    mgmt = args.mgmt_uri.rstrip("/") + "/api/management/v1"
    base = f"s3://{args.bucket}/{args.prefix}"

    def ok(resp: requests.Response, what: str) -> None:
        # 200/201 = created; 409 = already there (idempotent re-run).
        if resp.status_code not in (200, 201, 409):
            raise SystemExit(
                f"bootstrap {what} failed: HTTP {resp.status_code} {resp.text[:300]}"
            )

    catalog_body = {
        "catalog": {
            "type": "INTERNAL",
            "name": args.catalog,
            "properties": {"default-base-location": base},
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [base],
                "endpoint": args.origin_endpoint,
                "pathStyleAccess": True,
                "stsUnavailable": True,
                "region": args.origin_region,
            },
        }
    }
    ok(
        requests.post(f"{mgmt}/catalogs", headers=h, json=catalog_body, timeout=30),
        "create-catalog",
    )
    ok(
        requests.post(
            f"{mgmt}/catalogs/{args.catalog}/catalog-roles",
            headers=h,
            json={"catalogRole": {"name": "admin_role"}},
            timeout=30,
        ),
        "create-catalog-role",
    )
    ok(
        requests.put(
            f"{mgmt}/catalogs/{args.catalog}/catalog-roles/admin_role/grants",
            headers=h,
            json={"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
            timeout=30,
        ),
        "grant-manage-content",
    )
    ok(
        requests.put(
            f"{mgmt}/principal-roles/service_admin/catalog-roles/{args.catalog}",
            headers=h,
            json={"catalogRole": {"name": "admin_role"}},
            timeout=30,
        ),
        "assign-role",
    )
    return {"catalog": args.catalog, "base": base, "bootstrapped": True}


# --------------------------------------------------------------------------- #
# pyiceberg catalog (client-side, endpoint selectable)
# --------------------------------------------------------------------------- #


@dataclass
class S3Creds:
    """One S3 access profile (endpoint + credentials + region)."""

    endpoint: str
    access_key_id: str
    secret_access_key: str
    region: str


def rest_catalog(args: argparse.Namespace, s3: S3Creds):
    """Open a pyiceberg RestCatalog on Polaris with a FileIO pointed at ``s3``.

    The bearer token is minted here (pyiceberg's own exchange is rejected by
    Polaris). ``X-Iceberg-Access-Delegation`` is blanked so Polaris does not try
    to vend credentials it cannot mint against a static-credential origin — the
    client uses the ``s3.*`` credentials directly. The FileIO endpoint chooses
    which network path resolves the table's objects: the origin for seeding,
    the Verglas endpoint for the planning walk.
    """
    from pyiceberg.catalog.rest import RestCatalog

    token = polaris_token(args.catalog_uri)
    return RestCatalog(
        args.catalog,
        uri=args.catalog_uri,
        warehouse=args.catalog,
        token=token,
        **{
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": s3.endpoint,
            "s3.access-key-id": s3.access_key_id,
            "s3.secret-access-key": s3.secret_access_key,
            "s3.region": s3.region,
            "s3.path-style-access": "true",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )


def origin_creds(args: argparse.Namespace) -> S3Creds:
    """The origin S3 profile from the ambient AWS_* environment."""
    return S3Creds(
        endpoint=args.origin_endpoint,
        access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region=args.origin_region,
    )


def verglas_creds(args: argparse.Namespace) -> S3Creds:
    """The Verglas endpoint S3 profile (dev/static keys; any region signs)."""
    return S3Creds(
        endpoint=args.verglas_endpoint,
        access_key_id=args.verglas_access_key_id,
        secret_access_key=args.verglas_secret_access_key,
        region=args.verglas_region,
    )


# --------------------------------------------------------------------------- #
# seed
# --------------------------------------------------------------------------- #


def phase_seed(args: argparse.Namespace) -> dict:
    """dbgen at the scale factor and write each table as Iceberg through Polaris.

    Data files land in ``s3://<bucket>/<prefix>`` on the *origin* (the FileIO
    endpoint is the origin), so the Verglas cache starts genuinely cold and is
    warmed only by the server's watcher-driven walk. A small
    ``write.target-file-size-bytes`` fans the larger tables out into several
    Parquet files, giving the planning walk a realistic footer count. Idempotent:
    an existing table at the same identifier is dropped and rewritten.
    """
    import duckdb

    tables = args.tables.split(",") if args.tables else ALL_TABLES
    cat = rest_catalog(args, origin_creds(args))
    cat.create_namespace_if_not_exists(args.namespace)

    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")
    t0 = time.monotonic()
    con.execute(f"CALL dbgen(sf={args.scale})")
    gen_s = time.monotonic() - t0

    written = {}
    for t in tables:
        ident = f"{args.namespace}.{t}"
        arrow = con.execute(f"SELECT * FROM {t}").fetch_arrow_table()
        if cat.table_exists(ident):
            cat.drop_table(ident)
        tbl = cat.create_table(
            ident,
            schema=arrow.schema,
            properties={"write.target-file-size-bytes": str(args.target_file_size)},
        )
        w0 = time.monotonic()
        tbl.append(arrow)
        written[t] = {
            "rows": arrow.num_rows,
            "write_s": round(time.monotonic() - w0, 2),
        }
        print(
            f"[seed] wrote {ident}: {arrow.num_rows:,} rows "
            f"({written[t]['write_s']}s) on the origin",
            flush=True,
        )
    return {"scale": args.scale, "dbgen_s": round(gen_s, 2), "tables": written}


# --------------------------------------------------------------------------- #
# walk (the measurement)
# --------------------------------------------------------------------------- #


def fetch_counters(admin_endpoint: str) -> Optional[dict]:
    """Read the server's /admin/stats read-path counters, or None if unreachable."""
    try:
        r = requests.get(admin_endpoint.rstrip("/") + "/admin/stats", timeout=5)
        r.raise_for_status()
        return r.json()["counters"]
    except (requests.RequestException, KeyError, ValueError):
        return None


def resolve_pointers(args: argparse.Namespace, tables: list[str]) -> dict:
    """Resolve each table's current metadata.json location from Polaris.

    The pointer is endpoint-independent (a bare ``s3://`` URI); reading it from
    Polaris (not through Verglas) mirrors how a query engine learns the current
    metadata before it plans. The walk then reads that object — and everything it
    references — through the Verglas endpoint.
    """
    token = polaris_token(args.catalog_uri)
    root = f"{args.catalog_uri}/v1/{args.catalog}/namespaces/{args.namespace}/tables"
    out = {}
    for t in tables:
        r = requests.get(
            f"{root}/{t}", headers={"Authorization": f"Bearer {token}"}, timeout=30
        )
        r.raise_for_status()
        out[t] = r.json()["metadata-location"]
    return out


def planning_walk(args: argparse.Namespace, pointers: dict) -> tuple[int, int]:
    """Walk every table's planning metadata through the Verglas endpoint.

    For each table: read ``metadata.json`` (StaticTable), the manifest list, and
    every manifest through the Verglas FileIO; then read each Parquet data file's
    footer as a suffix range GET (``bytes=-FOOTER_WINDOW``) — byte-identical to
    what the warmer pins. Returns ``(files, footers)`` touched. This is exactly
    the object set a query engine reads to plan a scan, so warm-vs-cold here is
    the planning cost the server's warming targets.
    """
    import boto3
    from botocore.config import Config as BotoConfig
    from pyiceberg.table import StaticTable

    vg = verglas_creds(args)
    props = {
        "s3.endpoint": vg.endpoint,
        "s3.access-key-id": vg.access_key_id,
        "s3.secret-access-key": vg.secret_access_key,
        "s3.region": vg.region,
        "s3.path-style-access": "true",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }
    s3 = boto3.client(
        "s3",
        endpoint_url=vg.endpoint,
        aws_access_key_id=vg.access_key_id,
        aws_secret_access_key=vg.secret_access_key,
        region_name=vg.region,
        config=BotoConfig(
            s3={"addressing_style": "path"}, signature_version="s3v4"
        ),
    )

    files = 0
    footers = 0
    for _t, ml in pointers.items():
        tbl = StaticTable.from_metadata(ml, properties=props)  # metadata.json
        io = tbl.io
        snap = tbl.metadata.current_snapshot()
        if snap is None:
            continue
        for manifest in snap.manifests(io):  # manifest list + each manifest
            for entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
                df = entry.data_file
                files += 1
                if "PARQUET" not in str(df.file_format):
                    continue
                _, _, rest = df.file_path.partition("://")
                bucket, _, key = rest.partition("/")
                # Suffix range, matching the warmer's ReadRange::Suffix pin.
                s3.get_object(
                    Bucket=bucket, Key=key, Range=f"bytes=-{FOOTER_WINDOW}"
                )["Body"].read()
                footers += 1
    return files, footers


def phase_walk(args: argparse.Namespace) -> dict:
    """Time one planning walk and record the server's counter delta.

    The counter delta is the mechanism evidence: on a warmed table the walk is
    all ``meta_hits`` with zero ``backend_*`` (served from the pinned store); on
    a cold one it is ``meta_misses`` + backend round-trips to the origin.
    Appends a JSON record to ``--results`` and prints a one-line summary.
    """
    tables = args.tables.split(",") if args.tables else ALL_TABLES
    pointers = resolve_pointers(args, tables)

    before = fetch_counters(args.admin_endpoint)
    t0 = time.monotonic()
    files, footers = planning_walk(args, pointers)
    ms = (time.monotonic() - t0) * 1000.0
    after = fetch_counters(args.admin_endpoint)

    delta = {}
    if before and after:
        delta = {k: after.get(k, 0) - before.get(k, 0) for k in after}

    record = {
        "config": args.config,
        "run": args.run,
        "walk_ms": round(ms, 1),
        "tables": len(tables),
        "files": files,
        "footers": footers,
        "counters_delta": {
            k: delta.get(k, 0)
            for k in (
                "meta_hits",
                "meta_misses",
                "dram_hits",
                "disk_hits",
                "backend_fills",
                "backend_heads",
                # Footer suffix reads on small files are served straight from the
                # backend without a meta_miss/fill classification, so the byte
                # counter is what makes the cold leg's origin traffic visible.
                "backend_bytes_served",
                "meta_bytes_served",
            )
        },
    }
    if args.results:
        with open(args.results, "a") as f:
            f.write(json.dumps(record) + "\n")
    d = record["counters_delta"]
    print(
        f"[walk] config={args.config} run={args.run} {record['walk_ms']} ms  "
        f"files={files} footers={footers}  "
        f"meta_hits+{d['meta_hits']} meta_misses+{d['meta_misses']} "
        f"backend_heads+{d['backend_heads']} "
        f"backend_bytes+{d['backend_bytes_served']}",
        flush=True,
    )
    return record


# --------------------------------------------------------------------------- #
# report
# --------------------------------------------------------------------------- #

CONFIG_LABELS = {
    "A": "warming OFF, cold client walk (baseline)",
    "B": "warming ON, server-warmed, cold client walk",
    "C": "warm client walk (floor)",
}


def phase_report(args: argparse.Namespace) -> dict:
    """Render the A/B/C comparison table from the results JSONL.

    Averages the per-config runs, shows the counter delta of the first run of
    each config as the mechanism evidence, and computes the headline ratios
    (A/B = the collapse; B/C = how close server-warmed cold sits to the warm
    floor). Emits Markdown to ``--out`` and stdout.
    """
    records: list[dict] = []
    with open(args.results) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    by_config: dict[str, list[dict]] = {}
    for r in records:
        by_config.setdefault(r["config"], []).append(r)

    def avg(cfg: str) -> Optional[float]:
        rs = by_config.get(cfg)
        if not rs:
            return None
        return sum(r["walk_ms"] for r in rs) / len(rs)

    a, b, c = avg("A"), avg("B"), avg("C")
    lines = []
    lines.append("# Eager-warming live demonstration (#168)")
    lines.append("")
    lines.append(f"- machine: {args.machine_note}")
    lines.append(f"- cache medium: {args.cache_note}")
    lines.append(f"- dataset: {args.dataset_note}")
    lines.append(
        "- planning walk: metadata.json -> manifest list -> manifests -> "
        f"Parquet footers (suffix {FOOTER_WINDOW // 1024} KiB), through the "
        "Verglas S3 endpoint"
    )
    lines.append("")
    lines.append("## Planning-walk latency (lower is better)")
    lines.append("")
    lines.append("| config | description | runs (ms) | mean (ms) |")
    lines.append("|--------|-------------|-----------|-----------|")
    for cfg in ("A", "B", "C"):
        rs = by_config.get(cfg, [])
        runs = ", ".join(f"{r['walk_ms']:.1f}" for r in rs) or "—"
        mean = avg(cfg)
        lines.append(
            f"| {cfg} | {CONFIG_LABELS[cfg]} | {runs} | "
            f"{mean:.1f} |" if mean is not None else
            f"| {cfg} | {CONFIG_LABELS[cfg]} | {runs} | — |"
        )
    lines.append("")
    if a and b and c:
        lines.append(
            f"**Collapse: A/B = {a / b:.1f}x faster cold once the server warms "
            f"it.** Server-warmed cold (B) sits at {b / c:.2f}x the warm floor "
            f"(C) — i.e. within {abs(b - c):.0f} ms of warm."
        )
        lines.append("")
    lines.append("## Mechanism evidence (first run of each config)")
    lines.append("")
    lines.append(
        "| config | walk_ms | meta_hits | meta_misses | backend_heads | backend_bytes |"
    )
    lines.append(
        "|--------|---------|-----------|-------------|---------------|---------------|"
    )
    for cfg in ("A", "B", "C"):
        rs = by_config.get(cfg, [])
        if not rs:
            continue
        r = rs[0]
        d = r["counters_delta"]
        lines.append(
            f"| {cfg} | {r['walk_ms']:.1f} | {d['meta_hits']} | {d['meta_misses']} "
            f"| {d['backend_heads']} | {d['backend_bytes_served']:,} |"
        )
    lines.append("")
    lines.append(
        "B serves the whole planning walk from #50's pinned metadata store "
        "(`meta_hits` only, **zero backend bytes** — the server walked the "
        "metadata before the client asked). A pays a backend round-trip per "
        "planning object plus a footer fetch per Parquet file against the origin "
        "(`backend_bytes` > 0, `meta_hits` = 0)."
    )
    text = "\n".join(lines)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text + "\n")
    print(text)
    return {"a_ms": a, "b_ms": b, "c_ms": c}


def phase_teardown(args: argparse.Namespace) -> dict:
    """Drop the tables + namespace and the Polaris catalog.

    Prefix-scoped origin deletion is handled by ``run.sh`` (it owns the aws CLI
    and the .env). This drops the catalog metadata so a re-run starts clean.
    """
    tables = args.tables.split(",") if args.tables else ALL_TABLES
    try:
        cat = rest_catalog(args, origin_creds(args))
        for t in tables:
            ident = f"{args.namespace}.{t}"
            if cat.table_exists(ident):
                cat.drop_table(ident)
        try:
            cat.drop_namespace(args.namespace)
        except Exception:  # noqa: BLE001 - empty/absent namespace is fine
            pass
    except Exception as e:  # noqa: BLE001 - teardown is best-effort
        print(f"[teardown] catalog cleanup note: {e}", flush=True)
    return {"dropped": tables}


# --------------------------------------------------------------------------- #
# entrypoint
# --------------------------------------------------------------------------- #


def build_parser() -> argparse.ArgumentParser:
    """Assemble the subcommand parser; every input is an explicit flag."""
    p = argparse.ArgumentParser(description="Eager-warming demo driver (#168)")
    sub = p.add_subparsers(dest="phase", required=True)

    def common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--catalog-uri", required=True, help="Polaris REST base (…/api/catalog)")
        sp.add_argument("--catalog", default="demo")
        sp.add_argument("--namespace", default="tpch")
        sp.add_argument("--tables", default="", help="comma list; empty = all 8")
        sp.add_argument("--origin-endpoint", default=os.environ.get("AWS_ENDPOINT", ""))
        sp.add_argument("--origin-region", default=os.environ.get("AWS_REGION", "us-east-1"))

    bp = sub.add_parser("bootstrap")
    common(bp)
    bp.add_argument("--mgmt-uri", required=True, help="Polaris base (…:8181)")
    bp.add_argument("--bucket", required=True)
    bp.add_argument("--prefix", required=True)

    sp = sub.add_parser("seed")
    common(sp)
    sp.add_argument("--scale", type=float, default=1.0)
    sp.add_argument("--target-file-size", type=int, default=16 * 1024 * 1024)

    wp = sub.add_parser("walk")
    common(wp)
    wp.add_argument("--verglas-endpoint", required=True)
    wp.add_argument("--verglas-access-key-id", required=True)
    wp.add_argument("--verglas-secret-access-key", required=True)
    wp.add_argument("--verglas-region", default="us-east-1")
    wp.add_argument("--admin-endpoint", required=True)
    wp.add_argument("--config", required=True, choices=["A", "B", "C"])
    wp.add_argument("--run", type=int, required=True)
    wp.add_argument("--results", default="")

    rp = sub.add_parser("report")
    rp.add_argument("--results", required=True)
    rp.add_argument("--out", default="")
    rp.add_argument("--machine-note", default="unspecified machine")
    rp.add_argument("--cache-note", default="cache medium unspecified")
    rp.add_argument("--dataset-note", default="unspecified dataset")

    tp = sub.add_parser("teardown")
    common(tp)

    return p


def main() -> int:
    """Dispatch the requested phase and print its JSON result (walk/report self-print)."""
    args = build_parser().parse_args()
    phases = {
        "bootstrap": phase_bootstrap,
        "seed": phase_seed,
        "walk": phase_walk,
        "report": phase_report,
        "teardown": phase_teardown,
    }
    result = phases[args.phase](args)
    if args.phase not in ("walk", "report"):
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
