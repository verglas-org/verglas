#!/usr/bin/env python3
"""Engine matrix: DuckDB + Polars result diffing, direct-vs-Verglas (issue #23).

Wrong results are Verglas's existential risk. This harness is the tripwire that
runs on every PR: it reads *the same Iceberg fixture* two ways with two real
query engines and proves the bytes are identical.

Phases
------
- ``fixture``   Build a small Iceberg table into MinIO with pyiceberg: several
                snapshots (appends + one partial overwrite), an identity
                partition, and mixed column types (ints, strings, floats, a
                timestamp, nulls, a decimal). Records the metadata locations of
                the *final* snapshot and of an *older* snapshot (for time
                travel) in a JSON sidecar. Writes go straight to the origin so
                the Verglas read cache stays cold until the matrix runs.
- ``matrix``    For each engine (DuckDB via its ``iceberg`` extension; Polars
                via a pyiceberg scan -> Arrow) and each query (full scan,
                selective range predicate, few-column projection, snapshot time
                travel) run three legs and diff at the Arrow level:
                  * direct-to-MinIO           (baseline; never touches Verglas)
                  * Verglas pass 1 (MISS path; daemon cache starts empty)
                  * Verglas pass 2 (HIT  path; immediate re-run, cache warm)
                Every result is sorted deterministically, then schema and values
                are compared exactly. Any diff fails with a readable dump.
- ``selftest``  A MinIO-free guard that proves the comparator actually catches a
                mismatch (schema and value dumps). Assertion-free comparators
                are worthless as tripwires; this keeps the comparator honest in
                CI itself.

Fault injection
---------------
``--inject-fault {drop-row,mutate-cell,wrong-schema}`` deliberately corrupts the
Verglas-leg result before the diff, to demonstrate on a live stack that the
tripwire fires with a readable dump. Off by default; used only to record PR
evidence — never wired into CI.

Every credential arrives as an explicit flag (from ``run.sh``); this module
never reads customer secrets from anywhere but the flags handed to it, and
never logs them.
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Callable, Optional
from urllib.parse import urlparse

import duckdb
import polars as pl
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import StaticTable

# The single fixture table, addressed as ``<namespace>.<table>``.
TABLE_NAME = "events"

# How many mismatched rows a failing diff prints before truncating.
MAX_DUMP_ROWS = 20


# --------------------------------------------------------------------------- #
# S3 access profiles                                                           #
# --------------------------------------------------------------------------- #


@dataclass
class S3Target:
    """One S3 access profile: an endpoint plus the credentials to reach it.

    Used both for the Verglas endpoint (dev keys) and the direct MinIO origin
    (its static keys). ``host_port`` and ``use_ssl`` are derived for DuckDB's
    httpfs, which wants a bare host:port and an explicit SSL flag.
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
        """True when the endpoint speaks https, false for local http."""
        return urlparse(self.endpoint).scheme == "https"

    def pyiceberg_props(self) -> dict:
        """pyiceberg FileIO properties selecting this endpoint (path-style).

        MinIO and the local Verglas endpoint both require path-style
        addressing. pyiceberg's PyArrow FileIO maps these ``s3.*`` keys onto a
        ``pyarrow.fs.S3FileSystem``.
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

    bucket: str
    prefix: str
    namespace: str
    catalog_db: str
    sidecar: str
    verglas: S3Target
    origin: S3Target
    inject_fault: Optional[str]
    # Which fixture variant this run diffs: parquet | avro | mixed. Only labels
    # output and the deviation story — the matrix reads whatever data-file format
    # the sidecar's table happens to use, so the harness is format-agnostic.
    variant: str = "parquet"

    def warehouse(self) -> str:
        """The catalog warehouse root — always ``s3://<bucket>/<prefix>``."""
        return f"s3://{self.bucket}/{self.prefix}"

    def table_ident(self) -> str:
        """The dotted Iceberg identifier of the one fixture table."""
        return f"{self.namespace}.{TABLE_NAME}"


def require_prefix(prefix: str) -> str:
    """Reject an empty (or slashes-only) prefix.

    Seeding at the bucket root would scatter the fixture across the bucket. This
    guard mirrors the benchmarks' discipline and is load-bearing.
    """
    trimmed = prefix.strip().strip("/")
    if not trimmed:
        raise SystemExit("prefix must be a non-empty path within the bucket")
    return trimmed


def make_catalog(cfg: Config, target: S3Target) -> SqlCatalog:
    """Open the SQLite catalog with a FileIO pointed at ``target``.

    The catalog records only logical metadata *locations*; which network path
    resolves them is the FileIO's endpoint — exactly the integration point the
    direct-vs-Verglas comparison turns on.
    """
    props = {
        "uri": f"sqlite:///{cfg.catalog_db}",
        "warehouse": cfg.warehouse(),
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }
    props.update(target.pyiceberg_props())
    return SqlCatalog("matrix", **props)


# --------------------------------------------------------------------------- #
# fixture                                                                      #
# --------------------------------------------------------------------------- #

# The fixture schema, expressed once as a PyArrow schema. Mixed types: a long,
# two strings (one nullable, exercising nulls), a double and a float, a
# microsecond timestamp, a boolean, and a decimal(12,2). ``category`` is the
# identity partition key.
FIXTURE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("category", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("amount", pa.float64(), nullable=True),
        pa.field("ratio", pa.float32(), nullable=True),
        pa.field("ts", pa.timestamp("us"), nullable=True),
        pa.field("flag", pa.bool_(), nullable=True),
        pa.field("price", pa.decimal128(12, 2), nullable=True),
    ]
)

_EPOCH = dt.datetime(2024, 1, 1, 0, 0, 0)


def _batch(ids: range, category_of: Callable[[int], str]) -> pa.Table:
    """Build one deterministic Arrow batch for the given id range.

    Values are pure functions of ``id`` so every run produces byte-identical
    data; some cells are deliberately null to exercise the null path, and
    ``price`` is a decimal to exercise decimal encoding below the engines.
    """
    ids = list(ids)
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "category": pa.array([category_of(i) for i in ids], pa.string()),
            # Every 7th name is null.
            "name": pa.array(
                [None if i % 7 == 0 else f"name-{i:05d}" for i in ids], pa.string()
            ),
            # Every 5th amount is null; otherwise a spread-out double.
            "amount": pa.array(
                [None if i % 5 == 0 else round(i * 1.5 + 0.25, 2) for i in ids],
                pa.float64(),
            ),
            "ratio": pa.array([(i % 100) / 100.0 for i in ids], pa.float32()),
            "ts": pa.array(
                [_EPOCH + dt.timedelta(minutes=i) for i in ids], pa.timestamp("us")
            ),
            "flag": pa.array([i % 2 == 0 for i in ids], pa.bool_()),
            # Every 11th price is null; otherwise a decimal.
            "price": pa.array(
                [
                    None if i % 11 == 0 else decimal.Decimal(f"{i}.{i % 100:02d}")
                    for i in ids
                ],
                pa.decimal128(12, 2),
            ),
        },
        schema=FIXTURE_SCHEMA,
    )


def phase_fixture(cfg: Config) -> dict:
    """Build the multi-snapshot, partitioned, mixed-type Parquet fixture.

    Snapshot history (all Parquet data files, the pyiceberg default):
      S1  append ids 0..199   (categories A,B)
      S2  append ids 200..399 (categories B,C)      <- time-travel target
      S3  overwrite category A rows with mutated data (a genuine overwrite)
      S4  append ids 400..499 (category D)          <- final

    The metadata location after S2 and after S4 are recorded in the sidecar so
    the matrix phase can read either snapshot without depending on any
    engine's snapshot-selection syntax.

    This is the Parquet variant only. pyiceberg writes Parquet data files
    exclusively (verified against 0.11.1, which silently ignores
    ``write.format.default=avro`` and writes Parquet anyway). The Avro and
    mixed-format variants (#159) are built by the JVM fixture builder
    (``fixture-jvm/``) via iceberg-core + iceberg-data; the matrix phase reads
    all three identically. See README.md.
    """
    require_prefix(cfg.prefix)
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.expressions import EqualTo

    catalog = make_catalog(cfg, cfg.origin)
    catalog.create_namespace_if_not_exists(cfg.namespace)
    ident = cfg.table_ident()
    if catalog.table_exists(ident):
        catalog.drop_table(ident)

    # Identity partition on ``category``. The field id of ``category`` in the
    # Iceberg schema is resolved from the created table's schema below, so build
    # the table first with a schema, then set the spec — pyiceberg wants the
    # source field id, which it assigns on create.
    tbl = catalog.create_table(ident, schema=FIXTURE_SCHEMA)
    cat_field_id = tbl.schema().find_field("category").field_id
    with tbl.update_spec() as spec:
        spec.add_field("category", IdentityTransform(), "category_part")
    # Reload so the new spec is in effect for subsequent writes.
    tbl = catalog.load_table(ident)
    _ = (PartitionSpec, PartitionField, cat_field_id)  # documented extension point; ids auto-assigned

    def cat_ab(i: int) -> str:
        return "A" if i % 2 == 0 else "B"

    def cat_bc(i: int) -> str:
        return "B" if i % 2 == 0 else "C"

    # S1: ids 0..199 across A/B.
    tbl.append(_batch(range(0, 200), cat_ab))
    # S2: ids 200..399 across B/C. Capture this as the time-travel target.
    tbl.append(_batch(range(200, 400), cat_bc))
    tbl = catalog.load_table(ident)
    timetravel_metadata = tbl.metadata_location
    timetravel_snapshot = tbl.current_snapshot().snapshot_id

    # S3: a genuine partial overwrite — replace category A rows with mutated
    # data (names uppercased-ish via a different id offset so values differ).
    def cat_a(_i: int) -> str:
        return "A"

    overwrite_batch = _batch(range(1000, 1100), cat_a)
    tbl.overwrite(overwrite_batch, overwrite_filter=EqualTo("category", "A"))
    tbl = catalog.load_table(ident)

    # S4: append category D.
    def cat_d(_i: int) -> str:
        return "D"

    tbl.append(_batch(range(400, 500), cat_d))
    tbl = catalog.load_table(ident)
    final_metadata = tbl.metadata_location
    final_snapshot = tbl.current_snapshot().snapshot_id

    snapshots = [s.snapshot_id for s in tbl.metadata.snapshots]
    row_count = tbl.scan().to_arrow().num_rows

    sidecar = {
        "table_ident": ident,
        "final_metadata": final_metadata,
        "final_snapshot": final_snapshot,
        "timetravel_metadata": timetravel_metadata,
        "timetravel_snapshot": timetravel_snapshot,
        "snapshot_count": len(snapshots),
        "final_row_count": row_count,
        "data_file_format": "parquet",
    }
    with open(cfg.sidecar, "w") as f:
        json.dump(sidecar, f, indent=2)

    print(
        f"[fixture] {ident}: {len(snapshots)} snapshots, {row_count} final rows, "
        f"partitioned by category, Parquet data files",
        flush=True,
    )
    print(f"[fixture] wrote sidecar -> {cfg.sidecar}", flush=True)
    return sidecar


# --------------------------------------------------------------------------- #
# query specs                                                                  #
# --------------------------------------------------------------------------- #


@dataclass
class Query:
    """One matrix query: a name, whether it time-travels, and its parameters."""

    name: str
    kind: str  # full | predicate | projection | timetravel
    # For predicate: the inclusive lower bound on ``amount``.
    amount_min: float = 0.0
    # For projection: the columns to keep.
    columns: tuple = ()


QUERIES = [
    Query("full_scan", "full"),
    # Range-read heavy: a selective predicate on a non-partition numeric column
    # forces row-group range reads across the Parquet files.
    Query("selective_predicate", "predicate", amount_min=400.0),
    Query("projection", "projection", columns=("id", "category", "price")),
    Query("time_travel", "timetravel"),
]


def _metadata_for(query: Query, sidecar: dict) -> str:
    """Pick the metadata location a query reads: final, or the older snapshot."""
    if query.kind == "timetravel":
        return sidecar["timetravel_metadata"]
    return sidecar["final_metadata"]


# --------------------------------------------------------------------------- #
# DuckDB engine                                                                #
# --------------------------------------------------------------------------- #


def duckdb_conn_for(target: S3Target) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with httpfs+iceberg loaded and S3 pointed at ``target``.

    The S3 settings are per-connection, so each (leg, pass) gets its own
    connection and every scan re-issues GETs to exactly one endpoint — the
    Verglas hit pass genuinely re-fetches (served from the daemon's cache).
    """
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute(f"SET s3_endpoint='{target.host_port}'")
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_use_ssl={'true' if target.use_ssl else 'false'}")
    con.execute(f"SET s3_region='{target.region}'")
    con.execute(f"SET s3_access_key_id='{target.access_key_id}'")
    con.execute(f"SET s3_secret_access_key='{target.secret_access_key}'")
    return con


def read_duckdb(
    con: duckdb.DuckDBPyConnection, query: Query, metadata: str
) -> pa.Table:
    """Run one query through DuckDB's iceberg extension, returning an Arrow table.

    ``iceberg_scan`` is pointed straight at the pyiceberg-written metadata.json
    (final or an older snapshot's), so time travel needs no snapshot-selection
    syntax — the older metadata *is* the older snapshot. DuckDB plans the scan
    and pushes the predicate/projection into the Parquet reads over the
    connection's endpoint.
    """
    scan = f"iceberg_scan('{metadata}')"
    if query.kind == "full" or query.kind == "timetravel":
        sql = f"SELECT * FROM {scan}"
    elif query.kind == "predicate":
        sql = f"SELECT * FROM {scan} WHERE amount >= {query.amount_min}"
    elif query.kind == "projection":
        cols = ", ".join(query.columns)
        sql = f"SELECT {cols} FROM {scan}"
    else:  # pragma: no cover - guarded by QUERIES
        raise ValueError(f"unknown query kind: {query.kind}")
    return con.execute(sql).fetch_arrow_table()


# --------------------------------------------------------------------------- #
# Polars engine (pyiceberg scan -> Polars)                                     #
# --------------------------------------------------------------------------- #


def read_polars(target: S3Target, query: Query, metadata: str) -> pa.Table:
    """Run one query as a pyiceberg scan handed to Polars, returning Arrow.

    pyiceberg fetches the data files through ``target``'s endpoint (a fresh
    ``StaticTable`` per call, so the Verglas hit pass re-fetches); Polars then
    materializes and shapes the result. The predicate and projection are pushed
    into the pyiceberg scan (range-read heavy at the storage layer); the full
    and time-travel scans read everything. The older-snapshot metadata gives
    time travel without any snapshot-selection syntax.
    """
    from pyiceberg.expressions import GreaterThanOrEqual

    table = StaticTable.from_metadata(metadata, properties=target.pyiceberg_props())

    if query.kind in ("full", "timetravel"):
        arrow = table.scan().to_arrow()
        frame = pl.from_arrow(arrow)
    elif query.kind == "predicate":
        arrow = table.scan(
            row_filter=GreaterThanOrEqual("amount", query.amount_min)
        ).to_arrow()
        frame = pl.from_arrow(arrow)
    elif query.kind == "projection":
        arrow = table.scan(selected_fields=query.columns).to_arrow()
        frame = pl.from_arrow(arrow).select(list(query.columns))
    else:  # pragma: no cover - guarded by QUERIES
        raise ValueError(f"unknown query kind: {query.kind}")

    if isinstance(frame, pl.Series):  # single-column edge; keep it a frame
        frame = frame.to_frame()
    return frame.to_arrow()


# --------------------------------------------------------------------------- #
# Arrow-level diff harness                                                     #
# --------------------------------------------------------------------------- #


class ResultDiff(Exception):
    """Raised when two results differ; its message is the readable dump."""


def _canonical_schema(table: pa.Table) -> list[tuple]:
    """Schema as an order-sensitive list of ``(name, str(type))`` pairs."""
    return [(f.name, str(f.type)) for f in table.schema]


def _sorted(table: pa.Table) -> pa.Table:
    """Deterministically sort a table by all columns, nulls last.

    Sorting by every column (in schema order) makes the comparison independent
    of the order rows happen to come back in — the only thing that matters for
    correctness is the *set* of rows and their values.
    """
    keys = [(name, "ascending") for name in table.column_names]
    return table.sort_by(keys)


def _schema_diff_dump(name: str, left: pa.Table, right: pa.Table) -> str:
    """A readable schema-mismatch dump: the two field lists side by side."""
    ls, rs = _canonical_schema(left), _canonical_schema(right)
    lines = [f"SCHEMA MISMATCH in {name}:", "  direct (baseline):"]
    lines += [f"    {n}: {t}" for n, t in ls]
    lines.append("  verglas:")
    lines += [f"    {n}: {t}" for n, t in rs]
    only_left = [p for p in ls if p not in rs]
    only_right = [p for p in rs if p not in ls]
    if only_left:
        lines.append(f"  fields only in direct:  {only_left}")
    if only_right:
        lines.append(f"  fields only in verglas: {only_right}")
    return "\n".join(lines)


def _value_diff_dump(name: str, left: pa.Table, right: pa.Table) -> str:
    """A readable value-mismatch dump: row counts and the first N differing rows.

    Both tables are already sorted; rows are compared positionally after that,
    so the dump points at genuine value differences rather than ordering noise.
    """
    lines = [f"VALUE MISMATCH in {name}:"]
    lines.append(
        f"  row count: direct={left.num_rows} verglas={right.num_rows}"
    )
    lrows = left.to_pylist()
    rrows = right.to_pylist()
    shown = 0
    for i in range(min(len(lrows), len(rrows))):
        if lrows[i] != rrows[i]:
            lines.append(f"  row {i}:")
            lines.append(f"    direct : {lrows[i]}")
            lines.append(f"    verglas: {rrows[i]}")
            shown += 1
            if shown >= MAX_DUMP_ROWS:
                lines.append(f"  … (truncated at {MAX_DUMP_ROWS} differing rows)")
                break
    if shown == 0 and len(lrows) != len(rrows):
        # Same values where they overlap, but different lengths: show the tail.
        longer, label = (lrows, "direct") if len(lrows) > len(rrows) else (rrows, "verglas")
        extra = longer[min(len(lrows), len(rrows)) :][:MAX_DUMP_ROWS]
        lines.append(f"  extra rows only in {label}:")
        lines += [f"    {r}" for r in extra]
    return "\n".join(lines)


def compare(name: str, direct: pa.Table, verglas: pa.Table) -> None:
    """Compare two Arrow results exactly; raise ``ResultDiff`` with a dump on any diff.

    Schema (names + types, in order) must match, then values must match after a
    deterministic sort. This is the tripwire: identical bytes, or a failure a
    human can read.
    """
    if _canonical_schema(direct) != _canonical_schema(verglas):
        raise ResultDiff(_schema_diff_dump(name, direct, verglas))
    ldirect = _sorted(direct)
    lverglas = _sorted(verglas)
    if ldirect.num_rows != lverglas.num_rows or not ldirect.equals(lverglas):
        raise ResultDiff(_value_diff_dump(name, ldirect, lverglas))


# --------------------------------------------------------------------------- #
# fault injection (PR evidence only; never wired into CI)                      #
# --------------------------------------------------------------------------- #


def _inject(fault: str, table: pa.Table) -> pa.Table:
    """Corrupt a result to prove the tripwire fires on a live stack.

    ``drop-row``     silently loses the last row (a truncated read).
    ``mutate-cell``  flips one value (wrong bytes served).
    ``wrong-schema`` renames a column (a schema drift).
    """
    if fault == "drop-row":
        return table.slice(0, max(0, table.num_rows - 1))
    if fault == "mutate-cell":
        rows = table.to_pylist()
        if rows:
            first_col = table.column_names[0]
            v = rows[0][first_col]
            rows[0][first_col] = (v + 1) if isinstance(v, (int, float)) else "CORRUPT"
        return pa.Table.from_pylist(rows, schema=table.schema)
    if fault == "wrong-schema":
        names = list(table.column_names)
        names[-1] = names[-1] + "_drifted"
        return table.rename_columns(names)
    raise SystemExit(f"unknown fault: {fault}")


# --------------------------------------------------------------------------- #
# matrix                                                                       #
# --------------------------------------------------------------------------- #


def _read_leg(engine: str, target: S3Target, query: Query, metadata: str):
    """Dispatch a single read to the named engine on the given leg."""
    if engine == "duckdb":
        con = duckdb_conn_for(target)
        try:
            return read_duckdb(con, query, metadata)
        finally:
            con.close()
    if engine == "polars":
        return read_polars(target, query, metadata)
    raise ValueError(f"unknown engine: {engine}")


@dataclass
class LegOutcome:
    """The result of one engine read: either a table, or the error it raised.

    An engine that cannot decode a data-file format (e.g. Avro) raises rather
    than returning rows. Capturing that — instead of letting it crash the run —
    is what lets the matrix report a *per-engine reader gap* as an honest finding
    while still catching a Verglas-only failure as the tripwire it is.
    """

    ok: bool
    table: Optional[pa.Table]
    error: Optional[str]


def _read_outcome(engine: str, target: S3Target, query: Query, metadata: str) -> LegOutcome:
    """Read one leg, capturing an unreadable-format (or any) error as an outcome."""
    try:
        return LegOutcome(True, _read_leg(engine, target, query, metadata), None)
    except Exception as e:  # noqa: BLE001 — the error text is the finding
        return LegOutcome(False, None, f"{type(e).__name__}: {e}")


def _evaluate(
    label: str,
    direct: LegOutcome,
    miss: LegOutcome,
    hit: LegOutcome,
) -> tuple[dict, list[str], Optional[str]]:
    """Reconcile the three legs into a matrix row, plus any failures and finding.

    The honest-reader rules, in one place so they are unit-testable without MinIO:

    * **Direct read succeeded** — the normal path. Each Verglas leg must match the
      baseline exactly (``compare``). A mismatch, or a Verglas leg that itself
      failed to read what the baseline read, is a real failure (the tripwire).
    * **Direct read failed** — the engine cannot read this variant's data files at
      all (an Avro reader gap). If *both* Verglas legs fail the same way, that is a
      documented per-engine **finding**, not a diff: Verglas is symmetric with the
      origin. If a Verglas leg instead *returns* a result where the direct read
      errored, that is an anomaly — Verglas must never conjure bytes the origin
      could not — and is a failure.

    Returns ``(row, failures, finding)`` where ``row`` has ``miss``/``hit`` cells
    (``ok`` / ``DIFF`` / ``n/a``), ``failures`` are readable dumps, and ``finding``
    is a one-line per-engine note or ``None``.
    """
    failures: list[str] = []
    finding: Optional[str] = None

    if not direct.ok:
        # The engine cannot read this variant at the origin. Each Verglas leg
        # must fail the same way; anything else is an anomaly.
        row = {"rows": 0, "miss": "n/a", "hit": "n/a"}
        for leg_name, leg in (("miss", miss), ("hit", hit)):
            if leg.ok:
                msg = (
                    f"ANOMALY in {label} [{leg_name}]: the direct read errored "
                    f"({direct.error}) but the Verglas leg returned "
                    f"{leg.table.num_rows} rows — Verglas must never return bytes "
                    f"the origin could not."
                )
                failures.append(msg)
                row[leg_name] = "DIFF"
        if not failures:
            # Normalize the message so every query's identical reader gap collapses
            # to one per-engine finding (drop the per-file s3:// path).
            reason = re.sub(r"'s3://[^']*'", "<data file>", direct.error)
            finding = f"cannot read {label.split('/')[0]} data files — {reason}"
        return row, failures, finding

    row = {"rows": direct.table.num_rows, "miss": "ok", "hit": "ok"}
    for leg_name, leg in (("miss", miss), ("hit", hit)):
        if not leg.ok:
            failures.append(
                f"READ FAILURE in {label} [{leg_name}]: the direct read returned "
                f"{direct.table.num_rows} rows but the Verglas leg raised "
                f"{leg.error}."
            )
            row[leg_name] = "DIFF"
            continue
        try:
            compare(f"{label} [{leg_name} vs direct]", direct.table, leg.table)
        except ResultDiff as diff:
            failures.append(str(diff))
            row[leg_name] = "DIFF"
    return row, failures, finding


def _enumerate_data_files(metadata: str, props: dict) -> list[str]:
    """List a table's data-file paths by planning the scan (reads manifests only).

    pyiceberg reads Iceberg's Avro *manifests* fine — it is the Avro *data* files
    it cannot decode — so this enumerates the data files of any variant, including
    the ones no engine can read. Planning never opens a data file.
    """
    table = StaticTable.from_metadata(metadata, properties=props)
    return sorted({task.file.file_path for task in table.scan().plan_files()})


def _read_object(props: dict, path: str) -> bytes:
    """Fetch one object's raw bytes through the endpoint in ``props``.

    A fresh FileIO per call so the Verglas hit leg genuinely re-fetches (served
    from the daemon cache) rather than reusing an open handle.
    """
    from pyiceberg.io.pyarrow import PyArrowFileIO

    io = PyArrowFileIO(properties=props)
    with io.new_input(path).open() as f:
        return f.read()


def phase_datafile_bytes(cfg: Config, sidecar: dict) -> tuple[list[str], int]:
    """Byte-compare every data file direct vs Verglas (miss + hit).

    This is the Verglas tripwire that does not depend on an engine decoding the
    format: for the Avro and mixed variants — which neither DuckDB nor pyiceberg
    can read (see the per-engine findings) — it still proves the daemon serves the
    exact same Avro data-file bytes as the origin, on both a cold and a warm
    cache. For the Parquet variant it complements the engine-level diff. The data
    files are enumerated through the spec-complete manifests the JVM builder wrote.
    """
    metas = {sidecar["final_metadata"], sidecar["timetravel_metadata"]}
    paths: set[str] = set()
    for meta in metas:
        paths.update(_enumerate_data_files(meta, cfg.origin.pyiceberg_props()))

    failures: list[str] = []
    for path in sorted(paths):
        direct = _read_object(cfg.origin.pyiceberg_props(), path)
        for leg_name in ("miss", "hit"):
            served = _read_object(cfg.verglas.pyiceberg_props(), path)
            if served != direct:
                failures.append(
                    f"DATA-FILE BYTE MISMATCH [{leg_name}] {path}: "
                    f"direct={len(direct)}B verglas={len(served)}B "
                    f"(first differing byte at "
                    f"{next((i for i in range(min(len(direct), len(served))) if direct[i] != served[i]), 'len')})"
                )
    return failures, len(paths)


def phase_matrix(cfg: Config) -> dict:
    """Run every (engine, query) three legs and diff; return a report, fail on any diff.

    Legs, in order: direct (baseline, never touches Verglas so the cache stays
    empty), Verglas MISS (first touch), Verglas HIT (immediate re-run). Both
    Verglas legs are diffed against the direct baseline. When an engine cannot
    read the fixture's data-file format at all (an Avro reader gap), that is
    recorded as a per-engine finding rather than a diff — see :func:`_evaluate`.
    """
    require_prefix(cfg.prefix)
    with open(cfg.sidecar) as f:
        sidecar = json.load(f)

    engines = ["duckdb", "polars"]
    results = []
    failures = []
    findings = []

    for engine in engines:
        for query in QUERIES:
            metadata = _metadata_for(query, sidecar)
            label = f"{engine}/{query.name}"

            direct = _read_outcome(engine, cfg.origin, query, metadata)
            miss = _read_outcome(engine, cfg.verglas, query, metadata)
            hit = _read_outcome(engine, cfg.verglas, query, metadata)

            if cfg.inject_fault and hit.ok:
                hit = LegOutcome(True, _inject(cfg.inject_fault, hit.table), None)

            row, leg_failures, finding = _evaluate(label, direct, miss, hit)
            row = {"engine": engine, "query": query.name, **row}
            results.append(row)

            for f in leg_failures:
                print(f"[matrix] {label} FAILURE", flush=True)
                print(f, flush=True)
            failures.extend(leg_failures)
            if finding:
                note = f"{engine}: {finding}"
                if note not in findings:  # collapse the same gap across queries
                    findings.append(note)
                print(f"[matrix] {label} FINDING: {finding}", flush=True)
            elif not leg_failures:
                print(
                    f"[matrix] {label} OK ({row['rows']} rows, miss+hit match)",
                    flush=True,
                )

    # Object-level tripwire: prove Verglas serves identical data-file bytes even
    # for variants no engine can decode. Skipped only if fault injection is on
    # (that fault targets the engine legs, not the raw object store).
    datafile_checked = 0
    if not cfg.inject_fault:
        byte_failures, datafile_checked = phase_datafile_bytes(cfg, sidecar)
        for f in byte_failures:
            print(f"[matrix:{cfg.variant}] {f}", flush=True)
        failures.extend(byte_failures)
        if not byte_failures:
            print(
                f"[matrix:{cfg.variant}] data-file bytes: {datafile_checked} files "
                f"identical direct vs Verglas (miss+hit)",
                flush=True,
            )

    report = {
        "variant": cfg.variant,
        "engines": engines,
        "queries": [q.name for q in QUERIES],
        "results": results,
        "failures": len(failures),
        "findings": findings,
        "datafile_bytes_checked": datafile_checked,
        "sidecar": sidecar,
    }
    print(_render_matrix(report), flush=True)
    if failures:
        print(
            f"\n[matrix:{cfg.variant}] FAILED: {len(failures)} result diff(s) — see dumps above",
            flush=True,
        )
        raise SystemExit(1)
    if findings:
        print(
            f"\n[matrix:{cfg.variant}] PASS with findings: no result diffs; "
            f"{datafile_checked} data files served byte-identical by Verglas; "
            f"{len(findings)} per-engine reader gap(s) documented above.",
            flush=True,
        )
    else:
        print(
            f"\n[matrix:{cfg.variant}] PASS: all engines, miss and hit paths, "
            f"zero result diffs",
            flush=True,
        )
    return report


def _render_matrix(report: dict) -> str:
    """Render the pass/fail grid: one row per (engine, query), miss and hit."""
    variant = report.get("variant", "parquet")
    lines = ["", f"Engine matrix [{variant}] — direct vs Verglas (miss/hit)"]
    lines.append(f"  {'engine':>8}  {'query':>20}  {'rows':>6}  {'miss':>5}  {'hit':>5}")
    lines.append("  " + "-" * 52)
    for r in report["results"]:
        lines.append(
            f"  {r['engine']:>8}  {r['query']:>20}  {r['rows']:>6}  "
            f"{r['miss']:>5}  {r['hit']:>5}"
        )
    for f in report.get("findings", []):
        lines.append(f"  finding: {f}")
    checked = report.get("datafile_bytes_checked", 0)
    if checked:
        lines.append(
            f"  data-file bytes: {checked} files byte-identical direct vs Verglas"
        )
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# selftest — proves the comparator catches diffs (no MinIO)                    #
# --------------------------------------------------------------------------- #


def phase_selftest(_cfg: Config) -> dict:
    """Prove the comparator raises on schema and value diffs, passes on equality.

    A tripwire that never trips is worthless; this runs in CI with no
    dependencies so the comparator can never silently degrade to a no-op.
    """
    base = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})

    # 1. Identical tables compare equal (even reordered — the sort normalizes).
    reordered = pa.table({"id": [3, 1, 2], "v": ["c", "a", "b"]})
    compare("selftest/equal", base, reordered)

    # 2. A single wrong value is caught, with a value dump.
    wrong_value = pa.table({"id": [1, 2, 3], "v": ["a", "X", "c"]})
    try:
        compare("selftest/value", base, wrong_value)
        raise AssertionError("comparator missed a value diff")
    except ResultDiff as d:
        assert "VALUE MISMATCH" in str(d), str(d)
        assert "verglas" in str(d), str(d)

    # 3. A missing row is caught.
    missing_row = pa.table({"id": [1, 2], "v": ["a", "b"]})
    try:
        compare("selftest/rows", base, missing_row)
        raise AssertionError("comparator missed a row-count diff")
    except ResultDiff as d:
        assert "row count" in str(d), str(d)

    # 4. A schema drift (renamed column) is caught, with a schema dump.
    drifted = base.rename_columns(["id", "v_drifted"])
    try:
        compare("selftest/schema", base, drifted)
        raise AssertionError("comparator missed a schema diff")
    except ResultDiff as d:
        assert "SCHEMA MISMATCH" in str(d), str(d)

    # 5. A type change is caught by the schema check.
    typed = pa.table({"id": pa.array([1, 2, 3], pa.int32()), "v": ["a", "b", "c"]})
    try:
        compare("selftest/type", base, typed)
        raise AssertionError("comparator missed a type diff")
    except ResultDiff as d:
        assert "SCHEMA MISMATCH" in str(d), str(d)

    print("[selftest] comparator catches value, row, schema, and type diffs", flush=True)

    # 6. The finding/anomaly reconciliation (_evaluate) — the Avro-reader-gap
    #    honesty guard. A tripwire that turns a real diff into a silent "finding"
    #    would be worse than none, so pin the rules here without MinIO.
    tbl = pa.table({"id": [1, 2, 3]})
    other = pa.table({"id": [1, 2, 9]})
    ok = lambda t: LegOutcome(True, t, None)  # noqa: E731
    err = lambda: LegOutcome(False, None, "Unsupported file format: FileFormat.AVRO")  # noqa: E731

    # Equal reads through all three legs: clean pass, no failures, no finding.
    row, fails, finding = _evaluate("duckdb/full_scan", ok(tbl), ok(tbl), ok(tbl))
    assert fails == [] and finding is None and row["miss"] == "ok", (row, fails, finding)

    # A Verglas leg that differs from the baseline is a failure (the tripwire).
    row, fails, finding = _evaluate("duckdb/full_scan", ok(tbl), ok(other), ok(tbl))
    assert len(fails) == 1 and row["miss"] == "DIFF", (row, fails)

    # Engine can't read the format on ANY leg (direct + both Verglas fail): a
    # documented per-engine finding, not a diff, not a hard failure.
    row, fails, finding = _evaluate("polars/full_scan", err(), err(), err())
    assert fails == [] and finding is not None and row["miss"] == "n/a", (row, fails, finding)
    assert "cannot read polars" in finding, finding

    # Direct read failed but a Verglas leg returned rows: an anomaly (Verglas must
    # never conjure bytes the origin could not) — a failure, never a finding.
    row, fails, finding = _evaluate("duckdb/full_scan", err(), ok(tbl), err())
    assert len(fails) == 1 and finding is None and "ANOMALY" in fails[0], (row, fails)

    # Direct read succeeded but a Verglas leg failed to read it: a failure.
    row, fails, finding = _evaluate("duckdb/full_scan", ok(tbl), err(), ok(tbl))
    assert len(fails) == 1 and "READ FAILURE" in fails[0], (row, fails)

    print("[selftest] _evaluate classifies diffs, findings, and anomalies correctly", flush=True)
    return {"selftest": "ok"}


# --------------------------------------------------------------------------- #
# entrypoint                                                                   #
# --------------------------------------------------------------------------- #


def build_config(args: argparse.Namespace) -> Config:
    """Assemble a :class:`Config` from parsed flags."""
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
        bucket=args.bucket,
        prefix=require_prefix(args.prefix),
        namespace=args.namespace,
        catalog_db=args.catalog_db,
        sidecar=args.sidecar,
        verglas=verglas,
        origin=origin,
        inject_fault=args.inject_fault,
        variant=args.variant,
    )


def main() -> int:
    """Parse flags, run the requested phase."""
    p = argparse.ArgumentParser(description="Engine-matrix result-diffing driver")
    p.add_argument("phase", choices=["fixture", "matrix", "selftest"])
    p.add_argument("--bucket", default="verglas-test")
    p.add_argument("--prefix", default="engine-matrix")
    p.add_argument("--namespace", default="matrix")
    p.add_argument("--catalog-db", default="catalog.db")
    p.add_argument("--sidecar", default="fixture.json")
    p.add_argument(
        "--variant",
        choices=["parquet", "avro", "mixed"],
        default="parquet",
        help="which fixture variant this run diffs (labels output; the matrix is "
        "format-agnostic). Avro/mixed fixtures are built by the JVM builder.",
    )
    p.add_argument("--verglas-endpoint", default="http://127.0.0.1:8333")
    p.add_argument("--verglas-access-key-id", default="")
    p.add_argument("--verglas-secret-access-key", default="")
    p.add_argument("--verglas-region", default="us-east-1")
    p.add_argument("--origin-endpoint", default=os.environ.get("AWS_ENDPOINT", ""))
    p.add_argument("--origin-access-key-id", default=os.environ.get("AWS_ACCESS_KEY_ID", ""))
    p.add_argument(
        "--origin-secret-access-key", default=os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    )
    p.add_argument("--origin-region", default=os.environ.get("AWS_REGION", "us-east-1"))
    p.add_argument(
        "--inject-fault",
        choices=["drop-row", "mutate-cell", "wrong-schema"],
        default=None,
        help="corrupt the Verglas leg to prove the tripwire fires (PR evidence only)",
    )
    args = p.parse_args()

    cfg = build_config(args)

    if args.phase == "fixture":
        if cfg.variant != "parquet":
            raise SystemExit(
                f"the Python fixture phase builds Parquet only; the {cfg.variant} "
                f"variant is built by the JVM builder (fixture-jvm/). See run.sh."
            )
        report = phase_fixture(cfg)
    elif args.phase == "matrix":
        report = phase_matrix(cfg)
    else:
        report = phase_selftest(cfg)

    if args.phase != "matrix":
        print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
