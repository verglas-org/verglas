#!/usr/bin/env python3
"""Regenerates the pyiceberg-v2 fixture: a REAL Iceberg v2 table written by
pyiceberg, checked in so verglas-tables' lenient metadata parser is proven
against writer-authentic metadata (full embedded Avro schemas, field-id
labelled partition records, int status enums, sequence-number fields, nullable
unions everywhere) rather than only the crate's own hand-built manifests.

Table shape (small on purpose — the metadata shape, not the scale, is the
point): namespace `db`, table `events`, identity-partitioned on `category`,
two snapshots (append + append), a handful of rows per append.

Offline trick: production metadata embeds `s3://` URIs and the mapper keys on
them, so this script registers a local-directory filesystem as pyiceberg's
handler for the `s3` scheme. PyIceberg then writes byte-real metadata whose
embedded URIs are `s3://lake/warehouse/db/events/...` while the bytes land in
a temp dir — no MinIO, no network. Nothing about the written metadata is
post-processed; the files are copied into this directory verbatim, keyed by
their object key (the URI path under the bucket).

Usage (from the repo root):

    python3.13 -m venv /tmp/vg-pyiceberg-venv
    /tmp/vg-pyiceberg-venv/bin/pip install 'pyiceberg[sql-sqlite,pyarrow]==0.8.1' 'pyarrow==18.1.0'
    /tmp/vg-pyiceberg-venv/bin/python crates/verglas-tables/tests/fixtures/pyiceberg-v2/regenerate.py

Pinned: pyiceberg 0.8.1, pyarrow 18.1.0 (the versions the checked-in fixture
was produced with; also the engine-matrix pins).
"""

import json
import shutil
import sys
import tempfile
from pathlib import Path

import pyarrow as pa

BUCKET = "lake"
TABLE_LOCATION = f"s3://{BUCKET}/warehouse/db/events"
FIXTURE_DIR = Path(__file__).resolve().parent


def main() -> None:
    import pyiceberg

    if pyiceberg.__version__ != "0.8.1":
        sys.exit(
            f"pyiceberg {pyiceberg.__version__} found; the fixture is pinned to "
            "0.8.1 — regenerate with the pinned version or update this pin and "
            "the README together."
        )

    work = Path(tempfile.mkdtemp(prefix="vg-pyiceberg-fixture-"))
    warehouse = work / "warehouse-bytes"
    warehouse.mkdir()

    # Map the `s3` scheme onto a local directory: every s3://<bucket>/<key>
    # write lands at <warehouse>/<bucket>/<key>, while pyiceberg still embeds
    # the real s3:// URIs in everything it writes.
    import fsspec
    from fsspec.implementations.dirfs import DirFileSystem
    from fsspec.implementations.local import LocalFileSystem
    import pyiceberg.io.fsspec as picefs

    local_dir_fs = DirFileSystem(path=str(warehouse), fs=LocalFileSystem(auto_mkdir=True))

    class _FakeS3(DirFileSystem):
        """`s3` protocol backed by a local directory (fixture generation only)."""

        protocol = "s3"

    fsspec.register_implementation("s3", _FakeS3, clobber=True)
    picefs.SCHEME_TO_FS["s3"] = lambda properties: local_dir_fs

    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    catalog = SqlCatalog(
        "fixture",
        uri=f"sqlite:///{work}/catalog.db",
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )
    catalog.create_namespace("db")

    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "category", StringType(), required=True),
        NestedField(3, "amount", DoubleType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category")
    )
    table = catalog.create_table(
        "db.events", schema=schema, location=TABLE_LOCATION, partition_spec=spec
    )

    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("category", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
        ]
    )

    def batch(ids, categories, amounts):
        return pa.table(
            {"id": pa.array(ids, pa.int64()),
             "category": pa.array(categories, pa.string()),
             "amount": pa.array(amounts, pa.float64())},
            schema=arrow_schema,
        )

    # Snapshot 1: two partitions (a, b), six rows.
    table.append(batch([1, 2, 3, 4, 5, 6],
                       ["a", "a", "a", "b", "b", "b"],
                       [1.5, 2.5, None, 4.0, 5.0, 6.0]))
    # Snapshot 2: another append, partitions b and c.
    table.append(batch([7, 8, 9, 10],
                       ["b", "c", "c", "c"],
                       [7.0, None, 9.0, 10.0]))

    table = catalog.load_table("db.events")
    snapshots = [s.snapshot_id for s in table.snapshots()]
    current = table.current_snapshot().snapshot_id
    metadata_uri = table.metadata_location  # s3://lake/warehouse/db/events/metadata/xxxxx.metadata.json
    assert metadata_uri.startswith(f"s3://{BUCKET}/"), metadata_uri
    metadata_key = metadata_uri[len(f"s3://{BUCKET}/") :]

    data_files = sorted(
        str(task.file.file_path) for task in table.scan().plan_files()
    )
    data_keys = [uri[len(f"s3://{BUCKET}/") :] for uri in data_files]

    # Copy every written object into the fixture, keyed by its object key.
    # DirFileSystem does not strip the s3 scheme, so the bytes land under a
    # literal `s3:/<bucket>/` directory inside the temp warehouse.
    bucket_root = warehouse / "s3:" / BUCKET
    objects_root = FIXTURE_DIR / "objects"
    if objects_root.exists():
        shutil.rmtree(objects_root)
    for path in sorted(bucket_root.rglob("*")):
        if path.is_file():
            key = path.relative_to(bucket_root).as_posix()
            dest = objects_root / key
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dest)

    sidecar = {
        "pyiceberg_version": pyiceberg.__version__,
        "pyarrow_version": pa.__version__,
        "bucket": BUCKET,
        "table_location": TABLE_LOCATION,
        "metadata_key": metadata_key,
        "current_snapshot_id": current,
        "snapshot_ids": sorted(snapshots),
        "data_file_keys": data_keys,
    }
    (FIXTURE_DIR / "fixture.json").write_text(json.dumps(sidecar, indent=2) + "\n")

    shutil.rmtree(work)
    count = sum(1 for _ in objects_root.rglob("*") if _.is_file())
    print(f"wrote {count} objects under {objects_root}")
    print(json.dumps(sidecar, indent=2))


if __name__ == "__main__":
    main()
