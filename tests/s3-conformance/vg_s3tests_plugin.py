"""Verglas s3-tests harness plugin.

Two jobs, both required to run Ceph s3-tests against a Verglas endpoint:

1. Bucket lifecycle interceptor. Verglas deliberately does not own bucket
   lifecycle (the wildcard model: buckets are addressed, not created/listed;
   ListBuckets is empty; CreateBucket/DeleteBucket return NotImplemented).
   s3-tests, however, creates a fresh random bucket per test. So we intercept
   CreateBucket/DeleteBucket at the boto3 client/resource layer and service
   them DIRECTLY against the origin (MinIO) — modelling production, where
   buckets pre-exist at the origin and Verglas serves objects within them.
   Every object-level operation still flows through the Verglas endpoint.

2. Exact-nodeid skip list. pytest's own ``--deselect`` prefix-matches on the
   last node component (``test_multipart_upload`` also deselects
   ``test_multipart_upload_contents``), which would silently hide passing
   tests. We instead deselect only EXACT nodeid matches loaded from the
   curated skip list (``VG_SKIP_LIST``), and report the count.
"""

import io
import os

import boto3
import pytest
from botocore.awsrequest import AWSResponse

# ---- 1. bucket-lifecycle interceptor -------------------------------------- #

_ORIGIN_EP = os.environ["VG_ORIGIN_ENDPOINT"]
_ORIGIN_AK = os.environ["VG_ORIGIN_ACCESS_KEY"]
_ORIGIN_SK = os.environ["VG_ORIGIN_SECRET_KEY"]

_origin = boto3.client(
    "s3",
    endpoint_url=_ORIGIN_EP,
    aws_access_key_id=_ORIGIN_AK,
    aws_secret_access_key=_ORIGIN_SK,
    region_name="us-east-1",
)


def _resp(status, headers=None, body=b""):
    r = AWSResponse(url="", status_code=status, headers=headers or {}, raw=io.BytesIO(body))
    r._content = body
    return r


def _bucket_from(request):
    # URL path is /{bucket} or /{bucket}/ (possibly with a query string).
    return request.url.split("://", 1)[1].split("/", 1)[1].split("?", 1)[0].strip("/")


def _on_create_bucket(request, **kw):
    b = _bucket_from(request)
    try:
        _origin.create_bucket(Bucket=b)
    except (_origin.exceptions.BucketAlreadyOwnedByYou, _origin.exceptions.BucketAlreadyExists):
        pass
    return _resp(200, {"Location": "/" + b})


def _on_delete_bucket(request, **kw):
    b = _bucket_from(request)
    try:
        _origin.delete_bucket(Bucket=b)
    except _origin.exceptions.NoSuchBucket:
        pass
    return _resp(204)


def _wire(client):
    client.meta.events.register("before-send.s3.CreateBucket", _on_create_bucket)
    client.meta.events.register("before-send.s3.DeleteBucket", _on_delete_bucket)


_orig_client = boto3.client


def _patched_client(*a, **k):
    c = _orig_client(*a, **k)
    if (k.get("service_name") or (a[0] if a else None)) == "s3":
        _wire(c)
    return c


boto3.client = _patched_client

_orig_resource = boto3.resource


def _patched_resource(*a, **k):
    r = _orig_resource(*a, **k)
    if (k.get("service_name") or (a[0] if a else None)) == "s3":
        _wire(r.meta.client)
    return r


boto3.resource = _patched_resource


# ---- 2. exact-nodeid skip list -------------------------------------------- #

def _load_skip_list():
    path = os.environ.get("VG_SKIP_LIST")
    if not path:
        return set()
    skips = set()
    with open(path) as fh:
        for line in fh:
            line = line.split("#", 1)[0].strip()
            if line:
                skips.add(line)
    return skips


_SKIP = _load_skip_list()


def pytest_collection_modifyitems(config, items):
    """Deselect exactly the nodeids in the curated skip list (no prefix match)."""
    if not _SKIP:
        return
    keep, dropped = [], []
    for item in items:
        if item.nodeid in _SKIP:
            dropped.append(item)
        else:
            keep.append(item)
    if dropped:
        config.hook.pytest_deselected(items=dropped)
        items[:] = keep
    # Under VG_SKIP_LIST_STRICT (the nightly full run), warn about skip-list
    # entries that no longer match any collected test — the list is the
    # compatibility contract and must not rot. Off for partial (smoke) runs.
    if os.environ.get("VG_SKIP_LIST_STRICT"):
        present = {i.nodeid for i in items} | {d.nodeid for d in dropped}
        stale = sorted(s for s in _SKIP if s not in present)
        if stale:
            print("\n[vg-s3tests] WARNING: skip-list entries not found in collection:")
            for s in stale:
                print("  ", s)
