# Catalog REST interoperability fixture

This fixture sends real HTTP requests from PyIceberg 0.11.1 to a Node HTTP
server wrapped around the Worker/DO adapter. The Python side only uses the
standard `RestCatalog` API; it does not import Catalog implementation code or
inspect Durable Object storage. The Node side supplies a persisted-SQL seam and
valid deterministic Iceberg metadata for table creation.

Run it from `system/catalog` with the pinned environment:

```sh
/Users/jfbrown/code/cascadelabs/.venv/bin/python \
  interoperability/pyiceberg_rest_compat.py
```

The harness deliberately fails when the adapter cannot satisfy a standard
client operation. Do not convert those failures to expected failures: the
traceback identifies the REST request and the compatibility gap.
