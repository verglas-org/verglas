# verglas-query role worklog

- #3: Made `verglas-query` the sole SQL execution role and added streamed Arrow IPC responses for SDK consumers.
- #91: Updated query-node compatibility documentation for the renamed
  `verglas-server` process. Query-node continues to share wire contracts and
  configuration shapes with the foreground server.
- #58: Query workers now receive only a cache-local metadata gateway URI. Upstream Lakekeeper credentials, warehouse configuration, and catalog change state remain in the cache node.

- #66: Neutralized memory-grant and spill-path docs so fixed-memory query roles no longer reference Firecracker or microVMs.
- #81: Accepted the caller's scoped run bearer only through inherited ephemeral process state and applied it to database-catalog bootstrap. Query config, argv, summaries, and durable declarations remain token-free.
- #130: Replaced the selected cache URL with a required list of cache-ring S3 endpoints and passed the list into Iceberg FileIO. Query reads and metadata access now share stable per-object placement with writers.
