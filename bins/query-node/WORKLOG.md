# verglas-query role worklog

- #3: Made `verglas-query` the sole SQL execution role and added streamed Arrow IPC responses for SDK consumers.
- #91: Updated query-node compatibility documentation for the renamed
  `verglas-server` process. Query-node continues to share wire contracts and
  configuration shapes with the foreground server.
- #58: Query workers now receive only a cache-local metadata gateway URI. Upstream Lakekeeper credentials, warehouse configuration, and catalog change state remain in the cache node.
