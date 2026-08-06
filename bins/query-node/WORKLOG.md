# verglas-query role worklog

- #3: Made `verglas-query` the sole SQL execution role and added streamed Arrow IPC responses for SDK consumers.
- #91: Updated query-node compatibility documentation for the renamed
  `verglas-server` process. Query-node continues to share wire contracts and
  configuration shapes with the foreground server.
- #42: Bound typed positional arguments on streamed query requests while preserving optional time travel. This makes the isolated query role usable as Rill's live OLAP execution backend.
