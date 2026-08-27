# Query

Query is a prebuilt Worker and Durable Object that directly consumes frozen
Pipeline batches. Immutable configuration declares sources, grouped aggregate
views, and typed bounded endpoints. Turso durably stores materialized rows,
source watermarks, and replay receipts in the Query object's event transaction.

Workers bind it with `queries: [{ binding, query_name }]` and call
`env.ANALYTICS.query(endpoint, params)` or `describe()`.
