# Sink worklog

- #171: Added the prebuilt Iceberg Sink Worker and Durable Object with immutable configuration, bounded Pipeline batch validation, Turso idempotency receipts, and an explicit Catalog commit binding. The Sink delegates file and Iceberg authority to Catalog and retries the same deterministic batch identity after external commit or process failures.
- #171: The factual six-product run reproduced a 400 rejection because the stock Pipeline emits sink identity `primary_sink` while the stock Sink was configured as `primary`. Aligned the baked Sink identity with the Pipeline relation and service object so the deterministic batch fence names one object end to end.
