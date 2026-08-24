# Sink worklog

- #171: Added the prebuilt Iceberg Sink Worker and Durable Object with immutable configuration, bounded Pipeline batch validation, Turso idempotency receipts, and an explicit Catalog commit binding. The Sink delegates file and Iceberg authority to Catalog and retries the same deterministic batch identity after external commit or process failures.
