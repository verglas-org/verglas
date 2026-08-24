# Worklog

- #171: Added the prebuilt Pipeline Worker and Durable Object with immutable
  SQL/configuration, independent durable cursors, bounded rolling batches,
  deterministic Sink delivery, retry state, and honest stateless SQL parsing.
  Tests were written first and initially failed because the expected `worker.js`
  project did not exist; they now cover component builds, transforms, fan-out,
  crash retry, limits, grammar rejection, and dependency closure without
  destination-specific storage code.
