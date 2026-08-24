# Worklog

- #171: Added the prebuilt Pipeline Worker and Durable Object with immutable
  SQL/configuration, independent durable cursors, bounded rolling batches,
  deterministic Sink delivery, retry state, and honest stateless SQL parsing.
  Tests were written first and initially failed because the expected `worker.js`
  project did not exist; they now cover component builds, transforms, fan-out,
  crash retry, limits, grammar rejection, and dependency closure without
  destination-specific storage code.

- #171: Closed the frozen PP5 stateless SQL gap with non-recursive CTEs,
  derived-table subqueries, and bounded correlated `UNNEST(array_expr) AS alias`
  expansion. Added first-failing grammar and component tests for composition,
  aliases, malformed/multiple UNNEST, recursion rejection, and expansion
  ceilings; joins and other stateful syntax remain rejected. The first run
  failed for the expected reasons (`expected SELECT, received WITH`, unsupported
  `UNNEST`, and unsupported `[` syntax) before the parser/evaluator changes
  turned the suite green.
