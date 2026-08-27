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

- #171: Removed the obsolete broader-runtime comparison from the Pipeline README. The
  documented SQL boundary now describes the implemented evaluator directly and
  keeps the product contract independent of retired query-engine terminology.
- #176: Pipelines now register as Stream retention consumers and acknowledge only after every Sink confirms and the local cursor commits. Retry first catches the acknowledgment up to the durable cursor, so a lost remote acknowledgment delays cleanup without risking data loss. Pipeline also validates the contiguous union of Stream records and validation skips, advancing and acknowledging all-skipped ranges without creating empty Sink batches.
- #0: Corrected the immutable-SQL restart test to assert the initialization failure directly. Pipeline configuration mismatches are proven to fail before the object serves a request.
- #0: Added durable asynchronous Pipeline enqueue. Submission now acknowledges after the Pipeline alarm is committed, while Stream reads, Sink delivery, retries, and cursor advancement run in independent alarm events.
- #0: Delayed newly inserted Pipeline alarms by 100 ms so Verglas cannot
  reacquire the object for downstream work before the durable `202 queued`
  response has flushed; the regression test first witnessed the alarm being
  immediately due.
- #0: Consume the published JavaScript Worker SDK for component and runtime-surface tests. Pipeline no longer reaches into an SDK source tree owned by another repository.
