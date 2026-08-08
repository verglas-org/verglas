# verglas-scheduler

`verglas-scheduler` is the open-source durable Postgres run queue. It records work before
execution, gives one consumer a fenced claim, and records the result. It does
not decide where a run executes and it does not start a process or container.

This boundary keeps the queue independent of placement:

- The scheduler service claims a run and executes the local worker harness.
- Execution adapters live outside this crate; the queue only owns durable claims.

## Queue contract

A trigger creates an `Invocation` with a stable source identity. Manual and HTTP
requests use the caller's idempotency key. Cron uses the deployment, trigger,
and logical time. A data update uses the subscription and catalog event
sequence. Enqueueing the same invocation twice returns the same run.

A consumer claims a ready run for a bounded lease. Every recovery claim gets a
new generation. Only the owner of the current generation may complete the run;
a late completion from an expired worker is rejected. A failed run may name its
next retry time.

The queue must support:

- create or join an idempotent run;
- claim the next ready run;
- renew a live claim;
- complete or fail under the current claim generation;
- list unfinished work for startup recovery;
- return the earliest future cron or retry time;
- record an immutable attempt history.

The crate owns the queue types, storage interface, and state-machine rules.
Execution begins only after a caller receives a claimed run:

```text
trigger -> RunQueue -> claimed run -> local executor
```

`RunQueue` must not contain placement, host capacity, OCI, or an execution
abstraction. A deployment consumes the claim and does whatever execution is
appropriate outside this crate.

## On-prem service

The on-prem `verglas-scheduler` Docker service has no mounted state directory.
It connects to operator-supplied Postgres for queue state and to `verglas-rest`
for worker declarations. The Docker Compose `workers` profile is optional; with
no scheduler URL and Postgres URL, Verglas starts without worker scheduling.

Verglas forwards manual, HTTP, and catalog-change triggers to the scheduler
service. The scheduler durably enqueues and immediately claims new work instead
of waiting for a polling cycle. Cron uses an in-process timer for the earliest
stored deadline. On startup the service performs one recovery read, then reacts
to pushed triggers and its timer.

The local executor remains outside the queue crate. It resolves the worker
record, invokes the local harness, and completes the fenced claim.

## Implemented boundary

The crate provides deterministic invocation IDs, the `RunQueue` contract, a
Postgres implementation using transactional `SKIP LOCKED` claims, fenced and
renewable leases, completion expiry checks, attempt inspection, retry deadlines,
and cron planning. The Docker scheduler accepts pushed events, executes claimed
workers, and sleeps on exact durable deadlines.

The queue guarantees durable, fenced delivery. It does not promise exactly-once
effects inside customer code. A worker must use the run ID as its idempotency
key when committing externally visible work.
