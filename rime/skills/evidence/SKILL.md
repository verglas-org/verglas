---
name: evidence
description: Attach evidence-backed data and the exact queries that produced it to a graph node while coding or debugging, so claims are query-reproducible instead of taken on trust. Stream a process or log file into a table with `verglas workers follow`, analyze it with any Iceberg SQL client (for example DuckDB) against the lakehouse, and record the finding plus its provenance and confidence on a graph node with `verglas graph add-node`/`add-edge`. Use for test logs, debug sessions, OTEL-style telemetry, and any claim ("this passed", "this is the root cause") that another agent should be able to check. Do not use for editing source code or secrets.
---

# Evidence

Do not report "it passed" or "this is the root cause" from memory or a scrollback glance. Capture the run, query it, and attach the query plus its result to a graph node — so the claim is query-reproducible by whoever reads the graph next, not just trusted because you said so.

## 0. Open the tape first: one live debug dashboard

Before analyzing anything on Verglas Cloud, create the investigation's
dashboard ONCE so the user watches the debugging as it happens: copy
`references/debug-tape.dashboard.json`, substitute the target name and graph,
register it with `verglas --json dashboard create --file <spec>`, and print the
returned `url` immediately. Its three panes are all live sources — the raw
`LogStream` fills as capture streams, the slice table fills as you isolate
rows, and the proof `GraphView` grows as claims attach — so never re-create or
republish it during the investigation, and never inline rows into the spec.
Where Cloud dashboards are unavailable, skip this step; the graph record in
step 3 remains the durable proof.

## 1. Capture: `verglas workers follow`

Stream a process or a growing log file into a table as rows.

```bash
verglas --json workers follow -- <command...>          # wrap a command; every captured line becomes a row
verglas --json workers follow --file </abs/path.log>   # tail an existing log file instead
```

Streams until Ctrl-C (or the wrapped command exits), then tears the worker down; add `--keep` to leave it registered so `verglas workers run <name>` can dispatch it again. Use an absolute log path. `verglas workers list`/`verglas workers get <name>` inspect what is registered; `verglas workers delete <name>` archives one.

## 2. Analyze: SQL through any Iceberg client

The CLI carries no SQL verb; query the captured rows like any other Iceberg
table with an Iceberg client such as DuckDB, attached to the catalog from
`~/.verglas/config.toml` and reading through the runtime's S3 endpoint.

```sql
select * from ns.test_run where level = 'error';
select count(*) as failed from ns.test_run where status = 'fail';
```

Keep the exact SQL string — it is the artifact you attach next, not a paraphrase of what it found.

Materialize every narrowing step. When a query isolates the rows that matter
(the error lines, the failing span, the bad batch), land that result as an
append to the investigation's slice table (`table append <ns>.<target>_slices
<rows-file>`) instead of leaving it in your scrollback. Each append is one cut
through the data: the dashboard's slice pane replays the investigation in
order, and the slice rows plus their SQL are the durable proof of the issue —
not a claim about what the logs said, the actual rows with the query that
found them.

## 3. Attach: `verglas graph add-node` / `verglas graph add-edge`

Record the finding on a graph node, with the query that produced it as the edge's provenance and a confidence for how strongly the evidence supports the claim.

```bash
echo '[{"id":"evidence:run-42","labels":["evidence"],"properties":{"claim":"suite passed","result":"0 failed"}}]' \
  | verglas --json graph add-node <namespace> -

echo '[{"sourceId":"candidate:42","predicate":"evidenced_by","targetId":"evidence:run-42","provenance":"select count(*) as failed from ns.test_run where status = '\''fail'\''","confidence":1.0}]' \
  | verglas --json graph add-edge <namespace> -
```

`provenance` on the edge is the exact command or query that produced the evidence; `confidence` reflects how much that evidence supports the claim, not how confident you feel. A graph node with no query behind it is an opinion, not evidence.

Pin every claim to the state it measured. When the project is a git
repository, set a `state` property on the evidence node to `git rev-parse
HEAD` (append `-dirty` when the tree has uncommitted changes). When the work
is not git-based — document editing, table-only pipelines — use the Iceberg
snapshot id of the table the claim was measured against instead. Claims at
different states are succession, not contradiction: the newer state
supersedes the older. A claim without a state is unscoped and ranks below
pinned evidence when readers weigh it.

## Conventions

- One evidence node per captured claim, linked from the thing it evidences (a candidate, a bug, a decision) with an `evidenced_by` edge.
- Reuse the captured table as the source of truth; do not copy rows into a node's `properties` when a query can re-derive them.
- `--json` on every command for machine-readable output.

## Wired into RIME

RIME workers use this skill to attach their evaluator run and result to their candidate's graph node instead of returning an unsupported claim of success — see `skills/rime/SKILL.md` and `agents/rime-worker.md`.
