# Verglas Graph state contract

Use Verglas Graph as the durable experiment and evidence record. Do not use it as conversational memory, a prompt transcript, or the authoritative job queue.

## Entity model

- `Run` owns ordered `Wave` records.
- `Wave` identifies one AIDE parent and operation and dispatches one or more `Attempt` records.
- `Attempt` records scheduling preferences, assigned agent and model, actual agent and model, whether they matched, workspace identity and lifecycle state, lease generation, timing, tokens, cost, terminal state, and its produced `Candidate` when one exists.
- `Candidate` contains only compact selection properties and points to its parent. Solution bodies remain in their workspace or artifact store.
- `Evaluation` contains the scalar result and constraint failures.
- `MetricMeasurement` contains a raw value, unit, normalized utility, and evaluator identity.
- `GateEvaluation` contains one independently evaluated hard-gate result.
- `Artifact` contains an artifact identity, kind, and short summary. Raw contents remain outside the graph.
- `WaveClosed` is the visibility fence written after every attempt and evaluation in the wave.

Workspace identities are opaque. Do not assume Git worktrees, database clones, or lakehouse clones exist until the host supplies those facilities.

Record workspace lifecycle transitions as evidence: active, promoted, retained after promotion failure, and removed. The host resource inventory remains authoritative because a graph marker cannot prove physical deletion.

## Two state matrices

The candidate matrix drives AIDE selection. It contains candidate identity, parent, operation, status, debug depth, scalar score, named raw measurements and utilities, gate outcomes, child count, leaf state, wave identity, and policy eligibility. It includes the root and candidates from closed waves only.

The attempt matrix drives fleet observation. It contains attempt and wave identity, parent and operation, scheduling preferences, assigned and actual agent/model identity, their match observation, runtime state, timing, tokens, cost, workspace identity, produced candidate, failure reason, and lease generation. A model mismatch is operational evidence, not a correctness failure. Runtime cost never changes candidate correctness or reward.

The scheduler and its durable queue own claims, retries, leases, and fencing generations. The graph may record those values as evidence but must not grant execution authority.

## Visibility and restart

Write nodes and evidence edges before writing the `WaveClosed` marker. A graph reader must ignore candidates whose wave has no close marker. This prevents a fast attempt from influencing selection while slower siblings are still running.

Persist the coordinator checkpoint separately when the current Graph SDK cannot reconstruct arbitrary property queries efficiently. On restart, validate wave ordering, unique attempt and candidate identities, visible parentage, terminal closed-wave attempts, and contiguous candidate ordering. Rebuild AIDE state only from closed waves; retain open attempts solely for scheduler reconciliation.

## Context compiler

Compile a bounded context around the selected parent. Include its compact result, ancestors, sibling outcomes, failed gates, and a capped list of artifact summaries. Exclude solution bodies, raw test output, raw artifacts, full graph traversal results, and unrelated historical runs.

## Recursive harness experiments

`VerglasRecursiveGraphStore` coordinates an outer harness-search experiment. Each outer candidate records only prompt and work adjustment artifact metadata, then links to one or more initialized `VerglasGraphStore` inner runs and their held-out tasks. The outer evaluator must reject a candidate with no initialized inner run evidence or compact evidence artifact, and retain held-out metrics, evidence references, cost, and rejection status. Every proposed candidate must be evaluated before selection: a step may retain its incumbent parent after recording proposal rejection, or select only a proposed candidate whose outer evaluation is valid. `OuterStepClosed` is the final visibility fence; readers ignore an outer step until it exists. Prompt bodies, source code, diffs, and raw evaluator output never enter the graph.
