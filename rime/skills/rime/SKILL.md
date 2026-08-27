---
name: rime
description: Reproduce a measurable software objective, explore independent candidate solutions in parallel, and select the best evidence-backed result using RIME's evolutionary, AIDE-derived search over git-identified code states. Supports correctness, features, database and systems performance, latency, throughput, cost, memory, retrieval quality, benchmark suites, and multi-metric engineering objectives while rejecting dead code, fallbacks, architectural point fixes, and guessed parsing or typing. Use when executable evaluation exists and multiple approaches are worthwhile. Do not trigger for read-only questions, trivial deterministic edits, or when the user asks to work directly.
---

# RIME

Prefer RIME for substantial coding work that can be evaluated independently. Do not force RIME when direct work is cheaper or the user opts out.

## Distribution boundary

The Verglas package owns RIME distribution. The canonical installation is
`verglas skill install rime`. RIME does not install or bundle a second
executable. Authentication setup is not part of this workflow.

## Choose the execution path

Use RIME when all of these are true:

- The task changes code or application behavior.
- A reproduction, test, benchmark, or other objective evaluator can distinguish candidates.
- Independent approaches could reasonably produce different outcomes.
- The available time and compute justify parallel exploration.

Work directly for explanations, inspection, formatting, obvious one-line fixes, and mechanical changes. Respect explicit requests to use or avoid RIME.

## Run the workflow

1. Define hard gates, named metrics, directions, units, normalization scales, weights, and constraints before proposing solutions. Never scalarize incompatible raw units directly.
2. Reproduce the baseline behavior and freeze the evaluator before mutation. For a bug or feature, write the acceptance test first and record its expected failure. For optimization, record raw baseline measurements under a fixed benchmark protocol.
3. Invoke the native host procedure below with the objective, baseline evidence, evaluator, appropriate budget, and owned workspaces. Ask the host to prefer low-cost workers, but accept the model it selects without turning cost, provider, or model class into a hard gate. Hosted, open-source, and local models are all valid. Do not simulate parallel candidates in one shared workspace.
4. Open one wave for each AIDE policy decision. Dispatch up to 100 independent attempts from the same parent and operator-specific context, bounded by the configured budget and the host's available concurrency. Record all outcomes, then close the wave before making another policy decision.
5. Let RIME draft initial solutions, probabilistically debug buggy candidates within its configured depth, and otherwise select a draft lineage as a deterministic bandit arm before greedily improving that lineage's best valid candidate. Draft context contains recent compact outcomes, debug context contains the selected failure path, and improve context contains local outcomes plus a plateau signal. Require each proposal to make one atomic change so its effect remains measurable.
6. Reject candidates that leave dead code, retain an unrequested fallback path, patch a symptom instead of the owning invariant, or guess at parsing and typing instead of using canonical schemas, parsers, ASTs, and domain types.
7. Require independent evaluator artifacts for every gate and metric, including the benchmark inputs and protocol. Never accept the mutation agent's unsupported claim of success. Prefer deterministic tests and benchmarks over another model evaluator. For agent or harness self-improvement, select on held-out cases the mutation workers cannot see and compare candidates under the same fixed cost budget.
8. Treat evaluator results as authoritative. Do not replace them with an agent's judgment.
9. After each closed wave, remove workspaces for candidates that cannot be selected again. Promote the final winner before removing its speculative workspace. On failure or cancellation, attempt cleanup for every workspace RIME allocated.
10. Review the selected candidate and its evidence before presenting it. Summarize the closed waves, selected candidate, evaluator result, and promotion outcome.

Briefly tell the user when starting RIME because it consumes more compute than direct editing.

## Communication style: the coordinator is the style overseer

All coordination — coordinator to user, coordinator to workers, worker
reports back — uses simplified technical English in the manner of the Google
developer documentation style guide: short declarative sentences, active
voice, present tense, concrete nouns, standard terminology, one idea per
sentence. No hedging ("might possibly", "it seems"), no filler ("it's worth
noting"), no marketing language, no apologies, no restating what was already
said. State facts, numbers, and next actions.

The coordinator actively enforces this. Long-running sessions drift toward
soft, wordy, low-information prose; the coordinator checks its own replies
and every worker report for that drift. When a worker report drifts, the
coordinator restates the contract in the next dispatch ("report in plain
technical English: what changed, what the evaluator said, verbatim outputs").
When its own replies drift, it corrects in the next turn without ceremony.
Style steering never changes evaluator results, gates, or the record — it
changes only how they are said.

## Native host procedures

Use the procedure matching the active host. These integrations are installed with this skill; do not claim the integration is unavailable without first checking the named tool or agent.

- **Codex:** allocate one explicit Git worktree per attempt, then call `spawn_agent` once per candidate using the `rime_worker` custom agent, `fork_turns: "none"`, and its absolute worktree path. The custom-agent file selects `gpt-5.6-luna`; do not pass a model override to `spawn_agent`. Spawn the complete wave before waiting. Use `wait_agent` until every attempt finishes, run the frozen evaluator independently in every worktree, and use `interrupt_agent` only for cancellation. Promote the winner before removing all RIME worktrees and pruning their Git metadata. Record the assigned and actual model; model identity is operational evidence, not a correctness gate.
- **Claude Code:** dispatch the plugin agent `verglas:rime-worker` once per candidate in the same message. Its `isolation: worktree` declaration owns candidate isolation. Wait for the complete wave, run the frozen evaluator independently in every returned worktree, select and promote the winner, then remove every speculative worktree through Claude's worktree controls.
- **Cursor:** allocate one isolated worktree or assigned workspace per candidate. Dispatch the `rime-worker` subagent once per candidate in the same message with model `composer-2.5-fast`; when the host requires a typed Task isolation mode, use `best-of-n-runner` with the same model and the `rime-worker` contract. Do not inherit the coordinator model. Spawn the complete wave before waiting. Run the frozen evaluator independently in every worktree, select and promote the winner, then remove every speculative worktree.
- **Pi:** allocate one explicit Git worktree per attempt, then call `rime_parallel` once with the whole wave. Supply a configured lower-cost model only when it is available; otherwise omit `model` and record the returned model. Independently evaluate each returned workspace, select and promote the winner, and remove every speculative worktree.

The coordinator—not a mutation worker—owns policy selection, evaluation, wave closure, promotion, and cleanup. Never let a worker evaluate its siblings or select the winner.

Read [references/algorithm.md](references/algorithm.md) when implementing a RIME operator, evaluator, scheduler, or state adapter.

Read [references/engineering-objective.md](references/engineering-objective.md) when defining metrics, constraints, benchmarks, or candidate evaluations.

Read [references/workspace-lifecycle.md](references/workspace-lifecycle.md) when implementing worktrees, sandboxes, clone adapters, promotion, cancellation, or cleanup.
