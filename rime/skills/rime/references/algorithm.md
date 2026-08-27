# RIME algorithm contract

RIME is an evolutionary algorithm over code states: candidates are the population, draft/debug/improve are the mutation operators, the frozen evaluator is the fitness function, waves are generations, and git SHAs identify each state's parentage. It preserves AIDE's optimization loop:

1. Select a base solution with a fixed policy.
2. Ask a coding operator for a new solution using the base and a concise summary of the solution tree.
3. Evaluate the new solution independently with fixed hard gates, measured metrics, constraints, and a deterministic scalar utility.
4. Record the solution, score, and parent edge.
5. Repeat until the candidate budget is exhausted, then return the highest-scoring valid solution.

The policy priority remains:

1. **Draft** until the configured number of initial solutions exists.
2. **Debug** a randomly selected buggy leaf according to the configured debugging probability and maximum debugging depth.
3. **Improve** a valid draft lineage selected by deterministic bounded-score UCB.

Each root-level draft defines one lineage arm. The arm's best valid candidate is
its improvement parent. `lineageExplorationWeight` defaults to `1` and scales
the UCB exploration term; it must be finite and non-negative. The bounded score
leaves more exploration room for a lineage whose best score is farther from the
normalized maximum of one. Set the weight to `0` to retain greedy global-best
selection. Arm and candidate ordinal break ties, so completion order cannot
change a choice. Drafting and probabilistic debugging run before this policy.

If no valid candidate remains available for improvement, return to drafting.

RIME extends AIDE with lineage-bandit selection and parallel fan-out. At each policy decision, dispatch up to 100 independent proposals from the same selected parent and the same operator-specific candidate summary. The configured concurrency and each host's lower operational limit still apply. Require every improvement proposal to be one atomic change. Evaluate the entire batch before selecting the next parent. Never let completion order change state order or tie-breaking. Inject a seeded random source when a run must be exactly reproducible.

Every draft, debug, and improve proposal crosses the fleet boundary. Before allocating workspaces, call `fleet.assign` with `preferences: ["low-cost"]` and require only a non-empty host agent and model identity. The preference is advisory: accept hosted, open-source, local, fast, expensive, or coordinator-class models when the host selects them. Pass the assignment to `fleet.execute` and record the actual execution identity. Record whether assignment and execution matched, but do not fail or change candidate reward when they differ.

Represent each policy decision as a wave. A wave is invisible to the next policy decision until all of its attempts reach a terminal state and its closed-state fence has been recorded. Preserve proposal order when assigning candidate ordinals even when attempts finish out of order.

The candidate budget and concurrency are hard ceilings. Fleet execution and evaluation are injected boundaries. The fleet arranges model execution according to host availability and preferences, while the workspace pool arranges isolated code and data state; the search core must not assume a filesystem, Git implementation, database clone, agent host, model vendor, or deployment platform.

Proposal context is bounded and specific to its operator. Drafting receives the newest concise trajectory candidates and no artifact references. Debugging receives the selected buggy candidate, its ancestors, and only compact relevant evidence references. Improving receives the selected ancestry and sibling outcomes, plus the last four valid scores in closed candidate order; `stalled` is true only when a complete four-score window contains no score above its first value. Context never includes solution bodies or raw artifact contents.

When a workspace pool is present, allocate one owned workspace per attempt. After each closed wave, retain only the best valid candidate and still-debuggable buggy candidates. Promote the final speculative winner, then remove all owned workspaces; on failure, exhaust cleanup before returning the error. The caller owns the baseline workspace.

To persist evidence, pass a `RimeStateStore` with a unique run ID to `RimeSearch` as `stateStore`. The store writes normalized wave, attempt, candidate, evaluation, gate, and artifact state. It deliberately excludes solution contents because code and application artifacts belong in the workspace or artifact store rather than durable search state.
