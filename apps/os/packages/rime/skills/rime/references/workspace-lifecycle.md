# Workspace lifecycle

RIME owns every speculative workspace requested through its workspace pool. The pool supplies three host operations:

- `create`: fork an isolated workspace for one attempt from an optional parent workspace.
- `promote`: copy or merge the selected candidate into the caller's destination.
- `remove`: idempotently destroy one explicitly identified speculative workspace.

The opaque workspace may represent a Git worktree, container filesystem, database branch, lakehouse clone, or a coordinated bundle of them. RIME depends only on its stable identity and passes the host object to the proposal and evaluator.

## Bounded live set

After closing each wave, retain only:

- The highest-scoring valid candidate, because it is the only possible improvement parent.
- Unexpanded buggy candidates still within the configured debugging depth.

Remove non-best valid candidates, expanded buggy candidates, over-depth buggy candidates, and candidates made ineligible by gates or constraints once they cannot be debugged again. Allocate a debugging child's workspace before removing its parent.

The baseline workspace is caller-owned and is never removed by RIME.

## Finalization

If a speculative candidate wins, promote it first and remove its workspace only after promotion succeeds. Then remove every other remaining speculative workspace. If the baseline remains best or no valid candidate exists, remove every speculative workspace without promotion.

If promotion fails, retain only the winning workspace and return its identity on the error so recovery remains possible. Remove all other workspaces. This is the sole normal path that may intentionally retain a speculative workspace.

On proposal, evaluation, state, or cancellation failure, attempt every outstanding removal. Pass RIME's abort signal to workspace creation, proposal, and evaluation adapters so they can stop cooperatively. Do not stop after the first cleanup error. Surface the original failure together with every cleanup failure and never issue a second removal for the same workspace during that RIME process.

## Host adapter requirements

- Make `remove` safe to retry across process restarts even though RIME calls it at most once per process.
- Scope destructive operations to the exact workspace identity. Never remove a repository root, broad directory, or unresolved path.
- Make promotion atomic from the destination's perspective.
- Tag resources with run and attempt identities so a host reconciler can reclaim abandoned resources after process death.
- Use a lease or expiry for crash recovery. In-process cleanup cannot run after a forced process termination.
- Reconcile graph lifecycle evidence against the host's authoritative resource inventory. The graph records evidence; it does not prove that a filesystem or database resource is gone.

For Git, remove the registered worktree path and then prune Git's stale worktree metadata. A future Git adapter owns those commands; the RIME core must not execute shell-specific cleanup.
