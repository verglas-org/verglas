# Worklog

- #1: Extracted RIME into an independent public repository while retaining its full implementation history. Added standalone installation, development, CI, package metadata, and repository-boundary documentation without changing the search algorithm or host behavior.

- #112: Added the RIME parallel solution-search core while preserving AIDE's draft, debug, improve, evaluate, and select policy. Packaged its opt-in behavioral guidance as one portable skill for Pi, Codex, and Claude, while leaving workspace and data isolation behind injected execution boundaries for the follow-up.
- #112: Added evidence-backed coding quality gates for task correctness, dead code, fallback paths, architectural fit, and semantic rigor. Valid candidates now receive an AIDE scalar score from normalized task quality, architectural quality, and change surface, while failed gates become debuggable nodes and their evidence is retained in Verglas Graph.
- #112: Replaced candidate-only graph persistence with normalized run, wave, attempt, evaluation, gate, and artifact state. Closed-wave fences now prevent partial batches from changing AIDE selection, while separate candidate and attempt matrices, validated restart checkpoints, and bounded evidence context support parallel fleet execution without making the graph a job queue or memory store.
- #112: Generalized RIME's coding score into a declarative engineering objective with named maximize/minimize metrics, units, fixed normalization scales, weights, and hard numeric constraints. Performance and benchmark objectives now retain raw measurements and utilities in candidate and graph state, while the default coding profile continues to enforce evidence-backed correctness and structural-quality gates.
- #112: Added an injected workspace-pool lifecycle that creates one isolated workspace per attempt, incrementally removes candidates that cannot become AIDE parents, promotes the winner, and exhaustively cleans speculative resources on completion or failure. Workspace identities and lifecycle transitions are retained in state and graph evidence, while Git worktree and Verglas clone implementations remain host adapters for follow-up work.
- #112: Made the Verglas CLI the canonical RIME distributor instead of packaging a second CLI inside RIME. The portable skill now documents the release-owned install and machine-readable CLI boundary, while login remains deliberately deferred.
- #112: Replaced the unassigned proposal callback with a governed fleet boundary that assigns every speculative attempt to a host-defined light model tier and verifies the actual execution identity. RIME records requested, assigned, and executed agent/model evidence, accepts host aliases such as fast, Luna, or Sonnet when classified as light, and rejects heavy or mismatched workers with normal workspace cleanup.
- #112: Replaced the host-specific light/heavy taxonomy with an advisory low-cost fleet preference. RIME now accepts hosted, open-source, local, expensive, or assignment-mismatched execution without changing candidate validity, while retaining assigned and actual model identities as graph and state evidence.
- #112: Added executable host integrations instead of distributing instructions alone: Pi gets a parallel-wave extension, Codex gets a Luna worker, and Claude gets a Sonnet worker with native worktree isolation. RIME now permits waves of up to 100 candidates while retaining host-specific concurrency limits and the existing deterministic evaluation, promotion, and cleanup rules.
- #112: Added deterministic lineage-bandit improvement selection. Root drafts now define UCB arms, with a bounded-score exploration term choosing a lineage before its best valid candidate becomes the improve parent; zero weight retains greedy global-best selection.
- #112: Made proposal context operator-specific and bounded. Drafts receive only the newest concise trajectory, debugs receive the selected ancestry with compact evidence references, and improvements receive nearby outcomes plus a deterministic four-score stall signal; no context includes solution or artifact bodies.
- #112: Updated the portable workflow to require test-first frozen evaluators, lineage-bandit improvement, operator-specific context, and held-out fixed-cost selection for harness optimization. This keeps installed guidance aligned with the implemented search policy and the evaluation discipline that prevents reward hacking.
- #112: Added a recursive Verglas Graph coordinator for outer harness experiments. It stores only adjustment and held-out evidence metadata, requires initialized inner RIME run evidence before scoring, and closes an outer step only after every candidate has been evaluated and selected.
- #112: Tightened recursive outer-evaluation evidence so registering an inner run is not sufficient. An outer candidate now links directly to the successfully initialized RIME run, which then records its held-out task and experiment provenance.
- #112: Required every proposed outer candidate to retain a compact evidence artifact with its evaluation. Steps now evaluate every proposal before their closing fence, retain the incumbent when those recorded evaluations reject mutations, and select a proposal only when its outer evaluation is valid.
- Repository consolidation: Moved RIME back beside the Verglas CLI that owns its distribution, while retaining RIME as an independently testable package.
- #112: Added the missing Cursor host. Cursor now has a `rime-worker` subagent, two-loop hooks (coordinator session/prompt injection plus worker-stop follow-up), and one Verglas graph per project (`rime_<project>`). Hooks fail open and never write attempt scores; the coordinator still owns evaluation and graph evidence.
- #112: Pinned Cursor `rime-worker` to Composer (`composer-2.5-fast`) so workers no longer inherit the coordinator model. A mismatch remains operational evidence, not a correctness failure.
- Renamed the distributed plugin from `rime` to `verglas` in every host manifest (`.claude-plugin`, `.codex-plugin`, `.cursor-plugin`), so Claude Code now dispatches the skill and worker as `verglas:rime` and `verglas:rime-worker`. The skill directory, agent file, and RIME product name are unchanged; only the plugin identity moved. Added a `lakehouse` skill documenting the installed `verglas` CLI's `table`/`graph`/`index`/`query` verbs against the frozen help snapshots, and an `evidence` skill that captures a run with `workers follow`, analyzes it with `query`, and attaches the finding plus its provenance and confidence to a graph node with `graph add-node`/`add-edge`. Wired RIME workers to attach their evaluator run to their candidate's graph node with the `evidence` skill instead of an unsupported claim of success.
- Added `test/cli-reference.test.mjs`, a regression test that recursively scans every markdown file under `skills/` and `agents/` for `verglas <command>` references inside code spans and checks each top-level command against an embedded whitelist of the real CLI 0.1.1 commands (`skill`/`skills` exempted as a forward-looking distribution contract). Verified test-first: it passes against the current clean docs, and fails naming the offending file and command when a bogus `verglas boguscmd` reference is temporarily injected into a skill file and into `agents/rime-worker.md`.

- Run-tree rendering: the RIME skill's workflow gained step 11 — after each
  closed wave the coordinator renders the agent tree (coordinator, waves,
  workers, winner, evidence confidences) from `rime_<project>` graph queries.
  Claude Code renders it as a refreshed artifact page; Verglas Cloud renders
  the same graph through a json-render dashboard. Rendering is reporting only
  and never substitutes for evaluator output.

- Dynamic run-tree dashboard: replaced the per-wave re-render guidance with a
  create-once Cloud dashboard. `references/rime-run.dashboard.json` declares
  live sources only (follow tables for rime_<project> nodes/edges plus a graph
  k-hop view), so graph writes flow to the UI without republishing; the
  artifact-page path remains the local fallback.

- Live debug tape: the evidence skill now opens one create-once dashboard per
  investigation (references/debug-tape.dashboard.json: follow LogStream on the
  captured table, follow Table on a slices table, GraphView k-hop on the claim
  node) before analysis starts, and every narrowing query's result rows are
  appended to the slices table so the dashboard replays the agent cutting
  through the data. Slice rows plus their SQL are the durable proof; the graph
  record stays authoritative where dashboards are unavailable.

- Terminology and surface refresh: skills now describe Verglas as the lakehouse
  runtime, and the CLI-reference whitelist was cut to the real post-#146
  surface. That exposed and fixed stale teaching in two skills: lakehouse's
  `verglas index`/`verglas query`/`table metrics`/`compact` sections were
  replaced by the `vector` family and Iceberg-client SQL guidance, and
  evidence's analyze step and edge shapes now match the shipped CLI.

- #148 wiring: the RIME skill now queries precedents before freezing an
  evaluator and records each promotion as a Decision node linked to the
  winning candidate, rejected candidates, and gate evidence. graph-state.md
  documents the Decision entity and the QueryPrecedents ranking.

- State-pinned evidence: the evidence skill now requires a `state` property on
  every claim — `git rev-parse HEAD` (with a -dirty marker) when the project
  is a git repository, the relevant Iceberg snapshot id for non-git work such
  as document editing. RIME workers pin evaluator evidence to their worktree
  HEAD. Claims at different states are succession, not contradiction, and
  unscoped claims rank below pinned ones.
- Test-slop audit: dropped `doesNotMatch` guards against the pre-rename
  `rime:rime-worker` agent string from the package and lakehouse-skill tests.
  The positive assertions on the current `verglas:rime-worker` name already
  fail if the rename regresses.
