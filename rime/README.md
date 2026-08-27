# RIME

RIME is a universal agent package for evidence-backed parallel software
engineering. It adapts AIDE's draft, debug, improve, evaluate, and select loop
to coding work, then fans each policy decision out across independent candidate
workspaces.

RIME contains three separable pieces:

- **Candidate orchestration** opens deterministic parallel waves and preserves
  AIDE's selection policy.
- **Evaluator** gates correctness and software quality before ranking declared
  engineering metrics.
- **Workspace management** creates isolated candidates, promotes one winner,
  and cleans every speculative workspace.

## Install

Install the portable skill directly from GitHub with any host supported by the
`skills` installer:

```bash
npx skills add verglas-org/verglas/rime
```

Verglas distributions can install the same package with:

```bash
verglas skill install rime
```

The package includes native host integrations for Pi, Codex, Claude, and Cursor.
Pi loads the extension and skill from `package.json`; Codex uses the packaged
Luna worker; Claude uses the packaged Sonnet worktree agent; Cursor uses the
packaged Composer worker.

The distributed plugin is named `verglas` in every host manifest
(`.claude-plugin`, `.codex-plugin`, `.cursor-plugin`); the RIME skill and its
`rime-worker` agent keep their own names inside it, so Claude Code dispatches
them as `verglas:rime` and `verglas:rime-worker`.

## Library

```js
import { RimeSearch, codingObjective } from "@verglas/rime";

const search = new RimeSearch({
  objective: codingObjective(),
  fleet,
  workspacePool,
  evaluator,
  candidateBudget: 12,
  concurrency: 4,
});

const result = await search.run();
```

Fleet, evaluation, and workspace operations are injected boundaries. RIME does
not assume a model provider, filesystem, Git implementation, database-clone
service, or deployment platform.

## Development

```bash
npm test
npm pack --dry-run
```

The tests cover deterministic search, low-cost model preference, objective
normalization, state checkpoints, packaging, and workspace promotion and
cleanup.

## License

Apache-2.0.
