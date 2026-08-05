# Agent & Contributor Instructions

Instructions for AI agents and humans working in this repository. `CLAUDE.md` is a symlink to this file.

## Project state: PROTOTYPE

This project is pre-release. The rules that follow from that are strict:

- **No fallbacks.** Do not write legacy paths, compatibility shims, or "if the new way fails, try the old way" code. There is no old way. Delete code instead of deprecating it.
- **No versioning machinery.** Do not implement format versions, protocol version negotiation, migration paths, or feature flags for compatibility. Wire formats, on-disk layouts, and APIs may break freely between commits.
- **Upgradeability is a design consideration, not an implementation requirement.** Where the architecture defines an extension point that will one day carry version skew (peer RPC, on-disk extents, Puffin blob types, the successor-takeover protocol), *note the extension point in a comment* — but do not build the upgrade path. Each update does not need to be implemented as upgradeable at this time.

## Sources of truth

1. **docs/architecture/whitepaper.mdx** — the architecture and its reasoning. Read the relevant section before implementing; if an implementation decision contradicts the whitepaper, stop and raise it rather than silently diverging.
2. **GitHub issues** — every unit of work has an issue with Context / Work / Acceptance criteria. The acceptance criteria are the definition of done. Read the whole issue (including addenda after `---` separators) before writing code; cross-issue dependencies are tracked in the project board's "Blocked by" field.

## Worklog discipline (required for all transactions)

Every crate and binary carries a `WORKLOG.md`. **Every change that touches a crate MUST append an entry to that crate's worklog** — this is a required part of every commit/PR, not optional hygiene:

```markdown
- #<issue-number>: <2–3 sentence description of what was done and why,
  in plain language a future contributor can follow.>
```

Entries are append-only, newest last. A PR touching three crates updates three worklogs. Reviews reject PRs with missing worklog entries.

## Test-driven development (required)

Tests are written **first**, and they must fail before the implementation exists:

1. Derive the tests from the issue's acceptance criteria — before writing any implementation code.
2. Run them and confirm they **fail against the current code, for the expected reason**. A test that passes before the implementation exists is testing nothing.
3. Implement until green. The PR description attests the sequence ("tests written first; failed with `<error/assertion>`; pass after implementation").
4. Bug fixes start with a failing reproduction test, always — the fix commit turns it green.

The point: tests written after code tend to **confirm what the code does**; tests written first **execute the desired logic**. Never write a test by reading the implementation and encoding its current behavior.

## Coverage

- CI computes line coverage (`cargo llvm-cov`) on every PR and **fails below the floor** set in `ci.yml`.
- Coverage must never decrease in a PR. When your PR raises overall coverage, **raise the floor** in `ci.yml` to just below the new value (the ratchet is part of the definition of done).
- Long-term target: **≥90% line coverage**, with correctness-critical paths (cache read/write, invalidation ordering, ring routing, protocol surface) at effectively 100%.
- Coverage is a floor-guard, not the goal. Assertion-free tests written to move the percentage are rejected in review — the TDD rules above define what a real test is.

## Code quality

- **Every module has a top-level `//!` comment** stating its responsibility in 1–3 sentences.
- **Every method has a doc comment** (`///`) — public and private alike. Say what it does and any invariant it maintains; skip restating the signature.
- `just lint` must pass: `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings`. The workspace lints deny `unwrap_used`, `todo`, `unimplemented`, `dbg_macro` — write real error handling the first time.
- Every crate keeps integration tests in its `tests/` directory; new functionality lands with tests that map to the issue's acceptance criteria.
- Comments state constraints the code can't express ("never called from the hot path", "budget is a hard ceiling") — not narration of what the next line does.
- Write everything for humans, in plain direct English: code comments, issues, PR descriptions, reviews, and worklog entries. Short declarative sentences, one idea each. State what changed, why, and what is required. No marketing language, no dramatic phrasing, no filler.
- **A config field lands in the same PR as the feature it configures** — no field exists before its feature does.

## Standing invariants (violations are release-blocking, in any PR)

- **The turn-off test:** Verglas off = customer's lakehouse still works, just slower. Nothing may make serving depend on Verglas-only state.
- **Never write to customer tables or buckets autonomously.** Explicit customer-invoked index builds may attach derived Puffin statistics files to the target snapshot; no background operation may publish one without that authorization.
- **Slow is acceptable; wrong is never.** Degrade to backend fills, never to incorrect bytes. No code path may assume a key is locally owned (everything routes through the ring).
- **Budgets are hard ceilings** (DRAM, NVMe, CPU) — especially in colocated mode, where Verglas must be a provably polite tenant.
- **Hot paths do not lock, allocate, or aggregate** — record to tapes/snapshots and do the work in the background.

## GitHub workflow

- Branch per change; PR against `main`; reference the issue (`Closes #NN`). CI (fmt, clippy, build, test) must be green before review.
- Keep PRs scoped to one issue where possible. If a change grows beyond its issue, file the follow-up issue rather than expanding the PR.
- PR descriptions state what changed and how it was verified against the acceptance criteria — reviewers check the criteria, the worklog entries, and the standing invariants above.
- PR-review checklist: no code path may assume a key is locally owned — every read/fill/dedup/invalidation path resolves ownership through the ring (#17).
- Do not merge your own PRs without review unless explicitly told to.
