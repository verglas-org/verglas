# Agent & Contributor Instructions

Instructions for AI agents and humans working in this repository. `CLAUDE.md` is a symlink to this file.

## Project state: PROTOTYPE

This project is pre-release. The rules that follow from that are strict:

- **No fallbacks.** Do not write legacy paths, compatibility shims, or "if the new way fails, try the old way" code. There is no old way. Delete code instead of deprecating it.
- **No versioning machinery.** Do not implement format versions, protocol version negotiation, migration paths, or feature flags for compatibility. Wire formats, on-disk layouts, and APIs may break freely between commits.
- **Upgradeability is a design consideration, not an implementation requirement.** Where the architecture defines an extension point that will one day carry version skew (component ABI, on-disk cache entries, or host-capability requests), *note the extension point in a comment* — but do not build the upgrade path. Each update does not need to be implemented as upgradeable at this time.

## Repository layout

One cargo workspace covers the whole repository. `crates/` holds the runtime
infrastructure and narrow host capabilities. `system/` holds the prebuilt
Worker/Durable Object products, including the Turso-backed Catalog. The
TypeScript SDK under `sdks/typescript` is a separate package.

`cargo build --workspace` builds the Rust workspace. `just build`, `just test`,
and `just lint` are the entry points.

Things that are easy to get wrong:

- **The Catalog is a Worker/Durable Object product.** Its durable product
  state is held by Turso, and its privileged Iceberg commit capability is
  host-mediated by `verglas-runtime`. It has no separate catalog service or
  consensus layer in this repository.
- **`.cargo/config.toml` sets `--cfg tracing_unstable` for the whole
  workspace.** It gates `tracing`/`tracing-subscriber`'s `valuable` feature.
  Rustflags set in the environment *replace* that table rather than merging, so
  exporting `RUSTFLAGS` breaks the build. `tokio_unstable` was deliberately
  removed; do not reintroduce it.
- **One `iceberg`, and it is forked.** `[patch.crates-io]` redirects `iceberg`
  and its three sibling crates to `verglas-org/verglas-iceberg`, whose base is
  the Catalog fork plus a public `TableCommit::from_parts`. All four are
  pinned with `=` requirements: a caret requirement lets the higher crates.io
  0.10.1 outrank the fork's 0.10.0 and the patch silently stops applying.
  Unifying this is what allowed one workspace at all.

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
- Long-term target: **≥90% line coverage**, with correctness-critical paths (versioned cache fills, Turso commit gating, outbox recovery, and protocol surfaces) at effectively 100%. Measured all-feature baseline: 77.20% (2026-08-25); the floor in `ci.yml` sits just below the measured value and only moves up.
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

- **A managed binding is authoritative; a customer binding is not.** Verglas owns the object layout of a bucket it manages, and every read of that bucket routes through Verglas. A customer binding keeps the customer's own layout, and nothing may make serving from it depend on Verglas-only state.
- **Runtime shutdown is fenced.** Celld stops event admission, waits for the embedded Turso WAL checkpoint and Stream outbox fences, closes the event endpoint, and only then stops the child. Foyer contents may be discarded at any time because they are not durable state.
- **Never write to customer tables or buckets autonomously.** Every publication is an explicit Sink/Catalog operation authorized by the caller.
- **Slow is acceptable; wrong is never.** A local cache miss or invalid entry refills from the configured origin. Cached blocks are served only for the exact storage binding, object version, geometry, and range they name.
- **Budgets are hard ceilings** (DRAM, NVMe, CPU) — especially in colocated mode, where Verglas must be a provably polite tenant.
- **Hot paths do not lock, allocate, or aggregate** — record to tapes/snapshots and do the work in the background.

## GitHub workflow

- Branch per change; PR against `main`; reference the issue (`Closes #NN`). CI (fmt, clippy, build, test) must be green before review.
- Keep PRs scoped to one issue where possible. If a change grows beyond its issue, file the follow-up issue rather than expanding the PR.
- PR descriptions state what changed and how it was verified against the acceptance criteria — reviewers check the criteria, the worklog entries, and the standing invariants above.
- PR-review checklist: every cache read and fill is scoped to the declared storage binding and exact object version; every mutation crosses its Turso, Sink, or Catalog durability boundary before acknowledgement.
- Do not merge your own PRs without review unless explicitly told to.

## Cursor remote VM specific instructions

Durable, non-obvious notes for agents in the Cursor remote / dev VM. The
environment is defined by `.cursor/environment.json` (repo-file managed). There
is no install or start automation: build with the standard `cargo`/`just`
commands and configure the runtime through the Worker/Durable Object host.

Standard commands live in the `justfile` and `README.md`; use them
(`just build`/`just test`/`just lint`, or the underlying `cargo` commands). The
pinned toolchain (`1.96.1`, from `rust-toolchain.toml`) installs automatically on
the first `cargo` call.

Rust workspace facts:

- `cargo build --workspace`, `cargo clippy --workspace --all-targets`, and
  `cargo test --workspace` each take roughly ~3 min from cold because clippy and
  test recompile with their own drivers. `cargo fmt --all --check` is fast.
- CI runs one workspace-wide pipeline (`cargo fmt --check`, `cargo clippy
  --workspace --all-targets -- -D warnings`, `cargo test --workspace`) plus the
  coverage floor; run single crates locally only to iterate faster.
- Prefer per-crate `cargo test -p <pkg>` over `cargo test --workspace` on this VM.
  The workspace run serializes crates but runs each crate's tests multi-threaded;
  on 4 cores that can oversubscribe CPU and starve Tokio or Foyer background
  work. Run cache and runtime tests independently when investigating a timeout.
- The default `cargo test` path uses in-process mocks and needs no external
  services (see the standing test policy at the top of `.github/workflows/ci.yml`).
  Anything needing a real service is behind `#[ignore]`.

Runtime notes:

- Worker and Durable Object components run through the Wasmtime host and use
  Turso for Durable Object state.
- Foyer is the only runtime cache tier. It fills from the configured origin and
  all local cache contents are disposable.
- Iceberg Sink commits use the runtime's narrow host capability. Tenant
  components do not receive raw object-store or catalog credentials.
- Product E2E tests and component tests are self-contained; hosted provisioning
  and public management APIs live outside this repository.
