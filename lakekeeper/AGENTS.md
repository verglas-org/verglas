# AGENTS.md

## Meta-rules for this file
- Keep this file concise. For each line, ask: would removing it cause mistakes? If not, cut it.
- Write commands and rules, not prose. Be imperative.
- Don't repeat what's in Cargo.toml, CI configs, or code comments.
- Update this file like code — review changes in PRs.

## Project
Lakekeeper is an Apache Iceberg REST catalog written in Rust. This is the Verglas fork of it.

It lives in the `verglas` repository under `lakekeeper/`, as a nested cargo workspace with its own lockfile. It previously had its own repository (`verglas-org/verglas-lakekeeper`), which is now historical — do not push there. Read the "Repository layout: two cargo workspaces" section of the root `AGENTS.md` before running any cargo command: this tree does not build from the repository root, and `[patch.crates-io]` at the root does not reach it.

Canonical open-source upstream: https://github.com/lakekeeper/lakekeeper.

Sanitized public mirror: https://github.com/verglas-org/lakekeeper. Never push Verglas product changes there.

Upstream syncs are now a merge into a subdirectory of another repository rather than a same-root merge, so the fork's `weekly-upstream-sync.yml` no longer applies as written. Until it is replaced, pull upstream changes by hand and confirm the sanitized-mirror boundary still holds.

Keep Verglas-specific cache, authorization, image, and migration changes here. Keep generally useful Lakekeeper fixes suitable for the canonical upstream separate.

## Repository Structure

- `crates/lakekeeper`: catalog API, services, authorization traits, and domain logic.
- `crates/lakekeeper-bin`: server binary, configuration, Verglas cache delivery, and `serve-craft`.
- `crates/lakekeeper-storage-postgres`: PostgreSQL persistence, migrations, and transactional outbox.
- `crates/lakekeeper-storage-verglas`: CRaft-backed hosted Iceberg catalog storage (`serve-craft`).
- `crates/authz-verglas`: Cloudflare JWKS verifier for hosted catalog credentials.
- `crates/authz-openfga`, `authz/`: upstream authorization implementations and policy models.
- `docs/`, `site/`, `openapi/`: product documentation, documentation site, and generated API contract.
- `tests/`, `examples/`, `docker-compose/`: integration coverage and runnable deployments.
- `.github/workflows/`: **inert.** GitHub reads workflows only from the repository root, so nothing here runs. Live CI for this tree is `../.github/workflows/lakekeeper.yml`, which ports a subset of these gates; the rest are kept here as the reference for what still needs porting (#137).

## Build & Test
Uses [just](https://github.com/casey/just) as task runner. See `justfile` for all available recipes.

Key commands:
- Build: `cargo build`
- Test all: `just test` (includes doc tests)
- Unit tests only: `just unit-test`
- Test one: `cargo test -p <crate> <test_name>`
- Lint: `just check` (runs clippy with multiple feature combinations, format check, cargo-sort)
- Format: `just fix-format` (requires `cargo +nightly fmt` and `cargo sort`)
- Auto-fix: `just fix`

Clippy runs with multiple feature flag combinations — don't just run `cargo clippy --all-features`. Use `just check-clippy`.

## Workspace Crates
| Crate | Path | Purpose |
|-------|------|---------|
| lakekeeper | crates/lakekeeper | Core catalog logic |
| lakekeeper-bin | crates/lakekeeper-bin | Server binary |
| lakekeeper-io | crates/io | Storage I/O (S3, GCS, Azure, etc.) |
| iceberg-ext | crates/iceberg-ext | Iceberg format extensions |
| lakekeeper-authz-openfga | crates/authz-openfga | OpenFGA authorization |
| catalog-error-macros | crates/catalog-error-macros | Error derive macros |

## Authz
- OpenFGA model: `authz/openfga/` — validate with `just test-openfga`, update JSON with `just update-openfga`
- OPA policies: `authz/opa-bridge/` — check with `just check-opa` (requires `opa` and `regal` CLIs)

## Code Style
- Follow existing patterns in adjacent files.
- Use `thiserror` for error types, `tracing` for logging.
- Use `typed-builder` for struct construction.
- Use workspace dependencies (`{ workspace = true }`) — don't add versions directly.
- All crate versions use `version.workspace = true`.
- Minimize new dependencies — justify additions.
- Docs prose (`docs/docs/*.md`): one line per paragraph — no hard line wrapping. Rely on soft-wrap.

## Architecture
- Before adding new code, check if existing crates already solve the problem. Reuse over reinvention.
- Challenge duplication — if similar logic exists elsewhere, refactor to share it.
- New features should extend existing traits/interfaces where possible rather than introducing parallel abstractions.
- Cold path (management/admin routes): bypass per-process in-memory caches; read authoritative data from the DB.
- Hot authz path: may tolerate cache lag.
- After any write: invalidate the local replica's in-memory cache immediately.
- Never rely on per-process caches for cross-replica correctness — caches have no cross-replica invalidation.

## Rules
- Never skip or disable tests.
- Do not modify generated or vendored files.
- Release versioning is managed by release-please (`release-please/`).
- Write a clear PR description of the user-visible change; optionally add a `## Release notes` section. The docs-site Release Notes page (`site/docs/about/release-notes.md`) is summarised from PR descriptions at release; `CHANGELOG.md` (release-please) stays headlines-only. See `.github/RELEASING.md`.
- Never acquire a nested database connection. If a transaction is active, all subsequent queries must use that transaction — do not check out another connection from the read or write pool. Nested connections cause pool exhaustion and deadlocks.
- To return updated state after a write, read it back **in the same transaction** — a follow-up query may hit a lagging read replica and miss the write.
