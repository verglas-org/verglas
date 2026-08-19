# RIME wave 1: #164 Step 3 (object offload stream)

Base `8edfc90c`. Four candidates, evaluated by the coordinator against
`OBJECTIVE.md` on the four-node Docker cluster. No candidate scored itself.

## Outcome

**No candidate selected.** All three evaluable candidates fail gate G5a for the
same reason, and that reason is a gap in the issue rather than in their work.

## Results

| | A | B `d125f3e7` | C `0508c7d2` | D `e2781729` |
|---|---|---|---|---|
| Produced work | no | yes | yes | yes |
| G1 build | — | not run | pass | pass |
| G6 `propagate*` deleted | — | pass | pass | pass |
| G8 index via consensus | — | pass | pass | pass |
| G5a LIST enumerates keys | — | not measured | **fail** (`KeyCount: 0`) | **fail** (`KeyCount: 0`) |
| G5b GET returns bytes | — | not measured | pass (40/40) | pass (5/5) |
| M1 origin PUTs | — | not measured | **11** | invalid |

Baseline for M1 is 1000.

### C

10 pack objects under `_verglas/packs/default-storage/`, totalling ~4,095,000
bytes against 4,096,000 written. Nothing outstanding. `putcount=11`. Data is
complete and the system reached steady state.

### D

Reported `origin_put_delta=3` while still flushing. Packs kept landing for
20+ minutes: 3 → 8 → 9 → 11 → 12, covering 432 of 1000 objects when observation
stopped. Its drain does not force a flush; it relies on a periodic timer, which
section 4 does not permit. M1 is therefore not measurable for D.

### A

Produced no implementation. Stalled repeatedly.

## The finding

All three implementations make packed logical keys invisible to
`list-objects-v2`. None of them modified `crates/verglas-s3`, which owns the
LIST path, and issue #164 contains no occurrence of "list" or "enumerate".

Section 4 specifies a pack index that resolves reads, and the acceptance
criteria speak only of a "read of an acknowledged object". Nothing states that
enumeration must continue to work once keys live inside a pack. Three
independent approaches — an actor, journal state, and a format-first design —
each concluded the same thing from the same text.

An S3 endpoint whose LIST omits written objects is wrong regardless of what the
issue says, so G5a stays. The issue needs an addendum requiring that LIST
resolve through the pack index, and it belongs in section 4 alongside the
index, not in section 5's read-your-writes.

## Evaluator defects this wave exposed

Recorded because they invalidated results before they were caught.

1. **Fixed drain window rewarded deferred work.** D's `origin_put_delta=3` was
   a moving target. Replaced with polling to quiescence; runs now report
   `quiesced`.
2. **G5 conflated LIST and GET.** Reading back with `s3 cp --recursive` lists
   first, so D's enumeration bug scored as 1000 byte mismatches and read as
   data loss. It was not. Now reported separately.
3. **Shared Docker cache id.** All builds shared `verglas-engine-target`, so
   C compiled against D's `verglas-core` and failed with a field name that
   exists only in D's tree. Namespaced by `CACHE_NS`.
4. **Shared Docker image and compose project name.** C's cluster came up on
   D's binary. Caught before measuring. Namespaced per candidate.
5. **Shared git stash.** D stashed its implementation where every other
   worktree could see it. Workers are now told to commit, never stash.
6. **Two of four worktrees came from `main`, not the wave base.** A and B ran
   against a tree predating the absorption, where `OBJECTIVE.md` does not
   exist. Both were reset and restarted.
7. **Per-object `aws` readback is too slow to use.** 1000 sequential
   invocations take ~80 minutes and dominate the run. Parallelise or sample.

## Gate validity

Three of four code-quality gates were invalid on the base, all from the
Lakekeeper absorption, none attributable to a candidate:

| Gate | Base result |
|---|---|
| G1 `cargo check` | pass — valid |
| G2 clippy | `EXIT=101`, 5 errors, all in `lakekeeper` |
| G3 fmt | fail, 4 files, all in `lakekeeper` — since fixed |
| G4 `cargo test` | `EXIT=101`, 25 failures, all in `lakekeeper` |

The 25 test failures are cloud integration tests requiring live AWS, Azure, and
GCS credentials. Lakekeeper ran them under `cargo nextest` with the `default`
profile in `.config/nextest.toml`, which exists precisely to exclude tests
needing secrets and external services. `cargo test` ignores nextest profiles,
so the absorption wired them into the default test path. That contradicts the
standing policy stated at the top of `ci.yml`.

Only two failures are candidate-attributable: D has one clippy error and four
unformatted files of its own.

## Next

1. Amend #164 to require LIST resolution through the pack index.
2. Fix the base: either `#[ignore]` the 25 credential-dependent tests or move
   `just test` and `ci.yml` to `cargo nextest` with the `default` profile.
3. Run a debug wave on C, which is the strongest lineage: complete data,
   converged, 11 PUTs against a baseline of 1000.
