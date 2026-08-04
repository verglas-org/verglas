# PREFLIGHT — swapping to the worker-runtime daemon

This branch (`feat/fleet-catalog`) removes the source/MV/sink primitives and the
in-daemon memory workflow, and refocuses `verglasd` on the core engine (cache,
S3 serving, tables/catalog, and the local **worker** runtime). The live daemon
serves trading on **admin 8334 / S3 8333** under launchd **`org.verglas.verglas`**.
Do the swap in an off-hours window (market opens **Monday 13:30 UTC**). **The
operator runs the swap — not an agent.** Never touch the live daemon's ports or
launchd job until the swap step.

Installed binaries (all under `~/.cargo/bin`): `verglasd` (launchd daemon),
`verglas` (CLI), `verglas-mcp` (memory MCP — **deleted by this branch**),
`verglas-consolidate` (memory consolidator — **deleted by this branch**).

---

## 0. Build the artifacts

```
cd ~/code/verglas
cargo build --release            # workspace builds green
cargo test --workspace -j 6      # green except one environment-only failure*
```

\* `dev_nodes::sigkill_to_the_parent_still_stops_the_child_daemon` fails on a box
whose temp filesystem has < 20 GB free: `verglas dev` defaults `cache.capacity_bytes`
to 20 GB and the daemon rejects the config when the disk backing `cache.dir` is
smaller. It is unrelated to this change (it never reaches worker/memory code) and
passes on a box with disk headroom. Everything else is green.

Release binaries land at `target/release/{verglasd,verglas}`.

---

## 1. High-port pre-flight against the LIVE catalog (no swap yet)

Prove the new daemon boots, translates the legacy registry, and serves — on
throwaway ports, against the same catalog the live daemon uses — before touching
the launchd job.

```
# A scratch copy of the live config on high ports and a scratch cache dir.
cp ~/.verglas/config.toml /tmp/preflight-config.toml
# edit /tmp/preflight-config.toml:
#   [listen] admin_port = 18334   s3_port  = 18333
#   [cache]  dir = "/tmp/preflight-cache"   (and lower capacity_bytes to fit the disk)

VERGLAS_CONFIG=/tmp/preflight-config.toml \
  ./target/release/verglasd 2>&1 | tee /tmp/preflight.log
```

Watch `/tmp/preflight.log` for, in order:

1. Startup completes and `/admin/healthz` on **18334** turns `ok`:
   `curl -s localhost:18334/admin/healthz`.
2. **The translation line** (only if a legacy registry exists):
   `legacy→workers translation: N source(s) became workers (...); M MV/sink row(s) DROPPED (re-declare as workers): ...`.
   This is the loud drop log — copy the dropped names; those pipelines do NOT
   carry over and must be re-declared as workers by hand.
3. No panics; the S3 surface answers on **18333**.

Verify the workers registry the translation produced:

```
curl -s 'localhost:18334/v1/workers?view=all' | jq '.[].name'
```

Every live **source** should appear as a worker (a cron source keeps its
schedule as a cron trigger; a webhook source a webhook trigger; a hook/manual
source becomes on-demand). MVs and sinks will NOT appear — that is intended.

Stop the pre-flight daemon (`Ctrl-C` / kill the PID you spawned — **only** that
PID; never `pkill -f verglasd`, which would hit the live daemon).

---

## 2. Legacy → workers translation — what to verify

The translation runs automatically on first boot **only when the workers table is
empty**, so it is safe to re-run (idempotent by name). It:

- reads `verglas_sys.sources` and registers one `verglas_sys.workers` row per
  active source (`code` = the source's launch config, `output` = its target,
  `triggers` = cron/webhook/on-demand mirroring the source's trigger);
- reads `verglas_sys.mvs` and `verglas_sys.sinks` and **drops** them — logging
  each name — because the worker model has no automatic transform/egress mapping.

Confirm against the pre-flight `/v1/workers` list that every pipeline you rely on
is present as a worker. Re-declare any dropped MV/sink you still need as a worker
(`POST /v1/workers` with `code`+`triggers`+`output`).

---

## 3. Drop the legacy registry tables (operator step, after the swap is proven)

Nothing in the new daemon reads `verglas_sys.sources|mvs|sinks` after the
translation. Drop them so the registry stops carrying dead rows. This is a
**manual, deliberate** step — do it only once the swapped daemon is confirmed
healthy and the workers you need are present:

```
verglas query "DROP TABLE verglas_sys.sources"
verglas query "DROP TABLE verglas_sys.mvs"
verglas query "DROP TABLE verglas_sys.sinks"
```

(Or the equivalent catalog drop against the REST catalog.) Keep
`verglas_sys.workers` and `verglas_sys.watermarks`. Until you drop them the
translation's empty-table gate keeps it from re-running, so leaving them is
harmless — the drop is hygiene, not correctness.

---

## 4. Binary swap (bootout / swap / bootstrap)

Coordinated swap of `verglasd` + `verglas`, and **removal** of the deleted
memory binaries. Off-hours only.

```
LABEL=org.verglas.verglas
STAMP=$(date +%Y%m%d)

# 1. Stop the live daemon (frees 8333/8334).
launchctl bootout gui/$(id -u)/$LABEL         # or: launchctl unload <plist>

# 2. Back up and swap the daemon + CLI.
cp ~/.cargo/bin/verglasd ~/.cargo/bin/verglasd.bak-$STAMP
cp ~/.cargo/bin/verglas  ~/.cargo/bin/verglas.bak-$STAMP
cp target/release/verglasd ~/.cargo/bin/verglasd
cp target/release/verglas  ~/.cargo/bin/verglas

# 3. Retire the deleted memory binaries (memory moved to the OSS container track).
#    Back them up first, then remove so nothing invokes a stale memory engine.
mv ~/.cargo/bin/verglas-mcp          ~/.cargo/bin/verglas-mcp.removed-$STAMP
mv ~/.cargo/bin/verglas-consolidate  ~/.cargo/bin/verglas-consolidate.removed-$STAMP

# 4. Apply the graceful memory-hook stubs from section 5 (so no failing
#    background jobs spawn on session close / start).

# 5. Restart the daemon.
launchctl bootstrap gui/$(id -u) <plist>      # or: launchctl load <plist>

# 6. Confirm health on the LIVE ports.
curl -s localhost:8334/admin/healthz          # -> ok
curl -s 'localhost:8334/v1/workers?view=all' | jq '.[].name'
```

Roll back by reversing step 2/3 (restore the `.bak-$STAMP` binaries) and
re-bootstrapping.

---

## 5. Memory consumers that break on swap, and the graceful no-op left behind

`verglas-mcp` and `verglas-consolidate` are gone, and the CLI no longer has
`__capture`, `mv run`, `mv pause`, `sink pause`, or `source/mv/sink` verbs. The
operator's Claude Code hooks under `~/.verglas/agent/hooks` reference these.
**Every one of those hooks is already fail-open** (`nohup … & ; exit 0`,
`[ -x "$MCP" ] || exit 0`, `2>/dev/null`), so a swapped machine will **not
hard-error** an agent session — memory simply goes silent. The exact breakages:

| Hook (event) | Invocation that now fails | Effect | Graceful today? |
|---|---|---|---|
| `consolidate.sh` (Stop / SessionEnd / PreCompact) | `verglas __capture …` then `verglas mv run memory_consolidation …` | detached (`nohup … &`), errors written to `spool/consolidate.log`, hook still `exit 0` | yes — never blocks close |
| `session_start.sh` (SessionStart) | `verglas-mcp --session-context …` | `[ -x "$MCP" ]` is false once the binary is removed → empty context block → `exit 0` | yes |
| `prompt_recall.sh` (UserPromptSubmit) | `verglas-mcp --recall …` | `[ -x "$MCP" ] || exit 0` → no recall block | yes |
| `capture.sh` / `capture.py` | (only invoked by the above) | inert | yes |

Because `session_start.sh`/`prompt_recall.sh` guard on `[ -x "$MCP" ]`, removing
`verglas-mcp` (section 4 step 3) is what makes them cleanly no-op instead of
hitting a 404 on the daemon. `consolidate.sh` still spawns two doomed background
commands; to stop that noise, replace the three hooks with these no-op stubs
during the swap (they exit 0 and emit an empty/again-neutral envelope):

`~/.verglas/agent/hooks/consolidate.sh`:
```sh
#!/usr/bin/env bash
# Memory moved out of the daemon into the OSS container track. No-op.
exit 0
```

`~/.verglas/agent/hooks/session_start.sh`:
```sh
#!/usr/bin/env bash
# Memory moved out of the daemon. Emit an empty SessionStart context, exit 0.
echo '{"hookSpecificOutput":{"hookEventName":"SessionStart","additionalContext":""}}'
exit 0
```

`~/.verglas/agent/hooks/prompt_recall.sh`:
```sh
#!/usr/bin/env bash
# Memory moved out of the daemon. No recall injection.
exit 0
```

When the forked OSS memory provider (the container track) is ready, point these
hooks at it instead. The memory **lakehouse tables** (`agent_memory.memories`,
`agent_memory.nodes`, `agent_memory.edges`) are untouched data and survive the
swap — the new provider reads them as-is.
