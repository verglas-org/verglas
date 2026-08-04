---
name: verglas
description: >-
  Durable, cross-session memory and Verglas infrastructure for the agent, served
  over the tenant's MCP. Use to recall what you already know at the start of work
  or when a question touches a past decision/fact/preference ("what did we decide
  about X", "where did this number come from", "what changed since last time"), to
  remember a durable fact worth keeping ("remember that ..."), and to reach Verglas
  infra: query the lakehouse (SQL), list tables, list/run worker deployments,
  list/deploy containers, and read usage. Do NOT use for editing source code,
  running the app, transient scratch math, or secrets/credentials.
---

# Verglas: durable memory + platform, over MCP

Verglas gives the agent two things through the tenant's MCP endpoints:

1. **Durable memory** — a cognee knowledge-graph + vector store over the tenant's
   own lakehouse. Three tools: `recall`, `remember`, `session_context`.
2. **The platform itself** — the first-party Verglas MCP: `query_sql`,
   `list_tables`, `list_workers`, `run_worker`, `list_containers`,
   `deploy_container`, `usage_summary`.

Both live behind the tenant's MCP portal (`https://{slug}.verglas.dev/mcp`), which
aggregates them once you authenticate to it. The memory tools are also reached
directly by the installed lifecycle hooks (see below).

## Memory: recall / remember / session_context

- **At the start of work**, load prior context. The `session_start` hook injects a
  bounded `session_context` block automatically; you can also call it directly.
- **When a prompt touches something you might already know**, recall it. The
  `prompt_recall` hook calls `recall` for you each turn; call it directly for a
  targeted lookup (`recall(query, k)`). `recall` returns scored, structured
  results with provenance — the retrieval mode meant for grounding a turn.
- **When the user states a durable fact, decision, or preference** ("we use
  us-west-2", "always report timings in ms"), `remember(content, kind)` it. Kinds:
  observation | fact | instruction | outcome | reflection.

The `consolidate` hook posts the session's content via `remember` at session
close, so what you learned survives into the next session. All three hooks
**fail open** — if memory is unreachable they inject nothing and never block you.

Do NOT store secrets or credentials in memory.

## Platform: the first-party Verglas MCP tools

- `query_sql(sql)` — run a read query against the lakehouse.
- `list_tables()` — the Iceberg namespaces + tables.
- `list_workers()` / `run_worker(id)` — worker deployments and run-now.
- `list_containers()` / `deploy_container(catalog_id)` — container inventory + deploy.
- `usage_summary()` — usage vs plan caps.

## Installing / re-installing

`verglas skills install` (re-run any time — idempotent) writes these hooks and
this skill, and refreshes the memory endpoint + bearer from `verglas login`. Run
`verglas login` first if you have not.
