---
name: sf-verify
description: Full StreamForge verification sweep — builds, complete test suites, and live end-to-end checks. Use before committing non-trivial changes, after merges, or when asked "is everything still green?".
---

# sf-verify — the green bar, end to end

Run ALL of it; partial verification has repeatedly hidden real bugs here (stop-flush snapshot
loss, JsonElement key mismatch, frontier starvation were all caught by the live/cluster layers,
not by builds).

## 1. Build + tests (the baseline: 511 Orleans + ~153 Dapr green)

```bash
cd /Users/yuriyhabarov/work/crates-foundation
~/.dotnet/dotnet build orleans/StreamForge.sln          # 0 errors
~/.dotnet/dotnet test  orleans/StreamForge.sln          # Engine ~393 + Host ~118, all green
~/.dotnet/dotnet build dapr/StreamForge.Dapr.sln        # 0 errors
~/.dotnet/dotnet test  dapr/StreamForge.Dapr.sln        # ~153 tests, all green
cd web && bun run build                                 # tsc + vite clean (bun, never npm)
```

Rules: pre-existing tests must pass **unmodified** — if your change requires editing an existing
test's expectations, that's a behavior change to justify explicitly, not a fix. Host.Tests include
real Orleans TestCluster integration tests (seeds, partitioned tables, arrangements, frontiers) —
they are slower but non-negotiable. `ClusterSmokeTest` flake rule: if it's the *sole* failure, rerun
it in isolation before concluding red.

## 2. Live sweep (isolated instance, fresh temp DataDir so seeds exist)

```bash
~/.dotnet/dotnet run --project orleans/src/StreamForge.Host \
  --Http:Port 9199 --Grpc:Port 9299 --DataDir "$(mktemp -d)" &   # never 5199/5299
```

After ~15 s, as editor (`editor/editor123!`) via curl, confirm at minimum:
- `/api/tables` — all seeded tables `Running`, `error: null` (incl. `leg_exposure` = UNNEST,
  `order_states` = LATEST BY).
- `/api/pipelines` — seeds Running incl. "Hot symbol VWAP (nested)" (CTE + IN semi-join).
- `order_states` rows show real stage strings; `POST …/history/lookup` with a row returns the
  stage trail.
- Set a table's `parallelism` to 4 via PUT → `/rows` gains monotone `frontierEpoch`, `/metrics`
  shows `partitions[]` with stage `kind`s.
- `GET /api/sources/trades/proto` returns valid proto3; `/api/meta/grpc` lists dynamic entities.
- SPA serves at `/` (200) and `/docs`, `/scalar`, `/explorer` respond.

Kill the instance and confirm the ports are free.

## 3. Dapr live sweep (fixed ports — reset for a clean seed first)

Unlike Orleans, the Dapr flavor doesn't have a convenient arbitrary-port isolated-instance mode (it
needs its sidecar + the shared dev Redis) — verify against its normal fixed ports, reset first for a
clean seed:

```bash
bash dapr/tools/reset.sh
bash dapr/tools/run.sh &   # :5399 app, :3599/:4599 sidecar; never touches 5199/5299/9xxx
```

After ~20s (periodic supervisor sweeps, not a one-shot boot gate — allow longer than Orleans' ~15s),
as admin (`admin/admin123!`) via curl, confirm at minimum:
- `/api/tables` — seeded tables `Running` with no `POST .../start` call issued (`positions`,
  `leg_exposure`, `order_states`; `gold_tier_orders`/`hot_symbols` stay `Stopped` as seeded). Note:
  `/api/tables/{id}` resolves by the table's REST **id** (a GUID from the list response), not its
  name.
- `/api/tables/{id}/metrics` — `deltasIn`/`deltasOut` growing tick over tick for a Running table.
- `/api/pipelines` — seeded `Running` pipelines show growing `totalEventsIn`/`totalRowsOut` via
  `/api/pipelines/{id}/metrics`, no explicit start needed.
- `POST /api/tables` with `parallelism: 4` → `409` (Dapr flavor is classic-path only, decision D-F).
- SignalR: subscribing `table:{name}` and `source:{name}` over `/hubs/stream` should show live
  `tableDelta`/`sourceEvent` traffic on **both** flavors. (History: plan 005 W9 found the Orleans
  relay delivering 0 `tableDelta`/`pipelineResult` events on fresh boots — a startup race between
  `StreamBridgeService` and registry seeding, fixed in commit `134b5cc` with regression test
  `StreamBridgeServiceStartupRaceTests`; see `dapr/ARCHITECTURE.md` "Known live bug … — FIXED". If
  you ever see 0 events with growing REST metrics again, suspect a fresh-boot subscription race
  first.)

Kill the instance (`dapr stop --app-id streamforge-dapr` + explicit process kill if the plain
`dotnet run` process outlives the sidecar teardown — a known Dapr CLI quirk) and confirm
5399/3599/4599 are free.

## 4. Verdict discipline

Report actual numbers/output, not "should work". If any layer is red, stop and fix before
committing — never commit a red suite, never mark work complete with a failing gate.
