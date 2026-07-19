---
name: sf-verify
description: Full StreamForge verification sweep — builds, complete test suites, and live end-to-end checks. Use before committing non-trivial changes, after merges, or when asked "is everything still green?".
---

# sf-verify — the green bar, end to end

Run ALL of it; partial verification has repeatedly hidden real bugs here (stop-flush snapshot
loss, JsonElement key mismatch, frontier starvation were all caught by the live/cluster layers,
not by builds).

## 1. Build + tests (the baseline: 511 green)

```bash
cd /Users/yuriyhabarov/work/crates-foundation
~/.dotnet/dotnet build orleans/StreamForge.sln          # 0 errors
~/.dotnet/dotnet test  orleans/StreamForge.sln          # Engine ~393 + Host ~118, all green
cd web && bun run build                                 # tsc + vite clean (bun, never npm)
```

Rules: pre-existing tests must pass **unmodified** — if your change requires editing an existing
test's expectations, that's a behavior change to justify explicitly, not a fix. Host.Tests include
real Orleans TestCluster integration tests (seeds, partitioned tables, arrangements, frontiers) —
they are slower but non-negotiable.

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

## 3. Verdict discipline

Report actual numbers/output, not "should work". If any layer is red, stop and fix before
committing — never commit a red suite, never mark work complete with a failing gate.
