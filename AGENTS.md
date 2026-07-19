# crates-foundation — Agent Instructions

Streaming-SQL platform ("StreamForge") in two runtime flavors: `orleans/` (complete — Microsoft
Orleans 10) and `dapr/` (complete — Dapr, for polyglot processing and runtime comparison). Both
flavors share one runtime-agnostic core (`shared/`): Engine, Contracts, AppCore, Api, and the `web/`
SPA. Execution plans with acceptance criteria: [`plans/`](plans/README.md). Architecture:
[`orleans/ARCHITECTURE.md`](orleans/ARCHITECTURE.md) · [`dapr/ARCHITECTURE.md`](dapr/ARCHITECTURE.md)
· rationale: [`orleans/DESIGN.md`](orleans/DESIGN.md) · runtime comparison + measured latency:
[`orleans/docs/comparison.html`](orleans/docs/comparison.html) (opened directly from the repo — see
its own note on why `/docs` doesn't serve it automatically).

## Environment — non-negotiables

- **dotnet**: `~/.dotnet/dotnet` (SDK 10.0.3xx). It is **NOT on PATH** — always use the full path.
- **JS tooling**: **bun only, never npm** (build: `bun run build` in the web folder).
- **Ports**: Orleans dev server owns `5199` (REST/SignalR/SPA) + `5299` (gRPC h2c) and is often
  running — never bind or kill it. Dapr flavor: app `5399` (REST/SignalR/SPA), gRPC reserved `5499`
  (not yet served — phase 2), sidecar HTTP `3599` / gRPC `4599`; run via `dapr/tools/run.sh` (dapr
  runtime 1.18.x, containers `dapr_redis`/`dapr_placement`/`dapr_scheduler` from `dapr init`), reseed
  via `dapr/tools/reset.sh` + restart, stop via `dapr stop --app-id streamforge-dapr`. Test instances:
  pick 6xxx–9xxx via `--Http:Port … --Grpc:Port … --DataDir <temp>` and kill them when done.
- Seeds apply only to an **empty data dir** (`orleans/src/StreamForge.Host/data/`; delete to
  reseed). Logins: `admin/admin123!`, `editor/editor123!`, `viewer/viewer123!`.
- Git: remote `origin` = private `github.com/cotyar/crates-foundation`, branch `master`. Commit
  messages end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`. Push after stable
  committed waves.

## Build / test / run

```bash
~/.dotnet/dotnet build orleans/StreamForge.sln
~/.dotnet/dotnet test  orleans/StreamForge.sln     # 511 tests — the whole suite must be green
~/.dotnet/dotnet build dapr/StreamForge.Dapr.sln
~/.dotnet/dotnet test  dapr/StreamForge.Dapr.sln   # ~153 tests — the whole suite must be green
cd web && bun run build
~/.dotnet/dotnet run --project orleans/src/StreamForge.Host   # :5199 + :5299
cd dapr && ./tools/run.sh                                      # :5399 (needs `dapr init` done once)
```
Local skills (root `.claude/skills/`, `sf-` prefix) wrap the common workflows: `/sf-run` (both
flavors), `/sf-verify` (both flavors), `/sf-sql`, `/sf-client-gen`, `/sf-config` (catalog
export/import).

**Dapr flavor extras**: `dapr/tools/run.sh` starts the sidecar'd host on 5399 (sidecar 3599/4599);
`dapr/tools/reset.sh` SCANs and deletes this app's Redis keys to reseed (the Dapr-flavor equivalent
of deleting Orleans' `data/`); `dapr stop --app-id streamforge-dapr` stops it. Polyglot processors
(`dapr/processors/{python-enricher,ts-consumer,java-consumer}/`, each with its own `README.md` and
own sidecar/ports) prove the pub/sub contract (`dapr/POLYGLOT.md`) works from outside .NET:
`dapr run --app-id sf-enricher --app-port 8399 --dapr-http-port 3899 --dapr-grpc-port 4899
--resources-path dapr/components -- python3 main.py` (python-enricher), analogous `dapr run`
invocations with their own ports for `ts-consumer` (`bun run main.ts`) and `java-consumer`
(`gradle --no-daemon run`) — see each processor's README for exact ports/env vars.

## Hard rules (learned the expensive way)

1. **Frozen contracts, additive evolution.** `orleans/src/StreamForge.Engine/PublicApi.cs`,
   existing `StreamForge.Abstractions` members, and `web/src/api/types.ts` change additively only
   (next free `[Id(n)]`, optional fields). Never edit existing test expectations to make a
   refactor pass — behavior-preserving refactors keep the old tests green *unmodified*.
2. **The Engine stays pure.** No Orleans/Dapr/ASP.NET types inside `StreamForge.Engine` — it is
   the shared semantic core both runtimes depend on.
3. **Grain reentrancy**: `RegistryGrain` is non-reentrant with a `[MayInterleave]` allowlist.
   Any orchestrator↔worker call cycle deadlocks without allowlisting — check before adding cycles.
4. **Orleans streams serialize payloads**: dictionary subclasses need surrogates (see
   `EventRecordSurrogate`). Memory streams are not a free pass.
5. **Field numbers are forever**: dynamic-protobuf numbering persists in the registry (Active +
   Reserved); both reflection and proto downloads must obtain numbers there, keyed canonically by
   entity **id** even when resolved by name.
6. **SQL fine print**: aggregate JSON with `->` not `->>` (text sums to zero); pipeline-mode
   subqueries must be windowed; `LATEST BY` is table-mode only. Full list: DESIGN.md §D11.
7. **the client branding**: text wordmark only — never reproduce the the client logo graphic. Theme via
   tokens (`sf.theme`, light default); no raw hex outside the sanctioned `--sql-*`/`--chart-*` vars.

## Multi-agent wave discipline (how this repo was built)

- Orchestrator sequences **waves of parallel subagents** (Sonnet, maximum effort) with **strictly
  disjoint file ownership** per concurrent agent; anything shared (csproj, Program.cs, types.ts)
  is pre-assigned to exactly one owner or edited by the orchestrator between waves.
- Engine-exclusive work serializes (one agent owns `StreamForge.Engine/**` at a time); host/web
  tracks run in parallel with it.
- Every agent brief includes verification gates: build + **full** test suite (including the other
  flavor's regression suite once `shared/` exists) + live checks against a self-started instance
  on isolated ports, killed afterward. Commit (and push) between waves, one logical change per
  commit.
- Contracts that two concurrent agents must meet in the middle (stream envelopes, grain
  interfaces, DTO shapes) are pinned verbatim in both prompts, or pre-built by the orchestrator.
