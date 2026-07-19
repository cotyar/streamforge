# crates-foundation — Agent Instructions

Streaming-SQL platform ("StreamForge") in two runtime flavors: `orleans/` (complete — Microsoft
Orleans 10) and `dapr/` (in progress — Dapr, for polyglot processing). Runtime-agnostic core is
being extracted to `shared/`. Execution plans with acceptance criteria: [`plans/`](plans/README.md).
Architecture: [`orleans/ARCHITECTURE.md`](orleans/ARCHITECTURE.md) · rationale:
[`orleans/DESIGN.md`](orleans/DESIGN.md).

## Environment — non-negotiables

- **dotnet**: `~/.dotnet/dotnet` (SDK 10.0.3xx). It is **NOT on PATH** — always use the full path.
- **JS tooling**: **bun only, never npm** (build: `bun run build` in the web folder).
- **Ports**: Orleans dev server owns `5199` (REST/SignalR/SPA) + `5299` (gRPC h2c) and is often
  running — never bind or kill it. Dapr flavor: `5399`/`5499`. Test instances: pick 6xxx–9xxx via
  `--Http:Port … --Grpc:Port … --DataDir <temp>` and kill them when done.
- Seeds apply only to an **empty data dir** (`orleans/src/StreamForge.Host/data/`; delete to
  reseed). Logins: `admin/admin123!`, `editor/editor123!`, `viewer/viewer123!`.
- Git: remote `origin` = private `github.com/cotyar/crates-foundation`, branch `master`. Commit
  messages end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`. Push after stable
  committed waves.

## Build / test / run

```bash
~/.dotnet/dotnet build orleans/StreamForge.sln
~/.dotnet/dotnet test  orleans/StreamForge.sln     # 511 tests — the whole suite must be green
cd orleans/web && bun run build
~/.dotnet/dotnet run --project orleans/src/StreamForge.Host
```
Local skills (root `.claude/skills/`, `sf-` prefix) wrap the common workflows: `/sf-run`,
`/sf-verify`, `/sf-sql`, `/sf-client-gen`.

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
