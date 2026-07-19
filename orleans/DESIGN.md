# StreamForge — Design Decisions (Orleans implementation)

ADR-style record of the choices that shaped the codebase, with the rejected alternatives and the
known ceilings. Structure lives in [ARCHITECTURE.md](ARCHITECTURE.md); phase-by-phase execution
history in [`../plans/`](../plans/README.md).

## D1 — Pure-C# engine, runtime at the edges
The SQL compiler, both executors, and the dataflow primitives have **zero Orleans dependencies**.
Orleans appears only in Abstractions (serialization attributes) and Host (grains/transport).
*Why*: testability (400+ engine tests run without a cluster), and runtime portability — a second
host (Dapr or anything else) reuses the entire semantic core. This boundary is the single most
load-bearing decision in the repo; do not let runtime types leak inward.

## D2 — Interpreted operators, no codegen
Plans compile to interpreted operator chains, not IL/expression-tree codegen. *Why*: demo-scale
throughput is generator-bound anyway; interpretation keeps diagnostics, determinism tests, and the
M1 op decomposition tractable. *Ceiling*: per-event dispatch overhead; revisit only with evidence.

## D3 — DBSP/epochal dataflow, not full differential (the "no recursion" pact)
Partitioned tables use unitemporal epochs + frontiers (DBSP/Feldera model, same lineage as
TanStack DB's d2ts), **not** Materialize-style multitemporal timestamps. *Why*: multidimensional
time exists to support *iterative/recursive* dataflows; our dialect deliberately has no recursive
CTEs, so epochs suffice and the progress protocol stays simple enough to property-test.
*Consequence*: recursion stays out permanently (bounded traversals unroll into joins; "latest per
chain" is `LATEST BY` + row history). Reversing this decision means replacing the protocol layer —
plan 003 documents the full argument.

## D4 — Z-set weights everywhere in table mode
Every table change is a weighted delta; correctness under *updates* (retraction cascades through
joins, subtractable aggregates, multiset MIN/MAX) is structural, not bolted on. Semi/anti joins are
presence-based (refcounted keys), so duplicates never fan out and the last retraction flips
dependents. This is also why history, SignalR reconciliation, and the partitioned path compose:
they all speak deltas.

## D5 — Frozen public contracts, additive-only evolution
`PublicApi.cs`, existing Abstractions members, and `web/src/api/types.ts` are frozen; changes are
additive (next `[Id]`, nullable/optional fields). *Why*: it let a dozen concurrent subagent tracks
build against stable seams without merge chaos, and it keeps generated clients honest. The same
discipline produced M1's headline property: all pre-existing tests pass **unmodified** through the
operator-decomposition refactor (façades preserve behavior byte-for-byte).

## D6 — Dynamic protobuf with a persisted field-number registry
Typed gRPC surface is generated *at runtime* from entity schemas (descriptor factory + hand-rolled
wire encoder — C# protobuf has no `DynamicMessage`). Field numbers live in the registry
(`Active` + `Reserved`; numbers are never reused, removed fields reserve theirs forever), shared by
reflection, proto downloads, and the streaming payloads — so a client generated today still decodes
streams after schema edits. The typed-client "magic" is deliberately **standard-toolchain**: serve
a self-contained `.proto`, let protoc/Grpc.Tools do the typing. Runtime-emitted assemblies were
rejected (you'd get `dynamic` access — pointless).

## D7 — The delta stream is the event log (history without JournaledGrain)
Row history is a plain state grain consuming the table-delta stream, not Orleans event sourcing.
*Why*: JournaledGrain needs a log-consistency provider our JSON storage doesn't implement, and the
delta stream already *is* an ordered event log. Retention modes (All/LastN/FirstN/MinBy/MaxBy +
window) are a pure, grain-free class — unit-tested exhaustively.

## D8 — Rebuild-from-live over full checkpointing
On restart, persisted snapshots serve reads immediately while operator state rebuilds from live
traffic (`Rebuilding` flag). *Why*: honest scope — exactly-once replay logs are a different
project. Arrangements (M3) add periodic checkpoints because shared state amortizes the cost.
*Ceiling*: a table's aggregates reflect only post-restart traffic until warmed; documented, not
hidden.

## D9 — Parallelism is opt-in, classic path untouched
`Parallelism == 1` runs the original single-grain executor — zero risk to existing tables;
`≥ 2` deploys the stage-grain grid. Joins co-partition on the join key; scalar sides broadcast;
tables whose plans can't partition honestly stay classic. Reads gain a real guarantee
(`frontierEpoch`: all deltas ≤ F, none beyond — enforced by atomic per-epoch application, which
required fixing a genuine pre-existing stop-flush bug rather than claiming atomicity by convention).

## D10 — Grain reentrancy is allowlisted, never default
`RegistryGrain` (and the M4 coordinator callback) use `[MayInterleave]` with an explicit method
allowlist. *Why*: the Registry→Pipeline→Registry start cycle deadlocks without it; but blanket
`[Reentrant]` would let mutations interleave. Hard-won rule: any grain that orchestrates grains
which call back into it needs this treatment — check the allowlist before adding cycles.

## D11 — Streaming honesty rules (the semantic fine print)
Pinned, tested, and documented rather than fudged:
- Pipeline-mode subqueries (membership/scalar) must be **windowed**; their value is a rolling
  snapshot replaced at inner window close.
- `NOT IN` ignores subquery NULLs (documented deviation from three-valued SQL).
- Aggregate JSON legs with `->` (raw node); `->>` is text and silently sums to zero.
- `LATEST BY` keeps one row per key (older-timestamp arrivals ignored; upstream retraction drops
  the key — no multiset fallback history; that's what row history is for).
- Windows-in-windows: inner emissions carry the inner window's **end** timestamp.
- Late events: pipelines drop past watermark (bounded lateness); tables never drop — epoch-stamped
  at arrival.

## D12 — One process, JSON files, zero infrastructure
Localhost clustering, in-memory streams, one-file-per-grain JSON storage. *Why*: the demo must run
with `dotnet run` and nothing else. The Orleans programming model is production-shaped; a real
deployment swaps clustering/stream/storage providers without touching the engine. RBAC is built-in
JWT (no IdP) for the same reason.

## Known ceilings (quick list)
Table joins INNER-only · single-node topology · generator ~1 000 ev/s/source (1 ms timer floor) ·
arrangement partitions each subscribe the full input stream (filter-at-consumer) · no exactly-once
replay · JSON path keys are literals · correlated subqueries beyond equality rejected ·
pipeline names not enforced unique (name-resolution falls back only on unambiguous match).
