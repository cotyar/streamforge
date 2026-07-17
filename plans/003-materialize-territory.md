# 003 — Materialize Territory: Partitioned Differential Dataflow on Orleans

Status: **PROPOSED — design approved in principle** ("blocking on the whole table is a bad
option; solve it once properly"). Supersedes the interim one-level sharding idea (former task
"Table execution granularity") — do not build that; build this.

## Problem

Every materialized table today is ONE `TableGrain`: it subscribes to all inputs and runs the
whole compiled plan single-threaded (grain turn concurrency). A hot table serializes all delta
processing; a slow stage (fat join, wide GROUP BY) blocks the entire table, and reads contend
with writes on the same activation. We want parallel, incremental, non-blocking view
maintenance — what Materialize/differential-dataflow and DBSP/Feldera do — mapped onto
virtual actors.

## Positioning: differential (Materialize) vs DBSP (Feldera, TanStack DB)

Two proven implementation families:

- **Materialize** = timely + differential dataflow: multidimensional timestamps (multitemporal
  frontiers) supporting *iterative* dataflows (recursive queries), arrangements shared across
  dataflows, Naiad progress protocol. Maximum power, maximum protocol complexity.
- **DBSP** (Feldera; TanStack DB's `d2ts` is the same lineage in TypeScript) = unitemporal
  epochs: the input is a stream of Z-set deltas batched into totally-ordered epochs; every
  operator is a circuit node consuming/emitting per-epoch deltas. No multidimensional time.
  The DBSP theorem: any query built from linear + bilinear operators (all of SQL-without-
  recursion) incrementalizes mechanically.

**Decision: DBSP/epochal.** Our dialect has no recursion/iteration — multitemporal timestamps
buy nothing here. Our engine is *already* DBSP-lite (Z-set weights, subtractable aggregates,
bilinear joins); this plan adds the two things we skipped: **partitioned parallel execution**
and **epoch-based progress tracking**, plus differential's best idea, **shared arrangements**.
This is exactly the simplification Feldera and TanStack DB validate in production.

## Target architecture

```
                       ┌─ epoch markers ─────────────────────────────┐
 sources ─► IngestGrain(source, p)  ─┐                               ▼
                                     ├─► OperatorGrain(table, stage, p) ─► … ─► output stage
 table deltas ─► (upstream tables) ──┘        │  ▲                              │
                                              ▼  │ exchange (hash by key)       ▼
                                     ArrangementGrain(input, keySpec, p)   TableReadGrain(table)
                                                                            rows/search/metrics
```

- **Epochs**: ingestion stamps every delta batch with a monotone epoch (advance every 250 ms
  tick OR 1 000 events, whichever first, per source). An epoch marker flows through the graph;
  operator frontier = min over upstream frontiers. EMIT-FINAL/window close, read consistency,
  and flush points all key off frontier advancement — this replaces the current wall-clock
  watermark inside tables (pipelines keep their existing watermarking; this plan is
  tables-only).
- **Stages**: the table planner already builds a stage list (ingest → join → filter/project →
  reduce). Each stage becomes `P` operator instances (P = per-table parallelism config,
  default 4). Between stages, deltas are **exchanged**: routed by hash of the stage's key
  (join key into joins, group key into reduces, passthrough stays local). Each partition
  processes independently → no whole-table blocking.
- **Arrangements**: indexed Z-set state (a join input arranged by key; a reduce's per-group
  state) lives in `ArrangementGrain((inputName, keySpec), partition)` with
  `SnapshotAsync(epoch)` + `SubscribeAsync(fromEpoch)`. Multiple tables joining the same
  source on the same key **share one arrangement** (refcounted attach/detach, GC at zero).
  This is the Materialize insight that makes N views over one source cheap.
- **Reads**: `TableReadGrain` (per table) consumes the table's output delta stream, maintains
  the consolidated snapshot + search index (exactly what TableGrain does today for reads), and
  answers rows/search/metrics **at a consistent frontier** (state = sum of deltas ≤ F where
  F = min over output partitions' frontiers). REST/gRPC/SignalR surfaces unchanged.
- **Recovery**: unchanged philosophy — snapshot served immediately from persisted ReadGrain
  state, operator state rebuilt from live traffic (`Rebuilding` flag), now in parallel per
  partition. Checkpoint arrangement state every K epochs (existing JSON storage) to shorten
  rebuilds. No exactly-once replay log (explicit non-goal; demo-honest).

### Protocol details (the parts that bite)

- `DeltaBatch { edgeId, fromPartition, epoch, List<TableDelta> }` — ALL cross-grain movement
  is batched per (edge, epoch); target message count per tick ≈ stages × P², not per-row.
- Operator loop: buffer inputs per epoch → on frontier advance to E: process all batches ≤ E
  in deterministic order, emit output batches ≤ E, emit own frontier E. Deterministic =
  replayable = debuggable.
- Frontier tracker is a pure class (per-upstream high-water marks, min-combine) — property-
  tested to death; every historical dataflow bug is a frontier bug.
- Exchange spec is computed at plan time: `(stageId, keyExprs)`; key hash uses the engine's
  existing `TableKeyEncoding` canonical bytes.
- Skew: a hot key serializes its partition only (documented ceiling; per-key splitting of
  reduces is a known DBSP refinement, out of scope).

## Phases

### M0 — Epoch/frontier/exchange primitives (Engine, additive, ~2–3 days) — **can start now**
Pure, Orleans-free, no changes to existing executors: `Epoch`, `DeltaBatch`,
`FrontierTracker`, `ExchangeSpec` + hash router, per-epoch reorder/flush buffer.
Property-style tests (out-of-order arrival, duplicate markers, multi-upstream mins, empty
epochs). Acceptance: primitives library + tests green; zero behavior change elsewhere.

### M1 — Operator decomposition (Engine, ~1 week, exclusive Engine ownership)
Split `TableExecutor`'s monolithic evaluation into per-stage operator objects
(`IngestOp/JoinOp/FilterProjectOp/ReduceOp/UnnestOp-ready`) each with (a) explicit
serializable state, (b) `OnBatch(epoch, deltas) → outputs`, (c) `OnFrontier(epoch)`.
Single-partition composition of the new operators MUST reproduce the current executor
bit-for-bit — keep `TableExecutor` as a façade over the op-chain so all 200+ existing tests
run unchanged against the new internals. Acceptance: all existing tests green through the
façade + new per-op unit tests + a deterministic-replay test (same batches ⇒ same outputs).

### M2 — Partitioned execution on Orleans (Host, ~1–1.5 weeks)
`IngestGrain`, `OperatorGrain(table:stage:p)`, `TableReadGrain`; exchange via direct
grain-to-grain batched calls (streams reserved for source ingestion + final output delta
stream, which keeps SignalR/history/gRPC surfaces untouched); registry orchestration
(start/stop/restart = deploy/teardown of the grain graph); per-table `Parallelism` config
(default 4) replacing nothing (TableGrain retired behind ITableGrain-compatible ReadGrain, or
TableGrain becomes the ReadGrain — decide in-implementation, keep the grain interface stable).
Acceptance: seeded tables byte-equivalent outputs vs main; kill-and-rebuild works; a
deliberately hot table shows N partitions progressing independently (per-partition metrics).

### M3 — Shared arrangements (~1 week)
`ArrangementGrain` + planner reuse (same input+keySpec ⇒ attach, not build), refcount/GC,
checkpoint every K epochs. Acceptance: two tables joining `trades` on `symbol` show ONE
arrangement set in metrics; deleting one table keeps the other correct; restart rebuilds.

### M4 — Consistent reads + frontier-driven EMIT (~3–4 days)
ReadGrain snapshot-at-frontier; `/rows` gains `frontierEpoch` in the response; EMIT FINAL
table variants close on frontier not wall clock; late-input policy documented (epoch of
arrival — no retro-dating; unchanged from today's honesty).

### M5 — Observability UI (~3–4 days, parallel with M3/M4)
Table detail gains a dataflow panel: stage graph with per-partition frontier lag, batch
rates, arrangement sizes, rebuild progress; parallelism control (Editor) with restart
semantics. This delivers the original "granularity control" ask properly.

## Risks & mitigations

- **Frontier bugs** → pure trackers + property tests (M0), deterministic replay (M1), an
  invariant assert in every operator (frontier never regresses) that fails tests loudly.
- **Orleans messaging overhead** → per-epoch batching only; measure in M2 with a 50k ev/s
  soak before proceeding to M3; abort criterion: if p99 end-to-end delta latency at P=4
  exceeds the current single-grain baseline ×1.5 on seeds, stop and profile before M3.
- **Grain explosion** (tables × stages × P + arrangements) → fine at demo scale (≤ a few
  hundred activations); document the ceiling.
- **Migration risk** → M1's façade keeps the whole existing test suite as a regression net;
  M2 keeps TableGrain interface + all public surfaces (REST/gRPC/SignalR/history) unchanged.

## Effort & sequencing

M0 (now, parallel-safe) → M1 (exclusive Engine) → M2 → {M3, M5} parallel → M4. Roughly 4–5
agent-weeks; each phase lands as its own commit(s) with the suite green, so the plan can pause
after any phase and the platform still works.
