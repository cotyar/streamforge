# Plan 008 — REST discoverability, table joins, set operations, client ingress, lineage & plan view

**Status: IN PROGRESS** (P done; W1–W5 pending)

Depends on: 005 (shared core + both flavors), 006 (connectors), 007 (containers, admin, AI chat).

## Problem

The repository went public (`github.com/cotyar/streamsforge`) and five gaps surfaced:

1. **The console's API Explorer documents only gRPC.** Scalar/OpenAPI are already mounted at
   `/scalar` and `/openapi/v1.json`, but nothing in the SPA tells a visitor how to read a table, a
   pipeline's results, or a source over plain REST.
2. **Table mode allows INNER equi-joins only** (`Validator.cs:249-256` rejects LEFT/RIGHT/FULL and
   CROSS), so materialized tables cannot be merged the way users expect.
3. **Nothing can push data in.** Every transport is outbound; connectors are *pull*-based. External
   clients have no door.
4. **No set operations at all.** `UNION`/`UNION ALL` do not exist in the engine, so declarative
   fan-in is impossible — the workaround is several pipelines writing into one table.
5. **The dataflow is invisible.** No lineage graph, and the compiled plan the engine builds on every
   validate is thrown away instead of shown.

## Key decisions

- **D-A — Outer joins get their own op.** `TableOuterJoinOp` composes `TableJoinOp`'s bilinear
  product loop with `TableSemiAntiOp`'s presence-flip machinery, plus `WorkingRow.NullSide` padding.
  `TableJoinOp` keeps a **zero-line diff** — it is on every existing join and an empty diff is the
  only cheap proof of no regression.
- **D-B — Presence is `weight != 0`, not `weight > 0`.** Under an out-of-order
  retraction-before-assertion the `!= 0` rule self-heals; `> 0` leaves both a live product and a live
  pad.
- **D-C — Null-key handling is asymmetric.** A null-key *left* row pads immediately and is never
  indexed; a null-key *right* row emits nothing and is never indexed. Indexing the latter would make
  its bucket "present" and silently suppress pads for every left row under that key.
- **D-D — Composite equi-keys and residual predicates are in scope** (user decision).
  `ExtractEquiKey` returns a list of pairs; residuals are handled with per-(key, left-row) surviving
  match counts, at O(|L_k|·|R_k|) predicate evaluations per right delta.
- **D-E — CROSS needs no new op**: the planner synthesizes a constant key on both sides (the trick
  `BuildSemiAntiJoin` already uses), which collapses the bilinear join into the cartesian product.
  CROSS at `Parallelism > 1` throws "Use Parallelism = 1 for this table" rather than silently
  serializing onto one partition.
- **D-F — `UNION ALL` in both modes, `UNION` (distinct) in table mode only.** Pipeline mode has no
  Z-set weights, so weight-clamping is meaningless there and unbounded distinct is unbounded state —
  same reasoning that makes `LATEST BY` table-only (DESIGN §D11).
- **D-G — `GROUP BY ALL` expands in the parser**, using `Validator.ContainsAggregate` as the
  predicate. Zero executor and zero plan-DTO impact.
- **D-H — Ingress is admission control, not backpressure.** Neither `OnNextAsync` nor
  `PublishEventAsync` propagates consumer lag, so the only honest guarantee is a bounded buffer we
  own and measure. Success is `202 Accepted` ("buffered"), never `200`, and the status DTO surfaces
  `downstreamDroppedTotal` so the second loss point is visible.
- **D-I — The ingress buffer is a host-process singleton, not a grain/actor.** A grain inbox is
  unbounded and unobservable, which would make the policy choice decorative.
- **D-J — The plan view is a recompile-per-request read**, following
  `OrleansArrangementMetaFacade`'s precedent. Compiling is pure and never throws on bad SQL.

## Waves

| Wave | Scope | Size |
|---|---|---|
| **W1** | REST card in the API Explorer (`web/src/pages/ApiExplorerPage.tsx` only) | S |
| **W2** | Table OUTER (LEFT/RIGHT/FULL) + CROSS joins, composite keys, residuals | L |
| **W3** | `UNION` / `UNION ALL` + `GROUP BY ALL` | M–L |
| **W4** | Client ingress: policies, REST + bidi gRPC, status counters | L |
| **W5** | Lineage graph (React Flow) + per-node execution-plan view | M |

W2 sub-sequences as **2a** (CROSS path ‖ the unwired op) → **2b** (single owner: key-list refactor +
gate removal + executor/dataflow wiring, atomic because a half-wired gate yields LEFT joins that
silently produce INNER results) → **2c** (partitioned/arrangement/replay tests ‖ docs).

Full design detail, per-wave file ownership and the semantics traces live in the session plan file;
this document records the decisions and the acceptance criteria.

## Acceptance criteria

- Both suites green after every wave: `~/.dotnet/dotnet test orleans/StreamsForge.sln` and
  `dapr/StreamsForge.Dapr.sln`. Baseline at plan time: **897 + 181**.
- Test files are never edited; new files only. **One pre-approved exception**: W2 deletes exactly two
  `[Fact]` methods in `TableValidatorTests.cs` (`CrossJoinIsForbiddenInTableMode`,
  `LeftJoinIsNotSupportedInTableMode`) which assert the restriction being lifted — a pure subtraction,
  called out in that wave's commit message. Expected 897 → 895 + new.
- Contracts evolve additively only; the Engine gains no Orleans/Dapr/ASP.NET types.
- Outer joins: P=1 vs P=4 equivalence and replay determinism for LEFT/RIGHT/FULL/chained.
- Set operations: dedup proven through the REST rows endpoint; pipeline-mode `UNION` rejected with a
  diagnostic naming `UNION ALL`.
- Ingress: each policy demonstrated live against a stalled consumer, `429` carrying `Retry-After`,
  drop counters matching the response bodies, config export/import round-tripping an ingest source.
- Lineage/plan: graph matches the seeded catalog on both flavors; the physical stage graph renders on
  Orleans at P=4 and degrades cleanly to the logical plan on Dapr.

## Risks

1. **The W2 intermediate state** — a removed validator gate without the op wired into both the P=1
   façade and the P≥2 builder produces silently-wrong INNER results. Mitigated by making 2b a single
   atomic owner.
2. **Residual-aware outer joins** are the highest bug-density surface in the plan (negative weights ×
   intra-batch flip/unflip). Mitigated by op-level unit tests written before the gate lifts.
3. **The equi-key list refactor touches the shared pipeline path** — `ExecutorJoinTests` and
   `PipelineOpsUnitTests` staying green *unmodified* is the regression proof.
4. **Set operations introduce the first AST node above `SelectQuery`**; `Sources[0]` is dereferenced
   unguarded in six places, so a set-op root must never reach them.
5. **gRPC ingest is the repo's first bidi RPC and first non-Viewer gRPC method** — no call-level test
   harness exists, so its ack/limit logic is factored into a pure class and tested there.
6. **Null-padded rows carry NULL keys**, so downstream hash edges keyed on a padded column route them
   all to one partition. Correct, potentially skewed — named, not fixed.
