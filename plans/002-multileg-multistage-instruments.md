# 002 — Multileg & Multistage Financial Instruments

Status: **PROPOSED** (assessment approved in conversation 2026-07-18; not yet scheduled).
Scope: first-class multileg instruments (swaps, option strategies) and multistage lifecycles
(order/instrument state machines) across the whole stack: schema model → generators → streaming
SQL → materialized tables → typed gRPC clients.

## Current state (what already works, don't rebuild)

- JSON fields with declared `FieldDef.Children` give typed nested drill-down; `->` / `->>`
  operators (literal keys, chaining, NULL propagation); arrays ride along as schemaless values.
- Typed protobuf generation (`DescriptorFactory`) emits nested messages from Children; **no
  repeated/array support** — arrays currently degrade to `google.protobuf.Struct`.
- Session windows, interval joins, table-mode running aggregates; table row history with
  MinBy/MaxBy retention (plan-independent, landed separately) is a building block for stage
  machines.

## Phase L1 — Typed legs: model + generators + protos (~1–2 days, one agent)

1. `FieldDef`: add `bool IsArray` (`[Id(3)]`, default false; additive — frozen-contract safe).
   - Array of Json-with-Children = typed leg list; array of scalar = repeated scalar.
2. Sources UI (`SourcesPage` field editor + `SchemaNode` tree): "list" toggle per field, `[]`
   marker in the drill-down tree.
3. Generators (`MarketDataProfiles`): new profile `multileg`, seed source `structures`:
   - IR swaps: 2 legs `{leg_no, pay_rcv, notional, ccy, index|fixed_rate}`.
   - Option strategies (straddle/strangle/butterfly): 2–4 legs `{strike, expiry, cp, ratio}`.
4. `DescriptorFactory`: `IsArray` → `repeated` (repeated nested message + repeated scalars);
   `ProtoWireEncoder`: encode `List<object?>` per element (packed encoding optional — plain
   repeated is fine and simpler); `FieldNumberMap` unaffected (arrays don't change numbering).
5. SqlEditor autocomplete: after `legs -> 0 ->` complete the element's child keys (index step
   just unwraps the array level).
6. Tests: descriptor validity with repeated fields, wire round-trip for lists (empty list, one
   element, nested legs), generator shape checks.

Acceptance: `structures` streams typed swaps; `GET /api/sources/structures/proto` shows
`repeated Leg legs = n;`; a generated client decodes `List<Leg>`.

## Phase L2 — UNNEST in the engine (~1 week, the long pole, one agent + checker)

1. Grammar: `FROM structures s, UNNEST(s.legs) AS l` and `CROSS JOIN UNNEST(expr) AS alias`
   (both forms; comma form is sugar for the latter). New keyword `UNNEST`.
2. Semantics: one output row per array element; empty/NULL array → zero rows (document; a
   `LEFT UNNEST` NULL-padding variant is descoped — note in Limitations). Element fields
   addressable as `l.field` when the array is typed (Children known) or `l ->> 'field'`
   schemaless. Element scalars: the element itself binds to the alias (`l` = value).
3. Planner: `UnnestStage` runs after source ingestion, before WHERE; validator tracks element
   kinds from `Children`.
4. **Both modes**: pipelines and tables. Z-set note: unnest is linear (weight multiplies
   through per element), so table mode needs no new state — deltas map element-wise. Add
   executor tests proving retraction of a multileg row retracts all its unnested rows.
5. Tests: tokenizer/parser/validator (unknown alias, non-array arg diagnostics), executor
   (join-after-unnest against market data, per-ccy notional aggregate, session window over
   legs), table mode with retractions, qualified star `l.*`.

Acceptance: `SELECT s.id, l ->> 'ccy' AS ccy, SUM(l ->> 'notional') … FROM structures s,
UNNEST(s.legs) AS l GROUP BY ccy WINDOW TUMBLING(SIZE 5 SECONDS)` runs green with positioned
diagnostics on typos.

## Phase L3 — Multistage lifecycles (~2–3 days, one agent)

1. Generator profile `lifecycle`, seed source `order_events`: orders progress
   `NEW → ACK → PART_FILL×n → FILLED | CANCELED` with `order_id`, `stage`, `stage_ts`,
   cumulative `filled_qty`.
2. Table sugar `LATEST BY`: `SELECT … FROM order_events LATEST BY (order_id)` — validator +
   planner rewrite to the existing running-aggregate machinery (MAX by `stage_ts` per group),
   emitting one current row per key. (Alternative if rewrite is awkward: document the manual
   `GROUP BY order_id` + MAX pattern and skip the sugar — decide in-implementation, report.)
3. Seed table `order_states` (LATEST BY) + docs section; stage history is already covered by
   table row history (LastN/All modes show the stage trail per order).
4. Optional (descope freely): stage funnel mini-viz on the table page.

Acceptance: `order_states` shows exactly one row per live order updating through stages; row
history timeline shows the stage trail; typed client decodes `OrderStatesDelta`.

## Non-goals

Recursive/iterative queries, general LATERAL correlation, cross-row leg matching (that's a
join), typed arrays inside schemaless Struct subtrees, leg-level independent lifecycles.

## Sequencing / effort

L1 → L2 → L3 strictly (L2 consumes L1's typed arrays; L3's demo reads best after L2 exists,
though LATEST BY itself only needs L1). Total ≈ 2 weeks single-agent-equivalent; L1 and L3
are parallelizable with anything that doesn't touch Engine; L2 must own the Engine exclusively.
