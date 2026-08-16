# Wishlist from the ac-co OTC Terms demo

Collected while building the hedge-fund demo (ac-co.ai-4 `apps/websites/otc-terms`
+ `apps/office-addins/otc-addin`) against StreamForge. Ordered by value; every
item is small by design.

Each section keeps the original report and records what shipped underneath it —
including the places where the report turned out to be describing half of
something. **All 16 done.** What remains open inside them is
listed under each item and repeated at the bottom — mostly the Dapr flavour of the
backfill and coordinator-mode inputs, both blocked on seams rather than unwritten.

## 1. ✅ Configurable CORS origins (shipped 2026-08-15)

`shared/StreamForge.Api/StreamForgeApiExtensions.cs` — the `SpaDev` policy was
hardcoded to `http://localhost:5173`. Now reads `Cors:AllowedOrigins` (env form
`Cors__AllowedOrigins__0=…`) with the old value as the default. Used by
`deploy/orleans/compose.demo.yaml`. `dotnet build` clean; behavior unchanged
when unset.

## 2. ✅ CASE expression in SQL

No `CASE`/`WHEN` in the grammar. The demo's `trigger_monitor` table wants
`CASE WHEN t.downgrade_trigger_numeric - r.rating_numeric <= 0 THEN 'BREACHED' …`
— today every client computes status labels from the numeric `distance` column.

**Shipped:** searched CASE only, as scoped —
`CASE WHEN cond THEN expr [WHEN …] [ELSE expr] END`. No new AST node: the parser
desugars it into nested three-argument `IF(cond, then, else)` calls
(`Sql/Parser.cs` `ParseCaseBody`), the same trick `CAST(x AS type)` already plays
with the `TO_*` functions — a `FunctionCallExpr` already flows through the
Validator, the Planner's rewrites, the evaluator and aggregate detection, and a
new node would have to be taught to each of them. `IF` is callable directly too;
it is the same node with the same semantics either way.

Typing is as requested: branches must agree, a `Long`/`Double` mix widens to
`Double` (matching mixed arithmetic), and disagreeing branches are a compile
diagnostic. A non-boolean condition is rejected as well — truthiness here is
strict (`value is true`), so a non-boolean condition would otherwise silently
always take the else-branch. An omitted `ELSE` supplies NULL. There is no simple
`CASE expr WHEN value THEN …` form.

Evaluation short-circuits, so an N-branch CASE does N tests, not N².
Coverage: `orleans/tests/StreamForge.Engine.Tests/CaseExpressionTests.cs` (19
tests, both spellings). Console highlighting/formatting, `orleans/docs/index.html`,
`orleans/README.md` and the `sf-sql` skill know about it.

## 3. ✅ `decimal` case in field coercion (real CDC bug)

`shared/StreamForge.Engine/Runtime/FieldValueConversion.cs` — `TryToDouble` and
`TryToLong` have no `case decimal`. Postgres `numeric` columns arrive from the
CDC path as CLR `decimal` (`PgCdcSource.Cell` passes them through), so a
declared `Double` field gets coercion-failure → NULL for every `numeric` column.

**Shipped**, and widened to the rest of the same hole: both `Cell` mappings also
pass `short`/`ushort`/`byte`/`sbyte`/`uint`/`ulong` straight through (smallint,
tinyint), and none of those had an arm either. All three converters
(`TryToDouble`/`TryToLong`/`TryToBool`) now take them. Purely additive — every
one of these was a coercion *failure* before, so nothing that used to produce a
value produces a different one, which is what the class doc's "pinned to the
inbound path byte-for-byte" warning is actually protecting.

**Follow-up, also shipped:** `DateTime`/`DateTimeOffset` had no arm either, so a
date/time column declared as a `Timestamp` field NULLed out the same way. It was
first left open on the grounds that `DateTimeKind.Unspecified` has no correct
timezone to guess — which turned out to overstate the problem. Only one of the
three kinds is ambiguous at all: `Utc` and `Local` both carry their own offset
and convert exactly, and `DateTimeOffset` is unambiguous by construction. For
`Unspecified` there is no new policy to invent either — this file already reads a
zone-less timestamp as UTC on both of its string paths
(`DateTimeStyles.AssumeUniversal`), and `PgCdcSource.ToUnixMs` does the same to a
pgoutput commit timestamp. Reading it as `Local` instead would make the value
depend on the host process's timezone, which is a deploy detail rather than a
property of the data.

The rule lives in one private helper used by all three timestamp entry points
(`TryCoerce`'s `Timestamp` kind, `TO_TIMESTAMP`, and `ResolveTimestamp`) so they
cannot drift. `Timestamp` was split out of `Long`'s numeric-only rule to carry it;
`Long` deliberately still rejects a date/time, so a mis-declared column stays a
visible NULL instead of quietly becoming an epoch integer.

**Genuinely still open, and not additive:** a date/time coerced to a `String`
field renders as `08/15/2026 12:00:00` (invariant-culture `ToString`) — not
sortable, not ISO-8601, not parseable by the ISO reader on the way back in.
Fixing it would change a value the pipeline already produces today, so unlike
everything above it is a behavior change that needs a decision, not a bug fix.

## 4. ✅ cdc.md example doesn't parse (doc bug)

`docs/cdc.md` showed `SELECT * FROM "orders-cdc" LATEST BY id WHERE _op <> 'd'`
in three places. The grammar requires WHERE **before** LATEST BY, and LATEST BY
requires parentheses.

**Fixed in the doc** (`WHERE _op <> 'd' LATEST BY (id)`), not in the parser:
accepting both clause orders buys nothing and costs a grammar ambiguity forever.
The reason nothing caught this is that no test combined the two clauses — there
is one now (`LatestByTests.WhereComesBeforeLatestBy_theCdcMirrorPatternAsDocumented`),
asserting both that the documented form compiles and that the old form doesn't.

## 5. ✅ Config-update should reset the poll backoff

When a polled/CDC source is in failure backoff (e.g. wrong host), fixing the
config via `PUT /api/sources/{name}` or config import did NOT reschedule the
driver — it kept waiting out the old backoff (minutes), and even a
disable/enable cycle kept `nextRunMs`/`consecutiveFailures` intact.

**Shipped, and it was two bugs, not one.** The obvious half is that
`StartAsync` (which every one of those paths lands in) carried the failure streak
across a restart, so `BackoffPolicy.NextRun` scheduled the new definition off the
old definition's health. Both hosts now clear it.

The half the report couldn't see from outside: in the Orleans host that fix alone
is not enough. Grains are turn-based, not serialized end-to-end, so a poll cycle
that is sitting at an `await` lets `StartAsync` run in between — and then resumes,
bumps the streak the restart just cleared, and **re-arms the timer at its own
stale 30s backoff**, clobbering the timer the restart armed. The symptom is
identical to the original one and only appears under enough load to widen the
window (it reproduced reliably in the full test suite and never in isolation).
`ConnectorGrain` now carries a generation counter: a cycle captures it on entry
and discards its entire result — status, streak, cursor, timer — if a
Start/Stop replaced it mid-flight. The Dapr actor needs no equivalent; Dapr's
turn-based concurrency holds for the whole call, awaits included.

Covered by `ConnectorGrainClusterTests.Restarting_with_a_fixed_config_clears_the_failure_backoff`,
which fails without either half of the fix.

`POST /api/sources/{name}/poke` was **not** added — with the above, a config
change already polls promptly, which is what the endpoint was a workaround for.

## 6. ✅ Honor `PORT` env in the from-source host

`PORT=6199 dotnet run --project orleans/src/StreamForge.Host` ignored PORT and
bound 5199/5299.

**Shipped:** `PORT` moves the HTTP port and gRPC follows at `PORT+100` — the same
+100 relationship the two defaults already have, so `PORT=7101` gives 7101/7201
(verified by running it). `Http:Port`/`Grpc:Port` still win where set, so the two
can always be split apart. The gRPC port is now resolved once and shared with
`StreamForgeApiOptions`, which previously read it a second time from
configuration and would have reported 5299 while Kestrel bound something else.

## 7. ✅ Console SQL editor: `LATEST BY` (and friends) parsed as a table alias

`web/src/components/sqlScope.ts:147` — `CLAUSE_KEYWORDS` lacks `LATEST`,
`UNNEST`, `UNION`, `IN`, `EXISTS` (all reserved in
`shared/StreamForge.Engine/Sql/Parser.cs`), and `web/src/components/SqlEditor.tsx`
`KEYWORDS` lacks `LATEST`/`UNNEST`/`IN`/`EXISTS`. So `FROM trades LATEST BY (id) WHERE `
reads `LATEST` as an AS-less alias for `trades` and column completion after
`WHERE` yields nothing — and every table-mode CDC mirror is written exactly that
way. Fix is additive: add the five words to `CLAUSE_KEYWORDS` and the four to
`KEYWORDS`. Found while porting the editor into the ac-co demo's `/sql` page
(`apps/websites/otc-terms/app/sql/sql-scope.ts` carries the patched set).

**Shipped:** the five words `Parser.cs` reserves were added to `CLAUSE_KEYWORDS`
(`LATEST`, `UNNEST`, `UNION`, `IN`, `EXISTS`) and four to `KEYWORDS`, exactly as
scoped.

---

# Wave 3 — simulations, statistics, pricing (2026-08-15)

Context: the ac-co OTC demo is adding Excel custom functions (`SF.*` streaming
UDFs), what-if scenarios (overrides over the live cube) and a minimal
Monte-Carlo layer. Everything below is an engine capability the demo currently
works around client-side; each item says what the workaround is, so the demo is
never blocked on it. Ordered by value. Row contracts are spelled out so the demo
can swap generator/aggregate implementations without touching table SQL.

## 8. ✅ Parametric, seedable "scenario generator" source with run-on-demand

Today a generator is `SourceDefinition.GeneratorProfile` ∈ {trades, quotes,
orders, generic} + `EventsPerSecond` (`shared/StreamForge.Contracts/Models.cs:55-57`);
`MarketDataProfiles.cs:52,206-241` synthesises values with `Random.Shared` (no
seed), `orleans/src/StreamForge.Host/Grains/GeneratorGrain.cs:28-41` emits one
event per timer tick, and `IGeneratorGrain`
(`orleans/src/StreamForge.Abstractions/GrainInterfaces.cs:24`) has no
run-once/emit call. There is no way to say "give me a *batch* of N×K rows, now,
reproducibly".

Wanted (new profile `scenario` or a new kind — whichever is smaller):
- Spec: `paths` N, `instruments` K (inline JSON list `[{id, base, vol, group}]`
  or a reference to a source/table name), per-instrument distribution
  (`normal | lognormal | student_t(df)`), one common factor per `group` plus
  idiosyncratic noise (`rho`), horizon `days` D (1 = single-shock), `seed`.
- **Run on demand**: `POST /api/sources/{name}/run` with `{ run_id, seed?,
  overrides? }` → emits the whole batch (N×K×D rows) once, honouring
  `MaxBatchRows`/backpressure; `EventsPerSecond` ignored/0 for this kind.
  Return `{ accepted, rows }`.
- Row contract (what the demo's tables already consume from its Bun generator):
  `run_id string, path_id long, instrument_id string, day long, factor double,
  shock double, value double` (+ `_ts`). Deterministic: same seed → identical
  batch (this is what makes "before/after a CSA amendment" comparisons honest,
  and it fits the engine's replay invariant — the RNG is seeded per run, not
  process-global).
- Nice-to-have: `POST …/run` accepts `step: true` to emit only day d+1 for an
  existing run (see #9 for why).

Workaround in the demo: `apps/websites/otc-terms/lib/mc.ts` generates the same
rows in Bun and pushes them to an `ingest` source; tables are unchanged.

**Shipped.** The `scenario` profile with paths/instruments/distribution/rho/
horizon/seed, and `POST /api/sources/{name}/run` that publishes the batch.
The RNG is seeded per run, never process-global — determinism is the point, since
"before and after a CSA amendment" only compares honestly if exactly one thing
differs. Tests pin byte-identical repeats, exact equality within a group at ρ=1,
and independence at ρ=0.

`RunSourceAsync` sits on `ICatalogFacade` rather than in the endpoint because only
a runtime can publish: an endpoint that generated the batch itself would return
rows while emitting nothing — a run that looks successful and moves no data.

Not implemented on purpose: instruments by reference to another source (inline
list only; the reference is a hard validation error, not silently ignored), and
`step: true`, which belongs with #9 — no field was added, so nothing exists that
quietly does nothing.

## 9. ✅ Bounded feedback loop (instead of recursive SQL) — also the "scenario clock"

Recursive SQL was rejected for complexity; agreed. The streaming-native
workaround is an explicit, user-declared loop with a step bound: a table's
deltas are fed back as input to a source, carrying `step + 1`, and a `WHERE
step < D` in the consuming table terminates it. The engine guarantees delivery
and ordering only; termination is the user's job (like any dataflow iterate).

Today this is not expressible: sinks are `nats | file | postgres | mssql`
(`shared/StreamForge.Contracts/ConnectorModels.cs:136-150`), no HTTP sink, and
the only feedback edge is NATS pub → NATS sub (out of process, no cycle guard).

Wanted, smallest first:
- (a) `SinkKinds.Http`: POST a table's deltas (or a pipeline's rows) to a URL —
  for the loop that URL is `/api/sources/{name}/events` on the same host;
  template: `AppCore/Sinks/NatsSinkClient.cs`, config next to `NatsPubConfig`
  (`ConnectorModels.cs:157-166`), same fire-and-forget semantics.
- (b) or a native `loopback` sink→source pair (in-process, no HTTP), with an
  optional `maxDepth` guard that drops rows whose `step >= maxDepth` so a
  mistaken loop can't spin forever.

Why this is also the "scenario clock": path-dependent simulation (margin calls,
collateral triggers along a trajectory) needs step t+1 to start *after* the
engine has finished step t — a rate generator can't do that, a loop iteration
can't do otherwise. With #8's seed the whole trajectory is replayable.

Workaround in the demo: a Bun driver steps days outside and re-pushes state per
`(path_id, instrument_id)`, relying on `LATEST BY` supersession.

**Option (a) shipped, option (b) not.** `SinkKinds.Http` posts a table's deltas to
a URL, body wire-identical to the ingest endpoint's, with the `maxDepth` guard
dropping any row whose step has reached the bound. The native in-process
loopback sink→source pair was not attempted.

Named tradeoff: a maxDepth drop shares a counter with a network failure because
`SinkPublishCounters` is frozen — `LastError`'s text always separates them, so
nothing is silent. Pre-existing gap found and left alone: `ISinkTransport.Validate`
is not wired to any REST call site in this repo, so a missing URL is correctly
rejected and nothing shows an operator why.

## 10. ✅ Statistical aggregates: STDDEV/VAR, PERCENTILE_CONT/MEDIAN, COUNT(DISTINCT)

Aggregates are the closed set `COUNT, SUM, AVG, MIN, MAX`
(`shared/StreamForge.Engine/Sql/Ast.cs:93-97` `AggregateNames.All`, factories
`Runtime/Aggregators.cs:11-19` (stream) and `Runtime/ZAggregators.cs:14-22`
(Z-set, `Apply(value, weight)`), name check in `Sql/Parser.cs:905`).

Wanted:
- `STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP`, `VAR_POP` — subtractable (Σx, Σx², n),
  `SumZAggregator` at `ZAggregators.cs:34-50` is the template; must be
  weight-aware so retractions (LATEST BY supersession) don't drift the moments.
- `PERCENTILE_CONT(p)` / `MEDIAN` — the hard one: the Z path needs a weight-aware
  ordered multiset (SortedDictionary<value,count> like MIN/MAX already keep) and
  interpolation; exact is fine at demo sizes (≤ 10⁴ rows per group), a t-digest
  can come later.
- `COUNT(DISTINCT x)` — multiset of counts, subtractable.

Enables VaR/ES/breach-probability *in SQL* over the MC tables:
`SELECT run_id, PERCENTILE_CONT(0.05) OVER pnl … GROUP BY run_id`.
Workaround in the demo: percentiles computed client-side over `mc_path_pnl` rows.

**Shipped in two parts.** The one-argument family — `VAR_SAMP`/`VAR_POP`/
`STDDEV_SAMP`/`STDDEV_POP`/`MEDIAN`, with `VARIANCE`/`STDDEV` aliasing the sample
forms — needed only new accumulators. `PERCENTILE_CONT(p, x)` and
`COUNT(DISTINCT x)` needed grammar: a second aggregate argument and a `DISTINCT`
keyword. All of them subtract, proven by driving values through a `LATEST BY`
table and comparing against a from-scratch fold over what should remain.

Two things worth knowing. The moments are accumulated around an offset taken from
the first value seen, not as raw `Σx²`: over prices near 1e8 with a spread of 1
the textbook form is wrong by ~2 (measured, by forcing the offset to zero), and
Welford — the usual answer — cannot subtract. And `p` must be a literal, because a
per-row probability would silently mean "whichever row arrived first".

Fixed a pre-existing gap on the way: the parser kept only an aggregate's first
argument and dropped the rest, so `SUM(a, b)` compiled as `SUM(a)`.

## 11. ✅ Pricing / greeks scalar functions backed by QuantLib (via QLNet)

Scalars are a closed compile-time set: `Sql/Validator.cs:1182` `KnownFunctions`
+ arity/kind switches (`:1198-1212`) + `Runtime/ExpressionEvaluator.cs:188-219`
`EvalFunction`; the seam is documented at `Runtime/FieldValueConversion.cs:6-11`.

Wanted: a `StreamForge.Quant` set of scalar functions implemented on **QLNet**
(pure-.NET port of QuantLib, NuGet `QLNet`, BSD, no native libraries — ships in
the Orleans container unchanged). First set, all doubles in, double out:
- `BS_PRICE / BS_DELTA / BS_GAMMA / BS_VEGA / BS_THETA (spot, strike, t_years, r, q, vol, is_call)`
- `BOND_PRICE / BOND_DV01 / BOND_DURATION (face, coupon, years, yield, freq)`
- `IRS_NPV / IRS_DV01 (notional, fixed_rate, years, flat_rate, pay_fixed)` (flat curve)
- `FX_FWD (spot, r_dom, r_for, t_years)`
NULL in → NULL out; invalid domain (vol ≤ 0, t ≤ 0) → NULL + a diagnostic at
validate time when constant-foldable.

Tests, for now: QLNet + a small table of *independently known* values —
Black-Scholes closed-form vectors from Hull, put-call parity, DV01 of a par
bond, FX forward parity — enough to pin the plumbing (argument order, units,
day counts), which is where wrapper bugs live. Later hardening: generate a
vector table once with QuantLib-Python (`pip install QuantLib`) and assert
±1e-8 (same approach as ac-co's 850 HMRC payroll vectors).

Enables scenarios "curve +25 bp / vol +5 pts → positions reprice in-engine"
instead of exposure shocks. Workaround in the demo: exposure shocks only.

**Shipped**, on QLNet 1.13.1 — note 1.14 does not exist, 1.13.1 is the newest
stable. The Black family goes through QLNet's own `BlackCalculator`, which is
date-free (forward, stddev, discount) and so touches none of QLNet's
`Settings.evaluationDate` thread-static global. The bond/swap measures are
closed-form for exactly that reason: QLNet's instrument layer reads that global,
which is incompatible with a scalar that must be pure, total and concurrently
evaluated. Said plainly in the code so "QuantLib-backed" is not read as covering
more than it does.

Tests assert only against independently-known values — Hull's worked example,
put-call parity, `delta_call − delta_put = e^(−qT)`, gamma/vega equal across
call and put, a par bond at par, modified duration of a zero, DV01 against an
actual 1bp reprice, a par swap worth nothing, covered-interest parity.

## 12. ✅ Function / aggregate extension seam (makes 10–11 additive)

`Aggregator`, `IZAggregator`, `KnownFunctions` and `EvalFunction` are all
`internal`, so a `StreamForge.Quant` assembly cannot register anything without
editing the Engine (or `InternalsVisibleTo` in `shared/StreamForge.Engine/AssemblyInfo.cs`).
Wanted: a small public registry — `IScalarFunction { Name, Arity, ResultKind(args),
Eval(args) }` and `IAggregateFactory { Name, CreateStream(), CreateZ() }` — that
the Validator/Parser/Evaluator consult after the built-in switches. Console
intellisense (`web/src/components/SqlEditor.tsx` `FUNCTIONS`) should read the
same registry through `GET /api/sql/functions` so #10/#11 auto-complete.

**Shipped.** `SqlFunctions` with `IScalarFunction`/`IAggregateFunction`, consulted
by parser, validator and evaluator after the built-in switches. Built-ins always
win and a colliding registration is refused *at registration*, not resolved by
precedence — a third party redefining `SUM` would change the meaning of deployed
SQL and the damage would appear as wrong numbers rather than an error. An
aggregate must supply both accumulators, because a table maintains a Z-set and
one that cannot subtract cannot be maintained incrementally.

`GET /api/sql/functions` ships with it and the console's completion list reads it,
so a registered function autocompletes; built-ins stay static there so the editor
works before that resolves and if it fails. Verified live against a running host.

## 13. ✅ Explicit key retraction through ingest

Ingest is append-only by design (`AppCore/Connectors/Mapping/CdcEnvelope.cs:44-52`,
`Runtime/Ops/TableIngestOp.cs`), and retention is only allowed on P=1
non-join/non-aggregate tables (`Models.cs:262-266`). Clearing a scenario
override or an obsolete MC run therefore means pushing `active=false` and
filtering — the key is hidden, never freed, and downstream aggregates keep the
tombstone row's weight forever.

Wanted (either): a `_retract: true` envelope on `POST /api/sources/{name}/events`
that emits weight −1 for the last asserted row of that key in a `LATEST BY`
table (only meaningful for LATEST BY consumers — reject otherwise at validate),
or `DELETE /api/tables/{id}/rows?key=…` for LATEST BY tables. Either lets the
demo free `scenario_inputs` and `mc_paths` keys instead of tombstoning.

**Shipped.** A row may carry `_retract: true`; it flips the ingested weight to −1
and reaches `TableLatestByOp` as a key retraction that drops what is retained for
that key regardless of the arriving row's content. Unknown key and double retract
are dictionary-remove misses, so idempotence is by construction. Rejected at
validate time, by name, for any consumer that is not LATEST BY-shaped.

Three gaps, stated rather than left to be found: gRPC ingest bypasses the validate
gate (safe, but silent instead of refused); a `WHERE` over non-key columns can
drop a retraction before it reaches LATEST BY, since a retract row carries only
the key; and validation covers direct consumers, relying on each intermediate
being checked in turn for chains.

## 14. ✅ BUG: an aggregate created over an already-populated `LATEST BY` table collapses to zero

`shared/StreamForge.Engine/Runtime/Ops/TableReduceOp.cs` — `Groups.Remove(key)`
whenever a group's running weight touches zero; a group cannot carry negative
weight. `LATEST BY` emits a bare assert (+1) on first sight of a key and
retract(−1)+assert(+1) thereafter. So a GROUP BY table created *after* its
`LATEST BY` input already holds rows never sees the original asserts (no
backfill), receives the next retract first, hits weight −1 → clamped/removed,
then the assert re-creates the group with one row — the aggregate ping-pongs
between one row and none forever. Reproduced in the demo (a scenario cube
created mid-session showed one trade per group); the base cube is only correct
because it was created while its inputs were empty.

Wanted (either): (a) backfill on table creation — replay the upstream table's
current rows as asserts to the new consumer (this is also wishlist "no
historical backfill" from the /sql page); or (b) at minimum a diagnostic at
create time when an input `LATEST BY` table is non-empty, plus documented
guidance. Workaround used: the consumer CROSS JOINs a *derived*
`(SELECT … FROM src LATEST BY (k))` subquery — a nested operator with its own
empty key map — instead of the materialized table
(`apps/websites/otc-terms/lib/streamforge/provision-doc.ts`, and
`lib/streamforge/rebuild.ts` documents which tables must never be recreated
warm).

**Half shipped, and the half that was safe.** A group whose weight goes negative
is now kept — only exactly-zero drops it — so the unmatched retraction no longer
destroys the group for the following assert to rebuild from one row. The aggregate
reports *nothing* instead of a plausible wrong count, and a test shows that
replaying the missed asserts converges rather than double-counting, which is the
property any backfill will depend on. `TableExecutor.UnmatchedRetractions` counts
the occurrences, so an operator can see why.

**Not done: the actual fix**, option (a). Replaying the upstream's contents on
attach is a change to `TableGrain`'s subscription path in both runtimes, and it is
only correct if the snapshot and the delta stream agree on an epoch — otherwise
the backfill duplicates or misses rows and produces a wrong answer by a different
route. That needs its own pass.

## 15. ✅ Retract/assert of one upstream change should be applied atomically downstream

A LEFT JOIN onto an aggregate table observes the aggregate's retract(−1) before
its assert(+1) as two separate deltas: for part of every tick the joined column
reads NULL (`scenario_trigger_monitor.threshold_headroom` flaps to null in the
demo; the UI recomputes headroom from the stable side to hide it). Wanted:
deltas produced by one upstream epoch are applied to a downstream operator as
one batch (retracts and asserts together, differential-dataflow style) before
the downstream emits — or at least the SignalR `tableDelta` for one epoch
carries both so clients don't paint the intermediate state. Pointers:
`Runtime/Ops/TableReduceOp.cs:6-14` (emits retract then assert), join ops under
`Runtime/Ops/`, `Runtime/TableExecutorImpl.cs:41` (upstream deltas → alias
input).

**Shipped for #15 and #14(b)** (they landed together, since both are about a
consumer seeing a state that should never have been visible):

One upstream batch is now applied as ONE epoch — `TableExecutor.OnTableDeltaBatch`,
called once by both runtimes instead of looping per delta — and the epoch's output
is consolidated by row key so an intermediate assert/retract pair nets out before
leaving the table. A LEFT JOIN onto an aggregate no longer reads NULL mid-tick.
Consolidation is gated on the INPUT batch size, not the output: a single delta can
legitimately produce a same-content retract+assert pair, and gating on output
silently swallowed it (caught by an existing pinned test, not by review).

#14 shipped option **(b)**: `SnapshotFrontierEpoch` is coordinator-mode only, and
the bug hits classic `Parallelism == 1` tables, which have no such point. A correct
backfill needs an epoch on every element of the table-delta stream — a wire
contract every consumer must agree on. Both runtimes now warn, by table name and
row count, when a consumer attaches to an upstream that already holds rows.

Neither fix covers coordinator mode (`Parallelism >= 2`), which does not route
through `TableExecutor` at all.

## 16. ✅ Server-side delta coalescing per epoch on the SignalR hub

A 36,000-row Monte-Carlo run (200 paths × 36 trades × 5 days) produced ~100k
`tableDelta` messages; the browser (and Excel) fell minutes behind the engine on
the socket, not on rendering (the client already coalesces React flushes to
120 ms). Wanted: `/hubs/stream` should emit one `tableDelta` per (table, epoch)
containing all deltas of that epoch — and, optionally, collapse retract+assert
of the same key within the batch to a single assert (net Z-set) — instead of
one message per delta. This is also the client-visible half of #15. Pointers:
`orleans/src/StreamForge.Host` hub + `Streams:PushCapacity`, `TABLES__FLUSHMS`
(default 250) — the flush interval exists but the message granularity does not.
Ingest-side, the demo raised `MaxBatchRows`/`CapacityRows` to 5000/50000 and
that half is fine.

**Shipped, the batching half.** The bridge already sent one `tableDelta` per
engine publish — the message shape was never per-delta. The volume came from the
publish RATE: a bulk load publishes once per upstream batch, so tens of thousands
of rows meant tens of thousands of socket frames, each with its own SignalR
envelope. The cost is per message, not per row.

`StreamBridgeService` now accumulates a table's deltas and sends one message per
100 ms flush (under the client's own 120 ms render coalescing, so a merely-ticking
table gains no perceptible latency), with an early flush at 20,000 pending so
memory is bounded by (tables × cap). Deltas are never dropped to stay under the
cap — a dropped delta silently desynchronises the client's Z-set.

**Not done: the netting half.** Collapsing retract+assert of the same key needs
the engine's own row-identity rule, and a bridge that guessed at it would quietly
change what the client converges to. The engine already nets within an epoch
(#15's consolidation); pushing that knowledge out to the bridge is a separate
decision about where row identity lives.

Also untouched: `Streams:PushCapacity` and `TABLES__FLUSHMS` are unchanged — this
adds a message-granularity control that did not exist, rather than retuning the
ones that did.

---

# Second pass (2026-08-16) — the parts left open

- **9(b)** shipped: an in-process `loopback` sink writing into a per-source
  channel. A write never calls the reader (the reader runs only from its own drain
  timer), so a cycle cannot grow a stack or deadlock; unbounded, it is simply
  alive until a `WHERE` bounds it or the source is stopped. `maxDepth` is now one
  `SinkStepGuard` shared with the HTTP sink.
- **8's `step: true`** shipped, by reworking the generator from path-major to
  day-major so whole-run generation is literally begin-then-loop over the per-day
  function. Equivalence holds by construction, and the test asserts bitwise-equal
  rows.
- **14(a)** shipped for Orleans classic: `TableExecutor.LastEpoch` +
  `TableDeltaDto.Epoch` + `AttachSnapshotAsync` (rows and epoch read with no await
  between), subscribe-before-attach, and a cutoff filter on arrival. Correctness
  rests on grain non-reentrancy, not on timing. A warm-attached table equals a
  cold-built twin row for row.
- **15 for coordinator mode** shipped: `TableOutputGrain` published per partition
  arrival, before frontier consolidation; the publish moved into
  `TableGrain.OnOutputBatchAsync`, which already buffers per (partition, epoch).
- **16's netting** shipped, reusing the engine's canonical row key rather than a
  bridge-local guess.
- **13's two gaps** shipped: gRPC ingest runs the same validate gate as REST, and
  a key retraction bypasses `WHERE` (chosen over rejecting such tables at create
  time — a retraction is a key-level operation, not a row that must qualify).
- **9's `Validate` gap** shipped: sink validation is wired into table/pipeline
  create and update.

**Still open, and blocked on seams rather than effort:**
1. **The Dapr flavour of 14(a)** has only the wire field. It needs a method on
   `ITableActor`, and Dapr has no equivalent of the subscribe-before-attach
   ordering that grain non-reentrancy gives Orleans — routing is registered by the
   lifecycle orchestrator after the actor's `StartAsync` returns.
2. **A coordinator-mode table warm-attaching to its OWN upstream** — those inputs
   arrive via `TableIngestGrain`/`ArrangementGrain`, not `TableExecutor`.
3. **No end-to-end network test** for any REST or gRPC boundary: this repo has no
   such harness for any endpoint. Logic is extracted into pure seams and tested
   there; the plumbing is verified by compilation only.
4. **Two cluster tests failed once** under whole-solution load
   (`TableFrontierClusterTests`, `ShardedTableClusterTests`) and then passed in
   four consecutive runs, including two more full-solution ones. Not reproduced
   since; recorded rather than dismissed, because a load-only failure in this repo
   has already turned out to be a real race once.

---

## Wave 4 — after adoption (2026-08-16)

These four came out of *adopting* waves 1–3 in the ac-co OTC hedge-fund demo
(`apps/websites/otc-terms` in the ac-co.ai-4 repo) — three API-shape gaps and one
unbounded-state question that only appear once a client drives the new surfaces
from outside, rather than from a test inside the solution. None of them are
shipped, so none carry the ✅ the items above do; each says what the demo does
instead, so nothing is blocked on them.

## 17. `POST /api/sources` should accept `eventsPerSecond: 0` for `generatorProfile: "scenario"`

`shared/StreamForge.Api/Endpoints/SourceSchemaService.cs` — `SourceValidation.Validate`
adds `"eventsPerSecond must be > 0"` for any `kind: "generator"` source with
`EventsPerSecond <= 0` (`:82-89`), and a scenario-profile source is generator-kind.
But 0 is the only correct value for it: the profile's whole convention is that
rows come from an explicit `POST /api/sources/{name}/run`, never from a tick.
`SourceDefinition.Scenario`'s own doc comment says so in as many words —
"EventsPerSecond is ignored (must be 0, by convention — nothing enforces it)"
(`shared/StreamForge.Contracts/Models.cs:81-85`). Nothing enforces it; something
enforces the opposite.

Why it matters: `Validate` gates `POST`/`PUT /api/sources`
(`SourcesEndpoints.cs:55,84`) and the chat tools' create/update
(`Chat/ChatTools.cs:301,354`), but not config import — so today the ONLY way to
create a scenario-profile source is `POST /api/config/import`, and a client that
wants one source has to build and submit a whole config document (and the
assistant cannot create one at all).

The failure mode if you work around it with a positive rate is worse than the
rejection. `GeneratorGrain.StartAsync` arms its tick timer for any
`EventsPerSecond > 0` (`orleans/src/StreamForge.Host/Grains/GeneratorGrain.cs:59-66`),
and `MarketDataProfiles.GenerateEvent`
(`shared/StreamForge.AppCore/Generators/MarketDataProfiles.cs:52`) has no
`"scenario"` arm, so it falls through to `default: // generic` (`:206-216`), which
honours the declared schema and fills it with random values. The source then
accumulates random rows shaped exactly like path rows, interleaved with the real
ones a run produces — a silent wrong-data outcome, not an error.

Workaround in the demo: `apps/websites/otc-terms/lib/streamforge/provision-doc.ts`'s
`buildMcGenSource()` is submitted through `/api/config/import` — at provisioning
time, and again from `lib/mc-run.ts` (`syncGenSource`) whenever a run needs a
custom per-desk vol map, since vol lives on the instrument list and is therefore
a catalog change rather than a per-run override.

## 18. `POST /api/sources/{name}/run` should support `rows: false` / a summary-only response

`shared/StreamForge.Api/Endpoints/SourceRunEndpoints.cs:63` — the run endpoint
always echoes the whole generated batch back to the caller
(`ScenarioRunResponse(result.Accepted, result.Rows)`, `:80`). For the demo's
Monte-Carlo that is ~1.3 MB of JSON per simulated day (200 paths × 36
instruments), all of it discarded: the rows the caller actually wants are already
in the engine — only a runtime can publish, which is the whole reason
`RunSourceAsync` sits on `ICatalogFacade` (#8) — and the client reads them back
through the materialized tables.

Wanted: an opt-out flag (`rows: false` on `ScenarioRunRequest`) or a summary
response carrying just `{ runId, rowsGenerated, days, batches }`. Either shape is
fine; the point is that "emit the batch" and "return the batch" are currently the
same call.

Workaround in the demo: `apps/websites/otc-terms/lib/mc-run.ts` parses the payload
and reads only `accepted`, throwing the rows away. The cost is transfer plus a
JSON parse per day — one `POST …/run` with `step: true` per day, in a loop — and
it is what pushed the presenter's MC beat's settle time up.

## 19. Per-RunId step state should be bounded (TTL, or an explicit `POST …/run/{run_id}/close`)

A `step: true` scenario run keeps its per-RunId cursor in the generator's own
memory: `GeneratorGrain._runStates`, a `Dictionary<string, ScenarioRunState>`
keyed by RunId (`orleans/src/StreamForge.Host/Grains/GeneratorGrain.cs:42`,
populated at `:130-138`), and the identical field in the Dapr flavour
(`dapr/src/StreamForge.Dapr.Host/Actors/GeneratorActor.cs:115`). It is cleared in
exactly two places — `StartAsync` and `StopAsync` (`GeneratorGrain.cs:51,79`;
`GeneratorActor.cs:148,173`). There is no expiry, and no way for a client to say
"this run is finished": a completed run's state is indistinguishable from a
paused one, since stepping past the end is Accepted-with-0-rows rather than a
terminal state that frees anything (`GeneratorGrain.cs:141-145`).

So a long-lived host that serves many runs accumulates one entry per run id for
as long as the activation lives. The state itself is small, but it is unbounded
in the number of runs, which is the wrong shape regardless of the constant.

Wanted, either: a TTL on idle run state, or an explicit close/dispose call
(`POST /api/sources/{name}/run/{run_id}/close`) that removes the entry. The
run-complete case could also free itself, which would cover the common path
without a new endpoint.

Workaround in the demo: none — the demo restarts StreamForge often enough
(`lib/streamforge/rebuild.ts`) that it never bites. Which is exactly why it would
bite a real deployment first: the only thing keeping it invisible here is a
lifecycle no production host has.

## 20. `GET /api/sql/functions` should enumerate the built-in statistical aggregates and mark 2-arg forms

`shared/StreamForge.Api/Endpoints/SqlFunctionsEndpoints.cs:29-33` returns
`{ scalars, aggregates, registeredScalars, registeredAggregates }`, sourced from
`SqlFunctions.BuiltInScalarNames` / `BuiltInAggregateNames`
(`shared/StreamForge.Engine/Sql/SqlFunctions.cs:73-84`). But
`BuiltInAggregates` is still literally `["COUNT", "SUM", "AVG", "MIN", "MAX"]`
(`:80`) — the pre-#10 set. The language moved and this list did not: the parser's
own set is `AggregateNames.All`, which is those five *plus*
`Runtime.StatAggregatorNames.All` (`shared/StreamForge.Engine/Sql/Ast.cs:95-96`),
i.e. `VAR_SAMP`, `VAR_POP`, `STDDEV_SAMP`, `STDDEV_POP`, `VARIANCE`, `VAR`,
`STDDEV`, `STDEV`, `MEDIAN` and `PERCENTILE_CONT`
(`shared/StreamForge.Engine/Runtime/StatAggregators.cs:14-32`), alongside
`COUNT(DISTINCT x)`. All of them compile — verifiable from outside with
`POST /api/tables/validate` (`TablesEndpoints.cs:214`), and the demo's own
`mc_var` table is built on `PERCENTILE_CONT(0.05, pnl_usd)`, `MEDIAN` and
`STDDEV_SAMP`.

Worth noting where the drift came from: `SqlFunctions.cs:71-72` says
`BuiltInScalarNames` "is asserted against the Validator's own set by a test, which
is what keeps the two honest" — the aggregate list has no such test, and it is the
one that drifted.

Why it matters: a console or editor that builds its completion list from this
endpoint cannot offer the statistical aggregates, and has to hardcode them —
which is exactly the drift the endpoint exists to remove (its own doc comment,
`:8-12`, states that purpose).

Second half of the ask: the 2-arg forms need a shape hint — arity, or a signature
string — so a client can complete `PERCENTILE_CONT(p, x)` and `COUNT(DISTINCT x)`
correctly instead of as 1-arg calls. A flat `IReadOnlyList<string>` cannot carry
it, and `p` must be a literal (#10), which a completion list is the natural place
to tell someone.

Workaround in the demo: `apps/websites/otc-terms/app/sql/sql-editor.tsx` reads the
endpoint through the app's own server-side proxy
(`app/api/sql/functions/route.ts`, so the admin token stays out of the browser)
and unions all four lists into its completion set — which picks up the eleven
`registeredScalars` correctly, and picks up nothing for the aggregates. The
statistical aggregates are therefore ALSO listed statically in that file's
`AGGREGATE_FNS`, next to a comment pointing here. When this ships the merge
finds them already present and the static half can go.
