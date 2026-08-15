# Wishlist from the ac-co OTC Terms demo

Collected while building the hedge-fund demo (ac-co.ai-4 `apps/websites/otc-terms`
+ `apps/office-addins/otc-addin`) against StreamForge. Ordered by value; every
item is small by design.

Each section keeps the original report and records what shipped underneath it —
including the places where the report turned out to be describing half of
something. **Done: 1–7, 10, 11, 12. Half done: 14. Open: 8, 9, 13, 15.**

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

## 8. Parametric, seedable "scenario generator" source with run-on-demand

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

## 9. Bounded feedback loop (instead of recursive SQL) — also the "scenario clock"

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

## 13. Explicit key retraction through ingest

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

## 14. ◐ BUG: an aggregate created over an already-populated `LATEST BY` table collapses to zero

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

## 15. Retract/assert of one upstream change should be applied atomically downstream

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
