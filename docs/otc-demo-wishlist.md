# Wishlist from the ac-co OTC Terms demo

Collected while building the hedge-fund demo (ac-co.ai-4 `apps/websites/otc-terms`
+ `apps/office-addins/otc-addin`) against StreamForge. Ordered by value; every
item is small by design.

**All six are now done.** Each section keeps the original report and records what
shipped underneath it, including the one place the report turned out to be
describing half a bug.

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

## 7. Console SQL editor: `LATEST BY` (and friends) parsed as a table alias

`web/src/components/sqlScope.ts:147` — `CLAUSE_KEYWORDS` lacks `LATEST`,
`UNNEST`, `UNION`, `IN`, `EXISTS` (all reserved in
`shared/StreamForge.Engine/Sql/Parser.cs`), and `web/src/components/SqlEditor.tsx`
`KEYWORDS` lacks `LATEST`/`UNNEST`/`IN`/`EXISTS`. So `FROM trades LATEST BY (id) WHERE `
reads `LATEST` as an AS-less alias for `trades` and column completion after
`WHERE` yields nothing — and every table-mode CDC mirror is written exactly that
way. Fix is additive: add the five words to `CLAUSE_KEYWORDS` and the four to
`KEYWORDS`. Found while porting the editor into the ac-co demo's `/sql` page
(`apps/websites/otc-terms/app/sql/sql-scope.ts` carries the patched set).
