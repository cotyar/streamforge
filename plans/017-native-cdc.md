# Plan 017 — Native .NET CDC

**Status: DONE.** Waves A–G landed: `CdcStamp`/`CdcLsn`, `MsSqlCdcPlanner`/`MsSqlCdcSource`,
`PgRelationCache`/`PgTupleDecoder`/`PgCdcSource`, `CdcPreflight` with real bodies, both kinds registered,
`docs/cdc.md`, and wave G's live-DB suite in `Tests/Integration/**`. Suites green at `cbbe5b0`:
Orleans 2424, Dapr 695, excluding the 52 `DockerGate` tests, which skip without a local `postgres:17` /
`mcr.microsoft.com/mssql/server:2022-latest` image.

## Why

Plan 014 cut the embedded CDC reader — its cut #1 — and consumed Debezium's envelope over NATS instead.
That was the right call at the time: a real CDC reader means Postgres logical replication (a slot that
stops being drained pins WAL until the *source* database's disk fills), MSSQL's entirely different
capture-table model, LSN durability, and single-writer coordination across replicas, and plan 014 had none
of the primitives that make any of that safe.

017 reverses that cut for the two databases this connector already speaks natively, because **three of
014's four objections turned out to already be paid for by 014's own waves**, not because the objections
were wrong:

- *LSN durability* — 014's `PolledBatch.Cursor` is already durable, opaque, driver-persisted storage. An
  LSN is just one more string that fits it; nothing new had to be built to hold one.
- *Single-writer coordination across replicas* — an Orleans grain and a Dapr actor are both
  single-activation by construction (see `AGENTS.md` hard rule 3 on grain reentrancy), so there is only
  ever one process in this platform trying to stream a given slot or read a given capture instance at a
  time. Postgres's own "replication slot … is already active for PID …" error is the backstop underneath
  that, not the primary defense — belt over suspenders, not a mechanism this plan had to invent.
- *MSSQL's capture-table model* — turned out, on inspection, to be pull-shaped: `cdc.fn_cdc_get_all_changes_*`
  is a table-valued function over an LSN range, which is a near-exact fit for `IPolledTransport` as it
  already existed. `MsSqlCdcSource`'s own class doc calls this out directly: "this is genuinely the cheap
  half of plan 017" — no subscription to keep alive, no relation cache, no tuple decoder, just scalar LSN
  functions plus a bounded read.

**The fourth objection — an undrained slot pinning WAL — is NOT solved by architecture, and this plan does
not pretend otherwise.** Nothing in `IPolledTransport`, in Orleans, or in Dapr makes a forgotten, still-open
replication slot stop consuming WAL on the source database. That hazard is handled the only way a hazard
outside this platform's process boundary can be handled: a preflight probe that tells the operator about it
before they turn the source on (`CdcPreflight.ProbePostgresAsync`'s WAL-lag-against-`max_slot_wal_keep_size`
check), and documentation that says so in the one place an operator under time pressure will actually read
it — the descriptor `Help` text (see `TRANSPORTS.md`'s "Change data capture" section). An operator who
deletes a source without stopping it first, or who points `max_slot_wal_keep_size` at "unlimited" and walks
away, can still fill the source database's disk. This plan narrows the odds of that happening by surfacing
it loudly; it does not make it impossible.

Debezium-over-NATS is not replaced by any of this — see "Decisions" below.

## Decisions, and what they cost

**`DbSourceConfig` grows additively, rather than `ConnectorConfig` growing a class-typed `Cdc` property.**
The latter is the shape 014's own Contracts wave used for `Db` itself, and it is the more "correct" OOP
answer — a CDC source's config genuinely is not a polled source's config. It was rejected anyway, because
`SecretWalkTests` reflects over every class-typed property of `ConnectorConfig` and fails when a new one is
not populated in `FullConnector()` — exactly the guard plan 014 added on purpose, and exactly the guard this
plan's file-ownership brief forbids touching (`DatabaseConnectorsTests.cs` is the only pre-existing test file
this wave may edit, and `SecretWalkTests.cs` is not it). Reusing `DbSourceConfig` costs eight inert fields
per non-CDC kind — `SlotName`/`PublicationName`/`CaptureInstance`/`Tables`/`MaxPollMs`/`CreateSlotIfMissing`
sit unused on a plain `postgres` or `mssql` source, and `CursorColumn`/`Query`/`Where` sit unused (and are
actively rejected by `Validate()`) on a CDC source. That inertness is hidden from the console by each kind's
own `TransportDescriptor` — the fields simply are not in the list — so the cost is paid once, by a reader of
the raw `DbSourceConfig` class, not by every operator who opens the form.

**Connection per poll cycle, confirming the previous cursor on open (Postgres) — not a persistent
replication session.** `IPolledTransport` is a singleton per kind with no "this source was deleted"
notification, so a cached, long-lived replication connection would keep a slot open for a source that no
longer exists in the catalog — an undrained-but-held slot pins WAL exactly as if nobody had stopped the
source at all. Opening a fresh `LogicalReplicationConnection` every cycle makes that bug impossible by
construction: nothing is left running between cycles to leak. It also makes durability correct by
construction the other way — `PgCdcSource.PollAsync` confirms the cursor `PolledSourceCore` has *already
persisted*, before it starts streaming the next batch, so it can never acknowledge to Postgres data
StreamForge has not durably recorded. `MsSqlCdcSource` shares the connection-per-cycle shape (a fresh ADO.NET
connection per `PollAsync`) though it pays no confirm-handshake cost, since a capture table has no session
to leak the way a replication slot does. The cost, paid on the Postgres side specifically: a real network
handshake every cycle, and a latency floor at the schedule interval rather than sub-second tailing. The
named follow-up is a persistent replication session, kept alive across cycles and torn down explicitly on
source deletion — deferred because it needs the deletion-notification hook `IPolledTransport` does not have
today, which is a bigger seam change than this plan's scope.

**A batch always ends on a transaction boundary, in both dialects.** This was forced, not chosen, by a
concrete SQL Server case found during wave C/E: a `TOP (@batch)`-capped read can end mid-transaction, and
`rawRows.Count` alone cannot tell "this transaction had exactly this many rows" from "`TOP` cut it off here"
— both look identical from inside `MsSqlCdcPlanner.Complete`. Treating a `TOP`-truncated group as complete
and advancing the cursor past it is silent, permanent data loss — the exact failure `PolledSourceCore`'s
"failed cycle keeps the old cursor" rule exists to prevent, except this failure would not even trip that
rule, because nothing failed; it would just be wrong. The fix: when the only group produced by a capped read
might be incomplete, emit nothing, and re-read that one transaction bounded exactly to its own
`__$start_lsn` with no `TOP` at all — a read that can only ever return that transaction in full. Postgres
gets the same rule from the other direction: `PgCdcSource.DrainAsync` buffers rows per transaction (from
`BeginMessage` to `CommitMessage`) and only moves them into the emitted batch, cursor advanced to
`CommitMessage.TransactionEndLsn`, once the COMMIT is actually seen — rows under a transaction whose COMMIT
this cycle never reaches are discarded from the buffer, not emitted, because emitting an uncommitted
transaction is worse than holding it (it could still abort) and confirming a cursor past it would be an
unrecoverable gap. **`BatchSize`/rows-per-poll is therefore a target for a cycle's read, not a ceiling on
what one batch can emit** — a transaction bigger than the target is always delivered whole, over budget,
once resolved. The cost: one extra round trip on SQL Server the cycle a transaction happens to straddle the
cap, and up to one transaction's worth of overshoot on Postgres.

**Closed in follow-up: `ConnectorRuntimeStatus.EnvelopeSkippedTotal` now has a wire from a polled
transport.** `PolledBatch` grew a fourth, defaulted field, `EnvelopeSkipped` (0 for every pre-existing
construction, including every non-CDC `IPolledTransport`, which keeps `DbSource` and every polled test
compiling untouched) — the same trailing-defaulted-parameter shape `PollCycleResult` already used to solve
this exact "count something without touching a frozen contract" problem for the Debezium-envelope path.
`PolledSourceCore.RunCycleAsync` folds it into the `PollCycleResult` it returns in one expression, and
`ConnectorGrain`/`ConnectorActor` needed zero changes — they already read `PollCycleResult.EnvelopeSkipped`
and had since the field existed. `PgCdcSource` now increments a per-cycle counter in `DrainAsync`'s
`default` switch arm — the same branch that was already catching `TRUNCATE`/`TYPE`/`ORIGIN`/logical-decoding
messages and letting the cycle continue, just not counting them — and returns it on the batch.
`MsSqlCdcSource` stays at 0 deliberately, not by omission: its read uses the CDC `'all'` row filter, under
which `__$operation` is documented to be only 1/2/4, so there is no unrepresentable-message case reachable
the way pgoutput's is; the one way that contract could break (`__$operation` outside 1/2/4) already fails
the cycle loudly through `MsSqlCdcPlanner.Complete`'s own throw rather than skipping silently, which is a
stricter guarantee than a counter would add. Neither reader counts a row its own `Tables` filter excluded —
that is the operator's configuration doing what they asked, not an unrepresentable event, and counting it
would turn a working filter into permanent alarming noise.

## Waves

Every wave gates on both solutions building and testing green (Orleans and Dapr suites, filtering out
`Tests/Integration/**`, which needs live servers), plus a live check on isolated ports for any wave that
touches host wiring. Every wave ran on **Sonnet 5 high**, maximum effort — including the two hardest
(D's replication loop, C's cursor arithmetic), on the argument that a brief pinning the exact API surface
and the exact failure modes buys more correctness here than a larger model reasoning from a vaguer one.

### Round 1 — two fully parallel waves, zero shared files

| Wave | Owns | Delivers | Model |
|---|---|---|---|
| **A · Contracts** | `shared/StreamForge.Contracts/ConnectorModels.cs` | `SourceKinds.PostgresCdc`/`MsSqlCdc`, and the six CDC fields added additively to the EXISTING `DbSourceConfig` (`SlotName`, `PublicationName`, `CaptureInstance`, `Tables`, `MaxPollMs`, `CreateSlotIfMissing`) as `[Id(18)]`–`[Id(23)]` | Sonnet 5 high |
| **B · CDC primitives** | `CdcStamp.cs`, `CdcLsn.cs` + their tests | `CdcStamp` — the one place that writes `_op`/`_weight`/`_ts`/`_table` and the unchanged-TOAST sentinel, reusing `CdcEnvelope`'s vocabulary on purpose so a native row is indistinguishable downstream from a Debezium-fed one. `CdcLsn` — the LSN codec for both dialects, including a byte-wise MSSQL comparison (a 10-byte LSN parsed as `ulong` silently loses its two high bytes and misorders exactly at a byte-boundary carry). Pure, no I/O | Sonnet 5 high |

**Between rounds, the orchestrator placed `CdcPreflight.cs`** with its two method signatures and
`NotImplementedException` bodies. C and D delegate their `ISchemaProbe` to it while E fills it in, so all
three could compile concurrently — the "pre-built by the orchestrator" case AGENTS.md's wave discipline
names for a contract two parallel agents must meet in the middle.

### Round 2 — three fully parallel waves, disjoint files

| Wave | Owns | Delivers | Model | Depends |
|---|---|---|---|---|
| **C · MS SQL reader** | `MsSqlCdcPlanner.cs`, `MsSqlCdcSource.cs` + 2 test files | The capture-table reader. The planner is pure and holds the LSN arithmetic, the retention-breach check and the transaction-boundary cut; the source is the I/O shell around it | Sonnet 5 high | A, B |
| **D · Postgres reader** | `PgRelationCache.cs`, `PgTupleDecoder.cs`, `PgCdcSource.cs` + 2 test files | The logical-replication reader — one `LogicalReplicationConnection` per cycle, confirm-before-stream, transaction-buffered drain. Relation cache and tuple decoder split out pure so the parts that lose data are testable without a server | Sonnet 5 high | A, B |
| **E · CDC preflight** | `CdcPreflight.cs` bodies + tests | The two `ISchemaProbe` bodies. `wal_level`, slot activity and WAL lag against `max_slot_wal_keep_size`, publication coverage, per-table `relreplident`; `is_cdc_enabled`, the capture-table row, the retention window in wall-clock terms, and Azure SQL Database recognized so a missing Agent is reported rather than flagged | Sonnet 5 high | A, B |

**Found during implementation, wave C.** The first cut of `Complete` emitted a single `__$start_lsn` group
and advanced the cursor past it even when `TOP (@batch)` had truncated that group — the tail of an
oversized transaction was then permanently behind the cursor, with nothing reporting it. Silent data loss,
in the one connector that must never produce it. The fix is that `Complete` takes the caller's explicit
"this read was capped" flag and, for a capped single group, emits nothing and instead asks for a re-read
bounded at that group's own LSN with no `TOP` — the range then ends exactly at that transaction, so it
comes back whole. Deliberately over budget: **`BatchSize` is a target, not a ceiling.**

### Round 3 — two parallel waves

| Wave | Owns | Delivers | Model | Depends |
|---|---|---|---|---|
| **F · Registration + docs** | `DatabaseConnectors.cs`, `DatabaseConnectorsTests.cs`, one string in `DbSource.cs`, `TRANSPORTS.md`, `plans/README.md`, `AGENTS.md`, this document | Both kinds reachable by an operator, and plan 014's never-run docs wave (`TRANSPORTS.md` had no database or CDC section at all) finally written | Sonnet 5 high | A–E |
| **G · Live DB integration** | `Tests/Integration/**` | End-to-end against real servers on the plan-014 Docker harness: the three operations, monotonic cursor, resume-with-no-gap across a discarded source object, a failed cycle leaving the cursor untouched, replica-identity fidelity, and the oversized-transaction regression guard for wave C's bug | Sonnet 5 high | A–E |

## Design detail pinned across agents

- `PolledBatch(Rows, Cursor, HasMore)` was unchanged from plan 014 through the end of this plan's own waves
  — CDC did not get its own richer return shape at the time. A follow-up widened it additively with
  `EnvelopeSkipped` (default 0); see "Closed in follow-up" above.
- `DbSourceConfig`'s CDC-only fields are additive, next free `[Id(n)]` (`SlotName` = 18 through
  `CreateSlotIfMissing` = 23) — see "Decisions" above for why this class rather than a new one.
- `Kind` is fixed per class, never derived from `ISqlDialect.Kind` the way `DbSource.Kind` is:
  `PgCdcSource.Kind => SourceKinds.PostgresCdc` and `MsSqlCdcSource.Kind => SourceKinds.MsSqlCdc`, always —
  the dialect underneath a CDC reader is still Postgres or SQL Server, but "postgres-cdc"/"mssql-cdc" is the
  polled kind's own identity, not a derived one.
- `Validate()` on both CDC sources actively rejects the polled-only fields it inherits for free by reusing
  `DbSourceConfig` (`cursorColumn`, `cursorKind`, `query`, `where` on Postgres; the reverse on MSSQL for
  `captureInstance`) — a copy-pasted polled config is caught at validation time, not silently ignored.
- Row stamping (`_op`, `_weight`, `_ts`) goes through one shared helper, `CdcStamp`, on both dialects,
  deliberately reproducing `CdcEnvelope`'s op letters and weight sign — a row from the native path and a row
  from the Debezium-envelope path are indistinguishable downstream, so an operator's `LATEST BY <key> WHERE
  _op <> 'd'` table SQL does not need to know or care which path produced a given row.
- `postgres-cdc` refuses `Snapshot` in `Validate()` with a message that names the fix (`postgres`, then
  switch); `mssql-cdc` accepts `Snapshot` but its own `Describe()` Help text is explicit that it means
  "replay what CDC retention still has," not a full-table read.
- The `ISchemaProbe` bodies are the primary carrier of the WAL-pinning and retention hazards to an operator
  — see "Why" above for why the probe, not the architecture, is what makes those hazards visible.

## Cut, ranked

1. **A standalone `sf-cdc` host publishing to NATS.** Would decouple CDC's connection lifetime from the
   polled-cycle model entirely — a genuinely persistent replication session, no per-cycle reconnect cost —
   at the price of a fifth deployable, its own health surface, and a second way (besides `IPolledTransport`)
   for a source to reach the platform. Rejected because `IPolledTransport` already existed and fit both
   dialects' actual pull-shaped mechanics; a new host would duplicate the driver-cycle/cursor-persistence
   machinery this plan reused for free.
2. **A consistent-cut Postgres snapshot via `EXPORT_SNAPSHOT`.** Would let `postgres-cdc` offer a real
   backfill instead of refusing `Snapshot` outright, by pairing slot creation with a transaction-consistent
   read of the table as it stood at slot-creation time. Costs one connection held open across the whole
   backfill — which contradicts this plan's connection-per-cycle design — and a second read path inside a
   kind that is supposed to be CDC-only. Deferred; the documented two-step (`postgres` for backfill, then
   switch to `postgres-cdc`) covers the same outcome with an operator-visible seam instead of a hidden one.
3. **A persistent replication session** (the follow-up named under "Decisions" above) — kept alive across
   poll cycles instead of reopened every time, cutting the per-cycle handshake and the latency floor at the
   schedule interval. Needs a deletion-notification hook `IPolledTransport` does not have; adding one changes
   a contract three separate connector kinds already implement against.
4. **Ingress retraction into the Engine's Z-sets** — unchanged from plan 014's cut #2. A Debezium delete or
   a native CDC delete both still arrive as `_op = "d"`, `_weight = -1` sitting in the stream, not a real
   retraction; `LATEST BY <key> WHERE _op <> 'd'` hides the key without freeing it. Still a separate Engine
   project against a large frozen-API test suite, not something a connectors-project plan can absorb.
5. **MySQL/Oracle/MongoDB natively.** This plan's whole premise — "the two databases the connector already
   speaks" — does not extend to a database this project has no client for. Debezium-over-NATS remains the
   only route for those, unchanged and undiminished by this plan landing.
6. **SQL Server Change Tracking as a fallback** for an instance where CDC cannot be enabled (some managed
   tiers, or an operator without `sysadmin`). A real, narrower feature — Change Tracking answers "which rows
   changed," not "what did they change to or from" — and would need its own read plan and its own honest
   limits documented. Not attempted; `mssql-cdc`'s preflight instead reports plainly when CDC itself is not
   enabled, and the operator's fallback today is the polled `mssql` kind.
