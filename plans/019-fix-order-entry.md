# Plan 019 — FIX order entry: a first-class bidirectional connector

**Status: PLANNED. The design fork is CLOSED — option (b), a first-class bidirectional connector, was
chosen on 2026-08-18.** The plan below is written against that choice; options (a) and (c) are recorded at
the bottom for the record, not as live alternatives.

**Depends on**: 018 (the `fix` format, `shared/StreamForge.Connectors.Fix`, `QuickFIXn.Core` already
carried), 010 (`IInboundTransport`), 014 (`IPolledTransport`, the out-of-core connector project precedent).

## The problem this exists to solve

`NewOrderSingle` out and `ExecutionReport` back travel the **same** FIX session: one TCP connection, one
`SenderCompID`/`TargetCompID` pair, one pair of sequence-number streams. The platform's ingress and egress
are two independent registries with independent config, independent lifetimes and independent teardown —
`InboundTransports` (`shared/StreamForge.AppCore/Transports/InboundTransports.cs:29`) and `SinkTransports`
(`shared/StreamForge.AppCore/Sinks/ISinkTransport.cs:79-80`). Configure a `fix` source and a `fix` sink
against the same venue and you get **two logons with two sequence streams**, which a real counterparty will
reject, and should.

Option (b) is: one entity that declares both halves, registered once, over one live session — the third
seam after `IInboundTransport` and `IPolledTransport`.

## Decisions, and what they cost

### D1. The session lives in the connector grain/actor, because that is where the cluster singleton already is

Plan 019's earlier draft named the cluster-singleton guarantee as an open problem. Reconnaissance closed it:

- There is **no bespoke singleton or leader-election mechanism anywhere in this repo** — no
  `StatelessWorker`, no `IPlacementDirector`, no election. `RegistryGrain`'s "singleton" is a doc-comment
  convention meaning "one well-known key" (`orleans/src/StreamForge.Host/Grains/RegistryGrain.cs:24`).
- The sink publisher services are **plain `BackgroundService`s, not grains or actors** — Orleans
  `orleans/src/StreamForge.Host/Services/NatsPublisherService.cs`, Dapr
  `dapr/src/StreamForge.Dapr.Host/Streaming/NatsSinkPublisherService.cs`. Nothing guarantees one of them
  cluster-wide; that is a non-issue today only because both flavours' documented topology is single-instance
  (`orleans/ARCHITECTURE.md:93-95`).
- `ConnectorGrain` is keyed by source name and Orleans guarantees at most one activation per key
  (`orleans/src/StreamForge.Host/Grains/ConnectorGrain.cs:84-86`); `ConnectorActor` gets the same from Dapr
  actor placement (`dapr/src/StreamForge.Dapr.Host/Actors/ConnectorActor.cs:76-77`).

So: **the FIX session is owned by the connector driver, which already holds the inbound half, and the
outbound half is reached through it.** The singleton requirement is satisfied by the shape already in use
rather than by machinery this plan would have to invent and then be trusted with. Putting the session in the
egress layer instead would place an order session inside the one component with *no* single-instance
guarantee — the worst available choice, and it is available today by accident.

**Cost — corrected after waves C and D measured it**: the draft of this plan claimed order throughput would
be "bounded by one grain's turn rate", on the assumption that a send travels *through* the driver. It does
not. The driver **owns** the session; the send **finds** it — the proxy sink resolves the live session from
the process-local `DuplexSessions` map and calls `SendAsync` directly, so no grain turn is on the send path
at all (see D2, and `DuplexSessions`' own doc, which names routing sends through the grain as the
*upgrade* path, not the built one). Ordering is therefore whatever the session itself imposes, which for a
FIX session is exactly what sequence numbers already demand.

The real cost is the one `DuplexSessions` states: the map is process-local, so the proxy and the
session-holding grain must be in the same process. Both flavours' documented topology is single-instance,
so that holds today; on a multi-silo deployment `Find` returns null in the process that does not hold the
session, and **that** is when the grain-call hop — with its turn-rate bound — has to be paid.

### D2. Egress reaches the session through a stateless proxy sink, not a second connection

The outbound half is exposed as a sink kind whose `ISinkClient` **holds no connection**: it resolves the
connector grain/actor by the source name in its `SinkSpec` and forwards. Everything downstream keeps
working unchanged — `SinkSpec` still lives on `PipelineDefinition.Sinks`
(`shared/StreamForge.Contracts/Models.cs:442`) and `TableDefinition.Sinks` (`Models.cs:568`), `SinkSelection`
still decides eligibility (`shared/StreamForge.AppCore/Sinks/SinkSelection.cs:28-29`), `SinkFanout` still
dispatches (`shared/StreamForge.AppCore/Sinks/SinkFanout.cs:43-64`).

This is what makes the 30-second signature churn harmless. `SinkSelection.Signature` is a content hash of
the active sink list (`SinkSelection.cs:40-41`), compared on every refresh tick, and any field edit tears
the client down and rebuilds it (`NatsPublisherService.cs:114-125` for pipelines, `:199-210` for tables).
Tearing down a proxy that owns nothing costs nothing. Tearing down a live FIX session on an unrelated
config edit — which is exactly what option (a) would have done — re-logs-on an order session mid-flight.

**Cost**: two entities describe one connection (a source declaring the session, a sink pointing at it by
name), and a sink whose named source does not exist, or is not a duplex kind, must be a **validation
error at save time**, not a runtime surprise. That check is the wave's job, in `SinkTransports.Validate`
(`ISinkTransport.cs:145-169`).

### D3. Loud failure, within a never-throw contract — and the exact ceiling

`ISinkClient.PublishAsync` **never throws** and must not block past ~3s (`ISinkTransport.cs:6-13,24`), and
`IBatchSinkClient` states plainly that a batch "does not buy reliability, acknowledgement or retry"
(`shared/StreamForge.AppCore/Sinks/IBatchSinkClient.cs:13-14`). Callers await with no try/catch. That is not
negotiable from inside this plan — four call sites depend on it (`NatsPublisherService.cs:158,249`,
`NatsSinkPublisherService.cs:194,225`).

So "fail loudly" is delivered where an operator actually looks, not by throwing:

1. An order the session could not accept (not logged on, queue full, session mid-reconnect) is **never
   silently counted and forgotten**. It is recorded with its `ClOrdID` and the intended sequence number, the
   connector's runtime status goes to a distinct failed state, and the console shows it red.
2. The outbound half exposes a **rejects stream** — `35=3` (Reject) and `35=9` (OrderCancelReject) arriving
   on the inbound half are first-class operator-visible outcomes, not rows quietly appended to a table.
3. A duplex sink may declare `requireSession: true`, which makes a down session a **validation-time refusal
   to start the pipeline** rather than a runtime drop.

**The ceiling, stated rather than papered over**: this is still at-most-once at the seam. An order handed to
a live session and lost in the socket is the venue's resend problem, not something this design detects. Any
guarantee stronger than that needs an order state machine, which the last section of this plan explicitly
refuses to build.

### D4. A third registry, additively

`DuplexTransports` joins `InboundTransports` and `PolledTransports`, same static-list shape and same
rationale (`InboundTransports.cs:11-16`: DI cannot reach the connector driver, whose container is the
runtime's, not the host's). `TransportCatalog` is today `(Inbound, Outbound)`
(`shared/StreamForge.AppCore/Transports/TransportDescriptor.cs:140-142`) and `GET /api/transports` merges
the two inbound registries into one list precisely so the console need not know there are several
(`shared/StreamForge.Api/Endpoints/TransportsEndpoints.cs:34-48`). The draft of this decision said a duplex kind
would appear in **both** lists flagged `Duplex = true`. **Wave B corrected it, and its reading is the
better one**: the flag means "this kind implements `IDuplexTransport`", which is true of an inbound kind
like `fix` and false of the generic outbound proxy sink, since one proxy can point at any duplex source.
So `fix` carries the flag in `Inbound`, the `duplex` sink kind is an ordinary entry in `Outbound`, and both
existing kind pickers (`web/src/pages/SourcesPage.tsx:383-384`,
`web/src/components/SinksEditor.tsx:44-54`) keep working with no restructuring.

`SourceValidation.IsKnownKind` and its unknown-kind error string gain the third registry
(`shared/StreamForge.Api/Endpoints/SourceSchemaService.cs:32-33,64-72`).

### D5. Sequence-number persistence stops being optional

Plan 018 defaults a market-data session to `ResetOnLogon=Y` and an in-memory store because losing the count
costs some re-sent quotes (`shared/StreamForge.Contracts/ConnectorModels.cs:157-199`, store chosen at
`shared/StreamForge.Connectors.Fix/FixMessageSource.cs:72-74`). On an order session the store **is** the
record of what was sent. So for a duplex FIX session: `StorePath` is required, `ResetOnLogon` defaults to
false, and validation refuses the in-memory combination outright — with the message saying why, because a
silent default here is a gap the venue resolves by its own rules.

A documented recovery procedure ships with it: what an operator does after a container restart loses the
store, and what a resend request the platform cannot answer looks like from the venue's side.

### D6. A FIX dictionary, at last — outbound only

Plan 018 deliberately ships no dictionary (`UseDataDictionary=N`), and for inbound parsing that ceiling is
honest: unknown tags become `tag<N>` strings and groups are framed structurally. Outbound it is not —
required-field validation per `MsgType` is not optional when the message is an order. The dictionary is
introduced for **outbound construction and validation only**; the inbound parser
(`shared/StreamForge.AppCore/Connectors/Formats/FixParser.cs`) keeps its dictionary-free behaviour
unchanged, so no existing test moves.

### D7. Order identity and execution-report correlation are ordinary platform features

`ClOrdID` generation and uniqueness, and the `OrigClOrdID` chain for cancel/replace, are new — the platform
has no notion of an entity that is amended rather than appended.

Correlation, though, is something this platform is unusually well shaped for: `ExecutionReport`s arrive on
the inbound half as rows, so matching them to the orders that caused them is a **table keyed by `ClOrdID`**
written in the platform's own SQL, not bespoke join machinery. That is the plan's cheapest large win and it
should be an acceptance artefact, not a footnote.

### D8. Contracts — the exact next free ids

Verified against the current files, additive-only per hard rule 1 and the assembly's own doc
(`shared/StreamForge.Contracts/ConnectorModels.cs:3-7`):

| Type | Highest today | **Next free** |
|---|---|---|
| `ConnectorConfig` | `[Id(8)] Fix` (`ConnectorModels.cs:86`) | `[Id(9)]` |
| `SinkSpec` | `[Id(7)] Loopback` (`ConnectorModels.cs:226`) | `[Id(8)]` |
| `SourceDefinition` | `[Id(12)] Scenario` (`Models.cs:85`) | `[Id(13)]` |
| `FixSourceConfig` | `[Id(13)] QueueCapacity` (`ConnectorModels.cs:198`) | `[Id(14)]` |
| `PipelineDefinition` | `[Id(12)] Sinks` (`Models.cs:442`) | `[Id(13)]` |
| `TableDefinition` | `[Id(29)] KeyFields` (`Models.cs:729`) | `[Id(30)]` |

Note for the wave that touches `web/src/api/types.ts` (headed "FROZEN API CONTRACT", `types.ts:1-4`): the
TS `SinkSpec` is already a **partial** mirror — `db`/`http`/`loopback`/`name` exist server-side and are
absent there (`types.ts:157-163`), and `SinksEditor.tsx:75` builds sink config dynamically off the
descriptor. The duplex kind should follow that dynamic path rather than widening the typed mirror.

### D9. Lineage gains an outbound edge — the first one

The lineage graph has three node kinds, `'source' | 'pipeline' | 'table'`
(`web/src/pages/LineagePage.tsx:31`), and edges built only from declared *inputs*
(`LineagePage.tsx:51-70`). **Sinks do not appear on it at all** — for any entity, today. A duplex connector
is the first thing whose whole point is that data leaves through a node that is already drawn, so it gets
the first outbound edge: pipeline/table → source. Every existing sink kind gets the same edge for free once
the direction exists, which is a genuine improvement this plan pays for and everything else inherits.

## Waves

Verification gate on every wave: `~/.dotnet/dotnet build` + `test` both solutions green with **no
pre-existing test modified**, `cd web && bun run build` when `web/` is touched, `bun test admin/` when
`admin/` is touched, and a live check on isolated 6xxx–9xxx ports with the instance killed and its temp data
dir removed afterwards. One logical change per commit; commit between waves. Shared files
(`ConnectorModels.cs`, `Models.cs`, `types.ts`, both `.sln`) are pre-assigned to one owner or edited by the
orchestrator between waves.

| Wave | What | Model |
|---|---|---|
| 019-A | `IDuplexTransport` + `DuplexTransports` registry + descriptor `Duplex` flag + `TransportCatalog`/`/api/transports`/`SourceValidation` wiring. A fake duplex kind proves the seam, following `TransportRegistryTests.cs:35-39`'s static-ctor pattern | Sonnet 5 high |
| 019-B | The proxy sink client + `SinkTransports.Validate` refusing a sink whose named source is missing or not duplex; `SinkFanout`/`SinkSelection` untouched | Sonnet 5 high |
| 019-C | Orleans: `ConnectorGrain` owns the duplex session; the send path, the failed-state status, and the turn-rate measurement of D1 | Sonnet 5 high |
| 019-D | Dapr: the same in `ConnectorActor`. **The draft's parenthetical — "no generation counter needed, turns run to completion" — was wrong**: it holds for the actor's own state transitions but not for the background subscribe loop, whose async disposal runs outside every turn and can land after a restart has already published a new session. `DuplexSessions.Withdraw`'s identity check is what closes it | Sonnet 5 high |
| 019-E | `shared/StreamForge.Connectors.Fix`: `IFixMessageSource` gains a send side, `ToApp` stops being a no-op (`FixMessageSource.cs:293-295`), `FixDuplexTransport` registered alongside the inbound one | Sonnet 5 high |
| 019-F | The outbound FIX dictionary (D6) + required-field validation + `ClOrdID`/`OrigClOrdID` (D7) | Sonnet 5 high |
| 019-G | Mandatory sequence persistence + validation + the documented recovery procedure (D5) | Sonnet 5 high |
| 019-H | Console: duplex kind in both pickers, the rejects view, the failed-state badge | Sonnet 5 high |
| 019-I | Lineage outbound edges (D9) + `TRANSPORTS.md`/`AGENTS.md`/plan docs + the drop-copy acceptance test | Sonnet 5 high |

A → (B ∥ C ∥ D) → E → (F ∥ G) → H → I. C and D are disjoint by flavour.

## Verification

- **Drop-copy reconciliation**, the headline acceptance test: against a QuickFIX/n acceptor on an isolated
  7xxx port (the retry-across-range pattern from 018, macOS AirPlay holds `:7000`), send N orders through a
  duplex connector, receive N execution reports on its inbound half, and prove the sets match **by a
  platform SQL table keyed on `ClOrdID`** per D7 — not by ad-hoc test code.
- **One session, proven**: assert exactly one logon reaches the acceptor while both a source and a sink
  referencing it are active, and that editing an unrelated field on the sink (which provably re-signs and
  rebuilds the client, `SinkSelection.cs:40-41`) does **not** produce a second logon.
- **The belated-dispose race, proven** — waves C and D both pinned it, and the answer surprised the plan:
  actor-turn serialisation does **not** cover it. `StopAsync` cancels the subscribe loop's token and
  returns without awaiting its unwind, so a stop-then-immediate-start can have the old session's
  `DisposeAsync` land after the new session has already published. What closes it is `DuplexSessions`'
  compare-and-remove on reference identity, and both flavours now force the race deterministically rather
  than hoping to observe it.
- **Sequence persistence across restart**: kill and restart the instance, and assert the session resumes at
  the stored sequence numbers rather than resetting.
- **Loud failure**: stop the acceptor, publish an order, and assert the `ClOrdID` appears in the failed
  record and the connector's status is the failed state — no silent counter.
- **Regression**: the registered-kinds assertion in
  `shared/StreamForge.Connectors.Database.Tests/DatabaseConnectorsTests.cs` and
  `TransportRegistryTests.cs`'s catalog-shape test both still pass unmodified.

## Not in this plan

Pre-trade risk checks; an order state machine beyond what the counterparty reports; FIX 5.0 per-message
application-version negotiation; anything resembling an OMS. This is a transport for order flow, not a place
to keep orders.

## The fork, for the record

- **(a) A shared process-global session manager** keyed by `(host, port, sender, target, beginString)`.
  Smallest diff, rejected: the session's lifetime would be owned by neither of the two things that appear to
  own it, and a sink edit that changes the key would re-logon an order session mid-flight — an intermittent
  failure in the session layer of a production order path.
- **(c) A sidecar** speaking NATS on the inside, exactly how plan 014 consumes Debezium. Cheapest, rejected
  here because it puts a broker hop in the order path and teaches the platform nothing; it remains the right
  answer for anyone who does not need in-process order flow.
- **(b) chosen**: a first-class bidirectional connector. It is the option that leaves the platform with a
  new capability rather than a workaround, and D1 showed its hardest-looking requirement — the cluster
  singleton — is already satisfied by the driver shape.
