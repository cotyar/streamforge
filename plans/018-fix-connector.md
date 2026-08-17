# Plan 018 — FIX protocol, one direction: the `fix` wire format and a receive-only session source

**Status: DONE**

**What actually landed (2026-08-17), and where it diverged from the plan as written:**

- **`orleans/tests/StreamForge.Host.Tests/SecretWalkTests.cs` gained one line**, not zero. That fixture is
  a forcing function by its own doc comment — it is built by naming the properties rather than by
  reflection precisely so a NEW container on `ConnectorConfig` that nobody adds here shows up as a
  coverage gap instead of passing vacuously — and wave 014-A made the identical one-line addition
  for `ConnectorConfig.Db` when the database connectors landed. Wave C's `FixSourceConfig` is no
  different: `SecretWalkTests` now also constructs a `ConnectorConfig` with `Fix` set, so the walk covers
  `FixSourceConfig.Password`'s `[Secret]` attribute the same way it already covers every other credential
  field.
- **The acceptance test starts its counterparty acceptor on the first *bindable* port in the 7xxx band**,
  not a fixed one. Two reasons: `StreamForge.Connectors.Fix.Tests` is referenced from both `.sln` files, so
  running the Orleans and Dapr suites back to back binds the same fixed port twice within minutes for no
  reason; and on macOS, ControlCenter's AirPlay Receiver listens on `*:7000` — a port in the reserved
  6xxx–9xxx band is not guaranteed free just because it looks unused. `FixAcceptanceTests` retries the
  *real* `ThreadedSocketAcceptor.Start()` across `7000..7999` on `SocketException`, rather than probing
  with a throwaway `TcpListener` first (a probe binds loopback while QuickFIX/n binds `Any`, so a probe
  that succeeds where the acceptor then fails would be worse than no probe at all).
- **The Dapr flavour's `/api/transports` was not verified live.** Wave D wired `FixConnectors.RegisterAll()`
  into both hosts identically, and the Dapr solution builds and its full test suite is green — but no
  `dapr init` has been run on this machine, and port `5399` is off-limits per this file's own environment
  rules, so the live REST check (`GET /api/transports` showing the `fix` kind, a source created and rows
  landing in a table against an in-process acceptor) ran only against the Orleans flavour. Recorded
  honestly rather than papered over: the Dapr claim rests on identical wiring + a green suite, not on an
  observed live instance.

**Depends on**: 010 (the `IInboundTransport` seam), 014 (`TransportFieldTypes.Text`, the
`StreamForge.Connectors.Database` out-of-core project precedent)
**Explicitly does NOT cover**: order entry. A FIX session that both sends orders and receives execution
reports is [plan 019](019-fix-order-entry.md), and it is a different plan rather than a later wave of this
one — see that document's first section for the three reasons.

## What this delivers

Two things that are useful separately and better together:

1. **`fix` as a first-class payload format** — `FileFormats.Fix`, parsed by `FormatParsers`/`FixParser`
   like `ndjson`/`json`/`csv`. Every existing source kind that names a format gets it for free: a `file`
   or `folder` source replays a FIX log off disk, a `url` source reads one over HTTP, and a `nats` source
   ingests FIX-over-NATS. No new source kind, no new transport, no dependency.
2. **`fix` as a live source kind** — a receive-only FIX session (market data, drop-copy) as an
   `IInboundTransport` in a new out-of-core project `shared/StreamForge.Connectors.Fix`, on
   `QuickFIXn.Core`. It yields the raw FIX bytes off the wire and declares `FormatOf => FileFormats.Fix`,
   so it reuses (1)'s parser and the whole shared mapping/coercion/dedup path with nothing of its own.

(2) depending on (1) is the point. Had the format been done the cheap way — the transport converting FIX
to JSON internally and declaring `FormatOf => "json"` — the live session would work identically, and a
FIX log on disk would still be unreadable by this platform. One extra format constant buys four source
kinds instead of one.

## Decisions

**No FIX dictionary, and no dependency, for the format.** `tag=value` framing is self-describing; the
version-specific parts (which tags exist, what they are called, what type they hold) are handled by a
static table in the parser covering the common FIX 4.2/4.4/5.0 set, with `tag<N>` and `string` as the
fallback for everything else. Tags are globally unique across FIX versions by design — tag 35 is `MsgType`
in every version — so one table is correct rather than a compromise. `QuickFIXn.Core` is a dependency of
the **session** project only; `StreamForge.AppCore` gains nothing.

**Repeating groups are parsed into nested JSON arrays, dictionary-free.** `NoMDEntries=2` followed by two
entries becomes `"MDEntries": [{…},{…}]` — which is what makes `MappingSpec.ItemsPath` (`$.MDEntries[*]`)
fan a market-data snapshot out into one row per quote, using machinery that already exists. The framing
rule needs no dictionary: a counter tag from a known table introduces a group, the tag immediately after
it is the group's delimiter tag, the first entry establishes the group's tag set, and the group ends at
the first tag outside that set. Ceiling, stated and commented in code: **a group whose first entry omits
an optional field that a later entry carries terminates early**, and the trailing fields land on the
parent object. Upgrade path is a real dictionary, which this plan does not build.

**Values are typed from the tag table, not sniffed.** CSV sniffs `long`/`double`/`bool` out of untyped
cells because it has nothing better; FIX has a spec that says tag 44 is a price and tag 55 is a symbol.
Sniffing would turn `55=123` into the number 123. Known numeric tags → number, known FIX-Boolean tags
(`Y`/`N`) → bool, **everything else including every unknown tag → string**. An operator who wants more
has `ConnectorRowCoercion` (declared field types) and 009-C1's SQL conversion functions.

**Length-prefixed fields are honoured.** `RawDataLength(95)` → `RawData(96)` and its siblings (90/91,
212/213, 350/351, 354/355, 358/359, 360/361, 362/363, 364/365) carry a byte count precisely because the
value may contain the delimiter. The parser reads those verbatim by length. This is the same class of
rule as CSV's quoted fields, and skipping it corrupts exactly the messages that are hardest to debug.

**The delimiter is sniffed, following `FormatParsers.SniffDelimiter`'s doctrine.** Real sessions use SOH
(`\x01`); logs, tickets and test fixtures use `|` or `^`. Candidates `\x01`, `|`, `^` counted over the
first frame, highest wins, tie to the earlier, none → `\x01`. Same visible failure mode as CSV's: guess
wrong and the message becomes one unparseable field, obvious on the first poll.

**`fix` is ingress-only.** `FileFormats` is shared with the `file` **sink**, whose format select must NOT
gain it: `CsvFormatter` has no FIX twin, and writing FIX without a session to number the messages produces
something no counterparty will accept. Wave A adds the constant; `FileSinkTransport.Describe()`'s
`Options` array stays `[csv, ndjson]` and `FileSinkClient` is not touched.

**No implicit retraction.** A `35=8` execution report with `150=4` (Canceled) is *not* turned into a
`_weight: -1`. FIX application semantics are the operator's, expressed in SQL; the `CdcEnvelopes`
machinery from 014 exists for anyone who wants a declarative version of it. A FIX source is append-only
like every other source in this repo.

**Session state: in-memory by default, file-backed on request.** QuickFIX/n needs somewhere to keep
sequence numbers. A market-data session normally wants `ResetOnLogon=Y` and no persistence at all —
resending yesterday's quotes is worse than not resending them — so that is the default. Setting
`storePath` switches to `FileStoreFactory`, which is what a drop-copy session that must not lose its
place needs. One optional field, two honest behaviours; note that in a container `storePath` must be a
mounted volume, exactly as `FileSinkClient`'s path already is.

**Post-logon requests are raw FIX text, not a request builder.** A market-data session must SEND a
`MarketDataRequest(V)` to receive anything. `onLogon` is a `TransportFieldTypes.Text` field (added in 014
for precisely this shape of input) holding one raw FIX message per line, delimiter-sniffed the same way
the parser sniffs. It covers `MarketDataRequest`, `SecurityListRequest`, `TradeCaptureReportRequest` and
whatever else a venue wants, at the cost of no templating, no request/response correlation and no
resubscribe-on-reject. A typed request builder is a plan-019-sized decision, not a field.

**Session-level traffic never reaches the row path.** Logon/Heartbeat/TestRequest/ResendRequest/
SequenceReset/Logout are consumed by QuickFIX/n's session layer and never surface to the application
callback, so "receive-only" costs nothing to enforce. `msgTypes` (optional, comma-separated) filters the
application messages that do.

## What the format edit actually touches

The whole point of doing this honestly rather than hiding a JSON conversion inside one transport is that
the edit is bounded and enumerable. It is these ten places and nothing else:

| # | File | Change |
|---|---|---|
| 1 | `shared/StreamForge.Contracts/ConnectorModels.cs` | `FileFormats.Fix = "fix"` |
| 2 | `shared/StreamForge.AppCore/Connectors/Formats/FixParser.cs` | **new** — parser + tag table |
| 3 | `shared/StreamForge.AppCore/Connectors/ConnectorPollCycle.cs` | one arm in `ParseAndExtract`'s switch |
| 4 | `shared/StreamForge.Api/Endpoints/SourceSchemaService.cs` | `KnownFileFormats` + 3 error strings |
| 5 | `shared/StreamForge.AppCore/Connectors/Nats/NatsInboundTransport.cs` | `KnownFormats`, its error string, descriptor `Options` |
| 6 | `web/src/api/types.ts` | `FileFormat` union |
| 7 | `web/src/components/sources/FileFolderConfigEditor.tsx` | `FILE_FORMATS` array |
| 8 | `web/src/components/sources/UrlConfigEditor.tsx` | one `<SelectItem>` |
| 9 | `TRANSPORTS.md` | a "FIX" section |
| 10 | `CLAUDE.md` | one paragraph, matching the CSV/CDC ones |

Not touched, deliberately: `CsvFormatter`, `FileSinkClient`, `FileSinkTransport.Describe()`,
`web/src/components/sinks/*`.

## Feasibility, verified before wave C was written

A scratchpad probe (built and run, then deleted) settled the three things that would otherwise have been
discovered mid-wave:

- **`QuickFIXn.Core` 1.14.1 restores and builds under `net10.0`** (it ships a `net8.0` lib). Latest stable,
  ~2M downloads. Namespaces moved since the tutorials: `MemoryStoreFactory`/`FileStoreFactory` are in
  `QuickFix.Store`, `NullLogFactory`/`ScreenLogFactory` in `QuickFix.Logger`, `SocketInitiator` in
  `QuickFix.Transport`; `Message`, `Session`, `SessionSettings`, `ThreadedSocketAcceptor` and `IApplication`
  are in `QuickFix`.
- **`UseDataDictionary=N` works, so no `FIX44.xml` ships with the platform.** This is the setting that makes
  plan 018's no-dictionary decision hold all the way down: QuickFIX/n does the session layer and no message
  validation, hands the application message over intact, and `Message.ConstructString()` returns the raw
  SOH-delimited wire string — which is exactly the `byte[]` an `InboundMessage` carries and exactly what
  `FixParser` then parses. Without this, a version-specific XML dictionary would have become a deployment
  artifact for every flavour and every container image.
- **An acceptor and an initiator log on to each other in one process**, on a 7xxx port, with
  `StartTime=EndTime=00:00:00` (always-on) and `ResetOnLogon=Y`; an application message sent from the
  acceptor arrives at the initiator's `FromApp` within ~100ms, and both stop cleanly. So wave C's acceptance
  test needs no external venue, no Docker and no recorded capture — the counterparty is a fixture.

Two shapes wave C must therefore follow, both taken from the house style rather than invented:

- **A substitutable session seam**, mirroring `NatsInboundTransport`'s optional `Func<INatsMessageSource>`
  constructor parameter — so most tests drive a fake and only the acceptance test opens a socket.
- **A bounded channel bridging QuickFIX/n's callback threads to `IInboundSubscription`'s
  `IAsyncEnumerable`.** `FromApp` is a synchronous callback on the session's own thread; blocking it applies
  backpressure to the FIX session itself and eventually trips the counterparty's heartbeat timeout, which is
  a worse failure than dropping. Bounded + `DropOldest` + a counter the operator can see, with the capacity
  configurable — correct for market data (a stale quote is worthless), **wrong for drop-copy**, and that
  asymmetry is a stated ceiling, not a bug.

## Waves

Gates as always: `~/.dotnet/dotnet build` + `test` **both** solutions green with no pre-existing test file
modified, `cd web && bun run build` when `web/` is touched, and a live check on isolated 6xxx–9xxx ports
with the instance killed and its temp data dir removed. One logical change per commit.

| Wave | What | Files owned | Model |
|---|---|---|---|
| 018-A | The `fix` format: `FixParser` + tag/type/group/length tables, the switch arm, both validators, the NATS descriptor. Unit tests over real captured messages: a `35=W` snapshot with `NoMDEntries`, a `35=8` execution report, a `35=X` incremental refresh with nested `NoPartyIDs`, a `RawData` payload containing the delimiter, `|`-delimited and SOH-delimited inputs, a nested group, an unknown tag, a malformed frame. | items 1–5 | Sonnet 5 High |
| 018-B | Console: the format appears in the url/file/folder pickers with an honest one-line description. | items 6–8 | Sonnet 5 High |
| 018-C | `shared/StreamForge.Connectors.Fix` — `FixSourceConfig` in Contracts (`[Secret]` on `Password`), `FixInboundTransport : IInboundTransport`, `FixConnectors.RegisterAll()`, descriptor. Tests drive a **QuickFIX/n acceptor in-process** as the counterparty: logon, one `35=W`, one `35=X`, a `msgTypes` filter, a mid-session disconnect proving `SubscriberCore` reconnects. | new project + `SourceKinds.Fix` + `ConnectorConfig.[Id(8)] Fix` | Sonnet 5 High |
| 018-D | Host wiring: both `.sln` files, both host csprojs, `FixConnectors.RegisterAll()` beside `DatabaseConnectors.RegisterAll()` in both `Program.cs`. Live check: a source created over REST against an in-process acceptor, rows landing in a table. | sln/csproj/Program.cs ×2 | Sonnet 5 High |
| 018-E | Docs: `TRANSPORTS.md` FIX section (including the group-framing ceiling and the `storePath`/volume note), `CLAUDE.md` paragraph, `plans/README.md` row, this file's status. | items 9–10 | Sonnet 5 High |

A ∥ B (the constant `"fix"` is pinned above, so B needs nothing from A). C after A. D after C. E last.

## Verification

- **Format**: unit suite in `shared/StreamForge.AppCore.Tests` (or the Host test project, wherever
  `FormatParsers` is already covered) over the message shapes listed in wave A. A FIX log file replayed
  through a real `file` source end to end, rows visible in a table.
- **Session**: no external venue and no Docker — QuickFIX/n runs both halves, so the acceptance test is a
  self-contained acceptor on a 6xxx port that logs on, publishes, and disconnects.
- **Isolation**: both core solutions still build and pass **with `StreamForge.Connectors.Fix` removed from
  both `.sln` files** — the same property plan 014 asserts for the database project.
- **No leak**: `GET /api/config/export` on a catalog containing a FIX source shows `***` for `password`.
- **Regression**: `DatabaseConnectorsTests` asserts the exact set of registered polled kinds. FIX registers
  into `InboundTransports`, not `PolledTransports`, so that assertion is unaffected — wave C must confirm
  this rather than assume it, and must not edit that test.

## Deferred, named rather than forgotten

TLS beyond a bare `useSsl` flag (client certificates, CA pinning); FIXT.1.1 / FIX 5.0 application-version
negotiation beyond passing `DefaultApplVerID` through; FAST and SBE encodings (a different wire format,
not a different dictionary); a real FIX dictionary for exact group framing and per-version field types;
scheduled session windows (`StartTime`/`EndTime`, which venues do use); and everything in plan 019.
