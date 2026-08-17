# Adding a transport

A **transport** is how bytes get into StreamForge (a source kind) or out of it (a sink kind). NATS is the
reference implementation of both directions; this document is the recipe for the next one.

Built in today: `nats` inbound and outbound, and (plan 012) a `file` sink that appends rows to a local
file as CSV or NDJSON — the egress twin of the `file`/`folder` source kinds, and the proof that this seam
holds for a destination that is not a broker at all. `FileSinkTransport`/`FileSinkClient` are worth
reading next to the NATS pair: same interfaces, same fire-and-forget contract, a third of the code.

The design goal is stated as a test, not as a promise:
[`TransportRegistryTests`](orleans/tests/StreamForge.Host.Tests/TransportRegistryTests.cs) registers a
transport the repository has never heard of and asserts the platform validates it, masks its credentials,
drives its messages into rows, and offers it in the console. If a hardcoded per-kind branch ever creeps back
in, those tests fail.

---

## What you write, and what you don't

| You write | You do **not** touch |
|---|---|
| A config class in `shared/StreamForge.Contracts/ConnectorModels.cs` | `SecretsMasker` — secrets are found by `[Secret]` |
| One `IInboundTransport` and/or one `ISinkTransport` | `SourceValidation` — transports validate themselves |
| One line in `InboundTransports` / `SinkTransports` | `ConnectorGrain` (Orleans) and `ConnectorActor` (Dapr) |
| — | `NatsPublisherService` / `NatsSinkPublisherService` |
| — | Anything under `web/` — the console builds its form from your descriptor |

Before plan 010 this list was about fourteen places across both runtime flavors. The parts that were only
*incidentally* NATS-specific — the reconnect/backoff loop, payload→row mapping, ack discipline, secret
masking, eligibility filtering, form rendering — are now shared.

---

## Inbound (a source kind)

### 1. The config contract

`shared/StreamForge.Contracts/ConnectorModels.cs`. Additive only: the **next free `[Id(n)]`**, never a
renumber (field numbers are forever — see `CLAUDE.md`).

```csharp
public static class SourceKinds
{
    // …
    public const string Rv = "tibco.rv";
}

public sealed class ConnectorConfig
{
    // …existing [Id(0)]–[Id(6)]…
    [Id(7)] public RvSubConfig? Rv { get; set; }
}

[GenerateSerializer]
public sealed class RvSubConfig
{
    [Id(0)] public string Daemon { get; set; } = "";
    [Id(1)] public string Subject { get; set; } = "";
    [Id(2)] public string Format { get; set; } = FileFormats.JsonArray;
    [Id(3)] public string? Username { get; set; }
    [Id(4)] [Secret] public string? Password { get; set; }   // ← the ONLY thing masking needs
}
```

**`[Secret]` is the whole secrets story.** It makes the value mask as `***` on every read path (GET, list,
config export), and makes a written `***` restore the stored value on PUT. Getting this wrong used to mean a
credential exported in plaintext, silently; now the failure mode is a compile-visible missing attribute, and
[`SecretWalkTests`](orleans/tests/StreamForge.Host.Tests/SecretWalkTests.cs) fails if a known slot loses it.

Do **not** mark identifiers (`Username`, `Url`, `Subject`) — masking an identifier makes the form unusable and
the export unreadable.

### 2. The transport

```csharp
public sealed class RvInboundTransport : IInboundTransport
{
    public string Kind => SourceKinds.Rv;

    // Which payload format the shared parse path should use for this source.
    public string FormatOf(SourceDefinition def) => Config(def).Format;

    // Accumulates one message per problem; empty means accepted. Never throws.
    public void Validate(SourceDefinition def, List<string> errors)
    {
        var cfg = def.Connector?.Rv;
        if (cfg is null) { errors.Add("kind 'tibco.rv' requires connector.rv"); return; }
        if (string.IsNullOrWhiteSpace(cfg.Daemon)) errors.Add("connector.rv.daemon is required");
        if (string.IsNullOrWhiteSpace(cfg.Subject)) errors.Add("connector.rv.subject is required");
    }

    // One connection ATTEMPT. Called again on every reconnect — do not make it re-enterable.
    public IInboundSubscription Open(SourceDefinition def) => new RvSubscription(Config(def));

    public TransportDescriptor Describe() => /* see "The console form" below */;

    private static RvSubConfig Config(SourceDefinition def) =>
        def.Connector?.Rv ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'tibco.rv' but no rv config");
}

internal sealed class RvSubscription(RvSubConfig cfg) : IInboundSubscription
{
    public async IAsyncEnumerable<InboundMessage> SubscribeAsync([EnumeratorCancellation] CancellationToken ct)
    {
        // Connect here (not in Open), then yield until ct fires or the broker ends the stream.
        // Third element is the ack callback, or null when the transport has no redelivery to acknowledge.
        await foreach (var m in DialAndConsume(cfg, ct))
            yield return new InboundMessage(m.Subject, m.Bytes, AckAsync: null);
    }

    public ValueTask DisposeAsync() => /* close the connection */;
}
```

**Contract you get for free**, all in [`SubscriberCore`](shared/StreamForge.AppCore/Transports/SubscriberCore.cs):

- Reconnect forever with the D-E backoff (`min(30s · 2^(k-1), 15 min)`); a *clean* end of the stream
  reconnects immediately with no backoff and resets the failure counter.
- Payload → row through the one shared path a polled HTTP body uses: parse by `FormatOf`, extract per
  `MappingSpec`, coerce per `SourceDefinition.OnCoercionFailure`, dedup per `MappingSpec.DedupKeyField`,
  stamp `_source`/`_ts`.
- A bad message never tears down the subscription — it becomes a status error and the loop continues. Only
  an exception out of `SubscribeAsync` counts as a connection failure.
- Ack is called only on a clean outcome, so a message rejected by coercion is redelivered if your transport
  has redelivery.

**Contract you must honor:** `SubscribeAsync` must respect `ct` at every await, and must not swallow
connection errors — throwing is how you ask for a reconnect.

### 3. Register it

`shared/StreamForge.AppCore/Transports/InboundTransports.cs`:

```csharp
private static readonly List<IInboundTransport> Registered = [new NatsInboundTransport(), new RvInboundTransport()];
```

That is the entire wiring. Both connector drivers already dispatch through `InboundTransports.Find(kind)`.

---

## Outbound (a sink kind)

Same shape, two interfaces:

```csharp
public sealed class RvSinkTransport : ISinkTransport
{
    public string Kind => SinkKinds.Rv;

    // "Enough config to attempt a connection." A half-filled sink is NOT configured — returning true here
    // would produce failure counters and log noise for a sink nobody has finished setting up.
    public bool IsConfigured(SinkSpec spec) =>
        spec.Rv is { } r && !string.IsNullOrWhiteSpace(r.Daemon) && !string.IsNullOrWhiteSpace(r.Subject);

    public ISinkClient Create(SinkSpec spec, string entityKind, string entityName, Action<string, Exception>? onFailure) =>
        new RvSinkClient(spec.Rv!, entityKind, entityName, onFailure);

    public TransportDescriptor Describe() => /* below */;
}
```

`ISinkClient.PublishAsync` has a **hard contract**: it must never throw and never block the caller past its
own timeout. Both publisher services await it with no try/catch, deliberately — a sink that stalls would
stall the pipeline it is attached to. Count failures internally (`SinkPublishCounters`), report them through
the throttled `onFailure` callback, and drop. Copy
[`NatsSinkClient`](shared/StreamForge.AppCore/Sinks/NatsSinkClient.cs) — its per-publish linked
`CancellationTokenSource` is what makes "never blocks" true against a disconnected broker.

Register in `SinkTransports.Registered`, same as above.

---

## Polled sources (a database, or anything else pull-shaped)

Everything above is **push**-shaped: something else decides when a message exists, and `SubscribeAsync`
yields until it throws. A database table is the opposite — nothing arrives until you ask — and plan 014
added a second seam for exactly that: [`IPolledTransport`](shared/StreamForge.AppCore/Transports/IPolledTransport.cs),
implemented today by `StreamForge.Connectors.Database` (postgres, mssql, postgres-cdc, mssql-cdc).

**It is a *sibling* of `IInboundTransport`, not a generalization of it — on purpose.** `IInboundTransport.Open`
hands back an async enumerable, which means the polling loop (and its cursor — the one piece of state that
must survive anything) would have to live *inside the subscription instance*. That is in memory, and it is
lost on every silo recycle, actor deactivation and rebalance — precisely the thing that must not happen to a
cursor. `PollAsync(def, cursor, ct) -> PolledBatch` puts the loop in the *driver* instead, which already
persists its state once per cycle, so the cursor rides along for free instead of needing a subscription
object with its own lifecycle.

```csharp
public interface IPolledTransport
{
    string Kind { get; }
    void Validate(SourceDefinition def, List<string> errors);
    Task<PolledBatch> PollAsync(SourceDefinition def, string? cursor, CancellationToken ct);
    TransportDescriptor Describe();
}

public sealed record PolledBatch(IReadOnlyList<Dictionary<string, object?>> Rows, string? Cursor, bool HasMore);
```

- **`Cursor` is opaque.** The transport mints it, the driver persists it, nothing in between parses it — an
  LSN, a composite `(ts,id)` string and a plain bigint all fit without the platform ever learning what any
  of them mean. `null` means "leave the persisted cursor exactly as it was" (what an empty poll returns);
  it is **never** "reset to the beginning" — a transport that wants to restart a source has to say so with
  an actual value, not by returning null and hoping.
- **`HasMore` means "re-arm right now"** instead of waiting for the schedule. That single bit is what makes
  a paged backfill resumable: a large initial read pages across successive *driver* cycles, each persisting
  its own cursor, so a restart mid-backfill resumes where it stopped rather than starting over. Paging
  inside one `PollAsync` call would put those intermediate cursors back in memory, which is the exact
  failure this whole seam exists to avoid.
- **The load-bearing rule: a failed cycle keeps the OLD cursor** — enforced once, in
  [`PolledSourceCore`](shared/StreamForge.AppCore/Transports/PolledSourceCore.cs), not by each transport,
  because a transport bug is exactly the case the rule has to protect against, and a rule enforced by the
  code that might be buggy protects nothing. **Throwing from `PollAsync` is therefore a normal, expected
  outcome** — a database is down far more often than a config is wrong — not something your transport needs
  to catch and convert into an empty batch. `PolledSourceCore` turns the exception into a reported error
  with the incoming cursor handed back unchanged, so the same rows are re-read next cycle rather than
  skipped.
- **No `FormatOf`, no ledger, no mapping** — three deliberate divergences from `IInboundTransport`. A result
  set is already structured, so there is nothing to parse. There is no file whose mtime answers "did this
  change" — the cursor replaces that question outright. And for a row source the SELECT list (or the
  publication, for CDC) **is** the mapping; a JSONPath layer on top would be a second way to say the same
  thing, free to disagree with the first the moment a column is renamed in one and not the other. That is
  what `TransportDescriptor.Mapping = false` tells the console: stop offering a mapping editor for this kind.
- **`ISchemaProbe` is an optional capability**, not a second interface every polled transport must implement.
  `POST /api/transports/{kind}/probe` looks for it on the registered transport and 400s when it is absent —
  which is how schema discovery reaches the console without `StreamForge.Api` learning what a database (or
  anything else pull-shaped) actually is; it knows a probe returns fields and diagnostic notes, and nothing
  further. `TransportDescriptor.CanProbe` is what lets the console render the "Discover" button honestly
  instead of hopefully.

**The four kinds that exist today, and the one-line difference between the two pairs:** `postgres` and
`mssql` poll a table or your own query on a durable, monotonic cursor **column** — cheap, and it never sees
a transaction that commits after a later-timestamped one already moved the watermark past it (the honest
argument for the next pair). `postgres-cdc` and `mssql-cdc` read the source database's own change log
instead of polling anything — see below.

### Change data capture

**Using either kind — standalone with no StreamForge server, or as a source inside StreamForge, with the
worked examples and every operational hazard below spelled out in full — is [`docs/cdc.md`](docs/cdc.md).**
This section stays the *recipe* (what you'd change to add a third native CDC dialect); that document is the
*user guide* for the two that already exist.

The native CDC pair is `postgres-cdc` ([`PgCdcSource`](shared/StreamForge.Connectors.Database/PgCdcSource.cs))
and `mssql-cdc` ([`MsSqlCdcSource`](shared/StreamForge.Connectors.Database/MsSqlCdcSource.cs)), both
`IPolledTransport` like their cursor-polled siblings — CDC is still pull-shaped from this platform's point
of view, one cycle at a time, even though the *source* database is doing the pushing internally.

- **Postgres**: logical replication over a **slot + publication**, read via `Npgsql.Replication`'s pgoutput
  decoder. **SQL Server**: the built-in capture tables, read via `cdc.fn_cdc_get_all_changes_*` on a
  `binary(10)` LSN.

**The operational hazards, stated where an operator will actually read them (the descriptor `Help` text, not
just here):**

- An **undrained Postgres slot pins WAL until the *source* database's disk fills** — not StreamForge's disk,
  the database being read from. `max_slot_wal_keep_size` is the server-side safety valve; it is not a
  substitute for keeping the source running.
- **SQL Server CDC retention defaults to 3 days.** A source left stopped longer than that has permanently
  lost whatever retention already discarded — the next cycle fails loudly (a retention-breach check, not a
  silent skip) rather than quietly resuming from wherever it can.
- **`REPLICA IDENTITY FULL`** is what makes a Postgres `DELETE` carry more than its key columns. Without it,
  a delete event is genuinely partial (key columns only) — not fabricated, not dropped, just partial, and
  both the native reader and the Debezium envelope path (below) treat it identically.
- An **unchanged TOASTed column arrives as the sentinel `__debezium_unavailable_value`**, not its real
  content — Postgres never wrote the value into this change record in the first place, so there is nothing
  to decode; a consumer that treats the sentinel as real data corrupts silently.

**Delivery is at-least-once**, same ceiling as the cursor-polled kinds, and **a batch always ends on a
transaction boundary** in both dialects — rows from a transaction whose COMMIT (Postgres) or whose read
this cycle could not prove complete (SQL Server's `TOP`-capped read, re-read bounded to its own
`__$start_lsn` with no `TOP` when needed) are held back rather than emitted split or over-eagerly confirmed.
`BatchSize` is therefore a **target** for a cycle's read, never a hard ceiling on what one batch can emit —
a transaction larger than `BatchSize` is delivered whole, over budget, once the bounded re-read resolves it.
Emitting a truncated group and advancing the cursor past it would be silent, permanent data loss, which is
the failure this rule exists to make impossible.

**Backfill is asymmetric between the two dialects, and each says so in its own `Help` text:**

- `postgres-cdc` **refuses `Snapshot` outright** — a replication slot carries no history from before its own
  creation, so there is nothing to snapshot. Backfill with the polled `postgres` kind first, then switch the
  source to `postgres-cdc` to tail changes from where the backfill left off.
- `mssql-cdc`'s `Snapshot` means **"replay whatever the capture table still retains"**, not a full-table
  snapshot — CDC retention is finite (see the 3-day default above), so `Snapshot` here can only ever replay
  what has not aged out yet. For a true backfill, run the polled `mssql` kind first.

**`MappingSpec.Envelope = "debezium"` still exists**, and is still the right route for a database this
connector does not speak natively — MySQL, Oracle, MongoDB via Debezium Server emitting into a NATS source.
The native kinds are an *addition* to that path, not a replacement for it: they exist because this connector
already speaks Postgres and SQL Server natively, so a slot/capture-table reader was cheap; nothing about it
extends to a database this project has no client for.

**The honest limit, restated so it does not get lost between the two paths:** a StreamForge source is an
append-only `EventRecord` stream — `_weight` on an *inbound* row is just a column, a value like any other,
whichever path stamped it. The Engine's Z-set weights that make a table a genuine multiset are computed
*from* table SQL, not carried in from ingress. So neither `postgres-cdc`/`mssql-cdc` nor the Debezium
envelope path retracts the row a delete removed; it arrives as one more event (`_op = "d"`, `_weight = -1`
on the Debezium path) sitting in the stream next to every insert and update that came before it. The working
pattern is `LATEST BY <key>` + `WHERE _op <> 'd'` on the downstream table — which **hides** a deleted key
from query results but does **not free it**: the tombstone event, and everything before it, is still sitting
in the source's history. See [`CdcEnvelope`](shared/StreamForge.AppCore/Connectors/Mapping/CdcEnvelope.cs)'s
class doc for the canonical wording — this section restates it rather than diverging from it.

---

## FIX

Plan 018 adds two things that are useful separately and better together: a wire **format** (`fix`) that
any `url`/`file`/`folder`/`nats` source can name, and a receive-only session **source kind** (also spelled
`fix` — a coincidence of both being named after the protocol, not a conflict; a `SourceDefinition.Kind`
and a `FileFormats` value are two different registries) that speaks the session and declares
`FormatOf => FileFormats.Fix`, so it reuses the format's parser and the whole shared mapping/coercion/dedup
path with nothing of its own. Had the session converted FIX to JSON internally instead, it would have
worked identically and a FIX log on disk would still be unreadable by this platform — the format existing
independently is the point.

### The `fix` format

`format: "fix"` sits alongside `ndjson`/`json`/`csv`, parsed by
[`FixParser`](shared/StreamForge.AppCore/Connectors/Formats/FixParser.cs) into the same
one-`JsonElement`-per-item shape the other three parsers already produce.

- **tag=value, delimiter sniffed.** A real session speaks SOH (`\x01`); logs, tickets and test fixtures
  use `|` or `^`, because SOH doesn't paste into a text editor. The delimiter is sniffed once per input,
  following `FormatParsers.SniffDelimiter`'s own doctrine: highest count over the first frame wins, a tie
  goes to the earlier candidate, nothing counted falls back to SOH.
- **No FIX dictionary.** Names and types come from one static table covering the common FIX 4.2/4.4/5.0
  tag set — tag numbers are globally unique across FIX versions by design (tag 35 is `MsgType` in every
  version from 4.0 through 5.0), so one table is correct rather than a version-specific compromise. A tag
  this table doesn't know becomes `"tag<N>"`, a plain JSON string keyed to it. An operator who wants more
  has declared field types (`ConnectorRowCoercion`) and 009-C1's SQL conversion functions downstream —
  exactly what CSV's untyped columns already lean on.
- **Values are typed from that table, never sniffed.** CSV sniffs `long`/`double`/`bool` because it has
  nothing better to go on; FIX has a spec that says tag 44 is a price and tag 55 is a symbol, so sniffing
  would silently turn `55=123` into the number 123. Known numeric tags become a JSON number, the FIX
  `Boolean` type (exactly `"Y"`/`"N"`) becomes `true`/`false`, and every other tag — known or unknown —
  stays a string.
- **Repeating groups become nested JSON arrays.** `NoMDEntries=2` followed by two entries becomes
  `"MDEntries": [{…}, {…}]` — **this is the most useful thing about the format**: it is what lets
  `MappingSpec.ItemsPath = "$.MDEntries[*]"` fan one market-data snapshot out into one row per quote,
  using the exact same JSONPath machinery every other source's mapping already runs. The framing needs no
  dictionary: a counter tag from a known table opens a group, the tag right after it is the group's
  delimiter tag, the first entry establishes the group's tag set, and the group ends at the first tag
  outside that set (or the standard trailer — see the ceilings below).
- **Length-prefixed fields are read by byte count.** `RawDataLength(95)`/`RawData(96)` and its siblings
  (90/91, 212/213, 350/351, 354/355, 358/359, 360/361, 362/363, 364/365) carry a character count precisely
  because the value may contain the delimiter; the parser takes the paired data tag verbatim for exactly
  that many characters, delimiters and `=` signs included, instead of splitting on the next delimiter it
  finds. This is FIX's answer to CSV's quoted field, and skipping it corrupts exactly the messages
  (raw/encoded/XML payloads) that are hardest to debug afterward.

**The two ceilings, stated plainly — read these before leaning on group framing:**

- **Dictionary-free group framing infers a group's field set from its first entry.** A later entry that
  carries a field the first entry omitted terminates *that later entry* right at the tag the first entry
  never had — the trailing fields land on the *parent* object, not the entry they visually belong to.
- **A single-entry group is bounded only by the next delimiter tag, a repeated tag, or the standard
  trailer (93/89/10).** A single-entry group that is *not* the last thing at its nesting level absorbs
  whatever follows it, because there is no second entry to reveal where it actually ends.

Upgrade path for both: a real FIX dictionary, which knows a group's field set without needing a second
entry to reveal it. Neither is a corner case found later by accident — `FixParserTests` names both
explicitly, and the code carries a `// ponytail:` comment at the exact line that hits the first one.

**`fix` is ingress-only.** There is no FIX sink: `FileSinkTransport.Describe()`'s format list stays
`[csv, ndjson]`, and `FileSinkClient` has no FIX twin — writing FIX with no session to number the messages
produces something no counterparty would accept.

### The `fix` source kind

A persistent FIX session — market data, drop-copy — as an `IInboundTransport`, living out of the core in
[`shared/StreamForge.Connectors.Fix`](shared/StreamForge.Connectors.Fix) on `QuickFIXn.Core` 1.14.1,
registered the same way TIBCO Rendezvous would be (see "Transports whose client library cannot ship in
this repo", below): `QuickFIXn.Core` is a dependency of this one out-of-core project, not of
`StreamForge.AppCore`. This platform is always the FIX **initiator**, dialing out to the config's
`host`/`port`; the counterparty is always the acceptor.

**Receive-only, on purpose — order entry is a separate plan.** A FIX session that both sends orders and
receives execution reports is [`plans/019-fix-order-entry.md`](plans/019-fix-order-entry.md), and it is a
different plan rather than a later wave of this one, in one sentence: one FIX session spans this
platform's two independent registries (sources and sinks), `ISinkClient`'s never-throw fire-and-forget
contract is wrong for a `NewOrderSingle`, and sequence-number persistence becomes a cluster-singleton
correctness requirement the moment this side originates messages instead of only receiving them.

Config fields (`FixSourceConfig`, `connector.fix` in a `SourceDefinition`):

| Field | Purpose |
|---|---|
| `host` / `port` | Counterparty to dial. Always required; this side is always the initiator. |
| `senderCompId` / `targetCompId` | This session's own CompID and the counterparty's. |
| `beginString` | FIX version header (`FIX.4.0`–`FIX.4.4`, `FIXT.1.1`). Selects the header only — see `UseDataDictionary=N` below. |
| `username` / `password` | Optional; sent as tags 553/554 inside the Logon(A) message. QuickFIX/n has no built-in credential exchange, so this is this platform's own addition. `password` is `[Secret]` and masks to `***` on read and export. |
| `heartBtIntSeconds` | Session heartbeat interval, seconds. Must be > 0. |
| `resetOnLogon` | Reset sequence numbers to 1 every logon (default `true`). |
| `storePath` | Empty (default) = in-memory sequence-number store. Non-empty = file-backed. See the hazards below. |
| `useSsl` | Wraps the socket in TLS. Deferred beyond this bare flag: client certificates, CA pinning. |
| `onLogon` | Raw FIX text, one message per line, sent right after logon succeeds. See below. |
| `msgTypes` | Comma-separated MsgType (tag 35) include-filter, e.g. `W,X`. Empty = every application message. |
| `queueCapacity` | Capacity of the bounded, drop-oldest bridge queue. Default 10000. See the hazards below. |

**`UseDataDictionary=N` is set unconditionally**, so **no `FIX44.xml` (or any version's dictionary) is a
deployment artifact.** QuickFIX/n does the session layer and no message validation, and hands the
application message over intact — `Message.ConstructString()` returns the raw SOH-delimited wire string,
which is exactly the `byte[]` the format parser above then parses. `beginString` only selects which
version header this session claims, never a schema to validate against.

**`onLogon` is raw FIX text, not a request builder.** A market-data session must SEND something (a
`MarketDataRequest`, a `SecurityListRequest`, …) right after logon to receive anything at all — this field
holds one raw FIX message per line, delimiter-sniffed the same way the format parser sniffs a payload (SOH,
`|` or `^`, whichever the operator actually typed). No templating, no request/response correlation, no
resubscribe-on-reject: a typed request builder is a plan-019-sized decision, not a field on this class. A
send failure here fails the whole connection attempt — silence would be the worst possible symptom.

**Operational hazards — read these before enabling the kind:**

- **The bridge from QuickFIX/n's session thread is a bounded, `DropOldest` channel.** `FromApp` is a
  synchronous callback on the FIX session's own thread; blocking it would apply backpressure to the
  session itself and eventually trip the counterparty's heartbeat timeout, a worse failure than dropping.
  `DropOldest` is **right for market data** (a stale quote is worthless) and **wrong for drop-copy**,
  where every message matters. `queueCapacity` sets the bound; a running count of drops
  (`FixBridgeApplication.Dropped`) is logged to stderr on every increment rather than swallowed, and is a
  best-effort operator signal (it can race the channel's own reader by one) rather than an exact ledger.
- **`storePath` empty = in-memory store + `ResetOnLogon`, which is right for market data** — resending
  yesterday's quotes is worse than not resending them, so a clean slate every logon is the default. Set
  `storePath` for a drop-copy session that must not lose its place across restarts, and **in a container
  it must be a mounted volume** — the same requirement the `file` sink's `path` field already carries.
- **`AckAsync` is always null: at-most-once into the platform.** QuickFIX/n's sequence layer acknowledges
  a *session* — the counterparty need not resend an in-sequence message — which is not the same thing as
  this platform having processed the row into a table. Claiming otherwise would be exactly the lie a
  non-null `AckAsync` is not allowed to tell.
- **Session-level traffic never reaches the row path.** Logon/Heartbeat/TestRequest/ResendRequest/
  SequenceReset/Logout are consumed by QuickFIX/n's own session layer and never surface to the application
  callback, so "receive-only" costs nothing to enforce. `msgTypes` filters only the application messages
  that do reach it.

**Worked example** — a market-data source definition (`POST /api/sources` body) subscribing to EUR/USD
top-of-book and fanning its `MDEntries` group into one row per quote:

```json
{
  "name": "eurusd-md",
  "description": "EUR/USD top-of-book, FIX 4.4 market data, receive-only",
  "kind": "fix",
  "connector": {
    "fix": {
      "host": "fix.venue.example.com",
      "port": 9880,
      "senderCompId": "STREAMFORGE",
      "targetCompId": "VENUE",
      "beginString": "FIX.4.4",
      "heartBtIntSeconds": 30,
      "resetOnLogon": true,
      "storePath": "",
      "useSsl": false,
      "onLogon": "35=V|262=1|263=1|264=1|265=1|267=2|269=0|269=1|146=1|55=EUR/USD",
      "msgTypes": "W",
      "queueCapacity": 10000
    },
    "mapping": {
      "itemsPath": "$.MDEntries[*]",
      "fields": [
        { "sourcePath": "MDEntryType", "field": { "name": "side", "type": "String" } },
        { "sourcePath": "MDEntryPx", "field": { "name": "price", "type": "Double" } },
        { "sourcePath": "MDEntrySize", "field": { "name": "size", "type": "Double" } }
      ]
    }
  },
  "onCoercionFailure": "Null"
}
```

The `35=V` line in `onLogon` is a `MarketDataRequest`: `262=1` (MDReqID), `263=1` (Snapshot+Updates),
`264=1` (MarketDepth top-of-book), `265=1` (IncrementalRefresh), `267=2`+`269=0`+`269=1` (NoMDEntryTypes=2:
Bid, Offer), `146=1`+`55=EUR/USD` (NoRelatedSym=1: the symbol). The venue's `35=W` reply then arrives with
a `NoMDEntries` group, and `itemsPath` fans it into one row per entry with `side`/`price`/`size` pulled out
of each.

---

## The console form

`Describe()` returns a [`TransportDescriptor`](shared/StreamForge.AppCore/Transports/TransportDescriptor.cs),
served from `GET /api/transports` (Viewer). The SPA renders it with one generic component
([`TransportConfigEditor.tsx`](web/src/components/sources/TransportConfigEditor.tsx)) — there is no
per-transport React code, and adding one requires no change under `web/` at all.

```csharp
public TransportDescriptor Describe() => new()
{
    Kind = SourceKinds.Rv,
    Label = "TIBCO Rendezvous",
    Help = "A persistent subject subscription — not a poll schedule, so this kind ignores Schedule.",
    ConfigProperty = "rv",              // ← the ConnectorConfig / SinkSpec property holding your config
    Groups =
    [
        new TransportGroup { Key = "auth", Label = "Credentials" },
    ],
    Fields =
    [
        new TransportField { Key = "daemon", Label = "Daemon", Required = true, Mono = true, Placeholder = "tcp:7500" },
        new TransportField { Key = "subject", Label = "Subject", Required = true, Mono = true },
        new TransportField
        {
            Key = "format", Label = "Payload format", Type = TransportFieldTypes.Select,
            Options = [FileFormats.Ndjson, FileFormats.JsonArray, FileFormats.Csv], Default = FileFormats.JsonArray,
        },
        new TransportField { Key = "username", Label = "Username", Group = "auth" },
        new TransportField { Key = "password", Label = "Password", Type = TransportFieldTypes.Secret, Group = "auth" },
    ],
};
```

Field types: `string`, `secret`, `number`, `bool`, `select`.

- `ConfigProperty` must match the property name on `ConnectorConfig` / `SinkSpec`, camelCased as it goes on
  the wire. The console reads and writes `connector[configProperty]` generically.
- Every `secret` field **must** correspond to a `[Secret]` property. A test enforces the two agree — a field
  that renders masked but exports in plaintext is the exact bug the attribute exists to prevent.
- A group with `Optional = true` and an `ObjectKey` renders with an on/off switch and writes `null` for the
  whole nested object when off. That is how "core NATS vs a JetStream consumer" is expressible; use it for
  any opt-in feature block that must be *absent*, not blank.
- Groups without an `ObjectKey` are purely visual — their fields live flat on the config object.

**The descriptor deliberately has no conditional-visibility or cross-field rules.** Validation lives in
`Validate()` and the modal already renders the messages it returns; a rules language here would be a second
validator in TypeScript that drifts from the first.

---

## Transports whose client library cannot ship in this repo

TIBCO Rendezvous is the motivating case: `TIBCO.Rendezvous` is not on public NuGet — it ships with a
licensed Rendezvous installation and wraps a native library. Putting it in `shared/StreamForge.AppCore`
would make the main build require a license.

Put it in its own project that neither solution references, and register from host startup:

```csharp
// orleans/src/StreamForge.Host/Program.cs — before the host starts serving.
InboundTransports.Register(new RvInboundTransport());
SinkTransports.Register(new RvSinkTransport());
```

Registration must happen before any source starts. A duplicate `Kind` throws rather than silently shadowing
a built-in.

---

## What is deliberately *not* pluggable

**The `grpc` source kind stays its own branch** in both drivers. It subscribes to a remote StreamForge and
decodes typed protobuf frames against a schema fetched by reflection — it never asks "what format is this
payload", which is the question this seam is built around. Bending it into `IInboundTransport` would mean
widening the interface for exactly one implementation. `IInboundTransport` is the subject/topic +
opaque-payload family (NATS today; RV, MQTT, AMQP, Kafka next); `IPolledTransport` is the separate,
pull-shaped family covered above (postgres/mssql/postgres-cdc/mssql-cdc today) — between the two, most
future ingress kinds fit one or the other.

**The registries are static lists, not DI discovery.** Assembly scanning would buy nothing — transports are
compile-time known — and both connector drivers are constructed by runtime machinery (an Orleans grain, a
Dapr actor) whose container is not the host's; injecting a registry into a grain has already broken this
repo's test cluster once. `Register()` covers the out-of-tree case above.

---

## Checklist

- [ ] Config class with the next free `[Id(n)]`, `[Secret]` on every credential, nothing else marked
- [ ] `IInboundTransport` / `ISinkTransport` implemented, including `Describe()` — **or**, for a pull-shaped
      kind, `IPolledTransport` (plus `ISchemaProbe` if it can discover its own schema)
- [ ] Registered in `InboundTransports` / `SinkTransports` / `PolledTransports` (or from host startup, if
      out-of-tree)
- [ ] `~/.dotnet/dotnet test orleans/StreamForge.sln` and `dapr/StreamForge.Dapr.sln` — both suites green,
      **no existing test file modified**
- [ ] `cd web && bun run build` — should need no source change; it is a check that nothing regressed
- [ ] Live check on an isolated port (6xxx–9xxx, `--Http:Port … --Grpc:Port … --DataDir <temp>`, killed
      afterwards): the kind appears in `GET /api/transports`, an invalid config is rejected with your
      messages, credentials read back as `***`, a masked PUT round-trip preserves them, and the source arms
      and degrades rather than crashing against an unreachable broker
- [ ] For a polled kind specifically: a cycle that fails (kill the source mid-poll, or point it at an
      unreachable endpoint) leaves the persisted cursor untouched rather than advancing or nulling it — the
      one property `PolledSourceCore` exists to guarantee, and the one worth checking live rather than
      trusting the unit tests alone
