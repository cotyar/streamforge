# StreamsForge.Client (.NET)

.NET client for StreamsForge live tables: gRPC and SignalR behind one `ITransport`, a `LiveTable`
that keeps one table's Z-set current, ad-hoc SQL, and ingest. Targets `net10.0`.

It has **no ProjectReference to the engine**. The only coupling is build-time codegen: `Grpc.Tools`
compiles the engine's own `orleans/src/StreamsForge.Host/Protos/streamsforge.proto` into this
assembly, so the wire contract cannot drift from the server's, while the client stays shippable on
its own. See `clients/python/README.md` and `clients/typescript/README.md` for the shared design all
StreamsForge clients implement; this file covers what is .NET-specific.

## Build & test

```bash
~/.dotnet/dotnet build clients/dotnet/StreamsForge.Client.slnx
~/.dotnet/dotnet test  clients/dotnet/StreamsForge.Client.slnx
```

`dotnet` lives at `~/.dotnet/dotnet` and is **not on PATH** in this repo — always the full path.

## Quick start

```csharp
using StreamsForge.Client;

await using var sf = await StreamsForgeClient.ConnectAsync(new ConnectOptions
{
    Url = "http://localhost:5199",
    User = "admin",
    Password = "admin123!",
});

await using var table = await sf.TableAsync("trigger_monitor");   // subscribes, snapshots, replays
table.Changed += (_, e) => Console.WriteLine($"{e.Rows.Count} rows");
var filled = await table.WaitForAsync(rows => rows.Count > 0, TimeSpan.FromSeconds(30));
var px = table.Value("price", new Dictionary<string, object?> { ["symbol"] = "AAPL" });
```

Rows are `IReadOnlyDictionary<string, object?>`. `Rows` hands back a fresh immutable snapshot on
every read rather than a collection that mutates under the caller — subscribe to `Changed` instead
of polling.

## Config

Every string option falls back to an environment variable when left null (explicit argument wins):

| Option | Environment variable |
| --- | --- |
| `Url` | `STREAMSFORGE_BASE_URL` |
| `GrpcTarget` | `STREAMSFORGE_GRPC`, then `Url`'s host with **port + 100** |
| `User` / `Password` | `STREAMSFORGE_ADMIN_USER` / `STREAMSFORGE_ADMIN_PASS` |
| `IngestKey` | `SF_INGEST_KEY` |

`Token` supplies a pre-minted JWT and skips login until it expires (~11h). `IngestKey` is preferred
over the admin JWT for `PushAsync`, so a process that only feeds a source never needs to hold one.
`LoggerFactory` (default: no-op) receives the "connected via X transport" line and every transport
warning.

The port + 100 gRPC default follows `Program.cs`'s own convention and is only correct when the two
ports actually follow it — pass `GrpcTarget` explicitly otherwise.

## Transports

`ConnectOptions.Transport` is `Auto` (default), `Grpc` or `SignalR`. `Auto` probes gRPC first, falls
back to SignalR walking WebSockets → Server-Sent Events → Long Polling, and **always logs which one
it got**: a client that silently degrades and lets a caller believe it is on the fast path is worse
than one that fails loudly. `SignalRTransports` narrows the acceptable wire modes — a single flag
pins that exact mode with no fallback (the hub itself restricts none).

If gRPC is refused, check how the host was started: with `--urls`, `Program.cs`'s guard binds **no
gRPC port at all**. Start it with `--Http:Port <n> --Grpc:Port <n+100>` instead.

## TLS

`Url`/`GrpcTarget` of `https://host:port` connect over TLS (REST, gRPC ALPN h2, and SignalR); a bare
`host:port` or `http://` target stays plaintext exactly as before. `CaCertificatePath` names a PEM
file of one or more extra CAs to trust alongside the machine's own store — enough on its own for a
self-signed dev cert (e.g. `tools/tls/dev-cert.sh`'s output, which is its own trust anchor).
`AcceptAnyCertificate` skips validation entirely (dev only). A hostname/SAN mismatch always fails,
even against a trusted CA. Neither option changes a plaintext connection's behavior.

## LiveTable

`subscribe → buffer → snapshot → replay`, then live deltas. Subscription registration completes
*before* the snapshot read is issued, and deltas arriving while that read is in flight are buffered
and replayed — a delta emitted in that window must be neither dropped nor applied twice.
`Rows` (and `Value`/`WaitForAsync`) reflect a batch the instant it is applied — that is never
delayed by the notification window described below.
The reader loop reconnects with exponential backoff capped at 15 s, re-snapshotting each time (a
resumed subscription without a fresh snapshot would silently corrupt the Z-set, since deltas emitted
while it was down are gone, not buffered).

## Change notification: latency and backpressure

**This is a behaviour change.** The old `Changed` implementation did an *unconditional*
`await Task.Delay(120)` before every emit — even a lone update on an otherwise-quiet table paid the
full 120 ms, which is enough to hand back the entire latency win of the engine's push-stream
transport (`tableDelta` p50 115 ms → p50 1 ms with `--Streams:Transport push`; Dapr ~7 ms). The
current implementation is a **leading-edge / trailing-coalesce window**, `TableAsync`'s and
`SqlAsync`'s optional `flush` parameter (`TimeSpan?`, default `null` → `LiveTable.DefaultFlushWindow`
= **16 ms**, one frame at 60 Hz — a UI consumer cannot display more than one update per frame, so
that is the natural ceiling, not a compromise):

- **Leading edge**: if at least `flush` has elapsed since the last emit, the batch just applied
  fires `Changed` immediately — no delay, no wait.
- **Trailing coalesce**: otherwise the batch merges into a single pending emit scheduled for
  `lastEmit + flush`; further batches arriving before that fires merge into the same pending emit
  (at most one is ever pending). A burst inside one window still costs exactly one event.
- `flush = TimeSpan.Zero` disables coalescing entirely: one `Changed` per applied batch.
- The window governs **when** a consumer is told, never **what** the state is — `Rows` is always
  current the instant a batch lands, regardless of `flush` (see `WaitForAsync`'s doc for the
  consequence: its 50 ms poll interval is now the *whole* story, not poll-interval-plus-window).

**`Changed` is an event, and `WatchAsync(CancellationToken)` is offered ALONGSIDE it, not instead of
it** — deliberately. An async-enumerable of change notifications forces exactly one owner to
enumerate it, which fits .NET badly when a UI binding, a logger and a test assertion all want to
attach and detach independently over an object's lifetime; `FileSystemWatcher` and
`INotifyCollectionChanged` set the idiom `Changed` follows. The cost is the usual one: handlers run
synchronously on the reader loop and must not block it. A throwing handler is caught and logged,
never allowed to kill the reader.

`WatchAsync` is for the opposite, equally normal .NET shape: one caller that wants `await foreach`
instead of an event handler. Each call returns an **independent** enumerator backed by its own
`Channel.CreateBounded<...>(1)` with `BoundedChannelFullMode.DropOldest` — capacity 1, latest wins.
That shape is deliberate and not just "a queue with a size limit": these channel items are **state
snapshots**, not discrete events to be processed one by one — the table's current rows, republished
whenever they change. A back-pressured (blocking) queue is the wrong model for a stream of snapshots
for two reasons at once: it would stall the reader loop, which is obliged to keep draining the
transport regardless of whether any consumer is listening, and it would make a slow consumer observe
a growing backlog of snapshots it never asked to see — by the time it caught up, every one of them
but the last would already be stale. Dropping older, unread snapshots in favor of the newest one is
therefore not a compromise forced by the bounded channel; it is the correct policy independent of the
implementation. Subscribing hooks `Changed`; the enumeration finishing, being cancelled, or being
disposed unhooks it and completes the channel, so nothing keeps the enumerator (or the handler)
alive after the caller is done with it. Two concurrent `await foreach` loops never steal items from
each other. **Intermediate snapshots are skipped by design under a burst** — `WatchAsync` is not an
audit log of every batch; a caller that needs to see every one must use `Changed` instead.

## Reducer and cross-language conformance

`ZSet` is this client's own copy of the reducer (canonical-row identity, weight summation, group
supersession, the replay "already reflected" check). Every StreamsForge client implements it
independently, and `ConformanceTests` runs the shared `clients/conformance/zset-cases.json` suite
against this one — which is what turns "these agree" into something that fails on the same named
case in every language instead of drifting quietly.

## Key fields

`TableAsync(name)` resolves the table's row-identity key from the engine (`GET /api/tables`'s
`keyFields`, recomputed on every successful compile). A non-empty list is the resolved
`GROUP BY`/`LATEST BY` key; `[]` is an unkeyed global aggregate; an unknown table — or an engine
build that does not report the field — resolves to `null`, which means whole-row identity. This
client never had a hand-maintained key map to fall back to. Pass `keyFields` explicitly to skip
resolution entirely and always win.

## Row values: one column can arrive as two CLR types

`Struct.NumberValue` is always an IEEE-754 double, so the engine's converter deliberately sends a
`long` beyond 2^53 as a Struct **string** rather than lose precision. The same logical column can
therefore arrive as `double` on most rows and `string` on the rare large one. `RowCodec` passes
values through exactly as the wire sent them instead of guessing, so downstream code that assumes
one CLR type per column is wrong on real data. Nothing in the reference demo crosses this today
(epoch-ms tops out near 1.7e12) — a raw counter or a hashed id would.

## Ad-hoc SQL

`SqlAsync(sql, name)` validates, imports the table under the `adhoc_` namespace and returns a
`LiveTable` over it. `ValidateAsync` alone returns diagnostics without creating anything; a failed
validation throws `SqlException` carrying every `SqlDiagnostic` (message, line, column, severity).
`AdhocTablesAsync` lists what is under that namespace and `DropAdhocAsync` refuses any name outside
it.

## Ingest

`PushAsync(source, rows, idempotencyKey, partial)` goes over the gRPC bidi stream when the client
connected via gRPC — real backpressure — and falls back to REST otherwise. A rejected batch throws
`IngestRejectedException` with its per-row errors. Note the target is a **source**, not a
materialized table: tables are computed by the engine, never written to directly.

## Public surface

```csharp
StreamsForgeClient.ConnectAsync(ConnectOptions, CancellationToken) -> StreamsForgeClient
client.TransportName                                     // "grpc" | "signalr:ws" | ":sse" | ":lp"
client.TableAsync(name, keyFields?, timeout?, flush?, ct) -> LiveTable
client.SnapshotAsync(name, limit?, ct)                   // one-shot read, no subscription
client.ListTablesAsync(ct) / client.SearchAsync(name, query, limit?, ct)
client.ValidateAsync(sql, ct) -> ValidateResult
client.SqlAsync(sql, name, keyFields?, timeout?, flush?, ct) -> LiveTable
client.AdhocTablesAsync(ct) / client.DropAdhocAsync(name, ct)
client.PushAsync(source, rows, idempotencyKey?, partial?, ct) -> IngestAckResult

table.Rows / table.Ready / table.Seq / table.Reconnects / table.FlushWindow
table.Changed                                            // event, leading-edge/trailing-coalesce (default 16ms)
table.WatchAsync(ct)                                     // IAsyncEnumerable<Rows>, latest-wins, alongside Changed
table.Value(column, keys) / table.WaitForAsync(predicate, timeout, ct)
```

Errors: `StreamsForgeException` is the base; `AuthException`, `NotReadyException` (a brand-new table
that nobody has pushed to yet never fills — this is expected, not a failure of the client),
`IngestRejectedException`, `SqlException`.

Both `StreamsForgeClient` and `LiveTable` are `IAsyncDisposable` — `await using` is the intended
shape.

## Tests

- **`LiveTableTests`** — the `Changed`/`WatchAsync` coalescing window, unit-tested against a fake
  `ITransport` (`Fixtures/FakeTransport.cs`): no engine, no network, so timing assertions (leading
  edge fires with no artificial delay, a burst inside one window yields exactly one `Changed`,
  `flush = TimeSpan.Zero` emits per batch, a slow `WatchAsync` consumer sees only the latest rows,
  two concurrent enumerations don't steal from each other, cancelling one unsubscribes) aren't at
  the mercy of a real connection.
- **`ConformanceTests`** — the shared reducer suite. No engine, no network.
- **`ContractTests`** — both transports against an isolated engine the fixture publishes and boots
  on **9199/9299**. It refuses to bind 5199/5299 (the live dev server) or 6199 (the demo container),
  and reports a clear `SkipReason` (port collision, missing `dotnet`, publish failure) instead of
  colliding with whatever is already running. xunit v2 has no dynamic runtime skip, so a test whose
  fixture is unavailable fails loudly with that reason rather than passing vacuously — "0 contract
  tests ran" must never read as "contract passed".
- **`LiveSmokeTests`** — strictly read-only against the demo at 6199. That instance runs with
  `--urls`, so it has no gRPC port at all, which makes it the natural real-world check of `Auto`'s
  fallback: the gRPC probe genuinely fails and the client must say it fell back to SignalR. It never
  pushes, restarts or reconfigures shared infrastructure.
- **`TlsSupportTests`** — target parsing, the scheme-preserving gRPC guess, and the certificate
  validator (accept/reject built directly, no real handshake). No engine, no network.
- **`TlsTests`** — both live transports plus the no-CA negative case, against an isolated HTTPS/TLS
  gRPC engine on **7399/7499** (`Fixtures/TlsEngineFixture.cs`, sharing `EngineFixture`'s process
  lifecycle via `Fixtures/EngineProcess.cs`), certs generated fresh per run by `tools/tls/dev-cert.sh`.
  Same `SkipReason` philosophy as `ContractTests`.

## Not here yet

Typed rows. `RowCodec` is deliberately the single place a wire row becomes a dictionary, so a future
codegen path off `GET /api/tables/{id}/proto` only has to replace what happens there — the reducer
and everything above it are written against the row shape, not against `Struct` or `JsonElement`.
