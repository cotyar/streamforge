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
widening the interface for exactly one implementation. The transports that fit here are the
subject/topic + opaque-payload family: NATS today; RV, MQTT, AMQP, Kafka next.

**The registries are static lists, not DI discovery.** Assembly scanning would buy nothing — transports are
compile-time known — and both connector drivers are constructed by runtime machinery (an Orleans grain, a
Dapr actor) whose container is not the host's; injecting a registry into a grain has already broken this
repo's test cluster once. `Register()` covers the out-of-tree case above.

---

## Checklist

- [ ] Config class with the next free `[Id(n)]`, `[Secret]` on every credential, nothing else marked
- [ ] `IInboundTransport` / `ISinkTransport` implemented, including `Describe()`
- [ ] Registered in `InboundTransports` / `SinkTransports` (or from host startup, if out-of-tree)
- [ ] `~/.dotnet/dotnet test orleans/StreamForge.sln` and `dapr/StreamForge.Dapr.sln` — both suites green,
      **no existing test file modified**
- [ ] `cd web && bun run build` — should need no source change; it is a check that nothing regressed
- [ ] Live check on an isolated port (6xxx–9xxx, `--Http:Port … --Grpc:Port … --DataDir <temp>`, killed
      afterwards): the kind appears in `GET /api/transports`, an invalid config is rejected with your
      messages, credentials read back as `***`, a masked PUT round-trip preserves them, and the source arms
      and degrades rather than crashing against an unreachable broker
