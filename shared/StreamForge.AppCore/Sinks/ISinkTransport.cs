using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.AppCore.Sinks;

/// <summary>
/// Plan 010: one configured outbound sink's live connection, as both publisher services see it. Extracted
/// from <see cref="NatsSinkClient"/>, whose shape it is verbatim — the fire-and-forget contract stated there
/// (never throws, never blocks the caller past its own timeout, counts and throttles its own failures) is
/// part of this interface, not an incidental property of the NATS implementation. A transport that cannot
/// honor it must swallow and count internally rather than propagate, because the callers deliberately await
/// <see cref="PublishAsync"/> with no try/catch around it.
/// </summary>
public interface ISinkClient : IAsyncDisposable
{
    /// <summary>The pipeline id / table name this client was constructed for — exposed so a caller iterating
    /// many clients doesn't need a parallel dictionary just to log which one failed.</summary>
    string EntityName { get; }

    /// <summary>Lifetime publish counters. Cheap; safe to read from any thread.</summary>
    SinkPublishCounters Counters { get; }

    /// <summary>Publishes one message. NEVER throws — see this interface's doc comment.</summary>
    Task PublishAsync<T>(T payload, CancellationToken ct);
}

/// <summary>
/// Plan 010: everything the platform needs to know about a sink kind. Implementing this plus registering it
/// in <see cref="SinkTransports"/> is the whole cost of a new egress transport — both publisher services
/// (<c>NatsPublisherService</c> on Orleans, <c>NatsSinkPublisherService</c> on Dapr), <see cref="SinkSelection"/>
/// and <c>SecretsMasker</c> go through the registry and need no edit.
/// </summary>
public interface ISinkTransport
{
    /// <summary>The <see cref="SinkSpec.Kind"/> value this transport serves, e.g. <see cref="SinkKinds.Nats"/>.
    /// Compared case-insensitively (matching the pre-plan-010 filter), and must be unique in the registry.</summary>
    string Kind { get; }

    /// <summary>True if <paramref name="spec"/> carries enough configuration to actually connect. A
    /// half-filled sink (created via the API before its address is set) is "not yet configured" — nothing to
    /// connect to — rather than a sink that gets attempted and immediately fails; that distinction is what
    /// keeps a sink nobody has finished setting up from producing spurious failure counters and log lines.</summary>
    bool IsConfigured(SinkSpec spec);

    /// <summary>Opens a client for one configured sink on one entity. <paramref name="entityKind"/> is
    /// "pipeline" | "table" and is used for connection naming and log context;
    /// <paramref name="onFailure"/> receives (destination, exception) on a publish failure, already
    /// throttled by the implementation.</summary>
    ISinkClient Create(SinkSpec spec, string entityKind, string entityName, Action<string, Exception>? onFailure);

    /// <summary>Console form descriptor — see <see cref="TransportDescriptor"/> and the inbound twin.</summary>
    TransportDescriptor Describe();

    /// <summary>Appends a human-readable message per problem with <paramref name="spec"/>'s transport
    /// config — the sink-side twin of <see cref="StreamForge.AppCore.Transports.IInboundTransport.Validate"/>
    /// and <see cref="StreamForge.AppCore.Transports.IPolledTransport.Validate"/>. Never throws; an empty
    /// <paramref name="errors"/> on return means accepted.
    ///
    /// <para><b>Plan 014: there is no sink validation anywhere in this repo today.</b> A sink with a wrong
    /// host, a typo'd subject or any other broken config is silently <see cref="IsConfigured"/> == false
    /// (see that member's doc) and simply never runs — no error surfaces anywhere, no status field, no log
    /// line, nothing an operator can act on. A default no-op implementation means <see cref="NatsSinkTransport"/>
    /// and <see cref="FileSinkTransport"/> — which have shipped without validation since plan 009 B2 and
    /// plan 012 respectively — do not need to change to pick up this seam; a NEW transport (plan 014's
    /// database sink, which has a KeyColumns-required-for-upsert rule that genuinely deserves a real error
    /// message instead of "nothing happened") can finally implement it and refuse a broken config instead of
    /// quietly doing nothing. Wiring the actual call site (so this method's output reaches an operator) is a
    /// later wave's job — this default method is only the seam.</para></summary>
    void Validate(SinkSpec spec, List<string> errors) { }
}

/// <summary>Sink-side twin of <c>InboundTransports</c> — see that class's doc comment for why this is a plain
/// static list rather than DI discovery, and what <see cref="Register"/> exists for.</summary>
public static class SinkTransports
{
    private static readonly Lock Gate = new();

    // ponytail: a plain list, not a plugin host. Add built-in sink transports here.
    private static readonly List<ISinkTransport> Registered =
        [new NatsSinkTransport(), new FileSinkTransport(), new HttpSinkTransport(), new LoopbackSinkTransport()];

    public static ISinkTransport? Find(string? kind)
    {
        if (string.IsNullOrEmpty(kind))
        {
            return null;
        }

        lock (Gate)
        {
            return Registered.FirstOrDefault(t => string.Equals(t.Kind, kind, StringComparison.OrdinalIgnoreCase));
        }
    }

    public static IReadOnlyList<string> Kinds
    {
        get
        {
            lock (Gate)
            {
                return [.. Registered.Select(t => t.Kind)];
            }
        }
    }

    public static void Register(ISinkTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        lock (Gate)
        {
            if (Registered.Any(t => string.Equals(t.Kind, transport.Kind, StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException($"a sink transport for kind '{transport.Kind}' is already registered");
            }

            Registered.Add(transport);
        }
    }

    /// <summary>Wishlist item 13's gap 3 — the missing call site. <see cref="ISinkTransport.Validate"/>
    /// has existed since plan 014 (<see cref="HttpSinkTransport"/> implements it; see that type's own doc
    /// for "a missing URL is a validation error") but nothing in this repo ever CALLED it: a broken sink
    /// config was accepted by POST/PUT and then simply never ran — silently <see cref="ISinkTransport.IsConfigured"/>
    /// == false, "no error surfaces anywhere, no status field, no log line, nothing an operator can act
    /// on" (<see cref="ISinkTransport.Validate"/>'s own doc, naming this exact gap). TablesEndpoints and
    /// PipelinesEndpoints each call this once per create/update, over whichever sink list is actually
    /// being written — see each call site for exactly when that is; an update that isn't touching Sinks
    /// at all never re-validates a pre-existing definition nobody is asking to change, so a table/pipeline
    /// saved before this method existed can't suddenly become un-editable over an old sink nobody is
    /// touching.
    ///
    /// <para>Deliberately does NOT reject an unregistered <see cref="SinkSpec.Kind"/>: <see cref="Find"/>
    /// already answers null for one, and <see cref="SinkSelection.Active"/> already treats that the exact
    /// same "never selected, never runs" way <see cref="ISinkTransport.IsConfigured"/> == false does —
    /// widening THAT into a new rejection is a second decision this method does not make on its own
    /// authority; the ask was to wire the existing seam, not invent a new validation rule beside it.</para>
    ///
    /// <para>Every currently-registered transport but <see cref="HttpSinkTransport"/> inherits
    /// <see cref="ISinkTransport.Validate"/>'s default no-op — <see cref="NatsSinkTransport"/> and
    /// <see cref="FileSinkTransport"/> therefore add zero errors here no matter what they are given, so
    /// wiring this call site changes nothing for either of them; every existing NATS/file sink config,
    /// however incomplete, keeps saving exactly as it always has. HTTP is the only kind whose behavior
    /// actually moves — from "silently never runs" to "400, naming the field" — and that tightening is the
    /// literal, stated purpose of this call, not a side effect of it.</para></summary>
    public static void Validate(IReadOnlyList<SinkSpec> sinks, List<string> errors)
    {
        for (var i = 0; i < sinks.Count; i++)
        {
            var spec = sinks[i];
            var transport = Find(spec.Kind);
            if (transport is null)
            {
                continue;
            }

            var before = errors.Count;
            transport.Validate(spec, errors);
            if (errors.Count == before)
            {
                continue;
            }

            var label = string.IsNullOrEmpty(spec.Name) ? $"sinks[{i}] (kind '{spec.Kind}')" : $"sink '{spec.Name}'";
            for (var e = before; e < errors.Count; e++)
            {
                errors[e] = $"{label}: {errors[e]}";
            }
        }
    }
}

/// <summary>Plan 010: NATS as an <see cref="ISinkTransport"/>. The connection work is all
/// <see cref="NatsSinkClient"/>'s (plan 009 B2, unchanged) — this type only says which kind it serves, what
/// counts as configured, and how to construct one.</summary>
public sealed class NatsSinkTransport : ISinkTransport
{
    public string Kind => SinkKinds.Nats;

    public bool IsConfigured(SinkSpec spec) =>
        spec.Nats is { } n && !string.IsNullOrWhiteSpace(n.Url) && !string.IsNullOrWhiteSpace(n.Subject);

    public ISinkClient Create(SinkSpec spec, string entityKind, string entityName, Action<string, Exception>? onFailure) =>
        new NatsSinkClient(spec.Nats!, entityKind, entityName, onFailure);

    public TransportDescriptor Describe() => new()
    {
        Kind = SinkKinds.Nats,
        Label = "NATS",
        Help = "Fire-and-forget: a slow or absent broker drops messages rather than slowing the entity down.",
        ConfigProperty = "nats",
        Groups =
        [
            new TransportGroup
            {
                Key = "auth",
                Label = "Credentials",
                Help = "All optional. If more than one is set the server applies: .creds file, then token, then username+password.",
            },
        ],
        Fields =
        [
            new TransportField { Key = "url", Label = "Server URL", Required = true, Mono = true, Placeholder = "nats://localhost:4222" },
            new TransportField
            {
                Key = "subject", Label = "Subject", Required = true, Mono = true, Placeholder = "streamforge.{name}",
                Help = "{name} is replaced with this pipeline's id / table's name, so one spec can serve a whole catalog.",
            },
            new TransportField { Key = "token", Label = "Token", Type = TransportFieldTypes.Secret, Group = "auth" },
            new TransportField { Key = "username", Label = "Username", Group = "auth" },
            new TransportField { Key = "password", Label = "Password", Type = TransportFieldTypes.Secret, Group = "auth" },
            new TransportField
            {
                Key = "credentials", Label = ".creds file contents", Type = TransportFieldTypes.Secret, Group = "auth", Mono = true,
                Placeholder = "Paste the contents of a NATS .creds file",
            },
        ],
    };
}

/// <summary>Plan 012: a local file as an <see cref="ISinkTransport"/> — the egress twin of the
/// <c>file</c> source kind, and the platform's first sink that isn't a broker. All the work is
/// <see cref="FileSinkClient"/>'s; read its class doc for the append-only/fixed-header/no-rotation
/// contract this kind sells.</summary>
public sealed class FileSinkTransport : ISinkTransport
{
    public string Kind => SinkKinds.File;

    public bool IsConfigured(SinkSpec spec) => spec.File is { } f && !string.IsNullOrWhiteSpace(f.Path);

    public ISinkClient Create(SinkSpec spec, string entityKind, string entityName, Action<string, Exception>? onFailure) =>
        new FileSinkClient(spec.File!, entityKind, entityName, onFailure);

    public TransportDescriptor Describe() => new()
    {
        Kind = SinkKinds.File,
        Label = "File",
        Help = "Appends to a file on the HOST's filesystem, never truncates it. In a container the path must be a mounted volume. No rotation and no size cap — the file grows until something else prunes it.",
        ConfigProperty = "file",
        Fields =
        [
            new TransportField
            {
                Key = "path", Label = "Path", Required = true, Mono = true, Placeholder = "/data/out/{name}.csv",
                Help = "{name} is replaced with this pipeline's id / table's name. Missing directories are created.",
            },
            new TransportField
            {
                Key = "format", Label = "Format", Type = TransportFieldTypes.Select,
                Options = [FileFormats.Csv, FileFormats.Ndjson], Default = FileFormats.Csv,
                Help = "NDJSON writes the same record a NATS sink publishes, one JSON object per line. 'json' is absent on purpose: an append-only writer can never close the array.",
            },
            new TransportField
            {
                Key = "columns", Label = "CSV columns", Mono = true, Placeholder = "symbol,qty,_weight",
                Help = "Optional, CSV only. Empty = the first written row's column order (or the existing file's header). Fixed for the life of the file either way.",
            },
        ],
    };
}
