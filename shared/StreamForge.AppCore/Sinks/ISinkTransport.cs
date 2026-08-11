using StreamForge.Abstractions;

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
}

/// <summary>Sink-side twin of <c>InboundTransports</c> — see that class's doc comment for why this is a plain
/// static list rather than DI discovery, and what <see cref="Register"/> exists for.</summary>
public static class SinkTransports
{
    private static readonly Lock Gate = new();

    // ponytail: a plain list, not a plugin host. Add built-in sink transports here.
    private static readonly List<ISinkTransport> Registered = [new NatsSinkTransport()];

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
}
