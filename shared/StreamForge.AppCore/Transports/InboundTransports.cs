using StreamForge.AppCore.Connectors.Nats;

namespace StreamForge.AppCore.Transports;

/// <summary>
/// Plan 010: the one place that knows which message transports exist. Every consumer — both connector
/// drivers, <c>SourceValidation</c>, the schema service — asks here instead of testing
/// <c>kind == SourceKinds.Nats</c>, which is what collapsed the previous "add a transport, edit fourteen
/// places" into "add a transport, add a line here".
///
/// <para><b>Why a static list and not DI discovery.</b> Assembly scanning would buy nothing: transports are
/// compile-time known, and both flavors construct their connector driver (an Orleans grain, a Dapr actor)
/// through runtime machinery whose DI container is NOT the host's — injecting a registry into a grain has
/// already broken this repo's test cluster once. A static list is legible, has no startup ordering, and
/// works identically in a unit test, a silo, and an actor.</para>
///
/// <para><b><see cref="Register"/> is for transports that cannot live in this assembly.</b> The concrete
/// case is a broker whose client library is not publicly redistributable (TIBCO Rendezvous ships its .NET
/// assembly with a licensed installation, not on NuGet) — that implementation belongs in an optional project
/// the main build does not reference, and registers itself from host startup. Registration must happen
/// before any source starts; a duplicate <see cref="IInboundTransport.Kind"/> is a programming error and
/// throws rather than silently shadowing the built-in.</para>
/// </summary>
public static class InboundTransports
{
    private static readonly Lock Gate = new();

    // ponytail: a plain list, not a plugin host. Add built-in transports here.
    private static readonly List<IInboundTransport> Registered = [new NatsInboundTransport()];

    /// <summary>The transport serving <paramref name="kind"/>, or null if that kind is not a message
    /// transport (generator/url/file/folder/grpc/ingest all land here as null — they have their own
    /// drivers).</summary>
    public static IInboundTransport? Find(string? kind)
    {
        if (string.IsNullOrEmpty(kind))
        {
            return null;
        }

        lock (Gate)
        {
            return Registered.FirstOrDefault(t => string.Equals(t.Kind, kind, StringComparison.Ordinal));
        }
    }

    /// <summary>Every registered kind, for the "which kinds are recognized" surfaces (validation's known-kind
    /// set and its error message). Ordered as registered so that message is stable.</summary>
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

    /// <summary>Adds an out-of-tree transport. See this class's doc for the case this exists for.</summary>
    public static void Register(IInboundTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        lock (Gate)
        {
            if (Registered.Any(t => string.Equals(t.Kind, transport.Kind, StringComparison.Ordinal)))
            {
                throw new InvalidOperationException($"an inbound transport for kind '{transport.Kind}' is already registered");
            }

            Registered.Add(transport);
        }
    }
}
