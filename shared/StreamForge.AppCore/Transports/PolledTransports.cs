namespace StreamForge.AppCore.Transports;

/// <summary>
/// Plan 014: the one place that knows which POLLED transports exist — an exact mirror of
/// <see cref="InboundTransports"/>, including its reasoning about a static list over DI discovery (see that
/// class's doc: the connector drivers are an Orleans grain and a Dapr actor, constructed by runtime
/// machinery whose container is not the host's, and injecting a registry into a grain has already broken
/// this repo's test cluster once). Nothing about that argument changes direction with the seam, so it is
/// referenced here rather than re-argued.
///
/// <para><b>Why a second registry rather than an <c>IsPolled</c> flag on the first.</b> The two SPIs have
/// no member in common — one opens a subscription, the other returns a batch and a cursor — so a single
/// registry would hold a union type every consumer then downcasts. More usefully, a separate registry makes
/// "does this kind take a schedule, and should the console render a schedule editor for it" a registry
/// lookup (<see cref="Find"/> non-null) instead of the hardcoded kind array it is today. That is the whole
/// point: a kind this assembly has never heard of gets a schedule editor because it is registered here, not
/// because someone remembered to add it to a list.</para>
///
/// <para>The built-in list is deliberately <b>empty</b>. Plan 014 does not migrate <c>url</c>/<c>file</c>/
/// <c>folder</c> onto this seam — that would touch <c>ConnectorPollCycle</c>, <c>FileLedger</c>, both
/// drivers, <c>SourceValidation</c> and four SPA kind arrays, putting six passing test suites at risk for
/// no user-visible gain. The database transports live out of the core in
/// <c>StreamForge.Connectors.Database</c> and register themselves from host startup, which is the second
/// real call site <see cref="Register"/> never had.</para>
/// </summary>
public static class PolledTransports
{
    private static readonly Lock Gate = new();

    // ponytail: a plain list, not a plugin host. Empty on purpose — see this class's doc.
    private static readonly List<IPolledTransport> Registered = [];

    /// <summary>The transport serving <paramref name="kind"/>, or null if that kind is not a polled
    /// transport. Every built-in kind (generator/url/file/folder/grpc/ingest/nats) lands here as null —
    /// they have drivers of their own, and routing one of them through this seam by accident would silence
    /// its timer.</summary>
    public static IPolledTransport? Find(string? kind)
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

    /// <summary>Every registered kind, for the "which kinds are recognized" surfaces. Ordered as
    /// registered, so validation's error message is stable.</summary>
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

    /// <summary>Adds a transport that cannot live in this assembly. Registration must happen before any
    /// source starts; a duplicate <see cref="IPolledTransport.Kind"/> is a programming error and throws
    /// rather than silently shadowing what is already there.</summary>
    public static void Register(IPolledTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        lock (Gate)
        {
            if (Registered.Any(t => string.Equals(t.Kind, transport.Kind, StringComparison.Ordinal)))
            {
                throw new InvalidOperationException($"a polled transport for kind '{transport.Kind}' is already registered");
            }

            Registered.Add(transport);
        }
    }
}
