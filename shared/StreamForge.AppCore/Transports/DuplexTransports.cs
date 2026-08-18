namespace StreamForge.AppCore.Transports;

/// <summary>
/// Plan 019 (wave A): the one place that knows which DUPLEX transports exist — a third registry alongside
/// <see cref="InboundTransports"/> and <see cref="PolledTransports"/>, same static-list shape and the same
/// reasoning about a static list over DI discovery (see <see cref="InboundTransports"/>'s doc: the connector
/// drivers are an Orleans grain and a Dapr actor, constructed by runtime machinery whose container is not
/// the host's, and injecting a registry into a grain has already broken this repo's test cluster once).
///
/// <para><b>Why a third registry rather than a flag on <see cref="IInboundTransport"/>.</b> A duplex kind's
/// outbound half (<see cref="IDuplexTransport.OpenDuplex"/>) is reached from a completely different call
/// site — the proxy sink (wave 019-B), which resolves a session by source name, not by
/// <c>SourceDefinition.Kind</c> — and giving every <see cref="IInboundTransport"/> implementer a member it
/// almost never implements is the union-type mistake <see cref="PolledTransports"/>'s own doc already
/// rejects for the polled/message split.</para>
///
/// <para><b>The one design point that is not boilerplate: <see cref="Register"/> also registers into
/// <see cref="InboundTransports"/>.</b> Every existing inbound code path — <c>ConnectorGrain.ArmForKind</c>'s
/// <c>InboundTransports.Find</c> arm (<c>orleans/src/StreamForge.Host/Grains/ConnectorGrain.cs</c>),
/// <c>ConnectorActor</c>'s twin (<c>dapr/src/StreamForge.Dapr.Host/Actors/ConnectorActor.cs</c>),
/// <c>SourceValidation.IsKnownKind</c>, and <c>GET /api/transports</c>'s <c>Inbound</c> list — asks
/// <see cref="InboundTransports"/>, and none of them is going to be taught about a third registry just so a
/// duplex kind can be armed, validated and listed. Co-registering is what makes a duplex source behave like
/// any other message-transport source with zero changes to those four call sites, exactly the deal plan 010
/// made when it built <see cref="InboundTransports"/> in the first place.</para>
///
/// <para><b>Do not "simplify" this by registering only here and teaching the four call sites about
/// <see cref="DuplexTransports"/> too.</b> That would restore the "add a transport, edit N places" cost this
/// whole seam exists to avoid, for no benefit — a duplex kind's inbound half really is an ordinary
/// <see cref="IInboundTransport"/>, and every one of those four consumers is correct to treat it as
/// one.</para>
///
/// <para><b>Registration is atomic in effect.</b> If <paramref name="transport"/>'s kind is already present
/// in EITHER registry — this one, or <see cref="InboundTransports"/> directly — <see cref="Register"/>
/// throws and leaves BOTH untouched: the existence check runs, under this class's own lock, before either
/// list is mutated, and the co-registration into <see cref="InboundTransports"/> happens (and could itself
/// still throw, on a race) strictly before this registry records the transport. A duplicate kind is a
/// programming error, the same as it is for the other two registries.</para>
/// </summary>
public static class DuplexTransports
{
    private static readonly Lock Gate = new();

    // ponytail: a plain list, not a plugin host. Empty on purpose — the built-in FIX duplex kind arrives
    // in wave 019-E, out of this assembly, and registers itself from host startup exactly the way the
    // out-of-core database transports register into PolledTransports today.
    private static readonly List<IDuplexTransport> Registered = [];

    /// <summary>The transport serving <paramref name="kind"/>, or null if that kind is not a duplex
    /// transport. Every ordinary message-transport kind (nats) and every built-in kind lands here as null —
    /// only a kind registered through THIS class's <see cref="Register"/> resolves.</summary>
    public static IDuplexTransport? Find(string? kind)
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

    /// <summary>Every registered duplex kind. Ordered as registered, for the same stable-message reason
    /// <see cref="InboundTransports.Kinds"/> documents.</summary>
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

    /// <summary>Registers a duplex transport — see this class's doc for why that also registers
    /// <paramref name="transport"/> into <see cref="InboundTransports"/>, and for the atomicity this method
    /// guarantees. Registration must happen before any source starts.</summary>
    public static void Register(IDuplexTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        lock (Gate)
        {
            if (Registered.Any(t => string.Equals(t.Kind, transport.Kind, StringComparison.Ordinal)))
            {
                throw new InvalidOperationException($"a duplex transport for kind '{transport.Kind}' is already registered");
            }

            if (InboundTransports.Find(transport.Kind) is not null)
            {
                throw new InvalidOperationException($"an inbound transport for kind '{transport.Kind}' is already registered");
            }

            // If this throws (e.g. a race lost against another thread registering the same kind directly
            // into InboundTransports between the check above and here), it throws BEFORE Registered.Add
            // below runs — so this registry stays untouched exactly as InboundTransports does. That is the
            // "leave BOTH untouched" guarantee in practice, not just in the check above.
            InboundTransports.Register(transport);
            Registered.Add(transport);
        }
    }
}
