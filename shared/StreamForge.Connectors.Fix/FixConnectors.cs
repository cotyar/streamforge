using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Fix;

/// <summary>
/// The entire wiring surface of this assembly — one registration behind one call, made from each host's
/// startup (wave D). Mirrors <c>StreamForge.Connectors.Database.DatabaseConnectors</c>'s shape and
/// reasoning: <c>QuickFIXn.Core</c> is not a dependency either core assembly needs, so the FIX kind lives
/// in its own project the main build does not reference, and registers itself explicitly rather than via
/// assembly scanning (see <see cref="InboundTransports"/>'s own class doc for why a static list, not DI
/// discovery).
///
/// <para><b>One kind, one registry.</b> Unlike <c>DatabaseConnectors</c> (two dialects × source + sink,
/// plus two CDC sources), FIX is ingress-only and single-kind: <see cref="InboundTransports.Register"/>
/// with a <see cref="FixInboundTransport"/>, nothing else. There is no FIX sink — order entry (plan 019)
/// is a different plan, not a later wave of this one.</para>
///
/// <para><b>Registration is process-global and permanent</b> (<see cref="InboundTransports.Register"/>
/// throws on a duplicate kind), so <see cref="RegisterAll"/> is idempotent by choice rather than by luck —
/// two hosts in one test process, or a re-entrant startup, must not take the process down for a reason
/// that has nothing to do with the operator.</para>
/// </summary>
public static class FixConnectors
{
    private static readonly Lock Gate = new();
    private static bool _registered;

    /// <summary>Registers the <c>fix</c> inbound transport. Call once from host startup, before any
    /// source is opened.</summary>
    public static void RegisterAll()
    {
        lock (Gate)
        {
            if (_registered)
            {
                return;
            }

            _registered = true;
            InboundTransports.Register(new FixInboundTransport());
        }
    }
}
