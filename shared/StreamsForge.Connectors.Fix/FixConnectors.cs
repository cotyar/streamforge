using StreamsForge.AppCore.Transports;

namespace StreamsForge.Connectors.Fix;

/// <summary>
/// The entire wiring surface of this assembly — one registration behind one call, made from each host's
/// startup (wave D). Mirrors <c>StreamsForge.Connectors.Database.DatabaseConnectors</c>'s shape and
/// reasoning: <c>QuickFIXn.Core</c> is not a dependency either core assembly needs, so the FIX kind lives
/// in its own project the main build does not reference, and registers itself explicitly rather than via
/// assembly scanning (see <see cref="InboundTransports"/>'s own class doc for why a static list, not DI
/// discovery).
///
/// <para><b>Two kinds now, still one registration call.</b> Plan 018 shipped <see cref="FixInboundTransport"/>
/// (kind <c>fix</c>, receive-only, registered straight into <see cref="InboundTransports"/>). Plan 019 wave
/// E adds <see cref="FixDuplexTransport"/> (kind <c>fix-duplex</c>, an order-entry-capable session),
/// registered through <see cref="DuplexTransports.Register"/> instead — which co-registers it into
/// <see cref="InboundTransports"/> too (see that registry's own doc comment), so both kinds end up
/// reachable from <see cref="InboundTransports.Find"/> and this method's single call remains the entire
/// host-wiring surface. There is still no FIX-SPECIFIC sink: order entry's outbound half is the generic
/// <c>duplex</c> proxy sink (wave 019-B, in <c>StreamsForge.AppCore</c>) naming a <c>fix-duplex</c> source
/// by name, not a class in this project.</para>
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

    /// <summary>Registers the <c>fix</c> and <c>fix-duplex</c> inbound/duplex transports. Call once from
    /// host startup, before any source is opened.</summary>
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
            DuplexTransports.Register(new FixDuplexTransport());
        }
    }
}
