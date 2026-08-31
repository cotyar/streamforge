using System.Text.Json;
using StreamsForge.Abstractions;

namespace StreamsForge.AppCore.Sinks;

/// <summary>
/// Plan 009 B2: "which sinks are actually active" filter + change-detection signature, shared by both
/// flavors' background publisher services (<c>NatsPublisherService</c> on Orleans,
/// <c>NatsSinkPublisherService</c> on Dapr) so the two independently-implemented BackgroundServices agree
/// on exactly one definition of "eligible" and exactly one definition of "changed since last refresh",
/// rather than each reimplementing the same filter slightly differently.
/// </summary>
public static class SinkSelection
{
    private static readonly JsonSerializerOptions SignatureOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    /// <summary>The subset of <paramref name="sinks"/> the publishers can act on right now: Enabled, of a
    /// kind some <see cref="ISinkTransport"/> is registered for, and configured enough for that transport to
    /// connect (<see cref="ISinkTransport.IsConfigured"/> — see its doc for why a half-filled sink is
    /// "not yet configured" rather than "attempted and failing").
    ///
    /// <para>Plan 010: the eligibility rule used to be spelled out here in terms of NATS' own fields, which
    /// meant a second sink kind could not be added without editing this file. It is now the registry's
    /// question to answer.</para></summary>
    public static List<SinkSpec> Active(IEnumerable<SinkSpec>? sinks) =>
        [.. (sinks ?? []).Where(s => s.Enabled && SinkTransports.Find(s.Kind) is { } t && t.IsConfigured(s))];

    /// <summary>The name this filter shipped under in plan 009 B2, when NATS was the only sink kind — kept
    /// because <c>SinkSelectionTests</c> pins it, and identical to <see cref="Active"/> in behavior. New call
    /// sites should use <see cref="Active"/>.</summary>
    public static List<SinkSpec> ActiveNats(IEnumerable<SinkSpec>? sinks) => Active(sinks);

    /// <summary>A string that is equal for two active-sink lists if and only if their content is equal —
    /// used purely as a cheap change-detector so a periodic refresh only tears down and recreates a
    /// <see cref="NatsSinkClient"/> (which owns a live connection) when the sink's own configuration
    /// actually changed, not on every tick regardless.</summary>
    public static string Signature(IReadOnlyList<SinkSpec> activeSinks) =>
        JsonSerializer.Serialize(activeSinks, SignatureOptions);
}
