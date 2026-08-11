using System.Text.Json;
using StreamForge.Abstractions;

namespace StreamForge.AppCore.Sinks;

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

    /// <summary>The subset of <paramref name="sinks"/> this publisher can act on right now: Enabled,
    /// <c>Kind == SinkKinds.Nats</c>, and a non-null <c>Nats</c> config with a non-blank Url/Subject. A
    /// half-filled sink (e.g. created via the API before its Url is set) is treated as "not yet
    /// configured" — nothing to connect to — rather than as a sink that gets attempted and immediately
    /// fails; that distinction matters because the latter would produce a spurious failure counter/log
    /// for a sink nobody has finished setting up yet.</summary>
    public static List<SinkSpec> ActiveNats(IEnumerable<SinkSpec>? sinks) =>
        [.. (sinks ?? [])
            .Where(s =>
                s.Enabled &&
                string.Equals(s.Kind, SinkKinds.Nats, StringComparison.OrdinalIgnoreCase) &&
                s.Nats is { } n && !string.IsNullOrWhiteSpace(n.Url) && !string.IsNullOrWhiteSpace(n.Subject))];

    /// <summary>A string that is equal for two active-sink lists if and only if their content is equal —
    /// used purely as a cheap change-detector so a periodic refresh only tears down and recreates a
    /// <see cref="NatsSinkClient"/> (which owns a live connection) when the sink's own configuration
    /// actually changed, not on every tick regardless.</summary>
    public static string Signature(IReadOnlyList<SinkSpec> activeSinks) =>
        JsonSerializer.Serialize(activeSinks, SignatureOptions);
}
