using StreamsForge.Abstractions;
using StreamsForge.AppCore.Sinks;

namespace StreamsForge.AppCore.Transports;

/// <summary>
/// Plan 016 wave 4 — "what version is kind X, on THIS instance, right now" — the single place
/// <c>ConfigImportService</c>'s plugin-requirement gate (and, later, <c>GET /api/meta/instance</c>'s
/// "plugins" list, plan 016 wave 5) asks. Assembled fresh on every call from the same live registries
/// <c>SourceValidation.IsKnownKind</c> and <c>GET /api/transports</c> already read
/// (<see cref="InboundTransports"/>/<see cref="PolledTransports"/>/<see cref="SinkTransports"/> —
/// <see cref="DuplexTransports"/> needs no separate read here because it co-registers into
/// <see cref="InboundTransports"/>, the same shortcut <c>TransportsEndpoints</c> takes), so a
/// connector registered from host startup (the database/FIX assemblies, or a future out-of-tree
/// <c>Register</c> caller) is visible the moment it has registered, with no caching to go stale.
///
/// <para><b>The six source kinds with no <see cref="TransportDescriptor"/> at all</b> —
/// <see cref="SourceKinds.Generator"/>/<see cref="SourceKinds.Url"/>/<see cref="SourceKinds.File"/>/
/// <see cref="SourceKinds.Folder"/>/<see cref="SourceKinds.Grpc"/>/<see cref="SourceKinds.Ingest"/> —
/// are compiled directly into <c>StreamsForge.AppCore</c>/<c>StreamsForge.Api</c> rather than living
/// behind an <c>IInboundTransport</c>/<c>IPolledTransport</c> registration (see
/// <c>SourceValidation.BuiltInKinds</c>, the same set by the same name). They cannot be individually
/// absent from a build the way an optional connector assembly can, so there is nothing per-kind to
/// version independently — this class reports them all at the fixed floor <see cref="BuiltInVersion"/>,
/// bumped only if this class itself changes what one of them accepts or does. <c>file</c> is listed
/// once here even though it names both a source kind and (separately) a registered
/// <see cref="SinkTransports"/> kind of the same string — both resolve to the identical version, so a
/// <c>requires: [{ kind: "file" }]</c> entry is unambiguous in practice even though this map cannot
/// tell which direction the author meant.</para>
/// </summary>
public static class KindVersions
{
    private const string BuiltInVersion = "1.0.0";

    private static readonly IReadOnlyDictionary<string, string> BuiltIn = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        [SourceKinds.Generator] = BuiltInVersion,
        [SourceKinds.Url] = BuiltInVersion,
        [SourceKinds.File] = BuiltInVersion,
        [SourceKinds.Folder] = BuiltInVersion,
        [SourceKinds.Grpc] = BuiltInVersion,
        [SourceKinds.Ingest] = BuiltInVersion,
        // Plan 020 wave B — a built-in for the same reason Generator and Ingest are: its driver is a
        // grain, not a registered transport, so no registry can report its version.
        [SourceKinds.Crdt] = BuiltInVersion,
    };

    /// <summary>Every kind this instance knows about right now, mapped to its declared version —
    /// built-ins first, then every registered inbound/polled/sink transport (a registered kind sharing
    /// a name with a built-in, which cannot happen today, would win over it; order here mirrors
    /// "the more specific answer last" rather than anything load-bearing).</summary>
    public static IReadOnlyDictionary<string, string> All()
    {
        var result = new Dictionary<string, string>(BuiltIn, StringComparer.Ordinal);

        foreach (var kind in InboundTransports.Kinds)
        {
            if (InboundTransports.Find(kind) is { } t)
            {
                result[kind] = t.Describe().Version;
            }
        }

        foreach (var kind in PolledTransports.Kinds)
        {
            if (PolledTransports.Find(kind) is { } t)
            {
                result[kind] = t.Describe().Version;
            }
        }

        foreach (var kind in SinkTransports.Kinds)
        {
            if (SinkTransports.Find(kind) is { } t)
            {
                result[kind] = t.Describe().Version;
            }
        }

        return result;
    }

    /// <summary>The version this instance has for <paramref name="kind"/>, or null when the kind is not
    /// registered/built-in at all — the "not present" half of a plugin-requirement check.</summary>
    public static string? Resolve(string kind) => All().TryGetValue(kind, out var v) ? v : null;
}
