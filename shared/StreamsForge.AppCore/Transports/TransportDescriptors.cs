using StreamsForge.AppCore.Sinks;

namespace StreamsForge.AppCore.Transports;

/// <summary>
/// "Describe kind X, whichever registry it lives in" — one lookup for the code that needs a
/// <see cref="TransportDescriptor"/> without caring which seam the kind implements.
///
/// <para>It exists because the descriptor stopped being purely a console concern: <c>SecretsMasker</c>
/// reads it to learn which keys of an out-of-tree kind's <c>Settings</c> bag are secret (that bag is a
/// plain string dictionary, so <c>SecretWalk</c>'s <c>[Secret]</c> attributes cannot reach it). Every
/// caller asked all three registries in a slightly different order before this; one place to ask is the
/// point.</para>
/// </summary>
public static class TransportDescriptors
{
    /// <summary>The descriptor for a SOURCE kind — inbound first, then polled (duplex kinds co-register
    /// into <see cref="InboundTransports"/>, the same shortcut <c>TransportsEndpoints</c> and
    /// <see cref="KindVersions"/> already take). Null for a kind nobody registered, and for the built-in
    /// kinds that have no descriptor at all (generator/url/file/folder/grpc/ingest/crdt).</summary>
    public static TransportDescriptor? ForSource(string? kind) =>
        InboundTransports.Find(kind)?.Describe() ?? PolledTransports.Find(kind)?.Describe();

    /// <summary>The descriptor for a SINK kind, or null when nobody registered it.</summary>
    public static TransportDescriptor? ForSink(string? kind) => SinkTransports.Find(kind)?.Describe();

    /// <summary>The keys this descriptor declares as <c>secret</c> fields — what
    /// <c>SecretsMasker</c> masks inside a <c>Settings</c> bag. Ordinal, matching how the bag is read.</summary>
    public static HashSet<string> SecretKeys(TransportDescriptor? descriptor) =>
        descriptor is null
            ? []
            : [.. descriptor.Fields.Where(f => f.Type == TransportFieldTypes.Secret).Select(f => f.Key)];
}
