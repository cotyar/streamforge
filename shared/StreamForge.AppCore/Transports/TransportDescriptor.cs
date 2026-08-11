namespace StreamForge.AppCore.Transports;

/// <summary>
/// Plan 010 (console wave): what the SPA needs to render a transport's config form without containing a
/// line of code about that transport. Served as JSON from <c>GET /api/transports</c>; the console builds
/// one generic form from it, so a transport added to the backend gets a working editor with no SPA change
/// at all — the last of the fourteen places that used to need editing.
///
/// <para><b>Deliberately a flat field list plus optional groups, not a form DSL.</b> Conditional visibility,
/// cross-field rules and computed defaults all belong to server-side validation, which already returns a
/// list of messages the modal renders. Adding a rules language here would duplicate that validator in
/// TypeScript and let the two disagree — the console asks the server what is wrong, it does not decide.</para>
/// </summary>
public sealed record TransportDescriptor
{
    /// <summary>The <c>SourceDefinition.Kind</c> / <c>SinkSpec.Kind</c> value this describes.</summary>
    public required string Kind { get; init; }

    /// <summary>Human-facing name for the kind picker, e.g. "NATS".</summary>
    public required string Label { get; init; }

    /// <summary>One or two sentences shown under the picker: what this transport is, and its honest
    /// delivery ceiling. Optional.</summary>
    public string? Help { get; init; }

    /// <summary>Which property of <c>ConnectorConfig</c> (inbound) or <c>SinkSpec</c> (outbound) holds this
    /// transport's config object — "nats" for both NATS directions. The console reads and writes
    /// <c>connector[configProperty]</c> generically; without this it would need to know the shape.</summary>
    public required string ConfigProperty { get; init; }

    public IReadOnlyList<TransportField> Fields { get; init; } = [];

    public IReadOnlyList<TransportGroup> Groups { get; init; } = [];
}

/// <summary>One editable property of a transport's config object.</summary>
public sealed record TransportField
{
    /// <summary>Property name on the config object, camelCase as it appears on the wire (e.g. "queueGroup").
    /// For a field in a group with an <see cref="TransportGroup.ObjectKey"/>, the key is relative to that
    /// nested object.</summary>
    public required string Key { get; init; }

    public required string Label { get; init; }

    /// <summary>"string" | "secret" | "number" | "bool" | "select". A plain string rather than an enum
    /// because this crosses to TypeScript, where the enum's serialized casing has bitten this project
    /// before; see <see cref="TransportFieldTypes"/> for the constants.</summary>
    public string Type { get; init; } = TransportFieldTypes.String;

    /// <summary><see cref="TransportGroup.Key"/> this field belongs to, or null for the main body.</summary>
    public string? Group { get; init; }

    /// <summary>Rendered with a required marker. The SERVER still decides — this is a hint that saves a
    /// round-trip, never the check itself.</summary>
    public bool Required { get; init; }

    /// <summary>Render in the monospace face (addresses, subjects, paths).</summary>
    public bool Mono { get; init; }

    public string? Placeholder { get; init; }

    public string? Help { get; init; }

    /// <summary>Allowed values when <see cref="Type"/> is "select".</summary>
    public IReadOnlyList<string>? Options { get; init; }

    /// <summary>Initial value for a NEW entity, as a string the console coerces by <see cref="Type"/>.
    /// Null means empty/false/0.</summary>
    public string? Default { get; init; }
}

/// <summary>A labelled box of related fields. <see cref="Optional"/> + <see cref="ObjectKey"/> together
/// express the one structural shape transports keep needing: an opt-in feature block that is either absent
/// entirely or fully configured (NATS JetStream today).</summary>
public sealed record TransportGroup
{
    public required string Key { get; init; }

    public required string Label { get; init; }

    public string? Help { get; init; }

    /// <summary>Rendered with an on/off switch. When off, <see cref="ObjectKey"/> is written as null — which
    /// is what makes "core NATS, nothing left on the server" expressible at all, as opposed to a JetStream
    /// block with blank names that validation would then reject.</summary>
    public bool Optional { get; init; }

    /// <summary>When set, this group's fields live in a NESTED object under this property of the transport
    /// config (e.g. "jetStream") rather than directly on it.</summary>
    public string? ObjectKey { get; init; }
}

public static class TransportFieldTypes
{
    public const string String = "string";
    /// <summary>Masked input, and subject to the secrets-lite convention: read back as "***", and sending
    /// "***" unchanged keeps the stored value. Must correspond to a <c>[Secret]</c> property.</summary>
    public const string Secret = "secret";
    public const string Number = "number";
    public const string Bool = "bool";
    public const string Select = "select";
}

/// <summary><c>GET /api/transports</c> response.</summary>
public sealed record TransportCatalog(
    IReadOnlyList<TransportDescriptor> Inbound,
    IReadOnlyList<TransportDescriptor> Outbound);
