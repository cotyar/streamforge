namespace StreamsForge.AppCore.Transports;

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
///
/// <para><b>Plan 014 adds three booleans, not a capability object.</b> <see cref="Polled"/>,
/// <see cref="Mapping"/> and <see cref="CanProbe"/> each answer one question the console used to answer from
/// a hardcoded array of kind strings ("does this kind take a schedule", "does it get a mapping editor", "can
/// it discover its own schema"). Flags leave the flat-field doctrine above intact: they select which of the
/// console's EXISTING blocks render, and describe no behaviour the server would then have to honor a second
/// time.</para>
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

    /// <summary>Plan 014: this kind is driven by <c>IPolledTransport</c> — it runs on the source's Schedule,
    /// so the console renders the schedule editor for it. False for the message family, whose Schedule is
    /// ignored (a subscription has nothing to schedule) and where showing the editor invited exactly the
    /// misconfiguration it looks like it prevents.</summary>
    public bool Polled { get; init; }

    /// <summary>Plan 014: this kind's rows go through a <c>MappingSpec</c>, so the console offers the mapping
    /// editor. <b>Defaults to true</b> so every pre-014 transport keeps the editor it has today without
    /// touching its descriptor; a polled row source sets it false, because there the SELECT list already IS
    /// the mapping and a second way to say the same thing can only disagree with the first.</summary>
    public bool Mapping { get; init; } = true;

    /// <summary>Plan 014: the transport also implements <c>ISchemaProbe</c>, so the console renders its
    /// generic "Discover schema" button and posts to <c>/api/transports/{kind}/probe</c>. Declared rather
    /// than inferred client-side because the console has no way to type-test a server object — and a button
    /// rendered hopefully, which then 400s, is worse than no button.</summary>
    public bool CanProbe { get; init; }

    /// <summary>Plan 019: this kind's source half and sink half are two views of one live session — it
    /// implements <c>IDuplexTransport</c> and is registered through <c>DuplexTransports</c>, which also
    /// co-registers it into <c>InboundTransports</c> (see that registry's doc). Defaults to false so every
    /// pre-019 transport's descriptor is unchanged; a duplex kind's descriptor appears in the catalog's
    /// <c>Inbound</c> list (via the co-registration) with this flag true, and NOT in <c>Outbound</c> — the
    /// outbound half is a proxy sink kind of its own, added in wave 019-B, not this same descriptor
    /// duplicated into the other list.</summary>
    public bool Duplex { get; init; }

    /// <summary>Plan 016 wave 4: the KIND's contract version — a plain <c>major.minor.patch</c> triple
    /// (see <c>SemVerRange</c>), matched against a config document's declared
    /// <c>ConfigDocument.Requires</c> at import.
    ///
    /// <para><b>What this versions, and what it deliberately does not.</b> It is the wire/behavior
    /// contract this KIND promises — its config shape, its row mapping rules, what a caller can assume
    /// about how it behaves — not the assembly's own <c>AssemblyVersion</c> and not the platform's
    /// release number. Those diverge the moment a shipped connector's BEHAVIOR changes (a mapping rule
    /// gets fixed, a default flips) without every dependent recompiling against it; only bumping THIS
    /// number, by hand, on that kind of change is what makes a document's <c>requires</c> pin mean
    /// anything. A field-additive change (a new optional <see cref="TransportField"/>) does not need a
    /// bump — additive is always compatible, the same bargain <c>FieldNumberMap</c> makes for schemas.</para>
    ///
    /// <para><b>Default is "1.0.0"</b>, both here and on every in-tree kind's <c>Describe()</c> —
    /// chosen, not merely defaulted-to, so a kind that declares nothing satisfies the loosest possible
    /// requirement (<c>*</c>, or no <c>requires</c> entry at all) unchanged from before this field
    /// existed, and so every kind that ships today starts at the same baseline rather than an arbitrary
    /// per-kind number nobody chose on purpose.</para>
    /// </summary>
    public string Version { get; init; } = "1.0.0";
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
    /// <summary>Plan 014: a multiline string — SQL, principally. Same value and same validation as
    /// <see cref="String"/>; the only difference is that the console gives it a textarea, because a
    /// twelve-line query typed into a one-line input is unreviewable and therefore unreviewed.</summary>
    public const string Text = "text";
}

/// <summary><c>GET /api/transports</c> response.</summary>
/// <para>Plan 014: <see cref="Inbound"/> carries BOTH registries — message transports
/// (<c>IInboundTransport</c>) and polled ones (<c>IPolledTransport</c>). They are separate registries for a
/// driver-side reason — one arms a subscriber, the other arms a timer — that a form has no business knowing.
/// What the form needs is <see cref="TransportDescriptor.Polled"/> on the entry it is drawing, which is why
/// that flag exists.</para>
public sealed record TransportCatalog(
    IReadOnlyList<TransportDescriptor> Inbound,
    IReadOnlyList<TransportDescriptor> Outbound);
