namespace StreamsForge.Abstractions;

/// <summary>
/// Plan 006 (ingestion connectors) W3-B, moved here plan 009 wave D: the pure "which actor/grain type owns
/// this source" classification, originally written once — well — as <c>SourceKindDispatch</c> inside the
/// Dapr flavor's <c>DaprLifecycleOrchestrator.cs</c>, framework-free specifically so it is unit-testable
/// without a live Dapr sidecar (see dapr/tests/StreamsForge.Dapr.Tests/GeneratorLifecycleOrchestratorTests.cs's
/// class doc for that finding and ConnectorLifecycleOrchestratorTests.cs for the tests against this class).
/// The Orleans flavor re-derived the identical three-way split by hand — private <c>IsGeneratorKind</c>/
/// <c>IsIngestKind</c> helpers in RegistryGrain, plus hand-written <c>kind != "generator"</c>/
/// <c>kind == SourceKinds.Ingest</c> comparisons in GeneratorSupervisorService and IngestDrainPumpService.
/// Both flavors already reference this assembly (<c>StreamsForge.Abstractions</c>, next to
/// <see cref="SourceKinds"/> itself), so the classification now lives here once and both flavors call
/// <see cref="Classify"/> instead of re-deriving it. Plan 008 W4c shipped a latent defect that was exactly a
/// hand-written <c>kind != "generator"</c> in two status facades, which would have activated a pointless
/// connector grain/actor for every ingest source — precisely the class of bug one shared, tested
/// implementation removes.
/// </summary>
public static class SourceKindDispatch
{
    /// <summary>Plan 008 W4c added <see cref="Ingest"/> as a genuine third case (not folded into
    /// <see cref="Connector"/>) — an ingest-kind source has no actor/grain at all, so callers that used to
    /// assume "not Generator means Connector" (the old binary <see cref="Classify"/>) MUST switch on all
    /// three values explicitly now. Plan 020 wave B added a FOURTH, <see cref="Crdt"/> — a CRDT document
    /// has its own grain (D3) and is neither a generator nor a connector-driven source; "all three" above
    /// now reads "all four", and the same rule applies: enumerate, never assume a complement.</summary>
    public enum ActorKind { Generator, Connector, Ingest, Crdt }

    /// <summary>Null/empty/"generator" → <see cref="ActorKind.Generator"/> (the pre-006 default — existing
    /// sources with no <see cref="SourceDefinition.Kind"/> set at all deserialize with an empty string
    /// before the additive default kicks in, so empty is treated identically to "generator", not as an
    /// error). <see cref="SourceKinds.Ingest"/> → <see cref="ActorKind.Ingest"/> (plan 008 W4c — no actor at
    /// all). Every other value (url/file/folder/grpc, or any future kind — this dispatch is deliberately NOT
    /// an exhaustive switch over every <see cref="SourceKinds"/> constant) → <see cref="ActorKind.Connector"/>,
    /// so a not-yet-invented connector-shaped kind still routes to the connector path (which is itself
    /// responsible for rejecting a kind it doesn't know how to run) rather than silently falling through to
    /// the generator path. <see cref="SourceKinds.Ingest"/> is checked explicitly, ahead of that catch-all,
    /// specifically so it does NOT fall into the connector bucket — see the defect this fixed, noted in this
    /// class's own doc comment.</summary>
    public static ActorKind Classify(string? kind)
    {
        if (string.IsNullOrEmpty(kind) || kind == SourceKinds.Generator)
        {
            return ActorKind.Generator;
        }

        if (kind == SourceKinds.Ingest)
        {
            return ActorKind.Ingest;
        }

        // Plan 020 D3: a CRDT document is driven by a grain of its own, like a generator — NOT by the
        // connector driver. It is checked ahead of the catch-all for the same reason Ingest is: without
        // this line "crdt" falls into Connector, the connector driver activates for it, and the failure
        // is a source that looks armed and never emits.
        return kind == SourceKinds.Crdt ? ActorKind.Crdt : ActorKind.Connector;
    }
}
