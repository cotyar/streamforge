using StreamForge.Abstractions;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 009 wave D: table-driven proof, from the Orleans flavor, of <see cref="SourceKindDispatch.Classify"/>
/// — moved to StreamForge.Abstractions (shared/StreamForge.Contracts) so both flavors dispatch on the same
/// tested implementation instead of each hand-rolling its own copy (RegistryGrain used to carry private
/// IsGeneratorKind/IsIngestKind helpers; GeneratorSupervisorService and IngestDrainPumpService each carried
/// their own hand-written string comparisons). dapr/tests/StreamForge.Dapr.Tests/IngestKindDispatchTests.cs
/// and ConnectorLifecycleOrchestratorTests.cs already cover this exact method from the Dapr side (unmodified
/// here — this is a new file); this file is the Orleans-side equivalent, now that RegistryGrain/
/// GeneratorSupervisorService/IngestDrainPumpService actually call through it too.
/// </summary>
public class SourceKindDispatchTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(SourceKinds.Generator)]
    public void GeneratorLikeKinds_ClassifyAsGenerator(string? kind) =>
        Assert.Equal(SourceKindDispatch.ActorKind.Generator, SourceKindDispatch.Classify(kind));

    [Fact]
    public void IngestKind_ClassifiesAsIngest_NotConnector() =>
        Assert.Equal(SourceKindDispatch.ActorKind.Ingest, SourceKindDispatch.Classify(SourceKinds.Ingest));

    [Theory]
    [InlineData(SourceKinds.Url)]
    [InlineData(SourceKinds.File)]
    [InlineData(SourceKinds.Folder)]
    [InlineData(SourceKinds.Grpc)]
    public void KnownConnectorKinds_ClassifyAsConnector(string kind) =>
        Assert.Equal(SourceKindDispatch.ActorKind.Connector, SourceKindDispatch.Classify(kind));

    [Theory]
    [InlineData("banana")]
    [InlineData("not-a-real-kind")]
    [InlineData("Generator")] // case-sensitive: capitalized does NOT match the "generator" constant
    public void UnrecognizedKind_FallsThroughToConnector_NotGenerator(string kind)
    {
        // The dispatch is deliberately NOT an exhaustive switch over every SourceKinds constant (see
        // SourceKindDispatch.Classify's own doc comment) — a not-yet-invented connector-shaped kind must
        // still route to the connector path (which itself rejects what it cannot run) rather than silently
        // falling through to the generator path. This is also what keeps a brand-new kind (e.g. a concurrent
        // "nats" addition) working correctly without this dispatch needing to know about it in advance.
        Assert.Equal(SourceKindDispatch.ActorKind.Connector, SourceKindDispatch.Classify(kind));
    }

    [Fact]
    public void IngestIsCheckedAheadOfTheConnectorCatchAll()
    {
        // The one case that would silently misclassify if Ingest were not checked BEFORE the catch-all —
        // this is exactly the plan 008 W4c defect the class doc describes (a hand-written "kind !=
        // generator" gate treating ingest sources as connector-shaped and activating a pointless actor).
        var result = SourceKindDispatch.Classify(SourceKinds.Ingest);
        Assert.NotEqual(SourceKindDispatch.ActorKind.Connector, result);
        Assert.Equal(SourceKindDispatch.ActorKind.Ingest, result);
    }
}
