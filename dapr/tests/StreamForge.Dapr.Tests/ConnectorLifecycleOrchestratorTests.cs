using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Lifecycle;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 006 (ingestion connectors) W3-B: unit tests for <see cref="SourceKindDispatch"/> — the pure
/// classification behind <see cref="DaprLifecycleOrchestrator.NotifySourceChangedAsync"/>'s kind
/// dispatch (generator-kind sources go to <c>IGeneratorActor</c>, everything else to
/// <c>IConnectorActor</c>, and both branches also idempotently stop the counterpart kind's actor — see
/// that method's doc comment for why no separate "kind changed" branch is needed).
///
/// <para>Same finding as <see cref="GeneratorLifecycleOrchestratorTests"/>'s own class doc: every
/// <see cref="DaprLifecycleOrchestrator"/> method now makes a real Dapr actor-proxy call that requires a
/// live sidecar to complete (both the generator/connector branch AND the "stop the counterpart kind"
/// calls added in this wave), so the actual dispatch behavior is exercised live (isolated-port smoke, see
/// this wave's report) rather than unit-tested end to end. What IS unit-testable without a sidecar is the
/// classification decision itself, extracted into <see cref="SourceKindDispatch.Classify"/> specifically
/// for that reason — these tests cover it exhaustively.</para>
/// </summary>
public class ConnectorLifecycleOrchestratorTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(SourceKinds.Generator)]
    public void Classify_GeneratorLikeKinds_ReturnsGenerator(string? kind)
    {
        Assert.Equal(SourceKindDispatch.ActorKind.Generator, SourceKindDispatch.Classify(kind));
    }

    [Theory]
    [InlineData(SourceKinds.Url)]
    [InlineData(SourceKinds.File)]
    [InlineData(SourceKinds.Folder)]
    [InlineData(SourceKinds.Grpc)]
    [InlineData("some-future-kind")]
    public void Classify_ConnectorKinds_ReturnsConnector(string kind)
    {
        Assert.Equal(SourceKindDispatch.ActorKind.Connector, SourceKindDispatch.Classify(kind));
    }

    [Fact]
    public void Classify_IsCaseSensitive_UppercaseGeneratorIsNotTreatedAsGeneratorKind()
    {
        // Documents current behavior rather than mandating it: SourceDefinition.Kind is never
        // user-typed free text (the SPA's kind selector and the REST validation W4 adds both constrain
        // it to the SourceKinds constants), so case sensitivity here is a deliberate simplicity choice,
        // not an oversight — a stray "Generator" would be (mis)classified as a connector kind and
        // IConnectorActor.StartAsync would then reject it via its own kind switch in TickAsync.
        Assert.Equal(SourceKindDispatch.ActorKind.Connector, SourceKindDispatch.Classify("Generator"));
    }
}
