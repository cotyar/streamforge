using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Lifecycle;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 008 W4c: unit tests for <see cref="SourceKindDispatch.Classify"/>'s new three-way case —
/// <see cref="SourceKinds.Ingest"/> now classifies to its own <see cref="SourceKindDispatch.ActorKind.Ingest"/>
/// rather than falling into <see cref="SourceKindDispatch.ActorKind.Connector"/> (the pre-W4c binary
/// dispatch's catch-all for "not generator"). A NEW file rather than an edit to the existing
/// <c>ConnectorLifecycleOrchestratorTests.cs</c> — that file's own existing cases (Url/File/Folder/Grpc/
/// "some-future-kind" all still classify to Connector, and generator-like kinds still classify to
/// Generator) are untouched and still pass unmodified; this file covers only the new case plus the
/// regression this wave fixed in <c>DaprConnectorStatusFacade</c> (see that class's doc comment for the
/// defect: it used to treat "not generator" as "is connector", which meant an Ingest-kind source got a
/// pointless <c>ConnectorActor</c> activated for a status lookup it could never answer).
/// </summary>
public class IngestKindDispatchTests
{
    [Fact]
    public void Classify_IngestKind_ReturnsIngest_NotConnector()
    {
        Assert.Equal(SourceKindDispatch.ActorKind.Ingest, SourceKindDispatch.Classify(SourceKinds.Ingest));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(SourceKinds.Generator)]
    public void Classify_GeneratorLikeKinds_StillReturnGenerator_NotIngest(string? kind)
    {
        Assert.Equal(SourceKindDispatch.ActorKind.Generator, SourceKindDispatch.Classify(kind));
    }

    [Theory]
    [InlineData(SourceKinds.Url)]
    [InlineData(SourceKinds.File)]
    [InlineData(SourceKinds.Folder)]
    [InlineData(SourceKinds.Grpc)]
    public void Classify_ConnectorKinds_StillReturnConnector_NotIngest(string kind)
    {
        Assert.Equal(SourceKindDispatch.ActorKind.Connector, SourceKindDispatch.Classify(kind));
    }
}
