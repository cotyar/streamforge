using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Services;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 006 (ingestion connectors) W3-B: unit tests for <see cref="ConnectorSourceSweep.SelectConnectorSources"/>
/// — the pure filter behind <see cref="GeneratorSupervisorService"/>'s connector-kind sweep branch. The
/// actual <c>IsRunningAsync</c>/<c>StartAsync</c> actor-proxy calls per eligible source are I/O and require
/// a live sidecar (same finding as every other actor-bound service in this test project), so this file
/// covers only the eligibility decision — the part that determines WHICH sources the sweep even considers.
/// </summary>
public class ConnectorSupervisorSweepTests
{
    private static SourceDefinition Source(string name, bool enabled, string? kind) =>
        new() { Name = name, Enabled = enabled, Kind = kind ?? SourceKinds.Generator };

    [Fact]
    public void SelectConnectorSources_IncludesEnabledUrlFileFolderGrpcSources()
    {
        var sources = new List<SourceDefinition>
        {
            Source("s-url", enabled: true, SourceKinds.Url),
            Source("s-file", enabled: true, SourceKinds.File),
            Source("s-folder", enabled: true, SourceKinds.Folder),
            Source("s-grpc", enabled: true, SourceKinds.Grpc),
        };

        var selected = ConnectorSourceSweep.SelectConnectorSources(sources).Select(s => s.Name).ToList();

        Assert.Equal(["s-url", "s-file", "s-folder", "s-grpc"], selected);
    }

    [Fact]
    public void SelectConnectorSources_ExcludesGeneratorKindSources()
    {
        var sources = new List<SourceDefinition>
        {
            Source("gen-explicit", enabled: true, SourceKinds.Generator),
            Source("gen-default", enabled: true, kind: null),
            Source("gen-empty", enabled: true, kind: ""),
        };

        var selected = ConnectorSourceSweep.SelectConnectorSources(sources);

        Assert.Empty(selected);
    }

    [Fact]
    public void SelectConnectorSources_ExcludesDisabledConnectorSources()
    {
        var sources = new List<SourceDefinition>
        {
            Source("disabled-url", enabled: false, SourceKinds.Url),
        };

        var selected = ConnectorSourceSweep.SelectConnectorSources(sources);

        Assert.Empty(selected);
    }

    [Fact]
    public void SelectConnectorSources_MixedCatalog_ReturnsOnlyEnabledConnectorKindSources()
    {
        var sources = new List<SourceDefinition>
        {
            Source("gen1", enabled: true, SourceKinds.Generator),
            Source("url1", enabled: true, SourceKinds.Url),
            Source("url2-disabled", enabled: false, SourceKinds.Url),
            Source("grpc1", enabled: true, SourceKinds.Grpc),
        };

        var selected = ConnectorSourceSweep.SelectConnectorSources(sources).Select(s => s.Name).ToList();

        Assert.Equal(["url1", "grpc1"], selected);
    }
}
