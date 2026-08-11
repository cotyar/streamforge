using Orleans.TestingHost;
using StreamForge.Abstractions;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 008 W5: PipelineDefinition.SourceNames round-trip against a real RegistryGrain in a real Orleans
/// TestingHost cluster — mirrors MetadataRegistryRoundTripTests' pattern (same silo/client configurators,
/// same fixture shape) but for the new lineage field: populated on a successful compile at create time,
/// refreshed on update, left empty for SQL that doesn't currently compile (draft-friendly — never blocks
/// create/update on its own, exactly like tables' StreamInputs/TableInputs).
/// </summary>
public sealed class PipelineLineageTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private IRegistryGrain _registry = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<MetadataTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<MetadataTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        // Seeds "trades"/"quotes"/... so the pipeline SQL below has real sources to compile against.
        await _registry.EnsureInitializedAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task CreatePipelineAsync_CompilingSql_PopulatesSourceNames()
    {
        var created = await _registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "lineage_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT symbol FROM trades",
        });

        Assert.Equal(["trades"], created.SourceNames);
    }

    [Fact]
    public async Task CreatePipelineAsync_NonCompilingSql_LeavesSourceNamesEmpty()
    {
        var created = await _registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "lineage_bad_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT * FROM nonexistent_stream",
        });

        // Draft-friendly: creation itself must still succeed even though the SQL doesn't compile.
        Assert.Empty(created.SourceNames);
    }

    [Fact]
    public async Task UpdatePipelineAsync_SqlChangedToDifferentSource_RefreshesSourceNames()
    {
        var created = await _registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "lineage_upd_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT symbol FROM trades",
        });
        Assert.Equal(["trades"], created.SourceNames);

        created.Sql = "SELECT symbol FROM quotes";
        var updated = await _registry.UpdatePipelineAsync(created);

        Assert.NotNull(updated);
        Assert.Equal(["quotes"], updated!.SourceNames);

        // Re-fetch independently to confirm it's persisted registry state, not just the mutated
        // in-memory reference UpdatePipelineAsync happens to return.
        var refetched = await _registry.GetPipelineAsync(created.Id);
        Assert.Equal(["quotes"], refetched!.SourceNames);
    }

    [Fact]
    public async Task UpdatePipelineAsync_SqlChangedToNonCompiling_ClearsSourceNames()
    {
        var created = await _registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "lineage_clear_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT symbol FROM trades",
        });
        Assert.Equal(["trades"], created.SourceNames);

        created.Sql = "SELECT * FROM nonexistent_stream";
        var updated = await _registry.UpdatePipelineAsync(created);

        Assert.NotNull(updated);
        Assert.Empty(updated!.SourceNames);
    }
}
