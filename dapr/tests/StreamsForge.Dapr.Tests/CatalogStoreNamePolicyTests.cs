using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Catalog;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 016 wave 1-C — this flavor's half of the name policy, and it is the half that was missing
/// entirely: <c>CatalogStore</c> had <b>no rename guard at all</b>, not even the sharded-table one
/// Orleans has carried since plan 011 D2. A Running table could be renamed out from under its own
/// actor — whose id IS the table name — leaving the old actor running, the old history tier live under
/// the old key, and the new name addressing nothing until somebody restarted it.
///
/// <para>Same three conditions as <c>RegistryGrain.ValidateTableRenameAllowed</c> (Stopped, unsharded,
/// no dependents), same pipeline-name uniqueness, same messages, so the two flavors refuse for the same
/// reasons in the same words.</para>
///
/// <para>New file rather than an edit to <see cref="CatalogStoreTests"/>, per the file-ownership
/// rule.</para>
/// </summary>
public class CatalogStoreNamePolicyTests
{
    private const string Sql = "SELECT symbol, price FROM trades LATEST BY (symbol)";

    /// <summary>In-process, <c>CreateTableAsync</c> returns the very object the catalog stored — mutating
    /// it would change <c>existing</c> too and the store would never see a rename at all. Every update
    /// here therefore passes a FRESH definition carrying the created id, which is also what the actor
    /// runtime does for real (arguments cross a serialization boundary). Same idiom as
    /// <see cref="CatalogStoreShardByTests"/>.</summary>
    private static TableDefinition Renamed(TableDefinition created, string newName, string? sql = null) => new()
    {
        Id = created.Id,
        Name = newName,
        Sql = sql ?? created.Sql,
        HistoryEnabled = created.HistoryEnabled,
        ShardBy = [.. created.ShardBy],
    };

    private static (CatalogState State, TestLifecycleOrchestrator Orchestrator, CatalogStore Store) NewStore()
    {
        var state = new CatalogState();
        state.Sources.Add(new SourceDefinition
        {
            Name = "trades",
            Fields = [new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)],
        });
        var orchestrator = new TestLifecycleOrchestrator();
        return (state, orchestrator, new CatalogStore(state, orchestrator));
    }

    // =============================================================================================
    // Pipelines.
    // =============================================================================================

    [Fact]
    public async Task CreatePipeline_refuses_a_name_another_pipeline_already_uses()
    {
        var (state, _, store) = NewStore();
        await store.CreatePipelineAsync(new PipelineDefinition { Name = "p", Sql = "SELECT * FROM trades" });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.CreatePipelineAsync(new PipelineDefinition { Name = "p", Sql = "SELECT * FROM trades" }));

        Assert.Contains("already used by another pipeline", ex.Message, StringComparison.Ordinal);
        Assert.Single(state.Pipelines);
    }

    [Fact]
    public async Task UpdatePipeline_refuses_renaming_onto_another_pipelines_name()
    {
        var (_, _, store) = NewStore();
        await store.CreatePipelineAsync(new PipelineDefinition { Name = "taken", Sql = "SELECT * FROM trades" });
        var mine = await store.CreatePipelineAsync(new PipelineDefinition { Name = "mine", Sql = "SELECT * FROM trades" });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.UpdatePipelineAsync(new PipelineDefinition { Id = mine.Id, Name = "taken", Sql = mine.Sql }));
    }

    [Fact]
    public async Task UpdatePipeline_allows_a_free_name_and_does_not_collide_with_itself()
    {
        var (_, _, store) = NewStore();
        var created = await store.CreatePipelineAsync(new PipelineDefinition { Name = "old", Sql = "SELECT * FROM trades" });

        Assert.Equal("new", (await store.UpdatePipelineAsync(
            new PipelineDefinition { Id = created.Id, Name = "new", Sql = created.Sql }))!.Name);
        Assert.NotNull(await store.UpdatePipelineAsync(
            new PipelineDefinition { Id = created.Id, Name = "new", Sql = created.Sql }));
    }

    /// <summary>SUPERSEDED by plan 025 (table-over-pipeline) — rewritten in place rather than left to
    /// silently pass on stale text. A pipeline's compiled <c>OutputFields</c> now let a table name it as
    /// a relation exactly like a source, so a pipeline name must be unique against sources too (and
    /// tables), on the same "one relation name resolves to exactly one entity" terms this class's table
    /// tests already enforce for a table name colliding with a source. See <c>Catalog.CatalogStore.
    /// ValidateUniquePipelineName</c>'s rewritten doc comment for the full argument.</summary>
    [Fact]
    public async Task A_pipeline_may_no_longer_share_its_name_with_a_source()
    {
        var (_, _, store) = NewStore();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.CreatePipelineAsync(new PipelineDefinition { Name = "trades", Sql = "SELECT * FROM trades" }));

        Assert.Contains("already used by a stream source", ex.Message, StringComparison.Ordinal);
    }

    // =============================================================================================
    // Tables.
    // =============================================================================================

    [Fact]
    public async Task A_stopped_unsharded_table_with_no_dependents_renames()
    {
        var (state, _, store) = NewStore();
        var table = await store.CreateTableAsync(new TableDefinition { Name = "t", Sql = Sql });

        var updated = await store.UpdateTableAsync(Renamed(table, "t_renamed"));

        Assert.Equal("t_renamed", updated!.Name);
        Assert.Equal(table.Id, updated.Id);
        Assert.Equal("t_renamed", state.Tables.Single().Name);
    }

    /// <summary>The old name's history tier is torn down BEFORE the new name is stored — the Dapr twin
    /// of <c>RegistryGrain.ReleaseRenamedTiersAsync</c>. Asserted through the orchestrator's call log,
    /// which is the only observable this flavor offers without an actor runtime.</summary>
    [Fact]
    public async Task Renaming_disables_the_old_names_history_tier()
    {
        var (_, orchestrator, store) = NewStore();
        var table = await store.CreateTableAsync(new TableDefinition { Name = "t", Sql = Sql, HistoryEnabled = true });
        orchestrator.Calls.Clear();

        await store.UpdateTableAsync(Renamed(table, "t_renamed"));

        Assert.Contains("DisableTableHistory:t", orchestrator.Calls);
        Assert.DoesNotContain("DisableTableHistory:t_renamed", orchestrator.Calls);
        // …and the NEW name gets a tier, or history would silently stop working for the renamed table.
        Assert.Contains("ResetTableHistory:t_renamed", orchestrator.Calls);
    }

    [Fact]
    public async Task A_running_table_is_not_renameable()
    {
        var (_, _, store) = NewStore();
        var table = await store.CreateTableAsync(new TableDefinition { Name = "t", Sql = Sql });
        var running = await store.SetTableStatusAsync(table.Id, PipelineStatus.Running);
        Assert.Equal(PipelineStatus.Running, running!.Status);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.UpdateTableAsync(Renamed(running, "t_renamed")));
        Assert.Contains("Stop it first", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>ShardBy is stored but never started on this flavor (see <c>CatalogStoreShardByTests</c>).
    /// The rename guard checks it anyway: a definition can carry the field, and a guard that quietly
    /// means something different per flavor is worse than one redundant check.</summary>
    [Fact]
    public async Task A_sharded_table_is_not_renameable_even_though_it_could_never_run_here()
    {
        var (_, _, store) = NewStore();
        var table = await store.CreateTableAsync(new TableDefinition { Name = "t", Sql = Sql, ShardBy = ["symbol"] });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.UpdateTableAsync(Renamed(table, "t_renamed")));
        Assert.Contains("is sharded by", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_table_another_stopped_table_reads_from_is_not_renameable()
    {
        var (_, _, store) = NewStore();
        var upstream = await store.CreateTableAsync(new TableDefinition { Name = "up", Sql = Sql });
        var downstream = await store.CreateTableAsync(new TableDefinition
        {
            Name = "down",
            Sql = "SELECT symbol, COUNT(*) AS n FROM up GROUP BY symbol",
        });
        Assert.Contains("up", downstream.TableInputs);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.UpdateTableAsync(Renamed(upstream, "up_renamed")));
        Assert.Contains("down", ex.Message, StringComparison.Ordinal);
        Assert.Contains("read from it by name", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>A non-rename edit on a Running table with a dependent is untouched by any of this — the
    /// guard fires on the NAME changing and on nothing else.</summary>
    [Fact]
    public async Task A_non_rename_edit_is_unaffected()
    {
        var (_, _, store) = NewStore();
        var upstream = await store.CreateTableAsync(new TableDefinition { Name = "up", Sql = Sql });
        await store.CreateTableAsync(new TableDefinition
        {
            Name = "down",
            Sql = "SELECT symbol, COUNT(*) AS n FROM up GROUP BY symbol",
        });
        var running = await store.SetTableStatusAsync(upstream.Id, PipelineStatus.Running);

        var edited = Renamed(running!, "up");
        edited.Description = "edited while running, with a dependent, without renaming";
        var updated = await store.UpdateTableAsync(edited);

        Assert.Equal("up", updated!.Name);
        Assert.Equal("edited while running, with a dependent, without renaming", updated.Description);
    }
}
