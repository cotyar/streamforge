using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Catalog;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W4: unit tests for the actor-framework-free catalog logic ported from
/// Orleans' RegistryGrain (see CatalogStore's class doc). No Dapr sidecar, no actor runtime — CatalogStore
/// is a plain class over an in-memory CatalogState, which is exactly the point of factoring it out this
/// way.
/// </summary>
public class CatalogStoreTests
{
    private static (CatalogState State, CatalogStore Store, TestLifecycleOrchestrator Orchestrator) NewStore()
    {
        var state = new CatalogState();
        var orchestrator = new TestLifecycleOrchestrator();
        var store = new CatalogStore(state, orchestrator);
        return (state, store, orchestrator);
    }

    [Fact]
    public void EnsureInitialized_OnEmptyState_SeedsSourcesPipelinesAndTables()
    {
        var (state, store, _) = NewStore();

        var dirty = store.EnsureInitialized();

        Assert.True(dirty);
        Assert.NotEmpty(state.Sources);
        Assert.NotEmpty(state.Pipelines);
        Assert.NotEmpty(state.Tables);
    }

    [Fact]
    public void EnsureInitialized_KeepsSeededTableStatusesAsSeedCatalogDeclaresThem()
    {
        // Plan 005 W7-A UPDATE to the W4 "seed status" decision (same update W6 already made for
        // pipelines — see EnsureInitialized_KeepsSeededPipelineStatusesAsSeedCatalogDeclaresThem):
        // TableActor now exists, and TableSupervisorService's boot sweep resumes every seeded Running
        // table for real — a seeded "Running" table is no longer force-stopped. SeedCatalog.Tables()
        // seeds a mix of Running ("positions", "leg_exposure", "order_states") and Stopped
        // ("gold_tier_orders", "hot_symbols" — the latter deliberately, so starting a table-over-table
        // chain is a user action, not an implicit boot race) — both statuses must survive
        // EnsureInitialized untouched.
        var (state, store, _) = NewStore();

        store.EnsureInitialized();

        Assert.Contains(state.Tables, t => t.Status == PipelineStatus.Running);
        Assert.Contains(state.Tables, t => t.Status == PipelineStatus.Stopped);
    }

    [Fact]
    public void EnsureInitialized_KeepsSeededPipelineStatusesAsSeedCatalogDeclaresThem()
    {
        // Plan 005 W6 UPDATE to the W4 "seed status" decision: PipelineActor now exists, and
        // PipelineSupervisorService's boot sweep resumes every seeded Running pipeline for real — a
        // seeded "Running" pipeline is no longer force-stopped, mirroring Orleans' own
        // RegistryGrain.EnsureInitializedAsync resume-on-boot behavior. SeedCatalog.Pipelines() seeds a
        // mix of Running and Stopped pipelines (see that method's doc comment) — both statuses must
        // survive EnsureInitialized untouched.
        var (state, store, _) = NewStore();

        store.EnsureInitialized();

        Assert.Contains(state.Pipelines, p => p.Status == PipelineStatus.Running);
        Assert.Contains(state.Pipelines, p => p.Status == PipelineStatus.Stopped);
    }

    [Fact]
    public void EnsureInitialized_CompilesSeededTables_OutputFieldsPopulated()
    {
        var (state, store, _) = NewStore();

        store.EnsureInitialized();

        // "positions" is a plain running aggregate over "trades" (see SeedCatalog.Tables) — it should
        // compile cleanly against the seeded sources and get a non-empty output schema.
        var positions = state.Tables.First(t => t.Name == "positions");
        Assert.NotEmpty(positions.OutputFields);
        Assert.Null(positions.Error);
    }

    [Fact]
    public void EnsureInitialized_OnNonEmptyState_IsNotDirty()
    {
        var (_, store, _) = NewStore();
        store.EnsureInitialized();

        var dirtyAgain = store.EnsureInitialized();

        Assert.False(dirtyAgain);
    }

    [Fact]
    public async Task UpsertSourceAsync_NewSource_AddsAndNotifiesOrchestrator()
    {
        var (state, store, orchestrator) = NewStore();
        var def = new SourceDefinition { Name = "custom", Fields = [new FieldDef("x", FieldType.Long)], Enabled = true };

        await store.UpsertSourceAsync(def);

        Assert.Contains(state.Sources, s => s.Name == "custom");
        Assert.Contains("NotifySourceChanged:custom:True", orchestrator.Calls);
    }

    [Fact]
    public async Task DeleteSourceAsync_UnknownName_ReturnsFalse()
    {
        var (_, store, orchestrator) = NewStore();

        var removed = await store.DeleteSourceAsync("nope");

        Assert.False(removed);
        Assert.DoesNotContain(orchestrator.Calls, c => c.StartsWith("NotifySourceRemoved"));
    }

    [Fact]
    public async Task CreateTableAsync_DuplicateNameAgainstSource_Throws()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("x", FieldType.Long)] });

        var def = new TableDefinition { Name = "trades", Sql = "SELECT 1 AS x FROM trades" };

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.CreateTableAsync(def));
    }

    [Fact]
    public async Task CreateTableAsync_DuplicateNameAgainstAnotherTable_Throws()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });

        var dup = new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" };

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.CreateTableAsync(dup));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    [InlineData(16)]
    public async Task CreateTableAsync_ParallelismOtherThanOne_Throws(int parallelism)
    {
        // Decision D-F: partitioned execution is Orleans-only — the Dapr registry rejects ANY value
        // other than 1 (stricter than Orleans' own 1..16 range check).
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var def = new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades", Parallelism = parallelism };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.CreateTableAsync(def));
        Assert.Contains("Orleans-only", ex.Message);
    }

    [Fact]
    public async Task CreateTableAsync_ParallelismOne_Succeeds()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var def = new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades", Parallelism = 1 };

        var created = await store.CreateTableAsync(def);

        Assert.Equal(PipelineStatus.Stopped, created.Status);
        Assert.NotEmpty(created.Id);
        Assert.NotEmpty(created.OutputFields);
    }

    [Fact]
    public async Task UpdateTableAsync_UnknownId_ReturnsNull()
    {
        var (_, store, _) = NewStore();

        var result = await store.UpdateTableAsync(new TableDefinition { Id = "missing", Name = "x", Sql = "SELECT 1" });

        Assert.Null(result);
    }

    [Fact]
    public async Task DeleteTableAsync_RunningDependent_Throws()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var positions = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });
        var hot = await store.CreateTableAsync(new TableDefinition { Name = "hot", Sql = "SELECT qty FROM positions" });

        // Mark the dependent Running directly on state (bypassing SetTableStatusAsync's own runtime
        // call, which is irrelevant to this test).
        state.Tables.First(t => t.Id == hot.Id).Status = PipelineStatus.Running;

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.DeleteTableAsync(positions.Id));
    }

    [Fact]
    public async Task DeleteTableAsync_NoDependents_Succeeds()
    {
        var (state, store, orchestrator) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var positions = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });

        var removed = await store.DeleteTableAsync(positions.Id);

        Assert.True(removed);
        Assert.Empty(state.Tables);
        Assert.Contains(orchestrator.Calls, c => c.StartsWith("DisableTableHistory:positions"));
    }

    [Fact]
    public async Task SetTableStatusAsync_StopWithRunningDependent_Throws()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var positions = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });
        var hot = await store.CreateTableAsync(new TableDefinition { Name = "hot", Sql = "SELECT qty FROM positions" });
        state.Tables.First(t => t.Id == positions.Id).Status = PipelineStatus.Running;
        state.Tables.First(t => t.Id == hot.Id).Status = PipelineStatus.Running;

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.SetTableStatusAsync(positions.Id, PipelineStatus.Stopped));
    }

    [Fact]
    public async Task SetTableStatusAsync_StartMissingTableInput_SetsFailedWithoutThrowing()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var positions = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });
        var hot = await store.CreateTableAsync(new TableDefinition { Name = "hot", Sql = "SELECT qty FROM positions" });
        // positions (hot's table input) is left Stopped.

        var result = await store.SetTableStatusAsync(hot.Id, PipelineStatus.Running);

        Assert.NotNull(result);
        Assert.Equal(PipelineStatus.Failed, result!.Status);
        Assert.Contains("positions", result.Error);
    }

    [Fact]
    public async Task SetTableStatusAsync_StartWithOrchestratorFailure_SetsFailedStatusAndError()
    {
        var (state, store, orchestrator) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var positions = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });
        orchestrator.FailStarts = true;
        orchestrator.FailureMessage = "no runtime yet";

        var result = await store.SetTableStatusAsync(positions.Id, PipelineStatus.Running);

        Assert.Equal(PipelineStatus.Failed, result!.Status);
        Assert.Equal("no runtime yet", result.Error);
    }

    [Fact]
    public async Task SetTableStatusAsync_StartSucceeds_SetsRunningAndClearsError()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var positions = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });

        var result = await store.SetTableStatusAsync(positions.Id, PipelineStatus.Running);

        Assert.Equal(PipelineStatus.Running, result!.Status);
        Assert.Null(result.Error);
    }

    [Fact]
    public async Task CreatePipelineAsync_AssignsIdAndStopsAndPublishesLifecycle()
    {
        var (_, store, orchestrator) = NewStore();

        var created = await store.CreatePipelineAsync(new PipelineDefinition { Name = "p1", Sql = "SELECT 1" });

        Assert.NotEmpty(created.Id);
        Assert.Equal(PipelineStatus.Stopped, created.Status);
        Assert.Contains(orchestrator.Calls, c => c.StartsWith($"Lifecycle:{created.Id}:created"));
    }

    [Fact]
    public async Task SetPipelineStatusAsync_UnknownId_ReturnsNull()
    {
        var (_, store, _) = NewStore();

        var result = await store.SetPipelineStatusAsync("missing", PipelineStatus.Running);

        Assert.Null(result);
    }

    [Fact]
    public async Task SetPipelineStatusAsync_Start_PassesFullSourceListToOrchestrator()
    {
        // Plan 005 W6: CatalogStore already holds state.Sources in full at this call site (no facade
        // lookup needed — see ILifecycleOrchestrator.StartPipelineAsync's W6 signature-change doc
        // comment) and must pass ALL of it through, not just the pipeline's own dependencies — mirrors
        // PipelineGrain.StartAsync building schemas from every registered source before compiling.
        var (state, store, orchestrator) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        state.Sources.Add(new SourceDefinition { Name = "quotes", Fields = [new FieldDef("bid", FieldType.Double)] });
        var created = await store.CreatePipelineAsync(new PipelineDefinition { Name = "p1", Sql = "SELECT 1 FROM trades" });

        await store.SetPipelineStatusAsync(created.Id, PipelineStatus.Running);

        Assert.Contains($"StartPipeline:{created.Id}:2", orchestrator.Calls);
    }

    [Fact]
    public async Task SetPipelineStatusAsync_StartWithOrchestratorFailure_SetsFailedStatusAndError()
    {
        var (state, store, orchestrator) = NewStore();
        var created = await store.CreatePipelineAsync(new PipelineDefinition { Name = "p1", Sql = "SELECT 1" });
        orchestrator.FailStarts = true;
        orchestrator.FailureMessage = "compile failed";

        var result = await store.SetPipelineStatusAsync(created.Id, PipelineStatus.Running);

        Assert.Equal(PipelineStatus.Failed, result!.Status);
        Assert.Equal("compile failed", result.Error);
    }

    [Fact]
    public async Task SetPipelineStatusAsync_StartSucceeds_SetsRunningAndClearsError()
    {
        var (_, store, _) = NewStore();
        var created = await store.CreatePipelineAsync(new PipelineDefinition { Name = "p1", Sql = "SELECT 1" });

        var result = await store.SetPipelineStatusAsync(created.Id, PipelineStatus.Running);

        Assert.Equal(PipelineStatus.Running, result!.Status);
        Assert.Null(result.Error);
    }

    [Fact]
    public async Task SetPipelineStatusAsync_Stop_CallsOrchestratorStopPipeline()
    {
        var (_, store, orchestrator) = NewStore();
        var created = await store.CreatePipelineAsync(new PipelineDefinition { Name = "p1", Sql = "SELECT 1" });
        await store.SetPipelineStatusAsync(created.Id, PipelineStatus.Running);

        var result = await store.SetPipelineStatusAsync(created.Id, PipelineStatus.Stopped);

        Assert.Equal(PipelineStatus.Stopped, result!.Status);
        Assert.Contains($"StopPipeline:{created.Id}", orchestrator.Calls);
    }

    // ------------------------------------------------------------------
    // Table-over-pipeline (plan 025, PARITY.md D6) — a pipeline's compiled output schema
    // (PipelineDefinition.OutputFields) lets a table name it as a relation exactly like a source or
    // another table. See Catalog.CatalogStore.BuildTableStreamSchemas/ApplyCompileResult/
    // ApplyPipelineCompileResult for the implementation this exercises.
    // ------------------------------------------------------------------

    [Fact]
    public async Task CreatePipelineAsync_CompilesSql_SetsOutputFields()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });

        var created = await store.CreatePipelineAsync(new PipelineDefinition { Name = "p1", Sql = "SELECT qty FROM trades" });

        Assert.NotEmpty(created.OutputFields);
    }

    [Fact]
    public async Task UpdatePipelineAsync_RecompilesSql_UpdatesOutputFields()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition
        {
            Name = "trades",
            Fields = [new FieldDef("qty", FieldType.Long), new FieldDef("price", FieldType.Double)],
        });
        var created = await store.CreatePipelineAsync(new PipelineDefinition { Name = "p1", Sql = "SELECT qty FROM trades" });
        Assert.Single(created.OutputFields);

        created.Sql = "SELECT qty, price FROM trades";
        var updated = await store.UpdatePipelineAsync(created);

        Assert.Equal(2, updated!.OutputFields.Count);
    }

    [Fact]
    public void EnsureInitialized_BackfillsPipelineOutputFieldsForPreExistingRecords()
    {
        // Mirrors RegistryGrain.EnsureInitializedAsync's backfill: a pipeline persisted before OutputFields
        // existed (SourceNames already set, OutputFields still empty) gets it filled in from the data,
        // driven purely by the empty-field predicate — not by whether seeding just happened.
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var pipeline = new PipelineDefinition
        {
            Id = "p1",
            Name = "p1",
            Sql = "SELECT qty FROM trades",
            SourceNames = ["trades"],
            OutputFields = [],
        };
        state.Pipelines.Add(pipeline);

        var dirty = store.EnsureInitialized();

        Assert.True(dirty);
        Assert.NotEmpty(pipeline.OutputFields);
    }

    [Fact]
    public async Task CreateTableAsync_OverPipeline_ReportsPipelineInputNotStreamInput()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        await store.CreatePipelineAsync(new PipelineDefinition { Name = "vwap", Sql = "SELECT qty FROM trades" });

        var table = await store.CreateTableAsync(new TableDefinition { Name = "t1", Sql = "SELECT qty FROM vwap" });

        Assert.Empty(table.StreamInputs);
        Assert.Contains("vwap", table.PipelineInputs);
        Assert.NotEmpty(table.OutputFields);
    }

    [Fact]
    public async Task UpsertSourceAsync_NameCollidesWithPipeline_Throws()
    {
        var (_, store, _) = NewStore();
        await store.CreatePipelineAsync(new PipelineDefinition { Name = "p1", Sql = "SELECT 1" });

        var def = new SourceDefinition { Name = "p1", Fields = [new FieldDef("x", FieldType.Long)] };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.UpsertSourceAsync(def));
        Assert.Contains("already used by a pipeline", ex.Message);
    }

    [Fact]
    public async Task CreatePipelineAsync_NameCollidesWithSource_Throws()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("x", FieldType.Long)] });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.CreatePipelineAsync(new PipelineDefinition { Name = "trades", Sql = "SELECT 1" }));
        Assert.Contains("already used by a stream source", ex.Message);
    }

    [Fact]
    public async Task CreatePipelineAsync_NameCollidesWithTable_Throws()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.CreatePipelineAsync(new PipelineDefinition { Name = "positions", Sql = "SELECT 1" }));
        Assert.Contains("already used by a table", ex.Message);
    }

    [Fact]
    public async Task CreateTableAsync_DuplicateNameAgainstPipeline_Throws()
    {
        var (_, store, _) = NewStore();
        await store.CreatePipelineAsync(new PipelineDefinition { Name = "p1", Sql = "SELECT 1" });

        var def = new TableDefinition { Name = "p1", Sql = "SELECT 1 AS x" };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.CreateTableAsync(def));
        Assert.Contains("already used by a pipeline", ex.Message);
    }

    [Fact]
    public async Task SetTableStatusAsync_StartOverStoppedPipelineInput_SucceedsWithoutRefusal()
    {
        // Mirrors RegistryGrain.SetTableStatusAsync: unlike a table-over-table dependency, a stopped (or
        // never-started) PIPELINE input is NOT refused at start — the table simply receives nothing until
        // that pipeline runs (pipelines have no replay/attach, hard rule 6).
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        await store.CreatePipelineAsync(new PipelineDefinition { Name = "vwap", Sql = "SELECT qty FROM trades" });
        // The pipeline is created Stopped (CreatePipelineAsync's contract) and never started.
        var table = await store.CreateTableAsync(new TableDefinition { Name = "t1", Sql = "SELECT qty FROM vwap" });

        var result = await store.SetTableStatusAsync(table.Id, PipelineStatus.Running);

        Assert.Equal(PipelineStatus.Running, result!.Status);
        Assert.Null(result.Error);
    }

    [Fact]
    public async Task DeletePipelineAsync_WithRunningDependentTable_SucceedsWithoutRefusal()
    {
        // Mirrors RegistryGrain.DeletePipelineAsync: a table reading this pipeline is NOT refused and NOT
        // stopped — exactly what already happens when a SOURCE a table reads is deleted. Unlike
        // DeleteTableAsync's ThrowIfRunningDependents (table-over-table), there is no equivalent guard for
        // table-over-pipeline: a pipeline owns no durable state on the dependent's behalf.
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var pipeline = await store.CreatePipelineAsync(new PipelineDefinition { Name = "vwap", Sql = "SELECT qty FROM trades" });
        var table = await store.CreateTableAsync(new TableDefinition { Name = "t1", Sql = "SELECT qty FROM vwap" });
        await store.SetTableStatusAsync(table.Id, PipelineStatus.Running);

        var removed = await store.DeletePipelineAsync(pipeline.Id);

        Assert.True(removed);
        Assert.DoesNotContain(state.Pipelines, p => p.Id == pipeline.Id);
        Assert.Equal(PipelineStatus.Running, state.Tables.First(t => t.Id == table.Id).Status);
    }

    [Fact]
    public void EnsureFieldNumbers_AssignsSequentialNumbersAndIsStableAcrossCalls()
    {
        var (_, store, _) = NewStore();
        var fields = new List<FieldDef> { new("a", FieldType.String), new("b", FieldType.Long) };

        var json1 = store.EnsureFieldNumbers("source:x", fields);
        var json2 = store.EnsureFieldNumbers("source:x", fields);

        Assert.Equal(json1, json2);
        Assert.Contains("\"a\":1", json1);
        Assert.Contains("\"b\":2", json1);
    }

    [Fact]
    public void EnsureFieldNumbers_RemovedFieldNumberIsReservedNotReused()
    {
        var (_, store, _) = NewStore();
        var withTwo = new List<FieldDef> { new("a", FieldType.String), new("b", FieldType.Long) };
        store.EnsureFieldNumbers("source:x", withTwo);

        var onlyA = new List<FieldDef> { new("a", FieldType.String) };
        store.EnsureFieldNumbers("source:x", onlyA);

        // Re-add "b" — must NOT get number 2 back (still reserved) — the new number is 3.
        var json = store.EnsureFieldNumbers("source:x", withTwo);
        Assert.Contains("\"b\":3", json);
    }
}
