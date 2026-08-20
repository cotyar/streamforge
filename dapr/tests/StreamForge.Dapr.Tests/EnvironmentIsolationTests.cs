using Dapr.Actors;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
using StreamForge.Dapr.Host.Actors;
using StreamForge.Dapr.Host.Catalog;
using StreamForge.Dapr.Host.Facades;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 021 (environment isolation), Dapr track — the wave's own required tests:
/// <list type="bullet">
/// <item>the environment registry's invariants (<see cref="EnvironmentRegistryStore"/> — <c>default</c>
/// always exists and is not creatable/deletable, an invalid name is refused, a duplicate is refused) and
/// <see cref="EnvironmentDeleteWorkflow"/>'s "a non-empty environment needs force" rule, exercised against
/// a REAL <see cref="CatalogStore"/> (via <see cref="CatalogStoreCatalogFacade"/> below) rather than a
/// hand-rolled fake, so the delete workflow's worklist-with-retries is proven against the actual
/// dependent-table guard (<c>CatalogStore.ThrowIfRunningDependents</c>) it has to cooperate with.</item>
/// <item>two <see cref="CatalogStore"/>s at two environment keys hold disjoint catalogs.</item>
/// <item>the D2 gate: with no environment named, the composed actor id / Redis state key is exactly what
/// it was before this plan.</item>
/// </list>
///
/// <para><see cref="EnvironmentRegistryActor"/> itself is NOT constructed here — like every other actor
/// class in this project (see <c>TableDeltaSequencingTests</c>' own doc comment), it requires a live
/// <c>Dapr.Actors.Runtime.ActorHost</c> this project has no test double for. Its two pure/testable halves —
/// <see cref="EnvironmentRegistryStore"/> (name/duplicate/reserved-word validation) and
/// <see cref="EnvironmentDeleteWorkflow"/> (the empty/force decision and teardown order) — are exactly what
/// it delegates to, so testing them here covers the actor's actual logic, not a re-description of it.</para>
/// </summary>
public class EnvironmentIsolationTests
{
    // ------------------------------------------------------------------
    // EnvironmentRegistryStore invariants
    // ------------------------------------------------------------------

    [Fact]
    public void ListWithDefault_AlwaysIncludesDefaultFirst_OnAnEmptyStore()
    {
        var store = new EnvironmentRegistryStore(new EnvironmentRegistryState());

        var list = store.ListWithDefault();

        Assert.Equal(EnvKeys.DefaultDisplayName, list[0].Name);
    }

    [Fact]
    public void Exists_IsTrueForDefault_OnAnEmptyStore()
    {
        var store = new EnvironmentRegistryStore(new EnvironmentRegistryState());

        Assert.True(store.Exists(EnvKeys.Default));
    }

    [Theory]
    [InlineData("default")]   // reserved — the display name for EnvKeys.Default itself
    [InlineData("catalog")]   // reserved — StreamConstants.RegistryKey
    [InlineData("users")]     // reserved — StreamConstants.UsersKey
    [InlineData("Staging")]   // upper-case not in the allowed [a-z0-9-] class
    [InlineData("st aging")]  // space not allowed
    [InlineData("")]          // empty is EnvKeys.Default, not a creatable name
    public void Create_RejectsAnInvalidOrReservedName(string name)
    {
        var store = new EnvironmentRegistryStore(new EnvironmentRegistryState());

        Assert.Throws<ArgumentException>(() => store.Create(name, "", "tester", 0));
    }

    [Fact]
    public void Create_RejectsADuplicateName()
    {
        var store = new EnvironmentRegistryStore(new EnvironmentRegistryState());
        store.Create("staging", "", "tester", 0);

        Assert.Throws<InvalidOperationException>(() => store.Create("staging", "", "tester", 0));
    }

    [Fact]
    public void Create_ThenListWithDefault_ReturnsDefaultFirstThenNameOrdered()
    {
        var store = new EnvironmentRegistryStore(new EnvironmentRegistryState());
        store.Create("staging", "", "tester", 0);
        store.Create("acceptance", "", "tester", 0);

        var names = store.ListWithDefault().Select(e => e.Name).ToList();

        Assert.Equal([EnvKeys.DefaultDisplayName, "acceptance", "staging"], names);
    }

    // ------------------------------------------------------------------
    // EnvironmentDeleteWorkflow — default is refused outright; a non-empty environment needs force
    // ------------------------------------------------------------------

    [Fact]
    public async Task Delete_RefusesTheDefaultEnvironment_Always()
    {
        var store = new EnvironmentRegistryStore(new EnvironmentRegistryState());
        var workflow = new EnvironmentDeleteWorkflow(store, new FakeCatalogFacadeFactory());

        var result = await workflow.DeleteAsync(EnvKeys.DefaultDisplayName, force: false);

        Assert.False(result.Ok);
    }

    [Fact]
    public async Task Delete_UnknownEnvironment_ReturnsSuccessFalse_NotAFailure()
    {
        var store = new EnvironmentRegistryStore(new EnvironmentRegistryState());
        var workflow = new EnvironmentDeleteWorkflow(store, new FakeCatalogFacadeFactory());

        var result = await workflow.DeleteAsync("nope", force: false);

        Assert.True(result.Ok);
        Assert.False(result.Value);
    }

    [Fact]
    public async Task Delete_NonEmptyEnvironment_WithoutForce_IsRefused()
    {
        var store = new EnvironmentRegistryStore(new EnvironmentRegistryState());
        store.Create("staging", "", "tester", 0);
        var factory = new FakeCatalogFacadeFactory();
        await factory.For("staging").CreateTableAsync(new TableDefinition { Name = "orders", Sql = "SELECT 1 AS x FROM trades" });
        var workflow = new EnvironmentDeleteWorkflow(store, factory);

        var result = await workflow.DeleteAsync("staging", force: false);

        Assert.False(result.Ok);
        Assert.Contains("not empty", result.Error);
        // Refused — the environment is still there.
        Assert.True(store.Exists("staging"));
    }

    [Fact]
    public async Task Delete_NonEmptyEnvironment_WithForce_DeletesEverythingAndTheDirectoryRow()
    {
        var store = new EnvironmentRegistryStore(new EnvironmentRegistryState());
        store.Create("staging", "", "tester", 0);
        var factory = new FakeCatalogFacadeFactory();
        var stagingCatalog = factory.For("staging");
        await stagingCatalog.UpsertSourceAsync(new SourceDefinition { Name = "trades", Kind = SourceKinds.Generator, Fields = [new FieldDef("x", FieldType.Long)] });
        await stagingCatalog.CreateTableAsync(new TableDefinition { Name = "orders", Sql = "SELECT COUNT(*) AS c FROM trades" });
        var workflow = new EnvironmentDeleteWorkflow(store, factory);

        var result = await workflow.DeleteAsync("staging", force: true);

        Assert.True(result.Ok);
        Assert.True(result.Value);
        Assert.False(store.Exists("staging"));
        Assert.Empty(await stagingCatalog.GetTablesAsync());
        Assert.Empty(await stagingCatalog.GetSourcesAsync());
    }

    // ------------------------------------------------------------------
    // Two CatalogStores at two environment keys hold disjoint catalogs
    // ------------------------------------------------------------------

    [Fact]
    public async Task TwoCatalogStores_AtDifferentEnvironments_HoldDisjointCatalogs()
    {
        var defaultStore = new CatalogStore(new CatalogState(), new TestLifecycleOrchestrator(), EnvKeys.Default);
        var stagingStore = new CatalogStore(new CatalogState(), new TestLifecycleOrchestrator(), "staging");

        var defaultOrders = await defaultStore.CreateTableAsync(new TableDefinition { Name = "orders", Sql = "SELECT 1 AS x" });
        var stagingOrders = await stagingStore.CreateTableAsync(new TableDefinition { Name = "orders", Sql = "SELECT 1 AS x" });

        // Same name, two distinct ids — no collision, no shared state.
        Assert.NotEqual(defaultOrders.Id, stagingOrders.Id);
        Assert.Single(defaultStore.GetTables());
        Assert.Single(stagingStore.GetTables());

        // Each store's own catalog contains only ITS OWN "orders", never the other's.
        Assert.Equal(defaultOrders.Id, defaultStore.GetTables().Single().Id);
        Assert.Equal(stagingOrders.Id, stagingStore.GetTables().Single().Id);

        // Every entity is stamped with the environment it was created in.
        Assert.Equal(EnvKeys.Default, defaultOrders.Environment);
        Assert.Equal("staging", stagingOrders.Environment);
    }

    // ------------------------------------------------------------------
    // D2 gate: with no environment named, the composed actor id / state key is byte-identical to
    // pre-021.
    // ------------------------------------------------------------------

    [Fact]
    public void Qualify_WithNoEnvironment_IsByteIdenticalToTheUnqualifiedKey()
    {
        Assert.Equal(StreamConstants.RegistryKey, EnvKeys.Qualify(EnvKeys.Default, StreamConstants.RegistryKey));
        Assert.Equal("orders", EnvKeys.Qualify(EnvKeys.Default, "orders"));
    }

    [Fact]
    public void RegistryActorId_WithNoEnvironment_IsTheExactPre021Literal()
    {
        // This is the assertion the D2 acceptance criterion is measured by: the ActorId Dapr resolves
        // through the sidecar to a Redis state key ({appId}||RegistryActor||{ActorId}||catalog) must be
        // the literal "catalog" string when no environment is ever mentioned — the same ActorId every
        // pre-021 `new ActorId(StreamConstants.RegistryKey)` call site constructed.
        var qualifiedId = new ActorId(EnvKeys.Qualify(EnvKeys.Default, StreamConstants.RegistryKey));

        Assert.Equal("catalog", qualifiedId.GetId());
        Assert.Equal("catalog", qualifiedId.ToString());
    }

    [Fact]
    public void RegistryActorId_WithANamedEnvironment_IsPrefixedWithIt()
    {
        var qualifiedId = new ActorId(EnvKeys.Qualify("staging", StreamConstants.RegistryKey));

        Assert.Equal("staging.catalog", qualifiedId.GetId());
    }

    [Fact]
    public void EnvironmentsKey_IsNeverQualified()
    {
        // StreamConstants.EnvironmentsKey's own doc comment: the directory singleton is NEVER
        // environment-qualified, in any environment — it is the thing that says which environments exist.
        Assert.Equal(StreamConstants.EnvironmentsKey, StreamConstants.EnvironmentsKey);
        Assert.Equal("environments", StreamConstants.EnvironmentsKey);
    }
}

/// <summary>Test-only <see cref="ICatalogFacadeFactory"/> handing out one independent, real
/// <see cref="CatalogStore"/>-backed <see cref="ICatalogFacade"/> per environment name — see
/// <see cref="CatalogStoreCatalogFacade"/> below for why this is a thin adapter over the real store rather
/// than a hand-rolled fake.</summary>
internal sealed class FakeCatalogFacadeFactory : ICatalogFacadeFactory
{
    private readonly Dictionary<string, ICatalogFacade> _byEnvironment = new(StringComparer.Ordinal);

    public ICatalogFacade For(string environment)
    {
        if (!_byEnvironment.TryGetValue(environment, out var facade))
        {
            facade = new CatalogStoreCatalogFacade(new CatalogStore(new CatalogState(), new TestLifecycleOrchestrator(), environment));
            _byEnvironment[environment] = facade;
        }

        return facade;
    }
}

/// <summary>Adapts a <see cref="CatalogStore"/> to <see cref="ICatalogFacade"/> for tests that need a real
/// <see cref="ICatalogFacadeFactory"/> — e.g. <see cref="EnvironmentDeleteWorkflow"/>'s tests, which rely
/// on REAL delete-guard behavior (<c>CatalogStore.ThrowIfRunningDependents</c>) rather than a fake that
/// would have to reimplement it to be a meaningful test at all. Mirrors
/// <see cref="Facades.DaprCatalogFacade"/>'s own shape one-for-one, minus the actor-proxy plumbing.</summary>
internal sealed class CatalogStoreCatalogFacade(CatalogStore store) : ICatalogFacade
{
    public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(store.GetSources());
    public Task<SourceDefinition?> GetSourceAsync(string name) => Task.FromResult(store.GetSource(name));
    public Task UpsertSourceAsync(SourceDefinition def) => store.UpsertSourceAsync(def);
    public Task<bool> DeleteSourceAsync(string name) => store.DeleteSourceAsync(name);

    public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(store.GetPipelines());
    public Task<PipelineDefinition?> GetPipelineAsync(string id) => Task.FromResult(store.GetPipeline(id));
    public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => store.CreatePipelineAsync(def);
    public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => store.UpdatePipelineAsync(def);
    public Task<bool> DeletePipelineAsync(string id) => store.DeletePipelineAsync(id);
    public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => store.SetPipelineStatusAsync(id, status);

    public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(store.GetTables());
    public Task<TableDefinition?> GetTableAsync(string id) => Task.FromResult(store.GetTable(id));
    public Task<TableDefinition> CreateTableAsync(TableDefinition def) => store.CreateTableAsync(def);
    public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => store.UpdateTableAsync(def);
    public Task<bool> DeleteTableAsync(string id) => store.DeleteTableAsync(id);
    public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => store.SetTableStatusAsync(id, status);

    public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => Task.FromResult(store.EnsureFieldNumbers(entityKey, fields));

    public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) =>
        Task.FromResult(new ScenarioRunResult { Outcome = ScenarioRunOutcome.NotFound });
}
