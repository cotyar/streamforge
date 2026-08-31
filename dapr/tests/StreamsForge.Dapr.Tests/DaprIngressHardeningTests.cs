using Dapr.Client;
using Microsoft.Extensions.Logging.Abstractions;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Ingest;
using StreamsForge.Dapr.Host.Facades;
using StreamsForge.Dapr.Host.Ingest;
using StreamsForge.Host.Auth;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 009 A1: <see cref="DaprIngressFacade"/>'s additions — batch idempotency, row-level dedup,
/// per-source push-key validation, and honest InstanceId/Aggregated=false labelling. New file
/// alongside the existing plan 008 <c>DaprIngressFacadeTests</c> (that file's own doc comment explains
/// why non-Inline policies are safe to test without a live sidecar — same reasoning applies here).
/// </summary>
public class DaprIngressHardeningTests
{
    // Plan 021: see DaprIngressFacadeTests' identical helper for why this wraps `catalog` in a
    // single-environment ICatalogFacadeFactory.
    private static DaprIngressFacade NewFacade(FakeCatalogFacade catalog) =>
        new(new SingleEnvironmentCatalogFacadeFactory(catalog), new SourceIngressRegistry(), new DaprClientBuilder().Build(), NullLogger<DaprIngressFacade>.Instance);

    private sealed class SingleEnvironmentCatalogFacadeFactory(ICatalogFacade catalog) : ICatalogFacadeFactory
    {
        public ICatalogFacade For(string environment) => catalog;
    }

    private static SourceDefinition IngestSource(string name, IngestConfig? config = null) => new()
    {
        Name = name,
        Kind = SourceKinds.Ingest,
        Ingest = config ?? new IngestConfig { CapacityRows = 100, MaxBatchRows = 100 },
        Fields = [new FieldDef("price", FieldType.Double), new FieldDef("id", FieldType.String)],
    };

    // ------------------------------------------------------------------
    // A1.1 — batch idempotency.
    // ------------------------------------------------------------------

    [Fact]
    public async Task PushAsync_a_repeated_idempotency_key_replays_the_first_result_and_admits_nothing_new()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1");
        var facade = NewFacade(catalog);

        var rows = new List<Dictionary<string, object?>> { new() { ["price"] = 1.0 }, new() { ["price"] = 2.0 } };
        var first = await facade.PushAsync("s1", rows, partial: false, "key-1");
        var second = await facade.PushAsync("s1", rows, partial: false, "key-1");

        Assert.False(first.Replayed);
        Assert.True(second.Replayed);
        Assert.Equal(2, second.Accepted);

        var status = await facade.GetStatusAsync("s1");
        Assert.Equal(2, status!.TotalAccepted); // only the FIRST push actually admitted
        Assert.Equal(2, status.DepthRows);
    }

    [Fact]
    public async Task PushAsync_without_an_idempotency_key_admits_every_call()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1");
        var facade = NewFacade(catalog);

        await facade.PushAsync("s1", [new() { ["price"] = 1.0 }], partial: false);
        await facade.PushAsync("s1", [new() { ["price"] = 1.0 }], partial: false);

        var status = await facade.GetStatusAsync("s1");
        Assert.Equal(2, status!.TotalAccepted);
    }

    // ------------------------------------------------------------------
    // A1.1 — row-level dedup.
    // ------------------------------------------------------------------

    [Fact]
    public async Task PushAsync_a_row_level_duplicate_is_counted_as_Duplicate_and_the_accounting_invariant_holds()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1", new IngestConfig { CapacityRows = 100, MaxBatchRows = 100, DedupKeyField = "id" });
        var facade = NewFacade(catalog);

        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["id"] = "r1", ["price"] = 1.0 },
            new() { ["id"] = "r1", ["price"] = 2.0 }, // duplicate of r1
            new() { ["id"] = "r2", ["price"] = 3.0 },
            new() { ["price"] = "not-a-number" }, // invalid (coercion failure)
        };
        var result = await facade.PushAsync("s1", rows, partial: true);

        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(2, result.Accepted); // r1 (first) + r2
        Assert.Equal(1, result.Duplicate); // second r1
        Assert.Equal(1, result.Invalid);
        Assert.Equal(0, result.Dropped);
        Assert.Equal(rows.Count, result.Accepted + result.Dropped + result.Invalid + result.Duplicate);
    }

    // ------------------------------------------------------------------
    // A1.2 — per-source push keys.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ValidateKeyAsync_a_source_with_no_configured_keys_is_JWT_only_not_open()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1", new IngestConfig());
        var facade = NewFacade(catalog);

        Assert.False(await facade.ValidateKeyAsync("s1", "any-secret"));
        Assert.False(await facade.ValidateKeyAsync("s1", null));
        Assert.False(await facade.ValidateKeyAsync("s1", ""));
    }

    [Fact]
    public async Task ValidateKeyAsync_matches_the_correct_secret_only()
    {
        var catalog = new FakeCatalogFacade();
        const string secret = "sfk_correct-secret";
        var (hash, salt) = PasswordHasher.Hash(secret);
        catalog.Sources["s1"] = IngestSource("s1", new IngestConfig
        {
            CapacityRows = 10,
            MaxBatchRows = 10,
            Keys = [new IngestKey { Id = "k1", Hash = hash, Salt = salt, Label = "test" }],
        });
        var facade = NewFacade(catalog);

        Assert.True(await facade.ValidateKeyAsync("s1", secret));
        Assert.False(await facade.ValidateKeyAsync("s1", "wrong-secret"));
    }

    [Fact]
    public async Task ValidateKeyAsync_returns_false_for_unknown_or_non_ingest_sources()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["gen"] = new SourceDefinition { Name = "gen", Kind = SourceKinds.Generator };
        var facade = NewFacade(catalog);

        Assert.False(await facade.ValidateKeyAsync("nope", "key"));
        Assert.False(await facade.ValidateKeyAsync("gen", "key"));
    }

    [Fact]
    public async Task ValidateKeyAsync_stops_matching_once_a_key_is_removed_from_the_config()
    {
        var catalog = new FakeCatalogFacade();
        const string secret = "sfk_revocable";
        var (hash, salt) = PasswordHasher.Hash(secret);
        var config = new IngestConfig { CapacityRows = 10, MaxBatchRows = 10, Keys = [new IngestKey { Id = "k1", Hash = hash, Salt = salt }] };
        catalog.Sources["s1"] = IngestSource("s1", config);
        var facade = NewFacade(catalog);
        Assert.True(await facade.ValidateKeyAsync("s1", secret));

        config.Keys.RemoveAll(k => k.Id == "k1"); // mutating in place, same as what a revoke does via UpsertSourceAsync

        Assert.False(await facade.ValidateKeyAsync("s1", secret));
    }

    // ------------------------------------------------------------------
    // A1.3 — honest counters: no cluster on this flavor, so Aggregated is always false.
    // ------------------------------------------------------------------

    [Fact]
    public async Task GetStatusAsync_always_reports_Aggregated_false_and_a_nonempty_InstanceId()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1");
        var facade = NewFacade(catalog);

        await facade.PushAsync("s1", [new() { ["price"] = 1.0 }], partial: false);
        var status = await facade.GetStatusAsync("s1");

        Assert.False(status!.Aggregated);
        Assert.False(string.IsNullOrEmpty(status.InstanceId));
    }

    [Fact]
    public async Task GetStatusAsync_reports_Aggregated_false_even_before_any_push()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1");
        var facade = NewFacade(catalog);

        var status = await facade.GetStatusAsync("s1");

        Assert.NotNull(status);
        Assert.False(status!.Aggregated);
        Assert.False(string.IsNullOrEmpty(status.InstanceId));
    }

    private sealed class FakeCatalogFacade : ICatalogFacade
    {
    /// <summary>Interface conformance only — wishlist #8's run-on-demand needs a real runtime to
    /// publish, so a fake correctly reports that there is nothing to run.</summary>
    public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) =>
        Task.FromResult(new ScenarioRunResult { Outcome = ScenarioRunOutcome.NotFound });

        public Dictionary<string, SourceDefinition> Sources { get; } = [];

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(Sources.Values.ToList());

        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(Sources.TryGetValue(name, out var def) ? def : null);

        public Task UpsertSourceAsync(SourceDefinition def)
        {
            Sources[def.Name] = def;
            return Task.CompletedTask;
        }

        public Task<bool> DeleteSourceAsync(string name) => Task.FromResult(Sources.Remove(name));

        public Task<List<PipelineDefinition>> GetPipelinesAsync() => throw new NotImplementedException();
        public Task<PipelineDefinition?> GetPipelineAsync(string id) => throw new NotImplementedException();
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
        public Task<bool> DeletePipelineAsync(string id) => throw new NotImplementedException();
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();

        public Task<List<TableDefinition>> GetTablesAsync() => throw new NotImplementedException();
        public Task<TableDefinition?> GetTableAsync(string id) => throw new NotImplementedException();
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotImplementedException();
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotImplementedException();
        public Task<bool> DeleteTableAsync(string id) => throw new NotImplementedException();
        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();

        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotImplementedException();
    }
}
