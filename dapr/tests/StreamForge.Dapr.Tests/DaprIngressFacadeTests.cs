using Dapr.Client;
using Microsoft.Extensions.Logging.Abstractions;
using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using StreamForge.Dapr.Host.Ingest;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 008 W4c: unit tests for <see cref="DaprIngressFacade"/>'s admission-path logic — everything that
/// does NOT require reaching an actual Dapr sidecar.
///
/// <para><b>Why this is safe without a live sidecar:</b> constructing a <see cref="DaprClient"/> itself
/// does no I/O (the gRPC channel is lazy — same finding <c>GeneratorLifecycleOrchestratorTests</c>'s own
/// class doc documents for this project's other Dapr-client-holding types), and
/// <see cref="SourceIngressBuffer"/> only ever calls its drain delegate (which is what would reach the
/// sidecar) for <see cref="IngressOverflowPolicy.Inline"/> pushes — every other policy's
/// <c>PushAsync</c> either admits into the in-memory queue or rejects/too-large, with zero I/O either
/// way. This file deliberately sticks to non-Inline policies (Reject is the default and what most cases
/// below use) so every test here exercises real admission/coercion logic without ever touching a
/// sidecar. <see cref="DaprIngressFacade.DrainAsync"/> itself (and therefore Inline pushes, and the
/// separate <see cref="IngestDrainPumpService"/>) is exactly the kind of "requires a live sidecar" path
/// this project's convention (see <c>GeneratorLifecycleOrchestratorTests</c>) leaves to a live/scripted
/// check instead of a unit test.</para>
/// </summary>
public class DaprIngressFacadeTests
{
    private static DaprIngressFacade NewFacade(FakeCatalogFacade catalog) =>
        new(catalog, new SourceIngressRegistry(), new DaprClientBuilder().Build(), NullLogger<DaprIngressFacade>.Instance);

    private static SourceDefinition IngestSource(string name, IngestConfig? config = null) => new()
    {
        Name = name,
        Kind = SourceKinds.Ingest,
        Ingest = config ?? new IngestConfig(),
        Fields = [new FieldDef("price", FieldType.Double)],
    };

    [Fact]
    public async Task PushAsync_UnknownSource_ReturnsNotFound()
    {
        var facade = NewFacade(new FakeCatalogFacade());

        var result = await facade.PushAsync("nope", [], partial: false);

        Assert.Equal(IngestOutcome.NotFound, result.Outcome);
        Assert.NotNull(result.Error);
    }

    [Fact]
    public async Task PushAsync_NonIngestKindSource_ReturnsWrongKind()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["gen"] = new SourceDefinition { Name = "gen", Kind = SourceKinds.Generator };
        var facade = NewFacade(catalog);

        var result = await facade.PushAsync("gen", [new Dictionary<string, object?>()], partial: false);

        Assert.Equal(IngestOutcome.WrongKind, result.Outcome);
    }

    [Fact]
    public async Task PushAsync_InvalidRowWithoutPartial_ReturnsInvalid_AndNeverAdmits()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1");
        var facade = NewFacade(catalog);

        var rows = new List<Dictionary<string, object?>> { new() { ["price"] = "not-a-number" } };
        var result = await facade.PushAsync("s1", rows, partial: false);

        Assert.Equal(IngestOutcome.Invalid, result.Outcome);
        Assert.Equal(1, result.Invalid);
        Assert.Equal(0, result.Accepted);
        Assert.Single(result.RowErrors);

        // A whole-batch-rejected push never reaches the buffer at all (coercion happens BEFORE
        // admission) — status must show nothing accepted or invalid-recorded either.
        var status = await facade.GetStatusAsync("s1");
        Assert.Equal(0, status!.TotalAccepted);
        Assert.Equal(0, status.TotalInvalid);
    }

    [Fact]
    public async Task PushAsync_ValidRows_UnderCapacity_ReturnsAccepted()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1", new IngestConfig { CapacityRows = 10, MaxBatchRows = 10 });
        var facade = NewFacade(catalog);

        var rows = new List<Dictionary<string, object?>> { new() { ["price"] = 1.5 }, new() { ["price"] = 2.5 } };
        var result = await facade.PushAsync("s1", rows, partial: false);

        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(2, result.Accepted);
        Assert.Equal(0, result.Dropped);
        Assert.Equal(0, result.Invalid);
    }

    [Fact]
    public async Task PushAsync_PartialWithSomeInvalidRows_AdmitsTheValidOnesAndReportsInvalidCount()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1", new IngestConfig { CapacityRows = 10, MaxBatchRows = 10 });
        var facade = NewFacade(catalog);

        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["price"] = 1.5 },
            new() { ["price"] = "bad" },
        };
        var result = await facade.PushAsync("s1", rows, partial: true);

        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(1, result.Accepted);
        Assert.Equal(1, result.Invalid);
        Assert.Single(result.RowErrors);
    }

    [Fact]
    public async Task PushAsync_OverCapacity_UnderRejectPolicy_ReturnsOverloaded()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1", new IngestConfig
        {
            Policy = IngressOverflowPolicy.Reject,
            CapacityRows = 1,
            MaxBatchRows = 1,
        });
        var facade = NewFacade(catalog);

        // First push fills the one-row buffer (nothing drains it — Reject policy, no pump running here).
        var first = await facade.PushAsync("s1", [new Dictionary<string, object?> { ["price"] = 1.0 }], partial: false);
        Assert.Equal(IngestOutcome.Accepted, first.Outcome);

        // A second, individually-within-limits push now finds no free room.
        var second = await facade.PushAsync("s1", [new Dictionary<string, object?> { ["price"] = 2.0 }], partial: false);

        Assert.Equal(IngestOutcome.Overloaded, second.Outcome);
        Assert.True(second.RetryAfterMs > 0);
    }

    [Fact]
    public async Task PushAsync_OversizedBatch_ReturnsTooLarge()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1", new IngestConfig { CapacityRows = 10, MaxBatchRows = 1 });
        var facade = NewFacade(catalog);

        var rows = new List<Dictionary<string, object?>> { new() { ["price"] = 1.0 }, new() { ["price"] = 2.0 } };
        var result = await facade.PushAsync("s1", rows, partial: false);

        Assert.Equal(IngestOutcome.TooLarge, result.Outcome);
    }

    [Fact]
    public async Task GetStatusAsync_UnknownOrNonIngestSource_ReturnsNull()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["gen"] = new SourceDefinition { Name = "gen", Kind = SourceKinds.Generator };
        var facade = NewFacade(catalog);

        Assert.Null(await facade.GetStatusAsync("nope"));
        Assert.Null(await facade.GetStatusAsync("gen"));
    }

    [Fact]
    public async Task GetStatusAsync_IngestSourceNeverPushed_ReturnsZeroedStatusReflectingConfig()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1", new IngestConfig
        {
            Policy = IngressOverflowPolicy.DropOldest,
            CapacityRows = 42,
            MaxBatchRows = 7,
        });
        var facade = NewFacade(catalog);

        var status = await facade.GetStatusAsync("s1");

        Assert.NotNull(status);
        Assert.Equal(IngressOverflowPolicy.DropOldest, status!.Policy);
        Assert.Equal(42, status.CapacityRows);
        Assert.Equal(7, status.MaxBatchRows);
        Assert.Equal(0, status.TotalAccepted);
        Assert.Equal(0, status.DownstreamDropped);
    }

    [Fact]
    public async Task GetStatusAsync_AfterAPush_ReflectsAcceptedCounters_AndDownstreamDroppedStaysZero()
    {
        var catalog = new FakeCatalogFacade();
        catalog.Sources["s1"] = IngestSource("s1", new IngestConfig { CapacityRows = 10, MaxBatchRows = 10 });
        var facade = NewFacade(catalog);

        await facade.PushAsync("s1", [new Dictionary<string, object?> { ["price"] = 1.0 }], partial: false);
        var status = await facade.GetStatusAsync("s1");

        Assert.NotNull(status);
        Assert.Equal(1, status!.TotalAccepted);
        Assert.Equal(1, status.DepthRows); // still queued: nothing has drained it (Reject policy, no pump running here)
        // This flavor has no equivalent of Orleans' PushStreamBus.TotalDropped to sample — see
        // DaprIngressFacade.GetStatusAsync's doc comment for why this is an honest zero, not a stand-in
        // for "nothing is being lost".
        Assert.Equal(0, status.DownstreamDropped);
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
