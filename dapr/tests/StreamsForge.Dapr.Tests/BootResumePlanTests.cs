using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Services;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 025 (PARITY.md D6 bullet 2, "consumers before producers at boot, one resume pass"): unit tests for
/// <see cref="BootResumePlan"/> — the pure ordering behind <see cref="CatalogInitializationService"/>'s
/// one-shot boot resume. The pass's I/O half (<see cref="EntityResume"/>, which makes actor-proxy calls) is
/// unreachable without a live sidecar, same limitation every other actor-bound class in this project has;
/// what is testable — and what the whole debt item is about — is WHICH entities resume and in WHAT
/// ORDER.
/// </summary>
public class BootResumePlanTests
{
    private static PipelineDefinition Pipeline(string id, PipelineStatus status) =>
        new() { Id = id, Name = id, Status = status };

    private static TableDefinition Table(string name, PipelineStatus status, params string[] tableInputs) =>
        new() { Id = $"id-{name}", Name = name, Status = status, TableInputs = [.. tableInputs] };

    private static SourceDefinition Source(string name, bool enabled, string kind) =>
        new() { Name = name, Enabled = enabled, Kind = kind };

    [Fact]
    public void Build_PutsRunningPipelinesAndTablesInTheirOwnPhases_AndOnlyStartableSourcesInTheLast()
    {
        var phases = BootResumePlan.Build(
            [Pipeline("p1", PipelineStatus.Running)],
            [Table("t1", PipelineStatus.Running)],
            [Source("s1", enabled: true, SourceKinds.Url)]);

        // The phases ARE the ordering: the caller walks Pipelines, then Tables, then Sources. Consumers
        // before producers — see BootResumePlan's class doc for why the reverse loses rows outright.
        Assert.Equal(["p1"], phases.Pipelines.Select(p => p.Id));
        Assert.Equal(["t1"], phases.Tables.Select(t => t.Name));
        Assert.Equal(["s1"], phases.Sources.Select(s => s.Name));
    }

    [Fact]
    public void Build_ExcludesEntitiesTheCatalogDoesNotSayAreRunning()
    {
        var phases = BootResumePlan.Build(
            [Pipeline("stopped", PipelineStatus.Stopped), Pipeline("failed", PipelineStatus.Failed)],
            [Table("stopped", PipelineStatus.Stopped), Table("failed", PipelineStatus.Failed)],
            []);

        // Boot is not a place to promote anything: a Stopped or Failed entity stays that way until a user
        // (or a repaired config) starts it.
        Assert.Empty(phases.Pipelines);
        Assert.Empty(phases.Tables);
    }

    [Fact]
    public void Build_ExcludesDisabledSources()
    {
        var phases = BootResumePlan.Build([], [], [Source("off", enabled: false, SourceKinds.Url)]);

        Assert.Empty(phases.Sources);
    }

    [Fact]
    public void Build_ExcludesIngestAndCrdtKinds_WhichHaveNoDriverToStartOnThisFlavor()
    {
        var phases = BootResumePlan.Build([], [], [
            Source("ingest", enabled: true, SourceKinds.Ingest),
            Source("doc", enabled: true, SourceKinds.Crdt),
            Source("gen", enabled: true, SourceKinds.Generator),
            Source("url", enabled: true, SourceKinds.Url),
        ]);

        // Ingest has no actor by design (rows arrive through IIngressFacade); crdt has none on Dapr at all
        // (PARITY.md D5). Handing either to a driver would only log an error.
        Assert.Equal(["gen", "url"], phases.Sources.Select(s => s.Name));
    }

    [Fact]
    public void Build_ResumesAThreeTableChainUpstreamFirst()
    {
        // c reads b reads a. Declared in the WRONG order deliberately — the catalog has no inherent order.
        var phases = BootResumePlan.Build([], [
            Table("c", PipelineStatus.Running, "b"),
            Table("a", PipelineStatus.Running),
            Table("b", PipelineStatus.Running, "a"),
        ], []);

        Assert.Equal(["a", "b", "c"], phases.Tables.Select(t => t.Name));
    }

    [Fact]
    public void TopoSort_IgnoresAnUpstreamThatIsNotItselfResuming()
    {
        // "b" reads a table that exists but is Stopped: there is no edge to order against, because the
        // upstream is not in this pass at all. CatalogStore's own dependency guard is what refuses the
        // start; the sweep retries it. Ordering must not pretend to fix that.
        var all = new List<TableDefinition> { Table("stopped", PipelineStatus.Stopped), Table("b", PipelineStatus.Running, "stopped") };

        var sorted = BootResumePlan.TopoSortByTableInputs([all[1]], all);

        Assert.Equal(["b"], sorted.Select(t => t.Name));
    }

    [Fact]
    public void TopoSort_IgnoresAnUpstreamThatIsNotInTheCatalogAtAll()
    {
        var b = Table("b", PipelineStatus.Running, "nonexistent");

        var sorted = BootResumePlan.TopoSortByTableInputs([b], [b]);

        Assert.Equal(["b"], sorted.Select(t => t.Name));
    }

    [Fact]
    public void TopoSort_ToleratesACycle_ReturningEveryTableExactlyOnce()
    {
        // a ↔ b. The back edge is cut by the recursion stack, so this comes out in catalog order rather
        // than throwing or looping — the same behaviour as the Orleans original. Whatever order such a
        // group lands in, the entities that cannot start fail individually and the sweeps retry them; boot
        // is the wrong place to reject a catalog.
        var a = Table("a", PipelineStatus.Running, "b");
        var b = Table("b", PipelineStatus.Running, "a");

        var sorted = BootResumePlan.TopoSortByTableInputs([a, b], [a, b]);

        Assert.Equal(2, sorted.Count);
        Assert.Equal(["a", "b"], sorted.Select(t => t.Name).OrderBy(n => n, StringComparer.Ordinal));
    }

    [Fact]
    public void TopoSort_ToleratesASelfReference()
    {
        var a = Table("a", PipelineStatus.Running, "a");

        var sorted = BootResumePlan.TopoSortByTableInputs([a], [a]);

        Assert.Equal(["a"], sorted.Select(t => t.Name));
    }

    [Fact]
    public void TopoSort_ADiamondEmitsEachTableOnce_WithBothMiddlesBeforeTheSink()
    {
        // d reads b and c; both read a.
        var a = Table("a", PipelineStatus.Running);
        var b = Table("b", PipelineStatus.Running, "a");
        var c = Table("c", PipelineStatus.Running, "a");
        var d = Table("d", PipelineStatus.Running, "b", "c");
        var all = new List<TableDefinition> { d, c, b, a };

        var sorted = BootResumePlan.TopoSortByTableInputs(all, all);
        var order = sorted.Select(t => t.Name).ToList();

        Assert.Equal(4, order.Count);
        Assert.True(order.IndexOf("a") < order.IndexOf("b"));
        Assert.True(order.IndexOf("a") < order.IndexOf("c"));
        Assert.True(order.IndexOf("b") < order.IndexOf("d"));
        Assert.True(order.IndexOf("c") < order.IndexOf("d"));
    }
}

/// <summary>
/// Plan 025: unit tests for <see cref="BootGate"/> — the latch the four supervisor sweeps wait on before
/// their first pass. Exercised on a fresh INSTANCE rather than <see cref="BootGate.Shared"/>: the shared one
/// is process-wide by design (see that class's doc for why), and a test that completed it would leak into
/// every other test in this assembly.
/// </summary>
public class BootGateTests
{
    [Fact]
    public async Task WaitAsync_ReturnsTrueOnceTheBootPassCompletes()
    {
        var gate = new BootGate();
        var waiting = gate.WaitAsync(TimeSpan.FromSeconds(30), CancellationToken.None);

        Assert.False(gate.IsCompleted);
        gate.Complete();

        Assert.True(await waiting);
        Assert.True(gate.IsCompleted);
    }

    [Fact]
    public async Task WaitAsync_OnAnAlreadyCompletedGate_ReturnsImmediately()
    {
        var gate = new BootGate();
        gate.Complete();

        Assert.True(await gate.WaitAsync(TimeSpan.Zero, CancellationToken.None));
    }

    [Fact]
    public async Task WaitAsync_ReturnsFalseRatherThanThrowingWhenTheBootPassNeverCompletes()
    {
        // The bound is the whole point: a wedged boot pass must degrade the supervisors back to their
        // pre-plan-025 uncoordinated sweep, never disable self-healing for the life of the process.
        var gate = new BootGate();

        Assert.False(await gate.WaitAsync(TimeSpan.FromMilliseconds(20), CancellationToken.None));
    }

    [Fact]
    public void Complete_IsIdempotent()
    {
        var gate = new BootGate();
        gate.Complete();
        gate.Complete();

        Assert.True(gate.IsCompleted);
    }

    [Fact]
    public async Task WaitAsync_StillObservesCancellation_SoAShuttingDownHostUnwinds()
    {
        var gate = new BootGate();
        using var cts = new CancellationTokenSource();
        var waiting = gate.WaitAsync(TimeSpan.FromMinutes(5), cts.Token);

        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
    }
}
