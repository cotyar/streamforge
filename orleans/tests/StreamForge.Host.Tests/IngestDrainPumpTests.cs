using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using StreamForge.Host.Services;

namespace StreamForge.Host.Tests;

/// <summary>Plan 008 W4. These exist because the wave originally shipped without any driver behind
/// <see cref="SourceIngressBuffer.DrainAsync"/> on this flavor: pushes were admitted, counted, and
/// answered 202 — and then sat in the buffer forever, because nothing ever drained it. Every
/// admission-side test passed throughout. So the assertion that matters here is not "the policy is
/// right", it is "something actually moves rows out of the buffer".</summary>
public class IngestDrainPumpTests
{
    private static SourceDefinition IngestSource(string name) => new()
    {
        Name = name,
        Kind = SourceKinds.Ingest,
        Enabled = true,
        Fields = [new FieldDef("symbol", FieldType.String)],
        Ingest = new IngestConfig { CapacityRows = 100, MaxBatchRows = 100 },
    };

    [Fact]
    public async Task Sweep_PublishesWhatAPushLeftQueued()
    {
        var registry = new SourceIngressRegistry();
        var published = new List<Dictionary<string, object?>>();
        var def = IngestSource("push_src");

        var buffer = registry.GetOrCreate("push_src", def.Ingest!, (rows, _) =>
        {
            published.AddRange(rows);
            return Task.CompletedTask;
        });
        var result = await buffer.PushAsync([new Dictionary<string, object?> { ["symbol"] = "AAPL" }]);

        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Empty(published);   // buffered, NOT published — this is the state the bug left behind
        Assert.Equal(1, buffer.GetStatus().DepthRows);

        await IngestDrainPumpService.SweepAsync([def], registry, NullLogger.Instance, CancellationToken.None);

        Assert.Single(published);
        Assert.Equal("AAPL", published[0]["symbol"]);
        Assert.Equal(0, buffer.GetStatus().DepthRows);
        Assert.Equal(1, buffer.GetStatus().TotalPublished);
    }

    [Fact]
    public async Task Sweep_DropsBuffersWhoseSourceIsGone()
    {
        var registry = new SourceIngressRegistry();
        var config = new IngestConfig { CapacityRows = 10, MaxBatchRows = 10 };
        registry.GetOrCreate("deleted_src", config, (_, _) => Task.CompletedTask);
        registry.GetOrCreate("live_src", config, (_, _) => Task.CompletedTask);

        await IngestDrainPumpService.SweepAsync([IngestSource("live_src")], registry, NullLogger.Instance, CancellationToken.None);

        Assert.Null(registry.TryGet("deleted_src"));
        Assert.NotNull(registry.TryGet("live_src"));
    }

    [Fact]
    public async Task Sweep_KeepsGoingWhenOneSourcesPublishThrows()
    {
        var registry = new SourceIngressRegistry();
        var config = new IngestConfig { CapacityRows = 10, MaxBatchRows = 10 };
        var goodPublished = 0;

        var bad = registry.GetOrCreate("bad_src", config, (_, _) => throw new InvalidOperationException("stream is down"));
        var good = registry.GetOrCreate("good_src", config, (rows, _) => { goodPublished += rows.Count; return Task.CompletedTask; });
        await bad.PushAsync([new Dictionary<string, object?> { ["symbol"] = "A" }]);
        await good.PushAsync([new Dictionary<string, object?> { ["symbol"] = "B" }]);

        await IngestDrainPumpService.SweepAsync(
            [IngestSource("bad_src"), IngestSource("good_src")], registry, NullLogger.Instance, CancellationToken.None);

        Assert.Equal(1, goodPublished);
    }

    [Fact]
    public async Task Sweep_IgnoresNonIngestKinds()
    {
        var registry = new SourceIngressRegistry();
        registry.GetOrCreate("gen_src", new IngestConfig(), (_, _) => Task.CompletedTask);

        var generator = new SourceDefinition { Name = "gen_src", Kind = SourceKinds.Generator, Enabled = true };
        await IngestDrainPumpService.SweepAsync([generator], registry, NullLogger.Instance, CancellationToken.None);

        // Not an ingest source, so its buffer is not ours to keep — the reconcile drops it.
        Assert.Null(registry.TryGet("gen_src"));
    }
}
