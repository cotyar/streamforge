using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>Plan 008 W4: SourceIngressRegistry — one buffer per source name, rebuilt (not silently
/// kept stale) whenever the source's IngestConfig changes.</summary>
public class SourceIngressRegistryTests
{
    private static Func<IReadOnlyList<Dictionary<string, object?>>, CancellationToken, Task> NoopDrain
        => (_, _) => Task.CompletedTask;

    [Fact]
    public void GetOrCreate_returns_the_same_buffer_for_an_unchanged_config()
    {
        var registry = new SourceIngressRegistry();
        var config = new IngestConfig { Policy = IngressOverflowPolicy.Reject, CapacityRows = 100 };

        var first = registry.GetOrCreate("s1", config, NoopDrain);
        var second = registry.GetOrCreate("s1", config, NoopDrain);

        Assert.Same(first, second);
    }

    [Fact]
    public void GetOrCreate_rebuilds_the_buffer_when_capacity_changes()
    {
        var registry = new SourceIngressRegistry();
        var original = new IngestConfig { Policy = IngressOverflowPolicy.Reject, CapacityRows = 100 };
        var edited = new IngestConfig { Policy = IngressOverflowPolicy.Reject, CapacityRows = 200 };

        var first = registry.GetOrCreate("s1", original, NoopDrain);
        var second = registry.GetOrCreate("s1", edited, NoopDrain);

        Assert.NotSame(first, second);
        Assert.Equal(200, second.Config.CapacityRows);
    }

    [Fact]
    public void GetOrCreate_rebuilds_when_only_the_policy_changes()
    {
        var registry = new SourceIngressRegistry();
        var original = new IngestConfig { Policy = IngressOverflowPolicy.Reject, CapacityRows = 100 };
        var edited = new IngestConfig { Policy = IngressOverflowPolicy.DropOldest, CapacityRows = 100 };

        var first = registry.GetOrCreate("s1", original, NoopDrain);
        var second = registry.GetOrCreate("s1", edited, NoopDrain);

        Assert.NotSame(first, second);
        Assert.Equal(IngressOverflowPolicy.DropOldest, second.Config.Policy);
    }

    [Theory]
    [InlineData(nameof(IngestConfig.MaxWaitMs))]
    [InlineData(nameof(IngestConfig.MaxBatchRows))]
    [InlineData(nameof(IngestConfig.RejectUnknownFields))]
    public void GetOrCreate_rebuilds_when_any_fingerprinted_field_changes(string field)
    {
        var registry = new SourceIngressRegistry();
        var original = new IngestConfig { MaxWaitMs = 5000, MaxBatchRows = 1000, RejectUnknownFields = false };
        var edited = field switch
        {
            nameof(IngestConfig.MaxWaitMs) => new IngestConfig { MaxWaitMs = 6000, MaxBatchRows = 1000, RejectUnknownFields = false },
            nameof(IngestConfig.MaxBatchRows) => new IngestConfig { MaxWaitMs = 5000, MaxBatchRows = 2000, RejectUnknownFields = false },
            _ => new IngestConfig { MaxWaitMs = 5000, MaxBatchRows = 1000, RejectUnknownFields = true },
        };

        var first = registry.GetOrCreate("s1", original, NoopDrain);
        var second = registry.GetOrCreate("s1", edited, NoopDrain);

        Assert.NotSame(first, second);
    }

    [Fact]
    public void TryGet_returns_null_for_an_unknown_source()
    {
        var registry = new SourceIngressRegistry();

        Assert.Null(registry.TryGet("never-seen"));
    }

    [Fact]
    public void TryGet_returns_the_buffer_created_by_GetOrCreate()
    {
        var registry = new SourceIngressRegistry();

        var created = registry.GetOrCreate("s1", new IngestConfig(), NoopDrain);

        Assert.Same(created, registry.TryGet("s1"));
    }

    [Fact]
    public void Different_source_names_get_independent_buffers()
    {
        var registry = new SourceIngressRegistry();
        var config = new IngestConfig();

        var a = registry.GetOrCreate("a", config, NoopDrain);
        var b = registry.GetOrCreate("b", config, NoopDrain);

        Assert.NotSame(a, b);
    }

    [Fact]
    public void Remove_drops_the_buffer()
    {
        var registry = new SourceIngressRegistry();
        registry.GetOrCreate("s1", new IngestConfig(), NoopDrain);

        registry.Remove("s1");

        Assert.Null(registry.TryGet("s1"));
    }

    [Fact]
    public void Remove_of_an_unknown_source_is_a_noop()
    {
        var registry = new SourceIngressRegistry();

        registry.Remove("never-seen"); // must not throw
    }
}
