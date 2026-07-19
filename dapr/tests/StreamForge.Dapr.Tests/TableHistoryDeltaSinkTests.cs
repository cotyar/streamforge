using StreamForge.Abstractions.Streaming;
using StreamForge.Dapr.Host.Streaming;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-B: unit tests for <see cref="TableHistoryEnabledMap"/> (the pure
/// enable-map dictionary) and <see cref="TableHistoryDeltaSink.OnTableDeltaAsync"/>'s SKIP path — the only
/// part of the sink testable without a live Dapr sidecar, since the "forward" path resolves a live
/// <c>ActorProxy&lt;ITableHistoryActor&gt;</c> and calls it, exactly the same constraint
/// <see cref="PipelineEventRouterTests"/>'s own class doc describes for
/// <c>PipelineEventRouter.OnSourceEventsAsync</c>'s fan-out path. A brand new
/// <see cref="TableHistoryEnabledMap"/> instance is constructed per test (NOT
/// <see cref="TableHistoryEnabledMap.Instance"/>, the shared process-wide singleton — sharing that one
/// across tests would leak state between them) so every test starts from a clean, empty map.
/// </summary>
public class TableHistoryDeltaSinkTests
{
    // ------------------------------------------------------------------
    // TableHistoryEnabledMap — pure dictionary semantics
    // ------------------------------------------------------------------

    [Fact]
    public void IsEnabled_UnknownTable_DefaultsFalse()
    {
        var map = new TableHistoryEnabledMap();

        Assert.False(map.IsEnabled("never-configured"));
    }

    [Fact]
    public void SetEnabled_RecordsCurrentValue_TrueOrFalse()
    {
        var map = new TableHistoryEnabledMap();

        map.SetEnabled("t1", true);
        Assert.True(map.IsEnabled("t1"));

        // A later Reset can just as well turn history OFF — SetEnabled always overwrites with the
        // current value, it never "sticks" at true once set (mirrors ResetTableHistoryAsync's doc
        // comment: "a Reset can just as easily turn history off as on").
        map.SetEnabled("t1", false);
        Assert.False(map.IsEnabled("t1"));
    }

    [Fact]
    public void Remove_ClearsEntryEntirely()
    {
        var map = new TableHistoryEnabledMap();
        map.SetEnabled("t1", true);

        map.Remove("t1");

        Assert.False(map.IsEnabled("t1"));
    }

    [Fact]
    public void Remove_UnknownTable_IsANoOp()
    {
        var map = new TableHistoryEnabledMap();

        map.Remove("never-configured");
        // No assertion beyond "didn't throw".
    }

    // ------------------------------------------------------------------
    // TableHistoryDeltaSink — the skip path (no live actor needed)
    // ------------------------------------------------------------------

    [Fact]
    public async Task OnTableDeltaAsync_TableNotInMap_SkipsForwardingAndCompletesWithoutASidecarCall()
    {
        var map = new TableHistoryEnabledMap();
        var sink = new TableHistoryDeltaSink(map);

        // If this forwarded to an ITableHistoryActor proxy, invoking it without a live Dapr sidecar
        // would throw HttpRequestException (see GeneratorLifecycleOrchestratorTests' updated doc
        // comment for the exact exception this project already observed for the analogous pipeline/
        // registry actor calls) — the fact that this completes cleanly proves the enable-map gate
        // short-circuits before any actor proxy is even constructed.
        await sink.OnTableDeltaAsync(new TableDeltaEnvelope { Table = "unconfigured-table", Seq = 1, Deltas = [] });
    }

    [Fact]
    public async Task OnTableDeltaAsync_TableExplicitlyDisabled_SkipsForwarding()
    {
        var map = new TableHistoryEnabledMap();
        map.SetEnabled("positions", false);
        var sink = new TableHistoryDeltaSink(map);

        await sink.OnTableDeltaAsync(new TableDeltaEnvelope { Table = "positions", Seq = 1, Deltas = [] });
    }
}
