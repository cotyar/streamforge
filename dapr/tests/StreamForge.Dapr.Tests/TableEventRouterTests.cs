using Microsoft.Extensions.Logging.Abstractions;
using StreamForge.Abstractions.Streaming;
using StreamForge.Dapr.Host.Streaming;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-A: unit tests for <see cref="TableEventRouter"/>'s routing-table
/// logic (register/unregister/lookup, kind split, self-filter) — the actual dispatch methods
/// (<see cref="TableEventRouter.OnSourceEventsAsync"/>/<see cref="TableEventRouter.OnTableDeltaAsync"/>)
/// resolve a live <c>ITableActor</c> proxy per subscriber, which needs a Dapr sidecar to complete, so these
/// tests exercise the routing tables (<see cref="TableEventRouter.StreamSubscribersOf"/>/
/// <see cref="TableEventRouter.TableSubscribersOf"/>) and the pure self-filter
/// (<see cref="TableEventRouter.ExcludeSelf"/>) directly — mirroring how
/// <c>PipelineEventRouterTests</c> exercises the pure logic layer rather than the actor shell.
/// </summary>
public class TableEventRouterTests
{
    private static TableEventRouter NewRouter() => new(NullLogger<TableEventRouter>.Instance);

    [Fact]
    public void Register_SplitsSubscriptionsByKind_StreamVsTable()
    {
        var router = NewRouter();

        router.Register("hot_symbols", streamInputs: [], tableInputs: ["positions"]);
        router.Register("positions", streamInputs: ["trades"], tableInputs: []);

        Assert.Contains("positions", router.StreamSubscribersOf("trades"));
        Assert.Contains("hot_symbols", router.TableSubscribersOf("positions"));

        // The two indexes are genuinely split, not merged: "positions" never shows up as a STREAM
        // subscriber of "trades" via the table-input index, and "hot_symbols" never shows up as a table
        // subscriber of "trades" (it has no stream inputs at all).
        Assert.DoesNotContain("hot_symbols", router.StreamSubscribersOf("trades"));
        Assert.Empty(router.TableSubscribersOf("trades"));
        Assert.Empty(router.StreamSubscribersOf("positions"));
    }

    [Fact]
    public void Register_TableWithBothKindsOfInput_TrackedInBothIndexes()
    {
        var router = NewRouter();

        router.Register("mixed", streamInputs: ["trades"], tableInputs: ["positions"]);

        Assert.Contains("mixed", router.StreamSubscribersOf("trades"));
        Assert.Contains("mixed", router.TableSubscribersOf("positions"));
    }

    [Fact]
    public void Register_CalledAgainForSameTable_ReplacesItsPreviousSubscriptionSet()
    {
        var router = NewRouter();
        router.Register("t1", ["trades"], ["positions"]);

        router.Register("t1", ["quotes"], []);

        Assert.DoesNotContain("t1", router.StreamSubscribersOf("trades"));
        Assert.DoesNotContain("t1", router.TableSubscribersOf("positions"));
        Assert.Contains("t1", router.StreamSubscribersOf("quotes"));
    }

    [Fact]
    public void Register_EmptyInputSets_LeavesTableWithNoSubscriptions()
    {
        var router = NewRouter();
        router.Register("t1", ["trades"], ["positions"]);

        router.Register("t1", [], []);

        Assert.DoesNotContain("t1", router.StreamSubscribersOf("trades"));
        Assert.DoesNotContain("t1", router.TableSubscribersOf("positions"));
    }

    [Fact]
    public void Unregister_RemovesFromBothIndexes()
    {
        var router = NewRouter();
        router.Register("t1", ["trades"], ["positions"]);
        router.Register("t2", ["trades"], ["positions"]);

        router.Unregister("t1");

        Assert.DoesNotContain("t1", router.StreamSubscribersOf("trades"));
        Assert.DoesNotContain("t1", router.TableSubscribersOf("positions"));
        // t2's own subscriptions must survive t1's removal.
        Assert.Contains("t2", router.StreamSubscribersOf("trades"));
        Assert.Contains("t2", router.TableSubscribersOf("positions"));
    }

    [Fact]
    public void Unregister_UnknownTable_IsANoOp()
    {
        var router = NewRouter();

        router.Unregister("never-registered");
        // No assertion beyond "didn't throw".
    }

    [Fact]
    public async Task OnSourceEventsAsync_SourceWithNoSubscribers_CompletesWithoutError()
    {
        var router = NewRouter();

        await router.OnSourceEventsAsync(new SourceEventsEnvelope { Source = "unrouted" });
        // No assertion beyond "didn't throw".
    }

    [Fact]
    public async Task OnTableDeltaAsync_UpstreamWithNoSubscribers_CompletesWithoutError()
    {
        var router = NewRouter();

        await router.OnTableDeltaAsync(new TableDeltaEnvelope { Table = "unrouted" });
        // No assertion beyond "didn't throw".
    }

    // ------------------------------------------------------------------
    // ExcludeSelf — the pure self-filter OnTableDeltaAsync applies before dispatch (see class doc: "a
    // table must never receive its own output deltas"). Tested directly since the dispatch path itself
    // needs a live actor proxy to observe end-to-end.
    // ------------------------------------------------------------------

    [Fact]
    public void ExcludeSelf_RemovesOnlyTheUpstreamTablesOwnName()
    {
        var result = TableEventRouter.ExcludeSelf(["hot_symbols", "positions", "other"], "positions").ToList();

        Assert.DoesNotContain("positions", result);
        Assert.Contains("hot_symbols", result);
        Assert.Contains("other", result);
    }

    [Fact]
    public void ExcludeSelf_NoSelfPresent_ReturnsAllUnchanged()
    {
        var result = TableEventRouter.ExcludeSelf(["a", "b"], "positions").ToList();

        Assert.Equal(["a", "b"], result);
    }

    [Fact]
    public void ExcludeSelf_OnlySelfPresent_ReturnsEmpty()
    {
        var result = TableEventRouter.ExcludeSelf(["positions"], "positions").ToList();

        Assert.Empty(result);
    }
}
