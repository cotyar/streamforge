using Xunit;

namespace StreamsForge.Connectors.Fix.Tests;

/// <summary>
/// Plan 019 wave E: <see cref="FixDuplexSession.SendAsync"/> before any socket has ever been opened — no
/// <see cref="FixDuplexSession.SubscribeAsync"/> enumeration means <c>_app</c> is null, which is exactly
/// "not logged on" (see that class's own doc comment: one field, <c>FixBridgeApplication.ActiveSessionId</c>,
/// answers both "is there a socket" and "is it logged on"). That is what lets this whole file avoid opening
/// a socket at all — only <see cref="FixDuplexAcceptanceTests"/> does.
/// </summary>
public class FixDuplexSessionTests
{
    [Fact]
    public void IsReadyIsFalseBeforeAnyConnectionAttempt()
    {
        var session = new FixDuplexSession("fx-fresh", FixTestSupport.ValidConfig());
        Assert.False(session.IsReady);
    }

    [Fact]
    public async Task SendAsyncReportsFailureInsteadOfThrowingWhenNotLoggedOn()
    {
        var session = new FixDuplexSession("fx-not-connected", FixTestSupport.ValidConfig());

        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["MsgType"] = "D", ["ClOrdID"] = "ORD1", ["Symbol"] = "EUR/USD" },
        };

        var outcome = await session.SendAsync(rows, CancellationToken.None);

        Assert.Equal(0, outcome.Sent);
        Assert.Equal(1, outcome.Failed);
        var failure = Assert.Single(outcome.Failures);
        Assert.Equal("ORD1", failure.CorrelationId);
        Assert.Contains("not logged on", failure.Reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SendAsyncReportsAMappingFailureIndependentlyOfReadiness()
    {
        var session = new FixDuplexSession("fx-not-connected-2", FixTestSupport.ValidConfig());

        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["MsgType"] = "D", ["ClOrdID"] = "ORD1", ["NotARealFixField"] = "x" },
        };

        var outcome = await session.SendAsync(rows, CancellationToken.None);

        Assert.Equal(0, outcome.Sent);
        Assert.Equal(1, outcome.Failed);
        // The reason names the REAL problem (the column), not a generic "not ready" -- see SendAsync's own
        // doc comment for why mapping is checked before readiness.
        Assert.Contains("NotARealFixField", outcome.Failures[0].Reason);
    }

    [Fact]
    public async Task MultipleRowsInOneBatchEachReportTheirOwnFailure()
    {
        var session = new FixDuplexSession("fx-batch", FixTestSupport.ValidConfig());

        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["MsgType"] = "D", ["ClOrdID"] = "ORD1" },
            new() { ["MsgType"] = "D", ["ClOrdID"] = "ORD2" },
            new() { ["ClOrdID"] = "ORD3" }, // no MsgType -- a different failure reason
        };

        var outcome = await session.SendAsync(rows, CancellationToken.None);

        Assert.Equal(0, outcome.Sent);
        Assert.Equal(3, outcome.Failed);
        Assert.Equal(["ORD1", "ORD2", "ORD3"], outcome.Failures.Select(f => f.CorrelationId));
    }

    [Fact]
    public async Task CountersCountRowsNotCallsAndAreCumulativeForThisInstance()
    {
        var session = new FixDuplexSession("fx-counters", FixTestSupport.ValidConfig());

        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["MsgType"] = "D", ["ClOrdID"] = "ORD1" },
            new() { ["MsgType"] = "D", ["ClOrdID"] = "ORD2" },
            new() { ["MsgType"] = "D", ["ClOrdID"] = "ORD3" },
        };

        await session.SendAsync(rows, CancellationToken.None); // one CALL, three ROWS

        Assert.Equal(0, session.SentTotal);
        Assert.Equal(3, session.FailedTotal);
        Assert.NotNull(session.LastFailure);
        Assert.Equal("ORD3", session.LastFailure!.CorrelationId); // the most recent failure

        await session.SendAsync(rows, CancellationToken.None); // cumulative across calls, same instance
        Assert.Equal(6, session.FailedTotal);
    }

    [Fact]
    public async Task ANewInstanceStartsBackAtZeroEvenForTheSameSourceName()
    {
        // Plan 019's IDuplexSession.SentTotal doc: "scope: the life of THIS session instance, not the
        // source" -- a reconnect mints a brand-new object (SubscriberCore calls Open/OpenDuplex again),
        // which is what this test stands in for without actually reconnecting a socket.
        var first = new FixDuplexSession("fx-reconnect", FixTestSupport.ValidConfig());
        await first.SendAsync([new() { ["MsgType"] = "D", ["ClOrdID"] = "ORD1" }], CancellationToken.None);
        Assert.Equal(1, first.FailedTotal);

        var second = new FixDuplexSession("fx-reconnect", FixTestSupport.ValidConfig());
        Assert.Equal(0, second.FailedTotal);
        Assert.Equal(0, second.SentTotal);
        Assert.Null(second.LastFailure);
    }

    [Fact]
    public async Task EmptyBatchIsANoOp()
    {
        var session = new FixDuplexSession("fx-empty", FixTestSupport.ValidConfig());
        var outcome = await session.SendAsync([], CancellationToken.None);

        Assert.Equal(0, outcome.Sent);
        Assert.Equal(0, outcome.Failed);
        Assert.Equal(0, session.SentTotal);
        Assert.Equal(0, session.FailedTotal);
    }

    [Fact]
    public async Task DisposeIsSafeEvenWhenNeverPublishedOrConnected()
    {
        var session = new FixDuplexSession("fx-never-published", FixTestSupport.ValidConfig());
        await session.DisposeAsync(); // must not throw.
    }
}
