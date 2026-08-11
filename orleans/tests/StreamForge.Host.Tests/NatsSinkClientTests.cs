using StreamForge.Abstractions;
using StreamForge.AppCore.Sinks;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 009 B2: unit tests for <see cref="NatsSinkClient"/>'s fire-and-forget failure contract. There is
/// no NATS server in this environment (see the wave's own testing-reality note) or in CI, so these tests
/// exercise the REAL failure path against an address nothing is listening on
/// (<c>nats://127.0.0.1:1</c> — port 1 is never a NATS server, so the OS refuses the connection quickly)
/// rather than mocking the failure. What they pin: PublishAsync never throws, it counts+reports exactly
/// the failures it should, and it respects the log-throttle window — the three properties plan 009 B2
/// calls out as "a broken sink must not break the entity" and "visibly, with a counter and a log".
/// </summary>
public class NatsSinkClientTests
{
    private static NatsPubConfig UnreachableConfig(string subject = "sf.test") =>
        new() { Url = "nats://127.0.0.1:1", Subject = subject };

    [Fact]
    public async Task PublishAsync_AgainstAnUnreachableBroker_NeverThrows()
    {
        await using var client = new NatsSinkClient(UnreachableConfig(), "pipeline", "p1");

        // The whole point: no exception propagates to the caller, however badly the publish itself failed.
        await client.PublishAsync(new { value = 1 }, CancellationToken.None);
    }

    [Fact]
    public async Task PublishAsync_AgainstAnUnreachableBroker_CountsTheFailure()
    {
        await using var client = new NatsSinkClient(UnreachableConfig(), "pipeline", "p1");

        await client.PublishAsync(new { value = 1 }, CancellationToken.None);

        var counters = client.Counters;
        Assert.Equal(0, counters.Published);
        Assert.Equal(1, counters.Failed);
        Assert.NotNull(counters.LastError);
        Assert.True(counters.LastFailureAtMs > 0);
    }

    [Fact]
    public async Task PublishAsync_AgainstAnUnreachableBroker_ReportsTheFailureThroughTheCallback()
    {
        var reported = new List<(string Subject, Exception Ex)>();
        await using var client = new NatsSinkClient(
            UnreachableConfig("sf.reported"), "table", "t1", (subject, ex) => reported.Add((subject, ex)));

        await client.PublishAsync(new { value = 1 }, CancellationToken.None);

        var call = Assert.Single(reported);
        Assert.Equal("sf.reported", call.Subject);
    }

    [Fact]
    public async Task PublishAsync_RepeatedFailuresWithinTheThrottleWindow_ReportOnlyOnce()
    {
        var reportCount = 0;
        await using var client = new NatsSinkClient(
            UnreachableConfig(), "pipeline", "p1", (_, _) => Interlocked.Increment(ref reportCount));

        // Three failing publishes back-to-back, well inside NatsSinkClient.LogThrottleWindow (30s) —
        // only the first should reach the callback.
        await Task.WhenAll(
            client.PublishAsync(new { n = 1 }, CancellationToken.None),
            client.PublishAsync(new { n = 2 }, CancellationToken.None),
            client.PublishAsync(new { n = 3 }, CancellationToken.None));

        Assert.Equal(3, client.Counters.Failed);
        Assert.Equal(1, reportCount);
    }

    [Fact]
    public void ExpandsSubjectTemplateWithTheEntityName()
    {
        // EntitySpecific expansion is exercised indirectly (via the reported subject above using a
        // literal, non-templated subject) — this test pins the {name} substitution itself, independent
        // of any network call, via NatsConnectionSettings directly (the same helper NatsSinkClient uses).
        Assert.Equal("sf.pipeline.p1.out", StreamForge.AppCore.Nats.NatsConnectionSettings.ExpandSubject("sf.pipeline.{name}.out", "p1"));
    }
}
